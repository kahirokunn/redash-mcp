import { jest } from '@jest/globals';
import {
  BigQuerySchemaService,
  getBigQueryTableSchemaSchema,
  listBigQueryDatasetsSchema,
  listBigQueryTablesSchema,
  type SchemaDiscoveryClient,
} from '../bigQuerySchema.js';
import type { RedashQueryResult } from '../redashClient.js';

function queryResult(rows: Array<Record<string, unknown>>): RedashQueryResult {
  return {
    data: {
      columns: [],
      rows,
    },
  } as unknown as RedashQueryResult;
}

function createMockClient() {
  return {
    getDataSource: jest.fn<any>(),
    getDataSources: jest.fn<any>(),
    executeAdhocQuery: jest.fn<any>(),
    getSchema: jest.fn<any>(),
  };
}

describe('BigQuerySchemaService', () => {
  let client: ReturnType<typeof createMockClient>;
  let service: BigQuerySchemaService;

  beforeEach(() => {
    client = createMockClient();
    client.getDataSource.mockResolvedValue({
      id: 4,
      name: 'BigQuery',
      type: 'bigquery',
      options: {
        projectId: 'kanabell-prod',
        location: 'asia-northeast1',
      },
    });
    client.getDataSources.mockResolvedValue([]);
    service = new BigQuerySchemaService(client as unknown as SchemaDiscoveryClient);
  });

  it('blocks the unbounded Redash schema endpoint for BigQuery', async () => {
    await expect(service.getSchema(4)).rejects.toThrow(
      'Use list_bigquery_datasets, then list_bigquery_tables, and finally get_bigquery_table_schema'
    );
    expect(client.getSchema).not.toHaveBeenCalled();
  });

  it('keeps the existing schema endpoint for non-BigQuery data sources', async () => {
    client.getDataSource.mockResolvedValue({ id: 2, name: 'PostgreSQL', type: 'pg' });
    client.getSchema.mockResolvedValue({ schema: [] });

    await expect(service.getSchema(2)).resolves.toEqual({ schema: [] });
    expect(client.getSchema).toHaveBeenCalledWith(2);
  });

  it('falls back to the data source list when details omit the type', async () => {
    client.getDataSource.mockResolvedValue({ view_only: true });
    client.getDataSources.mockResolvedValue([
      { id: 4, name: 'BigQuery', type: 'bigquery_gce', view_only: true },
    ]);

    await expect(service.getSchema(4)).rejects.toThrow('Unbounded BigQuery schema retrieval is disabled');
    expect(client.getSchema).not.toHaveBeenCalled();
  });

  it('fails closed when the data source type cannot be determined', async () => {
    client.getDataSource.mockResolvedValue({ view_only: true });
    client.getDataSources.mockResolvedValue([]);

    await expect(service.getSchema(4)).rejects.toThrow(
      'Unable to determine data source 4 type; refusing schema discovery for safety'
    );
    expect(client.getSchema).not.toHaveBeenCalled();
  });

  it('lists one bounded page of datasets using Redash data source configuration', async () => {
    client.executeAdhocQuery.mockResolvedValue(queryResult([
      { catalog_name: 'kanabell-prod', schema_name: 'analytics', location: 'asia-northeast1' },
      { catalog_name: 'kanabell-prod', schema_name: 'raw', location: 'asia-northeast1' },
      { catalog_name: 'kanabell-prod', schema_name: 'staging', location: 'asia-northeast1' },
    ]));

    const input = listBigQueryDatasetsSchema.parse({ dataSourceId: '4', pageSize: '2' });
    const result = await service.listDatasets(input);

    expect(client.executeAdhocQuery).toHaveBeenCalledWith(
      expect.stringContaining('FROM `kanabell-prod`.`region-asia-northeast1`.INFORMATION_SCHEMA.SCHEMATA'),
      4
    );
    const query = client.executeAdhocQuery.mock.calls[0][0] as string;
    expect(query).toContain('LIMIT 3\nOFFSET 0');
    expect(result).toEqual({
      page: 1,
      pageSize: 2,
      hasMore: true,
      nextPage: 2,
      datasets: [
        { projectId: 'kanabell-prod', dataset: 'analytics', location: 'asia-northeast1' },
        { projectId: 'kanabell-prod', dataset: 'raw', location: 'asia-northeast1' },
      ],
    });
  });

  it('lists tables for only the requested dataset and page', async () => {
    client.executeAdhocQuery.mockResolvedValue(queryResult([
      {
        table_catalog: 'reporting-prod',
        table_schema: 'analytics',
        table_name: 'orders',
        table_type: 'BASE TABLE',
        creation_time: '2026-07-21T00:00:00Z',
      },
    ]));

    const input = listBigQueryTablesSchema.parse({
      dataSourceId: 4,
      projectId: 'reporting-prod',
      dataset: 'analytics',
      page: 2,
      pageSize: 2,
    });
    const result = await service.listTables(input);

    const query = client.executeAdhocQuery.mock.calls[0][0] as string;
    expect(query).toContain('FROM `reporting-prod`.`analytics`.INFORMATION_SCHEMA.TABLES');
    expect(query).toContain('LIMIT 3\nOFFSET 2');
    expect(result.hasMore).toBe(false);
    expect(result.nextPage).toBeNull();
    expect(result.tables[0]).toMatchObject({ name: 'orders', type: 'BASE TABLE' });
  });

  it('accepts Redash query_result wrappers and defaults an empty configured location to US', async () => {
    client.getDataSource.mockResolvedValue({
      id: 4,
      name: 'BigQuery',
      type: 'bigquery',
      options: { projectId: 'kanabell-prod', location: '' },
    });
    client.executeAdhocQuery.mockResolvedValue({
      query_result: queryResult([
        { catalog_name: 'kanabell-prod', schema_name: 'analytics', location: 'US' },
      ]),
    });

    const result = await service.listDatasets(listBigQueryDatasetsSchema.parse({ dataSourceId: 4 }));

    const query = client.executeAdhocQuery.mock.calls[0][0] as string;
    expect(query).toContain('FROM `kanabell-prod`.`region-us`.INFORMATION_SCHEMA.SCHEMATA');
    expect(result.datasets).toEqual([
      { projectId: 'kanabell-prod', dataset: 'analytics', location: 'US' },
    ]);
  });

  it('gets bounded column metadata for one table and escapes its name', async () => {
    client.executeAdhocQuery.mockResolvedValue(queryResult([
      {
        column_name: 'order_id',
        ordinal_position: 1,
        data_type: 'STRING',
        is_nullable: 'NO',
        is_partitioning_column: 'YES',
        clustering_ordinal_position: null,
      },
    ]));

    const input = getBigQueryTableSchemaSchema.parse({
      dataSourceId: 4,
      dataset: 'analytics',
      table: "order's",
      pageSize: 100,
    });
    const result = await service.getTableSchema(input);

    const query = client.executeAdhocQuery.mock.calls[0][0] as string;
    expect(query).toContain('FROM `kanabell-prod`.`analytics`.INFORMATION_SCHEMA.COLUMNS');
    expect(query).toContain("WHERE table_name = 'order\\'s'");
    expect(query).toContain('LIMIT 101\nOFFSET 0');
    expect(result.columns).toEqual([
      {
        name: 'order_id',
        position: 1,
        type: 'STRING',
        nullable: false,
        isPartitioningColumn: true,
        clusteringPosition: null,
      },
    ]);
  });

  it('rejects BigQuery-specific discovery for a different data source type', async () => {
    client.getDataSource.mockResolvedValue({ id: 2, name: 'MySQL', type: 'mysql' });
    const input = listBigQueryTablesSchema.parse({
      dataSourceId: 2,
      dataset: 'analytics',
    });

    await expect(service.listTables(input)).rejects.toThrow('is not a BigQuery data source');
    expect(client.executeAdhocQuery).not.toHaveBeenCalled();
  });

  it('rejects unsafe datasets and oversized pages before executing a query', () => {
    expect(() => listBigQueryTablesSchema.parse({
      dataSourceId: 4,
      dataset: 'analytics` UNION ALL SELECT secret',
    })).toThrow();
    expect(() => listBigQueryDatasetsSchema.parse({
      dataSourceId: 4,
      pageSize: 101,
    })).toThrow();
  });
});
