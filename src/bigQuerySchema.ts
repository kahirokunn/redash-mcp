import { z } from 'zod';
import type {
  RedashDataSource,
  RedashDataSourceDetails,
  RedashQueryResult,
  RedashSchema,
} from './redashClient.js';

const DEFAULT_PAGE_SIZE = 25;
const MAX_PAGE_SIZE = 100;
const MAX_PAGE = 1_000_000;

const pageSchema = z.coerce.number().int().min(1).max(MAX_PAGE).default(1)
  .describe('Page number (starts at 1)');
const pageSizeSchema = z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(DEFAULT_PAGE_SIZE)
  .describe('Number of results per page (maximum 100)');
const dataSourceIdSchema = z.coerce.number().int().positive()
  .describe('ID of the Redash BigQuery data source');
const projectIdSchema = z.string().min(1).max(256).regex(
  /^[A-Za-z0-9][A-Za-z0-9:._-]*$/,
  'projectId may contain only letters, numbers, colon, dot, underscore, and hyphen'
).describe('BigQuery project ID; defaults to the project configured on the Redash data source');
const locationSchema = z.string().min(1).max(128).regex(
  /^(?:region-)?[A-Za-z0-9_-]+$/,
  'location may contain only letters, numbers, underscore, and hyphen'
).describe('BigQuery location, such as asia-northeast1, EU, or US');
const datasetSchema = z.string().min(1).max(1024).regex(
  /^[A-Za-z0-9_]+$/,
  'dataset may contain only letters, numbers, and underscore'
).describe('BigQuery dataset to inspect, for example analytics');
const tableSchema = z.string().min(1).max(1024).refine(
  (value) => !/[\u0000-\u001F\u007F]/.test(value),
  'table must not contain control characters'
).describe('BigQuery table to inspect, for example orders');

const sharedPageFields = {
  dataSourceId: dataSourceIdSchema,
  projectId: projectIdSchema.optional(),
  page: pageSchema,
  pageSize: pageSizeSchema,
};

export const listBigQueryDatasetsSchema = z.object({
  ...sharedPageFields,
  location: locationSchema.optional(),
});

export const listBigQueryTablesSchema = z.object({
  ...sharedPageFields,
  dataset: datasetSchema,
});

export const getBigQueryTableSchemaSchema = z.object({
  ...sharedPageFields,
  dataset: datasetSchema,
  table: tableSchema,
});

type ListBigQueryDatasetsInput = z.infer<typeof listBigQueryDatasetsSchema>;
type ListBigQueryTablesInput = z.infer<typeof listBigQueryTablesSchema>;
type GetBigQueryTableSchemaInput = z.infer<typeof getBigQueryTableSchemaSchema>;

interface BigQueryDatasetEntry {
  projectId: string;
  dataset: string;
  location: string;
}

interface BigQueryTableEntry {
  projectId: string;
  dataset: string;
  name: string;
  type: string;
  createdAt: unknown;
}

interface BigQueryColumnEntry {
  name: string;
  position: number | null;
  type: string;
  nullable: boolean;
  isPartitioningColumn: boolean;
  clusteringPosition: number | null;
}

export interface SchemaDiscoveryClient {
  getDataSource(dataSourceId: number): Promise<RedashDataSourceDetails>;
  getDataSources(): Promise<RedashDataSource[]>;
  executeAdhocQuery(query: string, dataSourceId: number): Promise<RedashQueryResult>;
  getSchema(dataSourceId: number): Promise<RedashSchema>;
}

class UnboundedBigQuerySchemaError extends Error {
  constructor() {
    super(
      'Unbounded BigQuery schema retrieval is disabled because it can exhaust Redash server memory. ' +
      'Use list_bigquery_datasets, then list_bigquery_tables, and finally get_bigquery_table_schema.'
    );
    this.name = 'UnboundedBigQuerySchemaError';
  }
}

// Exact type identifiers emitted by Redash's query-runner registry for BigQuery.
const BIGQUERY_TYPES = new Set(['bigquery', 'bigquery_gce']);

function isBigQueryType(type: string): boolean {
  return BIGQUERY_TYPES.has(type.toLowerCase());
}

function quoteInformationSchemaQualifier(parts: string[]): string {
  if (parts.some((part) => part.includes('`'))) {
    throw new Error('BigQuery INFORMATION_SCHEMA qualifier must not contain backticks');
  }
  return parts.map((part) => `\`${part}\``).join('.');
}

function escapeBigQueryString(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function normalizeLocation(location: string): string {
  const validated = locationSchema.parse(location);
  return validated.replace(/^region-/i, '').toLowerCase();
}

function paginationSql(page: number, pageSize: number): string {
  const offset = (page - 1) * pageSize;
  return `LIMIT ${pageSize + 1}\nOFFSET ${offset}`;
}

function extractRows(result: RedashQueryResult): Array<Record<string, unknown>> {
  const response = result as unknown as {
    query_result?: RedashQueryResult;
    data?: { rows?: Array<Record<string, unknown>> };
  };
  const rows = response.query_result?.data?.rows ?? response.data?.rows;
  if (!Array.isArray(rows)) {
    throw new Error('Redash returned a query result without data.rows');
  }
  return rows;
}

function pageResult<T>(rows: T[], page: number, pageSize: number) {
  const hasMore = rows.length > pageSize;
  return {
    page,
    pageSize,
    hasMore,
    nextPage: hasMore ? page + 1 : null,
    items: rows.slice(0, pageSize),
  };
}

function stringValue(value: unknown): string {
  return value === null || value === undefined ? '' : String(value);
}

function nullableNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

type ResolvedDataSource = RedashDataSourceDetails & { type: string };

export class BigQuerySchemaService {
  // A data source's type and options are effectively immutable for the process
  // lifetime, so resolve each data source once instead of paying an extra
  // Redash round-trip on every tool call.
  private readonly dataSourceCache = new Map<number, Promise<ResolvedDataSource>>();

  constructor(private readonly client: SchemaDiscoveryClient) {}

  private resolveDataSource(dataSourceId: number): Promise<ResolvedDataSource> {
    const cached = this.dataSourceCache.get(dataSourceId);
    if (cached) {
      return cached;
    }
    const pending = this.lookupDataSource(dataSourceId);
    this.dataSourceCache.set(dataSourceId, pending);
    pending.catch(() => this.dataSourceCache.delete(dataSourceId));
    return pending;
  }

  private async lookupDataSource(dataSourceId: number): Promise<ResolvedDataSource> {
    let details: RedashDataSourceDetails = {};
    try {
      details = await this.client.getDataSource(dataSourceId);
      if (details.type) {
        return { ...details, type: details.type };
      }
    } catch {
      // A non-admin Redash user might not be allowed to read full details.
      // Fall back to the list endpoint, which still includes the data source type.
    }

    try {
      const listed = (await this.client.getDataSources()).find((dataSource) => dataSource.id === dataSourceId);
      if (listed?.type) {
        return {
          ...listed,
          ...details,
          type: listed.type,
          options: details.options ?? listed.options,
        };
      }
    } catch {
      // Report one stable, safety-oriented error below.
    }

    throw new Error(
      `Unable to determine data source ${dataSourceId} type; refusing schema discovery for safety`
    );
  }

  private async resolveBigQueryDataSource(dataSourceId: number): Promise<ResolvedDataSource> {
    const dataSource = await this.resolveDataSource(dataSourceId);
    if (!isBigQueryType(dataSource.type)) {
      throw new Error(`Data source ${dataSourceId} is not a BigQuery data source`);
    }
    return dataSource;
  }

  private resolveProjectId(inputProjectId: string | undefined, dataSource: RedashDataSourceDetails): string | undefined {
    const projectId = inputProjectId ?? dataSource.options?.projectId;
    return projectId ? projectIdSchema.parse(projectId) : undefined;
  }

  async getSchema(dataSourceId: number): Promise<RedashSchema> {
    const dataSource = await this.resolveDataSource(dataSourceId);
    if (isBigQueryType(dataSource.type)) {
      throw new UnboundedBigQuerySchemaError();
    }
    return this.client.getSchema(dataSourceId);
  }

  async listDatasets(input: ListBigQueryDatasetsInput) {
    const dataSource = await this.resolveBigQueryDataSource(input.dataSourceId);
    const projectId = this.resolveProjectId(input.projectId, dataSource);
    const location = normalizeLocation(input.location || dataSource.options?.location || 'US');
    const qualifier = quoteInformationSchemaQualifier([
      ...(projectId ? [projectId] : []),
      `region-${location}`,
    ]);
    const query = [
      'SELECT catalog_name, schema_name, location',
      `FROM ${qualifier}.INFORMATION_SCHEMA.SCHEMATA`,
      'ORDER BY schema_name',
      paginationSql(input.page, input.pageSize),
    ].join('\n');

    const rows = extractRows(await this.client.executeAdhocQuery(query, input.dataSourceId));
    const datasets: BigQueryDatasetEntry[] = rows.map((row) => ({
      projectId: stringValue(row.catalog_name),
      dataset: stringValue(row.schema_name),
      location: stringValue(row.location),
    }));
    const { items, ...pageInfo } = pageResult(datasets, input.page, input.pageSize);
    return { ...pageInfo, datasets: items };
  }

  async listTables(input: ListBigQueryTablesInput) {
    const dataSource = await this.resolveBigQueryDataSource(input.dataSourceId);
    const projectId = this.resolveProjectId(input.projectId, dataSource);
    const { dataset } = input;
    const qualifier = quoteInformationSchemaQualifier([
      ...(projectId ? [projectId] : []),
      dataset,
    ]);
    const query = [
      'SELECT table_catalog, table_schema, table_name, table_type, creation_time',
      `FROM ${qualifier}.INFORMATION_SCHEMA.TABLES`,
      'ORDER BY table_name',
      paginationSql(input.page, input.pageSize),
    ].join('\n');

    const rows = extractRows(await this.client.executeAdhocQuery(query, input.dataSourceId));
    const tables: BigQueryTableEntry[] = rows.map((row) => ({
      projectId: stringValue(row.table_catalog),
      dataset: stringValue(row.table_schema),
      name: stringValue(row.table_name),
      type: stringValue(row.table_type),
      createdAt: row.creation_time ?? null,
    }));
    const { items, ...pageInfo } = pageResult(tables, input.page, input.pageSize);
    return { dataset, ...pageInfo, tables: items };
  }

  async getTableSchema(input: GetBigQueryTableSchemaInput) {
    const dataSource = await this.resolveBigQueryDataSource(input.dataSourceId);
    const projectId = this.resolveProjectId(input.projectId, dataSource);
    const { dataset, table } = input;
    const qualifier = quoteInformationSchemaQualifier([
      ...(projectId ? [projectId] : []),
      dataset,
    ]);
    const query = [
      'SELECT column_name, ordinal_position, data_type, is_nullable,',
      '       is_partitioning_column, clustering_ordinal_position',
      `FROM ${qualifier}.INFORMATION_SCHEMA.COLUMNS`,
      `WHERE table_name = '${escapeBigQueryString(table)}'`,
      'ORDER BY ordinal_position',
      paginationSql(input.page, input.pageSize),
    ].join('\n');

    const rows = extractRows(await this.client.executeAdhocQuery(query, input.dataSourceId));
    const columns: BigQueryColumnEntry[] = rows.map((row) => ({
      name: stringValue(row.column_name),
      position: nullableNumber(row.ordinal_position),
      type: stringValue(row.data_type),
      nullable: stringValue(row.is_nullable).toUpperCase() === 'YES',
      isPartitioningColumn: stringValue(row.is_partitioning_column).toUpperCase() === 'YES',
      clusteringPosition: nullableNumber(row.clustering_ordinal_position),
    }));
    const { items, ...pageInfo } = pageResult(columns, input.page, input.pageSize);
    return { dataset, table, ...pageInfo, columns: items };
  }
}
