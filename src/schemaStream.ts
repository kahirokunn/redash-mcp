import type { Readable } from 'node:stream';
import chain from 'stream-chain';
import { parser } from 'stream-json';
import type { Token } from 'stream-json/parser.js';
import { pick } from 'stream-json/filters/pick.js';
import { streamArray } from 'stream-json/streamers/stream-array.js';
import { destroyQuietly } from './utils.js';

export interface SchemaTable {
  name: string;
  description?: string | null;
  columns: Array<{
    name: string;
    type: string;
    description?: string | null;
  }>;
}

export interface RedashSchemaPage {
  page: number;
  pageSize: number;
  hasMore: boolean;
  nextPage: number | null;
  schema: SchemaTable[];
}

export interface ReadSchemaPageOptions {
  page: number;
  pageSize: number;
  search?: string;
  deadlineMs: number;
}

// Reads one page of tables from a Redash schema response body without ever
// materializing the whole document: the body is parsed incrementally and the
// stream is destroyed as soon as the requested page is complete, which also
// aborts the underlying HTTP transfer.
export async function readSchemaPage(
  source: Readable,
  options: ReadSchemaPageOptions,
): Promise<RedashSchemaPage> {
  const { page, pageSize, search, deadlineMs } = options;
  const searchLower = search?.toLowerCase();
  const offset = (page - 1) * pageSize;

  // Distinguishes {"schema": []} (valid, empty) from an error payload such as
  // {"message": "..."} that lacks the schema key entirely.
  let sawSchemaKey = false;

  const pipeline = chain([
    source,
    // streamArray's assembler only reads packed values (keyValue/stringValue),
    // so the chunk-wise value tokens would be generated only to be discarded.
    parser({ streamValues: false }),
    pick({ filter: 'schema' }),
    (token: Token) => {
      sawSchemaKey = true;
      return token;
    },
    streamArray(),
  ]);

  // The axios timeout only covers time-to-first-response for streamed bodies,
  // so consumption needs its own deadline.
  const deadline = setTimeout(() => {
    pipeline.destroy(new Error(
      `Timed out reading schema response after ${deadlineMs}ms; raise REDASH_TIMEOUT for very large schemas`,
    ));
  }, deadlineMs);

  const collected: SchemaTable[] = [];
  let matched = 0;
  let hasMore = false;
  try {
    for await (const entry of pipeline as AsyncIterable<{ key: number; value: unknown }>) {
      const value = entry.value as SchemaTable;
      if (searchLower !== undefined) {
        const name = typeof value?.name === 'string' ? value.name : '';
        if (!name.toLowerCase().includes(searchLower)) {
          continue;
        }
      }
      matched += 1;
      if (matched <= offset) {
        continue;
      }
      if (collected.length === pageSize) {
        hasMore = true;
        break;
      }
      collected.push(value);
    }
  } finally {
    clearTimeout(deadline);
    destroyQuietly(pipeline);
    // stream-chain does not reliably propagate destroy back to its input, and
    // destroying the source is what actually aborts the HTTP transfer.
    destroyQuietly(source);
  }

  if (!sawSchemaKey) {
    throw new Error('Redash schema response did not contain a "schema" array');
  }

  return {
    page,
    pageSize,
    hasMore,
    nextPage: hasMore ? page + 1 : null,
    schema: collected,
  };
}
