/**
 * Test and development artifact producer, imported by path from the worker's
 * tests and from `scripts/`. It is not the production encoder, and nothing
 * that serves real data may mint artifacts here. Code outside this package
 * depends on its exports.
 *
 * It emits the workspace in `WORKSPACE_CONTRACT`, and through `encodeArtifact`
 * other schemas of int64, f64, timestamp, bool, utf8, json, dict and uuid
 * columns, each nullable or not. Where the format is observable it matches the
 * production encoder: dictionary entries ascend by UTF-8 bytes and a null
 * row's code is 0, a utf8 or json column's lengths part holds a 0 and then each
 * row's value length, and a table carries no primary-key index and refuses a
 * repeated key.
 *
 * It imports `node:crypto`, so keep it out of browser code. Like the decoder,
 * it assumes a little-endian runtime.
 */
import { createHash } from "node:crypto";
import {
  DIRECT_COLUMNAR_FORMAT_VERSION,
  type DirectColumnarArtifactIdentity,
  type DirectColumnarColumnKind,
  type DirectColumnarSchema,
  type TrustedArtifactChunks,
} from "./artifact.js";

const MAGIC = new TextEncoder().encode("CALQART1");
const textEncoder = new TextEncoder();
const align8 = (value: number) => (value + 7) & ~7;
const NUMERIC_BLOCK_ROWS = 1_024;

/**
 * The code a null row is spooled with: outside any dictionary, so the remap
 * makes it 0.
 */
const NULL_CODE = 0xffff_ffff;

/** `nullable-dict` is the only nullable column shape the fixtures need. */
type FixtureColumnType = DirectColumnarColumnKind | "nullable-dict";

type FixtureRow = Record<string, unknown>;

export interface EncodedArtifact {
  readonly bytes: Uint8Array;
  readonly byteLength: number;
  readonly digest: `sha256:${string}`;
  readonly identity: DirectColumnarArtifactIdentity;
  readonly rowCount: number;
}

const WORKSPACE_SCHEMA: Readonly<
  Record<string, Readonly<Record<string, FixtureColumnType>>>
> = {
  events: {
    id: "int64",
    organization_id: "dict",
    project_id: "dict",
    issue_id: "nullable-dict",
    level: "dict",
    message: "utf8",
    occurred_at: "timestamp",
    payload: "json",
  },
  issues: {
    id: "dict",
    organization_id: "dict",
    project_id: "dict",
    assignee_id: "nullable-dict",
    status: "dict",
    priority: "f64",
    title: "utf8",
    event_count: "f64",
    updated_at: "timestamp",
  },
  comments: {
    id: "dict",
    issue_id: "dict",
    author_id: "dict",
    body: "utf8",
    created_at: "timestamp",
  },
  issue_tags: { issue_id: "dict", tag: "dict" },
  users: { id: "dict", display_name: "utf8", email: "utf8", active: "bool" },
  projects: {
    id: "dict",
    organization_id: "dict",
    slug: "utf8",
    name: "utf8",
  },
  memberships: { organization_id: "dict", user_id: "dict", role: "dict" },
};

const PRIMARY_KEYS: Readonly<Record<string, readonly string[]>> = {
  events: ["id"],
  issues: ["id"],
  comments: ["id"],
  issue_tags: ["issue_id", "tag"],
  users: ["id"],
  projects: ["id"],
  memberships: ["organization_id", "user_id"],
};

const columnSpec = (
  type: FixtureColumnType,
): { readonly type: DirectColumnarColumnKind; readonly nullable: boolean } =>
  type === "nullable-dict"
    ? { type: "dict", nullable: true }
    : { type, nullable: false };

/**
 * Built in the exact key order `parseDirectColumnarSchema` reconstructs, so the
 * identity's `schemaHash` matches what the decoder recomputes.
 */
export const WORKSPACE_CONTRACT: DirectColumnarSchema = Object.freeze({
  format: "calque-direct-columnar",
  version: DIRECT_COLUMNAR_FORMAT_VERSION,
  tables: Object.freeze(
    Object.entries(WORKSPACE_SCHEMA).map(([name, columns]) =>
      Object.freeze({
        name,
        primaryKey: Object.freeze([...(PRIMARY_KEYS[name] ?? [])]),
        columns: Object.freeze(
          Object.entries(columns).map(([columnName, type]) =>
            Object.freeze({ name: columnName, ...columnSpec(type) }),
          ),
        ),
      }),
    ),
  ),
});

/**
 * Default identity for minted fixtures. Exported so a consumer that wants the
 * default rather than its own can bind against the same values.
 */
export const FIXTURE_ARTIFACT_IDENTITY: DirectColumnarArtifactIdentity =
  Object.freeze({
    sourceEpoch: "fixture-source-epoch-1",
    manifestVersion: "fixture-manifest-1",
    collectionSetHash: `sha256:${"1".repeat(64)}`,
    schemaHash: fixtureSchemaHash(WORKSPACE_CONTRACT),
    authorizationScopeVersion: "fixture-scope-1",
    authorizationScopeHash: `sha256:${"3".repeat(64)}`,
    cursor: "fixture-cursor-1",
  });

function concat(chunks: readonly Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function u32Bytes(values: ArrayLike<number>): Uint8Array {
  const bytes = new Uint8Array(values.length * 4);
  new Uint32Array(bytes.buffer).set(values);
  return bytes;
}

function f64Bytes(values: readonly number[]): Uint8Array {
  const bytes = new Uint8Array(values.length * 8);
  new Float64Array(bytes.buffer).set(values);
  return bytes;
}

function i64Bytes(values: readonly bigint[]): Uint8Array {
  const bytes = new Uint8Array(values.length * 8);
  new BigInt64Array(bytes.buffer).set(values);
  return bytes;
}

function bitWidth(maximum: bigint): number {
  return maximum === 0n ? 0 : maximum.toString(2).length;
}

function packValues(values: readonly bigint[], width: number): Uint8Array {
  const bytes = new Uint8Array(Math.ceil((values.length * width) / 8));
  for (let index = 0; index < values.length; index++) {
    let value = values[index] ?? 0n;
    const bitOffset = index * width;
    let byteOffset = Math.floor(bitOffset / 8);
    let shift = bitOffset % 8;
    let remaining = width;
    while (remaining > 0) {
      const take = Math.min(8 - shift, remaining);
      const mask = (1n << BigInt(take)) - 1n;
      bytes[byteOffset] =
        (bytes[byteOffset] ?? 0) | Number((value & mask) << BigInt(shift));
      value >>= BigInt(take);
      remaining -= take;
      byteOffset++;
      shift = 0;
    }
  }
  return bytes;
}

function encodeExactNumeric(
  values: readonly bigint[],
  timestamp: boolean,
): { readonly parts: readonly Uint8Array[]; readonly packed: boolean } {
  const raw = i64Bytes(values);
  if (values.length === 0) return { parts: [raw], packed: false };
  const metadata = new Uint8Array(
    Math.ceil(values.length / NUMERIC_BLOCK_ROWS) * 16,
  );
  const view = new DataView(metadata.buffer);
  const data: Uint8Array[] = [];
  let dataLength = 0;
  for (
    let start = 0, block = 0;
    start < values.length;
    start += NUMERIC_BLOCK_ROWS, block++
  ) {
    const source = values.slice(start, start + NUMERIC_BLOCK_ROWS);
    const scale =
      timestamp && source.every((value) => value % 1_000n === 0n) ? 1_000n : 1n;
    const scaled = source.map((value) => value / scale);
    const base = scaled.reduce((minimum, value) =>
      value < minimum ? value : minimum,
    );
    const deltas = scaled.map((value) => value - base);
    const width = bitWidth(
      deltas.reduce(
        (maximum, value) => (value > maximum ? value : maximum),
        0n,
      ),
    );
    const packed = packValues(deltas, width);
    const rawBlock = i64Bytes(source);
    const usePacked = packed.byteLength < rawBlock.byteLength;
    const selected = usePacked ? packed : rawBlock;
    const selectedWidth = usePacked ? width : 64;
    const header = block * 16;
    view.setUint32(header, dataLength, true);
    view.setBigInt64(header + 4, usePacked ? base : 0n, true);
    view.setUint8(header + 12, selectedWidth);
    view.setUint8(header + 13, usePacked && scale === 1_000n ? 3 : 0);
    data.push(selected);
    dataLength += selected.byteLength;
  }
  const packedData = concat(data);
  return metadata.byteLength + packedData.byteLength < raw.byteLength
    ? { parts: [metadata, packedData], packed: true }
    : { parts: [raw], packed: false };
}

function utf8Parts(values: readonly string[]): [Uint8Array, Uint8Array] {
  const offsets = new Uint32Array(values.length + 1);
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (const [index, value] of values.entries()) {
    const chunk = textEncoder.encode(value);
    chunks.push(chunk);
    length += chunk.byteLength;
    if (length > 0xffff_ffff)
      throw new Error("one UTF-8 column exceeds the u32 offset limit");
    offsets[index + 1] = length;
  }
  return [u32Bytes(offsets), concat(chunks)];
}

/** A variable-width column's parts: 0, each value's length, then the values. */
function valueLengthParts(values: readonly string[]): [Uint8Array, Uint8Array] {
  const lengths = new Uint32Array(values.length + 1);
  const chunks = values.map((value, index) => {
    const chunk = textEncoder.encode(value);
    lengths[index + 1] = chunk.byteLength;
    return chunk;
  });
  const data = concat(chunks);
  if (data.byteLength > 0xffff_ffff)
    throw new Error("one UTF-8 column exceeds the u32 offset limit");
  return [u32Bytes(lengths), data];
}

/**
 * Unsigned byte order over the UTF-8 encodings, as the production encoder
 * sorts. A UTF-16 string comparison would order surrogate pairs before
 * U+E000..U+FFFF.
 */
function compareUtf8(left: string, right: string): number {
  const a = textEncoder.encode(left);
  const b = textEncoder.encode(right);
  const shared = Math.min(a.byteLength, b.byteLength);
  for (let index = 0; index < shared; index++) {
    const difference = (a[index] ?? 0) - (b[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return a.byteLength - b.byteLength;
}

interface EncodedDictionary {
  readonly count: number;
  readonly bitWidth: number;
  readonly parts: readonly [Uint8Array, Uint8Array, Uint8Array];
}

/**
 * Interns in first-seen order while scanning rows, then sorts and rewrites the
 * codes through the `first_seen -> sorted` remap. `sortDictionaries: false`
 * stops before the remap, so a test can build an artifact that violates the
 * sorted-dictionary rule.
 */
function encodeDictionary(
  values: readonly (string | null)[],
  sortDictionaries: boolean,
): EncodedDictionary {
  const firstSeen = new Map<string, number>();
  const spooled = new Uint32Array(values.length);
  for (const [index, value] of values.entries()) {
    if (value === null) {
      spooled[index] = NULL_CODE;
      continue;
    }
    let code = firstSeen.get(value);
    if (code === undefined) {
      code = firstSeen.size;
      firstSeen.set(value, code);
    }
    spooled[index] = code;
  }

  const entries = [...firstSeen];
  const sorted = sortDictionaries
    ? entries.sort(([left], [right]) => compareUtf8(left, right))
    : entries;
  const remap = new Uint32Array(sorted.length);
  const dictionary = sorted.map(([value, firstSeenCode], sortedCode) => {
    remap[firstSeenCode] = sortedCode;
    return value;
  });

  const codes = new Uint32Array(values.length);
  for (let index = 0; index < codes.length; index++) {
    // A code outside `remap` is the null sentinel, or any code at all when the
    // dictionary is empty, and becomes 0.
    codes[index] = remap[spooled[index] ?? NULL_CODE] ?? 0;
  }
  const [offsets, data] = utf8Parts(dictionary);
  const width = bitWidth(BigInt(Math.max(0, dictionary.length - 1)));
  return {
    count: dictionary.length,
    bitWidth: width,
    parts: [
      packValues(
        Array.from(codes, (value) => BigInt(value)),
        width,
      ),
      offsets,
      data,
    ],
  };
}

interface EncodedColumn {
  readonly kind: DirectColumnarColumnKind;
  readonly nullable: boolean;
  readonly dictionaryCount: number | null;
  readonly bitWidth: number | null;
  readonly numericPacked: boolean;
  readonly parts: readonly Uint8Array[];
}

/**
 * An ISO string, which carries whole milliseconds, or a bigint count of
 * microseconds, which is how a test reaches sub-millisecond values and the
 * edges of the encoded range.
 */
function timestampMicros(value: unknown): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value !== "string") throw new Error("invalid timestamp value");
  const millis = Date.parse(value);
  if (!Number.isFinite(millis)) throw new Error("invalid timestamp value");
  return BigInt(millis) * 1_000n;
}

function encodeColumn(
  type: DirectColumnarColumnKind,
  nullable: boolean,
  values: readonly unknown[],
  sortDictionaries: boolean,
): EncodedColumn {
  const validity = nullable
    ? new Uint8Array(Math.ceil(values.length / 8))
    : undefined;
  const present = values.map((value, index) => {
    if (value === null || value === undefined) {
      if (!nullable) throw new Error(`non-null column contains null: ${type}`);
      return null;
    }
    if (validity)
      validity[index >>> 3] = (validity[index >>> 3] ?? 0) | (1 << (index & 7));
    return value;
  });

  let dictionaryCount: number | null = null;
  let packedWidth: number | null = null;
  let numericPacked = false;
  let parts: readonly Uint8Array[];
  switch (type) {
    case "int64":
      {
        const numeric = encodeExactNumeric(
          present.map((value) =>
            value === null ? 0n : BigInt(value as bigint | number),
          ),
          false,
        );
        parts = numeric.parts;
        numericPacked = numeric.packed;
      }
      break;
    case "f64":
      parts = [f64Bytes(present.map((value) => Number(value ?? 0)))];
      break;
    case "timestamp":
      {
        const numeric = encodeExactNumeric(
          present.map((value) =>
            value === null ? 0n : timestampMicros(value),
          ),
          true,
        );
        parts = numeric.parts;
        numericPacked = numeric.packed;
      }
      break;
    case "bool":
      parts = [Uint8Array.from(present, (value) => (value ? 1 : 0))];
      break;
    case "utf8":
      parts = valueLengthParts(present.map((value) => String(value ?? "")));
      break;
    case "json":
      parts = valueLengthParts(
        present.map((value) => (value === null ? "" : JSON.stringify(value))),
      );
      break;
    case "uuid": {
      const cells = new Uint8Array(present.length * 16);
      for (const [index, value] of present.entries()) {
        if (value === null) continue;
        if (!(value instanceof Uint8Array) || value.byteLength !== 16)
          throw new Error("invalid UUID value");
        cells.set(value, index * 16);
      }
      parts = [cells];
      break;
    }
    case "dict": {
      const dictionary = encodeDictionary(
        present.map((value) => (value === null ? null : String(value))),
        sortDictionaries,
      );
      dictionaryCount = dictionary.count;
      packedWidth = dictionary.bitWidth;
      parts = dictionary.parts;
      break;
    }
    default:
      throw new Error(`unsupported fixture column type ${type}`);
  }
  return {
    kind: type,
    nullable,
    dictionaryCount,
    bitWidth: packedWidth,
    numericPacked,
    parts: validity ? [validity, ...parts] : parts,
  };
}

/** Mirrors the decoder's `keyBytes` for the types the fixture tables key on. */
function keyBytes(
  parts: readonly unknown[],
  types: readonly DirectColumnarColumnKind[],
): Uint8Array {
  const chunks: Uint8Array[] = [];
  for (const [index, value] of parts.entries()) {
    switch (types[index]) {
      case "int64": {
        if (typeof value !== "bigint" && !Number.isSafeInteger(value))
          throw new Error("invalid primary key");
        const chunk = new Uint8Array(9);
        chunk[0] = 1;
        new DataView(chunk.buffer).setBigInt64(
          1,
          BigInt(value as bigint | number),
          true,
        );
        chunks.push(chunk);
        break;
      }
      case "utf8":
      case "dict": {
        if (typeof value !== "string") throw new Error("invalid primary key");
        const encoded = textEncoder.encode(value);
        const prefix = new Uint8Array(5);
        prefix[0] = 5;
        new DataView(prefix.buffer).setUint32(1, encoded.byteLength, true);
        chunks.push(prefix, encoded);
        break;
      }
      case "uuid": {
        if (!(value instanceof Uint8Array) || value.byteLength !== 16)
          throw new Error("invalid primary key");
        chunks.push(Uint8Array.of(7), value);
        break;
      }
      default:
        throw new Error("fixture primary keys are int64, utf8, dict or uuid");
    }
  }
  return concat(chunks);
}

/**
 * Refuses rows in which two share a primary key, as the production encoder
 * does, so a fixture cannot mint by accident what the decoder refuses.
 */
function checkUniqueKeys(
  rows: readonly FixtureRow[],
  fields: readonly string[],
  columns: readonly DirectColumnarSchema["tables"][number]["columns"][number][],
): void {
  const types = fields.map((field) => {
    const type = columns.find((column) => column.name === field)?.type;
    if (!type) throw new Error(`unknown primary-key column ${field}`);
    return type;
  });
  const seen = new Set<string>();
  for (const row of rows) {
    const key = keyBytes(
      fields.map((field) => row[field]),
      types,
    ).join(",");
    if (seen.has(key)) throw new Error("duplicate primary key");
    seen.add(key);
  }
}

interface EncodeOptions {
  /**
   * Emit dictionaries in first-seen order instead of sorted order, producing an
   * artifact that violates the format's sorted-dictionary rule. For testing the
   * decoder's guard only.
   */
  readonly sortDictionaries?: boolean;
  /**
   * `false` writes rows whose primary key repeats, which the production
   * encoder refuses. For testing the decoder's guard only.
   */
  readonly uniqueKeys?: boolean;
  /**
   * Identity written into the header and returned on the result, defaulting to
   * `FIXTURE_ARTIFACT_IDENTITY`. Its `schemaHash` must hash the schema being
   * encoded, because the decoder recomputes it; any other value throws here
   * rather than as a schema-hash mismatch at open time.
   */
  readonly identity?: DirectColumnarArtifactIdentity;
}

/**
 * Encodes one artifact from `tables`, which must supply a row array for every
 * table in the fixture workspace schema.
 */
export function encodeWorkspace(
  tables: ReadonlyMap<string, readonly FixtureRow[]>,
  options: EncodeOptions = {},
): EncodedArtifact {
  const identity = options.identity ?? FIXTURE_ARTIFACT_IDENTITY;
  if (identity.schemaHash !== FIXTURE_ARTIFACT_IDENTITY.schemaHash)
    throw new Error("fixture identity schemaHash must hash WORKSPACE_CONTRACT");
  return encodeArtifact(WORKSPACE_CONTRACT, tables, { ...options, identity });
}

/**
 * The `schemaHash` the decoder recomputes for `schema`, which it builds in
 * `parseDirectColumnarSchema`'s key order: `format`, `version`, `tables`; a
 * table's `name`, `primaryKey`, `columns`; a column's `name`, `type`,
 * `nullable`.
 */
export function fixtureSchemaHash(
  schema: DirectColumnarSchema,
): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(JSON.stringify(schema)).digest("hex")}`;
}

/**
 * The `chunks` a bootstrap plan names for `bytes`: the digest of each
 * consecutive `chunkBytes` of them, the last covering what is left. The server
 * names 4 MiB chunks; a test names smaller ones to cross chunk boundaries in an
 * artifact of a few kilobytes.
 */
export function artifactChunks(
  bytes: Uint8Array,
  chunkBytes = 4 * 1024 * 1024,
): TrustedArtifactChunks {
  const sha256: `sha256:${string}`[] = [];
  for (let start = 0; start < bytes.byteLength; start += chunkBytes)
    sha256.push(
      `sha256:${createHash("sha256")
        .update(bytes.subarray(start, start + chunkBytes))
        .digest("hex")}`,
    );
  return { bytes: chunkBytes, sha256 };
}

/**
 * `bytes` as the page's hashers pass it on to the engine worker: each chunk
 * `artifactChunks` names, in a buffer of its own, with its index and the
 * SHA-256 a hasher takes of it.
 */
export function hashedArtifactChunks(
  bytes: Uint8Array,
  chunkBytes = 4 * 1024 * 1024,
): {
  readonly index: number;
  readonly bytes: ArrayBuffer;
  readonly sha256: `sha256:${string}`;
}[] {
  const { sha256 } = artifactChunks(bytes, chunkBytes);
  return sha256.map((digest, index) => ({
    index,
    bytes: bytes.slice(index * chunkBytes, (index + 1) * chunkBytes).buffer,
    sha256: digest,
  }));
}

/**
 * `encodeWorkspace` for any `schema` whose columns are kinds this encoder
 * writes and whose primary keys are int64, utf8, dict or uuid. `tables` must
 * supply a row array for every table in it. The identity defaults to
 * `FIXTURE_ARTIFACT_IDENTITY` with `schema`'s hash, and a given identity must
 * carry that hash.
 */
export function encodeArtifact(
  schema: DirectColumnarSchema,
  tables: ReadonlyMap<string, readonly FixtureRow[]>,
  options: EncodeOptions = {},
): EncodedArtifact {
  const sortDictionaries = options.sortDictionaries ?? true;
  const uniqueKeys = options.uniqueKeys ?? true;
  const schemaHash = fixtureSchemaHash(schema);
  const identity = options.identity ?? {
    ...FIXTURE_ARTIFACT_IDENTITY,
    schemaHash,
  };
  if (identity.schemaHash !== schemaHash)
    throw new Error("fixture identity schemaHash must hash the schema");
  const payloadParts: Uint8Array[] = [];
  const tableHeaders: unknown[] = [];
  let payloadLength = 0;
  const emit = (buffer: Uint8Array) => {
    const aligned = align8(payloadLength);
    if (aligned !== payloadLength)
      payloadParts.push(new Uint8Array(aligned - payloadLength));
    payloadLength = aligned;
    const part = { offset: payloadLength, length: buffer.byteLength };
    payloadParts.push(buffer);
    payloadLength += buffer.byteLength;
    return part;
  };

  let rowCount = 0;
  for (const table of schema.tables) {
    const tableName = table.name;
    const rows = tables.get(tableName);
    if (!rows) throw new Error(`missing table ${tableName}`);
    rowCount += rows.length;
    const columns = table.columns.map(({ name, type, nullable }) => {
      const encoded = encodeColumn(
        type,
        nullable,
        rows.map((row) => row[name]),
        sortDictionaries,
      );
      const parts = encoded.parts.map(emit);
      return {
        name,
        type: encoded.kind,
        nullable: encoded.nullable,
        ...(encoded.dictionaryCount === null
          ? {}
          : { dictionaryCount: encoded.dictionaryCount }),
        ...(encoded.bitWidth === null ? {} : { bitWidth: encoded.bitWidth }),
        ...(encoded.numericPacked
          ? { encoding: { kind: "for", blockRows: NUMERIC_BLOCK_ROWS } }
          : {}),
        parts,
      };
    });
    const primaryKey = [...table.primaryKey];
    if (uniqueKeys) checkUniqueKeys(rows, primaryKey, table.columns);
    tableHeaders.push({
      name: tableName,
      rows: rows.length,
      primaryKey,
      columns,
    });
  }

  const header = textEncoder.encode(
    JSON.stringify({
      format: "calque-direct-columnar",
      version: DIRECT_COLUMNAR_FORMAT_VERSION,
      identity,
      tables: tableHeaders,
    }),
  );
  const prefix = MAGIC.byteLength + 4 + header.byteLength;
  const bytes = concat([
    MAGIC,
    u32Bytes([header.byteLength]),
    header,
    new Uint8Array(align8(prefix) - prefix),
    ...payloadParts,
  ]);
  return Object.freeze({
    bytes,
    byteLength: bytes.byteLength,
    digest:
      `sha256:${createHash("sha256").update(bytes).digest("hex")}` as const,
    identity,
    rowCount,
  });
}
