import {
  MAX_PACKED_OFFSET_BITS,
  packedBigIntAt,
  packedNumberReader,
  packedOffsetAt,
} from "./packed.js";

const MAGIC = new TextEncoder().encode("CALQART1");
const decoder = new TextDecoder("utf-8", { fatal: true });
const MAX_HEADER_BYTES = 1_048_576;
const align8 = (value: number) => (value + 7) & ~7;
const PLATFORM_LITTLE_ENDIAN =
  new Uint8Array(Uint16Array.of(1).buffer)[0] === 1;

const COLUMN_KINDS = [
  "int64",
  "uint64",
  "f64",
  "timestamp",
  "bool",
  "utf8",
  "bytes",
  "uuid",
  "json",
  "decimal",
  "date",
  "time",
  "interval",
  "dict",
] as const;
export type DirectColumnarColumnKind = (typeof COLUMN_KINDS)[number];
type ColumnKind = DirectColumnarColumnKind;
type Scalar = string | number | bigint | boolean | null | unknown;
const EXACT_KEY_KINDS = new Set<ColumnKind>([
  "int64",
  "uint64",
  "timestamp",
  "bool",
  "utf8",
  "bytes",
  "uuid",
  "decimal",
  "date",
  "time",
  "interval",
  "dict",
]);

export interface DirectColumnarColumnSchema {
  readonly name: string;
  readonly type: ColumnKind;
  readonly nullable: boolean;
}

export interface DirectColumnarTableSchema {
  readonly name: string;
  readonly primaryKey: readonly string[];
  readonly columns: readonly DirectColumnarColumnSchema[];
}

/**
 * Artifact format version. The container tag `CALQART1` versions the framing;
 * this versions the semantics. Other versions are rejected.
 */
export const DIRECT_COLUMNAR_FORMAT_VERSION = 4;

export interface DirectColumnarSchema {
  readonly format: "calque-direct-columnar";
  readonly version: typeof DIRECT_COLUMNAR_FORMAT_VERSION;
  readonly tables: readonly DirectColumnarTableSchema[];
}

export interface DirectColumnarArtifactIdentity {
  readonly sourceEpoch: string;
  readonly manifestVersion: string;
  readonly collectionSetHash: `sha256:${string}`;
  readonly schemaHash: `sha256:${string}`;
  readonly authorizationScopeVersion: string;
  /**
   * The scope this artifact was built for. A version alone cannot separate two
   * scopes published at the same authorization generation.
   */
  readonly authorizationScopeHash: `sha256:${string}`;
  readonly cursor: string;
}

export interface TrustedArtifact {
  readonly byteLength: number;
  readonly digest: `sha256:${string}`;
  /**
   * The plan's digest of each consecutive `bytes` bytes of the artifact, the
   * last covering what is left. With them the artifact can be verified while
   * it arrives (`receiveDirectColumnarArtifact`).
   */
  readonly chunks?: TrustedArtifactChunks;
  readonly identity: DirectColumnarArtifactIdentity;
  readonly schema: DirectColumnarSchema;
}

export interface TrustedArtifactChunks {
  readonly bytes: number;
  readonly sha256: readonly `sha256:${string}`[];
}

export interface ArtifactCapacity {
  readonly capacityBytes: number;
  readonly residentBytes: number;
  readonly standbyBytes: number;
  readonly safetyBytes: number;
}

export interface ArtifactColumn {
  readonly type: ColumnKind;
  readonly rows: number;
  valueAt(index: number): Scalar;
  numberAt?(index: number): number | null;
  orderedAt?(index: number): number | bigint | null;
}

/**
 * The query evaluator's access to a column's encoded cells, kept out of the
 * package entry point. Chunk statistics are built by the first query that can
 * use them, so they cost no artifact bytes and no activation work.
 *
 * A null cell satisfies no comparison with a non-null literal, `neq` included,
 * as in SQL. A null literal tests for NULL: `eq` asks whether the cell is null,
 * `neq` whether it is not.
 */
export interface ArtifactQueryVector {
  readonly rows: number;
  readonly chunkRows: number;
  compareAt(
    index: number,
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): boolean | undefined;
  prepareCompare(
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): ((index: number) => boolean) | undefined;
  /**
   * `prepareCompare` over a block of rows, defined whenever `prepareCompare`
   * is. The returned function sets `out[position]` for each position in
   * `selection[0..count)`: 2 where the comparison holds for
   * `addresses[position]`, 0 where it does not, and 1 where the address is at
   * or past `rows`, which is not a row of this vector to decide. It returns how
   * many positions it set to 1.
   */
  prepareBlockCompare(
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): ArtifactBlockCompare | undefined;
  /** The column's cells in their encoded form, for the kinds that have one. */
  readonly encoded: ArtifactEncodedColumn | undefined;
  chunkMayMatch(
    chunk: number,
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): boolean | undefined;
  statisticsBytes(): number;
  statisticsCapacityBytes(): number;
  statisticsRowsScanned(): number;
}

export type ArtifactBlockCompare = (
  addresses: Uint32Array,
  selection: Uint32Array,
  count: number,
  out: Uint8Array,
) => number;

/**
 * Read access to one column's encoded cells, for a query that groups, joins or
 * aggregates over them without reading every value out as a `Scalar`. `index`
 * is below `rows` and a valid row wherever a member reads a cell; the caller
 * checks both.
 */
export type ArtifactEncodedColumn =
  | {
      readonly kind: "dict";
      readonly rows: number;
      /** Codes run from 0 to `codes - 1`. */
      readonly codes: number;
      validAt(index: number): boolean;
      codeAt(index: number): number;
      /** The code whose entry reads back as exactly `value`, if one does. */
      codeOf(value: string): number | undefined;
      /**
       * Whether every entry decodes and no two decode to the same string, so
       * equal codes mean equal strings and reading a row cannot throw. The
       * fatal decoder strips a leading byte-order mark, so `EF BB BF` + `x`
       * decodes as `x` does. Computed once, on first call.
       */
      codesAreText(): boolean;
      /**
       * Whether code order is the order JavaScript compares the strings in:
       * `codesAreText()`, and no entry holds a character outside the Basic
       * Multilingual Plane. Entries are sorted by their UTF-8 bytes, which is
       * code point order; string comparison is UTF-16 code unit order, and
       * the two differ only where a surrogate pair meets a character from
       * U+E000 to U+FFFF. Computed once, on first call.
       */
      codesOrderAsText(): boolean;
    }
  | {
      readonly kind: "uuid";
      readonly rows: number;
      /**
       * Row `index` is `cells[index * 16]` through `cells[index * 16 + 15]`.
       */
      readonly cells: Uint8Array;
      validAt(index: number): boolean;
    }
  | {
      readonly kind: "text";
      readonly rows: number;
      /**
       * Row `index` is `bytes[offsets[index]]` up to
       * `bytes[offsets[index + 1]]`.
       */
      readonly offsets: Uint32Array;
      readonly bytes: Uint8Array;
      validAt(index: number): boolean;
      /**
       * Whether every row decodes and none starts with a byte-order mark,
       * which the fatal decoder strips, so equal bytes mean equal strings and
       * reading a row cannot throw. Computed once, on first call.
       */
      rowsAreText(): boolean;
    }
  | {
      readonly kind: "exact";
      readonly type: "int64" | "uint64" | "timestamp";
      readonly rows: number;
      validAt(index: number): boolean;
      /**
       * The stored integer, microseconds for a timestamp, when it is a safe
       * integer; otherwise `undefined`, and only `valueAt` reads it.
       */
      safeAt(index: number): number | undefined;
      /**
       * A number that orders as `valueAt(index)` does against the value of
       * every other row this returns a number for, and is equal exactly when
       * that value is: the integer itself, or a timestamp's text order
       * (`timestampTextOrder`). `undefined` exactly where `safeAt` is; where
       * it is a number, reading the value cannot throw.
       */
      orderAt(index: number): number | undefined;
    };

const QUERY_CHUNK_ROWS = 4_096;
const QUERY_VECTORS = new WeakMap<ArtifactColumn, ArtifactQueryVector>();

export function artifactQueryVector(
  column: ArtifactColumn,
): ArtifactQueryVector | undefined {
  return QUERY_VECTORS.get(column);
}

export interface ArtifactTable {
  readonly name: string;
  readonly rows: number;
  readonly primaryKey: readonly string[];
  column(name: string): ArtifactColumn;
  lookupKey(key: Scalar | readonly Scalar[]): number;
}

export interface DirectColumnarWorkspace {
  readonly artifactByteLength: number;
  readonly rowCount: number;
  table(name: string): ArtifactTable;
}

export class CapacityError extends Error {
  readonly code = "CALQUE_CAPACITY";

  constructor(
    readonly requiredBytes: number,
    readonly capacityBytes: number,
  ) {
    super(
      `direct-columnar capacity exceeded: ${requiredBytes} > ${capacityBytes}`,
    );
    this.name = "CapacityError";
  }
}

interface PartDefinition {
  readonly offset: number;
  readonly length: number;
}

interface ColumnDefinition {
  readonly name: string;
  readonly type: ColumnKind;
  readonly nullable: boolean;
  readonly dictionaryCount?: number;
  readonly bitWidth?: number;
  readonly encoding?: NumericEncodingDefinition;
  readonly parts: readonly PartDefinition[];
}

interface NumericEncodingDefinition {
  readonly kind: "for";
  readonly blockRows: number;
}

interface TableDefinition {
  readonly name: string;
  readonly rows: number;
  readonly primaryKey: readonly string[];
  readonly columns: readonly ColumnDefinition[];
}

interface HeaderDefinition {
  readonly format: "calque-direct-columnar";
  readonly version: typeof DIRECT_COLUMNAR_FORMAT_VERSION;
  readonly identity: DirectColumnarArtifactIdentity;
  readonly tables: readonly TableDefinition[];
}

interface CheckedPart {
  readonly offset: number;
  readonly length: number;
  readonly bytes: Uint8Array;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);

function exactKeys(
  value: unknown,
  expected: readonly string[],
): value is Record<string, unknown> {
  return (
    isRecord(value) &&
    Object.keys(value).length === expected.length &&
    expected.every((key) => Object.hasOwn(value, key))
  );
}

function safeNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function sameValues(
  left: readonly unknown[],
  right: readonly unknown[],
): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => Object.is(value, right[index]))
  );
}

function isColumnKind(value: unknown): value is ColumnKind {
  return (COLUMN_KINDS as readonly unknown[]).includes(value);
}

function boundedName(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    new TextEncoder().encode(value).byteLength <= 255 &&
    !value.includes("\0")
  );
}

function parseIdentity(value: unknown): DirectColumnarArtifactIdentity {
  if (
    !exactKeys(value, [
      "sourceEpoch",
      "manifestVersion",
      "collectionSetHash",
      "schemaHash",
      "authorizationScopeVersion",
      "authorizationScopeHash",
      "cursor",
    ]) ||
    !boundedName(value.sourceEpoch) ||
    !boundedName(value.manifestVersion) ||
    !boundedName(value.authorizationScopeVersion) ||
    !boundedName(value.cursor) ||
    typeof value.collectionSetHash !== "string" ||
    typeof value.schemaHash !== "string" ||
    typeof value.authorizationScopeHash !== "string" ||
    !/^sha256:[0-9a-f]{64}$/.test(value.collectionSetHash) ||
    !/^sha256:[0-9a-f]{64}$/.test(value.schemaHash) ||
    !/^sha256:[0-9a-f]{64}$/.test(value.authorizationScopeHash)
  )
    throw new Error("invalid direct-columnar artifact identity");
  return Object.freeze({
    sourceEpoch: value.sourceEpoch,
    manifestVersion: value.manifestVersion,
    collectionSetHash: value.collectionSetHash as `sha256:${string}`,
    schemaHash: value.schemaHash as `sha256:${string}`,
    authorizationScopeVersion: value.authorizationScopeVersion,
    authorizationScopeHash: value.authorizationScopeHash as `sha256:${string}`,
    cursor: value.cursor,
  });
}

function sameIdentity(
  left: DirectColumnarArtifactIdentity,
  right: DirectColumnarArtifactIdentity,
): boolean {
  return (
    left.sourceEpoch === right.sourceEpoch &&
    left.manifestVersion === right.manifestVersion &&
    left.collectionSetHash === right.collectionSetHash &&
    left.schemaHash === right.schemaHash &&
    left.authorizationScopeVersion === right.authorizationScopeVersion &&
    left.authorizationScopeHash === right.authorizationScopeHash &&
    left.cursor === right.cursor
  );
}

function parsePart(value: unknown): PartDefinition {
  if (
    !exactKeys(value, ["offset", "length"]) ||
    !safeNonNegativeInteger(value.offset) ||
    !safeNonNegativeInteger(value.length)
  )
    throw new Error("invalid direct-columnar segment");
  return { offset: value.offset, length: value.length };
}

function parseHeader(value: unknown): HeaderDefinition {
  if (
    !exactKeys(value, ["format", "version", "identity", "tables"]) ||
    value.format !== "calque-direct-columnar" ||
    value.version !== DIRECT_COLUMNAR_FORMAT_VERSION ||
    !Array.isArray(value.tables)
  )
    throw new Error("invalid direct-columnar header");
  const tables = value.tables.map((raw): TableDefinition => {
    if (
      !exactKeys(raw, ["name", "rows", "primaryKey", "columns"]) ||
      !boundedName(raw.name) ||
      !safeNonNegativeInteger(raw.rows) ||
      !Array.isArray(raw.primaryKey) ||
      !raw.primaryKey.every((item) => typeof item === "string") ||
      !Array.isArray(raw.columns)
    )
      throw new Error("invalid direct-columnar table");
    const columns = raw.columns.map((column): ColumnDefinition => {
      const keys = ["name", "type", "nullable"];
      if (isRecord(column) && column.dictionaryCount !== undefined)
        keys.push("dictionaryCount");
      if (isRecord(column) && column.bitWidth !== undefined)
        keys.push("bitWidth");
      if (isRecord(column) && column.encoding !== undefined)
        keys.push("encoding");
      keys.push("parts");
      if (
        !exactKeys(column, keys) ||
        !boundedName(column.name) ||
        !isColumnKind(column.type) ||
        typeof column.nullable !== "boolean" ||
        !Array.isArray(column.parts) ||
        (column.dictionaryCount !== undefined &&
          !safeNonNegativeInteger(column.dictionaryCount)) ||
        (column.bitWidth !== undefined &&
          (!safeNonNegativeInteger(column.bitWidth) || column.bitWidth > 64))
      )
        throw new Error("invalid direct-columnar column");
      let encoding: NumericEncodingDefinition | undefined;
      if (column.encoding !== undefined) {
        if (
          !exactKeys(column.encoding, ["kind", "blockRows"]) ||
          column.encoding.kind !== "for" ||
          !safeNonNegativeInteger(column.encoding.blockRows) ||
          column.encoding.blockRows === 0
        )
          throw new Error("invalid direct-columnar numeric encoding");
        encoding = { kind: "for", blockRows: column.encoding.blockRows };
      }
      const isDictionary = column.type === "dict";
      const isExactNumeric =
        column.type === "int64" ||
        column.type === "uint64" ||
        column.type === "timestamp";
      if (
        isDictionary !== (column.dictionaryCount !== undefined) ||
        (column.bitWidth !== undefined && !isDictionary) ||
        (encoding !== undefined && !isExactNumeric)
      )
        throw new Error("invalid direct-columnar column encoding metadata");
      return {
        name: column.name,
        type: column.type,
        nullable: column.nullable,
        ...(column.dictionaryCount === undefined
          ? {}
          : { dictionaryCount: column.dictionaryCount }),
        ...(column.bitWidth === undefined ? {} : { bitWidth: column.bitWidth }),
        ...(encoding === undefined ? {} : { encoding }),
        parts: column.parts.map(parsePart),
      };
    });
    return {
      name: raw.name,
      rows: raw.rows,
      primaryKey: raw.primaryKey,
      columns,
    };
  });
  return {
    format: value.format,
    version: value.version,
    identity: parseIdentity(value.identity),
    tables,
  };
}

/**
 * Checks a trusted schema, as a bootstrap plan carries it, and rebuilds it in
 * the key order whose canonical JSON the plan's `schemaHash` covers.
 */
export function parseDirectColumnarSchema(
  value: unknown,
): DirectColumnarSchema {
  if (
    !exactKeys(value, ["format", "version", "tables"]) ||
    value.format !== "calque-direct-columnar" ||
    value.version !== DIRECT_COLUMNAR_FORMAT_VERSION ||
    !Array.isArray(value.tables) ||
    value.tables.length === 0
  )
    throw new Error("invalid trusted direct-columnar schema");
  const tableNames = new Set<string>();
  const tables = value.tables.map((raw): DirectColumnarTableSchema => {
    if (
      !exactKeys(raw, ["name", "primaryKey", "columns"]) ||
      !boundedName(raw.name) ||
      tableNames.has(raw.name) ||
      !Array.isArray(raw.primaryKey) ||
      raw.primaryKey.length === 0 ||
      !raw.primaryKey.every(boundedName) ||
      new Set(raw.primaryKey).size !== raw.primaryKey.length ||
      !Array.isArray(raw.columns) ||
      raw.columns.length === 0
    )
      throw new Error("invalid trusted direct-columnar table schema");
    tableNames.add(raw.name);
    const columnNames = new Set<string>();
    const columns = raw.columns.map((column): DirectColumnarColumnSchema => {
      if (
        !exactKeys(column, ["name", "type", "nullable"]) ||
        !boundedName(column.name) ||
        columnNames.has(column.name) ||
        !isColumnKind(column.type) ||
        typeof column.nullable !== "boolean"
      )
        throw new Error("invalid trusted direct-columnar column schema");
      columnNames.add(column.name);
      return {
        name: column.name,
        type: column.type,
        nullable: column.nullable,
      };
    });
    if (raw.primaryKey.some((name) => !columnNames.has(name)))
      throw new Error("trusted direct-columnar primary key is not selected");
    for (const name of raw.primaryKey) {
      const column = columns.find((candidate) => candidate.name === name);
      if (!column || column.nullable || !EXACT_KEY_KINDS.has(column.type))
        throw new Error(
          "trusted direct-columnar primary key requires a non-null exact type",
        );
    }
    return {
      name: raw.name,
      primaryKey: Object.freeze([...raw.primaryKey]),
      columns: Object.freeze(columns),
    };
  });
  return Object.freeze({
    format: "calque-direct-columnar",
    version: DIRECT_COLUMNAR_FORMAT_VERSION,
    tables: Object.freeze(tables),
  });
}

function checkedPart(
  bytes: Uint8Array,
  payloadStart: number,
  part: PartDefinition,
  occupied: Array<readonly [number, number]>,
): CheckedPart {
  const payloadLength = bytes.byteLength - payloadStart;
  if (part.offset % 8 !== 0 || part.offset + part.length > payloadLength)
    throw new Error("invalid direct-columnar segment");
  if (
    occupied.some(
      ([start, end]) => part.offset < end && part.offset + part.length > start,
    )
  )
    throw new Error("overlapping direct-columnar segments");
  occupied.push([part.offset, part.offset + part.length]);
  const offset = payloadStart + part.offset;
  return {
    offset,
    length: part.length,
    bytes: bytes.subarray(offset, offset + part.length),
  };
}

function u32(bytes: Uint8Array, part: CheckedPart, count: number): Uint32Array {
  if (part.length !== count * 4) throw new Error("invalid u32 column length");
  return new Uint32Array(bytes.buffer, part.offset, count);
}

function requiredBitWidth(maximum: number): number {
  if (!safeNonNegativeInteger(maximum))
    throw new Error("invalid packed maximum");
  return maximum === 0 ? 0 : Math.floor(Math.log2(maximum)) + 1;
}

function packedByteLength(count: number, width: number): number {
  if (
    !safeNonNegativeInteger(count) ||
    !safeNonNegativeInteger(width) ||
    width > 64
  )
    throw new Error("invalid packed shape");
  const bits = count * width;
  if (!Number.isSafeInteger(bits)) throw new Error("packed shape is too large");
  return Math.ceil(bits / 8);
}

function validatePacked(bytes: Uint8Array, count: number, width: number): void {
  if (bytes.byteLength !== packedByteLength(count, width))
    throw new Error("invalid packed length");
  const used = (count * width) % 8;
  if (used !== 0 && bytes.byteLength > 0) {
    const last = at(bytes, bytes.byteLength - 1);
    if ((last & ~((1 << used) - 1)) !== 0)
      throw new Error("invalid packed padding");
  }
}

function at<T>(values: ArrayLike<T>, index: number): T {
  const value = values[index];
  if (value === undefined)
    throw new RangeError("index is outside the validated data");
  return value;
}

function stringAccessor(
  offsets: Uint32Array,
  bytes: Uint8Array,
): (index: number) => string {
  for (let index = 0; index < offsets.length - 1; index++)
    if (
      at(offsets, index) > at(offsets, index + 1) ||
      at(offsets, index + 1) > bytes.byteLength
    )
      throw new Error("invalid UTF-8 offsets");
  return stringReader(offsets, bytes);
}

/** Reads row `index` of `offsets` already non-decreasing and in bounds. */
function stringReader(
  offsets: Uint32Array,
  bytes: Uint8Array,
): (index: number) => string {
  return (index) => {
    if (
      !Number.isSafeInteger(index) ||
      index < 0 ||
      index >= offsets.length - 1
    )
      throw new RangeError("row is outside the column");
    return decoder.decode(
      bytes.subarray(at(offsets, index), at(offsets, index + 1)),
    );
  };
}

/**
 * Turns a variable-width column's lengths part, a 0 and then each row's value
 * length, into its offsets in place. The running sum is checked before each
 * store, so a sum past the values part is refused instead of wrapping in the
 * `Uint32Array`, and a short one is refused at the end; `stringReader` relies
 * on both. It runs only on verified bytes in a buffer nothing else reads.
 */
function valueOffsets(lengths: Uint32Array, valuesLength: number): Uint32Array {
  if (lengths[0] !== 0) throw new Error("invalid value-length prefix");
  const limit = Math.min(valuesLength, 0xffff_ffff);
  let sum = 0;
  for (let index = 1; index < lengths.length; index++) {
    sum += lengths[index] ?? 0;
    if (sum > limit) throw new Error("value lengths overrun the values part");
    lengths[index] = sum;
  }
  if (sum !== valuesLength)
    throw new Error("value lengths fall short of the values part");
  return lengths;
}

/**
 * Rejects a dictionary whose entries are not strictly ascending by UTF-8 bytes,
 * which also rejects duplicates. Dictionary columns are ordered by comparing
 * codes, so an unsorted dictionary would give a silently wrong row order. The
 * comparison is over raw bytes because decoded strings compare in UTF-16 code
 * unit order, which disagrees above the BMP.
 *
 * `offsets` must already be validated as non-decreasing and in bounds.
 */
function checkSortedDictionary(offsets: Uint32Array, bytes: Uint8Array): void {
  for (let index = 0; index + 2 < offsets.length; index++) {
    const start = at(offsets, index);
    const middle = at(offsets, index + 1);
    const end = at(offsets, index + 2);
    const shared = Math.min(middle - start, end - middle);
    let order = 0;
    for (let cursor = 0; order === 0 && cursor < shared; cursor++)
      order = at(bytes, start + cursor) - at(bytes, middle + cursor);
    if (order === 0) order = middle - start - (end - middle);
    if (order >= 0) throw new Error("unsorted direct-columnar dictionary");
  }
}

/**
 * The exact text a `timestamp` column reads back. Live changes carry
 * microseconds, and must format them with this so a row reads the same whether
 * it came from a change or a fresh artifact.
 */
export function directColumnarTimestampText(micros: bigint): string {
  return timestampText(micros);
}

function timestampText(micros: bigint): string {
  if (micros >= -MAX_SAFE_BIGINT && micros <= MAX_SAFE_BIGINT)
    return safeTimestampText(Number(micros));
  let millis = micros / 1_000n;
  let remainder = micros % 1_000n;
  if (remainder < 0) {
    millis -= 1n;
    remainder += 1_000n;
  }
  const numericMillis = Number(millis);
  if (!Number.isSafeInteger(numericMillis))
    throw new Error("timestamp is outside the exact JavaScript Date range");
  const base = isoMillisText(numericMillis);
  return remainder === 0n
    ? base
    : `${base.slice(0, -1)}${remainder.toString().padStart(3, "0")}Z`;
}

const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * `timestampText` for a safe-integer microsecond count, without a bigint.
 *
 * The floored quotient is exact. The true quotient's fraction is a multiple of
 * 1/1,000, and below 2^44, which bounds a safe count over 1,000, doubles are at
 * most 2^-9 apart, so rounding moves it by less than 1/1,000 and never across
 * an integer. `%` would be exact too, but V8 calls `fmod` for it past the
 * small-integer range.
 */
function safeTimestampText(micros: number): string {
  const millis = Math.floor(micros / 1_000);
  const remainder = micros - millis * 1_000;
  const base = isoMillisText(millis);
  return remainder === 0
    ? base
    : `${base.slice(0, -1)}${digits(THREE_DIGITS, remainder)}Z`;
}

/**
 * A number that orders as `safeTimestampText(micros)` orders as a string, and
 * is equal exactly when the text is.
 *
 * A safe count's text has a fixed-width `YYYY-MM-DDTHH:mm:ss.sss` prefix, which
 * orders as the millisecond count does. A whole millisecond's text then ends in
 * `Z`, which sorts after the three digits a fraction adds, so a whole
 * millisecond is keyed `micros + 1000`: a value no fraction produces, and exact
 * below 2^54.
 */
export function timestampTextOrder(micros: number): number {
  const millis = Math.floor(micros / 1_000);
  return micros - millis * 1_000 === 0 ? micros + 1_000 : micros;
}

const MAX_DATE_MILLIS = 8.64e15;
const DAY_MILLIS = 86_400_000;
const MAX_ISO_DAY_PREFIXES = 4_096;
const isoDayPrefixes = new Map<number, string>();

/**
 * `new Date(millis).toISOString()` without a `Date` per row: the date prefix is
 * cached per day, and the `THH:mm:ss.sssZ` suffix is arithmetic.
 */
function isoMillisText(millis: number): string {
  if (Math.abs(millis) > MAX_DATE_MILLIS) return new Date(millis).toISOString();
  const day = Math.floor(millis / DAY_MILLIS);
  let prefix = isoDayPrefixes.get(day);
  if (prefix === undefined) {
    if (isoDayPrefixes.size >= MAX_ISO_DAY_PREFIXES) isoDayPrefixes.clear();
    prefix = new Date(day * DAY_MILLIS).toISOString().slice(0, -13);
    isoDayPrefixes.set(day, prefix);
  }
  let within = millis - day * DAY_MILLIS;
  const hours = Math.floor(within / 3_600_000);
  within -= hours * 3_600_000;
  const minutes = Math.floor(within / 60_000);
  within -= minutes * 60_000;
  const seconds = Math.floor(within / 1_000);
  const fraction = within - seconds * 1_000;
  return `${prefix}${digits(TWO_DIGITS, hours)}:${digits(TWO_DIGITS, minutes)}:${digits(TWO_DIGITS, seconds)}.${digits(THREE_DIGITS, fraction)}Z`;
}

/** Zero-padded decimal text for every clock field and millisecond value. */
const TWO_DIGITS: readonly string[] = Array.from({ length: 100 }, (_u, value) =>
  String(value).padStart(2, "0"),
);
const THREE_DIGITS: readonly string[] = Array.from(
  { length: 1_000 },
  (_u, value) => String(value).padStart(3, "0"),
);

function digits(table: readonly string[], value: number): string {
  const text = table[value];
  if (text === undefined) throw new RangeError("clock field is out of range");
  return text;
}

function loadExactNumericColumn(
  bytes: Uint8Array,
  rows: number,
  definition: ColumnDefinition,
  parts: readonly CheckedPart[],
  isValid: (index: number) => boolean,
  checkRow: (index: number) => void,
): ArtifactColumn {
  const signed = definition.type !== "uint64";
  const timestamp = definition.type === "timestamp";
  const tag = timestamp ? 3 : signed ? 1 : 2;
  let readValue: (index: number) => bigint;
  let readSafe: ((index: number) => number | undefined) | undefined;
  // Folds each row's key bytes, the tag and the stored value's eight
  // little-endian bytes, into its state, reading the layout in order.
  let foldKeys: (states: Uint32Array) => void;
  if (definition.encoding === undefined) {
    const part = parts[0];
    if (parts.length !== 1 || !part || part.length !== rows * 8)
      throw new Error("invalid integer column length");
    const values = signed
      ? new BigInt64Array(bytes.buffer, part.offset, rows)
      : new BigUint64Array(bytes.buffer, part.offset, rows);
    readValue = (index) => at(values, index);
    foldKeys = (states) => {
      for (let row = 0; row < rows; row++)
        states[row] = fnvBytes(
          fnvByte(states[row] ?? 0, tag),
          part.bytes,
          row * 8,
          row * 8 + 8,
        );
    };
  } else {
    const metadata = parts[0];
    const data = parts[1];
    if (parts.length !== 2 || !metadata || !data)
      throw new Error("invalid FOR column shape");
    const blockRows = definition.encoding.blockRows;
    const blockCount = Math.ceil(rows / blockRows);
    if (metadata.length !== blockCount * 16)
      throw new Error("invalid FOR metadata length");
    const view = new DataView(bytes.buffer, metadata.offset, metadata.length);
    const blocks: Array<{
      readonly start: number;
      readonly width: number;
      readonly base: bigint;
      readonly scale: bigint;
      /**
       * `base` as a number when every value the block can encode, `base`
       * through `base + 2^width - 1` times `scale`, is a safe integer. Such a
       * block is read without a bigint and cannot overflow either domain.
       */
      readonly safeBase: number | undefined;
      readonly safeScale: number;
    }> = [];
    let expectedOffset = 0;
    for (let block = 0; block < blockCount; block++) {
      const header = block * 16;
      const offset = view.getUint32(header, true);
      const base = signed
        ? view.getBigInt64(header + 4, true)
        : view.getBigUint64(header + 4, true);
      const width = view.getUint8(header + 12);
      const scaleLog10 = view.getUint8(header + 13);
      if (
        offset !== expectedOffset ||
        width > 64 ||
        view.getUint8(header + 14) !== 0 ||
        view.getUint8(header + 15) !== 0 ||
        (scaleLog10 !== 0 &&
          !(definition.type === "timestamp" && scaleLog10 === 3)) ||
        (width === 64 && (base !== 0n || scaleLog10 !== 0))
      )
        throw new Error("invalid FOR block header");
      const count = Math.min(blockRows, rows - block * blockRows);
      const length = width === 64 ? count * 8 : packedByteLength(count, width);
      if (expectedOffset + length > data.length)
        throw new Error("invalid FOR data length");
      if (width !== 64)
        validatePacked(
          data.bytes.subarray(expectedOffset, expectedOffset + length),
          count,
          width,
        );
      const scale = scaleLog10 === 3 ? 1_000n : 1n;
      const safe =
        width <= MAX_PACKED_OFFSET_BITS &&
        base * scale >= -MAX_SAFE_BIGINT &&
        (base + (1n << BigInt(width)) - 1n) * scale <= MAX_SAFE_BIGINT;
      blocks.push({
        start: expectedOffset,
        width,
        base,
        scale,
        safeBase: safe ? Number(base) : undefined,
        safeScale: Number(scale),
      });
      expectedOffset += length;
    }
    if (expectedOffset !== data.length)
      throw new Error("invalid FOR terminal offset");
    readValue = (index) => {
      const blockIndex = Math.floor(index / blockRows);
      const within = index % blockRows;
      const block = at(blocks, blockIndex);
      if (block.width === 64) {
        const offset = data.offset + block.start + within * 8;
        return signed
          ? new DataView(bytes.buffer).getBigInt64(offset, true)
          : new DataView(bytes.buffer).getBigUint64(offset, true);
      }
      // A block starts on a byte boundary, and `within` is below its row
      // count, so a read from the whole data part stays inside the block.
      // Up to 45 bits the offset is exact as a number, which skips a bigint
      // shift per byte on every read, the validation pass below included.
      if (block.width <= MAX_PACKED_OFFSET_BITS)
        return (
          (block.base +
            BigInt(
              packedOffsetAt(data.bytes, block.width, within, block.start),
            )) *
          block.scale
        );
      const count = Math.min(blockRows, rows - blockIndex * blockRows);
      const blockBytes = data.bytes.subarray(
        block.start,
        block.start + packedByteLength(count, block.width),
      );
      return (
        (block.base + packedBigIntAt(blockBytes, block.width, within)) *
        block.scale
      );
    };
    readSafe = (index) => {
      const block = at(blocks, Math.floor(index / blockRows));
      if (block.safeBase === undefined) return undefined;
      // Exact: the sum and the product are integers inside the safe range.
      return (
        (block.safeBase +
          packedOffsetAt(
            data.bytes,
            block.width,
            index % blockRows,
            block.start,
          )) *
        block.safeScale
      );
    };
    foldKeys = (states) => {
      for (let block = 0; block < blockCount; block++) {
        const { start, width, safeBase, safeScale } = at(blocks, block);
        const first = block * blockRows;
        const end = Math.min(rows, first + blockRows);
        if (width === 64) {
          // Raw values, already the key's eight bytes.
          for (let row = first; row < end; row++)
            states[row] = fnvBytes(
              fnvByte(states[row] ?? 0, tag),
              data.bytes,
              start + (row - first) * 8,
              start + (row - first) * 8 + 8,
            );
        } else if (safeBase === undefined) {
          for (let row = first; row < end; row++)
            states[row] = fnvExact(
              fnvByte(states[row] ?? 0, tag),
              readValue(row),
            );
        } else if (width <= 24) {
          // `packedNumberReader` written out: this pass runs once a load,
          // largely before the optimizing compiler would inline its closure.
          const packed = data.bytes;
          const mask = (1 << width) - 1;
          for (let row = first, bit = start * 8; row < end; row++) {
            const shift = bit & 7;
            const byte = (bit - shift) / 8;
            bit += width;
            const value =
              (safeBase +
                ((((packed[byte] ?? 0) |
                  ((packed[byte + 1] ?? 0) << 8) |
                  ((packed[byte + 2] ?? 0) << 16) |
                  ((packed[byte + 3] ?? 0) << 24)) >>>
                  shift) &
                  mask)) *
              safeScale;
            states[row] = fnvSafe(fnvByte(states[row] ?? 0, tag), value);
          }
        } else {
          for (let row = first; row < end; row++)
            states[row] = fnvSafe(
              fnvByte(states[row] ?? 0, tag),
              (safeBase +
                packedOffsetAt(data.bytes, width, row - first, start)) *
                safeScale,
            );
        }
      }
    };
    const minimum = signed ? -(1n << 63n) : 0n;
    const maximum = signed ? (1n << 63n) - 1n : (1n << 64n) - 1n;
    for (let block = 0; block < blockCount; block++) {
      // A safe block's whole range lies inside both domains; this pass is a
      // cold-load cost, so it reads only the blocks that could overflow.
      if (at(blocks, block).safeBase !== undefined) continue;
      const end = Math.min(rows, (block + 1) * blockRows);
      for (let index = block * blockRows; index < end; index++) {
        const value = readValue(index);
        if (value < minimum || value > maximum)
          throw new Error("FOR numeric value overflows its exact domain");
      }
    }
  }
  const read = readValue;
  const safe = readSafe;
  const safeAt = (index: number) => {
    const fast = safe?.(index);
    if (fast !== undefined) return fast;
    const value = read(index);
    return value >= -MAX_SAFE_BIGINT && value <= MAX_SAFE_BIGINT
      ? Number(value)
      : undefined;
  };
  const encoded: ArtifactEncodedColumn = Object.freeze({
    kind: "exact",
    type: timestamp
      ? "timestamp"
      : definition.type === "uint64"
        ? "uint64"
        : "int64",
    rows,
    validAt: isValid,
    safeAt,
    orderAt: timestamp
      ? (index: number) => {
          const micros = safeAt(index);
          return micros === undefined ? undefined : timestampTextOrder(micros);
        }
      : safeAt,
  });
  const column: ArtifactColumn = Object.freeze({
    type: definition.type,
    rows,
    valueAt: (index: number) => {
      checkRow(index);
      if (!isValid(index)) return null;
      if (definition.type !== "timestamp") return read(index);
      const micros = safe?.(index);
      return micros === undefined
        ? timestampText(read(index))
        : safeTimestampText(micros);
    },
    orderedAt: (index: number) => {
      checkRow(index);
      return isValid(index) ? read(index) : null;
    },
  });
  registerKeyColumn(column, {
    fold: foldKeys,
    equal: (left, right) => read(left) === read(right),
  });
  // Timestamp text carries a variable number of fractional digits, so only the
  // exact operators agree with comparing the strings.
  return registerQueryVector(
    column,
    definition.type === "timestamp"
      ? queryVector(
          rows,
          isValid,
          read,
          (literal) => {
            if (typeof literal !== "string") return undefined;
            const parsed = Date.parse(literal);
            if (!Number.isFinite(parsed)) return undefined;
            const fraction = /\.(\d{1,6})Z$/u.exec(literal)?.[1] ?? "";
            const micros =
              BigInt(Math.trunc(parsed)) * 1_000n +
              BigInt(fraction.padEnd(6, "0").slice(3, 6) || "0");
            return timestampText(micros) === literal ? micros : undefined;
          },
          new Set(["eq", "neq"]),
          encoded,
        )
      : queryVector(
          rows,
          isValid,
          read,
          (literal) => (typeof literal === "bigint" ? literal : undefined),
          new Set(["eq", "neq", "lt", "lte", "gt", "gte"]),
          encoded,
        ),
  );
}

type EncodedOrdered = number | bigint;

interface ResidentChunkStatistic {
  readonly rows: number;
  readonly nullCount: number;
  readonly minimum?: EncodedOrdered;
  readonly maximum?: EncodedOrdered;
}

function statisticsCharge(chunks: number): number {
  return chunks === 0 ? 0 : 64 + chunks * 128;
}

function encodedCompare(left: EncodedOrdered, right: EncodedOrdered): number {
  if (typeof left !== typeof right) return 0;
  if (typeof left === "bigint" && typeof right === "bigint")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") {
    const leftNan = Number.isNaN(left);
    const rightNan = Number.isNaN(right);
    if (leftNan || rightNan) return leftNan === rightNan ? 0 : leftNan ? 1 : -1;
    if (left === 0 && right === 0) {
      if (Object.is(left, right)) return 0;
      return Object.is(left, -0) ? -1 : 1;
    }
    return left < right ? -1 : left > right ? 1 : 0;
  }
  return 0;
}

function encodedDecision(
  comparison: number,
  operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
): boolean {
  if (operator === "eq") return comparison === 0;
  if (operator === "neq") return comparison !== 0;
  if (operator === "lt") return comparison < 0;
  if (operator === "lte") return comparison <= 0;
  if (operator === "gt") return comparison > 0;
  return comparison >= 0;
}

/** A row-at-a-time comparison over a block, for kinds with no faster loop. */
function blockOf(
  rows: number,
  compare: ((index: number) => boolean) | undefined,
): ArtifactBlockCompare | undefined {
  if (compare === undefined) return undefined;
  return (addresses, selection, count, out) => {
    let outside = 0;
    for (let index = 0; index < count; index++) {
      const position = selection[index] ?? 0;
      const address = addresses[position] ?? rows;
      if (address >= rows) {
        out[position] = 1;
        outside++;
      } else out[position] = compare(address) ? 2 : 0;
    }
    return outside;
  };
}

function queryVector(
  rows: number,
  validAt: (index: number) => boolean,
  encodedAt: (index: number) => EncodedOrdered,
  encodeLiteral: (literal: Scalar) => EncodedOrdered | undefined,
  supported: {
    has(operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte"): boolean;
  },
  encoded?: ArtifactEncodedColumn,
  blockCompare?: (
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ) => ArtifactBlockCompare | undefined,
): ArtifactQueryVector {
  let statistics: readonly ResidentChunkStatistic[] | undefined;
  let scanned = 0;
  const ensureStatistics = (): readonly ResidentChunkStatistic[] => {
    if (statistics) return statistics;
    const chunks: ResidentChunkStatistic[] = [];
    for (let start = 0; start < rows; start += QUERY_CHUNK_ROWS) {
      const end = Math.min(rows, start + QUERY_CHUNK_ROWS);
      let nullCount = 0;
      let minimum: EncodedOrdered | undefined;
      let maximum: EncodedOrdered | undefined;
      for (let index = start; index < end; index++) {
        scanned++;
        if (!validAt(index)) {
          nullCount++;
          continue;
        }
        const value = encodedAt(index);
        if (minimum === undefined || encodedCompare(value, minimum) < 0)
          minimum = value;
        if (maximum === undefined || encodedCompare(value, maximum) > 0)
          maximum = value;
      }
      chunks.push({
        rows: end - start,
        nullCount,
        ...(minimum === undefined ? {} : { minimum }),
        ...(maximum === undefined ? {} : { maximum }),
      });
    }
    statistics = Object.freeze(chunks);
    return statistics;
  };
  const prepareCompare = (
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): ((index: number) => boolean) | undefined => {
    if (!supported.has(operator)) return undefined;
    const encoded = literal === null ? undefined : encodeLiteral(literal);
    return (index: number): boolean => {
      if (!validAt(index)) return operator === "eq" && literal === null;
      if (literal === null) return operator === "neq";
      if (encoded === undefined)
        return operator === "eq" ? false : operator === "neq" ? true : false;
      return encodedDecision(
        encodedCompare(encodedAt(index), encoded),
        operator,
      );
    };
  };
  return Object.freeze({
    rows,
    chunkRows: QUERY_CHUNK_ROWS,
    compareAt(
      index: number,
      operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
      literal: Scalar,
    ) {
      return prepareCompare(operator, literal)?.(index);
    },
    prepareCompare,
    prepareBlockCompare:
      blockCompare === undefined
        ? (
            operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
            literal: Scalar,
          ) => blockOf(rows, prepareCompare(operator, literal))
        : (
            operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
            literal: Scalar,
          ) =>
            supported.has(operator)
              ? blockCompare(operator, literal)
              : undefined,
    encoded,
    chunkMayMatch(
      chunk: number,
      operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
      literal: Scalar,
    ) {
      if (!supported.has(operator)) return undefined;
      const stat = ensureStatistics()[chunk];
      if (!stat) return undefined;
      if (literal === null) {
        if (operator === "eq") return stat.nullCount > 0;
        if (operator === "neq") return stat.nullCount < stat.rows;
        return false;
      }
      // A null row satisfies no comparison with a non-null literal.
      const encoded = encodeLiteral(literal);
      if (encoded === undefined)
        return operator === "neq" ? stat.nullCount < stat.rows : false;
      if (stat.minimum === undefined || stat.maximum === undefined)
        return false;
      const againstMinimum = encodedCompare(stat.minimum, encoded);
      const againstMaximum = encodedCompare(stat.maximum, encoded);
      if (operator === "eq") return againstMinimum <= 0 && againstMaximum >= 0;
      if (operator === "neq")
        return againstMinimum !== 0 || againstMaximum !== 0;
      if (operator === "lt") return againstMinimum < 0;
      if (operator === "lte") return againstMinimum <= 0;
      if (operator === "gt") return againstMaximum > 0;
      return againstMaximum >= 0;
    },
    // Charges boxed values, object metadata and alignment, not a packed struct.
    statisticsBytes: () => statisticsCharge(statistics?.length ?? 0),
    statisticsCapacityBytes: () =>
      statisticsCharge(Math.ceil(rows / QUERY_CHUNK_ROWS)),
    statisticsRowsScanned: () => scanned,
  });
}

/**
 * Equality over a UUID column's 16-byte cells, read in place rather than copied
 * per row. Only exact operators: nothing needs a UUID range.
 */
function uuidQueryVector(
  rows: number,
  validAt: (index: number) => boolean,
  cells: Uint8Array,
): ArtifactQueryVector {
  // The same cells as four words a row: a part starts 8-byte aligned.
  const words = new Uint32Array(cells.buffer, cells.byteOffset, rows * 4);
  /** The literal's four words, in the platform order `words` reads in. */
  const wordsOf = (literal: Uint8Array) =>
    new Uint32Array(Uint8Array.from(literal).buffer);
  const prepareCompare = (
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): ((index: number) => boolean) | undefined => {
    if (operator !== "eq" && operator !== "neq") return undefined;
    const equal = operator === "eq";
    if (literal === null)
      return (index: number) => (validAt(index) ? !equal : equal);
    // A null cell satisfies neither operator under a non-null literal.
    if (!(literal instanceof Uint8Array) || literal.byteLength !== 16)
      return (index: number) => !equal && validAt(index);
    const [t0, t1, t2, t3] = wordsOf(literal);
    return (index: number): boolean => {
      if (!validAt(index)) return false;
      const offset = index * 4;
      // Last word first: sequential and time-ordered UUIDs share a prefix, and
      // equality does not depend on the order the words are read in.
      return words[offset + 3] === t3 &&
        words[offset + 2] === t2 &&
        words[offset + 1] === t1 &&
        words[offset] === t0
        ? equal
        : !equal;
    };
  };
  // `prepareCompare` with each literal case hoisted out of the row loop.
  const prepareBlockCompare = (
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): ArtifactBlockCompare | undefined => {
    if (operator !== "eq" && operator !== "neq") return undefined;
    const hit = operator === "eq" ? 2 : 0;
    const miss = 2 - hit;
    if (literal === null)
      return (addresses, selection, count, out) => {
        let outside = 0;
        for (let index = 0; index < count; index++) {
          const position = selection[index] ?? 0;
          const address = addresses[position] ?? rows;
          if (address >= rows) {
            out[position] = 1;
            outside++;
          } else out[position] = validAt(address) ? miss : hit;
        }
        return outside;
      };
    if (!(literal instanceof Uint8Array) || literal.byteLength !== 16)
      return (addresses, selection, count, out) => {
        let outside = 0;
        for (let index = 0; index < count; index++) {
          const position = selection[index] ?? 0;
          const address = addresses[position] ?? rows;
          if (address >= rows) {
            out[position] = 1;
            outside++;
          } else out[position] = miss !== 0 && validAt(address) ? miss : 0;
        }
        return outside;
      };
    const [t0, t1, t2, t3] = wordsOf(literal);
    return (addresses, selection, count, out) => {
      let outside = 0;
      for (let index = 0; index < count; index++) {
        const position = selection[index] ?? 0;
        const address = addresses[position] ?? rows;
        if (address >= rows) {
          out[position] = 1;
          outside++;
          continue;
        }
        const offset = address * 4;
        const value =
          words[offset + 3] === t3 &&
          words[offset + 2] === t2 &&
          words[offset + 1] === t1 &&
          words[offset] === t0
            ? hit
            : miss;
        out[position] = value !== 0 && validAt(address) ? value : 0;
      }
      return outside;
    };
  };
  return Object.freeze({
    rows,
    chunkRows: QUERY_CHUNK_ROWS,
    compareAt(
      index: number,
      operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
      literal: Scalar,
    ) {
      return prepareCompare(operator, literal)?.(index);
    },
    prepareCompare,
    prepareBlockCompare,
    encoded: Object.freeze({ kind: "uuid", rows, cells, validAt }),
    chunkMayMatch: () => undefined,
    statisticsBytes: () => 0,
    statisticsCapacityBytes: () => 0,
    statisticsRowsScanned: () => 0,
  });
}

/**
 * Equality over a UTF-8 column's bytes, read in place without decoding, for a
 * column whose rows pass `rowsAreText`: a row then equals a string literal
 * exactly when its bytes are the literal's UTF-8 bytes. Only exact operators:
 * UTF-8 byte order is not the order JavaScript compares strings in.
 */
function textQueryVector(
  rows: number,
  validAt: (index: number) => boolean,
  offsets: Uint32Array,
  bytes: Uint8Array,
): ArtifactQueryVector {
  let areText: boolean | undefined;
  // Each row is an entry, as each code's text is in a dictionary.
  const rowsAreText = () =>
    (areText ??= dictionaryCodesAreText(offsets, bytes));
  const prepareCompare = (
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: Scalar,
  ): ((index: number) => boolean) | undefined => {
    if ((operator !== "eq" && operator !== "neq") || !rowsAreText())
      return undefined;
    const equal = operator === "eq";
    if (literal === null)
      return (index: number) => (validAt(index) ? !equal : equal);
    // A null cell satisfies neither operator under a non-null literal.
    if (typeof literal !== "string" || !literal.isWellFormed())
      return (index: number) => !equal && validAt(index);
    const target = new TextEncoder().encode(literal);
    const length = target.byteLength;
    return (index: number): boolean => {
      if (!validAt(index)) return false;
      const start = offsets[index] ?? 0;
      if ((offsets[index + 1] ?? 0) - start !== length) return !equal;
      for (let byte = 0; byte < length; byte++)
        if (bytes[start + byte] !== target[byte]) return !equal;
      return equal;
    };
  };
  // A filter the block path cannot take asks row by row, with one literal.
  let last:
    | {
        readonly operator: string;
        readonly literal: Scalar;
        readonly compare: ((index: number) => boolean) | undefined;
      }
    | undefined;
  return Object.freeze({
    rows,
    chunkRows: QUERY_CHUNK_ROWS,
    compareAt(
      index: number,
      operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
      literal: Scalar,
    ) {
      if (last?.operator !== operator || !Object.is(last.literal, literal))
        last = {
          operator,
          literal,
          compare: prepareCompare(operator, literal),
        };
      return last.compare?.(index);
    },
    prepareCompare,
    prepareBlockCompare: (
      operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
      literal: Scalar,
    ) => blockOf(rows, prepareCompare(operator, literal)),
    encoded: Object.freeze({
      kind: "text",
      rows,
      offsets,
      bytes,
      validAt,
      rowsAreText,
    }),
    chunkMayMatch: () => undefined,
    statisticsBytes: () => 0,
    statisticsCapacityBytes: () => 0,
    statisticsRowsScanned: () => 0,
  });
}

function registerQueryVector(
  column: ArtifactColumn,
  vector: ArtifactQueryVector,
): ArtifactColumn {
  QUERY_VECTORS.set(column, vector);
  return column;
}

function loadColumn(
  bytes: Uint8Array,
  payloadStart: number,
  rows: number,
  definition: ColumnDefinition,
  occupied: Array<readonly [number, number]>,
  timing: BindTiming,
): ArtifactColumn {
  const allParts = definition.parts.map((part) =>
    checkedPart(bytes, payloadStart, part, occupied),
  );
  const validity = definition.nullable ? allParts[0] : undefined;
  const parts = definition.nullable ? allParts.slice(1) : allParts;
  if (definition.nullable) {
    const required = Math.ceil(rows / 8);
    if (!validity || validity.length !== required)
      throw new Error("invalid direct-columnar validity bitmap");
    if (rows % 8 !== 0 && required > 0) {
      const usedMask = (1 << (rows % 8)) - 1;
      if ((at(validity.bytes, required - 1) & ~usedMask) !== 0)
        throw new Error("invalid direct-columnar validity padding");
    }
  }
  const isValid = (index: number) =>
    !validity || (at(validity.bytes, index >>> 3) & (1 << (index & 7))) !== 0;
  const checkRow = (index: number) => {
    if (!Number.isSafeInteger(index) || index < 0 || index >= rows)
      throw new RangeError("row is outside the column");
  };
  if (definition.type === "f64") {
    const part = parts[0];
    if (parts.length !== 1 || !part || part.length !== rows * 8)
      throw new Error("invalid f64 column length");
    const values = new Float64Array(bytes.buffer, part.offset, rows);
    for (let index = 0; index < values.length; index++)
      if (isValid(index) && !Number.isFinite(at(values, index)))
        throw new Error("non-finite numeric column value");
    const column: ArtifactColumn = Object.freeze({
      type: definition.type,
      rows,
      valueAt: (index: number) => {
        checkRow(index);
        if (!isValid(index)) return null;
        return at(values, index);
      },
      numberAt: (index: number) => {
        checkRow(index);
        return isValid(index) ? at(values, index) : null;
      },
      orderedAt: (index: number) => {
        checkRow(index);
        return isValid(index) ? at(values, index) : null;
      },
    });
    return registerQueryVector(
      column,
      queryVector(
        rows,
        isValid,
        (index) => at(values, index),
        (literal) => (typeof literal === "number" ? literal : undefined),
        new Set(["eq", "neq", "lt", "lte", "gt", "gte"]),
      ),
    );
  }
  if (
    definition.type === "int64" ||
    definition.type === "uint64" ||
    definition.type === "timestamp"
  )
    return loadExactNumericColumn(
      bytes,
      rows,
      definition,
      parts,
      isValid,
      checkRow,
    );
  if (definition.type === "bool") {
    const part = parts[0];
    if (parts.length !== 1 || !part || part.length !== rows)
      throw new Error("invalid boolean column length");
    for (let index = 0; index < part.bytes.length; index++)
      if (
        isValid(index) &&
        at(part.bytes, index) !== 0 &&
        at(part.bytes, index) !== 1
      )
        throw new Error("invalid boolean column value");
    const column: ArtifactColumn = Object.freeze({
      type: definition.type,
      rows,
      valueAt: (index: number) => {
        checkRow(index);
        return isValid(index) ? at(part.bytes, index) === 1 : null;
      },
      numberAt: (index: number) => {
        checkRow(index);
        return isValid(index) ? at(part.bytes, index) : null;
      },
    });
    registerKeyColumn(column, {
      fold: (states) => {
        for (let row = 0; row < rows; row++)
          states[row] = fnvByte(
            fnvByte(states[row] ?? 0, 4),
            part.bytes[row] ?? 0,
          );
      },
      equal: (left, right) => at(part.bytes, left) === at(part.bytes, right),
    });
    return registerQueryVector(
      column,
      queryVector(
        rows,
        isValid,
        (index) => at(part.bytes, index),
        (literal) =>
          typeof literal === "boolean" ? Number(literal) : undefined,
        new Set(["eq", "neq", "lt", "lte", "gt", "gte"]),
      ),
    );
  }
  if (
    definition.type === "utf8" ||
    definition.type === "bytes" ||
    definition.type === "json" ||
    definition.type === "decimal" ||
    definition.type === "date" ||
    definition.type === "time" ||
    definition.type === "interval"
  ) {
    const lengthsPart = parts[0];
    const dataPart = parts[1];
    if (parts.length !== 2 || !lengthsPart || !dataPart)
      throw new Error("invalid UTF-8 column shape");
    const started = now();
    const offsets = valueOffsets(
      u32(bytes, lengthsPart, rows + 1),
      dataPart.length,
    );
    timing.lengthsMs += now() - started;
    const read = stringReader(offsets, dataPart.bytes);
    const column: ArtifactColumn = Object.freeze({
      type: definition.type,
      rows,
      valueAt: (index: number) => {
        checkRow(index);
        if (!isValid(index)) return null;
        if (definition.type === "bytes")
          return Uint8Array.from(
            dataPart.bytes.subarray(at(offsets, index), at(offsets, index + 1)),
          );
        return definition.type === "json"
          ? (JSON.parse(read(index)) as unknown)
          : read(index);
      },
    });
    const tag = VARIABLE_KEY_TAGS[definition.type];
    if (tag !== undefined) {
      // The stored bytes, as the encoder hashed them. `valueAt` can differ
      // from them (a leading byte-order mark does not decode), so the index
      // is keyed on what the table holds, as the producer's index was.
      const values = dataPart.bytes;
      registerKeyColumn(column, {
        fold: (states) => {
          for (let row = 0; row < rows; row++) {
            const start = offsets[row] ?? 0;
            const end = offsets[row + 1] ?? 0;
            states[row] = fnvBytes(
              fnvU32(fnvByte(states[row] ?? 0, tag), end - start),
              values,
              start,
              end,
            );
          }
        },
        equal: (left, right) =>
          equalRanges(
            values,
            at(offsets, left),
            at(offsets, left + 1),
            at(offsets, right),
            at(offsets, right + 1),
          ),
      });
    }
    return definition.type === "utf8"
      ? registerQueryVector(
          column,
          textQueryVector(rows, isValid, offsets, dataPart.bytes),
        )
      : column;
  }
  if (definition.type === "uuid") {
    const part = parts[0];
    if (parts.length !== 1 || !part || part.length !== rows * 16)
      throw new Error("invalid UUID column length");
    const column: ArtifactColumn = Object.freeze({
      type: definition.type,
      rows,
      valueAt: (index: number) => {
        checkRow(index);
        if (!isValid(index)) return null;
        // `slice` copies: `part.bytes` is a plain `Uint8Array`, never a
        // `Buffer`, whose `slice` would share memory.
        return part.bytes.slice(index * 16, index * 16 + 16);
      },
    });
    registerKeyColumn(column, {
      fold: (states) => {
        for (let row = 0; row < rows; row++)
          states[row] = fnvBytes(
            fnvByte(states[row] ?? 0, 7),
            part.bytes,
            row * 16,
            row * 16 + 16,
          );
      },
      equal: (left, right) =>
        equalRanges(
          part.bytes,
          left * 16,
          left * 16 + 16,
          right * 16,
          right * 16 + 16,
        ),
    });
    return registerQueryVector(
      column,
      uuidQueryVector(rows, isValid, part.bytes),
    );
  }
  if (definition.type === "dict") {
    const codesPart = parts[0];
    const offsetsPart = parts[1];
    const dataPart = parts[2];
    const dictionaryCount = definition.dictionaryCount;
    const bitWidth = definition.bitWidth;
    if (
      parts.length !== 3 ||
      !codesPart ||
      !offsetsPart ||
      !dataPart ||
      dictionaryCount === undefined ||
      (bitWidth !== undefined &&
        bitWidth !== requiredBitWidth(Math.max(0, dictionaryCount - 1)))
    )
      throw new Error("invalid dictionary shape");
    const codeAt =
      bitWidth === undefined
        ? (() => {
            const raw = u32(bytes, codesPart, rows);
            return (index: number) => at(raw, index);
          })()
        : (() => {
            validatePacked(codesPart.bytes, rows, bitWidth);
            return packedNumberReader(codesPart.bytes, bitWidth);
          })();
    const offsets = u32(bytes, offsetsPart, dictionaryCount + 1);
    if (offsets[dictionaryCount] !== dataPart.length)
      throw new Error("invalid dictionary terminal offset");
    const read = stringAccessor(offsets, dataPart.bytes);
    checkSortedDictionary(offsets, dataPart.bytes);
    for (let index = 0; index < rows; index++) {
      const code = codeAt(index);
      if (isValid(index) && code >= dictionaryCount)
        throw new Error("invalid dictionary code");
      if (!isValid(index) && code !== 0)
        throw new Error("invalid nullable dictionary placeholder");
    }
    const cache = new Array<string | undefined>(dictionaryCount);
    const dictionaryAt = (code: number) => (cache[code] ??= read(code));
    const codeFor = (literal: Scalar): number | undefined => {
      if (typeof literal !== "string") return undefined;
      const target = new TextEncoder().encode(literal);
      let low = 0;
      let high = dictionaryCount;
      while (low < high) {
        const middle = low + ((high - low) >>> 1);
        const start = at(offsets, middle);
        const end = at(offsets, middle + 1);
        const candidate = dataPart.bytes.subarray(start, end);
        const shared = Math.min(candidate.length, target.length);
        let order = 0;
        for (let index = 0; index < shared; index++) {
          order = (candidate[index] ?? 0) - (target[index] ?? 0);
          if (order !== 0) break;
        }
        if (order === 0) order = candidate.length - target.length;
        if (order < 0) low = middle + 1;
        else high = middle;
      }
      if (low >= dictionaryCount) return undefined;
      const start = at(offsets, low);
      const end = at(offsets, low + 1);
      const candidate = dataPart.bytes.subarray(start, end);
      return equalBytes(candidate, target) ? low : undefined;
    };
    const column: ArtifactColumn = Object.freeze({
      type: definition.type,
      rows,
      valueAt: (index: number) => {
        checkRow(index);
        if (!isValid(index)) return null;
        const code = codeAt(index);
        return dictionaryAt(code);
      },
    });
    // Entries are strictly ascending, so equal codes are equal keys.
    registerKeyColumn(column, {
      fold: (states) => {
        for (let row = 0; row < rows; row++) {
          const code = codeAt(row);
          const start = at(offsets, code);
          const end = at(offsets, code + 1);
          states[row] = fnvBytes(
            fnvU32(fnvByte(states[row] ?? 0, 5), end - start),
            dataPart.bytes,
            start,
            end,
          );
        }
      },
      equal: (left, right) => codeAt(left) === codeAt(right),
    });
    let codesAreText: boolean | undefined;
    const areText = () =>
      (codesAreText ??= dictionaryCodesAreText(offsets, dataPart.bytes));
    let codesOrderAsText: boolean | undefined;
    // A four-byte sequence, the only encoding of a supplementary character,
    // is the only one whose lead byte is 0xF0 or above.
    const orderAsText = () =>
      (codesOrderAsText ??=
        areText() && dataPart.bytes.every((byte) => byte < 0xf0));
    // The code whose entry reads as `value`, which its UTF-8 bytes alone do
    // not settle: a lone surrogate encodes as U+FFFD's bytes.
    const codeOf = (value: Scalar) => {
      if (typeof value !== "string") return undefined;
      const code = codeFor(value);
      return code !== undefined && dictionaryAt(code) === value
        ? code
        : undefined;
    };
    const encoded: ArtifactEncodedColumn = Object.freeze({
      kind: "dict",
      rows,
      codes: dictionaryCount,
      validAt: isValid,
      codeAt,
      codeOf,
      codesAreText: areText,
      codesOrderAsText: orderAsText,
    });
    return registerQueryVector(
      column,
      queryVector(
        rows,
        isValid,
        codeAt,
        codeOf,
        // Code order is UTF-8 byte order but queries compare JavaScript
        // strings, so only equality is offered, and only while each code reads
        // as a text of its own: two entries that decode alike, one behind a
        // byte-order mark, hold one value under two codes.
        {
          has: (operator) =>
            (operator === "eq" || operator === "neq") && areText(),
        },
        encoded,
        // One code compare per valid row.
        (operator, literal) => {
          const hit = operator === "eq" ? 2 : 0;
          const miss = 2 - hit;
          const code = literal === null ? undefined : codeOf(literal);
          return (addresses, selection, count, out) => {
            let outside = 0;
            for (let index = 0; index < count; index++) {
              const position = selection[index] ?? 0;
              const address = addresses[position] ?? rows;
              if (address >= rows) {
                out[position] = 1;
                outside++;
              } else if (literal === null)
                out[position] = isValid(address) ? miss : hit;
              else {
                // A null row satisfies neither operator.
                const value =
                  code !== undefined && codeAt(address) === code ? hit : miss;
                out[position] = value !== 0 && isValid(address) ? value : 0;
              }
            }
            return outside;
          };
        },
      ),
    );
  }
  throw new Error("unknown direct-columnar column type");
}

/**
 * Whether every dictionary entry decodes, and none starts with a byte-order
 * mark the decoder would strip. The whole data part decodes exactly when each
 * entry does and no entry starts inside another's code point: a valid stream
 * is whole code points, each starting on a byte that is not a continuation
 * byte, so boundaries on such bytes cut it into valid entries.
 */
export function dictionaryCodesAreText(
  offsets: Uint32Array,
  bytes: Uint8Array,
): boolean {
  for (let code = 0; code + 1 < offsets.length; code++) {
    const start = at(offsets, code);
    if (start < bytes.length && (at(bytes, start) & 0xc0) === 0x80)
      return false;
    if (
      at(offsets, code + 1) - start >= 3 &&
      bytes[start] === 0xef &&
      bytes[start + 1] === 0xbb &&
      bytes[start + 2] === 0xbf
    )
      return false;
  }
  try {
    decoder.decode(bytes);
  } catch {
    return false;
  }
  return true;
}

/**
 * One primary-key column's share of the key index. `fold` folds each row's
 * canonical key bytes, as `keyBytes` builds them, into that row's FNV-1a state
 * in `states` without allocating; `equal` says whether two rows hold the same
 * key.
 */
interface KeyColumn {
  readonly fold: (states: Uint32Array) => void;
  readonly equal: (left: number, right: number) => boolean;
}

const KEY_COLUMNS = new WeakMap<ArtifactColumn, KeyColumn>();

function registerKeyColumn(column: ArtifactColumn, key: KeyColumn): void {
  KEY_COLUMNS.set(column, key);
}

/** `keyBytes`'s tag for each variable-width key kind. */
const VARIABLE_KEY_TAGS: Partial<Record<ColumnKind, number>> = {
  utf8: 5,
  bytes: 6,
  decimal: 8,
  date: 9,
  time: 10,
  interval: 11,
};

const FNV_OFFSET = 0x811c_9dc5;

function fnvByte(state: number, byte: number): number {
  return Math.imul(state ^ byte, 0x0100_0193);
}

/** Folds `value` modulo 2^32 in as four little-endian bytes. */
function fnvU32(state: number, value: number): number {
  state = fnvByte(state, value & 0xff);
  state = fnvByte(state, (value >>> 8) & 0xff);
  state = fnvByte(state, (value >>> 16) & 0xff);
  return fnvByte(state, value >>> 24);
}

/**
 * Folds a 64-bit integer in as its eight little-endian two's-complement
 * bytes. `fnvU32` reads a number modulo 2^32, which for an exact integer is
 * its low word.
 */
function fnvSafe(state: number, value: number): number {
  return fnvU32(fnvU32(state, value), Math.floor(value / 0x1_0000_0000));
}

/**
 * `fnvSafe` for any value; a bigint's `&` and `>>` act on its two's complement.
 */
function fnvExact(state: number, value: bigint): number {
  return fnvU32(
    fnvU32(state, Number(value & 0xffff_ffffn)),
    Number(value >> 32n),
  );
}

function fnvBytes(
  state: number,
  bytes: Uint8Array,
  start: number,
  end: number,
): number {
  for (let index = start; index < end; index++)
    state = fnvByte(state, at(bytes, index));
  return state;
}

function equalRanges(
  bytes: Uint8Array,
  leftStart: number,
  leftEnd: number,
  rightStart: number,
  rightEnd: number,
): boolean {
  if (leftEnd - leftStart !== rightEnd - rightStart) return false;
  for (let index = 0; index < leftEnd - leftStart; index++)
    if (at(bytes, leftStart + index) !== at(bytes, rightStart + index))
      return false;
  return true;
}

/**
 * A table's primary-key index, which the artifact does not carry: open
 * addressing with linear probing over `nextPowerOfTwo(max(1, 2 * rows))`
 * slots, each 0 or `row + 1`, a row probing from the FNV-1a hash of its
 * canonical key bytes, the hash `lookupKey` probes with. Refuses a table in
 * which two rows share a key: `lookupKey` would find only one of them, and
 * the overlay would read the other as absent.
 */
function buildKeyIndex(
  rows: number,
  keyColumns: readonly KeyColumn[],
): Uint32Array {
  // Every row's hash, one key column at a time; a probe compares two rows'
  // keys only when their hashes match.
  const hashes = new Uint32Array(rows).fill(FNV_OFFSET);
  for (const key of keyColumns) key.fold(hashes);
  const capacity = keyIndexCapacity(rows);
  const slots = new Uint32Array(capacity);
  const mask = capacity - 1;
  for (let row = 0; row < rows; row++) {
    const hash = hashes[row] ?? 0;
    let slot = hash & mask;
    for (;;) {
      const occupant = slots[slot] ?? 0;
      if (occupant === 0) break;
      if (
        hashes[occupant - 1] === hash &&
        keyColumns.every((key) => key.equal(occupant - 1, row))
      )
        throw new Error("duplicate direct-columnar primary key");
      slot = (slot + 1) & mask;
    }
    slots[slot] = row + 1;
  }
  return slots;
}

/** The slots `buildKeyIndex` allocates for a table of `rows` rows. */
function keyIndexCapacity(rows: number): number {
  let capacity = 1;
  while (capacity < rows * 2) capacity *= 2;
  return capacity;
}

/**
 * The peak memory of the primary-key indexes for tables of `rows` rows: every
 * table's slots, which stay resident, and the largest table's row hashes, since
 * `buildKeyIndex` builds one table at a time. The server computes the same
 * figure.
 */
function keyIndexBytes(rows: readonly number[]): number {
  let slots = 0;
  let hashes = 0;
  for (const count of rows) {
    slots += 4 * keyIndexCapacity(count);
    hashes = Math.max(hashes, 4 * count);
  }
  return slots + hashes;
}

/** Whether two byte strings hold the same bytes. */
export function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index++)
    if (left[index] !== right[index]) return false;
  return true;
}

/**
 * The microsecond count of a timestamp key in `timestampText`'s form, which
 * carries three fractional digits for a whole millisecond and six otherwise.
 * Any other spelling of the same instant is refused.
 */
function timestampMicros(value: unknown): bigint {
  if (typeof value !== "string") throw new Error("invalid primary key");
  const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}(\d{3})?Z$/u.exec(
    value,
  );
  if (!match) throw new Error("invalid primary key");
  const milliseconds = Date.parse(`${value.slice(0, 23)}Z`);
  if (!Number.isFinite(milliseconds)) throw new Error("invalid primary key");
  const micros = BigInt(milliseconds) * 1_000n + BigInt(match[1] ?? 0);
  if (timestampText(micros) !== value) throw new Error("invalid primary key");
  return micros;
}

function keyBytes(
  parts: readonly Scalar[],
  columns: readonly DirectColumnarColumnSchema[],
): Uint8Array {
  if (parts.length !== columns.length) throw new Error("invalid primary key");
  const chunks: Uint8Array[] = [];
  let length = 0;
  const fixed64 = (tag: number, value: bigint, signed: boolean) => {
    const chunk = new Uint8Array(9);
    chunk[0] = tag;
    const view = new DataView(chunk.buffer);
    if (signed) view.setBigInt64(1, value, true);
    else view.setBigUint64(1, value, true);
    chunks.push(chunk);
    length += chunk.byteLength;
  };
  const variable = (tag: number, value: Uint8Array) => {
    if (value.byteLength > 0xffff_ffff) throw new Error("invalid primary key");
    const prefix = new Uint8Array(5);
    prefix[0] = tag;
    new DataView(prefix.buffer).setUint32(1, value.byteLength, true);
    chunks.push(prefix, value);
    length += prefix.byteLength + value.byteLength;
  };
  for (let index = 0; index < parts.length; index++) {
    const value = parts[index];
    const type = columns[index]?.type;
    switch (type) {
      case "int64":
        if (typeof value !== "bigint") throw new Error("invalid primary key");
        fixed64(1, value, true);
        break;
      case "uint64":
        if (typeof value !== "bigint") throw new Error("invalid primary key");
        fixed64(2, value, false);
        break;
      case "timestamp":
        fixed64(3, timestampMicros(value), true);
        break;
      case "bool":
        if (typeof value !== "boolean") throw new Error("invalid primary key");
        chunks.push(Uint8Array.of(4, Number(value)));
        length += 2;
        break;
      case "utf8":
      case "dict":
        if (typeof value !== "string") throw new Error("invalid primary key");
        variable(5, new TextEncoder().encode(value));
        break;
      case "bytes":
        if (!(value instanceof Uint8Array))
          throw new Error("invalid primary key");
        variable(6, value);
        break;
      case "uuid":
        if (!(value instanceof Uint8Array) || value.byteLength !== 16)
          throw new Error("invalid primary key");
        chunks.push(Uint8Array.of(7), value);
        length += 17;
        break;
      case "decimal":
        if (typeof value !== "string") throw new Error("invalid primary key");
        variable(8, new TextEncoder().encode(value));
        break;
      case "date":
        if (typeof value !== "string") throw new Error("invalid primary key");
        variable(9, new TextEncoder().encode(value));
        break;
      case "time":
        if (typeof value !== "string") throw new Error("invalid primary key");
        variable(10, new TextEncoder().encode(value));
        break;
      case "interval":
        if (typeof value !== "string") throw new Error("invalid primary key");
        variable(11, new TextEncoder().encode(value));
        break;
      default:
        throw new Error("invalid primary key");
    }
  }
  const result = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function hashKeyBytes(bytes: Uint8Array): number {
  return fnvBytes(FNV_OFFSET, bytes, 0, bytes.byteLength) >>> 0;
}

/** A user-timing mark on the cold path, for a trace or a harness to read. */
function mark(name: string, detail?: unknown): void {
  globalThis.performance?.mark?.(
    `calque:${name}`,
    detail === undefined ? undefined : { detail },
  );
}

function now(): number {
  return globalThis.performance?.now?.() ?? 0;
}

/**
 * Time one interpretation spends in `valueOffsets` and in `buildKeyIndex`,
 * published with the `artifact:lengths` and `artifact:keys` marks.
 */
interface BindTiming {
  lengthsMs: number;
  keysMs: number;
}

async function digest(
  bytes: Uint8Array<ArrayBuffer>,
): Promise<`sha256:${string}`> {
  const result = await crypto.subtle.digest("SHA-256", bytes);
  return `sha256:${Array.from(new Uint8Array(result), (value) => value.toString(16).padStart(2, "0")).join("")}`;
}

/** A trusted artifact's identity and schema, once both are checked. */
interface ArtifactBinding {
  readonly identity: DirectColumnarArtifactIdentity;
  readonly schema: DirectColumnarSchema;
  /**
   * When a capacity is declared, the bytes it requires before the
   * primary-key indexes, and the capacity it declares.
   */
  readonly capacity:
    | { readonly requiredBytes: number; readonly capacityBytes: number }
    | undefined;
}

const SHA256_ID = /^sha256:[0-9a-f]{64}$/;

/**
 * Whether `chunks` names one digest for every `chunks.bytes` bytes of an
 * artifact `byteLength` long, the last covering what is left.
 */
function validChunks(chunks: unknown, byteLength: number): boolean {
  return (
    exactKeys(chunks, ["bytes", "sha256"]) &&
    safeNonNegativeInteger(chunks.bytes) &&
    Array.isArray(chunks.sha256) &&
    // A chunk of no bytes fails here too: no count equals `n / 0` rounded.
    chunks.sha256.length === Math.ceil(byteLength / chunks.bytes) &&
    chunks.sha256.every(
      (value) => typeof value === "string" && SHA256_ID.test(value),
    )
  );
}

/**
 * Checks the trusted binding, and the capacity an artifact of `artifactBytes`
 * needs, before any artifact byte is taken or read. `checkSchemaHash` checks
 * the schema's hash. The trusted schema names no row counts, so `interpret`
 * adds the primary-key indexes to the capacity from the verified header.
 */
function bindTrusted(
  trusted: TrustedArtifact,
  artifactBytes: number,
  capacity: ArtifactCapacity | undefined,
): ArtifactBinding {
  if (
    !(
      exactKeys(trusted, ["byteLength", "digest", "identity", "schema"]) ||
      exactKeys(trusted, [
        "byteLength",
        "chunks",
        "digest",
        "identity",
        "schema",
      ])
    ) ||
    !safeNonNegativeInteger(trusted.byteLength) ||
    !SHA256_ID.test(trusted.digest) ||
    (trusted.chunks !== undefined &&
      !validChunks(trusted.chunks, trusted.byteLength))
  )
    throw new Error("invalid trusted artifact binding");
  const trustedIdentity = parseIdentity(trusted.identity);
  const trustedSchema = parseDirectColumnarSchema(trusted.schema);
  if (trusted.byteLength !== artifactBytes)
    throw new Error("artifact length binding mismatch");
  if (capacity !== undefined) {
    if (
      !exactKeys(capacity, [
        "capacityBytes",
        "residentBytes",
        "standbyBytes",
        "safetyBytes",
      ]) ||
      !Object.values(capacity).every(safeNonNegativeInteger)
    )
      throw new Error("invalid direct-columnar capacity declaration");
    const required =
      capacity.residentBytes +
      capacity.standbyBytes +
      capacity.safetyBytes +
      trusted.byteLength;
    if (required > capacity.capacityBytes)
      throw new CapacityError(required, capacity.capacityBytes);
    return {
      identity: trustedIdentity,
      schema: trustedSchema,
      capacity: {
        requiredBytes: required,
        capacityBytes: capacity.capacityBytes,
      },
    };
  }
  return {
    identity: trustedIdentity,
    schema: trustedSchema,
    capacity: undefined,
  };
}

/** Refuses a trusted schema whose hash is not the one its identity names. */
async function checkSchemaHash(binding: ArtifactBinding): Promise<void> {
  if (
    (await digest(new TextEncoder().encode(JSON.stringify(binding.schema)))) !==
    binding.identity.schemaHash
  )
    throw new Error("trusted direct-columnar schema hash mismatch");
}

export async function openDirectColumnarArtifact(
  input: ArrayBuffer,
  trusted: TrustedArtifact,
  capacity?: ArtifactCapacity,
): Promise<DirectColumnarWorkspace> {
  if (!(input instanceof ArrayBuffer))
    throw new TypeError("artifact must be a transferable ArrayBuffer");
  const binding = bindTrusted(trusted, input.byteLength, capacity);
  await checkSchemaHash(binding);
  const owned = structuredClone(input, { transfer: [input] });
  mark("artifact:digest");
  if ((await digest(new Uint8Array(owned))) !== trusted.digest)
    throw new Error("artifact digest binding mismatch");
  mark("artifact:digested");
  return interpret(owned, binding).finish();
}

/** An artifact arriving in chunks; see `receiveDirectColumnarArtifact`. */
export interface ArtifactReceiver {
  /** How many chunks the plan names; `open` needs each accepted once. */
  readonly chunks: number;
  /**
   * Copies chunk `index` into place when `sha256`, the `sha256:<hex>` digest a
   * hasher took of `bytes`, matches the plan; the receiver computes no digest. It throws for an index outside the artifact, a chunk already held,
   * a wrong length or a wrong digest, and after a refusal `open` refuses the
   * artifact. An error found while interpreting the chunk is thrown by `open`.
   */
  accept(index: number, bytes: Uint8Array, sha256: string): void;
  /**
   * Interprets the rest of the artifact once every chunk is accepted. Rejects
   * when a chunk is missing or was refused.
   */
  open(): Promise<DirectColumnarWorkspace>;
}

/**
 * Receives an artifact while it downloads, one digested chunk at a time and in
 * any order. A chunk is copied into a buffer only the receiver holds once its
 * digest matches the plan, and once the schema matches its hash the receiver
 * interprets the header and then each table as the chunks held from the first
 * one on cover them. The plan binds every byte, so interpreting early reads
 * only what the server published.
 */
export function receiveDirectColumnarArtifact(
  trusted: TrustedArtifact,
  capacity?: ArtifactCapacity,
): ArtifactReceiver {
  const binding = bindTrusted(trusted, trusted.byteLength, capacity);
  const chunks = trusted.chunks;
  if (chunks === undefined)
    throw new Error("the trusted artifact names no chunk digests");
  const owned = new ArrayBuffer(trusted.byteLength);
  const bytes = new Uint8Array(owned);
  const interpretation = interpret(owned, binding);
  const accepted = new Uint8Array(chunks.sha256.length);
  let missing = chunks.sha256.length;
  /** How many chunks, from the first one on, are all held. */
  let held = 0;
  let schemaMatched = false;
  let refusal: { readonly error: Error } | undefined;
  let open = true;
  const advance = () => {
    if (schemaMatched)
      interpretation.advance(Math.min(held * chunks.bytes, bytes.byteLength));
  };
  // Settles either way, so a receiver that is never opened leaves no
  // rejection behind; `open` reports the mismatch.
  let schemaRefusal: { readonly error: unknown } | undefined;
  const schemaChecked = checkSchemaHash(binding).then(
    () => {
      // Marked so a trace shows when interpretation could begin.
      mark("artifact:schema");
      schemaMatched = true;
      advance();
    },
    (error: unknown) => {
      schemaRefusal = { error };
    },
  );
  return {
    chunks: chunks.sha256.length,
    accept(index, input, sha256) {
      if (!open || refusal) throw new Error("the artifact receiver is closed");
      const start = index * chunks.bytes;
      const end = Math.min(start + chunks.bytes, bytes.byteLength);
      let error: Error | undefined;
      if (!Number.isSafeInteger(index) || index < 0 || index >= accepted.length)
        error = new Error("artifact chunk index binding mismatch");
      else if (accepted[index] === 1)
        error = new Error("artifact chunk received twice");
      else if (input.byteLength !== end - start)
        error = new Error("artifact length binding mismatch");
      else if (sha256 !== chunks.sha256[index])
        error = new Error("artifact digest binding mismatch");
      if (error) {
        refusal = { error };
        throw error;
      }
      bytes.set(input, start);
      accepted[index] = 1;
      missing -= 1;
      while (held < accepted.length && accepted[held] === 1) held += 1;
      advance();
    },
    async open() {
      if (!open) throw new Error("the artifact receiver is closed");
      open = false;
      if (refusal) throw refusal.error;
      if (missing !== 0) throw new Error("artifact length binding mismatch");
      await schemaChecked;
      if (schemaRefusal) throw schemaRefusal.error;
      // Every chunk matched as it was accepted; the marks keep the phase
      // boundaries the cold-path harness reads.
      mark("artifact:digest");
      mark("artifact:digested");
      return interpretation.finish();
    },
  };
}

/**
 * Interprets an artifact whose bytes are verified from the first one up to
 * the end `advance` is given: the header once that end covers it, then each
 * table, in order, once it covers the table's parts. It reads no byte at or
 * past that end. The first error stops it, and `finish` throws it; otherwise
 * `finish`, called once every byte is verified, interprets the rest.
 */
function interpret(
  owned: ArrayBuffer,
  binding: ArtifactBinding,
): {
  advance(verified: number): void;
  finish(): DirectColumnarWorkspace;
} {
  const bytes = new Uint8Array(owned);
  const trustedIdentity = binding.identity;
  const trustedSchema = binding.schema;
  const occupied: Array<readonly [number, number]> = [];
  const tables = new Map<string, ArtifactTable>();
  const timing: BindTiming = { lengthsMs: 0, keysMs: 0 };
  let header: HeaderDefinition | undefined;
  let payloadStart = 0;
  let next = 0;
  let failure: { readonly error: unknown } | undefined;

  /** The header, once the verified bytes cover it. */
  const readHeader = (verified: number): HeaderDefinition | undefined => {
    if (header) return header;
    if (!PLATFORM_LITTLE_ENDIAN)
      throw new Error("direct-columnar requires a little-endian runtime");
    if (bytes.byteLength < 16)
      throw new Error("unknown direct-columnar format");
    if (verified < 16) return undefined;
    if (!MAGIC.every((value, index) => bytes[index] === value))
      throw new Error("unknown direct-columnar format");
    const headerLength = new DataView(owned).getUint32(MAGIC.byteLength, true);
    const headerEnd = MAGIC.byteLength + 4 + headerLength;
    if (
      headerLength === 0 ||
      headerLength > MAX_HEADER_BYTES ||
      headerEnd > bytes.byteLength
    )
      throw new Error("invalid direct-columnar header length");
    if (headerEnd > verified) return undefined;
    const parsed = parseHeader(
      JSON.parse(
        decoder.decode(bytes.subarray(MAGIC.byteLength + 4, headerEnd)),
      ) as unknown,
    );
    if (!sameIdentity(parsed.identity, trustedIdentity))
      throw new Error("direct-columnar artifact identity binding mismatch");
    if (parsed.tables.length !== trustedSchema.tables.length)
      throw new Error("direct-columnar schema binding mismatch");
    // The primary-key indexes the capacity must also hold, counted from the
    // verified header's rows before any column is read or index built.
    if (binding.capacity !== undefined) {
      const required =
        binding.capacity.requiredBytes +
        keyIndexBytes(parsed.tables.map((table) => table.rows));
      if (required > binding.capacity.capacityBytes)
        throw new CapacityError(required, binding.capacity.capacityBytes);
    }
    payloadStart = align8(headerEnd);
    header = parsed;
    return header;
  };

  /** Where the last of a table's parts ends, counted from the first byte. */
  const tableEnd = (table: TableDefinition): number => {
    let end = payloadStart;
    for (const column of table.columns)
      for (const part of column.parts)
        end = Math.max(end, payloadStart + part.offset + part.length);
    return end;
  };

  const addTable = (current: HeaderDefinition, tableIndex: number): void => {
    const table = current.tables[tableIndex];
    const expectedSchema = trustedSchema.tables[tableIndex];
    if (!table || !expectedSchema)
      throw new Error("direct-columnar schema binding mismatch");
    if (
      tables.has(table.name) ||
      table.name !== expectedSchema.name ||
      table.columns.length !== expectedSchema.columns.length ||
      !sameValues(table.primaryKey, expectedSchema.primaryKey)
    )
      throw new Error("invalid direct-columnar table");
    const columns = new Map<string, ArtifactColumn>();
    for (
      let columnIndex = 0;
      columnIndex < table.columns.length;
      columnIndex++
    ) {
      const column = table.columns[columnIndex];
      const expectedColumn = expectedSchema.columns[columnIndex];
      if (!column || !expectedColumn)
        throw new Error("direct-columnar schema binding mismatch");
      if (
        expectedColumn.name !== column.name ||
        expectedColumn.type !== column.type ||
        expectedColumn.nullable !== column.nullable ||
        columns.has(column.name)
      )
        throw new Error("direct-columnar schema binding mismatch");
      columns.set(
        column.name,
        loadColumn(bytes, payloadStart, table.rows, column, occupied, timing),
      );
    }
    mark(`artifact:columns:${table.name}`);
    const keyColumns = table.primaryKey.map((field) => {
      const column = columns.get(field);
      const key = column && KEY_COLUMNS.get(column);
      if (!key) throw new Error("primary-key column is unavailable");
      return key;
    });
    const started = now();
    const slots = buildKeyIndex(table.rows, keyColumns);
    timing.keysMs += now() - started;
    const indexAt = (slot: number) => at(slots, slot) - 1;
    const primaryKeyColumns = table.primaryKey.map((field) => {
      const column = expectedSchema.columns.find(
        (candidate) => candidate.name === field,
      );
      if (!column) throw new Error("primary-key column is unavailable");
      return column;
    });
    const keyAt = (row: number) =>
      table.primaryKey.map((field) => {
        const column = columns.get(field);
        if (!column) throw new Error("primary-key column is unavailable");
        return column.valueAt(row);
      });
    const lookupKey = (value: Scalar | readonly Scalar[]) => {
      const key =
        table.primaryKey.length === 1 && !Array.isArray(value)
          ? [value]
          : value;
      if (!Array.isArray(key) || key.length !== table.primaryKey.length)
        throw new Error("invalid primary key");
      const encoded = keyBytes(key, primaryKeyColumns);
      const mask = slots.length - 1;
      let slot = hashKeyBytes(encoded) & mask;
      for (let probes = 0; probes < slots.length; probes++) {
        const row = indexAt(slot);
        if (row === -1) return -1;
        if (equalBytes(encoded, keyBytes(keyAt(row), primaryKeyColumns)))
          return row;
        slot = (slot + 1) & mask;
      }
      throw new Error("invalid direct-columnar primary-key probe chain");
    };
    mark(`artifact:table:${table.name}`);
    tables.set(
      table.name,
      Object.freeze({
        name: table.name,
        rows: table.rows,
        primaryKey: Object.freeze([...table.primaryKey]),
        column: (name: string) => {
          const result = columns.get(name);
          if (!result) throw new Error("unknown column");
          return result;
        },
        lookupKey,
      }),
    );
  };

  const advance = (verified: number): void => {
    if (failure) return;
    try {
      const current = readHeader(verified);
      if (!current) return;
      while (next < current.tables.length) {
        const table = current.tables[next];
        if (table && tableEnd(table) > verified) return;
        addTable(current, next);
        next += 1;
      }
    } catch (error) {
      failure = { error };
    }
  };

  return {
    advance,
    finish() {
      // Every byte is verified: what is left is interpreted, and a part that
      // lies outside the artifact is refused where it is read.
      advance(Number.POSITIVE_INFINITY);
      if (failure) throw failure.error;
      mark("artifact:lengths", { lengthsMs: timing.lengthsMs });
      mark("artifact:keys", { keysMs: timing.keysMs });
      return Object.freeze({
        artifactByteLength: bytes.byteLength,
        rowCount: [...tables.values()].reduce(
          (total, table) => total + table.rows,
          0,
        ),
        table(name: string) {
          const result = tables.get(name);
          if (!result) throw new Error("unknown table");
          return result;
        },
      });
    },
  };
}
