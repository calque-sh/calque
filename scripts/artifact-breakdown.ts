#!/usr/bin/env node
// Where a direct-columnar artifact's bytes go, column by column, as stored and
// under the zstd Content-Encoding the server sends it with.
//
// Usage:
//   node --import ./scripts/register.mjs scripts/artifact-breakdown.ts \
//     <artifact> [--json <path>]
//
// <artifact> is a decoded artifact of format 4, or its zstd representation;
// `calque` keeps both under its state directory's `objects/`. For every column
// the report gives each part's stored bytes, its zstd size alone, and the
// column's marginal cost: the artifact's zstd size less that of the artifact
// without the column. It refuses a file whose parts, framing and padding do
// not cover every byte.
//
// Given a zstd representation, it recompresses the decoded bytes with the
// server's parameters and reports whether the result is the same bytes, so
// its zstd figures are the served ones.
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import process from "node:process";
import { constants, zstdCompressSync, zstdDecompressSync } from "node:zlib";

const TAG = "CALQART1";
const FORMAT = "calque-direct-columnar";
const VERSION = 4;
const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd] as const;

/** The parameters preparation encodes the zstd representation with. */
export const SERVER_ZSTD = {
  level: 12,
  windowLog: 23,
  workers: 12,
  jobBytes: 2 * 1024 * 1024,
  overlapLog: 5,
  hashLog: 21,
  checksum: false,
} as const;

type PartRole =
  | "validity"
  | "values"
  | "lengths"
  | "blocks"
  | "packed"
  | "codes"
  | "dictionary-offsets"
  | "dictionary-values";

interface PartBreakdown {
  readonly role: PartRole;
  /** From the start of the file, not of the payload. */
  readonly offset: number;
  readonly bytes: number;
  readonly zstdAlone: number;
}

interface ColumnBreakdown {
  readonly name: string;
  readonly kind: string;
  readonly nullable: boolean;
  /** `for` for a frame-of-reference numeric column, `dict`, or `plain`. */
  readonly encoding: string;
  readonly parts: readonly PartBreakdown[];
  readonly bytes: number;
  readonly zstdAlone: number;
  readonly marginal: number;
}

interface TableBreakdown {
  readonly name: string;
  readonly rows: number;
  readonly columns: readonly ColumnBreakdown[];
  readonly bytes: number;
  readonly marginal: number;
}

interface ArtifactBreakdown {
  readonly bytes: number;
  readonly sha256: string;
  readonly framing: {
    readonly tag: number;
    readonly headerLength: number;
    readonly header: number;
  };
  /** Zero bytes before each part that start it on an 8-byte boundary. */
  readonly padding: number;
  readonly tables: readonly TableBreakdown[];
  readonly zstd: {
    readonly bytes: number;
    readonly sha256: string;
    /** The whole less the artifact with its framing cut out. */
    readonly framingMarginal: number;
  };
  /** The zstd representation read, when the input was one. */
  readonly representation: {
    readonly bytes: number;
    readonly sha256: string;
    readonly sameBytes: boolean;
  } | null;
}

interface HeaderPart {
  readonly offset: number;
  readonly length: number;
}

interface HeaderColumn {
  readonly name: string;
  readonly type: string;
  readonly nullable: boolean;
  readonly encoding?: { readonly kind: string };
  readonly parts: readonly HeaderPart[];
}

interface HeaderTable {
  readonly name: string;
  readonly rows: number;
  readonly columns: readonly HeaderColumn[];
}

const VARIABLE_KINDS = new Set([
  "utf8",
  "bytes",
  "json",
  "decimal",
  "date",
  "time",
  "interval",
]);
const NUMERIC_KINDS = new Set(["int64", "uint64", "timestamp"]);
const FIXED_KINDS = new Set(["f64", "bool", "uuid"]);

/** The roles of a column's parts, in the order the encoder writes them. */
function partRoles(column: HeaderColumn): PartRole[] {
  const roles: PartRole[] = column.nullable ? ["validity"] : [];
  if (NUMERIC_KINDS.has(column.type)) {
    if (column.encoding === undefined) roles.push("values");
    else if (column.encoding.kind === "for") roles.push("blocks", "packed");
    else
      throw new Error(
        `${column.name}: unknown numeric encoding ${column.encoding.kind}`,
      );
  } else if (FIXED_KINDS.has(column.type)) roles.push("values");
  else if (VARIABLE_KINDS.has(column.type)) roles.push("lengths", "values");
  else if (column.type === "dict")
    roles.push("codes", "dictionary-offsets", "dictionary-values");
  else throw new Error(`${column.name}: unknown column kind ${column.type}`);
  return roles;
}

/** Compresses `bytes` the way the server's zstd representation is written. */
export function serverZstd(bytes: Uint8Array): Buffer {
  return zstdCompressSync(bytes, {
    params: {
      [constants.ZSTD_c_compressionLevel]: SERVER_ZSTD.level,
      [constants.ZSTD_c_windowLog]: SERVER_ZSTD.windowLog,
      [constants.ZSTD_c_nbWorkers]: SERVER_ZSTD.workers,
      [constants.ZSTD_c_jobSize]: SERVER_ZSTD.jobBytes,
      [constants.ZSTD_c_overlapLog]: SERVER_ZSTD.overlapLog,
      [constants.ZSTD_c_hashLog]: SERVER_ZSTD.hashLog,
      [constants.ZSTD_c_checksumFlag]: SERVER_ZSTD.checksum ? 1 : 0,
      [constants.ZSTD_c_contentSizeFlag]: 1,
      [constants.ZSTD_c_dictIDFlag]: 0,
    },
  });
}

/** `bytes` with every `[start, end)` range in `cut` removed. */
export function withoutRanges(
  bytes: Uint8Array,
  cut: readonly (readonly [number, number])[],
): Buffer {
  const kept: Uint8Array[] = [];
  let at = 0;
  for (const [start, end] of [...cut].sort((a, b) => a[0] - b[0])) {
    if (start < at || end < start || end > bytes.byteLength)
      throw new Error(`cannot cut [${String(start)}, ${String(end)})`);
    kept.push(bytes.subarray(at, start));
    at = end;
  }
  kept.push(bytes.subarray(at));
  return Buffer.concat(kept);
}

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

const align8 = (value: number): number => Math.ceil(value / 8) * 8;

function isZstd(bytes: Uint8Array): boolean {
  return ZSTD_MAGIC.every((value, index) => bytes[index] === value);
}

/** The header and the payload's start, refused unless it is format 4. */
function readHeader(bytes: Uint8Array): {
  readonly tables: readonly HeaderTable[];
  readonly headerBytes: number;
  readonly payloadStart: number;
} {
  if (
    bytes.byteLength < TAG.length + 4 ||
    Buffer.from(bytes.subarray(0, TAG.length)).toString("latin1") !== TAG
  )
    throw new Error(`not a direct-columnar artifact: no ${TAG} tag`);
  const headerBytes = Buffer.from(bytes).readUInt32LE(TAG.length);
  const headerEnd = TAG.length + 4 + headerBytes;
  if (headerEnd > bytes.byteLength)
    throw new Error("the header runs past the end of the file");
  const header = JSON.parse(
    Buffer.from(bytes.subarray(TAG.length + 4, headerEnd)).toString("utf8"),
  ) as { format?: unknown; version?: unknown; tables?: unknown };
  if (header.format !== FORMAT || header.version !== VERSION)
    throw new Error(
      `format ${String(header.format)} version ${String(header.version)} is not ${FORMAT} version ${String(VERSION)}`,
    );
  if (!Array.isArray(header.tables))
    throw new Error("the header names no tables");
  return {
    tables: header.tables as HeaderTable[],
    headerBytes,
    payloadStart: align8(headerEnd),
  };
}

/**
 * Every column's parts, refused unless the parts, the framing and zero
 * padding cover the file exactly once.
 */
function layout(bytes: Uint8Array) {
  const { tables, headerBytes, payloadStart } = readHeader(bytes);
  let end = TAG.length + 4 + headerBytes;
  let padding = 0;
  const pad = (from: number, to: number): void => {
    for (let at = from; at < to; at += 1)
      if (bytes[at] !== 0)
        throw new Error(`padding byte ${String(at)} is not zero`);
    padding += to - from;
  };
  const placed = tables.map((table) => ({
    name: table.name,
    rows: table.rows,
    columns: table.columns.map((column) => {
      const roles = partRoles(column);
      if (roles.length !== column.parts.length)
        throw new Error(
          `${table.name}.${column.name}: ${String(column.parts.length)} parts where a ${column.type} column has ${String(roles.length)}`,
        );
      const parts = column.parts.map((part, index) => {
        const offset = payloadStart + part.offset;
        if (offset % 8 !== 0 || offset < end)
          throw new Error(
            `${table.name}.${column.name}: a part at ${String(offset)} is misaligned or overlaps`,
          );
        pad(end, offset);
        end = offset + part.length;
        if (end > bytes.byteLength)
          throw new Error(
            `${table.name}.${column.name}: a part runs past the end of the file`,
          );
        return { role: roles[index]!, offset, bytes: part.length };
      });
      return {
        name: column.name,
        kind: column.type,
        nullable: column.nullable,
        encoding:
          column.encoding?.kind ?? (column.type === "dict" ? "dict" : "plain"),
        parts,
      };
    }),
  }));
  if (end !== bytes.byteLength)
    throw new Error(
      `${String(bytes.byteLength - end)} bytes follow the last part`,
    );
  return { tables: placed, headerBytes, padding, payloadStart };
}

/** The breakdown of a decoded artifact or of its zstd representation. */
export function breakdown(input: Uint8Array): ArtifactBreakdown {
  const encoded = isZstd(input) ? input : null;
  const bytes = encoded ? zstdDecompressSync(encoded) : input;
  const placed = layout(bytes);
  const whole = serverZstd(bytes);
  const without = (cut: readonly (readonly [number, number])[]): number =>
    whole.byteLength - serverZstd(withoutRanges(bytes, cut)).byteLength;
  const ranges = (parts: readonly { offset: number; bytes: number }[]) =>
    parts.map(
      (part) => [part.offset, part.offset + part.bytes] as [number, number],
    );
  const tables = placed.tables.map((table) => {
    const columns = table.columns.map((column) => {
      const parts = column.parts.map((part) => ({
        ...part,
        zstdAlone: serverZstd(
          bytes.subarray(part.offset, part.offset + part.bytes),
        ).byteLength,
      }));
      return {
        ...column,
        parts,
        bytes: parts.reduce((sum, part) => sum + part.bytes, 0),
        zstdAlone: parts.reduce((sum, part) => sum + part.zstdAlone, 0),
        marginal: without(ranges(parts)),
      };
    });
    return {
      name: table.name,
      rows: table.rows,
      columns,
      bytes: columns.reduce((sum, column) => sum + column.bytes, 0),
      marginal: without(ranges(columns.flatMap((column) => column.parts))),
    };
  });
  const wholeSha256 = sha256(whole);
  return {
    bytes: bytes.byteLength,
    sha256: sha256(bytes),
    framing: { tag: TAG.length, headerLength: 4, header: placed.headerBytes },
    padding: placed.padding,
    tables,
    zstd: {
      bytes: whole.byteLength,
      sha256: wholeSha256,
      framingMarginal: without([[0, TAG.length + 4 + placed.headerBytes]]),
    },
    representation: encoded
      ? {
          bytes: encoded.byteLength,
          sha256: sha256(encoded),
          sameBytes: sha256(encoded) === wholeSha256,
        }
      : null,
  };
}

const count = (value: number): string => value.toLocaleString("en-US");
const share = (value: number, of: number): string =>
  `${((100 * value) / of).toFixed(1)}%`;

/** The breakdown as a fixed-width table, the largest marginal cost first. */
export function formatBreakdown(result: ArtifactBreakdown): string {
  const columns = result.tables.flatMap((table) =>
    table.columns.map((column) => ({ table, column })),
  );
  const partBytes = columns.reduce((sum, { column }) => sum + column.bytes, 0);
  const framing =
    result.framing.tag + result.framing.headerLength + result.framing.header;
  const marginals = columns.reduce(
    (sum, { column }) => sum + column.marginal,
    0,
  );
  const lines = [
    `artifact ${count(result.bytes)} B, sha256 ${result.sha256}`,
    `zstd (level ${String(SERVER_ZSTD.level)}, window 2^${String(SERVER_ZSTD.windowLog)}, ${String(SERVER_ZSTD.workers)} workers in ${String(SERVER_ZSTD.jobBytes / 1024 / 1024)} MiB jobs at overlapLog ${String(SERVER_ZSTD.overlapLog)} and hashLog ${String(SERVER_ZSTD.hashLog)}, no checksum) ${count(result.zstd.bytes)} B, sha256 ${result.zstd.sha256}`,
  ];
  if (result.representation)
    lines.push(
      `representation read ${count(result.representation.bytes)} B, sha256 ${result.representation.sha256}: ${result.representation.sameBytes ? "the same bytes" : "DIFFERENT bytes"}`,
    );
  lines.push(
    "",
    "column                         kind       enc      rows       stored B   zstd alone   marginal  share  parts (stored B / zstd alone)",
  );
  const ranked = [...columns].sort(
    (a, b) => b.column.marginal - a.column.marginal,
  );
  for (const { table, column } of ranked)
    lines.push(
      [
        `${table.name}.${column.name}`.padEnd(30),
        column.kind.padEnd(10),
        column.encoding.padEnd(5),
        count(table.rows).padStart(8),
        count(column.bytes).padStart(14),
        count(column.zstdAlone).padStart(12),
        count(column.marginal).padStart(10),
        share(column.marginal, result.zstd.bytes).padStart(6),
        column.parts
          .map(
            (part) =>
              `${part.role} ${count(part.bytes)}/${count(part.zstdAlone)}`,
          )
          .join(", "),
      ].join(" "),
    );
  lines.push("", "table            rows       stored B   marginal");
  for (const table of result.tables)
    lines.push(
      [
        table.name.padEnd(12),
        count(table.rows).padStart(8),
        count(table.bytes).padStart(14),
        count(table.marginal).padStart(10),
      ].join(" "),
    );
  const kinds = new Map<string, { bytes: number; marginal: number }>();
  for (const { column } of columns) {
    const entry = kinds.get(column.kind) ?? { bytes: 0, marginal: 0 };
    entry.bytes += column.bytes;
    entry.marginal += column.marginal;
    kinds.set(column.kind, entry);
  }
  lines.push("", "kind             stored B   marginal");
  for (const [kind, entry] of [...kinds].sort(
    (a, b) => b[1].marginal - a[1].marginal,
  ))
    lines.push(
      [
        kind.padEnd(10),
        count(entry.bytes).padStart(14),
        count(entry.marginal).padStart(10),
      ].join(" "),
    );
  const rest = result.zstd.bytes - marginals - result.zstd.framingMarginal;
  lines.push(
    "",
    `stored: parts ${count(partBytes)} + framing ${count(framing)} (tag ${String(result.framing.tag)}, length ${String(result.framing.headerLength)}, JSON header ${count(result.framing.header)}) + padding ${count(result.padding)} = ${count(partBytes + framing + result.padding)} B, the file's ${count(result.bytes)}`,
    `zstd: whole ${count(result.zstd.bytes)} B = the columns' marginal costs ${count(marginals)} + framing's ${count(result.zstd.framingMarginal)} ${rest < 0 ? "-" : "+"} ${count(Math.abs(rest))}, which removing one column at a time does not attribute`,
  );
  return `${lines.join("\n")}\n`;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const jsonAt = args.indexOf("--json");
  const jsonPath = jsonAt === -1 ? null : args[jsonAt + 1];
  const positional =
    jsonAt === -1 ? args : args.filter((_, i) => i < jsonAt || i > jsonAt + 1);
  if (positional.length !== 1 || jsonPath === undefined) {
    process.stderr.write(
      "usage: artifact-breakdown.ts <artifact> [--json <path>]\n",
    );
    process.exit(2);
  }
  const result = breakdown(readFileSync(positional[0]!));
  if (jsonPath) writeFileSync(jsonPath, `${JSON.stringify(result, null, 2)}\n`);
  process.stdout.write(formatBreakdown(result));
}
