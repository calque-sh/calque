/**
 * The unpacker under node, on its own: every case of the fixture the packer
 * wrote (`spec/fixtures/packed-artifact`), in pieces and chunks of several
 * sizes, and a body for each refusal `spec/packed-artifact-v1.md` lists, put
 * together here with none of the unpacker's checks. The sample is the
 * fixture's `every-kind`.
 *
 * Where a body has more than one thing wrong, the unpacker names the first
 * it meets as the body arrives, which is not always the one `unpack` names
 * with the whole body in hand. Each such case says so.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { afterEach, describe, expect, test, vi } from "vitest";

import { artifactUnpacker, unpackArtifact } from "./columnar-unpacker.js";

type Bytes = Uint8Array<ArrayBuffer>;

const FIXTURE = new URL(
  "../../../../spec/fixtures/packed-artifact/",
  import.meta.url,
);

interface FixtureCase {
  readonly name: string;
  readonly body: "stored" | "packed";
  readonly artifactBytes: number;
  readonly artifactSha256: string;
  readonly packedBytes: number;
  readonly packedSha256: string;
  readonly hoistedParts: number;
  readonly transforms: { fk: number; int: number; sep: number };
  readonly escapes: number;
}

const MANIFEST = JSON.parse(
  readFileSync(new URL("manifest.json", FIXTURE), "utf8"),
) as { cases: FixtureCase[] };

function fixture(name: string, file: "artifact.bin" | "packed.bin"): Bytes {
  return new Uint8Array(readFileSync(new URL(`${name}/${file}`, FIXTURE)));
}

const sha256 = (bytes: Uint8Array): string =>
  createHash("sha256").update(bytes).digest("hex");

const MAGIC =
  "the body begins with neither a packed nor a stored artifact's magic";
const PREAMBLE = "the body's preamble cannot be read";
const HEADER = "the body's artifact header cannot be read";
const LONGER = "the body is longer than its artifact";
const TRUNCATED = "the body ends before its last part";
const TRAILING = "bytes follow the body's last part";
const defect = (table: number, column: number, name: string): string =>
  `the body's column ${column} of table ${table} has the defect ${name}`;

const encoder = new TextEncoder();
const align8 = (value: number): number => Math.ceil(value / 8) * 8;

function must<T>(value: T | undefined, what: string): T {
  if (value === undefined) throw new Error(`the test has no ${what}`);
  return value;
}

function concat(...pieces: readonly Uint8Array[]): Bytes {
  const bytes = new Uint8Array(
    pieces.reduce((total, piece) => total + piece.length, 0),
  );
  let at = 0;
  for (const piece of pieces) {
    bytes.set(piece, at);
    at += piece.length;
  }
  return bytes;
}

function u32(value: number): Bytes {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, true);
  return bytes;
}

function i64(value: bigint): Bytes {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigInt64(0, value, true);
  return bytes;
}

// The artifact, as the format's page describes it.

interface Extent {
  offset: number;
  length: number;
}

interface HeaderColumn {
  name: string;
  type: string;
  nullable?: boolean;
  encoding?: unknown;
  parts: Extent[];
}

interface Header {
  format: string;
  version: number;
  tables: {
    name: string;
    rows: number;
    primaryKey: string[];
    columns: HeaderColumn[];
  }[];
}

/** The header as JSON and the payload's start, read without any check. */
function headerOf(artifact: Uint8Array): {
  header: Header;
  payloadStart: number;
} {
  const length = new DataView(artifact.buffer, artifact.byteOffset).getUint32(
    8,
    true,
  );
  return {
    header: JSON.parse(
      new TextDecoder().decode(artifact.subarray(12, 12 + length)),
    ) as Header,
    payloadStart: align8(12 + length),
  };
}

/** An artifact of this header and payload. */
function frame(header: unknown, payload: Uint8Array): Bytes {
  const json = encoder.encode(JSON.stringify(header));
  return concat(
    encoder.encode("CALQART1"),
    u32(json.length),
    json,
    new Uint8Array(align8(12 + json.length) - 12 - json.length),
    payload,
  );
}

/** A column written by hand: its parts as the payload holds them. */
interface ForgedColumn {
  readonly name: string;
  readonly type: string;
  readonly nullable?: boolean;
  /** Whether the header says frame of reference in blocks of 1,024 rows. */
  readonly encoded?: boolean;
  readonly parts: readonly Uint8Array[];
  /** The order the header lists the parts in, when not the payload's. */
  readonly listed?: readonly number[];
}

interface ForgedTable {
  readonly name: string;
  readonly rows: number;
  readonly primaryKey: readonly string[];
  readonly columns: readonly ForgedColumn[];
}

/** An artifact holding `tables` in the order given, part after part. */
function forge(tables: readonly ForgedTable[]): Bytes {
  const payload: Uint8Array[] = [];
  let end = 0;
  const header = {
    format: "calque-direct-columnar",
    version: 4,
    tables: tables.map((table) => ({
      name: table.name,
      rows: table.rows,
      primaryKey: table.primaryKey,
      columns: table.columns.map((column) => {
        const extents = column.parts.map((part) => {
          payload.push(new Uint8Array(align8(end) - end), part);
          const offset = align8(end);
          end = offset + part.length;
          return { offset, length: part.length };
        });
        return {
          name: column.name,
          type: column.type,
          nullable: column.nullable ?? false,
          parts: column.listed
            ? column.listed.map((part) => must(extents[part], "such part"))
            : extents,
          ...(column.encoded
            ? { encoding: { kind: "for", blockRows: 1024 } }
            : {}),
        };
      }),
    })),
  };
  return frame(header, concat(...payload));
}

function bitmap(present: readonly boolean[]): Bytes {
  const bytes = new Uint8Array(Math.ceil(present.length / 8));
  present.forEach((here, row) => {
    if (here) bytes[row >> 3] = (bytes[row >> 3] ?? 0) | (1 << (row & 7));
  });
  return bytes;
}

const int64s = (values: readonly bigint[]): Bytes =>
  concat(...values.map((value) => i64(value)));

/** One uuid for each number: 16 bytes of it. */
const uuids = (...fills: readonly number[]): Bytes =>
  concat(...fills.map((fill) => new Uint8Array(16).fill(fill)));

/** A text column's lengths and values parts. */
function texts(values: readonly string[]): [Bytes, Bytes] {
  const encoded = values.map((value) => encoder.encode(value));
  return [
    concat(u32(0), ...encoded.map((value) => u32(value.length))),
    concat(...encoded),
  ];
}

/**
 * One frame-of-reference block: its header's base, width and scale, and each
 * row's number, the value itself at width 64.
 */
interface Block {
  readonly base: bigint;
  readonly bits: number;
  readonly scale: number;
  readonly numbers: readonly bigint[];
}

/** A frame-of-reference column's block headers and packed parts. */
function blocks(list: readonly Block[]): [Bytes, Bytes] {
  const headers: Uint8Array[] = [];
  const packed: number[] = [];
  for (const { base, bits, scale, numbers } of list) {
    headers.push(
      u32(packed.length),
      i64(base),
      Uint8Array.of(bits, scale, 0, 0),
    );
    if (bits === 64) {
      for (const number of numbers)
        packed.push(...i64(BigInt.asIntN(64, number)));
      continue;
    }
    const start = packed.length;
    for (let byte = 0; byte < Math.ceil((numbers.length * bits) / 8); byte++)
      packed.push(0);
    numbers.forEach((number, row) => {
      for (let bit = 0; bit < bits; bit++)
        if ((number >> BigInt(bit)) & 1n) {
          const at = row * bits + bit;
          packed[start + (at >> 3)] =
            (packed[start + (at >> 3)] ?? 0) | (1 << (at & 7));
        }
    });
  }
  return [concat(...headers), Uint8Array.from(packed)];
}

/** `codes` in `width` byte planes, least significant first. */
function planes(codes: readonly bigint[], width: number): Bytes {
  const bytes = new Uint8Array(codes.length * width);
  codes.forEach((code, row) => {
    for (let plane = 0; plane < width; plane++)
      bytes[plane * codes.length + row] = Number(
        (code >> BigInt(8 * plane)) & 0xffn,
      );
  });
  return bytes;
}

/**
 * The zigzag deltas of `values`, each from the one before and the first from
 * 0.
 */
function zigzags(values: readonly bigint[]): bigint[] {
  return values.map((value, index) => {
    const delta = value - (values[index - 1] ?? 0n);
    return delta >= 0n ? 2n * delta : -2n * delta - 1n;
  });
}

// The body, as the format's page describes it.

interface Entry {
  table: number;
  column: number;
  transform: "fk" | "int" | "sep";
  references: number | null;
  width: number;
  escapes: number;
  streamBytes: number;
}

interface Hoisted {
  table: number;
  column: number;
  part: number;
}

interface Preamble {
  version: number;
  hoisted: Hoisted[];
  columns: Entry[];
}

/** The preamble as the packer writes it. */
function spelled({ version, hoisted, columns }: Preamble): string {
  return JSON.stringify({
    version,
    hoisted: hoisted.map(({ table, column, part }) => ({
      table,
      column,
      part,
    })),
    columns: columns.map((entry) => ({
      table: entry.table,
      column: entry.column,
      transform: entry.transform,
      references: entry.references,
      width: entry.width,
      escapes: entry.escapes,
      streamBytes: entry.streamBytes,
    })),
  });
}

/** A packed body's first pieces: magic, preamble length and preamble. */
function framed(preamble: string | Uint8Array, rest: Uint8Array): Bytes {
  const bytes =
    typeof preamble === "string" ? encoder.encode(preamble) : preamble;
  return concat(encoder.encode("CALQPAK1"), u32(bytes.length), bytes, rest);
}

interface Listed extends Hoisted, Extent {
  /** How many parts its column has. */
  count: number;
}

/** Every part of an artifact, in header order. */
function listed(artifact: Uint8Array): Listed[] {
  return headerOf(artifact).header.tables.flatMap((table, tableIndex) =>
    table.columns.flatMap((column, columnIndex) =>
      column.parts.map(({ offset, length }, part) => ({
        table: tableIndex,
        column: columnIndex,
        part,
        offset,
        length,
        count: column.parts.length,
      })),
    ),
  );
}

const names = (part: Hoisted, other: Hoisted): boolean =>
  part.table === other.table &&
  part.column === other.column &&
  part.part === other.part;

/**
 * What a body holds for each part, in the body's order: the hoisted parts,
 * then every other part in payload order, a stream standing for its column's
 * last part, or for a `sep` column's last two.
 */
function laidOut(
  artifact: Uint8Array,
  preamble: Preamble,
): ({ part: Listed } | { stream: number })[] {
  const parts = listed(artifact);
  const pieces: ({ part: Listed } | { stream: number })[] = [];
  for (const entry of preamble.hoisted) {
    const part = parts.find((part) => names(part, entry));
    if (part) pieces.push({ part });
  }
  // The sort is stable, so equal extents stay in header order.
  for (const part of [...parts].sort(
    (a, b) => a.offset - b.offset || a.length - b.length,
  )) {
    if (preamble.hoisted.some((entry) => names(part, entry))) continue;
    const entry = preamble.columns.findIndex(
      ({ table, column }) => table === part.table && column === part.column,
    );
    const column = preamble.columns[entry];
    if (!column) {
      pieces.push({ part });
      continue;
    }
    const replaced = Math.max(
      0,
      part.count - (column.transform === "sep" ? 2 : 1),
    );
    if (part.part < replaced) pieces.push({ part });
    else if (part.part === replaced) pieces.push({ stream: entry });
  }
  return pieces;
}

/**
 * The body the format's page describes for these pieces, put together with
 * none of the unpacker's checks. `text` is the preamble's spelling when it
 * is not the packer's.
 */
function assemble(
  artifact: Uint8Array,
  preamble: Preamble,
  streams: readonly Uint8Array[],
  text: string = spelled(preamble),
): Bytes {
  const { payloadStart } = headerOf(artifact);
  return concat(
    framed(text, artifact.subarray(0, payloadStart)),
    ...laidOut(artifact, preamble).map((piece) =>
      "part" in piece
        ? artifact.subarray(
            payloadStart + piece.part.offset,
            payloadStart + piece.part.offset + piece.part.length,
          )
        : must(streams[piece.stream], "stream for the entry"),
    ),
  );
}

/** A packed body taken apart, to be changed and put together again. */
interface Taken {
  artifact: Bytes;
  preamble: Preamble;
  /** Per entry of the preamble's columns. */
  streams: Bytes[];
}

/** Takes apart the packer's `body` of `artifact`. */
function take(artifact: Bytes, body: Uint8Array): Taken {
  const length = new DataView(body.buffer, body.byteOffset).getUint32(8, true);
  const preamble = JSON.parse(
    new TextDecoder().decode(body.subarray(12, 12 + length)),
  ) as Preamble;
  const streams = preamble.columns.map(() => new Uint8Array(0));
  let at = 12 + length + headerOf(artifact).payloadStart;
  for (const piece of laidOut(artifact, preamble))
    if ("part" in piece) at += piece.part.length;
    else {
      const { streamBytes } = must(preamble.columns[piece.stream], "entry");
      streams[piece.stream] = body.slice(at, at + streamBytes);
      at += streamBytes;
    }
  const taken = { artifact, preamble, streams };
  // The documented layout, put together by the test, is the packer's.
  if (Buffer.compare(bodyOf(taken), body) !== 0)
    throw new Error("the fixture's body is not laid out as the page says");
  return taken;
}

const bodyOf = (taken: Taken, text?: string): Bytes =>
  assemble(taken.artifact, taken.preamble, taken.streams, text);

/** The indexes of a table and one of its columns. */
function named(taken: Taken, table: string, column: string): [number, number] {
  const { tables } = headerOf(taken.artifact).header;
  const at = tables.findIndex(({ name }) => name === table);
  const found = must(tables[at], `table ${table}`).columns.findIndex(
    ({ name }) => name === column,
  );
  if (found < 0) throw new Error(`the test has no column ${column}`);
  return [at, found];
}

/** The index of a column's entry among the preamble's columns. */
function entryOf(taken: Taken, table: string, column: string): number {
  const [tableIndex, columnIndex] = named(taken, table, column);
  const entry = taken.preamble.columns.findIndex(
    (entry) => entry.table === tableIndex && entry.column === columnIndex,
  );
  if (entry < 0) throw new Error(`${table}.${column} is not transformed`);
  return entry;
}

const entryAt = (taken: Taken, entry: number): Entry =>
  must(taken.preamble.columns[entry], "entry");

const streamAt = (taken: Taken, entry: number): Bytes =>
  must(taken.streams[entry], "stream");

/** Where one of a column's parts begins in the artifact, and its length. */
function partOf(
  taken: Taken,
  table: string,
  column: string,
  part: number,
): { start: number; length: number } {
  const [tableIndex, columnIndex] = named(taken, table, column);
  const { header, payloadStart } = headerOf(taken.artifact);
  const extent = must(
    header.tables[tableIndex]?.columns[columnIndex]?.parts[part],
    "part",
  );
  return { start: payloadStart + extent.offset, length: extent.length };
}

/** Replaces a column's stream and the length the preamble gives it. */
function restream(taken: Taken, entry: number, stream: Uint8Array): void {
  entryAt(taken, entry).streamBytes = stream.length;
  taken.streams[entry] = stream.slice();
}

const defectOf = (
  taken: Taken,
  table: string,
  column: string,
  name: string,
): string => defect(...named(taken, table, column), name);

/**
 * Why the unpacker refuses `body` as an artifact of `decodedBytes`: the same
 * given whole and given seven bytes at a time.
 */
function refusal(body: Uint8Array, decodedBytes: number): string {
  let whole: string | undefined;
  try {
    unpackArtifact(body, decodedBytes);
  } catch (error) {
    whole = (error as Error).message;
  }
  if (whole === undefined)
    throw new Error(`the body restored ${decodedBytes} bytes`);
  const unpacker = artifactUnpacker(decodedBytes, 4096, 0, true);
  let pieces: string | undefined;
  for (let at = 0; at < body.length && pieces === undefined; at += 7)
    pieces = unpacker.write(body.subarray(at, at + 7));
  expect(pieces ?? unpacker.end()).toBe(whole);
  // Once refused, a body stays refused for the same reason.
  expect(unpacker.write(Uint8Array.of(0))).toBe(whole);
  expect(unpacker.end()).toBe(whole);
  return whole;
}

const refusalOf = (taken: Taken, text?: string): string =>
  refusal(bodyOf(taken, text), taken.artifact.length);

function restores(body: Uint8Array, artifact: Uint8Array): void {
  expect(Buffer.compare(unpackArtifact(body, artifact.length), artifact)).toBe(
    0,
  );
}

/**
 * `body` restored in pieces of `pieceBytes` into chunks of `chunkBytes`,
 * each chunk taken as soon as `ready` has passed it and held to `artifact`
 * then. Returns how many chunks it took, or why the body was refused.
 */
function stream(
  body: Uint8Array,
  artifact: Uint8Array,
  pieceBytes: number,
  chunkBytes: number,
  skipChunks = 0,
  packed = true,
): number | string {
  const unpacker = artifactUnpacker(
    artifact.length,
    chunkBytes,
    skipChunks,
    packed,
  );
  const chunks = Math.ceil(artifact.length / chunkBytes);
  let index = 0;
  let ready = 0;
  const drain = (): void => {
    if (unpacker.ready() < ready) throw new Error("ready went back");
    ready = unpacker.ready();
    for (
      ;
      index < chunks &&
      ready >= Math.min((index + 1) * chunkBytes, artifact.length);
      index++
    ) {
      const chunk = unpacker.take(index);
      const expected = artifact.subarray(
        index * chunkBytes,
        (index + 1) * chunkBytes,
      );
      if (chunk.length !== expected.length)
        throw new Error(`chunk ${index} is ${chunk.length} bytes`);
      if (index >= skipChunks && Buffer.compare(chunk, expected) !== 0)
        throw new Error(`chunk ${index} is not the artifact's`);
    }
  };
  for (let at = 0; at < body.length; at += pieceBytes) {
    const refused = unpacker.write(body.subarray(at, at + pieceBytes));
    if (refused !== undefined) return refused;
    drain();
  }
  const refused = unpacker.end();
  if (refused !== undefined) return refused;
  drain();
  if (ready !== artifact.length) throw new Error("the artifact is not ready");
  return index;
}

/**
 * The fixture's `every-kind`, taken apart. Its tables are items, links,
 * nothing, owners, pair, ticks and zones.
 */
const SAMPLE = take(
  fixture("every-kind", "artifact.bin"),
  fixture("every-kind", "packed.bin"),
);
const sample = (): Taken => structuredClone(SAMPLE);

/**
 * A table of one column of zeros, which an `int` stream of no bytes stands
 * for. The packer writes no body longer than its artifact and the unpacker
 * refuses one, so an artifact forged here ends with this table, and its
 * body's preamble with `fillEntry`, which leaves the body the shorter.
 */
const FILL: ForgedTable = {
  name: "fill",
  rows: 100,
  primaryKey: [],
  columns: [{ name: "zero", type: "int64", parts: [new Uint8Array(800)] }],
};

/** The entry that packs `FILL`, as table `table`, to nothing. */
const fillEntry = (table: number): Entry => ({
  table,
  column: 0,
  transform: "int",
  references: null,
  width: 0,
  escapes: 0,
  streamBytes: 0,
});

/**
 * Table 0 of two key rows, then table 1 of three rows whose second column
 * is `column`, then the table of zeros.
 */
function keyed(column: ForgedColumn): Bytes {
  return forge([
    {
      name: "keys",
      rows: 2,
      primaryKey: ["id"],
      columns: [{ name: "id", type: "uuid", parts: [uuids(0xaa, 0xbb)] }],
    },
    {
      name: "rows",
      rows: 3,
      primaryKey: [],
      columns: [
        { name: "flag", type: "bool", parts: [Uint8Array.of(1, 0, 1)] },
        column,
      ],
    },
    FILL,
  ]);
}

/**
 * A body of `keyed`'s `artifact` whose preamble puts one transform on
 * column 1 of table 1, over `stream`.
 */
function oneStream(
  artifact: Bytes,
  transform: Entry["transform"],
  references: number | null,
  width: number,
  stream: Uint8Array,
): Taken {
  return {
    artifact,
    preamble: {
      version: 1,
      hoisted: [],
      columns: [
        {
          table: 1,
          column: 1,
          transform,
          references,
          width,
          escapes: 0,
          streamBytes: stream.length,
        },
        fillEntry(2),
      ],
    },
    streams: [stream.slice(), new Uint8Array(0)],
  };
}

describe("the fixture the packer wrote", () => {
  test("holds the five cases its manifest names, at their lengths and digests", () => {
    expect(MANIFEST.cases.map(({ name, body }) => [name, body])).toEqual([
      ["emit-fixture", "stored"],
      ["emit-key-fixture", "packed"],
      ["every-kind", "packed"],
      ["tables-smallest-first", "packed"],
      ["stored", "stored"],
    ]);
    for (const listed of MANIFEST.cases) {
      const artifact = fixture(listed.name, "artifact.bin");
      const packed = fixture(listed.name, "packed.bin");
      expect(artifact.length, listed.name).toBe(listed.artifactBytes);
      expect(sha256(artifact), listed.name).toBe(listed.artifactSha256);
      expect(packed.length, listed.name).toBe(listed.packedBytes);
      expect(sha256(packed), listed.name).toBe(listed.packedSha256);
      expect(new TextDecoder().decode(packed.subarray(0, 8)), listed.name).toBe(
        listed.body === "stored" ? "CALQART1" : "CALQPAK1",
      );
    }
  });

  test.each(MANIFEST.cases)(
    "$name is restored byte for byte from the whole body",
    ({ name, artifactBytes, artifactSha256 }) => {
      const restored = unpackArtifact(
        fixture(name, "packed.bin"),
        artifactBytes,
      );
      expect(restored.length).toBe(artifactBytes);
      expect(sha256(restored)).toBe(artifactSha256);
    },
  );

  test.each(MANIFEST.cases)(
    "$name is restored in pieces and chunks of every size, each chunk final once ready has passed it",
    ({ name }) => {
      const artifact = fixture(name, "artifact.bin");
      const packed = fixture(name, "packed.bin");
      for (const pieceBytes of [1, 2, 3, 7, 64, 1000, 65_536, packed.length])
        for (const chunkBytes of [1, 5, 8, 64, 4096, 4 * 1024 * 1024]) {
          // A chunk for every byte of the larger cases is the same proof,
          // slower.
          if (chunkBytes === 1 && artifact.length > 10_000) continue;
          expect(
            stream(packed, artifact, pieceBytes, chunkBytes),
            `${pieceBytes} ${chunkBytes}`,
          ).toBe(Math.ceil(artifact.length / chunkBytes));
        }
    },
  );

  test("the chunks an earlier response delivered are restored and left unwritten", () => {
    const artifact = fixture("every-kind", "artifact.bin");
    const packed = fixture("every-kind", "packed.bin");
    const chunkBytes = 4096;
    const chunks = Math.ceil(artifact.length / chunkBytes);
    for (const skipChunks of [1, 5, chunks - 1, chunks]) {
      // `stream` holds every chunk from `skipChunks` on to the artifact.
      expect(stream(packed, artifact, 1000, chunkBytes, skipChunks)).toBe(
        chunks,
      );
      const unpacker = artifactUnpacker(
        artifact.length,
        chunkBytes,
        skipChunks,
        true,
      );
      expect(unpacker.write(packed)).toBeUndefined();
      expect(unpacker.end()).toBeUndefined();
      expect(unpacker.ready()).toBe(artifact.length);
      // A skipped chunk has its length and none of the artifact's bytes.
      const skipped = unpacker.take(skipChunks - 1);
      expect(skipped.length).toBe(
        Math.min(chunkBytes, artifact.length - (skipChunks - 1) * chunkBytes),
      );
      expect(skipped.every((byte) => byte === 0)).toBe(true);
    }
  });

  test("ready stays in a text column's lengths until its stream is whole, and in a row an escape will fill", () => {
    const taken = sample();
    const body = bodyOf(taken);
    const { payloadStart } = headerOf(taken.artifact);
    /** Where each stream begins in the body. */
    const begins = new Map<number, number>();
    let at = 12 + spelled(taken.preamble).length + payloadStart;
    for (const piece of laidOut(taken.artifact, taken.preamble))
      if ("part" in piece) at += piece.part.length;
      else {
        begins.set(piece.stream, at);
        at += entryAt(taken, piece.stream).streamBytes;
      }
    const readyAfter = (bytes: number): number => {
      const unpacker = artifactUnpacker(taken.artifact.length, 4096, 0, true);
      expect(unpacker.write(body.subarray(0, bytes))).toBeUndefined();
      return unpacker.ready();
    };

    // ticks.level is 1,100 values: with all but its last byte in, the last
    // row's length is unknown and every earlier one is written.
    const level = entryOf(taken, "ticks", "level");
    const lengths = partOf(taken, "ticks", "level", 0);
    const values = partOf(taken, "ticks", "level", 1);
    const whole =
      must(begins.get(level), "stream") + streamAt(taken, level).length;
    expect(readyAfter(whole - 1)).toBe(lengths.start + lengths.length - 4);
    expect(readyAfter(whole)).toBeGreaterThanOrEqual(
      values.start + values.length,
    );

    // items.owner_id has 200 rows in two planes, and row 37 takes the one
    // escape, which follows the planes.
    const owner = entryOf(taken, "items", "owner_id");
    const uuidsAt = partOf(taken, "items", "owner_id", 0);
    const planesEnd = must(begins.get(owner), "stream") + 400;
    // Nothing of a row is known before its second plane's byte.
    expect(readyAfter(planesEnd - 200)).toBe(uuidsAt.start);
    expect(readyAfter(planesEnd - 200 + 10)).toBe(uuidsAt.start + 16 * 10);
    expect(readyAfter(planesEnd)).toBe(uuidsAt.start + 16 * 37);
    expect(readyAfter(planesEnd + 15)).toBe(uuidsAt.start + 16 * 37);
    expect(readyAfter(planesEnd + 16)).toBeGreaterThanOrEqual(
      uuidsAt.start + uuidsAt.length,
    );
  });
});

describe("room", () => {
  test("of an artifact is what fills the chunk being filled, and nothing once it is whole", () => {
    const artifact = fixture("stored", "artifact.bin");
    const unpacker = artifactUnpacker(artifact.length, 100, 0, false);
    expect(unpacker.room()).toBe(100);
    expect(unpacker.write(artifact.subarray(0, 30))).toBeUndefined();
    expect(unpacker.room()).toBe(70);
    expect(unpacker.write(artifact.subarray(30, 900))).toBeUndefined();
    // The last chunk is 65 bytes, and none is left to bring.
    expect(unpacker.room()).toBe(65);
    expect(unpacker.write(artifact.subarray(900))).toBeUndefined();
    expect(unpacker.room()).toBe(0);
    expect(unpacker.end()).toBeUndefined();
  });

  test("of a packed body is its current piece, 65,536 bytes at most, and nothing once it has ended", () => {
    const artifact = fixture("emit-key-fixture", "artifact.bin");
    const packed = fixture("emit-key-fixture", "packed.bin");
    const unpacker = artifactUnpacker(artifact.length, 4096, 0, true);
    // The magic, the preamble's length, the preamble.
    expect(unpacker.room()).toBe(8);
    let at = 0;
    let largest = 0;
    while (at < packed.length) {
      const room = unpacker.room();
      expect(room).toBeGreaterThan(0);
      expect(room).toBeLessThanOrEqual(65_536);
      largest = Math.max(largest, room);
      expect(unpacker.write(packed.subarray(at, at + room))).toBeUndefined();
      at += room;
    }
    expect(at).toBe(packed.length);
    expect(largest).toBeGreaterThan(4096);
    expect(unpacker.room()).toBe(0);
    expect(unpacker.end()).toBeUndefined();
  });
});

describe("a stored body", () => {
  const artifact = fixture("stored", "artifact.bin");

  test("is the artifact and restores only at its own length", () => {
    expect(Buffer.compare(fixture("stored", "packed.bin"), artifact)).toBe(0);
    restores(artifact, artifact);
    const length = artifact.length;
    expect(refusal(artifact, length - 1)).toBe(
      `the body decodes to more than ${length - 1} bytes`,
    );
    expect(refusal(artifact, length + 1)).toBe(
      `the body decodes to ${length} bytes, not ${length + 1}`,
    );
    expect(refusal(artifact.subarray(0, length - 1), length)).toBe(
      `the body decodes to ${length - 1} bytes, not ${length}`,
    );
    // Too short an artifact to begin either body.
    expect(refusal(artifact, 0)).toBe(LONGER);
    expect(refusal(artifact, 7)).toBe(LONGER);
  });

  test("is not unpacked when the body is said to be the artifact, whatever it begins with", () => {
    const packed = fixture("every-kind", "packed.bin");
    expect(stream(packed, packed, 1000, 4096, 0, false)).toBe(
      Math.ceil(packed.length / 4096),
    );
  });
});

describe("the unpacker refuses", () => {
  test("a body that begins with neither magic", () => {
    const body = bodyOf(sample());
    const length = SAMPLE.artifact.length;
    for (const bad of [
      encoder.encode("CALQPAQ1"),
      encoder.encode("calqpak1"),
      body.subarray(1),
    ])
      expect(refusal(bad, length)).toBe(MAGIC);
    for (let bit = 0; bit < 64; bit++) {
      const flipped = body.slice();
      flipped[bit >> 3] = (flipped[bit >> 3] ?? 0) ^ (1 << (bit & 7));
      expect(refusal(flipped, length), String(bit)).toBe(MAGIC);
    }
    // A body that ends inside the magic has not shown which it is.
    expect(refusal(new Uint8Array(0), length)).toBe(TRUNCATED);
    expect(refusal(encoder.encode("CALQPAK"), length)).toBe(TRUNCATED);
  });

  test("a preamble that runs past the body, or past what the artifact leaves it", () => {
    const body = bodyOf(sample());
    const length = SAMPLE.artifact.length;
    const magic = encoder.encode("CALQPAK1");
    expect(refusal(magic, length)).toBe(TRUNCATED);
    expect(refusal(concat(magic, Uint8Array.of(1, 0, 0)), length)).toBe(
      TRUNCATED,
    );
    expect(refusal(concat(magic, u32(5), encoder.encode("abcd")), length)).toBe(
      TRUNCATED,
    );
    const long = body.slice();
    // One byte more than the body has after the length.
    long.set(u32(body.length - 12 + 1), 8);
    expect(refusal(long, length)).toBe(TRUNCATED);
    // More than an artifact of this length leaves a body: no buffer is made.
    long.set(u32(length - 12 + 1), 8);
    expect(refusal(long, length)).toBe(LONGER);
    long.set(u32(1_000_000), 8);
    expect(refusal(long, length)).toBe(LONGER);
    // More than any preamble, whatever the artifact's length.
    for (const bytes of [2 * 1_048_576 + 1, 0xffffffff]) {
      long.set(u32(bytes), 8);
      expect(refusal(long, length)).toBe(PREAMBLE);
      expect(refusal(long, 2 ** 40)).toBe(PREAMBLE);
    }
  });

  /** The sample's body under another preamble. */
  function withPreamble(preamble: string | Uint8Array): string {
    const body = bodyOf(sample());
    const length = new DataView(body.buffer).getUint32(8, true);
    return refusal(
      framed(preamble, body.subarray(12 + length)),
      SAMPLE.artifact.length,
    );
  }

  test("a preamble that is not JSON with a version", () => {
    for (const preamble of [
      "",
      "version 1",
      "[]",
      "{}",
      "null",
      '{"version":"1"}',
      '{"version":-1}',
      '{"version":1.5}',
      '{"version":1',
      Uint8Array.of(0xff, 0xfe),
    ])
      expect(withPreamble(preamble), String(preamble)).toBe(PREAMBLE);
  });

  test("a version other than 1", () => {
    expect(withPreamble('{"version":2,"hoisted":[],"columns":[]}')).toBe(
      "the body is version 2, not 1",
    );
    expect(withPreamble('{"version":0,"hoisted":[],"columns":[]}')).toBe(
      "the body is version 0, not 1",
    );
    // Whatever else a later version's preamble holds.
    expect(withPreamble('{"version":7,"streams":{}}')).toBe(
      "the body is version 7, not 1",
    );
  });

  test("a preamble that lacks a member or holds one of another type", () => {
    const entry = (members: string): string =>
      `{"version":1,"hoisted":[],"columns":[{${members}}]}`;
    for (const preamble of [
      '{"version":1}',
      '{"version":1,"hoisted":[]}',
      '{"version":1,"columns":[]}',
      '{"version":1,"hoisted":{},"columns":[]}',
      '{"version":1,"hoisted":[{"table":0,"column":0}],"columns":[]}',
      entry(
        '"table":0,"column":1,"transform":"fk","references":3,"width":2,"escapes":1',
      ),
      entry(
        '"table":0,"transform":"fk","references":3,"width":2,"escapes":1,"streamBytes":416',
      ),
      entry(
        '"table":0,"column":1,"transform":"fk","references":3,"width":2e0,"escapes":1,"streamBytes":416',
      ),
      entry(
        '"table":0,"column":1,"transform":"zip","references":3,"width":2,"escapes":1,"streamBytes":416',
      ),
      entry(
        '"table":0,"column":1,"transform":"fk","references":3,"width":"2","escapes":1,"streamBytes":416',
      ),
      entry(
        '"table":-1,"column":1,"transform":"fk","references":3,"width":2,"escapes":1,"streamBytes":416',
      ),
      entry(
        '"table":0,"column":1,"transform":"fk","references":3,"width":2,"escapes":1.0,"streamBytes":416',
      ),
      // An integer of more digits than a double holds exactly.
      entry(
        '"table":0,"column":1,"transform":"fk","references":3,"width":2,"escapes":1,"streamBytes":1000000000000000',
      ),
    ])
      expect(withPreamble(preamble), preamble).toBe(PREAMBLE);
  });

  test("a preamble spelled otherwise than the packer writes it", () => {
    const written = spelled(SAMPLE.preamble);
    expect(
      written.startsWith(
        '{"version":1,"hoisted":[{"table":3,"column":0,"part":0},',
      ),
    ).toBe(true);
    for (const preamble of [
      written.replace("{", "{ "),
      `${written}\n`,
      ` ${written}`,
      written.replace('{"version":1,', '{"version":1,"extra":0,'),
      written.replace('"width":2,"escapes":1', '"escapes":1,"width":2'),
      // The letter f as a JSON escape.
      written.replace('"transform":"fk"', '"transform":"\\u0066k"'),
      written.replace('"references":null', '"references":null,"note":"x"'),
      // A reference left out reads as none, which the packer writes.
      written.replace('"references":null,', ""),
      written.replace('"width":2,', '"width":02,'),
      written.replace('"table":3,', '"table":3.0,'),
      // The members in another order, with every value as the packer's.
      `{${written.slice('{"version":1,'.length, -1)},"version":1}`,
    ]) {
      expect(preamble).not.toBe(written);
      expect(withPreamble(preamble), preamble).toBe(PREAMBLE);
    }
    expect(Buffer.compare(bodyOf(sample(), written), bodyOf(sample()))).toBe(0);
  });

  test("an artifact header it cannot read", () => {
    const taken = sample();
    taken.artifact[3] = (taken.artifact[3] ?? 0) ^ 0x20;
    expect(refusalOf(taken)).toBe(HEADER);

    const format = "calque-direct-columnar";
    const column = (parts: unknown, members: object = {}): unknown => ({
      name: "c",
      type: "bool",
      nullable: false,
      parts,
      ...members,
    });
    /**
     * An artifact of a table of one row, whose one column is `column`, and
     * of the table of zeros. `table` and `top` replace members of the first
     * table and of the header.
     */
    const headed = (
      column: unknown,
      table: object = {},
      top: object = {},
    ): Bytes =>
      frame(
        {
          format,
          version: 4,
          tables: [
            { name: "t", rows: 1, primaryKey: [], columns: [column], ...table },
            {
              name: "fill",
              rows: 100,
              primaryKey: [],
              columns: [
                {
                  name: "zero",
                  type: "int64",
                  nullable: false,
                  parts: [{ offset: 8, length: 800 }],
                },
              ],
            },
          ],
          ...top,
        },
        concat(Uint8Array.of(1), new Uint8Array(807)),
      );
    /** Its body: the header, the first table's byte and a stream of none. */
    const body = (artifact: Uint8Array): Bytes =>
      framed(
        spelled({ version: 1, hoisted: [], columns: [fillEntry(1)] }),
        artifact.subarray(
          0,
          align8(12 + new DataView(artifact.buffer).getUint32(8, true)) + 1,
        ),
      );
    const one = column([{ offset: 0, length: 1 }]);
    const good = headed(one);
    restores(body(good), good);

    // The body ends inside the artifact's first bytes or its header.
    const headAt = body(good).length - good.length + 800 + 8 - 1;
    // 0x43 is the C that begins the artifact's CALQART1 tag.
    expect(headAt).toBe(body(good).indexOf(0x43, 8));
    for (const bytes of [7, 11, 100])
      expect(refusal(body(good).subarray(0, headAt + bytes), good.length)).toBe(
        TRUNCATED,
      );
    const long = good.slice();
    // A header longer than an artifact's reader takes is given no buffer,
    // whatever the artifact's length, and neither is one the artifact has
    // no room for.
    for (const bytes of [0xffffffff, 1_048_577]) {
      long.set(u32(bytes), 8);
      expect(refusal(body(long), good.length)).toBe(HEADER);
      expect(refusal(body(long), 2 ** 40)).toBe(HEADER);
    }
    long.set(u32(1_048_576), 8);
    expect(refusal(body(long), good.length)).toBe(LONGER);
    const notJson = good.slice();
    notJson[12] = 0x5b;
    expect(refusal(body(notJson), good.length)).toBe(HEADER);

    const huge = 2 ** 53;
    const bad: [Bytes, string][] = [
      [headed(one, {}, { version: 3 }), "version 3"],
      [headed(one, {}, { format: "calque-direct" }), "another format"],
      [headed(one, {}, { tables: undefined }), "no tables"],
      [headed(one, {}, { tables: {} }), "tables that are no list"],
      [
        headed({ name: "c", type: "bool", parts: [{ offset: 0, length: 1 }] }),
        "no nullable",
      ],
      [
        headed(column([{ offset: 0, length: 1 }], { nullable: 0 })),
        "nullable of another type",
      ],
      [headed(one, { primaryKey: undefined }), "no primary key"],
      [headed(one, { primaryKey: "c" }), "a primary key that is no list"],
      [headed(one, { columns: undefined }), "no columns"],
      [headed(one, { rows: -1 }), "rows below zero"],
      [headed(one, { rows: 1.5 }), "rows that are no integer"],
      [headed(one, { rows: 2 ** 32 }), "rows past a u32"],
      [headed(one, { rows: "1" }), "rows of another type"],
      [headed(column(undefined)), "no parts"],
      [
        headed(column([{ offset: 8, length: 1 }])),
        "a first part past offset 0",
      ],
      [
        headed(
          column([
            { offset: 0, length: 1 },
            { offset: 1, length: 1 },
          ]),
        ),
        "a part not at the next multiple of 8",
      ],
      [headed(column([{ offset: 0, length: 9 }])), "a part within another"],
      [headed(column([{ offset: 0, length: 0 }])), "a gap before a part"],
      [
        headed(column([{ offset: 0, length: huge }])),
        "a length past any artifact",
      ],
      [headed(column([{ offset: -8, length: 1 }])), "an offset below zero"],
      [
        headed(column([{ offset: 0, length: 1.5 }])),
        "a length that is no integer",
      ],
      [headed(column([{ offset: 0 }])), "a part without a length"],
      [headed(column([{ length: 1 }])), "a part without an offset"],
      [headed(column([null])), "a part that is none"],
    ];
    for (const [artifact, name] of bad)
      expect(refusal(body(artifact), artifact.length), name).toBe(HEADER);
  });

  test("a decoded length other than the header's", () => {
    const body = bodyOf(sample());
    const length = SAMPLE.artifact.length;
    restores(body, SAMPLE.artifact);
    for (const expected of [length - 1, length + 1, Number.MAX_SAFE_INTEGER])
      expect(refusal(body, expected)).toBe(
        `the body restores ${length} bytes, not ${expected}`,
      );
    // Shorter than the body: refused before the header is in.
    expect(refusal(body, 0)).toBe(LONGER);
    expect(refusal(body, 4096)).toBe(LONGER);
  });

  test("a body longer than its artifact, as one that transforms nothing may be", () => {
    const untransformed = (name: string): [Bytes, Bytes] => {
      const artifact = fixture(name, "artifact.bin");
      return [
        artifact,
        assemble(artifact, { version: 1, hoisted: [], columns: [] }, []),
      ];
    };
    // All such a body leaves out is the padding between the parts, which
    // in an artifact of many parts is more than its preamble adds.
    const [many, shorter] = untransformed("every-kind");
    expect(shorter.length).toBeLessThan(many.length);
    restores(shorter, many);
    const [few, longer] = untransformed("stored");
    expect(longer.length).toBeGreaterThan(few.length);
    expect(refusal(longer, few.length)).toBe(LONGER);
  });

  test("a body that ends before its last part or after it", () => {
    const taken = sample();
    const body = bodyOf(taken);
    const length = taken.artifact.length;
    for (const kept of [body.length - 1, body.length - 5000, 12, 2000])
      expect(refusal(body.subarray(0, kept), length), String(kept)).toBe(
        TRUNCATED,
      );
    expect(refusal(concat(body, Uint8Array.of(0)), length)).toBe(TRAILING);
    expect(refusal(concat(body, new Uint8Array(5000)), length)).toBe(TRAILING);
    // As many bytes as the artifact has, and one more.
    expect(
      refusal(concat(body, new Uint8Array(length - body.length)), length),
    ).toBe(TRAILING);
    expect(
      refusal(concat(body, new Uint8Array(length - body.length + 1)), length),
    ).toBe(TRAILING);

    // A stream the preamble makes longer than the body has bytes for. With
    // the whole body in hand that is a body cut short. Here a stream of one
    // byte more takes the next part's first byte as a value's, and is
    // refused for it where it stands.
    const title = entryOf(taken, "items", "title");
    const longer = sample();
    entryAt(longer, title).streamBytes += 1;
    expect(refusalOf(longer)).toBe(
      defectOf(taken, "items", "title", "PartLength"),
    );
    // One that asks for more than an artifact of this length leaves.
    const written = spelled(taken.preamble);
    for (const streamBytes of ["100000", "999999999999999"])
      expect(
        refusalOf(
          taken,
          written.replace('"streamBytes":3044', `"streamBytes":${streamBytes}`),
        ),
      ).toBe(LONGER);
    // And one byte fewer, which leaves the last value without its
    // separator and a byte after the last part.
    const shorter = sample();
    entryAt(shorter, title).streamBytes -= 1;
    expect(refusalOf(shorter)).toBe(
      defectOf(taken, "items", "title", "Separators"),
    );
  });

  test("columns out of header order or listed twice", () => {
    const swapped = sample();
    swapped.preamble.columns.reverse();
    swapped.streams.reverse();
    expect(refusalOf(swapped)).toBe(PREAMBLE);

    const twice = sample();
    twice.preamble.columns.splice(4, 0, structuredClone(entryAt(twice, 4)));
    twice.streams.splice(4, 0, streamAt(twice, 4));
    expect(refusalOf(twice)).toBe(PREAMBLE);

    // Table order comes before column order: items.blob after zones.label.
    const earlier = sample();
    const last = entryAt(earlier, earlier.preamble.columns.length - 1);
    expect([last.table, last.column]).toEqual([6, 1]);
    [last.table, last.column] = [0, 13];
    expect(refusalOf(earlier)).toBe(PREAMBLE);
  });

  test("a column the artifact does not have", () => {
    for (const [table, column] of [
      [6, 2],
      [7, 0],
      [0xffffffff, 0xffffffff],
    ] as const) {
      const taken = sample();
      const last = entryAt(taken, taken.preamble.columns.length - 1);
      [last.table, last.column] = [table, column];
      expect(refusalOf(taken)).toBe(PREAMBLE);
    }
  });

  test("a member set on a transform that does not use it", () => {
    const position = entryOf(SAMPLE, "items", "position");
    const title = entryOf(SAMPLE, "items", "title");
    const changes: [number, (entry: Entry) => void][] = [
      [position, (entry) => (entry.references = 3)],
      [position, (entry) => (entry.escapes = 1)],
      [title, (entry) => (entry.references = 3)],
      [title, (entry) => (entry.escapes = 1)],
      [title, (entry) => (entry.width = 1)],
    ];
    for (const [entry, change] of changes) {
      const changed = sample();
      change(entryAt(changed, entry));
      expect(refusalOf(changed)).toBe(PREAMBLE);
    }
  });

  /** The transform of `from`'s entry, on `to` in its place. */
  function moved(from: [string, string], to: [string, string]): Taken {
    const taken = sample();
    const entry = entryOf(taken, ...from);
    const [table, column] = named(taken, ...to);
    const [changed] = taken.preamble.columns.splice(entry, 1);
    const [stream] = taken.streams.splice(entry, 1);
    const columns = taken.preamble.columns;
    let at = columns.findIndex(
      (entry) =>
        entry.table > table ||
        (entry.table === table && entry.column >= column),
    );
    if (at < 0) at = columns.length;
    if (columns[at]?.table === table && columns[at]?.column === column) {
      columns.splice(at, 1);
      taken.streams.splice(at, 1);
    }
    columns.splice(at, 0, {
      ...must(changed, "entry"),
      table,
      column,
    });
    taken.streams.splice(at, 0, must(stream, "stream"));
    return taken;
  }

  test("a transform on a column of another type", () => {
    // fk on an int64 column, with a table to refer to.
    const fk = sample();
    const position = entryAt(fk, entryOf(fk, "items", "position"));
    position.transform = "fk";
    position.references = 3;
    expect(refusalOf(fk)).toBe(defectOf(fk, "items", "position", "Type"));
    for (const [table, column, transform] of [
      ["items", "title", "int"],
      ["items", "meta", "int"],
      ["items", "position", "sep"],
      ["ticks", "at", "sep"],
    ] as const) {
      const changed = sample();
      const entry = entryAt(changed, entryOf(changed, table, column));
      entry.transform = transform;
      entry.width = 0;
      expect(refusalOf(changed), `${table}.${column}`).toBe(
        defectOf(changed, table, column, "Type"),
      );
    }
    // On the columns no transform fits: a uuid one for int and sep, and
    // others for each.
    for (const [to, from] of [
      [
        ["items", "id"],
        ["items", "position"],
      ],
      [
        ["items", "id"],
        ["items", "title"],
      ],
      [
        ["items", "total"],
        ["items", "position"],
      ],
      [
        ["items", "blob"],
        ["items", "title"],
      ],
      [
        ["items", "status"],
        ["items", "title"],
      ],
      [
        ["items", "score"],
        ["items", "owner_id"],
      ],
      [
        ["items", "day"],
        ["items", "owner_id"],
      ],
      [
        ["items", "span"],
        ["items", "owner_id"],
      ],
    ] as [[string, string], [string, string]][])
      expect(refusalOf(moved(from, to)), to.join(".")).toBe(
        defectOf(SAMPLE, ...to, "Type"),
      );
  });

  test("a transformed column whose parts are not its type's", () => {
    const parts = defect(1, 1, "Parts");
    const present = bitmap([true, true, true]);
    const [lengths, values] = texts(["a", "b", "c"]);
    const [headers, packed] = blocks([
      { base: 0n, bits: 8, scale: 0, numbers: [1n, 2n, 3n] },
    ]);
    const fk = (column: ForgedColumn): Taken =>
      oneStream(keyed(column), "fk", 0, 1, Uint8Array.of(0, 1, 0));
    const int = (column: ForgedColumn): Taken =>
      oneStream(keyed(column), "int", null, 1, Uint8Array.of(2, 2, 2));
    const sep = (column: ForgedColumn): Taken =>
      oneStream(
        keyed(column),
        "sep",
        null,
        0,
        encoder
          .encode("a\xffb\xffc\xff")
          .filter((byte) => byte !== 0xc3)
          .map((byte) => (byte === 0xbf ? 0xff : byte)),
      );
    const valuesOf = uuids(0xaa, 0xbb, 0xaa);
    const raw = int64s([1n, 2n, 3n]);

    // Each transform restores the column its parts are right for.
    for (const taken of [
      fk({ name: "c", type: "uuid", parts: [valuesOf] }),
      fk({
        name: "c",
        type: "uuid",
        nullable: true,
        parts: [present, valuesOf],
      }),
      int({ name: "c", type: "int64", parts: [raw] }),
      int({
        name: "c",
        type: "timestamp",
        encoded: true,
        parts: [headers, packed],
      }),
      sep({ name: "c", type: "utf8", parts: [lengths, values] }),
    ])
      restores(bodyOf(taken), taken.artifact);

    const bad: Taken[] = [
      // A row short, a row long, and a part too many.
      fk({ name: "c", type: "uuid", parts: [valuesOf.subarray(0, 47)] }),
      fk({
        name: "c",
        type: "uuid",
        parts: [concat(valuesOf, new Uint8Array(16))],
      }),
      fk({ name: "c", type: "uuid", parts: [valuesOf, new Uint8Array(0)] }),
      fk({ name: "c", type: "uuid", parts: [] }),
      // A nullable column without a bitmap of its rows.
      fk({ name: "c", type: "uuid", nullable: true, parts: [valuesOf] }),
      fk({
        name: "c",
        type: "uuid",
        nullable: true,
        parts: [Uint8Array.of(7, 0), valuesOf],
      }),
      fk({ name: "c", type: "uuid", nullable: true, parts: [] }),
      // A bitmap on a column that is not nullable.
      fk({ name: "c", type: "uuid", parts: [present, valuesOf] }),
      int({ name: "c", type: "int64", parts: [raw.subarray(0, 23)] }),
      int({ name: "c", type: "int64", parts: [raw, new Uint8Array(0)] }),
      int({ name: "c", type: "int64", parts: [headers, packed] }),
      int({ name: "c", type: "int64", encoded: true, parts: [raw] }),
      int({
        name: "c",
        type: "int64",
        encoded: true,
        parts: [headers.subarray(0, 15), packed],
      }),
      int({
        name: "c",
        type: "int64",
        encoded: true,
        parts: [concat(headers, headers), packed],
      }),
      int({
        name: "c",
        type: "int64",
        encoded: true,
        parts: [headers, packed, new Uint8Array(0)],
      }),
      sep({
        name: "c",
        type: "utf8",
        parts: [lengths.subarray(0, 12), values],
      }),
      sep({ name: "c", type: "utf8", parts: [lengths] }),
      sep({
        name: "c",
        type: "json",
        parts: [lengths, values, new Uint8Array(0)],
      }),
      sep({
        name: "c",
        type: "json",
        nullable: true,
        parts: [lengths, values],
      }),
      // The header lists the values part before the lengths part, which
      // the payload holds first.
      sep({
        name: "c",
        type: "utf8",
        parts: [values, lengths],
        listed: [1, 0],
      }),
      int({
        name: "c",
        type: "int64",
        encoded: true,
        parts: [packed, headers],
        listed: [1, 0],
      }),
      fk({
        name: "c",
        type: "uuid",
        nullable: true,
        parts: [valuesOf, present],
        listed: [1, 0],
      }),
    ];
    bad.forEach((taken, index) => {
      expect(refusalOf(taken), `case ${index}`).toBe(parts);
    });

    // An encoding other than frame of reference in blocks of 1,024 rows.
    for (const encoding of [
      { kind: "rle", blockRows: 1024 },
      { kind: "for", blockRows: 512 },
      "for",
    ]) {
      const good = keyed({
        name: "c",
        type: "int64",
        encoded: true,
        parts: [headers, packed],
      });
      const { header, payloadStart } = headerOf(good);
      must(header.tables[1]?.columns[1], "column").encoding = encoding;
      const artifact = frame(header, good.subarray(payloadStart));
      expect(
        refusalOf(oneStream(artifact, "int", null, 1, Uint8Array.of(2, 2, 2))),
      ).toBe(parts);
    }
  });

  test("a stream length other than its rows give", () => {
    const taken = sample();
    const owner = entryOf(taken, "items", "owner_id");
    const rows = defectOf(taken, "items", "owner_id", "Rows");
    // 200 rows of two bytes and one escape are 416 bytes.
    expect(streamAt(taken, owner).length).toBe(416);
    for (const [escapes, streamBytes] of [
      [1, 415],
      [1, 417],
      [0, 416],
      [2, 416],
      [1, 0],
      [999_999_999_999_999, 416],
    ] as const) {
      const changed = sample();
      entryAt(changed, owner).escapes = escapes;
      entryAt(changed, owner).streamBytes = streamBytes;
      expect(refusalOf(changed), `${escapes} ${streamBytes}`).toBe(rows);
    }

    // An int stream is its present rows at its width: 978 of 1,100 at 7.
    const at = entryOf(taken, "ticks", "at");
    expect(streamAt(taken, at).length).toBe(978 * 7);
    const present = defectOf(taken, "ticks", "at", "Rows");
    for (const width of [0, 1, 6]) {
      const changed = sample();
      entryAt(changed, at).width = width;
      expect(refusalOf(changed), `width ${width}`).toBe(present);
    }
    for (const stream of [
      streamAt(taken, at).subarray(0, 978 * 7 - 1),
      concat(streamAt(taken, at), Uint8Array.of(0)),
      // One delta for each row, null rows too.
      new Uint8Array(1100 * 7),
      new Uint8Array(0),
    ]) {
      const changed = sample();
      restream(changed, at, stream);
      expect(refusalOf(changed), String(stream.length)).toBe(present);
    }
  });

  test("a width the transform does not write", () => {
    // 300 owners take two bytes and 12 zones one.
    for (const [column, widths] of [
      ["owner_id", [0, 1, 3, 4]],
      ["zone_id", [0, 2, 3, 255]],
    ] as const) {
      const entry = entryOf(SAMPLE, "items", column);
      for (const width of widths) {
        const changed = sample();
        entryAt(changed, entry).width = width;
        entryAt(changed, entry).streamBytes =
          200 * width + 16 * entryAt(changed, entry).escapes;
        expect(refusalOf(changed), `${column} at ${width}`).toBe(
          defectOf(SAMPLE, "items", column, "Width"),
        );
      }
    }
    const position = entryOf(SAMPLE, "items", "position");
    for (const width of [8, 9, 255, 256]) {
      const changed = sample();
      entryAt(changed, position).width = width;
      restream(changed, position, new Uint8Array(200 * width));
      expect(refusalOf(changed), String(width)).toBe(
        defectOf(SAMPLE, "items", "position", "Width"),
      );
    }
  });

  test("a reference to no key part read before the stream", () => {
    const taken = sample();
    const owner = entryOf(taken, "items", "owner_id");
    const references = defectOf(taken, "items", "owner_id", "References");
    // None, the column's own table, no table, and two tables keyed on an
    // int64.
    for (const table of [null, 0, 7, 0xffffffff, 1, 4]) {
      const changed = sample();
      entryAt(changed, owner).references = table;
      expect(refusalOf(changed), String(table)).toBe(references);
    }

    // owners and zones follow items, so their keys must be hoisted.
    for (const [kept, column] of [
      [1, "owner_id"],
      [0, "zone_id"],
    ] as const) {
      const changed = sample();
      changed.preamble.hoisted = [must(taken.preamble.hoisted[kept], "part")];
      expect(refusalOf(changed)).toBe(
        defectOf(taken, "items", column, "References"),
      );
    }

    const key = (name: string): ForgedTable => ({
      name,
      rows: 2,
      primaryKey: ["id"],
      columns: [{ name: "id", type: "uuid", parts: [uuids(0xaa, 0xbb)] }],
    });
    const entry = (table: number, references: number): Entry => ({
      table,
      column: 0,
      transform: "fk",
      references,
      width: 1,
      escapes: 0,
      streamBytes: 2,
    });

    // Tables without a key part: no primary key, one of two names, one that
    // names no column, and one whose column is not a uuid, is nullable, or
    // is not one part of 16 bytes a row.
    const ids = uuids(0xaa, 0xbb);
    const keyless: [string[], ForgedColumn][] = [
      [[], { name: "id", type: "uuid", parts: [ids] }],
      [["id", "id"], { name: "id", type: "uuid", parts: [ids] }],
      [["key"], { name: "id", type: "uuid", parts: [ids] }],
      [["id"], { name: "id", type: "decimal", parts: [ids] }],
      [
        ["id"],
        {
          name: "id",
          type: "uuid",
          nullable: true,
          parts: [bitmap([true, true]), ids],
        },
      ],
      // Nullable, with the one part a key has.
      [["id"], { name: "id", type: "uuid", nullable: true, parts: [ids] }],
      [["id"], { name: "id", type: "uuid", parts: [ids, new Uint8Array(0)] }],
      [["id"], { name: "id", type: "uuid", parts: [ids.subarray(0, 31)] }],
    ];
    keyless.forEach(([primaryKey, id], index) => {
      const artifact = forge([
        { name: "keys", rows: 2, primaryKey, columns: [id] },
        {
          name: "uses",
          rows: 2,
          primaryKey: [],
          columns: [{ name: "key", type: "uuid", parts: [uuids(0xbb, 0xaa)] }],
        },
        FILL,
      ]);
      expect(
        refusalOf({
          artifact,
          preamble: {
            version: 1,
            hoisted: [],
            columns: [entry(1, 0), fillEntry(2)],
          },
          streams: [Uint8Array.of(1, 0), new Uint8Array(0)],
        }),
        `case ${index}`,
      ).toBe(defect(1, 0, "References"));
    });

    const artifact = forge([
      key("first"),
      key("second"),
      {
        name: "uses",
        rows: 2,
        primaryKey: ["key"],
        columns: [{ name: "key", type: "uuid", parts: [uuids(0xbb, 0xaa)] }],
      },
      FILL,
    ]);
    /** A body of it with these fk entries, each over its rows' codes. */
    const chained = (
      entries: readonly (readonly [Entry, readonly number[]])[],
      hoisted: Hoisted[] = [],
    ): Taken => ({
      artifact,
      preamble: {
        version: 1,
        hoisted,
        columns: [...entries.map(([entry]) => entry), fillEntry(3)],
      },
      streams: [
        ...entries.map(([, codes]) => Uint8Array.from(codes)),
        new Uint8Array(0),
      ],
    });
    // The third table's key in rows of the second's, which is stored first.
    restores(bodyOf(chained([[entry(2, 1), [1, 0]]])), artifact);
    // A key that is itself a stream cannot be read as rows.
    expect(
      refusalOf(
        chained([
          [entry(1, 0), [0, 1]],
          [entry(2, 1), [1, 0]],
        ]),
      ),
    ).toBe(defect(2, 0, "References"));
    // A key stored after its stream, unless it is hoisted.
    expect(refusalOf(chained([[entry(1, 2), [1, 0]]]))).toBe(
      defect(1, 0, "References"),
    );
    restores(
      bodyOf(
        chained([[entry(1, 2), [1, 0]]], [{ table: 2, column: 0, part: 0 }]),
      ),
      artifact,
    );
  });

  test("hoisted parts out of order, missing or replaced", () => {
    const hoist = (table: number, column: number, part: number): Hoisted => ({
      table,
      column,
      part,
    });
    const swapped = sample();
    swapped.preamble.hoisted.reverse();
    expect(refusalOf(swapped)).toBe(PREAMBLE);
    const twice = sample();
    twice.preamble.hoisted.unshift(hoist(3, 0, 0));
    expect(refusalOf(twice)).toBe(PREAMBLE);

    // ticks.level has two parts, and there are seven tables.
    for (const missing of [hoist(5, 2, 2), hoist(5, 4, 0), hoist(7, 0, 0)]) {
      const changed = sample();
      changed.preamble.hoisted = [missing];
      expect(refusalOf(changed)).toBe(PREAMBLE);
    }

    // The parts streams stand for: an fk column's values, a numeric column's
    // packed part, and a text column's lengths and values.
    for (const replaced of [
      hoist(0, 1, 0),
      hoist(0, 3, 1),
      hoist(0, 10, 1),
      hoist(0, 10, 2),
    ]) {
      const changed = sample();
      changed.preamble.hoisted.unshift(replaced);
      expect(refusalOf(changed)).toBe(PREAMBLE);
    }

    // Any stored part may be hoisted: here a validity part and block headers.
    const changed = sample();
    changed.preamble.hoisted = [
      hoist(0, 2, 0),
      hoist(0, 3, 0),
      hoist(3, 0, 0),
      hoist(6, 0, 0),
    ];
    const body = bodyOf(changed);
    restores(body, changed.artifact);
    expect(stream(body, changed.artifact, 7, 64)).toBe(
      Math.ceil(changed.artifact.length / 64),
    );
  });

  test("a row index past its table", () => {
    const taken = sample();
    const owner = streamAt(taken, entryOf(taken, "items", "owner_id"));
    // Row 0 becomes 301, one past the escape code of 300 owners.
    owner[0] = 0x2d;
    owner[200] = 0x01;
    const index = defectOf(taken, "items", "owner_id", "RowIndex");
    expect(refusalOf(taken)).toBe(index);
    owner[200] = 0xff;
    expect(refusalOf(taken)).toBe(index);
  });

  test("a null row with an index", () => {
    const taken = sample();
    const zone = streamAt(taken, entryOf(taken, "items", "zone_id"));
    // Row 1 of zone_id is null.
    expect(zone[1]).toBe(0);
    zone[1] = 1;
    expect(refusalOf(taken)).toBe(
      defectOf(taken, "items", "zone_id", "NullRow"),
    );
  });

  test("an escape past the escapes, and escapes no row uses", () => {
    const owner = entryOf(SAMPLE, "items", "owner_id");
    // Row 37 holds the escape code, 300, and its value follows the planes.
    expect([streamAt(SAMPLE, owner)[37], streamAt(SAMPLE, owner)[237]]).toEqual(
      [0x2c, 0x01],
    );
    const second = sample();
    streamAt(second, owner)[0] = 0x2c;
    streamAt(second, owner)[200] = 0x01;
    expect(refusalOf(second)).toBe(
      defectOf(SAMPLE, "items", "owner_id", "EscapePastEscapes"),
    );

    const none = sample();
    streamAt(none, owner)[37] = 0;
    streamAt(none, owner)[237] = 0;
    expect(refusalOf(none)).toBe(
      defectOf(SAMPLE, "items", "owner_id", "UnusedEscapes"),
    );

    // zone_id has no escape, and 12 zones make 12 its escape code.
    const lost = sample();
    streamAt(lost, entryOf(lost, "items", "zone_id"))[0] = 12;
    expect(refusalOf(lost)).toBe(
      defectOf(SAMPLE, "items", "zone_id", "EscapePastEscapes"),
    );
  });

  test("a value 2^51 or more from zero", () => {
    const artifact = keyed({
      name: "c",
      type: "int64",
      parts: [int64s([1n, 2n, 3n])],
    });
    const taken = (codes: readonly bigint[]): Taken =>
      oneStream(artifact, "int", null, 7, planes(codes, 7));
    // 2^51 - 1 and its negative are the last values inside.
    const limit = 1n << 52n;
    const all = (1n << 56n) - 1n;
    for (const inside of [
      [limit - 2n, 0n, 0n],
      [limit - 3n, 0n, 0n],
      [2n, 2n, 2n],
      // From one end of the range to the other and back.
      [limit - 2n, 2n * limit - 5n, 2n * limit - 4n],
    ]) {
      const packed = taken(inside);
      const restored = unpackArtifact(bodyOf(packed), artifact.length);
      // Held to the values the deltas give, computed here in integers.
      let value = 0n;
      const values = inside.map((code) => {
        value += code & 1n ? -((code + 1n) >> 1n) : code >> 1n;
        return value;
      });
      const { start } = partOf(packed, "rows", "c", 0);
      expect(
        Buffer.compare(restored.subarray(start, start + 24), int64s(values)),
        String(inside),
      ).toBe(0);
    }
    for (const outside of [
      [limit, 0n, 0n],
      [limit - 1n, 0n, 0n],
      [limit - 2n, 2n, 0n],
      [limit - 3n, 0n, 1n],
      [all, 0n, 0n],
      [0n, 0n, all - 1n],
      // The smallest codes a double does not hold exactly.
      [2n * limit, 0n, 0n],
      [2n * limit + 1n, 0n, 0n],
    ])
      expect(refusalOf(taken(outside)), String(outside)).toBe(
        defect(1, 1, "Value"),
      );
  });

  test("a block whose offset is not the bytes written", () => {
    // ticks.at has two blocks; its parts are validity, headers and packed.
    const headers = partOf(SAMPLE, "ticks", "at", 1);
    expect(headers.length).toBe(32);
    const block = defectOf(SAMPLE, "ticks", "at", "Block");
    const second = sample();
    second.artifact[headers.start + 16] =
      (second.artifact[headers.start + 16] ?? 0) ^ 1;
    expect(refusalOf(second)).toBe(block);
    const first = sample();
    first.artifact[headers.start] = 1;
    expect(refusalOf(first)).toBe(block);
  });

  test("a block of another scale, width or base", () => {
    const headers = partOf(SAMPLE, "ticks", "at", 1);
    // Its blocks are stored in milliseconds.
    expect(SAMPLE.artifact[headers.start + 13]).toBe(3);
    for (const [at, byte] of [
      // The scale: 1, 2 and 4 are not 0 or 3.
      [13, 1],
      [13, 2],
      [16 + 13, 4],
      // The width.
      [12, 65],
      [12, 255],
      // The base's top byte: 2^56 and beyond, and below zero.
      [11, 1],
      [16 + 11, 0x80],
    ] as const) {
      const changed = sample();
      changed.artifact[headers.start + at] = byte;
      expect(refusalOf(changed), `byte ${at} as ${byte}`).toBe(
        defectOf(SAMPLE, "ticks", "at", "Block"),
      );
    }
    // 2^51 itself is outside, in either sign; one less is a base, though
    // not one this block's values fit.
    const position = partOf(SAMPLE, "items", "position", 0);
    for (const base of [
      1n << 51n,
      -(1n << 51n),
      (1n << 63n) - 1n,
      -(1n << 63n),
      (1n << 51n) - 1n,
    ]) {
      const changed = sample();
      changed.artifact.set(i64(base), position.start + 4);
      expect(refusalOf(changed), String(base)).toBe(
        defectOf(SAMPLE, "items", "position", "Block"),
      );
    }

    // The scales above also leave their blocks too narrow for the values.
    // Here the values fit, were another scale read as 0.
    const scaled = (scale: number): Taken => {
      const [headers, packed] = blocks([
        { base: 0n, bits: 8, scale, numbers: [1n, 2n, 3n] },
      ]);
      return oneStream(
        keyed({
          name: "c",
          type: "int64",
          encoded: true,
          parts: [headers, packed],
        }),
        "int",
        null,
        1,
        Uint8Array.of(2, 2, 2),
      );
    };
    restores(bodyOf(scaled(0)), scaled(0).artifact);
    for (const scale of [1, 2, 4, 255])
      expect(refusalOf(scaled(scale)), `scale ${scale}`).toBe(
        defect(1, 1, "Block"),
      );

    // A block of no bits holds only its base, and a stream of no planes
    // gives each present row 0, which no other base is.
    const flat = (base: bigint, scale: number): Taken => {
      const [headers, packed] = blocks([
        { base, bits: 0, scale, numbers: [0n, 0n, 0n] },
      ]);
      return oneStream(
        keyed({
          name: "c",
          type: "int64",
          nullable: true,
          encoded: true,
          parts: [bitmap([false, true, false]), headers, packed],
        }),
        "int",
        null,
        0,
        new Uint8Array(0),
      );
    };
    for (const scale of [0, 3]) {
      restores(bodyOf(flat(0n, scale)), flat(0n, scale).artifact);
      for (const base of [5n, -1n])
        expect(refusalOf(flat(base, scale)), `${base} at scale ${scale}`).toBe(
          defect(1, 1, "Block"),
        );
    }
  });

  test("a base 2^51 from zero whose block would hold its rows", () => {
    // Values 0 to 2 fit 52 bits over a base of -2^51, were that a base.
    const limit = 1n << 51n;
    const taken = (base: bigint): Taken => {
      const [headers, packed] = blocks([
        {
          base,
          bits: 52,
          scale: 0,
          numbers: [0n, 1n, 2n].map((value) => value - base),
        },
      ]);
      return oneStream(
        keyed({
          name: "c",
          type: "int64",
          encoded: true,
          parts: [headers, packed],
        }),
        "int",
        null,
        1,
        Uint8Array.of(0, 2, 2),
      );
    };
    restores(bodyOf(taken(1n - limit)), taken(1n - limit).artifact);
    expect(refusalOf(taken(-limit))).toBe(defect(1, 1, "Block"));

    // No value is as large as a base of 2^51, so only a block of null rows
    // comes down to the base alone. It is refused though no row reads it.
    const nulls = (base: bigint): Taken => {
      const [headers, packed] = blocks([
        { base, bits: 0, scale: 0, numbers: [0n, 0n, 0n] },
      ]);
      return oneStream(
        keyed({
          name: "c",
          type: "int64",
          nullable: true,
          encoded: true,
          parts: [bitmap([false, false, false]), headers, packed],
        }),
        "int",
        null,
        0,
        new Uint8Array(0),
      );
    };
    restores(bodyOf(nulls(limit - 1n)), nulls(limit - 1n).artifact);
    expect(refusalOf(nulls(limit))).toBe(defect(1, 1, "Block"));
  });

  test("a block of width 64 at any base and scale, as the format reads none of them there", () => {
    const wide = (base: bigint, scale: number): Taken => {
      const [headers, packed] = blocks([
        { base, bits: 64, scale, numbers: [5n, -7n, 1n << 50n] },
      ]);
      return oneStream(
        keyed({
          name: "c",
          type: "timestamp",
          encoded: true,
          parts: [headers, packed],
        }),
        "int",
        null,
        7,
        planes(zigzags([5n, -7n, 1n << 50n]), 7),
      );
    };
    for (const [base, scale] of [
      [0n, 0],
      [1n << 60n, 7],
      [-(1n << 63n), 255],
    ] as const)
      restores(bodyOf(wide(base, scale)), wide(base, scale).artifact);
  });

  test("a value its block cannot hold", () => {
    // ticks.at is in whole milliseconds: a microsecond more is no multiple.
    const at = entryOf(SAMPLE, "ticks", "at");
    const odd = sample();
    expect(streamAt(odd, at)[1]).toBe(0x20);
    streamAt(odd, at)[1] = 0x22;
    expect(refusalOf(odd)).toBe(defectOf(SAMPLE, "ticks", "at", "Block"));

    // items.position counts 0, 10, 20 from a base of 0 in 11 bits.
    const position = entryOf(SAMPLE, "items", "position");
    const block = defectOf(SAMPLE, "items", "position", "Block");
    expect([...streamAt(SAMPLE, position).subarray(0, 3)]).toEqual([0, 20, 20]);
    const below = sample();
    streamAt(below, position)[0] = 1;
    expect(refusalOf(below)).toBe(block);
    const wide = sample();
    streamAt(wide, position)[199] = 0xfe;
    expect(refusalOf(wide)).toBe(block);
  });

  test("blocks that run past the packed part or end before it", () => {
    const length = defect(1, 1, "PartLength");
    const taken = (headers: Uint8Array, packed: Uint8Array): Taken =>
      oneStream(
        keyed({
          name: "c",
          type: "int64",
          encoded: true,
          parts: [headers, packed],
        }),
        "int",
        null,
        1,
        Uint8Array.of(2, 2, 2),
      );
    // Three bytes of blocks in a part of two, and in one of four; and the
    // same at width 64, 24 bytes of rows.
    for (const bits of [8, 64]) {
      const [headers, packed] = blocks([
        { base: 0n, bits, scale: 0, numbers: [1n, 2n, 3n] },
      ]);
      const good = taken(headers, packed);
      restores(bodyOf(good), good.artifact);
      expect(
        refusalOf(taken(headers, packed.subarray(0, packed.length - 1))),
      ).toBe(length);
      expect(refusalOf(taken(headers, concat(packed, Uint8Array.of(0))))).toBe(
        length,
      );
    }
  });

  test("a separator count other than the present rows", () => {
    for (const column of ["title", "note"]) {
      const entry = entryOf(SAMPLE, "items", column);
      const separators = defectOf(SAMPLE, "items", column, "Separators");
      const stream = streamAt(SAMPLE, entry);
      const separator = stream.indexOf(0xff);
      const value = stream.findIndex((byte) => byte !== 0xff);

      // A separator fewer is also a value byte more than the values part
      // holds, and the unpacker meets that first.
      const fewer = sample();
      streamAt(fewer, entry)[separator] = 0x78;
      expect(refusalOf(fewer)).toBe(
        defectOf(SAMPLE, "items", column, "PartLength"),
      );
      const more = sample();
      streamAt(more, entry)[value] = 0xff;
      expect(refusalOf(more)).toBe(separators);
      const dropped = sample();
      restream(dropped, entry, stream.subarray(0, stream.length - 1));
      expect(refusalOf(dropped)).toBe(separators);
      const added = sample();
      restream(added, entry, concat(stream, Uint8Array.of(0xff)));
      expect(refusalOf(added)).toBe(separators);
    }
    // An all-null column has no separator.
    const one = sample();
    restream(one, entryOf(one, "items", "ghost"), Uint8Array.of(0xff));
    expect(refusalOf(one)).toBe(defectOf(one, "items", "ghost", "Separators"));
  });

  test("values that do not add up to the values part", () => {
    const title = entryOf(SAMPLE, "items", "title");
    const length = defectOf(SAMPLE, "items", "title", "PartLength");
    const stream = streamAt(SAMPLE, title);
    const longer = sample();
    restream(longer, title, concat(Uint8Array.of(0x78), stream));
    expect(refusalOf(longer)).toBe(length);
    const value = stream.findIndex((byte) => byte !== 0xff);
    const shorter = sample();
    restream(
      shorter,
      title,
      concat(stream.subarray(0, value), stream.subarray(value + 1)),
    );
    expect(refusalOf(shorter)).toBe(length);
    // A value of 64 bytes or more is written straight from the body.
    const long = sample();
    restream(long, title, concat(new Uint8Array(100).fill(0x78), stream));
    expect(refusalOf(long)).toBe(length);
  });

  test("bytes after the last separator", () => {
    const title = entryOf(SAMPLE, "items", "title");
    const stream = streamAt(SAMPLE, title);
    // The last separator moves to the front: as many separators and as many
    // value bytes, the last of them belonging to no row.
    const rotated = concat(
      stream.subarray(stream.length - 1),
      stream.subarray(0, stream.length - 1),
    );
    expect(rotated[0]).toBe(0xff);
    const changed = sample();
    restream(changed, title, rotated);
    expect(refusalOf(changed)).toBe(
      defectOf(SAMPLE, "items", "title", "PartLength"),
    );
  });
});

describe("a body nobody packed", () => {
  /**
   * A few kilobytes with each transform, a hoisted key, an escape, null rows
   * and blocks in whole milliseconds, packed here by hand.
   */
  function small(): Taken {
    const rows = 37;
    const index = Array.from({ length: rows }, (_unused, row) => row);
    const parents = [0x11, 0x22, 0x33, 0x44, 0x55];
    const linked = index.map((row) => row % 10 !== 3);
    const labelled = index.map((row) => row % 4 !== 0);
    const labels = index.map((row) =>
      labelled[row] ? must(["a", "", "bc"][row % 3], "label") : "",
    );
    const micros = index.map(
      (row) => 1_760_000_000_000_000n + BigInt(row) * 2000n,
    );
    const [lengths, values] = texts(labels);
    const [headers, packed] = blocks([
      {
        base: 1_760_000_000_000n,
        bits: 7,
        scale: 3,
        numbers: index.map((row) => BigInt(2 * row)),
      },
    ]);
    const artifact = forge([
      {
        name: "kids",
        rows,
        primaryKey: ["n"],
        columns: [
          {
            name: "n",
            type: "int64",
            parts: [int64s(index.map((row) => BigInt(3 * row)))],
          },
          {
            name: "parent_id",
            type: "uuid",
            nullable: true,
            parts: [
              bitmap(linked),
              uuids(
                ...index.map((row) =>
                  !linked[row]
                    ? 0
                    : row === 9
                      ? 0x99
                      : must(parents[row % 5], "parent"),
                ),
              ),
            ],
          },
          {
            name: "label",
            type: "utf8",
            nullable: true,
            parts: [bitmap(labelled), lengths, values],
          },
          {
            name: "at",
            type: "timestamp",
            encoded: true,
            parts: [headers, packed],
          },
        ],
      },
      {
        name: "parents",
        rows: parents.length,
        primaryKey: ["id"],
        columns: [{ name: "id", type: "uuid", parts: [uuids(...parents)] }],
      },
    ]);
    const streams = [
      planes(zigzags(index.map((row) => BigInt(3 * row))), 1),
      concat(
        planes(
          index.map((row) =>
            !linked[row] ? 0n : row === 9 ? 5n : BigInt(row % 5),
          ),
          1,
        ),
        uuids(0x99),
      ),
      concat(
        ...index.flatMap((row) =>
          labelled[row]
            ? [encoder.encode(must(labels[row], "label")), Uint8Array.of(0xff)]
            : [],
        ),
      ),
      planes(zigzags(micros), 7),
    ];
    const entry = (
      column: number,
      transform: Entry["transform"],
      width: number,
    ): Entry => ({
      table: 0,
      column,
      transform,
      references: transform === "fk" ? 1 : null,
      width,
      escapes: transform === "fk" ? 1 : 0,
      streamBytes: must(streams[column], "stream").length,
    });
    return {
      artifact,
      preamble: {
        version: 1,
        hoisted: [{ table: 1, column: 0, part: 0 }],
        columns: [
          entry(0, "int", 1),
          entry(1, "fk", 1),
          entry(2, "sep", 0),
          entry(3, "int", 7),
        ],
      },
      streams,
    };
  }

  test("restores the artifact it was packed from, in pieces of every size", () => {
    const taken = small();
    const body = bodyOf(taken);
    expect(body.length).toBeLessThan(4096);
    restores(body, taken.artifact);
    for (const pieceBytes of [1, 3, 64, body.length])
      for (const chunkBytes of [1, 7, 512])
        expect(stream(body, taken.artifact, pieceBytes, chunkBytes)).toBe(
          Math.ceil(taken.artifact.length / chunkBytes),
        );
  });

  test("restores text values shorter and longer than the pieces it moves them in", () => {
    // Lengths either side of what is moved a byte at a time, of one byte of
    // a length, of the 8,192 bytes gathered at once and of a 65,536-byte
    // piece of the body, with null rows and empty values between them.
    const sizes = [
      0, 15, -1, 16, 17, 0, 255, 256, -1, -1, 300, 8191, 1, 8192, 8193, 70_000,
      0, 3,
    ];
    const labels = sizes.map((size, row) =>
      String.fromCharCode(97 + (row % 26)).repeat(Math.max(size, 0)),
    );
    const [lengths, values] = texts(labels);
    const artifact = forge([
      {
        name: "notes",
        rows: sizes.length,
        primaryKey: [],
        columns: [
          {
            name: "text",
            type: "utf8",
            nullable: true,
            parts: [bitmap(sizes.map((size) => size >= 0)), lengths, values],
          },
        ],
      },
      FILL,
    ]);
    const text = concat(
      ...labels.flatMap((label, row) =>
        must(sizes[row], "size") < 0
          ? []
          : [encoder.encode(label), Uint8Array.of(0xff)],
      ),
    );
    const body = bodyOf({
      artifact,
      preamble: {
        version: 1,
        hoisted: [],
        columns: [
          {
            table: 0,
            column: 0,
            transform: "sep",
            references: null,
            width: 0,
            escapes: 0,
            streamBytes: text.length,
          },
          fillEntry(1),
        ],
      },
      streams: [text, new Uint8Array(0)],
    });
    restores(body, artifact);
    for (const pieceBytes of [1, 8192, 65_536])
      expect(stream(body, artifact, pieceBytes, 4096)).toBe(
        Math.ceil(artifact.length / 4096),
      );
  });

  test("restores a text value whose length takes four bytes", () => {
    const labels = ["a", "b".repeat(2 ** 24 + 5), "c"];
    const [lengths, values] = texts(labels);
    const artifact = forge([
      {
        name: "notes",
        rows: labels.length,
        primaryKey: [],
        columns: [{ name: "text", type: "utf8", parts: [lengths, values] }],
      },
      FILL,
    ]);
    const text = concat(
      ...labels.flatMap((label) => [
        encoder.encode(label),
        Uint8Array.of(0xff),
      ]),
    );
    const body = bodyOf({
      artifact,
      preamble: {
        version: 1,
        hoisted: [],
        columns: [
          {
            table: 0,
            column: 0,
            transform: "sep",
            references: null,
            width: 0,
            escapes: 0,
            streamBytes: text.length,
          },
          fillEntry(1),
        ],
      },
      streams: [text, new Uint8Array(0)],
    });
    restores(body, artifact);
    expect(stream(body, artifact, 65_536, 4 << 20)).toBe(
      Math.ceil(artifact.length / (4 << 20)),
    );
  });

  test("restores a row index from each plane a key part of more than 65,535 rows gives it", () => {
    // Of 65,538 keys, row 65,537 takes the third plane, and so does the
    // escape code, 65,538. Each key holds its row's number.
    const keyRows = 65_538;
    const keys = new Uint8Array(16 * keyRows);
    for (let row = 0; row < keyRows; row++)
      new DataView(keys.buffer).setUint32(16 * row, row + 1, true);
    const key = (row: number): Uint8Array =>
      keys.subarray(16 * row, 16 * row + 16);
    const artifact = forge([
      {
        name: "keys",
        rows: keyRows,
        primaryKey: ["id"],
        columns: [{ name: "id", type: "uuid", parts: [keys] }],
      },
      {
        name: "rows",
        rows: 5,
        primaryKey: [],
        columns: [
          {
            name: "key_id",
            type: "uuid",
            parts: [
              concat(key(65_537), key(258), uuids(0x99), key(3), key(65_536)),
            ],
          },
        ],
      },
      FILL,
    ]);
    const codes = concat(
      planes([65_537n, 258n, 65_538n, 3n, 65_536n], 3),
      uuids(0x99),
    );
    const body = bodyOf({
      artifact,
      preamble: {
        version: 1,
        hoisted: [],
        columns: [
          {
            table: 1,
            column: 0,
            transform: "fk",
            references: 0,
            width: 3,
            escapes: 1,
            streamBytes: codes.length,
          },
          fillEntry(2),
        ],
      },
      streams: [codes, new Uint8Array(0)],
    });
    restores(body, artifact);
    expect(stream(body, artifact, 5, 65_536)).toBe(
      Math.ceil(artifact.length / 65_536),
    );
  });

  test("restores a column of 2^30 zeros a block at a time, from its headers", () => {
    // Every block has no bits and a base of 0, so the packed part is empty
    // and the stream too: 16 MiB of block headers stand for the rows, 64 of
    // them to a byte of the body. A row at a time that is 2^30 turns of the
    // row loop for a body brotli sends in a few hundred bytes, and a block
    // at a time 2^20.
    const rows = 2 ** 30;
    const artifact = forge([
      {
        name: "zeros",
        rows,
        primaryKey: [],
        columns: [
          {
            name: "c",
            type: "int64",
            encoded: true,
            parts: [new Uint8Array((16 * rows) / 1024), new Uint8Array(0)],
          },
        ],
      },
      FILL,
    ]);
    const body = bodyOf({
      artifact,
      preamble: {
        version: 1,
        hoisted: [],
        columns: [fillEntry(0), fillEntry(1)],
      },
      streams: [new Uint8Array(0), new Uint8Array(0)],
    });
    const started = performance.now();
    restores(body, artifact);
    expect(performance.now() - started).toBeLessThan(1000);
  });

  /** What became of `body`: why it was refused, or the bytes it restored. */
  function attempt(body: Uint8Array, decodedBytes: number): string | Bytes {
    try {
      return unpackArtifact(body, decodedBytes);
    } catch (error) {
      // Only a refusal: anything else the unpacker throws fails the test.
      if (
        !(error instanceof Error) ||
        !/^(the body|bytes follow)/u.test(error.message)
      )
        throw error;
      return error.message;
    }
  }

  test("with one bit flipped is refused or restores other bytes", () => {
    const taken = small();
    const body = bodyOf(taken);
    let refused = 0;
    for (let bit = 0; bit < 8 * body.length; bit++) {
      body[bit >> 3] = (body[bit >> 3] ?? 0) ^ (1 << (bit & 7));
      const restored = attempt(body, taken.artifact.length);
      if (typeof restored === "string") refused += 1;
      else if (Buffer.compare(restored, taken.artifact) === 0)
        throw new Error(`bit ${bit} restores the artifact`);
      body[bit >> 3] = (body[bit >> 3] ?? 0) ^ (1 << (bit & 7));
    }
    expect(refused).toBeGreaterThan(body.length);
    restores(body, taken.artifact);
  });

  test("with a preamble of any numbers is refused or restores decodedBytes", () => {
    const taken = small();
    const numbers = [
      0, 1, 2, 3, 4, 7, 8, 16, 255, 256, 65_536, 0xffffffff,
      999_999_999_999_999,
    ];
    const members = [
      "table",
      "column",
      "references",
      "width",
      "escapes",
      "streamBytes",
    ] as const;
    const tried = (changed: Taken): void => {
      const restored = attempt(bodyOf(changed), taken.artifact.length);
      if (typeof restored !== "string")
        expect(restored.length).toBe(taken.artifact.length);
    };
    taken.preamble.columns.forEach((_entry, entry) => {
      for (const number of numbers)
        for (const member of members) {
          const changed = structuredClone(taken);
          entryAt(changed, entry)[member] = number;
          tried(changed);
        }
      for (const transform of ["fk", "int", "sep"] as const)
        for (const references of [null, 0, 1]) {
          const changed = structuredClone(taken);
          entryAt(changed, entry).transform = transform;
          entryAt(changed, entry).references = references;
          tried(changed);
          entryAt(changed, entry).width = 0;
          tried(changed);
        }
    });
    for (const number of numbers)
      for (const member of ["table", "column", "part"] as const) {
        const changed = structuredClone(taken);
        must(changed.preamble.hoisted[0], "hoisted part")[member] = number;
        tried(changed);
      }
    const none = structuredClone(taken);
    none.preamble.hoisted = [];
    tried(none);
  });

  describe("and mangled", () => {
    const Real = Uint8Array;
    afterEach(() => {
      vi.unstubAllGlobals();
    });

    test("is refused or restores decodedBytes, the same in pieces, in buffers of no more than twice the artifact", () => {
      const artifact = fixture("every-kind", "artifact.bin");
      const packed = fixture("every-kind", "packed.bin");
      const decodedBytes = artifact.length;
      let state = 2026;
      const below = (limit: number): number => {
        state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
        return Math.floor((state / 2 ** 32) * limit);
      };
      /** The bytes of every buffer made while it is stubbed in. */
      let made = 0;
      class Counted extends Real {
        constructor(...parameters: unknown[]) {
          super(...(parameters as [number]));
          if (typeof parameters[0] === "number") made += parameters[0];
        }
      }
      let refused = 0;
      let most = 0;
      for (let round = 0; round < 3000; round++) {
        let mangled = packed.slice();
        const at = below(mangled.length);
        const run = Math.min(1 + below(24), mangled.length - at);
        const kind = below(6);
        if (kind === 0) mangled = mangled.slice(0, at);
        else if (kind === 1)
          for (let byte = at; byte < at + run; byte++)
            mangled[byte] = below(256);
        else if (kind === 2)
          mangled = concat(
            mangled.subarray(0, at),
            Real.from({ length: run }, () => below(256)),
            mangled.subarray(at),
          );
        else if (kind === 3)
          mangled = concat(mangled.subarray(0, at), mangled.subarray(at + run));
        else if (kind === 4) {
          const from = below(mangled.length - run + 1);
          mangled.copyWithin(at, from, from + run);
        } else mangled.fill(below(2) ? 0xff : 0, at, at + run);

        vi.stubGlobal("Uint8Array", Counted);
        made = 0;
        const unpacker = artifactUnpacker(decodedBytes, 4096, 0, true);
        let whole = unpacker.write(mangled) ?? unpacker.end();
        const buffers = made;
        vi.unstubAllGlobals();
        most = Math.max(most, buffers);
        if (whole === undefined) {
          // Every chunk of it, whatever it holds: the digests judge that.
          let restored = 0;
          for (let index = 0; index * 4096 < decodedBytes; index++)
            restored += unpacker.take(index).length;
          expect(restored).toBe(decodedBytes);
          whole = "restored";
        } else refused += 1;
        if (round % 10 === 0) {
          const pieces = artifactUnpacker(decodedBytes, 4096, 0, true);
          let said: string | undefined;
          const pieceBytes = 1 + below(300);
          for (
            let from = 0;
            from < mangled.length && said === undefined;
            from += pieceBytes
          )
            said = pieces.write(mangled.subarray(from, from + pieceBytes));
          expect(said ?? pieces.end() ?? "restored", String(round)).toBe(whole);
        }
      }
      expect(refused).toBeGreaterThan(1000);
      // The chunks, which are the artifact's length; what is kept of the
      // body, each piece of which is held to what the artifact's length
      // leaves the body before a buffer is made for it; and the 8 KiB that
      // rows are rebuilt in.
      expect(most).toBeLessThanOrEqual(2 * decodedBytes + 8192);
    });
  });
});
