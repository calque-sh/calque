import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { describe, expect, test, vi, type MockInstance } from "vitest";
import {
  CapacityError,
  DIRECT_COLUMNAR_FORMAT_VERSION,
  openDirectColumnarArtifact,
  receiveDirectColumnarArtifact,
  type DirectColumnarArtifactIdentity,
  type DirectColumnarSchema,
  type DirectColumnarWorkspace,
  type TrustedArtifact,
} from "./artifact.js";
import { OverlayEngine, artifactBase, type SqlValue } from "./overlay.js";
import {
  ColumnarQueryEngine,
  inspectQueryExecution,
  type QuerySpecification,
} from "./query.js";
import { referenceQuery } from "./query-reference.fixture.js";
import {
  encodeArtifact,
  encodeWorkspace,
  FIXTURE_ARTIFACT_IDENTITY,
  WORKSPACE_CONTRACT,
  type EncodedArtifact,
} from "./artifact.fixture.js";

const trusted = (encoded: EncodedArtifact) => ({
  byteLength: encoded.byteLength,
  digest: encoded.digest,
  identity: encoded.identity,
  schema: WORKSPACE_CONTRACT,
});

const RUST_IDENTITY = {
  sourceEpoch: "source-epoch-1",
  manifestVersion: "manifest-1",
  collectionSetHash: `sha256:${"1".repeat(64)}`,
  schemaHash:
    "sha256:2c3a79cf0ece0f4763bfe35bff9ac4aa9bb70df10811a2012e4b78d529dea20c",
  authorizationScopeVersion: "scope-1",
  authorizationScopeHash: `sha256:${"2".repeat(64)}`,
  cursor: "cursor-1",
} as const;

function fixtureTables(): Map<string, Array<Record<string, unknown>>> {
  return new Map([
    [
      "events",
      [
        {
          id: 1,
          organization_id: "org-1",
          project_id: "project-1",
          issue_id: "issue-1",
          level: "error",
          message: "boom",
          occurred_at: "2026-08-09T00:00:00.000Z",
          payload: { trace: true },
        },
      ],
    ],
    [
      "issues",
      [
        {
          id: "issue-1",
          organization_id: "org-1",
          project_id: "project-1",
          assignee_id: null,
          status: "open",
          priority: 2,
          title: "Broken",
          event_count: 1,
          updated_at: "2026-08-09T00:00:00.000Z",
        },
      ],
    ],
    [
      "comments",
      [
        {
          id: "comment-1",
          issue_id: "issue-1",
          author_id: "user-1",
          body: "Investigating",
          created_at: "2026-08-09T00:00:00.000Z",
        },
      ],
    ],
    ["issue_tags", [{ issue_id: "issue-1", tag: "regression" }]],
    [
      "users",
      [
        {
          id: "user-1",
          display_name: "Ada",
          email: "ada@example.test",
          active: true,
        },
      ],
    ],
    [
      "projects",
      [
        {
          id: "project-1",
          organization_id: "org-1",
          slug: "core",
          name: "Core",
        },
      ],
    ],
    [
      "memberships",
      [{ organization_id: "org-1", user_id: "user-1", role: "admin" }],
    ],
  ]);
}

function fixture(): EncodedArtifact {
  return encodeWorkspace(fixtureTables());
}

function ownedBuffer(bytes: Uint8Array): ArrayBuffer {
  return Uint8Array.from(bytes).buffer;
}

/** The fixture's binding, with the digest of every `size` bytes of it. */
function chunked(bytes: Uint8Array, size: number): TrustedArtifact {
  const sha256: `sha256:${string}`[] = [];
  for (let start = 0; start < bytes.byteLength; start += size)
    sha256.push(
      `sha256:${createHash("sha256")
        .update(bytes.subarray(start, start + size))
        .digest("hex")}`,
    );
  return {
    byteLength: bytes.byteLength,
    digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    chunks: { bytes: size, sha256 },
    identity: FIXTURE_ARTIFACT_IDENTITY,
    schema: WORKSPACE_CONTRACT,
  };
}

interface HashedChunk {
  readonly index: number;
  readonly bytes: Uint8Array;
  readonly sha256: string;
}

/** `bytes` in chunks of `size`, each with the digest a hasher takes of it. */
function hashedChunks(bytes: Uint8Array, size: number): HashedChunk[] {
  const result: HashedChunk[] = [];
  for (let start = 0; start < bytes.byteLength; start += size) {
    const chunk = bytes.slice(start, start + size);
    result.push({
      index: result.length,
      bytes: chunk,
      sha256: `sha256:${createHash("sha256").update(chunk).digest("hex")}`,
    });
  }
  return result;
}

/** The marks a spy on `performance.mark` has seen, without their prefix. */
function marked(mark: MockInstance<typeof performance.mark>): string[] {
  return mark.mock.calls.map(([name]) => name.replace(/^calque:/u, ""));
}

/**
 * Resolves once the receiver created since `mark` was last cleared has
 * matched its schema to the schema's hash.
 */
async function schemaMatched(
  mark: MockInstance<typeof performance.mark>,
): Promise<void> {
  await vi.waitFor(
    () => {
      expect(marked(mark)).toContain("artifact:schema");
    },
    { interval: 1 },
  );
}

/** Every value of every table in the fixture's contract, row by row. */
function everyValue(workspace: DirectColumnarWorkspace): unknown[] {
  return WORKSPACE_CONTRACT.tables.map(({ name, columns }) => {
    const table = workspace.table(name);
    return Array.from({ length: table.rows }, (_, row) =>
      columns.map((column) => table.column(column.name).valueAt(row)),
    );
  });
}

interface HeaderPart {
  readonly offset: number;
  readonly length: number;
}

interface HeaderColumn {
  readonly name: string;
  readonly nullable: boolean;
  readonly dictionaryCount?: number;
  readonly bitWidth?: number;
  readonly parts: readonly HeaderPart[];
}

interface HeaderTable {
  readonly name: string;
  readonly rows: number;
  readonly columns: readonly HeaderColumn[];
}

interface ArtifactHeader {
  readonly version: number;
  readonly tables: readonly HeaderTable[];
}

function readHeader(bytes: Uint8Array): {
  header: ArtifactHeader;
  payloadStart: number;
} {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const headerLength = view.getUint32(8, true);
  const header = JSON.parse(
    new TextDecoder().decode(bytes.subarray(12, 12 + headerLength)),
  ) as ArtifactHeader;
  return { header, payloadStart: (12 + headerLength + 7) & ~7 };
}

/** Where the last of each table's parts ends, in the header's order. */
function tableEnds(
  bytes: Uint8Array,
): { readonly name: string; readonly end: number }[] {
  const { header, payloadStart } = readHeader(bytes);
  return header.tables.map(({ name, columns }) => ({
    name,
    end: Math.max(
      payloadStart,
      ...columns.flatMap(({ parts }) =>
        parts.map((part) => payloadStart + part.offset + part.length),
      ),
    ),
  }));
}

/** Reads a dictionary column's raw codes and entries out of a payload. */
function readDictionary(
  bytes: Uint8Array,
  tableName: string,
  columnName: string,
): { codes: number[]; entries: string[] } {
  const { header, payloadStart } = readHeader(bytes);
  const table = header.tables.find(({ name }) => name === tableName);
  const column = table?.columns.find(({ name }) => name === columnName);
  // A nullable dictionary leads with its validity bitmap.
  const raw = column?.parts ?? [];
  const [codesPart, offsetsPart, dataPart] =
    raw.length === 4 ? raw.slice(1) : raw;
  if (!table || !column || !codesPart || !offsetsPart || !dataPart)
    throw new Error("fixture is missing the dictionary column");
  const slice = (part: HeaderPart) =>
    bytes.subarray(
      payloadStart + part.offset,
      payloadStart + part.offset + part.length,
    );
  const u32 = (part: HeaderPart) =>
    Array.from(new Uint32Array(Uint8Array.from(slice(part)).buffer));
  const unpack = (part: HeaderPart, width: number, count: number) => {
    const packed = slice(part);
    return Array.from({ length: count }, (_, index) => {
      const bitOffset = index * width;
      let value = 0;
      const bytes = Math.ceil(((bitOffset % 8) + width) / 8);
      for (let byte = 0; byte < bytes; byte++)
        value +=
          (packed[Math.floor(bitOffset / 8) + byte] ?? 0) * 2 ** (byte * 8);
      return Math.floor(value / 2 ** (bitOffset % 8)) % 2 ** width;
    });
  };
  const offsets = u32(offsetsPart);
  const data = slice(dataPart);
  return {
    codes: unpack(codesPart, column.bitWidth ?? 0, table.rows),
    entries: offsets
      .slice(1)
      .map((end, index) =>
        new TextDecoder().decode(data.subarray(offsets[index], end)),
      ),
  };
}

/**
 * Each block of a numeric column by the path the decoder reads it with:
 * `word` for offsets of up to 24 bits, `offset` up to 45, `bigint` for wider
 * offsets and `raw` for 64-bit values, marked `×1000` when it is stored in
 * milliseconds. A column with no blocks is `plain`.
 */
function numericLayout(
  bytes: Uint8Array,
  tableName: string,
  columnName: string,
): string[] {
  const { header, payloadStart } = readHeader(bytes);
  const column = header.tables
    .find(({ name }) => name === tableName)
    ?.columns.find(({ name }) => name === columnName) as
    (HeaderColumn & { readonly encoding?: unknown }) | undefined;
  if (!column) throw new Error(`fixture is missing ${tableName}.${columnName}`);
  if (column.encoding === undefined) return ["plain"];
  const metadata = column.parts[column.nullable ? 1 : 0];
  if (!metadata) throw new Error(`${tableName}.${columnName} has no blocks`);
  const view = new DataView(
    bytes.buffer,
    bytes.byteOffset + payloadStart + metadata.offset,
    metadata.length,
  );
  return Array.from({ length: metadata.length / 16 }, (_value, block) => {
    const width = view.getUint8(block * 16 + 12);
    const path =
      width <= 24
        ? "word"
        : width <= 45
          ? "offset"
          : width < 64
            ? "bigint"
            : "raw";
    return view.getUint8(block * 16 + 13) === 3 ? `${path} ×1000` : path;
  });
}

/**
 * A variable-width column's lengths part, as a view into `bytes`, so a test
 * can read it or change it in place.
 */
function lengthsPart(
  bytes: Uint8Array,
  tableName: string,
  columnName: string,
): Uint32Array {
  const { header, payloadStart } = readHeader(bytes);
  const column = header.tables
    .find(({ name }) => name === tableName)
    ?.columns.find(({ name }) => name === columnName);
  const part = column?.parts[column.nullable ? 1 : 0];
  if (!part) throw new Error(`fixture is missing ${tableName}.${columnName}`);
  return new Uint32Array(
    bytes.buffer,
    bytes.byteOffset + payloadStart + part.offset,
    part.length / 4,
  );
}

/** The fixture with one `events` row per message. */
function withMessages(messages: readonly string[]): EncodedArtifact {
  const tables = fixtureTables();
  const event = tables.get("events")?.[0];
  if (!event) throw new Error("fixture is missing events");
  tables.set(
    "events",
    messages.map((message, index) => ({ ...event, id: index + 1, message })),
  );
  return encodeWorkspace(tables);
}

/** mulberry32: a small generator whose sequence a seed fixes. */
function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 2 ** 32;
  };
}

/** `count` distinct values from `next`, in the order it first produced them. */
function distinct<T>(
  count: number,
  next: () => T,
  key: (value: T) => string = String,
): T[] {
  const seen = new Map<string, T>();
  while (seen.size < count) {
    const value = next();
    if (!seen.has(key(value))) seen.set(key(value), value);
  }
  return [...seen.values()];
}

/**
 * Two keys of each shape whose canonical bytes hash alike under FNV-1a, found
 * by search, so an index build must tell them apart by comparing keys.
 */
const COLLIDING_TEXT = ["key-122789", "key-339192"] as const;
const COLLIDING_INT64 = [10_223_429n, 158_435_433n] as const;
const COLLIDING_UUID = [
  "8e5400002e94b3f60000000000000000",
  "c5b90000358ea1d70000000000000000",
].map((hex) => Uint8Array.from(Buffer.from(hex, "hex")));

/** The schema `emit_fixture` writes, as the Rust test binds it. */
const RUST_SCHEMA = {
  format: "calque-direct-columnar" as const,
  version: 4 as const,
  tables: [
    {
      name: "items",
      primaryKey: ["signed", "raw", "uuid"],
      columns: [
        { name: "id", type: "dict" as const, nullable: false },
        { name: "score", type: "f64" as const, nullable: true },
        { name: "signed", type: "int64" as const, nullable: false },
        { name: "unsigned", type: "uint64" as const, nullable: false },
        { name: "active", type: "bool" as const, nullable: false },
        {
          name: "observed_at",
          type: "timestamp" as const,
          nullable: false,
        },
        { name: "note", type: "utf8" as const, nullable: true },
        { name: "raw", type: "bytes" as const, nullable: false },
        { name: "uuid", type: "uuid" as const, nullable: false },
        { name: "amount", type: "decimal" as const, nullable: false },
        { name: "day", type: "date" as const, nullable: false },
        { name: "clock", type: "time" as const, nullable: false },
        { name: "span", type: "interval" as const, nullable: false },
        { name: "meta", type: "json" as const, nullable: true },
        { name: "alias", type: "uuid" as const, nullable: true },
      ],
    },
  ],
};

/** Runs one of the Rust encoder's fixture examples; returns the bytes. */
async function emitRustFixture(
  artifactPath: string,
  example = "emit_fixture",
): Promise<Buffer> {
  const repository = new URL("../../../../", import.meta.url);
  // This checkout's own target directory: with a `CARGO_TARGET_DIR` that
  // other worktrees share, cargo could run another worktree's encoder.
  const run = spawnSync(
    "cargo",
    [
      "run",
      "--quiet",
      "--target-dir",
      fileURLToPath(new URL("target", repository)),
      "-p",
      "direct-columnar",
      "--example",
      example,
      "--",
      artifactPath,
    ],
    { cwd: repository, encoding: "utf8" },
  );
  expect(run.status, run.stderr).toBe(0);
  return readFile(artifactPath);
}

describe("browser direct-columnar artifact", () => {
  test("opens the exact bytes emitted by the bounded Rust encoder", async () => {
    const directory = await mkdtemp(join(tmpdir(), "calque-columnar-"));
    const artifactPath = join(directory, "fixture.calque");
    try {
      const bytes = await emitRustFixture(artifactPath);
      const input = Uint8Array.from(bytes);
      // The fixture interns `βeta` before `alpha`, so a sorted dictionary can
      // only come from the Rust remap.
      expect(readHeader(input).header.version).toBe(4);
      expect(readDictionary(input, "items", "id")).toEqual({
        codes: [1, 0],
        entries: ["alpha", "βeta"],
      });
      // The golden vector: each variable-width column's lengths part as the
      // Rust encoder wrote it, a 0 and then each row's length, a null row's 0.
      const variable = [
        "note",
        "raw",
        "amount",
        "day",
        "clock",
        "span",
        "meta",
      ];
      expect(
        Object.fromEntries(
          variable.map((name) => [
            name,
            Array.from(lengthsPart(input, "items", name)),
          ]),
        ),
      ).toEqual({
        note: [0, 9, 0],
        raw: [0, 3, 0],
        amount: [0, 16, 1],
        day: [0, 10, 10],
        clock: [0, 15, 8],
        span: [0, 13, 4],
        meta: [0, 11, 0],
      });
      const workspace = await openDirectColumnarArtifact(input.buffer, {
        byteLength: input.byteLength,
        digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
        identity: RUST_IDENTITY,
        schema: RUST_SCHEMA,
      });
      const items = workspace.table("items");
      expect(items.column("id").valueAt(0)).toBe("βeta");
      expect(items.column("id").valueAt(1)).toBe("alpha");
      expect(
        items.lookupKey([
          -(1n << 63n),
          Uint8Array.of(0, 0xff, 7),
          new Uint8Array(16).fill(0xab),
        ]),
      ).toBe(0);
      expect(
        items.lookupKey([
          (1n << 63n) - 1n,
          new Uint8Array(),
          new Uint8Array(16),
        ]),
      ).toBe(1);
      expect(items.column("score").valueAt(0)).toBe(42.5);
      expect(items.column("score").valueAt(1)).toBeNull();
      expect(items.column("signed").valueAt(0)).toBe(-(1n << 63n));
      expect(items.column("signed").valueAt(1)).toBe((1n << 63n) - 1n);
      expect(items.column("unsigned").valueAt(0)).toBe((1n << 64n) - 1n);
      expect(items.column("observed_at").valueAt(0)).toBe(
        "1969-12-31T23:59:59.999999Z",
      );
      expect(items.column("observed_at").valueAt(1)).toBe(
        "2025-07-26T20:00:00.123456Z",
      );
      expect(items.column("note").valueAt(0)).toBe("déjà vu");
      expect(items.column("note").valueAt(1)).toBeNull();
      expect(items.column("raw").valueAt(0)).toEqual(Uint8Array.of(0, 0xff, 7));
      const mutableCopy = items.column("raw").valueAt(0) as Uint8Array;
      mutableCopy[0] = 99;
      expect(items.column("raw").valueAt(0)).toEqual(Uint8Array.of(0, 0xff, 7));
      expect(items.column("uuid").valueAt(0)).toEqual(
        new Uint8Array(16).fill(0xab),
      );
      const uuidCopy = items.column("uuid").valueAt(0) as Uint8Array;
      uuidCopy.fill(0);
      expect(items.column("uuid").valueAt(0)).toEqual(
        new Uint8Array(16).fill(0xab),
      );
      expect(items.column("alias").valueAt(0)).toEqual(
        new Uint8Array(16).fill(0xab),
      );
      expect(items.column("alias").valueAt(1)).toBeNull();
      expect(items.column("amount").valueAt(0)).toBe("-1234567890.0123");
      expect(items.column("day").valueAt(0)).toBe("2026-08-09");
      expect(items.column("clock").valueAt(0)).toBe("22:30:01.123456");
      expect(items.column("span").valueAt(0)).toBe("P1MT2.000003S");
      expect(items.column("meta").valueAt(0)).toEqual({ ok: true });
      expect(items.column("meta").valueAt(1)).toBeNull();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
    // `cargo run` compiles the example on a cold target directory, which takes
    // far longer than vitest's 5s default.
  }, 300_000);

  // `lookupKey` probes with the canonical bytes of the key value it is given,
  // which must hash as the index's builder hashed the stored key: a mismatch
  // leaves the row where no probe of its key looks. Each table is searched
  // row by row with the key its own columns return, for every exact key kind,
  // and the numeric tables hold one block for each path the decoder reads a
  // stored value by, and the plain layout.
  test("finds every row of a Rust-encoded table by its own key, for each exact key kind and numeric layout", async () => {
    const directory = await mkdtemp(join(tmpdir(), "calque-columnar-"));
    try {
      const bytes = Uint8Array.from(
        await emitRustFixture(
          join(directory, "keys.calque"),
          "emit_key_fixture",
        ),
      );
      const header = readHeader(bytes).header as unknown as {
        readonly identity: DirectColumnarArtifactIdentity;
        readonly tables: readonly {
          readonly name: string;
          readonly primaryKey: readonly string[];
          readonly columns: readonly {
            readonly name: string;
            readonly type: DirectColumnarSchema["tables"][number]["columns"][number]["type"];
            readonly nullable: boolean;
          }[];
        }[];
      };
      const schema: DirectColumnarSchema = {
        format: "calque-direct-columnar",
        version: DIRECT_COLUMNAR_FORMAT_VERSION,
        tables: header.tables.map(({ name, primaryKey, columns }) => ({
          name,
          primaryKey,
          columns: columns.map(({ name, type, nullable }) => ({
            name,
            type,
            nullable,
          })),
        })),
      };
      // Read before the open, which takes the buffer.
      expect(
        ["int64", "int64_plain", "uint64", "timestamp", "timestamp_millis"].map(
          (name) => numericLayout(bytes, name, "key"),
        ),
      ).toEqual([
        ["word", "offset", "bigint", "raw"],
        ["plain"],
        ["word", "offset", "bigint", "raw"],
        ["word", "offset", "bigint"],
        ["word ×1000", "offset ×1000", "bigint ×1000"],
      ]);
      const workspace = await openDirectColumnarArtifact(bytes.buffer, {
        byteLength: bytes.byteLength,
        digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
        identity: header.identity,
        schema,
      });
      expect(schema.tables.map(({ name }) => name)).toEqual([
        "int64",
        "int64_plain",
        "uint64",
        "timestamp",
        "timestamp_millis",
        "bool",
        "utf8",
        "dict",
        "bytes",
        "uuid",
        "decimal",
        "date",
        "time",
        "interval",
        "composite",
      ]);
      for (const { name, primaryKey } of schema.tables) {
        const table = workspace.table(name);
        const rows = Array.from({ length: table.rows }, (_value, row) => row);
        expect(
          rows.map((row) =>
            table.lookupKey(
              primaryKey.map(
                (field) => table.column(field).valueAt(row) as SqlValue,
              ),
            ),
          ),
          name,
        ).toEqual(rows);
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }, 300_000);

  test("decides UUID equality from the artifact bytes and defers overlay rows", async () => {
    const directory = await mkdtemp(join(tmpdir(), "calque-columnar-"));
    try {
      const bytes = await emitRustFixture(join(directory, "fixture.calque"));
      const input = Uint8Array.from(bytes);
      const workspace = await openDirectColumnarArtifact(input.buffer, {
        byteLength: input.byteLength,
        digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
        identity: RUST_IDENTITY,
        schema: RUST_SCHEMA,
      });
      const columns = (RUST_SCHEMA.tables[0]?.columns ?? []).map(
        ({ name }) => name,
      );
      const schemas = [{ name: "items", columns }];
      const overlay = new OverlayEngine([
        {
          name: "items",
          columns,
          base: artifactBase(workspace.table("items"), columns),
        },
      ]);
      // Row 0 holds sixteen 0xab bytes in both UUID columns; row 1 holds
      // sixteen zeros in `uuid` and a null in `alias`.
      const high = new Uint8Array(16).fill(0xab);
      const lastDiffers = Uint8Array.from(high);
      lastDiffers[15] = 0xac;
      const firstDiffers = Uint8Array.from(high);
      firstDiffers[0] = 0xaa;
      const literals: readonly SqlValue[] = [
        high,
        new Uint8Array(16),
        lastDiffers,
        firstDiffers,
        high.subarray(0, 15),
        new Uint8Array(17).fill(0xab),
        null,
        "abababababababababababababababab",
      ];
      const check = (expectEncoded: boolean) => {
        for (const uuidColumn of ["uuid", "alias"])
          for (const literal of literals)
            for (const operator of ["eq", "neq"] as const) {
              const column = {
                kind: "field",
                field: { source: "item", column: uuidColumn },
              } as const;
              const specification: QuerySpecification = {
                version: 1,
                from: { collection: "items", as: "item" },
                // A comparison refuses a null literal; the NULL test reads
                // the same encoded cells.
                where:
                  literal === null
                    ? {
                        kind: "is-null",
                        expression: column,
                        negated: operator === "neq",
                      }
                    : {
                        kind: "compare",
                        operator,
                        left: column,
                        right: { kind: "literal", value: literal },
                      },
                select: [
                  {
                    kind: "field",
                    field: { source: "item", column: "signed" },
                    as: "signed",
                  },
                ],
              };
              const engine = new ColumnarQueryEngine(overlay, {
                capacityBytes: 1 << 24,
              });
              expect(engine.query(specification)).toEqual(
                referenceQuery(overlay, schemas, specification),
              );
              const inspection = inspectQueryExecution(engine);
              expect(inspection.encodedComparisons).toBe(2);
              expect(inspection.materializedComparisons).toBe(
                expectEncoded ? 0 : 1,
              );
            }
      };
      check(true);
      // A pending insert has no base cells, so its comparison is materialized
      // while both artifact rows stay encoded.
      overlay.applyOptimistic({
        mutationId: "uuid-insert",
        actions: [
          {
            kind: "insert",
            collection: "items",
            primaryKey: Uint8Array.of(9),
            key: [9n, Uint8Array.of(9), high],
            values: columns.map((name) =>
              name === "signed"
                ? 9n
                : name === "raw"
                  ? Uint8Array.of(9)
                  : name === "uuid"
                    ? high
                    : (workspace
                        .table("items")
                        .column(name)
                        .valueAt(0) as SqlValue),
            ),
          },
        ],
      });
      check(false);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }, 300_000);

  test("takes byte ownership, verifies, and exposes only data accessors", async () => {
    const encoded = fixture();
    const input = ownedBuffer(encoded.bytes);
    const workspace = await openDirectColumnarArtifact(input, trusted(encoded));

    expect(input.byteLength).toBe(0);
    expect(workspace.rowCount).toBe(7);
    expect(workspace.table("events").column("id").valueAt(0)).toBe(1n);
    expect(workspace.table("events").lookupKey(1n)).toBe(0);
    expect(workspace.table("issues").lookupKey("issue-1")).toBe(0);
    expect(workspace.table("issues").column("title").valueAt(0)).toBe("Broken");
    expect(workspace.table("events").column("payload").valueAt(0)).toEqual({
      trace: true,
    });
    expect(Object.keys(workspace)).toEqual([
      "artifactByteLength",
      "rowCount",
      "table",
    ]);
  });

  test("emits dictionary entries in ascending UTF-8 byte order", async () => {
    const tables = fixtureTables();
    // First-seen order is `βeta`, `😀`, `�`, `alpha`, so every code moves.
    //
    // `😀` (U+1F600) and `�` separate a UTF-8 byte sort from a UTF-16 one:
    // in UTF-8 the replacement character (ef bf bd) precedes the emoji
    // (f0 9f 98 80), while in UTF-16 the emoji's lead surrogate (d83d) precedes
    // U+FFFD. A UTF-16 comparison would emit codes [1, 2, 3, 0] here.
    tables.set("issue_tags", [
      { issue_id: "issue-1", tag: "βeta" },
      { issue_id: "issue-1", tag: "\u{1F600}" },
      { issue_id: "issue-1", tag: "�" },
      { issue_id: "issue-1", tag: "alpha" },
    ]);
    const encoded = encodeWorkspace(tables);

    expect(readHeader(encoded.bytes).header.version).toBe(4);
    expect(readDictionary(encoded.bytes, "issue_tags", "tag")).toEqual({
      codes: [1, 3, 2, 0],
      entries: ["alpha", "βeta", "�", "\u{1F600}"],
    });

    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
    );
    const tags = workspace.table("issue_tags");
    expect(tags.column("tag").valueAt(0)).toBe("βeta");
    expect(tags.column("tag").valueAt(3)).toBe("alpha");
    expect(tags.lookupKey(["issue-1", "\u{1F600}"])).toBe(1);
  });

  test("codes a null dictionary row as 0", async () => {
    const tables = fixtureTables();
    const event = tables.get("events")?.[0];
    if (!event) throw new Error("fixture is missing events");
    tables.set("events", [
      { ...event, id: 1, issue_id: null },
      { ...event, id: 2, issue_id: "zzz" },
      { ...event, id: 3, issue_id: "aaa" },
    ]);
    const encoded = encodeWorkspace(tables);

    expect(readDictionary(encoded.bytes, "events", "issue_id")).toEqual({
      codes: [0, 1, 0],
      entries: ["aaa", "zzz"],
    });

    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
    );
    const issueId = workspace.table("events").column("issue_id");
    expect(issueId.valueAt(0)).toBeNull();
    expect(issueId.valueAt(1)).toBe("zzz");
    expect(issueId.valueAt(2)).toBe("aaa");
  });

  test("ships value lengths and reads the values back through the offsets they rebuild", async () => {
    const encoded = withMessages(["boom", "", "déjà"]);
    expect(Array.from(lengthsPart(encoded.bytes, "events", "message"))).toEqual(
      [0, 4, 0, 6],
    );
    // A dictionary's offsets stay cumulative.
    expect(readDictionary(encoded.bytes, "events", "level").entries).toEqual([
      "error",
    ]);
    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
    );
    const message = workspace.table("events").column("message");
    expect([0, 1, 2].map((row) => message.valueAt(row))).toEqual([
      "boom",
      "",
      "déjà",
    ]);
  });

  test("refuses value lengths that do not add up to the values part, after a valid digest", async () => {
    const cases: readonly (readonly [
      string,
      (lengths: Uint32Array) => void,
      string,
    ])[] = [
      [
        "a nonzero lead",
        (lengths) => {
          lengths[0] = 1;
        },
        "invalid value-length prefix",
      ],
      [
        "a sum past the values part",
        (lengths) => {
          lengths[1] = (lengths[1] ?? 0) + 1;
        },
        "value lengths overrun the values part",
      ],
      [
        "a sum short of the values part",
        (lengths) => {
          lengths[3] = (lengths[3] ?? 0) - 1;
        },
        "value lengths fall short of the values part",
      ],
      // Modulo 2^32 these still add up to the values part's length, so a
      // sum kept in a u32 would accept them.
      [
        "a sum that wraps a u32",
        (lengths) => {
          lengths[2] = 0xffff_ffff;
          lengths[3] = (lengths[3] ?? 0) + 1;
        },
        "value lengths overrun the values part",
      ],
    ];
    const encoded = withMessages(["boom", "", "déjà"]);
    for (const [name, tamper, refusal] of cases) {
      const tampered = Uint8Array.from(encoded.bytes);
      tamper(lengthsPart(tampered, "events", "message"));
      await expect(
        openDirectColumnarArtifact(tampered.buffer, {
          ...trusted(encoded),
          digest: `sha256:${createHash("sha256").update(tampered).digest("hex")}`,
        }),
        name,
      ).rejects.toThrow(refusal);
    }
  });

  test("refuses bytes of another format version before reading a column", async () => {
    const encoded = fixture();
    const tampered = Uint8Array.from(encoded.bytes);
    const headerLength = new DataView(tampered.buffer).getUint32(8, true);
    const header = new TextDecoder().decode(
      tampered.subarray(12, 12 + headerLength),
    );
    // The header is ASCII up to its version, so the string index is the byte.
    const version = header.indexOf('"version":4');
    expect(version).toBeGreaterThan(0);
    tampered.set(new TextEncoder().encode('"version":3'), 12 + version);
    await expect(
      openDirectColumnarArtifact(tampered.buffer, {
        ...trusted(encoded),
        digest: `sha256:${createHash("sha256").update(tampered).digest("hex")}`,
      }),
    ).rejects.toThrow("invalid direct-columnar header");
    // A binding that names the previous version is refused before any byte.
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), {
        ...trusted(encoded),
        schema: { ...WORKSPACE_CONTRACT, version: 3 as unknown as 4 },
      }),
    ).rejects.toThrow("invalid trusted direct-columnar schema");
  });

  test("rejects a dictionary that is not sorted by UTF-8 bytes", async () => {
    const tables = fixtureTables();
    tables.set("issue_tags", [
      { issue_id: "issue-1", tag: "βeta" },
      { issue_id: "issue-1", tag: "alpha" },
    ]);
    const encoded = encodeWorkspace(tables, { sortDictionaries: false });

    expect(readDictionary(encoded.bytes, "issue_tags", "tag")).toEqual({
      codes: [0, 1],
      entries: ["βeta", "alpha"],
    });
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), trusted(encoded)),
    ).rejects.toThrow("unsorted direct-columnar dictionary");
  });

  test("rejects a dictionary ordered by UTF-16 code unit", async () => {
    const tables = fixtureTables();
    // `😀` (f0 9f 98 80) then `�` (ef bf bd) is descending in UTF-8 bytes, so
    // the guard must reject it — but ascending in UTF-16, where the emoji's
    // lead surrogate (d83d) precedes U+FFFD. A guard that compared decoded
    // strings would accept this artifact and then sort its rows wrongly.
    tables.set("issue_tags", [
      { issue_id: "issue-1", tag: "\u{1F600}" },
      { issue_id: "issue-1", tag: "�" },
    ]);
    const encoded = encodeWorkspace(tables, { sortDictionaries: false });

    expect(readDictionary(encoded.bytes, "issue_tags", "tag")).toEqual({
      codes: [0, 1],
      entries: ["\u{1F600}", "�"],
    });
    expect("\u{1F600}" < "�").toBe(true);
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), trusted(encoded)),
    ).rejects.toThrow("unsorted direct-columnar dictionary");
  });

  // The artifact carries no index, so a repeated key is the decoder's to
  // catch: `lookupKey` would find one of the rows, and the overlay would read
  // the other as absent and turn a canonical insert into a duplicate.
  test("refuses a table whose primary key repeats, after a valid digest", async () => {
    const cases: readonly (readonly [string, Record<string, unknown>[]])[] = [
      [
        "events",
        [1, 2, 1].map((id) => ({ ...fixtureTables().get("events")?.[0], id })),
      ],
      [
        "issue_tags",
        [
          { issue_id: "issue-1", tag: "a" },
          { issue_id: "issue-2", tag: "a" },
          { issue_id: "issue-1", tag: "a" },
        ],
      ],
    ];
    for (const [table, rows] of cases) {
      const tables = fixtureTables();
      tables.set(table, rows);
      expect(() => encodeWorkspace(tables)).toThrow("duplicate primary key");
      const encoded = encodeWorkspace(tables, { uniqueKeys: false });
      await expect(
        openDirectColumnarArtifact(
          ownedBuffer(encoded.bytes),
          trusted(encoded),
        ),
        table,
      ).rejects.toThrow("duplicate direct-columnar primary key");
    }
  });

  test("builds each table's primary-key index: every row found by its key, and absent keys missed", async () => {
    const alphabet = ["a", "b", "z", "0", "-", "é", "ß", "漢", "😀"];
    for (const seed of [1, 2, 3, 4, 5, 6]) {
      const random = generator(seed);
      const size = (maximum: number) => Math.floor(random() * (maximum + 1));
      const word = () =>
        Array.from(
          { length: size(6) },
          () => alphabet[Math.floor(random() * alphabet.length)],
        ).join("");
      // Seeds cycle the int64 layouts: a packed column whose blocks read as
      // numbers, a plain one (its range spans all of int64), and a packed one
      // whose second block is raw 64-bit values and reads as bigints.
      const small = () => BigInt(Math.floor(random() * 2_000_000) - 1_000_000);
      const extreme = () =>
        BigInt.asIntN(
          64,
          (BigInt(Math.floor(random() * 2 ** 32)) << 32n) |
            BigInt(Math.floor(random() * 2 ** 32)),
        );
      const ids =
        seed % 3 === 0
          ? [...COLLIDING_INT64, ...distinct(size(200), small)]
          : seed % 3 === 1
            ? [
                -(1n << 63n),
                (1n << 63n) - 1n,
                ...COLLIDING_INT64,
                ...distinct(size(200), extreme),
              ]
            : [
                ...COLLIDING_INT64,
                ...distinct(1_100, small),
                ...distinct(40, extreme),
              ];
      const uniqueIds = distinct(
        ids.length,
        (() => {
          let next = 0;
          return () => ids[next++] as bigint;
        })(),
      );
      const template = fixtureTables();
      const row = (table: string) => template.get(table)?.[0] ?? {};
      const texts = (count: number) => [
        ...COLLIDING_TEXT,
        ...distinct(count, word).filter(
          (value) => !(COLLIDING_TEXT as readonly string[]).includes(value),
        ),
      ];
      const pairs = (count: number) =>
        distinct(
          count,
          () => [word(), word()] as const,
          (pair) => JSON.stringify(pair),
        );
      const tables = new Map<string, Record<string, unknown>[]>([
        ["events", uniqueIds.map((id) => ({ ...row("events"), id }))],
        ["issues", texts(size(150)).map((id) => ({ ...row("issues"), id }))],
        [
          "comments",
          texts(size(150)).map((id) => ({ ...row("comments"), id })),
        ],
        [
          "issue_tags",
          pairs(size(150)).map(([issue_id, tag]) => ({ issue_id, tag })),
        ],
        ["users", texts(size(20)).map((id) => ({ ...row("users"), id }))],
        // Empty on some seeds: a table of no rows has a one-slot index.
        [
          "projects",
          distinct(size(3), word).map((id) => ({ ...row("projects"), id })),
        ],
        [
          "memberships",
          pairs(size(150)).map(([organization_id, user_id]) => ({
            organization_id,
            user_id,
            role: "admin",
          })),
        ],
      ]);
      const encoded = encodeWorkspace(tables);
      const workspace = await openDirectColumnarArtifact(
        ownedBuffer(encoded.bytes),
        trusted(encoded),
      );
      const layout = readHeader(encoded.bytes)
        .header.tables.find(({ name }) => name === "events")
        ?.columns.find(({ name }) => name === "id") as
        { encoding?: unknown } | undefined;
      expect(layout?.encoding === undefined, `seed ${seed}`).toBe(
        seed % 3 === 1,
      );
      for (const table of WORKSPACE_CONTRACT.tables) {
        const rows = tables.get(table.name) ?? [];
        const keyOf = (values: Record<string, unknown>) => {
          const key = table.primaryKey.map((field) => values[field]);
          return key.length === 1 ? key[0] : key;
        };
        const opened = workspace.table(table.name);
        expect(
          rows.map((values) => opened.lookupKey(keyOf(values))),
          `${table.name}, seed ${seed}`,
        ).toEqual(rows.map((_values, index) => index));
      }
      // `word` never spells "absent", and no generated id is 3^39.
      for (const table of WORKSPACE_CONTRACT.tables)
        expect(
          workspace
            .table(table.name)
            .lookupKey(
              table.name === "events"
                ? 3n ** 39n
                : table.primaryKey.length === 1
                  ? "absent"
                  : ["absent", "absent"],
            ),
          `${table.name}, seed ${seed}`,
        ).toBe(-1);
    }
  });

  test("builds the index over uuid, utf8 and composite uuid and utf8 keys", async () => {
    const random = generator(7);
    const uuid = () =>
      Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256));
    const hex = (bytes: Uint8Array) =>
      Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
    const schema: DirectColumnarSchema = {
      format: "calque-direct-columnar",
      version: DIRECT_COLUMNAR_FORMAT_VERSION,
      tables: [
        {
          name: "things",
          primaryKey: ["id"],
          columns: [
            { name: "id", type: "uuid", nullable: false },
            { name: "label", type: "utf8", nullable: true },
          ],
        },
        {
          name: "tags",
          primaryKey: ["thing_id", "tag"],
          columns: [
            { name: "thing_id", type: "uuid", nullable: false },
            { name: "tag", type: "utf8", nullable: false },
          ],
        },
        {
          name: "names",
          primaryKey: ["name"],
          columns: [{ name: "name", type: "utf8", nullable: false }],
        },
      ],
    };
    // The colliding pair first, and again under one tag: a composite key
    // whose leading column collides and whose rest is equal collides whole.
    const things = [
      ...COLLIDING_UUID,
      ...distinct(300, uuid, hex).filter(
        (id) => !COLLIDING_UUID.some((colliding) => hex(colliding) === hex(id)),
      ),
    ];
    const tags = distinct(
      400,
      (() => {
        const leading = COLLIDING_UUID.map((thing) => [thing, "p1"] as const);
        return () =>
          leading.shift() ??
          ([
            things[Math.floor(random() * things.length)] as Uint8Array,
            ["", "p1", "p2", "é", "😀"][Math.floor(random() * 5)] as string,
          ] as const);
      })(),
      ([thing, tag]) => `${hex(thing)}/${tag}`,
    );
    const names = [
      ...COLLIDING_TEXT,
      "",
      ...distinct(200, () => `n${Math.floor(random() * 1e9)}`),
    ];
    const tables = new Map<string, Record<string, unknown>[]>([
      ["things", things.map((id) => ({ id, label: null }))],
      ["tags", tags.map(([thing_id, tag]) => ({ thing_id, tag }))],
      ["names", names.map((name) => ({ name }))],
    ]);
    const encoded = encodeArtifact(schema, tables);
    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema,
      },
    );
    expect(things.map((id) => workspace.table("things").lookupKey(id))).toEqual(
      things.map((_id, index) => index),
    );
    expect(
      tags.map((key) => workspace.table("tags").lookupKey([...key])),
    ).toEqual(tags.map((_key, index) => index));
    expect(
      names.map((name) => workspace.table("names").lookupKey(name)),
    ).toEqual(names.map((_name, index) => index));
    expect(workspace.table("things").lookupKey(new Uint8Array(16))).toBe(-1);
    expect(workspace.table("tags").lookupKey([new Uint8Array(16), "p1"])).toBe(
      -1,
    );
    expect(workspace.table("names").lookupKey("absent")).toBe(-1);
  });

  test("accepts the sorted dictionary the same rows produce", async () => {
    const tables = fixtureTables();
    tables.set("issue_tags", [
      { issue_id: "issue-1", tag: "\u{1F600}" },
      { issue_id: "issue-1", tag: "�" },
    ]);
    const encoded = encodeWorkspace(tables);

    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
    );
    expect(workspace.table("issue_tags").column("tag").valueAt(0)).toBe(
      "\u{1F600}",
    );
  });

  test("mints bytes under a caller-supplied identity", async () => {
    // A consumer with its own trusted identity binds against it, so the header
    // has to carry that identity rather than the fixture default.
    const identity = {
      ...FIXTURE_ARTIFACT_IDENTITY,
      sourceEpoch: "diagnostic-source-epoch-1",
      manifestVersion: "diagnostic-manifest-1",
      authorizationScopeHash: `sha256:${"2".repeat(64)}`,
      cursor: "diagnostic-cursor-1",
    } as const;
    const encoded = encodeWorkspace(fixtureTables(), { identity });

    expect(encoded.identity).toEqual(identity);
    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      { ...trusted(encoded), identity },
    );
    expect(workspace.rowCount).toBe(7);
    // The default identity does not bind to these bytes.
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), {
        ...trusted(encoded),
        identity: FIXTURE_ARTIFACT_IDENTITY,
      }),
    ).rejects.toThrow("artifact identity binding mismatch");
  });

  test("refuses an identity whose schemaHash is not the contract's", () => {
    expect(() =>
      encodeWorkspace(fixtureTables(), {
        identity: {
          ...FIXTURE_ARTIFACT_IDENTITY,
          schemaHash: `sha256:${"4".repeat(64)}`,
        },
      }),
    ).toThrow("schemaHash must hash WORKSPACE_CONTRACT");
  });

  test("rejects capacity before taking ownership", async () => {
    const encoded = fixture();
    const input = ownedBuffer(encoded.bytes);
    await expect(
      openDirectColumnarArtifact(input, trusted(encoded), {
        capacityBytes: encoded.byteLength - 1,
        residentBytes: 0,
        standbyBytes: 0,
        safetyBytes: 0,
      }),
    ).rejects.toBeInstanceOf(CapacityError);
    expect(input.byteLength).toBe(encoded.byteLength);
  });

  test("counts the primary-key indexes it builds against the capacity", async () => {
    const encoded = fixture();
    // Seven one-row tables: two four-byte slots each, and one row's hash
    // while a table's index is built. The server computes the same 60 bytes.
    const indexBytes = 7 * 2 * 4 + 4;
    const capacity = (capacityBytes: number) => ({
      capacityBytes,
      residentBytes: 0,
      standbyBytes: 0,
      safetyBytes: 0,
    });
    const short = capacity(encoded.byteLength + indexBytes - 1);
    const refused = openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
      short,
    );
    await expect(refused).rejects.toBeInstanceOf(CapacityError);
    await expect(refused).rejects.toMatchObject({
      requiredBytes: encoded.byteLength + indexBytes,
      capacityBytes: encoded.byteLength + indexBytes - 1,
    });
    // A receiver counts the header's rows as the header arrives, and refuses
    // the artifact when it is opened: an accept refuses only its own chunk.
    const mark = vi.spyOn(performance, "mark");
    try {
      const receiver = receiveDirectColumnarArtifact(
        chunked(encoded.bytes, 96),
        short,
      );
      await schemaMatched(mark);
      for (const chunk of hashedChunks(encoded.bytes, 96))
        receiver.accept(chunk.index, chunk.bytes, chunk.sha256);
      await expect(receiver.open()).rejects.toBeInstanceOf(CapacityError);
    } finally {
      mark.mockRestore();
    }
    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
      capacity(encoded.byteLength + indexBytes),
    );
    expect(
      WORKSPACE_CONTRACT.tables.map(({ name }) => workspace.table(name).rows),
    ).toEqual([1, 1, 1, 1, 1, 1, 1]);
  });

  test("rounds each table's key slots up to a power of two against the capacity", async () => {
    const encoded = withMessages(["a", "b", "c", "d", "e"]);
    // Five events rows take the power of two at or above ten: sixteen
    // four-byte slots. The six one-row tables take two each, and the events
    // rows' hashes are the largest held while an index is built.
    const indexBytes = 16 * 4 + 6 * 2 * 4 + 5 * 4;
    const capacity = (capacityBytes: number) => ({
      capacityBytes,
      residentBytes: 0,
      standbyBytes: 0,
      safetyBytes: 0,
    });
    const refused = openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
      capacity(encoded.byteLength + indexBytes - 1),
    );
    await expect(refused).rejects.toBeInstanceOf(CapacityError);
    await expect(refused).rejects.toMatchObject({
      requiredBytes: encoded.byteLength + indexBytes,
    });
    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
      capacity(encoded.byteLength + indexBytes),
    );
    expect(workspace.table("events").rows).toBe(5);
  });

  test("checks the complete digest before interpreting corrupted bytes", async () => {
    const encoded = fixture();
    const input = new Uint8Array(ownedBuffer(encoded.bytes));
    input[0] = 0;
    await expect(
      openDirectColumnarArtifact(input.buffer, {
        byteLength: input.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema: WORKSPACE_CONTRACT,
      }),
    ).rejects.toThrow("artifact digest binding mismatch");
    expect(input.buffer.byteLength).toBe(0);
  });

  test("opens an artifact from its chunks in any order, each checked against its digest", async () => {
    const encoded = fixture();
    // A chunk size that leaves a shorter last chunk.
    const binding = chunked(encoded.bytes, 96);
    expect(encoded.byteLength % 96).not.toBe(0);
    const chunks = hashedChunks(encoded.bytes, 96);
    expect(chunks.length).toBeGreaterThan(3);
    const inOrder = receiveDirectColumnarArtifact(binding);
    expect(inOrder.chunks).toBe(chunks.length);
    for (const chunk of chunks)
      inOrder.accept(chunk.index, chunk.bytes, chunk.sha256);
    const workspace = await inOrder.open();
    expect(workspace.rowCount).toBe(7);
    expect(workspace.artifactByteLength).toBe(encoded.byteLength);
    expect(workspace.table("events").lookupKey(1n)).toBe(0);
    expect(workspace.table("issues").column("title").valueAt(0)).toBe("Broken");
    await expect(inOrder.open()).rejects.toThrow("receiver is closed");
    const [first] = chunks;
    if (!first) throw new Error("the fixture has no chunk");
    expect(() => {
      inOrder.accept(first.index, first.bytes, first.sha256);
    }).toThrow("receiver is closed");

    // The last chunk first, then the rest from the far end in: every chunk
    // lands where its index puts it, whatever order the hashers answer in.
    const shuffled = [
      ...chunks.slice(-1),
      ...chunks.slice(0, -1).filter((_, index) => index % 2 === 1),
      ...chunks
        .slice(0, -1)
        .filter((_, index) => index % 2 === 0)
        .reverse(),
    ];
    expect(shuffled.map(({ index }) => index).sort((a, b) => a - b)).toEqual(
      chunks.map(({ index }) => index),
    );
    const reordered = receiveDirectColumnarArtifact(binding);
    for (const chunk of shuffled)
      reordered.accept(chunk.index, chunk.bytes, chunk.sha256);
    expect(everyValue(await reordered.open())).toEqual(everyValue(workspace));
  });

  test("interprets each table while the rest downloads, once the chunks from the first one on cover it", async () => {
    const encoded = fixture();
    const binding = chunked(encoded.bytes, 96);
    const chunks = hashedChunks(encoded.bytes, 96);
    const ends = tableEnds(encoded.bytes);
    expect(
      ends.filter(({ end }) => end <= (chunks.length - 1) * 96).length,
    ).toBeGreaterThan(1);
    const whole = everyValue(
      await openDirectColumnarArtifact(ownedBuffer(encoded.bytes), binding),
    );
    // In order, and the last chunk first, then the rest from the far end in:
    // nothing past a chunk not yet held is interpreted.
    const orders = [
      chunks,
      [...chunks.slice(-1), ...chunks.slice(0, -1).reverse()],
    ];
    const mark = vi.spyOn(performance, "mark");
    try {
      for (const order of orders) {
        mark.mockClear();
        const receiver = receiveDirectColumnarArtifact(binding);
        await schemaMatched(mark);
        const held = new Set<number>();
        for (const chunk of order) {
          receiver.accept(chunk.index, chunk.bytes, chunk.sha256);
          held.add(chunk.index);
          let prefix = 0;
          while (held.has(prefix)) prefix += 1;
          const verified = Math.min(prefix * 96, encoded.byteLength);
          expect(
            marked(mark).filter((name) => name.startsWith("artifact:table:")),
            `after chunk ${String(chunk.index)}`,
          ).toEqual(
            ends
              .filter(({ end }) => end <= verified)
              .map(({ name }) => `artifact:table:${name}`),
          );
        }
        expect(everyValue(await receiver.open())).toEqual(whole);
      }
    } finally {
      mark.mockRestore();
    }
  });

  test("interprets nothing against a schema that does not match its hash", async () => {
    const encoded = fixture();
    const binding = chunked(encoded.bytes, 96);
    const schema = structuredClone(WORKSPACE_CONTRACT);
    const issueTable = schema.tables.find(({ name }) => name === "issues");
    if (!issueTable) throw new Error("fixture schema is missing issues");
    (issueTable.columns[0] as { type: string }).type = "utf8";
    const decode = vi.spyOn(TextDecoder.prototype, "decode");
    try {
      const relabelled = receiveDirectColumnarArtifact({ ...binding, schema });
      for (const chunk of hashedChunks(encoded.bytes, 96))
        relabelled.accept(chunk.index, chunk.bytes, chunk.sha256);
      await expect(relabelled.open()).rejects.toThrow(
        "trusted direct-columnar schema hash mismatch",
      );
      expect(decode).not.toHaveBeenCalled();
    } finally {
      decode.mockRestore();
    }
  });

  test("refuses a received artifact with any byte changed, having read only the chunks before it", async () => {
    const encoded = fixture();
    const binding = chunked(encoded.bytes, 96);
    // One byte in each chunk, the last one included. A changed first byte
    // would fail as an unknown format if it were read; any other change only a
    // digest can catch.
    const positions = [0];
    for (let start = 96; start < encoded.byteLength; start += 96)
      positions.push(Math.min(start + 50, encoded.byteLength - 1));
    positions.push(encoded.byteLength - 1);
    const decode = vi.spyOn(TextDecoder.prototype, "decode");
    const mark = vi.spyOn(performance, "mark");
    try {
      const reads: number[] = [];
      for (const position of positions) {
        const tampered = Uint8Array.from(encoded.bytes);
        tampered[position] = (tampered[position] ?? 0) ^ 0x01;
        mark.mockClear();
        const receiver = receiveDirectColumnarArtifact(binding);
        await schemaMatched(mark);
        decode.mockClear();
        // The hasher's digest is honest: it is of the changed bytes.
        const refused = Math.floor(position / 96);
        for (const chunk of hashedChunks(tampered, 96)) {
          if (chunk.index === refused) {
            expect(
              () => {
                receiver.accept(chunk.index, chunk.bytes, chunk.sha256);
              },
              `byte ${String(position)}`,
            ).toThrow("artifact digest binding mismatch");
            break;
          }
          receiver.accept(chunk.index, chunk.bytes, chunk.sha256);
        }
        await expect(
          receiver.open(),
          `byte ${String(position)}`,
        ).rejects.toThrow("artifact digest binding mismatch");
        for (const [input] of decode.mock.calls) {
          if (!ArrayBuffer.isView(input))
            throw new Error("the receiver decoded something other than bytes");
          expect(
            input.byteOffset + input.byteLength,
            `byte ${String(position)}`,
          ).toBeLessThanOrEqual(refused * 96);
        }
        reads.push(decode.mock.calls.length);
      }
      // A change in the last chunk is refused after the tables before it
      // were read: the bound above held while the receiver was reading.
      expect(reads.at(-1)).toBeGreaterThan(0);
    } finally {
      mark.mockRestore();
      decode.mockRestore();
    }
  });

  test("refuses a chunk whose digest, index or length is not the plan's", async () => {
    const encoded = fixture();
    const binding = chunked(encoded.bytes, 96);
    const chunks = hashedChunks(encoded.bytes, 96);
    expect(encoded.byteLength % 96).not.toBe(0);
    const last = chunks.length - 1;
    const middle = Math.floor(last / 2);
    const chunk = (index: number): HashedChunk => {
      const found = chunks[index];
      if (!found) throw new Error(`the fixture has no chunk ${String(index)}`);
      return found;
    };
    const other = (index: number) =>
      `sha256:${createHash("sha256")
        .update(chunk(index).bytes.subarray(1))
        .digest("hex")}`;
    const cases: ReadonlyArray<{
      readonly name: string;
      /** Accepted before the refused chunk. */
      readonly before: readonly number[];
      readonly index: number;
      readonly bytes: Uint8Array;
      readonly sha256: string;
      readonly error: string;
    }> = [
      ...[0, middle, last].map((index) => ({
        name: `a wrong digest for chunk ${String(index)}`,
        before: [],
        index,
        bytes: chunk(index).bytes,
        sha256: other(index),
        error: "artifact digest binding mismatch",
      })),
      {
        name: "another chunk's digest",
        before: [],
        index: 0,
        bytes: chunk(0).bytes,
        sha256: chunk(1).sha256,
        error: "artifact digest binding mismatch",
      },
      {
        name: "a chunk taken twice",
        before: [middle],
        index: middle,
        bytes: chunk(middle).bytes,
        sha256: chunk(middle).sha256,
        error: "artifact chunk received twice",
      },
      ...[-1, chunks.length, 1.5, Number.NaN].map((index) => ({
        name: `index ${String(index)}`,
        before: [],
        index,
        bytes: chunk(0).bytes,
        sha256: chunk(0).sha256,
        error: "artifact chunk index binding mismatch",
      })),
      {
        name: "a short chunk",
        before: [],
        index: 0,
        bytes: chunk(0).bytes.subarray(1),
        sha256: chunk(0).sha256,
        error: "artifact length binding mismatch",
      },
      {
        name: "a long chunk",
        before: [],
        index: 0,
        bytes: Uint8Array.from([...chunk(0).bytes, 0]),
        sha256: chunk(0).sha256,
        error: "artifact length binding mismatch",
      },
      {
        name: "a whole chunk's length for the shorter last chunk",
        before: [],
        index: last,
        bytes: new Uint8Array(96),
        sha256: chunk(last).sha256,
        error: "artifact length binding mismatch",
      },
    ];
    for (const refused of cases) {
      const receiver = receiveDirectColumnarArtifact(binding);
      for (const index of refused.before)
        receiver.accept(index, chunk(index).bytes, chunk(index).sha256);
      expect(() => {
        receiver.accept(refused.index, refused.bytes, refused.sha256);
      }, refused.name).toThrow(refused.error);
      // A refusal is final: the right chunk cannot follow the wrong one.
      const next = chunk(refused.index === 0 ? 1 : 0);
      expect(() => {
        receiver.accept(next.index, next.bytes, next.sha256);
      }, refused.name).toThrow("receiver is closed");
      await expect(receiver.open(), refused.name).rejects.toThrow(
        refused.error,
      );
    }

    // Every chunk but the last, and every chunk but a middle one.
    for (const absent of [last, middle]) {
      const receiver = receiveDirectColumnarArtifact(binding);
      for (const each of chunks)
        if (each.index !== absent)
          receiver.accept(each.index, each.bytes, each.sha256);
      await expect(
        receiver.open(),
        `chunk ${String(absent)} missing`,
      ).rejects.toThrow("artifact length binding mismatch");
    }
  });

  test("refuses chunk digests that do not cover the whole artifact", async () => {
    const encoded = fixture();
    const binding = chunked(encoded.bytes, 96);
    const digests = binding.chunks?.sha256 ?? [];
    for (const chunks of [
      // Too few would leave the rest unverified; too many name bytes that
      // do not exist.
      { bytes: 96, sha256: digests.slice(0, -1) },
      { bytes: 96, sha256: [...digests, digests[0]] },
      { bytes: 0, sha256: digests },
      { bytes: 96.5, sha256: digests },
      { bytes: 96, sha256: digests.map((value) => value.toUpperCase()) },
      { bytes: 96, sha256: digests, extra: true },
    ])
      expect(() =>
        receiveDirectColumnarArtifact({
          ...binding,
          chunks,
        } as unknown as TrustedArtifact),
      ).toThrow("invalid trusted artifact binding");
    expect(() => receiveDirectColumnarArtifact(trusted(encoded))).toThrow(
      "names no chunk digests",
    );
    expect(() =>
      receiveDirectColumnarArtifact(binding, {
        capacityBytes: encoded.byteLength - 1,
        residentBytes: 0,
        standbyBytes: 0,
        safetyBytes: 0,
      }),
    ).toThrow(CapacityError);
    // The schema a receiver is bound to must hash to its identity's
    // `schemaHash`; it is checked before any byte is read.
    const schema = structuredClone(WORKSPACE_CONTRACT);
    const issueTable = schema.tables.find(({ name }) => name === "issues");
    if (!issueTable) throw new Error("fixture schema is missing issues");
    (issueTable.columns[0] as { type: string }).type = "utf8";
    const relabelled = receiveDirectColumnarArtifact({ ...binding, schema });
    for (const chunk of hashedChunks(encoded.bytes, 96))
      relabelled.accept(chunk.index, chunk.bytes, chunk.sha256);
    await expect(relabelled.open()).rejects.toThrow(
      "trusted direct-columnar schema hash mismatch",
    );
    // A whole-buffer open still checks the whole digest, chunks or none.
    const tampered = Uint8Array.from(encoded.bytes);
    tampered[200] = (tampered[200] ?? 0) ^ 0x01;
    await expect(
      openDirectColumnarArtifact(ownedBuffer(tampered), binding),
    ).rejects.toThrow("artifact digest binding mismatch");
  });

  test("refuses a verified artifact whose last part runs past its end", async () => {
    const encoded = fixture();
    const end = Math.max(...tableEnds(encoded.bytes).map(({ end }) => end));
    const truncated = encoded.bytes.slice(0, end - 1);
    const binding = chunked(truncated, 96);
    await expect(
      openDirectColumnarArtifact(ownedBuffer(truncated), binding),
    ).rejects.toThrow("invalid direct-columnar segment");
    const mark = vi.spyOn(performance, "mark");
    try {
      const receiver = receiveDirectColumnarArtifact(binding);
      await schemaMatched(mark);
      for (const chunk of hashedChunks(truncated, 96))
        receiver.accept(chunk.index, chunk.bytes, chunk.sha256);
      await expect(receiver.open()).rejects.toThrow(
        "invalid direct-columnar segment",
      );
    } finally {
      mark.mockRestore();
    }
  });

  test("rejects malformed structure after a valid byte digest", async () => {
    const encoded = fixture();
    const input = new Uint8Array(ownedBuffer(encoded.bytes));
    new DataView(input.buffer).setUint32(8, 0, true);
    const digest =
      `sha256:${createHash("sha256").update(input).digest("hex")}` as const;
    await expect(
      openDirectColumnarArtifact(input.buffer, {
        byteLength: input.byteLength,
        digest,
        identity: encoded.identity,
        schema: WORKSPACE_CONTRACT,
      }),
    ).rejects.toThrow("invalid direct-columnar header length");
  });

  test("binds the verified artifact to the generated collection schema", async () => {
    const encoded = fixture();
    const schema = structuredClone(WORKSPACE_CONTRACT);
    const issueTable = schema.tables.find(({ name }) => name === "issues");
    if (!issueTable) throw new Error("fixture schema is missing issues");
    (issueTable.columns[0] as { type: string }).type = "utf8";
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), {
        byteLength: encoded.byteLength,
        digest: encoded.digest,
        identity: encoded.identity,
        schema,
      }),
    ).rejects.toThrow("trusted direct-columnar schema hash mismatch");
  });

  test("rejects relabelling valid bytes for another cursor or authorization scope", async () => {
    const encoded = fixture();
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), {
        ...trusted(encoded),
        identity: { ...encoded.identity, cursor: "other-cursor" },
      }),
    ).rejects.toThrow("artifact identity binding mismatch");
    await expect(
      openDirectColumnarArtifact(ownedBuffer(encoded.bytes), {
        ...trusted(encoded),
        identity: {
          ...encoded.identity,
          authorizationScopeVersion: "other-scope",
        },
      }),
    ).rejects.toThrow("artifact identity binding mismatch");
  });

  test("bounds every public row and key access", async () => {
    const encoded = fixture();
    const workspace = await openDirectColumnarArtifact(
      ownedBuffer(encoded.bytes),
      trusted(encoded),
    );
    expect(() => workspace.table("issues").column("title").valueAt(1)).toThrow(
      RangeError,
    );
    expect(() =>
      workspace.table("issues").lookupKey(["issue-1", "extra"]),
    ).toThrow("invalid primary key");
    expect(() => workspace.table("missing")).toThrow("unknown table");
  });
});
