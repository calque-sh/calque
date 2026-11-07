import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { zstdDecompressSync } from "node:zlib";

import {
  breakdown,
  formatBreakdown,
  SERVER_ZSTD,
  serverZstd,
  withoutRanges,
} from "./artifact-breakdown.ts";
import { CARGO_TARGET_ARGUMENTS } from "./lib/cargo-target.ts";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/** The bytes one of the Rust encoder's fixture examples writes. */
function emit(example: "emit_fixture" | "emit_key_fixture"): Buffer {
  const directory = mkdtempSync(join(tmpdir(), "calque-breakdown-"));
  try {
    const path = join(directory, "artifact.calque");
    const run = spawnSync(
      "cargo",
      [
        "run",
        "--quiet",
        ...CARGO_TARGET_ARGUMENTS,
        "-p",
        "direct-columnar",
        "--example",
        example,
        "--",
        path,
      ],
      { cwd: repositoryRoot, encoding: "utf8" },
    );
    assert.equal(run.status, 0, run.stderr);
    return readFileSync(path);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

const fixture = emit("emit_fixture");
const keys = emit("emit_key_fixture");

function columnsOf(result: ReturnType<typeof breakdown>) {
  return result.tables.flatMap((table) =>
    table.columns.map((column) => ({
      column: `${table.name}.${column.name}`,
      roles: column.parts.map((part) => `${part.role} ${String(part.bytes)}`),
    })),
  );
}

test("every column of the encoder's fixture reports the parts the encoder wrote, by role", () => {
  assert.deepEqual(columnsOf(breakdown(fixture)), [
    {
      column: "items.id",
      roles: ["codes 1", "dictionary-offsets 12", "dictionary-values 10"],
    },
    { column: "items.score", roles: ["validity 1", "values 16"] },
    { column: "items.signed", roles: ["values 16"] },
    { column: "items.unsigned", roles: ["values 16"] },
    { column: "items.active", roles: ["values 2"] },
    { column: "items.observed_at", roles: ["values 16"] },
    { column: "items.note", roles: ["validity 1", "lengths 12", "values 9"] },
    { column: "items.raw", roles: ["lengths 12", "values 3"] },
    { column: "items.uuid", roles: ["values 32"] },
    { column: "items.amount", roles: ["lengths 12", "values 17"] },
    { column: "items.day", roles: ["lengths 12", "values 20"] },
    { column: "items.clock", roles: ["lengths 12", "values 23"] },
    { column: "items.span", roles: ["lengths 12", "values 17"] },
    { column: "items.meta", roles: ["validity 1", "lengths 12", "values 11"] },
    { column: "items.alias", roles: ["validity 1", "values 32"] },
  ]);
});

test("a numeric column the encoder packed reports its block headers and packed values", () => {
  const result = breakdown(keys);
  const key = (table: string) => {
    const found = result.tables.find((entry) => entry.name === table);
    assert.ok(found, table);
    return {
      rows: found.rows,
      roles: found.columns[0]!.parts.map((part) => part.role),
      blocks: found.columns[0]!.parts.find((part) => part.role === "blocks")
        ?.bytes,
      encoding: found.columns[0]!.encoding,
    };
  };
  // One 16-byte header per 1,024 rows.
  assert.deepEqual(key("int64"), {
    rows: 4_096,
    roles: ["blocks", "packed"],
    blocks: 64,
    encoding: "for",
  });
  assert.deepEqual(key("timestamp_millis"), {
    rows: 3_072,
    roles: ["blocks", "packed"],
    blocks: 48,
    encoding: "for",
  });
  // Random 64-bit values pack no narrower, so the encoder keeps them raw.
  assert.deepEqual(key("int64_plain"), {
    rows: 300,
    roles: ["values"],
    blocks: undefined,
    encoding: "plain",
  });
});

test("the parts, the framing and the zero padding add up to the file", () => {
  for (const bytes of [fixture, keys]) {
    const result = breakdown(bytes);
    const parts = result.tables
      .flatMap((table) => table.columns)
      .reduce((sum, column) => sum + column.bytes, 0);
    const framing =
      result.framing.tag + result.framing.headerLength + result.framing.header;
    assert.equal(result.bytes, bytes.byteLength);
    assert.equal(parts + framing + result.padding, bytes.byteLength);
    assert.match(
      formatBreakdown(result),
      new RegExp(
        `= ${bytes.byteLength.toLocaleString("en-US")} B, the file's ${bytes.byteLength.toLocaleString("en-US")}\n`,
      ),
    );
  }
});

test("a column's marginal cost is the whole zstd size less the size without its parts", () => {
  const result = breakdown(fixture);
  const table = result.tables[0]!;
  for (const column of table.columns) {
    const cut = withoutRanges(
      fixture,
      column.parts.map((part) => [part.offset, part.offset + part.bytes]),
    );
    assert.equal(cut.byteLength, fixture.byteLength - column.bytes);
    assert.equal(
      column.marginal,
      result.zstd.bytes - serverZstd(cut).byteLength,
      column.name,
    );
  }
  assert.deepEqual(
    [
      ...withoutRanges(Uint8Array.of(0, 1, 2, 3, 4, 5), [
        [4, 5],
        [1, 3],
      ]),
    ],
    [0, 3, 5],
  );
});

test("it compresses with the parameters of the server's zstd representation", () => {
  const source = readFileSync(
    join(
      repositoryRoot,
      "server/crates/snapshot-pipeline/src/representations.rs",
    ),
    "utf8",
  );
  const constant = (name: string) =>
    source.match(new RegExp(`const ${name}: \\w+ = ([^;]+);`))?.[1];
  assert.equal(constant("ZSTD_LEVEL"), String(SERVER_ZSTD.level));
  assert.equal(constant("ZSTD_WINDOW_LOG"), String(SERVER_ZSTD.windowLog));
  assert.equal(constant("ZSTD_WORKERS"), String(SERVER_ZSTD.workers));
  assert.equal(constant("ZSTD_JOB_BYTES"), "2 * 1024 * 1024");
  assert.equal(SERVER_ZSTD.jobBytes, 2 * 1024 * 1024);
  assert.equal(constant("ZSTD_OVERLAP_LOG"), String(SERVER_ZSTD.overlapLog));
  assert.equal(constant("ZSTD_HASH_LOG"), String(SERVER_ZSTD.hashLog));
  assert.match(source, /encoder\.include_checksum\(false\)\?;/);
  assert.match(source, /encoder\.include_contentsize\(true\)\?;/);
  assert.equal(SERVER_ZSTD.checksum, false);

  // The frame descriptor writes the content size and no checksum or
  // dictionary.
  const frame = serverZstd(keys);
  const descriptor = frame[4]!;
  assert.ok(descriptor >> 6 !== 0 || (descriptor & 0x20) !== 0);
  assert.equal(descriptor & 0x04, 0);
  assert.equal(descriptor & 0x03, 0);
  assert.deepEqual(zstdDecompressSync(frame), keys);
});

test("a zstd representation reads as the artifact it decodes to", () => {
  const encoded = serverZstd(fixture);
  const fromDecoded = breakdown(fixture);
  const fromEncoded = breakdown(encoded);
  assert.deepEqual(fromEncoded.tables, fromDecoded.tables);
  assert.deepEqual(fromEncoded.representation, {
    bytes: encoded.byteLength,
    sha256: fromDecoded.zstd.sha256,
    sameBytes: true,
  });
  assert.equal(fromDecoded.representation, null);
});

test("a file whose parts and framing do not cover it exactly is refused", () => {
  const header = fixture.indexOf('"version":4');
  assert.ok(header > 0);
  const version5 = Buffer.from(fixture);
  version5.write('"version":5', header, "latin1");
  assert.throws(() => breakdown(version5), /version 5 is not/);

  const untagged = Buffer.from(fixture);
  untagged[0] = 0x58;
  assert.throws(() => breakdown(untagged), /no CALQART1 tag/);

  assert.throws(
    () => breakdown(fixture.subarray(0, fixture.byteLength - 1)),
    /runs past the end of the file/,
  );
  assert.throws(
    () => breakdown(Buffer.concat([fixture, Buffer.alloc(8)])),
    /8 bytes follow the last part/,
  );

  const result = breakdown(fixture);
  const parts = result.tables[0]!.columns.flatMap((column) => column.parts);
  const gap = parts.findIndex(
    (part, index) =>
      index > 0 &&
      part.offset > parts[index - 1]!.offset + parts[index - 1]!.bytes,
  );
  assert.ok(gap > 0);
  const dirty = Buffer.from(fixture);
  dirty[parts[gap]!.offset - 1] = 1;
  assert.throws(
    () => breakdown(dirty),
    new RegExp(`padding byte ${String(parts[gap]!.offset - 1)} is not zero`),
  );
});
