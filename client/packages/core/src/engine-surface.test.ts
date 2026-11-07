/**
 * One logical value, one shape. Materialization (`ArtifactColumn.valueAt`,
 * which `EngineWorkerClient.rows` returns verbatim) and projection
 * (`ColumnarQueryEngine.query`) both produce application-visible values, and a
 * grid consumes both. This test compares the two against each other over every
 * column kind, so no hand-written literal can agree with the side that is
 * wrong.
 */
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";
import {
  openDirectColumnarArtifact,
  type DirectColumnarColumnKind,
} from "./artifact.js";
import { OverlayEngine, artifactBase } from "./overlay.js";
import { ColumnarQueryEngine, type QuerySpecification } from "./query.js";

const IDENTITY = {
  sourceEpoch: "source-epoch-1",
  manifestVersion: "manifest-1",
  collectionSetHash: `sha256:${"1".repeat(64)}`,
  schemaHash:
    "sha256:2c3a79cf0ece0f4763bfe35bff9ac4aa9bb70df10811a2012e4b78d529dea20c",
  authorizationScopeVersion: "scope-1",
  authorizationScopeHash: `sha256:${"2".repeat(64)}`,
  cursor: "cursor-1",
} as const;

/** Every kind in `DirectColumnarColumnKind`, in the emitter's column order. */
const COLUMNS: readonly {
  readonly name: string;
  readonly type: DirectColumnarColumnKind;
  readonly nullable: boolean;
}[] = [
  { name: "id", type: "dict", nullable: false },
  { name: "score", type: "f64", nullable: true },
  { name: "signed", type: "int64", nullable: false },
  { name: "unsigned", type: "uint64", nullable: false },
  { name: "active", type: "bool", nullable: false },
  { name: "observed_at", type: "timestamp", nullable: false },
  { name: "note", type: "utf8", nullable: true },
  { name: "raw", type: "bytes", nullable: false },
  { name: "uuid", type: "uuid", nullable: false },
  { name: "amount", type: "decimal", nullable: false },
  { name: "day", type: "date", nullable: false },
  { name: "clock", type: "time", nullable: false },
  { name: "span", type: "interval", nullable: false },
  { name: "meta", type: "json", nullable: true },
  { name: "alias", type: "uuid", nullable: true },
];

const SCHEMA = {
  format: "calque-direct-columnar" as const,
  version: 4 as const,
  tables: [
    {
      name: "items",
      primaryKey: ["signed", "raw", "uuid"],
      columns: COLUMNS,
    },
  ],
};

async function emitFixture(): Promise<{
  readonly bytes: Uint8Array<ArrayBuffer>;
  readonly cleanup: () => Promise<void>;
}> {
  const directory = await mkdtemp(join(tmpdir(), "calque-engine-surface-"));
  const artifactPath = join(directory, "fixture.calque");
  const cleanup = () => rm(directory, { recursive: true, force: true });
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
      "emit_fixture",
      "--",
      artifactPath,
    ],
    { cwd: repository, encoding: "utf8" },
  );
  expect(run.status, run.stderr).toBe(0);
  return { bytes: Uint8Array.from(await readFile(artifactPath)), cleanup };
}

describe("engine value surfaces", () => {
  test("materialization and projection agree on every column kind", async () => {
    const { bytes, cleanup } = await emitFixture();
    try {
      const digest =
        `sha256:${createHash("sha256").update(bytes).digest("hex")}` as const;
      const workspace = await openDirectColumnarArtifact(bytes.buffer, {
        byteLength: bytes.byteLength,
        digest,
        identity: IDENTITY,
        schema: SCHEMA,
      });
      const table = workspace.table("items");
      const names = COLUMNS.map((column) => column.name);

      // The materialization surface, as the engine worker builds an
      // `EngineWorkerClient.rows` response.
      const accessors = names.map((name) => table.column(name));
      const materialized = Array.from({ length: table.rows }, (_, row) =>
        accessors.map((column) => column.valueAt(row)),
      );

      // The projection surface, over the same artifact through the overlay.
      const engine = new ColumnarQueryEngine(
        new OverlayEngine([
          { name: "items", columns: names, base: artifactBase(table, names) },
        ]),
        { capacityBytes: 8_000_000 },
      );
      const specification: QuerySpecification = {
        version: 1,
        from: { collection: "items", as: "items" },
        select: names.map((name) => ({
          kind: "field" as const,
          field: { source: "items", column: name },
          as: name,
        })),
        orderBy: [{ by: "signed", direction: "asc" }],
      };
      const projected = engine.query(specification, {
        maxRows: 64,
        maxBytes: 64_000,
      });

      expect(projected.columns).toEqual(names);
      expect(projected.rows).toHaveLength(materialized.length);

      // Compare surface against surface, kind by kind, so the failure names the
      // column that disagrees rather than reporting one opaque mismatch.
      const divergent: string[] = [];
      for (const [index, column] of COLUMNS.entries()) {
        const fromRows = materialized.map((row) => row[index]);
        const fromQuery = projected.rows.map((row) => row[index]);
        try {
          expect(fromQuery).toEqual(fromRows);
        } catch {
          divergent.push(`${column.name} (${column.type})`);
        }
      }
      expect(divergent).toEqual([]);
    } finally {
      await cleanup();
    }
    // `cargo run` may compile the example first; the artifact suite uses the
    // same budget for the same reason.
  }, 300_000);
});
