import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { describe, expect, it } from "vitest";

import { ARTIFACT_CHUNK_BYTES, parseBootstrapPlan } from "./plan.js";

const COLUMNAR_VECTOR = new URL(
  "../../../../spec/bootstrap-plan.direct-columnar.example.json",
  import.meta.url,
);

function plan(): Record<string, unknown> {
  return {
    protocolVersion: 1,
    planId: "plan-1",
    expiresAt: "2099-01-01T00:00:00Z",
    source: {
      sourceEpoch: "epoch-1",
      publicationGeneration: 1,
      schemaGeneration: 1,
      authorizationGeneration: 1,
      authorizationPolicyHash: `sha256:${"9".repeat(64)}`,
    },
    authorization: {
      policyHash: `sha256:${"9".repeat(64)}`,
      scopeHash: `sha256:${"a".repeat(64)}`,
      scopeToken: "scope-secret",
    },
    requiredTables: ["public.tasks"],
    residentTables: [],
    activationCursor: "cursor-h",
    nextResidentStateProof: "proof-1",
    artifacts: [
      {
        artifactId: "artifact-1",
        attachAs: "stage_0000",
        snapshotCursor: "cursor-p",
        tables: ["public.tasks"],
        decodedSha256: "b".repeat(64),
        decodedBytes: 4096,
        bufferCapacityBytes: 8192,
        installOrder: 0,
        runtime: {
          format: "directColumnar",
          formatVersion: 4,
          tableSetHash: `sha256:${"c".repeat(64)}`,
          shardProfileHash: `sha256:${"d".repeat(64)}`,
          indexProfileHash: `sha256:${"e".repeat(64)}`,
          schemaHash: `sha256:${"3".repeat(64)}`,
          collectionSetHash: `sha256:${"4".repeat(64)}`,
          identity: {
            manifestVersion: "manifest-1",
            authorizationScopeVersion: "scope-1",
          },
          chunks: {
            bytes: 4 * 1024 * 1024,
            sha256: [`sha256:${"5".repeat(64)}`],
          },
          schema: {
            format: "calque-direct-columnar",
            version: 4,
            tables: [
              {
                name: "tasks",
                primaryKey: ["id"],
                columns: [
                  { name: "id", type: "int64", nullable: false },
                  { name: "title", type: "utf8", nullable: true },
                ],
              },
            ],
          },
          tables: [
            {
              tableId: "public.tasks",
              sourceSchema: "public",
              sourceTable: "tasks",
              localTable: "tasks",
              schemaHash: `sha256:${"1".repeat(64)}`,
              columnProfileHash: `sha256:${"2".repeat(64)}`,
              rowCount: 0,
              primaryKeyCodec: "pk-v1/protobuf",
              columns: [
                { columnId: 1, name: "id" },
                { columnId: 2, name: "title" },
              ],
            },
          ],
        },
        selectedRepresentation: {
          representationId: `sha256:${"b".repeat(64)}`,
          encoding: "identity",
          encodedBytes: 4096,
          downloadUrl: "https://sync.test/artifact-1",
        },
      },
    ],
    changeStream: {
      endpoint: "https://sync.test/v1/changes",
      wireProtocol: "sync.change.v1",
      fromCursorExclusive: "cursor-p",
      throughCursorInclusive: "cursor-h",
      longPollTimeoutMs: 30_000,
      maxBatchBytes: 1024 * 1024,
    },
    memory: memoryPlan(16 * 1024 * 1024),
  };
}

function memoryPlan(minimumSafeBudgetBytes: number) {
  return {
    minimumSafeBudgetBytes,
    stagingOverlapBytes: 1024 * 1024,
    queryResultReserveBytes: 4 * 1024 * 1024,
    catchUpReserveBytes: 2 * 1024 * 1024,
    clientHeapReserveBytes: defaultClientHeapReserveBytes(
      minimumSafeBudgetBytes,
    ),
  };
}

function defaultClientHeapReserveBytes(memoryBudgetBytes: number): number {
  return Math.min(
    64 * 1024 * 1024,
    Math.max(
      1,
      Math.min(4 * 1024 * 1024, memoryBudgetBytes),
      Math.floor(memoryBudgetBytes / 8),
    ),
    memoryBudgetBytes,
  );
}

describe("bootstrap plan validation", () => {
  it("accepts an exact artifact runtime contract", () => {
    expect(parseBootstrapPlan(plan())).toMatchObject({
      planId: "plan-1",
      requiredTables: ["public.tasks"],
    });
  });

  it("rejects lossy integers, overlapping tables, and mismatched fences", () => {
    const lossy = plan();
    (lossy.source as Record<string, unknown>).schemaGeneration =
      Number.MAX_SAFE_INTEGER + 1;
    expect(() => parseBootstrapPlan(lossy)).toThrow("schemaGeneration");

    const overlap = plan();
    overlap.residentTables = ["public.tasks"];
    expect(() => parseBootstrapPlan(overlap)).toThrow("overlap");

    const fence = plan();
    (fence.changeStream as Record<string, unknown>).throughCursorInclusive =
      "cursor-other";
    expect(() => parseBootstrapPlan(fence)).toThrow("activationCursor");

    const policy = plan();
    (policy.authorization as Record<string, unknown>).policyHash =
      `sha256:${"8".repeat(64)}`;
    expect(() => parseBootstrapPlan(policy)).toThrow(
      "must equal source.authorizationPolicyHash",
    );
  });

  it("rejects unsafe delivery URLs and incomplete or ambiguous installs", () => {
    const missingRepresentationId = plan();
    const missingIdArtifact = (
      missingRepresentationId.artifacts as Record<string, unknown>[]
    )[0];
    if (!missingIdArtifact) throw new Error("fixture artifact missing");
    delete (missingIdArtifact.selectedRepresentation as Record<string, unknown>)
      .representationId;
    expect(() => parseBootstrapPlan(missingRepresentationId)).toThrow(
      "representationId",
    );

    const malformedRepresentationId = plan();
    const malformedIdArtifact = (
      malformedRepresentationId.artifacts as Record<string, unknown>[]
    )[0];
    if (!malformedIdArtifact) throw new Error("fixture artifact missing");
    (
      malformedIdArtifact.selectedRepresentation as Record<string, unknown>
    ).representationId = "not-content-addressed";
    expect(() => parseBootstrapPlan(malformedRepresentationId)).toThrow(
      "representationId",
    );

    const unsafeUrl = plan();
    const unsafeArtifact = (
      unsafeUrl.artifacts as Record<string, unknown>[]
    )[0];
    if (!unsafeArtifact) throw new Error("fixture artifact missing");
    (
      unsafeArtifact.selectedRepresentation as Record<string, unknown>
    ).downloadUrl = "ftp://localhost/artifact-1";
    expect(() => parseBootstrapPlan(unsafeUrl)).toThrow("HTTPS");

    const missing = plan();
    missing.requiredTables = ["public.tasks", "public.users"];
    expect(() => parseBootstrapPlan(missing)).toThrow(
      "do not cover required table public.users",
    );

    const ambiguous = plan();
    const original = (ambiguous.artifacts as Record<string, unknown>[])[0];
    if (!original) throw new Error("fixture artifact missing");
    const duplicate = structuredClone(original);
    const originalRuntimeTable = (
      (original.runtime as Record<string, unknown>).tables as Record<
        string,
        unknown
      >[]
    )[0];
    if (!originalRuntimeTable) throw new Error("fixture runtime table missing");
    duplicate.tables = ["public.users"];
    (duplicate.runtime as Record<string, unknown>).tables = [
      {
        ...originalRuntimeTable,
        tableId: "public.users",
      },
    ];
    duplicate.installOrder = 1;
    (ambiguous.artifacts as Record<string, unknown>[]).push(duplicate);
    ambiguous.requiredTables = ["public.tasks", "public.users"];
    expect(
      (ambiguous.artifacts as Record<string, unknown>[]).map(
        (artifact) => artifact.artifactId,
      ),
    ).toEqual(["artifact-1", "artifact-1"]);
    expect(() => parseBootstrapPlan(ambiguous)).toThrow("artifactId");
  });
});

type Mutable = Record<string, unknown>;

function firstArtifact(value: Mutable): Mutable {
  const artifact = (value.artifacts as Mutable[])[0];
  if (!artifact) throw new Error("fixture artifact missing");
  return artifact;
}

function firstRuntimeTable(runtime: Mutable): Mutable {
  const table = (runtime.tables as Mutable[])[0];
  if (!table) throw new Error("fixture runtime table missing");
  return table;
}

function firstSchemaTable(runtime: Mutable): {
  name: string;
  columns: { type: string }[];
} {
  const table = (runtime.schema as { tables: Mutable[] }).tables[0];
  if (!table) throw new Error("fixture schema table missing");
  return table as { name: string; columns: { type: string }[] };
}

/** The only artifact runtime the client accepts; `plan()` already builds it. */
function columnarPlan(): Mutable {
  return plan();
}

describe("direct-columnar bootstrap plans", () => {
  it("parses the checked-in vector the server-side schema test validates", () => {
    const parsed = parseBootstrapPlan(
      JSON.parse(readFileSync(COLUMNAR_VECTOR, "utf8")) as unknown,
    );
    const runtime = parsed.artifacts[0]?.runtime;
    if (runtime?.format !== "directColumnar")
      throw new Error("expected a columnar runtime");
    expect(runtime.identity).toEqual({
      manifestVersion: "manifest_01JZCOLUMNAR",
      authorizationScopeVersion: "authorization-generation-9",
    });
    expect(runtime.tables[0]?.columns?.map((column) => column.name)).toEqual(
      runtime.schema.tables[0]?.columns.map((column) => column.name),
    );
    // The decoder recomputes schemaHash over the canonical JSON of the schema
    // it was handed, so the vector's hash has to be that digest or the plan
    // could never open the bytes it describes.
    expect(runtime.schemaHash).toBe(
      `sha256:${createHash("sha256")
        .update(JSON.stringify(runtime.schema))
        .digest("hex")}`,
    );
    expect(parsed.changeStream.throughCursorInclusive).toBe(
      parsed.activationCursor,
    );
    expect(runtime.chunks.sha256).toHaveLength(
      Math.ceil(
        (parsed.artifacts[0]?.decodedBytes ?? 0) / ARTIFACT_CHUNK_BYTES,
      ),
    );
  });

  it("accepts a columnar artifact with its identity, schema and wire ids", () => {
    const parsed = parseBootstrapPlan(columnarPlan());
    expect(parsed.changeStream.endpoint).toBe("https://sync.test/v1/changes");
    const runtime = parsed.artifacts[0]?.runtime;
    expect(runtime?.format).toBe("directColumnar");
    if (runtime?.format !== "directColumnar")
      throw new Error("expected a columnar runtime");
    expect(runtime.schema.tables[0]?.columns.map((c) => c.type)).toEqual([
      "int64",
      "utf8",
    ]);
    expect(runtime.identity.manifestVersion).toBe("manifest-1");
    expect(runtime.tables[0]?.columns).toEqual([
      { columnId: 1, name: "id" },
      { columnId: 2, name: "title" },
    ]);
  });

  it("requires the change stream for a columnar artifact", () => {
    // The spec requires every plan to name the stream its mount continues on;
    // a columnar mount without one would be a download, not a Calque mount.
    const columnar = columnarPlan();
    delete columnar.changeStream;
    expect(() => parseBootstrapPlan(columnar)).toThrow("changeStream");
  });

  it("refuses a SQLite artifact runtime and an unnamed format", () => {
    // Neither `format: "sqlite"` nor a plan that names no format is a runtime
    // this client can open. Both are refused at the plan, before a byte is
    // asked for, rather than accepted and failed later at the mount.
    const sqlite = columnarPlan();
    const sqliteRuntime = firstArtifact(sqlite).runtime as Mutable;
    sqliteRuntime.format = "sqlite";
    sqliteRuntime.sqliteBuildProfileHash = `sha256:${"f".repeat(64)}`;
    sqliteRuntime.pageSize = 4096;
    expect(() => parseBootstrapPlan(sqlite)).toThrow("runtime.format");

    const unnamed = columnarPlan();
    delete (firstArtifact(unnamed).runtime as Mutable).format;
    expect(() => parseBootstrapPlan(unnamed)).toThrow("runtime.format");
  });

  it("refuses the retired Brotli representation", () => {
    const brotli = columnarPlan();
    (firstArtifact(brotli).selectedRepresentation as Mutable).encoding = "br";
    expect(() => parseBootstrapPlan(brotli)).toThrow(
      "selectedRepresentation.encoding",
    );
  });

  it("admits the large-window brotli representation, which is not the browser's br", () => {
    const largeWindow = columnarPlan();
    (firstArtifact(largeWindow).selectedRepresentation as Mutable).encoding =
      "brotli-large-window";
    expect(
      parseBootstrapPlan(largeWindow).artifacts[0]?.selectedRepresentation
        .encoding,
    ).toBe("brotli-large-window");
  });

  it("admits the packed large-window brotli representation, under that name alone", () => {
    const packed = columnarPlan();
    (firstArtifact(packed).selectedRepresentation as Mutable).encoding =
      "packed-brotli-large-window";
    expect(
      parseBootstrapPlan(packed).artifacts[0]?.selectedRepresentation.encoding,
    ).toBe("packed-brotli-large-window");
    for (const encoding of [
      "packed",
      "packed-brotli",
      "packed-zstd",
      "Packed-Brotli-Large-Window",
    ]) {
      (firstArtifact(packed).selectedRepresentation as Mutable).encoding =
        encoding;
      expect(() => parseBootstrapPlan(packed), encoding).toThrow(
        "selectedRepresentation.encoding",
      );
    }
  });

  it("carries one digest for each 4 MiB chunk of the artifact, and refuses any other chunk description", () => {
    const MIB = 1024 * 1024;
    const digests = (count: number) =>
      Array.from(
        { length: count },
        (_unused, index) => `sha256:${index.toString(16).padStart(64, "0")}`,
      );
    const chunked = (decodedBytes: number, chunks: unknown) => {
      const value = columnarPlan();
      const artifact = firstArtifact(value);
      artifact.decodedBytes = decodedBytes;
      artifact.bufferCapacityBytes = decodedBytes;
      (artifact.selectedRepresentation as Mutable).encodedBytes = decodedBytes;
      (artifact.runtime as Mutable).chunks = chunks;
      return value;
    };
    expect(ARTIFACT_CHUNK_BYTES).toBe(4 * MIB);
    // Without them no chunk could be verified as it arrives.
    const unchunked = columnarPlan();
    delete (firstArtifact(unchunked).runtime as Mutable).chunks;
    expect(() => parseBootstrapPlan(unchunked)).toThrow("runtime.chunks");
    // The last chunk is what is left: one byte past a whole chunk is a
    // chunk of its own.
    for (const [bytes, count] of [
      [1, 1],
      [4 * MIB - 1, 1],
      [4 * MIB, 1],
      [4 * MIB + 1, 2],
      [12 * MIB, 3],
    ] as const)
      expect(
        parseBootstrapPlan(
          chunked(bytes, { bytes: 4 * MIB, sha256: digests(count) }),
        ).artifacts[0]?.runtime.chunks,
      ).toEqual({ bytes: 4 * MIB, sha256: digests(count) });

    const refused: readonly (readonly [number, unknown, string])[] = [
      // A count that leaves bytes unbound, or names a chunk past the end.
      [4 * MIB + 1, { bytes: 4 * MIB, sha256: digests(1) }, "chunks.sha256"],
      [8 * MIB, { bytes: 4 * MIB, sha256: digests(3) }, "chunks.sha256"],
      [8 * MIB, { bytes: 4 * MIB, sha256: [] }, "chunks.sha256"],
      [8 * MIB, { bytes: 4 * MIB, sha256: digests(2)[0] }, "chunks.sha256"],
      // Any other size, even with the count that size would need.
      [8 * MIB, { bytes: 0, sha256: [] }, "chunks.bytes"],
      [8 * MIB, { bytes: 2 * MIB, sha256: digests(4) }, "chunks.bytes"],
      [8 * MIB, { bytes: 4 * MIB - 1, sha256: digests(3) }, "chunks.bytes"],
      [8 * MIB, { bytes: 4 * MIB + 1, sha256: digests(2) }, "chunks.bytes"],
      [8 * MIB, { bytes: 8 * MIB, sha256: digests(1) }, "chunks.bytes"],
      [8 * MIB, { bytes: String(4 * MIB), sha256: digests(2) }, "chunks.bytes"],
      // A digest that is not `sha256:` and 64 lowercase hex characters.
      [4 * MIB, { bytes: 4 * MIB, sha256: ["A".repeat(64)] }, "sha256[0]"],
      [
        8 * MIB,
        { bytes: 4 * MIB, sha256: [digests(1)[0], `sha256:${"A".repeat(64)}`] },
        "sha256[1]",
      ],
      [
        4 * MIB,
        { bytes: 4 * MIB, sha256: [`sha256:${"a".repeat(63)}`] },
        "sha256[0]",
      ],
      [4 * MIB, { bytes: 4 * MIB, sha256: [7] }, "sha256[0]"],
      // Nothing but the two fields, and an object.
      [4 * MIB, { bytes: 4 * MIB, sha256: digests(1), last: 1 }, "chunks"],
      [4 * MIB, { bytes: 4 * MIB }, "chunks"],
      [4 * MIB, [4 * MIB, digests(1)], "chunks"],
      [4 * MIB, null, "chunks"],
    ];
    for (const [bytes, chunks, path] of refused)
      expect(() => parseBootstrapPlan(chunked(bytes, chunks)), path).toThrow(
        path,
      );
  });

  it("rejects an unsupported format version, an unknown column kind, a schema that misses a table, and duplicate column ids", () => {
    const version = columnarPlan();
    (firstArtifact(version).runtime as Mutable).formatVersion = 3;
    expect(() => parseBootstrapPlan(version)).toThrow("formatVersion");

    const kind = columnarPlan();
    const kindColumn = firstSchemaTable(firstArtifact(kind).runtime as Mutable)
      .columns[0];
    if (!kindColumn) throw new Error("fixture column missing");
    kindColumn.type = "varchar";
    expect(() => parseBootstrapPlan(kind)).toThrow(
      "runtime.schema invalid trusted direct-columnar column schema",
    );

    const missing = columnarPlan();
    firstSchemaTable(firstArtifact(missing).runtime as Mutable).name = "other";
    expect(() => parseBootstrapPlan(missing)).toThrow(
      "must exactly match runtime.tables",
    );

    const duplicateColumn = columnarPlan();
    const columns = firstRuntimeTable(
      firstArtifact(duplicateColumn).runtime as Mutable,
    ).columns as { columnId: number }[];
    const second = columns[1];
    if (!second) throw new Error("fixture column missing");
    second.columnId = 1;
    expect(() => parseBootstrapPlan(duplicateColumn)).toThrow(
      "duplicate column IDs",
    );
  });
});
