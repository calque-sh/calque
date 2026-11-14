import { describe, expect, it } from "vitest";

import { createGeneratedClientSchema } from "../client-options.js";
import type { GeneratedClientSchemaDescriptor } from "../generated-schema.js";
import { parseBootstrapPlan } from "../plan.js";
import { resolveClientSchema } from "../schema-registry.js";
import type { ClientSchema } from "../schema.js";
import {
  bindColumnarPlan,
  checkRenewal,
  renewablePlan,
} from "./plan-binding.js";

/** What a session checks of a bootstrap plan before it mounts. */
function verifyColumnarBootstrapPlan(schema: ClientSchema, plan: unknown) {
  bindColumnarPlan(
    resolveClientSchema(schema).descriptor.collections,
    parseBootstrapPlan(plan),
  );
}

const descriptor: GeneratedClientSchemaDescriptor = {
  collections: [
    {
      id: "events",
      tableId: "public.events",
      columns: [
        { name: "id", kind: "int64" },
        { name: "level", kind: "text" },
      ],
      primaryKey: ["id"],
    },
  ],
};

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
    requiredTables: ["public.events"],
    residentTables: [],
    activationCursor: "cursor-p",
    nextResidentStateProof: "proof-1",
    artifacts: [
      {
        artifactId: "artifact-1",
        attachAs: "stage_0000",
        snapshotCursor: "cursor-p",
        tables: ["public.events"],
        decodedSha256: "b".repeat(64),
        decodedBytes: 16,
        bufferCapacityBytes: 16,
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
                name: "events",
                primaryKey: ["id"],
                columns: [
                  { name: "id", type: "int64", nullable: false },
                  { name: "level", type: "dict", nullable: false },
                ],
              },
            ],
          },
          tables: [
            {
              tableId: "public.events",
              sourceSchema: "public",
              sourceTable: "events",
              localTable: "events",
              schemaHash: `sha256:${"1".repeat(64)}`,
              columnProfileHash: `sha256:${"2".repeat(64)}`,
              rowCount: 3,
              primaryKeyCodec: "pk-v1/protobuf",
              columns: [
                { columnId: 1, name: "id" },
                { columnId: 2, name: "level" },
              ],
            },
          ],
        },
        selectedRepresentation: {
          representationId: `sha256:${"b".repeat(64)}`,
          encoding: "identity",
          encodedBytes: 16,
          downloadUrl: "https://sync.test/artifact-1",
        },
      },
    ],
    changeStream: {
      endpoint: "https://sync.test/v1/changes",
      wireProtocol: "sync.change.v1",
      fromCursorExclusive: "cursor-p",
      throughCursorInclusive: "cursor-p",
      longPollTimeoutMs: 30_000,
      maxBatchBytes: 1024 * 1024,
    },
    memory: {
      minimumSafeBudgetBytes: 16 * 1024 * 1024,
      stagingOverlapBytes: 1024 * 1024,
      queryResultReserveBytes: 4 * 1024 * 1024,
      catchUpReserveBytes: 2 * 1024 * 1024,
      clientHeapReserveBytes: 4 * 1024 * 1024,
    },
  };
}

function firstRuntime(value: Record<string, unknown>): Record<string, unknown> {
  const artifact = (value.artifacts as Record<string, unknown>[])[0];
  if (!artifact) throw new Error("fixture artifact missing");
  return artifact.runtime as Record<string, unknown>;
}

describe("verifyColumnarBootstrapPlan", () => {
  const schema = createGeneratedClientSchema(descriptor);

  it("accepts a plan that serves every generated collection with compatible kinds", () => {
    expect(() => verifyColumnarBootstrapPlan(schema, plan())).not.toThrow();
  });

  it("binds collections by relation and takes the wire table id from the plan", () => {
    const hashed = plan();
    const wire = "table:9f2c:g18";
    hashed.requiredTables = [wire];
    const artifact = (hashed.artifacts as Record<string, unknown>[])[0];
    if (!artifact) throw new Error("fixture artifact missing");
    artifact.tables = [wire];
    const table = (firstRuntime(hashed).tables as Record<string, unknown>[])[0];
    if (!table) throw new Error("fixture table missing");
    table.tableId = wire;
    const bound = bindColumnarPlan(
      descriptor.collections,
      parseBootstrapPlan(hashed),
    );
    expect(bound.contract.tables.map(({ tableId }) => tableId)).toEqual([wire]);
    expect(bound.artifact.tables).toEqual([wire]);

    // The relation, not the wire id, is what the generated file must match.
    const otherRelation = structuredClone(hashed);
    const otherTable = (
      firstRuntime(otherRelation).tables as Record<string, unknown>[]
    )[0];
    if (!otherTable) throw new Error("fixture table missing");
    otherTable.sourceSchema = "audit";
    expect(() => verifyColumnarBootstrapPlan(schema, otherRelation)).toThrow(
      "the generated bindings do not match what the server serves: the server does not serve public.events, and no generated collection reads audit.events; regenerate them with calque-bindings from the calque.config.json the server runs",
    );
    // A plan serving a relation no collection names is refused too.
    const extra = structuredClone(hashed);
    const extraRuntime = firstRuntime(extra) as {
      tables: Record<string, unknown>[];
      schema: { tables: Record<string, unknown>[] };
    };
    const extraTable = structuredClone(extraRuntime.tables[0]);
    if (!extraTable) throw new Error("fixture table missing");
    extraTable.tableId = "table:0000:g18";
    extraTable.sourceTable = "audits";
    extraTable.localTable = "audits";
    extraRuntime.tables.push(extraTable);
    const extraSchema = structuredClone(extraRuntime.schema.tables[0]);
    if (!extraSchema) throw new Error("fixture schema table missing");
    extraSchema.name = "audits";
    extraRuntime.schema.tables.push(extraSchema);
    const extraArtifact = (extra.artifacts as Record<string, unknown>[])[0];
    if (!extraArtifact) throw new Error("fixture artifact missing");
    extraArtifact.tables = [wire, "table:0000:g18"];
    expect(() => verifyColumnarBootstrapPlan(schema, extra)).toThrow(
      /: no generated collection reads public\.audits; regenerate them with calque-bindings /u,
    );
  });

  it("refuses a plan that does not parse under the protocol", () => {
    const broken = plan();
    delete broken.memory;
    expect(() => verifyColumnarBootstrapPlan(schema, broken)).toThrow(
      /invalid bootstrap plan: memory/u,
    );
  });

  it("refuses a plan whose physical column kind drifted from the generated kind", () => {
    const drifted = plan();
    const runtime = (
      drifted.artifacts as { runtime: Record<string, unknown> }[]
    )[0]?.runtime;
    const schemaValue = runtime?.schema as {
      tables: { columns: { type: string }[] }[];
    };
    const level = schemaValue.tables[0]?.columns[1];
    if (!level) throw new Error("fixture column missing");
    level.type = "f64";
    expect(() => verifyColumnarBootstrapPlan(schema, drifted)).toThrow(
      /: events\.level is served as f64, not text; regenerate/u,
    );
  });

  it("binds an int32 column to the int64 column the server serves for it, and to nothing else", () => {
    const int32 = createGeneratedClientSchema({
      collections: descriptor.collections.map((collection) => ({
        ...collection,
        columns: [
          { name: "id", kind: "int32" },
          { name: "level", kind: "int32" },
        ],
      })),
    });
    const served = (type: string) => {
      const value = plan();
      const schemaValue = firstRuntime(value).schema as {
        tables: { columns: { type: string }[] }[];
      };
      const level = schemaValue.tables[0]?.columns[1];
      if (!level) throw new Error("fixture column missing");
      level.type = type;
      return value;
    };
    expect(() =>
      verifyColumnarBootstrapPlan(int32, served("int64")),
    ).not.toThrow();
    expect(() => verifyColumnarBootstrapPlan(int32, served("f64"))).toThrow(
      /: events\.level is served as f64, not int32; regenerate/u,
    );
  });

  it("refuses a plan whose primary key differs from the generated key", () => {
    const keyed = plan();
    const schemaValue = firstRuntime(keyed).schema as {
      tables: { primaryKey: string[] }[];
    };
    const events = schemaValue.tables[0];
    if (!events) throw new Error("fixture table missing");
    events.primaryKey = ["level"];
    expect(() => verifyColumnarBootstrapPlan(schema, keyed)).toThrow(
      /: events is keyed by level, not id; regenerate/u,
    );
  });

  it("refuses a plan whose tables carry no wire column ids", () => {
    const unbound = plan();
    const table = (
      firstRuntime(unbound).tables as Record<string, unknown>[]
    )[0];
    if (!table) throw new Error("fixture table missing");
    delete table.columns;
    expect(() => verifyColumnarBootstrapPlan(schema, unbound)).toThrow(
      /carries no wire column ids/u,
    );
  });

  it("refuses a plan whose wire column ids do not name the key column", () => {
    const keyless = plan();
    const table = (
      firstRuntime(keyless).tables as Record<string, unknown>[]
    )[0];
    if (!table) throw new Error("fixture table missing");
    table.columns = [{ columnId: 2, name: "level" }];
    expect(() => verifyColumnarBootstrapPlan(schema, keyless)).toThrow(
      /does not carry key column id/u,
    );
  });

  it("refuses a plan that does not require every generated collection", () => {
    // An artifact that happens to carry a table the plan did not require is
    // no promise to keep it whole. The relation join alone cannot see this,
    // since the artifact's tables are still complete. It takes two
    // collections, because a one-collection plan can only drop to the empty
    // `requiredTables` the parser refuses first.
    const twoCollections = createGeneratedClientSchema({
      ...descriptor,
      collections: [
        ...descriptor.collections,
        {
          id: "audit",
          tableId: "public.audit",
          columns: [{ name: "id", kind: "int64" }],
          primaryKey: ["id"],
        },
      ],
    });
    const twoTablePlan = (): Record<string, unknown> => {
      const value = plan();
      value.requiredTables = ["public.events", "public.audit"];
      const artifact = (value.artifacts as Record<string, unknown>[])[0];
      if (!artifact) throw new Error("fixture artifact missing");
      artifact.tables = ["public.events", "public.audit"];
      const runtime = firstRuntime(value);
      ((runtime.schema as Record<string, unknown>).tables as unknown[]).push({
        name: "audit",
        primaryKey: ["id"],
        columns: [{ name: "id", type: "int64", nullable: false }],
      });
      (runtime.tables as unknown[]).push({
        tableId: "public.audit",
        sourceSchema: "public",
        sourceTable: "audit",
        localTable: "audit",
        schemaHash: `sha256:${"5".repeat(64)}`,
        columnProfileHash: `sha256:${"6".repeat(64)}`,
        rowCount: 1,
        primaryKeyCodec: "pk-v1/protobuf",
        columns: [{ columnId: 1, name: "id" }],
      });
      return value;
    };
    expect(() =>
      verifyColumnarBootstrapPlan(twoCollections, twoTablePlan()),
    ).not.toThrow();

    const partial = twoTablePlan();
    partial.requiredTables = ["public.events"];
    expect(() => verifyColumnarBootstrapPlan(twoCollections, partial)).toThrow(
      /does not match the generated collections/u,
    );
  });

  it("refuses a schema that carries no collections", () => {
    const collectionless = createGeneratedClientSchema({
      collections: [],
    });
    expect(() => verifyColumnarBootstrapPlan(collectionless, plan())).toThrow(
      /no generated collections/u,
    );
  });

  it("refuses a value that is not a generated schema", () => {
    expect(() =>
      verifyColumnarBootstrapPlan({ collections: [] } as never, plan()),
    ).toThrow(/schema is invalid/u);
  });
});

describe("a renewal plan", () => {
  /** The fixture plan renewed from `cursor`: it installs nothing. */
  function renewal(cursor: string): Record<string, unknown> {
    const value = plan();
    value.planId = "plan-2";
    value.residentTables = ["public.events"];
    value.residentCursor = cursor;
    value.activationCursor = cursor;
    value.artifacts = [];
    Object.assign(value.changeStream as Record<string, unknown>, {
      fromCursorExclusive: cursor,
      throughCursorInclusive: cursor,
    });
    return value;
  }

  const held = renewablePlan(parseBootstrapPlan(plan()));

  it("continues the plan it renews from the cursor the client presented", () => {
    expect(() =>
      checkRenewal(held, parseBootstrapPlan(renewal("cursor-q")), "cursor-q"),
    ).not.toThrow();
  });

  it.each<[string, (value: Record<string, unknown>) => void, RegExp]>([
    [
      "installs an artifact",
      (value) => {
        value.artifacts = plan().artifacts;
        value.residentTables = [];
      },
      /install no artifact/u,
    ],
    [
      "moves to another source",
      (value) => {
        (value.source as Record<string, unknown>).sourceEpoch = "epoch-2";
      },
      /keep the source and scope/u,
    ],
    [
      "moves to another schema generation",
      (value) => {
        (value.source as Record<string, unknown>).schemaGeneration = 2;
      },
      /keep the source and scope/u,
    ],
    [
      "moves to another authorization generation",
      (value) => {
        (value.source as Record<string, unknown>).authorizationGeneration = 2;
      },
      /keep the source and scope/u,
    ],
    [
      "moves to another scope",
      (value) => {
        (value.authorization as Record<string, unknown>).scopeHash =
          `sha256:${"b".repeat(64)}`;
      },
      /keep the source and scope/u,
    ],
    [
      "requires other tables",
      (value) => {
        value.requiredTables = ["public.issues"];
        value.residentTables = ["public.issues"];
      },
      /keep the tables/u,
    ],
    [
      "continues from another cursor",
      (value) => {
        value.residentCursor = "cursor-r";
      },
      /continue from the cursor/u,
    ],
    [
      "activates at another cursor",
      (value) => {
        value.activationCursor = "cursor-r";
        (value.changeStream as Record<string, unknown>).throughCursorInclusive =
          "cursor-r";
      },
      /continue from the cursor/u,
    ],
    [
      "streams from another cursor",
      (value) => {
        (value.changeStream as Record<string, unknown>).fromCursorExclusive =
          "cursor-r";
      },
      /continue from the cursor/u,
    ],
  ])("is refused when it %s", (_name, change, refusal) => {
    const value = renewal("cursor-q");
    change(value);
    expect(() =>
      checkRenewal(held, parseBootstrapPlan(value), "cursor-q"),
    ).toThrow(refusal);
  });
});
