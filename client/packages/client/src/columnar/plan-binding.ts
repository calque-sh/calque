import type {
  DirectColumnarColumnKind,
  DirectColumnarTableSchema,
  TrustedArtifact,
} from "@calque/core";
import type { ColumnarChangeContract } from "@calque/worker/columnar";

import { SyncError } from "../error.js";
import type {
  GeneratedClientCollectionDescriptor,
  GeneratedClientColumnKind,
} from "../generated-schema.js";
import {
  hashIdBytes,
  sameSet,
  type BootstrapPlan,
  type PlanArtifact,
  type PlanArtifactColumnarRuntime,
  type PlanArtifactTable,
} from "../plan.js";

/**
 * Binds one parsed bootstrap plan to the collections a generated schema
 * declares, before any byte reaches the engine. A plan that does not describe
 * exactly one direct-columnar artifact covering every collection, as when the
 * generated file predates the served schema, fails with `INVALID_RESPONSE`
 * and recovery `reset`.
 *
 * Collections and plan tables meet on the source relation, `schema.table`.
 * The wire `tableId` is hashed over one source's identity and schema
 * generation, so the binding takes it from the plan: a generated file that
 * carried it would bind an application build to one database.
 */

const PHYSICAL_KINDS: Readonly<
  Record<GeneratedClientColumnKind, readonly DirectColumnarColumnKind[]>
> = {
  bool: ["bool"],
  int32: ["int64"],
  int64: ["int64"],
  f64: ["f64"],
  decimal: ["decimal"],
  text: ["utf8", "dict"],
  bytes: ["bytes"],
  uuid: ["uuid"],
  json: ["json"],
  date: ["date"],
  time: ["time"],
  timestamp: ["timestamp"],
};

interface BoundColumnarPlan {
  readonly artifact: PlanArtifact;
  readonly trusted: TrustedArtifact;
  /** The change contract the plan's wire column ids compile to. */
  readonly contract: ColumnarChangeContract;
}

function invalid(detail: string): SyncError {
  return new SyncError("INVALID_RESPONSE", "sync", "reset", detail);
}

/**
 * The physical schema of one plan table. `parseBootstrapPlan` requires the
 * schema's table names to be exactly the runtime tables' `localTable`s, so a
 * miss here is a parser defect, not a plan the server could have sent.
 */
function schemaTableOf(
  runtime: PlanArtifactColumnarRuntime,
  localTable: string,
): DirectColumnarTableSchema {
  const table = runtime.schema.tables.find(
    (candidate) => candidate.name === localTable,
  );
  if (!table)
    throw new TypeError(
      `parsed bootstrap plan lost the schema/table invariant for ${localTable}`,
    );
  return table;
}

/** The source relation a plan table was resolved from, `schema.table`. */
function relationOf(table: PlanArtifactTable): string {
  return `${table.sourceSchema}.${table.sourceTable}`;
}

/**
 * Checks the generated descriptor against the artifact the plan serves: the
 * plan must serve exactly the generated relations, each collection must map
 * to an artifact table of the same name, carry the same columns with
 * compatible kinds, and share the primary key.
 */
function checkCollections(
  collections: readonly GeneratedClientCollectionDescriptor[],
  artifact: PlanArtifact,
  runtime: PlanArtifactColumnarRuntime,
): void {
  const drift = (detail: string): SyncError =>
    invalid(
      `the generated bindings do not match what the server serves: ${detail}; regenerate them with calque-bindings from the calque.config.json the server runs`,
    );
  const served = new Map(
    runtime.tables.map((table) => [relationOf(table), table] as const),
  );
  const generated = collectionRelations(collections);
  if (
    served.size !== runtime.tables.length ||
    !sameSet([...served.keys()], generated)
  ) {
    const unserved = generated.filter((relation) => !served.has(relation));
    const ungenerated = [...served.keys()]
      .filter((relation) => !generated.includes(relation))
      .sort();
    throw drift(
      [
        ...(unserved.length === 0
          ? []
          : [`the server does not serve ${unserved.join(", ")}`]),
        ...(ungenerated.length === 0
          ? []
          : [`no generated collection reads ${ungenerated.join(", ")}`]),
        ...(unserved.length + ungenerated.length === 0
          ? ["the plan serves one relation twice"]
          : []),
      ].join(", and "),
    );
  }
  for (const collection of collections) {
    const planTable = served.get(collection.tableId);
    if (!planTable || !artifact.tables.includes(planTable.tableId))
      throw drift(`the server does not serve ${collection.tableId}`);
    if (planTable.localTable !== collection.id)
      throw drift(
        `${collection.tableId} is served as ${planTable.localTable}, not ${collection.id}`,
      );
    const schemaTable = schemaTableOf(runtime, collection.id);
    const physical = new Map(
      schemaTable.columns.map((column) => [column.name, column.type]),
    );
    for (const column of collection.columns) {
      const kind = physical.get(column.name);
      if (kind === undefined)
        throw drift(
          `the server serves no column ${collection.id}.${column.name}`,
        );
      if (!PHYSICAL_KINDS[column.kind].includes(kind))
        throw drift(
          `${collection.id}.${column.name} is served as ${kind}, not ${column.kind}`,
        );
    }
    if (
      schemaTable.primaryKey.length !== collection.primaryKey.length ||
      !schemaTable.primaryKey.every(
        (name, index) => collection.primaryKey[index] === name,
      )
    )
      throw drift(
        `${collection.id} is keyed by ${schemaTable.primaryKey.join(", ")}, not ${collection.primaryKey.join(", ")}`,
      );
  }
}

/**
 * The change contract the plan's wire column ids compile to. Every plan names
 * a change stream, so a columnar plan whose tables carry no wire ids is one
 * the client could open but never keep current; it is refused before any byte
 * is fetched rather than opened frozen.
 */
function changeContract(
  plan: BootstrapPlan,
  runtime: PlanArtifactColumnarRuntime,
): ColumnarChangeContract {
  return {
    planId: plan.planId,
    generation: {
      sourceEpoch: plan.source.sourceEpoch,
      publicationGeneration: BigInt(plan.source.publicationGeneration),
      schemaGeneration: BigInt(plan.source.schemaGeneration),
      authorizationGeneration: BigInt(plan.source.authorizationGeneration),
      authorizationScopeHash: hashIdBytes(plan.authorization.scopeHash),
    },
    tables: runtime.tables.map((table) => {
      const columns = table.columns;
      if (columns === undefined)
        throw invalid(
          `plan table ${table.tableId} carries no wire column ids; the change stream cannot be bound`,
        );
      const primaryKey = schemaTableOf(
        runtime,
        table.localTable,
      ).primaryKey.map((name) => {
        const column = columns.find((candidate) => candidate.name === name);
        if (!column)
          throw invalid(
            `plan table ${table.tableId} does not carry key column ${name}`,
          );
        return column.columnId;
      });
      return {
        tableId: table.tableId,
        collection: table.localTable,
        columns: columns.map(({ columnId, name }) => ({ columnId, name })),
        primaryKey,
      };
    }),
  };
}

/**
 * The source relations the generated collections require, `schema.table`
 * each, sorted: what the session asks the server for, and what the binding
 * requires the plan to serve exactly.
 */
export function collectionRelations(
  collections: readonly GeneratedClientCollectionDescriptor[],
): string[] {
  return collections.map(({ tableId }) => tableId).sort();
}

/**
 * Binds a parsed plan to the generated collections, or throws the
 * INVALID_RESPONSE the session reports.
 */
export function bindColumnarPlan(
  collections: readonly GeneratedClientCollectionDescriptor[],
  plan: BootstrapPlan,
): BoundColumnarPlan {
  if (collections.length === 0)
    throw new TypeError("the schema carries no generated collections");
  if (plan.artifacts.length !== 1 || plan.residentTables.length !== 0)
    throw invalid("bootstrap plan must serve exactly one columnar artifact");
  const artifact = plan.artifacts[0];
  if (!artifact) throw invalid("bootstrap plan has no artifact");
  // `parseBootstrapPlan` already refused every runtime format but
  // `directColumnar`, so there is one artifact runtime left to bind.
  const runtime = artifact.runtime;
  // The parser requires every required table to be covered, the resident
  // set is empty, and this is the only artifact; `checkCollections` adds
  // that the tables it carries are exactly the generated relations.
  checkCollections(collections, artifact, runtime);
  // ...and `requiredTables` adds that the server said so. An artifact that
  // happens to carry a table the plan did not require is no promise to keep
  // that table whole, so a plan that under- or over-states the generated set
  // is refused before any byte is fetched.
  if (
    !sameSet(
      plan.requiredTables,
      runtime.tables.map((table) => table.tableId),
    )
  )
    throw invalid(
      "bootstrap plan requires a table set that does not match the generated collections",
    );
  const trusted: TrustedArtifact = {
    byteLength: artifact.decodedBytes,
    digest: `sha256:${artifact.decodedSha256}`,
    identity: {
      sourceEpoch: plan.source.sourceEpoch,
      manifestVersion: runtime.identity.manifestVersion,
      collectionSetHash: runtime.collectionSetHash as `sha256:${string}`,
      schemaHash: runtime.schemaHash as `sha256:${string}`,
      authorizationScopeVersion: runtime.identity.authorizationScopeVersion,
      authorizationScopeHash: plan.authorization
        .scopeHash as `sha256:${string}`,
      cursor: artifact.snapshotCursor,
    },
    schema: runtime.schema,
    chunks: runtime.chunks,
  };
  return Object.freeze({
    artifact,
    trusted,
    contract: changeContract(plan, runtime),
  });
}
