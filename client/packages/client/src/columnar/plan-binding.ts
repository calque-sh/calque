import type {
  ColumnarEnumLabels,
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
 * generated file predates the served schema, fails with `SCHEMA_MISMATCH` and
 * recovery `none`; one that contradicts itself fails with `INVALID_RESPONSE`.
 * The artifact may serve more tables than the collections read: the engine
 * installs only the bound tables, the change contract names the rest as
 * ignored, and every byte of the artifact is still verified.
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
  /**
   * Each enum column's labels in declaration order, which the server reads
   * when it starts, so a value added since the bindings were generated still
   * orders as PostgreSQL orders it.
   */
  readonly enumLabels: readonly ColumnarEnumLabels[];
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
 * plan must serve every generated relation, each collection must map to an
 * artifact table of the same name, carry the same columns with compatible
 * kinds, and share the primary key. Tables the artifact serves beyond the
 * generated relations are allowed. Returns the plan table each collection
 * binds to, in the collections' order.
 */
function checkCollections(
  collections: readonly GeneratedClientCollectionDescriptor[],
  artifact: PlanArtifact,
  runtime: PlanArtifactColumnarRuntime,
): readonly PlanArtifactTable[] {
  const drift = (detail: string): SyncError =>
    new SyncError(
      "SCHEMA_MISMATCH",
      "sync",
      "none",
      `the generated bindings do not match what the server serves: ${detail}; this page was built from an older configuration, so load a build generated from the calque.config.json the server runs`,
    );
  const served = new Map(
    runtime.tables.map((table) => [relationOf(table), table] as const),
  );
  if (served.size !== runtime.tables.length)
    throw invalid("the plan serves one relation twice");
  const unserved = collectionRelations(collections).filter(
    (relation) => !served.has(relation),
  );
  if (unserved.length > 0)
    throw drift(`the server does not serve ${unserved.join(", ")}`);
  return collections.map((collection) => {
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
    return planTable;
  });
}

/**
 * The change contract the plan's wire column ids compile to. Every plan names
 * a change stream, so a columnar plan whose bound tables carry no wire ids is
 * one the client could open but never keep current; it is refused before any
 * byte is fetched rather than opened frozen. The stream names every table the
 * artifact serves, so the tables no collection reads are listed as ignored.
 */
function changeContract(
  plan: BootstrapPlan,
  runtime: PlanArtifactColumnarRuntime,
  bound: readonly PlanArtifactTable[],
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
    ignoredTableIds: runtime.tables
      .filter((table) => !bound.includes(table))
      .map((table) => table.tableId),
    tables: bound.map((table) => {
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
 * requires the plan to serve and name as required.
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
  // that it serves every generated relation.
  const bound = checkCollections(collections, artifact, runtime);
  // ...and `requiredTables` adds that the server said so: a plan that under-
  // or over-states what the page asked for is refused before any byte is
  // fetched. The artifact may carry tables beyond them; the engine ignores
  // those.
  if (
    !sameSet(
      plan.requiredTables,
      bound.map((table) => table.tableId),
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
    contract: changeContract(plan, runtime, bound),
    enumLabels: bound.flatMap((table) =>
      (table.columns ?? []).flatMap(({ name, enumLabels }) =>
        enumLabels === undefined
          ? []
          : [
              {
                collection: table.localTable,
                column: name,
                labels: enumLabels,
              },
            ],
      ),
    ),
  });
}

/** What a mount's plans keep fixed across renewals. */
export interface RenewablePlan {
  readonly source: BootstrapPlan["source"];
  readonly scopeHash: string;
  /**
   * The wire ids of the tables the mount holds, which the change stream
   * names: every table the artifact served.
   */
  readonly tableIds: readonly string[];
  /** The wire ids of the tables the page asked for. */
  readonly requiredTableIds: readonly string[];
}

export function renewablePlan(plan: BootstrapPlan): RenewablePlan {
  return Object.freeze({
    source: plan.source,
    scopeHash: plan.authorization.scopeHash,
    tableIds: Object.freeze([
      ...plan.residentTables,
      ...plan.artifacts.flatMap((artifact) => artifact.tables),
    ]),
    requiredTableIds: Object.freeze([...plan.requiredTables]),
  });
}

/**
 * Checks that `plan` renews `held` from `cursor`: it installs nothing, names
 * the same source, generations, scope and tables, and continues the change
 * stream from the cursor the request presented. Anything else throws the
 * INVALID_RESPONSE the renewal gives up on.
 */
export function checkRenewal(
  held: RenewablePlan,
  plan: BootstrapPlan,
  cursor: string,
): void {
  const { source } = plan;
  if (plan.artifacts.length !== 0)
    throw invalid("a renewal plan must install no artifact");
  if (
    source.sourceEpoch !== held.source.sourceEpoch ||
    source.publicationGeneration !== held.source.publicationGeneration ||
    source.schemaGeneration !== held.source.schemaGeneration ||
    source.authorizationGeneration !== held.source.authorizationGeneration ||
    source.authorizationPolicyHash !== held.source.authorizationPolicyHash ||
    plan.authorization.scopeHash !== held.scopeHash
  )
    throw invalid("a renewal plan must keep the source and scope it renews");
  if (
    !sameSet(plan.requiredTables, held.requiredTableIds) ||
    !sameSet(plan.residentTables, held.tableIds)
  )
    throw invalid("a renewal plan must keep the tables it renews");
  if (
    plan.residentCursor !== cursor ||
    plan.activationCursor !== cursor ||
    plan.changeStream.fromCursorExclusive !== cursor
  )
    throw invalid(
      "a renewal plan must continue from the cursor the client presented",
    );
}
