/**
 * The differential oracle's other side: SQLite change application over
 * `node:sqlite`. It is a different mechanism from `overlay.ts`, destructive
 * writes into the tables reads come from with a recorded inverse program per
 * action, so the two can disagree. `applyBatch` undoes the pending actions,
 * applies the canonical batch, drops acknowledged mutations and replays the
 * survivors; `rejectMutation` undoes and replays. Each runs inside one
 * `BEGIN IMMEDIATE … COMMIT`.
 */
import { DatabaseSync } from "node:sqlite";

import type {
  CanonicalBatch,
  KeyPart,
  OptimisticAction,
  OptimisticMutation,
  SqlValue,
} from "./overlay.js";

type Affinity = "TEXT" | "INTEGER" | "REAL" | "BLOB";

interface ReferenceColumn {
  readonly name: string;
  readonly affinity: Affinity;
  readonly nullable: boolean;
}

export interface ReferenceCollectionSchema {
  readonly name: string;
  readonly columns: readonly ReferenceColumn[];
  /** Column ordinals, in the order key parts arrive. */
  readonly primaryKey: readonly number[];
}

class ReferenceError_ extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(`${code}: ${message}`);
    this.name = "ReferenceError_";
  }
}

type Bindable = null | number | bigint | string | Uint8Array;

interface KeyBinding {
  readonly sql: string;
  readonly values: readonly Bindable[];
}

type InverseAction =
  | {
      readonly kind: "delete";
      readonly collection: string;
      readonly key: KeyBinding;
    }
  | {
      readonly kind: "insert";
      readonly collection: string;
      readonly values: readonly Bindable[];
    }
  | {
      readonly kind: "restore";
      readonly collection: string;
      readonly key: KeyBinding;
      readonly values: readonly Bindable[];
    };

interface PendingMutation {
  readonly mutation: OptimisticMutation;
  inverses: readonly InverseAction[];
}

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const placeholders = (count: number) =>
  Array.from({ length: count }, () => "?").join(", ");

function bindable(value: SqlValue): Bindable {
  if (typeof value === "boolean") return value ? 1 : 0;
  return value;
}

function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined)
    throw new ReferenceError_("INVALID_REQUEST", "index");
  return value;
}

export class SqliteReference {
  readonly #db = new DatabaseSync(":memory:");
  readonly #schemas = new Map<string, ReferenceCollectionSchema>();
  #pending: PendingMutation[] = [];

  constructor(
    schemas: readonly ReferenceCollectionSchema[],
    seed: ReadonlyMap<string, readonly (readonly SqlValue[])[]>,
  ) {
    for (const schema of schemas) {
      this.#schemas.set(schema.name, schema);
      const columns = schema.columns
        .map(
          (column) =>
            `${quote(column.name)} ${column.affinity}${column.nullable ? "" : " NOT NULL"}`,
        )
        .join(", ");
      const key = schema.primaryKey
        .map((ordinal) => quote(at(schema.columns, ordinal).name))
        .join(", ");
      this.#db.exec(
        `CREATE TABLE ${quote(schema.name)} (${columns}, PRIMARY KEY (${key}))`,
      );
      const rows = seed.get(schema.name) ?? [];
      if (rows.length > 0) {
        const statement = this.#db.prepare(
          `INSERT INTO ${quote(schema.name)} VALUES (${placeholders(schema.columns.length)})`,
        );
        for (const row of rows) statement.run(...row.map(bindable));
      }
    }
  }

  close(): void {
    this.#db.close();
  }

  pendingMutationIds(): readonly string[] {
    return this.#pending.map((entry) => entry.mutation.mutationId);
  }

  /** Unordered; the harness sorts both sides identically. */
  materialize(collection: string): SqlValue[][] {
    const schema = this.#requireSchema(collection);
    const names = schema.columns.map((column) => quote(column.name)).join(", ");
    const rows = this.#db
      .prepare(`SELECT ${names} FROM ${quote(collection)}`)
      .all() as Record<string, SqlValue>[];
    return rows.map((row) =>
      schema.columns.map((column) => row[column.name] ?? null),
    );
  }

  // ------------------------------------------------------------ the shape

  applyBatch(batch: CanonicalBatch): readonly string[] {
    const next = this.#pending.map((entry) => ({ ...entry }));
    const acknowledged = new Set<string>();
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      this.#undoPending(this.#pending);
      for (const transaction of batch.transactions) {
        for (const operation of transaction.operations)
          this.#applyOperation(operation);
        const ids = new Set(transaction.mutationIds);
        if (ids.size !== transaction.mutationIds.length)
          throw new ReferenceError_(
            "CHANGE_STREAM_INVALID",
            "duplicate mutation acknowledgements",
          );
        for (const id of ids) acknowledged.add(id);
        for (let index = next.length - 1; index >= 0; index--) {
          const candidate = next[index];
          if (candidate && ids.has(candidate.mutation.mutationId))
            next.splice(index, 1);
        }
      }
      this.#replayPending(next);
      this.#db.exec("COMMIT");
    } catch (error) {
      this.#rollback();
      throw error;
    }
    this.#pending = next;
    return [...acknowledged].sort();
  }

  submitMutation(mutation: OptimisticMutation): void {
    this.#db.exec("BEGIN IMMEDIATE");
    let inverses: readonly InverseAction[];
    try {
      inverses = this.#applyMutationActions(mutation.actions);
      this.#db.exec("COMMIT");
    } catch (error) {
      this.#rollback();
      throw error;
    }
    this.#pending.push({ mutation, inverses });
  }

  rejectMutation(mutationId: string): void {
    const rejected = this.#pending.find(
      (entry) => entry.mutation.mutationId === mutationId,
    );
    if (!rejected)
      throw new ReferenceError_("INVALID_REQUEST", "mutation is not pending");
    const next = this.#pending
      .filter((entry) => entry !== rejected)
      .map((entry) => ({ ...entry }));
    this.#db.exec("BEGIN IMMEDIATE");
    try {
      this.#undoPending(this.#pending);
      this.#replayPending(next);
      this.#db.exec("COMMIT");
    } catch (error) {
      this.#rollback();
      throw error;
    }
    this.#pending = next;
  }

  #rollback(): void {
    try {
      this.#db.exec("ROLLBACK");
    } catch {
      // The transaction was already unwound by the failing statement.
    }
  }

  // ------------------------------------------------------------- canonical

  #applyOperation(
    operation: CanonicalBatch["transactions"][number]["operations"][number],
  ): void {
    const schema = this.#requireSchema(operation.collection);
    const target = quote(schema.name);
    if (operation.kind === "truncate") {
      this.#db.exec(`DELETE FROM ${target}`);
      return;
    }
    if (operation.kind === "insert") {
      this.#db
        .prepare(
          `INSERT INTO ${target} VALUES (${placeholders(schema.columns.length)})`,
        )
        .run(...operation.values.map(bindable));
      return;
    }
    const key = this.#keyBinding(schema, operation.key);
    if (operation.kind === "delete") {
      const result = this.#db
        .prepare(`DELETE FROM ${target} WHERE ${key.sql}`)
        .run(...key.values);
      this.#requireOneChangedRow("delete", schema.name, result.changes);
      return;
    }
    const assignments = operation.changed
      .map((change) => `${quote(at(schema.columns, change.column).name)} = ?`)
      .join(", ");
    const result = this.#db
      .prepare(`UPDATE ${target} SET ${assignments} WHERE ${key.sql}`)
      .run(
        ...operation.changed.map((change) => bindable(change.value)),
        ...key.values,
      );
    this.#requireOneChangedRow("update", schema.name, result.changes);
  }

  // -------------------------------------------------------------- pending

  #applyMutationActions(
    actions: readonly OptimisticAction[],
  ): readonly InverseAction[] {
    const inverses: InverseAction[] = [];
    for (const action of actions) {
      const schema = this.#requireSchema(action.collection);
      const target = quote(schema.name);
      if (action.kind === "insert") {
        this.#db
          .prepare(
            `INSERT INTO ${target} VALUES (${placeholders(schema.columns.length)})`,
          )
          .run(...action.values.map(bindable));
        inverses.push({
          kind: "delete",
          collection: schema.name,
          key: this.#keyBinding(schema, action.key),
        });
        continue;
      }
      const key = this.#keyBinding(schema, action.key);
      const before = this.#selectExactRow(schema, key);
      if (action.kind === "delete") {
        const result = this.#db
          .prepare(`DELETE FROM ${target} WHERE ${key.sql}`)
          .run(...key.values);
        this.#requireOneChangedRow(
          "optimistic delete",
          schema.name,
          result.changes,
        );
        inverses.push({
          kind: "insert",
          collection: schema.name,
          values: before,
        });
        continue;
      }
      if (action.changes.length === 0)
        throw new ReferenceError_(
          "INVALID_REQUEST",
          "optimistic patch must change at least one column",
        );
      const seen = new Set<number>();
      const assignments: string[] = [];
      const bindings: Bindable[] = [];
      for (const change of action.changes) {
        const column = at(schema.columns, change.column);
        if (
          seen.has(change.column) ||
          schema.primaryKey.includes(change.column)
        )
          throw new ReferenceError_(
            "INVALID_REQUEST",
            "optimistic patch has duplicate or primary-key columns",
          );
        seen.add(change.column);
        if (change.mode === "increment") {
          if (
            typeof change.value !== "number" &&
            typeof change.value !== "bigint"
          )
            throw new ReferenceError_(
              "INVALID_REQUEST",
              "optimistic increment requires a numeric value",
            );
          assignments.push(`${quote(column.name)} = ${quote(column.name)} + ?`);
        } else {
          assignments.push(`${quote(column.name)} = ?`);
        }
        bindings.push(bindable(change.value));
      }
      const result = this.#db
        .prepare(
          `UPDATE ${target} SET ${assignments.join(", ")} WHERE ${key.sql}`,
        )
        .run(...bindings, ...key.values);
      this.#requireOneChangedRow(
        "optimistic patch",
        schema.name,
        result.changes,
      );
      inverses.push({
        kind: "restore",
        collection: schema.name,
        key,
        values: before,
      });
    }
    return inverses;
  }

  #selectExactRow(
    schema: ReferenceCollectionSchema,
    key: KeyBinding,
  ): readonly Bindable[] {
    const names = schema.columns.map((column) => quote(column.name)).join(", ");
    const rows = this.#db
      .prepare(`SELECT ${names} FROM ${quote(schema.name)} WHERE ${key.sql}`)
      .all(...key.values) as Record<string, Bindable>[];
    const row = rows[0];
    if (rows.length !== 1 || !row)
      throw new ReferenceError_(
        "INVALID_REQUEST",
        "optimistic mutation target must match exactly one row",
      );
    return schema.columns.map((column) => row[column.name] ?? null);
  }

  #undoPending(pending: readonly PendingMutation[]): void {
    for (let index = pending.length - 1; index >= 0; index--) {
      const entry = pending[index];
      if (!entry) continue;
      for (
        let inverseIndex = entry.inverses.length - 1;
        inverseIndex >= 0;
        inverseIndex--
      ) {
        const inverse = entry.inverses[inverseIndex];
        if (inverse) this.#applyInverse(inverse);
      }
    }
  }

  #applyInverse(inverse: InverseAction): void {
    const schema = this.#requireSchema(inverse.collection);
    const target = quote(schema.name);
    if (inverse.kind === "delete") {
      const result = this.#db
        .prepare(`DELETE FROM ${target} WHERE ${inverse.key.sql}`)
        .run(...inverse.key.values);
      this.#requireOneChangedRow(
        "optimistic undo delete",
        schema.name,
        result.changes,
      );
      return;
    }
    if (inverse.kind === "insert") {
      this.#db
        .prepare(
          `INSERT INTO ${target} VALUES (${placeholders(inverse.values.length)})`,
        )
        .run(...inverse.values);
      return;
    }
    const assignments = schema.columns
      .map((column) => `${quote(column.name)} = ?`)
      .join(", ");
    const result = this.#db
      .prepare(`UPDATE ${target} SET ${assignments} WHERE ${inverse.key.sql}`)
      .run(...inverse.values, ...inverse.key.values);
    this.#requireOneChangedRow(
      "optimistic undo restore",
      schema.name,
      result.changes,
    );
  }

  #replayPending(pending: PendingMutation[]): void {
    for (const entry of pending)
      entry.inverses = this.#applyMutationActions(entry.mutation.actions);
  }

  // --------------------------------------------------------------- helpers

  #requireSchema(name: string): ReferenceCollectionSchema {
    const schema = this.#schemas.get(name);
    if (!schema)
      throw new ReferenceError_(
        "CHANGE_STREAM_INVALID",
        `unknown table ${name}`,
      );
    return schema;
  }

  #keyBinding(
    schema: ReferenceCollectionSchema,
    key: readonly KeyPart[],
  ): KeyBinding {
    if (key.length !== schema.primaryKey.length)
      throw new ReferenceError_("CHANGE_STREAM_INVALID", "invalid primary key");
    return {
      sql: schema.primaryKey
        .map((ordinal) => `${quote(at(schema.columns, ordinal).name)} = ?`)
        .join(" AND "),
      values: key.map((part) => bindable(part)),
    };
  }

  #requireOneChangedRow(
    operation: string,
    collection: string,
    changed: number | bigint,
  ): void {
    if (Number(changed) !== 1)
      throw new ReferenceError_(
        "RESNAPSHOT_REQUIRED",
        `${operation} for ${collection} matched ${String(changed)} rows instead of exactly one`,
      );
  }
}
