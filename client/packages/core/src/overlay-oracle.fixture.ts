/**
 * The randomized differential oracle for the overlay.
 *
 * One generated action stream is driven against two implementations that share
 * no code: the two-plane `OverlayEngine` over a frozen `CALQART1` base, and
 * `SqliteReference`, which applies changes to real SQL tables with recorded
 * inverse programs. After every step both sides are fully materialized and
 * compared. A failure reports its seed and the shrunken step list.
 */
import {
  OverlayEngine,
  artifactBase,
  type CanonicalBatch,
  type CanonicalOperation,
  type CanonicalTransaction,
  type KeyPart,
  type OptimisticAction,
  type OptimisticMutation,
  type OverlayCollectionSchema,
  type SqlValue,
} from "./overlay.js";
import {
  SqliteReference,
  type ReferenceCollectionSchema,
} from "./sqlite-reference.fixture.js";
import { openDirectColumnarArtifact } from "./artifact.js";
import { encodeWorkspace, WORKSPACE_CONTRACT } from "./artifact.fixture.js";

// ------------------------------------------------------------------ random

/** mulberry32: small, seeded, and identical across runs. */
function rng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b_79f5) >>> 0;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

const pick = <T>(random: () => number, values: readonly T[]): T | undefined =>
  values.length === 0
    ? undefined
    : values[Math.floor(random() * values.length) % values.length];

const integer = (random: () => number, low: number, high: number) =>
  low + Math.floor(random() * (high - low + 1));

// ----------------------------------------------------------------- schemas

interface OracleColumn {
  readonly name: string;
  readonly affinity: "TEXT" | "REAL";
  readonly nullable: boolean;
  /** Eligible for `mode: "increment"`. */
  readonly numeric: boolean;
}

interface OracleCollection {
  readonly name: string;
  readonly columns: readonly OracleColumn[];
  readonly primaryKey: readonly number[];
}

const text = (name: string, nullable = false): OracleColumn => ({
  name,
  affinity: "TEXT",
  nullable,
  numeric: false,
});
const real = (name: string): OracleColumn => ({
  name,
  affinity: "REAL",
  nullable: false,
  numeric: true,
});

/**
 * Three of the fixture workspace's tables: a single-key collection with two
 * incrementable columns and a nullable one, a second single-key collection, and
 * a composite-key collection.
 */
export const ORACLE_COLLECTIONS: readonly OracleCollection[] = [
  {
    name: "issues",
    columns: [
      text("id"),
      text("organization_id"),
      text("project_id"),
      text("assignee_id", true),
      text("status"),
      real("priority"),
      text("title"),
      real("event_count"),
      text("updated_at"),
    ],
    primaryKey: [0],
  },
  {
    name: "comments",
    columns: [
      text("id"),
      text("issue_id"),
      text("author_id"),
      text("body"),
      text("created_at"),
    ],
    primaryKey: [0],
  },
  {
    name: "memberships",
    columns: [text("organization_id"), text("user_id"), text("role")],
    primaryKey: [0, 1],
  },
];

const referenceSchemas: readonly ReferenceCollectionSchema[] =
  ORACLE_COLLECTIONS.map((collection) => ({
    name: collection.name,
    columns: collection.columns.map((column) => ({
      name: column.name,
      affinity: column.affinity,
      nullable: column.nullable,
    })),
    primaryKey: collection.primaryKey,
  }));

// -------------------------------------------------------------- key coding

const encoder = new TextEncoder();

/**
 * Stands in for the canonical `PrimaryKey` protobuf bytes, which arrive already
 * encoded on the wire. The overlay only ever compares these for identity.
 */
export const keyBytesOf = (parts: readonly KeyPart[]): Uint8Array =>
  encoder.encode(JSON.stringify(parts));

const keyOf = (parts: readonly KeyPart[]) => JSON.stringify(parts);

// ------------------------------------------------------------------- base

const TIMESTAMPS = [
  "2026-08-01T00:00:00.000Z",
  "2026-08-02T01:02:03.000Z",
  "2026-08-03T04:05:06.000Z",
];

function seedWorkspace(random: () => number) {
  const issues = Array.from({ length: 14 }, (_unused, index) => ({
    id: `issue-${index}`,
    organization_id: `org-${index % 2}`,
    project_id: `project-${index % 3}`,
    assignee_id: index % 4 === 0 ? null : `user-${index % 5}`,
    status: pick(random, ["open", "closed", "triaged"]) ?? "open",
    priority: integer(random, 0, 9),
    title: `title-${index}`,
    event_count: integer(random, 0, 40),
    updated_at: TIMESTAMPS[index % TIMESTAMPS.length] ?? TIMESTAMPS[0],
  }));
  const comments = Array.from({ length: 10 }, (_unused, index) => ({
    id: `comment-${index}`,
    issue_id: `issue-${index % 14}`,
    author_id: `user-${index % 5}`,
    body: `body-${index}`,
    created_at: TIMESTAMPS[index % TIMESTAMPS.length] ?? TIMESTAMPS[0],
  }));
  const memberships = Array.from({ length: 8 }, (_unused, index) => ({
    organization_id: `org-${index % 2}`,
    user_id: `user-${index}`,
    role: pick(random, ["owner", "member", "viewer"]) ?? "member",
  }));
  return new Map<string, Array<Record<string, unknown>>>([
    ["events", []],
    ["issues", issues],
    ["comments", comments],
    ["issue_tags", []],
    ["users", []],
    ["projects", []],
    ["memberships", memberships],
  ]);
}

interface OraclePair {
  readonly overlay: OverlayEngine;
  readonly reference: SqliteReference;
  readonly base: ReadonlyMap<string, readonly SqlValue[][]>;
}

/**
 * Builds one real artifact, opens it, and seeds SQLite from the *decoded* base
 * so both sides start from byte-identical values.
 */
export async function buildPair(seed: number): Promise<OraclePair> {
  const encoded = encodeWorkspace(seedWorkspace(rng(seed)));
  const buffer = new ArrayBuffer(encoded.byteLength);
  new Uint8Array(buffer).set(encoded.bytes);
  const workspace = await openDirectColumnarArtifact(buffer, {
    byteLength: encoded.byteLength,
    digest: encoded.digest,
    identity: encoded.identity,
    schema: WORKSPACE_CONTRACT,
  });

  const schemas: OverlayCollectionSchema[] = [];
  const baseRows = new Map<string, SqlValue[][]>();
  for (const collection of ORACLE_COLLECTIONS) {
    const table = workspace.table(collection.name);
    const names = collection.columns.map((column) => column.name);
    schemas.push({
      name: collection.name,
      columns: names,
      base: artifactBase(table, names),
    });
    const accessors = names.map((name) => table.column(name));
    baseRows.set(
      collection.name,
      Array.from({ length: table.rows }, (_unused, row) =>
        accessors.map((column) => column.valueAt(row) as SqlValue),
      ),
    );
  }
  return {
    overlay: new OverlayEngine(schemas),
    reference: new SqliteReference(referenceSchemas, baseRows),
    base: baseRows,
  };
}

// ----------------------------------------------------------- materializing

type Materialized = ReadonlyMap<string, readonly string[]>;

function token(value: SqlValue): string {
  if (value === null) return "null";
  if (typeof value === "number")
    return `n:${Object.is(value, -0) ? "0" : String(value)}`;
  if (typeof value === "bigint") return `n:${String(value)}`;
  if (typeof value === "boolean") return `n:${value ? "1" : "0"}`;
  if (typeof value === "string") return `s:${value}`;
  return `b:${[...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

const rowToken = (row: readonly SqlValue[]) => row.map(token).join("");

export function materializeOverlay(engine: OverlayEngine): Materialized {
  const out = new Map<string, readonly string[]>();
  for (const collection of ORACLE_COLLECTIONS)
    out.set(collection.name, engine.rows(collection.name).map(rowToken).sort());
  return out;
}

export function materializeReference(reference: SqliteReference): Materialized {
  const out = new Map<string, readonly string[]>();
  for (const collection of ORACLE_COLLECTIONS)
    out.set(
      collection.name,
      reference.materialize(collection.name).map(rowToken).sort(),
    );
  return out;
}

export function diff(left: Materialized, right: Materialized): string | null {
  for (const collection of ORACLE_COLLECTIONS) {
    const a = left.get(collection.name) ?? [];
    const b = right.get(collection.name) ?? [];
    if (a.length !== b.length)
      return `${collection.name}: overlay has ${a.length} rows, sqlite has ${b.length}`;
    for (let index = 0; index < a.length; index++)
      if (a[index] !== b[index])
        return `${collection.name}[${index}]: overlay ${String(a[index])} != sqlite ${String(b[index])}`;
  }
  return null;
}

// ------------------------------------------------------------------- steps

export type Step =
  | { readonly kind: "canonical"; readonly batch: CanonicalBatch }
  | { readonly kind: "optimistic"; readonly mutation: OptimisticMutation }
  | { readonly kind: "reject"; readonly mutationId: string };

interface ModelRow {
  readonly key: readonly KeyPart[];
  readonly values: SqlValue[];
}

/**
 * The generator's own view of the *canonical* plane, used only to choose
 * targets whose preconditions hold on both sides. It never participates in an
 * assertion.
 */
class Model {
  readonly canonical = new Map<string, Map<string, ModelRow>>();
  /** Keys a surviving pending action requires to be present. */
  readonly requiredPresent = new Map<string, Set<string>>();
  /** Keys a surviving pending action requires to be absent canonically. */
  readonly requiredAbsent = new Map<string, Set<string>>();
  readonly merged = new Map<string, Set<string>>();
  readonly pendingKeys = new Map<string, Set<string>>();

  constructor(base: ReadonlyMap<string, readonly (readonly SqlValue[])[]>) {
    for (const collection of ORACLE_COLLECTIONS) {
      const rows = new Map<string, ModelRow>();
      for (const values of base.get(collection.name) ?? []) {
        const key = collection.primaryKey.map(
          (ordinal) => values[ordinal] as KeyPart,
        );
        rows.set(keyOf(key), { key, values: [...values] });
      }
      this.canonical.set(collection.name, rows);
      this.merged.set(collection.name, new Set(rows.keys()));
      this.requiredPresent.set(collection.name, new Set());
      this.requiredAbsent.set(collection.name, new Set());
      this.pendingKeys.set(collection.name, new Set());
    }
  }

  rows(collection: string): Map<string, ModelRow> {
    const rows = this.canonical.get(collection);
    if (!rows) throw new Error("unknown collection");
    return rows;
  }

  set(collection: string, name: "merged" | "present" | "absent" | "pending") {
    const map =
      name === "merged"
        ? this.merged
        : name === "present"
          ? this.requiredPresent
          : name === "absent"
            ? this.requiredAbsent
            : this.pendingKeys;
    const value = map.get(collection);
    if (!value) throw new Error("unknown collection");
    return value;
  }
}

interface GeneratedStream {
  readonly seed: number;
  readonly steps: readonly Step[];
}

const STATUSES = ["open", "closed", "triaged", "resolved"];
const ROLES = ["owner", "member", "viewer"];

function randomValue(
  random: () => number,
  collection: OracleCollection,
  ordinal: number,
  key: readonly KeyPart[],
): SqlValue {
  const column = collection.columns[ordinal];
  if (!column) throw new Error("unknown column");
  const keyIndex = collection.primaryKey.indexOf(ordinal);
  if (keyIndex >= 0) return key[keyIndex] as SqlValue;
  if (column.numeric) return integer(random, 0, 50);
  if (column.nullable && random() < 0.25) return null;
  if (column.name === "status") return pick(random, STATUSES) ?? "open";
  if (column.name === "role") return pick(random, ROLES) ?? "member";
  if (column.name === "updated_at" || column.name === "created_at")
    return `2026-09-0${integer(random, 1, 9)}T00:00:00.000000Z`;
  return `${column.name}-${integer(random, 0, 999)}`;
}

const fullRow = (
  random: () => number,
  collection: OracleCollection,
  key: readonly KeyPart[],
): SqlValue[] =>
  collection.columns.map((_unused, ordinal) =>
    randomValue(random, collection, ordinal, key),
  );

function freshKey(
  random: () => number,
  collection: OracleCollection,
): readonly KeyPart[] {
  const suffix = integer(random, 0, 99_999);
  return collection.primaryKey.map((_unused, index) =>
    index === 0
      ? `new-${collection.name}-${suffix}`
      : `part-${index}-${suffix}`,
  );
}

/**
 * Generates a stream whose preconditions hold on both implementations, so a
 * disagreement is never a differently reported precondition failure. It never
 * emits four shapes: a canonical insert of a key a pending insert already
 * claims, a canonical delete of a key a pending action requires, a truncate
 * under any pending action for that collection, and an optimistic action whose
 * target depends on another pending mutation's membership effect. The
 * adversarial suite covers those four directly.
 */
export function generateStream(
  seed: number,
  length: number,
  base: ReadonlyMap<string, readonly (readonly SqlValue[])[]>,
): GeneratedStream {
  const random = rng(seed);
  const model = new Model(base);
  const steps: Step[] = [];
  const pending: { id: string; actions: OptimisticAction[] }[] = [];
  let nextMutation = 0;

  const releaseGuards = () => {
    for (const collection of ORACLE_COLLECTIONS) {
      model.set(collection.name, "present").clear();
      model.set(collection.name, "absent").clear();
      model.set(collection.name, "pending").clear();
    }
    for (const entry of pending)
      for (const action of entry.actions) {
        const key = keyOf(action.key);
        model.set(action.collection, "pending").add(key);
        if (action.kind === "insert")
          model.set(action.collection, "absent").add(key);
        else model.set(action.collection, "present").add(key);
      }
  };

  const recomputeMerged = () => {
    for (const collection of ORACLE_COLLECTIONS)
      model.merged.set(
        collection.name,
        new Set(model.rows(collection.name).keys()),
      );
    for (const entry of pending)
      for (const action of entry.actions) {
        const live = model.set(action.collection, "merged");
        const key = keyOf(action.key);
        if (action.kind === "insert") live.add(key);
        else if (action.kind === "delete") live.delete(key);
      }
  };

  for (let step = 0; step < length; step++) {
    const roll = random();
    if (roll < 0.42) {
      // ---------------------------------------------------- canonical batch
      const acknowledged = new Set<string>();
      if (pending.length > 0 && random() < 0.4) {
        const victim = pick(random, pending);
        if (victim) acknowledged.add(victim.id);
      }
      for (let index = pending.length - 1; index >= 0; index--) {
        const entry = pending[index];
        if (entry && acknowledged.has(entry.id)) pending.splice(index, 1);
      }
      releaseGuards();
      recomputeMerged();

      const operations: CanonicalOperation[] = [];
      const opCount = integer(random, 1, 4);
      for (let index = 0; index < opCount; index++) {
        const collection = pick(random, ORACLE_COLLECTIONS);
        if (!collection) continue;
        const rows = model.rows(collection.name);
        const guardPresent = model.set(collection.name, "present");
        const guardAbsent = model.set(collection.name, "absent");
        const touched = model.set(collection.name, "pending");
        const choice = random();
        if (choice < 0.06 && touched.size === 0 && rows.size > 0) {
          operations.push({ kind: "truncate", collection: collection.name });
          rows.clear();
          continue;
        }
        if (choice < 0.4 || rows.size === 0) {
          const key = freshKey(random, collection);
          const id = keyOf(key);
          if (rows.has(id) || guardAbsent.has(id)) continue;
          const values = fullRow(random, collection, key);
          operations.push({
            kind: "insert",
            collection: collection.name,
            primaryKey: keyBytesOf(key),
            key,
            values,
          });
          rows.set(id, { key, values });
          continue;
        }
        const target = pick(random, [...rows.values()]);
        if (!target) continue;
        const id = keyOf(target.key);
        if (choice < 0.72) {
          const ordinals = collection.columns
            .map((_unused, ordinal) => ordinal)
            .filter((ordinal) => !collection.primaryKey.includes(ordinal));
          const chosen = ordinals.filter(() => random() < 0.4);
          const written = chosen.length > 0 ? chosen : ordinals.slice(0, 1);
          // Half the updates name every column, as pgoutput's new tuple does:
          // the key and each unwritten column carry the value the row holds.
          const changed =
            random() < 0.5
              ? collection.columns.map((_unused, ordinal) => ({
                  column: ordinal,
                  value: written.includes(ordinal)
                    ? randomValue(random, collection, ordinal, target.key)
                    : (target.values[ordinal] ?? null),
                }))
              : written.map((ordinal) => ({
                  column: ordinal,
                  value: randomValue(random, collection, ordinal, target.key),
                }));
          operations.push({
            kind: "update",
            collection: collection.name,
            primaryKey: keyBytesOf(target.key),
            key: target.key,
            changed,
          });
          for (const change of changed)
            target.values[change.column] = change.value;
          continue;
        }
        if (guardPresent.has(id) || touched.has(id)) continue;
        operations.push({
          kind: "delete",
          collection: collection.name,
          primaryKey: keyBytesOf(target.key),
          key: target.key,
        });
        rows.delete(id);
      }
      if (operations.length === 0 && acknowledged.size === 0) continue;
      const transactions: CanonicalTransaction[] = [
        { operations, mutationIds: [...acknowledged] },
      ];
      steps.push({ kind: "canonical", batch: { transactions } });
      recomputeMerged();
      continue;
    }

    if (roll < 0.85 && pending.length < 24) {
      // ------------------------------------------------ optimistic mutation
      const actions: OptimisticAction[] = [];
      const count = integer(random, 1, 3);
      for (let index = 0; index < count; index++) {
        const collection = pick(random, ORACLE_COLLECTIONS);
        if (!collection) continue;
        const rows = model.rows(collection.name);
        const live = model.set(collection.name, "merged");
        const choice = random();
        if (choice < 0.25) {
          const key = freshKey(random, collection);
          const id = keyOf(key);
          if (rows.has(id) || live.has(id)) continue;
          actions.push({
            kind: "insert",
            collection: collection.name,
            primaryKey: keyBytesOf(key),
            key,
            values: fullRow(random, collection, key),
          });
          live.add(id);
          continue;
        }
        // Optimistic patches and deletes target rows that exist *canonically*
        // and are still live in the merged view, so rejecting any mutation
        // cannot invalidate a survivor's precondition.
        const candidates = [...rows.values()].filter((row) =>
          live.has(keyOf(row.key)),
        );
        const target = pick(random, candidates);
        if (!target) continue;
        const id = keyOf(target.key);
        if (choice < 0.4) {
          actions.push({
            kind: "delete",
            collection: collection.name,
            primaryKey: keyBytesOf(target.key),
            key: target.key,
          });
          live.delete(id);
          continue;
        }
        const ordinals = collection.columns
          .map((_unused, ordinal) => ordinal)
          .filter((ordinal) => !collection.primaryKey.includes(ordinal));
        // Biased toward the incrementable columns: `mode: "increment"` is why
        // pending mutations must be refolded, so it must be well sampled.
        const chosen = ordinals.filter((ordinal) =>
          collection.columns[ordinal]?.numeric
            ? random() < 0.7
            : random() < 0.25,
        );
        const changes = (chosen.length > 0 ? chosen : ordinals.slice(0, 1)).map(
          (ordinal) => {
            const column = collection.columns[ordinal];
            return column?.numeric && random() < 0.8
              ? {
                  column: ordinal,
                  value: integer(random, -5, 5),
                  mode: "increment" as const,
                }
              : {
                  column: ordinal,
                  value: randomValue(random, collection, ordinal, target.key),
                  mode: "set" as const,
                };
          },
        );
        actions.push({
          kind: "patch",
          collection: collection.name,
          primaryKey: keyBytesOf(target.key),
          key: target.key,
          changes,
        });
      }
      if (actions.length === 0) continue;
      const mutationId = `m-${nextMutation++}`;
      pending.push({ id: mutationId, actions });
      releaseGuards();
      steps.push({ kind: "optimistic", mutation: { mutationId, actions } });
      continue;
    }

    // ----------------------------------------------------------- rejection
    const victim = pick(random, pending);
    if (!victim) continue;
    pending.splice(pending.indexOf(victim), 1);
    releaseGuards();
    recomputeMerged();
    steps.push({ kind: "reject", mutationId: victim.id });
  }
  return { seed, steps };
}

// ------------------------------------------------------------------ runner

interface Divergence {
  readonly index: number;
  readonly detail: string;
}

export function applyStepToOverlay(engine: OverlayEngine, step: Step): void {
  if (step.kind === "canonical") engine.applyCanonical(step.batch);
  else if (step.kind === "optimistic") engine.applyOptimistic(step.mutation);
  else engine.dropMutation(step.mutationId);
}

export function applyStepToReference(
  reference: SqliteReference,
  step: Step,
): void {
  if (step.kind === "canonical") reference.applyBatch(step.batch);
  else if (step.kind === "optimistic") reference.submitMutation(step.mutation);
  else reference.rejectMutation(step.mutationId);
}

/** Applies `steps` to both sides, comparing the full state after every one. */
export async function runSteps(
  seed: number,
  steps: readonly Step[],
): Promise<Divergence | null> {
  const { overlay, reference } = await buildPair(seed);
  try {
    const initial = diff(
      materializeOverlay(overlay),
      materializeReference(reference),
    );
    if (initial) return { index: -1, detail: `base: ${initial}` };
    for (const [index, step] of steps.entries()) {
      const overlayError = attempt(() => {
        applyStepToOverlay(overlay, step);
      });
      const referenceError = attempt(() => {
        applyStepToReference(reference, step);
      });
      if (overlayError !== null || referenceError !== null)
        return {
          index,
          detail: `overlay threw [${overlayError ?? "nothing"}]; sqlite threw [${referenceError ?? "nothing"}]`,
        };
      const detail = diff(
        materializeOverlay(overlay),
        materializeReference(reference),
      );
      if (detail) return { index, detail };
      const orderDetail = checkOrder(overlay);
      if (orderDetail) return { index, detail: orderDetail };
    }
    return null;
  } finally {
    reference.close();
  }
}

function attempt(action: () => void): string | null {
  try {
    action();
    return null;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
}

/**
 * The order vector must hold each live address once, and its length must be
 * the collection's row count.
 */
function checkOrder(engine: OverlayEngine): string | null {
  for (const collection of ORACLE_COLLECTIONS) {
    const view = engine.createView(collection.name);
    const seen = new Set<number>();
    for (const address of view.order) {
      if (seen.has(address))
        return `${collection.name}: order vector repeats address ${String(address)}`;
      seen.add(address);
    }
    if (view.order.length !== engine.rowCount(collection.name))
      return `${collection.name}: order vector length disagrees with the row count`;
  }
  return null;
}

/**
 * Delta-debugging: keep the minimal prefix that still diverges, then drop
 * individual earlier steps for as long as the divergence survives.
 */
export async function shrink(
  seed: number,
  steps: readonly Step[],
  index: number,
): Promise<readonly Step[]> {
  let best = steps.slice(0, index + 1);
  let progress = true;
  while (progress && best.length > 1) {
    progress = false;
    for (let position = best.length - 2; position >= 0; position--) {
      const candidate = [
        ...best.slice(0, position),
        ...best.slice(position + 1),
      ];
      if (await runSteps(seed, candidate)) {
        best = candidate;
        progress = true;
        break;
      }
    }
  }
  return best;
}

export function renderSteps(steps: readonly Step[]): string {
  return JSON.stringify(
    steps,
    (_key, value: unknown) =>
      value instanceof Uint8Array
        ? `0x${[...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`
        : typeof value === "bigint"
          ? `${String(value)}n`
          : value,
    2,
  );
}

// ---------------------------------------------------------------- coverage

/**
 * What the generator emitted. If a weight goes to zero or an early `continue`
 * swallows a case, every seed stays green while the suite stops testing that
 * delta kind, so the tests assert floors over this tally. The key union is
 * closed, so a new delta kind without a counter is a type error.
 */
export type CoverageKey =
  | "canonicalBatches"
  | "canonicalInsert"
  | "canonicalUpdate"
  | "canonicalFullTuple"
  | "canonicalDelete"
  | "canonicalTruncate"
  | "acknowledgements"
  | "optimisticMutations"
  | "optimisticInsert"
  | "optimisticPatch"
  | "optimisticDelete"
  | "patchSet"
  | "patchIncrement"
  | "rejections";

export type Coverage = Record<CoverageKey, number>;

export const emptyCoverage = (): Coverage => ({
  canonicalBatches: 0,
  canonicalInsert: 0,
  canonicalUpdate: 0,
  canonicalFullTuple: 0,
  canonicalDelete: 0,
  canonicalTruncate: 0,
  acknowledgements: 0,
  optimisticMutations: 0,
  optimisticInsert: 0,
  optimisticPatch: 0,
  optimisticDelete: 0,
  patchSet: 0,
  patchIncrement: 0,
  rejections: 0,
});

/** Accumulates one stream's tally into `into`, so seeds aggregate. */
export function tallyCoverage(
  steps: readonly Step[],
  into: Coverage,
): Coverage {
  for (const step of steps) {
    if (step.kind === "reject") {
      into.rejections++;
      continue;
    }
    if (step.kind === "canonical") {
      into.canonicalBatches++;
      for (const transaction of step.batch.transactions) {
        into.acknowledgements += transaction.mutationIds.length;
        for (const operation of transaction.operations) {
          if (operation.kind === "insert") into.canonicalInsert++;
          else if (operation.kind === "update") {
            into.canonicalUpdate++;
            const width = ORACLE_COLLECTIONS.find(
              ({ name }) => name === operation.collection,
            )?.columns.length;
            if (operation.changed.length === width) into.canonicalFullTuple++;
          } else if (operation.kind === "delete") into.canonicalDelete++;
          else into.canonicalTruncate++;
        }
      }
      continue;
    }
    into.optimisticMutations++;
    for (const action of step.mutation.actions) {
      if (action.kind === "insert") into.optimisticInsert++;
      else if (action.kind === "delete") into.optimisticDelete++;
      else {
        into.optimisticPatch++;
        for (const change of action.changes) {
          if (change.mode === "increment") into.patchIncrement++;
          else into.patchSet++;
        }
      }
    }
  }
  return into;
}
