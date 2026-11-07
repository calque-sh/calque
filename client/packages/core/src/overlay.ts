/**
 * The immutable base and its single mutable overlay.
 *
 * A collection is a frozen artifact base plus one overlay with two planes: a
 * canonical plane written only by CDC operations and merged in place, and a
 * pending plane holding the deltas of unconfirmed optimistic mutations. A read
 * observes `base ⊕ canonical ⊕ pending`, in that order. The base is never
 * patched and the canonical plane is never unwound.
 *
 * The canonical plane holds state rather than a log: a row updated fifty times
 * holds one entry. Unwinding a mutation is a list removal followed by a refold,
 * so there is no inverse program that could fail to apply.
 *
 * A plane holds no row objects. It is a `PlaneStore` of parallel typed arrays
 * indexed by slot, and a row costs the columns it touched rather than the
 * collection's width.
 */
import {
  artifactQueryVector,
  equalBytes,
  type ArtifactBlockCompare,
  type ArtifactEncodedColumn,
  type ArtifactQueryVector,
  type ArtifactTable,
} from "./artifact.js";

/** A slot no delta has written. Reads fall through it to the plane beneath. */
export const UNSET: unique symbol = Symbol("unset");
type Unset = typeof UNSET;

export type SqlValue = null | boolean | number | bigint | string | Uint8Array;

type OverlayQueryJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly OverlayQueryJsonValue[]
  | { readonly [key: string]: OverlayQueryJsonValue };

/** What a query reads from a cell: a stored value, or a parsed `json` value. */
export type OverlayQueryValue =
  | SqlValue
  | readonly OverlayQueryJsonValue[]
  | { readonly [key: string]: OverlayQueryJsonValue };

/** A decoded primary-key part, as `ArtifactTable.lookupKey` accepts it. */
export type KeyPart = string | number | bigint | boolean | Uint8Array;

/** The immutable half. `valueAt` takes a column ordinal, never a name. */
export interface OverlayBase {
  readonly rows: number;
  lookupKey(key: readonly KeyPart[]): number;
  valueAt(column: number, row: number): SqlValue;
}

export interface OverlayCollectionSchema {
  readonly name: string;
  /** Column ordinals are indices into this array. */
  readonly columns: readonly string[];
  readonly base: OverlayBase;
  /**
   * The collection's primary key by column name. The planner sizes an equijoin
   * on this key at one right row per left row; without it the join is planned
   * as unbounded.
   */
  readonly primaryKey?: readonly string[];
}

export type OverlayErrorCode =
  | "RESNAPSHOT_REQUIRED"
  | "VIEW_STALE"
  | "CHANGE_STREAM_INVALID"
  | "INVALID_REQUEST"
  | "CAPACITY_EXCEEDED";

export class OverlayError extends Error {
  constructor(
    readonly code: OverlayErrorCode,
    message: string,
  ) {
    super(`${code}: ${message}`);
    this.name = "OverlayError";
  }
}

/** What an insert, update and delete share: the collection and row key. */
export interface CanonicalKeyedOperation {
  readonly collection: string;
  /** Canonical `PrimaryKey` bytes. */
  readonly primaryKey: Uint8Array;
  /** The same key, decoded, for `base.lookupKey`. Translated once per delta. */
  readonly key: readonly KeyPart[];
}

export interface CanonicalInsert extends CanonicalKeyedOperation {
  readonly kind: "insert";
  /** A complete row image: one value per column ordinal. */
  readonly values: readonly SqlValue[];
}

export interface CanonicalUpdate extends CanonicalKeyedOperation {
  readonly kind: "update";
  readonly changed: readonly ColumnChange[];
}

export interface CanonicalDelete extends CanonicalKeyedOperation {
  readonly kind: "delete";
}

export interface CanonicalTruncate {
  readonly kind: "truncate";
  readonly collection: string;
}

export interface ColumnChange {
  readonly column: number;
  readonly value: SqlValue;
}

export type CanonicalOperation =
  CanonicalInsert | CanonicalUpdate | CanonicalDelete | CanonicalTruncate;

export interface CanonicalTransaction {
  readonly operations: readonly CanonicalOperation[];
  /** Mutation ids this transaction acknowledges. */
  readonly mutationIds: readonly string[];
}

export interface CanonicalBatch {
  readonly transactions: readonly CanonicalTransaction[];
}

export type PatchMode = "set" | "increment";

export interface OptimisticChange {
  readonly column: number;
  readonly value: SqlValue;
  readonly mode: PatchMode;
}

export interface OptimisticInsert extends CanonicalKeyedOperation {
  readonly kind: "insert";
  readonly values: readonly SqlValue[];
}

export interface OptimisticDelete extends CanonicalKeyedOperation {
  readonly kind: "delete";
}

export interface OptimisticPatch extends CanonicalKeyedOperation {
  readonly kind: "patch";
  readonly changes: readonly OptimisticChange[];
}

export type OptimisticAction =
  OptimisticInsert | OptimisticDelete | OptimisticPatch;

export interface OptimisticMutation {
  readonly mutationId: string;
  readonly actions: readonly OptimisticAction[];
}

export interface OverlayView {
  readonly collection: string;
  readonly generation: number;
  readonly order: Uint32Array;
}

/**
 * Exported for `query.ts` and kept out of the package entry point, because it
 * exposes the overlay's address space to whoever holds it.
 */
export interface OverlayQuerySnapshot {
  readonly collection: string;
  readonly columns: readonly string[];
  readonly primaryKey: readonly string[] | undefined;
  readonly generation: number;
  readonly order: Uint32Array;
  readonly baseRows: number;
  valueAt(address: number, column: number): OverlayQueryValue;
  compareEncodedAt(
    address: number,
    column: number,
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: SqlValue,
  ): boolean | undefined;
  prepareEncodedComparison(
    column: number,
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: SqlValue,
  ): ((address: number) => boolean | undefined) | undefined;
  /**
   * `prepareEncodedComparison` over a block of addresses, with the contract of
   * `ArtifactQueryVector.prepareBlockCompare`: a position reads 1, and counts
   * in the return, wherever the comparison would answer `undefined`.
   */
  prepareEncodedBlock(
    column: number,
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: SqlValue,
  ): ArtifactBlockCompare | undefined;
  /**
   * The base's encoded cells for one column, or `undefined` when it has none
   * or the collection is truncated. They answer for an address below
   * `baseRows` unless `dirty` says the overlay changed that cell.
   */
  baseEncoded(column: number): OverlayBaseEncoded | undefined;
  baseChunkRows(column: number): number | undefined;
  baseChunkMayMatch(
    column: number,
    chunk: number,
    operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
    literal: SqlValue,
  ): boolean | undefined;
  statisticsBytes(): number;
  statisticsCapacityBytes(): number;
  statisticsRowsScanned(): number;
}

interface OverlayBaseEncoded {
  readonly cells: ArtifactEncodedColumn;
  /**
   * Whether the overlay changed the base cell at an address, or `undefined`
   * when it changed no base cell of this column and no row needs asking.
   */
  readonly dirty: ((address: number) => boolean) | undefined;
}

interface OverlayBaseQueryVectors {
  readonly columns: readonly (ArtifactQueryVector | undefined)[];
}

const BASE_QUERY_VECTORS = new WeakMap<OverlayBase, OverlayBaseQueryVectors>();

export interface CanonicalBatchResult {
  readonly acknowledgedMutationIds: readonly string[];
  readonly changedCollections: readonly string[];
}

/** How many mutations may be pending, and how many actions each may carry. */
export const MAX_PENDING_MUTATIONS = 256;
export const MAX_OPTIMISTIC_ACTIONS = 256;

/**
 * Per touched row, whatever the width: the `Map<address, slot>` entry and one
 * slot in each of a `PlaneStore`'s three typed arrays, rounded up so the budget
 * errs high. Cells are charged by the two constants below.
 */
const OVERLAY_ROW_FIXED_BYTES = 42;

/**
 * Per cell in a sparse run: a slot in the shared value slab and its column
 * ordinal in a `Uint16Array`.
 */
const OVERLAY_SPARSE_CELL_BYTES = 12;

/**
 * Per cell in a dense run, whose capacity reached `width`, so cell `i` is
 * column `i` and no ordinal is stored. A full row image is dense.
 */
const OVERLAY_DENSE_CELL_BYTES = 9;

/**
 * The overlay's memory budget. Exhausting it raises `RESNAPSHOT_REQUIRED` under
 * its own reason, so an operator can tell it from a retention gap.
 *
 * It fits a bulk write that changes one column of every row of a 10^6-row,
 * 20-column base. It is no larger because the overlay shares the tab with the
 * base artifact and the application: a resnapshot recovers, an out-of-memory
 * tab crash does not. `OverlayOptions.budgetBytes` overrides it.
 */
export const OVERLAY_BUDGET_BYTES = 256 * 1024 * 1024;

/**
 * Addresses at or above this select the pending plane's `inserted` array. An
 * address names a row of `base ∪ inserted`; with two planes each owning an
 * `inserted` array the top bit selects which one, keeping an address a plain
 * `u32` and resolution `O(1)`.
 */
const PENDING_INSERT_BASE = 0x8000_0000;

/**
 * Column ordinals live in a `Uint16Array`. PostgreSQL stops at 1,600 columns,
 * but the bound is checked rather than assumed.
 */
const MAX_COLUMNS = 0xffff;

const HEX = Array.from({ length: 256 }, (_unused, value) =>
  value.toString(16).padStart(2, "0"),
);

function keyHex(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += HEX[byte] ?? "";
  return out;
}

function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new OverlayError("INVALID_REQUEST", "index");
  return value;
}

// --------------------------------------------------------------- cell store

/**
 * Capacity classes for a sparse cell run. A larger run becomes dense, `width`
 * slots with cell `i` holding column `i`, so a read scans at most eight cells
 * to find a column.
 */
const SPARSE_CAPACITIES = [1, 2, 4, 8] as const;

/**
 * The capacity a run holding `count` cells has. Monotone in `count`, so a row's
 * capacity is a function of its count and need not be stored: a run only ever
 * grows, and a row that stops growing is freed whole.
 */
function capacityFor(count: number, width: number): number {
  if (count === 0) return 0;
  for (const capacity of SPARSE_CAPACITIES)
    if (count <= capacity && capacity < width) return capacity;
  return width;
}

function sparseClass(capacity: number): number {
  const index = SPARSE_CAPACITIES.indexOf(
    capacity as (typeof SPARSE_CAPACITIES)[number],
  );
  if (index < 0)
    throw new OverlayError("INVALID_REQUEST", "not a sparse capacity");
  return index;
}

/**
 * Reads one slab slot. `??` would be wrong here: a delta can write SQL `NULL`,
 * and `null ?? UNSET` would turn that write into no write. Only an out-of-range
 * read yields `undefined`.
 */
function cell(
  slab: readonly (SqlValue | Unset)[],
  index: number,
): SqlValue | Unset {
  const value = slab[index];
  return value === undefined ? UNSET : value;
}

const EMPTY_U8 = new Uint8Array(0);
const EMPTY_U16 = new Uint16Array(0);
const EMPTY_U32 = new Uint32Array(0);

const SLOT_SEED = 16;

/**
 * One plane's rows, stored columnar. A row is a dense slot number into parallel
 * typed arrays, and its written cells live in one of two shared slabs. A row
 * stores no key: the plane's `patched` map and `inserted` list address it.
 *
 * Slots are never freed one at a time. The pending plane is cleared on every
 * fold and the canonical plane on a truncate, so `reset()` is the only
 * deallocation.
 */
class PlaneStore {
  readonly #width: number;

  #slots = 0;
  #slotCapacity = 0;
  #tombstone = EMPTY_U8;
  #cellStart = EMPTY_U32;
  #cellCount = EMPTY_U32;

  /** Sparse runs: capacity 1, 2, 4 or 8 cells, each carrying its ordinal. */
  #sparseValues: (SqlValue | Unset)[] = [];
  #sparseColumns = EMPTY_U16;
  #sparseUsed = 0;
  readonly #sparseFree: number[][] = SPARSE_CAPACITIES.map(() => []);

  /** Dense runs: exactly `width` cells, cell `i` being column `i`. */
  #denseValues: (SqlValue | Unset)[] = [];
  #denseUsed = 0;
  readonly #denseFree: number[] = [];

  constructor(width: number) {
    this.#width = width;
  }

  get slotCount(): number {
    return this.#slots;
  }

  /**
   * What this plane costs against the budget: a fixed charge per row and a
   * charge per allocated cell. `O(1)`, because `#assertBudget` runs on every
   * apply.
   */
  get bytes(): number {
    return (
      this.#slots * OVERLAY_ROW_FIXED_BYTES +
      this.#sparseUsed * OVERLAY_SPARSE_CELL_BYTES +
      this.#denseUsed * OVERLAY_DENSE_CELL_BYTES
    );
  }

  allocateSlot(): number {
    const slot = this.#slots;
    this.#reserve(slot + 1);
    this.#tombstone[slot] = 0;
    this.#cellStart[slot] = 0;
    this.#cellCount[slot] = 0;
    this.#slots = slot + 1;
    return slot;
  }

  tombstoneAt(slot: number): boolean {
    return this.#tombstone[slot] === 1;
  }

  setTombstone(slot: number, value: boolean): void {
    this.#tombstone[slot] = value ? 1 : 0;
  }

  /** `UNSET` when no delta in this plane wrote the column. */
  cellAt(slot: number, column: number): SqlValue | Unset {
    const count = this.#cellCount[slot] ?? 0;
    if (count === 0) return UNSET;
    const start = this.#cellStart[slot] ?? 0;
    if (count === this.#width) return cell(this.#denseValues, start + column);
    for (let index = 0; index < count; index++)
      if (this.#sparseColumns[start + index] === column)
        return cell(this.#sparseValues, start + index);
    return UNSET;
  }

  /** Calls `visit` with each column this plane wrote for the row. */
  writtenColumns(slot: number, visit: (column: number) => void): void {
    const count = this.#cellCount[slot] ?? 0;
    if (count === 0) return;
    const start = this.#cellStart[slot] ?? 0;
    if (count === this.#width) {
      for (let column = 0; column < count; column++)
        if (cell(this.#denseValues, start + column) !== UNSET) visit(column);
      return;
    }
    for (let index = 0; index < count; index++)
      visit(this.#sparseColumns[start + index] ?? 0);
  }

  /**
   * Writes one cell, growing the row's run when it names a column the row has
   * not touched. `UNSET` is never written: an unwritten column is the absence
   * of a cell, unlike a column written to SQL `NULL`.
   */
  setCell(slot: number, column: number, value: SqlValue): void {
    const width = this.#width;
    const count = this.#cellCount[slot] ?? 0;
    const start = this.#cellStart[slot] ?? 0;

    if (count === width && width > 0) {
      this.#denseValues[start + column] = value;
      return;
    }
    for (let index = 0; index < count; index++)
      if (this.#sparseColumns[start + index] === column) {
        this.#sparseValues[start + index] = value;
        return;
      }

    const capacity = capacityFor(count, width);
    const grown = capacityFor(count + 1, width);
    if (grown === capacity) {
      this.#sparseColumns[start + count] = column;
      this.#sparseValues[start + count] = value;
      this.#cellCount[slot] = count + 1;
      return;
    }
    if (grown === width) {
      const dense = this.#allocateDense();
      for (let index = 0; index < count; index++)
        this.#denseValues[dense + (this.#sparseColumns[start + index] ?? 0)] =
          cell(this.#sparseValues, start + index);
      this.#denseValues[dense + column] = value;
      if (count > 0) this.#freeSparse(start, capacity);
      this.#cellStart[slot] = dense;
      this.#cellCount[slot] = width;
      return;
    }
    const moved = this.#allocateSparse(grown);
    for (let index = 0; index < count; index++) {
      this.#sparseColumns[moved + index] =
        this.#sparseColumns[start + index] ?? 0;
      this.#sparseValues[moved + index] = cell(
        this.#sparseValues,
        start + index,
      );
    }
    this.#sparseColumns[moved + count] = column;
    this.#sparseValues[moved + count] = value;
    if (count > 0) this.#freeSparse(start, capacity);
    this.#cellStart[slot] = moved;
    this.#cellCount[slot] = count + 1;
  }

  /**
   * A complete row image: the run becomes dense in one step, never grown into.
   */
  setRowImage(slot: number, values: readonly SqlValue[]): void {
    this.clearCells(slot);
    if (this.#width === 0) return;
    const dense = this.#allocateDense();
    for (let column = 0; column < this.#width; column++)
      this.#denseValues[dense + column] = values[column] ?? null;
    this.#cellStart[slot] = dense;
    this.#cellCount[slot] = this.#width;
  }

  /**
   * Returns the row's cells to the free lists and drops its value references.
   */
  clearCells(slot: number): void {
    const count = this.#cellCount[slot] ?? 0;
    if (count === 0) return;
    const start = this.#cellStart[slot] ?? 0;
    if (count === this.#width) this.#freeDense(start);
    else this.#freeSparse(start, capacityFor(count, this.#width));
    this.#cellStart[slot] = 0;
    this.#cellCount[slot] = 0;
  }

  /**
   * Drops every buffer rather than keeping capacity, so `bytes` never charges a
   * cleared plane for its former peak.
   */
  reset(): void {
    this.#slots = 0;
    this.#slotCapacity = 0;
    this.#tombstone = EMPTY_U8;
    this.#cellStart = EMPTY_U32;
    this.#cellCount = EMPTY_U32;
    this.#sparseValues = [];
    this.#sparseColumns = EMPTY_U16;
    this.#sparseUsed = 0;
    for (const bucket of this.#sparseFree) bucket.length = 0;
    this.#denseValues = [];
    this.#denseUsed = 0;
    this.#denseFree.length = 0;
  }

  materialize(slot: number): InspectedRow {
    const values: (SqlValue | Unset)[] = [];
    for (let column = 0; column < this.#width; column++)
      values.push(this.cellAt(slot, column));
    return Object.freeze({
      tombstone: this.tombstoneAt(slot),
      values,
    });
  }

  // ------------------------------------------------------------- internals

  #reserve(slots: number): void {
    if (slots <= this.#slotCapacity) return;
    let capacity = this.#slotCapacity === 0 ? SLOT_SEED : this.#slotCapacity;
    while (capacity < slots) capacity *= 2;
    this.#tombstone = grow(Uint8Array, this.#tombstone, capacity);
    this.#cellStart = grow(Uint32Array, this.#cellStart, capacity);
    this.#cellCount = grow(Uint32Array, this.#cellCount, capacity);
    this.#slotCapacity = capacity;
  }

  #allocateDense(): number {
    const reused = this.#denseFree.pop();
    if (reused !== undefined) return reused;
    const start = this.#denseUsed;
    for (let index = 0; index < this.#width; index++)
      this.#denseValues.push(UNSET);
    this.#denseUsed = start + this.#width;
    return start;
  }

  #freeDense(start: number): void {
    for (let index = 0; index < this.#width; index++)
      this.#denseValues[start + index] = UNSET;
    this.#denseFree.push(start);
  }

  #allocateSparse(capacity: number): number {
    const bucket = this.#sparseFree[sparseClass(capacity)];
    const reused = bucket?.pop();
    if (reused !== undefined) return reused;
    const start = this.#sparseUsed;
    const needed = start + capacity;
    if (needed > this.#sparseColumns.length) {
      let columns =
        this.#sparseColumns.length === 0 ? 64 : this.#sparseColumns.length;
      while (columns < needed) columns *= 2;
      this.#sparseColumns = grow(Uint16Array, this.#sparseColumns, columns);
    }
    for (let index = 0; index < capacity; index++)
      this.#sparseValues.push(UNSET);
    this.#sparseUsed = needed;
    return start;
  }

  #freeSparse(start: number, capacity: number): void {
    for (let index = 0; index < capacity; index++)
      this.#sparseValues[start + index] = UNSET;
    this.#sparseFree[sparseClass(capacity)]?.push(start);
  }
}

interface TypedArrayConstructor<T> {
  new (length: number): T;
}

function grow<T extends { set(source: T, offset: number): void }>(
  kind: TypedArrayConstructor<T>,
  current: T,
  capacity: number,
): T {
  const next = new kind(capacity);
  next.set(current, 0);
  return next;
}

// ------------------------------------------------------------------- planes

interface PlaneState {
  readonly store: PlaneStore;
  /** Address -> slot. Keyed by *address*: the key translation happens once. */
  readonly patched: Map<number, number>;
  /** Inserted position -> slot. Rows with no base row. */
  readonly inserted: number[];
  /** Key hex -> inserted position. Bounded by client-side inserts. */
  readonly insertedByKey: Map<string, number>;
}

function emptyPlane(width: number): PlaneState {
  return {
    store: new PlaneStore(width),
    patched: new Map(),
    inserted: [],
    insertedByKey: new Map(),
  };
}

interface Collection {
  readonly schema: OverlayCollectionSchema;
  readonly canonical: PlaneState;
  readonly pending: PlaneState;
  /**
   * Suppresses the whole base; `TRUNCATE_AUTHORIZED_TABLE` has no per-row form.
   */
  truncated: boolean;
  /**
   * Membership moved since the last rebuild; a pure column patch never sets it.
   */
  orderDirty: boolean;
  /** Anything moved since the last publish; drives the view generation. */
  dirty: boolean;
  /** The pending plane currently holds an insert, a delete, or a revival. */
  pendingMembership: boolean;
  orderBuffer: Uint32Array;
  order: Uint32Array;
  generation: number;
  /**
   * `#baseDirt`'s answer for one generation. Every public write publishes
   * before it returns, so no query snapshots a plane written under a
   * generation it has not moved; a sliced query's restart relies on the same.
   */
  baseDirt: BaseDirt | undefined;
}

/**
 * Per column, the query chunks of the base in which either plane has written
 * a cell, one flag per chunk of the column's base vector; null for a column
 * with no written base cell.
 */
interface BaseDirt {
  readonly generation: number;
  readonly columns: readonly (Uint8Array | null)[];
}

/** A dropped pending action, kept for diagnostics and for tests. */
export interface DroppedAction {
  readonly mutationId: string;
  readonly collection: string;
  readonly actionIndex: number;
  readonly reason: "duplicate-key" | "absent-row";
}

export interface OverlayOptions {
  /**
   * The overlay's share of `ArtifactCapacity`, in bytes. Exhausting it requires
   * a fresh base. Defaults to `OVERLAY_BUDGET_BYTES`.
   */
  readonly budgetBytes?: number;
}

export class OverlayEngine {
  readonly #collections = new Map<string, Collection>();
  #pendingMutations: OptimisticMutation[] = [];
  #dropped: DroppedAction[] = [];
  readonly #budgetBytes: number;
  /**
   * Set when a delta application threw after its first plane write. Writes
   * mutate the planes in place, so the planes now hold a half-applied
   * transaction PostgreSQL never had. Every error `applyCanonical` raises
   * already requires a fresh base, so the engine seals the mount rather than
   * paying for an undo journal: the torn state is never published or read.
   */
  #torn: string | undefined;
  /**
   * Cleared when a delta application begins and set just before its first plane
   * write. A delta refused by a precondition leaves the mount usable; a throw
   * after the first write seals it.
   */
  #wrote = false;

  constructor(
    schemas: readonly OverlayCollectionSchema[],
    options: OverlayOptions = {},
  ) {
    this.#budgetBytes = options.budgetBytes ?? OVERLAY_BUDGET_BYTES;
    for (const schema of schemas) {
      if (this.#collections.has(schema.name))
        throw new OverlayError("INVALID_REQUEST", "duplicate collection");
      if (schema.base.rows >= PENDING_INSERT_BASE)
        throw new OverlayError("CAPACITY_EXCEEDED", "base is too large");
      if (schema.columns.length > MAX_COLUMNS)
        throw new OverlayError("CAPACITY_EXCEEDED", "too many columns");
      if (schema.primaryKey?.some((column) => !schema.columns.includes(column)))
        throw new OverlayError(
          "INVALID_REQUEST",
          "primary key names a column the collection does not carry",
        );
      const sealedSchema: OverlayCollectionSchema = Object.freeze({
        name: schema.name,
        columns: Object.freeze([...schema.columns]),
        base: schema.base,
        ...(schema.primaryKey === undefined
          ? {}
          : { primaryKey: Object.freeze([...schema.primaryKey]) }),
      });
      const width = sealedSchema.columns.length;
      const collection: Collection = {
        schema: sealedSchema,
        canonical: emptyPlane(width),
        pending: emptyPlane(width),
        truncated: false,
        orderDirty: true,
        dirty: true,
        pendingMembership: false,
        orderBuffer: EMPTY_U32,
        order: EMPTY_U32,
        generation: 0,
        baseDirt: undefined,
      };
      this.#collections.set(schema.name, collection);
    }
    INSPECTORS.set(this, (collection) => this.#inspect(collection));
    QUERY_SNAPSHOTTERS.set(this, (names) =>
      names.map((name) => {
        this.#assertIntact();
        const target = this.#require(name);
        this.#settle(target);
        // Computed at most once per snapshot, and a snapshot lives for one
        // query.
        let dirt: BaseDirt | undefined;
        const dirtOf = (column: number) =>
          (dirt ??= this.#baseDirt(target)).columns[column] ?? null;
        return Object.freeze({
          collection: name,
          columns: target.schema.columns,
          primaryKey: target.schema.primaryKey,
          generation: target.generation,
          // The query evaluator is synchronous and trusted. Keeping this
          // internal avoids the full address-vector copy `createView` needs at
          // the application boundary.
          order: target.order,
          baseRows: target.schema.base.rows,
          valueAt: (address: number, column: number) =>
            this.#read(target, address, column),
          compareEncodedAt: (
            address: number,
            column: number,
            operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
            literal: SqlValue,
          ) => {
            if (
              address >= target.schema.base.rows ||
              target.truncated ||
              this.#baseCellDirty(target, address, column)
            )
              return undefined;
            return BASE_QUERY_VECTORS.get(target.schema.base)?.columns[
              column
            ]?.compareAt(address, operator, literal);
          },
          prepareEncodedComparison: (
            column: number,
            operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
            literal: SqlValue,
          ) => {
            const prepared = BASE_QUERY_VECTORS.get(
              target.schema.base,
            )?.columns[column]?.prepareCompare(operator, literal);
            if (!prepared || target.truncated) return undefined;
            return (address: number) =>
              address < target.schema.base.rows &&
              !this.#baseCellDirty(target, address, column)
                ? prepared(address)
                : undefined;
          },
          prepareEncodedBlock: (
            column: number,
            operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
            literal: SqlValue,
          ): ArtifactBlockCompare | undefined => {
            const compare = BASE_QUERY_VECTORS.get(target.schema.base)?.columns[
              column
            ]?.prepareBlockCompare(operator, literal);
            if (!compare || target.truncated) return undefined;
            if (dirtOf(column) === null) return compare;
            return (addresses, selection, count, out) => {
              let undecided = compare(addresses, selection, count, out);
              for (let index = 0; index < count; index++) {
                const position = selection[index] ?? 0;
                if (
                  out[position] !== 1 &&
                  this.#baseCellDirty(target, addresses[position] ?? 0, column)
                ) {
                  out[position] = 1;
                  undecided++;
                }
              }
              return undecided;
            };
          },
          baseEncoded: (column: number): OverlayBaseEncoded | undefined => {
            const cells = BASE_QUERY_VECTORS.get(target.schema.base)?.columns[
              column
            ]?.encoded;
            if (!cells || target.truncated) return undefined;
            return {
              cells,
              dirty:
                dirtOf(column) === null
                  ? undefined
                  : (address: number) =>
                      this.#baseCellDirty(target, address, column),
            };
          },
          baseChunkRows: (column: number) =>
            BASE_QUERY_VECTORS.get(target.schema.base)?.columns[column]
              ?.chunkRows,
          baseChunkMayMatch: (
            column: number,
            chunk: number,
            operator: "eq" | "neq" | "lt" | "lte" | "gt" | "gte",
            literal: SqlValue,
          ) => {
            const vector = BASE_QUERY_VECTORS.get(target.schema.base)?.columns[
              column
            ];
            // The flags are per chunk of this vector: `#baseDirt` divided the
            // base by its `chunkRows`.
            if (!vector || target.truncated || dirtOf(column)?.[chunk] === 1)
              return undefined;
            return vector.chunkMayMatch(chunk, operator, literal);
          },
          statisticsBytes: () =>
            (BASE_QUERY_VECTORS.get(target.schema.base)?.columns ?? []).reduce(
              (total, vector) => total + (vector?.statisticsBytes() ?? 0),
              0,
            ),
          statisticsCapacityBytes: () =>
            (BASE_QUERY_VECTORS.get(target.schema.base)?.columns ?? []).reduce(
              (total, vector) =>
                total + (vector?.statisticsCapacityBytes() ?? 0),
              0,
            ),
          statisticsRowsScanned: () =>
            (BASE_QUERY_VECTORS.get(target.schema.base)?.columns ?? []).reduce(
              (total, vector) => total + (vector?.statisticsRowsScanned() ?? 0),
              0,
            ),
        });
      }),
    );
  }

  // ---------------------------------------------------------------- reads

  collectionNames(): readonly string[] {
    return [...this.#collections.keys()];
  }

  generation(collection: string): number {
    this.#assertIntact();
    return this.#require(collection).generation;
  }

  rowCount(collection: string): number {
    this.#assertIntact();
    const target = this.#require(collection);
    this.#settle(target);
    return target.order.length;
  }

  /**
   * `O(window)`: per cell, at most two plane lookups before the base. Nothing
   * here resolves a key or a column name.
   */
  rows(
    collection: string,
    offset = 0,
    limit = Number.POSITIVE_INFINITY,
    columns?: readonly number[],
  ): SqlValue[][] {
    this.#assertIntact();
    const target = this.#require(collection);
    this.#settle(target);
    return this.#window(target, target.order, offset, limit, columns);
  }

  /**
   * A view is a snapshot of membership. Reading current values at snapshot
   * addresses after the overlay moved is a torn read, so a stale view is
   * refused rather than served.
   */
  createView(collection: string, order?: Uint32Array): OverlayView {
    this.#assertIntact();
    const target = this.#require(collection);
    this.#settle(target);
    return Object.freeze({
      collection,
      generation: target.generation,
      order: order ?? Uint32Array.from(target.order),
    });
  }

  rowsFromView(
    view: OverlayView,
    offset = 0,
    limit = Number.POSITIVE_INFINITY,
    columns?: readonly number[],
  ): SqlValue[][] {
    this.#assertIntact();
    const target = this.#require(view.collection);
    this.#settle(target);
    if (view.generation !== target.generation)
      throw new OverlayError(
        "VIEW_STALE",
        "view was built at an older overlay",
      );
    return this.#window(target, view.order, offset, limit, columns);
  }

  #window(
    target: Collection,
    order: Uint32Array,
    offset: number,
    limit: number,
    columns: readonly number[] | undefined,
  ): SqlValue[][] {
    const width = target.schema.columns.length;
    const fields =
      columns ?? Array.from({ length: width }, (_unused, index) => index);
    const start = Math.max(0, Math.trunc(offset));
    const end = Math.min(
      order.length,
      limit === Number.POSITIVE_INFINITY ? order.length : start + limit,
    );
    const out: SqlValue[][] = [];
    for (let position = start; position < end; position++) {
      const address = order[position] ?? 0;
      out.push(fields.map((column) => this.#read(target, address, column)));
    }
    return out;
  }

  // ------------------------------------------------------- delta application

  /**
   * Applies a CDC batch beneath the pending plane, then refolds the surviving
   * mutations on top. The refold is required: an `increment` is a function of
   * the canonical value beneath it.
   */
  applyCanonical(batch: CanonicalBatch): CanonicalBatchResult {
    const result = this.#sealing("canonical batch", () =>
      this.#applyCanonical(batch),
    );
    // Outside the seal: a batch that applied completely and then read over
    // budget is a correct state that needs a fresh base, not a torn one.
    this.#assertBudget();
    return result;
  }

  #applyCanonical(batch: CanonicalBatch): CanonicalBatchResult {
    const acknowledged = new Set<string>();
    const changed = new Set<string>();
    const survivors = [...this.#pendingMutations];
    for (const transaction of batch.transactions) {
      for (const operation of transaction.operations) {
        this.#applyCanonicalOperation(operation);
        changed.add(operation.collection);
      }
      const ids = new Set(transaction.mutationIds);
      if (ids.size !== transaction.mutationIds.length)
        throw new OverlayError(
          "CHANGE_STREAM_INVALID",
          "canonical transaction contains duplicate mutation acknowledgements",
        );
      for (const id of ids) acknowledged.add(id);
      // An acknowledged mutation's deltas are dropped in the same step that
      // applies the canonical delta carrying the server's values.
      for (let index = survivors.length - 1; index >= 0; index--) {
        const candidate = survivors[index];
        if (candidate && ids.has(candidate.mutationId)) {
          for (const action of candidate.actions)
            changed.add(action.collection);
          survivors.splice(index, 1);
        }
      }
    }
    this.#pendingMutations = survivors;
    this.#fold();
    return {
      acknowledgedMutationIds: [...acknowledged].sort(),
      changedCollections: [...changed].sort(),
    };
  }

  /**
   * Applies one mutation's actions onto the pending plane. No refold is needed:
   * pending is the topmost layer and this mutation is the newest, so it is
   * already being applied last.
   */
  applyOptimistic(mutation: OptimisticMutation): void {
    this.#sealing("optimistic mutation", () => this.#applyOptimistic(mutation));
    this.#assertBudget();
  }

  #applyOptimistic(mutation: OptimisticMutation): void {
    if (this.#pendingMutations.length >= MAX_PENDING_MUTATIONS)
      throw new OverlayError(
        "CAPACITY_EXCEEDED",
        `at most ${MAX_PENDING_MUTATIONS} mutations may be pending`,
      );
    if (mutation.actions.length === 0)
      throw new OverlayError(
        "INVALID_REQUEST",
        "optimistic mutation must carry at least one action",
      );
    if (mutation.actions.length > MAX_OPTIMISTIC_ACTIONS)
      throw new OverlayError(
        "CAPACITY_EXCEEDED",
        `at most ${MAX_OPTIMISTIC_ACTIONS} actions per mutation`,
      );
    if (
      this.#pendingMutations.some(
        (pending) => pending.mutationId === mutation.mutationId,
      )
    )
      throw new OverlayError("INVALID_REQUEST", "mutation is already pending");
    for (const action of mutation.actions) this.#require(action.collection);
    this.#pendingMutations.push(mutation);
    this.#applyMutation(mutation);
    this.#publish();
  }

  /**
   * Removes a pending mutation and refolds the survivors. There is no inverse
   * to run, because nothing beneath the pending plane was overwritten.
   */
  dropMutation(mutationId: string): void {
    this.#sealing("mutation drop", () => this.#dropMutation(mutationId));
  }

  #dropMutation(mutationId: string): void {
    const index = this.#pendingMutations.findIndex(
      (pending) => pending.mutationId === mutationId,
    );
    if (index < 0)
      throw new OverlayError("INVALID_REQUEST", "mutation is not pending");
    this.#pendingMutations.splice(index, 1);
    this.#fold();
  }

  pendingMutationIds(): readonly string[] {
    return this.#pendingMutations.map((pending) => pending.mutationId);
  }

  droppedActions(): readonly DroppedAction[] {
    return this.#dropped;
  }

  #inspect(collection: string): InspectedOverlay {
    this.#assertIntact();
    const target = this.#require(collection);
    this.#settle(target);
    return Object.freeze({
      canonical: planeView(target.canonical, target.schema.base.rows),
      pending: planeView(target.pending, PENDING_INSERT_BASE),
      truncated: target.truncated,
      order: target.order,
    });
  }

  /**
   * Distinct touched rows across both planes; the budget is a function of it.
   */
  overlayRowCount(): number {
    let total = 0;
    for (const collection of this.#collections.values())
      total +=
        collection.canonical.store.slotCount +
        collection.pending.store.slotCount;
    return total;
  }

  /**
   * The bytes the budget charges: a fixed cost per touched row, a cost per
   * allocated cell, and the order vector. It is meant to read above what the
   * engine holds, because a budget that under-charges turns a
   * `RESNAPSHOT_REQUIRED` into an out-of-memory tab. The `insertedByKey` index
   * and the pending mutations are not charged; `MAX_PENDING_MUTATIONS` and
   * `MAX_OPTIMISTIC_ACTIONS` bound them.
   */
  estimatedBytes(): number {
    let total = 0;
    for (const collection of this.#collections.values())
      total +=
        collection.canonical.store.bytes +
        collection.pending.store.bytes +
        collection.orderBuffer.byteLength;
    return total;
  }

  #assertBudget(): void {
    if (this.estimatedBytes() > this.#budgetBytes)
      throw new OverlayError(
        "RESNAPSHOT_REQUIRED",
        "OVERLAY_BUDGET_EXHAUSTED: the overlay must be discarded for a fresh base",
      );
  }

  // ------------------------------------------------------------- internals

  #require(name: string): Collection {
    const collection = this.#collections.get(name);
    if (!collection)
      throw new OverlayError(
        "CHANGE_STREAM_INVALID",
        `operation references unknown collection ${name}`,
      );
    return collection;
  }

  /**
   * Refuses every read once a delta application has torn the planes. Reported
   * as `RESNAPSHOT_REQUIRED` because that is the only recovery: the mount is
   * replaced from a fresh base at a fresh cursor.
   */
  #assertIntact(): void {
    if (this.#torn !== undefined)
      throw new OverlayError(
        "RESNAPSHOT_REQUIRED",
        `the overlay is sealed after a partial delta application: ${this.#torn}`,
      );
  }

  /**
   * Runs one delta application. A throw after its first plane write seals the
   * engine.
   */
  #sealing<T>(what: string, apply: () => T): T {
    this.#assertIntact();
    this.#wrote = false;
    try {
      return apply();
    } catch (error) {
      if (this.#wrote)
        this.#torn = `${what}: ${error instanceof Error ? error.message : String(error)}`;
      throw error;
    }
  }

  /**
   * True once a delta application tore the planes; the mount must be replaced.
   */
  get sealed(): boolean {
    return this.#torn !== undefined;
  }

  /** Clears the pending plane and replays the survivors in submission order. */
  #fold(): void {
    this.#wrote = true;
    for (const collection of this.#collections.values()) {
      if (collection.pending.store.slotCount > 0) {
        collection.pending.patched.clear();
        collection.pending.inserted.length = 0;
        collection.pending.insertedByKey.clear();
        collection.pending.store.reset();
        collection.dirty = true;
        if (collection.pendingMembership) collection.orderDirty = true;
        collection.pendingMembership = false;
      }
    }
    this.#dropped = [];
    for (const mutation of this.#pendingMutations)
      this.#applyMutation(mutation);
    this.#publish();
  }

  /**
   * Advances the generation of every collection whose data actually moved, so a
   * view survives an idle interaction and is invalidated only by real movement.
   */
  #publish(): void {
    for (const collection of this.#collections.values()) {
      if (collection.dirty) {
        collection.generation++;
        collection.dirty = false;
      }
    }
  }

  #settle(collection: Collection): void {
    if (collection.orderDirty) this.#rebuildOrder(collection);
  }

  #applyMutation(mutation: OptimisticMutation): void {
    for (const [index, action] of mutation.actions.entries()) {
      const reason = this.#applyOptimisticAction(action);
      if (reason)
        this.#dropped.push({
          mutationId: mutation.mutationId,
          collection: action.collection,
          actionIndex: index,
          reason,
        });
    }
  }

  // --------------------------------------------------------------- address

  /**
   * Canonical operations resolve under the pending plane, never through it. The
   * key is hex-encoded only when this plane holds inserted rows, so a stream of
   * updates allocates no string per operation.
   */
  #resolveCanonical(
    target: Collection,
    operation: CanonicalKeyedOperation,
  ): number {
    const byKey = target.canonical.insertedByKey;
    if (byKey.size > 0) {
      const position = byKey.get(keyHex(operation.primaryKey));
      if (position !== undefined) return target.schema.base.rows + position;
    }
    const row = target.schema.base.lookupKey(operation.key);
    return row >= 0 ? row : -1;
  }

  #resolveMerged(
    target: Collection,
    operation: CanonicalKeyedOperation,
  ): number {
    const byKey = target.pending.insertedByKey;
    if (byKey.size > 0) {
      const position = byKey.get(keyHex(operation.primaryKey));
      if (position !== undefined) return PENDING_INSERT_BASE + position;
    }
    return this.#resolveCanonical(target, operation);
  }

  /** -1 when this plane holds no row at `address`. */
  #canonicalSlot(target: Collection, address: number): number {
    if (address >= PENDING_INSERT_BASE) return -1;
    if (address >= target.schema.base.rows)
      return target.canonical.inserted[address - target.schema.base.rows] ?? -1;
    return target.canonical.patched.get(address) ?? -1;
  }

  #pendingSlot(target: Collection, address: number): number {
    if (address >= PENDING_INSERT_BASE)
      return target.pending.inserted[address - PENDING_INSERT_BASE] ?? -1;
    return target.pending.patched.get(address) ?? -1;
  }

  /** Liveness in `base ⊕ canonical`, as a CDC operation sees it. */
  #liveCanonical(target: Collection, address: number): boolean {
    const slot = this.#canonicalSlot(target, address);
    if (slot >= 0) return !target.canonical.store.tombstoneAt(slot);
    return address < target.schema.base.rows && !target.truncated;
  }

  /** Liveness in `base ⊕ canonical ⊕ pending`, as a read sees it. */
  #liveMerged(target: Collection, address: number): boolean {
    const slot = this.#pendingSlot(target, address);
    if (slot >= 0) return !target.pending.store.tombstoneAt(slot);
    return this.#liveCanonical(target, address);
  }

  #read(target: Collection, address: number, column: number): SqlValue {
    const pending = this.#pendingSlot(target, address);
    if (pending >= 0) {
      const value = target.pending.store.cellAt(pending, column);
      if (value !== UNSET) return value;
    }
    const canonical = this.#canonicalSlot(target, address);
    if (canonical >= 0) {
      const value = target.canonical.store.cellAt(canonical, column);
      if (value !== UNSET) return value;
    }
    if (address < target.schema.base.rows && !target.truncated)
      return target.schema.base.valueAt(column, address);
    throw new OverlayError(
      "RESNAPSHOT_REQUIRED",
      "overlay row has no value for a column and no base row beneath it",
    );
  }

  #baseCellDirty(target: Collection, address: number, column: number): boolean {
    const pending = this.#pendingSlot(target, address);
    if (pending >= 0 && target.pending.store.cellAt(pending, column) !== UNSET)
      return true;
    const canonical = this.#canonicalSlot(target, address);
    return (
      canonical >= 0 &&
      target.canonical.store.cellAt(canonical, column) !== UNSET
    );
  }

  /**
   * Where `#baseCellDirty` holds, by column and base chunk. One walk of both
   * planes' patched rows answers every column, and its answer is kept until
   * the next publish, so the live queries a publication runs share one walk.
   */
  #baseDirt(target: Collection): BaseDirt {
    const kept = target.baseDirt;
    if (kept?.generation === target.generation) return kept;
    const rows = target.schema.base.rows;
    const vectors = BASE_QUERY_VECTORS.get(target.schema.base)?.columns;
    const columns: (Uint8Array | null)[] = target.schema.columns.map(
      () => null,
    );
    for (const plane of [target.canonical, target.pending])
      for (const [address, slot] of plane.patched) {
        if (address >= rows) continue;
        plane.store.writtenColumns(slot, (column) => {
          const chunkRows = vectors?.[column]?.chunkRows ?? rows;
          (columns[column] ??= new Uint8Array(Math.ceil(rows / chunkRows)))[
            Math.floor(address / chunkRows)
          ] = 1;
        });
      }
    return (target.baseDirt = { generation: target.generation, columns });
  }

  // ------------------------------------------------------ canonical writes

  #applyCanonicalOperation(operation: CanonicalOperation): void {
    const target = this.#require(operation.collection);
    if (operation.kind === "truncate") {
      // Legal only when it clears the complete local authorized table, so no
      // partial form is needed: suppress the base and empty both maps.
      this.#wrote = true;
      target.truncated = true;
      target.canonical.patched.clear();
      target.canonical.inserted.length = 0;
      target.canonical.insertedByKey.clear();
      target.canonical.store.reset();
      target.orderDirty = true;
      target.dirty = true;
      return;
    }
    const width = target.schema.columns.length;
    const store = target.canonical.store;
    const address = this.#resolveCanonical(target, operation);

    if (operation.kind === "insert") {
      if (address >= 0 && this.#liveCanonical(target, address))
        throw new OverlayError(
          "RESNAPSHOT_REQUIRED",
          `canonical insert for ${operation.collection} matched a live row`,
        );
      if (operation.values.length !== width)
        throw new OverlayError(
          "CHANGE_STREAM_INVALID",
          "insert must carry a complete row image",
        );
      this.#wrote = true;
      if (address >= 0) {
        const existing = this.#canonicalSlot(target, address);
        if (existing >= 0) {
          store.setTombstone(existing, false);
          store.setRowImage(existing, operation.values);
        } else {
          // A base address whose base row is suppressed by a truncate.
          const slot = store.allocateSlot();
          store.setRowImage(slot, operation.values);
          target.canonical.patched.set(address, slot);
        }
      } else {
        const slot = store.allocateSlot();
        store.setRowImage(slot, operation.values);
        target.canonical.insertedByKey.set(
          keyHex(operation.primaryKey),
          target.canonical.inserted.length,
        );
        target.canonical.inserted.push(slot);
      }
      target.orderDirty = true;
      target.dirty = true;
      return;
    }

    // Anything but one live address means the base and the server disagree
    // about which rows exist.
    if (address < 0 || !this.#liveCanonical(target, address))
      throw new OverlayError(
        "RESNAPSHOT_REQUIRED",
        `canonical ${operation.kind} for ${operation.collection} matched no live row`,
      );
    // Every column is checked before the first write, so a throw leaves no
    // half-patched row.
    if (operation.kind === "update")
      for (const change of operation.changed)
        if (change.column < 0 || change.column >= width)
          throw new OverlayError("CHANGE_STREAM_INVALID", "unknown column");

    let slot = this.#canonicalSlot(target, address);
    // pgoutput sends an update's whole new tuple, so most cells it names hold
    // the value already beneath them. Writing one would mark the cell dirty
    // and move every query over its column from the encoded base to per-row
    // reads for as long as the overlay lives.
    const changes =
      operation.kind === "update"
        ? operation.changed.filter(
            (change) => !this.#baseHolds(target, address, slot, change),
          )
        : [];
    if (operation.kind === "update" && changes.length === 0) return;

    this.#wrote = true;
    if (slot < 0) {
      slot = store.allocateSlot();
      target.canonical.patched.set(address, slot);
    }

    if (operation.kind === "delete") {
      store.setTombstone(slot, true);
      store.clearCells(slot);
      target.orderDirty = true;
      target.dirty = true;
      return;
    }
    for (const change of changes)
      store.setCell(slot, change.column, change.value);
    target.dirty = true;
  }

  /**
   * Whether the base already holds `change` for a cell no canonical delta has
   * written, so that skipping the write changes no read. Identity, not SQL
   * equality: `-0` is not `0`, and two spellings of one numeric are two values.
   */
  #baseHolds(
    target: Collection,
    address: number,
    slot: number,
    change: ColumnChange,
  ): boolean {
    if (address >= target.schema.base.rows || target.truncated) return false;
    if (
      slot >= 0 &&
      target.canonical.store.cellAt(slot, change.column) !== UNSET
    )
      return false;
    const base = target.schema.base.valueAt(change.column, address);
    return base instanceof Uint8Array
      ? change.value instanceof Uint8Array && equalBytes(base, change.value)
      : Object.is(base, change.value);
  }

  // -------------------------------------------------------- pending writes

  /**
   * Returns a drop reason when the action's precondition does not hold. The
   * action leaves the fold and its mutation waits for the server's verdict,
   * because an optimistic guess must not fail a canonical batch. Only a pending
   * insert may clear a tombstone.
   */
  #applyOptimisticAction(
    action: OptimisticAction,
  ): DroppedAction["reason"] | undefined {
    const target = this.#require(action.collection);
    const width = target.schema.columns.length;
    const store = target.pending.store;
    const address = this.#resolveMerged(target, action);
    const live = address >= 0 && this.#liveMerged(target, address);

    if (action.kind === "insert") {
      if (live) return "duplicate-key";
      if (action.values.length !== width)
        throw new OverlayError(
          "INVALID_REQUEST",
          "optimistic insert must carry a complete row image",
        );
      if (address >= 0) {
        const existing = this.#pendingSlot(target, address);
        if (existing >= 0) {
          store.setTombstone(existing, false);
          store.setRowImage(existing, action.values);
        } else {
          const slot = store.allocateSlot();
          store.setRowImage(slot, action.values);
          target.pending.patched.set(address, slot);
        }
      } else {
        const slot = store.allocateSlot();
        store.setRowImage(slot, action.values);
        target.pending.insertedByKey.set(
          keyHex(action.primaryKey),
          target.pending.inserted.length,
        );
        target.pending.inserted.push(slot);
      }
      target.orderDirty = true;
      target.pendingMembership = true;
      target.dirty = true;
      return undefined;
    }

    if (!live) return "absent-row";
    let slot = this.#pendingSlot(target, address);
    if (slot < 0) {
      slot = store.allocateSlot();
      target.pending.patched.set(address, slot);
    }

    if (action.kind === "delete") {
      store.setTombstone(slot, true);
      store.clearCells(slot);
      target.orderDirty = true;
      target.pendingMembership = true;
      target.dirty = true;
      return undefined;
    }

    if (action.changes.length === 0)
      throw new OverlayError(
        "INVALID_REQUEST",
        "optimistic patch must change at least one column",
      );
    const seen = new Set<number>();
    for (const change of action.changes) {
      if (
        change.column < 0 ||
        change.column >= width ||
        seen.has(change.column)
      )
        throw new OverlayError(
          "INVALID_REQUEST",
          "optimistic patch has duplicate or unknown columns",
        );
      seen.add(change.column);
      // `#read` is evaluated before the write lands, so an `increment` sees the
      // value beneath it and never its own result.
      store.setCell(
        slot,
        change.column,
        change.mode === "increment"
          ? increment(this.#read(target, address, change.column), change.value)
          : change.value,
      );
    }
    target.dirty = true;
    return undefined;
  }

  // ---------------------------------------------------------- order vector

  /**
   * One coalesced linear pass over the base, skipping the addresses membership
   * changes removed. Only a membership change dirties the vector; a pure column
   * patch never does.
   */
  #rebuildOrder(target: Collection): void {
    const rows = target.schema.base.rows;
    const canonicalInserts = target.canonical.inserted.length;
    const pendingInserts = target.pending.inserted.length;
    const upperBound = rows + canonicalInserts + pendingInserts;
    if (target.orderBuffer.length < upperBound)
      target.orderBuffer = new Uint32Array(upperBound);
    const buffer = target.orderBuffer;
    let count = 0;

    if (target.truncated) {
      // The base is suppressed wholesale; only addresses an explicit delta
      // resurrected are live, and they stay in base order.
      const candidates = new Set<number>();
      for (const address of target.canonical.patched.keys())
        if (address < rows) candidates.add(address);
      for (const address of target.pending.patched.keys())
        if (address < rows) candidates.add(address);
      for (const address of [...candidates].sort((a, b) => a - b))
        if (this.#liveMerged(target, address)) buffer[count++] = address;
    } else {
      const dead: number[] = [];
      for (const address of target.canonical.patched.keys())
        if (address < rows && !this.#liveMerged(target, address))
          dead.push(address);
      for (const address of target.pending.patched.keys())
        if (
          address < rows &&
          !this.#liveMerged(target, address) &&
          !target.canonical.patched.has(address)
        )
          dead.push(address);
      dead.sort((a, b) => a - b);
      let next = 0;
      const first = dead.length;
      for (let address = 0; address < rows; address++) {
        if (next < first && dead[next] === address) {
          next++;
          continue;
        }
        buffer[count++] = address;
      }
    }
    for (let index = 0; index < canonicalInserts; index++) {
      const address = rows + index;
      if (this.#liveMerged(target, address)) buffer[count++] = address;
    }
    for (let index = 0; index < pendingInserts; index++) {
      const address = PENDING_INSERT_BASE + index;
      if (this.#liveMerged(target, address)) buffer[count++] = address;
    }
    target.order = buffer.subarray(0, count);
    target.orderDirty = false;
  }
}

// ------------------------------------------------------------- inspection

/**
 * Inspection of the stored representation, for tests and capacity diagnostics.
 * It is exported from this module but not the package entry, and kept off
 * `OverlayEngine` so the row layout stays out of the public declarations.
 */
interface InspectedRow {
  /** The row is absent from the merged view. */
  readonly tombstone: boolean;
  /** Expanded to full width; `UNSET` where no delta in this plane wrote. */
  readonly values: readonly (SqlValue | Unset)[];
}

interface InspectedPlane {
  /** Rows this plane holds at a base or canonical-inserted address. */
  readonly patchedRows: number;
  /** Rows this plane holds that have no base row. */
  readonly insertedRows: number;
  /** This plane's share of `estimatedBytes()`. */
  readonly bytes: number;
  /** Materializes the row at `address`, or `undefined`. `O(width)`. */
  row(address: number): InspectedRow | undefined;
}

interface InspectedOverlay {
  readonly canonical: InspectedPlane;
  readonly pending: InspectedPlane;
  /**
   * Suppresses the whole base; `TRUNCATE_AUTHORIZED_TABLE` has no per-row form.
   */
  readonly truncated: boolean;
  /** Logical position -> address, over the merged planes. */
  readonly order: Uint32Array;
}

const INSPECTORS = new WeakMap<
  OverlayEngine,
  (collection: string) => InspectedOverlay
>();

const QUERY_SNAPSHOTTERS = new WeakMap<
  OverlayEngine,
  (collections: readonly string[]) => readonly OverlayQuerySnapshot[]
>();

/**
 * Opens merged, generation-bound collection snapshots for the query evaluator.
 * Not re-exported by `index.ts`; application code receives only sealed query
 * results, never these addresses or accessors.
 */
export function snapshotOverlayForQuery(
  engine: OverlayEngine,
  collections: readonly string[],
): readonly OverlayQuerySnapshot[] {
  const snapshot = QUERY_SNAPSHOTTERS.get(engine);
  if (!snapshot)
    throw new OverlayError("INVALID_REQUEST", "not an overlay engine");
  return snapshot(collections);
}

function planeView(plane: PlaneState, insertedBase: number): InspectedPlane {
  return Object.freeze({
    patchedRows: plane.patched.size,
    insertedRows: plane.inserted.length,
    bytes: plane.store.bytes,
    row: (address: number): InspectedRow | undefined => {
      const slot =
        address >= insertedBase
          ? plane.inserted[address - insertedBase]
          : plane.patched.get(address);
      return slot === undefined ? undefined : plane.store.materialize(slot);
    },
  });
}

export function inspectOverlay(
  engine: OverlayEngine,
  collection: string,
): InspectedOverlay {
  const inspect = INSPECTORS.get(engine);
  if (!inspect)
    throw new OverlayError("INVALID_REQUEST", "not an overlay engine");
  return inspect(collection);
}

/**
 * SQL's `x = x + ?`: arithmetic over NULL is NULL, and the result keeps the
 * operand widths. Non-numeric operands are refused rather than coerced.
 */
function increment(current: SqlValue, delta: SqlValue): SqlValue {
  if (typeof delta !== "number" && typeof delta !== "bigint")
    throw new OverlayError(
      "INVALID_REQUEST",
      "optimistic increment requires a numeric value",
    );
  if (current === null) return null;
  if (typeof current === "number")
    return current + (typeof delta === "number" ? delta : Number(delta));
  if (typeof current === "bigint")
    return (
      current + (typeof delta === "bigint" ? delta : BigInt(Math.trunc(delta)))
    );
  throw new OverlayError(
    "INVALID_REQUEST",
    "optimistic increment requires a numeric column value",
  );
}

/** Binds an `ArtifactTable` to the ordinal-addressed base the overlay reads. */
export function artifactBase(
  table: ArtifactTable,
  columns: readonly string[],
): OverlayBase {
  const accessors = columns.map((name) => table.column(name));
  const base: OverlayBase = Object.freeze({
    rows: table.rows,
    lookupKey: (key: readonly KeyPart[]) => table.lookupKey(key),
    valueAt: (column: number, row: number) =>
      at(accessors, column).valueAt(row) as SqlValue,
  });
  BASE_QUERY_VECTORS.set(base, {
    columns: Object.freeze(accessors.map(artifactQueryVector)),
  });
  return base;
}
