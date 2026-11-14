import type { MutationAccepted } from "../mutation.js";
import type { LiveQueryResource } from "../resource.js";

/**
 * The type-level description of one generated collection. It has no runtime
 * form: generation writes `CollectionShape<Row, Key, Insert, Patch, Delete>`
 * into `calque.generated.ts` and `createSyncClient` reads it back through the
 * schema capability's type argument. `Insert` and `Patch` are `never`, and
 * `Delete` is `false`, for operations no configured command allows, which
 * removes the corresponding facade method at the type level. `Int32Column`
 * names the `int2` and `int4` columns of `Row`: numbers whose `sum` is a
 * bigint.
 */
export interface CollectionShape<
  Row extends object,
  Key extends object,
  Insert = never,
  Patch = never,
  Delete extends boolean = false,
  Int32Column extends string = never,
> {
  readonly row: Row;
  readonly key: Key;
  readonly insert: Insert;
  readonly patch: Patch;
  readonly delete: Delete;
  readonly int32Columns: Int32Column;
}

/** The record of collection shapes one generated schema carries. */
export type CollectionShapes = {
  readonly [collection: string]: CollectionShape<
    object,
    object,
    unknown,
    unknown,
    boolean,
    string
  >;
};

declare const predicateCapability: unique symbol;

/**
 * A sealed filter built by `eq`, `and`, `isNull` and the other predicate
 * helpers. Application code composes predicates but cannot inspect or forge
 * one; the query plan they compile to never crosses the facade.
 */
export interface QueryPredicate<Row extends object = object> {
  readonly [predicateCapability]: (row: Row) => void;
}

/** `run`'s bounds on its result, and its cancellation. */
export interface QueryRunOptions {
  /**
   * Maximum complete rows returned. Must be an integer from 1 through 10,000;
   * the default is 1,000. Overflow rejects with RESULT_TOO_LARGE and returns no
   * partial rows.
   */
  readonly maxRows?: number;
  /**
   * Maximum bytes retained for the complete result. Must be an integer from 1
   * through 16 MiB; the default is 4 MiB. Overflow rejects with
   * RESULT_TOO_LARGE and returns no partial result.
   */
  readonly maxBytes?: number;
  /** Aborting cancels this caller's interest; the result is discarded. */
  readonly signal?: AbortSignal;
}

/** `subscribe`'s options: `run`'s bounds, without its `signal`. */
export type QueryLiveOptions = Omit<QueryRunOptions, "signal">;

/** `window`'s byte bound and cancellation. */
export interface QueryWindowOptions {
  /**
   * Maximum bytes retained for the window's rows. Must be an integer from 1
   * through 16 MiB; the default is 4 MiB. Overflow rejects with
   * RESULT_TOO_LARGE and returns no partial result.
   */
  readonly maxBytes?: number;
  /** Aborting cancels this caller's interest and stops the query. */
  readonly signal?: AbortSignal;
}

/** `subscribeWindow`'s options: `window`'s, without its `signal`. */
export type QueryLiveWindowOptions = Omit<QueryWindowOptions, "signal">;

/** A run of consecutive rows from a query's whole result, in its order. */
export interface QueryWindow<Out extends object> {
  /** The position of `rows[0]` in the whole result. */
  readonly offset: number;
  /** How many rows the whole result has. */
  readonly total: number;
  /** Fewer than asked for where the whole result ends. */
  readonly rows: readonly Out[];
}

/** `orderBy`'s direction; `asc` when omitted. */
export type QueryOrderDirection = "asc" | "desc";
/**
 * Where `orderBy` puts NULLs. They sort `last` when this is omitted, in
 * either direction; PostgreSQL instead puts them first under `desc`.
 */
export type QueryNullPlacement = "first" | "last";
/**
 * `inner` keeps the rows that have a match; `left` keeps every row and gives
 * one with no match NULL joined columns.
 */
export type QueryJoinKind = "inner" | "left";

/** The columns of a joined collection, addressed as `alias.column`. */
export type JoinedColumns<
  Alias extends string,
  Row extends object,
  Kind extends QueryJoinKind,
> = {
  readonly [
    Column in keyof Row & string as `${Alias}.${Column}`
  ]: Kind extends "left" ? Row[Column] | null : Row[Column];
};

/** The output key of a projected column: the part after the join alias. */
export type ProjectedName<Column extends string> =
  Column extends `${string}.${infer Name}` ? Name : Column;

/**
 * A row of `select(...columns)`: each selected column, a joined one under its
 * name without the alias.
 */
export type ProjectedRow<
  Row extends object,
  Column extends keyof Row & string,
> = {
  readonly [C in Column as ProjectedName<C>]: Row[C];
};

/** The columns of `Row` whose values are numbers, which `sum` adds. */
export type NumericColumn<Row extends object> = {
  readonly [Column in keyof Row & string]: [NonNullable<Row[Column]>] extends [
    number | bigint,
  ]
    ? Column
    : never;
}[keyof Row & string];

/**
 * What `sum` over `Column` returns: a bigint over an integer column, as
 * PostgreSQL's `sum` over `int2` and `int4` is, and a number over a float one.
 */
export type SumValue<
  Row extends object,
  Int32Column extends string,
  Column extends keyof Row & string,
> = Column extends Int32Column
  ? bigint
  : [NonNullable<Row[Column]>] extends [bigint]
    ? bigint
    : number;

/** A join compares integers by value, whether `number` or `bigint`. */
type JoinValue<Value> = [NonNullable<Value>] extends [number | bigint]
  ? number | bigint
  : NonNullable<Value>;

/** The columns of `Joined` whose values can equal a `Value`. */
export type MatchingColumn<Joined extends object, Value> = {
  readonly [Column in keyof Joined & string]: [
    JoinValue<Joined[Column]>,
  ] extends [JoinValue<Value>]
    ? [JoinValue<Value>] extends [JoinValue<Joined[Column]>]
      ? Column
      : never
    : never;
}[keyof Joined & string];

/** What `join` accepts: a generated collection facade, read by its row type. */
export interface JoinSource<
  Row extends object,
  Int32Column extends string = never,
> {
  readonly query: () => QueryBuilder<Row, Row, Int32Column>;
}

/**
 * A query over one collection, its joins, and the projection accumulated so
 * far. Every method returns a new builder; none executes. `Row` is the set of
 * addressable columns, `Out` the shape of one result row, and `Int32Column`
 * the columns of `Row` that hold `int2` or `int4` values.
 */
export interface QueryBuilder<
  Row extends object,
  Out extends object = Row,
  Int32Column extends string = never,
> {
  readonly where: (
    predicate: QueryPredicate<Row>,
  ) => QueryBuilder<Row, Out, Int32Column>;
  readonly select: <const Column extends keyof Row & string>(
    ...columns: readonly Column[]
  ) => QueryBuilder<Row, ProjectedRow<Row, Column>, Int32Column>;
  readonly selectAs: <Column extends keyof Row & string, Alias extends string>(
    column: Column,
    alias: Alias,
  ) => QueryBuilder<
    Row,
    Out & { readonly [A in Alias]: Row[Column] },
    Int32Column
  >;
  readonly join: <
    Joined extends object,
    Alias extends string,
    Left extends keyof Row & string,
    Kind extends QueryJoinKind = "inner",
    JoinedInt32Column extends string = never,
  >(
    collection: JoinSource<Joined, JoinedInt32Column>,
    alias: Alias,
    on: {
      readonly left: Left;
      readonly right: MatchingColumn<Joined, Row[Left]>;
    },
    kind?: Kind,
  ) => QueryBuilder<
    Row & JoinedColumns<Alias, Joined, Kind>,
    Out,
    Int32Column | `${Alias}.${JoinedInt32Column}`
  >;
  readonly groupBy: <const Column extends keyof Row & string>(
    ...columns: readonly Column[]
  ) => GroupedQueryBuilder<Row, ProjectedRow<Row, Column>, Int32Column>;
  /**
   * Counts the rows. Without a `groupBy` first, it and `sum`, `min` and `max`
   * reduce every row the query selects to one result row, present even when
   * no row matches.
   */
  readonly count: <Alias extends string = "count">(
    alias?: Alias,
  ) => GroupedQueryBuilder<Row, { readonly [A in Alias]: number }, Int32Column>;
  /**
   * The sum of `column`'s non-null values, or null when there are none: a
   * bigint over an integer column, as PostgreSQL's over `int2` and `int4` is,
   * and a number over a float one.
   */
  readonly sum: <
    Column extends NumericColumn<Row>,
    Alias extends string = Column,
  >(
    column: Column,
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    { readonly [A in Alias]: SumValue<Row, Int32Column, Column> | null },
    Int32Column
  >;
  /** The least non-null value of `column`, or null when there is none. */
  readonly min: <
    Column extends keyof Row & string,
    Alias extends string = Column,
  >(
    column: Column,
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    { readonly [A in Alias]: Row[Column] | null },
    Int32Column
  >;
  /** The greatest non-null value of `column`, or null when there is none. */
  readonly max: <
    Column extends keyof Row & string,
    Alias extends string = Column,
  >(
    column: Column,
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    { readonly [A in Alias]: Row[Column] | null },
    Int32Column
  >;
  readonly orderBy: (
    column: keyof Out & string,
    direction?: QueryOrderDirection,
    nulls?: QueryNullPlacement,
  ) => QueryBuilder<Row, Out, Int32Column>;
  readonly limit: (count: number) => QueryBuilder<Row, Out, Int32Column>;
  readonly run: (options?: QueryRunOptions) => Promise<readonly Out[]>;
  /**
   * Reads rows `offset` through `offset + count - 1` of the whole result, in
   * its order, with the whole result's row count. `count` is an integer from
   * 0 through 10,000; the whole result has no such bound. While the
   * collections the query reads stay where they are, the next window over the
   * same query reads only its own rows.
   */
  readonly window: (
    offset: number,
    count: number,
    options?: QueryWindowOptions,
  ) => Promise<QueryWindow<Out>>;
  readonly subscribe: (
    options?: QueryLiveOptions,
  ) => LiveQueryResource<readonly Out[]>;
  /** `window`, kept current as the collections it reads change. */
  readonly subscribeWindow: (
    offset: number,
    count: number,
    options?: QueryLiveWindowOptions,
  ) => LiveQueryResource<QueryWindow<Out>>;
}

/**
 * A grouped or aggregated query. Once a group key or an aggregate exists the
 * projection is the group columns plus the aggregates, so `select` is gone;
 * ordering addresses output names, aggregates included.
 */
export interface GroupedQueryBuilder<
  Row extends object,
  Out extends object,
  Int32Column extends string = never,
> {
  readonly count: <Alias extends string = "count">(
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    Out & { readonly [A in Alias]: number },
    Int32Column
  >;
  /**
   * The sum of `column`'s non-null values, or null when there are none: a
   * bigint over an integer column, as PostgreSQL's over `int2` and `int4` is,
   * and a number over a float one.
   */
  readonly sum: <
    Column extends NumericColumn<Row>,
    Alias extends string = Column,
  >(
    column: Column,
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    Out & {
      readonly [A in Alias]: SumValue<Row, Int32Column, Column> | null;
    },
    Int32Column
  >;
  /** The least non-null value of `column`, or null when there is none. */
  readonly min: <
    Column extends keyof Row & string,
    Alias extends string = Column,
  >(
    column: Column,
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    Out & { readonly [A in Alias]: Row[Column] | null },
    Int32Column
  >;
  /** The greatest non-null value of `column`, or null when there is none. */
  readonly max: <
    Column extends keyof Row & string,
    Alias extends string = Column,
  >(
    column: Column,
    alias?: Alias,
  ) => GroupedQueryBuilder<
    Row,
    Out & { readonly [A in Alias]: Row[Column] | null },
    Int32Column
  >;
  readonly orderBy: (
    column: keyof Out & string,
    direction?: QueryOrderDirection,
    nulls?: QueryNullPlacement,
  ) => GroupedQueryBuilder<Row, Out, Int32Column>;
  readonly limit: (count: number) => GroupedQueryBuilder<Row, Out, Int32Column>;
  readonly run: (options?: QueryRunOptions) => Promise<readonly Out[]>;
  /**
   * Reads rows `offset` through `offset + count - 1` of the whole result, in
   * its order, with the whole result's row count. `count` is an integer from
   * 0 through 10,000; the whole result has no such bound. While the
   * collections the query reads stay where they are, the next window over the
   * same query reads only its own rows.
   */
  readonly window: (
    offset: number,
    count: number,
    options?: QueryWindowOptions,
  ) => Promise<QueryWindow<Out>>;
  readonly subscribe: (
    options?: QueryLiveOptions,
  ) => LiveQueryResource<readonly Out[]>;
  /** `window`, kept current as the collections it reads change. */
  readonly subscribeWindow: (
    offset: number,
    count: number,
    options?: QueryLiveWindowOptions,
  ) => LiveQueryResource<QueryWindow<Out>>;
}

/** The read half of a collection facade, present for every collection. */
interface CollectionFacadeBase<
  Shape extends CollectionShape<
    object,
    object,
    unknown,
    unknown,
    boolean,
    string
  >,
> {
  readonly query: () => QueryBuilder<
    Shape["row"],
    Shape["row"],
    Shape["int32Columns"]
  >;
  /**
   * Reads `count` complete rows starting at `start` in engine order, over the
   * whole collection. It is the window a virtualized grid asks for, and it has
   * no query form because engine order is not a sort.
   */
  readonly slice: (
    start: number,
    count: number,
    options?: Pick<QueryRunOptions, "signal">,
  ) => Promise<readonly Shape["row"][]>;
}

type InsertMethod<Insert> = [Insert] extends [never]
  ? object
  : {
      /**
       * Inserts `row`, which carries every column: a nullable one takes
       * `null` when it has no value. No PostgreSQL column default applies.
       * Resolves once the server commits it; rejects with a
       * `MutationSubmissionError` when the server refuses it.
       */
      readonly insert: (row: Insert) => Promise<MutationAccepted>;
    };

type UpdateMethod<Key, Patch> = [Patch] extends [never]
  ? object
  : {
      /**
       * Sets the writable columns `patch` names on the row with `key`.
       * Resolves once the server commits it; rejects with a
       * `MutationSubmissionError` when the server refuses it.
       */
      readonly update: (key: Key, patch: Patch) => Promise<MutationAccepted>;
    };

type DeleteMethod<Key, Delete> = Delete extends true
  ? {
      /**
       * Deletes the row with `key`. Resolves once the server commits it;
       * rejects with a `MutationSubmissionError` when the server refuses it.
       */
      readonly delete: (key: Key) => Promise<MutationAccepted>;
    }
  : object;

/**
 * The public surface of one generated collection: `query`, `slice`, and the
 * mutation methods its configured commands allow.
 */
export type CollectionFacade<
  Shape extends CollectionShape<
    object,
    object,
    unknown,
    unknown,
    boolean,
    string
  >,
> = CollectionFacadeBase<Shape> &
  InsertMethod<Shape["insert"]> &
  UpdateMethod<Shape["key"], Shape["patch"]> &
  DeleteMethod<Shape["key"], Shape["delete"]>;

/**
 * One facade per collection in the schema, by name: the record
 * `SyncClient.db` holds.
 */
export type CollectionFacades<Collections extends CollectionShapes> = {
  readonly [Collection in keyof Collections]: CollectionFacade<
    Collections[Collection]
  >;
};
