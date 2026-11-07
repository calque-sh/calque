import type {
  QueryAggregateOperator,
  QueryField,
  QueryJoin,
  QueryLiteral,
  QueryOrder,
  QueryPredicate as CorePredicate,
  QueryProjection,
  QuerySpecification,
} from "@calque/core";

import type { GeneratedClientColumnKind } from "../generated-schema.js";
import { uuidBytes } from "../uuid.js";
import {
  sealColumnName,
  resolvePredicate,
  type PredicateNode,
} from "./predicates.js";
import {
  cellReader,
  materializeRows,
  sealLiveOptions,
  sealLiveWindowOptions,
  sealRunOptions,
  sealWindow,
  sealWindowOptions,
  type CellReaders,
  type FacadePort,
} from "./port.js";
import { collectionOfFacade } from "./registry.js";
import type {
  GroupedQueryBuilder,
  QueryBuilder,
  QueryJoinKind,
  QueryNullPlacement,
  QueryOrderDirection,
} from "./types.js";

const JOIN_KINDS = new Set<QueryJoinKind>(["inner", "left"]);
const DIRECTIONS = new Set<QueryOrderDirection>(["asc", "desc"]);
const NULL_PLACEMENTS = new Set<QueryNullPlacement>(["first", "last"]);

interface Projection {
  readonly column: string;
  readonly as: string;
}

interface Aggregate {
  readonly operator: QueryAggregateOperator;
  readonly column?: string;
  readonly as: string;
}

interface Join {
  readonly collection: string;
  readonly as: string;
  readonly kind: QueryJoinKind;
  readonly left: string;
  readonly right: string;
}

interface State {
  readonly collection: string;
  readonly columns: ReadonlyMap<string, GeneratedClientColumnKind>;
  readonly joins: readonly Join[];
  readonly where: readonly PredicateNode[];
  readonly select: readonly Projection[] | undefined;
  readonly groupBy: readonly string[];
  readonly aggregates: readonly Aggregate[];
  readonly orderBy: readonly QueryOrder[];
  readonly limit: number | undefined;
}

/** A column reference resolved against the builder's sources. */
interface ResolvedField extends QueryField {
  readonly name: string;
  readonly kind: GeneratedClientColumnKind;
}

interface CompiledQuery {
  readonly specification: QuerySpecification;
  readonly readers: CellReaders;
}

function invalid(message: string): never {
  throw new TypeError(message);
}

function isGrouped(state: State): boolean {
  return state.groupBy.length > 0 || state.aggregates.length > 0;
}

function outputName(reference: string, joins: readonly Join[]): string {
  const dot = reference.indexOf(".");
  if (dot < 0) return reference;
  const alias = reference.slice(0, dot);
  return joins.some((join) => join.as === alias)
    ? reference.slice(dot + 1)
    : reference;
}

function resolve(
  state: State,
  port: FacadePort,
  reference: string,
): ResolvedField {
  const name = sealColumnName(reference);
  const dot = name.indexOf(".");
  if (dot >= 0) {
    const alias = name.slice(0, dot);
    const join = state.joins.find((candidate) => candidate.as === alias);
    if (join) {
      const column = name.slice(dot + 1);
      const kind = port.columnsOf(join.collection)?.get(column);
      if (kind === undefined)
        invalid(`${join.collection} has no column ${column}`);
      return { source: alias, column, name: column, kind };
    }
  }
  const kind = state.columns.get(name);
  if (kind === undefined) invalid(`${state.collection} has no column ${name}`);
  return { source: state.collection, column: name, name, kind };
}

/** The output names the current projection produces, in order. */
function outputNames(state: State): readonly string[] {
  if (isGrouped(state))
    return [
      ...state.groupBy.map((column) => outputName(column, state.joins)),
      ...state.aggregates.map(({ as }) => as),
    ];
  if (state.select) return state.select.map(({ as }) => as);
  return [...state.columns.keys()];
}

function assertFreshOutputName(state: State, name: string): void {
  if (outputNames(state).includes(name))
    invalid(`output name ${name} is already projected`);
}

function compilePredicate(
  state: State,
  port: FacadePort,
  node: PredicateNode,
): CorePredicate {
  switch (node.kind) {
    case "compare": {
      const resolved = resolve(state, port, node.column);
      return {
        kind: "compare",
        operator: node.operator,
        left: { kind: "field", field: field(resolved) },
        right: { kind: "literal", value: literal(resolved, node.value) },
      };
    }
    case "is-null": {
      const { source, column } = resolve(state, port, node.column);
      return {
        kind: "is-null",
        expression: { kind: "field", field: { source, column } },
        ...(node.negated ? { negated: true } : {}),
      };
    }
    case "and":
    case "or":
      return {
        kind: node.kind,
        predicates: node.predicates.map((child) =>
          compilePredicate(state, port, child),
        ),
      };
    case "not":
      return {
        kind: "not",
        predicate: compilePredicate(state, port, node.predicate),
      };
  }
}

/** The kinds whose values can be equal: an int32 is held as an int64. */
function joinKey(kind: GeneratedClientColumnKind): GeneratedClientColumnKind {
  return kind === "int32" ? "int64" : kind;
}

/** A compared value in the form the engine holds `resolved`'s cells in. */
function literal(resolved: ResolvedField, value: QueryLiteral): QueryLiteral {
  const name = `${resolved.source}.${resolved.column}`;
  if (resolved.kind === "uuid") return uuidBytes(value, name);
  if (resolved.kind === "int32" && typeof value === "number") {
    if (!Number.isSafeInteger(value))
      invalid(`${name} holds integers, so it cannot be compared with ${value}`);
    return BigInt(value);
  }
  return value;
}

function field(resolved: ResolvedField): QueryField {
  return { source: resolved.source, column: resolved.column };
}

function compile(state: State, port: FacadePort): CompiledQuery {
  const select: QueryProjection[] = [];
  const readers = new Map<string, (value: unknown) => unknown>();
  const read = (kind: GeneratedClientColumnKind, as: string): void => {
    const reader = cellReader(kind);
    if (reader !== undefined) readers.set(as, reader);
  };
  const project = (resolved: ResolvedField, as: string): void => {
    select.push({ kind: "field", field: field(resolved), as });
    read(resolved.kind, as);
  };
  if (isGrouped(state)) {
    for (const column of state.groupBy) {
      const resolved = resolve(state, port, column);
      project(resolved, resolved.name);
    }
    for (const aggregate of state.aggregates) {
      const resolved =
        aggregate.column === undefined
          ? undefined
          : resolve(state, port, aggregate.column);
      select.push({
        kind: "aggregate",
        operator: aggregate.operator,
        ...(resolved === undefined ? {} : { field: field(resolved) }),
        as: aggregate.as,
      });
      // Only a min or max is one of the column's values. A sum over an int32
      // column stays the engine's int64, as PostgreSQL's sum(int4) is a
      // bigint, and a count is a number already.
      if (
        resolved !== undefined &&
        (aggregate.operator === "min" || aggregate.operator === "max")
      )
        read(resolved.kind, aggregate.as);
    }
  } else if (state.select) {
    for (const projection of state.select)
      project(resolve(state, port, projection.column), projection.as);
  } else {
    for (const [column, kind] of state.columns)
      project({ source: state.collection, column, name: column, kind }, column);
  }
  const names = new Set(select.map(({ as }) => as));
  for (const order of state.orderBy)
    if (!names.has(order.by))
      invalid(`orderBy references ${order.by}, which is not projected`);
  const joins: QueryJoin[] = state.joins.map((join) => ({
    kind: join.kind,
    as: join.as,
    collection: join.collection,
    on: {
      left: field(resolve(state, port, join.left)),
      right: { source: join.as, column: join.right },
    },
  }));
  const where =
    state.where.length === 0
      ? undefined
      : state.where.length === 1 && state.where[0] !== undefined
        ? compilePredicate(state, port, state.where[0])
        : {
            kind: "and" as const,
            predicates: state.where.map((node) =>
              compilePredicate(state, port, node),
            ),
          };
  const specification: QuerySpecification = {
    version: 1,
    from: { collection: state.collection, as: state.collection },
    ...(joins.length === 0 ? {} : { joins }),
    ...(where === undefined ? {} : { where }),
    select,
    ...(state.groupBy.length === 0
      ? {}
      : {
          groupBy: state.groupBy.map((column) =>
            field(resolve(state, port, column)),
          ),
        }),
    ...(state.orderBy.length === 0 ? {} : { orderBy: state.orderBy }),
    ...(state.limit === undefined ? {} : { limit: state.limit }),
  };
  return { specification, readers };
}

function dependencies(state: State): readonly string[] {
  return Object.freeze([
    ...new Set([
      state.collection,
      ...state.joins.map((join) => join.collection),
    ]),
  ]);
}

function sealLimit(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0)
    invalid("limit must be a non-negative safe integer");
  return value as number;
}

function sealOrder(
  state: State,
  column: unknown,
  direction: unknown,
  nulls: unknown,
): QueryOrder {
  const by = sealColumnName(column);
  if (!outputNames(state).includes(by))
    invalid(`orderBy references ${by}, which is not projected`);
  if (
    direction !== undefined &&
    !DIRECTIONS.has(direction as QueryOrderDirection)
  )
    invalid("orderBy direction must be asc or desc");
  if (nulls !== undefined && !NULL_PLACEMENTS.has(nulls as QueryNullPlacement))
    invalid("orderBy null placement must be first or last");
  if (state.orderBy.some((order) => order.by === by))
    invalid(`orderBy already orders by ${by}`);
  return Object.freeze({
    by,
    direction: (direction ?? "asc") as QueryOrderDirection,
    ...(nulls === undefined ? {} : { nulls: nulls as QueryNullPlacement }),
  });
}

function sealAlias(value: unknown, fallback?: string): string {
  if (value === undefined && fallback !== undefined) return fallback;
  const alias = sealColumnName(value);
  if (alias.includes(".")) invalid("an output name cannot contain a dot");
  return alias;
}

function aggregate(
  state: State,
  port: FacadePort,
  operator: QueryAggregateOperator,
  column: unknown,
  alias: unknown,
): State {
  let name: string | undefined;
  if (column !== undefined) name = resolve(state, port, column as string).name;
  const as = sealAlias(alias, operator === "count" ? "count" : name);
  // An ungrouped query's first aggregate replaces its projection.
  if (isGrouped(state)) assertFreshOutputName(state, as);
  return {
    ...state,
    select: undefined,
    aggregates: [
      ...state.aggregates,
      {
        operator,
        ...(column === undefined ? {} : { column: column as string }),
        as,
      },
    ],
  };
}

function next(state: State, patch: Partial<State>): State {
  return Object.freeze({ ...state, ...patch });
}

/**
 * The runtime shape behind both typed builder interfaces. Parameters arrive
 * as `unknown` because the interfaces' generics exist for application code;
 * every value is sealed here before it reaches the state.
 */
type RuntimeBuilder = Readonly<Record<string, (...args: never[]) => unknown>>;

async function run(
  state: State,
  port: FacadePort,
  options: unknown,
): Promise<readonly Readonly<Record<string, unknown>>[]> {
  const sealed = sealRunOptions(options);
  const { specification, readers } = compile(state, port);
  return materializeRows(await port.runQuery(specification, sealed), readers);
}

async function window(
  state: State,
  port: FacadePort,
  offset: unknown,
  count: unknown,
  options: unknown,
): Promise<{
  readonly offset: number;
  readonly total: number;
  readonly rows: readonly Readonly<Record<string, unknown>>[];
}> {
  const bounds = sealWindow(offset, count);
  const sealed = sealWindowOptions(options);
  const { specification, readers } = compile(state, port);
  const result = await port.runWindow(specification, bounds, sealed);
  return Object.freeze({
    offset: result.offset,
    total: result.total,
    rows: materializeRows(result, readers),
  });
}

function subscribe(state: State, port: FacadePort, options: unknown) {
  const sealed = sealLiveOptions(options);
  const { specification, readers } = compile(state, port);
  return port.subscribeQuery(
    specification,
    dependencies(state),
    sealed,
    readers,
  );
}

function subscribeWindow(
  state: State,
  port: FacadePort,
  offset: unknown,
  count: unknown,
  options: unknown,
) {
  const bounds = sealWindow(offset, count);
  const sealed = sealLiveWindowOptions(options);
  const { specification, readers } = compile(state, port);
  return port.subscribeWindow(
    specification,
    dependencies(state),
    bounds,
    sealed,
    readers,
  );
}

function order(
  state: State,
  column: unknown,
  direction: unknown,
  nulls: unknown,
): State {
  return next(state, {
    orderBy: [...state.orderBy, sealOrder(state, column, direction, nulls)],
  });
}

function grouped(
  state: State,
  port: FacadePort,
): GroupedQueryBuilder<object, object> {
  const view: RuntimeBuilder = {
    count: (alias?: unknown) =>
      grouped(aggregate(state, port, "count", undefined, alias), port),
    sum: (column: unknown, alias?: unknown) =>
      grouped(aggregate(state, port, "sum", column, alias), port),
    min: (column: unknown, alias?: unknown) =>
      grouped(aggregate(state, port, "min", column, alias), port),
    max: (column: unknown, alias?: unknown) =>
      grouped(aggregate(state, port, "max", column, alias), port),
    orderBy: (column: unknown, direction?: unknown, nulls?: unknown) =>
      grouped(order(state, column, direction, nulls), port),
    limit: (count: unknown) =>
      grouped(next(state, { limit: sealLimit(count) }), port),
    run: (options?: unknown) => run(state, port, options),
    window: (offset: unknown, count: unknown, options?: unknown) =>
      window(state, port, offset, count, options),
    subscribe: (options?: unknown) => subscribe(state, port, options),
    subscribeWindow: (offset: unknown, count: unknown, options?: unknown) =>
      subscribeWindow(state, port, offset, count, options),
  };
  return Object.freeze(view) as unknown as GroupedQueryBuilder<object, object>;
}

function sealColumns(columns: readonly unknown[], label: string): string[] {
  if (columns.length === 0) invalid(`${label} needs at least one column`);
  return columns.map((column) => sealColumnName(column));
}

function builder(state: State, port: FacadePort): QueryBuilder<object, object> {
  const view: RuntimeBuilder = {
    where: (predicate: unknown) => {
      const node = resolvePredicate(predicate);
      // Resolving now surfaces an unknown column at the call, not at run().
      compilePredicate(state, port, node);
      return builder(next(state, { where: [...state.where, node] }), port);
    },
    select: (...columns: readonly unknown[]) => {
      if (isGrouped(state))
        invalid("select cannot follow groupBy or an aggregate");
      const projections: Projection[] = [];
      const seen = new Set<string>();
      for (const column of sealColumns(columns, "select")) {
        const resolved = resolve(state, port, column);
        if (seen.has(resolved.name))
          invalid(
            `output name ${resolved.name} is projected twice; use selectAs`,
          );
        seen.add(resolved.name);
        projections.push({ column, as: resolved.name });
      }
      return builder(next(state, { select: projections }), port);
    },
    selectAs: (column: unknown, alias: unknown) => {
      if (isGrouped(state))
        invalid("selectAs cannot follow groupBy or an aggregate");
      const reference = sealColumnName(column);
      resolve(state, port, reference);
      const as = sealAlias(alias);
      // Without a select, the result already holds every column of the
      // collection, and the alias joins them.
      const current =
        state.select ??
        [...state.columns.keys()].map((name) => ({ column: name, as: name }));
      if (current.some((projection) => projection.as === as))
        invalid(`output name ${as} is already projected`);
      return builder(
        next(state, { select: [...current, { column: reference, as }] }),
        port,
      );
    },
    join: (
      collection: unknown,
      alias: unknown,
      on: unknown,
      kind?: unknown,
    ) => {
      const target = collectionOfFacade(collection);
      const as = sealAlias(alias);
      if (as === state.collection || state.joins.some((join) => join.as === as))
        invalid(`join alias ${as} is already in use`);
      const joinKind = (kind ?? "inner") as QueryJoinKind;
      if (!JOIN_KINDS.has(joinKind)) invalid("join kind must be inner or left");
      if (typeof on !== "object" || on === null)
        invalid("join needs an on clause");
      const { left, right } = on as { left?: unknown; right?: unknown };
      const leftReference = sealColumnName(left);
      const leftKind = resolve(state, port, leftReference).kind;
      const rightColumn = sealColumnName(right);
      const targetColumns = port.columnsOf(target);
      if (!targetColumns) invalid(`${target} is not served by this client`);
      const rightKind = targetColumns.get(rightColumn);
      if (rightKind === undefined)
        invalid(`${target} has no column ${rightColumn}`);
      if (joinKey(leftKind) !== joinKey(rightKind))
        invalid(
          `join compares ${leftReference} (${leftKind}) with ${target}.${rightColumn} (${rightKind}), so no row would match`,
        );
      return builder(
        next(state, {
          joins: [
            ...state.joins,
            {
              collection: target,
              as,
              kind: joinKind,
              left: leftReference,
              right: rightColumn,
            },
          ],
        }),
        port,
      );
    },
    groupBy: (...columns: readonly unknown[]) => {
      if (isGrouped(state)) invalid("groupBy was already applied");
      const sealed = sealColumns(columns, "groupBy");
      const seen = new Set<string>();
      for (const column of sealed) {
        const resolved = resolve(state, port, column);
        if (seen.has(resolved.name))
          invalid(`output name ${resolved.name} is projected twice`);
        seen.add(resolved.name);
      }
      return grouped(next(state, { select: undefined, groupBy: sealed }), port);
    },
    count: (alias?: unknown) =>
      grouped(aggregate(state, port, "count", undefined, alias), port),
    sum: (column: unknown, alias?: unknown) =>
      grouped(aggregate(state, port, "sum", column, alias), port),
    min: (column: unknown, alias?: unknown) =>
      grouped(aggregate(state, port, "min", column, alias), port),
    max: (column: unknown, alias?: unknown) =>
      grouped(aggregate(state, port, "max", column, alias), port),
    orderBy: (column: unknown, direction?: unknown, nulls?: unknown) =>
      builder(order(state, column, direction, nulls), port),
    limit: (count: unknown) =>
      builder(next(state, { limit: sealLimit(count) }), port),
    run: (options?: unknown) => run(state, port, options),
    window: (offset: unknown, count: unknown, options?: unknown) =>
      window(state, port, offset, count, options),
    subscribe: (options?: unknown) => subscribe(state, port, options),
    subscribeWindow: (offset: unknown, count: unknown, options?: unknown) =>
      subscribeWindow(state, port, offset, count, options),
  };
  return Object.freeze(view) as unknown as QueryBuilder<object, object>;
}

export function createQueryBuilder(
  port: FacadePort,
  collection: string,
  columns: ReadonlyMap<string, GeneratedClientColumnKind>,
): QueryBuilder<object, object> {
  return builder(
    Object.freeze({
      collection,
      columns,
      joins: [],
      where: [],
      select: undefined,
      groupBy: [],
      aggregates: [],
      orderBy: [],
      limit: undefined,
    }),
    port,
  );
}
