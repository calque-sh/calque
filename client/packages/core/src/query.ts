import {
  directColumnarTimestampText,
  equalBytes,
  timestampTextOrder,
  type ArtifactBlockCompare,
  type ArtifactEncodedColumn,
} from "./artifact.js";
import { ByteKeyTable, ByteStringTable, GroupKeyTable } from "./query-keys.js";
import {
  snapshotOverlayForQuery,
  type OverlayEngine,
  type OverlayQuerySnapshot,
  type OverlayQueryValue,
  type SqlValue,
} from "./overlay.js";

const encoder = new TextEncoder();
const MISSING_ADDRESS = 0xffff_ffff;

// Intrinsics captured once, so byte-literal validation cannot be steered by an
// input object's own properties or prototype.
const TYPED_ARRAY_PROTOTYPE = Object.getPrototypeOf(
  Uint8Array.prototype,
) as object;
const TYPED_ARRAY_TAG = intrinsic(
  Object.getOwnPropertyDescriptor(TYPED_ARRAY_PROTOTYPE, Symbol.toStringTag)
    ?.get,
);
const TYPED_ARRAY_BYTE_LENGTH = intrinsic(
  Object.getOwnPropertyDescriptor(TYPED_ARRAY_PROTOTYPE, "byteLength")?.get,
);
function intrinsic(value: unknown): (...args: never[]) => unknown {
  if (typeof value !== "function")
    throw new Error("typed-array intrinsics are unavailable");
  return value as (...args: never[]) => unknown;
}

export const MAX_QUERY_SPEC_BYTES = 64 * 1024;
export const MAX_QUERY_RESULT_ROWS = 10_000;
export const MAX_QUERY_RESULT_BYTES = 16 * 1024 * 1024;
export const DEFAULT_QUERY_RESULT_ROWS = 1_000;
export const DEFAULT_QUERY_RESULT_BYTES = 4 * 1024 * 1024;

const MAX_QUERY_JOINS = 8;
const MAX_QUERY_PROJECTIONS = 256;
const MAX_QUERY_GROUP_FIELDS = 64;
const MAX_QUERY_SORTS = 64;
const MAX_QUERY_PREDICATE_NODES = 1_024;
const MAX_QUERY_PREDICATE_DEPTH = 64;
const MAX_QUERY_RESERVATIONS = 256;
const RESERVATION_METADATA_BYTES = 512;
/** One entry of the hash index a join builds over its right source. */
const JOIN_INDEX_ROW_BYTES = 96;

/**
 * Execution as a generator that yields between bounded pieces of work, so a
 * caller can stop a long query between two of them and let other work run. A
 * yield carries nothing: everything the query needs stays in the generator.
 */
type Sliced<T> = Generator<void, T, void>;

/**
 * Rows a sliced loop handles between two yields: well under a millisecond of
 * the slowest per-row work, and few enough yields that asking the clock at
 * each costs nothing measurable.
 */
const SLICE_ROWS = 4_096;

/**
 * How many times a task restarts because a collection it reads moved before
 * it finished. The attempt after the last restart runs to completion in one
 * step, so a query under a steady stream of changes costs at most one
 * unsliced execution rather than never finishing.
 */
export const MAX_QUERY_RESTARTS = 3;

export type QueryErrorCode =
  "INVALID_QUERY" | "CAPACITY_EXCEEDED" | "RESULT_TOO_LARGE";

export class QueryError extends Error {
  constructor(
    readonly code: QueryErrorCode,
    message: string,
    readonly requiredBytes?: number,
    readonly capacityBytes?: number,
  ) {
    super(`${code}: ${message}`);
    this.name = "QueryError";
  }
}

/**
 * A literal value. Binary values are `Uint8Array` in literals, rows and
 * projections alike, with no JSON spelling: specifications and results cross
 * `postMessage`, whose structured clone keeps `Uint8Array` and `bigint`.
 */
export type QueryLiteral =
  null | boolean | number | bigint | string | Uint8Array;

export type QueryJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly QueryJsonValue[]
  | { readonly [key: string]: QueryJsonValue };

export type QueryOutputValue = QueryLiteral | QueryJsonValue;

type QueryValue = OverlayQueryValue;

export interface QueryField {
  readonly source: string;
  readonly column: string;
}

export type QueryExpression =
  | { readonly kind: "field"; readonly field: QueryField }
  | { readonly kind: "literal"; readonly value: QueryLiteral };

export type QueryComparisonOperator =
  "eq" | "neq" | "lt" | "lte" | "gt" | "gte";

export type QueryPredicate =
  | {
      readonly kind: "compare";
      readonly operator: QueryComparisonOperator;
      readonly left: QueryExpression;
      readonly right: QueryExpression;
    }
  | {
      readonly kind: "is-null";
      readonly expression: QueryExpression;
      readonly negated?: boolean;
    }
  | { readonly kind: "and"; readonly predicates: readonly QueryPredicate[] }
  | { readonly kind: "or"; readonly predicates: readonly QueryPredicate[] }
  | { readonly kind: "not"; readonly predicate: QueryPredicate };

export interface QuerySource {
  readonly collection: string;
  readonly as: string;
}

export interface QueryJoin {
  readonly kind: "inner" | "left";
  readonly collection: string;
  readonly as: string;
  /** The join condition: `left` equals `right`. */
  readonly on: {
    readonly left: QueryField;
    readonly right: QueryField;
  };
}

export type QueryAggregateOperator = "count" | "sum" | "min" | "max";

export type QueryProjection =
  | { readonly kind: "field"; readonly field: QueryField; readonly as: string }
  | {
      readonly kind: "aggregate";
      readonly operator: QueryAggregateOperator;
      /** Omitted only for `count(*)`. */
      readonly field?: QueryField;
      readonly as: string;
    };

export interface QueryOrder {
  /** A projection alias, so aggregate results can be ordered too. */
  readonly by: string;
  readonly direction: "asc" | "desc";
  /** Null placement is independent of direction. */
  readonly nulls?: "first" | "last";
}

export interface QuerySpecification {
  readonly version: 1;
  readonly from: QuerySource;
  readonly joins?: readonly QueryJoin[];
  readonly where?: QueryPredicate;
  readonly select: readonly QueryProjection[];
  readonly groupBy?: readonly QueryField[];
  readonly orderBy?: readonly QueryOrder[];
  readonly limit?: number;
}

export interface ValidatedQuerySpecification extends QuerySpecification {
  readonly joins: readonly QueryJoin[];
  readonly groupBy: readonly QueryField[];
  readonly orderBy: readonly QueryOrder[];
}

export interface QueryResult {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly QueryOutputValue[])[];
}

export interface ColumnarQueryCapacity {
  /** Aggregate pool carved from the activation's immutable memory plan. */
  readonly capacityBytes: number;
  readonly maxReservations?: number;
}

export interface QueryExecutionOptions {
  readonly maxRows?: number;
  readonly maxBytes?: number;
}

interface NormalizedQueryOptions {
  readonly maxRows: number;
  readonly maxBytes: number;
}

interface ValidationState {
  predicateNodes: number;
  readonly ancestors: Set<object>;
}

function invalid(message: string): never {
  throw new QueryError("INVALID_QUERY", message);
}

function capacity(
  message: string,
  required?: number,
  available?: number,
): never {
  throw new QueryError("CAPACITY_EXCEEDED", message, required, available);
}

function boundedName(value: unknown, label: string): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.includes("\0") ||
    encoder.encode(value).byteLength > 255
  )
    invalid(`${label} must contain 1..255 UTF-8 bytes and no NUL`);
  return value;
}

function dataRecord(
  value: unknown,
  required: readonly string[],
  optional: readonly string[],
  label: string,
  state: ValidationState,
): Readonly<Record<string, unknown>> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    invalid(`${label} must be a data object`);
  let prototype: object | null;
  let keys: readonly PropertyKey[];
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
  } catch {
    invalid(`${label} must be inert data`);
  }
  if (prototype !== Object.prototype && prototype !== null)
    invalid(`${label} must be a plain data object`);
  if (state.ancestors.has(value)) invalid(`${label} must not be cyclic`);
  const allowed = new Set([...required, ...optional]);
  if (
    keys.some((key) => typeof key !== "string" || !allowed.has(key)) ||
    required.some((key) => !keys.includes(key))
  )
    invalid(`${label} has unknown or missing fields`);
  const out: Record<string, unknown> = Object.create(null) as Record<
    string,
    unknown
  >;
  state.ancestors.add(value);
  try {
    for (const key of keys) {
      if (typeof key !== "string") invalid(`${label} has a symbol field`);
      let descriptor: PropertyDescriptor | undefined;
      try {
        descriptor = Object.getOwnPropertyDescriptor(value, key);
      } catch {
        invalid(`${label} must be inert data`);
      }
      if (!descriptor || !("value" in descriptor))
        invalid(`${label}.${key} must be a data property`);
      out[key] = descriptor.value;
    }
  } finally {
    state.ancestors.delete(value);
  }
  return out;
}

function dataArray(
  value: unknown,
  maximum: number,
  label: string,
): readonly unknown[] {
  if (!Array.isArray(value) || value.length > maximum)
    invalid(`${label} must be an array of at most ${maximum} items`);
  const out: unknown[] = [];
  for (let index = 0; index < value.length; index++) {
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    } catch {
      invalid(`${label} must be inert data`);
    }
    if (!descriptor || !("value" in descriptor))
      invalid(`${label} must be dense and contain only data properties`);
    out.push(descriptor.value);
  }
  return out;
}

/**
 * A byte literal must be a real `Uint8Array`. The tag and `byteLength` getters
 * read internal slots, so a proxy or look-alike is rejected here rather than at
 * a later read.
 */
function validateBytes(value: unknown): Uint8Array {
  try {
    if (
      Reflect.apply(TYPED_ARRAY_TAG, value, []) !== "Uint8Array" ||
      !Number.isSafeInteger(Reflect.apply(TYPED_ARRAY_BYTE_LENGTH, value, []))
    )
      invalid("query literal must be a Uint8Array or a JSON scalar");
  } catch (error) {
    if (error instanceof QueryError) throw error;
    invalid("query literal must be a Uint8Array or a JSON scalar");
  }
  // Copied through the base constructor rather than `slice`, which a subclass
  // could redirect: the caller keeps its own buffer, and a resizable or shared
  // one could otherwise change under an already validated specification.
  return new Uint8Array(value as Uint8Array);
}

function validateLiteral(value: unknown): QueryLiteral {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "string"
  )
    return value;
  return validateBytes(value);
}

function validateField(
  value: unknown,
  label: string,
  state: ValidationState,
): QueryField {
  const raw = dataRecord(value, ["source", "column"], [], label, state);
  return Object.freeze({
    source: boundedName(raw.source, `${label}.source`),
    column: boundedName(raw.column, `${label}.column`),
  });
}

function validateExpression(
  value: unknown,
  label: string,
  state: ValidationState,
): QueryExpression {
  const raw = dataRecord(value, ["kind"], ["field", "value"], label, state);
  if (raw.kind === "field" && Object.keys(raw).length === 2)
    return Object.freeze({
      kind: "field",
      field: validateField(raw.field, `${label}.field`, state),
    });
  if (raw.kind === "literal" && Object.keys(raw).length === 2)
    return Object.freeze({
      kind: "literal",
      value: validateLiteral(raw.value),
    });
  invalid(`${label} has an invalid expression kind`);
}

function validatePredicate(
  value: unknown,
  depth: number,
  state: ValidationState,
): QueryPredicate {
  if (depth > MAX_QUERY_PREDICATE_DEPTH)
    invalid(`predicate depth exceeds ${MAX_QUERY_PREDICATE_DEPTH}`);
  state.predicateNodes++;
  if (state.predicateNodes > MAX_QUERY_PREDICATE_NODES)
    invalid(`predicate nodes exceed ${MAX_QUERY_PREDICATE_NODES}`);
  const raw = dataRecord(
    value,
    ["kind"],
    [
      "operator",
      "left",
      "right",
      "expression",
      "negated",
      "predicates",
      "predicate",
    ],
    "predicate",
    state,
  );
  if (raw.kind === "compare") {
    if (
      !["eq", "neq", "lt", "lte", "gt", "gte"].includes(String(raw.operator)) ||
      Object.keys(raw).length !== 4
    )
      invalid("comparison predicate is invalid");
    const left = validateExpression(raw.left, "predicate.left", state);
    const right = validateExpression(raw.right, "predicate.right", state);
    // A comparison with NULL is never true, so the literal could only be a
    // mistake for a NULL test.
    if (
      (left.kind === "literal" && left.value === null) ||
      (right.kind === "literal" && right.value === null)
    )
      invalid(
        "comparison literal must not be null; test for NULL with is-null (isNull or isNotNull)",
      );
    return Object.freeze({
      kind: "compare",
      operator: raw.operator as QueryComparisonOperator,
      left,
      right,
    });
  }
  if (raw.kind === "is-null") {
    if (
      ![2, 3].includes(Object.keys(raw).length) ||
      (raw.negated !== undefined && typeof raw.negated !== "boolean")
    )
      invalid("is-null predicate is invalid");
    return Object.freeze({
      kind: "is-null",
      expression: validateExpression(
        raw.expression,
        "predicate.expression",
        state,
      ),
      ...(raw.negated === undefined ? {} : { negated: raw.negated }),
    });
  }
  if (raw.kind === "and" || raw.kind === "or") {
    if (Object.keys(raw).length !== 2) invalid("logical predicate is invalid");
    const predicates = dataArray(
      raw.predicates,
      MAX_QUERY_PREDICATE_NODES,
      "predicate.predicates",
    );
    if (predicates.length === 0)
      invalid("logical predicate must contain at least one child");
    return Object.freeze({
      kind: raw.kind,
      predicates: Object.freeze(
        predicates.map((child) => validatePredicate(child, depth + 1, state)),
      ),
    });
  }
  if (raw.kind === "not" && Object.keys(raw).length === 2)
    return Object.freeze({
      kind: "not",
      predicate: validatePredicate(raw.predicate, depth + 1, state),
    });
  invalid("predicate kind is invalid");
}

function validateSource(
  value: unknown,
  label: string,
  state: ValidationState,
): QuerySource {
  const raw = dataRecord(value, ["collection", "as"], [], label, state);
  return Object.freeze({
    collection: boundedName(raw.collection, `${label}.collection`),
    as: boundedName(raw.as, `${label}.as`),
  });
}

function validateJoin(value: unknown, state: ValidationState): QueryJoin {
  const raw = dataRecord(
    value,
    ["kind", "collection", "as", "on"],
    [],
    "join",
    state,
  );
  if (raw.kind !== "inner" && raw.kind !== "left")
    invalid("join kind must be inner or left");
  const on = dataRecord(raw.on, ["left", "right"], [], "join.on", state);
  return Object.freeze({
    kind: raw.kind,
    collection: boundedName(raw.collection, "join.collection"),
    as: boundedName(raw.as, "join.as"),
    on: Object.freeze({
      left: validateField(on.left, "join.on.left", state),
      right: validateField(on.right, "join.on.right", state),
    }),
  });
}

function validateProjection(
  value: unknown,
  state: ValidationState,
): QueryProjection {
  const raw = dataRecord(
    value,
    ["kind", "as"],
    ["field", "operator"],
    "projection",
    state,
  );
  const as = boundedName(raw.as, "projection.as");
  if (raw.kind === "field" && Object.keys(raw).length === 3)
    return Object.freeze({
      kind: "field",
      field: validateField(raw.field, "projection.field", state),
      as,
    });
  if (raw.kind === "aggregate") {
    if (!Object.hasOwn(raw, "operator"))
      invalid("aggregate operator is missing");
    if (!["count", "sum", "min", "max"].includes(String(raw.operator)))
      invalid("aggregate operator is invalid");
    if (raw.field === undefined && raw.operator !== "count")
      invalid("only count may omit its field");
    return Object.freeze({
      kind: "aggregate",
      operator: raw.operator as QueryAggregateOperator,
      ...(raw.field === undefined
        ? {}
        : { field: validateField(raw.field, "projection.field", state) }),
      as,
    });
  }
  invalid("projection kind is invalid");
}

function validateOrder(value: unknown, state: ValidationState): QueryOrder {
  const raw = dataRecord(value, ["by", "direction"], ["nulls"], "order", state);
  if (raw.direction !== "asc" && raw.direction !== "desc")
    invalid("order direction must be asc or desc");
  if (raw.nulls !== undefined && raw.nulls !== "first" && raw.nulls !== "last")
    invalid("order null placement must be first or last");
  return Object.freeze({
    by: boundedName(raw.by, "order.by"),
    direction: raw.direction,
    ...(raw.nulls === undefined ? {} : { nulls: raw.nulls }),
  });
}

/** Validates, normalizes, and deeply snapshots an inert query specification. */
export function validateQuerySpecification(
  input: unknown,
): ValidatedQuerySpecification {
  return canonicalized(input).specification;
}

/**
 * The text two specifications share exactly when they ask for the same query:
 * the validated, normalized specification in one encoding. It throws what
 * `validateQuerySpecification` throws.
 */
export function canonicalQuerySpecification(input: unknown): string {
  return canonicalized(input).canonical;
}

function canonicalized(input: unknown): {
  readonly specification: ValidatedQuerySpecification;
  readonly canonical: string;
} {
  const state: ValidationState = { predicateNodes: 0, ancestors: new Set() };
  const specification = validateQueryInternal(input, state);
  const canonical = canonicalQuery(specification);
  if (encoder.encode(canonical).byteLength > MAX_QUERY_SPEC_BYTES)
    invalid(`query specification exceeds ${MAX_QUERY_SPEC_BYTES} bytes`);
  return { specification, canonical };
}

function validateQueryInternal(
  input: unknown,
  state: ValidationState,
): ValidatedQuerySpecification {
  const raw = dataRecord(
    input,
    ["version", "from", "select"],
    ["joins", "where", "groupBy", "orderBy", "limit"],
    "query",
    state,
  );
  if (raw.version !== 1) invalid("query version must be 1");
  const joins = dataArray(raw.joins ?? [], MAX_QUERY_JOINS, "query.joins").map(
    (join) => validateJoin(join, state),
  );
  const select = dataArray(
    raw.select,
    MAX_QUERY_PROJECTIONS,
    "query.select",
  ).map((projection) => validateProjection(projection, state));
  if (select.length === 0) invalid("query.select must not be empty");
  const groupBy = dataArray(
    raw.groupBy ?? [],
    MAX_QUERY_GROUP_FIELDS,
    "query.groupBy",
  ).map((field) => validateField(field, "query.groupBy field", state));
  const orderBy = dataArray(
    raw.orderBy ?? [],
    MAX_QUERY_SORTS,
    "query.orderBy",
  ).map((order) => validateOrder(order, state));
  if (
    raw.limit !== undefined &&
    (!Number.isSafeInteger(raw.limit) || (raw.limit as number) < 0)
  )
    invalid("query.limit must be a non-negative safe integer");
  const result: ValidatedQuerySpecification = Object.freeze({
    version: 1,
    from: validateSource(raw.from, "query.from", state),
    joins: Object.freeze(joins),
    ...(raw.where === undefined
      ? {}
      : { where: validatePredicate(raw.where, 1, state) }),
    select: Object.freeze(select),
    groupBy: Object.freeze(groupBy),
    orderBy: Object.freeze(orderBy),
    ...(raw.limit === undefined ? {} : { limit: raw.limit as number }),
  });
  return result;
}

function canonicalQuery(specification: ValidatedQuerySpecification): string {
  return JSON.stringify(specification, (_key, value: unknown) => {
    if (typeof value === "bigint") return { $bigint: String(value) };
    if (value instanceof Uint8Array) return { $bytes: hex(value) };
    if (typeof value === "number") {
      if (Number.isNaN(value)) return { $number: "nan" };
      if (value === Number.POSITIVE_INFINITY) return { $number: "+infinity" };
      if (value === Number.NEGATIVE_INFINITY) return { $number: "-infinity" };
      if (Object.is(value, -0)) return { $number: "-0" };
    }
    return value;
  });
}

interface CompiledField {
  readonly source: number;
  readonly column: number;
  readonly key: string;
}

type CompiledExpression =
  | { readonly kind: "field"; readonly field: CompiledField }
  | { readonly kind: "literal"; readonly value: QueryValue };

type CompiledPredicate =
  | {
      readonly kind: "compare";
      readonly operator: QueryComparisonOperator;
      readonly left: CompiledExpression;
      readonly right: CompiledExpression;
    }
  | {
      readonly kind: "is-null";
      readonly expression: CompiledExpression;
      readonly negated: boolean;
    }
  | {
      readonly kind: "and";
      readonly predicates: readonly CompiledPredicate[];
    }
  | {
      readonly kind: "or";
      readonly predicates: readonly CompiledPredicate[];
    };

interface CompiledJoin {
  readonly kind: "inner" | "left";
  readonly left: CompiledField;
  readonly right: CompiledField;
  readonly rightSource: number;
}

type CompiledProjection =
  | {
      readonly kind: "field";
      readonly field: CompiledField;
      readonly as: string;
    }
  | {
      readonly kind: "aggregate";
      readonly operator: QueryAggregateOperator;
      readonly field?: CompiledField;
      readonly as: string;
    };

interface CompiledOrder {
  readonly column: number;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
}

interface CompiledQuery {
  readonly joins: readonly CompiledJoin[];
  readonly predicate?: CompiledPredicate;
  readonly projections: readonly CompiledProjection[];
  readonly groupBy: readonly CompiledField[];
  readonly orders: readonly CompiledOrder[];
  readonly aggregateMode: boolean;
}

function fieldIdentity(field: QueryField): string {
  return `${field.source.length}:${field.source}${field.column.length}:${field.column}`;
}

/** The operator whose comparison is the negation of the given one's. */
const COMPLEMENT_OPERATOR: Readonly<
  Record<QueryComparisonOperator, QueryComparisonOperator>
> = {
  eq: "neq",
  neq: "eq",
  lt: "gte",
  gte: "lt",
  lte: "gt",
  gt: "lte",
};

function compileQuery(
  specification: ValidatedQuerySpecification,
  snapshots: readonly OverlayQuerySnapshot[],
): CompiledQuery {
  const aliases = [
    specification.from.as,
    ...specification.joins.map((join) => join.as),
  ];
  if (new Set(aliases).size !== aliases.length)
    invalid("query source aliases must be unique");
  const sourceByAlias = new Map(aliases.map((alias, index) => [alias, index]));
  const columns = snapshots.map(
    (snapshot) => new Map(snapshot.columns.map((name, index) => [name, index])),
  );
  const compileField = (field: QueryField): CompiledField => {
    const source = sourceByAlias.get(field.source);
    if (source === undefined) invalid(`unknown query source ${field.source}`);
    const column = columns[source]?.get(field.column);
    if (column === undefined)
      invalid(`unknown column ${field.source}.${field.column}`);
    return { source, column, key: fieldIdentity(field) };
  };
  const compileExpression = (
    expression: QueryExpression,
  ): CompiledExpression =>
    expression.kind === "field"
      ? { kind: "field", field: compileField(expression.field) }
      : { kind: "literal", value: expression.value };
  // Predicates follow SQL's three-valued logic: a comparison with a NULL
  // operand is UNKNOWN, and a row is selected only when the predicate is TRUE.
  // Compiling pushes every `not` onto the leaves, after which Kleene `and` and
  // `or` are monotone, so the predicate is TRUE exactly when it is TRUE with
  // every UNKNOWN leaf read as false: the evaluators stay two-valued, with a
  // leaf false whenever it is UNKNOWN. Complementing an operator is exact
  // because `sameTypeCompare` is a total order within a type (NaN greatest, -0
  // below 0). An ordering comparison `sameTypeCompare` cannot decide, as
  // across types, is false under an operator and its complement alike, so it
  // behaves as UNKNOWN; `eq` and `neq` across types stay two-valued, unequal.
  const compilePredicate = (
    predicate: QueryPredicate,
    negated: boolean,
  ): CompiledPredicate => {
    if (predicate.kind === "not")
      return compilePredicate(predicate.predicate, !negated);
    if (predicate.kind === "compare")
      return {
        kind: "compare",
        operator: negated
          ? COMPLEMENT_OPERATOR[predicate.operator]
          : predicate.operator,
        left: compileExpression(predicate.left),
        right: compileExpression(predicate.right),
      };
    if (predicate.kind === "is-null")
      return {
        kind: "is-null",
        expression: compileExpression(predicate.expression),
        negated: (predicate.negated ?? false) !== negated,
      };
    return {
      kind: negated
        ? predicate.kind === "and"
          ? "or"
          : "and"
        : predicate.kind,
      predicates: predicate.predicates.map((child) =>
        compilePredicate(child, negated),
      ),
    };
  };
  const joins = specification.joins.map((join, index): CompiledJoin => {
    const left = compileField(join.on.left);
    const right = compileField(join.on.right);
    const rightSource = index + 1;
    if (left.source >= rightSource || right.source !== rightSource)
      invalid("join.on must connect a prior source to the source being joined");
    return { kind: join.kind, left, right, rightSource };
  });
  const projections = specification.select.map(
    (projection): CompiledProjection =>
      projection.kind === "field"
        ? {
            kind: "field",
            field: compileField(projection.field),
            as: projection.as,
          }
        : {
            kind: "aggregate",
            operator: projection.operator,
            ...(projection.field === undefined
              ? {}
              : { field: compileField(projection.field) }),
            as: projection.as,
          },
  );
  const aliasesSeen = new Set<string>();
  for (const projection of projections) {
    if (aliasesSeen.has(projection.as))
      invalid(`duplicate projection alias ${projection.as}`);
    aliasesSeen.add(projection.as);
  }
  const groupBy = specification.groupBy.map(compileField);
  if (new Set(groupBy.map((field) => field.key)).size !== groupBy.length)
    invalid("groupBy fields must be unique");
  const aggregateMode =
    groupBy.length > 0 ||
    projections.some((projection) => projection.kind === "aggregate");
  if (aggregateMode) {
    const grouped = new Set(groupBy.map((field) => field.key));
    for (const projection of projections)
      if (projection.kind === "field" && !grouped.has(projection.field.key))
        invalid(
          `projected field ${projection.field.key} must occur in groupBy`,
        );
  }
  const orders = specification.orderBy.map((order): CompiledOrder => {
    const column = projections.findIndex(
      (projection) => projection.as === order.by,
    );
    if (column < 0) invalid(`order references unknown projection ${order.by}`);
    return {
      column,
      direction: order.direction,
      nulls: order.nulls ?? "last",
    };
  });
  return {
    joins,
    ...(specification.where === undefined
      ? {}
      : { predicate: compilePredicate(specification.where, false) }),
    projections,
    groupBy,
    orders,
    aggregateMode,
  };
}

interface AddressRelation {
  readonly vectors: readonly Uint32Array[];
  readonly rows: number;
}

interface QueryExecutionInspection {
  readonly rowsExamined: number;
  readonly chunksExamined: number;
  readonly chunksSkipped: number;
  readonly encodedComparisons: number;
  readonly materializedComparisons: number;
  /**
   * The left rows the plan's joins read, summed over its joins, with those
   * of a cut base whose joins left too few rows and the plan then read again.
   */
  readonly rowsJoined: number;
  readonly statisticsRowsScanned: number;
  readonly residentStatisticsBytes: number;
}

interface MutableQueryExecutionInspection {
  rowsExamined: number;
  chunksExamined: number;
  chunksSkipped: number;
  encodedComparisons: number;
  materializedComparisons: number;
  rowsJoined: number;
}

interface QueryExecutionFeatures {
  readonly chunkPruning: boolean;
  /**
   * Decide encoded predicates a block of rows at a time. Off, each row is
   * decided alone, with the same results and counters.
   */
  readonly blockFilter: boolean;
  /**
   * Cut the base source to a small limit's best rows before the joins, where
   * `limitBeforeJoins` says the base alone ranks them. Off, the joins read
   * every base row the filter keeps.
   */
  readonly limitBeforeJoins: boolean;
}

const DEFAULT_QUERY_FEATURES: QueryExecutionFeatures = Object.freeze({
  chunkPruning: true,
  blockFilter: true,
  limitBeforeJoins: true,
});
const QUERY_FEATURES = new WeakMap<
  ColumnarQueryEngine,
  QueryExecutionFeatures
>();

/** Switches execution features off for tests and measurements. */
export function configureQueryMeasurement(
  engine: ColumnarQueryEngine,
  features: Partial<QueryExecutionFeatures>,
): void {
  QUERY_FEATURES.set(engine, {
    ...DEFAULT_QUERY_FEATURES,
    ...features,
  });
}

const QUERY_INSPECTORS = new WeakMap<
  ColumnarQueryEngine,
  QueryExecutionInspection
>();

/** The execution counters, for tests and measurements. */
export function inspectQueryExecution(
  engine: ColumnarQueryEngine,
): QueryExecutionInspection {
  return (
    QUERY_INSPECTORS.get(engine) ?? {
      rowsExamined: 0,
      chunksExamined: 0,
      chunksSkipped: 0,
      encodedComparisons: 0,
      materializedComparisons: 0,
      rowsJoined: 0,
      statisticsRowsScanned: 0,
      residentStatisticsBytes: 0,
    }
  );
}

function relationValue(
  relation: AddressRelation,
  row: number,
  field: CompiledField,
  snapshots: readonly OverlayQuerySnapshot[],
): QueryValue {
  const address = relation.vectors[field.source]?.[row];
  if (address === undefined) invalid("query relation address is unavailable");
  if (address === MISSING_ADDRESS) return null;
  const snapshot = snapshots[field.source];
  if (!snapshot) invalid("query source is unavailable");
  return snapshot.valueAt(address, field.column);
}

function expressionValue(
  expression: CompiledExpression,
  relation: AddressRelation,
  row: number,
  snapshots: readonly OverlayQuerySnapshot[],
): QueryValue {
  return expression.kind === "literal"
    ? expression.value
    : relationValue(relation, row, expression.field, snapshots);
}

function exactEqual(left: QueryValue, right: QueryValue): boolean {
  if (left instanceof Uint8Array || right instanceof Uint8Array)
    return (
      left instanceof Uint8Array &&
      right instanceof Uint8Array &&
      equalBytes(left, right)
    );
  if (
    (typeof left === "object" && left !== null) ||
    (typeof right === "object" && right !== null)
  )
    return (
      typeof left === "object" &&
      left !== null &&
      typeof right === "object" &&
      right !== null &&
      canonicalJson(left) === canonicalJson(right)
    );
  return Object.is(left, right);
}

function compareNumber(left: number, right: number): number {
  const leftNan = Number.isNaN(left);
  const rightNan = Number.isNaN(right);
  if (leftNan || rightNan) return leftNan === rightNan ? 0 : leftNan ? 1 : -1;
  if (left === 0 && right === 0) {
    if (Object.is(left, right)) return 0;
    return Object.is(left, -0) ? -1 : 1;
  }
  return left < right ? -1 : left > right ? 1 : 0;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const shared = Math.min(left.length, right.length);
  for (let index = 0; index < shared; index++) {
    const difference = (left[index] ?? 0) - (right[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return left.length - right.length;
}

interface JsonWalkState {
  nodes: number;
  readonly ancestors: Set<object>;
}

function jsonNumberToken(value: number): string {
  if (Number.isNaN(value)) return "nan";
  if (value === Number.POSITIVE_INFINITY) return "+infinity";
  if (value === Number.NEGATIVE_INFINITY) return "-infinity";
  if (Object.is(value, -0)) return "-0";
  return String(value);
}

function jsonDataProperties(
  value: object,
  state: JsonWalkState,
): readonly (readonly [string, unknown])[] {
  state.nodes++;
  if (state.nodes > 100_000) invalid("JSON query value is too large");
  if (state.ancestors.has(value))
    invalid("JSON query value must not be cyclic");
  const prototype = Object.getPrototypeOf(value);
  if (
    !Array.isArray(value) &&
    prototype !== Object.prototype &&
    prototype !== null
  )
    invalid("JSON query value must contain only plain data objects");
  const properties: Array<readonly [string, unknown]> = [];
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor || !("value" in descriptor))
        invalid("JSON query arrays must be dense data arrays");
      properties.push([String(index), descriptor.value]);
    }
    return properties;
  }
  const keys = Reflect.ownKeys(value);
  if (keys.some((key) => typeof key !== "string"))
    invalid("JSON query objects must not contain symbol fields");
  for (const key of (keys as string[]).sort()) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !("value" in descriptor))
      invalid("JSON query objects must contain only data properties");
    properties.push([key, descriptor.value]);
  }
  return properties;
}

function canonicalJson(
  value: unknown,
  state: JsonWalkState = { nodes: 0, ancestors: new Set() },
): string {
  if (value === null) return "z";
  if (typeof value === "boolean") return value ? "t" : "f";
  if (typeof value === "number") return `n${jsonNumberToken(value)}`;
  if (typeof value === "string") return `s${value.length}:${value}`;
  if (typeof value !== "object" || value instanceof Uint8Array)
    invalid("JSON query value contains a non-JSON member");
  const properties = jsonDataProperties(value, state);
  state.ancestors.add(value);
  try {
    return Array.isArray(value)
      ? `a${properties
          .map((property) => {
            const child = property[1];
            const token = canonicalJson(child, state);
            return `${token.length}:${token}`;
          })
          .join("")}`
      : `o${properties
          .map(([key, child]) => {
            const token = canonicalJson(child, state);
            return `${key.length}:${key}${token.length}:${token}`;
          })
          .join("")}`;
  } finally {
    state.ancestors.delete(value);
  }
}

function sealJson(
  value: unknown,
  state: JsonWalkState = { nodes: 0, ancestors: new Set() },
): QueryJsonValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  )
    return value;
  if (typeof value !== "object" || value instanceof Uint8Array)
    invalid("JSON query value contains a non-JSON member");
  const properties = jsonDataProperties(value, state);
  state.ancestors.add(value);
  try {
    if (Array.isArray(value))
      return Object.freeze(
        properties.map((property) => sealJson(property[1], state)),
      );
    const result: Record<string, QueryJsonValue> = Object.create(
      null,
    ) as Record<string, QueryJsonValue>;
    for (const [key, child] of properties) result[key] = sealJson(child, state);
    return Object.freeze(result);
  } finally {
    state.ancestors.delete(value);
  }
}

function sameTypeCompare(
  left: QueryValue,
  right: QueryValue,
): number | undefined {
  if (left === null || right === null || typeof left !== typeof right)
    return undefined;
  if (typeof left === "number" && typeof right === "number")
    return compareNumber(left, right);
  if (typeof left === "bigint" && typeof right === "bigint")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "string" && typeof right === "string")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean")
    return Number(left) - Number(right);
  if (left instanceof Uint8Array && right instanceof Uint8Array)
    return compareBytes(left, right);
  if (
    typeof left === "object" &&
    left !== null &&
    typeof right === "object" &&
    right !== null
  ) {
    const a = canonicalJson(left);
    const b = canonicalJson(right);
    return a < b ? -1 : a > b ? 1 : 0;
  }
  return undefined;
}

function matchesPredicate(
  predicate: CompiledPredicate,
  relation: AddressRelation,
  row: number,
  snapshots: readonly OverlayQuerySnapshot[],
  inspection: MutableQueryExecutionInspection,
): boolean {
  if (predicate.kind === "and" || predicate.kind === "or")
    return predicate.kind === "and"
      ? predicate.predicates.every((child) =>
          matchesPredicate(child, relation, row, snapshots, inspection),
        )
      : predicate.predicates.some((child) =>
          matchesPredicate(child, relation, row, snapshots, inspection),
        );
  if (predicate.kind === "is-null") {
    if (predicate.expression.kind === "field") {
      const field = predicate.expression.field;
      const address = relation.vectors[field.source]?.[row];
      const snapshot = snapshots[field.source];
      if (address !== undefined && address !== MISSING_ADDRESS && snapshot) {
        const encoded = snapshot.compareEncodedAt(
          address,
          field.column,
          "eq",
          null,
        );
        if (encoded !== undefined) {
          inspection.encodedComparisons++;
          return predicate.negated ? !encoded : encoded;
        }
      }
    }
    inspection.materializedComparisons++;
    const result =
      expressionValue(predicate.expression, relation, row, snapshots) === null;
    return predicate.negated ? !result : result;
  }
  const encoded = encodedPredicateDecision(predicate, relation, row, snapshots);
  if (encoded !== undefined) {
    inspection.encodedComparisons++;
    return encoded;
  }
  inspection.materializedComparisons++;
  const left = expressionValue(predicate.left, relation, row, snapshots);
  const right = expressionValue(predicate.right, relation, row, snapshots);
  // UNKNOWN, which `compileQuery` lets a leaf read as false.
  if (left === null || right === null) return false;
  if (predicate.operator === "eq") return exactEqual(left, right);
  if (predicate.operator === "neq") return !exactEqual(left, right);
  const comparison = sameTypeCompare(left, right);
  if (comparison === undefined) return false;
  if (predicate.operator === "lt") return comparison < 0;
  if (predicate.operator === "lte") return comparison <= 0;
  if (predicate.operator === "gt") return comparison > 0;
  return comparison >= 0;
}

function reverseOperator(
  operator: QueryComparisonOperator,
): QueryComparisonOperator {
  if (operator === "lt") return "gt";
  if (operator === "lte") return "gte";
  if (operator === "gt") return "lt";
  if (operator === "gte") return "lte";
  return operator;
}

function encodedPredicateDecision(
  predicate: Extract<CompiledPredicate, { readonly kind: "compare" }>,
  relation: AddressRelation,
  row: number,
  snapshots: readonly OverlayQuerySnapshot[],
): boolean | undefined {
  const fieldExpression =
    predicate.left.kind === "field" && predicate.right.kind === "literal"
      ? predicate.left
      : predicate.right.kind === "field" && predicate.left.kind === "literal"
        ? predicate.right
        : undefined;
  const literalExpression =
    predicate.left.kind === "literal" && predicate.right.kind === "field"
      ? predicate.left
      : predicate.right.kind === "literal" && predicate.left.kind === "field"
        ? predicate.right
        : undefined;
  if (!fieldExpression || !literalExpression) return undefined;
  const field = fieldExpression.field;
  const address = relation.vectors[field.source]?.[row];
  const snapshot = snapshots[field.source];
  if (
    address === undefined ||
    address === MISSING_ADDRESS ||
    !snapshot ||
    (typeof literalExpression.value === "object" &&
      literalExpression.value !== null &&
      !(literalExpression.value instanceof Uint8Array))
  )
    return undefined;
  const operator =
    predicate.left.kind === "literal"
      ? reverseOperator(predicate.operator)
      : predicate.operator;
  return snapshot.compareEncodedAt(
    address,
    field.column,
    operator,
    literalExpression.value as SqlValue,
  );
}

function valueKey(value: QueryValue): string {
  if (value === null) return "z";
  if (typeof value === "boolean") return value ? "t" : "f";
  if (typeof value === "bigint") return `i${String(value)}`;
  if (typeof value === "string") return `s${value.length}:${value}`;
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "nnan";
    if (Object.is(value, -0)) return "n-0";
    return `n${String(value)}`;
  }
  if (value instanceof Uint8Array) return `x${bytesKey(value)}`;
  return `j${canonicalJson(value)}`;
}

/**
 * One UTF-16 code unit per byte, so equal texts mean equal bytes. Keys never
 * leave a query, and a grouped or joined scan builds one per row, where one
 * `fromCharCode` call is cheaper than two hex digits a byte.
 */
function bytesKey(value: Uint8Array): string {
  if (value.length <= BYTES_KEY_CHUNK)
    return String.fromCharCode.apply(null, value as unknown as number[]);
  let key = "";
  for (let start = 0; start < value.length; start += BYTES_KEY_CHUNK)
    key += String.fromCharCode.apply(
      null,
      value.subarray(start, start + BYTES_KEY_CHUNK) as unknown as number[],
    );
  return key;
}

/** Arguments per `fromCharCode` call, far below any engine's spread limit. */
const BYTES_KEY_CHUNK = 4_096;

/**
 * A join's right source as buckets of addresses, one bucket per distinct key,
 * each in `right.order` order: bucket `b` is `addresses[starts[b]]` up to
 * `addresses[starts[b + 1]]`, never empty.
 */
interface JoinIndex {
  readonly starts: Int32Array;
  readonly addresses: Uint32Array;
  /** The bucket whose key equals the left value of `row`, or -1. */
  bucketOf(row: number): number;
}

/**
 * Buckets `order`'s positions by `keys` (-1 for none) into a `JoinIndex`'s
 * layout, keeping each bucket in position order.
 */
function joinBuckets(
  order: Uint32Array,
  keys: Int32Array,
  buckets: number,
): { readonly starts: Int32Array; readonly addresses: Uint32Array } {
  const starts = new Int32Array(buckets + 1);
  for (const key of keys)
    if (key >= 0) starts[key + 1] = (starts[key + 1] ?? 0) + 1;
  for (let bucket = 0; bucket < buckets; bucket++)
    starts[bucket + 1] = (starts[bucket + 1] ?? 0) + (starts[bucket] ?? 0);
  const addresses = new Uint32Array(starts[buckets] ?? 0);
  const next = starts.slice(0, buckets);
  for (let position = 0; position < keys.length; position++) {
    const key = keys[position] ?? -1;
    if (key < 0) continue;
    addresses[next[key] ?? 0] = order[position] ?? 0;
    next[key] = (next[key] ?? 0) + 1;
  }
  return { starts, addresses };
}

/** The join index keyed by `valueKey`, for any column. */
function* valueJoinIndex(
  relation: AddressRelation,
  join: CompiledJoin,
  snapshots: readonly OverlayQuerySnapshot[],
  right: OverlayQuerySnapshot,
): Sliced<JoinIndex> {
  const buckets = new Map<string, number>();
  const keys = new Int32Array(right.order.length);
  for (let position = 0; position < keys.length; position++) {
    if (position % SLICE_ROWS === SLICE_ROWS - 1) yield;
    const value = right.valueAt(right.order[position] ?? 0, join.right.column);
    if (value === null) {
      keys[position] = -1;
      continue;
    }
    const key = valueKey(value);
    let bucket = buckets.get(key);
    if (bucket === undefined) {
      bucket = buckets.size;
      buckets.set(key, bucket);
    }
    keys[position] = bucket;
  }
  return {
    ...joinBuckets(right.order, keys, buckets.size),
    bucketOf: (row) => {
      const value = relationValue(relation, row, join.left, snapshots);
      return value === null ? -1 : (buckets.get(valueKey(value)) ?? -1);
    },
  };
}

/**
 * The join index of a UUID right column, keyed by the 16 bytes themselves, so a
 * clean base cell on either side is hashed in place, never read. Any other
 * right row is read and must be 16 bytes or null. `undefined` when the right
 * column has no UUID cells or holds some other value; `valueJoinIndex` keys it
 * then. As under `valueKey`, a left value that is not 16 bytes matches nothing.
 */
function* uuidJoinIndex(
  relation: AddressRelation,
  join: CompiledJoin,
  snapshots: readonly OverlayQuerySnapshot[],
  right: OverlayQuerySnapshot,
): Sliced<JoinIndex | undefined> {
  const rightBase = right.baseEncoded(join.right.column);
  if (rightBase?.cells.kind !== "uuid") return undefined;
  const rightCells = rightBase.cells;
  const rightDirty = rightBase.dirty;
  const table = new ByteKeyTable();
  const keys = new Int32Array(right.order.length);
  for (let position = 0; position < keys.length; position++) {
    if (position % SLICE_ROWS === SLICE_ROWS - 1) yield;
    const address = right.order[position] ?? 0;
    if (address < right.baseRows && !rightDirty?.(address)) {
      keys[position] = rightCells.validAt(address)
        ? table.add(rightCells.cells, address * 16)
        : -1;
      continue;
    }
    const value = right.valueAt(address, join.right.column);
    if (value === null) keys[position] = -1;
    else if (value instanceof Uint8Array && value.byteLength === 16)
      keys[position] = table.add(value, 0);
    else return undefined;
  }
  const leftVector = relation.vectors[join.left.source];
  const leftSnapshot = snapshots[join.left.source];
  const leftBase = leftSnapshot?.baseEncoded(join.left.column);
  const leftCells =
    leftBase?.cells.kind === "uuid" ? leftBase.cells : undefined;
  const leftDirty = leftBase?.dirty;
  const leftRows = leftSnapshot?.baseRows ?? 0;
  return {
    ...joinBuckets(right.order, keys, table.size),
    bucketOf: (row) => {
      const address = leftVector?.[row];
      if (
        leftCells &&
        address !== undefined &&
        address < leftRows &&
        !leftDirty?.(address)
      )
        return leftCells.validAt(address)
          ? table.find(leftCells.cells, address * 16)
          : -1;
      const value = relationValue(relation, row, join.left, snapshots);
      return value instanceof Uint8Array && value.byteLength === 16
        ? table.find(value, 0)
        : -1;
    },
  };
}

function* applyJoin(
  relation: AddressRelation,
  join: CompiledJoin,
  snapshots: readonly OverlayQuerySnapshot[],
  charge: (rows: number, rightRows: number) => void,
): Sliced<AddressRelation> {
  const right = snapshots[join.rightSource];
  if (!right) invalid("join source is unavailable");
  const index =
    (yield* uuidJoinIndex(relation, join, snapshots, right)) ??
    (yield* valueJoinIndex(relation, join, snapshots, right));
  const { starts, addresses } = index;
  const unmatched = join.kind === "left" ? 1 : 0;
  let rows = 0;
  for (let row = 0; row < relation.rows; row++) {
    if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
    const bucket = index.bucketOf(row);
    rows = checkedAdd(
      rows,
      bucket < 0
        ? unmatched
        : (starts[bucket + 1] ?? 0) - (starts[bucket] ?? 0),
      "join cardinality",
    );
  }
  charge(rows, right.order.length);
  const sources = relation.vectors.length;
  const vectors = Array.from(
    { length: sources + 1 },
    () => new Uint32Array(rows),
  );
  const rightVector = vectors[sources];
  if (!rightVector) invalid("join right relation vector is unavailable");
  let output = 0;
  const copyLeft = (row: number, count: number) => {
    for (let source = 0; source < sources; source++) {
      const vector = vectors[source];
      if (!vector) invalid("join relation vector is unavailable");
      vector.fill(relation.vectors[source]?.[row] ?? 0, output, output + count);
    }
  };
  for (let row = 0; row < relation.rows; row++) {
    if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
    const bucket = index.bucketOf(row);
    if (bucket < 0) {
      if (unmatched === 0) continue;
      copyLeft(row, 1);
      rightVector[output++] = MISSING_ADDRESS;
      continue;
    }
    const start = starts[bucket] ?? 0;
    const end = starts[bucket + 1] ?? 0;
    copyLeft(row, end - start);
    for (let match = start; match < end; match++)
      rightVector[output++] = addresses[match] ?? 0;
  }
  return { vectors, rows };
}

function expressionReadsOnly(
  expression: CompiledExpression,
  source: number,
): boolean {
  return expression.kind === "literal" || expression.field.source === source;
}

function predicateReadsOnly(
  predicate: CompiledPredicate,
  source: number,
): boolean {
  if (predicate.kind === "and" || predicate.kind === "or")
    return predicate.predicates.every((child) =>
      predicateReadsOnly(child, source),
    );
  if (predicate.kind === "is-null")
    return expressionReadsOnly(predicate.expression, source);
  return (
    expressionReadsOnly(predicate.left, source) &&
    expressionReadsOnly(predicate.right, source)
  );
}

function* applyFilter(
  relation: AddressRelation,
  predicate: CompiledPredicate | undefined,
  snapshots: readonly OverlayQuerySnapshot[],
  inspection: MutableQueryExecutionInspection,
  features: QueryExecutionFeatures,
): Sliced<AddressRelation> {
  if (!predicate) return relation;
  const tally = { comparisons: 0 };
  const input = relation.vectors[0];
  const single = relation.vectors.length === 1;
  const prepared = single
    ? prepareEncodedPredicate(predicate, snapshots[0], tally)
    : undefined;
  // Defined whenever `prepared` is, unless `blockFilter` is off: every leaf
  // with a prepared comparison has a block one.
  const block =
    features.blockFilter && prepared && snapshots[0]
      ? prepareBlockPredicate(predicate, snapshots[0])
      : undefined;
  if (block && prepared && input && snapshots[0])
    return yield* filterBlocks(
      relation,
      input,
      predicate,
      block,
      snapshots,
      inspection,
      features,
    );
  const vectors = relation.vectors.map(() => new Uint32Array(relation.rows));
  let rows = 0;
  let activeChunkStart = 0;
  let activeChunkEnd = 0;
  let activeChunkMayMatch = true;
  const chunkRows =
    features.chunkPruning && single
      ? predicateChunkRows(predicate, snapshots[0])
      : undefined;
  let pace = 0;
  for (let row = 0; row < relation.rows; row++) {
    if (++pace === SLICE_ROWS) {
      pace = 0;
      yield;
    }
    if (features.chunkPruning && single) {
      const address = input?.[row];
      const snapshot = snapshots[0];
      if (
        address !== undefined &&
        snapshot &&
        chunkRows !== undefined &&
        address < snapshot.baseRows
      ) {
        // The active chunk as an address range: the same test as comparing
        // `Math.floor(address / chunkRows)`, without a division a row.
        if (address < activeChunkStart || address >= activeChunkEnd) {
          const chunk = Math.floor(address / chunkRows);
          activeChunkStart = chunk * chunkRows;
          activeChunkEnd = activeChunkStart + chunkRows;
          inspection.chunksExamined++;
          activeChunkMayMatch =
            predicateChunkMayMatch(predicate, snapshot, chunk) !== false;
          if (!activeChunkMayMatch) inspection.chunksSkipped++;
        }
        if (!activeChunkMayMatch) {
          while (row + 1 < relation.rows) {
            const next = input?.[row + 1];
            if (
              next === undefined ||
              next >= snapshot.baseRows ||
              next < activeChunkStart ||
              next >= activeChunkEnd
            )
              break;
            row++;
          }
          continue;
        }
      }
    }
    inspection.rowsExamined++;
    const address = input?.[row];
    tally.comparisons = 0;
    const preparedDecision =
      prepared && address !== undefined ? prepared(address) : undefined;
    inspection.encodedComparisons += tally.comparisons;
    if (preparedDecision !== undefined) {
      if (!preparedDecision) continue;
    } else if (
      !matchesPredicate(predicate, relation, row, snapshots, inspection)
    )
      continue;
    for (let source = 0; source < relation.vectors.length; source++) {
      const vector = vectors[source];
      if (!vector) invalid("filter relation vector is unavailable");
      vector[rows] = relation.vectors[source]?.[row] ?? 0;
    }
    rows++;
  }
  return { vectors: vectors.map((vector) => vector.subarray(0, rows)), rows };
}

/**
 * `applyFilter` over a single source whose predicate has a block form. It
 * decides runs of up to `FILTER_BLOCK_ROWS` consecutive rows in place in
 * `input`, ending a run where a base address leaves the active statistics
 * chunk, so its results and counters are the row path's.
 */
function* filterBlocks(
  relation: AddressRelation,
  input: Uint32Array,
  predicate: CompiledPredicate,
  block: BlockPredicate,
  snapshots: readonly OverlayQuerySnapshot[],
  inspection: MutableQueryExecutionInspection,
  features: QueryExecutionFeatures,
): Sliced<AddressRelation> {
  const snapshot = snapshots[0];
  if (!snapshot) invalid("filter source is unavailable");
  const output = new Uint32Array(relation.rows);
  let kept = 0;
  const baseRows = snapshot.baseRows;
  const chunkRows = features.chunkPruning
    ? predicateChunkRows(predicate, snapshot)
    : undefined;
  let activeChunkStart = 0;
  let activeChunkEnd = 0;
  let activeChunkMayMatch = true;
  let sliceStart = 0;
  let row = 0;
  while (row < relation.rows) {
    if (row - sliceStart >= SLICE_ROWS) {
      sliceStart = row;
      yield;
    }
    const limit = Math.min(relation.rows, row + FILTER_BLOCK_ROWS);
    let end = limit;
    if (chunkRows !== undefined)
      for (end = row; end < limit; end++) {
        const address = input[end] ?? 0;
        if (
          address < baseRows &&
          (!activeChunkMayMatch ||
            address < activeChunkStart ||
            address >= activeChunkEnd)
        )
          break;
      }
    if (end > row) {
      inspection.rowsExamined += end - row;
      kept = decideBlock(
        block,
        input.subarray(row, end),
        row,
        predicate,
        relation,
        snapshots,
        inspection,
        output,
        kept,
      );
      row = end;
      continue;
    }
    // A base address in another chunk than the active one, or in the active
    // one when it was skipped.
    const address = input[row] ?? 0;
    if (chunkRows === undefined) invalid("filter chunk is unavailable");
    if (address < activeChunkStart || address >= activeChunkEnd) {
      const chunk = Math.floor(address / chunkRows);
      activeChunkStart = chunk * chunkRows;
      activeChunkEnd = activeChunkStart + chunkRows;
      inspection.chunksExamined++;
      activeChunkMayMatch =
        predicateChunkMayMatch(predicate, snapshot, chunk) !== false;
      if (!activeChunkMayMatch) inspection.chunksSkipped++;
    }
    if (activeChunkMayMatch) continue;
    for (row++; row < relation.rows; row++) {
      const next = input[row] ?? 0;
      if (next >= baseRows || next < activeChunkStart || next >= activeChunkEnd)
        break;
    }
  }
  return { vectors: [output.subarray(0, kept)], rows: kept };
}

/**
 * Decides the rows `first..first + addresses.length - 1` of a single-source
 * relation, whose base addresses are `addresses`, in row order, and appends
 * each one that passes to `output` after the first `kept`; returns the new
 * row count.
 */
function decideBlock(
  block: BlockPredicate,
  addresses: Uint32Array,
  first: number,
  predicate: CompiledPredicate,
  relation: AddressRelation,
  snapshots: readonly OverlayQuerySnapshot[],
  inspection: MutableQueryExecutionInspection,
  output: Uint32Array,
  kept: number,
): number {
  inspection.encodedComparisons += block.evaluate(addresses);
  for (let position = 0; position < addresses.length; position++) {
    const decision = block.out[position];
    if (decision === FALSE) continue;
    const address = addresses[position] ?? 0;
    if (decision === UNDECIDED) {
      if (
        !matchesPredicate(
          predicate,
          relation,
          first + position,
          snapshots,
          inspection,
        )
      )
        continue;
    }
    output[kept++] = address;
  }
  return kept;
}

/**
 * Rows the block evaluator decides together. Its scratch is a few bytes a row
 * per nesting level (`filterScratchBytes`), so this bounds it, and it is large
 * enough that the per-block calls are noise beside the row loops.
 */
const FILTER_BLOCK_ROWS = 1_024;

/**
 * The block evaluator's three values, ordered so that `or` is the maximum,
 * `and` the minimum and a negated is-null leaf `TRUE - value`: the logic of
 * `prepareEncodedPredicate` with `undefined` as `UNDECIDED`. `UNDECIDED` is a
 * cell the base cannot decide, not SQL's UNKNOWN, which `compileQuery` has
 * already turned into false leaves.
 */
const FALSE = 0;
const UNDECIDED = 1;
const TRUE = 2;

/** Positions `0..FILTER_BLOCK_ROWS - 1`, the selection of a whole block. */
const ALL_POSITIONS = Uint32Array.from(
  { length: FILTER_BLOCK_ROWS },
  (_unused, index) => index,
);

interface BlockPredicate {
  /** `FALSE`, `UNDECIDED` or `TRUE` per position after `evaluate`. */
  readonly out: Uint8Array;
  /**
   * Decides each position `p` of `addresses`, at most `FILTER_BLOCK_ROWS`,
   * as the base address `addresses[p]`; returns the leaves it decided. A
   * longer block throws.
   */
  evaluate(addresses: Uint32Array): number;
}

/**
 * Sets `out[position]` for each position in `selection[0..count)` and returns
 * how many leaf comparisons it decided, the count `tally` keeps.
 */
type BlockNode = (
  selection: Uint32Array,
  count: number,
  out: Uint8Array,
) => number;

/**
 * `prepareEncodedPredicate` over a block of rows. Every node runs one tight
 * loop over the positions still open instead of one closure call per row, and
 * the result for every row is the scalar one: an `or` asks its next child only
 * about the positions no earlier child made `TRUE`, as the scalar loop returns
 * early, and an `and` only about those none made `FALSE`, so the same leaves
 * are decided for the same rows.
 *
 * Each nesting level of `and`/`or` owns one selection and one output buffer
 * its children write to. A child runs to completion before its sibling starts,
 * so siblings share their level's buffers, and a node never writes the
 * selection it was given.
 */
function prepareBlockPredicate(
  predicate: CompiledPredicate,
  snapshot: OverlayQuerySnapshot,
): BlockPredicate | undefined {
  // The block `evaluate` was last given, which every leaf reads.
  let addresses: Uint32Array = new Uint32Array();
  const levels: { selection: Uint32Array; out: Uint8Array }[] = [];
  const compile = (
    node: CompiledPredicate,
    depth: number,
  ): BlockNode | undefined => {
    if (node.kind === "and" || node.kind === "or") {
      const children: BlockNode[] = [];
      for (const child of node.predicates) {
        const compiled = compile(child, depth + 1);
        if (!compiled) return undefined;
        children.push(compiled);
      }
      const level = (levels[depth] ??= {
        selection: new Uint32Array(FILTER_BLOCK_ROWS),
        out: new Uint8Array(FILTER_BLOCK_ROWS),
      });
      const any = node.kind === "or";
      const settled = any ? TRUE : FALSE;
      const [first, ...rest] = children;
      return (selection, count, out) => {
        if (!first) {
          for (let index = 0; index < count; index++)
            out[selection[index] ?? 0] = any ? FALSE : TRUE;
          return 0;
        }
        // The first child writes the node's output directly: `FALSE` is the
        // identity of the maximum and `TRUE` of the minimum.
        let decided = first(selection, count, out);
        let remaining = 0;
        for (let index = 0; index < count; index++) {
          const position = selection[index] ?? 0;
          if (out[position] !== settled)
            level.selection[remaining++] = position;
        }
        for (const child of rest) {
          if (remaining === 0) break;
          decided += child(level.selection, remaining, level.out);
          let kept = 0;
          for (let index = 0; index < remaining; index++) {
            const position = level.selection[index] ?? 0;
            const before = out[position] ?? 0;
            const value = level.out[position] ?? 0;
            const merged = any
              ? Math.max(before, value)
              : Math.min(before, value);
            out[position] = merged;
            if (merged !== settled) level.selection[kept++] = position;
          }
          remaining = kept;
        }
        return decided;
      };
    }
    let compare: ArtifactBlockCompare | undefined;
    let negated = false;
    if (node.kind === "compare") {
      const comparable = predicateFieldLiteral(node);
      if (!comparable || comparable.field.source !== 0) return undefined;
      compare = snapshot.prepareEncodedBlock(
        comparable.field.column,
        comparable.operator,
        comparable.literal,
      );
    } else if (
      node.expression.kind === "field" &&
      node.expression.field.source === 0
    ) {
      compare = snapshot.prepareEncodedBlock(
        node.expression.field.column,
        "eq",
        null,
      );
      negated = node.negated;
    }
    if (!compare) return undefined;
    const leaf = compare;
    if (!negated)
      return (selection, count, out) =>
        count - leaf(addresses, selection, count, out);
    return (selection, count, out) => {
      const undecided = leaf(addresses, selection, count, out);
      for (let index = 0; index < count; index++) {
        const position = selection[index] ?? 0;
        out[position] = TRUE - (out[position] ?? 0);
      }
      return count - undecided;
    };
  };
  const root = compile(predicate, 0);
  if (!root) return undefined;
  const out = new Uint8Array(FILTER_BLOCK_ROWS);
  return {
    out,
    evaluate: (block) => {
      // Past `FILTER_BLOCK_ROWS`, every selection would read position 0 and
      // `out` would drop the decisions.
      if (block.length > FILTER_BLOCK_ROWS) invalid("filter block is too long");
      addresses = block;
      return root(ALL_POSITIONS, block.length, out);
    },
  };
}

/**
 * What `prepareBlockPredicate` allocates for a predicate: the output buffer of
 * one block, and one selection and output buffer for each level of `and`/`or`
 * nesting.
 */
function filterScratchBytes(predicate: QueryPredicate): number {
  const levels = (node: QueryPredicate): number =>
    node.kind === "and" || node.kind === "or"
      ? 1 + Math.max(0, ...node.predicates.map(levels))
      : node.kind === "not"
        ? levels(node.predicate)
        : 0;
  return checkedProduct(
    FILTER_BLOCK_ROWS,
    checkedAdd(
      1,
      checkedProduct(levels(predicate), 5, "filter scratch"),
      "filter scratch",
    ),
    "filter scratch",
  );
}

type EncodedDecision = (address: number) => boolean | undefined;

/**
 * The whole predicate over encoded vectors, or `undefined` when any leaf has
 * none. `undefined` from the result means a cell the base cannot decide (an
 * overlay-dirty cell or an overlay row), and the caller falls back to
 * `matchesPredicate` for that row. `tally` counts the leaves one call decided.
 */
function prepareEncodedPredicate(
  predicate: CompiledPredicate,
  snapshot: OverlayQuerySnapshot | undefined,
  tally: { comparisons: number },
): EncodedDecision | undefined {
  if (!snapshot) return undefined;
  if (predicate.kind === "and" || predicate.kind === "or") {
    const children: EncodedDecision[] = [];
    for (const child of predicate.predicates) {
      const prepared = prepareEncodedPredicate(child, snapshot, tally);
      if (!prepared) return undefined;
      children.push(prepared);
    }
    // `undefined` is a cell the base cannot decide, not SQL's UNKNOWN, which
    // `compileQuery` has already turned into false leaves: one decisive child
    // settles the row even after a child the base could not decide.
    const decisive = predicate.kind === "or";
    return (address) => {
      let undecided = false;
      for (const child of children) {
        const decision = child(address);
        if (decision === decisive) return decisive;
        if (decision === undefined) undecided = true;
      }
      return undecided ? undefined : !decisive;
    };
  }
  let compare: EncodedDecision | undefined;
  let negated = false;
  if (predicate.kind === "compare") {
    const comparable = predicateFieldLiteral(predicate);
    if (!comparable || comparable.field.source !== 0) return undefined;
    compare = snapshot.prepareEncodedComparison(
      comparable.field.column,
      comparable.operator,
      comparable.literal,
    );
  } else if (
    predicate.expression.kind === "field" &&
    predicate.expression.field.source === 0
  ) {
    compare = snapshot.prepareEncodedComparison(
      predicate.expression.field.column,
      "eq",
      null,
    );
    negated = predicate.negated;
  }
  if (!compare) return undefined;
  const leaf = compare;
  return (address) => {
    const decision = leaf(address);
    if (decision === undefined) return undefined;
    tally.comparisons++;
    return negated ? !decision : decision;
  };
}

function predicateFieldLiteral(predicate: CompiledPredicate):
  | {
      readonly field: CompiledField;
      readonly operator: QueryComparisonOperator;
      readonly literal: SqlValue;
    }
  | undefined {
  if (predicate.kind !== "compare") return undefined;
  const field =
    predicate.left.kind === "field" && predicate.right.kind === "literal"
      ? predicate.left.field
      : predicate.right.kind === "field" && predicate.left.kind === "literal"
        ? predicate.right.field
        : undefined;
  const literal =
    predicate.left.kind === "literal" && predicate.right.kind === "field"
      ? predicate.left.value
      : predicate.right.kind === "literal" && predicate.left.kind === "field"
        ? predicate.right.value
        : undefined;
  if (
    !field ||
    literal === undefined ||
    (typeof literal === "object" &&
      literal !== null &&
      !(literal instanceof Uint8Array))
  )
    return undefined;
  return {
    field,
    operator:
      predicate.left.kind === "literal"
        ? reverseOperator(predicate.operator)
        : predicate.operator,
    literal: literal as SqlValue,
  };
}

function predicateChunkRows(
  predicate: CompiledPredicate,
  snapshot: OverlayQuerySnapshot | undefined,
): number | undefined {
  if (!snapshot) return undefined;
  if (predicate.kind === "compare") {
    const comparable = predicateFieldLiteral(predicate);
    return comparable?.field.source === 0
      ? snapshot.baseChunkRows(comparable.field.column)
      : undefined;
  }
  if (
    predicate.kind === "is-null" &&
    predicate.expression.kind === "field" &&
    predicate.expression.field.source === 0
  )
    return snapshot.baseChunkRows(predicate.expression.field.column);
  if (predicate.kind === "and" || predicate.kind === "or") {
    const sizes = predicate.predicates
      .map((child) => predicateChunkRows(child, snapshot))
      .filter((value): value is number => value !== undefined);
    return sizes.length > 0 && sizes.every((value) => value === sizes[0])
      ? sizes[0]
      : undefined;
  }
  return undefined;
}

function predicateChunkMayMatch(
  predicate: CompiledPredicate,
  snapshot: OverlayQuerySnapshot,
  chunk: number,
): boolean | undefined {
  if (predicate.kind === "compare") {
    const comparable = predicateFieldLiteral(predicate);
    if (!comparable || comparable.field.source !== 0) return undefined;
    return snapshot.baseChunkMayMatch(
      comparable.field.column,
      chunk,
      comparable.operator,
      comparable.literal,
    );
  }
  if (predicate.kind === "is-null") {
    if (
      predicate.expression.kind !== "field" ||
      predicate.expression.field.source !== 0
    )
      return undefined;
    const contains = snapshot.baseChunkMayMatch(
      predicate.expression.field.column,
      chunk,
      "eq",
      null,
    );
    if (contains === undefined) return undefined;
    // A nullable chunk can still contain non-null rows. The statistic exposes
    // only null count here, so negated null predicates conservatively scan.
    return predicate.negated ? undefined : contains;
  }
  if (predicate.kind === "and") {
    let unknown = false;
    for (const child of predicate.predicates) {
      const decision = predicateChunkMayMatch(child, snapshot, chunk);
      if (decision === false) return false;
      if (decision === undefined) unknown = true;
    }
    return unknown ? undefined : true;
  }
  let unknown = false;
  for (const child of predicate.predicates) {
    const decision = predicateChunkMayMatch(child, snapshot, chunk);
    if (decision === true) return true;
    if (decision === undefined) unknown = true;
  }
  return unknown ? undefined : false;
}

interface AggregateState {
  readonly operator: QueryAggregateOperator;
  count: number;
  numericKind?: "number" | "bigint";
  sumNumber: number;
  sumBigint: bigint;
  /**
   * Safe-integer terms of a bigint sum not yet added to `sumBigint`. Always a
   * safe integer, so exact; added in when the next term would leave that
   * range, and when the sum finishes.
   */
  sumSafe: number;
  value?: QueryValue;
  /**
   * While a `min` or `max` has compared only encoded values, the address of
   * the row that holds the extreme so far, whose value is read only when the
   * aggregate finishes; -1 otherwise.
   */
  extremeAddress: number;
  /** The extreme's `orderAt` key, meaningful while `extremeAddress` is. */
  extremeKey: number;
}

interface GroupState {
  readonly keyValues: readonly QueryValue[];
  readonly aggregates: readonly (AggregateState | undefined)[];
  readonly ordinal: number;
}

function newAggregate(
  projection: CompiledProjection,
): AggregateState | undefined {
  return projection.kind === "aggregate"
    ? {
        operator: projection.operator,
        count: 0,
        sumNumber: 0,
        sumBigint: 0n,
        sumSafe: 0,
        extremeAddress: -1,
        extremeKey: 0,
      }
    : undefined;
}

function updateAggregate(
  state: AggregateState,
  projection: Extract<CompiledProjection, { readonly kind: "aggregate" }>,
  value: QueryValue | undefined,
): void {
  if (projection.operator === "count") {
    if (projection.field === undefined || value !== null) state.count++;
    return;
  }
  if (value === null || value === undefined) return;
  if (projection.operator === "sum") {
    if (typeof value !== "number" && typeof value !== "bigint")
      invalid("sum requires a numeric column");
    if (state.numericKind !== undefined && state.numericKind !== typeof value)
      invalid("sum cannot mix number and bigint values");
    state.numericKind = typeof value === "number" ? "number" : "bigint";
    if (typeof value === "number") state.sumNumber += value;
    else state.sumBigint += value;
    state.count++;
    return;
  }
  if (state.value === undefined) {
    state.value = value;
    return;
  }
  const comparison = totalCompare(value, state.value);
  if (
    (projection.operator === "min" && comparison < 0) ||
    (projection.operator === "max" && comparison > 0)
  )
    state.value = value;
}

function finishAggregate(state: AggregateState): QueryValue {
  if (state.operator === "count") return state.count;
  if (state.operator === "sum") {
    if (state.count === 0) return null;
    return state.numericKind === "bigint"
      ? state.sumBigint + BigInt(state.sumSafe)
      : state.sumNumber;
  }
  return state.value ?? null;
}

interface ProjectedRow {
  readonly values: readonly QueryValue[];
  readonly ordinal: number;
}

/** The field id `groupKeyReader` gives null. */
const NULL_KEY_ID = -1;

/**
 * Ids for values outside a field's encoded form, one per distinct `valueKey`,
 * counting down from -2 so they meet no encoded id and not `NULL_KEY_ID`.
 */
class SpilledKeys {
  readonly #ids = new Map<string, number>();

  idOf(value: QueryValue): number {
    const key = valueKey(value);
    let id = this.#ids.get(key);
    if (id === undefined) {
      id = -2 - this.#ids.size;
      this.#ids.set(key, id);
    }
    return id;
  }
}

/**
 * The address of `field` in `row`, or `undefined` when that row has none, which
 * `relationValue` refuses.
 */
function fieldAddress(
  relation: AddressRelation,
  row: number,
  field: CompiledField,
): number | undefined {
  return relation.vectors[field.source]?.[row];
}

/**
 * Reads one group-by field as a 32-bit id per row, equal exactly when the
 * values' `valueKey`s are. A dictionary code, an interned UUID, an interned
 * UTF-8 text or a numbered safe integer stands for a clean base cell without
 * reading it; anything else is read and given the id its value's encoded form
 * has, or a spilled one. Reading an encoded cell cannot throw (`codesAreText`,
 * `rowsAreText`), so skipping the read changes no row's outcome.
 */
function groupKeyReader(
  relation: AddressRelation,
  field: CompiledField,
  snapshots: readonly OverlayQuerySnapshot[],
): (row: number) => number {
  const spilled = new SpilledKeys();
  const read = (row: number): number => {
    const value = relationValue(relation, row, field, snapshots);
    return value === null ? NULL_KEY_ID : spilled.idOf(value);
  };
  const snapshot = snapshots[field.source];
  const base = snapshot?.baseEncoded(field.column);
  if (!snapshot || !base) return read;
  const { cells, dirty } = base;
  const baseRows = snapshot.baseRows;
  const column = field.column;
  if (cells.kind === "dict" && cells.codesAreText())
    return (row) => {
      const address = fieldAddress(relation, row, field);
      if (address === undefined || address === MISSING_ADDRESS)
        return read(row);
      if (address < baseRows && !dirty?.(address))
        return cells.validAt(address) ? cells.codeAt(address) : NULL_KEY_ID;
      const value = snapshot.valueAt(address, column);
      if (value === null) return NULL_KEY_ID;
      return (
        (typeof value === "string" ? cells.codeOf(value) : undefined) ??
        spilled.idOf(value)
      );
    };
  if (cells.kind === "uuid") {
    const interned = new ByteKeyTable();
    return (row) => {
      const address = fieldAddress(relation, row, field);
      if (address === undefined || address === MISSING_ADDRESS)
        return read(row);
      if (address < baseRows && !dirty?.(address))
        return cells.validAt(address)
          ? interned.add(cells.cells, address * 16)
          : NULL_KEY_ID;
      const value = snapshot.valueAt(address, column);
      if (value === null) return NULL_KEY_ID;
      return value instanceof Uint8Array && value.byteLength === 16
        ? interned.add(value, 0)
        : spilled.idOf(value);
    };
  }
  if (cells.kind === "text" && cells.rowsAreText()) {
    const interned = new ByteStringTable();
    const { offsets, bytes } = cells;
    return (row) => {
      const address = fieldAddress(relation, row, field);
      if (address === undefined || address === MISSING_ADDRESS)
        return read(row);
      if (address < baseRows && !dirty?.(address))
        return cells.validAt(address)
          ? interned.add(
              bytes,
              offsets[address] ?? 0,
              offsets[address + 1] ?? 0,
            )
          : NULL_KEY_ID;
      const value = snapshot.valueAt(address, column);
      if (value === null) return NULL_KEY_ID;
      // A well-formed string has one UTF-8 form, and a clean row that reads
      // as it holds exactly that form; a lone surrogate encodes as U+FFFD.
      if (typeof value !== "string" || !value.isWellFormed())
        return spilled.idOf(value);
      const encoded = encoder.encode(value);
      return interned.add(encoded, 0, encoded.byteLength);
    };
  }
  if (cells.kind === "exact" && cells.type !== "timestamp") {
    // An integer column reads as bigints; a safe one is numbered by value.
    const numbered = new Map<number, number>();
    const idOfSafe = (value: number) => {
      let id = numbered.get(value);
      if (id === undefined) {
        id = numbered.size;
        numbered.set(value, id);
      }
      return id;
    };
    return (row) => {
      const address = fieldAddress(relation, row, field);
      if (address === undefined || address === MISSING_ADDRESS)
        return read(row);
      if (address < baseRows && !dirty?.(address)) {
        if (!cells.validAt(address)) return NULL_KEY_ID;
        const value = cells.safeAt(address);
        return value === undefined ? read(row) : idOfSafe(value);
      }
      const value = snapshot.valueAt(address, column);
      if (value === null) return NULL_KEY_ID;
      return typeof value === "bigint" &&
        value >= -MAX_SAFE_BIGINT &&
        value <= MAX_SAFE_BIGINT
        ? idOfSafe(Number(value))
        : spilled.idOf(value);
    };
  }
  return read;
}

type AggregateUpdate = (state: AggregateState, row: number) => void;

/**
 * `updateAggregate` for one aggregate projection, reading a clean base cell in
 * its encoded form where the operator allows: `count` of a UUID or integer
 * field needs only validity, an integer `sum` a safe integer, and `min` or
 * `max` over integers or timestamps an `orderAt` key, with the extreme row
 * read once when the group finishes. Any other row is read and handed to
 * `updateAggregate`, in the order `groupRows` reads them; that
 * includes every dictionary or timestamp cell a `count` meets, since reading
 * one can throw.
 */
function aggregateUpdate(
  relation: AddressRelation,
  projection: Extract<CompiledProjection, { readonly kind: "aggregate" }>,
  snapshots: readonly OverlayQuerySnapshot[],
): AggregateUpdate {
  const field = projection.field;
  // `count(*)` counts every row and reads nothing.
  if (field === undefined)
    return (state) => {
      state.count++;
    };
  const read: AggregateUpdate = (state, row) =>
    updateAggregate(
      state,
      projection,
      relationValue(relation, row, field, snapshots),
    );
  const snapshot = snapshots[field.source];
  const base = snapshot?.baseEncoded(field.column);
  if (!snapshot || !base) return read;
  const { cells, dirty } = base;
  const baseRows = snapshot.baseRows;
  const clean = (address: number) => address < baseRows && !dirty?.(address);
  if (
    projection.operator === "count" &&
    (cells.kind === "uuid" ||
      (cells.kind === "exact" && cells.type !== "timestamp"))
  )
    return (state, row) => {
      const address = fieldAddress(relation, row, field);
      if (address === undefined || !clean(address)) return read(state, row);
      if (cells.validAt(address)) state.count++;
    };
  if (cells.kind !== "exact") return read;
  if (projection.operator === "sum" && cells.type !== "timestamp")
    return (state, row) => {
      const address = fieldAddress(relation, row, field);
      if (address === undefined || !clean(address)) return read(state, row);
      if (!cells.validAt(address)) return;
      const value = cells.safeAt(address);
      if (value === undefined) return read(state, row);
      if (state.numericKind === "number")
        invalid("sum cannot mix number and bigint values");
      state.numericKind = "bigint";
      const sum = state.sumSafe + value;
      if (Number.isSafeInteger(sum)) state.sumSafe = sum;
      else {
        state.sumBigint += BigInt(state.sumSafe);
        state.sumSafe = value;
      }
      state.count++;
    };
  if (projection.operator !== "min" && projection.operator !== "max")
    return read;
  const least = projection.operator === "min";
  const column = field.column;
  // A row read as a value: a non-null one ends the encoded extreme, which is
  // read first so that `updateAggregate` compares the two values.
  const extremeRead: AggregateUpdate = (state, row) => {
    const value = relationValue(relation, row, field, snapshots);
    if (value !== null && state.extremeAddress >= 0) {
      state.value = snapshot.valueAt(state.extremeAddress, column);
      state.extremeAddress = -1;
    }
    updateAggregate(state, projection, value);
  };
  return (state, row) => {
    const address = fieldAddress(relation, row, field);
    if (address === undefined || !clean(address))
      return extremeRead(state, row);
    // Once a value has been read, the extreme is a value and every row is.
    if (state.value !== undefined) return read(state, row);
    if (!cells.validAt(address)) return;
    const key = cells.orderAt(address);
    if (key === undefined) return extremeRead(state, row);
    if (
      state.extremeAddress < 0 ||
      (least ? key < state.extremeKey : key > state.extremeKey)
    ) {
      state.extremeAddress = address;
      state.extremeKey = key;
    }
  };
}

/**
 * Rows `start` to `end` of a non-aggregate plan's relation, projected. Only
 * the rows a result publishes are read, plus every row of a plan another plan
 * reads from.
 */
function* projectRange(
  relation: AddressRelation,
  query: CompiledQuery,
  snapshots: readonly OverlayQuerySnapshot[],
  start: number,
  end: number,
): Sliced<ProjectedRow[]> {
  const fields = query.projections.map((projection) => {
    if (projection.kind !== "field")
      invalid("aggregate projection requires aggregate mode");
    return projection.field;
  });
  const rows: ProjectedRow[] = [];
  for (let row = start; row < end; row++) {
    if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
    rows.push({
      values: fields.map((field) =>
        relationValue(relation, row, field, snapshots),
      ),
      ordinal: row,
    });
  }
  return rows;
}

/** An aggregate plan's groups, in the order their first rows came. */
function* groupRows(
  relation: AddressRelation,
  query: CompiledQuery,
  snapshots: readonly OverlayQuerySnapshot[],
  chargeGroups: (groups: number) => void,
): Sliced<ProjectedRow[]> {
  const groups: GroupState[] = [];
  const global = query.groupBy.length === 0;
  if (global) {
    chargeGroups(1);
    groups.push({
      keyValues: [],
      aggregates: query.projections.map(newAggregate),
      ordinal: 0,
    });
  }
  const readers = query.groupBy.map((field) =>
    groupKeyReader(relation, field, snapshots),
  );
  const table = new GroupKeyTable(readers.length);
  // One scratch tuple for the whole scan; a row that opens a group adds a copy.
  const ids = new Int32Array(readers.length);
  const updates = query.projections.map((projection) =>
    projection.kind === "aggregate"
      ? aggregateUpdate(relation, projection, snapshots)
      : undefined,
  );
  // `count(*)` reads nothing, so a single group of nothing else is the
  // relation's row count without a pass over it.
  const counted =
    global &&
    query.projections.every(
      (projection) =>
        projection.kind === "aggregate" &&
        projection.operator === "count" &&
        projection.field === undefined,
    );
  if (counted)
    for (const aggregate of groups[0]?.aggregates ?? [])
      if (aggregate) aggregate.count = relation.rows;
  for (let row = 0; !counted && row < relation.rows; row++) {
    if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
    let group = groups[0];
    if (!global) {
      for (let field = 0; field < readers.length; field++)
        ids[field] = readers[field]?.(row) ?? NULL_KEY_ID;
      let index = table.find(ids);
      if (index < 0) {
        const keyValues = query.groupBy.map((field) =>
          relationValue(relation, row, field, snapshots),
        );
        chargeGroups(groups.length + 1);
        index = table.add(ids);
        groups.push({
          keyValues,
          aggregates: query.projections.map(newAggregate),
          ordinal: index,
        });
      }
      group = groups[index];
    }
    if (!group) invalid("group state is unavailable");
    for (let index = 0; index < updates.length; index++) {
      const update = updates[index];
      const aggregate = group.aggregates[index];
      if (update && aggregate) update(aggregate, row);
    }
  }
  // A `min` or `max` still holding an encoded extreme reads that one row.
  for (const [index, projection] of query.projections.entries()) {
    if (projection.kind !== "aggregate" || projection.field === undefined)
      continue;
    const snapshot = snapshots[projection.field.source];
    for (const group of groups) {
      const aggregate = group.aggregates[index];
      if (!aggregate || aggregate.extremeAddress < 0) continue;
      if (!snapshot) invalid("query source is unavailable");
      aggregate.value = snapshot.valueAt(
        aggregate.extremeAddress,
        projection.field.column,
      );
      aggregate.extremeAddress = -1;
    }
  }
  const groupIndex = new Map(
    query.groupBy.map((field, index) => [field.key, index]),
  );
  return groups.map((group) => ({
    values: query.projections.map((projection, index) => {
      if (projection.kind === "field") {
        const keyIndex = groupIndex.get(projection.field.key);
        if (keyIndex === undefined) invalid("group projection is unavailable");
        return group.keyValues[keyIndex] ?? null;
      }
      const aggregate = group.aggregates[index];
      if (!aggregate) invalid("aggregate state is unavailable");
      return finishAggregate(aggregate);
    }),
    ordinal: group.ordinal,
  }));
}

function valueRank(value: Exclude<QueryValue, null>): number {
  if (typeof value === "boolean") return 0;
  if (typeof value === "number") return 1;
  if (typeof value === "bigint") return 2;
  if (typeof value === "string") return 3;
  if (value instanceof Uint8Array) return 4;
  return 5;
}

function totalCompare(left: QueryValue, right: QueryValue): number {
  if (left === null || right === null)
    return left === right ? 0 : left === null ? -1 : 1;
  const leftRank = valueRank(left);
  const rightRank = valueRank(right);
  if (leftRank !== rightRank) return leftRank - rightRank;
  return sameTypeCompare(left, right) ?? 0;
}

function compareOrdered(
  left: ProjectedRow,
  right: ProjectedRow,
  orders: readonly CompiledOrder[],
): number {
  for (const order of orders) {
    const a = left.values[order.column] ?? null;
    const b = right.values[order.column] ?? null;
    if (a === null || b === null) {
      if (a === b) continue;
      return a === null
        ? order.nulls === "first"
          ? -1
          : 1
        : order.nulls === "first"
          ? 1
          : -1;
    }
    const comparison = totalCompare(a, b);
    if (comparison !== 0)
      return order.direction === "desc" ? -comparison : comparison;
  }
  // Never reverse the deterministic provenance tie-breaker.
  return left.ordinal - right.ordinal;
}

/**
 * The largest limit a sort serves by keeping the best rows as it scans rather
 * than ordering them all. Each kept row is an insertion into a sorted array,
 * so a scan costs up to `rows * limit` moves.
 */
const MAX_TOP_ROWS = 64;

/** Rows an insertion sort orders before the merge passes take over. */
const SORT_RUN = 32;

/**
 * One ordering column of a relation sort.
 *
 * A column whose base cells carry an order-preserving number (an exact
 * integer's or timestamp's `orderAt`, or a dictionary code when the codes
 * order as their texts do) gets a key per row: that number, negated for a
 * descending order, or -Infinity or Infinity for null by its placement. A row
 * no such number stands for, such as an overlay value the encoded form does
 * not hold or a cell outside the safe range, gets NaN, and its value is read
 * whenever it meets another row. Any other column compares values from
 * `valueAt`.
 *
 * Two keys are equal exactly when the two values are, and order as
 * `totalCompare` orders them, so either form compares rows exactly as
 * `compareOrdered` compares their projected values.
 */
interface SortColumn {
  readonly keys: Float64Array | undefined;
  /** How many keys are NaN. */
  readonly loose: number;
  /** A keyless column's value at a relation row. */
  readonly valueAt: ((row: number) => QueryValue) | undefined;
  readonly field: CompiledField;
  readonly nullsFirst: boolean;
  readonly descending: boolean;
}

const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * Canonical timestamp text: milliseconds always, and three more digits when
 * the microseconds are not whole milliseconds (`directColumnarTimestampText`).
 */
const TIMESTAMP_TEXT =
  /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})(\d{3})?Z$/u;

/**
 * The `orderAt` key of the timestamp that reads back as `text`, or
 * `undefined` when no safe microsecond count does. Only a text that
 * round-trips through the formatter gets a key, so the key orders as the text
 * does against every base cell's.
 */
function timestampKey(text: string): number | undefined {
  const match = TIMESTAMP_TEXT.exec(text);
  if (!match) return undefined;
  const millis = Date.parse(`${match[1] ?? ""}Z`);
  if (!Number.isSafeInteger(millis)) return undefined;
  const micros = millis * 1_000 + Number(match[2] ?? 0);
  if (!Number.isSafeInteger(micros)) return undefined;
  return directColumnarTimestampText(BigInt(micros)) === text
    ? timestampTextOrder(micros)
    : undefined;
}

/**
 * The key a clean base cell of `cells` would have for `value`, or `undefined`
 * when the value is not one such a cell can hold.
 */
function overlayKey(
  cells: ArtifactEncodedColumn,
  value: QueryValue,
): number | undefined {
  if (cells.kind === "dict")
    return typeof value === "string" ? cells.codeOf(value) : undefined;
  if (cells.kind !== "exact") return undefined;
  if (cells.type === "timestamp")
    return typeof value === "string" ? timestampKey(value) : undefined;
  return typeof value === "bigint" &&
    value <= MAX_SAFE_BIGINT &&
    value >= -MAX_SAFE_BIGINT
    ? Number(value)
    : undefined;
}

function* sortColumn(
  relation: AddressRelation,
  field: CompiledField,
  order: CompiledOrder,
  snapshots: readonly OverlayQuerySnapshot[],
): Sliced<SortColumn> {
  const nullsFirst = order.nulls === "first";
  const descending = order.direction === "desc";
  const snapshot = snapshots[field.source];
  const base = snapshot?.baseEncoded(field.column);
  const cells = base?.cells;
  const cleanKey =
    cells?.kind === "exact"
      ? (address: number) => cells.orderAt(address)
      : cells?.kind === "dict" && cells.codesOrderAsText()
        ? (address: number) => cells.codeAt(address)
        : undefined;
  if (!snapshot || !base || !cells || !cleanKey) {
    // A UUID cell reads as its 16 bytes or null and an overlay value as
    // itself, neither of which can throw, so such a column reads a row only
    // when a comparison reaches it, once: a tie-breaker on an id then reads
    // just the rows that tie on every column before it. Any other column
    // reads every row once, so a read that throws does so whichever rows the
    // sort compares.
    if (cells?.kind === "uuid") {
      const read = new Array<QueryValue | undefined>(relation.rows);
      return {
        keys: undefined,
        loose: 0,
        valueAt: (row) => {
          let value = read[row];
          if (value === undefined) {
            value = relationValue(relation, row, field, snapshots);
            read[row] = value;
          }
          return value;
        },
        field,
        nullsFirst,
        descending,
      };
    }
    const values: QueryValue[] = [];
    for (let row = 0; row < relation.rows; row++) {
      if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
      values.push(relationValue(relation, row, field, snapshots));
    }
    return {
      keys: undefined,
      loose: 0,
      valueAt: (row) => values[row] ?? null,
      field,
      nullsFirst,
      descending,
    };
  }
  const vector = relation.vectors[field.source];
  if (!vector) invalid("query relation vector is unavailable");
  const { dirty } = base;
  const { baseRows } = snapshot;
  const nullKey = nullsFirst
    ? Number.NEGATIVE_INFINITY
    : Number.POSITIVE_INFINITY;
  const sign = descending ? -1 : 1;
  const keys = new Float64Array(relation.rows);
  let loose = 0;
  for (let row = 0; row < relation.rows; row++) {
    if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
    const address = vector[row];
    if (address === undefined) invalid("query relation address is unavailable");
    // `null` for a null value, `undefined` for one no key stands for.
    let key: number | null | undefined;
    if (address === MISSING_ADDRESS) key = null;
    else if (address < baseRows && !dirty?.(address))
      key = cells.validAt(address) ? cleanKey(address) : null;
    else {
      const value = snapshot.valueAt(address, field.column);
      key = value === null ? null : overlayKey(cells, value);
    }
    if (key === undefined) {
      keys[row] = Number.NaN;
      loose++;
    } else keys[row] = key === null ? nullKey : sign * key;
  }
  return { keys, loose, valueAt: undefined, field, nullsFirst, descending };
}

function compareSortValues(
  left: QueryValue,
  right: QueryValue,
  column: SortColumn,
): number {
  if (left === null || right === null) {
    if (left === right) return 0;
    return (left === null) === column.nullsFirst ? -1 : 1;
  }
  const comparison = totalCompare(left, right);
  return column.descending ? -comparison : comparison;
}

/**
 * The order of two relation rows under `columns`, with the row number as the
 * last tie-breaker, never reversed: the provenance tie-breaker
 * `compareOrdered` applies to projected rows.
 */
function sortComparator(
  columns: readonly SortColumn[],
  relation: AddressRelation,
  snapshots: readonly OverlayQuerySnapshot[],
): (left: number, right: number) => number {
  const keys: Float64Array[] = [];
  for (const column of columns)
    if (column.keys && column.loose === 0) keys.push(column.keys);
  if (keys.length === columns.length) {
    const [only] = keys;
    if (only && keys.length === 1)
      return (left, right) => {
        const a = only[left] ?? 0;
        const b = only[right] ?? 0;
        return a < b ? -1 : a > b ? 1 : left - right;
      };
    return (left, right) => {
      for (const column of keys) {
        const a = column[left] ?? 0;
        const b = column[right] ?? 0;
        if (a < b) return -1;
        if (a > b) return 1;
      }
      return left - right;
    };
  }
  return (left, right) => {
    for (const column of columns) {
      let comparison: number;
      if (column.keys) {
        const a = column.keys[left] ?? 0;
        const b = column.keys[right] ?? 0;
        if (a < b) return -1;
        if (a > b) return 1;
        if (a === b) continue;
        comparison = compareSortValues(
          relationValue(relation, left, column.field, snapshots),
          relationValue(relation, right, column.field, snapshots),
          column,
        );
      } else
        comparison = compareSortValues(
          column.valueAt?.(left) ?? null,
          column.valueAt?.(right) ?? null,
          column,
        );
      if (comparison !== 0) return comparison;
    }
    return left - right;
  };
}

/**
 * `0` to `rows - 1` in `compare` order: insertion-sorted runs, then bottom-up
 * merge passes, each merge yielding every `SLICE_ROWS` rows it places.
 * `compare` must be a total order, so the result does not depend on stability.
 */
function* sortPermutation(
  rows: number,
  compare: (left: number, right: number) => number,
): Sliced<Uint32Array> {
  let source = new Uint32Array(rows);
  for (let index = 0; index < rows; index++) source[index] = index;
  for (let start = 0; start < rows; start += SORT_RUN) {
    if (start % SLICE_ROWS === 0 && start > 0) yield;
    const end = Math.min(start + SORT_RUN, rows);
    for (let index = start + 1; index < end; index++) {
      const item = source[index] ?? 0;
      let slot = index - 1;
      while (slot >= start && compare(source[slot] ?? 0, item) > 0) {
        source[slot + 1] = source[slot] ?? 0;
        slot--;
      }
      source[slot + 1] = item;
    }
  }
  let target = new Uint32Array(rows);
  let pace = 0;
  for (let width = SORT_RUN; width < rows; width *= 2) {
    for (let left = 0; left < rows; left += 2 * width) {
      const middle = Math.min(left + width, rows);
      const end = Math.min(left + 2 * width, rows);
      let first = left;
      let second = middle;
      let out = left;
      while (first < middle && second < end) {
        if (++pace === SLICE_ROWS) {
          pace = 0;
          yield;
        }
        const a = source[first] ?? 0;
        const b = source[second] ?? 0;
        if (compare(a, b) <= 0) {
          target[out++] = a;
          first++;
        } else {
          target[out++] = b;
          second++;
        }
      }
      target.set(source.subarray(first, middle), out);
      out += middle - first;
      target.set(source.subarray(second, end), out);
    }
    [source, target] = [target, source];
  }
  return source;
}

/** The first `limit` of `0` to `rows - 1` in `compare` order, `limit` small. */
function* topPermutation(
  rows: number,
  limit: number,
  compare: (left: number, right: number) => number,
): Sliced<Uint32Array> {
  const kept: number[] = [];
  for (let row = 0; row < rows; row++) {
    if (row % SLICE_ROWS === SLICE_ROWS - 1) yield;
    const last = kept[kept.length - 1];
    if (kept.length === limit && last !== undefined && compare(row, last) > 0)
      continue;
    let low = 0;
    let high = kept.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (compare(kept[middle] ?? 0, row) < 0) low = middle + 1;
      else high = middle;
    }
    kept.splice(low, 0, row);
    if (kept.length > limit) kept.pop();
  }
  return Uint32Array.from(kept);
}

/** The relation's rows in `permutation` order. */
function* permute(
  relation: AddressRelation,
  permutation: Uint32Array,
): Sliced<AddressRelation> {
  const vectors: Uint32Array[] = [];
  for (const vector of relation.vectors) {
    const permuted = new Uint32Array(permutation.length);
    for (let index = 0; index < permutation.length; index++) {
      if (index % (16 * SLICE_ROWS) === 16 * SLICE_ROWS - 1) yield;
      permuted[index] = vector[permutation[index] ?? 0] ?? 0;
    }
    vectors.push(permuted);
  }
  return { vectors, rows: permutation.length };
}

/** The relation's first `rows` rows, `rows` at most its own. */
function firstRows(relation: AddressRelation, rows: number): AddressRelation {
  return rows === relation.rows
    ? relation
    : {
        vectors: relation.vectors.map((vector) => vector.subarray(0, rows)),
        rows,
      };
}

/**
 * A non-aggregate plan's relation in result order and cut to `limit`: still
 * address vectors, so a result reads only the rows it publishes. A small
 * limit keeps the best rows as it scans; anything else sorts every row.
 */
function* orderRelation(
  relation: AddressRelation,
  query: CompiledQuery,
  snapshots: readonly OverlayQuerySnapshot[],
  limit: number | undefined,
): Sliced<AddressRelation> {
  const rows =
    limit === undefined ? relation.rows : Math.min(limit, relation.rows);
  if (query.orders.length === 0) return firstRows(relation, rows);
  const columns: SortColumn[] = [];
  for (const order of query.orders) {
    const projection = query.projections[order.column];
    if (projection?.kind !== "field")
      invalid("aggregate projection requires aggregate mode");
    columns.push(
      yield* sortColumn(relation, projection.field, order, snapshots),
    );
  }
  const compare = sortComparator(columns, relation, snapshots);
  const permutation =
    rows < relation.rows && rows <= MAX_TOP_ROWS
      ? yield* topPermutation(relation.rows, rows, compare)
      : (yield* sortPermutation(relation.rows, compare)).subarray(0, rows);
  return yield* permute(relation, permutation);
}

/** An aggregate plan's groups in result order, cut to `limit`. */
function* orderGroups(
  groups: readonly ProjectedRow[],
  orders: readonly CompiledOrder[],
  limit: number | undefined,
): Sliced<readonly ProjectedRow[]> {
  const rows =
    limit === undefined ? groups.length : Math.min(limit, groups.length);
  if (orders.length === 0)
    return rows === groups.length ? groups : groups.slice(0, rows);
  const group = (index: number): ProjectedRow => {
    const row = groups[index];
    if (!row) invalid("group state is unavailable");
    return row;
  };
  const permutation = yield* sortPermutation(groups.length, (left, right) =>
    compareOrdered(group(left), group(right), orders),
  );
  return Array.from(permutation.subarray(0, rows), group);
}

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function resultValue(value: QueryValue): QueryOutputValue {
  // Copied, not aliased: an artifact accessor hands out its own copy already,
  // but an overlay row hands out the array it is still holding, and a result is
  // caller-owned. A uuid stays bytes here; `@calque/client` hands it to the
  // application as text.
  if (value instanceof Uint8Array) return new Uint8Array(value);
  if (typeof value === "object" && value !== null) return sealJson(value);
  return value;
}

function payloadBytes(value: QueryValue): number {
  if (value === null || typeof value === "boolean") return 1;
  if (typeof value === "number" || typeof value === "bigint") return 8;
  if (typeof value === "string") return encoder.encode(value).byteLength;
  if (value instanceof Uint8Array) return value.byteLength;
  return encoder.encode(canonicalJson(value)).byteLength;
}

function sealResult(
  projections: readonly CompiledProjection[],
  rows: readonly ProjectedRow[],
  maxBytes: number,
): QueryResult {
  let bytes = projections.reduce(
    (total, projection) =>
      checkedAdd(
        total,
        encoder.encode(projection.as).byteLength,
        "result bytes",
      ),
    0,
  );
  for (const row of rows)
    for (const value of row.values) {
      bytes = checkedAdd(bytes, payloadBytes(value), "result bytes");
      if (bytes > maxBytes)
        throw new QueryError(
          "RESULT_TOO_LARGE",
          `query result exceeds maxBytes (${maxBytes}); select fewer columns, add .limit(n), pass { maxBytes } up to ${MAX_QUERY_RESULT_BYTES}, or read it in pages with .window(offset, count)`,
        );
    }
  return Object.freeze({
    columns: Object.freeze(projections.map((projection) => projection.as)),
    rows: Object.freeze(
      rows.map((row) => Object.freeze(row.values.map(resultValue))),
    ),
  });
}

function normalizeOptions(
  options: QueryExecutionOptions,
): NormalizedQueryOptions {
  const maxRows = options.maxRows ?? DEFAULT_QUERY_RESULT_ROWS;
  const maxBytes = options.maxBytes ?? DEFAULT_QUERY_RESULT_BYTES;
  if (
    !Number.isSafeInteger(maxRows) ||
    maxRows < 1 ||
    maxRows > MAX_QUERY_RESULT_ROWS
  )
    invalid(`maxRows must be an integer in 1..${MAX_QUERY_RESULT_ROWS}`);
  if (
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 1 ||
    maxBytes > MAX_QUERY_RESULT_BYTES
  )
    invalid(`maxBytes must be an integer in 1..${MAX_QUERY_RESULT_BYTES}`);
  return { maxRows, maxBytes };
}

function checkedAdd(left: number, right: number, label: string): number {
  const result = left + right;
  if (!Number.isSafeInteger(result) || result < 0)
    capacity(`${label} exceeds the safe integer range`);
  return result;
}

function checkedProduct(left: number, right: number, label: string): number {
  if (
    !Number.isSafeInteger(left) ||
    !Number.isSafeInteger(right) ||
    left < 0 ||
    right < 0
  )
    capacity(`${label} is outside the safe integer range`);
  if (left !== 0 && right > Math.floor(Number.MAX_SAFE_INTEGER / left))
    capacity(`${label} exceeds the safe integer range`);
  return left * right;
}

/**
 * What one row of the widest relation this plan can build costs, in bytes. The
 * same number sizes the reservation before execution and the growth an
 * unbounded join asks for once its output is counted. A join allocates a fresh
 * address vector per source while the relation it consumes is still live, so a
 * row is charged two full widths. An aggregate plan's per-group cost is
 * `groupRowBytes`, charged per group rather than per relation row.
 */
function relationRowBytes(specification: ValidatedQuerySpecification): number {
  const sources = specification.joins.length + 1;
  let bytes =
    specification.joins.length === 0
      ? 0
      : checkedProduct(sources, 8, "join address vectors");
  if (specification.where)
    bytes = checkedAdd(
      bytes,
      checkedProduct(sources, 4, "filter vectors"),
      "relation row",
    );
  if (isAggregatePlan(specification)) return bytes;
  if (specification.orderBy.length > 0)
    bytes = checkedAdd(bytes, sortRowBytes(specification), "relation row");
  return bytes;
}

/**
 * What ordering one relation row costs: its slot in the permutation and in
 * the merge buffer, its addresses in the ordered relation, and per ordering
 * column an 8-byte key, or a value charged as a projected cell where the
 * column has no keys (`sortColumn`).
 */
function sortRowBytes(specification: ValidatedQuerySpecification): number {
  return checkedAdd(
    checkedAdd(
      8,
      checkedProduct(specification.joins.length + 1, 4, "ordered addresses"),
      "sort row",
    ),
    checkedProduct(specification.orderBy.length, 40, "sort keys"),
    "sort row",
  );
}

/** What one group of an aggregate plan costs in bytes; 0 for other plans. */
function groupRowBytes(specification: ValidatedQuerySpecification): number {
  if (!isAggregatePlan(specification)) return 0;
  const aggregates = specification.select.filter(
    (projection) => projection.kind === "aggregate",
  ).length;
  return checkedAdd(
    projectedRowBytes(specification),
    checkedAdd(
      128,
      checkedAdd(
        checkedProduct(specification.groupBy.length, 32, "group keys"),
        checkedProduct(aggregates, 64, "aggregate states"),
        "group entries",
      ),
      "group entries",
    ),
    "group row",
  );
}

/** One projected output row: its cells, and its ordering slot. */
function projectedRowBytes(specification: ValidatedQuerySpecification): number {
  const bytes = checkedAdd(
    64,
    checkedProduct(specification.select.length, 32, "projection cells"),
    "projection rows",
  );
  return specification.orderBy.length > 0
    ? checkedAdd(bytes, 16, "projection rows")
    : bytes;
}

/** The rule `compileQuery` applies to set `aggregateMode`. */
function isAggregatePlan(specification: ValidatedQuerySpecification): boolean {
  return (
    specification.groupBy.length > 0 ||
    specification.select.some((projection) => projection.kind === "aggregate")
  );
}

/**
 * How many groups an aggregate plan reserves before it runs: the one global
 * group when nothing is grouped, and none when something is, because only
 * counting says how many distinct keys the relation holds. `groupCharge` pays
 * for each group before `groupRows` creates it.
 */
function reservedGroups(specification: ValidatedQuerySpecification): number {
  if (!isAggregatePlan(specification)) return 0;
  return specification.groupBy.length === 0 ? 1 : 0;
}

/**
 * What the planner knows about one source of a plan: how many rows it can
 * yield, and the uniqueness fact that bounds a join into it.
 */
interface PlanSourceFacts {
  readonly rows: number;
  /** The collection's primary key, when its schema declares one. */
  readonly primaryKey: readonly string[] | undefined;
}

function planSourceFacts(
  specification: ValidatedQuerySpecification,
  collections: ReadonlyMap<string, OverlayQuerySnapshot>,
): readonly PlanSourceFacts[] {
  return [specification.from, ...specification.joins].map(({ collection }) => {
    const snapshot = collections.get(collection);
    if (!snapshot) invalid(`query collection ${collection} is unavailable`);
    return { rows: snapshot.order.length, primaryKey: snapshot.primaryKey };
  });
}

/**
 * Whether a schema fact says this equijoin matches at most one right row per
 * left row: the right source holds at most one row, or the equality is on its
 * whole primary key, which `parseDirectColumnarSchema` accepts only over
 * non-null exact-typed columns. A composite key never qualifies, because a join
 * has one equality. Nothing else counts; there is no selectivity guess.
 */
function joinFanoutIsOne(join: QueryJoin, right: PlanSourceFacts): boolean {
  if (right.rows <= 1) return true;
  const key = right.primaryKey;
  return (
    key !== undefined && key.length === 1 && key[0] === join.on.right.column
  );
}

/**
 * An upper bound on the relation cardinality after every join, or `undefined`
 * when no fact bounds it. A fanout of one means an inner join can only drop
 * left rows and a left join keeps one output row per left row, so the bound
 * stays the left source's cardinality; multiplying the sides would reserve the
 * Cartesian product of a key equijoin.
 */
function estimatedJoinedRows(
  specification: ValidatedQuerySpecification,
  facts: readonly PlanSourceFacts[],
): number | undefined {
  const rows = facts[0]?.rows ?? 0;
  for (let index = 0; index < specification.joins.length; index++) {
    const join = specification.joins[index];
    const right = facts[index + 1];
    if (!join || !right || !joinFanoutIsOne(join, right)) return undefined;
  }
  return rows;
}

/**
 * The limit a plan can apply to its base source before its joins, or
 * `undefined`: that of a plan without aggregates whose limit `orderRelation`
 * serves by keeping its best rows as it scans, and `maxRows` admits, and
 * whose filter and order keys read only the base.
 *
 * A join keeps its left rows in order and each left row's matches together,
 * so the rows joined from the base's best rows come first and the first
 * `limit` of them are the result. That fails only when an inner join leaves
 * fewer than `limit` rows while other base rows remain; the plan then joins
 * every base row, having paid one more scan of the base.
 */
function limitBeforeJoins(
  specification: ValidatedQuerySpecification,
  compiled: CompiledQuery,
  maxRows: number | undefined,
): number | undefined {
  const { limit } = specification;
  return !compiled.aggregateMode &&
    compiled.joins.length > 0 &&
    limit !== undefined &&
    limit <= MAX_TOP_ROWS &&
    (maxRows === undefined || maxRows >= limit) &&
    (compiled.predicate === undefined ||
      predicateReadsOnly(compiled.predicate, 0)) &&
    compiled.orders.every((order) => {
      const projection = compiled.projections[order.column];
      return projection?.kind === "field" && projection.field.source === 0;
    })
    ? limit
    : undefined;
}

/**
 * What a result costs to publish, whatever its plan read to produce it: the
 * specification, and the rows and bytes the options admit.
 */
function resultReservation(
  specification: ValidatedQuerySpecification,
  options: NormalizedQueryOptions,
): number {
  const specBytes = encoder.encode(canonicalQuery(specification)).byteLength;
  const total = checkedAdd(4 * 1024, specBytes, "query reservation");
  const publishedRows = Math.min(
    options.maxRows,
    specification.limit ?? options.maxRows,
  );
  const result = checkedAdd(
    checkedProduct(options.maxBytes, 2, "query result reservation"),
    checkedAdd(
      checkedProduct(publishedRows, 64, "query row metadata"),
      checkedAdd(
        checkedProduct(
          checkedProduct(
            publishedRows,
            specification.select.length,
            "query result cells",
          ),
          32,
          "query result cells",
        ),
        checkedProduct(
          specification.select.length,
          128,
          "query column metadata",
        ),
        "query result metadata",
      ),
      "query result metadata",
    ),
    "query result reservation",
  );
  return checkedAdd(total, result, "query reservation");
}

function estimateReservation(
  specification: ValidatedQuerySpecification,
  collections: ReadonlyMap<string, OverlayQuerySnapshot>,
  options: NormalizedQueryOptions,
): number {
  const facts = planSourceFacts(specification, collections);
  let total = resultReservation(specification, options);
  if (specification.limit === 0)
    return checkedAdd(total, RESERVATION_METADATA_BYTES, "query reservation");

  // The hash index each join builds over its right source, which is
  // proportional to that collection's rows.
  for (let index = 0; index < specification.joins.length; index++)
    total = checkedAdd(
      total,
      checkedProduct(
        facts[index + 1]?.rows ?? 0,
        JOIN_INDEX_ROW_BYTES,
        "join index reservation",
      ),
      "query reservation",
    );
  // The block evaluator's scratch, fixed by the predicate's shape.
  if (specification.where)
    total = checkedAdd(
      total,
      filterScratchBytes(specification.where),
      "query reservation",
    );
  // Everything proportional to relation rows, for as many rows as the schema
  // bounds. A plan no fact bounds reserves none of it here; `executePlan`
  // charges it against the rows it counts.
  total = checkedAdd(
    total,
    checkedProduct(
      estimatedJoinedRows(specification, facts) ?? 0,
      relationRowBytes(specification),
      "relation reservation",
    ),
    "query reservation",
  );
  total = checkedAdd(
    total,
    checkedProduct(
      reservedGroups(specification),
      groupRowBytes(specification),
      "group reservation",
    ),
    "query reservation",
  );
  return checkedAdd(total, RESERVATION_METADATA_BYTES, "query reservation");
}

/**
 * Grows a live reservation, or refuses. Held by `ColumnarQueryEngine.query`
 * for the duration of one execution, so a plan whose cardinality no schema
 * fact bounds pays for the rows it has counted.
 */
interface QueryReservationGrowth {
  grow(bytes: number, detail: string): void;
}

/**
 * Charges relation rows against the reservation as execution discovers them,
 * before any address vector is allocated. It starts at the cardinality
 * `estimateReservation` paid for, so a plan the schema bounds never grows, and
 * runs on every plan, so an under-estimate becomes a growth rather than an
 * unreserved allocation.
 */
function relationCharge(
  specification: ValidatedQuerySpecification,
  facts: readonly PlanSourceFacts[],
  growth: QueryReservationGrowth,
): (rows: number, detail: () => string) => void {
  const rowBytes = relationRowBytes(specification);
  let charged = estimatedJoinedRows(specification, facts) ?? 0;
  return (rows, detail) => {
    if (rows <= charged) return;
    const extra = checkedProduct(rows - charged, rowBytes, "relation growth");
    charged = rows;
    growth.grow(extra, detail());
  };
}

/**
 * Charges an aggregate plan's groups against the reservation as `groupRows`
 * discovers them, before each is created. Starts at the groups
 * `estimateReservation` already paid for, so a global aggregate never grows.
 */
function groupCharge(
  specification: ValidatedQuerySpecification,
  growth: QueryReservationGrowth,
): (groups: number) => void {
  const groupBytes = groupRowBytes(specification);
  let charged = reservedGroups(specification);
  return (groups) => {
    if (groups <= charged) return;
    const extra = checkedProduct(groups - charged, groupBytes, "group growth");
    charged = groups;
    growth.grow(extra, `grouping produced ${groups} groups`);
  };
}

/**
 * What a plan produced. A plan without aggregates keeps its result as address
 * vectors in result order, so a caller reads only the rows it publishes; an
 * aggregate plan's result is its groups.
 */
type PlanOutput =
  | {
      readonly kind: "relation";
      readonly compiled: CompiledQuery;
      readonly snapshots: readonly OverlayQuerySnapshot[];
      readonly relation: AddressRelation;
    }
  | {
      readonly kind: "groups";
      readonly compiled: CompiledQuery;
      readonly rows: readonly ProjectedRow[];
    };

function outputRows(output: PlanOutput): number {
  return output.kind === "relation" ? output.relation.rows : output.rows.length;
}

/** Rows `start` to `end` of a plan's result, projected. */
function* outputRange(
  output: PlanOutput,
  start: number,
  end: number,
): Sliced<readonly ProjectedRow[]> {
  if (output.kind === "groups") return output.rows.slice(start, end);
  return yield* projectRange(
    output.relation,
    output.compiled,
    output.snapshots,
    start,
    end,
  );
}

function tooManyRows(rows: number, maxRows: number): QueryError {
  return new QueryError(
    "RESULT_TOO_LARGE",
    `query result has ${rows} rows, above maxRows (${maxRows}); add .limit(n), pass { maxRows } up to ${MAX_QUERY_RESULT_ROWS}, or read it in pages with .window(offset, count)`,
  );
}

function sourceLabel(source: QuerySource | QueryJoin): string {
  return `${source.as} (${source.collection})`;
}

/** `relation` through every join of the plan, each charged as it grows. */
function* joinSources(
  relation: AddressRelation,
  specification: ValidatedQuerySpecification,
  compiled: CompiledQuery,
  snapshots: readonly OverlayQuerySnapshot[],
  inspection: MutableQueryExecutionInspection,
  charge: (rows: number, detail: () => string) => void,
): Sliced<AddressRelation> {
  let joined = relation;
  for (const [index, join] of compiled.joins.entries()) {
    const leftRows = joined.rows;
    inspection.rowsJoined += leftRows;
    joined = yield* applyJoin(joined, join, snapshots, (rows, rightRows) => {
      charge(rows, () => {
        const edge = specification.joins[index];
        return edge === undefined
          ? `join ${index} produced ${rows} rows`
          : `${edge.kind} join ${sourceLabel(edge)} on ${edge.on.left.source}.${edge.on.left.column} = ${edge.on.right.source}.${edge.on.right.column} produced ${rows} rows from ${leftRows} left rows and ${rightRows} right rows`;
      });
    });
  }
  return joined;
}

function* executePlan(
  specification: ValidatedQuerySpecification,
  collections: ReadonlyMap<string, OverlayQuerySnapshot>,
  inspection: MutableQueryExecutionInspection,
  features: QueryExecutionFeatures,
  growth: QueryReservationGrowth,
  maxRows: number | undefined,
): Sliced<PlanOutput> {
  const snapshots = [specification.from, ...specification.joins].map(
    ({ collection }) => {
      const snapshot = collections.get(collection);
      if (!snapshot) invalid(`query collection ${collection} is unavailable`);
      return snapshot;
    },
  );
  const compiled = compileQuery(specification, snapshots);
  if (specification.limit === 0)
    return compiled.aggregateMode
      ? { kind: "groups", compiled, rows: [] }
      : {
          kind: "relation",
          compiled,
          snapshots,
          relation: {
            vectors: snapshots.map(() => new Uint32Array()),
            rows: 0,
          },
        };
  // The same facts both charges are measured against, and the same ones
  // `estimateReservation` already reserved from.
  const facts = planSourceFacts(specification, collections);
  const charge = relationCharge(specification, facts, growth);
  let relation: AddressRelation = {
    vectors: [snapshots[0]?.order ?? new Uint32Array()],
    rows: snapshots[0]?.order.length ?? 0,
  };
  charge(
    relation.rows,
    () => `source ${sourceLabel(specification.from)} has ${relation.rows} rows`,
  );
  // A predicate that reads only the base source decides each joined row from
  // its base row. Filtering first leaves the joined relation the same rows in
  // the same order, built from fewer, and lets the filter use the base's
  // encoded vectors and chunk statistics.
  const filterFirst =
    compiled.predicate !== undefined &&
    compiled.joins.length > 0 &&
    predicateReadsOnly(compiled.predicate, 0);
  if (filterFirst)
    relation = yield* applyFilter(
      relation,
      compiled.predicate,
      snapshots,
      inspection,
      features,
    );
  // Where a limit applies to the base, any filter has already run, so
  // `relation` is the base rows the joins would read.
  const early = features.limitBeforeJoins
    ? limitBeforeJoins(specification, compiled, maxRows)
    : undefined;
  if (early !== undefined) {
    const best = yield* orderRelation(relation, compiled, snapshots, early);
    const joined = yield* joinSources(
      best,
      specification,
      compiled,
      snapshots,
      inspection,
      charge,
    );
    if (joined.rows >= early || best.rows === relation.rows)
      return {
        kind: "relation",
        compiled,
        snapshots,
        relation: firstRows(joined, Math.min(early, joined.rows)),
      };
  }
  relation = yield* joinSources(
    relation,
    specification,
    compiled,
    snapshots,
    inspection,
    charge,
  );
  if (!filterFirst)
    relation = yield* applyFilter(
      relation,
      compiled.predicate,
      snapshots,
      inspection,
      features,
    );
  if (compiled.aggregateMode) {
    const groups = yield* groupRows(
      relation,
      compiled,
      snapshots,
      groupCharge(specification, growth),
    );
    const rows = yield* orderGroups(
      groups,
      compiled.orders,
      specification.limit,
    );
    if (maxRows !== undefined && rows.length > maxRows)
      throw tooManyRows(rows.length, maxRows);
    return { kind: "groups", compiled, rows };
  }
  // Refused before the sort, which is most of the work.
  const rows =
    specification.limit === undefined
      ? relation.rows
      : Math.min(specification.limit, relation.rows);
  if (maxRows !== undefined && rows > maxRows) throw tooManyRows(rows, maxRows);
  return {
    kind: "relation",
    compiled,
    snapshots,
    relation: yield* orderRelation(
      relation,
      compiled,
      snapshots,
      specification.limit,
    ),
  };
}

/** What a result retained for later windows holds on to. */
function retainedBytes(output: PlanOutput): number {
  const rows =
    output.kind === "relation"
      ? checkedProduct(
          output.relation.rows,
          checkedProduct(output.relation.vectors.length, 4, "ordered result"),
          "ordered result",
        )
      : checkedProduct(
          output.rows.length,
          checkedAdd(
            64,
            checkedProduct(
              output.compiled.projections.length,
              32,
              "ordered result",
            ),
            "ordered result",
          ),
          "ordered result",
        );
  return checkedAdd(rows, RESERVATION_METADATA_BYTES, "ordered result");
}

/**
 * The ordered results windows are read from, at most this many at once. A
 * grid scrolling one view reads the same one over and over; a few more keep a
 * view the page switches back to.
 */
const MAX_ORDERED_RESULTS = 4;

/** A window over a query's whole result, in result order. */
export interface QueryWindow {
  readonly offset: number;
  /** At most `MAX_QUERY_RESULT_ROWS`; 0 asks only for the total. */
  readonly count: number;
}

export interface QueryWindowOptions {
  readonly maxBytes?: number;
}

/**
 * Rows `offset` to `offset + count` of a query's whole result, fewer where
 * the result ends, and how many rows the whole result has.
 */
export interface QueryWindowResult extends QueryResult {
  readonly offset: number;
  readonly total: number;
}

export interface QueryCompletion<Result> {
  readonly result: Result;
  /**
   * The generation of every collection the query read. The whole execution
   * ran at these generations.
   */
  readonly generations: Readonly<Record<string, number>>;
}

/**
 * One query executed in steps, so its caller can run other work between them.
 *
 * `step` runs until the query finishes or until `pause` returns true between
 * two slices of work, and returns the completion, or `undefined` when there is
 * more to do. A step that finds a collection the query reads has moved since
 * the last one discards everything computed so far and starts again at the
 * new state: a result is always computed at one state, never assembled from
 * two. After `MAX_QUERY_RESTARTS` restarts the next attempt ignores `pause`.
 *
 * `cancel` releases the reservation. A task that failed, finished, or was
 * cancelled holds nothing.
 */
export interface QueryTask<Result> {
  step(pause: () => boolean): QueryCompletion<Result> | undefined;
  cancel(): void;
}

interface Reservation {
  readonly id: number;
  /** Grows during execution when a counted relation exceeds its estimate. */
  bytes: number;
}

/** A finished result kept so that later windows over it skip the plan. */
interface OrderedResult {
  readonly key: string;
  readonly generations: ReadonlyMap<string, number>;
  readonly output: PlanOutput;
  readonly reservation: Reservation;
  /**
   * Tasks reading from it. An entry in use is never evicted; one a newer
   * result replaced keeps its reservation until the last of them ends.
   */
  readers: number;
}

interface Attempt<Result> {
  readonly collections: ReadonlyMap<string, OverlayQuerySnapshot>;
  readonly reservation: Reservation;
  readonly inspection: MutableQueryExecutionInspection;
  readonly statisticsRowsBefore: number;
  readonly cached: OrderedResult | undefined;
  readonly work: Sliced<{
    readonly result: Result;
    readonly output: PlanOutput;
  }>;
}

/** Runs a task to completion in one step, for the synchronous entry points. */
function complete<Result>(task: QueryTask<Result>): Result {
  const completion = task.step(() => false);
  if (!completion)
    throw new Error("a query ran without pausing but did not finish");
  return completion.result;
}

function normalizeWindow(window: QueryWindow): QueryWindow {
  const { offset, count } = window;
  if (!Number.isSafeInteger(offset) || offset < 0)
    invalid("window offset must be a non-negative safe integer");
  if (
    !Number.isSafeInteger(count) ||
    count < 0 ||
    count > MAX_QUERY_RESULT_ROWS
  )
    invalid(`window count must be an integer in 0..${MAX_QUERY_RESULT_ROWS}`);
  return { offset, count };
}

function statisticsSum(
  snapshots: Iterable<OverlayQuerySnapshot>,
  measure: (snapshot: OverlayQuerySnapshot) => number,
): number {
  let total = 0;
  for (const snapshot of snapshots) total += measure(snapshot);
  return total;
}

/**
 * Query evaluator over one overlay. Its memory pool is fixed when it is
 * created. Each execution takes a single-use reservation before allocating and
 * releases it when it finishes, fails or is cancelled; results kept for later
 * windows hold their own and are given up first when a new one does not fit.
 */
export class ColumnarQueryEngine {
  readonly #overlay: OverlayEngine;
  readonly #capacityBytes: number;
  readonly #maxReservations: number;
  readonly #reservations = new Map<number, number>();
  /** Statistics each collection holds resident, as last observed. */
  readonly #statistics = new Map<string, number>();
  /** Least recently read first. */
  readonly #ordered: OrderedResult[] = [];
  #reservedBytes = 0;
  #nextReservation = 1;

  constructor(overlay: OverlayEngine, capacity: ColumnarQueryCapacity) {
    if (
      !Number.isSafeInteger(capacity.capacityBytes) ||
      capacity.capacityBytes <= 0
    )
      invalid("query capacity must be a positive safe integer");
    const maxReservations = capacity.maxReservations ?? MAX_QUERY_RESERVATIONS;
    if (
      !Number.isSafeInteger(maxReservations) ||
      maxReservations < 1 ||
      maxReservations > MAX_QUERY_RESERVATIONS
    )
      invalid(`maxReservations must be in 1..${MAX_QUERY_RESERVATIONS}`);
    this.#overlay = overlay;
    this.#capacityBytes = capacity.capacityBytes;
    this.#maxReservations = maxReservations;
  }

  /**
   * What `query` reserves before it executes this specification.
   *
   * Exact for a plan whose joins the primary keys bound and which groups by
   * nothing. Otherwise it is a floor: the rest is charged against counted rows
   * and groups during execution, so a pool this size may still refuse.
   */
  estimateReservationBytes(
    specification: unknown,
    options: QueryExecutionOptions = {},
  ): number {
    const validated = validateQuerySpecification(specification);
    const normalized = normalizeOptions(options);
    const collections = this.#snapshots(validated);
    return checkedAdd(
      estimateReservation(validated, collections, normalized),
      statisticsSum(collections.values(), (snapshot) =>
        Math.max(
          0,
          snapshot.statisticsCapacityBytes() - snapshot.statisticsBytes(),
        ),
      ),
      "query statistics reservation",
    );
  }

  query(
    specification: unknown,
    options: QueryExecutionOptions = {},
  ): QueryResult {
    return complete(this.start(specification, options));
  }

  /** `window` over the whole result, in one uninterrupted execution. */
  window(
    specification: unknown,
    window: QueryWindow,
    options: QueryWindowOptions = {},
  ): QueryWindowResult {
    return complete(this.startWindow(specification, window, options));
  }

  /**
   * `query` as a task. The specification and options are checked here, so an
   * invalid query throws before any step.
   */
  start(
    specification: unknown,
    options: QueryExecutionOptions = {},
  ): QueryTask<QueryResult> {
    const validated = validateQuerySpecification(specification);
    const normalized = normalizeOptions(options);
    return this.#task(
      validated,
      normalized,
      normalized.maxRows,
      undefined,
      function* (output) {
        return sealResult(
          output.compiled.projections,
          yield* outputRange(output, 0, outputRows(output)),
          normalized.maxBytes,
        );
      },
    );
  }

  /**
   * `window` as a task. The whole result is kept, as addresses, while the
   * collections it read stay where they were, so the next window over the
   * same specification reads only its own rows.
   */
  startWindow(
    specification: unknown,
    window: QueryWindow,
    options: QueryWindowOptions = {},
  ): QueryTask<QueryWindowResult> {
    const { specification: checked, canonical } = canonicalized(specification);
    const { offset, count } = normalizeWindow(window);
    const normalized = normalizeOptions({
      maxRows: Math.max(1, count),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    });
    return this.#task(
      checked,
      normalized,
      undefined,
      canonical,
      function* (output) {
        const total = outputRows(output);
        const start = Math.min(offset, total);
        const sealed = sealResult(
          output.compiled.projections,
          yield* outputRange(output, start, Math.min(offset + count, total)),
          normalized.maxBytes,
        );
        return Object.freeze({ ...sealed, offset: start, total });
      },
    );
  }

  #task<Result>(
    validated: ValidatedQuerySpecification,
    options: NormalizedQueryOptions,
    maxRows: number | undefined,
    orderedKey: string | undefined,
    publish: (output: PlanOutput) => Sliced<Result>,
  ): QueryTask<Result> {
    let attempt: Attempt<Result> | undefined;
    let restarts = 0;
    let finished = false;
    const dispose = () => {
      const current = attempt;
      if (!current) return;
      attempt = undefined;
      const read = current.cached;
      if (read && --read.readers === 0 && !this.#ordered.includes(read))
        this.#release(read.reservation);
      this.#observeStatistics(current.collections.values());
      this.#release(current.reservation);
    };
    const moved = (current: Attempt<Result>) => {
      for (const snapshot of current.collections.values())
        if (
          this.#overlay.generation(snapshot.collection) !== snapshot.generation
        )
          return true;
      return false;
    };
    const begin = (): Attempt<Result> => {
      const collections = this.#snapshots(validated);
      this.#observeStatistics(collections.values());
      const cached =
        orderedKey === undefined
          ? undefined
          : this.#cached(orderedKey, collections);
      const required = cached
        ? checkedAdd(
            resultReservation(validated, options),
            RESERVATION_METADATA_BYTES,
            "query reservation",
          )
        : checkedAdd(
            estimateReservation(validated, collections, options),
            statisticsSum(collections.values(), (snapshot) =>
              Math.max(
                0,
                snapshot.statisticsCapacityBytes() - snapshot.statisticsBytes(),
              ),
            ),
            "query statistics reservation",
          );
      // Held before the reservation, which may otherwise evict it to fit.
      if (cached) cached.readers++;
      let reservation: Reservation;
      try {
        reservation = this.#reserve(required);
      } catch (error) {
        if (cached) cached.readers--;
        throw error;
      }
      const inspection: MutableQueryExecutionInspection = {
        rowsExamined: 0,
        chunksExamined: 0,
        chunksSkipped: 0,
        encodedComparisons: 0,
        materializedComparisons: 0,
        rowsJoined: 0,
      };
      const features = QUERY_FEATURES.get(this) ?? DEFAULT_QUERY_FEATURES;
      const growth: QueryReservationGrowth = {
        grow: (bytes, detail) => this.#grow(reservation, bytes, detail),
      };
      return {
        collections,
        reservation,
        inspection,
        statisticsRowsBefore: statisticsSum(collections.values(), (snapshot) =>
          snapshot.statisticsRowsScanned(),
        ),
        cached,
        work: (function* () {
          const output =
            cached?.output ??
            (yield* executePlan(
              validated,
              collections,
              inspection,
              features,
              growth,
              maxRows,
            ));
          return { result: yield* publish(output), output };
        })(),
      };
    };
    return {
      step: (pause) => {
        if (finished) throw new Error("this query task has already ended");
        try {
          if (attempt && moved(attempt)) {
            dispose();
            restarts++;
          }
          const current = (attempt ??= begin());
          const sliced = restarts < MAX_QUERY_RESTARTS;
          while (true) {
            const next = current.work.next();
            if (!next.done) {
              if (sliced && pause()) return undefined;
              continue;
            }
            const snapshots = [...current.collections.values()];
            const generations = Object.freeze(
              Object.fromEntries(
                snapshots.map((snapshot) => [
                  snapshot.collection,
                  snapshot.generation,
                ]),
              ),
            );
            QUERY_INSPECTORS.set(this, {
              ...current.inspection,
              statisticsRowsScanned:
                statisticsSum(snapshots, (snapshot) =>
                  snapshot.statisticsRowsScanned(),
                ) - current.statisticsRowsBefore,
              residentStatisticsBytes: statisticsSum(snapshots, (snapshot) =>
                snapshot.statisticsBytes(),
              ),
            });
            finished = true;
            dispose();
            if (orderedKey !== undefined && !current.cached)
              this.#remember(
                orderedKey,
                current.collections,
                next.value.output,
              );
            return { result: next.value.result, generations };
          }
        } catch (error) {
          finished = true;
          dispose();
          if (error instanceof QueryError) throw error;
          if (error instanceof RangeError)
            capacity(`query allocation failed: ${String(error)}`);
          throw error;
        }
      },
      cancel: () => {
        finished = true;
        dispose();
      },
    };
  }

  #snapshots(
    specification: ValidatedQuerySpecification,
  ): ReadonlyMap<string, OverlayQuerySnapshot> {
    const names = new Set(
      [specification.from, ...specification.joins].map(
        ({ collection }) => collection,
      ),
    );
    const snapshots = snapshotOverlayForQuery(this.#overlay, [...names]);
    return new Map(
      snapshots.map((snapshot) => [snapshot.collection, snapshot]),
    );
  }

  #observeStatistics(snapshots: Iterable<OverlayQuerySnapshot>): void {
    for (const snapshot of snapshots)
      this.#statistics.set(snapshot.collection, snapshot.statisticsBytes());
  }

  #residentStatisticsBytes(): number {
    let total = 0;
    for (const bytes of this.#statistics.values()) total += bytes;
    return total;
  }

  /** The kept result for `key`, when it was computed at these snapshots. */
  #cached(
    key: string,
    collections: ReadonlyMap<string, OverlayQuerySnapshot>,
  ): OrderedResult | undefined {
    const index = this.#ordered.findIndex((entry) => entry.key === key);
    const entry = this.#ordered[index];
    if (!entry) return undefined;
    let current = entry.generations.size === collections.size;
    for (const snapshot of collections.values())
      if (entry.generations.get(snapshot.collection) !== snapshot.generation)
        current = false;
    if (!current) {
      if (entry.readers === 0) this.#forget(index);
      return undefined;
    }
    this.#ordered.splice(index, 1);
    this.#ordered.push(entry);
    return entry;
  }

  #remember(
    key: string,
    collections: ReadonlyMap<string, OverlayQuerySnapshot>,
    output: PlanOutput,
  ): void {
    const existing = this.#ordered.findIndex((entry) => entry.key === key);
    if (existing >= 0) {
      const [older] = this.#ordered.splice(existing, 1);
      if (older?.readers === 0) this.#release(older.reservation);
    }
    let reservation: Reservation;
    try {
      reservation = this.#reserve(retainedBytes(output), true);
    } catch (error) {
      // A result the pool cannot keep is not kept.
      if (error instanceof QueryError) return;
      throw error;
    }
    this.#ordered.push({
      key,
      generations: new Map(
        [...collections.values()].map((snapshot) => [
          snapshot.collection,
          snapshot.generation,
        ]),
      ),
      output,
      reservation,
      readers: 0,
    });
    while (this.#ordered.length > MAX_ORDERED_RESULTS && this.#evict());
  }

  #forget(index: number): void {
    const [entry] = this.#ordered.splice(index, 1);
    if (entry) this.#release(entry.reservation);
  }

  /** Gives up the least recently read kept result no task is reading. */
  #evict(): boolean {
    const index = this.#ordered.findIndex((entry) => entry.readers === 0);
    if (index < 0) return false;
    this.#forget(index);
    return true;
  }

  /**
   * A new reservation, giving up kept results no task is reading until it
   * fits. A kept result's own reservation (`kept`) is not an active one, and
   * does not count against `maxReservations`.
   */
  #reserve(bytes: number, kept = false): Reservation {
    if (
      !kept &&
      this.#reservations.size - this.#ordered.length >= this.#maxReservations
    )
      capacity(
        `at most ${this.#maxReservations} query reservations may be active`,
      );
    const required = () =>
      checkedAdd(
        this.#residentStatisticsBytes(),
        checkedAdd(this.#reservedBytes, bytes, "query capacity"),
        "query capacity",
      );
    while (required() > this.#capacityBytes && this.#evict());
    if (required() > this.#capacityBytes)
      capacity(
        `query structures require ${required()} bytes; pool is ${this.#capacityBytes}`,
        required(),
        this.#capacityBytes,
      );
    if (this.#nextReservation > Number.MAX_SAFE_INTEGER)
      capacity("query reservation sequence is exhausted");
    const reservation = { id: this.#nextReservation++, bytes };
    this.#reservations.set(reservation.id, bytes);
    this.#reservedBytes += bytes;
    return reservation;
  }

  /**
   * Adds to a live reservation, or refuses with the join and the counted rows
   * that did not fit. It never raises `capacityBytes`.
   */
  #grow(reservation: Reservation, bytes: number, detail: string): void {
    if (bytes <= 0) return;
    if (this.#reservations.get(reservation.id) !== reservation.bytes)
      throw new QueryError(
        "CAPACITY_EXCEEDED",
        "query reservation ownership was lost",
      );
    const grown = checkedAdd(reservation.bytes, bytes, "query capacity");
    const required = () =>
      checkedAdd(
        this.#residentStatisticsBytes(),
        checkedAdd(this.#reservedBytes, bytes, "query capacity"),
        "query capacity",
      );
    while (required() > this.#capacityBytes && this.#evict());
    if (required() > this.#capacityBytes)
      capacity(
        `${detail}; query structures require ${required()} bytes; pool is ${this.#capacityBytes}`,
        required(),
        this.#capacityBytes,
      );
    reservation.bytes = grown;
    this.#reservations.set(reservation.id, grown);
    this.#reservedBytes += bytes;
  }

  #release(reservation: Reservation): void {
    const bytes = this.#reservations.get(reservation.id);
    if (bytes !== reservation.bytes)
      throw new QueryError(
        "CAPACITY_EXCEEDED",
        "query reservation ownership was lost",
      );
    this.#reservations.delete(reservation.id);
    this.#reservedBytes -= bytes;
  }
}
