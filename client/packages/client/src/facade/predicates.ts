import { CONTROL_CHARACTER } from "../limits.js";
import type { QueryPredicate } from "./types.js";

type PredicateLiteral = boolean | number | bigint | string | Uint8Array;

type PredicateOperator = "eq" | "neq" | "lt" | "lte" | "gt" | "gte";

/**
 * The inert description behind a sealed predicate. Column references are
 * unresolved names (`level` or `project.slug`); the builder resolves them
 * against its own source aliases when it compiles the query.
 */
export type PredicateNode =
  | {
      readonly kind: "compare";
      readonly operator: PredicateOperator;
      readonly column: string;
      readonly value: PredicateLiteral;
    }
  | {
      readonly kind: "is-null";
      readonly column: string;
      readonly negated: boolean;
    }
  | { readonly kind: "and"; readonly predicates: readonly PredicateNode[] }
  | { readonly kind: "or"; readonly predicates: readonly PredicateNode[] }
  | { readonly kind: "not"; readonly predicate: PredicateNode };

const MAX_COLUMN_CODE_UNITS = 256;
const nodes = new WeakMap<object, PredicateNode>();

function seal<Row extends object>(node: PredicateNode): QueryPredicate<Row> {
  const capability = Object.freeze(Object.create(null)) as QueryPredicate<Row>;
  nodes.set(capability, node);
  return capability;
}

/** Recovers the node behind a predicate the helpers produced, or throws. */
export function resolvePredicate(value: unknown): PredicateNode {
  const node =
    typeof value === "object" && value !== null ? nodes.get(value) : undefined;
  if (!node)
    throw new TypeError(
      "predicate must be built by the Calque predicate helpers",
    );
  return node;
}

export function sealColumnName(value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > MAX_COLUMN_CODE_UNITS ||
    CONTROL_CHARACTER.test(value)
  )
    throw new TypeError("column must be a non-empty column name");
  return value;
}

function sealLiteral(
  operator: PredicateOperator,
  value: unknown,
): PredicateLiteral {
  // A comparison with NULL is never true, as in SQL, so a null literal could
  // only be a mistake for a NULL test.
  if (value === null)
    throw new TypeError(
      `${operator}() cannot compare with null; use isNull() or isNotNull()`,
    );
  if (
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    typeof value === "string"
  )
    return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new TypeError("predicate value must be a finite number");
    return value;
  }
  if (value instanceof Uint8Array) return Uint8Array.from(value);
  throw new TypeError(
    "predicate value must be a boolean, number, bigint, string, or bytes",
  );
}

function compare<Row extends object, Column extends keyof Row & string>(
  operator: PredicateOperator,
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return seal({
    kind: "compare",
    operator,
    column: sealColumnName(column),
    value: sealLiteral(operator, value),
  });
}

/**
 * Matches rows whose `column` equals `value`. As in SQL, a row whose
 * `column` is NULL matches no comparison.
 */
export function eq<Row extends object, Column extends keyof Row & string>(
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return compare("eq", column, value);
}

/**
 * Matches rows whose `column` differs from `value`. As in SQL, a row whose
 * `column` is NULL matches no comparison.
 */
export function neq<Row extends object, Column extends keyof Row & string>(
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return compare("neq", column, value);
}

/**
 * Matches rows whose `column` is less than `value`. As in SQL, a row whose
 * `column` is NULL matches no comparison.
 */
export function lt<Row extends object, Column extends keyof Row & string>(
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return compare("lt", column, value);
}

/**
 * Matches rows whose `column` is at most `value`. As in SQL, a row whose
 * `column` is NULL matches no comparison.
 */
export function lte<Row extends object, Column extends keyof Row & string>(
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return compare("lte", column, value);
}

/**
 * Matches rows whose `column` is greater than `value`. As in SQL, a row whose
 * `column` is NULL matches no comparison.
 */
export function gt<Row extends object, Column extends keyof Row & string>(
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return compare("gt", column, value);
}

/**
 * Matches rows whose `column` is at least `value`. As in SQL, a row whose
 * `column` is NULL matches no comparison.
 */
export function gte<Row extends object, Column extends keyof Row & string>(
  column: Column,
  value: NonNullable<Row[Column]>,
): QueryPredicate<Row> {
  return compare("gte", column, value);
}

/** Matches rows whose `column` is NULL. */
export function isNull<Row extends object>(
  column: keyof Row & string,
): QueryPredicate<Row> {
  return seal({
    kind: "is-null",
    column: sealColumnName(column),
    negated: false,
  });
}

/** Matches rows whose `column` is not NULL. */
export function isNotNull<Row extends object>(
  column: keyof Row & string,
): QueryPredicate<Row> {
  return seal({
    kind: "is-null",
    column: sealColumnName(column),
    negated: true,
  });
}

function combine<Row extends object>(
  kind: "and" | "or",
  predicates: readonly QueryPredicate<Row>[],
): QueryPredicate<Row> {
  if (predicates.length === 0)
    throw new TypeError(`${kind}() needs at least one predicate`);
  return seal({
    kind,
    predicates: Object.freeze(predicates.map(resolvePredicate)),
  });
}

/** Matches rows that every predicate matches; needs at least one. */
export function and<Row extends object>(
  ...predicates: readonly QueryPredicate<Row>[]
): QueryPredicate<Row> {
  return combine("and", predicates);
}

/** Matches rows that any predicate matches; needs at least one. */
export function or<Row extends object>(
  ...predicates: readonly QueryPredicate<Row>[]
): QueryPredicate<Row> {
  return combine("or", predicates);
}

/**
 * Matches rows `predicate` rejects. As in SQL, a row it cannot decide, such
 * as one whose compared column is NULL, matches neither it nor its `not`.
 */
export function not<Row extends object>(
  predicate: QueryPredicate<Row>,
): QueryPredicate<Row> {
  return seal({ kind: "not", predicate: resolvePredicate(predicate) });
}
