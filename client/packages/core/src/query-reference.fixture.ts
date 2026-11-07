/**
 * Independent, naive query oracle.
 *
 * It materializes merged overlay rows through the public read API, performs
 * nested-loop joins, finds groups by linear scan, and projects row arrays. It
 * imports only the query data types; no evaluator helper or private snapshot is
 * shared with the direct columnar implementation.
 */
import type { OverlayEngine, SqlValue } from "./overlay.js";
import type {
  QueryExpression,
  QueryField,
  QueryJoin,
  QueryJsonValue,
  QueryPredicate,
  QueryProjection,
  QueryResult,
  QuerySource,
  QuerySpecification,
} from "./query.js";

export interface QueryReferenceSchema {
  readonly name: string;
  readonly columns: readonly string[];
}

interface ReferenceSource {
  readonly alias: string;
  readonly columns: readonly string[];
  readonly rows: readonly (readonly ReferenceValue[])[];
}

type ReferenceValue =
  | SqlValue
  | readonly QueryJsonValue[]
  | { readonly [key: string]: QueryJsonValue };

type Binding = readonly (readonly ReferenceValue[] | null)[];

interface ReferenceProjectedRow {
  readonly values: readonly ReferenceValue[];
  readonly ordinal: number;
}

function byteEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index++)
    if (left[index] !== right[index]) return false;
  return true;
}

function scalarEqual(left: ReferenceValue, right: ReferenceValue): boolean {
  if (left instanceof Uint8Array || right instanceof Uint8Array)
    return (
      left instanceof Uint8Array &&
      right instanceof Uint8Array &&
      byteEqual(left, right)
    );
  if (
    typeof left === "object" &&
    left !== null &&
    typeof right === "object" &&
    right !== null
  )
    return stableJson(left) === stableJson(right);
  return Object.is(left, right);
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    if (typeof value === "number" && Object.is(value, -0)) return "-0";
    if (typeof value === "number" && Number.isNaN(value)) return "nan";
    return JSON.stringify(value);
  }
  if (Array.isArray(value))
    return `[${value.map((child) => stableJson(child)).join(",")}]`;
  return `{${Object.keys(value)
    .sort()
    .map(
      (key) =>
        `${JSON.stringify(key)}:${stableJson((value as Record<string, unknown>)[key])}`,
    )
    .join(",")}}`;
}

function numberOrder(left: number, right: number): number {
  if (Number.isNaN(left) || Number.isNaN(right)) {
    if (Number.isNaN(left) && Number.isNaN(right)) return 0;
    return Number.isNaN(left) ? 1 : -1;
  }
  if (left === 0 && right === 0) {
    if (Object.is(left, right)) return 0;
    return Object.is(left, -0) ? -1 : 1;
  }
  if (left < right) return -1;
  if (left > right) return 1;
  return 0;
}

function byteOrder(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.byteLength, right.byteLength);
  for (let index = 0; index < length; index++) {
    const difference = (left[index] ?? 0) - (right[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return left.byteLength - right.byteLength;
}

function sameKindOrder(
  left: ReferenceValue,
  right: ReferenceValue,
): number | undefined {
  if (left === null || right === null || typeof left !== typeof right)
    return undefined;
  if (typeof left === "number" && typeof right === "number")
    return numberOrder(left, right);
  if (typeof left === "bigint" && typeof right === "bigint")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "string" && typeof right === "string")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean")
    return Number(left) - Number(right);
  if (left instanceof Uint8Array && right instanceof Uint8Array)
    return byteOrder(left, right);
  if (
    typeof left === "object" &&
    left !== null &&
    typeof right === "object" &&
    right !== null
  ) {
    const a = stableJson(left);
    const b = stableJson(right);
    return a < b ? -1 : a > b ? 1 : 0;
  }
  return undefined;
}

function rank(value: Exclude<ReferenceValue, null>): number {
  if (typeof value === "boolean") return 0;
  if (typeof value === "number") return 1;
  if (typeof value === "bigint") return 2;
  if (typeof value === "string") return 3;
  if (value instanceof Uint8Array) return 4;
  return 5;
}

function scalarOrder(left: ReferenceValue, right: ReferenceValue): number {
  if (left === null || right === null)
    return left === right ? 0 : left === null ? -1 : 1;
  const difference = rank(left) - rank(right);
  return difference === 0 ? (sameKindOrder(left, right) ?? 0) : difference;
}

function sourceIndex(
  sources: readonly ReferenceSource[],
  alias: string,
): number {
  const index = sources.findIndex((source) => source.alias === alias);
  if (index < 0) throw new Error(`oracle: unknown source ${alias}`);
  return index;
}

function fieldValue(
  field: QueryField,
  binding: Binding,
  sources: readonly ReferenceSource[],
): ReferenceValue {
  const source = sourceIndex(sources, field.source);
  const row = binding[source];
  if (row === null || row === undefined) return null;
  const column = sources[source]?.columns.indexOf(field.column) ?? -1;
  if (column < 0) throw new Error(`oracle: unknown column ${field.column}`);
  return row[column] ?? null;
}

function expressionValue(
  expression: QueryExpression,
  binding: Binding,
  sources: readonly ReferenceSource[],
): ReferenceValue {
  return expression.kind === "literal"
    ? expression.value
    : fieldValue(expression.field, binding, sources);
}

/**
 * SQL's three-valued logic, evaluated node by node, with `null` for UNKNOWN.
 * A comparison with a NULL operand is UNKNOWN, and so is an ordering
 * comparison `sameKindOrder` cannot decide, as across types; `eq` and `neq`
 * across types are two-valued. `and`, `or` and `not` are Kleene's, and a row
 * is kept only when its predicate is TRUE.
 */
function predicateTruth(
  predicate: QueryPredicate,
  binding: Binding,
  sources: readonly ReferenceSource[],
): boolean | null {
  if (predicate.kind === "not") {
    const truth = predicateTruth(predicate.predicate, binding, sources);
    return truth === null ? null : !truth;
  }
  if (predicate.kind === "and" || predicate.kind === "or") {
    const truths = predicate.predicates.map((child) =>
      predicateTruth(child, binding, sources),
    );
    // TRUE settles an `or` and FALSE an `and`, whatever else is UNKNOWN.
    const decisive = predicate.kind === "or";
    if (truths.includes(decisive)) return decisive;
    return truths.includes(null) ? null : !decisive;
  }
  if (predicate.kind === "is-null") {
    const isNull =
      expressionValue(predicate.expression, binding, sources) === null;
    return predicate.negated ? !isNull : isNull;
  }
  const left = expressionValue(predicate.left, binding, sources);
  const right = expressionValue(predicate.right, binding, sources);
  if (left === null || right === null) return null;
  if (predicate.operator === "eq") return scalarEqual(left, right);
  if (predicate.operator === "neq") return !scalarEqual(left, right);
  const comparison = sameKindOrder(left, right);
  if (comparison === undefined) return null;
  if (predicate.operator === "lt") return comparison < 0;
  if (predicate.operator === "lte") return comparison <= 0;
  if (predicate.operator === "gt") return comparison > 0;
  return comparison >= 0;
}

function sameGroupKey(
  left: readonly ReferenceValue[],
  right: readonly ReferenceValue[],
) {
  return (
    left.length === right.length &&
    left.every((value, index) => scalarEqual(value, right[index] ?? null))
  );
}

function aggregate(
  projection: Extract<QueryProjection, { readonly kind: "aggregate" }>,
  bindings: readonly Binding[],
  sources: readonly ReferenceSource[],
): ReferenceValue {
  const values =
    projection.field === undefined
      ? []
      : bindings.map((binding) =>
          fieldValue(projection.field as QueryField, binding, sources),
        );
  if (projection.operator === "count")
    return projection.field === undefined
      ? bindings.length
      : values.filter((value) => value !== null).length;
  const present = values.filter(
    (value): value is Exclude<ReferenceValue, null> => value !== null,
  );
  const first = present[0];
  if (first === undefined) return null;
  if (projection.operator === "sum") {
    if (typeof first === "bigint")
      return present.reduce(
        (total: bigint, value) => total + (value as bigint),
        0n,
      );
    return present.reduce(
      (total: number, value) => total + (value as number),
      0,
    );
  }
  let selected = first;
  for (const value of present.slice(1)) {
    const comparison = scalarOrder(value, selected);
    if (
      (projection.operator === "min" && comparison < 0) ||
      (projection.operator === "max" && comparison > 0)
    )
      selected = value;
  }
  return selected;
}

function referenceSource(
  overlay: OverlayEngine,
  schemas: readonly QueryReferenceSchema[],
  source: QuerySource | QueryJoin,
): ReferenceSource {
  const schema = schemas.find(
    (candidate) => candidate.name === source.collection,
  );
  if (!schema)
    throw new Error(`oracle: unknown collection ${source.collection}`);
  return {
    alias: source.as,
    columns: schema.columns,
    rows: overlay.rows(source.collection),
  };
}

interface ReferencePlanResult {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly ReferenceValue[])[];
}

function referencePlan(
  overlay: OverlayEngine,
  schemas: readonly QueryReferenceSchema[],
  specification: QuerySpecification,
): ReferencePlanResult {
  const sourceSpecs = [specification.from, ...(specification.joins ?? [])];
  const sources = sourceSpecs.map((source) =>
    referenceSource(overlay, schemas, source),
  );
  let bindings: Binding[] = sources[0]?.rows.map((row) => [row]) ?? [];
  for (const [joinIndex, join] of (specification.joins ?? []).entries()) {
    const rightSource = joinIndex + 1;
    const expanded: Binding[] = [];
    for (const binding of bindings) {
      let matches = 0;
      const left = fieldValue(join.on.left, binding, sources);
      if (left !== null)
        for (const rightRow of sources[rightSource]?.rows ?? []) {
          const candidate = [...binding, rightRow];
          const right = fieldValue(join.on.right, candidate, sources);
          if (right !== null && scalarEqual(left, right)) {
            expanded.push(candidate);
            matches++;
          }
        }
      if (matches === 0 && join.kind === "left")
        expanded.push([...binding, null]);
    }
    bindings = expanded;
  }
  if (specification.where)
    bindings = bindings.filter(
      (binding) =>
        predicateTruth(
          specification.where as QueryPredicate,
          binding,
          sources,
        ) === true,
    );

  const grouped =
    (specification.groupBy?.length ?? 0) > 0 ||
    specification.select.some((projection) => projection.kind === "aggregate");
  let projected: ReferenceProjectedRow[];
  if (!grouped) {
    projected = bindings.map((binding, ordinal) => ({
      values: specification.select.map((projection) => {
        if (projection.kind !== "field")
          throw new Error("oracle: invalid projection");
        return fieldValue(projection.field, binding, sources);
      }),
      ordinal,
    }));
  } else {
    const groups: Array<{
      readonly key: readonly ReferenceValue[];
      readonly bindings: Binding[];
      readonly ordinal: number;
    }> = [];
    if ((specification.groupBy?.length ?? 0) === 0)
      groups.push({ key: [], bindings: [], ordinal: 0 });
    for (const binding of bindings) {
      const key = (specification.groupBy ?? []).map((field) =>
        fieldValue(field, binding, sources),
      );
      let group = groups.find((candidate) => sameGroupKey(candidate.key, key));
      if (!group) {
        group = { key, bindings: [], ordinal: groups.length };
        groups.push(group);
      }
      group.bindings.push(binding);
    }
    projected = groups.map((group) => ({
      values: specification.select.map((projection) => {
        if (projection.kind === "aggregate")
          return aggregate(projection, group.bindings, sources);
        const keyIndex = (specification.groupBy ?? []).findIndex(
          (field) =>
            field.source === projection.field.source &&
            field.column === projection.field.column,
        );
        return group.key[keyIndex] ?? null;
      }),
      ordinal: group.ordinal,
    }));
  }

  const orders = specification.orderBy ?? [];
  projected.sort((left, right) => {
    for (const order of orders) {
      const column = specification.select.findIndex(
        (projection) => projection.as === order.by,
      );
      const a = left.values[column] ?? null;
      const b = right.values[column] ?? null;
      if (a === null || b === null) {
        if (a === b) continue;
        const nullFirst = (order.nulls ?? "last") === "first";
        return a === null ? (nullFirst ? -1 : 1) : nullFirst ? 1 : -1;
      }
      const comparison = scalarOrder(a, b);
      if (comparison !== 0)
        return order.direction === "desc" ? -comparison : comparison;
    }
    return left.ordinal - right.ordinal;
  });
  const limited =
    specification.limit === undefined
      ? projected
      : projected.slice(0, specification.limit);
  return {
    columns: specification.select.map((projection) => projection.as),
    rows: limited.map((row) => row.values),
  };
}

/** Executes one already-valid data query as a row-oriented reference. */
export function referenceQuery(
  overlay: OverlayEngine,
  schemas: readonly QueryReferenceSchema[],
  specification: QuerySpecification,
): QueryResult {
  const result = referencePlan(overlay, schemas, specification);
  return { columns: result.columns, rows: result.rows };
}
