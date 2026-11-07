/**
 * The column kind a generated collection declares, which the client holds
 * the served artifact's physical column to, and the TypeScript type it hands
 * the application. `text` covers both physical spellings, `utf8` and `dict`.
 */
export interface GeneratedType {
  readonly kind:
    | "bool"
    | "int32"
    | "int64"
    | "f64"
    | "decimal"
    | "text"
    | "bytes"
    | "uuid"
    | "json"
    | "date"
    | "time"
    | "timestamp";
  readonly typescript: string;
}

/**
 * The PostgreSQL types `calque serve` enrolls, by their `pg_catalog` name,
 * as `calque init` writes them. An `int2` or `int4` is a `number` and an
 * `int8` a `bigint`; a uuid is its lowercase text, a `numeric` its decimal
 * text, a date, time or timestamp its ISO 8601 text, and JSON the parsed
 * value.
 */
export const POSTGRES_TYPES: ReadonlyMap<string, GeneratedType> = new Map<
  string,
  GeneratedType
>([
  ["bool", { kind: "bool", typescript: "boolean" }],
  ["int2", { kind: "int32", typescript: "number" }],
  ["int4", { kind: "int32", typescript: "number" }],
  ["int8", { kind: "int64", typescript: "bigint" }],
  ["float4", { kind: "f64", typescript: "number" }],
  ["float8", { kind: "f64", typescript: "number" }],
  ["numeric", { kind: "decimal", typescript: "string" }],
  ["text", { kind: "text", typescript: "string" }],
  ["varchar", { kind: "text", typescript: "string" }],
  ["bpchar", { kind: "text", typescript: "string" }],
  ["uuid", { kind: "uuid", typescript: "string" }],
  ["bytea", { kind: "bytes", typescript: "Uint8Array" }],
  ["json", { kind: "json", typescript: "unknown" }],
  ["jsonb", { kind: "json", typescript: "unknown" }],
  ["date", { kind: "date", typescript: "string" }],
  ["time", { kind: "time", typescript: "string" }],
  ["timestamp", { kind: "timestamp", typescript: "string" }],
  ["timestamptz", { kind: "timestamp", typescript: "string" }],
]);
