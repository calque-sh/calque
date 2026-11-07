import { isBoundedIdentifier } from "./limits.js";

/** Throws unless `value` is a well-formed mutation ID. */
export function assertMutationId(value: unknown): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    !isBoundedIdentifier(value)
  ) {
    throw new TypeError("mutation ID must be a valid bounded identifier");
  }
}
