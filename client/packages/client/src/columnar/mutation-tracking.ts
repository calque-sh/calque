import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import type { MutationStatus } from "../mutation.js";

export const CRUD_COMMAND_VERSION = 1;
export const CRUD_COMMAND_PREFIX = "crud:";
/** Terminal statuses beyond this many are forgotten oldest first. */
export const MAX_RETAINED_MUTATION_STATUSES = 512;
/** How many lost-response envelopes one session keeps for `retryMutation`. */
export const MAX_RESUBMITTABLE_MUTATIONS = 64;
/** How many times the client resends an undecided write before its call rejects. */
export const AUTOMATIC_MUTATION_RESENDS = 4;
/** The wait before the first resend after a lost response; each later one doubles it. */
export const MUTATION_RESEND_DELAY_MS = 250;
/** The longest wait the client takes from a server's retry-later answer. */
export const MAX_MUTATION_RETRY_AFTER_MS = 30_000;

/** The row a mutation writes, as every status of it names the row. */
export type MutationRow = Pick<MutationStatus, "collection" | "key">;

/** `key` with only the key columns, and uuids as lowercase text like rows. */
export function rowKey(
  descriptor: GeneratedClientCollectionDescriptor,
  key: Readonly<Record<string, unknown>>,
): MutationRow["key"] {
  const row: Record<string, unknown> = {};
  for (const column of descriptor.primaryKey) {
    const value = key[column];
    row[column] =
      typeof value === "string" &&
      descriptor.columns.some(
        ({ name, kind }) => name === column && kind === "uuid",
      )
        ? value.toLowerCase()
        : value;
  }
  return Object.freeze(row);
}

export function terminal(status: MutationStatus): boolean {
  return (
    status.state !== "submitting" && status.state !== "awaiting_canonical_echo"
  );
}
