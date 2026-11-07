import type { MutationStatus, SyncClient } from "@calque/client";

type Client = Pick<SyncClient, "subscribeMutations">;

/** One client's latest write to each row, until that write is canonical. */
class PendingWrites {
  readonly #latest = new Map<string, MutationStatus>();
  readonly #listeners = new Map<string, Set<() => void>>();

  constructor(client: Client) {
    client.subscribeMutations((status) => {
      this.#record(status);
    });
  }

  get(row: string): MutationStatus | undefined {
    return this.#latest.get(row);
  }

  subscribe(row: string, listener: () => void): () => void {
    const listeners = this.#listeners.get(row) ?? new Set<() => void>();
    this.#listeners.set(row, listeners);
    listeners.add(listener);
    return () => {
      listeners.delete(listener);
      if (listeners.size === 0 && this.#listeners.get(row) === listeners)
        this.#listeners.delete(row);
    };
  }

  #record(status: MutationStatus): void {
    const row = rowId(status.collection, status.key);
    if (status.state === "submitting") this.#latest.set(row, status);
    else if (this.#latest.get(row)?.mutationId !== status.mutationId) return;
    else if (status.state === "canonical") this.#latest.delete(row);
    else this.#latest.set(row, status);
    for (const listener of [...(this.#listeners.get(row) ?? [])]) listener();
  }
}

const pendingWrites = new WeakMap<Client, PendingWrites>();

/** The pending writes of `client`, recorded from the first call on. */
export function pendingWritesOf(client: Client): PendingWrites {
  let writes = pendingWrites.get(client);
  if (!writes) {
    writes = new PendingWrites(client);
    pendingWrites.set(client, writes);
  }
  return writes;
}

/** One text per row, whatever order the key's columns were written in. */
export function rowId(
  collection: string,
  key: Readonly<Record<string, unknown>>,
): string {
  return JSON.stringify([
    collection,
    ...Object.keys(key)
      .sort()
      .map((column) => [column, cell(key[column])]),
  ]);
}

function cell(value: unknown): unknown {
  if (typeof value === "bigint") return ["bigint", value.toString()];
  if (value instanceof Uint8Array) return ["bytes", [...value]];
  return value;
}
