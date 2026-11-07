import type { SyncClient, SyncPublication, SyncStatus } from "../client.js";
import type { CollectionShapes } from "../facade/types.js";
import type { MutationStatus } from "../mutation.js";
import type { ColumnarSession } from "./session.js";

/** The frozen public client over one direct-columnar session. */
export function publicColumnarClient<Collections extends CollectionShapes>(
  session: ColumnarSession,
): SyncClient<Collections> {
  const client = {
    get status(): SyncStatus {
      return session.status;
    },
    subscribe: (listener: () => void) => session.subscribe(listener),
    subscribePublications: (listener: (publication: SyncPublication) => void) =>
      session.subscribePublications(listener),
    mutationStatus: (mutationId: string) => session.mutationStatus(mutationId),
    subscribeMutation: (mutationId: string, listener: () => void) =>
      session.subscribeMutation(mutationId, listener),
    subscribeMutations: (listener: (status: MutationStatus) => void) =>
      session.subscribeMutations(listener),
    retryMutation: (mutationId: string) => session.retryMutation(mutationId),
    refresh: (signal?: AbortSignal) => session.refresh(signal),
    reset: () => session.reset(),
    close: () => session.close(),
    db: session.db,
  };
  return Object.freeze(client) as unknown as SyncClient<Collections>;
}
