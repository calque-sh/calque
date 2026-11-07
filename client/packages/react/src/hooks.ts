import type {
  LiveQueryResource,
  LiveQuerySnapshot,
  MutationStatus,
  QueryLiveOptions,
  QueryLiveWindowOptions,
  QueryWindow,
  RegisteredCollections,
  SyncStatus,
} from "@calque/client";
import { useCallback, useMemo, useRef, useSyncExternalStore } from "react";

import { useSyncClient } from "./context.js";
import { pendingWritesOf, rowId } from "./pending-writes.js";

/**
 * What the facade hooks accept: a query built from a generated collection
 * facade, `db.events.query().where(...)`. The hook subscribes it; the rows
 * it publishes carry the builder's projected type.
 */
export interface LiveQuerySource<Row> {
  readonly subscribe: (
    options?: QueryLiveOptions,
  ) => LiveQueryResource<readonly Row[]>;
}

/**
 * Subscribes the component to a live query and returns its snapshot:
 * `loading`, `ready` with the rows, or `error`. It re-renders when the
 * snapshot changes, and renders `loading` on the server.
 */
export function useLiveQuery<Row>(
  query: LiveQuerySource<Row>,
  options?: QueryLiveOptions,
): LiveQuerySnapshot<readonly Row[]> {
  const resource = useLiveResource(query, options);
  return useSyncExternalStore(
    resource.subscribe,
    resource.getSnapshot,
    resource.getServerSnapshot,
  );
}

/**
 * `useLiveQuery` for Suspense: it suspends while the query loads and throws
 * its error to the nearest error boundary, so it returns only `ready`.
 */
export function useSuspenseLiveQuery<Row>(
  query: LiveQuerySource<Row>,
  options?: QueryLiveOptions,
): Extract<LiveQuerySnapshot<readonly Row[]>, { readonly status: "ready" }> {
  const resource = useLiveResource(query, options);
  const snapshot = useSyncExternalStore(
    resource.subscribe,
    resource.getSnapshot,
    resource.getServerSnapshot,
  );
  if (snapshot.status === "loading") throw resource.promise;
  if (snapshot.status === "error") throw asError(snapshot.error);
  return snapshot;
}

/** What `useLiveWindow` accepts: a query built from a generated facade. */
export interface LiveWindowSource<Row extends object> {
  readonly subscribeWindow: (
    offset: number,
    count: number,
    options?: QueryLiveWindowOptions,
  ) => LiveQueryResource<QueryWindow<Row>>;
}

/** `useLiveWindow`'s snapshot: `stale` while it shows the previous window. */
export type LiveWindowSnapshot<Row extends object> = LiveQuerySnapshot<
  QueryWindow<Row>
> & { readonly stale: boolean };

/**
 * Subscribes the component to rows `offset` through `offset + count - 1` of
 * a query's result, with the result's total row count. While a new range or
 * query loads it keeps returning the last window it read, marked `stale`, so
 * a scrolling or re-filtered list never empties.
 */
export function useLiveWindow<Row extends object>(
  query: LiveWindowSource<Row>,
  offset: number,
  count: number,
  options?: QueryLiveWindowOptions,
): LiveWindowSnapshot<Row> {
  if (
    typeof query !== "object" ||
    query === null ||
    typeof query.subscribeWindow !== "function"
  )
    throw new TypeError("useLiveWindow needs a generated collection query");
  const resource = query.subscribeWindow(offset, count, options);
  const snapshot = useSyncExternalStore(
    resource.subscribe,
    resource.getSnapshot,
    resource.getServerSnapshot,
  );
  const settled = useRef(snapshot);
  if (snapshot.status !== "loading") settled.current = snapshot;
  const shown = settled.current;
  const stale = shown !== snapshot;
  return useMemo(() => ({ ...shown, stale }), [shown, stale]);
}

function useLiveResource<Row>(
  query: LiveQuerySource<Row>,
  options: QueryLiveOptions | undefined,
): LiveQueryResource<readonly Row[]> {
  if (
    typeof query !== "object" ||
    query === null ||
    typeof query.subscribe !== "function"
  )
    throw new TypeError("useLiveQuery needs a generated collection query");
  return query.subscribe(options);
}

/** The client's `status`, re-rendering on every status change. */
export function useSyncStatus(): SyncStatus;
/** `selector(status)`, re-rendering on every status change. */
export function useSyncStatus<T>(selector: (status: SyncStatus) => T): T;
export function useSyncStatus<T>(
  selector?: (status: SyncStatus) => T,
): SyncStatus | T {
  const client = useSyncClient();
  const subscribe = useCallback(
    (listener: () => void) => client.subscribe(listener),
    [client],
  );
  const getSnapshot = useCallback(() => client.status, [client]);
  const status = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  return selector ? selector(status) : status;
}

/**
 * The status of one mutation, re-rendering when it changes; `undefined` for
 * an `undefined` id or one the client has not issued or has dropped.
 */
export function useMutationStatus(
  mutationId: string | undefined,
): MutationStatus | undefined {
  const client = useSyncClient();
  const subscribe = useCallback(
    (listener: () => void) =>
      mutationId === undefined
        ? () => {}
        : client.subscribeMutation(mutationId, listener),
    [client, mutationId],
  );
  const getSnapshot = useCallback(
    () =>
      mutationId === undefined ? undefined : client.mutationStatus(mutationId),
    [client, mutationId],
  );
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

/** A registered collection's name, or any name without a registration. */
export type PendingWriteCollection = [RegisteredCollections] extends [never]
  ? string
  : keyof RegisteredCollections & string;

/** The key of a row of `Collection`, as its generated facade takes it. */
export type PendingWriteKey<Collection extends string> = [
  RegisteredCollections,
] extends [never]
  ? Readonly<Record<string, unknown>>
  : Collection extends keyof RegisteredCollections
    ? RegisteredCollections[Collection]["key"]
    : never;

/**
 * The status of the latest write to one row, re-rendering as it changes:
 * `submitting` while the request is in flight, then `undefined` once the
 * write is canonical. Until the next write to that row, one the server
 * refused stays `rejected` or `conflict`, and one it did not settle stays
 * `retryable` or `ambiguous` unless its echo arrives. A uuid in `key` is
 * lowercase text, as rows read it.
 */
export function usePendingWrite<Collection extends PendingWriteCollection>(
  collection: Collection,
  key: PendingWriteKey<Collection>,
): MutationStatus | undefined {
  if (typeof collection !== "string" || typeof key !== "object" || !key)
    throw new TypeError("usePendingWrite needs a collection and a row key");
  const writes = pendingWritesOf(useSyncClient());
  const row = rowId(collection, key);
  const subscribe = useCallback(
    (listener: () => void) => writes.subscribe(row, listener),
    [writes, row],
  );
  const getSnapshot = useCallback(() => writes.get(row), [writes, row]);
  return useSyncExternalStore(subscribe, getSnapshot, noPendingWrite);
}

function noPendingWrite(): undefined {
  return undefined;
}

function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
