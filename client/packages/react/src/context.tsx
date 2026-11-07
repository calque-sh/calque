import type {
  CollectionShapes,
  RegisteredCollections,
  SyncClient,
} from "@calque/client";
import { createContext, useContext, type PropsWithChildren } from "react";

import { pendingWritesOf } from "./pending-writes.js";

/** `SyncProvider`'s props: the client its descendants use. */
export interface SyncProviderProps extends PropsWithChildren {
  /** A client for the registered collections, or any client without them. */
  readonly client: [RegisteredCollections] extends [never]
    ? Omit<SyncClient, "db">
    : SyncClient;
}

const SyncContext = createContext<Omit<SyncClient, "db"> | undefined>(
  undefined,
);

/** Makes `client` the one the Calque hooks below it use. */
export function SyncProvider({ client, children }: SyncProviderProps) {
  pendingWritesOf(client);
  return <SyncContext value={client}>{children}</SyncContext>;
}

/**
 * The client from the nearest `SyncProvider`, typed with the collections the
 * generated bindings registered. Throws a TypeError outside a provider.
 */
export function useSyncClient<
  Collections extends CollectionShapes = RegisteredCollections,
>(): SyncClient<Collections> {
  const client = useContext(SyncContext);
  if (!client) throw new TypeError("Calque hooks require a SyncProvider");
  return client as SyncClient<Collections>;
}
