import type { CollectionShape, SyncClient } from "@calque/client";
import type { JSX } from "react";

import {
  SyncProvider,
  useLiveQuery,
  usePendingWrite,
  useSyncClient,
} from "../../src/index.js";

export function IssueTitle(): JSX.Element {
  const { db } = useSyncClient();
  const issues = useLiveQuery(db.issues.query().select("id", "title"));
  const first = issues.status === "ready" ? issues.result[0] : undefined;
  const write = usePendingWrite("issues", { id: first?.id ?? "" });
  // @ts-expect-error the registered collections have no audit_log.
  void db.audit_log;
  // @ts-expect-error an issue's key is its id.
  usePendingWrite("issues", { title: "" });
  // @ts-expect-error a pending write names a registered collection.
  usePendingWrite("audit_log", { id: "" });
  return (
    <span>
      {first?.title}:{write?.state}
    </span>
  );
}

export function App({ client }: { readonly client: SyncClient }): JSX.Element {
  return (
    <SyncProvider client={client}>
      <IssueTitle />
    </SyncProvider>
  );
}

type Tasks = {
  readonly tasks: CollectionShape<
    { readonly id: bigint },
    { readonly id: bigint }
  >;
};

export function OtherApp({
  client,
}: {
  readonly client: SyncClient<Tasks>;
}): JSX.Element {
  // @ts-expect-error the provider takes a client of the registered collections.
  return <SyncProvider client={client}>{null}</SyncProvider>;
}
