import {
  eq,
  type CollectionShape,
  type LiveQueryResource,
  type SyncClient,
} from "@calque/client";
import type { JSX } from "react";

import {
  SyncProvider,
  useLiveQuery,
  useSuspenseLiveQuery,
  useSyncStatus,
} from "../src/index.js";

export function ConsumerProvider({
  client,
  children,
}: {
  readonly client: SyncClient;
  readonly children: JSX.Element;
}): JSX.Element {
  return <SyncProvider client={client}>{children}</SyncProvider>;
}

interface TaskRow {
  readonly id: bigint;
  readonly status: string;
}
type TaskCollections = {
  readonly tasks: CollectionShape<TaskRow, { readonly id: bigint }>;
};

export function TaskCount({
  client,
}: {
  readonly client: SyncClient<TaskCollections>;
}): JSX.Element {
  const result = useLiveQuery(client.db.tasks.query().select("id"));
  const state = useSyncStatus((status) => status.state);
  return (
    <span>
      {state}:{result.status}
    </span>
  );
}

export function publicResource(
  client: SyncClient<TaskCollections>,
): LiveQueryResource<readonly Pick<TaskRow, "id">[]> {
  return client.db.tasks.query().select("id").subscribe();
}

export function OpenTasks({
  client,
}: {
  readonly client: SyncClient<TaskCollections>;
}): JSX.Element {
  const open = useLiveQuery(
    client.db.tasks.query().where(eq("status", "open")).select("id"),
    { maxRows: 100 },
  );
  const first: bigint | undefined =
    open.status === "ready" ? open.result[0]?.id : undefined;
  // @ts-expect-error the projection dropped status; only selected columns exist.
  const status = open.status === "ready" ? open.result[0]?.status : undefined;
  void status;
  return <span>{String(first)}</span>;
}

export function OpenTaskCount({
  client,
}: {
  readonly client: SyncClient<TaskCollections>;
}): JSX.Element {
  const counted = useSuspenseLiveQuery(
    client.db.tasks.query().groupBy("status").count(),
  );
  const count: number | bigint | undefined = counted.result[0]?.count;
  return <span>{String(count)}</span>;
}
