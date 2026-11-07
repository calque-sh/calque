# @calque/react

React hooks for `@calque/client`. `SyncProvider` puts a client in context,
and the hooks below it subscribe components to live queries, the client's
status, and the writes it has in flight.

```sh
npm install @calque/client @calque/react
```

The bindings `calque-bindings` generates register their collections with
`@calque/client`, so `useSyncClient()` is typed without a type argument:

```tsx
import { createSyncClient } from "@calque/client";
import { SyncProvider, useLiveQuery, useSyncClient } from "@calque/react";
import { schema } from "./calque.generated.js";

const client = createSyncClient({ endpoint, auth, schema });

function Titles() {
  const { db } = useSyncClient();
  const tasks = useLiveQuery(db.tasks.query().select("id", "title"));
  if (tasks.status !== "ready") return <p>{tasks.status}</p>;
  return <p>{tasks.result.map((task) => task.title).join(", ")}</p>;
}

export const App = () => (
  <SyncProvider client={client}>
    <Titles />
  </SyncProvider>
);
```

`useSuspenseLiveQuery` is `useLiveQuery` under Suspense, and `useLiveWindow`
reads one range of a large result. `usePendingWrite("tasks", { id })` follows
the latest write to one row: `submitting` until the server answers, then
`awaiting_canonical_echo`, then `undefined` once PostgreSQL's change reaches
the client. A write the server refused stays `rejected` or `conflict` until
the next write to that row.
