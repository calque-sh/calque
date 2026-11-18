---
kind: added
---

`SyncError` has two new codes. `SCHEMA_SUSPENDED` (recovery `retry`) means the
source's live schema no longer matches the server's configuration: the client
waits it out like `ARTIFACT_PREPARING`, on the server's `Retry-After`, and
reports it only when the wait would pass 120 s. `SCHEMA_MISMATCH` (recovery
`none`) replaces `INVALID_RESPONSE` when the bootstrap plan does not bind the
page's generated bindings, as when a served table or column the bindings name
was renamed, dropped or retyped. Its message names the table and column; load a
build generated from the current configuration. A served column the bindings do
not name is still ignored. A `switch` over `SyncError["code"]` that ends in
`satisfies never` needs a case for each.
