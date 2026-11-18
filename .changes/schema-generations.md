---
kind: added
---

A schema change no longer needs `calque destroy`. Saving other columns or
tables in `calque.config.json` under `calque dev`, starting `calque serve` with
them, altering the publication, renaming a served enum value, or renaming,
retyping or dropping a published column moves the source to its next schema
generation: Calque prepares the working set again from a snapshot, and open
pages rebuild on their own. When the live schema no longer has a table or
column the configuration names, the source is suspended instead, answering
`/readyz` and `/v1/bootstrap` with `503` and `SCHEMA_SUSPENDED`, until the
schema or the configuration matches again; `calque serve` still refuses to
start on such a schema. `docs/postgresql.md` describes each case.
