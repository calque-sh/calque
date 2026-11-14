# @calque/config

`calque-bindings` renders a Calque configuration, the `calque.config.json`
that `calque init` writes and `calque serve` runs, into the typed bindings a
page passes to `createSyncClient` from `@calque/client`. The Vite plugin from
`@calque/client/vite` writes the same bytes with `renderBindingsFiles`, the
function of this package that `calque-bindings` runs, when `vite` starts,
whenever the configuration changes, and before `vite build`, so a Vite page
does not run `calque-bindings` by hand.

```sh
npm install -D @calque/config
```

## calque-bindings

```sh
calque-bindings [--config <file>] [--out <file>] [--check]
```

| Flag              | Default               | What it does                                                          |
| ----------------- | --------------------- | --------------------------------------------------------------------- |
| `--config <file>` | `calque.config.json`  | The configuration to read.                                            |
| `--out <file>`    | `calque.generated.ts` | The bindings to write, a `.ts` file. The start module goes beside it. |
| `--check`         |                       | Writes nothing, and fails if either file differs from its render.     |
| `-h`, `--help`    |                       | Prints the usage.                                                     |

It exits 0 on success, 1 when the configuration cannot be read or compiled, a
file cannot be written, or `--check` finds a file out of date, and 2 on a
usage error. Outside Vite, run it again after every change to
`calque.config.json`. Run `calque-bindings --check` in CI. `calque serve`
refuses to start when the configuration no longer matches the database's
tables, and the client refuses to open when the bindings do not match what the
server serves.

## The generated files

`calque.generated.ts` declares, for each collection, where `Issues` is the
collection id `issues` in PascalCase:

- `IssuesRow`, one row: a readonly property per column, `| null` when the
  column is nullable.
- `IssuesKey`, the primary-key columns.
- `IssuesInsert`, when a command allows inserts: the whole row. Every column
  is required, a nullable one takes `null`, and no PostgreSQL default applies.
- `IssuesPatch`, when a command allows updates: any of the writable columns.

It also exports `CalqueCollections`, registers it with `@calque/client` so
`SyncClient` and the `@calque/react` hooks are typed without a type argument,
and exports `schema`, the value `createSyncClient({ schema })` takes.

`calque.start.generated.ts`, named `<name>.start.generated.ts` after `--out`,
exports `requiredTables` and imports nothing, so a page can start its
bootstrap request before the client library loads.

`--check` compares both files byte for byte with their render, so exclude
them from formatters.

## Tenants

A scope of kind `partitioned` gives each grant one tenant's rows. The scope
names the grant attribute that holds the tenant, and each collection under it
either names its tenant column, which must lead its primary key, or is shared
whole by every tenant:

```json
{
  "scopes": [
    {
      "id": "tenant",
      "source": "app",
      "kind": "partitioned",
      "attribute": "tenant_id",
      "dataVersion": 1
    }
  ],
  "collections": [
    {
      "id": "tasks",
      "primaryKey": ["tenant_id", "id"],
      "scope": "tenant",
      "partitionColumn": "tenant_id"
    },
    {
      "id": "priorities",
      "primaryKey": ["id"],
      "scope": "tenant",
      "shared": true
    }
  ]
}
```

The collections above are cut to the fields that matter here.
`calque init --tenant-column tenant_id` writes the whole file from an
inspected database: a table whose key leads with `tenant_id` is partitioned,
and a table without that column is shared. Every tenant column has one type,
`int2`, `int4`, `int8` or `uuid`, and the grant's attribute must have the same
type. The bindings do not change: `TasksKey`
still includes `tenant_id`, and a page sees only its own tenant's rows, so
it writes its own tenant's value there. The backend mints each grant with the
user's tenant, for instance `calque-grant mint --attribute tenant_id=int8:42` from
`@calque/grant`.

## Column types

| PostgreSQL                                 | TypeScript                  |
| ------------------------------------------ | --------------------------- |
| `bool`                                     | `boolean`                   |
| `int2`, `int4`                             | `number`                    |
| `int8`                                     | `bigint`                    |
| `float4`, `float8`                         | `number`                    |
| `numeric`                                  | `string`, the decimal text  |
| `text`, `varchar`, `bpchar`                | `string`                    |
| `uuid`                                     | `string`, lowercase         |
| `bytea`                                    | `Uint8Array`                |
| `json`, `jsonb`                            | `unknown`, the parsed value |
| `date`, `time`, `timestamp`, `timestamptz` | `string`, ISO 8601          |

`sum` over an `int2`, `int4` or `int8` column is a `bigint`, as PostgreSQL's
`sum` over `int2` and `int4` is, and over a `float4` or `float8` column a
`number`.

`calque-bindings` refuses a column of any other type, as `calque serve` does.
