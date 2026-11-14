/** How a collection's rows are encoded; `calque-columnar` is the only value. */
export type CollectionEncoding = "calque-columnar";
/** The writes a crud command can allow on its collection. */
export type CrudOperation = "insert" | "update" | "delete";

export interface ApplicationDefinition {
  /** The application's id, in the form every id in the configuration takes. */
  readonly id: string;
  /** The application's version; any non-empty string. */
  readonly version: string;
}

export interface ProtocolDefinition {
  /** The configuration format version; must be 1. */
  readonly manifest: 1;
  /** The artifact protocol version; `calque serve` accepts only 1. */
  readonly artifact: number;
  /** The change-stream protocol version; `calque serve` accepts only 1. */
  readonly changeStream: number;
  /** The mutation protocol version; `calque serve` accepts only 1. */
  readonly mutation: number;
}

export interface SourceDefinition {
  /** The source's id, which scopes and collections name. */
  readonly id: string;
  /**
   * The environment variable that holds the PostgreSQL connection string;
   * a connection string here is refused.
   */
  readonly databaseUrlEnv: string;
  /**
   * The PostgreSQL publication the collections' tables belong to; a
   * PostgreSQL identifier of at most 63 bytes.
   */
  readonly publication: string;
  /**
   * The replication slot's name; a PostgreSQL identifier of at most 63
   * bytes.
   */
  readonly slot: string;
}

interface ScopeDefinitionBase {
  /** The scope's id, which collections name. */
  readonly id: string;
  /** The source the scope covers. */
  readonly source: string;
  /**
   * The authorization generation, an integer from 1. Raising it and
   * restarting refuses every grant minted for an older generation and
   * publishes a new artifact; lowering it changes nothing.
   */
  readonly dataVersion: number;
}

/** Every principal reads every row of the scope's collections. */
export interface SourceWideScopeDefinition extends ScopeDefinitionBase {
  readonly kind: "source-wide";
}

/**
 * Each principal reads one tenant's rows of the scope's partitioned
 * collections, and every row of its shared ones. The tenant is the value of
 * the grant attribute `attribute`; a grant without it is refused.
 */
export interface PartitionedScopeDefinition extends ScopeDefinitionBase {
  readonly kind: "partitioned";
  /**
   * The grant attribute that carries the tenant. Its type must be the
   * partition columns' type: `int2`, `int4`, `int8` or `uuid`.
   */
  readonly attribute: string;
}

export type ScopeDefinition =
  SourceWideScopeDefinition | PartitionedScopeDefinition;

export interface ColumnDefinition {
  readonly name: string;
  /**
   * The column's type by its `pg_catalog` name, as `calque init` writes it:
   * `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`, `text`,
   * `varchar`, `bpchar`, `uuid`, `bytea`, `json`, `jsonb`, `date`, `time`,
   * `timestamp` or `timestamptz`. `calque serve` refuses to start when the
   * table's column has another type.
   */
  readonly postgresType: string;
  /**
   * Whether the column admits NULL. `calque serve` refuses to start when the
   * table's column differs.
   */
  readonly nullable: boolean;
}

export interface CollectionDefinition {
  /** The collection's id, and the name of its facade under `client.db`. */
  readonly id: string;
  /** The source the table lives in; it must be the source of `scope` too. */
  readonly source: string;
  readonly kind: "table";
  /** The PostgreSQL schema and table the collection reads. */
  readonly relation: { readonly schema: string; readonly table: string };
  /**
   * The columns the collection selects. Key and writable columns must be
   * among them.
   */
  readonly columns: readonly ColumnDefinition[];
  /**
   * The table's primary key columns, in key order. `calque serve` refuses to
   * start when the table's primary key differs.
   */
  readonly primaryKey: readonly string[];
  /** The scope the collection's rows are served under. */
  readonly scope: string;
  /**
   * Under a partitioned scope, the column that names a row's tenant. It must
   * be the first column of `primaryKey`, and every partitioned collection's
   * must have one type. A collection of a partitioned scope sets this or
   * `shared`, never both; one of a source-wide scope sets neither.
   */
  readonly partitionColumn?: string;
  /**
   * Under a partitioned scope, `true` serves every row of the collection to
   * every tenant.
   */
  readonly shared?: true;
  readonly adapter: "direct-columnar";
  readonly encoding: CollectionEncoding;
}

export interface CrudCommandDefinition {
  readonly id: string;
  readonly kind: "crud";
  readonly collection: string;
  /**
   * The facade methods the command enables. Listing `insert` requires every
   * non-key column in `writableColumns`.
   */
  readonly operations: readonly CrudOperation[];
  /**
   * The columns `update` may change and `insert` writes besides the key;
   * key columns are never writable.
   */
  readonly writableColumns: readonly string[];
}

export type CommandDefinition = CrudCommandDefinition;

/** A `calque.config.json`, as `compileConfig` accepts it. */
export interface CalqueConfigDefinition {
  /** The application's id and version. */
  readonly application: ApplicationDefinition;
  /** The protocol versions the configuration targets. */
  readonly protocol: ProtocolDefinition;
  /** The PostgreSQL sources; `calque serve` requires exactly one. */
  readonly sources: readonly SourceDefinition[];
  /** The scopes; `calque serve` requires exactly one. */
  readonly scopes: readonly ScopeDefinition[];
  /** The tables the client loads; at least one. */
  readonly collections: readonly CollectionDefinition[];
  /** The write commands; at least one. */
  readonly commands: readonly CommandDefinition[];
}

/**
 * The compiled configuration: the definition's declarations, each sorted by
 * id, under a format version.
 */
export interface CalqueManifest {
  /** The manifest format, `calque.config.v1`. */
  readonly schemaVersion: "calque.config.v1";
  readonly application: ApplicationDefinition;
  readonly protocol: ProtocolDefinition;
  readonly sources: readonly SourceDefinition[];
  readonly scopes: readonly ScopeDefinition[];
  /** Each collection's columns sorted by name. */
  readonly collections: readonly CollectionDefinition[];
  /** Each command's lists sorted. */
  readonly commands: readonly CommandDefinition[];
}
