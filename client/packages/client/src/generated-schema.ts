export const GENERATED_CLIENT_COLUMN_KINDS = [
  "bool",
  "int32",
  "int64",
  "f64",
  "decimal",
  "text",
  "bytes",
  "uuid",
  "json",
  "date",
  "time",
  "timestamp",
] as const;

/**
 * The logical kind a generated facade declared for one column, so the client
 * can refuse an artifact whose physical column no longer matches the type the
 * application compiled against. `text` admits both physical spellings, `utf8`
 * and `dict`. `int32`, an `int2` or `int4`, is stored as an `int64` and read
 * as a `number`.
 */
export type GeneratedClientColumnKind =
  (typeof GENERATED_CLIENT_COLUMN_KINDS)[number];

export interface GeneratedClientCollectionColumnDescriptor {
  readonly name: string;
  readonly kind: GeneratedClientColumnKind;
}

export type GeneratedClientCrudOperation = "insert" | "update" | "delete";

/** The writes a collection's facade offers, and the columns they may set. */
export interface GeneratedClientCollectionWritesDescriptor {
  readonly operations: readonly GeneratedClientCrudOperation[];
  readonly writableColumns: readonly string[];
}

/**
 * One generated collection. `tableId` names the source relation as
 * `schema.table`, and the server's plan reports the wire table id it resolved
 * to, so a generated file is not bound to one database. `id` is both the
 * facade name and the artifact table the collection materializes into.
 */
export interface GeneratedClientCollectionDescriptor {
  readonly id: string;
  readonly tableId: string;
  readonly columns: readonly GeneratedClientCollectionColumnDescriptor[];
  readonly primaryKey: readonly string[];
  readonly writes?: GeneratedClientCollectionWritesDescriptor;
}

export interface GeneratedClientSchemaDescriptor {
  readonly collections: readonly GeneratedClientCollectionDescriptor[];
}
