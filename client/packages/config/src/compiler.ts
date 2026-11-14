import type {
  CollectionDefinition,
  CommandDefinition,
  CrudCommandDefinition,
  CrudOperation,
  CalqueConfigDefinition,
  CalqueManifest,
  ScopeDefinition,
  SourceDefinition,
} from "./domain.js";
import { POSTGRES_TYPES } from "./postgres-types.js";

const ID = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/;
const ENVIRONMENT_VARIABLE = /^[A-Z_][A-Z0-9_]*$/;
const CRUD_OPERATIONS = new Set<CrudOperation>(["insert", "update", "delete"]);
const ENCODINGS = new Set(["calque-columnar"]);

export class ConfigError extends Error {
  readonly path: string;

  constructor(path: string, message: string) {
    super(`${path}: ${message}`);
    this.name = "ConfigError";
    this.path = path;
  }
}

function object(value: unknown, path: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new ConfigError(path, "must be an object");
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null)
    throw new ConfigError(path, "must be a plain data object");
  return value as Record<string, unknown>;
}

function exact(
  value: Record<string, unknown>,
  allowed: readonly string[],
  path: string,
): void {
  const extras = Object.keys(value).filter((key) => !allowed.includes(key));
  if (extras.length > 0)
    throw new ConfigError(
      path,
      `contains unknown fields: ${extras.sort().join(", ")}`,
    );
}

function string(value: unknown, path: string): string {
  if (typeof value !== "string" || value.length === 0)
    throw new ConfigError(path, "must be a non-empty string");
  return value;
}

function id(value: unknown, path: string): string {
  const result = string(value, path);
  if (!ID.test(result))
    throw new ConfigError(
      path,
      "must start with a lowercase letter and use lowercase letters and digits, joined by single '.', '_' or '-' (for example incident-ops)",
    );
  return result;
}

function positiveInteger(value: unknown, path: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1)
    throw new ConfigError(path, "must be a positive safe integer");
  return value as number;
}

function postgresIdentifier(value: unknown, path: string): string {
  const result = string(value, path);
  if (Buffer.byteLength(result, "utf8") > 63 || result.includes("\0"))
    throw new ConfigError(
      path,
      "must be a valid PostgreSQL identifier of at most 63 bytes",
    );
  return result;
}

function postgresType(value: unknown, path: string): string {
  const name = string(value, path);
  if (!POSTGRES_TYPES.has(name))
    throw new ConfigError(
      path,
      `${name} is not a type calque serve enrolls; it must be one of ${[...POSTGRES_TYPES.keys()].join(", ")}, by its pg_catalog name as calque init writes it`,
    );
  return name;
}

function array(
  value: unknown,
  path: string,
  { allowEmpty = false }: { allowEmpty?: boolean } = {},
): readonly unknown[] {
  if (!Array.isArray(value) || (value.length === 0 && !allowEmpty))
    throw new ConfigError(path, "must be a non-empty array");
  for (let index = 0; index < value.length; index += 1)
    if (!Object.hasOwn(value, index))
      throw new ConfigError(path, "must not be sparse");
  return value;
}

function unique(values: readonly string[], path: string): void {
  if (new Set(values).size !== values.length)
    throw new ConfigError(path, "must not contain duplicates");
}

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function sorted<T extends { readonly id: string }>(values: readonly T[]): T[] {
  return [...values].sort((left, right) => compareStrings(left.id, right.id));
}

function source(value: unknown, path: string): SourceDefinition {
  const item = object(value, path);
  if ("connectionEnv" in item)
    throw new ConfigError(
      `${path}.connectionEnv`,
      "is now databaseUrlEnv; rename the field",
    );
  exact(item, ["id", "databaseUrlEnv", "publication", "slot"], path);
  const databaseUrlEnv = string(item.databaseUrlEnv, `${path}.databaseUrlEnv`);
  if (!ENVIRONMENT_VARIABLE.test(databaseUrlEnv))
    throw new ConfigError(
      `${path}.databaseUrlEnv`,
      "must name an environment variable, not contain a connection string",
    );
  return {
    id: id(item.id, `${path}.id`),
    databaseUrlEnv,
    publication: postgresIdentifier(item.publication, `${path}.publication`),
    slot: postgresIdentifier(item.slot, `${path}.slot`),
  };
}

function grantAttribute(value: unknown, path: string): string {
  const result = string(value, path);
  // The rule the grant verifier applies to an attribute name.
  if (
    Buffer.byteLength(result, "utf8") > 512 ||
    [...result].some((character) => /\p{Cc}/u.test(character))
  )
    throw new ConfigError(
      path,
      "must be a grant attribute name: at most 512 bytes, with no control character",
    );
  return result;
}

function scope(value: unknown, path: string): ScopeDefinition {
  const item = object(value, path);
  const kind = string(item.kind, `${path}.kind`);
  const base = () => ({
    id: id(item.id, `${path}.id`),
    source: id(item.source, `${path}.source`),
    dataVersion: positiveInteger(item.dataVersion, `${path}.dataVersion`),
  });
  if (kind === "source-wide") {
    exact(item, ["id", "source", "dataVersion", "kind"], path);
    return { ...base(), kind };
  }
  if (kind === "partitioned") {
    exact(item, ["id", "source", "dataVersion", "kind", "attribute"], path);
    return {
      ...base(),
      kind,
      attribute: grantAttribute(item.attribute, `${path}.attribute`),
    };
  }
  throw new ConfigError(`${path}.kind`, "must be source-wide or partitioned");
}

function collection(value: unknown, path: string): CollectionDefinition {
  const item = object(value, path);
  exact(
    item,
    [
      "id",
      "source",
      "kind",
      "relation",
      "columns",
      "primaryKey",
      "scope",
      "partitionColumn",
      "shared",
      "adapter",
      "encoding",
    ],
    path,
  );
  if (item.kind !== "table")
    throw new ConfigError(`${path}.kind`, "must be table");
  const relation = object(item.relation, `${path}.relation`);
  exact(relation, ["schema", "table"], `${path}.relation`);
  const columns = array(item.columns, `${path}.columns`)
    .map((candidate, index) => {
      const columnPath = `${path}.columns[${index}]`;
      const column = object(candidate, columnPath);
      exact(column, ["name", "postgresType", "nullable"], columnPath);
      if (typeof column.nullable !== "boolean")
        throw new ConfigError(`${columnPath}.nullable`, "must be boolean");
      return {
        name: postgresIdentifier(column.name, `${columnPath}.name`),
        postgresType: postgresType(
          column.postgresType,
          `${columnPath}.postgresType`,
        ),
        nullable: column.nullable,
      };
    })
    .sort((left, right) => compareStrings(left.name, right.name));
  unique(
    columns.map(({ name }) => name),
    `${path}.columns`,
  );
  const primaryKey = array(item.primaryKey, `${path}.primaryKey`).map(
    (column, index) =>
      postgresIdentifier(column, `${path}.primaryKey[${index}]`),
  );
  unique(primaryKey, `${path}.primaryKey`);
  const selected = new Set(columns.map(({ name }) => name));
  for (const column of primaryKey)
    if (!selected.has(column))
      throw new ConfigError(path, `key column ${column} is not selected`);
  if (item.adapter !== "direct-columnar")
    throw new ConfigError(`${path}.adapter`, "must be direct-columnar");
  if (typeof item.encoding !== "string" || !ENCODINGS.has(item.encoding))
    throw new ConfigError(
      `${path}.encoding`,
      "is not a supported configured format",
    );
  const collectionId = id(item.id, `${path}.id`);
  const schema = postgresIdentifier(relation.schema, `${path}.relation.schema`);
  const table = postgresIdentifier(relation.table, `${path}.relation.table`);
  // The server names each served table after its relation's table, and the
  // client binds a collection only to the served table of its own id, so a
  // collection named anything else compiles and then never binds.
  if (collectionId !== table)
    throw new ConfigError(
      `${path}.id`,
      `collection ${collectionId} reads ${schema}.${table}, and a collection id must equal its relation's table name, ${table}`,
    );
  return {
    id: collectionId,
    source: id(item.source, `${path}.source`),
    kind: item.kind,
    relation: { schema, table },
    columns,
    primaryKey,
    scope: id(item.scope, `${path}.scope`),
    ...(item.partitionColumn === undefined
      ? {}
      : {
          partitionColumn: postgresIdentifier(
            item.partitionColumn,
            `${path}.partitionColumn`,
          ),
        }),
    ...(item.shared === undefined
      ? {}
      : { shared: sharedFlag(item.shared, `${path}.shared`) }),
    adapter: item.adapter,
    encoding: item.encoding as CollectionDefinition["encoding"],
  };
}

function sharedFlag(value: unknown, path: string): true {
  if (value !== true) throw new ConfigError(path, "must be true when present");
  return true;
}

const PARTITION_TYPES = new Set(["int2", "int4", "int8", "uuid"]);

/**
 * Checks each collection's partitioning against its scope: under a
 * partitioned scope a collection is partitioned by the first column of its
 * key, or shared, and the partition columns have one type.
 */
function checkPartitions(
  scopes: readonly ScopeDefinition[],
  collections: readonly CollectionDefinition[],
): void {
  for (const declared of scopes) {
    const members = collections.filter(({ scope }) => scope === declared.id);
    if (declared.kind === "source-wide") {
      for (const entry of members)
        if (entry.partitionColumn !== undefined || entry.shared !== undefined)
          throw new ConfigError(
            "$.collections",
            `collection ${entry.id} sets ${entry.partitionColumn === undefined ? "shared" : "partitionColumn"}, which only a partitioned scope reads; scope ${declared.id} is source-wide`,
          );
      continue;
    }
    const types = new Set<string>();
    for (const entry of members) {
      if (
        (entry.partitionColumn === undefined) ===
        (entry.shared === undefined)
      )
        throw new ConfigError(
          "$.collections",
          `collection ${entry.id} is in partitioned scope ${declared.id}, so it sets exactly one of partitionColumn, naming the column that holds the tenant, or shared: true`,
        );
      if (entry.partitionColumn === undefined) continue;
      if (entry.primaryKey[0] !== entry.partitionColumn)
        throw new ConfigError(
          "$.collections",
          `collection ${entry.id} is partitioned by ${entry.partitionColumn}, which must be the first column of its primaryKey, not ${entry.primaryKey[0]}`,
        );
      const column = entry.columns.find(
        ({ name }) => name === entry.partitionColumn,
      );
      if (!column || !PARTITION_TYPES.has(column.postgresType))
        throw new ConfigError(
          "$.collections",
          `collection ${entry.id} is partitioned by ${entry.partitionColumn}, of type ${column?.postgresType}; a partition column is int2, int4, int8 or uuid`,
        );
      types.add(column.postgresType);
    }
    if (types.size === 0)
      throw new ConfigError(
        "$.scopes",
        `partitioned scope ${declared.id} has no partitioned collection`,
      );
    if (types.size > 1)
      throw new ConfigError(
        "$.collections",
        `the partition columns of scope ${declared.id} have types ${[...types].sort().join(", ")}; they must have one type, the type of grant attribute ${declared.attribute}`,
      );
  }
}

function command(
  value: unknown,
  path: string,
  collections: ReadonlyMap<string, CollectionDefinition>,
): CommandDefinition {
  const item = object(value, path);
  const kind = string(item.kind, `${path}.kind`);
  if (kind === "crud") {
    exact(
      item,
      ["id", "kind", "collection", "operations", "writableColumns"],
      path,
    );
    const collectionId = id(item.collection, `${path}.collection`);
    const target = collections.get(collectionId);
    if (!target)
      throw new ConfigError(
        `${path}.collection`,
        `references unknown collection ${collectionId}`,
      );
    const operations = array(item.operations, `${path}.operations`)
      .map((operation, index) => {
        if (
          typeof operation !== "string" ||
          !CRUD_OPERATIONS.has(operation as CrudOperation)
        )
          throw new ConfigError(
            `${path}.operations[${index}]`,
            "is not insert, update, or delete",
          );
        return operation as CrudOperation;
      })
      .sort();
    unique(operations, `${path}.operations`);
    const writableColumns = array(
      item.writableColumns,
      `${path}.writableColumns`,
      { allowEmpty: true },
    )
      .map((column, index) =>
        postgresIdentifier(column, `${path}.writableColumns[${index}]`),
      )
      .sort();
    unique(writableColumns, `${path}.writableColumns`);
    const selected = new Set(target.columns.map(({ name }) => name));
    for (const column of writableColumns)
      if (!selected.has(column))
        throw new ConfigError(
          `${path}.writableColumns`,
          `references unselected column ${column}`,
        );
    const identity = new Set(target.primaryKey);
    for (const column of writableColumns)
      if (identity.has(column))
        throw new ConfigError(
          `${path}.writableColumns`,
          `cannot mutate identity column ${column}`,
        );
    const commandId = id(item.id, `${path}.id`);
    if (operations.includes("update") && writableColumns.length === 0)
      throw new ConfigError(
        `${path}.writableColumns`,
        `command ${commandId} lists update, so it must name at least one writable column`,
      );
    if (operations.includes("insert")) {
      const writable = new Set(writableColumns);
      const omitted = target.columns
        .map(({ name }) => name)
        .filter((name) => !identity.has(name) && !writable.has(name));
      if (omitted.length > 0)
        throw new ConfigError(
          `${path}.writableColumns`,
          `command ${commandId} lists insert, but its writableColumns omit ${omitted.join(", ")}: an insert writes every non-key column of ${collectionId}, so add ${omitted.length === 1 ? "that column" : "those columns"} to writableColumns or remove insert from operations`,
        );
    }
    return {
      id: commandId,
      kind,
      collection: collectionId,
      operations,
      writableColumns,
    } satisfies CrudCommandDefinition;
  }
  if (kind === "backend")
    throw new ConfigError(
      `${path}.kind`,
      "backend commands are not supported; declare a crud command",
    );
  throw new ConfigError(`${path}.kind`, "must be crud");
}

function deepFreeze<T>(value: T): Readonly<T> {
  if (ArrayBuffer.isView(value)) return value;
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value as Record<string, unknown>))
      deepFreeze(child);
  }
  return value;
}

/**
 * Validates a `calque.config.json` against the rules `calque serve` applies
 * before it connects, and returns it deeply frozen with every declaration
 * sorted by id.
 */
export function compileConfig(
  definition: CalqueConfigDefinition,
): CalqueManifest {
  const root = object(definition, "$" as const);
  exact(
    root,
    ["application", "protocol", "sources", "scopes", "collections", "commands"],
    "$",
  );
  const application = object(root.application, "$.application");
  exact(application, ["id", "version"], "$.application");
  const protocol = object(root.protocol, "$.protocol");
  exact(
    protocol,
    ["manifest", "artifact", "changeStream", "mutation"],
    "$.protocol",
  );
  for (const field of ["manifest", "artifact", "changeStream", "mutation"])
    if (protocol[field] !== 1)
      throw new ConfigError(`$.protocol.${field}`, "must equal 1");
  const sources = sorted(
    array(root.sources, "$.sources").map((value, index) =>
      source(value, `$.sources[${index}]`),
    ),
  );
  if (sources.length !== 1)
    throw new ConfigError("$.sources", "must hold exactly one source");
  unique(
    sources.map(({ id }) => id),
    "$.sources",
  );
  const sourceIds = new Set(sources.map(({ id }) => id));
  const scopes = sorted(
    array(root.scopes, "$.scopes").map((value, index) =>
      scope(value, `$.scopes[${index}]`),
    ),
  );
  if (scopes.length !== 1)
    throw new ConfigError("$.scopes", "must hold exactly one scope");
  unique(
    scopes.map(({ id }) => id),
    "$.scopes",
  );
  for (const entry of scopes)
    if (!sourceIds.has(entry.source))
      throw new ConfigError(
        "$.scopes",
        `scope ${entry.id} references unknown source ${entry.source}`,
      );
  const scopeById = new Map(scopes.map((entry) => [entry.id, entry]));
  const collections = sorted(
    array(root.collections, "$.collections").map((value, index) =>
      collection(value, `$.collections[${index}]`),
    ),
  );
  unique(
    collections.map(({ id }) => id),
    "$.collections",
  );
  for (const entry of collections) {
    if (!sourceIds.has(entry.source))
      throw new ConfigError(
        "$.collections",
        `collection ${entry.id} references unknown source ${entry.source}`,
      );
    const declaredScope = scopeById.get(entry.scope);
    if (!declaredScope)
      throw new ConfigError(
        "$.collections",
        `collection ${entry.id} references unknown scope ${entry.scope}`,
      );
  }
  checkPartitions(scopes, collections);
  const collectionById = new Map(collections.map((entry) => [entry.id, entry]));
  const commands = sorted(
    array(root.commands, "$.commands").map((value, index) =>
      command(value, `$.commands[${index}]`, collectionById),
    ),
  );
  unique(
    commands.map(({ id }) => id),
    "$.commands",
  );
  const writer = new Map<string, string>();
  for (const { id: commandId, collection } of commands) {
    const other = writer.get(collection);
    if (other !== undefined)
      throw new ConfigError(
        "$.commands",
        `commands ${other} and ${commandId} both write collection ${collection}; a collection takes one crud command`,
      );
    writer.set(collection, commandId);
  }
  const manifest: CalqueManifest = {
    schemaVersion: "calque.config.v1",
    application: {
      id: id(application.id, "$.application.id"),
      version: string(application.version, "$.application.version"),
    },
    protocol: {
      manifest: 1,
      artifact: positiveInteger(protocol.artifact, "$.protocol.artifact"),
      changeStream: positiveInteger(
        protocol.changeStream,
        "$.protocol.changeStream",
      ),
      mutation: positiveInteger(protocol.mutation, "$.protocol.mutation"),
    },
    sources,
    scopes,
    collections,
    commands,
  };
  return deepFreeze(manifest) as CalqueManifest;
}
