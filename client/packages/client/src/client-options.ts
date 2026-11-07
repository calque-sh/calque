import { base64Decode } from "@bufbuild/protobuf/wire";
import type { RevocationBarrierVerificationKey } from "@calque/protocol";

import {
  DEFAULT_MEMORY_BUDGET_BYTES,
  defaultClientHeapReserveBytes,
  PAGE_PATH,
  storedDeviceId,
} from "./bootstrap-start.js";
import type { InternalSyncClientOptions, SyncClientOptions } from "./client.js";
import {
  CLIENT_METADATA_RESERVE_BYTES,
  CONTROL_CHARACTER,
  utf8ByteLengthAtMost,
} from "./limits.js";
import type { CollectionShapes } from "./facade/types.js";
import {
  GENERATED_CLIENT_COLUMN_KINDS,
  type GeneratedClientCollectionColumnDescriptor,
  type GeneratedClientCollectionDescriptor,
  type GeneratedClientCollectionWritesDescriptor,
  type GeneratedClientColumnKind,
  type GeneratedClientCrudOperation,
  type GeneratedClientSchemaDescriptor,
} from "./generated-schema.js";
import {
  registerClientSchema,
  resolveClientSchema,
  type RegisteredClientSchema,
} from "./schema-registry.js";
import type { ClientSchema } from "./schema.js";

const COLLECTION_COLUMN_KINDS: ReadonlySet<string> = new Set(
  GENERATED_CLIENT_COLUMN_KINDS,
);
const CRUD_OPERATIONS: ReadonlySet<string> =
  new Set<GeneratedClientCrudOperation>(["insert", "update", "delete"]);

// These charges exceed pointer-only layouts, so they cover the retained frozen
// graph and the derived table-name index whatever the engine's private object
// representation.
const RETAINED_ARRAY_BASE_BYTES = 64;
const RETAINED_ARRAY_SLOT_BYTES = 16;
const RETAINED_RECORD_BYTES = 256;
const RETAINED_STRING_BASE_BYTES = 64;
const SCHEMA_OVER_RESERVE = "ClientSchema exceeds the client heap reserve";
const MINIMUM_RETAINED_SCHEMA_BYTES =
  RETAINED_RECORD_BYTES + RETAINED_ARRAY_BASE_BYTES;
// The client heap reserve is carved out of the memory budget and must hold
// the metadata pool and an empty schema, so no smaller budget can be served.
const MINIMUM_CLIENT_HEAP_RESERVE_BYTES =
  CLIENT_METADATA_RESERVE_BYTES + MINIMUM_RETAINED_SCHEMA_BYTES;
const STRING_ENDS_WITH = String.prototype.endsWith;
const FUNCTION_CALL = Function.prototype.call;
const ARRAY_IS_ARRAY = Array.isArray;
const NUMBER_IS_SAFE_INTEGER = Number.isSafeInteger;
const OBJECT_GET_OWN_PROPERTY_DESCRIPTOR = Object.getOwnPropertyDescriptor;
const REFLECT_OWN_KEYS = Reflect.ownKeys;
const REGEXP_TEST = RegExp.prototype.test;
const URL_CONSTRUCTOR = URL;
const STRING_CONSTRUCTOR = String;
const STRING_ENDS_WITH_VALUE = FUNCTION_CALL.bind(STRING_ENDS_WITH) as (
  value: string,
  search: string,
) => boolean;
const REGEXP_TEST_VALUE = FUNCTION_CALL.bind(REGEXP_TEST) as (
  expression: RegExp,
  value: string,
) => boolean;

export const CLIENT_CONSTRUCTION_LIMITS = Object.freeze({
  endpointCodeUnits: 8_192,
  deviceIdBytes: 1_024,
  collections: 1_024,
  columnsPerCollection: 1_024,
  primaryKeyColumns: 64,
  identifierCodeUnits: 256,
  aggregateSchemaBytes: 4 * 1024 * 1024,
});

export interface SealedSyncClientOptions {
  readonly endpointUrl: URL;
  readonly auth: SyncClientOptions["auth"];
  readonly schema: GeneratedClientSchemaDescriptor;
  readonly memoryBudgetBytes: number;
  readonly clientHeapReserveBytes: number;
  readonly schemaRetainedBytes: number;
  readonly deviceId: string;
  /** Whether the application named `deviceId` rather than taking the default. */
  readonly deviceIdChosen: boolean;
  readonly fetch: typeof globalThis.fetch;
  readonly grantVerificationKey?: RevocationBarrierVerificationKey;
}

const PUBLIC_KEYS = Object.freeze([
  "endpoint",
  "auth",
  "schema",
  "memoryBudgetBytes",
  "deviceId",
  "fetch",
  "grantVerificationKey",
] as const);

const PUBLIC_REQUIRED_KEYS = Object.freeze([
  "endpoint",
  "auth",
  "schema",
] as const);

const INTERNAL_KEYS = Object.freeze([
  ...PUBLIC_KEYS,
  "clientHeapReserveBytes",
] as const);

const INTERNAL_REQUIRED_KEYS = Object.freeze([
  ...PUBLIC_REQUIRED_KEYS,
  "clientHeapReserveBytes",
] as const);

export function sealSyncClientOptions(
  value: SyncClientOptions,
): SealedSyncClientOptions {
  const captured = captureRecord(
    value,
    PUBLIC_KEYS,
    PUBLIC_REQUIRED_KEYS,
    "SyncClientOptions",
  );
  const page = pageMemory();
  const memoryBudgetBytes = positiveSafeInteger(
    captured.memoryBudgetBytes ??
      page?.memoryBudgetBytes ??
      DEFAULT_MEMORY_BUDGET_BYTES,
    "SyncClientOptions memoryBudgetBytes must be a positive safe integer",
  );
  if (memoryBudgetBytes < MINIMUM_CLIENT_HEAP_RESERVE_BYTES)
    throw new TypeError(
      `SyncClientOptions memoryBudgetBytes must be at least ${MINIMUM_CLIENT_HEAP_RESERVE_BYTES}`,
    );
  // The page's reserve only beside the page's budget, as its starter sent it.
  const reserve =
    page?.memoryBudgetBytes === memoryBudgetBytes
      ? page.clientHeapReserveBytes
      : undefined;
  return sealCapturedOptions(
    captured,
    memoryBudgetBytes,
    reserve !== undefined &&
      Number.isSafeInteger(reserve) &&
      reserve >= MINIMUM_CLIENT_HEAP_RESERVE_BYTES &&
      reserve <= memoryBudgetBytes
      ? reserve
      : defaultClientHeapReserveBytes(memoryBudgetBytes),
  );
}

export function sealInternalSyncClientOptions(
  value: InternalSyncClientOptions,
): SealedSyncClientOptions {
  const captured = captureRecord(
    value,
    INTERNAL_KEYS,
    INTERNAL_REQUIRED_KEYS,
    "InternalSyncClientOptions",
  );
  const memoryBudgetBytes = positiveSafeInteger(
    captured.memoryBudgetBytes,
    "InternalSyncClientOptions memoryBudgetBytes must be a positive safe integer",
  );
  const clientHeapReserveBytes = positiveSafeInteger(
    captured.clientHeapReserveBytes,
    "InternalSyncClientOptions clientHeapReserveBytes must be a positive safe integer",
  );
  if (clientHeapReserveBytes > memoryBudgetBytes)
    throw new TypeError(
      "InternalSyncClientOptions clientHeapReserveBytes must not exceed memoryBudgetBytes",
    );
  return sealCapturedOptions(
    captured,
    memoryBudgetBytes,
    clientHeapReserveBytes,
  );
}

/**
 * The memory the page's inline starter asked for: its `calque-plan`
 * element's, which `renderBootstrap` writes, else its `calque-bootstrap`
 * element's, which the Vite plugin writes.
 */
function pageMemory():
  | {
      readonly memoryBudgetBytes: number;
      readonly clientHeapReserveBytes?: number;
    }
  | undefined {
  for (const id of ["calque-plan", "calque-bootstrap"]) {
    const text = (globalThis.document as Document | undefined)?.getElementById(
      id,
    )?.textContent;
    let value: {
      memoryBudgetBytes?: unknown;
      clientHeapReserveBytes?: unknown;
    };
    try {
      value = JSON.parse(text || "null") ?? {};
    } catch {
      continue;
    }
    const { memoryBudgetBytes, clientHeapReserveBytes } = value;
    if (typeof memoryBudgetBytes === "number")
      return typeof clientHeapReserveBytes === "number"
        ? { memoryBudgetBytes, clientHeapReserveBytes }
        : { memoryBudgetBytes };
  }
  return undefined;
}

function sealCapturedOptions(
  captured: Readonly<Record<string, unknown>>,
  memoryBudgetBytes: number,
  clientHeapReserveBytes: number,
): SealedSyncClientOptions {
  if (clientHeapReserveBytes < MINIMUM_CLIENT_HEAP_RESERVE_BYTES)
    throw new TypeError(
      `SyncClientOptions clientHeapReserveBytes must be at least ${MINIMUM_CLIENT_HEAP_RESERVE_BYTES}`,
    );
  if (typeof captured.auth !== "function")
    throw new TypeError("SyncClientOptions auth must be a function");
  const auth = captured.auth as SyncClientOptions["auth"];
  const fetcher =
    captured.fetch === undefined
      ? globalThis.fetch.bind(globalThis)
      : captured.fetch;
  if (typeof fetcher !== "function")
    throw new TypeError("SyncClientOptions fetch must be a function");
  const endpointUrl = sealEndpoint(captured.endpoint);
  const deviceId = boundedUtf8String(
    captured.deviceId ?? storedDeviceId(),
    CLIENT_CONSTRUCTION_LIMITS.deviceIdBytes,
    "SyncClientOptions deviceId is invalid",
  );
  const schemaLimit = clientHeapReserveBytes - CLIENT_METADATA_RESERVE_BYTES;
  const sealedSchema = resolveClientSchema(captured.schema);
  if (sealedSchema.retainedBytes > schemaLimit)
    throw new TypeError(SCHEMA_OVER_RESERVE);
  const grantVerificationKey = sealGrantVerificationKey(
    captured.grantVerificationKey,
  );
  const sealed = {
    endpointUrl,
    auth,
    schema: sealedSchema.descriptor,
    memoryBudgetBytes,
    clientHeapReserveBytes,
    schemaRetainedBytes: sealedSchema.retainedBytes,
    deviceId,
    deviceIdChosen: captured.deviceId !== undefined,
    fetch: fetcher as typeof globalThis.fetch,
    ...(grantVerificationKey ? { grantVerificationKey } : {}),
  } satisfies SealedSyncClientOptions;
  return Object.freeze(sealed);
}

/**
 * A malformed key throws rather than being ignored, which would leave a
 * client that silently refuses every revocation.
 */
function sealGrantVerificationKey(
  value: unknown,
): RevocationBarrierVerificationKey | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null)
    throw new TypeError("SyncClientOptions grantVerificationKey is invalid");
  const { keyId, publicKey } = value as Record<string, unknown>;
  if (
    typeof keyId !== "string" ||
    keyId.length === 0 ||
    keyId.length > CLIENT_CONSTRUCTION_LIMITS.identifierCodeUnits
  )
    throw new TypeError(
      "SyncClientOptions grantVerificationKey keyId is invalid",
    );
  // 32 bytes in unpadded base64url are 43 characters, and the last one
  // carries two unused bits that must be zero: the only spelling `calque
  // serve` decodes, so a key the server takes is a key the client takes.
  if (
    typeof publicKey !== "string" ||
    !/^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$/u.test(publicKey)
  )
    throw new TypeError(
      "SyncClientOptions grantVerificationKey publicKey must be a 32-byte Ed25519 key in unpadded base64url (43 characters)",
    );
  return Object.freeze({ keyId, publicKey: base64Decode(publicKey) });
}

export function createGeneratedClientSchema<
  Collections extends CollectionShapes = never,
>(descriptor: GeneratedClientSchemaDescriptor): ClientSchema<Collections> {
  const sealed = sealClientSchema(descriptor);
  return registerClientSchema(
    sealed.descriptor,
    sealed.retainedBytes,
  ) as ClientSchema<Collections>;
}

function sealClientSchema(value: unknown): RegisteredClientSchema {
  const captured = captureRecord(
    value,
    ["collections"] as const,
    ["collections"] as const,
    "ClientSchema",
  );
  const budget = new SchemaByteBudget();
  budget.record();
  const collectionLength = captureDenseArrayLength(
    captured.collections,
    CLIENT_CONSTRUCTION_LIMITS.collections,
    "ClientSchema collections are invalid",
  );
  budget.array(collectionLength);
  const collectionValues = captureDenseArray(
    captured.collections,
    CLIENT_CONSTRUCTION_LIMITS.collections,
    "ClientSchema collections are invalid",
    collectionLength,
  );
  const collections: GeneratedClientCollectionDescriptor[] = new Array(
    collectionValues.length,
  );
  const collectionIds = new Set<string>();
  const collectionTableIds = new Set<string>();
  for (let index = 0; index < collectionValues.length; index += 1) {
    const collection = sealCollection(collectionValues[index], budget);
    if (collectionIds.has(collection.id))
      throw new TypeError("ClientSchema contains duplicate collection IDs");
    collectionIds.add(collection.id);
    if (collectionTableIds.has(collection.tableId))
      throw new TypeError(
        "ClientSchema contains duplicate collection table IDs",
      );
    collectionTableIds.add(collection.tableId);
    collections[index] = collection;
  }

  const descriptor = Object.freeze({
    collections: Object.freeze(collections),
  });
  return Object.freeze({ descriptor, retainedBytes: budget.retainedBytes });
}

function sealCollection(
  value: unknown,
  budget: SchemaByteBudget,
): GeneratedClientCollectionDescriptor {
  const message = "ClientSchema collection is invalid";
  const captured = captureRecord(
    value,
    ["id", "tableId", "columns", "primaryKey", "writes"] as const,
    ["id", "tableId", "columns", "primaryKey"] as const,
    "ClientSchema collection",
  );
  const id = budget.identifier(captured.id, message);
  const tableId = budget.identifier(captured.tableId, message);
  const columnValues = captureDenseArray(
    captured.columns,
    CLIENT_CONSTRUCTION_LIMITS.columnsPerCollection,
    message,
  );
  if (columnValues.length === 0) throw new TypeError(message);
  budget.array(columnValues.length);
  const columns: GeneratedClientCollectionColumnDescriptor[] = new Array(
    columnValues.length,
  );
  const columnNames = new Set<string>();
  for (let index = 0; index < columnValues.length; index += 1) {
    const column = captureRecord(
      columnValues[index],
      ["name", "kind"] as const,
      ["name", "kind"] as const,
      "ClientSchema collection column",
    );
    const name = budget.identifier(column.name, message);
    if (
      typeof column.kind !== "string" ||
      !COLLECTION_COLUMN_KINDS.has(column.kind)
    )
      throw new TypeError(message);
    if (columnNames.has(name))
      throw new TypeError("ClientSchema collection has duplicate columns");
    columnNames.add(name);
    budget.record();
    columns[index] = Object.freeze({
      name,
      kind: column.kind as GeneratedClientColumnKind,
    });
  }
  const primaryKey = sealColumnNameList(
    captured.primaryKey,
    CLIENT_CONSTRUCTION_LIMITS.primaryKeyColumns,
    columnNames,
    budget,
    message,
  );
  if (primaryKey.length === 0) throw new TypeError(message);
  let writes: GeneratedClientCollectionWritesDescriptor | undefined;
  if (captured.writes !== undefined) {
    const declared = captureRecord(
      captured.writes,
      ["operations", "writableColumns"] as const,
      ["operations", "writableColumns"] as const,
      "ClientSchema collection writes",
    );
    const operationValues = captureDenseArray(declared.operations, 3, message);
    if (operationValues.length === 0) throw new TypeError(message);
    const operations: GeneratedClientCrudOperation[] = [];
    for (const operation of operationValues) {
      if (
        typeof operation !== "string" ||
        !CRUD_OPERATIONS.has(operation) ||
        operations.includes(operation as GeneratedClientCrudOperation)
      )
        throw new TypeError(message);
      operations.push(operation as GeneratedClientCrudOperation);
    }
    const writableColumns = sealColumnNameList(
      declared.writableColumns,
      CLIENT_CONSTRUCTION_LIMITS.columnsPerCollection,
      columnNames,
      budget,
      message,
    );
    budget.record();
    writes = Object.freeze({
      operations: Object.freeze(operations),
      writableColumns,
    });
  }
  budget.record();
  return Object.freeze({
    id,
    tableId,
    columns: Object.freeze(columns),
    primaryKey,
    ...(writes === undefined ? {} : { writes }),
  });
}

function sealColumnNameList(
  value: unknown,
  maximumLength: number,
  known: ReadonlySet<string>,
  budget: SchemaByteBudget,
  message: string,
): readonly string[] {
  const values = captureDenseArray(value, maximumLength, message);
  budget.array(values.length);
  const names: string[] = new Array(values.length);
  const seen = new Set<string>();
  for (let index = 0; index < values.length; index += 1) {
    const name = budget.identifier(values[index], message);
    if (!known.has(name) || seen.has(name)) throw new TypeError(message);
    seen.add(name);
    names[index] = name;
  }
  return Object.freeze(names);
}

class SchemaByteBudget {
  #aggregateTextBytes = 0;
  #retainedBytes = 0;

  get retainedBytes(): number {
    return this.#retainedBytes;
  }

  array(length: number): void {
    this.#chargeRetained(
      checkedSum(
        RETAINED_ARRAY_BASE_BYTES,
        checkedProduct(length, RETAINED_ARRAY_SLOT_BYTES, SCHEMA_OVER_RESERVE),
        SCHEMA_OVER_RESERVE,
      ),
      SCHEMA_OVER_RESERVE,
    );
  }

  record(): void {
    this.#chargeRetained(RETAINED_RECORD_BYTES, SCHEMA_OVER_RESERVE);
  }

  identifier(value: unknown, message: string): string {
    const result = boundedIdentifier(
      value,
      CLIENT_CONSTRUCTION_LIMITS.identifierCodeUnits,
      message,
    );
    this.#chargeText(result, utf8ByteLengthAtMost(result, 1024), message);
    return result;
  }

  #chargeText(value: string, bytes: number, message: string): void {
    const next = this.#aggregateTextBytes + bytes;
    if (
      !NUMBER_IS_SAFE_INTEGER(next) ||
      next > CLIENT_CONSTRUCTION_LIMITS.aggregateSchemaBytes
    )
      throw new TypeError(message);
    const retained = checkedSum(
      RETAINED_STRING_BASE_BYTES,
      checkedProduct(value.length, 2, message),
      message,
    );
    this.#chargeRetained(retained, message);
    this.#aggregateTextBytes = next;
  }

  #chargeRetained(bytes: number, message: string): void {
    this.#retainedBytes = checkedSum(this.#retainedBytes, bytes, message);
  }
}

/**
 * `value`'s own data properties, each of which `allowed` must name and among
 * which every `required` key must be. An accessor is refused without being
 * called. Refusals start with `label` and name the key at fault.
 */
function captureRecord<
  const Allowed extends readonly string[],
  const Required extends readonly Allowed[number][],
>(
  value: unknown,
  allowed: Allowed,
  required: Required,
  label: string,
): Readonly<Record<Allowed[number], unknown>> {
  if (typeof value !== "object" || value === null)
    throw new TypeError(`${label} must be an object`);
  const invalid = `${label} is invalid`;
  let keys: readonly PropertyKey[];
  try {
    keys = REFLECT_OWN_KEYS(value);
  } catch {
    throw new TypeError(invalid);
  }
  const allowedSet = new Set<string>(allowed);
  const present = new Set<string>();
  for (const key of keys) {
    // `String`, because a symbol in a template literal throws.
    if (typeof key !== "string" || !allowedSet.has(key))
      throw new TypeError(
        `${label} has unknown key "${STRING_CONSTRUCTOR(key)}"`,
      );
    present.add(key);
  }
  for (const key of required) {
    if (!present.has(key))
      throw new TypeError(`${label} is missing required key "${key}"`);
  }
  const captured = Object.create(null) as Record<string, unknown>;
  for (const key of allowed) {
    if (!present.has(key)) continue;
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = OBJECT_GET_OWN_PROPERTY_DESCRIPTOR(value, key);
    } catch {
      throw new TypeError(invalid);
    }
    if (!descriptor || !("value" in descriptor))
      throw new TypeError(
        `${label} ${key} must be a data property, not a getter or setter`,
      );
    captured[key] = descriptor.value;
  }
  return Object.freeze(captured) as Readonly<Record<Allowed[number], unknown>>;
}

function captureDenseArray(
  value: unknown,
  maximumLength: number,
  message: string,
  capturedLength?: number,
): readonly unknown[] {
  const length =
    capturedLength ?? captureDenseArrayLength(value, maximumLength, message);
  let keys: readonly PropertyKey[];
  try {
    keys = REFLECT_OWN_KEYS(value as object);
  } catch {
    throw new TypeError(message);
  }
  if (keys.length !== length + 1) throw new TypeError(message);
  const expected = new Set<string>(["length"]);
  for (let index = 0; index < length; index += 1) expected.add(String(index));
  for (const key of keys) {
    if (typeof key !== "string" || !expected.has(key))
      throw new TypeError(message);
  }
  const captured: unknown[] = new Array(length);
  for (let index = 0; index < length; index += 1) {
    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = OBJECT_GET_OWN_PROPERTY_DESCRIPTOR(value, String(index));
    } catch {
      throw new TypeError(message);
    }
    if (!descriptor || !("value" in descriptor)) throw new TypeError(message);
    captured[index] = descriptor.value;
  }
  return Object.freeze(captured);
}

function captureDenseArrayLength(
  value: unknown,
  maximumLength: number,
  message: string,
): number {
  let isArray = false;
  try {
    isArray = ARRAY_IS_ARRAY(value);
  } catch {
    throw new TypeError(message);
  }
  if (!isArray) throw new TypeError(message);
  let lengthDescriptor: PropertyDescriptor | undefined;
  try {
    lengthDescriptor = OBJECT_GET_OWN_PROPERTY_DESCRIPTOR(value, "length");
  } catch {
    throw new TypeError(message);
  }
  if (!lengthDescriptor || !("value" in lengthDescriptor))
    throw new TypeError(message);
  const length = lengthDescriptor.value;
  if (!NUMBER_IS_SAFE_INTEGER(length) || length < 0 || length > maximumLength)
    throw new TypeError(message);
  return length;
}

/**
 * The endpoint as a URL whose path ends in `/`. The rules are
 * `allowedEndpoint`'s; this one says which rule a refused endpoint broke,
 * without repeating the endpoint, which may carry credentials.
 */
function sealEndpoint(value: unknown): URL {
  const limit = CLIENT_CONSTRUCTION_LIMITS.endpointCodeUnits;
  if (typeof value !== "string" || value.length === 0 || value.length > limit)
    throw new TypeError(
      `SyncClientOptions endpoint must be a string of 1 to ${limit} characters`,
    );
  if (REGEXP_TEST_VALUE(CONTROL_CHARACTER, value))
    throw new TypeError(
      "SyncClientOptions endpoint must not contain control characters",
    );
  let result: URL;
  try {
    result = REGEXP_TEST_VALUE(PAGE_PATH, value)
      ? new URL_CONSTRUCTOR(value, globalThis.location?.href)
      : new URL_CONSTRUCTOR(value);
  } catch {
    throw new TypeError(
      "SyncClientOptions endpoint is not an absolute URL, or a path on a page",
    );
  }
  const localHttp =
    result.protocol === "http:" &&
    (result.hostname === "localhost" || result.hostname === "127.0.0.1");
  if (result.protocol !== "https:" && !localHttp)
    throw new TypeError(
      "SyncClientOptions endpoint must be an https: URL, or an http: URL on localhost or 127.0.0.1",
    );
  if (result.username.length > 0 || result.password.length > 0)
    throw new TypeError(
      "SyncClientOptions endpoint must not contain a user name or password",
    );
  if (result.hash.length > 0)
    throw new TypeError(
      "SyncClientOptions endpoint must not contain a fragment",
    );
  if (!STRING_ENDS_WITH_VALUE(result.pathname, "/")) result.pathname += "/";
  return result;
}

function boundedIdentifier(
  value: unknown,
  maximumCodeUnits: number,
  message: string,
): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > maximumCodeUnits ||
    REGEXP_TEST_VALUE(CONTROL_CHARACTER, value)
  )
    throw new TypeError(message);
  return value;
}

function positiveSafeInteger(value: unknown, message: string): number {
  if (!NUMBER_IS_SAFE_INTEGER(value) || (value as number) <= 0)
    throw new TypeError(message);
  return value as number;
}

function boundedUtf8String(
  value: unknown,
  maximumBytes: number,
  message: string,
): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    REGEXP_TEST_VALUE(CONTROL_CHARACTER, value) ||
    utf8ByteLengthAtMost(value, maximumBytes) > maximumBytes
  )
    throw new TypeError(message);
  return value;
}

function checkedProduct(left: number, right: number, message: string): number {
  const result = left * right;
  if (!NUMBER_IS_SAFE_INTEGER(result)) throw new TypeError(message);
  return result;
}

function checkedSum(left: number, right: number, message: string): number {
  const result = left + right;
  if (!NUMBER_IS_SAFE_INTEGER(result)) throw new TypeError(message);
  return result;
}
