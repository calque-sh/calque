import {
  DIRECT_COLUMNAR_FORMAT_VERSION,
  parseDirectColumnarSchema,
  type DirectColumnarSchema,
  type TrustedArtifactChunks,
} from "@calque/core";

import { isBoundedIdentifier } from "./limits.js";

const HASH_ID = /^sha256:[0-9a-f]{64}$/;
const SHA256 = /^[0-9a-f]{64}$/;
const MAX_SAFE = Number.MAX_SAFE_INTEGER;
/**
 * The bytes each digest in `runtime.chunks` covers, the last covering what is
 * left. Fixed by `spec/bootstrap-plan.schema.json`, so no plan can make the
 * worker hash a chunk of no bytes or name a digest for every few bytes.
 */
export const ARTIFACT_CHUNK_BYTES = 4 * 1024 * 1024;

export interface BootstrapPlan {
  readonly protocolVersion: 1;
  readonly planId: string;
  readonly expiresAt: string;
  readonly source: PlanSource;
  readonly authorization: PlanAuthorization;
  readonly requiredTables: readonly string[];
  readonly residentTables: readonly string[];
  readonly residentCursor?: string;
  readonly activationCursor: string;
  readonly nextResidentStateProof: string;
  readonly artifacts: readonly PlanArtifact[];
  readonly changeStream: PlanChangeStream;
  readonly memory: PlanMemory;
}

interface PlanSource {
  readonly sourceEpoch: string;
  readonly publicationGeneration: number;
  readonly schemaGeneration: number;
  readonly authorizationGeneration: number;
  readonly authorizationPolicyHash: string;
}

interface PlanAuthorization {
  readonly policyHash: string;
  readonly scopeHash: string;
  readonly scopeToken: string;
}

export interface PlanArtifact {
  readonly artifactId: string;
  readonly attachAs: string;
  readonly snapshotCursor: string;
  readonly tables: readonly string[];
  readonly decodedSha256: string;
  readonly decodedBytes: number;
  readonly bufferCapacityBytes: number;
  readonly installOrder: number;
  readonly runtime: PlanArtifactColumnarRuntime;
  readonly selectedRepresentation: {
    readonly representationId: string;
    /**
     * `zstd` is the browser's own content coding. The brotli encodings arrive
     * as stored, under no `Content-Encoding`, for the decoder worker, which
     * also unpacks a packed body into the artifact.
     */
    readonly encoding:
      | "identity"
      | "zstd"
      | "brotli-large-window"
      | "packed-brotli-large-window";
    readonly encodedBytes: number;
    readonly downloadUrl: string;
  };
}

/**
 * `spec/bootstrap-plan.schema.json`'s direct-columnar `artifactRuntime`: the
 * trusted physical schema and the two identity strings the encoded header
 * binds. The decoder recomputes `schemaHash` from `schema`, so a plan cannot
 * claim one without the other.
 */
export interface PlanArtifactColumnarRuntime {
  readonly tableSetHash: string;
  readonly shardProfileHash: string;
  readonly indexProfileHash: string;
  readonly tables: readonly PlanArtifactTable[];
  readonly format: "directColumnar";
  readonly schemaHash: string;
  readonly collectionSetHash: string;
  readonly formatVersion: typeof DIRECT_COLUMNAR_FORMAT_VERSION;
  readonly schema: DirectColumnarSchema;
  readonly identity: {
    readonly manifestVersion: string;
    readonly authorizationScopeVersion: string;
  };
  /**
   * The SHA-256 of each `ARTIFACT_CHUNK_BYTES` of the decoded artifact, in
   * order, the last covering what is left. The worker verifies each chunk
   * against its digest as it arrives, before any row of it is visible.
   */
  readonly chunks: TrustedArtifactChunks;
}

interface PlanArtifactColumn {
  readonly columnId: number;
  readonly name: string;
}

export interface PlanArtifactTable {
  readonly tableId: string;
  readonly sourceSchema: string;
  readonly sourceTable: string;
  readonly localTable: string;
  readonly schemaHash: string;
  readonly columnProfileHash: string;
  readonly rowCount: number;
  readonly primaryKeyCodec: string;
  /** Wire column ids, which the binding needs for the change contract. */
  readonly columns?: readonly PlanArtifactColumn[];
}

interface PlanChangeStream {
  readonly endpoint: string;
  readonly fromCursorExclusive: string;
  readonly throughCursorInclusive: string;
  readonly longPollTimeoutMs: number;
  readonly maxBatchBytes: number;
}

interface PlanMemory {
  readonly minimumSafeBudgetBytes: number;
  readonly stagingOverlapBytes: number;
  readonly queryResultReserveBytes: number;
  readonly catchUpReserveBytes: number;
  readonly clientHeapReserveBytes: number;
}

export function parseBootstrapPlan(value: unknown): BootstrapPlan {
  const plan = object(value, "bootstrap plan");
  if (plan.protocolVersion !== 1) fail("protocolVersion", "must be 1");
  const planId = id(plan.planId, "planId");
  const expiresAt = string(plan.expiresAt, "expiresAt");
  if (!Number.isFinite(Date.parse(expiresAt))) fail("expiresAt", "is invalid");
  const sourceValue = object(plan.source, "source");
  const source: PlanSource = {
    sourceEpoch: id(sourceValue.sourceEpoch, "source.sourceEpoch"),
    publicationGeneration: integer(
      sourceValue.publicationGeneration,
      "source.publicationGeneration",
    ),
    schemaGeneration: integer(
      sourceValue.schemaGeneration,
      "source.schemaGeneration",
    ),
    authorizationGeneration: integer(
      sourceValue.authorizationGeneration,
      "source.authorizationGeneration",
    ),
    authorizationPolicyHash: hashId(
      sourceValue.authorizationPolicyHash,
      "source.authorizationPolicyHash",
    ),
  };
  const authorizationValue = object(plan.authorization, "authorization");
  const authorization: PlanAuthorization = {
    policyHash: hashId(
      authorizationValue.policyHash,
      "authorization.policyHash",
    ),
    scopeHash: hashId(authorizationValue.scopeHash, "authorization.scopeHash"),
    scopeToken: string(
      authorizationValue.scopeToken,
      "authorization.scopeToken",
    ),
  };
  if (authorization.policyHash !== source.authorizationPolicyHash)
    fail(
      "authorization.policyHash",
      "must equal source.authorizationPolicyHash",
    );
  const requiredTables = ids(plan.requiredTables, "requiredTables", 1, 256);
  const residentTables = ids(
    plan.residentTables ?? [],
    "residentTables",
    0,
    1024,
  );
  const activationCursor = string(plan.activationCursor, "activationCursor");
  const residentCursor = optionalString(plan.residentCursor, "residentCursor");
  const nextResidentStateProof = string(
    plan.nextResidentStateProof,
    "nextResidentStateProof",
  );
  const artifactsValue = array(plan.artifacts, "artifacts");
  const artifacts = artifactsValue.map(parseArtifact);
  validateArtifactCoverage(artifacts, residentTables, requiredTables);
  const changeValue = object(plan.changeStream, "changeStream");
  if (changeValue.wireProtocol !== "sync.change.v1")
    fail("changeStream.wireProtocol", "is unsupported");
  const changeStream: PlanChangeStream = {
    endpoint: url(changeValue.endpoint, "changeStream.endpoint"),
    fromCursorExclusive: string(
      changeValue.fromCursorExclusive,
      "changeStream.fromCursorExclusive",
    ),
    throughCursorInclusive: string(
      changeValue.throughCursorInclusive,
      "changeStream.throughCursorInclusive",
    ),
    longPollTimeoutMs: integerRange(
      changeValue.longPollTimeoutMs,
      "changeStream.longPollTimeoutMs",
      1_000,
      120_000,
    ),
    maxBatchBytes: integerRange(
      changeValue.maxBatchBytes,
      "changeStream.maxBatchBytes",
      1_024,
      16 * 1024 * 1024,
    ),
  };
  if (changeStream.throughCursorInclusive !== activationCursor)
    fail("changeStream.throughCursorInclusive", "must equal activationCursor");
  const memoryValue = object(plan.memory, "memory");
  const memory: PlanMemory = {
    minimumSafeBudgetBytes: integer(
      memoryValue.minimumSafeBudgetBytes,
      "memory.minimumSafeBudgetBytes",
    ),
    stagingOverlapBytes: integer(
      memoryValue.stagingOverlapBytes,
      "memory.stagingOverlapBytes",
    ),
    queryResultReserveBytes: integer(
      memoryValue.queryResultReserveBytes,
      "memory.queryResultReserveBytes",
    ),
    catchUpReserveBytes: integer(
      memoryValue.catchUpReserveBytes,
      "memory.catchUpReserveBytes",
    ),
    clientHeapReserveBytes: integer(
      memoryValue.clientHeapReserveBytes,
      "memory.clientHeapReserveBytes",
    ),
  };
  if (
    memory.stagingOverlapBytes > memory.minimumSafeBudgetBytes ||
    memory.queryResultReserveBytes > memory.minimumSafeBudgetBytes ||
    memory.catchUpReserveBytes > memory.minimumSafeBudgetBytes ||
    memory.clientHeapReserveBytes > memory.minimumSafeBudgetBytes ||
    memory.clientHeapReserveBytes === 0
  )
    fail("memory", "contains a reserve larger than its minimum safe budget");
  return {
    protocolVersion: 1,
    planId,
    expiresAt,
    source,
    authorization,
    requiredTables,
    residentTables,
    ...(residentCursor ? { residentCursor } : {}),
    activationCursor,
    nextResidentStateProof,
    artifacts,
    changeStream,
    memory,
  };
}

function parseArtifact(value: unknown, index: number): PlanArtifact {
  const path = `artifacts[${index}]`;
  const artifact = object(value, path);
  const tables = ids(artifact.tables, `${path}.tables`, 1, 1024);
  const runtimeValue = object(artifact.runtime, `${path}.runtime`);
  const runtimeTables = array(
    runtimeValue.tables,
    `${path}.runtime.tables`,
  ).map((table, tableIndex) =>
    parseArtifactTable(table, `${path}.runtime.tables[${tableIndex}]`),
  );
  if (
    !sameSet(
      tables,
      runtimeTables.map((table) => table.tableId),
    )
  )
    fail(`${path}.runtime.tables`, "must exactly match artifact tables");
  const representation = object(
    artifact.selectedRepresentation,
    `${path}.selectedRepresentation`,
  );
  const encoding = representation.encoding;
  if (
    encoding !== "identity" &&
    encoding !== "zstd" &&
    encoding !== "brotli-large-window" &&
    encoding !== "packed-brotli-large-window"
  )
    fail(`${path}.selectedRepresentation.encoding`, "is unsupported");
  const decodedBytes = integerRange(
    artifact.decodedBytes,
    `${path}.decodedBytes`,
    1,
    MAX_SAFE,
  );
  return {
    artifactId: id(artifact.artifactId, `${path}.artifactId`),
    attachAs: string(artifact.attachAs, `${path}.attachAs`),
    snapshotCursor: string(artifact.snapshotCursor, `${path}.snapshotCursor`),
    tables,
    decodedSha256: sha256(artifact.decodedSha256, `${path}.decodedSha256`),
    decodedBytes,
    bufferCapacityBytes: integerRange(
      artifact.bufferCapacityBytes,
      `${path}.bufferCapacityBytes`,
      1,
      MAX_SAFE,
    ),
    installOrder: integer(artifact.installOrder, `${path}.installOrder`),
    runtime: parseArtifactRuntime(
      runtimeValue,
      runtimeTables,
      decodedBytes,
      `${path}.runtime`,
    ),
    selectedRepresentation: {
      representationId: hashId(
        representation.representationId,
        `${path}.selectedRepresentation.representationId`,
      ),
      encoding,
      encodedBytes: integerRange(
        representation.encodedBytes,
        `${path}.selectedRepresentation.encodedBytes`,
        1,
        MAX_SAFE,
      ),
      downloadUrl: url(
        representation.downloadUrl,
        `${path}.selectedRepresentation.downloadUrl`,
      ),
    },
  };
}

function parseArtifactRuntime(
  runtime: Record<string, unknown>,
  tables: readonly PlanArtifactTable[],
  decodedBytes: number,
  path: string,
): PlanArtifactColumnarRuntime {
  const base = {
    tableSetHash: hashId(runtime.tableSetHash, `${path}.tableSetHash`),
    shardProfileHash: hashId(
      runtime.shardProfileHash,
      `${path}.shardProfileHash`,
    ),
    indexProfileHash: hashId(
      runtime.indexProfileHash,
      `${path}.indexProfileHash`,
    ),
    tables,
  };
  if (runtime.format === "directColumnar") {
    if (runtime.formatVersion !== DIRECT_COLUMNAR_FORMAT_VERSION)
      fail(`${path}.formatVersion`, "is unsupported");
    const identity = object(runtime.identity, `${path}.identity`);
    const schema = parseColumnarSchema(runtime.schema, `${path}.schema`);
    const schemaTables = schema.tables.map((table) => table.name);
    if (
      !sameSet(
        schemaTables,
        tables.map((table) => table.localTable),
      )
    )
      fail(`${path}.schema.tables`, "must exactly match runtime.tables");
    return {
      ...base,
      format: "directColumnar",
      schemaHash: hashId(runtime.schemaHash, `${path}.schemaHash`),
      collectionSetHash: hashId(
        runtime.collectionSetHash,
        `${path}.collectionSetHash`,
      ),
      formatVersion: DIRECT_COLUMNAR_FORMAT_VERSION,
      schema,
      identity: {
        manifestVersion: string(
          identity.manifestVersion,
          `${path}.identity.manifestVersion`,
        ),
        authorizationScopeVersion: string(
          identity.authorizationScopeVersion,
          `${path}.identity.authorizationScopeVersion`,
        ),
      },
      chunks: parseChunks(runtime.chunks, decodedBytes, `${path}.chunks`),
    };
  }
  return fail(`${path}.format`, "is unsupported");
}

/**
 * One digest for each `ARTIFACT_CHUNK_BYTES` of an artifact `decodedBytes`
 * long, and nothing else.
 */
function parseChunks(
  value: unknown,
  decodedBytes: number,
  path: string,
): TrustedArtifactChunks {
  const chunks = object(value, path);
  if (!sameSet(Object.keys(chunks), ["bytes", "sha256"]))
    fail(path, "must hold exactly bytes and sha256");
  if (chunks.bytes !== ARTIFACT_CHUNK_BYTES)
    fail(`${path}.bytes`, `must be ${ARTIFACT_CHUNK_BYTES}`);
  const digests = array(chunks.sha256, `${path}.sha256`);
  if (digests.length !== Math.ceil(decodedBytes / ARTIFACT_CHUNK_BYTES))
    fail(
      `${path}.sha256`,
      "must name one digest for each chunk of the artifact",
    );
  return {
    bytes: ARTIFACT_CHUNK_BYTES,
    sha256: digests.map(
      (digest, index) =>
        hashId(digest, `${path}.sha256[${index}]`) as `sha256:${string}`,
    ),
  };
}

function parseColumnarSchema(
  value: unknown,
  path: string,
): DirectColumnarSchema {
  try {
    return parseDirectColumnarSchema(value);
  } catch (error) {
    fail(path, error instanceof Error ? error.message : "is invalid");
  }
}

function parseArtifactTable(value: unknown, path: string): PlanArtifactTable {
  const table = object(value, path);
  const columns =
    table.columns === undefined
      ? undefined
      : parseArtifactColumns(table.columns, `${path}.columns`);
  return {
    tableId: id(table.tableId, `${path}.tableId`),
    sourceSchema: string(table.sourceSchema, `${path}.sourceSchema`),
    sourceTable: string(table.sourceTable, `${path}.sourceTable`),
    localTable: string(table.localTable, `${path}.localTable`),
    schemaHash: hashId(table.schemaHash, `${path}.schemaHash`),
    columnProfileHash: hashId(
      table.columnProfileHash,
      `${path}.columnProfileHash`,
    ),
    rowCount: integer(table.rowCount, `${path}.rowCount`),
    primaryKeyCodec: string(table.primaryKeyCodec, `${path}.primaryKeyCodec`),
    ...(columns === undefined ? {} : { columns }),
  };
}

function parseArtifactColumns(
  value: unknown,
  path: string,
): readonly PlanArtifactColumn[] {
  const columns = array(value, path).map((columnValue, index) => {
    const column = object(columnValue, `${path}[${index}]`);
    return {
      columnId: integerRange(
        column.columnId,
        `${path}[${index}].columnId`,
        1,
        MAX_SAFE,
      ),
      name: string(column.name, `${path}[${index}].name`),
    };
  });
  if (columns.length === 0) fail(path, "must be non-empty");
  if (new Set(columns.map((column) => column.columnId)).size !== columns.length)
    fail(path, "contains duplicate column IDs");
  if (new Set(columns.map((column) => column.name)).size !== columns.length)
    fail(path, "contains duplicate column names");
  return columns;
}

function validateArtifactCoverage(
  artifacts: readonly PlanArtifact[],
  residentTables: readonly string[],
  requiredTables: readonly string[],
): void {
  const seenOrders = new Set<number>();
  const seenArtifactIds = new Set<string>();
  const seenAttachments = new Set<string>();
  const seenTables = new Set(residentTables);
  for (const artifact of artifacts) {
    if (seenOrders.has(artifact.installOrder))
      fail("artifacts.installOrder", "must be unique");
    seenOrders.add(artifact.installOrder);
    if (seenArtifactIds.has(artifact.artifactId))
      fail("artifacts.artifactId", "must be unique");
    seenArtifactIds.add(artifact.artifactId);
    if (!/^stage_[0-9]{4}$/.test(artifact.attachAs))
      fail("artifacts.attachAs", "must match stage_0000");
    if (seenAttachments.has(artifact.attachAs))
      fail("artifacts.attachAs", "must be unique");
    seenAttachments.add(artifact.attachAs);
    if (artifact.bufferCapacityBytes < artifact.decodedBytes)
      fail("artifacts.bufferCapacityBytes", "is smaller than decodedBytes");
    if (
      artifact.selectedRepresentation.encoding === "identity" &&
      artifact.selectedRepresentation.encodedBytes !== artifact.decodedBytes
    )
      fail(
        "artifacts.selectedRepresentation.encodedBytes",
        "must equal decodedBytes",
      );
    for (const table of artifact.tables) {
      if (seenTables.has(table)) fail("artifacts.tables", "overlap");
      seenTables.add(table);
    }
  }
  if (
    seenOrders.size !== artifacts.length ||
    artifacts.some((artifact) => artifact.installOrder >= artifacts.length)
  )
    fail("artifacts.installOrder", "must be contiguous from zero");
  for (const table of requiredTables) {
    if (!seenTables.has(table))
      fail("artifacts.tables", `do not cover required table ${table}`);
  }
}

function object(value: unknown, path: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    fail(path, "must be an object");
  return value as Record<string, unknown>;
}

function array(value: unknown, path: string): readonly unknown[] {
  if (!Array.isArray(value)) fail(path, "must be an array");
  return value;
}

function string(value: unknown, path: string): string {
  if (typeof value !== "string" || value.length === 0)
    fail(path, "must be non-empty");
  return value;
}

function optionalString(value: unknown, path: string): string | undefined {
  return value === undefined ? undefined : string(value, path);
}

function id(value: unknown, path: string): string {
  const result = string(value, path);
  if (!isBoundedIdentifier(result)) fail(path, "is not a valid ID");
  return result;
}

function ids(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): readonly string[] {
  const result = array(value, path).map((item, index) =>
    id(item, `${path}[${index}]`),
  );
  if (result.length < minimum || result.length > maximum)
    fail(path, `must contain ${minimum}..${maximum} IDs`);
  if (new Set(result).size !== result.length) fail(path, "contains duplicates");
  return result;
}

function hashId(value: unknown, path: string): string {
  const result = string(value, path);
  if (!HASH_ID.test(result)) fail(path, "must be a SHA-256 ID");
  return result;
}

function sha256(value: unknown, path: string): string {
  const result = string(value, path);
  if (!SHA256.test(result)) fail(path, "must be a SHA-256 digest");
  return result;
}

function integer(value: unknown, path: string): number {
  return integerRange(value, path, 0, MAX_SAFE);
}

function integerRange(
  value: unknown,
  path: string,
  minimum: number,
  maximum: number,
): number {
  if (
    !Number.isSafeInteger(value) ||
    (value as number) < minimum ||
    (value as number) > maximum
  )
    fail(path, `must be an integer in ${minimum}..${maximum}`);
  return value as number;
}

function url(value: unknown, path: string): string {
  const result = string(value, path);
  let parsed: URL;
  try {
    parsed = new URL(result);
  } catch {
    fail(path, "must be an absolute URL");
  }
  const isLocalHttp =
    parsed.protocol === "http:" &&
    (parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1");
  if (parsed.protocol !== "https:" && !isLocalHttp)
    fail(path, "must use HTTPS except on localhost");
  if (parsed.username || parsed.password || parsed.hash)
    fail(path, "must not contain credentials or a fragment");
  return result;
}

export function sameSet(
  left: readonly string[],
  right: readonly string[],
): boolean {
  return (
    left.length === right.length && left.every((value) => right.includes(value))
  );
}

/** The 32 bytes of a `sha256:` hash id the parser accepted. */
export function hashIdBytes(hashId: string): Uint8Array {
  const hex = hashId.slice("sha256:".length);
  return Uint8Array.from({ length: 32 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

function fail(path: string, reason: string): never {
  throw new TypeError(`invalid bootstrap plan: ${path} ${reason}`);
}
