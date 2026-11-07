/**
 * The direct-columnar bootstrap a browser gate can serve without a server.
 * The artifact comes from core's `src/artifact.fixture.ts` and the plan is
 * derived from the same contract, so the two cannot drift. That module loads
 * by a computed specifier, which types as `any`, so the shapes this file
 * needs are declared below.
 */
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { zstdCompressSync } from "node:zlib";

/** The physical column kinds `GENERATED_KIND` maps to a generated kind. */
type PhysicalKind =
  | "bool"
  | "int64"
  | "f64"
  | "decimal"
  | "utf8"
  | "dict"
  | "bytes"
  | "uuid"
  | "json"
  | "date"
  | "time"
  | "timestamp";

interface ContractColumn {
  readonly name: string;
  readonly type: PhysicalKind;
  readonly nullable: boolean;
}

interface ContractTable {
  readonly name: string;
  readonly primaryKey: readonly string[];
  readonly columns: readonly ContractColumn[];
}

interface Contract {
  readonly format: string;
  readonly version: number;
  readonly tables: readonly ContractTable[];
}

interface FixtureIdentity {
  readonly sourceEpoch: string;
  readonly manifestVersion: string;
  readonly collectionSetHash: string;
  readonly schemaHash: string;
  readonly authorizationScopeVersion: string;
  readonly authorizationScopeHash: string;
  readonly cursor: string;
}

interface EncodedArtifact {
  readonly bytes: Uint8Array;
  readonly byteLength: number;
  readonly digest: string;
  readonly identity: FixtureIdentity;
}

interface DevFixtureModule {
  readonly WORKSPACE_CONTRACT: Contract;
  readonly encodeWorkspace: (
    tables: ReadonlyMap<string, readonly Record<string, unknown>[]>,
  ) => EncodedArtifact;
  readonly artifactChunks: (bytes: Uint8Array) => {
    readonly bytes: number;
    readonly sha256: readonly string[];
  };
}

/** The generated column kind each physical kind is declared as. */
const GENERATED_KIND: Readonly<Record<PhysicalKind, string>> = {
  bool: "bool",
  int64: "int64",
  f64: "f64",
  decimal: "decimal",
  utf8: "text",
  dict: "text",
  bytes: "bytes",
  uuid: "uuid",
  json: "json",
  date: "date",
  time: "time",
  timestamp: "timestamp",
};

const POLICY_HASH = `sha256:${"9".repeat(64)}`;

/** The two representations the Chromium native-decode profile accepts. */
export type ArtifactEncoding = "identity" | "zstd";

export const ARTIFACT_ENCODINGS: readonly ArtifactEncoding[] = [
  "identity",
  "zstd",
];

export interface ColumnarConsumerFixture {
  /** The encoded artifact body the server writes, per encoding. */
  readonly bodies: Readonly<Record<ArtifactEncoding, Buffer>>;
  /** The generated schema descriptor whose collections the plan serves. */
  readonly descriptor: Record<string, unknown>;
  /** The bootstrap plan for one selected representation. */
  readonly planFor: (encoding: ArtifactEncoding) => Record<string, unknown>;
}

/**
 * Mints the fixture. `origin` is the base the page is served from; the plan's
 * download and change-stream URLs are absolute under it, because the client
 * requires an absolute URL and refuses anything but HTTPS off localhost.
 */
export async function buildColumnarConsumerFixture(
  origin: string,
): Promise<ColumnarConsumerFixture> {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
  const devFixturePath = join(
    root,
    "client/packages/core/src/artifact.fixture.ts",
  );
  const devFixture = (await import(
    pathToFileURL(devFixturePath).href
  )) as DevFixtureModule;
  const contract = devFixture.WORKSPACE_CONTRACT;
  const encoded = devFixture.encodeWorkspace(
    new Map(contract.tables.map((table) => [table.name, []])),
  );
  const identity = encoded.identity;
  const relationOf = (table: ContractTable): string => `public.${table.name}`;

  const descriptor = {
    collections: contract.tables.map((table) => ({
      id: table.name,
      tableId: relationOf(table),
      columns: table.columns.map((column) => ({
        name: column.name,
        kind: GENERATED_KIND[column.type],
      })),
      primaryKey: [...table.primaryKey],
    })),
  };

  const runtimeTables = contract.tables.map((table) => ({
    tableId: relationOf(table),
    sourceSchema: "public",
    sourceTable: table.name,
    localTable: table.name,
    schemaHash: `sha256:${"1".repeat(64)}`,
    columnProfileHash: `sha256:${"2".repeat(64)}`,
    rowCount: 0,
    primaryKeyCodec: "pk-v1/protobuf",
    columns: table.columns.map((column, index) => ({
      columnId: index + 1,
      name: column.name,
    })),
  }));

  const bodies: Record<ArtifactEncoding, Buffer> = {
    identity: Buffer.from(encoded.bytes),
    zstd: zstdCompressSync(Buffer.from(encoded.bytes)),
  };

  const planFor = (encoding: ArtifactEncoding): Record<string, unknown> => ({
    protocolVersion: 1,
    planId: "browser-consumer-plan-1",
    expiresAt: "2099-01-01T00:00:00Z",
    source: {
      sourceEpoch: identity.sourceEpoch,
      publicationGeneration: 1,
      schemaGeneration: 1,
      authorizationGeneration: 1,
      authorizationPolicyHash: POLICY_HASH,
    },
    authorization: {
      policyHash: POLICY_HASH,
      scopeHash: identity.authorizationScopeHash,
      scopeToken: "browser-consumer-scope-token",
    },
    requiredTables: runtimeTables.map((table) => table.tableId).sort(),
    residentTables: [],
    activationCursor: identity.cursor,
    nextResidentStateProof: "browser-consumer-proof-1",
    artifacts: [
      {
        artifactId: "browser-consumer-artifact-1",
        attachAs: "stage_0000",
        snapshotCursor: identity.cursor,
        tables: runtimeTables.map((table) => table.tableId).sort(),
        decodedSha256: encoded.digest.slice("sha256:".length),
        decodedBytes: encoded.byteLength,
        bufferCapacityBytes: encoded.byteLength,
        installOrder: 0,
        runtime: {
          format: "directColumnar",
          formatVersion: contract.version,
          tableSetHash: `sha256:${"c".repeat(64)}`,
          shardProfileHash: `sha256:${"d".repeat(64)}`,
          indexProfileHash: `sha256:${"e".repeat(64)}`,
          schemaHash: identity.schemaHash,
          collectionSetHash: identity.collectionSetHash,
          identity: {
            manifestVersion: identity.manifestVersion,
            authorizationScopeVersion: identity.authorizationScopeVersion,
          },
          chunks: devFixture.artifactChunks(encoded.bytes),
          schema: contract,
          tables: runtimeTables,
        },
        selectedRepresentation: {
          representationId: `sha256:${createHash("sha256")
            .update(bodies[encoding])
            .digest("hex")}`,
          encoding,
          encodedBytes: bodies[encoding].byteLength,
          downloadUrl: `${origin}/__calque/artifact/${encoding}`,
        },
      },
    ],
    changeStream: {
      endpoint: `${origin}/__calque/change`,
      wireProtocol: "sync.change.v1",
      fromCursorExclusive: identity.cursor,
      throughCursorInclusive: identity.cursor,
      longPollTimeoutMs: 30_000,
      maxBatchBytes: 1024 * 1024,
    },
    memory: {
      minimumSafeBudgetBytes: 16 * 1024 * 1024,
      stagingOverlapBytes: 1024 * 1024,
      queryResultReserveBytes: 4 * 1024 * 1024,
      catchUpReserveBytes: 2 * 1024 * 1024,
      clientHeapReserveBytes: 4 * 1024 * 1024,
    },
  });

  return { bodies, descriptor, planFor };
}
