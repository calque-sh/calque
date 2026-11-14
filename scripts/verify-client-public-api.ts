import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";

import {
  assertPublicDeclarationGraph,
  collectPublicDeclarationGraph,
} from "./lib/public-declaration-graph.ts";
// The client package's TypeScript, which this gate loads at run time: the
// root's 7.x ships no JavaScript compiler API.
import type * as TypeScriptApi from "../client/packages/client/node_modules/typescript";

interface UnionVariantShape {
  readonly members: readonly string[];
  readonly optional: readonly string[];
  readonly resultStates?: readonly string[];
}

/** The declaration and runtime surface `client/packages/client/public-api.json` pins. */
interface PublicApiAllowlist {
  readonly packageSubpaths: readonly string[];
  readonly reachableDeclarationGraphSha256: string;
  readonly rootValues: readonly string[];
  readonly rootTypes: readonly string[];
  readonly syncClientMembers: readonly string[];
  readonly syncClientReadonlyDataMembers: readonly string[];
  readonly syncClientReadonlyFunctionMembers: readonly string[];
  readonly syncClientOptions: readonly string[];
  readonly syncAuthMembers: readonly string[];
  readonly syncClientAuthReturnTypes: readonly string[];
  readonly syncStatusVariants: Readonly<Record<string, readonly string[]>>;
  readonly mutationResultVariants: Readonly<Record<string, UnionVariantShape>>;
  readonly mutationStatusVariants: Readonly<Record<string, UnionVariantShape>>;
  readonly mutationSubmissionErrorMembers: readonly string[];
  readonly mutationSubmissionErrorCodes: readonly string[];
  readonly mutationSubmissionErrorRuntimeOwnKeys: readonly string[];
  readonly liveQueryResourceMembers: readonly string[];
  readonly syncPublicationMembers: readonly string[];
  readonly liveQueryResourceReadonlyDataMembers: readonly string[];
  readonly liveQueryResourceReadonlyFunctionMembers: readonly string[];
  readonly generatedValues: readonly string[];
  readonly generatedTypes: readonly string[];
  readonly internalValues: readonly string[];
  readonly internalTypes: readonly string[];
  readonly bootstrapStartValues: readonly string[];
  readonly bootstrapStartTypes: readonly string[];
  readonly bootstrapStartDeclarationGraphSha256: string;
  readonly bootstrapRenderValues: readonly string[];
  readonly bootstrapRenderTypes: readonly string[];
  readonly bootstrapRenderDeclarationGraphSha256: string;
  readonly vitePluginValues: readonly string[];
  readonly vitePluginTypes: readonly string[];
  readonly vitePluginDeclarationGraphSha256: string;
  readonly reactRootValues: readonly string[];
  readonly reactRootTypes: readonly string[];
}

interface PackageExportConditions {
  readonly types?: string;
  readonly import?: string;
}

interface PackageManifest {
  readonly exports: Readonly<
    Record<string, PackageExportConditions | undefined>
  >;
}

/**
 * The built entry points, as this gate calls them. `import()` on a computed,
 * cache-busted specifier is typed `any`, so without these the compiler would
 * not check the runtime half of the gate.
 */
interface ClientRootRuntime {
  readonly MutationSubmissionError: new (
    mutationId: string,
    code: string,
    message: string,
  ) => object;
  readonly SyncError: Function;
  readonly [exportName: string]: unknown;
}

interface MutationResultSample {
  readonly mutationId: string;
  readonly state: string;
  readonly message: string;
  readonly conflict?: {
    readonly kind: string;
    readonly applicationPayload: Uint8Array;
  };
  readonly retryAfterMs?: bigint;
}

interface ClientMutationRuntime {
  freezeMutationResult(sample: MutationResultSample): object;
}

interface ClientGeneratedRuntime {
  createGeneratedClientSchema(descriptor: {
    collections: readonly unknown[];
  }): object;
  readonly [exportName: string]: unknown;
}

// The digest of the built `@calque/react` declaration graph. It covers the
// declarations' doc comments as well as their shapes.
const CURRENT_REACT_DECLARATION_GRAPH_SHA256 =
  "b7f7e0dec8134a194d06f64a3081266fdaae487ac08a3b70af8d013d1fb26cc5";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const clientRoot = join(root, "client/packages/client");
const requireFromClient = createRequire(join(clientRoot, "package.json"));
const ts = requireFromClient("typescript") as typeof TypeScriptApi;
const allowlist = JSON.parse(
  readFileSync(join(clientRoot, "public-api.json"), "utf8"),
) as PublicApiAllowlist;
const clientPackage = JSON.parse(
  readFileSync(join(clientRoot, "package.json"), "utf8"),
) as PackageManifest;
assertExact(
  "client package export subpaths",
  Object.keys(clientPackage.exports),
  allowlist.packageSubpaths,
);
const clientExportTargets: readonly (readonly [string, string, string])[] = [
  [".", "./dist/index.d.ts", "./dist/index.js"],
  ["./generated", "./dist/generated.d.ts", "./dist/generated.js"],
  ["./internal", "./dist/internal.d.ts", "./dist/internal.js"],
  [
    "./bootstrap-start",
    "./dist/bootstrap-start-entry.d.ts",
    "./dist/bootstrap-start-entry.js",
  ],
  [
    "./bootstrap-render",
    "./dist/bootstrap-render-entry.d.ts",
    "./dist/bootstrap-render-entry.js",
  ],
  ["./vite", "./dist/vite.d.ts", "./dist/vite.js"],
];
for (const [
  subpath,
  expectedDeclaration,
  expectedRuntime,
] of clientExportTargets) {
  const conditions = clientPackage.exports[subpath];
  assertExact(
    `client package ${subpath} export conditions`,
    Object.keys(conditions ?? {}),
    ["types", "import"],
  );
  if (
    conditions?.types !== expectedDeclaration ||
    conditions?.import !== expectedRuntime
  ) {
    throw new Error(`client package ${subpath} export targets drifted`);
  }
}

const rootDeclarationPath = join(clientRoot, "dist/index.d.ts");
const rootDeclaration = sourceFile(rootDeclarationPath);
const exported = declarationExports(rootDeclaration);
assertExact(
  "root runtime declaration exports",
  exported.values,
  allowlist.rootValues,
);
assertExact(
  "root type declaration exports",
  exported.types,
  allowlist.rootTypes,
);
assertNoMechanismSurface("root declaration exports", [
  ...exported.values,
  ...exported.types,
]);

const clientDeclaration = sourceFile(join(clientRoot, "dist/client.d.ts"));
assertExact(
  "SyncClient members",
  interfaceMembers(clientDeclaration, "SyncClient"),
  allowlist.syncClientMembers,
);
assertReadonlyInterfaceShape(
  clientDeclaration,
  "SyncClient",
  allowlist.syncClientReadonlyDataMembers,
  allowlist.syncClientReadonlyFunctionMembers,
);
assertExact(
  "SyncClientOptions members",
  interfaceMembers(clientDeclaration, "SyncClientOptions"),
  allowlist.syncClientOptions,
);
assertReadonlyInterfaceShape(
  clientDeclaration,
  "SyncClientOptions",
  allowlist.syncClientOptions.filter((member) => member !== "auth"),
  ["auth"],
);
assertExactInterfacePropertyTypes(clientDeclaration, "SyncClientOptions", {
  endpoint: { optional: false, type: "string" },
  auth: { optional: false, type: "() => SyncAuth | Promise<SyncAuth>" },
  schema: { optional: false, type: "ClientSchema<Collections>" },
  memoryBudgetBytes: { optional: true, type: "number" },
  deviceId: { optional: true, type: "string" },
  fetch: { optional: true, type: "typeof globalThis.fetch" },
  // The authority's Ed25519 key comes from configuration, never from the plan.
  // A named type lets the pin hold it to two strings with no callback.
  grantVerificationKey: { optional: true, type: "GrantVerificationKey" },
});
assertExact(
  "GrantVerificationKey members",
  interfaceMembers(clientDeclaration, "GrantVerificationKey"),
  ["keyId", "publicKey"],
);
assertReadonlyInterfaceShape(
  clientDeclaration,
  "GrantVerificationKey",
  ["keyId", "publicKey"],
  [],
);
assertExactAuthCallback(clientDeclaration, allowlist.syncClientAuthReturnTypes);
// Declared beside the starter, whose `startedAuth` returns it, so the
// starter's module needs no import from the client's.
const syncAuthDeclaration = sourceFile(
  join(clientRoot, "dist/bootstrap-start.d.ts"),
);
assertExact(
  "SyncAuth members",
  interfaceMembers(syncAuthDeclaration, "SyncAuth"),
  allowlist.syncAuthMembers,
);
assertReadonlyInterfaceShape(
  syncAuthDeclaration,
  "SyncAuth",
  allowlist.syncAuthMembers,
  [],
);
// The documentation of the options a public query takes must not name the
// machinery underneath it.
assertInterfaceTextExcludes(
  sourceFile(join(clientRoot, "dist/facade/types.d.ts")),
  "QueryRunOptions",
  ["worker", "cursor", "SQLite", "progress-handler", "transport"],
);
const schemaDeclaration = sourceFile(join(clientRoot, "dist/schema.d.ts"));
assertOpaqueClientSchema(schemaDeclaration);
assertExactUnionVariants(
  clientDeclaration,
  "SyncStatus",
  allowlist.syncStatusVariants,
);

const mutationDeclaration = sourceFile(join(clientRoot, "dist/mutation.d.ts"));
assertExactDiscriminatedUnionVariantShapes(
  mutationDeclaration,
  "MutationResult",
  allowlist.mutationResultVariants,
);
assertReadonlyUnionVariantNestedTypeLiteralShape(
  mutationDeclaration,
  "MutationResult",
  "conflict",
  "conflict",
  ["applicationPayload", "kind"],
);
assertExactDiscriminatedUnionVariantShapes(
  mutationDeclaration,
  "MutationStatus",
  allowlist.mutationStatusVariants,
);
assertMutationSubmissionErrorShape(
  mutationDeclaration,
  allowlist.mutationSubmissionErrorMembers,
  allowlist.mutationSubmissionErrorCodes,
);

const resourceDeclaration = sourceFile(join(clientRoot, "dist/resource.d.ts"));
assertExact(
  "LiveQueryResource members",
  interfaceMembers(resourceDeclaration, "LiveQueryResource"),
  allowlist.liveQueryResourceMembers,
);
assertReadonlyInterfaceShape(
  resourceDeclaration,
  "LiveQueryResource",
  allowlist.liveQueryResourceReadonlyDataMembers,
  allowlist.liveQueryResourceReadonlyFunctionMembers,
);

// Pinned by shape: a widened member would be a new promise about what a
// batch says.
assertExact(
  "SyncPublication members",
  interfaceMembers(clientDeclaration, "SyncPublication"),
  allowlist.syncPublicationMembers,
);
assertReadonlyInterfaceShape(
  clientDeclaration,
  "SyncPublication",
  allowlist.syncPublicationMembers,
  [],
);

for (const declaration of [
  "index.d.ts",
  "client.d.ts",
  "error.d.ts",
  "mutation.d.ts",
  "resource.d.ts",
  "schema.d.ts",
]) {
  const contents = readFileSync(join(clientRoot, "dist", declaration), "utf8");
  if (contents.includes("@calque/worker")) {
    throw new Error(
      `public declaration ${declaration} leaks @calque/worker ownership`,
    );
  }
}
assertPublicDeclarationGraph({
  ts,
  entryPath: rootDeclarationPath,
  declarationRoots: [{ root: join(clientRoot, "dist"), prefix: "dist" }],
  expectedSha256: allowlist.reachableDeclarationGraphSha256,
  label: "reachable root",
});

const rootRuntime: ClientRootRuntime = await import(
  `${pathToFileURL(join(clientRoot, "dist/index.js")).href}?api-check=${Date.now()}`
);
assertExact(
  "root runtime exports",
  Object.keys(rootRuntime),
  allowlist.rootValues,
);
assertNoMechanismSurface("root runtime exports", Object.keys(rootRuntime));
const mutationError = new rootRuntime.MutationSubmissionError(
  "api-verifier-mutation",
  "MUTATION_AMBIGUOUS",
  "ambiguous",
);
assertExact(
  "MutationSubmissionError runtime own keys",
  Object.keys(mutationError),
  allowlist.mutationSubmissionErrorRuntimeOwnKeys,
);
if (!(mutationError instanceof rootRuntime.SyncError)) {
  throw new Error("MutationSubmissionError does not inherit from SyncError");
}
if (!Object.isFrozen(mutationError)) {
  throw new Error("MutationSubmissionError runtime instance is not frozen");
}
const mutationRuntime: ClientMutationRuntime = await import(
  `${pathToFileURL(join(clientRoot, "dist/mutation.js")).href}?api-check=${Date.now()}`
);
const mutationPayload = new Uint8Array([1, 2, 3]);
const mutationResultSamples = {
  already_canonical: {
    mutationId: "api-verifier-canonical",
    state: "already_canonical",
    message: "canonical",
  },
  awaiting_canonical_echo: {
    mutationId: "api-verifier-awaiting",
    state: "awaiting_canonical_echo",
    message: "accepted",
  },
  conflict: {
    mutationId: "api-verifier-conflict",
    state: "conflict",
    message: "conflict",
    conflict: { kind: "version", applicationPayload: mutationPayload },
  },
  rejected: {
    mutationId: "api-verifier-rejected",
    state: "rejected",
    message: "rejected",
  },
  retryable: {
    mutationId: "api-verifier-retryable",
    state: "retryable",
    message: "retryable",
    retryAfterMs: 250n,
  },
} satisfies Record<string, MutationResultSample>;
for (const [state, sample] of Object.entries(mutationResultSamples)) {
  const result = mutationRuntime.freezeMutationResult(sample);
  const expectedVariant = allowlist.mutationResultVariants[state];
  if (!expectedVariant) {
    throw new Error(`public-api.json pins no MutationResult.${state} variant`);
  }
  assertExact(
    `MutationResult.${state} runtime own keys`,
    Object.keys(result),
    expectedVariant.members,
  );
  if (!Object.isFrozen(result)) {
    throw new Error(`MutationResult.${state} runtime instance is not frozen`);
  }
}
if (!Object.isFrozen(mutationResultSamples.conflict.conflict)) {
  throw new Error("MutationResult.conflict runtime container is not frozen");
}
if (Object.isFrozen(mutationPayload)) {
  throw new Error(
    "MutationResult conflict payload must remain application-owned",
  );
}
const generatedRuntime: ClientGeneratedRuntime = await import(
  `${pathToFileURL(join(clientRoot, "dist/generated.js")).href}?api-check=${Date.now()}`
);
const generatedDeclaration = declarationExports(
  sourceFile(join(clientRoot, "dist/generated.d.ts")),
);
assertExact(
  "generated runtime declaration exports",
  generatedDeclaration.values,
  allowlist.generatedValues,
);
assertExact(
  "generated type declaration exports",
  generatedDeclaration.types,
  allowlist.generatedTypes,
);
assertExact(
  "generated runtime exports",
  Object.keys(generatedRuntime),
  allowlist.generatedValues,
);
const internalRuntime: Readonly<Record<string, unknown>> = await import(
  `${pathToFileURL(join(clientRoot, "dist/internal.js")).href}?api-check=${Date.now()}`
);
const internalDeclaration = declarationExports(
  sourceFile(join(clientRoot, "dist/internal.d.ts")),
);
assertExact(
  "internal runtime declaration exports",
  internalDeclaration.values,
  allowlist.internalValues,
);
assertExact(
  "internal type declaration exports",
  internalDeclaration.types,
  allowlist.internalTypes,
);
assertExact(
  "internal runtime exports",
  Object.keys(internalRuntime),
  allowlist.internalValues,
);
// The starter a page evaluates before the client library. Its module imports
// only the starter's two modules, and those import nothing: an added import
// would put client code in the page's first chunk, ahead of the request the
// starter sends.
const bootstrapStartDeclarationPath = join(
  clientRoot,
  "dist/bootstrap-start-entry.d.ts",
);
const bootstrapStartDeclaration = declarationExports(
  sourceFile(bootstrapStartDeclarationPath),
);
assertExact(
  "bootstrap-start runtime declaration exports",
  bootstrapStartDeclaration.values,
  allowlist.bootstrapStartValues,
);
assertExact(
  "bootstrap-start type declaration exports",
  bootstrapStartDeclaration.types,
  allowlist.bootstrapStartTypes,
);
assertNoMechanismSurface("bootstrap-start declaration exports", [
  ...bootstrapStartDeclaration.values,
  ...bootstrapStartDeclaration.types,
]);
assertPublicDeclarationGraph({
  ts,
  entryPath: bootstrapStartDeclarationPath,
  declarationRoots: [{ root: join(clientRoot, "dist"), prefix: "dist" }],
  expectedSha256: allowlist.bootstrapStartDeclarationGraphSha256,
  label: "reachable bootstrap-start",
});
const bootstrapStartRuntime: Readonly<Record<string, unknown>> = await import(
  `${pathToFileURL(join(clientRoot, "dist/bootstrap-start-entry.js")).href}?api-check=${Date.now()}`
);
assertExact(
  "bootstrap-start runtime exports",
  Object.keys(bootstrapStartRuntime),
  allowlist.bootstrapStartValues,
);
const inlineStarter = bootstrapStartRuntime.BOOTSTRAP_START_SCRIPT;
if (
  typeof inlineStarter !== "string" ||
  bootstrapStartRuntime.BOOTSTRAP_START_SCRIPT_SHA256 !==
    `sha256-${createHash("sha256").update(inlineStarter, "utf8").digest("base64")}`
) {
  throw new Error(
    "BOOTSTRAP_START_SCRIPT_SHA256 is not the SHA-256 of BOOTSTRAP_START_SCRIPT",
  );
}
// The backend's half of a rendered bootstrap: one function and the types of
// its parameters. It imports only the starter's module, so a backend that
// renders pages never loads the client library.
const bootstrapRenderDeclarationPath = join(
  clientRoot,
  "dist/bootstrap-render-entry.d.ts",
);
const bootstrapRenderDeclaration = declarationExports(
  sourceFile(bootstrapRenderDeclarationPath),
);
assertExact(
  "bootstrap-render runtime declaration exports",
  bootstrapRenderDeclaration.values,
  allowlist.bootstrapRenderValues,
);
assertExact(
  "bootstrap-render type declaration exports",
  bootstrapRenderDeclaration.types,
  allowlist.bootstrapRenderTypes,
);
assertNoMechanismSurface("bootstrap-render declaration exports", [
  ...bootstrapRenderDeclaration.values,
  ...bootstrapRenderDeclaration.types,
]);
assertPublicDeclarationGraph({
  ts,
  entryPath: bootstrapRenderDeclarationPath,
  declarationRoots: [{ root: join(clientRoot, "dist"), prefix: "dist" }],
  expectedSha256: allowlist.bootstrapRenderDeclarationGraphSha256,
  label: "reachable bootstrap-render",
});
const bootstrapRenderRuntime: Readonly<Record<string, unknown>> = await import(
  `${pathToFileURL(join(clientRoot, "dist/bootstrap-render-entry.js")).href}?api-check=${Date.now()}`
);
assertExact(
  "bootstrap-render runtime exports",
  Object.keys(bootstrapRenderRuntime),
  allowlist.bootstrapRenderValues,
);
// The Vite plugin: one function and its options type. In Node it loads the
// starter's modules for their constants and nothing else of the client, and
// `@calque/config` when it renders the bindings.
const vitePluginDeclarationPath = join(clientRoot, "dist/vite.d.ts");
const vitePluginDeclaration = declarationExports(
  sourceFile(vitePluginDeclarationPath),
);
assertExact(
  "vite runtime declaration exports",
  vitePluginDeclaration.values,
  allowlist.vitePluginValues,
);
assertExact(
  "vite type declaration exports",
  vitePluginDeclaration.types,
  allowlist.vitePluginTypes,
);
assertPublicDeclarationGraph({
  ts,
  entryPath: vitePluginDeclarationPath,
  declarationRoots: [{ root: join(clientRoot, "dist"), prefix: "dist" }],
  expectedSha256: allowlist.vitePluginDeclarationGraphSha256,
  label: "reachable vite",
});
const vitePluginRuntime: Readonly<Record<string, unknown>> = await import(
  `${pathToFileURL(join(clientRoot, "dist/vite.js")).href}?api-check=${Date.now()}`
);
assertExact(
  "vite runtime exports",
  Object.keys(vitePluginRuntime),
  allowlist.vitePluginValues,
);
for (const [module, imports] of [
  [
    "dist/bootstrap-start-entry.js",
    ["./bootstrap-start.js", "./bootstrap-start-script.js"],
  ],
  ["dist/bootstrap-start.js", []],
  ["dist/bootstrap-start-script.js", []],
  ["dist/bootstrap-render-entry.js", ["./bootstrap-render.js"]],
  ["dist/bootstrap-render.js", ["./bootstrap-start.js"]],
  [
    "dist/vite.js",
    [
      "node:fs",
      "node:path",
      "node:process",
      "./bootstrap-start.js",
      "./bootstrap-start-script.js",
      "@calque/config",
    ],
  ],
] as const) {
  assertExact(
    `${module} imports`,
    ts
      .preProcessFile(
        readFileSync(join(clientRoot, module), "utf8"),
        true,
        true,
      )
      .importedFiles.map(({ fileName }) => fileName),
    imports,
  );
}

const generatedSchema = generatedRuntime.createGeneratedClientSchema({
  collections: [],
});
assertExact(
  "generated ClientSchema own keys",
  Reflect.ownKeys(generatedSchema),
  [],
);
if (
  !Object.isFrozen(generatedSchema) ||
  Object.getPrototypeOf(generatedSchema) !== null
) {
  throw new Error(
    "generated ClientSchema is not a frozen null-prototype capability",
  );
}

const reactRoot = join(root, "client/packages/react");
const reactPackage = JSON.parse(
  readFileSync(join(reactRoot, "package.json"), "utf8"),
);
assertExact(
  "React package export subpaths",
  Object.keys(reactPackage.exports),
  ["."],
);
assertExact(
  "React package root export conditions",
  Object.keys(reactPackage.exports["."] ?? {}),
  ["types", "import"],
);
if (
  reactPackage.exports["."]?.types !== "./dist/index.d.ts" ||
  reactPackage.exports["."]?.import !== "./dist/index.js"
) {
  throw new Error("React package root export targets drifted");
}
const reactDeclaration = declarationExports(
  sourceFile(join(reactRoot, "dist/index.d.ts")),
);
assertExact(
  "React root runtime declaration exports",
  reactDeclaration.values,
  allowlist.reactRootValues,
);
assertExact(
  "React root type declaration exports",
  reactDeclaration.types,
  allowlist.reactRootTypes,
);
assertPublicDeclarationGraph({
  ts,
  entryPath: join(reactRoot, "dist/index.d.ts"),
  declarationRoots: [
    { root: join(reactRoot, "dist"), prefix: "react" },
    { root: join(clientRoot, "dist"), prefix: "client" },
  ],
  expectedSha256: CURRENT_REACT_DECLARATION_GRAPH_SHA256,
  label: "reachable React root",
});
const reactRuntime: Readonly<Record<string, unknown>> = await import(
  `${pathToFileURL(join(reactRoot, "dist/index.js")).href}?api-check=${Date.now()}`
);
assertExact(
  "React root runtime exports",
  Object.keys(reactRuntime),
  allowlist.reactRootValues,
);

// `@calque/core` is pinned by shape but skips the client's mechanism-leak
// audit: core is the machinery that audit hides, so names such as
// `OverlayEngine` are its published vocabulary.
const CORE_ROOT_VALUES: readonly string[] = [
  "CapacityError",
  "ColumnarQueryEngine",
  "DEFAULT_QUERY_RESULT_BYTES",
  "DEFAULT_QUERY_RESULT_ROWS",
  // The client checks a plan against the decoder's own definitions before it
  // downloads the artifact.
  "DIRECT_COLUMNAR_FORMAT_VERSION",
  "MAX_OPTIMISTIC_ACTIONS",
  "MAX_PENDING_MUTATIONS",
  // The worker bounds a frame's restarts as the engine bounds a query's.
  "MAX_QUERY_RESTARTS",
  "MAX_QUERY_RESULT_BYTES",
  "MAX_QUERY_RESULT_ROWS",
  "MAX_QUERY_SPEC_BYTES",
  "OVERLAY_BUDGET_BYTES",
  "OverlayEngine",
  "OverlayError",
  "QueryError",
  "artifactBase",
  "canonicalQuerySpecification",
  // The worker decodes change frames outside this package with this same
  // microseconds-to-text conversion.
  "directColumnarTimestampText",
  "equalBytes",
  "openDirectColumnarArtifact",
  "parseDirectColumnarSchema",
  "receiveDirectColumnarArtifact",
  "validateQuerySpecification",
];
const CORE_ROOT_TYPES: readonly string[] = [
  "ArtifactCapacity",
  "ArtifactColumn",
  "ArtifactReceiver",
  "ArtifactTable",
  "CanonicalBatch",
  "CanonicalBatchResult",
  "CanonicalDelete",
  "CanonicalInsert",
  "CanonicalKeyedOperation",
  "CanonicalOperation",
  "CanonicalTransaction",
  "CanonicalTruncate",
  "CanonicalUpdate",
  "ColumnChange",
  "ColumnarQueryCapacity",
  "DirectColumnarArtifactIdentity",
  "DirectColumnarColumnKind",
  "DirectColumnarColumnSchema",
  "DirectColumnarSchema",
  "DirectColumnarTableSchema",
  "DirectColumnarWorkspace",
  "DroppedAction",
  "KeyPart",
  "OptimisticAction",
  "OptimisticChange",
  "OptimisticDelete",
  "OptimisticInsert",
  "OptimisticMutation",
  "OptimisticPatch",
  "OverlayBase",
  "OverlayCollectionSchema",
  "OverlayErrorCode",
  "OverlayOptions",
  "OverlayView",
  "PatchMode",
  "QueryAggregateOperator",
  "QueryComparisonOperator",
  "QueryCompletion",
  "QueryErrorCode",
  "QueryExecutionOptions",
  "QueryExpression",
  "QueryField",
  "QueryJoin",
  "QueryJsonValue",
  "QueryLiteral",
  "QueryOrder",
  "QueryOutputValue",
  "QueryPredicate",
  "QueryProjection",
  "QueryResult",
  "QuerySource",
  "QuerySpecification",
  "QueryTask",
  "QueryWindow",
  "QueryWindowOptions",
  "QueryWindowResult",
  "SqlValue",
  "TrustedArtifact",
  "TrustedArtifactChunks",
  "ValidatedQuerySpecification",
];
// Every declaration reachable from `dist/index.d.ts`, digested, so a changed
// field type or a widened union fails the gate.
const CORE_DECLARATION_GRAPH_SHA256 =
  "975591e5bb2b1586087af2db40406058c22d2b486523b6a63020c852737e5e52";
// Core is the bottom of the client stack and names no package above it.
const CORE_FORBIDDEN_UPWARD_PACKAGES: readonly string[] = [
  "@calque/client",
  "@calque/react",
  "@calque/worker",
];

const coreRoot = join(root, "client/packages/core");
const coreRootDeclarationPath = join(coreRoot, "dist/index.d.ts");
if (!existsSync(coreRootDeclarationPath)) {
  throw new Error(
    `@calque/core is not built, so its public API is unverified: ${coreRootDeclarationPath} is missing. Run \`pnpm --filter @calque/core build\` first.`,
  );
}
const corePackage = JSON.parse(
  readFileSync(join(coreRoot, "package.json"), "utf8"),
) as PackageManifest;
assertExact("core package export subpaths", Object.keys(corePackage.exports), [
  ".",
]);
const coreExportTargets: readonly (readonly [string, string, string])[] = [
  [".", "./dist/index.d.ts", "./dist/index.js"],
];
for (const [
  subpath,
  expectedDeclaration,
  expectedRuntime,
] of coreExportTargets) {
  const conditions = corePackage.exports[subpath];
  assertExact(
    `core package ${subpath} export conditions`,
    Object.keys(conditions ?? {}),
    ["types", "import"],
  );
  if (
    conditions?.types !== expectedDeclaration ||
    conditions?.import !== expectedRuntime
  ) {
    throw new Error(`core package ${subpath} export targets drifted`);
  }
}
const coreDeclaration = declarationExports(sourceFile(coreRootDeclarationPath));
assertExact(
  "core root runtime declaration exports",
  coreDeclaration.values,
  CORE_ROOT_VALUES,
);
assertExact(
  "core root type declaration exports",
  coreDeclaration.types,
  CORE_ROOT_TYPES,
);
const coreGraph = collectPublicDeclarationGraph({
  ts,
  entryPath: coreRootDeclarationPath,
  declarationRoots: [{ root: join(coreRoot, "dist"), prefix: "dist" }],
  label: "reachable core root",
});
if (coreGraph.sha256 !== CORE_DECLARATION_GRAPH_SHA256) {
  throw new Error(
    `reachable core root declaration graph drifted:\nactual=${coreGraph.sha256}\nexpected=${CORE_DECLARATION_GRAPH_SHA256}\nsymbols=${coreGraph.records
      .map((record) => `${record.path}:${record.symbol}`)
      .join(",")}`,
  );
}
for (const record of coreGraph.records) {
  const upward = CORE_FORBIDDEN_UPWARD_PACKAGES.filter((name) =>
    record.text.includes(name),
  );
  if (upward.length > 0) {
    throw new Error(
      `core declaration ${record.path}:${record.symbol} names a package above it: ${upward.join(", ")}`,
    );
  }
}
const coreRuntime: Readonly<Record<string, unknown>> = await import(
  `${pathToFileURL(join(coreRoot, "dist/index.js")).href}?api-check=${Date.now()}`
);
assertExact(
  "core root runtime exports",
  Object.keys(coreRuntime),
  CORE_ROOT_VALUES,
);

const publicConsumer = readFileSync(
  join(clientRoot, "tests/public-api.consumer.ts"),
  "utf8",
);
for (const requiredNegativeBoundary of [
  "GeneratedClientSchemaDescriptor",
  "void publication.planId",
  "void client.status.residentTables",
]) {
  if (!publicConsumer.includes(requiredNegativeBoundary)) {
    throw new Error(
      `public negative consumer omits ${requiredNegativeBoundary}`,
    );
  }
}
for (const requiredPositiveBoundary of [
  "createSyncClient({",
  "await client.retryMutation(",
  "client.subscribePublications(",
]) {
  if (!publicConsumer.includes(requiredPositiveBoundary)) {
    throw new Error(
      `public positive consumer omits ${requiredPositiveBoundary}`,
    );
  }
}
if (!publicConsumer.includes('from "@calque/client"')) {
  throw new Error("public consumer does not compile against the package root");
}
runPackagedConsumerTypecheck();
assertDemoPathReachesOnlyThePackageRoot();

process.stdout.write(
  [
    "Client public API runtime export allowlist: PASS",
    "Client public API declaration allowlist: PASS",
    "Client bootstrap starter exports, inline script hash, and imports nothing outside itself: PASS",
    "Client Vite plugin exports, and imports only Node, the starter and @calque/config: PASS",
    "Client public API reachable declaration graph: PASS",
    "Client React public API reachable declaration graph: PASS",
    "Core public API export map and declaration allowlist: PASS",
    "Core public API reachable declaration graph: PASS",
    "Core public API layering (no dependency on client, react or worker): PASS",
    "Client public API exact construction options: PASS",
    "Client public API publication payload shape: PASS",
    "Client public API positive and negative consumers: PASS",
    "Demo path reaches only the package root: PASS",
    "Client public API mechanism leak audit: PASS",
    "Client public API verification: PASS",
    "",
  ].join("\n"),
);

/**
 * Requires the demo dashboard and its harnesses to reach the client
 * through the package root only: what they need to render or be measured, an
 * application needs too. A generated bindings module may import
 * `createGeneratedClientSchema` from `@calque/client/generated` and nothing
 * else.
 */
function assertDemoPathReachesOnlyThePackageRoot(): void {
  const scriptsRoot = join(root, "scripts");
  const pageRoot = join(clientRoot, "tests/demo");
  const files = [
    ...collectSourceFiles(pageRoot),
    ...readdirSync(scriptsRoot)
      .filter((name) => /^demo[\w.-]*\.(?:ts|mjs)$/u.test(name))
      .map((name) => join(scriptsRoot, name))
      .sort(),
  ];
  const inventory = files.map((file) => relative(root, file));
  // An empty or truncated inventory would pass vacuously.
  for (const required of [
    "client/packages/client/tests/demo/main.ts",
    "client/packages/client/tests/demo/vite.config.mjs",
    "scripts/demo.ts",
    "scripts/demo-latency-e2e.ts",
  ]) {
    if (!inventory.includes(required)) {
      throw new Error(
        `the demo path inventory does not contain ${required}; the internal-import check would pass vacuously`,
      );
    }
  }
  assertNoRelativeReachOutOfTheDemoPath(files, pageRoot, scriptsRoot);
  const internalSpecifier = /["']@calque\/client\/internal["']/u;
  const generatedSpecifier =
    /(?:from|import)\s*\(?\s*["']@calque\/client\/generated["']/gu;
  const generatedImportClause =
    /import\s+\{([^}]*)\}\s+from\s+["']@calque\/client\/generated["']/gu;
  for (const file of files) {
    const source = readFileSync(file, "utf8");
    const named = relative(root, file);
    if (internalSpecifier.test(source)) {
      throw new Error(
        `${named} reaches @calque/client/internal: the demo renders on the public surface, so what it needs belongs on the package root`,
      );
    }
    const references = source.match(generatedSpecifier) ?? [];
    if (references.length === 0) continue;
    if (!source.startsWith("// Generated by calque-bindings. Do not edit.")) {
      throw new Error(
        `${named} imports @calque/client/generated, which only generated bindings may import`,
      );
    }
    const clauses = [...source.matchAll(generatedImportClause)];
    if (clauses.length !== references.length) {
      throw new Error(
        `${named} reaches @calque/client/generated outside a named import`,
      );
    }
    const imported = clauses.flatMap(([, clause]) =>
      (clause ?? "")
        .split(",")
        .map((name) => name.trim())
        .filter((name) => name.length > 0),
    );
    assertExact(`${named} generated imports`, imported, [
      "createGeneratedClientSchema",
    ]);
  }
}

/**
 * Refuses a relative import that leaves the importing file's own root, the
 * page directory or `scripts/`. `../../src/columnar/session.js` reaches what
 * `@calque/client/internal` reaches with no specifier the subpath check could
 * match.
 */
function assertNoRelativeReachOutOfTheDemoPath(
  files: readonly string[],
  pageRoot: string,
  scriptsRoot: string,
): void {
  const anySpecifier =
    /(?:\bfrom\s*|\bimport\s*|\brequire\s*)\(?\s*["']([^"']+)["']/gu;
  const specifiersOf = (source: string): readonly string[] =>
    [...source.matchAll(anySpecifier)].map(([, specifier]) => specifier ?? "");
  // Proves the extractor sees the page's own root import before trusting it
  // to have seen no relative one.
  const page = join(pageRoot, "main.ts");
  const pageSpecifiers = specifiersOf(readFileSync(page, "utf8"));
  if (!pageSpecifiers.includes("@calque/client")) {
    throw new Error(
      `${relative(root, page)} does not import @calque/client; the relative-import check reads no specifiers and would pass vacuously`,
    );
  }
  for (const file of files) {
    const owner = file.startsWith(`${pageRoot}/`) ? pageRoot : scriptsRoot;
    for (const specifier of specifiersOf(readFileSync(file, "utf8"))) {
      if (!specifier.startsWith(".")) continue;
      const target = resolve(dirname(file), specifier);
      if (target === owner || target.startsWith(`${owner}/`)) continue;
      throw new Error(
        `${relative(root, file)} reaches ${relative(root, target)} by relative path: the demo renders on the public surface, so what it needs belongs on the package root`,
      );
    }
  }
}

function collectSourceFiles(directory: string): readonly string[] {
  return readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) return collectSourceFiles(path);
      return /\.(?:ts|tsx|mts|js|mjs)$/u.test(entry.name) ? [path] : [];
    })
    .sort();
}

/** Registered bindings change every `SyncClient`, so each case is its own program. */
function runPackagedConsumerTypecheck(): void {
  const compiler = requireFromClient.resolve("typescript/bin/tsc");
  for (const program of [
    "tests/package-consumer/tsconfig.json",
    "tests/typed-consumer/tsconfig.json",
  ]) {
    const result = spawnSync(
      process.execPath,
      [compiler, "-p", join(clientRoot, program)],
      { cwd: clientRoot, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
    );
    if (result.status !== 0) {
      process.stderr.write(result.stdout ?? "");
      process.stderr.write(result.stderr ?? "");
      throw new Error(`packaged client consumer typecheck failed: ${program}`);
    }
  }
}

function assertNoMechanismSurface(
  label: string,
  names: readonly string[],
): void {
  const prohibited =
    /artifactmemorytrace|runtimememorysnapshot|residen|cursor|sqlite|measurement|worker|postmessage|transport|heap|lease|reservation|generation|tableid|columnid|primarykey|createsql|ddl|wire/iu;
  const leaked = names.filter((name) => prohibited.test(name));
  if (leaked.length > 0) {
    throw new Error(`${label} leaks internal mechanisms: ${leaked.join(", ")}`);
  }
}

function assertOpaqueClientSchema(source: TypeScriptApi.SourceFile): void {
  const declaration = findInterface(source, "ClientSchema");
  if (declaration.members.length !== 1) {
    throw new Error("ClientSchema must contain exactly one nominal member");
  }
  const [member] = declaration.members;
  if (
    !member ||
    !ts.isPropertySignature(member) ||
    !member.name ||
    !ts.isComputedPropertyName(member.name) ||
    !member.modifiers?.some(
      (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
    ) ||
    member.questionToken ||
    member.type?.kind !== ts.SyntaxKind.NeverKeyword
  ) {
    throw new Error(
      "ClientSchema is not an opaque readonly nominal capability",
    );
  }
}

function assertInterfaceTextExcludes(
  source: TypeScriptApi.SourceFile,
  interfaceName: string,
  prohibited: readonly string[],
): void {
  const text = findInterface(source, interfaceName).getFullText(source);
  const leaked = prohibited.filter((term) =>
    text.toLocaleLowerCase("en-US").includes(term.toLocaleLowerCase("en-US")),
  );
  if (leaked.length > 0) {
    throw new Error(
      `${interfaceName} documentation leaks: ${leaked.join(", ")}`,
    );
  }
}

function sourceFile(path: string): TypeScriptApi.SourceFile {
  return ts.createSourceFile(
    path,
    readFileSync(path, "utf8"),
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS,
  );
}

function declarationExports(source: TypeScriptApi.SourceFile): {
  values: string[];
  types: string[];
} {
  const values: string[] = [];
  const types: string[] = [];
  for (const statement of source.statements) {
    if (!ts.isExportDeclaration(statement)) {
      const exported =
        ts.canHaveModifiers(statement) &&
        ts
          .getModifiers(statement)
          ?.some(({ kind }) => kind === ts.SyntaxKind.ExportKeyword);
      if (!exported) continue;
      if (
        ts.isInterfaceDeclaration(statement) ||
        ts.isTypeAliasDeclaration(statement)
      )
        types.push(statement.name.text);
      else if (
        (ts.isFunctionDeclaration(statement) ||
          ts.isClassDeclaration(statement)) &&
        statement.name
      )
        values.push(statement.name.text);
      else if (ts.isVariableStatement(statement))
        for (const { name } of statement.declarationList.declarations)
          if (ts.isIdentifier(name)) values.push(name.text);
      continue;
    }
    const clause = statement.exportClause;
    if (!clause || !ts.isNamedExports(clause)) continue;
    for (const element of clause.elements) {
      const destination =
        statement.isTypeOnly || element.isTypeOnly ? types : values;
      destination.push(element.name.text);
    }
  }
  return { values, types };
}

function interfaceMembers(
  source: TypeScriptApi.SourceFile,
  interfaceName: string,
): string[] {
  const declaration = findInterface(source, interfaceName);
  return declaration.members.map((member) => {
    if (!member.name || !ts.isIdentifier(member.name)) {
      throw new Error(`${interfaceName} contains a non-identifier member`);
    }
    return member.name.text;
  });
}

function findInterface(
  source: TypeScriptApi.SourceFile,
  interfaceName: string,
): TypeScriptApi.InterfaceDeclaration {
  const declaration = source.statements.find(
    (statement): statement is TypeScriptApi.InterfaceDeclaration =>
      ts.isInterfaceDeclaration(statement) &&
      statement.name.text === interfaceName,
  );
  if (!declaration) throw new Error(`missing interface ${interfaceName}`);
  return declaration;
}

function assertReadonlyUnionVariantNestedTypeLiteralShape(
  source: TypeScriptApi.SourceFile,
  aliasName: string,
  state: string,
  memberName: string,
  expectedMembers: readonly string[],
): void {
  const declaration = source.statements.find(
    (statement): statement is TypeScriptApi.TypeAliasDeclaration =>
      ts.isTypeAliasDeclaration(statement) && statement.name.text === aliasName,
  );
  if (!declaration || !ts.isUnionTypeNode(declaration.type)) {
    throw new Error(`missing union type alias ${aliasName}`);
  }
  const variant = declaration.type.types.find(
    (candidate): candidate is TypeScriptApi.TypeLiteralNode =>
      ts.isTypeLiteralNode(candidate) &&
      candidate.members.some(
        (member) =>
          ts.isPropertySignature(member) &&
          member.name !== undefined &&
          ts.isIdentifier(member.name) &&
          member.name.text === "state" &&
          member.type !== undefined &&
          stringLiteralTypeValues(member.type).includes(state),
      ),
  );
  if (!variant || !ts.isTypeLiteralNode(variant)) {
    throw new Error(`${aliasName} has no ${state} variant`);
  }
  const property = variant.members.find(
    (member): member is TypeScriptApi.PropertySignature =>
      ts.isPropertySignature(member) &&
      member.name &&
      ts.isIdentifier(member.name) &&
      member.name.text === memberName,
  );
  if (!property?.type || !ts.isTypeLiteralNode(property.type)) {
    throw new Error(
      `${aliasName}.${state}.${memberName} is not a type literal`,
    );
  }
  const actualMembers = property.type.members.map((member) => {
    if (
      !ts.isPropertySignature(member) ||
      !member.name ||
      !ts.isIdentifier(member.name) ||
      member.questionToken ||
      !member.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
      )
    ) {
      throw new Error(
        `${aliasName}.${state}.${memberName} contains a non-readonly property`,
      );
    }
    return member.name.text;
  });
  assertExact(
    `${aliasName}.${state}.${memberName} members`,
    actualMembers,
    expectedMembers,
  );
}

function assertExactDiscriminatedUnionVariantShapes(
  source: TypeScriptApi.SourceFile,
  aliasName: string,
  expectedVariants: Readonly<Record<string, UnionVariantShape>>,
): void {
  const declaration = source.statements.find(
    (statement): statement is TypeScriptApi.TypeAliasDeclaration =>
      ts.isTypeAliasDeclaration(statement) && statement.name.text === aliasName,
  );
  if (!declaration || !ts.isUnionTypeNode(declaration.type)) {
    throw new Error(`missing union type alias ${aliasName}`);
  }
  const actual: Record<
    string,
    { members: string[]; optional: string[]; resultStates?: string[] }
  > = {};
  for (const variant of declaration.type.types) {
    if (!ts.isTypeLiteralNode(variant)) {
      throw new Error(`${aliasName} contains a non-literal variant`);
    }
    const members: string[] = [];
    const optional: string[] = [];
    let states: string[] | undefined;
    let resultStates: string[] | undefined;
    for (const member of variant.members) {
      if (
        !ts.isPropertySignature(member) ||
        !member.name ||
        !ts.isIdentifier(member.name) ||
        !member.modifiers?.some(
          (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
        )
      ) {
        throw new Error(`${aliasName} contains a non-readonly property`);
      }
      const name = member.name.text;
      members.push(name);
      if (member.questionToken) optional.push(name);
      if (name === "state") states = stringLiteralTypeValues(member.type);
      if (name === "result") {
        if (
          !member.type ||
          !ts.isTypeReferenceNode(member.type) ||
          !ts.isIdentifier(member.type.typeName) ||
          member.type.typeName.text !== "MutationResultFor" ||
          member.type.typeArguments?.length !== 1
        ) {
          throw new Error(
            `${aliasName}.${name} is not exactly MutationResultFor<State>`,
          );
        }
        resultStates = stringLiteralTypeValues(member.type.typeArguments[0]);
      }
      if (name === "error")
        assertNamedTypeReference(
          aliasName,
          name,
          member,
          "MutationSubmissionError",
        );
    }
    if (!states?.length) throw new Error(`${aliasName} variant has no state`);
    for (const state of states) {
      if (actual[state]) throw new Error(`${aliasName} repeats state ${state}`);
      actual[state] = {
        members: [...members].sort(),
        optional: [...optional].sort(),
        ...(resultStates === undefined ? {} : { resultStates }),
      };
    }
  }
  assertExact(
    `${aliasName} states`,
    Object.keys(actual),
    Object.keys(expectedVariants),
  );
  for (const [state, expected] of Object.entries(expectedVariants)) {
    const observed = actual[state];
    if (!observed) {
      throw new Error(`${aliasName} declares no ${state} variant`);
    }
    assertExact(
      `${aliasName}.${state} members`,
      observed.members,
      expected.members,
    );
    assertExact(
      `${aliasName}.${state} optional members`,
      observed.optional,
      expected.optional,
    );
    if (expected.resultStates !== undefined) {
      assertExact(
        `${aliasName}.${state} result states`,
        observed.resultStates ?? [],
        expected.resultStates,
      );
    } else if (observed.resultStates !== undefined) {
      throw new Error(`${aliasName}.${state} unexpectedly declares a result`);
    }
  }
}

function assertMutationSubmissionErrorShape(
  source: TypeScriptApi.SourceFile,
  expectedMembers: readonly string[],
  codes: readonly string[],
): void {
  const declaration = source.statements.find(
    (statement): statement is TypeScriptApi.ClassDeclaration =>
      ts.isClassDeclaration(statement) &&
      statement.name?.text === "MutationSubmissionError",
  );
  if (!declaration) throw new Error("missing class MutationSubmissionError");
  const heritage = declaration.heritageClauses?.find(
    (clause) => clause.token === ts.SyntaxKind.ExtendsKeyword,
  );
  if (
    heritage?.types.length !== 1 ||
    heritage.types[0]?.expression.getText(source) !== "SyncError"
  ) {
    throw new Error("MutationSubmissionError must extend SyncError exactly");
  }
  const properties = declaration.members.filter(ts.isPropertyDeclaration);
  const actualMembers = properties.map((property) => {
    if (
      !property.name ||
      !ts.isIdentifier(property.name) ||
      !property.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
      ) ||
      property.questionToken
    ) {
      throw new Error(
        "MutationSubmissionError contains a non-readonly or optional property",
      );
    }
    return property.name.text;
  });
  assertExact(
    "MutationSubmissionError members",
    actualMembers,
    expectedMembers,
  );
  const code = properties.find(
    (property) =>
      ts.isIdentifier(property.name) && property.name.text === "code",
  );
  assertExact(
    "MutationSubmissionError codes",
    code?.type ? stringLiteralTypeValues(code.type) : [],
    codes,
  );
  const constructors = declaration.members.filter(ts.isConstructorDeclaration);
  const [onlyConstructor] = constructors;
  if (constructors.length !== 1 || !onlyConstructor) {
    throw new Error("MutationSubmissionError must declare one constructor");
  }
  const parameters = onlyConstructor.parameters;
  if (
    parameters.length !== 4 ||
    parameters[0]?.name.getText(source) !== "mutationId" ||
    parameters[0]?.type?.getText(source) !== "string" ||
    parameters[1]?.name.getText(source) !== "code" ||
    parameters[2]?.name.getText(source) !== "message" ||
    parameters[2]?.type?.getText(source) !== "string" ||
    parameters[3]?.name.getText(source) !== "options" ||
    !parameters[3]?.questionToken ||
    parameters[3]?.type?.getText(source).replace(/\s+/g, " ") !==
      'ErrorOptions & { readonly recovery?: "none"; readonly conflict?: MutationConflict; }'
  ) {
    throw new Error("MutationSubmissionError constructor surface drifted");
  }
}

function stringLiteralTypeValues(
  type: TypeScriptApi.TypeNode | undefined,
): string[] {
  const values = type && ts.isUnionTypeNode(type) ? type.types : [type];
  return values.map((value) => {
    if (
      !value ||
      !ts.isLiteralTypeNode(value) ||
      !ts.isStringLiteral(value.literal)
    ) {
      throw new Error("state discriminant is not a string literal union");
    }
    return value.literal.text;
  });
}

function assertNamedTypeReference(
  aliasName: string,
  memberName: string,
  member: TypeScriptApi.PropertySignature,
  expectedTypeName: string,
): void {
  if (
    !member.type ||
    !ts.isTypeReferenceNode(member.type) ||
    !ts.isIdentifier(member.type.typeName) ||
    member.type.typeName.text !== expectedTypeName
  ) {
    throw new Error(
      `${aliasName}.${memberName} is not exactly ${expectedTypeName}`,
    );
  }
}

function assertExactUnionVariants(
  source: TypeScriptApi.SourceFile,
  aliasName: string,
  expectedVariants: Readonly<Record<string, readonly string[]>>,
): void {
  const declaration = source.statements.find(
    (statement): statement is TypeScriptApi.TypeAliasDeclaration =>
      ts.isTypeAliasDeclaration(statement) && statement.name.text === aliasName,
  );
  if (!declaration || !ts.isUnionTypeNode(declaration.type)) {
    throw new Error(`missing union type alias ${aliasName}`);
  }
  const actual: Record<string, string[]> = {};
  for (const variant of declaration.type.types) {
    if (!ts.isTypeLiteralNode(variant)) {
      throw new Error(`${aliasName} contains a non-literal variant`);
    }
    let state: string | undefined;
    const members: string[] = [];
    for (const member of variant.members) {
      if (
        !ts.isPropertySignature(member) ||
        !member.name ||
        !ts.isIdentifier(member.name) ||
        member.questionToken ||
        !member.modifiers?.some(
          (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
        )
      ) {
        throw new Error(`${aliasName} contains a non-readonly property`);
      }
      members.push(member.name.text);
      if (member.name.text === "state") {
        if (
          !member.type ||
          !ts.isLiteralTypeNode(member.type) ||
          !ts.isStringLiteral(member.type.literal)
        ) {
          throw new Error(`${aliasName} state is not a string literal`);
        }
        state = member.type.literal.text;
      }
      if (member.name.text === "error") {
        if (
          !member.type ||
          !ts.isTypeReferenceNode(member.type) ||
          !ts.isIdentifier(member.type.typeName) ||
          member.type.typeName.text !== "SyncError"
        ) {
          throw new Error(`${aliasName} error is not exactly SyncError`);
        }
      }
    }
    if (!state) throw new Error(`${aliasName} variant has no state`);
    if (actual[state]) throw new Error(`${aliasName} repeats state ${state}`);
    actual[state] = members.sort();
  }
  assertExact(
    `${aliasName} states`,
    Object.keys(actual),
    Object.keys(expectedVariants),
  );
  for (const [state, members] of Object.entries(expectedVariants)) {
    const observed = actual[state];
    if (!observed) {
      throw new Error(`${aliasName} declares no ${state} variant`);
    }
    assertExact(`${aliasName}.${state} members`, observed, members);
  }
}

function assertReadonlyInterfaceShape(
  source: TypeScriptApi.SourceFile,
  interfaceName: string,
  expectedDataMembers: readonly string[],
  expectedFunctionMembers: readonly string[],
): void {
  const declaration = findInterface(source, interfaceName);
  const dataMembers: string[] = [];
  const functionMembers: string[] = [];
  for (const member of declaration.members) {
    if (
      !ts.isPropertySignature(member) ||
      !member.name ||
      !ts.isIdentifier(member.name) ||
      !member.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
      )
    ) {
      throw new Error(
        `${interfaceName} member ${member.name?.getText(source) ?? "<unknown>"} is not a readonly property signature`,
      );
    }
    const destination =
      member.type !== undefined && ts.isFunctionTypeNode(member.type)
        ? functionMembers
        : dataMembers;
    destination.push(member.name.text);
  }
  assertExact(
    `${interfaceName} readonly data members`,
    dataMembers,
    expectedDataMembers,
  );
  assertExact(
    `${interfaceName} readonly function members`,
    functionMembers,
    expectedFunctionMembers,
  );
}

function assertExactInterfacePropertyTypes(
  source: TypeScriptApi.SourceFile,
  interfaceName: string,
  expected: Readonly<Record<string, { optional: boolean; type: string }>>,
): void {
  const declaration = findInterface(source, interfaceName);
  const actual: Record<string, { optional: boolean; type: string }> = {};
  for (const member of declaration.members) {
    if (
      !ts.isPropertySignature(member) ||
      !member.name ||
      !ts.isIdentifier(member.name) ||
      !member.type ||
      !member.modifiers?.some(
        (modifier) => modifier.kind === ts.SyntaxKind.ReadonlyKeyword,
      )
    ) {
      throw new Error(
        `${interfaceName} contains a non-readonly typed property signature`,
      );
    }
    actual[member.name.text] = {
      optional: Boolean(member.questionToken),
      type: member.type.getText(source),
    };
  }
  const actualNames = Object.keys(actual);
  const expectedNames = Object.keys(expected);
  assertExact(
    `${interfaceName} exact property names`,
    actualNames,
    expectedNames,
  );
  for (const name of expectedNames) {
    if (JSON.stringify(actual[name]) !== JSON.stringify(expected[name])) {
      throw new Error(
        `${interfaceName}.${name} type drifted:\nactual=${JSON.stringify(actual[name])}\nexpected=${JSON.stringify(expected[name])}`,
      );
    }
  }
}

function assertExactAuthCallback(
  source: TypeScriptApi.SourceFile,
  expectedReturnTypes: readonly string[],
): void {
  const declaration = findInterface(source, "SyncClientOptions");
  const property = declaration.members.find(
    (member): member is TypeScriptApi.PropertySignature =>
      ts.isPropertySignature(member) &&
      member.name &&
      ts.isIdentifier(member.name) &&
      member.name.text === "auth",
  );
  if (
    !property?.type ||
    !ts.isFunctionTypeNode(property.type) ||
    property.type.parameters.length !== 0
  ) {
    throw new Error(
      "SyncClientOptions.auth is not an exact zero-argument callback",
    );
  }
  const returnTypes = ts.isUnionTypeNode(property.type.type)
    ? property.type.type.types.map((type) => type.getText(source))
    : [property.type.type.getText(source)];
  assertExact(
    "SyncClientOptions.auth return types",
    returnTypes,
    expectedReturnTypes,
  );
}

function assertExact(
  label: string,
  actual: readonly (string | symbol)[],
  expected: readonly (string | symbol)[],
): void {
  const sortedActual = [...actual].sort();
  const sortedExpected = [...expected].sort();
  if (JSON.stringify(sortedActual) !== JSON.stringify(sortedExpected)) {
    throw new Error(
      `${label} drifted:\nactual=${JSON.stringify(sortedActual)}\nexpected=${JSON.stringify(sortedExpected)}`,
    );
  }
}
