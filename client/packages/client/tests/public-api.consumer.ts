import {
  createSyncClient,
  type ClientSchema,
  type MutationResult,
  type MutationStatus,
  SyncError,
  type SyncClient,
  type SyncPublication,
} from "@calque/client";
import { schema } from "./calque.generated.js";
// @ts-expect-error the fake-worker client factory exists only under /internal.
import { createInternalColumnarClient } from "@calque/client";
// @ts-expect-error bootstrap transport parsing is internal.
import { parseBootstrapPlan } from "@calque/client";
// @ts-expect-error core's query errors reach applications as SyncError.
import { QueryError } from "@calque/client";
// @ts-expect-error core's query error codes are not application error codes.
import type { QueryErrorCode } from "@calque/client";
// @ts-expect-error generated schema descriptors exist only under /internal.
import type { GeneratedClientSchemaDescriptor } from "@calque/client";
export type ForbiddenGeneratedSchema = GeneratedClientSchemaDescriptor;

// @ts-expect-error ClientSchema is an opaque generated capability.
const forgedSchema: ClientSchema = { collections: [] };
void forgedSchema;

export function createConsumerClient(): SyncClient {
  return createSyncClient({
    endpoint: "https://sync.example.test",
    auth: () => ({ bearerToken: "example" }),
    schema,
    memoryBudgetBytes: 256 * 1024 * 1024,
    deviceId: "example-installation",
  });
}

createSyncClient({
  endpoint: "https://sync.example.test",
  // @ts-expect-error auth callbacks return an object carrying bearerToken, not a bare token.
  auth: () => "legacy-token-only-auth",
  schema,
  memoryBudgetBytes: 256 * 1024 * 1024,
  deviceId: "invalid-token-only-auth",
});

createSyncClient({
  endpoint: "https://sync.example.test",
  auth: () => ({ bearerToken: "example" }),
  // @ts-expect-error schemas are generated opaque capabilities, not inline wire descriptors.
  schema: { collections: [] },
  memoryBudgetBytes: 256 * 1024 * 1024,
  deviceId: "invalid-inline-schema",
});

createSyncClient({
  endpoint: "https://sync.example.test",
  auth: () => ({ bearerToken: "example" }),
  // @ts-expect-error ordinary applications do not construct transport workers.
  worker: () => new Worker("worker.js"),
  schema,
  memoryBudgetBytes: 256 * 1024 * 1024,
  deviceId: "invalid-worker-option",
});

/**
 * Rendering several collections at one cursor, on the public surface: the
 * batch says which collections it moved and when the source committed it, and
 * the live queries it moved are already re-reading when this runs.
 */
export function renderAtOneCursor(client: SyncClient): () => void {
  return client.subscribePublications((publication: SyncPublication) => {
    void publication.cursor;
    void publication.changedCollections;
    void publication.latestCommitTimeUnixMicros;
    void publication.transactionCount;
    // @ts-expect-error published batches are immutable.
    publication.cursor = "forged";
    // @ts-expect-error published collection lists are immutable.
    publication.changedCollections.push("late");
    // @ts-expect-error engine generations are not application state.
    void publication.collectionGenerations;
    // @ts-expect-error plan identity is not application state.
    void publication.planId;
  });
}

// @ts-expect-error publication subscription is a readonly capability.
void ((client: SyncClient) => (client.subscribePublications = () => () => {}));

export async function ordinaryOperations(client: SyncClient): Promise<void> {
  // @ts-expect-error public client operations are readonly capabilities.
  client.refresh = async () => undefined;
  // @ts-expect-error public client state is readonly.
  client.status = { state: "closed" };

  const unsubscribeStatus = client.subscribe(() => undefined);

  if (client.status.state === "ready") {
    // @ts-expect-error residency topology is not application state.
    void client.status.residentTables;
  }
  if (client.status.state === "initializing") {
    // @ts-expect-error required-table topology is not application state.
    void client.status.requiredTables;
  }
  if (client.status.state === "locked") {
    const reason: "revoked" | "expired" = client.status.reason;
    void reason;
    void client.status.error.code;
  }

  const mutationResult = await client.retryMutation("ambiguous-mutation");
  // @ts-expect-error server transaction receipts are internal idempotency data.
  void mutationResult.serverTransactionId;
  // @ts-expect-error canonical cursors are internal synchronization metadata.
  void mutationResult.observedCanonicalCursor;
  if (mutationResult.state === "conflict") {
    void mutationResult.conflict;
    // @ts-expect-error retry delay exists only on retryable outcomes.
    void mutationResult.retryAfterMs;
  } else if (mutationResult.state === "retryable") {
    void mutationResult.retryAfterMs;
    // @ts-expect-error conflict detail exists only on conflict outcomes.
    void mutationResult.conflict;
  } else {
    // @ts-expect-error conflict detail is irrelevant to this outcome.
    void mutationResult.conflict;
    // @ts-expect-error retry delay is irrelevant to this outcome.
    void mutationResult.retryAfterMs;
  }
  const mutationStatus = client.mutationStatus(mutationResult.mutationId);
  if (mutationStatus?.state === "conflict") {
    void mutationStatus.result.conflict;
    // @ts-expect-error status/result state correlation excludes retry delay.
    void mutationStatus.result.retryAfterMs;
  }
  if (mutationStatus?.state === "retryable") {
    void mutationStatus.result.retryAfterMs;
    // @ts-expect-error status/result state correlation excludes conflict data.
    void mutationStatus.result.conflict;
  }

  const unsubscribeMutation = client.subscribeMutation(
    mutationResult.mutationId,
    () => undefined,
  );
  await client.refresh();
  unsubscribeMutation();
  unsubscribeStatus();
  await client.reset();
  await client.close();
}

const invalidAcceptedMutationResult: MutationResult = {
  mutationId: "invalid-accepted-result",
  state: "awaiting_canonical_echo",
  message: "accepted",
  // @ts-expect-error accepted results cannot carry an invariant-zero retry delay.
  retryAfterMs: 0n,
};
void invalidAcceptedMutationResult;

// @ts-expect-error conflict results require conflict details.
const invalidConflictMutationResult: MutationResult = {
  mutationId: "invalid-conflict-result",
  state: "conflict",
  message: "conflict",
};
void invalidConflictMutationResult;

const rejectedResult: Extract<MutationResult, { state: "rejected" }> = {
  mutationId: "invalid-canonical-result",
  state: "rejected",
  message: "rejected",
};
const invalidCanonicalStatus: MutationStatus = {
  mutationId: rejectedResult.mutationId,
  collection: "tasks",
  key: { id: 1n },
  state: "canonical",
  // @ts-expect-error canonical status can retain only awaiting or already-canonical results.
  result: rejectedResult,
};
void invalidCanonicalStatus;

export function unregistered(client: SyncClient): void {
  // @ts-expect-error without registered bindings, `db` names no collection.
  void client.db.issues.query();
}

export async function recoverFrom(error: SyncError, client: SyncClient) {
  switch (error.code) {
    case "AUTH_FAILED":
    case "INTEGRITY_FAILED":
    case "INTERNAL_ERROR":
      await client.reset();
      return;
    case "RESNAPSHOT_REQUIRED":
      await client.reset();
      return;
    // The client has already suspended, cancelled and tried to rebuild from
    // `auth()` by the time an application sees either code: it arrives as a
    // `locked` status, and `reset()` after the application re-authorizes is
    // what is left.
    case "AUTHORIZATION_REVOKED":
    case "AUTHORIZATION_EXPIRED":
      await client.reset();
      return;
    case "NETWORK_ERROR":
    case "SERVER_ERROR":
    case "MUTATION_AMBIGUOUS":
      return;
    // The client waits this one out on the server's schedule and reports it
    // as `status.state === "preparing"`; an application that sees the code
    // has already waited longer than the client's bound.
    case "ARTIFACT_PREPARING":
      return;
    case "CANCELLED":
    case "CAPACITY_EXCEEDED":
    case "CLOSED":
    case "INVALID_QUERY":
    case "INVALID_RESPONSE":
    case "QUERY_FAILED":
    case "RESULT_TOO_LARGE":
    case "MUTATION_REJECTED":
    case "UNSUPPORTED_BROWSER":
      return;
    default:
      error.code satisfies never;
  }
}

void createInternalColumnarClient;
void parseBootstrapPlan;
void QueryError;
void (undefined as unknown as QueryErrorCode);
