import type { DirectColumnarTableSchema } from "@calque/core";
import type {
  ColumnarChangeTableBinding,
  ColumnarMountState,
  EngineWorkerClient,
} from "@calque/worker/columnar";
import type {
  ColumnarLivePublication,
  ColumnarLiveSession,
} from "@calque/worker/columnar-live";

import type { SyncPublication } from "../client.js";
import type { GeneratedClientCollectionDescriptor } from "../generated-schema.js";
import type { RenewablePlan } from "./plan-binding.js";

const decoder = new TextDecoder();

/** What one collection's mutations encode against, fixed at activation. */
export interface MutationTable {
  readonly descriptor: GeneratedClientCollectionDescriptor;
  readonly schema: DirectColumnarTableSchema;
  readonly binding: ColumnarChangeTableBinding;
}

/** What the mount's current plan grants; each renewal replaces it. */
export interface ActivationGrant {
  readonly planId: string;
  readonly scopeToken: string;
  /** When the plan expires, in milliseconds since the Unix epoch. */
  readonly expiresAt: number;
  /** What the next renewal presents for the tables the mount holds. */
  readonly residentStateProof: string;
}

export interface Activation {
  readonly engine: EngineWorkerClient;
  state: ColumnarMountState;
  /**
   * The mount's canonical cursor, as text; the artifact's until a batch moves
   * it.
   */
  cursor: string;
  /** Set when the change stream failed after the fence but before adoption. */
  failure: unknown;
  live: ColumnarLiveSession | undefined;
  grant: ActivationGrant;
  readonly plan: RenewablePlan;
  /** Ends the renewals of this activation's plan. */
  readonly renewal: AbortController;
  readonly tables: ReadonlyMap<string, MutationTable>;
}

export interface PublishedBatch {
  readonly activation: Activation;
  readonly applied: ColumnarLivePublication;
  readonly publication: SyncPublication;
}

/** Records one applied batch on its activation, whether adopted or not. */
export function recordPublication(
  activation: Activation,
  applied: ColumnarLivePublication,
): SyncPublication {
  activation.state = applied.state;
  if (applied.state.cursor)
    activation.cursor = decoder.decode(applied.state.cursor);
  return Object.freeze({
    cursor: activation.cursor,
    latestCommitTimeUnixMicros: applied.latestCommitTimeUnixMicros ?? null,
    changedCollections: Object.freeze([...applied.changedCollections]),
    transactionCount: applied.transactionCount,
  });
}

export async function teardownActivation(
  activation: Activation,
): Promise<void> {
  activation.renewal.abort();
  activation.live?.stop();
  activation.engine.close();
  await activation.live?.stopped();
}
