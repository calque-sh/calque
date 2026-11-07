import type { SyncAuth } from "./bootstrap-start.js";
import { SyncError } from "./error.js";
import type { MutationResult, MutationStatus } from "./mutation.js";
import { publicColumnarClient } from "./columnar/public-columnar-client.js";
import { ColumnarSession } from "./columnar/session.js";
import {
  createColumnarBrowserDecoder,
  createColumnarBrowserHasher,
  createColumnarBrowserWorker,
  type ColumnarDecoderWorker,
  type ColumnarEngineWorker,
  type ColumnarHasherWorker,
} from "./columnar/worker.js";
import type { CollectionFacades, CollectionShapes } from "./facade/types.js";
import type { ClientSchema } from "./schema.js";
import {
  sealInternalSyncClientOptions,
  sealSyncClientOptions,
} from "./client-options.js";

export type { SyncAuth } from "./bootstrap-start.js";

/** What `createSyncClient` takes: `endpoint`, `auth` and `schema` are required. */
export interface SyncClientOptions<
  Collections extends CollectionShapes = never,
> {
  /**
   * Base URL of `calque serve`: `https:`, or `http:` on `localhost` or
   * `127.0.0.1`, with no credentials or fragment. A path such as `/_calque/`
   * is read against the page's URL. The client posts `v1/bootstrap` and
   * `v1/mutations` under it.
   */
  readonly endpoint: string;
  /**
   * Returns the grant the client presents. Called before each mount (at
   * creation, on `reset()` and on every rebuild) and before each mutation, so
   * return the newest grant the application holds.
   */
  readonly auth: () => SyncAuth | Promise<SyncAuth>;
  /** The `schema` the generated `calque.generated.ts` exports. */
  readonly schema: ClientSchema<Collections>;
  /**
   * The bytes the client may hold: the loaded collections plus its query,
   * catch-up and heap reserves. A working set that does not fit fails with
   * `CAPACITY_EXCEEDED` before the artifact downloads. Defaults to the
   * budget the page's starter asked for, from the `calque-plan` element
   * `renderBootstrap` writes or the `calque-bootstrap` element the Vite
   * plugin's `bootstrap` option writes, and otherwise to 1 GiB. With the
   * page's budget the client also takes the page's heap reserve.
   */
  readonly memoryBudgetBytes?: number;
  /**
   * Identifies this browser installation across reloads. Defaults to an id
   * the client keeps in `localStorage`.
   */
  readonly deviceId?: string;
  /**
   * The `fetch` every request goes through: the bootstrap, the artifact, the
   * change stream and mutations. Defaults to `globalThis.fetch`.
   */
  readonly fetch?: typeof globalThis.fetch;
  /**
   * The public half of the key the application's backend signs grants with,
   * which it also signs revocations with. With it, the client clears a
   * revoked user's rows as soon as the revocation arrives, then rebuilds
   * through `auth()`. The key comes from the application, not from the
   * server, so the server cannot forge a revocation. Without it, a revocation
   * reaches the client as an expired grant at its next change request, and
   * the old rows stay on screen until the rebuild through `auth()` replaces
   * them.
   */
  readonly grantVerificationKey?: GrantVerificationKey;
}

/**
 * An Ed25519 public key, in the shape `calque serve
 * --grant-verification-key-env` reads and `calque-dev-grant` prints as
 * `verificationKeyEnvironment`. `createSyncClient` throws on a malformed key.
 */
export interface GrantVerificationKey {
  readonly keyId: string;
  /** Raw 32-byte Ed25519 public key in unpadded base64url: 43 characters. */
  readonly publicKey: string;
}

/** The client's state, as `SyncClient.status` holds it. */
export type SyncStatus =
  /**
   * The client is building its mount from a grant `auth()` returns, a plan
   * and an artifact. It starts here and comes back here on `reset()` and on
   * every rebuild. Queries and mutations wait for `ready`, and reject if the
   * client reaches `error`, `locked` or `closed` instead.
   */
  | { readonly state: "initializing" }
  /**
   * The artifact for the current authorization generation is not published
   * yet, as after a revocation. The client retries on the server's schedule,
   * and no query resolves until it reaches `ready`.
   */
  | { readonly state: "preparing" }
  /**
   * Every collection in the plan is loaded at one source cursor, and the
   * client follows the change stream from there. Queries and mutations run.
   */
  | { readonly state: "ready" }
  /**
   * A failure the client will not rebuild from on its own. Queries and
   * mutations reject with `error`; `error.recovery` says whether `refresh()`
   * or `reset()` can help.
   */
  | { readonly state: "error"; readonly error: SyncError }
  /**
   * The application's authorization no longer admits this session, and the
   * session will not try again on its own. `revoked`: a revocation barrier
   * retired the grant's generation and the rebuild from `auth()` failed.
   * `expired`: the grant expired and the same rebuild failed. Every
   * subscription publishes `error`, queries reject and pending mutations are
   * cancelled. `reset()` is the only way out.
   */
  | {
      readonly state: "locked";
      readonly reason: "revoked" | "expired";
      readonly error: SyncError;
    }
  /** `close()` ran. Nothing leaves this state. */
  | { readonly state: "closed" };

/**
 * One published batch of committed source transactions, applied to every
 * collection it touches or to none.
 */
export interface SyncPublication {
  /**
   * An opaque id of the source state the collections now represent. Two
   * publications with the same value describe the same state; compare it only
   * for equality.
   */
  readonly cursor: string;
  /**
   * The newest PostgreSQL commit time in the batch, in microseconds since the
   * Unix epoch, or `null` when the batch carried no transaction. It is the
   * source's clock, not the browser's.
   */
  readonly latestCommitTimeUnixMicros: bigint | null;
  /**
   * The collections this batch moved, by the ids the generated schema gives
   * them. Empty when a poll answered with no source change.
   */
  readonly changedCollections: readonly string[];
  /** How many source transactions the batch carried. */
  readonly transactionCount: number;
}

/**
 * Generated bindings add `collections: CalqueCollections` here, so a
 * `SyncClient` without a type argument, and `useSyncClient()`, are typed.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type -- filled by module augmentation
export interface Register {}

/** The collections the generated bindings registered, or `never` without any. */
export type RegisteredCollections = Register extends {
  readonly collections: infer Collections extends CollectionShapes;
}
  ? Collections
  : never;

/**
 * The facades of one client's generated collections, or an empty record
 * without collection shapes.
 */
export type SyncClientCollections<Collections extends CollectionShapes> = [
  Collections,
] extends [never]
  ? Readonly<Record<string, never>>
  : CollectionFacades<Collections>;

/**
 * One client over one generated schema: its collections under `db`, its
 * status and publications, the mutations it issued, and the calls that
 * refresh, rebuild or close it.
 */
export interface SyncClient<
  Collections extends CollectionShapes = RegisteredCollections,
> {
  /**
   * The client's current state; read it again whenever a `subscribe` listener
   * runs.
   */
  readonly status: SyncStatus;
  /** Calls `listener` after every `status` change; returns the unsubscribe. */
  readonly subscribe: (listener: () => void) => () => void;
  /**
   * Runs `listener` after each batch the client publishes, once every live
   * query whose collection moved has started re-reading; returns the
   * unsubscribe. A query's `promise`, awaited inside the listener, resolves
   * once its rows hold at this batch's state or a later one.
   */
  readonly subscribePublications: (
    listener: (publication: SyncPublication) => void,
  ) => () => void;
  /**
   * The generated collection facades, one per collection in the schema, as in
   * `db.events.query().where(...).run()`.
   */
  readonly db: SyncClientCollections<Collections>;
  /**
   * The latest status of the mutation with this id, or `undefined` for an id
   * the client has not issued or has dropped. Past 512 statuses it drops the
   * oldest settled one that has no listener.
   */
  readonly mutationStatus: (mutationId: string) => MutationStatus | undefined;
  /**
   * Calls `listener` after every change to that mutation's status; returns
   * the unsubscribe. A status with a listener is never dropped.
   */
  readonly subscribeMutation: (
    mutationId: string,
    listener: () => void,
  ) => () => void;
  /**
   * Calls `listener` with every mutation's status as it changes, from the
   * optimistic apply on; returns the unsubscribe.
   */
  readonly subscribeMutations: (
    listener: (status: MutationStatus) => void,
  ) => () => void;
  /**
   * Resends an `ambiguous` or `retryable` mutation under its original id, so
   * a server that already committed it answers from its receipt. Resolves at
   * once for a mutation already `canonical`; rejects with `MUTATION_REJECTED`
   * when there is nothing to resend, as after a `reset()`.
   */
  readonly retryMutation: (mutationId: string) => Promise<MutationResult>;
  /**
   * Resolves once the client is ready; from the `error` state it runs
   * `reset()` instead. Rejects with the status's error if the client fails or
   * is `locked`, and with `CANCELLED` once `signal` has aborted.
   */
  readonly refresh: (signal?: AbortSignal) => Promise<void>;
  /**
   * Drops the current mount and builds a new one from a fresh `auth()` grant,
   * plan and artifact; the only way out of `locked`. Pending mutations become
   * `ambiguous`. Resolves when the new mount is ready or has failed.
   */
  readonly reset: () => Promise<void>;
  /**
   * Stops the client for good: live queries publish a `CLOSED` error, later
   * queries and mutations fail with it, and `status` ends at `closed`. A
   * second call does nothing.
   */
  readonly close: () => Promise<void>;
}

export interface InternalSyncClientOptions extends SyncClientOptions {
  /**
   * The part of `memoryBudgetBytes` held for the client's main-thread heap,
   * which `createSyncClient` derives from the budget.
   */
  readonly clientHeapReserveBytes: number;
}

/**
 * Creates a client for one generated schema, with its collections under `db`.
 * Throws on a schema that declares no collection.
 */
export function createSyncClient<Collections extends CollectionShapes = never>(
  options: SyncClientOptions<Collections>,
): SyncClient<Collections> {
  return publicColumnarClient<Collections>(
    new ColumnarSession(
      sealSyncClientOptions(options as SyncClientOptions),
      createColumnarBrowserWorker,
      createColumnarBrowserHasher,
      createColumnarBrowserDecoder,
    ),
  );
}

/**
 * Creates a client over caller-supplied engine, hasher and decoder workers,
 * for tests and harnesses that run fakes or run under Node.
 */
export function createInternalColumnarClient<
  Collections extends CollectionShapes = never,
>(
  options: InternalSyncClientOptions,
  workerFactory: () => ColumnarEngineWorker,
  hasherFactory: () => ColumnarHasherWorker = createColumnarBrowserHasher,
  decoderFactory: () => ColumnarDecoderWorker = createColumnarBrowserDecoder,
): SyncClient<Collections> {
  return publicColumnarClient<Collections>(
    new ColumnarSession(
      sealInternalSyncClientOptions(options),
      workerFactory,
      hasherFactory,
      decoderFactory,
    ),
  );
}
