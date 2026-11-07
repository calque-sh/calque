import { create } from "@bufbuild/protobuf";
import {
  canonicalQuerySpecification,
  equalBytes,
  type QueryExecutionOptions,
  type QueryResult,
  type QuerySpecification,
  type QueryWindow,
  type QueryWindowResult,
} from "@calque/core";
import { mutation } from "@calque/protocol";
import {
  ArtifactDecoder,
  ArtifactHashers,
  awaitedLater,
  delay,
  EngineWorkerClient,
  hasherCount,
  type FrameCompletion,
  type ColumnarMountState,
} from "@calque/worker/columnar";
import type { ColumnarLivePublication } from "@calque/worker/columnar-live";

import { isAbortSignalAborted } from "../abort-signal.js";
import type { SealedSyncClientOptions } from "../client-options.js";
import type { SyncPublication, SyncStatus } from "../client.js";
import { SyncError } from "../error.js";
import { createCollectionFacades } from "../facade/collection.js";
import {
  type CellReaders,
  type FacadePort,
  type QueryRow,
  type QueryWindowRows,
  type SealedQueryRunOptions,
  type SealedQueryWindowOptions,
} from "../facade/port.js";
import type {
  GeneratedClientCollectionDescriptor,
  GeneratedClientColumnKind,
  GeneratedClientCrudOperation,
} from "../generated-schema.js";
import {
  freezeMutationResult,
  freezeMutationStatus,
  MutationSubmissionError,
  type MutationResult,
  type MutationStatus,
} from "../mutation.js";
import { assertMutationId } from "../mutation-id.js";
import {
  assertObserver,
  ObserverRegistry,
  once,
  reportObserverError,
} from "../observer.js";
import type { LiveQueryResource } from "../resource.js";
import {
  BOOTSTRAP_PROTOCOL_VERSION,
  bootstrapUrl,
  differingBootstrapParameter,
  discardStartedBootstrap,
  type StartedBootstrap,
  takeStartedBootstrap,
} from "../bootstrap-start.js";
import {
  ArtifactPreparingError,
  ArtifactCapabilityError,
  MAX_PREPARING_WAIT_MS,
  artifactTransportProfile,
  DIRECT_COLUMNAR_ARTIFACT_MEDIA_TYPE,
  fetchArtifact,
  mutationOutcomeIsDefinitive,
  mutationOutcomeResult,
  readBootstrapPlan,
  requestBootstrapPlan,
  sealAuthSnapshot,
  submitMutationBatch,
} from "../transport.js";
import type { BootstrapPlan } from "../plan.js";
import {
  recordPublication,
  teardownActivation,
  type Activation,
  type MutationTable,
  type PublishedBatch,
} from "./activation.js";
import { afterNextPaint } from "./after-next-paint.js";
import {
  artifactBodyRefusal,
  sendArtifact,
  sendDecodedArtifact,
  type HandedChunks,
} from "./artifact-download.js";
import { BOOTSTRAP_RETRY_DELAYS_MS, PlanOutlived } from "./bootstrap-retry.js";
import { encodeCrudOperation } from "./mutation-encoding.js";
import {
  CRUD_COMMAND_PREFIX,
  CRUD_COMMAND_VERSION,
  MAX_RESUBMITTABLE_MUTATIONS,
  MAX_RETAINED_MUTATION_STATUSES,
  rowKey,
  terminal,
  type MutationRow,
} from "./mutation-tracking.js";
import { bindColumnarPlan, collectionRelations } from "./plan-binding.js";
import {
  LOADING,
  QuerySubscription,
  type SubscriptionResult,
} from "./query-subscription.js";
import {
  activationError,
  cancelled,
  optimismRefused,
  queryError,
} from "./session-errors.js";
import {
  LOCKED_ABANDON_MESSAGE,
  MAX_RESNAPSHOT_REBUILDS,
  REBUILD_ABANDON_MESSAGE,
  RESET_ABANDON_MESSAGE,
  RESNAPSHOT_REBUILD_WINDOW_MS,
  REVOKED_ABANDON_MESSAGE,
  type AuthorizationRecovery,
} from "./session-recovery.js";
import { mark } from "./user-timing.js";
import type {
  ColumnarDecoderWorker,
  ColumnarEngineWorker,
  ColumnarHasherWorker,
} from "./worker.js";

const encoder = new TextEncoder();

type PublicationListener = (publication: SyncPublication) => void;

/**
 * Every public call reads the mount of the current epoch, so a `reset()` or
 * `close()` in flight drops an older result rather than delivering it late.
 */
export class ColumnarSession {
  readonly #options: SealedSyncClientOptions;
  readonly #workerFactory: () => ColumnarEngineWorker;
  readonly #hasherFactory: () => ColumnarHasherWorker;
  readonly #decoderFactory: () => ColumnarDecoderWorker;
  readonly #collections: readonly GeneratedClientCollectionDescriptor[];
  readonly #columns = new Map<
    string,
    ReadonlyMap<string, GeneratedClientColumnKind>
  >();
  /** The session's own id, or the id of the started request it adopted. */
  #clientInstanceId: string = crypto.randomUUID();
  /** The configured device, or the device of the started request it adopted. */
  #deviceId: string;
  /**
   * The bootstrap request the page started, taken from the slot by the first
   * client constructed and considered only by its first activation.
   */
  #started: StartedBootstrap | undefined;
  readonly #listeners = new ObserverRegistry<true>();
  readonly #publicationListeners = new Set<PublicationListener>();
  readonly #subscriptions = new Map<string, QuerySubscription>();
  #nextSubscriptionKey = 0;
  /**
   * Subscriptions with a read in flight, including one nobody listens to that
   * a caller awaits.
   */
  readonly #reading = new Set<QuerySubscription>();
  /** Whether `#frames` is running, and whether it should read once more. */
  #framing = false;
  #frameAgain = false;
  /** The frame in the worker, withdrawn once none of its members is wanted. */
  #frame:
    | {
        readonly members: readonly QuerySubscription[];
        readonly controller: AbortController;
      }
    | undefined;
  /** Optimistic writes applied since the page last adopted a frame. */
  readonly #unshown = new Set<string>();
  /**
   * Writes a frame has put on the page that the browser has not painted yet,
   * and the publications held until it has, in order.
   */
  #painting:
    | { readonly mutationIds: Set<string>; readonly held: PublishedBatch[] }
    | undefined;
  readonly #mutations = new Map<string, MutationStatus>();
  readonly #mutationListeners = new Map<string, ObserverRegistry<true>>();
  readonly #everyMutationListeners = new Set<
    (status: MutationStatus) => void
  >();
  /**
   * The envelopes of mutations whose response never arrived, by identifier,
   * which `retryMutation` resends verbatim.
   */
  readonly #resubmittable = new Map<
    string,
    {
      readonly activation: Activation;
      readonly row: MutationRow;
      readonly request: mutation.MutationBatchRequest;
    }
  >();
  readonly db: Readonly<Record<string, object>>;
  /**
   * Aborted when the session moves to another epoch, which ends the load the
   * epoch has in flight and keeps it from ever activating.
   */
  #epoch = new AbortController();
  #status: SyncStatus = { state: "initializing" };
  #activation: Activation | undefined;
  #pending: Promise<void> | undefined;
  #closed = false;
  /** Explicit snapshot rebuilds started inside the current window. */
  #rebuilds = 0;
  /** When the window those rebuilds are counted against closes. */
  #rebuildWindowEndsAt: number | undefined;
  /**
   * Set while the session rebuilds because its authorization was revoked or
   * expired. A rebuild that then fails for any reason locks the session
   * rather than reporting an error an application might retry.
   */
  #recovering: AuthorizationRecovery | undefined;
  /** The last epoch that activated again for a plan a download outlived. */
  #replanned: AbortSignal | undefined;

  constructor(
    options: SealedSyncClientOptions,
    workerFactory: () => ColumnarEngineWorker,
    hasherFactory: () => ColumnarHasherWorker,
    decoderFactory: () => ColumnarDecoderWorker,
  ) {
    if (options.schema.collections.length === 0)
      throw new TypeError("the schema carries no generated collections");
    this.#options = options;
    this.#deviceId = options.deviceId;
    this.#workerFactory = workerFactory;
    this.#hasherFactory = hasherFactory;
    this.#decoderFactory = decoderFactory;
    this.#collections = options.schema.collections;
    for (const collection of this.#collections)
      this.#columns.set(
        collection.id,
        new Map(collection.columns.map(({ name, kind }) => [name, kind])),
      );
    const port: FacadePort = {
      columnsOf: (collection) => this.columnsOf(collection),
      runQuery: (specification, options) =>
        this.runQuery(specification, options),
      runWindow: (specification, window, options) =>
        this.runWindow(specification, window, options),
      subscribeQuery: (specification, dependencies, options, readers) =>
        this.subscribeQuery(specification, dependencies, options, readers),
      subscribeWindow: (
        specification,
        dependencies,
        window,
        options,
        readers,
      ) =>
        this.subscribeWindow(
          specification,
          dependencies,
          window,
          options,
          readers,
        ),
      slice: (collection, start, count, columns, signal) =>
        this.slice(collection, start, count, columns, signal),
      mutate: (collection, operation, key, values) =>
        this.mutateCollection(collection, operation, key, values),
    };
    this.db = createCollectionFacades(port, this.#collections);
    this.#started = takeStartedBootstrap();
    this.#start(this.#epoch.signal);
  }

  get status(): SyncStatus {
    return this.#status;
  }

  subscribe(listener: () => void): () => void {
    assertObserver(listener);
    this.#listeners.set(listener, true);
    return once(() => {
      this.#listeners.delete(listener);
    });
  }

  /** Runs `listener` after each publication the session adopts. */
  subscribePublications(listener: PublicationListener): () => void {
    assertObserver(listener);
    this.#publicationListeners.add(listener);
    return once(() => {
      this.#publicationListeners.delete(listener);
    });
  }

  mutationStatus(mutationId: string): MutationStatus | undefined {
    assertMutationId(mutationId);
    return this.#mutations.get(mutationId);
  }

  subscribeMutation(mutationId: string, listener: () => void): () => void {
    assertMutationId(mutationId);
    assertObserver(listener);
    let registry = this.#mutationListeners.get(mutationId);
    if (!registry) {
      registry = new ObserverRegistry<true>();
      this.#mutationListeners.set(mutationId, registry);
    }
    registry.set(listener, true);
    return once(() => {
      registry.delete(listener);
      if (registry.size === 0) this.#mutationListeners.delete(mutationId);
    });
  }

  subscribeMutations(listener: (status: MutationStatus) => void): () => void {
    assertObserver(listener);
    this.#everyMutationListeners.add(listener);
    return once(() => {
      this.#everyMutationListeners.delete(listener);
    });
  }

  /**
   * Resends a mutation the server never decided, after a lost response or a
   * `RETRYABLE_FAILURE`, as the identical envelope under its original id, so a
   * server that committed it answers from its receipt instead of executing it
   * again. A mutation the echo already confirmed returns that result.
   */
  async retryMutation(mutationId: string): Promise<MutationResult> {
    assertMutationId(mutationId);
    const status = this.#mutations.get(mutationId);
    if (status?.state === "canonical") return status.result;
    const resubmittable = this.#resubmittable.get(mutationId);
    if (!resubmittable)
      throw new MutationSubmissionError(
        mutationId,
        "MUTATION_REJECTED",
        `mutation ${mutationId} has no submission awaiting a retry`,
      );
    const activation = await this.#ready(undefined);
    if (resubmittable.activation !== activation)
      // The mount that held the delta is gone. Resubmitting against a new
      // activation would carry a base cursor and a scope token from an epoch
      // that no longer exists; the fresh mount's rows are the answer instead.
      throw new MutationSubmissionError(
        mutationId,
        "MUTATION_AMBIGUOUS",
        `mutation ${mutationId} was submitted before the current activation`,
        { recovery: "none" },
      );
    const auth = sealAuthSnapshot(await this.#options.auth());
    // Recovery `none`: the teardown that replaced the mount cleared the
    // envelope, so a second retry would answer MUTATION_REJECTED.
    if (this.#activation !== activation) throw cancelled("mutation", "none");
    // The echo can confirm the mutation while this waits for the mount and
    // the credentials. Resending then would put the status back to awaiting
    // an echo that has already been applied, and nothing would move it again.
    const confirmed = this.#mutations.get(mutationId);
    if (confirmed?.state === "canonical") return confirmed.result;
    this.#setMutationStatus({
      mutationId,
      ...resubmittable.row,
      state: "submitting",
    });
    return this.#submitMutation(
      activation,
      mutationId,
      resubmittable.row,
      resubmittable.request,
      auth.bearerToken,
    );
  }

  async refresh(signal?: AbortSignal): Promise<void> {
    this.#assertOpen();
    if (this.#status.state === "error") {
      await this.reset();
      return;
    }
    await this.#ready(signal);
  }

  async reset(): Promise<void> {
    this.#assertOpen();
    // The application asked for a fresh start; a lock, or a rebuild that
    // would end in one, is over.
    this.#recovering = undefined;
    await this.#restart(RESET_ABANDON_MESSAGE);
  }

  /**
   * Tears the current activation down and activates a fresh one behind the
   * same facades. The old engine closes before the new one opens, so no query
   * can compose rows from two epochs.
   */
  async #restart(abandonMessage: string, revoked = false): Promise<void> {
    const epoch = this.#nextEpoch();
    const previous = this.#activation;
    // Suspended before anything can yield: from here facade calls park on the
    // gate and `#execute` drops rows a read already in the worker returns.
    this.#activation = undefined;
    // Callers parked in #ready wait on this gate while the old change stream
    // is torn down. Leaving #pending on the settled previous attempt would have
    // them re-poll it on every microtask, starving the macrotask on which the
    // aborted change request reports back, so the teardown could never finish.
    const gate: PromiseWithResolvers<void> = Promise.withResolvers();
    this.#pending = gate.promise;
    this.#setStatus({ state: "initializing" });
    // Synchronous and on the suspend's turn, so nothing reads between them.
    this.#abandonPendingMutations(abandonMessage);
    if (revoked) this.#discardResidentQueryState();
    try {
      if (previous) await teardownActivation(previous);
      if (epoch.aborted) return;
      // Activates from a fresh `auth()` grant, not the one a barrier retired.
      this.#start(epoch);
    } finally {
      gate.resolve();
    }
    await this.#pending;
  }

  /** Ends the current epoch and returns the signal of the next one. */
  #nextEpoch(): AbortSignal {
    this.#epoch.abort(cancelled("sync"));
    this.#epoch = new AbortController();
    return this.#epoch.signal;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#nextEpoch();
    const previous = this.#activation;
    this.#activation = undefined;
    this.#painted();
    const closedError = new SyncError(
      "CLOSED",
      "client",
      "none",
      "the client is closed",
    );
    for (const subscription of this.#subscriptions.values())
      subscription.publish(
        Object.freeze({ status: "error", error: closedError }),
      );
    this.#subscriptions.clear();
    if (previous) await teardownActivation(previous);
    this.#setStatus({ state: "closed" });
  }

  columnsOf(
    collection: string,
  ): ReadonlyMap<string, GeneratedClientColumnKind> | undefined {
    return this.#columns.get(collection);
  }

  async runQuery(
    specification: QuerySpecification,
    options: SealedQueryRunOptions,
  ): Promise<QueryResult> {
    const activation = await this.#ready(options.signal);
    const executionOptions: QueryExecutionOptions = {
      ...(options.maxRows === undefined ? {} : { maxRows: options.maxRows }),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    };
    return (
      await this.#execute(activation, (engine) =>
        engine.query(specification, executionOptions, options.signal),
      )
    ).result;
  }

  async runWindow(
    specification: QuerySpecification,
    window: QueryWindow,
    options: SealedQueryWindowOptions,
  ): Promise<QueryWindowResult> {
    const activation = await this.#ready(options.signal);
    return (
      await this.#execute(activation, (engine) =>
        engine.window(
          specification,
          window,
          options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes },
          options.signal,
        ),
      )
    ).result;
  }

  subscribeQuery(
    specification: QuerySpecification,
    dependencies: readonly string[],
    options: QueryExecutionOptions,
    readers: CellReaders,
  ): LiveQueryResource<readonly QueryRow[]> {
    return this.#subscribe<readonly QueryRow[]>(
      specification,
      dependencies,
      options,
      undefined,
      readers,
    );
  }

  subscribeWindow(
    specification: QuerySpecification,
    dependencies: readonly string[],
    window: QueryWindow,
    options: QueryExecutionOptions,
    readers: CellReaders,
  ): LiveQueryResource<QueryWindowRows> {
    return this.#subscribe<QueryWindowRows>(
      specification,
      dependencies,
      options,
      window,
      readers,
    );
  }

  #subscribe<Result extends SubscriptionResult>(
    specification: QuerySpecification,
    dependencies: readonly string[],
    options: QueryExecutionOptions,
    window: QueryWindow | undefined,
    readers: CellReaders,
  ): LiveQueryResource<Result> {
    this.#assertOpen();
    // Each subscription keeps its canonical text, so subscribing again
    // canonicalizes one specification, not every live one. An invalid
    // specification throws here, before any rows.
    const canonical = canonicalQuerySpecification(specification);
    for (const candidate of this.#subscriptions.values())
      if (
        candidate.options.maxRows === options.maxRows &&
        candidate.options.maxBytes === options.maxBytes &&
        candidate.window?.offset === window?.offset &&
        candidate.window?.count === window?.count &&
        candidate.canonical === canonical
      )
        return candidate as unknown as LiveQueryResource<Result>;
    const key = `subscription-${this.#nextSubscriptionKey++}`;
    const subscription = new QuerySubscription<Result>(
      this,
      key,
      specification,
      canonical,
      dependencies,
      options,
      window,
      readers,
    );
    this.#subscriptions.set(key, subscription);
    return subscription;
  }

  async slice(
    collection: string,
    start: number,
    count: number,
    columns: readonly string[],
    signal: AbortSignal | undefined,
  ): Promise<readonly (readonly unknown[])[]> {
    const activation = await this.#ready(signal);
    const total = activation.state.collectionRows[collection] ?? 0;
    const available = Math.max(0, Math.min(count, total - start));
    if (available === 0) return Object.freeze([]);
    return (
      await this.#execute(activation, (engine) =>
        engine.rows(collection, start, available, columns, signal),
      )
    ).rows;
  }

  /**
   * Applies one facade mutation to the overlay, nudges the change stream so
   * the poll in flight restarts carrying the new pending id, then submits it.
   * Only the canonical echo confirms it.
   */
  async mutateCollection(
    collection: string,
    operation: GeneratedClientCrudOperation,
    key: Readonly<Record<string, unknown>>,
    values: Readonly<Record<string, unknown>> | undefined,
  ): Promise<MutationResult> {
    const activation = await this.#ready(undefined);
    const table = activation.tables.get(collection);
    if (!table)
      throw new SyncError(
        "MUTATION_REJECTED",
        "mutation",
        "none",
        `${operation} on ${collection}: the collection is not served by this session`,
      );
    const commandName = `${CRUD_COMMAND_PREFIX}${table.binding.tableId}`;
    const encoded = encodeCrudOperation({
      descriptor: table.descriptor,
      schema: table.schema,
      binding: table.binding,
      operation,
      key,
      values,
    });
    const auth = sealAuthSnapshot(await this.#options.auth());
    if (this.#activation !== activation) throw cancelled("mutation");
    const baseCursor = activation.state.cursor;
    if (!baseCursor)
      throw new SyncError(
        "MUTATION_REJECTED",
        "mutation",
        "none",
        "the mount has no cursor to base a mutation on",
      );
    const mutationId = crypto.randomUUID();
    const createdAtUnixMicros = BigInt(Date.now()) * 1_000n;
    let applied: { readonly state: ColumnarMountState };
    try {
      applied = await activation.engine.mutate(mutationId, [encoded.bytes]);
    } catch (error) {
      if (this.#activation !== activation) throw cancelled("mutation");
      throw optimismRefused(error);
    }
    if (this.#activation !== activation) throw cancelled("mutation");
    activation.state = applied.state;
    this.#unshown.add(mutationId);
    const row = { collection, key: rowKey(table.descriptor, key) };
    this.#setMutationStatus({ mutationId, ...row, state: "submitting" });
    this.#rerun(activation, false);
    activation.live?.nudge();

    const request = create(mutation.MutationBatchRequestSchema, {
      protocolVersion: 1,
      clientInstanceId: this.#clientInstanceId,
      deviceId: this.#deviceId,
      authorizationScopeToken: encoder.encode(activation.scopeToken),
      mutations: [
        create(mutation.MutationEnvelopeSchema, {
          mutationId,
          commandName,
          commandVersion: CRUD_COMMAND_VERSION,
          canonicalInput: encoded.bytes,
          baseCursor: create(mutation.CursorSchema, { token: baseCursor }),
          createdAtUnixMicros,
          expectedVersions: [],
          declaredFootprint: create(mutation.MutationFootprintSchema, {
            rows: [
              create(mutation.AffectedRowSchema, {
                tableId: encoded.tableId,
                primaryKey: encoded.primaryKey,
              }),
            ],
            tableIds: [encoded.tableId],
          }),
        }),
      ],
    });
    return this.#submitMutation(
      activation,
      mutationId,
      row,
      request,
      auth.bearerToken,
    );
  }

  /**
   * POSTs one mutation envelope and reconciles the answer, for the first
   * submission and for `retryMutation` alike.
   */
  async #submitMutation(
    activation: Activation,
    mutationId: string,
    row: MutationRow,
    request: mutation.MutationBatchRequest,
    bearerToken: string,
  ): Promise<MutationResult> {
    let outcome: mutation.MutationOutcome;
    let result: MutationResult;
    try {
      outcome = await submitMutationBatch({
        fetch: this.#options.fetch,
        endpoint: this.#options.endpointUrl,
        bearerToken,
        request,
      });
      // Mapped inside the try: an outcome the mapping cannot read is a
      // response the client could not read, so it takes the ambiguous branch
      // below, which keeps the envelope for `retryMutation`.
      result = mutationOutcomeResult(outcome);
    } catch (error) {
      const failure =
        error instanceof MutationSubmissionError
          ? error
          : new MutationSubmissionError(
              mutationId,
              "MUTATION_AMBIGUOUS",
              "mutation submission failed; the mutation may have committed",
              { cause: error },
            );
      if (failure.code === "MUTATION_REJECTED") {
        // A definitive refusal of the batch itself: nothing was committed.
        this.#resubmittable.delete(mutationId);
        await this.#dropMutation(activation, mutationId);
        this.#setMutationStatus({
          mutationId,
          ...row,
          state: "rejected",
          result: freezeMutationResult({
            mutationId,
            state: "rejected",
            message: failure.message,
          }),
        });
      } else if (this.#activation !== activation) {
        // Tearing the mount down already told the application why the
        // mutation is over, so a late answer neither reopens it for retry nor
        // replaces that reason.
        if (failure.code === "MUTATION_AMBIGUOUS")
          throw new MutationSubmissionError(
            mutationId,
            "MUTATION_AMBIGUOUS",
            "the mount this mutation was submitted on was torn down before its submission settled; the mutation may have committed",
            { cause: failure, recovery: "none" },
          );
      } else {
        // Ambiguous, or the credentials failed: the delta stays so an echo
        // can still confirm it, and the envelope stays for a resend under the
        // same id.
        this.#retain(activation, mutationId, row, request);
        this.#setMutationStatus({
          mutationId,
          ...row,
          state: "ambiguous",
          error: failure,
        });
        if (failure.code === "AUTH_FAILED") this.#fail(failure);
      }
      throw failure;
    }
    // A retryable outcome settles nothing, so its envelope survives for a
    // resend; every other outcome leaves nothing to resend.
    if (result.state !== "retryable") this.#resubmittable.delete(mutationId);
    if (this.#activation !== activation) return result;
    if (
      result.state === "awaiting_canonical_echo" ||
      result.state === "already_canonical"
    ) {
      // Committed: the delta stays until the echo applies the canonical row
      // and drops it in the same step. `already_canonical` answers a resend,
      // which races the echo of the attempt it repeats: dropping here would
      // hide the row until that echo, or fail the session if the echo had
      // already dropped it. An echo that arrived first made the status
      // canonical.
      if (this.#mutations.get(mutationId)?.state === "submitting")
        this.#setMutationStatus({
          mutationId,
          ...row,
          state: "awaiting_canonical_echo",
          result,
        });
    } else if (mutationOutcomeIsDefinitive(outcome.code)) {
      await this.#dropMutation(activation, mutationId);
      if (result.state === "conflict")
        this.#setMutationStatus({
          mutationId,
          ...row,
          state: "conflict",
          result,
        });
      else if (result.state === "rejected")
        this.#setMutationStatus({
          mutationId,
          ...row,
          state: "rejected",
          result,
        });
    } else if (result.state === "retryable") {
      // A first submission answered RETRYABLE_FAILURE never passed the
      // ambiguous branch, which retains the others.
      this.#retain(activation, mutationId, row, request);
      this.#setMutationStatus({
        mutationId,
        ...row,
        state: "retryable",
        result,
      });
    }
    return result;
  }

  /**
   * Retains one envelope for a later `retryMutation`, dropping the oldest past
   * the bound. A dropped envelope costs the retry, not correctness: the echo
   * still confirms a mutation that committed.
   */
  #retain(
    activation: Activation,
    mutationId: string,
    row: MutationRow,
    request: mutation.MutationBatchRequest,
  ): void {
    if (
      !this.#resubmittable.has(mutationId) &&
      this.#resubmittable.size >= MAX_RESUBMITTABLE_MUTATIONS
    ) {
      const oldest = this.#resubmittable.keys().next();
      if (!oldest.done) this.#resubmittable.delete(oldest.value);
    }
    this.#resubmittable.set(mutationId, { activation, row, request });
  }

  async #dropMutation(
    activation: Activation,
    mutationId: string,
  ): Promise<void> {
    this.#unshown.delete(mutationId);
    if (this.#activation !== activation) return;
    try {
      const dropped = await activation.engine.drop(mutationId);
      if (this.#activation !== activation) return;
      activation.state = dropped.state;
      this.#rerun(activation, false);
    } catch (error) {
      if (this.#activation !== activation) return;
      const reported = this.#rebuildOrReport(activationError(error));
      if (reported) this.#fail(reported);
    }
  }

  #setMutationStatus(status: MutationStatus): void {
    if (!this.#mutations.has(status.mutationId))
      this.#forgetOldestTerminalStatus();
    const frozen = freezeMutationStatus(status);
    this.#mutations.set(status.mutationId, frozen);
    this.#mutationListeners.get(status.mutationId)?.notify();
    for (const listener of [...this.#everyMutationListeners])
      try {
        listener(frozen);
      } catch (error) {
        reportObserverError(error);
      }
  }

  #forgetOldestTerminalStatus(): void {
    if (this.#mutations.size < MAX_RETAINED_MUTATION_STATUSES) return;
    for (const [mutationId, status] of this.#mutations) {
      if (!terminal(status) || this.#mutationListeners.has(mutationId))
        continue;
      this.#mutations.delete(mutationId);
      return;
    }
  }

  /** The echo named this mutation: canonical, whatever the POST said. */
  #confirmMutation(mutationId: string): void {
    this.#resubmittable.delete(mutationId);
    this.#unshown.delete(mutationId);
    const status = this.#mutations.get(mutationId);
    if (!status || status.state === "canonical") return;
    const result =
      status.state === "awaiting_canonical_echo"
        ? status.result
        : freezeMutationResult({
            mutationId,
            state: "already_canonical" as const,
            message: "mutation was observed in the canonical change stream",
          });
    this.#setMutationStatus({
      mutationId,
      collection: status.collection,
      key: status.key,
      state: "canonical",
      result,
    });
  }

  /**
   * The mount that held these deltas is gone; whether the server committed
   * them is unknown until a fresh activation shows the rows.
   */
  #abandonPendingMutations(message: string): void {
    // A held echo has confirmed its mutations already.
    this.#painted();
    this.#unshown.clear();
    // Every retained envelope names the activation being torn down, so none
    // may be resent.
    this.#resubmittable.clear();
    for (const [mutationId, status] of this.#mutations) {
      if (terminal(status)) continue;
      this.#setMutationStatus({
        mutationId,
        collection: status.collection,
        key: status.key,
        state: "ambiguous",
        // Recovery `none`: the envelope went with the map cleared above, so
        // `retryMutation` would answer MUTATION_REJECTED.
        error: new MutationSubmissionError(
          mutationId,
          "MUTATION_AMBIGUOUS",
          message,
          { recovery: "none" },
        ),
      });
    }
  }

  /** Reads `subscription` in the next frame, after the one in flight. */
  frame(subscription: QuerySubscription): void {
    this.#reading.add(subscription);
    this.#frameAgain = true;
    if (this.#framing) return;
    this.#framing = true;
    void this.#frames();
  }

  /**
   * Reads every stale subscription somebody wants in one frame, which the
   * worker answers at one state, and publishes them all on one turn: no
   * frame of the page holds two source states. Frames repeat until no read
   * is left behind the newest published state.
   */
  async #frames(): Promise<void> {
    try {
      while (this.#frameAgain) {
        this.#frameAgain = false;
        await this.#readFrame();
      }
    } finally {
      this.#framing = false;
    }
  }

  async #readFrame(): Promise<void> {
    let activation: Activation;
    try {
      activation = await this.#ready(undefined);
    } catch (error) {
      const reported = queryError(error);
      for (const subscription of this.#reading) subscription.fail(reported);
      this.#reading.clear();
      return;
    }
    const members = [...this.#subscriptions.values()].filter(
      (subscription) => subscription.listenerCount > 0,
    );
    for (const subscription of this.#reading)
      if (!subscription.wanted) {
        subscription.drop();
        this.#reading.delete(subscription);
      } else if (!members.includes(subscription)) members.push(subscription);
    const published = activation.state.collectionGenerations;
    const stale = new Set(
      members.filter((subscription) => subscription.isStale(published)),
    );
    if (stale.size === 0) {
      this.#settleReads(published);
      return;
    }
    const controller = new AbortController();
    this.#frame = { members, controller };
    let answer: FrameCompletion;
    try {
      answer = await this.#execute(activation, (engine) =>
        engine.frame(
          members.map((subscription) =>
            subscription.member(stale.has(subscription)),
          ),
          controller.signal,
        ),
      );
    } catch (error) {
      if (controller.signal.aborted || this.#activation !== activation) {
        this.#frameAgain = true;
        return;
      }
      const reported = queryError(error);
      for (const subscription of stale) {
        subscription.fail(reported);
        this.#reading.delete(subscription);
      }
      this.#settleReads(activation.state.collectionGenerations);
      return;
    } finally {
      this.#frame = undefined;
    }
    const changed: QuerySubscription[] = [];
    members.forEach((subscription, index) => {
      if (
        subscription.wanted &&
        subscription.adopt(answer.generations, answer.results[index] ?? null)
      )
        changed.push(subscription);
    });
    if (changed.length > 0) this.#holdForPaint();
    this.#unshown.clear();
    this.#settleReads(activation.state.collectionGenerations);
    for (const subscription of changed) subscription.notify();
  }

  /**
   * Holds the publications that confirm the writes this frame puts on the
   * page until the browser has painted it, so the page shows each write
   * before it shows the write confirmed.
   */
  #holdForPaint(): void {
    if (this.#unshown.size === 0) return;
    if (this.#painting) {
      for (const mutationId of this.#unshown)
        this.#painting.mutationIds.add(mutationId);
      return;
    }
    const painting = { mutationIds: new Set(this.#unshown), held: [] };
    if (
      afterNextPaint(() => {
        if (this.#painting === painting) this.#painted();
      })
    )
      this.#painting = painting;
  }

  /** Adopts the publications held for a paint, in order. */
  #painted(): void {
    const held = this.#painting?.held ?? [];
    this.#painting = undefined;
    for (const publication of held)
      if (this.#activation === publication.activation)
        this.#adoptPublication(publication);
      else
        for (const mutationId of publication.applied.acknowledgedMutationIds)
          this.#confirmMutation(mutationId);
  }

  /**
   * Settles every read the newest state has not moved past, drops the ones
   * nobody wants, and asks for another frame for the rest.
   */
  #settleReads(published: Readonly<Record<string, number>>): void {
    for (const subscription of this.#reading)
      if (!subscription.wanted) {
        subscription.drop();
        this.#reading.delete(subscription);
      } else if (subscription.settle(published))
        this.#reading.delete(subscription);
    if (this.#reading.size > 0) this.#frameAgain = true;
  }

  release(subscription: QuerySubscription): void {
    if (this.#subscriptions.get(subscription.key) === subscription)
      this.#subscriptions.delete(subscription.key);
    // Nobody is left to read the frame's rows, so the worker stops computing
    // them and gives back their share of the query pool.
    const frame = this.#frame;
    if (frame?.members.every((member) => !member.wanted))
      frame.controller.abort();
  }

  /**
   * Puts a released subscription back among the ones publications re-run,
   * and says whether its rows are older than the state last published.
   */
  retain(subscription: QuerySubscription): boolean {
    if (this.#closed) return false;
    this.#subscriptions.set(subscription.key, subscription);
    const activation = this.#activation;
    return (
      activation !== undefined &&
      subscription.isStale(activation.state.collectionGenerations)
    );
  }

  async #execute<Answer>(
    activation: Activation,
    run: (engine: EngineWorkerClient) => Promise<Answer>,
  ): Promise<Answer> {
    let answer: Answer;
    try {
      answer = await run(activation.engine);
    } catch (error) {
      if (this.#activation !== activation) throw cancelled("query");
      throw queryError(error);
    }
    if (this.#activation !== activation) throw cancelled("query");
    return answer;
  }

  #assertOpen(): void {
    if (this.#closed)
      throw new SyncError("CLOSED", "client", "none", "the client is closed");
  }

  async #ready(signal: AbortSignal | undefined): Promise<Activation> {
    while (true) {
      this.#assertOpen();
      if (signal && isAbortSignalAborted(signal))
        throw new SyncError("CANCELLED", "query", "none", "query cancelled");
      const activation = this.#activation;
      if (activation) return activation;
      const status = this.#status;
      if (status.state === "error" || status.state === "locked")
        throw status.error;
      const pending = this.#pending;
      if (!pending)
        throw new SyncError(
          "INTERNAL_ERROR",
          "client",
          "reset",
          "the session has no activation in flight",
        );
      await pending;
    }
  }

  #setStatus(status: SyncStatus): void {
    this.#status = status;
    this.#listeners.notify();
  }

  #start(epoch: AbortSignal): void {
    const attempt = this.#activate(epoch).then(
      (activation) => {
        if (epoch.aborted) {
          void teardownActivation(activation);
          return;
        }
        if (activation.failure !== undefined) {
          // The change stream failed on the fence's own turn, before this
          // handler ran.
          void teardownActivation(activation);
          const reported = this.#rebuildOrReport(
            activationError(activation.failure),
          );
          if (reported) this.#settle(reported);
          return;
        }
        // An authorized plan is live again: whatever revoked or expired the
        // previous one is behind it.
        this.#recovering = undefined;
        this.#activation = activation;
        this.#rerun(activation, true);
        mark("ready");
        this.#setStatus({ state: "ready" });
      },
      (error: unknown) => {
        if (epoch.aborted) return;
        if (error instanceof PlanOutlived && this.#replanned !== epoch) {
          // A fresh grant and plan, and so a fresh capability: a plan
          // expires no later than the grant it was issued under.
          this.#replanned = epoch;
          mark("artifact:replan");
          this.#start(epoch);
          // This attempt settles with the new one, so a reset() or rebuild
          // awaiting it waits for the activation that replaced it.
          return this.#pending;
        }
        const reported = this.#rebuildOrReport(
          activationError(error instanceof PlanOutlived ? error.error : error),
        );
        if (reported) this.#settle(reported);
      },
    );
    this.#pending = attempt;
  }

  /**
   * Starts a rebuild for `RESNAPSHOT_REQUIRED`, `AUTHORIZATION_REVOKED` or
   * `AUTHORIZATION_EXPIRED` and returns `undefined`. Any other error, or one of
   * those once the window's rebuild budget is spent, is returned for the
   * caller to report, a spent budget with recovery `reset`. A revocation discards the revoked scope's rows before
   * rebuilding; an expiry keeps painting them until the rebuilt mount replaces
   * them, since they were authorized when they arrived. A rebuild after either
   * authorization code that then fails locks the session.
   */
  #rebuildOrReport(error: SyncError): SyncError | undefined {
    const revoked = error.code === "AUTHORIZATION_REVOKED";
    const expired = error.code === "AUTHORIZATION_EXPIRED";
    if (!revoked && !expired && error.code !== "RESNAPSHOT_REQUIRED")
      return error;
    const now = Date.now();
    if (
      this.#rebuildWindowEndsAt === undefined ||
      now >= this.#rebuildWindowEndsAt
    ) {
      this.#rebuildWindowEndsAt = now + RESNAPSHOT_REBUILD_WINDOW_MS;
      this.#rebuilds = 0;
    }
    if (this.#rebuilds >= MAX_RESNAPSHOT_REBUILDS)
      return new SyncError(
        error.code,
        revoked || expired ? "auth" : "sync",
        "reset",
        `the ${revoked ? "revocation" : expired ? "expiry" : "snapshot"} rebuild did not reach a live cursor within ${MAX_RESNAPSHOT_REBUILDS} attempts in ${RESNAPSHOT_REBUILD_WINDOW_MS} ms; the client is not synchronized (${error.message})`,
        { cause: error },
      );
    this.#rebuilds += 1;
    if (revoked) this.#recovering = "revoked";
    else if (expired) this.#recovering ??= "expired";
    void this.#restart(
      revoked ? REVOKED_ABANDON_MESSAGE : REBUILD_ABANDON_MESSAGE,
      revoked,
    );
    return undefined;
  }

  /**
   * Publishes `loading` to every subscription, so the synchronous
   * `getSnapshot()` stops returning the revoked scope's rows on the turn the
   * barrier arrived. Rows already returned to application code cannot be
   * retracted.
   */
  #discardResidentQueryState(): void {
    for (const subscription of this.#subscriptions.values())
      subscription.publish(LOADING);
  }

  /**
   * Reports a failure no rebuild will follow: as a lock when the session
   * lost its authorization, and as an error otherwise.
   */
  #settle(error: SyncError): void {
    const reason = this.#lockReason(error);
    if (reason) this.#lock(reason, error);
    else this.#setStatus({ state: "error", error });
  }

  #lockReason(error: SyncError): AuthorizationRecovery | undefined {
    if (this.#recovering) return this.#recovering;
    if (error.code === "AUTHORIZATION_REVOKED") return "revoked";
    if (error.code === "AUTHORIZATION_EXPIRED") return "expired";
    return undefined;
  }

  /**
   * The session's authorization is gone and `auth()` could not replace it:
   * every subscription publishes the lock's error in place of its rows, every
   * facade call rejects with it, and only `reset()` leaves. A tab left open
   * past its grant stops showing the grant's rows.
   */
  #lock(reason: AuthorizationRecovery, cause: SyncError): void {
    const activation = this.#activation;
    this.#recovering = undefined;
    this.#nextEpoch();
    this.#activation = undefined;
    this.#pending = undefined;
    const error = new SyncError(
      reason === "revoked" ? "AUTHORIZATION_REVOKED" : "AUTHORIZATION_EXPIRED",
      "auth",
      "reset",
      `${reason === "revoked" ? "this session's authorization generation was revoked" : "this session's grant expired"} and auth() did not yield a grant the server accepts; the session is locked until reset() (${cause.message})`,
      { cause },
    );
    this.#abandonPendingMutations(LOCKED_ABANDON_MESSAGE);
    for (const subscription of this.#subscriptions.values())
      subscription.publish(Object.freeze({ status: "error", error }));
    this.#setStatus({ state: "locked", reason, error });
    if (activation) void teardownActivation(activation);
  }

  #fail(error: SyncError): void {
    if (this.#lockReason(error)) {
      this.#settle(error);
      return;
    }
    const activation = this.#activation;
    this.#nextEpoch();
    this.#activation = undefined;
    this.#setStatus({ state: "error", error });
    this.#abandonPendingMutations(
      `the session failed before the canonical echo (${error.code}); the mutation may have committed`,
    );
    if (activation) void teardownActivation(activation);
  }

  #rerun(activation: Activation, everything: boolean): void {
    if (everything)
      for (const subscription of [
        ...this.#subscriptions.values(),
        ...this.#reading,
      ])
        subscription.forget();
    for (const subscription of this.#subscriptions.values())
      if (
        subscription.listenerCount > 0 &&
        subscription.isStale(activation.state.collectionGenerations)
      )
        void subscription.run();
  }

  /**
   * Adopts one published batch: the mount's new state, a re-run of every
   * subscribed query whose collection generation moved with it, and the
   * publication listeners, in that order, so a listener that reads a
   * subscription's `promise` joins the re-read. A batch confirming a write the
   * browser has not painted yet records the new state at once and adopts the
   * rest after the paint, and so does every batch behind it.
   */
  published(applied: ColumnarLivePublication): void {
    const activation = this.#activation;
    if (!activation) return;
    // The stream serving this mount is the only evidence that a rebuild
    // recovered anything.
    this.#rebuilds = 0;
    const batch = {
      activation,
      applied,
      publication: recordPublication(activation, applied),
    };
    const painting = this.#painting;
    if (
      painting &&
      (painting.held.length > 0 ||
        applied.acknowledgedMutationIds.some((mutationId) =>
          painting.mutationIds.has(mutationId),
        ))
    ) {
      painting.held.push(batch);
      return;
    }
    this.#adoptPublication(batch);
  }

  #adoptPublication({
    activation,
    applied,
    publication,
  }: PublishedBatch): void {
    for (const mutationId of applied.acknowledgedMutationIds)
      this.#confirmMutation(mutationId);
    this.#rerun(activation, false);
    for (const listener of [...this.#publicationListeners]) {
      if (this.#activation !== activation) return;
      try {
        listener(publication);
      } catch (error) {
        reportObserverError(error);
      }
    }
  }

  /**
   * Requests the bootstrap plan, retrying a `NETWORK_ERROR` on
   * `BOOTSTRAP_RETRY_DELAYS_MS` and a `503 ARTIFACT_PREPARING` on the server's
   * `Retry-After`, up to `MAX_PREPARING_WAIT_MS` from the first such answer,
   * while the status is `preparing`. The bound is wall time, because a server
   * may hold each request open while it prepares. Any other answer is
   * reported without a second request.
   */
  async #requestPlan(
    bearerToken: string,
    transport: ReturnType<typeof artifactTransportProfile>,
    epoch: AbortSignal,
    adopted: Promise<Response> | undefined,
  ): Promise<BootstrapPlan> {
    let preparingSince: number | undefined;
    // An adopted request is the first attempt; every later one is the
    // session's own.
    let first = adopted;
    for (let attempt = 0; ;) {
      try {
        const response = first;
        first = undefined;
        if (response)
          return await readBootstrapPlan(response, this.#options.endpointUrl);
        return await requestBootstrapPlan({
          fetch: this.#options.fetch,
          endpoint: this.#options.endpointUrl,
          bearerToken,
          transport,
          // The server resolves each relation to the table id its plan names.
          requiredTables: collectionRelations(this.#collections),
          memoryBudgetBytes: this.#options.memoryBudgetBytes,
          clientHeapReserveBytes: this.#options.clientHeapReserveBytes,
          clientInstanceId: this.#clientInstanceId,
          deviceId: this.#deviceId,
        });
      } catch (error) {
        if (error instanceof ArtifactPreparingError) {
          // The server's schedule, not an attempt of ours: waiting out an
          // artifact the server is publishing does not consume the budget for
          // a service that is not answering at all.
          preparingSince ??= Date.now();
          if (
            Date.now() - preparingSince + error.retryAfterMs >
            MAX_PREPARING_WAIT_MS
          )
            throw error;
          epoch.throwIfAborted();
          this.#setStatus({ state: "preparing" });
          await delay(error.retryAfterMs, epoch);
          epoch.throwIfAborted();
          continue;
        }
        const delayMs = BOOTSTRAP_RETRY_DELAYS_MS[attempt];
        attempt += 1;
        if (
          delayMs === undefined ||
          !(error instanceof SyncError) ||
          error.code !== "NETWORK_ERROR"
        )
          throw error;
        await delay(delayMs, epoch);
        epoch.throwIfAborted();
      }
    }
  }

  async #activate(epoch: AbortSignal): Promise<Activation> {
    const started = this.#started;
    this.#started = undefined;
    mark("activate");
    let auth: ReturnType<typeof sealAuthSnapshot>;
    let transport: ReturnType<typeof artifactTransportProfile>;
    let adopted: Promise<Response> | undefined;
    try {
      auth = sealAuthSnapshot(await this.#options.auth());
      epoch.throwIfAborted();
      transport = artifactTransportProfile();
      adopted = this.#adopt(started, auth.bearerToken, transport);
    } finally {
      if (started && adopted === undefined) discardStartedBootstrap(started);
    }
    // The hashers load while the plan is requested. The epoch ending
    // terminates them at once: a load waiting for the engine worker to make
    // room for a chunk would otherwise hold them until the chunk's deadline.
    const hashers = new ArtifactHashers(this.#hasherFactory, hasherCount());
    const closeHashers = (): void => hashers.close();
    epoch.addEventListener("abort", closeHashers, { once: true });
    try {
      return await this.#mount(epoch, auth, transport, hashers, adopted);
    } finally {
      epoch.removeEventListener("abort", closeHashers);
      hashers.close();
    }
  }

  /**
   * The started request's response, if the session would have sent exactly
   * that request itself; it then takes the request's `clientInstanceId` and,
   * unless the application named one, its `deviceId`, which the server's
   * plan binds. If it would not, `undefined`, and the
   * session warns, naming the parameter but never its value, and sends its
   * own.
   */
  #adopt(
    started: StartedBootstrap | undefined,
    bearerToken: string,
    transport: ReturnType<typeof artifactTransportProfile>,
  ): Promise<Response> | undefined {
    if (!started) return undefined;
    const own = {
      url: bootstrapUrl(this.#options.endpointUrl).href,
      bearerToken,
      transportProfile: transport.name,
      protocolVersion: BOOTSTRAP_PROTOCOL_VERSION,
      requiredTables: collectionRelations(this.#collections),
      acceptedEncodings: transport.acceptedEncodings,
      memoryBudgetBytes: this.#options.memoryBudgetBytes,
      clientHeapReserveBytes: this.#options.clientHeapReserveBytes,
      deviceId: this.#options.deviceIdChosen
        ? this.#options.deviceId
        : started.parameters.deviceId,
    };
    const differing = differingBootstrapParameter(started.parameters, own);
    if (differing !== undefined) {
      mark("bootstrap:refused");
      console.warn(
        `@calque/client: the page started a bootstrap request whose ${differing} differs from the client's, so the client sends its own`,
      );
      return undefined;
    }
    this.#clientInstanceId = started.clientInstanceId;
    this.#deviceId = own.deviceId;
    mark("bootstrap:adopted");
    return started.response;
  }

  async #mount(
    epoch: AbortSignal,
    auth: ReturnType<typeof sealAuthSnapshot>,
    transport: ReturnType<typeof artifactTransportProfile>,
    hashers: ArtifactHashers,
    adopted: Promise<Response> | undefined,
  ): Promise<Activation> {
    mark("bootstrap:request");
    const plan = await this.#requestPlan(
      auth.bearerToken,
      transport,
      epoch,
      adopted,
    );
    mark("bootstrap:plan");
    epoch.throwIfAborted();
    // The plan is bound to the generated collections before any byte is
    // requested: a drifted generated file never opens a mount.
    const {
      artifact,
      trusted,
      contract: planContract,
    } = bindColumnarPlan(this.#collections, plan);
    // With no key configured, the mount refuses every barrier.
    const contract = this.#options.grantVerificationKey
      ? {
          ...planContract,
          revocationBarrierKey: this.#options.grantVerificationKey,
        }
      : planContract;
    if (plan.memory.minimumSafeBudgetBytes > this.#options.memoryBudgetBytes)
      throw new SyncError(
        "CAPACITY_EXCEEDED",
        "sync",
        "none",
        `the working set needs a memoryBudgetBytes of at least ${String(plan.memory.minimumSafeBudgetBytes)}; this client has ${String(this.#options.memoryBudgetBytes)}`,
      );
    const capacity = {
      capacityBytes: this.#options.memoryBudgetBytes,
      residentBytes: 0,
      standbyBytes: 0,
      safetyBytes:
        plan.memory.queryResultReserveBytes +
        plan.memory.catchUpReserveBytes +
        plan.memory.clientHeapReserveBytes,
    };
    // The worker and the change stream's module load while the artifact
    // downloads, so neither waits behind the last byte.
    const engine = new EngineWorkerClient(this.#workerFactory() as Worker);
    mark("worker:spawned");
    const liveModule = awaitedLater(import("@calque/worker/columnar-live"));
    // A hasher failing or the worker refusing a chunk ends the download,
    // every hasher and the engine worker, and the load reports that, not the
    // cancelled read.
    const download = new AbortController();
    // The hashers take each chunk's digest as it downloads and pass the
    // chunk on to the worker, which compares the digest with the plan's; it
    // reads no byte of a chunk before the chunk has matched, and releases
    // nothing until every chunk has.
    const receiving = engine.receive(
      trusted,
      hashers,
      capacity,
      plan.memory.queryResultReserveBytes,
      download.signal,
      contract,
    );
    // The decoder worker of the response being read, when the plan selected
    // `brotli-large-window` or `packed-brotli-large-window`. No other plan
    // spawns one.
    let decoder: ArtifactDecoder | undefined;
    let refusal: { readonly error: unknown } | undefined;
    const refuse = (error: unknown): void => {
      refusal ??= { error };
      download.abort();
      hashers.fail(error);
      decoder?.close();
    };
    hashers.failed.catch(refuse);
    receiving.opened.catch(refuse);
    const abandon = (): void => {
      download.abort();
      decoder?.close();
    };
    epoch.addEventListener("abort", abandon, { once: true });
    try {
      const chunkBytes = trusted.chunks?.bytes;
      if (chunkBytes === undefined)
        throw new Error("the plan names no chunk digests");
      const handed: HandedChunks = { count: 0, last: undefined };
      const { encoding, encodedBytes } = artifact.selectedRepresentation;
      const decoded =
        encoding === "brotli-large-window" ||
        encoding === "packed-brotli-large-window"
          ? encoding
          : undefined;
      // A request or a body the network cut is asked for again, on the
      // bootstrap's schedule, and the chunks it already delivered are kept.
      // A 401 or a 404 asks for a new plan. Any other answer from the server,
      // and a refusal, stays final.
      for (let attempt = 0; ; attempt += 1) {
        // A refusal that arrives while the load waits out a delay ends the
        // wait, and the load ends here instead of asking again.
        if (refusal) throw refusal.error;
        try {
          if (decoded) {
            // Spawned before the request, so the worker loads while the
            // artifact is requested, and again for each response: a decoder
            // reads one body from its first byte.
            const spawned = new ArtifactDecoder(this.#decoderFactory, {
              decodedBytes: artifact.decodedBytes,
              chunkBytes,
              skipChunks: handed.count,
              packed: decoded === "packed-brotli-large-window",
            });
            decoder = spawned;
            mark("decoder:spawned");
            spawned.failed.catch((error: unknown) => {
              refuse(artifactBodyRefusal(error, decoded));
            });
          }
          mark("artifact:request");
          const response = await fetchArtifact(
            this.#options.fetch,
            transport,
            artifact,
            DIRECT_COLUMNAR_ARTIFACT_MEDIA_TYPE,
            download.signal,
          );
          mark("artifact:response");
          if (decoder && decoded)
            await sendDecodedArtifact(
              response,
              decoded,
              encodedBytes,
              artifact.decodedBytes,
              chunkBytes,
              decoder,
              hashers,
              receiving,
              handed,
            );
          else
            await sendArtifact(
              response,
              artifact.decodedBytes,
              chunkBytes,
              hashers,
              receiving,
              handed,
            );
          break;
        } catch (error) {
          // The decoder served this response and no other.
          decoder?.close();
          decoder = undefined;
          // The server no longer honours the plan's capability, so the load
          // goes back to `auth()` and the bootstrap, which refuses a revoked
          // grant before it names any URL.
          if (error instanceof ArtifactCapabilityError)
            throw new PlanOutlived(error);
          const delayMs = BOOTSTRAP_RETRY_DELAYS_MS[attempt];
          if (
            refusal ||
            delayMs === undefined ||
            !(error instanceof SyncError) ||
            error.code !== "NETWORK_ERROR" ||
            error.scope !== "sync"
          )
            throw error;
          // The artifact URL's capability is good for as long as the plan
          // is. A retry that would go out at or after that goes back to the
          // bootstrap instead.
          if (Date.now() + delayMs >= Date.parse(plan.expiresAt))
            throw new PlanOutlived(error);
          // A harness keeps the first mark of each name, so this mark and
          // `artifact:replan` show that the load asked again.
          mark(`artifact:retry:${attempt + 1}`);
          await delay(delayMs, download.signal);
          epoch.throwIfAborted();
        }
      }

      let state: ColumnarMountState;
      try {
        state = (await Promise.race([receiving.opened, hashers.failed])).state;
        mark("open:done");
      } finally {
        hashers.close();
      }
      // The plan bound every table start to the artifact's snapshot cursor,
      // so a mount opened anywhere else cannot run the plan's catch-up.
      const snapshotCursor = encoder.encode(artifact.snapshotCursor);
      if (!state.cursor || !equalBytes(state.cursor, snapshotCursor))
        throw new SyncError(
          "INTEGRITY_FAILED",
          "sync",
          "reset",
          "the mount opened away from the artifact's snapshot cursor",
        );
      const { ColumnarLiveSession } = await liveModule;
      mark("live:module");
      epoch.throwIfAborted();
      // What each collection's mutations encode against, fixed here so a
      // reset cannot encode one activation's row against another's schema.
      const tables = new Map<string, MutationTable>();
      for (const descriptor of this.#collections) {
        const schema = trusted.schema.tables.find(
          ({ name }) => name === descriptor.id,
        );
        const binding = contract.tables.find(
          ({ collection }) => collection === descriptor.id,
        );
        if (schema && binding)
          tables.set(descriptor.id, { descriptor, schema, binding });
      }
      // The change stream catches up from the snapshot cursor to the
      // activation fence, and the activation resolves only once the mount
      // stands on the fence, so the artifact's cut is never read as live
      // state.
      const tableIds = contract.tables.map((table) => table.tableId);
      const activation: Activation = {
        engine,
        state,
        cursor: artifact.snapshotCursor,
        failure: undefined,
        live: undefined,
        scopeToken: plan.authorization.scopeToken,
        tables,
      };
      const live = new ColumnarLiveSession(engine, {
        endpoint: plan.changeStream.endpoint,
        planId: plan.planId,
        authorizationScopeToken: plan.authorization.scopeToken,
        tableIds,
        clientInstanceId: this.#clientInstanceId,
        deviceId: this.#deviceId,
        cursor: state.cursor,
        activationFence: {
          cursor: encoder.encode(plan.changeStream.throughCursorInclusive),
          tableStarts: tableIds.map((tableId) => ({
            tableId,
            snapshotCursor,
          })),
        },
        maxBatchBytes: plan.changeStream.maxBatchBytes,
        longPollTimeoutMs: plan.changeStream.longPollTimeoutMs,
        // The server sends a barrier body only to a poll that says it can
        // authenticate one.
        acceptRevocationBarrier:
          this.#options.grantVerificationKey !== undefined,
        fetch: this.#options.fetch,
        // Every poll names the deltas this mount still holds, so the server
        // can acknowledge them on the echo.
        pendingMutationIds: () => activation.state.pendingMutationIds,
        // A batch always moves this activation's own record, and reaches the
        // subscriptions only once the session adopted the activation.
        onPublished: (applied) => {
          if (this.#activation === activation) this.published(applied);
          else recordPublication(activation, applied);
        },
        onFailed: (error) => {
          if (this.#activation !== activation) {
            activation.failure = error;
            return;
          }
          // A mount that can no longer be advanced is rebuilt.
          const reported = this.#rebuildOrReport(activationError(error));
          if (reported) this.#fail(reported);
        },
      });
      activation.live = live;
      live.start();
      // The epoch ending stops the change stream, which ends the wait for the
      // fence.
      const stopLive = (): void => live.stop();
      epoch.addEventListener("abort", stopLive, { once: true });
      try {
        await live.live;
      } finally {
        epoch.removeEventListener("abort", stopLive);
      }
      return activation;
    } catch (error) {
      // `live` rejects only once the change stream stopped or failed, so the
      // engine is all that is left to release.
      engine.close();
      throw refusal ? refusal.error : error;
    } finally {
      epoch.removeEventListener("abort", abandon);
    }
  }
}
