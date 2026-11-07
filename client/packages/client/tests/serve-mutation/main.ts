import {
  createSyncClient,
  MutationSubmissionError,
  type LiveQueryResource,
  type LiveQuerySnapshot,
  type SyncPublication,
} from "@calque/client";

// The bindings the manual demo and self-hosted pages mount: `calque
// serve` runs here against the same manifest.
import { schema } from "../demo/calque.generated.js";

/**
 * The page `scripts/serve-mutation-e2e.ts` drives against a running
 * `calque serve`, on the public client surface only.
 *
 * Two pages are opened per run, differing only in their grant: one minted
 * with `--command tasks=update` performs the write, and one minted without
 * `--command` gets 404 for it. The reader never applied an optimistic delta,
 * so any value it shows for the row arrived from PostgreSQL through the
 * change stream.
 *
 * Every request to `/v1/mutations` is recorded at the client's `fetch`
 * option with the status the server answered, so the harness can tell a
 * refused write from one never sent.
 */

/** What the harness injects before any page script runs. */
interface ServeMutationPageConfig {
  readonly endpoint: string;
  readonly grant: string;
  readonly deviceId: string;
  /** Names this page in the harness's failure messages. */
  readonly role: string;
}

type ProjectRow = readonly [number, string, string];
type TaskRow = readonly [number, string, boolean];

/** One `POST /v1/mutations`, as this page's own `fetch` saw it. */
interface MutationRequestRecord {
  readonly url: string;
  /** The status the server answered, or null when no response arrived. */
  readonly status: number | null;
  /** What `fetch` threw, when it threw. */
  readonly failure: string | null;
  readonly atEpochMs: number;
}

/** One facade mutation call, settled either way. */
interface MutationAttempt {
  readonly collection: string;
  readonly key: number;
  readonly title: string;
  readonly settled: "accepted" | "refused";
  readonly state: string | null;
  /** What the server said about the outcome, when it answered one. */
  readonly resultMessage: string | null;
  readonly mutationId: string | null;
  readonly errorName: string | null;
  readonly errorCode: string | null;
  readonly errorMessage: string | null;
  /** `client.mutationStatus(mutationId).state` the moment the call settled. */
  readonly statusAtSettle: string | null;
  readonly calledAtEpochMs: number;
  readonly settledAtEpochMs: number;
}

/** What the writer page records when the canonical echo lands. */
interface CanonicalEcho {
  readonly mutationId: string;
  readonly observedAtEpochMs: number;
  readonly publicationsAtEcho: number;
  readonly cursorAtEcho: string | null;
  /** The row as the facade reads it once the echo has superseded the delta. */
  readonly title: string | null;
}

interface ServeMutationPageState {
  readonly role: string;
  readonly state: "initializing" | "ready" | "error" | "closed";
  readonly failure: string | null;
  readonly cursor: string | null;
  readonly publications: number;
  readonly projects: readonly ProjectRow[];
  readonly tasks: readonly TaskRow[];
  /** Whether the generated facade exposes each write method at all. */
  readonly facadeWrites: {
    readonly insert: boolean;
    readonly update: boolean;
    readonly delete: boolean;
  };
  readonly mutationRequests: readonly MutationRequestRecord[];
  readonly attempts: readonly MutationAttempt[];
  /** `mutationStatus` for every attempt that got an id, read now. */
  readonly mutationStatuses: readonly (readonly [string, string])[];
  readonly canonicalEchoes: readonly CanonicalEcho[];
  /** Publications that named `tasks`, in order, as `[cursor, count]`. */
  readonly taskPublications: readonly (readonly [string, number])[];
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  var __calqueServeMutationConfig: ServeMutationPageConfig | undefined;
  var __calqueServeMutationState: () => ServeMutationPageState;
  /**
   * One typed facade mutation, settled either way. The harness drives exactly
   * one per page; the page never writes on its own.
   */
  var __calqueServeMutationUpdateTask: (
    id: number,
    title: string,
  ) => Promise<MutationAttempt>;
}

const configuration = globalThis.__calqueServeMutationConfig;
if (!configuration) {
  throw new Error(
    "__calqueServeMutationConfig must be injected before the page runs; this page has no defaults",
  );
}

const statusElement = requiredElement("status");
const summaryElement = requiredElement("summary");

function requiredElement(id: string): HTMLElement {
  const element = document.querySelector(`#${id}`);
  if (!(element instanceof HTMLElement))
    throw new Error(`the page is missing #${id}`);
  return element;
}

interface Project {
  readonly id: bigint;
  readonly name: string;
  readonly status: string;
}

interface Task {
  readonly id: bigint;
  readonly title: string;
  readonly done: boolean;
}

let pageState: ServeMutationPageState["state"] = "initializing";
let failure: string | null = null;
let cursor: string | null = null;
let publications = 0;
let projectRows: readonly ProjectRow[] = [];
let taskRows: readonly TaskRow[] = [];
let projects: LiveQueryResource<readonly Project[]> | undefined;
let tasks: LiveQueryResource<readonly Task[]> | undefined;
let disposed = false;
let renderQueue = Promise.resolve();
const mutationRequests: MutationRequestRecord[] = [];
const attempts: MutationAttempt[] = [];
const canonicalEchoes: CanonicalEcho[] = [];
const taskPublications: (readonly [string, number])[] = [];
const watchedMutations = new Set<string>();

/**
 * The client's `fetch`, passed as the public `fetch` option rather than
 * patched onto `globalThis`: it records what this client sent to
 * `/v1/mutations` and what came back, including the case where nothing came
 * back at all. Nothing here changes the request or the response.
 */
async function recordingFetch(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = input instanceof Request ? input.url : String(input);
  if (!url.includes("/v1/mutations")) return globalThis.fetch(input, init);
  const atEpochMs = Date.now();
  try {
    const response = await globalThis.fetch(input, init);
    mutationRequests.push({
      url,
      status: response.status,
      failure: null,
      atEpochMs,
    });
    return response;
  } catch (error) {
    mutationRequests.push({
      url,
      status: null,
      failure: error instanceof Error ? error.message : String(error),
      atEpochMs,
    });
    throw error;
  }
}

const client = createSyncClient({
  endpoint: configuration.endpoint,
  auth: () => ({
    bearerToken: configuration.grant,
  }),
  schema,
  deviceId: configuration.deviceId,
  fetch: recordingFetch,
  // The same budget the other pages declare. A smaller one is refused with
  // 422 CAPACITY_EXCEEDED: the plan's requirement includes the server's own
  // reserves, not just the artifact.
  memoryBudgetBytes: 1024 * 1024 * 1024,
});

/**
 * Whether the generated facade exposes each write method. The facade is built
 * from the generated schema, so both pages have `update` to call; only the
 * server knows the grant's command capability, so it refuses on the wire.
 */
function facadeWrites(): ServeMutationPageState["facadeWrites"] {
  const facade = client.db.tasks as unknown as Record<string, unknown>;
  return {
    insert: typeof facade.insert === "function",
    update: typeof facade.update === "function",
    delete: typeof facade.delete === "function",
  };
}

globalThis.__calqueServeMutationState = () => ({
  role: configuration.role,
  state: pageState,
  failure,
  cursor,
  publications,
  projects: projectRows,
  tasks: taskRows,
  facadeWrites: facadeWrites(),
  mutationRequests: [...mutationRequests],
  attempts: [...attempts],
  mutationStatuses: [...watchedMutations].map(
    (id) => [id, client.mutationStatus(id)?.state ?? "forgotten"] as const,
  ),
  canonicalEchoes: [...canonicalEchoes],
  taskPublications: [...taskPublications],
});

/**
 * Watches one submitted mutation to its canonical echo. `canonical` is set
 * only from `acknowledgedMutationIds` on an applied change batch, so reaching
 * it is the echo arriving; the row is read afterwards, once the echo has
 * superseded the optimistic delta, and recorded with it.
 */
function watchToCanonical(mutationId: string, key: number): void {
  if (watchedMutations.has(mutationId)) return;
  watchedMutations.add(mutationId);
  const stop = client.subscribeMutation(mutationId, () => {
    if (client.mutationStatus(mutationId)?.state !== "canonical") return;
    stop();
    const observedAtEpochMs = Date.now();
    const publicationsAtEcho = publications;
    const cursorAtEcho = cursor;
    renderQueue = renderQueue
      .then(async () => {
        await refresh();
        canonicalEchoes.push({
          mutationId,
          observedAtEpochMs,
          publicationsAtEcho,
          cursorAtEcho,
          title: taskRows.find((row) => row[0] === key)?.[1] ?? null,
        });
      })
      .catch((error: unknown) => failClosed(error));
  });
}

globalThis.__calqueServeMutationUpdateTask = async (
  id: number,
  title: string,
): Promise<MutationAttempt> => {
  const calledAtEpochMs = Date.now();
  try {
    const result = await client.db.tasks.update({ id: BigInt(id) }, { title });
    watchToCanonical(result.mutationId, id);
    const attempt: MutationAttempt = {
      collection: "tasks",
      key: id,
      title,
      settled: "accepted",
      state: result.state,
      resultMessage: result.message,
      mutationId: result.mutationId,
      errorName: null,
      errorCode: null,
      errorMessage: null,
      statusAtSettle: client.mutationStatus(result.mutationId)?.state ?? null,
      calledAtEpochMs,
      settledAtEpochMs: Date.now(),
    };
    attempts.push(attempt);
    // The optimistic delta is readable now; re-reading keeps the reported
    // rows in step with what the facade would return.
    renderQueue = renderQueue
      .then(() => refresh())
      .catch((error: unknown) => failClosed(error));
    return attempt;
  } catch (error) {
    const mutationId =
      error instanceof MutationSubmissionError ? error.mutationId : null;
    if (mutationId !== null) watchedMutations.add(mutationId);
    const attempt: MutationAttempt = {
      collection: "tasks",
      key: id,
      title,
      settled: "refused",
      state: null,
      resultMessage: null,
      mutationId,
      errorName: error instanceof Error ? error.name : null,
      errorCode: error instanceof MutationSubmissionError ? error.code : null,
      errorMessage: error instanceof Error ? error.message : String(error),
      statusAtSettle:
        mutationId === null
          ? null
          : (client.mutationStatus(mutationId)?.state ?? null),
      calledAtEpochMs,
      settledAtEpochMs: Date.now(),
    };
    attempts.push(attempt);
    renderQueue = renderQueue
      .then(() => refresh())
      .catch((refreshError: unknown) => failClosed(refreshError));
    return attempt;
  }
};

function render(): void {
  statusElement.textContent = pageState;
  statusElement.dataset.state = pageState;
  summaryElement.textContent = `${configuration?.role ?? "page"}: ${projectRows.length} projects, ${taskRows.length} tasks, ${publications} publications`;
}

function readySnapshot<Rows>(snapshot: LiveQuerySnapshot<Rows>): Rows {
  if (snapshot.status === "error") throw snapshot.error;
  if (snapshot.status === "loading")
    throw new Error("a live query has no result after its read settled");
  return snapshot.result;
}

/** Waits for both live queries' current reads, then renders one cursor. */
async function refresh(): Promise<void> {
  if (!projects || !tasks) throw new Error("the live queries are not open");
  await Promise.all([projects.promise, tasks.promise]);
  projectRows = readySnapshot(projects.getSnapshot()).map(
    (project) => [Number(project.id), project.name, project.status] as const,
  );
  taskRows = readySnapshot(tasks.getSnapshot()).map(
    (task) => [Number(task.id), task.title, task.done] as const,
  );
  render();
}

function failClosed(error: unknown): void {
  if (disposed) return;
  pageState = "error";
  failure = error instanceof Error ? error.message : String(error);
  render();
}

function onStatus(): void {
  if (disposed) return;
  const status = client.status;
  if (status.state === "ready") {
    if (projects === undefined) {
      projects = client.db.projects.query().orderBy("id").subscribe();
      tasks = client.db.tasks.query().orderBy("id").subscribe();
      // The resources hold their listeners so the client keeps them live;
      // rendering follows publications so both tables show one cursor.
      projects.subscribe(() => undefined);
      tasks.subscribe(() => undefined);
      renderQueue = renderQueue
        .then(async () => {
          await refresh();
          pageState = "ready";
          render();
        })
        .catch((error: unknown) => failClosed(error));
    }
    return;
  }
  if (status.state === "error") {
    failClosed(status.error);
    return;
  }
  if (status.state === "closed") {
    pageState = "closed";
    render();
  }
}

function onPublication(publication: SyncPublication): void {
  publications += 1;
  cursor = publication.cursor;
  if (publication.changedCollections.includes("tasks"))
    taskPublications.push([publication.cursor, publication.transactionCount]);
  if (publication.changedCollections.length === 0) return;
  renderQueue = renderQueue
    .then(() => refresh())
    .catch((error: unknown) => failClosed(error));
}

globalThis.addEventListener("pagehide", () => {
  disposed = true;
  void client.close();
});

render();
client.subscribe(onStatus);
client.subscribePublications(onPublication);
onStatus();
