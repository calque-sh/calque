import {
  createSyncClient,
  type LiveQueryResource,
  type LiveQuerySnapshot,
  type SyncPublication,
} from "@calque/client";

// The manual demo page's bindings: the self-hosted composition serves
// the same two collections.
import { schema } from "../demo/calque.generated.js";

/**
 * The page `scripts/selfhost-compose-e2e.ts` drives against the containerized
 * server, on the public client surface only. Unlike the manual demo
 * dashboard, which reaches `calque dev` through a same-origin Vite proxy, it
 * addresses the composition's HTTPS edge with a signed dataset grant, so the
 * run covers TLS, the cross-origin allowlist and the forwarded `Host`.
 */

/** What the harness injects before any page script runs. */
interface SelfhostPageConfig {
  readonly endpoint: string;
  readonly grant: string;
}

type ProjectRow = readonly [number, string, string];
type TaskRow = readonly [number, string, boolean];

/** What the harness reads back out of the page. */
interface SelfhostPageState {
  readonly state: "initializing" | "ready" | "error" | "closed";
  readonly failure: string | null;
  readonly cursor: string | null;
  readonly publications: number;
  readonly projects: readonly ProjectRow[];
  readonly tasks: readonly TaskRow[];
  /** Every artifact-object URL the page fetched, in order. */
  readonly artifactRequests: readonly string[];
  /** The origin of every other URL the page fetched, deduplicated. */
  readonly fetchOrigins: readonly string[];
}

/** One `client.db.tasks.update`, as the facade returned it. */
interface SelfhostWrite {
  readonly mutationId: string;
  readonly state: string;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  var __calqueSelfhostConfig: SelfhostPageConfig | undefined;
  var __calqueSelfhostState: () => SelfhostPageState;
  /** Renames one task through the generated facade. */
  var __calqueSelfhostUpdateTask: (
    id: number,
    title: string,
  ) => Promise<SelfhostWrite>;
  /** `client.mutationStatus(mutationId).state`, or null once forgotten. */
  var __calqueSelfhostMutationState: (mutationId: string) => string | null;
}

const configuration = globalThis.__calqueSelfhostConfig;
if (!configuration) {
  throw new Error(
    "__calqueSelfhostConfig must be injected before the page runs; this page has no defaults",
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

let pageState: SelfhostPageState["state"] = "initializing";
let failure: string | null = null;
let cursor: string | null = null;
let publications = 0;
let projectRows: readonly ProjectRow[] = [];
let taskRows: readonly TaskRow[] = [];
let projects: LiveQueryResource<readonly Project[]> | undefined;
let tasks: LiveQueryResource<readonly Task[]> | undefined;
let disposed = false;
let renderQueue = Promise.resolve();

// Recorded here rather than from Playwright's network events, so the harness
// asserts what this page fetched. Installed before the client exists so no
// fetch escapes it.
const artifactRequests: string[] = [];
const fetchOrigins = new Set<string>();
const nativeFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = input instanceof Request ? input.url : String(input);
  if (url.includes("/v1/artifact-objects/")) artifactRequests.push(url);
  else fetchOrigins.add(new URL(url, globalThis.location.href).origin);
  return nativeFetch(input, init);
};

globalThis.__calqueSelfhostState = () => ({
  state: pageState,
  failure,
  cursor,
  publications,
  projects: projectRows,
  tasks: taskRows,
  artifactRequests: [...artifactRequests],
  fetchOrigins: [...fetchOrigins],
});

const client = createSyncClient({
  endpoint: configuration.endpoint,
  auth: () => ({
    bearerToken: configuration.grant,
  }),
  schema,
  deviceId: `selfhost-${globalThis.crypto.randomUUID()}`,
  // The same budget the manual demo page declares. A smaller one is
  // refused with 422 CAPACITY_EXCEEDED: the plan's requirement includes the
  // server's own reserves, not just the artifact.
  memoryBudgetBytes: 1024 * 1024 * 1024,
});

function render(): void {
  statusElement.textContent = pageState;
  statusElement.dataset.state = pageState;
  summaryElement.textContent = `${projectRows.length} projects, ${taskRows.length} tasks, ${publications} publications`;
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
  if (publication.changedCollections.length === 0) return;
  renderQueue = renderQueue
    .then(() => refresh())
    .catch((error: unknown) => failClosed(error));
}

globalThis.__calqueSelfhostUpdateTask = async (id, title) => {
  const result = await client.db.tasks.update({ id: BigInt(id) }, { title });
  return { mutationId: result.mutationId, state: result.state };
};

globalThis.__calqueSelfhostMutationState = (mutationId) =>
  client.mutationStatus(mutationId)?.state ?? null;

globalThis.addEventListener("pagehide", () => {
  disposed = true;
  void client.close();
});

render();
client.subscribe(onStatus);
client.subscribePublications(onPublication);
onStatus();
