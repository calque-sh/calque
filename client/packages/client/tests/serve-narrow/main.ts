import {
  createSyncClient,
  type LiveQueryResource,
  type SyncPublication,
} from "@calque/client";

import { schema } from "./calque.generated.js";

/**
 * The page `scripts/serve-narrow-e2e.ts` drives against a `calque serve`
 * whose manifest serves `projects` and `tasks`. This page's bindings name
 * `projects` only, so it reads a working set narrower than the one the server
 * serves.
 */

interface NarrowPageConfig {
  readonly endpoint: string;
  readonly grant: string;
  readonly deviceId: string;
}

interface NarrowPageState {
  readonly state: "initializing" | "ready" | "error" | "closed";
  readonly failure: string | null;
  readonly cursor: string | null;
  readonly projects: readonly (readonly [number, string, string])[];
  /** Every collection named by a publication, in order. */
  readonly publishedCollections: readonly (readonly string[])[];
  readonly collections: readonly string[];
  /** Each client status the page saw, so a silent rebuild shows. */
  readonly statuses: readonly string[];
}

declare global {
  var __calqueNarrowConfig: NarrowPageConfig | undefined;
  var __calqueNarrowState: () => NarrowPageState;
}

const configuration = globalThis.__calqueNarrowConfig;
if (!configuration) {
  throw new Error(
    "__calqueNarrowConfig must be injected before the page runs; this page has no defaults",
  );
}

const statusElement = requiredElement("status");

function requiredElement(id: string): HTMLElement {
  const element = document.querySelector(`#${id}`);
  if (!(element instanceof HTMLElement))
    throw new Error(`the page is missing #${id}`);
  return element;
}

let pageState: NarrowPageState["state"] = "initializing";
let failure: string | null = null;
let cursor: string | null = null;
let rows: NarrowPageState["projects"] = [];
let projects: LiveQueryResource<
  readonly { id: bigint; name: string; status: string }[]
> | null = null;
let disposed = false;
let renderQueue = Promise.resolve();
const publishedCollections: (readonly string[])[] = [];
const statuses: string[] = [];

const client = createSyncClient({
  endpoint: configuration.endpoint,
  auth: () => ({ bearerToken: configuration.grant }),
  schema,
  deviceId: configuration.deviceId,
  memoryBudgetBytes: 1024 * 1024 * 1024,
});

globalThis.__calqueNarrowState = () => ({
  state: pageState,
  failure,
  cursor,
  projects: rows,
  publishedCollections: [...publishedCollections],
  collections: Object.keys(client.db),
  statuses: [...statuses],
});

function render(): void {
  statusElement.textContent = pageState;
}

function failClosed(error: unknown): void {
  if (disposed) return;
  pageState = "error";
  failure = error instanceof Error ? error.message : String(error);
  render();
}

async function refresh(): Promise<void> {
  if (!projects) throw new Error("the live query is not open");
  await projects.promise;
  const snapshot = projects.getSnapshot();
  if (snapshot.status === "error") throw snapshot.error;
  if (snapshot.status === "loading")
    throw new Error("a live query has no result after its read settled");
  rows = snapshot.result.map(
    (project) => [Number(project.id), project.name, project.status] as const,
  );
}

function onStatus(): void {
  if (disposed) return;
  const status = client.status;
  if (statuses.at(-1) !== status.state) statuses.push(status.state);
  if (status.state === "ready") {
    if (projects === null) {
      projects = client.db.projects.query().orderBy("id").subscribe();
      projects.subscribe(() => undefined);
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
  cursor = publication.cursor;
  publishedCollections.push([...publication.changedCollections]);
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
