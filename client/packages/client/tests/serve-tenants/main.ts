import {
  createSyncClient,
  type LiveQueryResource,
  type LiveQuerySnapshot,
} from "@calque/client";

import { schema } from "./calque.generated.js";

/**
 * The page `scripts/serve-tenants-e2e.ts` drives against a `calque serve`
 * whose scope is partitioned by `tenant_id`. Each page holds one tenant's
 * grant and renders that tenant's tasks and the shared priorities.
 *
 * A `MutationObserver` records every title the page ever painted, so a row
 * painted and replaced within one turn is still seen. The client's `fetch`
 * option records every response this client was handed, and can hand it
 * another tenant's bytes: the artifact, when the harness names another
 * tenant's artifact URL before the page loads, or the next change response,
 * when the harness gives it one at runtime. It can also put another tenant's
 * artifact entry in this page's bootstrap plan, so the client fetches that
 * artifact as the plan's own and only the scope hash tells them apart.
 */

interface TenantsPageConfig {
  readonly endpoint: string;
  readonly grant: string;
  readonly deviceId: string;
  /** Names this page in the harness's failure messages. */
  readonly role: string;
  /** Text that must never reach this page; any response holding it is recorded. */
  readonly foreignMarkers: readonly string[];
  /** Another tenant's artifact URL, fetched in place of this page's own. */
  readonly artifactFrom: string | null;
  /** Another tenant's plan entry for its artifact, put in place of this plan's own. */
  readonly planArtifactFrom: unknown;
}

/** A response another page received, carried across by the harness. */
interface CarriedResponse {
  readonly status: number;
  readonly headers: readonly (readonly [string, string])[];
  readonly base64: string;
}

interface StatusRecord {
  readonly atEpochMs: number;
  readonly state: string;
  readonly message: string | null;
}

interface TenantsPageState {
  readonly role: string;
  readonly state: "initializing" | "ready" | "error" | "closed";
  readonly failure: string | null;
  readonly statuses: readonly StatusRecord[];
  readonly bootstraps: number;
  /** This page's own artifact URL, from its own bootstrap plan. */
  readonly artifactUrl: string | null;
  /** This page's own plan entry for its artifact. */
  readonly planArtifact: unknown;
  readonly cursor: string | null;
  readonly tasks: readonly (readonly [number, number, string])[];
  readonly priorities: readonly (readonly [number, string])[];
  /** Every task title any paint inserted, in order. */
  readonly painted: readonly string[];
  /** Foreign markers found in a response this page did not substitute. */
  readonly foreignReceived: readonly string[];
  /** Whether a substituted response was handed to the client. */
  readonly substituted: readonly string[];
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  var __calqueTenantsConfig: TenantsPageConfig | undefined;
  var __calqueTenantsState: () => TenantsPageState;
  /** The change response this page received whose body holds `marker`. */
  var __calqueTenantsChangeResponseWith: (
    marker: string,
  ) => CarriedResponse | null;
  /** Answers this page's pending or next change request with `response`. */
  var __calqueTenantsAnswerChange: (response: CarriedResponse) => void;
}

const configuration = globalThis.__calqueTenantsConfig;
if (!configuration) {
  throw new Error(
    "__calqueTenantsConfig must be injected before the page runs; this page has no defaults",
  );
}

function requiredElement(id: string): HTMLElement {
  const element = document.querySelector(`#${id}`);
  if (!(element instanceof HTMLElement))
    throw new Error(`the page is missing #${id}`);
  return element;
}

const statusElement = requiredElement("status");
const tasksElement = requiredElement("tasks");
const prioritiesElement = requiredElement("priorities");

let pageState: TenantsPageState["state"] = "initializing";
let failure: string | null = null;
let cursor: string | null = null;
let bootstraps = 0;
let artifactUrl: string | null = null;
let planArtifact: unknown = null;
let taskRows: TenantsPageState["tasks"] = [];
let priorityRows: TenantsPageState["priorities"] = [];
const statuses: StatusRecord[] = [];
const painted: string[] = [];
const foreignReceived = new Set<string>();
const substituted: string[] = [];
interface ReceivedResponse {
  readonly status: number;
  readonly headers: readonly (readonly [string, string])[];
  readonly chunks: Uint8Array[];
}

const changeResponses: ReceivedResponse[] = [];

const text = new TextDecoder();

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function fromBase64(encoded: string): Uint8Array<ArrayBuffer> {
  const binary = atob(encoded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1)
    bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function joined(chunks: readonly Uint8Array[]): Uint8Array {
  const bytes = new Uint8Array(
    chunks.reduce((total, chunk) => total + chunk.length, 0),
  );
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

function headerList(headers: Headers): [string, string][] {
  const list: [string, string][] = [];
  headers.forEach((value, name) => list.push([name, value]));
  return list;
}

/**
 * Reads a copy of `response` without holding up the client's own read. A
 * change stream stays open, so its bytes are kept as they arrive.
 */
function inspect(response: Response, isChange: boolean): void {
  const reader = response.clone().body?.getReader();
  if (!reader) return;
  const received: ReceivedResponse = {
    status: response.status,
    headers: headerList(response.headers),
    chunks: [],
  };
  if (isChange) changeResponses.push(received);
  const read = async (): Promise<void> => {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      received.chunks.push(value);
      const body = text.decode(joined(received.chunks));
      for (const marker of configuration?.foreignMarkers ?? [])
        if (body.includes(marker)) foreignReceived.add(marker);
    }
  };
  void read().catch(() => undefined);
}

/** Another page's change response, waiting for this page's next request. */
let carriedChange: Response | null = null;
let awaitingChange: ((response: Response) => void) | null = null;

async function tenantsFetch(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = new URL(
    input instanceof Request ? input.url : String(input),
    configuration?.endpoint,
  ).href;
  if (url.endsWith("/v1/bootstrap")) {
    bootstraps += 1;
    let response = await globalThis.fetch(input, init);
    inspect(response, false);
    if (response.ok) {
      const plan = (await response.clone().json()) as {
        artifacts: { selectedRepresentation: { downloadUrl: string } }[];
      };
      planArtifact = plan.artifacts[0] ?? null;
      if (configuration?.planArtifactFrom && plan.artifacts.length > 0) {
        substituted.push("plan artifact");
        plan.artifacts[0] =
          configuration.planArtifactFrom as (typeof plan.artifacts)[number];
        response = new Response(JSON.stringify(plan), {
          status: response.status,
          headers: headerList(response.headers).filter(
            ([name]) =>
              name !== "content-length" && name !== "content-encoding",
          ),
        });
      }
      const download = plan.artifacts[0]?.selectedRepresentation.downloadUrl;
      artifactUrl =
        download === undefined
          ? null
          : new URL(download, configuration?.endpoint).href;
    }
    return response;
  }
  if (configuration?.artifactFrom && url === artifactUrl) {
    substituted.push("artifact");
    return globalThis.fetch(configuration.artifactFrom, init);
  }
  const method =
    input instanceof Request ? input.method : (init?.method ?? "GET");
  if (method !== "POST" || !url.includes("/change")) {
    const response = await globalThis.fetch(input, init);
    inspect(response, false);
    return response;
  }
  if (carriedChange !== null) {
    const carried = carriedChange;
    carriedChange = null;
    return carried;
  }
  const real = globalThis.fetch(input, init).then((response) => {
    inspect(response, true);
    return response;
  });
  const carried = new Promise<Response>((resolve) => {
    awaitingChange = resolve;
  });
  try {
    return await Promise.race([real, carried]);
  } finally {
    awaitingChange = null;
  }
}

globalThis.__calqueTenantsChangeResponseWith = (marker) => {
  for (const received of changeResponses) {
    const bytes = joined(received.chunks);
    if (text.decode(bytes).includes(marker))
      return {
        status: received.status,
        headers: received.headers,
        base64: base64(bytes),
      };
  }
  return null;
};

globalThis.__calqueTenantsAnswerChange = (carried) => {
  substituted.push("change");
  const response = new Response(fromBase64(carried.base64), {
    status: carried.status,
    headers: carried.headers
      .filter(
        ([name]) => name !== "content-length" && name !== "content-encoding",
      )
      .map(([name, value]): [string, string] => [name, value]),
  });
  if (awaitingChange === null) carriedChange = response;
  else awaitingChange(response);
};

const client = createSyncClient({
  endpoint: configuration.endpoint,
  auth: () => ({ bearerToken: configuration.grant }),
  schema,
  deviceId: configuration.deviceId,
  fetch: tenantsFetch,
  memoryBudgetBytes: 1024 * 1024 * 1024,
});

globalThis.__calqueTenantsState = () => ({
  role: configuration.role,
  state: pageState,
  failure,
  statuses: [...statuses],
  bootstraps,
  artifactUrl,
  planArtifact,
  cursor,
  tasks: taskRows,
  priorities: priorityRows,
  painted: [...painted],
  foreignReceived: [...foreignReceived],
  substituted: [...substituted],
});

new MutationObserver((records) => {
  for (const record of records)
    for (const node of Array.from(record.addedNodes))
      if (node instanceof HTMLElement && node.dataset.title !== undefined)
        painted.push(node.dataset.title);
}).observe(tasksElement, { childList: true });

let tasks:
  | LiveQueryResource<
      readonly { id: bigint; tenant_id: bigint; title: string }[]
    >
  | undefined;
let priorities:
  LiveQueryResource<readonly { id: bigint; label: string }[]> | undefined;
let renderQueue = Promise.resolve();

function readySnapshot<Rows>(snapshot: LiveQuerySnapshot<Rows>): Rows {
  if (snapshot.status === "error") throw snapshot.error;
  if (snapshot.status === "loading")
    throw new Error("a live query has no result after its read settled");
  return snapshot.result;
}

async function refresh(): Promise<void> {
  if (!tasks || !priorities) return;
  await Promise.all([tasks.promise, priorities.promise]);
  taskRows = readySnapshot(tasks.getSnapshot()).map(
    (task) => [Number(task.tenant_id), Number(task.id), task.title] as const,
  );
  priorityRows = readySnapshot(priorities.getSnapshot()).map(
    (priority) => [Number(priority.id), priority.label] as const,
  );
  tasksElement.replaceChildren(
    ...taskRows.map(([tenant, id, title]) => {
      const item = document.createElement("li");
      item.dataset.title = title;
      item.textContent = `${String(tenant)}/${String(id)} ${title}`;
      return item;
    }),
  );
  prioritiesElement.replaceChildren(
    ...priorityRows.map(([id, label]) => {
      const item = document.createElement("li");
      item.textContent = `${String(id)} ${label}`;
      return item;
    }),
  );
}

function failClosed(error: unknown): void {
  pageState = "error";
  failure = error instanceof Error ? error.message : String(error);
  statusElement.textContent = pageState;
}

function onStatus(): void {
  const status = client.status;
  statuses.push({
    atEpochMs: Date.now(),
    state: status.state,
    message: status.state === "error" ? status.error.message : null,
  });
  if (status.state === "ready" && tasks === undefined) {
    tasks = client.db.tasks.query().orderBy("id").subscribe();
    priorities = client.db.priorities.query().orderBy("id").subscribe();
    tasks.subscribe(() => undefined);
    priorities.subscribe(() => undefined);
    renderQueue = renderQueue
      .then(async () => {
        await refresh();
        pageState = "ready";
        statusElement.textContent = pageState;
      })
      .catch(failClosed);
  } else if (status.state === "error") failClosed(status.error);
  else if (status.state === "closed") pageState = "closed";
}

client.subscribePublications((publication) => {
  cursor = publication.cursor;
  if (publication.changedCollections.length === 0) return;
  renderQueue = renderQueue.then(() => refresh()).catch(failClosed);
});

globalThis.addEventListener("pagehide", () => {
  void client.close();
});

client.subscribe(onStatus);
onStatus();
