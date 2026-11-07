import { fromBinary } from "@bufbuild/protobuf";
import {
  createSyncClient,
  MutationSubmissionError,
  type LiveQuerySnapshot,
  type GrantVerificationKey,
} from "@calque/client";
import { mutation } from "@calque/protocol";

// The generated bindings every serve page mounts; see `tests/serve-mutation`.
import { schema } from "../demo/calque.generated.js";

/**
 * The page `scripts/serve-revocation-e2e.ts` drives against a running
 * `calque serve`: it renders every task row the live query publishes, shows
 * a lock banner when the session locks, and its `auth()` hands out whatever
 * grant the harness last gave it, or refuses.
 *
 * A `MutationObserver` on the table body records every row the page inserted,
 * with the time, the session state and the grant the mount was bootstrapped
 * with, so a revoked row painted and replaced within one turn is still
 * recorded. The client's `fetch` option records every bootstrap and mutation
 * POST, and can hold a POST so a write is in flight when the barrier lands.
 */

interface RevocationPageConfig {
  readonly endpoint: string;
  readonly deviceId: string;
  /** Names this page in the harness's failure messages. */
  readonly role: string;
  readonly grantVerificationKey: GrantVerificationKey;
  readonly grant: { readonly token: string; readonly label: string };
}

interface StatusRecord {
  readonly atEpochMs: number;
  readonly state: string;
  readonly reason: string | null;
  readonly code: string | null;
  readonly message: string | null;
}

/** One `MutationObserver` delivery on the task table. */
interface PaintRecord {
  readonly atEpochMs: number;
  readonly sessionState: string;
  /** The grant the last successful bootstrap presented. */
  readonly mountedGrant: string | null;
  /** Row ids this delivery inserted, in order, even if since removed. */
  readonly inserted: readonly number[];
  readonly insertedTitles: readonly string[];
  readonly removed: number;
  /** The table's rows once the delivery was applied. */
  readonly rows: readonly number[];
}

interface AuthRecord {
  readonly atEpochMs: number;
  /** The grant handed out, or null when `auth()` refused. */
  readonly grant: string | null;
}

interface BootstrapRecord {
  readonly atEpochMs: number;
  readonly grant: string | null;
  readonly status: number | null;
}

interface MutationRequestRecord {
  readonly mutationIds: readonly string[];
  readonly grant: string | null;
  readonly calledAtEpochMs: number;
  readonly sentAtEpochMs: number;
  readonly status: number | null;
  readonly failure: string | null;
}

interface MutationTransition {
  readonly atEpochMs: number;
  readonly mutationId: string;
  readonly state: string;
  readonly code: string | null;
  readonly message: string | null;
}

interface UpdateAttempt {
  readonly id: number;
  readonly title: string;
  readonly settled: "pending" | "accepted" | "refused";
  readonly mutationId: string | null;
  readonly errorCode: string | null;
  readonly errorMessage: string | null;
  readonly calledAtEpochMs: number;
  readonly settledAtEpochMs: number | null;
}

interface RevocationPageState {
  readonly role: string;
  readonly status: StatusRecord;
  readonly statuses: readonly StatusRecord[];
  readonly lockBanner: { readonly visible: boolean; readonly text: string };
  readonly rows: readonly (readonly [number, string])[];
  readonly paints: readonly PaintRecord[];
  readonly auths: readonly AuthRecord[];
  readonly bootstraps: readonly BootstrapRecord[];
  readonly mutationRequests: readonly MutationRequestRecord[];
  readonly mutationTransitions: readonly MutationTransition[];
  readonly attempts: readonly UpdateAttempt[];
  readonly heldMutations: number;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations to `typeof globalThis`.
  var __calqueRevocationConfig: RevocationPageConfig | undefined;
  var __calqueRevocationState: () => RevocationPageState;
  /** What `auth()` returns from now on; `null` makes it refuse. */
  var __calqueRevocationSetGrant: (
    token: string | null,
    label: string | null,
  ) => void;
  /** Holds every mutation POST the client makes until released. */
  var __calqueRevocationHoldMutations: () => void;
  var __calqueRevocationReleaseMutations: () => void;
  /** Starts one facade update and returns before it settles. */
  var __calqueRevocationStartUpdate: (id: number, title: string) => number;
}

const configuration = globalThis.__calqueRevocationConfig;
if (!configuration) {
  throw new Error(
    "__calqueRevocationConfig must be injected before the page runs; this page has no defaults",
  );
}

const statusElement = requiredElement("status");
const lockElement = requiredElement("lock");
const tableBody = requiredElement("tasks");

function requiredElement(id: string): HTMLElement {
  const element = document.querySelector(`#${id}`);
  if (!(element instanceof HTMLElement))
    throw new Error(`the page is missing #${id}`);
  return element;
}

let grant: { token: string; label: string } | null = configuration.grant;
/** Every token this page handed out, so a request can be named by grant. */
const grantLabels = new Map<string, string>([
  [configuration.grant.token, configuration.grant.label],
]);
let mountedGrant: string | null = null;
const statuses: StatusRecord[] = [];
const paints: PaintRecord[] = [];
const auths: AuthRecord[] = [];
const bootstraps: BootstrapRecord[] = [];
const mutationRequests: MutationRequestRecord[] = [];
const mutationTransitions: MutationTransition[] = [];
const attempts: UpdateAttempt[] = [];
const watchedMutations = new Set<string>();
let hold: { promise: Promise<void>; release: () => void } | undefined;
let heldMutations = 0;

globalThis.__calqueRevocationSetGrant = (token, label) => {
  if (token === null || label === null) {
    grant = null;
    return;
  }
  grant = { token, label };
  grantLabels.set(token, label);
};

globalThis.__calqueRevocationHoldMutations = () => {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  hold = { promise, release };
};

globalThis.__calqueRevocationReleaseMutations = () => {
  hold?.release();
  hold = undefined;
};

function grantOf(init: RequestInit | undefined): string | null {
  const authorization = new Headers(init?.headers).get("authorization") ?? "";
  const token = authorization.replace(/^Bearer /u, "");
  return grantLabels.get(token) ?? null;
}

/**
 * The client's `fetch`, passed as the public `fetch` option. Bootstraps and
 * mutation POSTs are recorded with the grant they presented and the status
 * that came back; a mutation POST additionally waits while the harness holds
 * mutations. Everything else passes through untouched.
 */
async function recordingFetch(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = input instanceof Request ? input.url : String(input);
  if (url.includes("/v1/bootstrap")) {
    const atEpochMs = Date.now();
    const presented = grantOf(init);
    try {
      const response = await globalThis.fetch(input, init);
      bootstraps.push({ atEpochMs, grant: presented, status: response.status });
      if (response.ok) mountedGrant = presented;
      return response;
    } catch (error) {
      bootstraps.push({ atEpochMs, grant: presented, status: null });
      throw error;
    }
  }
  if (!url.includes("/v1/mutations")) return globalThis.fetch(input, init);
  const calledAtEpochMs = Date.now();
  const mutationIds =
    init?.body instanceof Uint8Array
      ? fromBinary(
          mutation.MutationBatchRequestSchema,
          init.body,
        ).mutations.map((envelope) => envelope.mutationId)
      : [];
  for (const mutationId of mutationIds) watchMutation(mutationId);
  const held = hold;
  if (held) {
    heldMutations += 1;
    await held.promise;
    heldMutations -= 1;
  }
  const sentAtEpochMs = Date.now();
  const presented = grantOf(init);
  try {
    const response = await globalThis.fetch(input, init);
    mutationRequests.push({
      mutationIds,
      grant: presented,
      calledAtEpochMs,
      sentAtEpochMs,
      status: response.status,
      failure: null,
    });
    return response;
  } catch (error) {
    mutationRequests.push({
      mutationIds,
      grant: presented,
      calledAtEpochMs,
      sentAtEpochMs,
      status: null,
      failure: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

const client = createSyncClient({
  endpoint: configuration.endpoint,
  auth: () => {
    const atEpochMs = Date.now();
    if (grant === null) {
      auths.push({ atEpochMs, grant: null });
      throw new Error("the application holds no grant for this user");
    }
    auths.push({ atEpochMs, grant: grant.label });
    return { bearerToken: grant.token };
  },
  schema,
  deviceId: configuration.deviceId,
  fetch: recordingFetch,
  memoryBudgetBytes: 1024 * 1024 * 1024,
  grantVerificationKey: configuration.grantVerificationKey,
});

function watchMutation(mutationId: string): void {
  if (watchedMutations.has(mutationId)) return;
  watchedMutations.add(mutationId);
  const record = () => {
    const status = client.mutationStatus(mutationId);
    if (!status) return;
    const error = status.state === "ambiguous" ? status.error : undefined;
    const result =
      "result" in status ? (status.result as { message?: string }) : undefined;
    mutationTransitions.push({
      atEpochMs: Date.now(),
      mutationId,
      state: status.state,
      code: error?.code ?? null,
      message: error?.message ?? result?.message ?? null,
    });
  };
  client.subscribeMutation(mutationId, record);
  record();
}

globalThis.__calqueRevocationStartUpdate = (id, title) => {
  const index = attempts.length;
  const calledAtEpochMs = Date.now();
  attempts.push({
    id,
    title,
    settled: "pending",
    mutationId: null,
    errorCode: null,
    errorMessage: null,
    calledAtEpochMs,
    settledAtEpochMs: null,
  });
  client.db.tasks.update({ id: BigInt(id) }, { title }).then(
    (result) => {
      attempts[index] = {
        id,
        title,
        settled: "accepted",
        mutationId: result.mutationId,
        errorCode: null,
        errorMessage: null,
        calledAtEpochMs,
        settledAtEpochMs: Date.now(),
      };
    },
    (error: unknown) => {
      attempts[index] = {
        id,
        title,
        settled: "refused",
        mutationId:
          error instanceof MutationSubmissionError ? error.mutationId : null,
        errorCode:
          error instanceof Error && "code" in error
            ? String((error as { code: unknown }).code)
            : null,
        errorMessage: error instanceof Error ? error.message : String(error),
        calledAtEpochMs,
        settledAtEpochMs: Date.now(),
      };
    },
  );
  return index;
};

function statusRecord(): StatusRecord {
  const status = client.status;
  const error =
    status.state === "error" || status.state === "locked"
      ? status.error
      : undefined;
  return {
    atEpochMs: Date.now(),
    state: status.state,
    reason: status.state === "locked" ? status.reason : null,
    code: error?.code ?? null,
    message: error?.message ?? null,
  };
}

function renderedRows(): (readonly [number, string])[] {
  return Array.from(tableBody.querySelectorAll("tr")).map(
    (row) => [Number(row.dataset.id), row.dataset.title ?? ""] as const,
  );
}

globalThis.__calqueRevocationState = () => ({
  role: configuration.role,
  status: statusRecord(),
  statuses: [...statuses],
  lockBanner: {
    visible: !lockElement.hidden,
    text: lockElement.textContent ?? "",
  },
  rows: renderedRows(),
  paints: [...paints],
  auths: [...auths],
  bootstraps: [...bootstraps],
  mutationRequests: [...mutationRequests],
  mutationTransitions: [...mutationTransitions],
  attempts: [...attempts],
  heldMutations,
});

new MutationObserver((records) => {
  const inserted: number[] = [];
  const insertedTitles: string[] = [];
  let removed = 0;
  for (const record of records) {
    for (const node of Array.from(record.addedNodes)) {
      if (!(node instanceof HTMLTableRowElement)) continue;
      inserted.push(Number(node.dataset.id));
      insertedTitles.push(node.dataset.title ?? "");
    }
    removed += record.removedNodes.length;
  }
  paints.push({
    atEpochMs: Date.now(),
    sessionState: client.status.state,
    mountedGrant,
    inserted,
    insertedTitles,
    removed,
    rows: renderedRows().map(([id]) => id),
  });
}).observe(tableBody, { childList: true });

interface Task {
  readonly id: bigint;
  readonly title: string;
}

/** Renders what the live query publishes, and nothing when it has no rows. */
function renderRows(snapshot: LiveQuerySnapshot<readonly Task[]>): void {
  const rows = snapshot.status === "ready" ? snapshot.result : [];
  tableBody.replaceChildren(
    ...rows.map((task) => {
      const row = document.createElement("tr");
      row.dataset.id = String(task.id);
      row.dataset.title = task.title;
      const id = document.createElement("td");
      id.textContent = String(task.id);
      const title = document.createElement("td");
      title.textContent = task.title;
      row.append(id, title);
      return row;
    }),
  );
}

function onStatus(): void {
  const record = statusRecord();
  const last = statuses.at(-1);
  if (last?.state !== record.state || last.reason !== record.reason)
    statuses.push(record);
  statusElement.textContent = record.state;
  statusElement.dataset.state = record.state;
  const status = client.status;
  if (status.state === "locked") {
    lockElement.hidden = false;
    lockElement.dataset.reason = status.reason;
    lockElement.textContent =
      status.reason === "revoked"
        ? "Your access to this workspace was revoked."
        : "Your session expired. Sign in again to continue.";
  } else {
    lockElement.hidden = true;
  }
}

const tasks = client.db.tasks.query().orderBy("id").subscribe();
tasks.subscribe(() => renderRows(tasks.getSnapshot()));
client.subscribe(onStatus);
onStatus();

globalThis.addEventListener("pagehide", () => {
  void client.close();
});
