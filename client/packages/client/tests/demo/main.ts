import {
  createSyncClient,
  type LiveQueryResource,
  type LiveQuerySnapshot,
  type SyncPublication,
} from "@calque/client";
import { renderedConnection } from "@calque/client/bootstrap-start";

import { schema } from "./calque.generated.js";

/**
 * The manual demo dashboard, on the public surface only: a client from
 * `createSyncClient` over the generated bindings, facade queries through
 * `db.<collection>`, and a render loop driven by
 * `client.subscribePublications`. Awaiting both resources inside one
 * publication puts the two tables on screen at one cursor, and the
 * publication's source commit time is what the latency harness measures
 * commit-to-presented-frame against.
 */

/**
 * One publication the page rendered, recorded for the latency harness.
 * Timestamps are epoch milliseconds from the page's own clock so they can be
 * compared with the source's commit clock and with presented-frame times
 * observed from outside the page.
 */
interface DemoPublication {
  readonly sequence: number;
  readonly cursor: string;
  readonly commitTimeUnixMicros: string | null;
  readonly changedCollections: readonly string[];
  readonly transactionCount: number;
  /** When the client reported the batch published. */
  readonly publishedAtEpochMs: number;
  /** When the dashboard DOM held the new state. */
  readonly renderedAtEpochMs: number;
  /** The next animation frame after the render, when one has fired. */
  frameCallbackAtEpochMs: number | null;
}

/** What `__calqueDemoMutate` reports back to a harness driving the page. */
interface DemoMutation {
  readonly mutationId: string;
  readonly state: string;
  readonly failure?: string;
}

declare global {
  // `var` is required: TypeScript only attaches ambient global `var`
  // declarations (not `let`/`const`) to the `typeof globalThis` type.
  var __calqueDemoPublications: DemoPublication[];
  /**
   * Renames one task through the generated facade and reports where the
   * mutation got to, without awaiting the canonical echo, so a harness can
   * put a write in flight and reload the page underneath it.
   */
  var __calqueDemoMutate: (id: string, title: string) => Promise<DemoMutation>;
  /** The state the session holds for one mutation, or `undefined`. */
  var __calqueDemoMutationState: (mutationId: string) => string | undefined;
  /**
   * `id → title` as the facade reads it now, optimistic overlay included.
   * The dashboard renders only on publications, so a harness that needs
   * pending local state asks the facade.
   */
  var __calqueDemoTaskTitles: () => Promise<readonly (readonly string[])[]>;
}

const MEMORY_BUDGET_BYTES = 1024 * 1024 * 1024;

const { endpoint, bearerToken } = renderedConnection();

const deviceStorageKey = "calque-demo-device";
const existingDeviceId = globalThis.localStorage.getItem(deviceStorageKey);
const deviceId = existingDeviceId ?? `local-${globalThis.crypto.randomUUID()}`;
if (existingDeviceId === null) {
  globalThis.localStorage.setItem(deviceStorageKey, deviceId);
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
  readonly project_id: bigint;
  readonly name: string;
  readonly status: string;
}

interface Dashboard {
  readonly projects: readonly Project[];
  readonly tasks: readonly Task[];
}

type TaskFilter = "all" | "open" | "done";
type PageState = "initializing" | "ready" | "error" | "closed";

const elements = {
  status: requiredElement("status", HTMLOutputElement),
  syncDot: requiredElement("sync-dot", HTMLSpanElement),
  syncDetail: requiredElement("sync-detail", HTMLElement),
  notice: requiredElement("notice", HTMLDivElement),
  cursorNote: requiredElement("cursor-note", HTMLSpanElement),
  metricProjects: requiredElement("metric-projects", HTMLElement),
  metricOpen: requiredElement("metric-open", HTMLElement),
  metricTotal: requiredElement("metric-total", HTMLElement),
  metricCompleted: requiredElement("metric-completed", HTMLElement),
  metricRate: requiredElement("metric-rate", HTMLElement),
  projectRows: requiredElement("project-rows", HTMLTableSectionElement),
  taskRows: requiredElement("task-rows", HTMLTableSectionElement),
  taskSummary: requiredElement("task-summary", HTMLParagraphElement),
  taskSearch: requiredElement("task-search", HTMLInputElement),
};

globalThis.__calqueDemoPublications = [];

const client = createSyncClient({
  endpoint,
  auth: () => ({ bearerToken }),
  schema,
  deviceId,
  memoryBudgetBytes: MEMORY_BUDGET_BYTES,
});

/**
 * The two live queries the dashboard renders: every project, and every task
 * joined to its project. Both are built through the generated facades and
 * subscribed once the client is ready; the client re-reads each one after a
 * publication moved one of its collections.
 */
function projectsQuery() {
  return client.db.projects.query().orderBy("id");
}

function tasksQuery() {
  return client.db.tasks
    .query()
    .join(client.db.projects, "p", { left: "project_id", right: "id" })
    .select("id", "title", "done", "project_id", "p.name", "p.status")
    .orderBy("project_id")
    .orderBy("id");
}

let projects: LiveQueryResource<readonly Project[]> | undefined;
let tasks: LiveQueryResource<readonly Task[]> | undefined;
let dashboard: Dashboard | undefined;
let pageState: PageState = "initializing";
let taskFilter: TaskFilter = "all";
let disposed = false;
let publicationSequence = 0;
let lastCursor: string | undefined;
// Publications render in the order the client published them. A batch that
// arrives while the previous one is still being read waits its turn, so the
// dashboard never shows a newer cursor's rows under an older cursor's note.
let renderQueue = Promise.resolve();

function setState(state: PageState, detail: string) {
  pageState = state;
  elements.status.dataset.state = state;
  elements.status.value = statusLabel(state);
  elements.syncDot.dataset.state = state;
  elements.syncDetail.textContent = detail;
}

/**
 * Reacts to the client's status. `ready` is reported only once the mount
 * stands at the plan's activation cursor, so the first render never shows
 * the artifact's cut as if it were live; the subscriptions are opened here
 * and the dashboard renders from their first snapshots.
 */
function onStatus() {
  if (disposed) return;
  const status = client.status;
  if (status.state === "ready") {
    if (projects === undefined) {
      projects = projectsQuery().subscribe();
      tasks = tasksQuery().subscribe();
      // The resources hold their listeners so the client keeps them live;
      // rendering follows publications, below, so both tables always show
      // one cursor.
      projects.subscribe(() => undefined);
      tasks.subscribe(() => undefined);
      renderQueue = renderQueue
        .then(() => goLive())
        .catch((error: unknown) => failClosed(error));
    }
    return;
  }
  if (status.state === "error") {
    failClosed(status.error);
    return;
  }
  if (status.state === "closed") setState("closed", "The client is closed");
}

/** The first render, at the activation cursor and never before it. */
async function goLive() {
  if (disposed) return;
  await refreshDashboard();
  if (disposed) return;
  showCursorNote(lastCursor);
  setState("ready", "Receiving live changes");
  showNotice(
    "Connected. Run pnpm demo:change in another terminal and watch the dashboard advance without reloading.",
    false,
  );
}

/**
 * One canonical publication the client applied. Taken on the turn the client
 * reported it, before the render queue: the harness measures publish-to-render
 * from here. The subscriptions whose collections moved are already re-reading;
 * the render waits for both so the two tables show one cursor.
 */
function onPublication(publication: SyncPublication) {
  const publishedAtEpochMs = performance.timeOrigin + performance.now();
  lastCursor = publication.cursor;
  // An empty long-poll answer moves nothing; there is nothing to render.
  if (publication.changedCollections.length === 0) return;
  renderQueue = renderQueue
    .then(() => publish(publication, publishedAtEpochMs))
    .catch((error: unknown) => failClosed(error));
}

async function publish(
  publication: SyncPublication,
  publishedAtEpochMs: number,
) {
  if (disposed || pageState !== "ready") return;
  await refreshDashboard();
  if (disposed) return;
  const record: DemoPublication = {
    sequence: ++publicationSequence,
    cursor: publication.cursor,
    commitTimeUnixMicros:
      publication.latestCommitTimeUnixMicros === null
        ? null
        : publication.latestCommitTimeUnixMicros.toString(),
    changedCollections: publication.changedCollections,
    transactionCount: publication.transactionCount,
    publishedAtEpochMs,
    renderedAtEpochMs: performance.timeOrigin + performance.now(),
    frameCallbackAtEpochMs: null,
  };
  globalThis.__calqueDemoPublications.push(record);
  requestAnimationFrame((frameTime) => {
    record.frameCallbackAtEpochMs = performance.timeOrigin + frameTime;
  });
  showCursorNote(publication.cursor);
}

function showCursorNote(cursor: string | undefined) {
  const position =
    cursor === undefined ? "" : ` · cursor ${shortCursor(cursor)}`;
  elements.cursorNote.textContent = `Complete local view · ${dashboard?.projects.length ?? 0} projects and ${dashboard?.tasks.length ?? 0} tasks${position}`;
}

/**
 * The client is terminal on any failure it reports: a torn batch, a cursor
 * that does not continue the mount, a plan the bindings do not match. The
 * page says so and stops; it never re-polls a stream that has already been
 * refused once.
 */
function failClosed(error: unknown) {
  if (disposed) return;
  setState("error", "The sync client stopped");
  showNotice(formatError(error), true);
}

/** Waits for both live queries' current reads, then renders their snapshots. */
async function refreshDashboard() {
  if (!projects || !tasks) throw new Error("the live queries are not open");
  await Promise.all([projects.promise, tasks.promise]);
  dashboard = {
    projects: readySnapshot(projects.getSnapshot()),
    tasks: readySnapshot(tasks.getSnapshot()),
  };
  renderOverview();
  renderProjects();
  renderTasks();
}

function readySnapshot<Rows>(snapshot: LiveQuerySnapshot<Rows>): Rows {
  if (snapshot.status === "error") throw snapshot.error;
  if (snapshot.status === "loading")
    throw new Error("a live query has no result after its read settled");
  return snapshot.result;
}

function renderOverview() {
  if (!dashboard) return;
  const total = dashboard.tasks.length;
  const completed = dashboard.tasks.filter((task) => task.done).length;
  elements.metricProjects.textContent = String(dashboard.projects.length);
  elements.metricTotal.textContent = `${total} total tasks`;
  elements.metricCompleted.textContent = String(completed);
  elements.metricOpen.textContent = String(total - completed);
  elements.metricRate.textContent = `${completionRate(completed, total)}%`;
}

function renderProjects() {
  if (!dashboard) return;
  const rows = dashboard.projects.map((project) => {
    const projectTasks =
      dashboard?.tasks.filter((task) => task.project_id === project.id) ?? [];
    const completed = projectTasks.filter((task) => task.done).length;
    return tableRow([
      cellWithText(project.name, `Project ${project.id}`),
      cellWithBadge(
        humanizeStatus(project.status),
        `status-${cssToken(project.status)}`,
      ),
      progressCell(completionRate(completed, projectTasks.length)),
      cellWithText(
        String(projectTasks.length),
        `${completed} completed`,
        "numeric",
      ),
    ]);
  });
  elements.projectRows.replaceChildren(
    ...(rows.length > 0 ? rows : [emptyRow(4, "No projects are available")]),
  );
}

function renderTasks() {
  if (!dashboard) return;
  const search = elements.taskSearch.value.trim().toLowerCase();
  const visible = dashboard.tasks
    .filter((task) =>
      taskFilter === "all"
        ? true
        : taskFilter === "done"
          ? task.done
          : !task.done,
    )
    .filter(
      (task) =>
        search === "" ||
        task.title.toLowerCase().includes(search) ||
        task.name.toLowerCase().includes(search),
    )
    .toSorted((left, right) =>
      left.done !== right.done
        ? Number(left.done) - Number(right.done)
        : left.project_id !== right.project_id
          ? Number(left.project_id - right.project_id)
          : Number(left.id - right.id),
    );
  const rows = visible.map((task) =>
    tableRow([
      cellWithBadge(
        task.done ? "Done" : "Open",
        task.done ? "task-done" : "task-open",
      ),
      cellWithText(task.title, task.done ? "Completed" : "Ready for work"),
      cellWithText(task.name, humanizeStatus(task.status)),
      cellWithText(String(task.id), undefined, "numeric"),
    ]),
  );
  elements.taskRows.replaceChildren(
    ...(rows.length > 0
      ? rows
      : [emptyRow(4, "No tasks match this local filter")]),
  );
  const suffix = rows.length === 1 ? "task" : "tasks";
  elements.taskSummary.textContent = `${rows.length} ${suffix} · joined and ordered by the columnar engine, filtered on the page`;
}

function selectFilter(button: HTMLButtonElement) {
  const requested = button.dataset.filter;
  if (requested !== "all" && requested !== "open" && requested !== "done")
    return;
  taskFilter = requested;
  for (const filter of Array.from(
    document.querySelectorAll<HTMLButtonElement>(".filter"),
  )) {
    filter.classList.toggle("is-selected", filter === button);
  }
  renderTasks();
}

function cellWithText(
  primary: string,
  secondary?: string,
  className?: string,
): HTMLTableCellElement {
  const cell = document.createElement("td");
  if (className) cell.className = className;
  const strong = document.createElement("strong");
  strong.textContent = primary;
  cell.append(strong);
  if (secondary) {
    const small = document.createElement("small");
    small.textContent = secondary;
    cell.append(small);
  }
  return cell;
}

function cellWithBadge(label: string, className: string): HTMLTableCellElement {
  const cell = document.createElement("td");
  const badge = document.createElement("span");
  badge.className = `status-pill ${className}`;
  badge.textContent = label;
  cell.append(badge);
  return cell;
}

function progressCell(rate: number): HTMLTableCellElement {
  const cell = document.createElement("td");
  const wrapper = document.createElement("div");
  wrapper.className = "progress-cell";
  const track = document.createElement("span");
  track.className = "progress-track";
  const fill = document.createElement("span");
  fill.style.width = `${rate}%`;
  track.append(fill);
  const label = document.createElement("small");
  label.textContent = `${rate}%`;
  wrapper.append(track, label);
  cell.append(wrapper);
  return cell;
}

function tableRow(cells: readonly HTMLTableCellElement[]): HTMLTableRowElement {
  const row = document.createElement("tr");
  row.append(...cells);
  return row;
}

function emptyRow(columns: number, message: string): HTMLTableRowElement {
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.colSpan = columns;
  cell.className = "empty";
  cell.textContent = message;
  row.append(cell);
  return row;
}

function completionRate(completed: number, total: number): number {
  return total === 0 ? 0 : Math.round((100 * completed) / total);
}

function humanizeStatus(value: string): string {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function cssToken(value: string): string {
  return value
    .replaceAll("_", "-")
    .replace(/[^a-z0-9-]/giu, "")
    .toLowerCase();
}

function statusLabel(state: PageState): string {
  if (state === "initializing") return "Connecting";
  if (state === "ready") return "Live";
  if (state === "error") return "Attention needed";
  return "Closed";
}

function shortCursor(cursor: string): string {
  return cursor.length > 12
    ? `${cursor.slice(0, 6)}…${cursor.slice(-6)}`
    : cursor;
}

function showNotice(message: string, error: boolean) {
  elements.notice.textContent = message;
  elements.notice.dataset.kind = error ? "error" : "info";
}

function formatError(error: unknown): string {
  const messages: string[] = [];
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (
    current instanceof Error &&
    messages.length < 4 &&
    !seen.has(current)
  ) {
    seen.add(current);
    messages.push(`${current.name}: ${current.message}`);
    current = current.cause;
  }
  return messages.length > 0 ? messages.join(" · ") : "Unknown demo error";
}

function requiredElement<T extends Element>(
  id: string,
  constructor: new (...arguments_: never[]) => T,
): T {
  const element = document.getElementById(id);
  if (!(element instanceof constructor)) {
    throw new Error(`Manual harness element ${id} is missing`);
  }
  return element;
}

elements.taskSearch.addEventListener("input", renderTasks);
for (const filter of Array.from(
  document.querySelectorAll<HTMLButtonElement>(".filter"),
)) {
  filter.addEventListener("click", () => selectFilter(filter));
}

globalThis.__calqueDemoMutate = async (id, title) => {
  try {
    const result = await client.db.tasks.update({ id: BigInt(id) }, { title });
    return { mutationId: result.mutationId, state: result.state };
  } catch (error) {
    const failure = error as {
      readonly mutationId?: string;
      readonly code?: string;
      readonly message?: string;
    };
    return {
      mutationId: failure.mutationId ?? "",
      state: failure.code ?? "unknown",
      failure: failure.message ?? String(error),
    };
  }
};

globalThis.__calqueDemoMutationState = (mutationId) =>
  client.mutationStatus(mutationId)?.state;

globalThis.__calqueDemoTaskTitles = async () => {
  const rows = await client.db.tasks
    .query()
    .select("id", "title")
    .orderBy("id")
    .run();
  return rows.map((row) => [String(row.id), row.title]);
};

globalThis.addEventListener("pagehide", () => {
  disposed = true;
  void client.close();
});

setState("initializing", "Loading the working set…");
client.subscribe(onStatus);
client.subscribePublications(onPublication);
onStatus();
