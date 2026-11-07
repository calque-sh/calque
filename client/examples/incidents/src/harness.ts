import "./main.js";
import type { MutationStatus } from "@calque/client";
import { client } from "./client.js";

// The entry of `vite build --mode harness`, the build the benchmark and the
// end-to-end test drive. It reads the page from its DOM and the client's
// public listeners, and publishes what they check on `globalThis.__incidents`.
// Times are `performance.now()`.

interface GridView {
  readonly level: string;
  readonly sort: string;
}

interface ShownIssue {
  readonly key: string;
  readonly detail: {
    readonly title: string;
    readonly status: string;
    readonly priority: string;
    readonly project: string;
    readonly slug: string;
    readonly assignee: string | null;
  } | null;
  readonly comments: readonly {
    readonly id: string;
    readonly body: string;
    readonly author: string;
    readonly createdAt: string;
  }[];
  readonly tags: readonly string[];
  readonly eventCount: number;
  readonly eventIds: readonly string[];
}

interface TrackedWrite {
  readonly sequence: number;
  readonly collection: "comments" | "issues";
  readonly operation: "insert" | "update" | "delete";
  key: string | null;
  /**
   * What the page shows for the row once the write applies; `null` once
   * deleted.
   */
  readonly expected: string | null;
  readonly calledAt: number;
  submittedAt: number | null;
  mutationId: string | null;
  state: MutationStatus["state"] | "calling";
  canonicalAt: number | null;
  optimisticShownAt: number | null;
  confirmedShownAt: number | null;
  failure: string | null;
}

interface ShownValue {
  readonly kind: "issue-title" | "issue-status";
  readonly value: string;
  readonly domAt: number;
  paintedAt: number | null;
}

const MAX_RECORDS = 512;
const ROW_HEIGHT = 34;
const ID_COLUMN = 1;

const coldLoad = {
  readyAt: null as number | null,
  firstWindowAt: null as number | null,
  gridPaintedAt: null as number | null,
  chartAt: null as number | null,
  issueAt: null as number | null,
};
const writes: TrackedWrite[] = [];
/** The comments on screen when an insert was asked for, to find the new one. */
const commentsBefore = new WeakMap<TrackedWrite, ReadonlySet<string>>();
const shownValues: ShownValue[] = [];
let publications = 0;
let publicationLog: readonly {
  readonly commitTimeUnixMicros: string | null;
  readonly at: number;
  readonly changedCollections: readonly string[];
}[] = [];

const text = (element: Element | null | undefined) =>
  element?.textContent ?? "";

function afterPaint(then: (at: number) => void): void {
  requestAnimationFrame(() =>
    requestAnimationFrame(() => then(performance.now())),
  );
}

function gridPanel(): Element | null {
  return document.querySelector("section.grid-panel");
}

function readGrid() {
  const panel = gridPanel();
  const table = panel?.querySelector("table[role=grid]");
  if (!panel || !table || panel.getAttribute("aria-busy") !== "false")
    return null;
  const rows = Number(table.getAttribute("aria-rowcount")) - 1;
  const visible = [...table.querySelectorAll("tbody tr")];
  const top = Number(visible[0]?.getAttribute("aria-rowindex") ?? 2) - 2;
  const ids = visible.map((row, index) =>
    text(row.querySelector(`#glide-cell-${ID_COLUMN}-${top + index}`)),
  );
  if (ids.some((id) => id === "")) return null;
  const select = (name: string) =>
    panel.querySelector<HTMLSelectElement>(`select[data-control=${name}]`)
      ?.value ?? "";
  const view: GridView = { level: select("level"), sort: select("sort") };
  return { view, rows, top, ids };
}

function readChart() {
  const bars = document.querySelectorAll(
    "section.chart-panel g[data-status] rect[data-count]",
  );
  if (bars.length === 0) return null;
  return [...bars].map((bar) => ({
    status: bar.parentElement?.getAttribute("data-status") ?? "",
    priority: bar.getAttribute("data-priority") ?? "",
    count: Number(bar.getAttribute("data-count")),
  }));
}

function readIssue(): ShownIssue | null {
  const view = document.querySelector("article.issue-view");
  if (!view || view.querySelector("[aria-busy=true]")) return null;
  const field = (name: string) => view.querySelector(`[data-field=${name}]`);
  const title = field("title");
  return {
    key: view.getAttribute("data-issue") ?? "",
    detail: title
      ? {
          title: text(title),
          status: text(field("status")),
          priority:
            view.querySelector<HTMLSelectElement>(
              "select[data-control=issue-priority]",
            )?.value ?? "",
          project: text(field("project")),
          slug: text(field("slug")),
          assignee: field("assignee") ? text(field("assignee")) : null,
        }
      : null,
    comments: [...view.querySelectorAll("li[data-comment-id]")].map(
      (comment) => ({
        id: comment.getAttribute("data-comment-id") ?? "",
        body: text(comment.querySelector("p")),
        author: text(comment.querySelector("strong")),
        createdAt: comment.querySelector("time")?.dateTime ?? "",
      }),
    ),
    tags: [...view.querySelectorAll(".tag")].map((tag) => text(tag)),
    eventCount: Number(view.querySelector("h3 data")?.getAttribute("value")),
    eventIds: [...view.querySelectorAll("tr[data-event-id]")].map(
      (row) => row.getAttribute("data-event-id") ?? "",
    ),
  };
}

/** What the page shows for a written row, or `undefined` when it is gone. */
function shownFor(write: TrackedWrite): string | undefined {
  if (write.collection === "issues") {
    const view = document.querySelector(`article[data-issue="${write.key}"]`);
    return view ? text(view.querySelector("[data-field=status]")) : undefined;
  }
  if (write.key === null) {
    const added = [...document.querySelectorAll("li[data-comment-id]")].find(
      (comment) =>
        !commentsBefore
          .get(write)
          ?.has(comment.getAttribute("data-comment-id") ?? "") &&
        text(comment.querySelector("p")) === write.expected,
    );
    if (!added) return undefined;
    write.key = added.getAttribute("data-comment-id");
  }
  const comment = document.querySelector(`li[data-comment-id="${write.key}"]`);
  return comment ? text(comment.querySelector("p")) : undefined;
}

function observeWrites(now: number): void {
  for (const write of writes) {
    if (write.confirmedShownAt !== null || write.mutationId === null) continue;
    const shown = shownFor(write);
    const reflected =
      write.expected === null ? shown === undefined : shown === write.expected;
    if (!reflected) continue;
    if (write.canonicalAt === null) write.optimisticShownAt ??= now;
    else write.confirmedShownAt = now;
  }
}

let lastTitle: string | undefined;
let lastStatus: string | undefined;

function record(kind: ShownValue["kind"], value: string, now: number): void {
  const shown: ShownValue = { kind, value, domAt: now, paintedAt: null };
  shownValues.push(shown);
  if (shownValues.length > MAX_RECORDS) shownValues.shift();
  afterPaint((at) => {
    shown.paintedAt = at;
  });
}

function observe(): void {
  const now = performance.now();
  if (
    coldLoad.firstWindowAt === null &&
    gridPanel()?.getAttribute("aria-busy") === "false"
  ) {
    coldLoad.firstWindowAt = now;
    afterPaint((at) => {
      coldLoad.gridPaintedAt = at;
    });
  }
  if (coldLoad.chartAt === null && readChart() !== null) coldLoad.chartAt = now;
  if (coldLoad.issueAt === null && readIssue() !== null) coldLoad.issueAt = now;
  const title = document.querySelector("article.issue-view [data-field=title]");
  const status = document.querySelector(
    "article.issue-view [data-field=status]",
  );
  if (title && text(title) !== lastTitle) {
    lastTitle = text(title);
    record("issue-title", lastTitle, now);
  }
  if (status && text(status) !== lastStatus) {
    lastStatus = text(status);
    record("issue-status", lastStatus, now);
  }
  observeWrites(now);
}

new MutationObserver(observe).observe(document.body, {
  subtree: true,
  childList: true,
  characterData: true,
  attributes: true,
});

function intend(
  collection: TrackedWrite["collection"],
  operation: TrackedWrite["operation"],
  key: string | null,
  expected: string | null,
): void {
  const write: TrackedWrite = {
    sequence: writes.length + 1,
    collection,
    operation,
    key,
    expected,
    calledAt: performance.now(),
    submittedAt: null,
    mutationId: null,
    state: "calling",
    canonicalAt: null,
    optimisticShownAt: null,
    confirmedShownAt: null,
    failure: null,
  };
  writes.push(write);
  commentsBefore.set(
    write,
    new Set(
      [...document.querySelectorAll("li[data-comment-id]")].map(
        (comment) => comment.getAttribute("data-comment-id") ?? "",
      ),
    ),
  );
}

document.addEventListener(
  "click",
  (event) => {
    const button = (event.target as Element).closest("button[data-action]");
    const comment = button?.closest("li[data-comment-id]");
    const key = comment?.getAttribute("data-comment-id") ?? null;
    switch (button?.getAttribute("data-action")) {
      case "add-comment": {
        const draft = document.querySelector<HTMLTextAreaElement>(
          "textarea[data-control=new-comment]",
        );
        intend("comments", "insert", null, draft?.value.trim() ?? "");
        break;
      }
      case "save": {
        const edit = comment?.querySelector("textarea");
        intend("comments", "update", key, edit?.value.trim() ?? "");
        break;
      }
      case "delete":
        intend("comments", "delete", key, null);
        break;
    }
  },
  { capture: true },
);

document.addEventListener(
  "change",
  (event) => {
    const select = event.target as HTMLSelectElement;
    if (select.getAttribute("data-control") !== "issue-status") return;
    const issue = select.closest("article[data-issue]");
    intend(
      "issues",
      "update",
      issue?.getAttribute("data-issue") ?? null,
      select.value,
    );
  },
  { capture: true },
);

client.subscribeMutations((status) => {
  const now = performance.now();
  let write = writes.find(
    (candidate) => candidate.mutationId === status.mutationId,
  );
  if (!write) {
    write = writes.find((candidate) => candidate.mutationId === null);
    if (!write) return;
    write.mutationId = status.mutationId;
  }
  write.state = status.state;
  if (status.state === "rejected" || status.state === "conflict")
    write.failure = status.result.message;
  if (status.state === "ambiguous") write.failure = status.error.message;
  if (status.state === "canonical") {
    write.canonicalAt ??= now;
    observeWrites(now);
  }
});

client.subscribePublications((publication) => {
  if (publication.changedCollections.length === 0) return;
  publications += 1;
  publicationLog = [
    ...publicationLog,
    {
      commitTimeUnixMicros:
        publication.latestCommitTimeUnixMicros === null
          ? null
          : String(publication.latestCommitTimeUnixMicros),
      at: performance.now(),
      changedCollections: publication.changedCollections,
    },
  ].slice(-MAX_RECORDS);
});

client.subscribe(() => {
  if (client.status.state === "ready") coldLoad.readyAt ??= performance.now();
});

const probe = {
  coldLoad,
  get status() {
    return client.status.state;
  },
  get error() {
    const status = client.status;
    if ("error" in status) return status.error.message;
    const shown = document.querySelector("p.error");
    return shown ? text(shown) : null;
  },
  get grid() {
    return readGrid();
  },
  get chart() {
    return readChart();
  },
  get issue() {
    return readIssue();
  },
  get publications() {
    return publications;
  },
  get publicationLog() {
    return publicationLog;
  },
  mutations: () => writes.map((write) => ({ ...write })),
  shown: () => shownValues.map((value) => ({ ...value })),
  scrollGrid: (row: number) => {
    const scroller = gridPanel()?.querySelector(".dvn-scroller");
    if (!scroller) throw new Error("the grid is not mounted");
    scroller.scrollTop = row * ROW_HEIGHT;
  },
  /** Called by a test's `fetch` wrapper as `POST /v1/mutations` leaves. */
  submitted: () => {
    const write = writes.find(
      (candidate) =>
        candidate.mutationId !== null && candidate.submittedAt === null,
    );
    if (write) write.submittedAt = performance.now();
  },
  db: client.db,
};

declare global {
  // Only a `var` declaration joins `typeof globalThis`.
  var __incidents: typeof probe;
}
globalThis.__incidents = probe;
