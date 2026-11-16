import type { SyncStatus } from "@calque/client";
import { SyncProvider, useLiveQuery, useSyncStatus } from "@calque/react";
import { useState } from "react";
import { createPortal } from "react-dom";
import { createRoot } from "react-dom/client";
import { client, db } from "../client.js";
import { Events, type Filter } from "./events.js";
import { Facets } from "./facets.js";
import { Issues } from "./issues.js";
import { useProjects } from "./projects.js";
import { useReady } from "./ready.js";

const STATUS_LABELS: Record<SyncStatus["state"], string> = {
  initializing: "Connecting",
  preparing: "Preparing",
  ready: "Live",
  error: "Stopped",
  locked: "Signed out",
  closed: "Closed",
};

const counts = [
  db.comments.query().count(),
  db.events.query().count(),
  db.issue_tags.query().count(),
  db.issues.query().count(),
  db.memberships.query().count(),
  db.projects.query().count(),
  db.users.query().count(),
] as const;

function Status() {
  const status = useSyncStatus();
  // `counts` never changes length, so each render calls the same hooks.
  const tables = counts.map((count) => useLiveQuery(count));
  const rows = tables.every((table) => table.status === "ready")
    ? tables.reduce((sum, table) => sum + (table.result[0]?.count ?? 0), 0)
    : undefined;
  useReady("rows", rows !== undefined);
  return (
    <>
      <span className="sync" data-state={status.state}>
        {STATUS_LABELS[status.state]}
      </span>
      {rows === undefined ? null : (
        <span className="rows" data-rows={rows}>
          {rows.toLocaleString("en-US")} rows in this tab
        </span>
      )}
      {"error" in status ? (
        <span className="failure" role="alert">
          {status.error.message}
        </span>
      ) : null}
    </>
  );
}

function Console() {
  const [filter, setFilter] = useState<Filter>({
    level: undefined,
    service: undefined,
    order: "newest",
    since: undefined,
  });
  const projects = useProjects();
  const status = document.getElementById("status");
  return (
    <>
      {status ? createPortal(<Status />, status) : null}
      <Facets filter={filter} projects={projects} onFilter={setFilter} />
      <Events filter={filter} projects={projects} onFilter={setFilter} />
      <Issues />
    </>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("the page has no #root element");
createRoot(root).render(
  <SyncProvider client={client}>
    <Console />
  </SyncProvider>,
);
