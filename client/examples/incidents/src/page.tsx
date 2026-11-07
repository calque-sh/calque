import type { SyncStatus } from "@calque/client";
import { SyncProvider, useSyncStatus } from "@calque/react";
import { createRoot } from "react-dom/client";
import { client } from "./client.js";
import { EventsGrid } from "./events-grid.js";
import { IssuePanel } from "./issue-panel.js";
import { StatusChart } from "./status-chart.js";
import "./style.css";

const STATUS_LABELS: Record<SyncStatus["state"], string> = {
  initializing: "Connecting",
  preparing: "Preparing",
  ready: "Live",
  error: "Stopped",
  locked: "Signed out",
  closed: "Closed",
};

function Header() {
  const status = useSyncStatus();
  return (
    <>
      <header className="page-header">
        <div>
          <p className="eyebrow">Calque</p>
          <h1>Incident operations</h1>
        </div>
        <output className="sync">
          <span className="dot" data-state={status.state} />
          {STATUS_LABELS[status.state]}
        </output>
      </header>
      {status.state === "preparing" ? (
        <p className="notice">calque is preparing the working set…</p>
      ) : null}
      {"error" in status ? (
        <p className="notice error" role="alert">
          {status.error.message}
        </p>
      ) : null}
    </>
  );
}

function Page() {
  return (
    <main>
      <Header />
      <div className="layout">
        <EventsGrid />
        <StatusChart />
        <IssuePanel />
      </div>
    </main>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("the page has no #root element");
createRoot(root).render(
  <SyncProvider client={client}>
    <Page />
  </SyncProvider>,
);
