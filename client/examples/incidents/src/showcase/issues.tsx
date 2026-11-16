import { and, eq } from "@calque/client";
import { useLiveQuery, usePendingWrite } from "@calque/react";
import { useRef, useState } from "react";
import { db } from "../client.js";
import { STATUSES } from "../status-chart.js";
import { useElapsed, useReady } from "./ready.js";

const OPEN_SHOWN = 4;
const UPDATED = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const openIssues = db.issues
  .query()
  .join(db.projects, "project", { left: "project_id", right: "id" })
  .where(and(eq("status", "unresolved"), eq("is_sentinel", false)))
  .select("id", "title", "priority", "updated_at", "project.slug")
  .orderBy("priority", "desc")
  .orderBy("updated_at", "desc")
  .orderBy("id")
  .limit(OPEN_SHOWN + 1);

const byStatus = db.issues
  .query()
  .where(eq("is_sentinel", false))
  .groupBy("status")
  .count();

export function Issues() {
  const open = useLiveQuery(openIssues);
  const pinned = useRef<string>(undefined);
  if (pinned.current === undefined && open.status === "ready")
    pinned.current = open.result[0]?.id;
  const id = pinned.current;
  const rest =
    open.status === "ready"
      ? open.result.filter((issue) => issue.id !== id).slice(0, OPEN_SHOWN)
      : [];

  return (
    <aside className="issues" aria-label="Issues">
      <h2>Top open issue</h2>
      {id === undefined ? (
        <p className="quiet">{open.status === "ready" ? "None open" : ""}</p>
      ) : (
        <Issue id={id} />
      )}
      <Statuses />
      <h2>Next open issues</h2>
      <ol className="open">
        {rest.map((issue) => (
          <li key={issue.id} data-issue={issue.id}>
            <Priority value={issue.priority} />
            <span className="title">{issue.title}</span>
            <span className="service">{issue.slug}</span>
          </li>
        ))}
      </ol>
    </aside>
  );
}

function Issue({ id }: { readonly id: string }) {
  const detail = useLiveQuery(
    db.issues
      .query()
      .join(db.projects, "project", { left: "project_id", right: "id" })
      .join(db.users, "assignee", { left: "assignee_id", right: "id" }, "left")
      .where(eq("id", id))
      .select(
        "title",
        "status",
        "priority",
        "updated_at",
        "project.slug",
        "assignee.display_name",
      ),
  );
  const write = usePendingWrite("issues", { id });
  const [since, setSince] = useState<number>();
  const wrote = useRef(false);
  if (write !== undefined) wrote.current = true;
  const confirmed = useElapsed(
    String(since),
    since,
    wrote.current && write === undefined,
  );
  const issue = detail.status === "ready" ? detail.result[0] : undefined;
  useReady("issue", issue !== undefined);
  if (!issue) return <p className="quiet">Reading the issue</p>;

  const resolved = issue.status === "resolved";
  const setStatus = (status: string, timeStamp: number) => {
    wrote.current = false;
    setSince(timeStamp);
    void db.issues
      .update({ id }, { status, updated_at: new Date().toISOString() })
      .catch(reportError);
  };

  return (
    <article className="issue" data-issue={id} data-status={issue.status}>
      <h3>
        <Priority value={issue.priority} />
        {issue.title}
      </h3>
      <p>
        {issue.slug}, {issue.display_name ?? "unassigned"}
      </p>
      <p>Updated {UPDATED.format(new Date(issue.updated_at))}</p>
      <div className="act">
        <button
          type="button"
          data-action={resolved ? "reopen" : "resolve"}
          onClick={(input) =>
            setStatus(resolved ? "unresolved" : "resolved", input.timeStamp)
          }
        >
          {resolved ? "Reopen" : "Resolve"}
        </button>
        <WriteState write={write?.state} confirmed={confirmed} />
      </div>
    </article>
  );
}

function WriteState({
  write,
  confirmed,
}: {
  readonly write: string | undefined;
  readonly confirmed: number | undefined;
}) {
  if (write === "submitting" || write === "awaiting_canonical_echo")
    return <span className="saving">Saving to PostgreSQL</span>;
  if (write !== undefined)
    return (
      <span className="refused" role="alert">
        PostgreSQL refused the change ({write})
      </span>
    );
  if (confirmed === undefined) return null;
  return (
    <span className="confirmed" data-ms={Math.round(confirmed)}>
      Confirmed by PostgreSQL in {Math.max(1, Math.round(confirmed))} ms
    </span>
  );
}

function Statuses() {
  const counts = useLiveQuery(byStatus);
  useReady("statuses", counts.status === "ready");
  const count = (status: string) =>
    counts.status === "ready"
      ? (counts.result.find((group) => group.status === status)?.count ?? 0)
      : 0;
  const largest = Math.max(1, ...STATUSES.map(count));
  return (
    <>
      <h2>Issues by status</h2>
      <ul className="statuses">
        {STATUSES.map((status) => (
          <li key={status} data-status={status}>
            <span>{status[0]?.toUpperCase() + status.slice(1)}</span>
            <i style={{ width: `${(count(status) / largest) * 100}%` }} />
            <data value={count(status)}>
              {counts.status === "ready"
                ? count(status).toLocaleString("en-US")
                : ""}
            </data>
          </li>
        ))}
      </ul>
    </>
  );
}

function Priority({ value }: { readonly value: number }) {
  return (
    <span className="priority" data-priority={value}>
      P{value}
    </span>
  );
}
