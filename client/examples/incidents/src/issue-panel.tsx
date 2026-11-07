import { and, eq } from "@calque/client";
import { useLiveQuery, usePendingWrite } from "@calque/react";
import { useState } from "react";
import type { CommentsInsert, IssuesPatch } from "./calque.generated.js";
import { db } from "./client.js";
import { STATUSES } from "./status-chart.js";

const PRIORITIES = [1, 2, 3, 4, 5] as const;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/iu;

const openIssues = db.issues
  .query()
  .join(db.projects, "project", { left: "project_id", right: "id" })
  .join(db.users, "assignee", { left: "assignee_id", right: "id" }, "left")
  .where(and(eq("status", "unresolved"), eq("is_sentinel", false)))
  .select(
    "id",
    "title",
    "priority",
    "updated_at",
    "project.slug",
    "assignee.display_name",
  )
  .orderBy("priority", "desc")
  .orderBy("updated_at", "desc")
  .orderBy("id")
  .limit(40);

const actingUser = db.users
  .query()
  .where(and(eq("active", true), eq("is_sentinel", false)))
  .select("id", "display_name")
  .orderBy("id")
  .limit(1);

function issueInAddress(): string | undefined {
  const issue = new URL(location.href).searchParams.get("issue");
  return issue !== null && UUID.test(issue) ? issue : undefined;
}

export function IssuePanel() {
  const [chosen, setChosen] = useState(issueInAddress);
  const issues = useLiveQuery(openIssues);
  const list = issues.status === "ready" ? issues.result : [];
  const selected = chosen ?? list[0]?.id;

  const choose = (id: string) => {
    setChosen(id);
    const url = new URL(location.href);
    url.searchParams.set("issue", id);
    history.replaceState(null, "", url);
  };

  return (
    <section className="panel issue-panel" aria-label="Issues">
      <div className="issue-list">
        <h2>Open issues</h2>
        {issues.status === "error" ? (
          <p className="error">{issues.error.message}</p>
        ) : null}
        <ol>
          {list.map((issue) => (
            <li key={issue.id}>
              <button
                type="button"
                data-issue={issue.id}
                aria-current={issue.id === selected}
                onClick={() => choose(issue.id)}
              >
                <span className={`priority p${issue.priority}`}>
                  P{issue.priority}
                </span>
                <strong>{issue.title}</strong>
                <small>
                  {issue.slug} · {issue.display_name ?? "unassigned"}
                </small>
              </button>
            </li>
          ))}
        </ol>
      </div>
      {selected ? (
        <article key={selected} className="issue-view" data-issue={selected}>
          <IssueHeader id={selected} />
          <Tags id={selected} />
          <Comments id={selected} />
          <LatestEvents id={selected} />
        </article>
      ) : (
        <p className="muted">Choosing an issue…</p>
      )}
    </section>
  );
}

function IssueHeader({ id }: { readonly id: string }) {
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
        "event_count",
        "updated_at",
        "project.name",
        "project.slug",
        "assignee.display_name",
      ),
  );
  if (detail.status === "error")
    return <p className="error">{detail.error.message}</p>;
  if (detail.status === "loading")
    return (
      <p className="muted" aria-busy="true">
        Joining the issue to its project and assignee…
      </p>
    );
  const issue = detail.result[0];
  if (!issue)
    return <p className="muted">This issue is not in the local working set.</p>;

  const update = (patch: IssuesPatch) => {
    void db.issues
      .update({ id }, { ...patch, updated_at: new Date().toISOString() })
      .catch(reportError);
  };

  return (
    <header className="issue-header">
      <div>
        <p className="eyebrow">
          <span data-field="project">{issue.name}</span> ·{" "}
          <span data-field="slug">{issue.slug}</span>
        </p>
        <h2 data-field="title">{issue.title}</h2>
        <p className="muted">
          {issue.display_name ? (
            <>
              Assigned to{" "}
              <span data-field="assignee">{issue.display_name}</span>
            </>
          ) : (
            "Unassigned"
          )}{" "}
          · {issue.event_count} events recorded · updated{" "}
          <Timestamp value={issue.updated_at} />
        </p>
      </div>
      <div className="issue-controls">
        <span className={`status ${issue.status}`} data-field="status">
          {issue.status}
        </span>
        <select
          data-control="issue-status"
          aria-label="Status"
          value={issue.status}
          onChange={(event) => update({ status: event.target.value })}
        >
          {STATUSES.map((status) => (
            <option key={status}>{status}</option>
          ))}
        </select>
        <select
          data-control="issue-priority"
          aria-label="Priority"
          value={issue.priority}
          onChange={(event) => update({ priority: Number(event.target.value) })}
        >
          {PRIORITIES.map((priority) => (
            <option key={priority} value={priority}>
              P{priority}
            </option>
          ))}
        </select>
        <WriteState collection="issues" id={id} />
      </div>
    </header>
  );
}

function Tags({ id }: { readonly id: string }) {
  const tags = useLiveQuery(
    db.issue_tags
      .query()
      .where(eq("issue_id", id))
      .select("tag")
      .orderBy("tag"),
  );
  return (
    <div className="tags" aria-busy={tags.status === "loading"}>
      {tags.status === "ready"
        ? tags.result.map(({ tag }) => (
            <span key={tag} className="tag">
              {tag}
            </span>
          ))
        : null}
    </div>
  );
}

function Comments({ id }: { readonly id: string }) {
  const comments = useLiveQuery(
    db.comments
      .query()
      .join(db.users, "author", { left: "author_id", right: "id" })
      .where(eq("issue_id", id))
      .select("id", "body", "created_at", "author.display_name")
      .orderBy("created_at")
      .orderBy("id"),
  );
  const user = useLiveQuery(actingUser);
  const author = user.status === "ready" ? user.result[0] : undefined;
  const [draft, setDraft] = useState("");
  const [editing, setEditing] = useState<{ id: string; body: string }>();

  const add = () => {
    const body = draft.trim();
    if (!author || !body) return;
    const comment = newComment(id, author.id, body);
    setDraft("");
    void db.comments.insert(comment).catch(reportError);
  };
  const save = () => {
    const body = editing?.body.trim();
    if (!editing || !body) return;
    setEditing(undefined);
    void db.comments.update({ id: editing.id }, { body }).catch(reportError);
  };

  if (comments.status === "error")
    return <p className="error">{comments.error.message}</p>;
  return (
    <>
      <h3>
        Comments{" "}
        <small>
          {comments.status === "ready"
            ? comments.result.length.toLocaleString()
            : "…"}
        </small>
      </h3>
      <ul className="comments" aria-busy={comments.status === "loading"}>
        {(comments.status === "ready" ? comments.result : []).map((comment) => (
          <li key={comment.id} data-comment-id={comment.id}>
            <div className="comment-meta">
              <strong>{comment.display_name}</strong>
              <Timestamp value={comment.created_at} />
              <WriteState collection="comments" id={comment.id} />
            </div>
            {editing?.id === comment.id ? (
              <div className="comment-edit">
                <textarea
                  data-control="comment-edit"
                  value={editing.body}
                  onChange={(event) =>
                    setEditing({ id: comment.id, body: event.target.value })
                  }
                />
                <button type="button" data-action="save" onClick={save}>
                  Save
                </button>
                <button type="button" onClick={() => setEditing(undefined)}>
                  Cancel
                </button>
              </div>
            ) : (
              <>
                <p>{comment.body}</p>
                <div className="comment-actions">
                  <button
                    type="button"
                    data-action="edit"
                    onClick={() =>
                      setEditing({ id: comment.id, body: comment.body })
                    }
                  >
                    Edit
                  </button>
                  <button
                    type="button"
                    data-action="delete"
                    onClick={() =>
                      void db.comments
                        .delete({ id: comment.id })
                        .catch(reportError)
                    }
                  >
                    Delete
                  </button>
                </div>
              </>
            )}
          </li>
        ))}
      </ul>
      <div className="composer">
        <textarea
          data-control="new-comment"
          placeholder={
            author ? `Comment as ${author.display_name}` : "Loading users…"
          }
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
        />
        <button
          type="button"
          data-action="add-comment"
          disabled={!author || !draft.trim()}
          onClick={add}
        >
          Add comment
        </button>
      </div>
    </>
  );
}

function LatestEvents({ id }: { readonly id: string }) {
  const count = useLiveQuery(
    db.events.query().where(eq("issue_id", id)).count(),
  );
  const events = useLiveQuery(
    db.events
      .query()
      .where(eq("issue_id", id))
      .select("id", "level", "message", "occurred_at")
      .orderBy("occurred_at", "desc")
      .orderBy("id", "desc")
      .limit(12),
  );
  const total = count.status === "ready" ? (count.result[0]?.count ?? 0) : null;
  return (
    <>
      <h3>
        Latest events{" "}
        <small aria-busy={total === null}>
          {total === null ? (
            "…"
          ) : (
            <>
              <data value={total}>{total.toLocaleString()}</data> for this issue
            </>
          )}
        </small>
      </h3>
      <table className="issue-events" aria-busy={events.status === "loading"}>
        <tbody>
          {events.status === "ready"
            ? events.result.map((event) => (
                <tr key={String(event.id)} data-event-id={String(event.id)}>
                  <td className={`level ${event.level}`}>{event.level}</td>
                  <td>{event.message}</td>
                  <td className="numeric">
                    <Timestamp value={event.occurred_at} />
                  </td>
                </tr>
              ))
            : null}
        </tbody>
      </table>
    </>
  );
}

function Timestamp({ value }: { readonly value: string }) {
  return <time dateTime={value}>{new Date(value).toLocaleString()}</time>;
}

function WriteState({
  collection,
  id,
}: {
  readonly collection: "comments" | "issues";
  readonly id: string;
}) {
  const write = usePendingWrite(collection, { id });
  if (!write) return null;
  const state =
    write.state === "submitting" || write.state === "awaiting_canonical_echo"
      ? "pending"
      : "failed";
  return <span className={`badge ${state}`}>{state}</span>;
}

function newComment(
  issueId: string,
  authorId: string,
  body: string,
): CommentsInsert {
  const now = new Date();
  return {
    id: crypto.randomUUID(),
    issue_id: issueId,
    author_id: authorId,
    body,
    created_at: now.toISOString(),
    // The fixture keeps `bench_index` unique; seeded rows count up from zero.
    bench_index:
      BigInt(now.getTime()) * 1_000_000n +
      BigInt(Math.floor(Math.random() * 1_000_000)),
    is_sentinel: false,
  };
}
