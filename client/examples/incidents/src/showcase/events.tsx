import { and, eq, or } from "@calque/client";
import { useLiveQuery, useLiveWindow } from "@calque/react";
import { useLayoutEffect, useRef, useState } from "react";
import { db } from "../client.js";
import type { Projects } from "./projects.js";
import { useElapsed, useReady } from "./ready.js";

export interface Filter {
  readonly level: string | undefined;
  readonly service:
    { readonly name: string; readonly projects: readonly string[] } | undefined;
  readonly order: "newest" | "oldest";
  /** The `timeStamp` of the input that set this filter. */
  readonly since: number | undefined;
}

type Predicate = Parameters<ReturnType<typeof db.events.query>["where"]>[0];

/** The events `filter` selects, less its `level` or `service` condition. */
export function matching(filter: Filter, ignore?: "level" | "service") {
  const conditions: Predicate[] = [eq("is_sentinel", false)];
  if (filter.level !== undefined && ignore !== "level")
    conditions.push(eq("level", filter.level));
  if (filter.service !== undefined && ignore !== "service")
    conditions.push(
      or(
        ...filter.service.projects.map((id): Predicate => eq("project_id", id)),
      ),
    );
  return db.events.query().where(and(...conditions));
}

const ROW = 28;
/** Rows per block; the list reads the block in view and one on each side. */
const BLOCK = 64;
const TIME = new Intl.DateTimeFormat("en-GB", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

export function Events({
  filter,
  projects,
  onFilter,
}: {
  readonly filter: Filter;
  readonly projects: Projects | undefined;
  readonly onFilter: (filter: Filter) => void;
}) {
  const scroller = useRef<HTMLDivElement>(null);
  const [top, setTop] = useState(0);
  const [height, setHeight] = useState(0);
  useLayoutEffect(() => {
    const element = scroller.current;
    if (!element) return;
    const observer = new ResizeObserver(() => setHeight(element.clientHeight));
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const first = Math.floor(top / ROW);
  const offset = Math.max(0, (Math.floor(first / BLOCK) - 1) * BLOCK);
  const direction = filter.order === "newest" ? "desc" : "asc";
  const shown = useLiveWindow(
    matching(filter)
      .select("id", "level", "message", "occurred_at", "project_id")
      .orderBy("occurred_at", direction)
      .orderBy("id", direction),
    offset,
    3 * BLOCK,
  );
  const events = shown.status === "ready" ? shown.result : undefined;
  useReady("events", events !== undefined && projects !== undefined);
  const key = `${filter.level}|${filter.service?.name}|${filter.order}`;
  useLayoutEffect(() => {
    if (scroller.current) scroller.current.scrollTop = 0;
    setTop(0);
  }, [key]);
  const elapsed = useElapsed(
    key,
    filter.since,
    shown.status === "ready" && !shown.stale,
  );
  const arrivedAfter = useFirstNewestId();

  const total = events?.total ?? 0;
  const last = Math.min(total, first + Math.ceil(height / ROW));
  const rows = [];
  for (let index = first; index < last; index += 1) {
    const event = events?.rows[index - events.offset];
    rows.push(
      event === undefined ? (
        <div key={index} className="event" style={{ top: index * ROW }}>
          <span className="placeholder" />
        </div>
      ) : (
        <div
          key={index}
          className="event"
          style={{ top: index * ROW }}
          data-level={event.level}
          data-arrived={
            arrivedAfter !== undefined && event.id > arrivedAfter
              ? ""
              : undefined
          }
        >
          <time dateTime={event.occurred_at}>
            {TIME.format(new Date(event.occurred_at))}
          </time>
          <span className="level">{event.level}</span>
          <span className="service">
            {projects?.slug.get(event.project_id)}
          </span>
          <span className="message">{event.message}</span>
        </div>
      ),
    );
  }

  return (
    <section className="events" data-region="events" aria-label="Events">
      <header>
        <h2>Events</h2>
        <p className="count" aria-live="polite">
          {events === undefined ? (
            "Reading every event"
          ) : (
            <>
              <b data-total={total}>{total.toLocaleString("en-US")}</b>
              {filter.level === undefined && filter.service === undefined
                ? " events"
                : " match"}
              {elapsed === undefined ? null : (
                <span className="elapsed" data-ms={Math.round(elapsed)}>
                  , filtered and sorted in {Math.max(1, Math.round(elapsed))} ms
                </span>
              )}
            </>
          )}
        </p>
        <div className="order" role="group" aria-label="Order">
          {(["newest", "oldest"] as const).map((order) => (
            <button
              key={order}
              type="button"
              data-order={order}
              aria-pressed={filter.order === order}
              onClick={(input) =>
                onFilter({ ...filter, order, since: input.timeStamp })
              }
            >
              {order === "newest" ? "Newest first" : "Oldest first"}
            </button>
          ))}
        </div>
      </header>
      <div
        ref={scroller}
        className="scroller"
        onScroll={(scroll) => setTop(scroll.currentTarget.scrollTop)}
      >
        <div className="rows" style={{ height: total * ROW }}>
          {rows}
        </div>
      </div>
      <footer>
        {events === undefined
          ? null
          : total === 0
            ? "No events match"
            : `Rows ${(first + 1).toLocaleString("en-US")} to ${last.toLocaleString("en-US")} of ${total.toLocaleString("en-US")}, all of them in the page`}
      </footer>
    </section>
  );
}

const newestId = db.events.query().max("id");

/** The newest event id when the page first read the events. */
function useFirstNewestId(): bigint | undefined {
  const newest = useLiveQuery(newestId);
  const first = useRef<bigint | null | undefined>(undefined);
  if (first.current === undefined && newest.status === "ready")
    first.current = newest.result[0]?.id ?? null;
  return first.current ?? undefined;
}
