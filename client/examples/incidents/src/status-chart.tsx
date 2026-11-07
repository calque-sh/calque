import { useLiveQuery } from "@calque/react";
import { db } from "./client.js";

export const STATUSES = [
  "unresolved",
  "investigating",
  "resolved",
  "ignored",
  "archived",
] as const;

const PRIORITY_COLORS: Readonly<Record<string, string>> = {
  "1": "#c7d2cc",
  "2": "#8fb3a3",
  "3": "#4f8f78",
  "4": "#e0a44a",
  "5": "#c2413b",
};
const WIDTH = 520;
const BAR_HEIGHT = 30;
const GAP = 14;
const LABEL_WIDTH = 108;
const PLOT_WIDTH = WIDTH - LABEL_WIDTH - 64;

export function StatusChart() {
  const counts = useLiveQuery(
    db.issues.query().groupBy("status", "priority").count().orderBy("priority"),
  );

  return (
    <section className="panel chart-panel" aria-label="Issues by status">
      <div className="panel-heading">
        <h2>Issues by status and priority</h2>
        <div className="legend" aria-hidden="true">
          {Object.entries(PRIORITY_COLORS).map(([priority, color]) => (
            <span key={priority}>
              <i style={{ background: color }} />P{priority}
            </span>
          ))}
        </div>
      </div>
      {counts.status === "error" ? (
        <p className="error">{counts.error.message}</p>
      ) : counts.status === "loading" ? (
        <p className="muted">Grouping every issue…</p>
      ) : (
        <Bars counts={counts.result} />
      )}
    </section>
  );
}

function Bars({
  counts,
}: {
  readonly counts: readonly {
    readonly status: string;
    readonly priority: number;
    readonly count: number;
  }[];
}) {
  const bars = STATUSES.map((status) => {
    const segments = counts.filter((bucket) => bucket.status === status);
    const total = segments.reduce((sum, { count }) => sum + count, 0);
    return { status, segments, total };
  });
  const largest = Math.max(1, ...bars.map(({ total }) => total));
  const height = bars.length * (BAR_HEIGHT + GAP);

  return (
    <svg
      role="img"
      aria-label="Issue count per status, stacked by priority"
      width={WIDTH}
      height={height}
      viewBox={`0 0 ${WIDTH} ${height}`}
    >
      {bars.map(({ status, segments, total }, row) => {
        const y = row * (BAR_HEIGHT + GAP);
        let x = LABEL_WIDTH;
        return (
          <g key={status} data-status={status}>
            <text x={0} y={y + BAR_HEIGHT * 0.66} className="chart-label">
              {status}
            </text>
            {segments.map(({ priority, count }) => {
              const width = (count / largest) * PLOT_WIDTH;
              const left = x;
              x += width;
              return (
                <rect
                  key={priority}
                  x={left}
                  y={y}
                  width={Math.max(width, 0.5)}
                  height={BAR_HEIGHT}
                  fill={PRIORITY_COLORS[priority] ?? "#94a3b8"}
                  data-priority={priority}
                  data-count={count}
                >
                  <title>{`${status}, priority ${priority}: ${count.toLocaleString()}`}</title>
                </rect>
              );
            })}
            <text x={x + 6} y={y + BAR_HEIGHT * 0.66} className="chart-count">
              {total.toLocaleString()}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
