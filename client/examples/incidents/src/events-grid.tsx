import { eq } from "@calque/client";
import { useLiveWindow } from "@calque/react";
import {
  DataEditorCore,
  GridCellKind,
  ImageWindowLoaderImpl,
  loadingCellRenderer,
  markerCellRenderer,
  textCellRenderer,
  type DataEditorCoreProps,
  type DataEditorRef,
  type GridCell,
  type GridColumn,
  type Item,
} from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import { useCallback, useRef, useState } from "react";
import { db } from "./client.js";

const COLUMNS = [
  { id: "id", title: "Event", width: 96 },
  { id: "level", title: "Level", width: 84 },
  { id: "message", title: "Message", width: 360 },
  { id: "project_id", title: "Project", width: 150 },
  { id: "issue_id", title: "Issue", width: 150 },
  { id: "occurred_at", title: "Occurred", width: 220 },
] as const satisfies readonly GridColumn[];
const FIELDS = COLUMNS.map(({ id }) => id);

const LEVELS = ["debug", "info", "warning", "error", "fatal"] as const;
type Level = (typeof LEVELS)[number] | "all";
type Order = "engine" | "newest" | "oldest";

/** Rows per block; the grid reads the block in view and one on each side. */
const BLOCK = 64;

// Without an initial size glide renders once empty, measures, then renders
// again. This is the grid panel's content box.
const SIZE = [878, 430] as const;
// Only the cells this grid draws: the default editor also registers markdown
// and image cells, which pull `marked` and a carousel into the bundle.
const RENDERERS = [
  textCellRenderer,
  loadingCellRenderer,
  markerCellRenderer,
] as NonNullable<DataEditorCoreProps["renderers"]>;
const LOADING: GridCell = { kind: GridCellKind.Loading, allowOverlay: false };

function eventsQuery(level: Level, order: Order) {
  const query = db.events.query();
  const filtered = (
    level === "all" ? query : query.where(eq("level", level))
  ).select(...FIELDS);
  if (order === "engine") return filtered;
  const direction = order === "newest" ? "desc" : "asc";
  return filtered.orderBy("occurred_at", direction).orderBy("id", direction);
}

export function EventsGrid() {
  const [level, setLevel] = useState<Level>("all");
  const [order, setOrder] = useState<Order>("engine");
  const [top, setTop] = useState(0);
  const offset = Math.max(0, (Math.floor(top / BLOCK) - 1) * BLOCK);
  const shown = useLiveWindow(eventsQuery(level, order), offset, 3 * BLOCK);
  const events = shown.status === "ready" ? shown.result : undefined;
  const editor = useRef<DataEditorRef>(null);
  const [images] = useState(() => new ImageWindowLoaderImpl());

  const getCellContent = useCallback(
    ([column, row]: Item): GridCell => {
      const field = FIELDS[column];
      const event = events?.rows[row - events.offset];
      if (field === undefined || event === undefined) return LOADING;
      const text = String(event[field] ?? "");
      return {
        kind: GridCellKind.Text,
        data: text,
        displayData: text,
        allowOverlay: false,
      };
    },
    [events],
  );

  const scrollToTop = () => {
    setTop(0);
    editor.current?.scrollTo(0, 0, "vertical");
  };

  return (
    <section
      className="panel grid-panel"
      aria-label="Events"
      aria-busy={events === undefined || shown.stale}
    >
      <div className="panel-heading">
        <h2>Events</h2>
        <label>
          Level
          <select
            data-control="level"
            value={level}
            onChange={(event) => {
              setLevel(event.target.value as Level);
              scrollToTop();
            }}
          >
            <option value="all">All levels</option>
            {LEVELS.map((option) => (
              <option key={option}>{option}</option>
            ))}
          </select>
        </label>
        <label>
          Order
          <select
            data-control="sort"
            value={order}
            onChange={(event) => {
              setOrder(event.target.value as Order);
              scrollToTop();
            }}
          >
            <option value="engine">Engine order</option>
            <option value="newest">Newest first</option>
            <option value="oldest">Oldest first</option>
          </select>
        </label>
        <output>
          {events === undefined
            ? "Loading events…"
            : `${events.total.toLocaleString()} ${level === "all" ? "events" : "events match"}`}
        </output>
      </div>
      <DataEditorCore
        ref={editor}
        renderers={RENDERERS}
        imageWindowLoader={images}
        columns={COLUMNS}
        rows={events?.total ?? 0}
        getCellContent={getCellContent}
        onVisibleRegionChanged={(region) => setTop(region.y)}
        width={SIZE[0]}
        height={SIZE[1]}
        initialSize={SIZE}
        rowMarkers="number"
        smoothScrollY
      />
    </section>
  );
}
