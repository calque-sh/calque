import { useLiveQuery } from "@calque/react";
import { matching, type Filter } from "./events.js";
import type { Projects } from "./projects.js";
import { useReady } from "./ready.js";

const LEVELS = ["fatal", "error", "warning", "info", "debug"] as const;
const SERVICES_SHOWN = 8;

export function Facets({
  filter,
  projects,
  onFilter,
}: {
  readonly filter: Filter;
  readonly projects: Projects | undefined;
  readonly onFilter: (filter: Filter) => void;
}) {
  const levels = useLiveQuery(
    matching(filter, "level").groupBy("level").count(),
  );
  const byProject = useLiveQuery(
    matching(filter, "service").groupBy("project_id").count(),
  );
  const services =
    byProject.status === "ready" && projects !== undefined
      ? [...projects.services]
          .map(([name, ids]) => ({
            name,
            ids,
            count: byProject.result
              .filter((group) => ids.includes(group.project_id))
              .reduce((sum, group) => sum + group.count, 0),
          }))
          .sort(
            (left, right) =>
              right.count - left.count || left.name.localeCompare(right.name),
          )
          .slice(0, SERVICES_SHOWN)
      : [];
  useReady("levels", levels.status === "ready");
  useReady("services", services.length > 0);

  const levelCount = (level: string) =>
    levels.status === "ready"
      ? (levels.result.find((group) => group.level === level)?.count ?? 0)
      : undefined;
  const allLevels =
    levels.status === "ready"
      ? levels.result.reduce((sum, group) => sum + group.count, 0)
      : undefined;

  return (
    <nav className="facets" aria-label="Filters">
      <h2>Level</h2>
      <ul>
        {[undefined, ...LEVELS].map((level) => (
          <li key={level ?? "all"}>
            <button
              type="button"
              data-level={level ?? "all"}
              aria-pressed={filter.level === level}
              onClick={(input) =>
                onFilter({ ...filter, level, since: input.timeStamp })
              }
            >
              <span>{level ?? "All levels"}</span>
              <Count
                value={level === undefined ? allLevels : levelCount(level)}
              />
            </button>
          </li>
        ))}
      </ul>
      <h2>Service</h2>
      <ul>
        <li>
          <button
            type="button"
            aria-pressed={filter.service === undefined}
            onClick={(input) =>
              onFilter({
                ...filter,
                service: undefined,
                since: input.timeStamp,
              })
            }
          >
            <span>All services</span>
          </button>
        </li>
        {services.map(({ name, ids, count }) => (
          <li key={name}>
            <button
              type="button"
              data-service={name}
              aria-pressed={filter.service?.name === name}
              onClick={(input) =>
                onFilter({
                  ...filter,
                  service:
                    filter.service?.name === name
                      ? undefined
                      : { name, projects: ids },
                  since: input.timeStamp,
                })
              }
            >
              <span>{name}</span>
              <Count value={count} />
            </button>
          </li>
        ))}
      </ul>
    </nav>
  );
}

function Count({ value }: { readonly value: number | undefined }) {
  return (
    <data value={value ?? ""}>
      {value === undefined ? "" : value.toLocaleString("en-US")}
    </data>
  );
}
