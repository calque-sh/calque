import { eq } from "@calque/client";
import { useLiveQuery } from "@calque/react";
import { useMemo } from "react";
import { db } from "../client.js";

export interface Projects {
  readonly slug: ReadonlyMap<string, string>;
  /** Each service's name, with the ids of its projects, one per region. */
  readonly services: ReadonlyMap<string, readonly string[]>;
}

const projects = db.projects
  .query()
  .where(eq("is_sentinel", false))
  .select("id", "name", "slug");

/**
 * The projects, read once into maps: a hundred rows, so the page looks a
 * service up here rather than joining eighty thousand events to them.
 */
export function useProjects(): Projects | undefined {
  const rows = useLiveQuery(projects);
  return useMemo(() => {
    if (rows.status !== "ready") return undefined;
    const slug = new Map<string, string>();
    const services = new Map<string, string[]>();
    for (const project of rows.result) {
      slug.set(project.id, project.slug);
      services.set(project.name, [
        ...(services.get(project.name) ?? []),
        project.id,
      ]);
    }
    return { slug, services };
  }, [rows]);
}
