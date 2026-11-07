/**
 * A workspace package's name and every dependency name its manifest declares.
 * Names that are not workspace packages are ignored by the sort.
 */
export interface BuildOrderPackage {
  readonly name: string;
  readonly dependencies: readonly string[];
}

/**
 * Orders workspace packages so each comes after the workspace packages it
 * depends on, smallest name first where the graph leaves the order free. Each
 * package's types come from its built `dist/`, and `pnpm --filter <pkg> build`
 * never builds dependencies. Throws on a dependency cycle, naming the packages
 * still unplaced.
 */
export function workspaceBuildOrder<T extends BuildOrderPackage>(
  packages: readonly T[],
): T[] {
  const workspaceNames = new Set(packages.map((entry) => entry.name));
  const placed = new Set<string>();
  const ordered: T[] = [];
  let remaining = [...packages].sort((left, right) =>
    left.name.localeCompare(right.name),
  );
  while (remaining.length > 0) {
    const [next] = remaining.filter((entry) =>
      entry.dependencies.every(
        (dependency) =>
          !workspaceNames.has(dependency) || placed.has(dependency),
      ),
    );
    if (next === undefined) {
      throw new Error(
        `workspace dependency cycle among: ${remaining
          .map((entry) => entry.name)
          .join(", ")}`,
      );
    }
    placed.add(next.name);
    ordered.push(next);
    remaining = remaining.filter((entry) => entry !== next);
  }
  return ordered;
}
