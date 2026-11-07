import { existsSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { DEMO_BINDINGS } from "../demo.ts";

export const repositoryRoot = resolve(
  dirname(fileURLToPath(import.meta.url)),
  "../..",
);

// An unbuilt generator fails the check rather than skipping it.
const configDist = join(repositoryRoot, "client/packages/config/dist/index.js");
if (!existsSync(configDist)) {
  throw new Error(
    `@calque/config is not built, so generated client bindings are unverified: ${configDist} is missing. Run \`pnpm --filter @calque/config build\` first.`,
  );
}
const { renderBindingsFiles, staleBindingsFiles } = (await import(
  configDist
)) as typeof import("../../client/packages/config/dist/index.js");

/** One pinned render: a configuration and the bindings it renders to. */
interface PinnedBindings {
  readonly config: string;
  readonly output: string;
}

/** Every `calque-bindings` render the repository checks in. */
export const PINNED_BINDINGS: readonly PinnedBindings[] = Object.freeze([
  {
    config: "tests/fixtures/config/incident-ops.config.json",
    output: "tests/fixtures/config/incident-ops.generated.ts",
  },
  {
    config: "client/examples/incidents/calque.config.json",
    output: "client/packages/client/tests/typed-consumer/calque.generated.ts",
  },
  {
    config: "client/examples/incidents/calque.config.json",
    output: "client/examples/incidents/src/calque.generated.ts",
  },
  DEMO_BINDINGS,
  {
    config: "tests/fixtures/config/serve-tenants.config.json",
    output: "client/packages/client/tests/serve-tenants/calque.generated.ts",
  },
  // Rendered from what the README's self-host walk writes, so the committed
  // files are the walk's output (`pnpm quickstart:e2e` checks it).
  {
    config: "client/examples/quickstart/calque.config.json",
    output: "client/examples/quickstart/calque.generated.ts",
  },
]);

export function renderPinnedBindings(
  pin: PinnedBindings,
  root: string = repositoryRoot,
) {
  return renderBindingsFiles(join(root, pin.config), join(root, pin.output));
}

/** Renders each pin twice and fails unless both renders match the files on disk. */
export function checkPinnedBindings(
  pins: readonly PinnedBindings[] = PINNED_BINDINGS,
  root: string = repositoryRoot,
): number {
  for (const pin of pins) {
    const first = renderPinnedBindings(pin, root);
    const second = renderPinnedBindings(pin, root);
    if (
      first.some(
        ({ bytes }, index) =>
          Buffer.compare(bytes, second[index]?.bytes ?? new Uint8Array()) !== 0,
      )
    )
      throw new Error(
        `generated client bindings are not deterministic: ${pin.config}`,
      );
    const stale = staleBindingsFiles(first);
    if (stale.length > 0)
      throw new Error(
        `generated client bindings drifted: ${stale.map((path) => relative(root, path)).join(", ")} (regenerate with pnpm exec calque-bindings --config ${pin.config} --out ${pin.output})`,
      );
  }
  return pins.length;
}
