import { basename, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { build, type Rolldown } from "vite";
import { beforeAll, describe, expect, test } from "vitest";

// Beside `vite.config.ts` rather than in `src/`: it runs on the Node side of
// the build, and `src/` is the browser bundle.
const pageRoot = fileURLToPath(new URL(".", import.meta.url));
const repositoryRoot = fileURLToPath(new URL("../../..", import.meta.url));

type Output = Rolldown.RolldownOutput["output"];

/** The page alone, and the page with the harness entry the benchmark loads. */
const MODES = ["production", "harness"] as const;
type Mode = (typeof MODES)[number];

const outputs = new Map<Mode, Output>();

function chunksOf(mode: Mode): Rolldown.OutputChunk[] {
  const output = outputs.get(mode);
  if (!output) throw new Error(`the ${mode} page was not built`);
  return output.flatMap((item) => (item.type === "chunk" ? [item] : []));
}

beforeAll(async () => {
  for (const mode of MODES) {
    // Built in memory, so `dist` is left as it was.
    const result = await build({
      configFile: fileURLToPath(new URL("vite.config.ts", import.meta.url)),
      root: pageRoot,
      mode,
      logLevel: "silent",
      build: { write: false },
    });
    if (!("output" in result)) throw new Error("expected one build output");
    outputs.set(mode, result.output);
  }
}, 600_000);

describe.each(MODES)("the %s build", (mode) => {
  test("starts with a chunk holding the plugin's start module and the client's starter, and nothing else", () => {
    const chunks = chunksOf(mode);
    const [starter, ...others] = chunks.filter(
      (chunk) => chunk.name === "start",
    );
    if (!starter) throw new Error("no start chunk");
    expect(others).toEqual([]);
    expect(
      starter.moduleIds
        .map((id) => (id.startsWith("\0") ? id : relative(repositoryRoot, id)))
        .sort(),
    ).toEqual([
      "\0calque:start",
      "client/packages/client/dist/bootstrap-start-entry.js",
      "client/packages/client/dist/bootstrap-start.js",
    ]);
    expect(starter.imports).toEqual([]);

    const [entry] = chunks.filter((chunk) => chunk.isEntry);
    if (!entry) throw new Error("no entry chunk");
    const at = (file: string) => entry.code.indexOf(`./${basename(file)}`);
    expect(at(starter.fileName)).toBeGreaterThan(-1);
    for (const file of entry.imports)
      expect(at(starter.fileName)).toBeLessThanOrEqual(at(file));
  });
});

test("only the harness build publishes the page on __incidents", () => {
  const publishes = (mode: Mode) =>
    chunksOf(mode).some((chunk) => chunk.code.includes("__incidents"));
  expect(publishes("production")).toBe(false);
  expect(publishes("harness")).toBe(true);
});
