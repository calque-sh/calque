import {
  copyFileSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { run } from "./cli.js";

const fixture = fileURLToPath(
  new URL(
    "../../../../tests/fixtures/config/incident-ops.config.json",
    import.meta.url,
  ),
);

function calqueBindings(...args: string[]) {
  let stdout = "";
  let stderr = "";
  const code = run(
    args,
    { write: (text: string) => (stdout += text) },
    { write: (text: string) => (stderr += text) },
  );
  return { code, stdout, stderr };
}

function scratch(): string {
  return mkdtempSync(join(tmpdir(), "calque-bindings-"));
}

describe("calque-bindings", () => {
  const initial = process.cwd();
  let directory: string;

  beforeEach(() => {
    directory = scratch();
    process.chdir(directory);
  });

  afterEach(() => {
    process.chdir(initial);
  });

  it("reads calque.config.json and writes calque.generated.ts beside the start module by default", () => {
    copyFileSync(fixture, join(directory, "calque.config.json"));

    expect(calqueBindings("--check")).toEqual({
      code: 1,
      stdout: "",
      stderr:
        "calque-bindings: calque.generated.ts and calque.start.generated.ts are out of date with calque.config.json; run calque-bindings to regenerate\n",
    });
    expect(existsSync(join(directory, "calque.generated.ts"))).toBe(false);

    expect(calqueBindings()).toEqual({
      code: 0,
      stdout: "Wrote calque.generated.ts\nWrote calque.start.generated.ts\n",
      stderr: "",
    });
    expect(
      readFileSync(join(directory, "calque.generated.ts"), "utf8"),
    ).toContain("export const schema");
    expect(
      readFileSync(join(directory, "calque.start.generated.ts"), "utf8"),
    ).toContain(
      'export const requiredTables: readonly string[] = [\n  "public.issues",\n];',
    );
    expect(calqueBindings("--check")).toEqual({
      code: 0,
      stdout:
        "calque.generated.ts and calque.start.generated.ts are current with calque.config.json\n",
      stderr: "",
    });

    writeFileSync(
      join(directory, "calque.start.generated.ts"),
      "export const requiredTables = [];\n",
    );
    expect(calqueBindings("--check")).toMatchObject({
      code: 1,
      stderr:
        "calque-bindings: calque.start.generated.ts is out of date with calque.config.json; run calque-bindings to regenerate\n",
    });
  });

  it("takes the configuration and the output from --config and --out", () => {
    const out = join(directory, "src", "bindings.ts");
    expect(calqueBindings("--config", fixture, "--out", out)).toMatchObject({
      code: 1,
      stderr: `calque-bindings: cannot write ${out}: ENOENT: no such file or directory, open '${out}'\n`,
    });

    const flat = join(directory, "bindings.ts");
    expect(calqueBindings("--config", fixture, "--out", flat)).toEqual({
      code: 0,
      stdout: `Wrote ${flat}\nWrote ${join(directory, "bindings.start.generated.ts")}\n`,
      stderr: "",
    });
    writeFileSync(flat, "// edited\n");
    expect(
      calqueBindings("--config", fixture, "--out", flat, "--check").stderr,
    ).toBe(
      `calque-bindings: ${flat} is out of date with ${fixture}; run calque-bindings --config ${fixture} --out ${flat} to regenerate\n`,
    );
  });

  it("prints its help on --help and -h", () => {
    for (const flag of ["--help", "-h"]) {
      const { code, stdout, stderr } = calqueBindings(flag);
      expect(code).toBe(0);
      expect(stderr).toBe("");
      expect(stdout).toMatch(
        /^usage: calque-bindings \[--config <file>\] \[--out <file>\] \[--check\]\n/u,
      );
      expect(stdout).toContain("(default: calque.config.json)");
      expect(stdout).toContain("calque.generated.ts); the start module");
    }
  });

  it("prints its usage for an unknown flag or an --out that is not TypeScript", () => {
    const inspection = calqueBindings("--inspection", "calque.inspection.json");
    expect(inspection.code).toBe(2);
    expect(inspection.stderr).toMatch(
      /^calque-bindings: Unknown option '--inspection'/u,
    );
    expect(inspection.stderr).toMatch(/\nusage: calque-bindings /u);

    expect(calqueBindings("--out", "bindings.js")).toEqual({
      code: 2,
      stdout: "",
      stderr:
        "calque-bindings: --out must name a .ts file, not bindings.js\nusage: calque-bindings [--config <file>] [--out <file>] [--check]\n",
    });
  });

  it("names the configuration and what to do when it cannot be read", () => {
    expect(calqueBindings()).toEqual({
      code: 1,
      stdout: "",
      stderr:
        "calque-bindings: calque.config.json does not exist; run calque init to write it, or name the configuration with --config\n",
    });

    writeFileSync(join(directory, "calque.config.json"), "{");
    expect(calqueBindings().stderr).toMatch(
      /^calque-bindings: calque\.config\.json is not valid JSON: /u,
    );

    const definition = JSON.parse(readFileSync(fixture, "utf8")) as {
      collections: { columns: { postgresType: string }[] }[];
    };
    const column = definition.collections[0]?.columns[0];
    if (!column) throw new Error("fixture column is missing");
    column.postgresType = "pg_catalog.uuid";
    writeFileSync(
      join(directory, "calque.config.json"),
      JSON.stringify(definition),
    );
    const refused = calqueBindings();
    expect(refused.code).toBe(1);
    expect(refused.stderr).toMatch(
      /^calque-bindings: calque\.config\.json: \$\.collections\[0\]\.columns\[0\]\.postgresType: pg_catalog\.uuid is not a type calque serve enrolls; it must be one of bool, int2, /u,
    );
    expect(refused.stderr).not.toMatch(/\n\s+at /u);
    expect(existsSync(join(directory, "calque.generated.ts"))).toBe(false);
  });
});
