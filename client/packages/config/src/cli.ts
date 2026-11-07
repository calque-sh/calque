import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import {
  BindingsError,
  fileError,
  renderBindingsFiles,
  staleBindingsFiles,
} from "./bindings.js";

interface Output {
  write(text: string): unknown;
}

const DEFAULT_CONFIG = "calque.config.json";
const DEFAULT_OUT = "calque.generated.ts";

const USAGE =
  "usage: calque-bindings [--config <file>] [--out <file>] [--check]\n";

const HELP = `${USAGE}
Writes the typed client bindings for a Calque configuration, and the start
module beside them.

  --config <file>  the configuration to read (default: ${DEFAULT_CONFIG})
  --out <file>     the bindings to write, a .ts file (default:
                   ${DEFAULT_OUT}); the start module goes beside it,
                   as <name>.start.generated.ts
  --check          write nothing, and fail if either file is not what
                   calque-bindings would write
  -h, --help       print this help
`;

/**
 * `calque-bindings`: writes the typed bindings to `--out` and the start module
 * beside it. `--check` writes nothing and fails when either file on disk is
 * not the exact render. Returns the exit code.
 */
export function run(
  args: readonly string[],
  stdout: Output,
  stderr: Output,
): number {
  let values;
  try {
    ({ values } = parseArgs({
      args: [...args],
      options: {
        config: { type: "string", default: DEFAULT_CONFIG },
        out: { type: "string", default: DEFAULT_OUT },
        check: { type: "boolean", default: false },
        help: { type: "boolean", short: "h", default: false },
      },
    }));
  } catch (error) {
    stderr.write(`calque-bindings: ${(error as Error).message}\n${USAGE}`);
    return 2;
  }
  const { config, out, check, help } = values;
  if (help) {
    stdout.write(HELP);
    return 0;
  }
  if (!out.endsWith(".ts")) {
    stderr.write(
      `calque-bindings: --out must name a .ts file, not ${out}\n${USAGE}`,
    );
    return 2;
  }
  let files;
  try {
    files = renderBindingsFiles(config, out);
  } catch (error) {
    if (!(error instanceof BindingsError)) throw error;
    const missing = fileError(error.cause) && error.cause.code === "ENOENT";
    stderr.write(
      `calque-bindings: ${error.message}${missing ? ", or name the configuration with --config" : ""}\n`,
    );
    return 1;
  }
  const paths = files.map(({ path }) => path);
  if (check) {
    const stale = staleBindingsFiles(files);
    if (stale.length > 0) {
      const command = [
        "calque-bindings",
        ...(config === DEFAULT_CONFIG ? [] : ["--config", config]),
        ...(out === DEFAULT_OUT ? [] : ["--out", out]),
      ].join(" ");
      stderr.write(
        `calque-bindings: ${stale.join(" and ")} ${stale.length === 1 ? "is" : "are"} out of date with ${config}; run ${command} to regenerate\n`,
      );
      return 1;
    }
    stdout.write(`${paths.join(" and ")} are current with ${config}\n`);
    return 0;
  }
  for (const { path, bytes } of files) {
    try {
      writeFileSync(path, bytes);
    } catch (error) {
      if (!fileError(error)) throw error;
      stderr.write(`calque-bindings: cannot write ${path}: ${error.message}\n`);
      return 1;
    }
    stdout.write(`Wrote ${path}\n`);
  }
  return 0;
}
