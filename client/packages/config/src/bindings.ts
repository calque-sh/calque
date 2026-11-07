import { readFileSync } from "node:fs";
import { compileConfig, ConfigError } from "./compiler.js";
import type { CalqueConfigDefinition } from "./domain.js";
import { generateBindings } from "./generate.js";

/** One file `calque-bindings` writes, with its exact bytes. */
export interface BindingsFile {
  readonly path: string;
  readonly bytes: Uint8Array;
}

/**
 * Thrown by `renderBindingsFiles` for bindings it cannot render: an `out`
 * that is not a `.ts` file, or a configuration it cannot read, that is not
 * JSON, or that the compiler refuses. The message names the file and what is
 * wrong with it.
 */
export class BindingsError extends Error {
  override readonly name = "BindingsError";
}

/** The start module's path beside the bindings at `out`. */
function startModulePath(out: string): string {
  if (!out.endsWith(".ts"))
    throw new BindingsError(`the bindings must be a .ts file, not ${out}`);
  return out.replace(/(?:\.generated)?\.ts$/u, ".start.generated.ts");
}

/** A file the operating system refused to read or write. */
export function fileError(error: unknown): error is Error & { code: string } {
  return error instanceof Error && "code" in error && "path" in error;
}

function definitionAt(config: string): CalqueConfigDefinition {
  let text;
  try {
    text = readFileSync(config, "utf8");
  } catch (error) {
    if (!fileError(error)) throw error;
    throw new BindingsError(
      error.code === "ENOENT"
        ? `${config} does not exist; run calque init to write it`
        : `cannot read ${config}: ${error.message}`,
      { cause: error },
    );
  }
  try {
    return JSON.parse(text) as CalqueConfigDefinition;
  } catch (error) {
    if (!(error instanceof SyntaxError)) throw error;
    throw new BindingsError(`${config} is not valid JSON: ${error.message}`, {
      cause: error,
    });
  }
}

/**
 * Renders the bindings to `out` and the start module beside it from the
 * `calque.config.json` at `config`. Throws a `BindingsError` for a
 * configuration it cannot render.
 */
export function renderBindingsFiles(
  config: string,
  out: string,
): readonly BindingsFile[] {
  const definition = definitionAt(config);
  let generated;
  try {
    generated = generateBindings(compileConfig(definition));
  } catch (error) {
    if (!(error instanceof ConfigError)) throw error;
    throw new BindingsError(`${config}: ${error.message}`, { cause: error });
  }
  const encoder = new TextEncoder();
  return [
    { path: out, bytes: encoder.encode(generated.bindings) },
    { path: startModulePath(out), bytes: encoder.encode(generated.start) },
  ];
}

/** The paths of `files` whose bytes on disk differ from the render. */
export function staleBindingsFiles(
  files: readonly BindingsFile[],
): readonly string[] {
  return files
    .filter(({ path, bytes }) => {
      try {
        return !readFileSync(path).equals(bytes);
      } catch {
        return true;
      }
    })
    .map(({ path }) => path);
}
