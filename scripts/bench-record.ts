/** What a run writes: log lines, sample records and summary.json, with the host's local paths removed. */
import { spawnSync } from "node:child_process";
import {
  readdirSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { homedir, tmpdir } from "node:os";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  relative,
  resolve,
} from "node:path";
import { fileURLToPath } from "node:url";
import { type BenchOptions, withConditions } from "./bench-conditions.ts";
import {
  type LocalPlaces,
  withoutLocalPaths,
} from "./verify-no-local-paths.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * A URL as a record may keep it: a query string can carry a capability, such
 * as an artifact's `cap`, so only its presence is kept.
 */
export function redactedUrl(url: string): string {
  const query = url.indexOf("?");
  return query === -1 ? url : `${url.slice(0, query)}?<redacted>`;
}

let hostPlaces: LocalPlaces | null = null;

/**
 * `text` with the checkout's path made relative to it, and the repository's
 * other checkouts, the temporary directories and the home directory written
 * `<checkout>`, `<tmp>` and `<home>`, so a record names neither the user nor
 * the machine and passes `verify-no-local-paths`.
 */
export function withoutHostPaths(text: string): string {
  hostPlaces ??= {
    checkout: root,
    // Every checkout of the repository; none when git cannot be run.
    checkouts: [
      ...(
        spawnSync("git", ["worktree", "list", "--porcelain"], {
          cwd: root,
          encoding: "utf8",
        }).stdout ?? ""
      ).matchAll(/^worktree (.+)$/gmu),
    ].map(([, checkout]) => checkout ?? ""),
    home: homedir(),
    temporary: [...new Set([tmpdir(), realpathSync(tmpdir())])],
  };
  return withoutLocalPaths(text, hostPlaces);
}

/** `path` relative to the checkout, or its file name alone outside it. */
export function checkoutPath(path: string): string {
  const inside = relative(root, path);
  return inside.startsWith("..") || isAbsolute(inside)
    ? `${basename(path)} (outside the checkout)`
    : inside;
}

export function writeRecord(
  run: { readonly directory: string; readonly options: BenchOptions },
  name: string,
  record: object,
): void {
  const conditioned = withConditions(run.options, record);
  writeFileSync(
    join(run.directory, "samples", `${name}.json`),
    withoutHostPaths(`${JSON.stringify(conditioned, null, 2)}\n`),
  );
}

export function log(message: string): void {
  process.stdout.write(`[bench] ${withoutHostPaths(message)}\n`);
}

/** Writes a run's summary.json and logs where. */
export function writeRunSummary(directory: string, summary: object): void {
  const path = join(directory, "summary.json");
  writeFileSync(
    path,
    withoutHostPaths(`${JSON.stringify(summary, null, 2)}\n`),
  );
  log(`wrote ${path}`);
}

/**
 * Rewrites each log under the run's `logs/` through `withoutHostPaths`. calque
 * prints its state directory's path, so this runs once calque has stopped.
 */
export function scrubRunLogs(directory: string): void {
  const logs = join(directory, "logs");
  for (const name of readdirSync(logs)) {
    const path = join(logs, name);
    writeFileSync(path, withoutHostPaths(readFileSync(path, "utf8")));
  }
}
