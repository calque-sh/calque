/** How a run stops: on a signal, or at its own end. */
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { log, writeRunSummary } from "./bench-record.ts";

/** How a run stops: on a signal, or at its own end. */
interface RunStop {
  readonly onSignal: (signal: NodeJS.Signals) => void;
  /** The run's own end, with what aborted it or null. */
  readonly finish: (aborted: string | null) => Promise<void>;
}

/**
 * A run stops once. The first SIGINT or SIGTERM stops what the run started,
 * writes summary.json from what the run already holds, and exits 130 without
 * judging: a verdict over a stopped run's samples would read as the run's own.
 * A run that ends by itself stops through `finish`. A later signal finds the
 * stop running and changes nothing, and `finish` after a signal waits for that
 * stop's exit.
 */
export function runStop(
  directory: string,
  summary: (aborted: string | null) => object,
  cleanup: () => Promise<void>,
  exit: (code: number) => void = (code) => process.exit(code),
): RunStop {
  let stopping: Promise<void> | null = null;
  return {
    onSignal: (signal) => {
      if (stopping !== null) {
        log(`${signal}: already stopping`);
        return;
      }
      stopping = cleanup().finally(() => {
        try {
          writeRunSummary(directory, {
            ...summary(signal),
            coldSamplesWritten: readdirSync(join(directory, "samples")).filter(
              (name) => /^cold-\d+\.json$/u.test(name),
            ).length,
          });
        } finally {
          exit(130);
        }
      });
    },
    finish: (aborted) =>
      (stopping ??= cleanup().then(() => {
        writeRunSummary(directory, summary(aborted));
      })),
  };
}

/**
 * Hands SIGINT and SIGTERM to `stop` for the rest of node's life. A second
 * Ctrl-C signals node again: the terminal sends SIGINT to the whole
 * foreground process group, and pnpm sends the script SIGTERM. A `once`
 * listener leaves that second signal to the default action, which ends node
 * partway through the stop, and a node a signal ends fires no 'exit'.
 * Signal listeners keep no event loop alive, so a run that ends by itself
 * leaves them installed.
 */
export function listenForStop(stop: Pick<RunStop, "onSignal">): void {
  process.on("SIGINT", stop.onSignal);
  process.on("SIGTERM", stop.onSignal);
}
