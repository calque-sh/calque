/**
 * Bootstrap requests the realtime session posts beside its page
 * (`--bootstrap-probe <n>`), to read what calque plans a page that names only
 * some of the served tables while the load runs and the scope is cut again.
 * Each round posts `PROBE_REQUESTS` requests: one for each of the first n
 * table sets of `NARROW_SETS`, and one naming every table for each of the
 * rest, so every arm sends the same number of bootstraps.
 */
import { requestPlan } from "./bench-page-server.ts";
import { log } from "./bench-record.ts";

/** The incidents page's tables in table sets a narrower page could name. */
export const NARROW_SETS: readonly (readonly string[])[] = [
  ["comments", "issue_tags", "issues", "memberships", "projects", "users"],
  ["issues", "projects", "users"],
  ["events"],
  ["comments", "issues"],
  ["memberships", "projects", "users"],
  ["issue_tags", "issues"],
  ["events", "issues"],
  ["comments", "users"],
];

export const PROBE_REQUESTS = NARROW_SETS.length;
export const PROBE_ROUND_MS = 3_000;

export interface ProbeAnswer {
  /** Milliseconds since the probe started. */
  readonly atMs: number;
  /** The index in `NARROW_SETS`, or null for a request naming every table. */
  readonly set: number | null;
  readonly failure: string | null;
  /** Whether the plan installs one artifact of exactly its required tables. */
  readonly exact: boolean | null;
  readonly tables: number | null;
  /** The tables of the plan's first artifact. */
  readonly installed: readonly string[] | null;
  readonly cursor: string | null;
  readonly encoding: string | null;
  readonly encodedBytes: number | null;
  readonly decodedBytes: number | null;
}

interface PlannedInstall {
  readonly tables: readonly string[];
  readonly snapshotCursor: string;
  readonly decodedBytes: number;
  readonly selectedRepresentation: {
    readonly encoding: string;
    readonly encodedBytes: number;
  };
}

/** What a plan's text says of the artifacts it installs, for `named`. */
export function answerOf(
  text: string,
  named: readonly string[],
): Omit<ProbeAnswer, "atMs" | "set"> {
  const plan = JSON.parse(text) as {
    readonly requiredTables: readonly string[];
    readonly artifacts: readonly PlannedInstall[];
  };
  if (plan.requiredTables.length !== named.length)
    throw new Error(
      `the plan requires ${String(plan.requiredTables.length)} tables, not the ${String(named.length)} the probe named`,
    );
  const [install] = plan.artifacts;
  // A plan names its tables by their wire ids, not the page's names.
  const exact =
    plan.artifacts.length === 1 &&
    install !== undefined &&
    [...install.tables].sort().join() ===
      [...plan.requiredTables].sort().join();
  return {
    failure: null,
    exact,
    tables: plan.artifacts.reduce(
      (total, artifact) => total + artifact.tables.length,
      0,
    ),
    installed: install === undefined ? null : [...install.tables],
    cursor: install?.snapshotCursor ?? null,
    encoding: install?.selectedRepresentation.encoding ?? null,
    encodedBytes: install?.selectedRepresentation.encodedBytes ?? null,
    decodedBytes: install?.decodedBytes ?? null,
  };
}

/** The requests of one round: the first `narrow` sets, then every table. */
export function roundRequests(
  narrow: number,
  everyTable: readonly string[],
): readonly { readonly set: number | null; readonly tables: string[] }[] {
  return Array.from({ length: PROBE_REQUESTS }, (_, index) => {
    if (index >= narrow) return { set: null, tables: [...everyTable] };
    const names = NARROW_SETS[index] ?? [];
    const tables = everyTable.filter((table) =>
      names.some((name) => table.endsWith(`.${name}`)),
    );
    if (tables.length !== names.length)
      throw new Error(
        `the page names no table for every one of ${names.join(", ")}`,
      );
    return { set: index, tables };
  });
}

export class BootstrapProbe {
  /** When the first round started, as an ISO date, to read beside calque's log. */
  readonly startedAt = new Date().toISOString();
  private readonly answers: ProbeAnswer[] = [];
  private stopped = false;
  private readonly running: Promise<void>;

  private constructor(
    connection: { readonly endpoint: string; readonly bearerToken: string },
    backendEndpoint: string,
    requests: ReturnType<typeof roundRequests>,
  ) {
    const started = performance.now();
    this.running = (async () => {
      while (!this.stopped) {
        const round = performance.now();
        await Promise.all(
          requests.map(async ({ set, tables }) => {
            const atMs = performance.now() - started;
            try {
              const text = await requestPlan(
                connection,
                backendEndpoint,
                tables,
              );
              this.answers.push({ atMs, set, ...answerOf(text, tables) });
            } catch (error) {
              this.answers.push({
                atMs,
                set,
                failure: error instanceof Error ? error.message : String(error),
                exact: null,
                tables: null,
                installed: null,
                cursor: null,
                encoding: null,
                encodedBytes: null,
                decodedBytes: null,
              });
            }
          }),
        );
        const left = PROBE_ROUND_MS - (performance.now() - round);
        if (left > 0 && !this.stopped)
          await new Promise((done) => setTimeout(done, left));
      }
    })();
  }

  static start(
    connection: { readonly endpoint: string; readonly bearerToken: string },
    backendEndpoint: string,
    everyTable: readonly string[],
    narrow: number,
  ): BootstrapProbe {
    log(
      `bootstrap probe: ${String(narrow)} narrow sets and ${String(PROBE_REQUESTS - narrow)} requests of every table every ${String(PROBE_ROUND_MS)} ms`,
    );
    return new BootstrapProbe(
      connection,
      backendEndpoint,
      roundRequests(narrow, everyTable),
    );
  }

  async stop(): Promise<readonly ProbeAnswer[]> {
    this.stopped = true;
    await this.running;
    const narrow = this.answers.filter((answer) => answer.set !== null);
    const exact = narrow.filter((answer) => answer.exact === true).length;
    log(
      `bootstrap probe: ${String(this.answers.length)} plans, ${String(exact)} of ${String(narrow.length)} narrow ones of exactly their tables`,
    );
    return [...this.answers].sort((a, b) => a.atMs - b.atMs);
  }
}
