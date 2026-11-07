/** The run context every phase reads, and the accessors over it. */
import {
  type BrotliServed,
  calqueServed,
  type DecoderFiles,
  type PlannedArtifact,
  plannedObject,
} from "./bench-brotli.ts";
import type { CalqueServer } from "./bench-calque.ts";
import {
  type BenchOptions,
  type BenchPlan,
  rendersPlan,
} from "./bench-conditions.ts";
import { FrameHasher, type Roi } from "./bench-frames.ts";
import type { HostRecord } from "./bench-host.ts";
import type { LaunchTarget, ScreenshotCalibration } from "./bench-launch.ts";
import { KernelShaper } from "./bench-netshape.ts";
import type {
  OracleTable,
  QueryExpectation,
  SourceState,
} from "./bench-oracle.ts";
import { type Connection, writePageControl } from "./bench-page-server.ts";
import type { BenchPostgres } from "./bench-postgres.ts";
import { redactedUrl } from "./bench-record.ts";
import type { EnvironmentReading } from "./bench-results.ts";
import type { ChildHandle } from "./demo.ts";

export interface Run {
  readonly id: string;
  readonly directory: string;
  readonly options: BenchOptions;
  readonly plan: BenchPlan;
  readonly calque: string;
  readonly host: HostRecord;
  readonly pageUrl: string;
  readonly pageOrigin: string;
  readonly calquePort: number;
  /** The metrics listener's port. No browser connects to it, so it is never shaped. */
  readonly calqueMetricsPort: number;
  /** calque's browser-visible base URL behind the edge, or null without one. */
  readonly calquePublicBaseUrl: string | null;
  /** The ports the kernel shaper shapes: the ones the browser connects to. */
  readonly shapedPorts: readonly number[];
  readonly hasher: FrameHasher;
  readonly sourceEnvironment: NodeJS.ProcessEnv;
  readonly adminEnvironment: NodeJS.ProcessEnv;
  readonly serviceEnvironment: NodeJS.ProcessEnv;
  readonly tables: readonly OracleTable[];
  /** Every running `calque dev`, for the interrupt handler. */
  readonly servers: Set<ChildHandle>;
  /** How the run's browser is launched, checked before the run. */
  readonly browser: LaunchTarget;
  readonly stateDirectory: string;
  /** The environment read before and after every step, in order. */
  readonly readings: EnvironmentReading[];
  server: CalqueServer | null;
  connection: Connection | null;
  source: SourceState | null;
  /** What PostgreSQL says the post-load query returns, read with `source`. */
  queryExpectation: QueryExpectation | null;
  /** The reference pass, which every sample is judged against. */
  reference: ReferencePass | null;
  /** How many browsers each session launched, in order. */
  readonly launches: number[];
  /** The kernel shaper under `--network shaped:`, or null. */
  readonly shaper: KernelShaper | null;
  /** The PostgreSQL server the run seeds and serves from. */
  readonly postgres: BenchPostgres;
  /**
   * The file the page server reads, on every request, for what to render
   * into the page (`showPage`).
   */
  readonly pageControl: string;
  /** The client's decoder as the served build carries it. */
  readonly decoder: DecoderFiles;
  /**
   * The artifact calque planned for the run's own request after its hold, or
   * null before then and where the plan names no single compressed artifact.
   */
  calqueArtifact: PlannedArtifact | null;
}

export interface ReferencePass {
  readonly full: string;
  readonly grid: string | null;
  readonly gridRoi: Roi;
  /** The reference pass's last frame, saved beside a sample that misses it. */
  readonly png: Buffer;
  /** Whether a screenshot of the reference page hashed as its frame did. */
  readonly screenshotCalibration: ScreenshotCalibration;
  readonly gridRows: number | null;
  readonly chart: string;
  /** The grid's picture once the post-load query has run. */
  readonly queryGrid: string;
}

export function referenceOf(run: Run): ReferencePass {
  if (!run.reference) throw new Error("the run has no reference pass");
  return run.reference;
}

/**
 * Has the page server render the run's connection into the page from the
 * next request on, as the page ships (`rendersPlan`). The control file is
 * replaced whole, so no request reads half of it.
 */
export function showPage(run: Run): void {
  if (!run.connection || !run.server)
    throw new Error("the run has no connection to render");
  writePageControl(run.pageControl, {
    connection: run.connection,
    renderedPlan: rendersPlan(run.options),
    backendEndpoint: `${run.server.endpoint}/`,
  });
}

/**
 * The brotli object a run's pages load, as `brotliSample` compares a sample
 * with it: calque's own object, packed or plain, or null before the hold and
 * where calque plans its zstd one.
 */
export function servedBrotli(
  run: Pick<Run, "decoder" | "calqueArtifact" | "pageUrl">,
): BrotliServed | null {
  return run.calqueArtifact === null
    ? null
    : calqueServed(run.calqueArtifact, redactedUrl, {
        moduleUrl: new URL(run.decoder.module.path, run.pageUrl).href,
        workerUrl: new URL(run.decoder.worker.path, run.pageUrl).href,
      });
}

/**
 * The one artifact object a run's pages are expected to load, as
 * `judgeArtifactObject` reads a ledger for it: the object calque planned
 * after the hold, or null before then.
 */
export function expectedObject(
  run: Pick<Run, "calqueArtifact">,
): Pick<BrotliServed, "objectUrl" | "artifactRoute"> | null {
  return run.calqueArtifact === null
    ? null
    : plannedObject(run.calqueArtifact, redactedUrl);
}
