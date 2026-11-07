/** The conditions a bench run takes: its command-line options, dataset, network and sample counts, and the records that name them. */
import { resolve } from "node:path";
import type { BackgroundQos } from "./bench-background.ts";
import { edgeRecord } from "./bench-edge.ts";
import {
  parseShapedNetwork,
  SHAPED_PREFIX,
  shapedFloorMs,
  type ShapedNetwork,
} from "./bench-netshape.ts";
import {
  type DatasetName,
  DATASETS,
  LOAD_TRANSACTIONS_PER_SECOND,
} from "./bench-oracle.ts";

export interface BenchOptions {
  readonly smoke: boolean;
  readonly calque: string | null;
  /** `--dataset <name>`: the dataset the rows are cut from. */
  readonly dataset: DatasetName;
  /**
   * `--network <profile>`: the emulated network, the kernel-shaped one, or
   * null for none.
   */
  readonly network: NetworkProfile | ShapedNetwork | null;
  /** `--diagnostic`: keep the raw traces and the artifact, and judge nothing. */
  readonly diagnostic: boolean;
  /**
   * `--hold-until-logged <text>`: what the serving calque logs before the
   * reference pass starts, or null to hold for what `holdText` names.
   */
  readonly holdUntilLogged: string | null;
  /**
   * `--realtime-only`: one preparation, the reference pass and the realtime
   * session, and no cold sample.
   */
  readonly realtimeOnly: boolean;
  /**
   * `--postgres-bin <dir>`: the directory of the PostgreSQL 18 binaries the
   * run makes its own cluster with, or null for the `postgres:18` container.
   */
  readonly postgresBin: string | null;
  /**
   * Whether the browser reaches the page and calque through the pinned Caddy
   * edge with TLS and HTTP/2; `--no-edge` reaches both over plain HTTP/1.1.
   */
  readonly edge: boolean;
  /**
   * `--background-clients <n>`: n clients, each holding a plan and following
   * the change stream through the realtime session.
   */
  readonly backgroundClients?: number;
  /**
   * `--background-qos utility`: the background clients' processes run under
   * `taskpolicy -c utility`, below calque's, the page's and the edge's
   * scheduling priority, with their sockets as they were.
   */
  readonly backgroundQos?: BackgroundQos;
  /**
   * `--load-rate <tx/s>`: the writer's transactions a second in place of
   * `LOAD_TRANSACTIONS_PER_SECOND`.
   */
  readonly loadRate?: number;
  /**
   * `--tail <transactions>`: the load's transactions committed after the hold
   * and before the reference pass, so every page catches up through them from
   * the artifact's cut.
   */
  readonly tail?: number;
  /**
   * `--recut-tail-ratio <ratio>`: the tail, in bytes per decoded artifact
   * byte, at which `calque dev` cuts its artifact again, in place of its
   * default.
   */
  readonly recutTailRatio?: number;
  /**
   * `--recut-min-interval <seconds>`: the least time between two of
   * `calque dev`'s cuts, in place of its default.
   */
  readonly recutMinIntervalSeconds?: number;
}

/** A shaped rate in both directions, and latency added before each response. */
interface NetworkProfile {
  readonly megabitsPerSecond: number;
  readonly latencyMs: number;
}

/** The conditions a run takes unless told otherwise. */
const DEFAULT_DATASET: DatasetName = "content-v1";

export const DEFAULT_NETWORK: NetworkProfile = Object.freeze({
  megabitsPerSecond: 100,
  latencyMs: 30,
});

export const NETWORK_CAVEAT =
  "Chrome's network emulation serializes the browser's native decode with the wire, measured on Chromium 149; a real link keeps receiving during decode, so an emulated cold number errs pessimistic. It adds its latency before a response starts, not to a response already open, so every live batch after a streamed response's first skips the one-way time a real link adds, and an emulated realtime number errs optimistic by about half the round trip. It models no TCP slow start or loss.";

/** `off`, `<Mbit/s>/<latency ms>`, or `shaped:<Mbit/s>/<RTT ms>`. */
export function parseNetwork(
  text: string,
): NetworkProfile | ShapedNetwork | null {
  if (text === "off") return null;
  if (text.startsWith(SHAPED_PREFIX))
    return parseShapedNetwork(text.slice(SHAPED_PREFIX.length));
  const match = /^(\d+(?:\.\d+)?)\/(\d+(?:\.\d+)?)$/u.exec(text);
  if (!match || Number(match[1]) <= 0)
    throw new Error(
      "--network takes off or <Mbit/s>/<latency ms>, like 100/30, or shaped:<Mbit/s>/<RTT ms>, like shaped:500/20",
    );
  return { megabitsPerSecond: Number(match[1]), latencyMs: Number(match[2]) };
}

/** The reasons a run is never judged; each also labels it directional. */
export function unjudgedReasons(options: BenchOptions): string[] {
  return [
    ...(options.diagnostic ? ["diagnostic run"] : []),
    ...(options.network !== null && "shaper" in options.network
      ? ["kernel-shaped second reading"]
      : []),
    ...(options.realtimeOnly ? ["realtime only"] : []),
    ...(options.postgresBin === null
      ? []
      : ["PostgreSQL outside its container"]),
    ...(options.edge ? [] : ["no production edge"]),
    ...(options.backgroundClients === undefined
      ? []
      : [`${String(options.backgroundClients)} background clients`]),
    ...(options.backgroundQos === undefined
      ? []
      : [`background clients at ${options.backgroundQos} QoS`]),
    ...(options.loadRate === undefined
      ? []
      : [
          `load of ${String(options.loadRate)} transactions/s, not ${String(LOAD_TRANSACTIONS_PER_SECOND)}`,
        ]),
    ...(options.tail === undefined
      ? []
      : [
          `${String(options.tail)} transactions written before the reference pass`,
        ]),
    ...(options.recutTailRatio === undefined
      ? []
      : [`re-cut at a tail ratio of ${String(options.recutTailRatio)}`]),
    ...(options.recutMinIntervalSeconds === undefined
      ? []
      : [`re-cut at most every ${String(options.recutMinIntervalSeconds)} s`]),
  ];
}

/**
 * The profile Chrome emulates: none under `--network off` or under the kernel
 * shaper, so the two are never stacked.
 */
export function emulatedNetwork(
  network: BenchOptions["network"],
): NetworkProfile | null {
  return network === null || "shaper" in network ? null : network;
}

/**
 * Whether the page server renders a bootstrap plan into every page, as an
 * application's backend does. Not under the kernel shaper: there its own
 * request to calque crosses the shaped link, and a rendered plan gains time
 * only for a backend close to the Calque server.
 */
export function rendersPlan(options: Pick<BenchOptions, "network">): boolean {
  return options.network === null || !("shaper" in options.network);
}

const USAGE =
  "usage: pnpm bench [-- --smoke] [--calque <path>] [--dataset <content-v1|v1>] [--network <off|Mbit/s/latency ms|shaped:Mbit/s/RTT ms>] [--diagnostic] [--hold-until-logged <text>] [--realtime-only] [--postgres-bin <dir>] [--no-edge] [--background-clients <n>] [--background-qos utility] [--load-rate <tx/s>] [--tail <transactions>] [--recut-tail-ratio <ratio>] [--recut-min-interval <seconds>]";

export function parseBenchArguments(
  arguments_: readonly string[],
): BenchOptions {
  const rest = arguments_[0] === "--" ? arguments_.slice(1) : arguments_;
  let smoke = false;
  let calque: string | null = null;
  let dataset: DatasetName | null = null;
  let network: BenchOptions["network"] | undefined;
  let diagnostic = false;
  let holdUntilLogged: string | null = null;
  let realtimeOnly = false;
  let postgresBin: string | null = null;
  let edge = true;
  let backgroundClients: number | null = null;
  let backgroundQos: BackgroundQos | null = null;
  let loadRate: number | null = null;
  let tail: number | null = null;
  let recutTailRatio: number | null = null;
  let recutMinIntervalSeconds: number | null = null;
  const value = (index: number) => {
    const next = rest[index + 1];
    if (next === undefined || next.startsWith("--")) throw new Error(USAGE);
    return next;
  };
  for (let index = 0; index < rest.length; index += 1) {
    const argument = rest[index];
    if (argument === "--smoke" && !smoke) smoke = true;
    else if (argument === "--calque" && calque === null) {
      calque = resolve(value(index));
      index += 1;
    } else if (argument === "--dataset" && dataset === null) {
      const name = value(index);
      dataset =
        (Object.keys(DATASETS) as DatasetName[]).find(
          (candidate) => candidate === name,
        ) ?? null;
      if (dataset === null) throw new Error(USAGE);
      index += 1;
    } else if (argument === "--network" && network === undefined) {
      network = parseNetwork(value(index));
      index += 1;
    } else if (argument === "--diagnostic" && !diagnostic) diagnostic = true;
    else if (argument === "--hold-until-logged" && holdUntilLogged === null) {
      holdUntilLogged = value(index);
      if (holdUntilLogged.trim() === "") throw new Error(USAGE);
      index += 1;
    } else if (argument === "--realtime-only" && !realtimeOnly)
      realtimeOnly = true;
    else if (argument === "--postgres-bin" && postgresBin === null) {
      postgresBin = resolve(value(index));
      index += 1;
    } else if (argument === "--no-edge" && edge) edge = false;
    else if (
      argument === "--background-clients" &&
      backgroundClients === null
    ) {
      backgroundClients = parseClientCount(value(index));
      index += 1;
    } else if (argument === "--background-qos" && backgroundQos === null) {
      if (value(index) !== "utility") throw new Error(USAGE);
      backgroundQos = "utility";
      index += 1;
    } else if (argument === "--load-rate" && loadRate === null) {
      loadRate = parseLoadRate(value(index));
      index += 1;
    } else if (argument === "--tail" && tail === null) {
      tail = parseTail(value(index));
      index += 1;
    } else if (argument === "--recut-tail-ratio" && recutTailRatio === null) {
      recutTailRatio = parseRecutTailRatio(value(index));
      index += 1;
    } else if (
      argument === "--recut-min-interval" &&
      recutMinIntervalSeconds === null
    ) {
      recutMinIntervalSeconds = parseRecutMinInterval(value(index));
      index += 1;
    } else throw new Error(USAGE);
  }
  // The clients follow the realtime session, and would load every cold sample.
  if (backgroundClients !== null && !realtimeOnly)
    throw new Error(
      "--background-clients follows the realtime session, so it needs --realtime-only",
    );
  if (backgroundQos !== null && backgroundClients === null)
    throw new Error(
      "--background-qos places the background clients, so it needs --background-clients",
    );
  if (loadRate !== null && !realtimeOnly)
    throw new Error(
      "--load-rate sets the realtime session's writer, so it needs --realtime-only",
    );
  if (tail !== null && realtimeOnly)
    throw new Error(
      "--tail is caught up by the cold samples, so it takes no --realtime-only",
    );
  return {
    smoke,
    calque,
    dataset: dataset ?? DEFAULT_DATASET,
    network: network === undefined ? DEFAULT_NETWORK : network,
    diagnostic,
    holdUntilLogged,
    realtimeOnly,
    postgresBin,
    edge,
    ...(backgroundClients === null ? {} : { backgroundClients }),
    ...(backgroundQos === null ? {} : { backgroundQos }),
    ...(loadRate === null ? {} : { loadRate }),
    ...(tail === null ? {} : { tail }),
    ...(recutTailRatio === null ? {} : { recutTailRatio }),
    ...(recutMinIntervalSeconds === null ? {} : { recutMinIntervalSeconds }),
  };
}

/** `--background-clients`'s value: a whole number from 1 to 100,000. */
export function parseClientCount(text: string): number {
  const count = /^[0-9]+$/u.test(text) ? Number(text) : Number.NaN;
  if (!Number.isSafeInteger(count) || count < 1 || count > 100_000)
    throw new Error(
      "--background-clients takes a whole number from 1 to 100000",
    );
  return count;
}

/** `--load-rate`'s value: a whole number of transactions a second, 1 to 1,000. */
export function parseLoadRate(text: string): number {
  const rate = /^[0-9]+$/u.test(text) ? Number(text) : Number.NaN;
  if (!Number.isSafeInteger(rate) || rate < 1 || rate > 1_000)
    throw new Error(
      "--load-rate takes a whole number of transactions a second from 1 to 1000",
    );
  return rate;
}

/** `--tail`'s value: a whole number of transactions, 1 to 5,000,000. */
export function parseTail(text: string): number {
  const transactions = /^[0-9]+$/u.test(text) ? Number(text) : Number.NaN;
  if (
    !Number.isSafeInteger(transactions) ||
    transactions < 1 ||
    transactions > 5_000_000
  )
    throw new Error(
      "--tail takes a whole number of transactions from 1 to 5000000",
    );
  return transactions;
}

/** `--recut-tail-ratio`'s value: a decimal from 0 to 1,000, as calque takes. */
export function parseRecutTailRatio(text: string): number {
  const ratio = /^[0-9]+(\.[0-9]+)?$/u.test(text) ? Number(text) : Number.NaN;
  if (!Number.isFinite(ratio) || ratio > 1_000)
    throw new Error("--recut-tail-ratio takes a decimal from 0 to 1000");
  return ratio;
}

/** `--recut-min-interval`'s value: whole seconds, 0 to 86,400. */
export function parseRecutMinInterval(text: string): number {
  const seconds = /^[0-9]+$/u.test(text) ? Number(text) : Number.NaN;
  if (!Number.isSafeInteger(seconds) || seconds > 86_400)
    throw new Error("--recut-min-interval takes whole seconds from 0 to 86400");
  return seconds;
}

/** The re-cut arguments a run passes to `calque dev`. */
export function recutArguments(
  options: Pick<BenchOptions, "recutTailRatio" | "recutMinIntervalSeconds">,
): string[] {
  return [
    ...(options.recutTailRatio === undefined
      ? []
      : ["--recut-tail-ratio", String(options.recutTailRatio)]),
    ...(options.recutMinIntervalSeconds === undefined
      ? []
      : ["--recut-min-interval", String(options.recutMinIntervalSeconds)]),
  ];
}

export function loadTransactionsPerSecond(options: BenchOptions): number {
  return options.loadRate ?? LOAD_TRANSACTIONS_PER_SECOND;
}

export interface BenchPlan {
  readonly preparations: number;
  readonly warmups: number;
  readonly cold: number;
  readonly traced: number;
  readonly probeWarmups: number;
  readonly probes: number;
  /** Blank-page sessions that measure the browser's memory without calque. */
  readonly emptyPages: number;
}

/**
 * A full run's sample counts, or `--smoke`'s. A realtime-only run keeps the
 * probes and prepares once.
 */
export function benchPlan(smoke: boolean, realtimeOnly = false): BenchPlan {
  if (realtimeOnly)
    return {
      ...benchPlan(smoke),
      preparations: 1,
      warmups: 0,
      cold: 0,
      traced: 0,
      emptyPages: 0,
    };
  return smoke
    ? {
        preparations: 1,
        warmups: 1,
        cold: 3,
        traced: 1,
        probeWarmups: 1,
        probes: 5,
        emptyPages: 1,
      }
    : {
        preparations: 3,
        warmups: 2,
        cold: 30,
        traced: 3,
        probeWarmups: 3,
        probes: 30,
        emptyPages: 5,
      };
}

/**
 * A sample record with the conditions it ran under as `conditions`: dataset,
 * network (`null` for `--network off`), whether its page carried a rendered
 * plan, and the edge or null. A key of its own, because a cold, warm-up or
 * traced sample's `network` is its request ledger.
 */
export function withConditions(
  options: Pick<BenchOptions, "dataset" | "network" | "edge">,
  record: object,
): object {
  return {
    ...record,
    conditions: {
      dataset: options.dataset,
      network: options.network,
      renderedPlan: rendersPlan(options),
      edge: edgeRecord(options.edge),
    },
  };
}

export interface ShapingWitness {
  /** The emulated latency, or null under `--network off`. */
  readonly latencyMs: number | null;
  /** Each fetch, from the call to its body's last byte, in the page. */
  readonly fetchMs: readonly number[];
  readonly fastestMs: number | null;
  /**
   * Whether the probes ran shaped, or null where the witness does not apply:
   * nothing was emulated, or the emulated latency is 0 ms, which no fetch
   * can fall short of.
   */
  readonly shaped: boolean | null;
}

/** The same witness under the kernel shaper, against `shapedFloorMs`. */
export interface KernelShapingWitness {
  readonly rttMs: number;
  readonly floorMs: number;
  readonly fetchMs: readonly number[];
  readonly fastestMs: number | null;
  /** Null where the RTT leaves no floor above dummynet's clock. */
  readonly shaped: boolean | null;
}

/** The floor every new request pays, or null where there is none to reach. */
export function shapingFloorMs(
  network: BenchOptions["network"],
): number | null {
  if (network === null) return null;
  const floorMs =
    "shaper" in network ? shapedFloorMs(network) : network.latencyMs;
  return floorMs > 0 ? floorMs : null;
}

/**
 * The emulated latency is a floor under every new request, so the fastest of
 * a few uncached fetches reaches it only while the conditions hold. A slow
 * host can only lengthen a fetch, so the error runs toward `shaped`. Under the
 * kernel shaper the floor is the RTT less dummynet's clock, since a request on
 * an open connection and its response each cross the link once.
 */
export function judgeShaping(
  network: BenchOptions["network"],
  fetchMs: readonly number[],
): ShapingWitness | KernelShapingWitness {
  const fastestMs = fetchMs.length === 0 ? null : Math.min(...fetchMs);
  const floorMs = shapingFloorMs(network);
  const shaped =
    floorMs === null ? null : fastestMs !== null && fastestMs >= floorMs;
  if (network !== null && "shaper" in network)
    return {
      rttMs: network.rttMs,
      floorMs: shapedFloorMs(network),
      fetchMs,
      fastestMs,
      shaped,
    };
  return { latencyMs: network?.latencyMs ?? null, fetchMs, fastestMs, shaped };
}

/** Why a witness that did not hold says the probes ran unshaped. */
export function unshapedFailure(
  shaping: ShapingWitness | KernelShapingWitness,
) {
  const fastest = `the fastest of ${String(shaping.fetchMs.length)} uncached fetches after them took ${shaping.fastestMs?.toFixed(1) ?? "?"} ms`;
  return "rttMs" in shaping
    ? `the probes ran unshaped: ${fastest}, under the floor of ${String(shaping.floorMs)} ms for the kernel shaper's ${String(shaping.rttMs)} ms RTT`
    : `the probes ran unshaped: ${fastest}, under the emulated ${String(shaping.latencyMs)} ms`;
}

/** The read-out's line for a shaping witness. */
export function shapingLine(
  shaping: ShapingWitness | KernelShapingWitness,
): string {
  if ("rttMs" in shaping)
    return shaping.shaped === null
      ? `network: the kernel shaper's ${String(shaping.rttMs)} ms RTT leaves no floor above dummynet's clock, so the shaping witness does not apply`
      : `network: the fastest of ${String(shaping.fetchMs.length)} uncached fetches after the probes took ${shaping.fastestMs?.toFixed(1) ?? "?"} ms, against a floor of ${String(shaping.floorMs)} ms under the kernel shaper's ${String(shaping.rttMs)} ms RTT`;
  if (shaping.shaped === null)
    return shaping.latencyMs === null
      ? "network: not emulated, so the shaping witness does not apply"
      : "network: 0 ms of emulated latency, so the shaping witness does not apply";
  return `network: the fastest of ${String(shaping.fetchMs.length)} uncached fetches after the probes took ${shaping.fastestMs?.toFixed(1) ?? "?"} ms, against ${String(shaping.latencyMs)} ms of emulated latency`;
}
