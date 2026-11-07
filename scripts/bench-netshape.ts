/**
 * The kernel loopback shaper under `pnpm bench -- --network
 * shaped:<Mbit/s>/<RTT ms>`.
 *
 * `calque-netshape` runs as root through a sudoers rule for its three verbs,
 * installed by hand as `scripts/netshape/README.md` describes. The runner
 * refuses unless the installed script is byte for byte the checkout's
 * source, binds every listener the page reaches to a port the shaper accepts,
 * calls `up` before the reference pass and `down` on every exit, and checks
 * before the first sample and after the last that the clamp and the delay
 * reach those ports, and that the clamp reaches no other.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer } from "node:net";

/** Where the shaper is installed by hand, root:wheel 0755. */
export const NETSHAPE = "/usr/local/libexec/calque-netshape";
/** The reviewed source, relative to the checkout. */
export const NETSHAPE_SOURCE = "scripts/netshape/calque-netshape";
/** The kernel check's probe, relative to the checkout. */
export const NETSHAPE_PROBE = "scripts/netshape/probe.py";
/** The ports the shaper accepts, below the ephemeral range. */
export const SHAPED_PORT_MIN = 45_600;
export const SHAPED_PORT_MAX = 45_699;
/** The shaper's scrub `max-mss`; loopback's own MSS is about 16 KB. */
const MSS_CLAMP = 1_460;
/**
 * dummynet reads its clock in whole milliseconds (ip_dummynet.c), so each
 * crossing of a pipe can fall up to 1 ms short of half the RTT.
 */
const DUMMYNET_CLOCK_MS = 1;
/** How many connects the kernel check makes to each port. */
const KERNEL_CHECK_CONNECTS = 3;

/** `--network shaped:<Mbit/s>/<RTT ms>`: the kernel shaper on loopback. */
export interface ShapedNetwork {
  readonly shaper: "calque-netshape";
  readonly megabitsPerSecond: number;
  readonly rttMs: number;
}

export const SHAPED_PREFIX = "shaped:";

/**
 * What the kernel shaper models, for `plan.network`. It replaces Chrome's
 * emulation, whose biases it does not have.
 */
export const SHAPER_CAVEAT =
  "calque-netshape shapes every packet to and from the page server's and calque's ports on lo0 through one pair of dummynet pipes, at the rate each way and half the RTT of delay each way, and clamps their MSS to 1460, so the handshake, the CORS preflight, TCP slow start and ACK clocking pay the link, and the browser decodes while the kernel keeps receiving. It adds no random loss and no jitter: a packet is dropped only when the pipe's drop-tail queue, about one bandwidth-delay product from 64 KiB to 1 MiB, is full. It is still loopback: no NIC, and the browser, the page server, calque and PostgreSQL share the host's CPUs.";

/** `<Mbit/s>/<RTT ms>` within the bounds the shaper accepts. */
export function parseShapedNetwork(text: string): ShapedNetwork {
  const match = /^([1-9]\d{0,3})\/(0|[1-9]\d{0,3})$/u.exec(text);
  const megabitsPerSecond = Number(match?.[1]);
  const rttMs = Number(match?.[2]);
  if (!match || megabitsPerSecond > 2_000 || rttMs > 1_000 || rttMs % 2 !== 0)
    throw new Error(
      "--network shaped: takes <Mbit/s>/<RTT ms> as calque-netshape accepts them, whole Mbit/s from 1 to 2000 and an even RTT from 0 to 1000 ms, like shaped:500/20",
    );
  return { shaper: "calque-netshape", megabitsPerSecond, rttMs };
}

/**
 * The least time a request and its response can take over the shaper: the
 * RTT, less dummynet's clock on each of the two crossings.
 */
export function shapedFloorMs(network: ShapedNetwork): number {
  return network.rttMs - 2 * DUMMYNET_CLOCK_MS;
}

// Ports ---------------------------------------------------------------------------

function listens(port: number): Promise<boolean> {
  const server = createServer();
  return new Promise((resolveListen) => {
    server.once("error", () => resolveListen(false));
    server.listen({ host: "127.0.0.1", port }, () =>
      server.close(() => resolveListen(true)),
    );
  });
}

/** A port in 45600–45699 that is free on 127.0.0.1 and not in `taken`. */
export async function availableShapedPort(
  taken: ReadonlySet<number> = new Set(),
): Promise<number> {
  const count = SHAPED_PORT_MAX - SHAPED_PORT_MIN + 1;
  const start = Math.floor(Math.random() * count);
  for (let offset = 0; offset < count; offset += 1) {
    const port = SHAPED_PORT_MIN + ((start + offset) % count);
    if (!taken.has(port) && (await listens(port))) return port;
  }
  throw new Error(
    `no port in ${String(SHAPED_PORT_MIN)}-${String(SHAPED_PORT_MAX)} is free on 127.0.0.1`,
  );
}

// The shaper's status ---------------------------------------------------------------

/** `status` with nothing shaped: no rule, no pipe, no pf reference. */
export function shaperDisabled(status: string): boolean {
  return status.trim() === "enable token: none";
}

/**
 * What `status` after `up` gets wrong: pipe 55601 at another rate or delay,
 * or no pf reference. `dnctl pipe show` prints only the first pipe it is
 * given, so 55602, configured the same way just after it, is not shown.
 */
export function shaperStatusProblems(
  status: string,
  network: ShapedNetwork,
): string[] {
  const problems: string[] = [];
  const pipe = /^0*55601: +(\d+(?:\.\d+)?) Mbit\/s +(\d+) ms /mu.exec(status);
  if (!pipe) problems.push("no pipe 55601");
  else {
    if (Number(pipe[1]) !== network.megabitsPerSecond)
      problems.push(
        `pipe 55601 runs at ${pipe[1] ?? "?"} Mbit/s, not ${String(network.megabitsPerSecond)}`,
      );
    if (Number(pipe[2]) !== network.rttMs / 2)
      problems.push(
        `pipe 55601 delays ${pipe[2] ?? "?"} ms, not ${String(network.rttMs / 2)}`,
      );
  }
  if (!/^enable token: \d+$/mu.test(status))
    problems.push("pf holds no enable token from it");
  return problems;
}

// The kernel check ------------------------------------------------------------------

/** One port's connects, as the probe reports them. */
interface PortReading {
  readonly port: number;
  readonly connectMs: readonly number[];
  readonly mss: readonly number[];
}

/** The probe's report: the shaped ports, and its own unshaped control. */
export interface KernelProbe {
  readonly shaped: readonly PortReading[];
  readonly control: PortReading;
}

interface KernelCheck extends KernelProbe {
  readonly floorMs: number;
  readonly problems: readonly string[];
  readonly passed: boolean;
}

function portReading(value: unknown): PortReading {
  const reading = value as Partial<Record<keyof PortReading, unknown>> | null;
  const numbers = (list: unknown) =>
    Array.isArray(list) && list.every((item) => typeof item === "number");
  if (
    typeof reading?.port !== "number" ||
    !numbers(reading.connectMs) ||
    !numbers(reading.mss)
  )
    throw new Error(`the probe reported ${JSON.stringify(value)}`);
  return reading as PortReading;
}

/** `probe.py witness`'s JSON, refused unless every reading has its fields. */
export function parseKernelProbe(text: string): KernelProbe {
  const report = JSON.parse(text) as { shaped?: unknown; control?: unknown };
  if (!Array.isArray(report.shaped))
    throw new Error("the probe reported no shaped ports");
  return {
    shaped: report.shaped.map(portReading),
    control: portReading(report.control),
  };
}

/**
 * Every listed port, and only those, clamped to at most 1460 with its fastest
 * connect at least `shapedFloorMs`, and the control outside the shaper's
 * range above the clamp. A clamp that reaches the control is the leak a
 * `port = N` scrub rule causes on macOS; the script refuses such rules, and
 * this checks the kernel.
 */
export function judgeKernel(
  network: ShapedNetwork,
  ports: readonly number[],
  probe: KernelProbe,
): KernelCheck {
  const floorMs = shapedFloorMs(network);
  const problems: string[] = [];
  const reported = probe.shaped.map((reading) => reading.port);
  if (JSON.stringify(reported) !== JSON.stringify(ports))
    problems.push(
      `the probe reported ports ${reported.join(", ")}, not ${ports.join(", ")}`,
    );
  for (const reading of probe.shaped) {
    if (reading.connectMs.length === 0 || reading.mss.length === 0)
      problems.push(`port ${String(reading.port)}: no connect`);
    const wide = reading.mss.filter((mss) => mss > MSS_CLAMP);
    if (wide.length > 0)
      problems.push(
        `port ${String(reading.port)}: MSS ${wide.join(", ")}, above the clamp's ${String(MSS_CLAMP)}`,
      );
    const fastest = Math.min(...reading.connectMs);
    if (floorMs > 0 && fastest < floorMs)
      problems.push(
        `port ${String(reading.port)}: the fastest connect took ${fastest.toFixed(3)} ms, under ${String(floorMs)} ms`,
      );
  }
  const control = probe.control;
  if (control.port >= SHAPED_PORT_MIN && control.port <= SHAPED_PORT_MAX)
    problems.push(
      `the control's port ${String(control.port)} is inside the shaper's range`,
    );
  if (control.mss.length === 0) problems.push("the control made no connect");
  const clamped = control.mss.filter((mss) => mss <= MSS_CLAMP);
  if (clamped.length > 0)
    problems.push(
      `the control on port ${String(control.port)} settled on MSS ${clamped.join(", ")}: the clamp reaches an unshaped port`,
    );
  return { ...probe, floorMs, problems, passed: problems.length === 0 };
}

/** The read-out's line for one kernel check. */
export function kernelLine(when: string, check: KernelCheck): string {
  const ports = check.shaped.map(
    (reading) =>
      `port ${String(reading.port)} MSS ${reading.mss.join("/")}, fastest connect ${Math.min(...reading.connectMs).toFixed(1)} ms`,
  );
  return `kernel check ${when}: ${check.passed ? "held" : `failed (${check.problems.join("; ")})`}: ${ports.join("; ")}, against a floor of ${String(check.floorMs)} ms; control port ${String(check.control.port)} MSS ${check.control.mss.join("/")}`;
}

// The preflight ----------------------------------------------------------------------

/** One request in a cold sample's `network.byRequest`, as far as it is read here. */
export interface LedgerRequest {
  readonly url: string;
  readonly type: string | null;
  readonly status: number | null;
  readonly sentMs: number;
  readonly endedMs: number | null;
}

/**
 * The CORS preflight before `/v1/bootstrap` in one cold sample's ledger, sent
 * to end in milliseconds: the request to that path Chrome types `Other`, which
 * calque answers 204. The `Fetch` that follows it is the GET. Null when the
 * ledger shows none, or shows one that never ended.
 */
export function bootstrapPreflightMs(
  requests: readonly LedgerRequest[],
): number | null {
  const preflight = requests.find(
    (request) =>
      request.type === "Other" &&
      request.status === 204 &&
      /\/v1\/bootstrap(?:\?|$)/u.test(request.url),
  );
  return preflight?.endedMs == null
    ? null
    : preflight.endedMs - preflight.sentMs;
}

/**
 * Whether the preflight before `/v1/bootstrap` paid the link in every judged
 * cold sample: `shaped` is false when the fastest took less than
 * `shapedFloorMs`, and null, with the reason, when the run is not kernel-shaped
 * or a sample's ledger does not show the preflight. Chrome's emulation adds no
 * latency to a preflight; this witness exposes that.
 */
interface PreflightWitness {
  readonly shaped: boolean | null;
  readonly floorMs: number | null;
  readonly fastestMs: number | null;
  readonly samples: readonly {
    readonly name: string;
    readonly ms: number | null;
  }[];
  readonly reason: string | null;
}

export function judgePreflight(
  network: ShapedNetwork | { readonly latencyMs: number } | null,
  samples: readonly {
    readonly name: string;
    readonly requests: readonly LedgerRequest[];
  }[],
): PreflightWitness {
  if (network === null || !("shaper" in network))
    return {
      shaped: null,
      floorMs: null,
      fastestMs: null,
      samples: [],
      reason: "not a kernel-shaped run",
    };
  const floorMs = shapedFloorMs(network);
  const read = samples.map((sample) => ({
    name: sample.name,
    ms: bootstrapPreflightMs(sample.requests),
  }));
  const shown = read.flatMap((sample) =>
    sample.ms === null ? [] : [sample.ms],
  );
  const fastestMs = shown.length === 0 ? null : Math.min(...shown);
  const missing = read.length - shown.length;
  if (read.length === 0 || missing > 0)
    return {
      shaped: null,
      floorMs,
      fastestMs,
      samples: read,
      reason:
        read.length === 0
          ? "no judged cold sample"
          : `${String(missing)} of ${String(read.length)} judged cold samples' ledgers show no ended OPTIONS to /v1/bootstrap answered 204`,
    };
  return {
    shaped: fastestMs !== null && fastestMs >= floorMs,
    floorMs,
    fastestMs,
    samples: read,
    reason: null,
  };
}

/** The read-out's line for the preflight witness. */
export function preflightLine(witness: PreflightWitness): string {
  if (witness.shaped === null)
    return `preflight: not shown: ${witness.reason ?? "?"}`;
  return `preflight: the OPTIONS to /v1/bootstrap took ${witness.fastestMs?.toFixed(1) ?? "?"} ms at the fastest of ${String(witness.samples.length)} judged cold samples, against a floor of ${String(witness.floorMs)} ms${witness.shaped ? "" : ", so the preflight was not shaped"}`;
}

// The shaper ------------------------------------------------------------------------

interface CommandResult {
  readonly status: number | null;
  readonly stdout: string;
  readonly stderr: string;
  readonly error: string | null;
}

export type RunCommand = (
  command: string,
  arguments_: readonly string[],
) => CommandResult;

const runCommand: RunCommand = (command, arguments_) => {
  const result = spawnSync(command, arguments_, {
    encoding: "utf8",
    timeout: 60_000,
  });
  return {
    status: result.status,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? "",
    error: result.error?.message ?? null,
  };
};

interface ShaperSeams {
  readonly run: RunCommand;
  readonly read: (path: string) => Uint8Array;
}

const defaultSeams: ShaperSeams = { run: runCommand, read: readFileSync };

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/**
 * One run's use of `calque-netshape`. `down` runs at most once, only after an
 * `up` was attempted, and never throws, so every exit path can call it.
 */
export class KernelShaper {
  #sha256: string | null = null;
  #python: string | null = null;
  #ports: readonly number[] = [];
  #upAttempted = false;
  #downDone = false;
  #statusAfterUp: string | null = null;
  #statusAfterDown: string | null = null;
  #before: KernelCheck | null = null;
  #after: KernelCheck | null = null;

  constructor(
    readonly network: ShapedNetwork,
    /** The checkout's `NETSHAPE_SOURCE` and `NETSHAPE_PROBE`. */
    private readonly source: string,
    private readonly probe: string,
    private readonly seams: ShaperSeams = defaultSeams,
  ) {}

  /** Refuses unless the installed script's SHA-256 is the source's. */
  requireInstalled(): void {
    const installed = sha256(this.seams.read(NETSHAPE));
    const reviewed = sha256(this.seams.read(this.source));
    if (installed !== reviewed)
      throw new Error(
        `${NETSHAPE} has SHA-256 ${installed} and ${NETSHAPE_SOURCE} has ${reviewed}: install the reviewed script as scripts/netshape/README.md says before a shaped run`,
      );
    this.#sha256 = installed;
  }

  #sudo(verb: string, arguments_: readonly string[] = []): string {
    const result = this.seams.run("sudo", [
      "-n",
      NETSHAPE,
      verb,
      ...arguments_,
    ]);
    if (result.error !== null || result.status !== 0)
      throw new Error(
        `sudo -n ${NETSHAPE} ${[verb, ...arguments_].join(" ")} failed: ${result.error ?? (result.stderr.trim() || `exit ${String(result.status)}`)}`,
      );
    return result.stdout;
  }

  /**
   * Before anything starts: the installed script is the source, sudo's rule
   * answers, nothing is shaped yet, and the probe's Python runs.
   */
  preflight(): void {
    this.requireInstalled();
    const status = this.#sudo("status");
    if (!shaperDisabled(status))
      throw new Error(
        `calque-netshape is already up, held by another run or left by one that could not tear down; with no run going, sudo -n ${NETSHAPE} down:\n${status.trim()}`,
      );
    const python = this.seams.run("python3", ["--version"]);
    if (python.error !== null || python.status !== 0)
      throw new Error(
        `the kernel check needs python3: ${python.error ?? python.stderr.trim()}`,
      );
    this.#python = python.stdout.trim();
  }

  /**
   * `up` on `ports`, its status read back, and the kernel check before the
   * first sample. Any failure throws, and the run judges nothing.
   */
  engage(ports: readonly number[]): void {
    this.requireInstalled();
    this.#ports = [...ports];
    this.#upAttempted = true;
    this.#sudo("up", [
      String(this.network.megabitsPerSecond),
      String(this.network.rttMs),
      ...ports.map(String),
    ]);
    this.#statusAfterUp = this.#sudo("status");
    const problems = shaperStatusProblems(this.#statusAfterUp, this.network);
    if (problems.length > 0)
      throw new Error(
        `calque-netshape's status after up: ${problems.join("; ")}`,
      );
    this.#before = this.#check();
    if (!this.#before.passed)
      throw new Error(
        `the kernel check before the first sample failed: ${this.#before.problems.join("; ")}`,
      );
  }

  /** The kernel check after the last sample: a shaping failure, or null. */
  checkAfter(): string | null {
    try {
      this.#after = this.#check();
    } catch (error) {
      return `the kernel check after the last sample failed: ${error instanceof Error ? error.message : String(error)}`;
    }
    return this.#after.passed
      ? null
      : `the kernel check after the last sample failed: ${this.#after.problems.join("; ")}`;
  }

  #check(): KernelCheck {
    const result = this.seams.run("python3", [
      this.probe,
      "witness",
      String(KERNEL_CHECK_CONNECTS),
      ...this.#ports.map(String),
    ]);
    if (result.error !== null || result.status !== 0)
      throw new Error(
        `the probe failed: ${result.error ?? (result.stderr.trim() || `exit ${String(result.status)}`)}`,
      );
    return judgeKernel(
      this.network,
      this.#ports,
      parseKernelProbe(result.stdout),
    );
  }

  /**
   * `down` and its status read back, once, if `up` was attempted. Returns
   * whether the status then read disabled, or null with nothing to tear
   * down.
   */
  down(): boolean | null {
    if (!this.#upAttempted || this.#downDone) return null;
    this.#downDone = true;
    try {
      this.#sudo("down");
      this.#statusAfterDown = this.#sudo("status");
    } catch (error) {
      this.#statusAfterDown = `down failed: ${error instanceof Error ? error.message : String(error)}`;
    }
    return shaperDisabled(this.#statusAfterDown);
  }

  /** The kernel checks so far: before the first sample, after the last. */
  get checks(): {
    readonly before: KernelCheck | null;
    readonly after: KernelCheck | null;
  } {
    return { before: this.#before, after: this.#after };
  }

  /** The instrument as `plan.network` records it. */
  record() {
    return {
      ...this.network,
      installed: NETSHAPE,
      sha256: this.#sha256,
      python: this.#python,
      ports: this.#ports,
      statusAfterUp: this.#statusAfterUp,
      statusAfterDown: this.#statusAfterDown,
      kernelChecks: { before: this.#before, after: this.#after },
    };
  }
}
