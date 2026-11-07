/**
 * The guard that keeps the background clients from taking the host's network
 * memory. Clients that fall behind leave their unread responses in socket
 * buffers, and across thousands of loopback sockets that once ran macOS out
 * of mbufs, which also dropped Docker's link to its VM.
 *
 * On macOS the guard reads `netstat -m` before the clients start and every
 * second while they run. A denied request for network memory, or mbufs in use
 * past `SAFE_FRACTION` of the pool, stops the clients and voids the arm.
 * Elsewhere it never trips.
 */
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

/** What one `netstat -m` reading holds that the guard judges. */
export interface NetworkMemory {
  /** Bytes in mbufs and mbuf clusters in use, caches excluded. */
  readonly inUseBytes: number;
  /** Requests for network memory the kernel denied since boot. */
  readonly deniedRequests: number;
}

/**
 * The share of the mbuf pool the clients may leave in use: half, well short
 * of the 80% (`kern.ipc.mb_memory_pressure_percentage`) at which macOS starts
 * reclaiming sockets' memory.
 */
export const SAFE_FRACTION = 0.5;
const POLL_MS = 1_000;
const MBUF_BYTES = 256;
const CLUSTER_BYTES: Readonly<Record<string, number>> = {
  "2KB": 2_048,
  "4KB": 4_096,
  "16KB": 16_384,
};

/** The mbufs and clusters in use and the denied requests `text` reports. */
export function parseNetstatMemory(text: string): NetworkMemory {
  const mbufs = /^(\d+)\/\d+ mbufs in use/mu.exec(text);
  const denied = /^(\d+) requests for memory denied/mu.exec(text);
  if (!mbufs?.[1] || !denied?.[1])
    throw new Error(
      "netstat -m reported no mbufs in use or no denied requests",
    );
  let inUseBytes = Number(mbufs[1]) * MBUF_BYTES;
  for (const [, count, size] of text.matchAll(
    /^(\d+)\/\d+(?:\/\d+)? mbuf (2KB|4KB|16KB) clusters in use/gmu,
  ))
    inUseBytes += Number(count) * (CLUSTER_BYTES[size ?? ""] ?? 0);
  return { inUseBytes, deniedRequests: Number(denied[1]) };
}

/**
 * Why the clients must stop, judged against `before`, the reading taken
 * before they started, or null. `poolBytes` is the mbuf pool:
 * `kern.ipc.nmbclusters` 2 KB clusters.
 */
export function networkMemoryProblem(
  before: NetworkMemory,
  now: NetworkMemory,
  poolBytes: number,
): string | null {
  const denied = now.deniedRequests - before.deniedRequests;
  if (denied > 0)
    return `the host denied ${String(denied)} requests for network memory`;
  if (now.inUseBytes >= SAFE_FRACTION * poolBytes)
    return `the host had ${mib(now.inUseBytes)} MiB of network memory in use, ${percent(now.inUseBytes / poolBytes)} of its ${mib(poolBytes)} MiB pool and past the ${percent(SAFE_FRACTION)} the background clients may take`;
  return null;
}

const mib = (bytes: number) => (bytes / 2 ** 20).toFixed(0);
const percent = (share: number) => `${(share * 100).toFixed(0)}%`;

async function readNetworkMemory(): Promise<NetworkMemory> {
  const { stdout } = await execFileAsync("netstat", ["-m"]);
  return parseNetstatMemory(stdout);
}

function readPoolBytes(): number {
  const clusters = Number(
    execFileSync("sysctl", ["-n", "kern.ipc.nmbclusters"], {
      encoding: "utf8",
    }).trim(),
  );
  if (!Number.isInteger(clusters) || clusters <= 0)
    throw new Error("sysctl reported no kern.ipc.nmbclusters");
  return clusters * CLUSTER_BYTES["2KB"]!;
}

/** Raised when the guard voids the arm, with the reason it was voided. */
export class NetworkMemoryVoid extends Error {
  constructor(readonly reason: string) {
    super(`the arm is void: ${reason}`);
  }
}

/** Watches the host's network memory while the background clients run. */
export class NetworkMemoryGuard {
  #reason: string | null = null;
  #timer: NodeJS.Timeout | undefined;

  private constructor(
    private readonly before: NetworkMemory | null,
    private readonly poolBytes: number,
  ) {}

  /**
   * Reads the host before the clients start, and throws `NetworkMemoryVoid`
   * if it already has more network memory in use than the clients may take.
   * A host whose memory cannot be read is refused too: the guard fails
   * closed.
   */
  static async before(): Promise<NetworkMemoryGuard> {
    if (process.platform !== "darwin") return new NetworkMemoryGuard(null, 0);
    let before: NetworkMemory;
    let poolBytes: number;
    try {
      before = await readNetworkMemory();
      poolBytes = readPoolBytes();
    } catch (error) {
      throw new NetworkMemoryVoid(
        `the network memory guard could not read the host: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    const problem = networkMemoryProblem(before, before, poolBytes);
    if (problem !== null)
      throw new NetworkMemoryVoid(`before the clients started, ${problem}`);
    return new NetworkMemoryGuard(before, poolBytes);
  }

  /** Why the guard tripped, or null while it has not. */
  get reason(): string | null {
    return this.#reason;
  }

  /** Reads the host every second and calls `trip` once, on the first problem. */
  watch(trip: (reason: string) => void): void {
    const before = this.before;
    if (before === null) return;
    const poll = async () => {
      let problem: string | null;
      try {
        problem = networkMemoryProblem(
          before,
          await readNetworkMemory(),
          this.poolBytes,
        );
      } catch (error) {
        problem = `the network memory guard could not read the host: ${error instanceof Error ? error.message : String(error)}`;
      }
      if (this.#timer === undefined) return;
      if (problem === null) {
        this.#timer = setTimeout(() => void poll(), POLL_MS);
        return;
      }
      this.#timer = undefined;
      this.#reason = problem;
      trip(problem);
    };
    this.#timer = setTimeout(() => void poll(), POLL_MS);
  }

  stop(): void {
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }
}
