/** The host a run measures on: its clocks, its load and memory readings, and the record of what it is. */
import { execFile, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { cpus, freemem, loadavg, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import type { BenchPostgres, PostgresRecord } from "./bench-postgres.ts";
import { selectProcessTree } from "./bench-processes.ts";
import { checkoutPath } from "./bench-record.ts";
import {
  type BenchCpuReading,
  DescendantCpu,
  type EnvironmentReading,
  parseCpuTable,
  parseCpuTime,
  parseMemoryFreePercent,
  parsePowerSource,
} from "./bench-results.ts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * Milliseconds the host has slept since boot. On macOS Node's hrtime reads
 * mach_continuous_time, which counts on while the host sleeps, and Chrome's
 * monotonic timestamps read mach_absolute_time, which stops; the two differ
 * by this. Python's ctypes reads both clocks back to back in one process.
 * On Linux Node and Chrome both read CLOCK_MONOTONIC, so nothing is read.
 */
function readHostSleptMs(): number {
  if (process.platform !== "darwin") return 0;
  const result = spawnSync("python3", ["-I", "-c", HOST_SLEPT_PY], {
    encoding: "utf8",
  });
  if (result.error || result.status !== 0)
    throw new Error(
      `could not read the host's sleep time: ${result.error?.message ?? result.stderr}`,
    );
  return Number(BigInt(result.stdout.trim())) / 1_000_000;
}

const HOST_SLEPT_PY = `import ctypes
lib = ctypes.CDLL(None)
lib.mach_absolute_time.restype = ctypes.c_uint64
lib.mach_continuous_time.restype = ctypes.c_uint64
class Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]
timebase = Timebase()
lib.mach_timebase_info(ctypes.byref(timebase))
ticks = lib.mach_continuous_time() - lib.mach_absolute_time()
print(ticks * timebase.numer // timebase.denom)`;

/** The host's sleep time as last read; see `rereadHostSleep`. */
let hostSleptMs: number | null = null;

/**
 * The host's monotonic clock in milliseconds, on Chrome's monotonic clock:
 * Node's hrtime less the host's sleep time as last read.
 */
export function hrtimeMs(): number {
  hostSleptMs ??= readHostSleptMs();
  return Number(process.hrtime.bigint()) / 1_000_000 - hostSleptMs;
}

/**
 * Read the host's sleep time again and return how long the host slept since
 * the last read. Host readings taken across a sleep disagree with Chrome's
 * timestamps by that much; readings taken after this call agree again.
 */
export function rereadHostSleep(): number {
  const previous = hostSleptMs ?? readHostSleptMs();
  hostSleptMs = readHostSleptMs();
  return hostSleptMs - previous;
}

/**
 * The host sleeps for seconds at least, and two reads with no sleep between
 * differ by the nanoseconds between the two clocks' reads.
 */
export const HOST_SLEEP_TOLERANCE_MS = 1;

export function output(
  command: string,
  arguments_: readonly string[],
): string | null {
  const result = spawnSync(command, arguments_, {
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
  });
  return result.error || result.status !== 0 ? null : result.stdout;
}

const descendantCpu = new DescendantCpu();

function readBenchCpu(postgres: BenchPostgres): BenchCpuReading {
  const atMs = hrtimeMs();
  const usage = process.cpuUsage();
  const table = output("ps", ["-axo", "pid=,ppid=,time=,comm="]);
  const processes = table === null ? null : parseCpuTable(table);
  const descendants =
    processes === null
      ? null
      : descendantCpu.observe(
          selectProcessTree(processes, process.pid).filter(
            // The `ps` that printed the table is the runner's child too.
            (row) =>
              row.pid !== process.pid &&
              row.command !== "ps" &&
              !row.command.endsWith("/ps"),
          ),
        );
  return {
    atMs,
    runnerSeconds: (usage.user + usage.system) / 1_000_000,
    descendantsSeconds: descendants?.seconds ?? null,
    descendantsAlive: descendants?.alive ?? null,
    postgresSeconds: postgres.cpuSeconds(processes),
  };
}

export function readEnvironment(postgres: BenchPostgres): EnvironmentReading {
  const [one = 0, five = 0, fifteen = 0] = loadavg();
  return {
    loadAverage: [one, five, fifteen],
    freeMemoryBytes: freemem(),
    memoryFreePercent: parseMemoryFreePercent(
      output("memory_pressure", []) ?? "",
    ),
    powerSource: parsePowerSource(output("pmset", ["-g", "batt"]) ?? ""),
    otherContainersRunning: postgres.otherContainersRunning(),
    benchCpu: readBenchCpu(postgres),
  };
}

export interface HostRecord extends PostgresRecord {
  readonly cpuModel: string;
  readonly cpuCount: number;
  readonly memoryBytes: number;
  readonly os: string;
  readonly commit: string | null;
  readonly dirtyPaths: readonly string[];
  readonly node: string;
  readonly playwright: string | null;
  /** The binary, by `checkoutPath`, and its digest. */
  readonly calque: { readonly path: string; readonly sha256: string };
  browser: unknown;
}

export function readHost(calque: string, postgres: BenchPostgres): HostRecord {
  const playwrightPackage = join(root, "node_modules/playwright/package.json");
  return {
    cpuModel:
      output("sysctl", ["-n", "machdep.cpu.brand_string"])?.trim() ??
      cpus()[0]?.model ??
      "unknown",
    cpuCount: cpus().length,
    memoryBytes: totalmem(),
    os: [
      output("sw_vers", ["-productName"])?.trim(),
      output("sw_vers", ["-productVersion"])?.trim(),
      process.arch,
    ]
      .filter(Boolean)
      .join(" "),
    ...postgres.record(),
    commit: output("git", ["-C", root, "rev-parse", "HEAD"])?.trim() ?? null,
    dirtyPaths: (output("git", ["-C", root, "status", "--porcelain"]) ?? "")
      .split("\n")
      .filter((line) => line.length > 0),
    node: process.version,
    playwright: existsSync(playwrightPackage)
      ? ((
          JSON.parse(readFileSync(playwrightPackage, "utf8")) as {
            version?: string;
          }
        ).version ?? null)
      : null,
    calque: {
      path: checkoutPath(calque),
      sha256: createHash("sha256").update(readFileSync(calque)).digest("hex"),
    },
    browser: null,
  };
}

export const execFileAsync = promisify(execFile);

/** Seconds of CPU `ps` has charged `pid`, user and system together. */
export async function processCpuSeconds(pid: number): Promise<number> {
  const { stdout } = await execFileAsync("ps", [
    "-o",
    "time=",
    "-p",
    String(pid),
  ]);
  const seconds = parseCpuTime(stdout.trim());
  if (seconds === null)
    throw new Error(`ps reported no CPU time for ${String(pid)}`);
  return seconds;
}
