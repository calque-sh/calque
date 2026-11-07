/**
 * The PostgreSQL 18 server a `pnpm bench` run seeds and serves from.
 *
 * By default it is the benchmark's `postgres:18` container. Under
 * `--postgres-bin <dir>` it is a cluster the runner makes with that
 * directory's `initdb` in a new temporary directory, starts with its
 * `pg_ctl`, and deletes when the run ends, for a host whose Docker does not
 * answer. Both start empty with the same superuser, password, database and
 * server flags, listen on 127.0.0.1 only, and run `docker/postgres-init`
 * against the database. A native run is never judged, since the benchmark's
 * frozen environment is the container.
 *
 * The runner reads and writes the server through the host's `psql`: one call
 * per statement, or a persistent session whose statements end in a marker row.
 */
import { spawn, spawnSync, type SpawnSyncReturns } from "node:child_process";
import {
  appendFileSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { psqlArguments } from "./bench-fixture.ts";
import { selectProcessTree } from "./bench-processes.ts";
import {
  DescendantCpu,
  parseCgroupCpuSeconds,
  parseOtherContainers,
  type CpuRow,
} from "./bench-results.ts";
import { delay, exitWithin } from "./demo-process.ts";
import { runChecked, sanitizePostgresRoutingEnvironment } from "./demo.ts";

const CONTAINER = "calque-bench";
const CONTAINER_LABEL_KEY = "calque.bench";
const CONTAINER_LABEL_VALUE = "true";
const POSTGRES_IMAGE = "postgres:18";
/**
 * How long a Docker query may take. A healthy daemon answers in well under a
 * second; one that has stopped answering would otherwise hold the run for
 * good. `docker run` has none, since it may pull the image.
 */
const DOCKER_ANSWER_MS = 30_000;
/** How long `pg_ctl` waits for the native server to start or stop. */
const PG_CTL_WAIT_SECONDS = 60;

/** The container's `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB`. */
const SUPERUSER = "calque";
const SUPERUSER_PASSWORD = "calque";
const DATABASE = "calque_dev";

/** `docker-compose.yml`'s server flags, which both servers take. */
export const SERVER_FLAGS: readonly (readonly [string, string])[] = [
  ["wal_level", "logical"],
  ["max_replication_slots", "10"],
  ["max_wal_senders", "10"],
  ["timezone", "UTC"],
];

/** What a run's host record says of its PostgreSQL server. */
export interface PostgresRecord {
  /** Docker's CPUs, memory, version and OS; null for a native server. */
  readonly docker: unknown;
  /** The container image's digest; null for a native server. */
  readonly postgresImage: string | null;
  /** The native server's `postgres --version`; null for the container. */
  readonly nativePostgres: string | null;
}

export interface BenchPostgres {
  /**
   * Starts an empty server on 127.0.0.1:`port`, with its database made and
   * `docker/postgres-init` run against it.
   */
  start(port: number): void;
  /** Stops and deletes what `start` made, and does nothing when it made nothing. */
  remove(): void;
  /**
   * The server's CPU time so far, in seconds. The native server reads it from
   * `processes`, a `ps` table taken at the same reading; null when unread.
   */
  cpuSeconds(processes: readonly CpuRow[] | null): number | null;
  /**
   * Whether a container other than the benchmark's own was running; null when
   * Docker was not read.
   */
  otherContainersRunning(): boolean | null;
  record(): PostgresRecord;
}

function docker(arguments_: readonly string[]): SpawnSyncReturns<string> {
  return spawnSync("docker", arguments_, {
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    timeout: DOCKER_ANSWER_MS,
  });
}

function dockerOutput(arguments_: readonly string[]): string | null {
  const result = docker(arguments_);
  return result.error || result.status !== 0 ? null : result.stdout;
}

/** The benchmark's own container's label, or null when Docker has none. */
function containerLabel(): string | null {
  const result = docker([
    "inspect",
    "--format",
    `{{index .Config.Labels "${CONTAINER_LABEL_KEY}"}}`,
    CONTAINER,
  ]);
  if (result.error) {
    const answer =
      (result.error as NodeJS.ErrnoException).code === "ETIMEDOUT"
        ? `Docker did not answer within ${String(DOCKER_ANSWER_MS / 1000)} s`
        : `docker could not run (${result.error.message})`;
    throw new Error(
      `${answer}. Start Docker, or pass --postgres-bin <dir> to run PostgreSQL 18's own binaries for a run that judges nothing.`,
    );
  }
  return result.status === 0 ? result.stdout.trim() : null;
}

/**
 * The benchmark's container: `postgres:18` under `docker run --init`, named
 * `calque-bench` and labelled `calque.bench=true`. It refuses a
 * container of that name without the label, and removes its own, volume
 * included.
 */
export class ContainerPostgres implements BenchPostgres {
  readonly #initDirectory: string;

  constructor(initDirectory: string) {
    this.#initDirectory = initDirectory;
  }

  start(port: number): void {
    const label = containerLabel();
    if (label !== null) {
      if (label !== CONTAINER_LABEL_VALUE)
        throw new Error(
          `a container named ${CONTAINER} exists without the label ${CONTAINER_LABEL_KEY}=${CONTAINER_LABEL_VALUE}; the benchmark removes only its own`,
        );
      runChecked("docker", ["rm", "-f", "-v", CONTAINER]);
    }
    runChecked("docker", [
      "run",
      "-d",
      "--init",
      "--name",
      CONTAINER,
      "--label",
      `${CONTAINER_LABEL_KEY}=${CONTAINER_LABEL_VALUE}`,
      "-p",
      `127.0.0.1:${String(port)}:5432`,
      "-e",
      `POSTGRES_USER=${SUPERUSER}`,
      "-e",
      `POSTGRES_PASSWORD=${SUPERUSER_PASSWORD}`,
      "-e",
      `POSTGRES_DB=${DATABASE}`,
      "-v",
      `${this.#initDirectory}:/docker-entrypoint-initdb.d:ro`,
      POSTGRES_IMAGE,
      ...SERVER_FLAGS.flatMap(([name, value]) => ["-c", `${name}=${value}`]),
    ]);
  }

  remove(): void {
    let label: string | null;
    try {
      label = containerLabel();
    } catch {
      return;
    }
    if (label === CONTAINER_LABEL_VALUE) docker(["rm", "-f", "-v", CONTAINER]);
  }

  cpuSeconds(): number | null {
    const stat = dockerOutput([
      "exec",
      CONTAINER,
      "cat",
      "/sys/fs/cgroup/cpu.stat",
    ]);
    return stat === null ? null : parseCgroupCpuSeconds(stat);
  }

  otherContainersRunning(): boolean | null {
    const table = dockerOutput([
      "ps",
      "--format",
      `{{.Names}}\t{{.Label "${CONTAINER_LABEL_KEY}"}}`,
    ]);
    return table === null ? null : parseOtherContainers(table).length > 0;
  }

  record(): PostgresRecord {
    const info = dockerOutput([
      "info",
      "--format",
      "{{json .NCPU}} {{json .MemTotal}} {{json .ServerVersion}} {{json .OperatingSystem}}",
    ]);
    const [ncpu, memTotal, serverVersion, operatingSystem] = (info ?? "")
      .trim()
      .split(" ")
      .map((field) => {
        try {
          return JSON.parse(field) as unknown;
        } catch {
          return null;
        }
      });
    return {
      docker: { ncpu, memTotal, serverVersion, operatingSystem },
      postgresImage:
        dockerOutput([
          "image",
          "inspect",
          POSTGRES_IMAGE,
          "--format",
          "{{index .RepoDigests 0}}",
        ])?.trim() ?? null,
      nativePostgres: null,
    };
  }
}

/** The major version in `postgres --version`'s output, or null. */
export function parsePostgresMajor(output: string): number | null {
  const match = /^postgres \(PostgreSQL\) ([0-9]+)/u.exec(output.trim());
  return match?.[1] === undefined ? null : Number(match[1]);
}

/**
 * The lines the native cluster's `postgresql.conf` ends with. initdb writes
 * the host's time zone into `log_timezone`; the container's is UTC. There is
 * no Unix socket, as a temporary directory's path can pass its length limit.
 */
export function nativeServerSettings(port: number): string {
  return [
    "",
    "# pnpm bench: the container's flags and listener.",
    ...SERVER_FLAGS.map(([name, value]) => `${name} = '${value}'`),
    "log_timezone = 'UTC'",
    "listen_addresses = '127.0.0.1'",
    `port = ${String(port)}`,
    "unix_socket_directories = ''",
    "",
  ].join("\n");
}

/**
 * A cluster of the PostgreSQL 18 binaries in `bin`, made by `initdb` as the
 * container's entrypoint makes its own: UTF-8 and `en_US.UTF-8`, the
 * superuser's password from a file, SCRAM over TCP. Its server log goes to
 * `logFile`, beside the run's other logs.
 */
export class NativePostgres implements BenchPostgres {
  readonly #bin: string;
  readonly #initDirectory: string;
  readonly #logFile: string;
  readonly #cpu = new DescendantCpu();
  #directory: string | null = null;
  #postmaster: number | null = null;
  #version: string | null = null;

  constructor(bin: string, initDirectory: string, logFile: string) {
    this.#bin = bin;
    this.#initDirectory = initDirectory;
    this.#logFile = logFile;
  }

  #run(program: string, arguments_: readonly string[]): string {
    return runChecked(join(this.#bin, program), arguments_, {
      env: sanitizePostgresRoutingEnvironment(process.env),
    }).stdout;
  }

  #data(): string {
    if (this.#directory === null) throw new Error("PostgreSQL is not running");
    return join(this.#directory, "data");
  }

  start(port: number): void {
    const version = this.#run("postgres", ["--version"]).trim();
    if (parsePostgresMajor(version) !== 18)
      throw new Error(
        `--postgres-bin needs PostgreSQL 18, as the container runs, and its postgres reports ${JSON.stringify(version)}`,
      );
    this.#version = version;
    const unsupported = readdirSync(this.#initDirectory).filter(
      (name) => !name.endsWith(".sql"),
    );
    if (unsupported.length > 0)
      throw new Error(
        `docker/postgres-init holds ${unsupported.join(", ")}, which only the container's entrypoint runs`,
      );

    this.#directory = mkdtempSync(join(tmpdir(), "calque-bench-postgres-"));
    const passwordFile = join(this.#directory, "password");
    writeFileSync(passwordFile, `${SUPERUSER_PASSWORD}\n`, { mode: 0o600 });
    this.#run("initdb", [
      "--pgdata",
      this.#data(),
      "--username",
      SUPERUSER,
      `--pwfile=${passwordFile}`,
      "--auth-host=scram-sha-256",
      "--auth-local=trust",
      "--encoding=UTF8",
      "--locale=en_US.UTF-8",
    ]);
    appendFileSync(
      join(this.#data(), "postgresql.conf"),
      nativeServerSettings(port),
    );
    // `-l` sends the server's output to the log, not to this process's pipes,
    // which the server would otherwise hold open after `pg_ctl` returns.
    this.#run("pg_ctl", [
      "--pgdata",
      this.#data(),
      "--log",
      this.#logFile,
      "--wait",
      `--timeout=${String(PG_CTL_WAIT_SECONDS)}`,
      "start",
    ]);
    const pid = Number(
      readFileSync(join(this.#data(), "postmaster.pid"), "utf8").split("\n")[0],
    );
    this.#postmaster = Number.isSafeInteger(pid) && pid > 0 ? pid : null;

    const psql = (database: string, ...arguments_: string[]) =>
      runChecked(
        join(this.#bin, "psql"),
        ["--no-psqlrc", "--quiet", "--set=ON_ERROR_STOP=1", ...arguments_],
        {
          env: {
            ...sanitizePostgresRoutingEnvironment(process.env),
            PGHOST: "127.0.0.1",
            PGPORT: String(port),
            PGUSER: SUPERUSER,
            PGPASSWORD: SUPERUSER_PASSWORD,
            PGDATABASE: database,
          },
        },
      );
    psql("postgres", "--command", `CREATE DATABASE ${DATABASE}`);
    for (const name of readdirSync(this.#initDirectory).sort())
      psql(DATABASE, "--file", join(this.#initDirectory, name));
  }

  remove(): void {
    if (this.#directory === null) return;
    const data = this.#data();
    const stop = (mode: string) =>
      spawnSync(
        join(this.#bin, "pg_ctl"),
        [
          "--pgdata",
          data,
          "--mode",
          mode,
          "--wait",
          `--timeout=${String(PG_CTL_WAIT_SECONDS)}`,
          "stop",
        ],
        {
          encoding: "utf8",
          env: sanitizePostgresRoutingEnvironment(process.env),
        },
      );
    // A server that never started, or already stopped, has no postmaster.pid
    // to stop; one that ignores a fast shutdown is stopped without one.
    const fast = stop("fast");
    if (fast.status !== 0 && this.#postmaster !== null) stop("immediate");
    rmSync(this.#directory, { recursive: true, force: true });
    this.#directory = null;
    this.#postmaster = null;
  }

  /**
   * The postmaster's process tree, each process at the last reading that saw
   * it: a backend that came and went between two readings is not counted.
   * `pg_ctl` leaves the postmaster outside the runner's tree, so the runner's
   * descendants never count it as well.
   */
  cpuSeconds(processes: readonly CpuRow[] | null): number | null {
    if (processes === null || this.#postmaster === null) return null;
    return this.#cpu.observe(selectProcessTree(processes, this.#postmaster))
      .seconds;
  }

  otherContainersRunning(): null {
    return null;
  }

  record(): PostgresRecord {
    return { docker: null, postgresImage: null, nativePostgres: this.#version };
  }
}

export const SOURCE_ROLE = "calque_sync";

export const ADMIN_ROLE = "calque";

export function databaseUrl(
  port: number,
  role: string,
  password: string,
): string {
  return `postgresql://${role}:${password}@127.0.0.1:${String(port)}/calque_dev`;
}

const PSQL_ARGUMENTS = [...psqlArguments, "--field-separator=\t"];

/** SQL through the host's `psql`; rows come back tab-separated. */
export function runSql(sql: string, environment: NodeJS.ProcessEnv): string {
  const result = spawnSync("psql", PSQL_ARGUMENTS, {
    env: environment,
    encoding: "utf8",
    input: sql,
    maxBuffer: 1024 * 1024 * 1024,
  });
  if (result.error)
    throw new Error(`psql could not run: ${result.error.message}`);
  if (result.status !== 0)
    throw new Error(`psql failed: ${result.stderr.trim()}`);
  return result.stdout;
}

export async function waitForPostgres(
  environment: NodeJS.ProcessEnv,
): Promise<void> {
  const deadline = Date.now() + 120_000;
  let last = "";
  while (Date.now() < deadline) {
    try {
      if (runSql("SELECT 1;\n", environment).trim() === "1") return;
    } catch (error) {
      last = error instanceof Error ? error.message : String(error);
    }
    await delay(500);
  }
  throw new Error(
    `PostgreSQL did not accept ${SOURCE_ROLE} within 120 s: ${last}`,
  );
}

/** One persistent `psql` whose statements are awaited by a marker row. */
export class PsqlSession {
  readonly #child;
  readonly #waiters = new Map<
    string,
    {
      readonly resolve: (lines: string[]) => void;
      readonly reject: (error: Error) => void;
    }
  >();
  #pending = "";
  #output: string[] = [];
  #stderr = "";
  #exit: Error | null = null;
  readonly #exited: Promise<void>;

  constructor(environment: NodeJS.ProcessEnv, name: string) {
    this.#child = spawn("psql", PSQL_ARGUMENTS, {
      env: { ...environment, PGAPPNAME: name },
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.#child.stdout.setEncoding("utf8");
    this.#child.stdout.on("data", (chunk: string) => {
      this.#pending += chunk;
      const lines = this.#pending.split("\n");
      this.#pending = lines.pop() ?? "";
      for (const line of lines) {
        const waiter = this.#waiters.get(line.trim());
        if (!waiter) {
          this.#output.push(line);
          continue;
        }
        this.#waiters.delete(line.trim());
        const output = this.#output;
        this.#output = [];
        waiter.resolve(output);
      }
    });
    this.#child.stderr.setEncoding("utf8");
    this.#child.stderr.on("data", (chunk: string) => {
      this.#stderr = (this.#stderr + chunk).slice(-8_192);
    });
    this.#exited = new Promise((resolveExit) => {
      this.#child.once("exit", (code, signal) => {
        this.#exit = new Error(
          `psql ${name} exited (${String(code ?? signal)}): ${this.#stderr.trim()}`,
        );
        for (const waiter of this.#waiters.values()) waiter.reject(this.#exit);
        this.#waiters.clear();
        resolveExit();
      });
    });
  }

  async run(sql: string, marker: string): Promise<void> {
    await this.query(sql, marker);
  }

  /** The rows `sql` printed before its marker. */
  query(sql: string, marker: string): Promise<string[]> {
    if (this.#exit) return Promise.reject(this.#exit);
    return new Promise((resolveRun, rejectRun) => {
      this.#waiters.set(marker, { resolve: resolveRun, reject: rejectRun });
      this.#child.stdin.write(`${sql}SELECT '${marker}';\n`);
    });
  }

  async close(): Promise<void> {
    if (!this.#exit) this.#child.stdin.end();
    await exitWithin(this.#exited, 10_000);
    if (!this.#exit) this.#child.kill("SIGKILL");
  }
}
