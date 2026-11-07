/**
 * The Caddy release the benchmark's edge runs: the version the
 * self-host composition pins as `caddy:2.11.4-alpine`, as the release's own
 * build for the benchmark's one platform.
 *
 * The build is pinned twice, as the browser is. The archive's SHA-512, which
 * the release publishes in `caddy_2.11.4_checksums.txt`, proves the download;
 * the binary's SHA-256 proves what runs, and is recomputed before every run.
 */
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

import { fileSha256 } from "./bench-browser.ts";
import { download } from "./bench-fetch-browser.ts";

export interface CaddyPin {
  readonly version: string;
  readonly platform: "mac_arm64";
  readonly url: string;
  readonly archiveBytes: number;
  /** As the release's checksums file lists it. */
  readonly archiveSha512: string;
  /** The `caddy` binary the archive holds. */
  readonly binarySha256: string;
}

export const CADDY: CaddyPin = Object.freeze({
  version: "2.11.4",
  platform: "mac_arm64",
  url: "https://github.com/caddyserver/caddy/releases/download/v2.11.4/caddy_2.11.4_mac_arm64.tar.gz",
  archiveBytes: 16_448_366,
  archiveSha512:
    "3190ae0df98b59ab4b6021556fa35adc3c526a4f3e138776b0eaec8a037cc26121cbbb1ad53453f565551b47d37d5ba4755e2c2c3652256737fe2ce9e53c8ec0",
  binarySha256:
    "e9ebf99dfd4b72259debe1830c83e86c63fb89a88e28b4e7c5e78a35fa76c92d",
});

/** Where the pinned build is fetched to, shared by every worktree. */
export function caddyDirectory(
  pin: CaddyPin = CADDY,
  environment: NodeJS.ProcessEnv = process.env,
): string {
  return join(
    environment.CALQUE_BENCH_CADDY_CACHE ??
      join(homedir(), ".cache", "calque", "caddy"),
    pin.version,
    pin.platform,
  );
}

interface PinnedCaddy {
  readonly executablePath: string;
  readonly version: string;
  readonly binarySha256: string;
}

/**
 * The pinned binary, after its SHA-256 has been recomputed and matched.
 * Every run that has an edge calls this, through `fetchCaddy`, before it
 * starts the edge.
 */
export function requireCaddy(
  pin: CaddyPin = CADDY,
  directory: string = caddyDirectory(pin),
): PinnedCaddy {
  const executablePath = join(directory, "caddy");
  if (!existsSync(executablePath))
    throw new Error(`Caddy ${pin.version} is not at ${executablePath}`);
  const { sha256 } = fileSha256(executablePath);
  if (sha256 !== pin.binarySha256)
    throw new Error(
      `${executablePath} has SHA-256 ${sha256}; the pin is ${pin.binarySha256}`,
    );
  return { executablePath, version: pin.version, binarySha256: sha256 };
}

/** The archive's path in the cache, beside the directory it unpacks to. */
export function caddyArchivePath(directory: string): string {
  return `${directory}.tar.gz`;
}

function untar(archive: string, into: string): void {
  mkdirSync(into, { recursive: true });
  const result = spawnSync(
    "tar",
    ["-x", "-z", "-f", archive, "-C", into, "caddy"],
    {
      stdio: ["ignore", "inherit", "inherit"],
    },
  );
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(`tar exited ${String(result.status)} unpacking ${archive}`);
}

interface FetchCaddyOptions {
  readonly pin?: CaddyPin;
  readonly directory?: string;
  readonly log?: (line: string) => void;
}

/**
 * The pinned binary, fetched into the cache first when the cache does not
 * hold it. The archive is unpacked only after its length and SHA-512 match
 * the pin, and the binary replaces what the cache holds only after its
 * SHA-256 does.
 */
export async function fetchCaddy(
  options: FetchCaddyOptions = {},
): Promise<PinnedCaddy> {
  const pin = options.pin ?? CADDY;
  const directory = options.directory ?? caddyDirectory(pin);
  const log = options.log ?? ((line) => process.stdout.write(`${line}\n`));

  if (existsSync(join(directory, "caddy")))
    try {
      return requireCaddy(pin, directory);
    } catch (error) {
      log(
        `replacing the cached Caddy: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

  mkdirSync(dirname(directory), { recursive: true });
  const archive = caddyArchivePath(directory);
  if (!existsSync(archive) || statSync(archive).size !== pin.archiveBytes) {
    log(`downloading ${pin.url}`);
    await download(pin.url, archive);
  }
  const bytes = readFileSync(archive);
  const sha512 = createHash("sha512").update(bytes).digest("hex");
  if (bytes.length !== pin.archiveBytes || sha512 !== pin.archiveSha512) {
    rmSync(archive, { force: true });
    throw new Error(
      `${archive} is ${String(bytes.length)} bytes with SHA-512 ${sha512}; the pin is ${String(pin.archiveBytes)} bytes with ${pin.archiveSha512}`,
    );
  }

  const staging = `${directory}.staging`;
  rmSync(staging, { recursive: true, force: true });
  try {
    untar(archive, staging);
    const { sha256 } = fileSha256(join(staging, "caddy"));
    if (sha256 !== pin.binarySha256)
      throw new Error(
        `the extracted binary's SHA-256 is ${sha256}; the pin is ${pin.binarySha256}`,
      );
    chmodSync(join(staging, "caddy"), 0o755);
    rmSync(directory, { recursive: true, force: true });
    renameSync(staging, directory);
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
  const installed = requireCaddy(pin, directory);
  log(`installed: ${installed.executablePath}`);
  return installed;
}
