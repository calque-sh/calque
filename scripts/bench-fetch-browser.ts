/**
 * Fetches the Chrome for Testing build the benchmark pins into the
 * shared cache, and installs it only once both pins hold: the archive's
 * length and SHA-256 before anything is unpacked, then the extracted bundle's
 * digest before it replaces what the cache holds.
 *
 *   pnpm bench:fetch-browser
 *
 * With a pin still null it stops at that pin and prints the observed value,
 * which is how the pins are first recorded. The archive stays in the cache
 * between runs and is verified again every time it is used.
 */
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  renameSync,
  rmSync,
  statSync,
  writeSync,
} from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  bundleDigest,
  CHROME_FOR_TESTING,
  chromeForTestingApp,
  chromeForTestingDirectory,
  type ChromeForTestingPin,
  requireArchive,
  requireChromeForTesting,
} from "./bench-browser.ts";

interface FetchOptions {
  readonly pin?: ChromeForTestingPin;
  readonly directory?: string;
  /** Extracts `archive` into `into`; `ditto` unless a test replaces it. */
  readonly unpack?: (archive: string, into: string) => void;
  readonly log?: (line: string) => void;
}

function ditto(archive: string, into: string): void {
  const result = spawnSync("ditto", ["-x", "-k", archive, into], {
    stdio: ["ignore", "inherit", "inherit"],
  });
  if (result.error) throw result.error;
  if (result.status !== 0)
    throw new Error(
      `ditto exited ${String(result.status)} unpacking ${archive}`,
    );
}

/** The archive's path in the cache, beside the directory it unpacks to. */
export function archivePath(directory: string): string {
  return join(dirname(directory), `${basename(directory)}.zip`);
}

/**
 * Downloads `url` to `path` through a `.partial` file. When the response
 * carries Google Cloud Storage's MD5 (`x-goog-hash: md5=...`), the bytes are
 * checked against it; the pin is checked separately by the caller.
 */
export async function download(url: string, path: string): Promise<void> {
  const response = await fetch(url);
  if (!response.ok || response.body === null)
    throw new Error(`GET ${url} answered ${String(response.status)}`);
  const md5 = createHash("md5");
  const partial = `${path}.partial`;
  const descriptor = openSync(partial, "w");
  try {
    const reader = response.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      md5.update(value);
      for (let offset = 0; offset < value.length;)
        offset += writeSync(descriptor, value, offset);
    }
  } finally {
    closeSync(descriptor);
  }
  const served = /(?:^|,)\s*md5=([^,\s]+)/.exec(
    response.headers.get("x-goog-hash") ?? "",
  )?.[1];
  const observed = md5.digest("base64");
  if (served !== undefined && served !== observed) {
    rmSync(partial, { force: true });
    throw new Error(
      `${url} arrived with MD5 ${observed}; the server sent ${served}`,
    );
  }
  renameSync(partial, path);
}

export async function fetchChromeForTesting(
  options: FetchOptions = {},
): Promise<string> {
  const pin = options.pin ?? CHROME_FOR_TESTING;
  const directory = options.directory ?? chromeForTestingDirectory(pin);
  const unpack = options.unpack ?? ditto;
  const log = options.log ?? ((line) => process.stdout.write(`${line}\n`));

  if (pin.bundleSha256 !== null && existsSync(chromeForTestingApp(directory)))
    try {
      const present = requireChromeForTesting(pin, directory);
      log(`already installed: ${present.executablePath}`);
      return present.executablePath;
    } catch (error) {
      log(
        `replacing the cached build: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

  mkdirSync(dirname(directory), { recursive: true });
  const archive = archivePath(directory);
  if (!existsSync(archive) || statSync(archive).size !== pin.archiveBytes) {
    log(`downloading ${pin.url}`);
    await download(pin.url, archive);
  }
  let verified: ReturnType<typeof requireArchive>;
  try {
    verified = requireArchive(archive, pin);
  } catch (error) {
    // A pinned archive that fails its pin is downloaded again next time; an
    // unpinned one is kept, since recording its pin is why it was fetched.
    if (pin.archiveSha256 !== null) rmSync(archive, { force: true });
    throw error;
  }
  log(`archive ${String(verified.bytes)} bytes, SHA-256 ${verified.sha256}`);

  const staging = `${directory}.staging`;
  rmSync(staging, { recursive: true, force: true });
  try {
    unpack(archive, staging);
    const digest = bundleDigest(chromeForTestingApp(staging));
    if (pin.bundleSha256 === null)
      throw new Error(
        `${pin.version} has no bundle pin; the extracted bundle's digest is ${digest}`,
      );
    if (digest !== pin.bundleSha256)
      throw new Error(
        `the extracted bundle's digest is ${digest}; the pin is ${pin.bundleSha256}`,
      );
    rmSync(directory, { recursive: true, force: true });
    renameSync(staging, directory);
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
  const installed = requireChromeForTesting(pin, directory);
  log(`installed: ${installed.executablePath}`);
  return installed.executablePath;
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === fileURLToPath(import.meta.url)) {
  try {
    if (process.platform !== "darwin" || process.arch !== "arm64")
      throw new Error(
        `the pinned build is ${CHROME_FOR_TESTING.platform}; this host is ${process.platform}-${process.arch}`,
      );
    await fetchChromeForTesting();
  } catch (error) {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
