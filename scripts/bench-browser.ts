/**
 * The browser the benchmark drives: one pinned Chrome for Testing
 * build whose screencast can keep the last frame.
 *
 * Chromium 149 drops a screencast frame that arrives while three are in
 * flight and never sends it; a page that has stopped painting then never
 * presents its final state. Chromium 154 adds `maxFramesInFlight` and
 * `sendLastFrame` to `Page.startScreencast`, and 149 ignores both without an
 * error, so the runner judges the version every session reports.
 *
 * The build is pinned twice. The archive's SHA-256 proves the download; the
 * bundle digest, over every file and symlink in the extracted app, proves what
 * runs, since on macOS the executable is a small launcher and the code lives
 * in the framework beside it.
 */
import { createHash } from "node:crypto";
import {
  closeSync,
  existsSync,
  lstatSync,
  openSync,
  readdirSync,
  readlinkSync,
  readSync,
  realpathSync,
} from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, relative, sep } from "node:path";
import type { CDPSession } from "playwright";

import type { PictureSizeFault } from "./bench-frames.ts";

export interface ChromeForTestingPin {
  readonly version: string;
  readonly platform: "mac-arm64";
  readonly url: string;
  readonly archiveBytes: number;
  /** Null until the archive has been fetched and hashed on the reference host. */
  readonly archiveSha256: string | null;
  /** Null until the verified archive has been extracted and digested. */
  readonly bundleSha256: string | null;
}

export const CHROME_FOR_TESTING: ChromeForTestingPin = Object.freeze({
  version: "154.0.8037.57",
  platform: "mac-arm64",
  url: "https://storage.googleapis.com/chrome-for-testing-public/154.0.8037.57/mac-arm64/chrome-mac-arm64.zip",
  archiveBytes: 191_429_663,
  archiveSha256:
    "0e6b3439469c1b8b95b2e89c72ea29f7af00fb2c28a8878358a0b6002b6d3a64",
  bundleSha256:
    "78f28d9d6401b94bd3900d321a52856118925b12da129361c2d1777d080c350c",
});

/**
 * `Page.startScreencast`'s parameters as Chromium 154 defines them.
 * Playwright 1.61.1's protocol types declare 149's parameters as a type
 * alias, which no declaration can extend; a value of this type still passes
 * where the alias is expected, and the compiler checks the fields both have.
 */
interface ScreencastParameters {
  readonly format: "png";
  readonly everyNthFrame: number;
  /** Frames sent and not yet acknowledged before Chrome drops the next. */
  readonly maxFramesInFlight: number;
  /** Keep the latest dropped frame and send it on the next acknowledgement. */
  readonly sendLastFrame: boolean;
}

/**
 * Three frames in flight is Chromium 149's own limit, so the only difference
 * from 149 is that a frame Chrome would drop is kept and sent on the next
 * acknowledgement. Passing the default keeps a later change of default from
 * changing the instrument.
 */
export const SCREENCAST_PARAMETERS: ScreencastParameters = Object.freeze({
  format: "png",
  everyNthFrame: 1,
  maxFramesInFlight: 3,
  sendLastFrame: true,
});

/** Every screencast the runner starts goes through here. */
export async function startScreencast(
  cdp: Pick<CDPSession, "send">,
): Promise<void> {
  await cdp.send("Page.startScreencast", SCREENCAST_PARAMETERS);
}

/** Refuses a session whose browser is not the pinned build. */
export function requireBrowserVersion(
  reported: string,
  pin: ChromeForTestingPin = CHROME_FOR_TESTING,
): void {
  if (reported !== pin.version)
    throw new Error(
      `the browser reported version ${reported}, not ${pin.version}`,
    );
}

/** Where the pinned build is fetched to, shared by every worktree. */
export function chromeForTestingDirectory(
  pin: ChromeForTestingPin = CHROME_FOR_TESTING,
  environment: NodeJS.ProcessEnv = process.env,
): string {
  return join(
    environment.CALQUE_BENCH_BROWSER_CACHE ??
      join(homedir(), ".cache", "calque", "chrome-for-testing"),
    pin.version,
    pin.platform,
  );
}

export function chromeForTestingApp(directory: string): string {
  return join(directory, "chrome-mac-arm64", "Google Chrome for Testing.app");
}

export function chromeForTestingExecutable(directory: string): string {
  return join(
    chromeForTestingApp(directory),
    "Contents",
    "MacOS",
    "Google Chrome for Testing",
  );
}

/** A file's SHA-256 and length, read in 8 MiB pieces. */
export function fileSha256(path: string): {
  readonly sha256: string;
  readonly bytes: number;
} {
  const hash = createHash("sha256");
  const buffer = Buffer.alloc(8 << 20);
  const descriptor = openSync(path, "r");
  let bytes = 0;
  try {
    for (;;) {
      const read = readSync(descriptor, buffer, 0, buffer.length, null);
      if (read === 0) break;
      hash.update(buffer.subarray(0, read));
      bytes += read;
    }
  } finally {
    closeSync(descriptor);
  }
  return { sha256: hash.digest("hex"), bytes };
}

/**
 * The SHA-256 of one line per entry under `root`, sorted by relative path:
 * each directory, each file with its SHA-256 and whether it is executable,
 * and each symlink with its target. Symlinks are recorded, never followed,
 * and refused unless they resolve inside `root`, so that the digest covers
 * every byte a link reaches.
 */
export function bundleDigest(root: string): string {
  const lines: string[] = [];
  // The system's realpath, which resolves `..` after a link as the kernel
  // does; Node's own resolves it lexically.
  const realRoot = realpathSync.native(root);
  const walk = (directory: string) => {
    for (const name of readdirSync(directory)) {
      const path = join(directory, name);
      const entry = relative(root, path);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink()) {
        const target = readlinkSync(path);
        const reached = existsSync(path)
          ? relative(realRoot, realpathSync.native(path))
          : null;
        if (
          reached === null ||
          reached === ".." ||
          reached.startsWith(`..${sep}`) ||
          isAbsolute(reached)
        )
          throw new Error(
            `${path} links to ${target}, which does not resolve inside ${root}`,
          );
        lines.push(`link ${entry} -> ${target}`);
      } else if (stat.isDirectory()) {
        lines.push(`dir ${entry}`);
        walk(path);
      } else if (stat.isFile())
        lines.push(
          `file ${entry} ${fileSha256(path).sha256} ${(stat.mode & 0o111) === 0 ? "-" : "x"}`,
        );
      else throw new Error(`${path} is neither a file, a directory nor a link`);
    }
  };
  walk(root);
  lines.sort((a, b) => {
    const left = a.slice(a.indexOf(" ") + 1);
    const right = b.slice(b.indexOf(" ") + 1);
    return left < right ? -1 : left > right ? 1 : 0;
  });
  return createHash("sha256")
    .update(`${lines.join("\n")}\n`)
    .digest("hex");
}

/** Refuses an archive whose length or SHA-256 is not the pin's. */
export function requireArchive(
  path: string,
  pin: ChromeForTestingPin = CHROME_FOR_TESTING,
): { readonly sha256: string; readonly bytes: number } {
  const observed = fileSha256(path);
  if (pin.archiveSha256 === null)
    throw new Error(
      `${pin.version} has no archive pin; ${path} is ${String(observed.bytes)} bytes with SHA-256 ${observed.sha256}`,
    );
  if (
    observed.bytes !== pin.archiveBytes ||
    observed.sha256 !== pin.archiveSha256
  )
    throw new Error(
      `${path} is ${String(observed.bytes)} bytes with SHA-256 ${observed.sha256}; the pin is ${String(pin.archiveBytes)} bytes with ${pin.archiveSha256}`,
    );
  return observed;
}

interface PinnedBrowser {
  readonly executablePath: string;
  readonly bundleSha256: string;
}

/**
 * The pinned build's executable, after its bundle digest has been recomputed
 * and matched. Every run calls this before its first launch.
 */
export function requireChromeForTesting(
  pin: ChromeForTestingPin = CHROME_FOR_TESTING,
  directory: string = chromeForTestingDirectory(pin),
): PinnedBrowser {
  if (pin.bundleSha256 === null)
    throw new Error(`Chrome for Testing ${pin.version} has no bundle pin yet`);
  const app = chromeForTestingApp(directory);
  if (!existsSync(app))
    throw new Error(
      `Chrome for Testing ${pin.version} is not at ${app}; run pnpm bench:fetch-browser`,
    );
  const digest = bundleDigest(app);
  if (digest !== pin.bundleSha256)
    throw new Error(
      `${app} has bundle digest ${digest}; the pin is ${pin.bundleSha256}`,
    );
  return {
    executablePath: chromeForTestingExecutable(directory),
    bundleSha256: digest,
  };
}

/**
 * An error that measured the instrument rather than the page: the frame
 * hasher refused a picture for its size, or Chrome refused a screenshot on
 * every attempt the runner makes. Each is carried as data by the error that
 * reports it, never read from a message.
 */
export type InstrumentFault =
  | PictureSizeFault
  | { readonly kind: "screenshot-refused"; readonly attempts: number };

/**
 * The instrument fault an error carries as its `fault` property, or null. A
 * timeout, a page crash or any other error carries none.
 */
export function instrumentFault(error: unknown): InstrumentFault | null {
  const fault: unknown =
    typeof error === "object" && error !== null && "fault" in error
      ? error.fault
      : null;
  if (typeof fault !== "object" || fault === null || !("kind" in fault))
    return null;
  return fault.kind === "picture-size" || fault.kind === "screenshot-refused"
    ? (fault as InstrumentFault)
    : null;
}

/** The launchd services jetsam ended within a stretch of the host's log. */
export interface JetsamKills {
  readonly count: number;
  /** Local timestamps, as the unified log prints them. */
  readonly first: string | null;
  readonly last: string | null;
}

const JETSAM_EXIT =
  /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) .*OS_REASON_JETSAM/u;

/** The jetsam exits in `log show --style compact` output. */
export function parseJetsamKills(text: string): JetsamKills {
  const stamps = text.split("\n").flatMap((line) => {
    const stamp = JETSAM_EXIT.exec(line)?.[1];
    return stamp === undefined ? [] : [stamp];
  });
  return {
    count: stamps.length,
    first: stamps[0] ?? null,
    last: stamps.at(-1) ?? null,
  };
}

/** A time as `log show --start` reads it: local, to the second. */
export function unifiedLogTime(at: Date): string {
  const two = (value: number) => String(value).padStart(2, "0");
  return `${String(at.getFullYear())}-${two(at.getMonth() + 1)}-${two(at.getDate())} ${two(at.getHours())}:${two(at.getMinutes())}:${two(at.getSeconds())}`;
}

/** How long before a browser failure the host's jetsam exits are read. */
export const BROWSER_EXIT_LOOKBACK_MS = 30_000;

/**
 * Why a browser or its page went away during a run. On the macOS host the
 * benchmark has run on, every such loss came a second or two after macOS
 * jetsam ended a wave of idle services for memory, with no crash report from
 * Chrome, so a loss inside a wave, or with free memory under 15%, reads as the
 * host's.
 */
export type BrowserExitCause = "host-memory" | "unknown";

export function browserExitCause(
  jetsamKills: JetsamKills | null,
  freeMemoryPercent: number | null,
): BrowserExitCause {
  return (jetsamKills?.count ?? 0) > 0 ||
    (freeMemoryPercent !== null && freeMemoryPercent < 15)
    ? "host-memory"
    : "unknown";
}

/** Whether an error says Chrome, its page or its context went away. */
export function isBrowserLoss(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /Target crashed|Target page, context or browser has been closed|Browser has been closed|browser has disconnected/iu.test(
    message,
  );
}
