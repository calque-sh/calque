import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import type { CDPSession } from "playwright";

import {
  browserExitCause,
  bundleDigest,
  CHROME_FOR_TESTING,
  chromeForTestingApp,
  chromeForTestingDirectory,
  chromeForTestingExecutable,
  type ChromeForTestingPin,
  fileSha256,
  isBrowserLoss,
  parseJetsamKills,
  requireArchive,
  requireBrowserVersion,
  requireChromeForTesting,
  startScreencast,
  unifiedLogTime,
} from "./bench-browser.ts";
import { archivePath, fetchChromeForTesting } from "./bench-fetch-browser.ts";

function scratch(t: test.TestContext): string {
  const directory = mkdtempSync(join(tmpdir(), "bench-browser-"));
  t.after(() => {
    rmSync(directory, { recursive: true, force: true });
  });
  return directory;
}

function sha256(bytes: string | Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** A small app bundle shaped like the real one: a launcher, a framework, links. */
function fakeApp(directory: string): string {
  const app = chromeForTestingApp(directory);
  const versions = join(
    app,
    "Contents",
    "Frameworks",
    "Framework.framework",
    "Versions",
  );
  mkdirSync(join(app, "Contents", "MacOS"), { recursive: true });
  mkdirSync(join(versions, "A"), { recursive: true });
  writeFileSync(chromeForTestingExecutable(directory), "launcher");
  chmodSync(chromeForTestingExecutable(directory), 0o755);
  writeFileSync(join(versions, "A", "Framework"), "the browser");
  writeFileSync(join(app, "Contents", "Info.plist"), "<plist/>");
  symlinkSync("A", join(versions, "Current"));
  return app;
}

test("a session is refused unless its browser reports the pinned version", () => {
  requireBrowserVersion("154.0.8037.57");
  for (const reported of ["149.0.7827.55", "154.0.8037.56"])
    assert.throws(
      () => {
        requireBrowserVersion(reported);
      },
      {
        message: `the browser reported version ${reported}, not 154.0.8037.57`,
      },
    );
});

test("the build is cached per version and platform, under an overridable root", () => {
  assert.equal(
    chromeForTestingDirectory(CHROME_FOR_TESTING, {
      CALQUE_BENCH_BROWSER_CACHE: "/cache",
    }),
    "/cache/154.0.8037.57/mac-arm64",
  );
  assert.match(
    chromeForTestingDirectory(CHROME_FOR_TESTING, {}),
    /\/\.cache\/calque\/chrome-for-testing\/154\.0\.8037\.57\/mac-arm64$/,
  );
  assert.equal(
    chromeForTestingExecutable("/d"),
    "/d/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
  );
});

test("a file's digest is its SHA-256 and length, across read pieces", (t) => {
  const directory = scratch(t);
  const bytes = Buffer.alloc((8 << 20) + 3, 7);
  writeFileSync(join(directory, "big"), bytes);
  writeFileSync(join(directory, "empty"), "");
  assert.deepEqual(fileSha256(join(directory, "big")), {
    sha256: sha256(bytes),
    bytes: bytes.length,
  });
  assert.deepEqual(fileSha256(join(directory, "empty")), {
    sha256: sha256(""),
    bytes: 0,
  });
});

test("the bundle digest changes with any byte, mode, link target or entry", (t) => {
  const directory = scratch(t);
  const app = fakeApp(directory);
  const framework = join(
    app,
    "Contents/Frameworks/Framework.framework/Versions/A/Framework",
  );
  const current = join(
    app,
    "Contents/Frameworks/Framework.framework/Versions/Current",
  );
  const digest = bundleDigest(app);
  assert.match(digest, /^[0-9a-f]{64}$/);
  assert.equal(bundleDigest(app), digest);

  const changes: [string, () => void, () => void][] = [
    [
      "a framework byte",
      () => {
        writeFileSync(framework, "the browsex");
      },
      () => {
        writeFileSync(framework, "the browser");
      },
    ],
    [
      "the launcher's execute bit",
      () => {
        chmodSync(chromeForTestingExecutable(directory), 0o644);
      },
      () => {
        chmodSync(chromeForTestingExecutable(directory), 0o755);
      },
    ],
    [
      "a link target, even one reaching the same directory",
      () => {
        unlinkSync(current);
        symlinkSync(join("..", "Versions", "A"), current);
      },
      () => {
        unlinkSync(current);
        symlinkSync("A", current);
      },
    ],
    [
      "an added file",
      () => {
        writeFileSync(join(app, "Contents", "extra"), "");
      },
      () => {
        unlinkSync(join(app, "Contents", "extra"));
      },
    ],
    [
      "an added directory",
      () => {
        mkdirSync(join(app, "Contents", "Resources"));
      },
      () => {
        rmSync(join(app, "Contents", "Resources"), { recursive: true });
      },
    ],
  ];
  for (const [name, change, undo] of changes) {
    change();
    assert.notEqual(bundleDigest(app), digest, name);
    undo();
    assert.equal(bundleDigest(app), digest, `${name}, undone`);
  }
});

test("the bundle digest records a link and does not follow it", (t) => {
  const app = fakeApp(scratch(t));
  // Followed, a link to its own directory would never end the walk.
  symlinkSync(".", join(app, "Contents", "loop"));
  assert.match(bundleDigest(app), /^[0-9a-f]{64}$/u);
});

test("the bundle digest refuses a link that resolves outside the app", (t) => {
  for (const [name, target] of [
    ["absolute", null],
    ["relative", "../../outside"],
    ["dangling", "Info.plist.missing"],
    // Lexically `Contents/outside`, but `up` is the app itself, so `up/..`
    // is the directory beside it.
    ["through a link", "up/../outside"],
  ] as const) {
    const app = fakeApp(scratch(t));
    const outside = join(dirname(app), "outside");
    writeFileSync(outside, "not the browser");
    symlinkSync("..", join(app, "Contents", "up"));
    assert.match(bundleDigest(app), /^[0-9a-f]{64}$/u, name);
    symlinkSync(target ?? outside, join(app, "Contents", "outward"));
    assert.throws(
      () => bundleDigest(app),
      /links to .* which does not resolve inside/u,
      name,
    );
  }
});

test("the bundle digest does not depend on where the bundle is", (t) => {
  const one = scratch(t);
  const two = scratch(t);
  assert.equal(bundleDigest(fakeApp(one)), bundleDigest(fakeApp(two)));
});

function pin(overrides: Partial<ChromeForTestingPin>): ChromeForTestingPin {
  return { ...CHROME_FOR_TESTING, ...overrides };
}

test("an archive is refused unless its length and SHA-256 are the pin's", (t) => {
  const directory = scratch(t);
  const archive = join(directory, "chrome.zip");
  writeFileSync(archive, "archive");
  const digest = sha256("archive");

  assert.throws(
    () =>
      requireArchive(archive, pin({ archiveBytes: 7, archiveSha256: null })),
    {
      message: `154.0.8037.57 has no archive pin; ${archive} is 7 bytes with SHA-256 ${digest}`,
    },
  );
  assert.throws(
    () =>
      requireArchive(
        archive,
        pin({ archiveBytes: 7, archiveSha256: sha256("other") }),
      ),
    /the pin is 7 bytes with/,
  );
  assert.throws(
    () =>
      requireArchive(archive, pin({ archiveBytes: 8, archiveSha256: digest })),
    /the pin is 8 bytes with/,
  );
  assert.deepEqual(
    requireArchive(archive, pin({ archiveBytes: 7, archiveSha256: digest })),
    { sha256: digest, bytes: 7 },
  );
});

test("a run is refused unless the extracted bundle matches the pin", (t) => {
  const directory = scratch(t);
  assert.throws(
    () => requireChromeForTesting(pin({ bundleSha256: null }), directory),
    /has no bundle pin yet/,
  );
  assert.throws(
    () =>
      requireChromeForTesting(pin({ bundleSha256: "0".repeat(64) }), directory),
    /is not at .*; run pnpm bench:fetch-browser/,
  );
  const app = fakeApp(directory);
  const digest = bundleDigest(app);
  assert.throws(
    () =>
      requireChromeForTesting(pin({ bundleSha256: "0".repeat(64) }), directory),
    {
      message: `${app} has bundle digest ${digest}; the pin is ${"0".repeat(64)}`,
    },
  );
  assert.deepEqual(
    requireChromeForTesting(pin({ bundleSha256: digest }), directory),
    {
      executablePath: chromeForTestingExecutable(directory),
      bundleSha256: digest,
    },
  );
});

test("the shipped pin names Chrome for Testing 154.0.8037.57 for mac-arm64", () => {
  assert.equal(CHROME_FOR_TESTING.version, "154.0.8037.57");
  assert.match(CHROME_FOR_TESTING.archiveSha256 ?? "", /^[0-9a-f]{64}$/u);
  assert.match(CHROME_FOR_TESTING.bundleSha256 ?? "", /^[0-9a-f]{64}$/u);
  assert.equal(
    CHROME_FOR_TESTING.url,
    `https://storage.googleapis.com/chrome-for-testing-public/${CHROME_FOR_TESTING.version}/${CHROME_FOR_TESTING.platform}/chrome-${CHROME_FOR_TESTING.platform}.zip`,
  );
});

interface Served {
  readonly url: string;
  readonly requests: () => number;
}

async function serve(
  t: test.TestContext,
  body: Buffer,
  md5: string | null,
): Promise<Served> {
  let requests = 0;
  const server = createServer((_request, response) => {
    requests += 1;
    response.writeHead(200, {
      "content-length": String(body.length),
      ...(md5 === null ? {} : { "x-goog-hash": `crc32c=AAAAAA==,md5=${md5}` }),
    });
    response.end(body);
  });
  await new Promise<void>((resolveListen) => {
    server.listen(0, "127.0.0.1", resolveListen);
  });
  t.after(() => {
    server.close();
  });
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${String(port)}/chrome-mac-arm64.zip`,
    requests: () => requests,
  };
}

function md5Base64(bytes: Buffer): string {
  return createHash("md5").update(bytes).digest("base64");
}

/** A fetch with a fake unpacker that records each call and builds `fakeApp`. */
function fetchInto(
  directory: string,
  pinned: ChromeForTestingPin,
): { run: () => Promise<string>; unpacked: string[]; lines: string[] } {
  const unpacked: string[] = [];
  const lines: string[] = [];
  return {
    unpacked,
    lines,
    run: () =>
      fetchChromeForTesting({
        pin: pinned,
        directory,
        unpack: (archive, into) => {
          unpacked.push(archive);
          fakeApp(into);
        },
        log: (line) => lines.push(line),
      }),
  };
}

const ARCHIVE = Buffer.from("the archive's bytes");

test("an archive that fails its SHA-256 pin is never unpacked, and is dropped", async (t) => {
  const served = await serve(t, ARCHIVE, md5Base64(ARCHIVE));
  const directory = join(scratch(t), "154", "mac-arm64");
  const fetcher = fetchInto(
    directory,
    pin({
      url: served.url,
      archiveBytes: ARCHIVE.length,
      archiveSha256: sha256("other bytes"),
      bundleSha256: "0".repeat(64),
    }),
  );
  await assert.rejects(fetcher.run(), /the pin is 19 bytes with/);
  assert.deepEqual(fetcher.unpacked, []);
  assert.equal(existsSync(archivePath(directory)), false);
  assert.equal(existsSync(directory), false);
});

test("an unpinned archive is kept, not unpacked, and its SHA-256 printed", async (t) => {
  const served = await serve(t, ARCHIVE, null);
  const directory = join(scratch(t), "154", "mac-arm64");
  const fetcher = fetchInto(
    directory,
    pin({ url: served.url, archiveBytes: ARCHIVE.length, archiveSha256: null }),
  );
  await assert.rejects(fetcher.run(), {
    message: `154.0.8037.57 has no archive pin; ${archivePath(directory)} is 19 bytes with SHA-256 ${sha256(ARCHIVE)}`,
  });
  assert.deepEqual(fetcher.unpacked, []);
  assert.equal(existsSync(archivePath(directory)), true);
});

test("bytes that fail the server's MD5 are refused before the pin is read", async (t) => {
  const served = await serve(t, ARCHIVE, md5Base64(Buffer.from("other")));
  const directory = join(scratch(t), "154", "mac-arm64");
  const fetcher = fetchInto(
    directory,
    pin({
      url: served.url,
      archiveBytes: ARCHIVE.length,
      archiveSha256: sha256(ARCHIVE),
    }),
  );
  await assert.rejects(fetcher.run(), /arrived with MD5 .*; the server sent/);
  assert.deepEqual(fetcher.unpacked, []);
  assert.equal(existsSync(archivePath(directory)), false);
  assert.equal(existsSync(`${archivePath(directory)}.partial`), false);
});

test("a bundle is installed only when its digest is the pin's", async (t) => {
  const served = await serve(t, ARCHIVE, md5Base64(ARCHIVE));
  const root = scratch(t);
  const expected = bundleDigest(fakeApp(join(root, "reference")));
  const directory = join(root, "154", "mac-arm64");
  const archivePinned = {
    url: served.url,
    archiveBytes: ARCHIVE.length,
    archiveSha256: sha256(ARCHIVE),
  };

  const unpinned = fetchInto(
    directory,
    pin({ ...archivePinned, bundleSha256: null }),
  );
  await assert.rejects(unpinned.run(), {
    message: `154.0.8037.57 has no bundle pin; the extracted bundle's digest is ${expected}`,
  });
  assert.equal(unpinned.unpacked.length, 1);
  assert.equal(existsSync(directory), false);
  assert.equal(existsSync(`${directory}.staging`), false);

  const wrong = fetchInto(
    directory,
    pin({ ...archivePinned, bundleSha256: "0".repeat(64) }),
  );
  await assert.rejects(wrong.run(), {
    message: `the extracted bundle's digest is ${expected}; the pin is ${"0".repeat(64)}`,
  });
  assert.equal(existsSync(directory), false);

  const right = fetchInto(
    directory,
    pin({ ...archivePinned, bundleSha256: expected }),
  );
  assert.equal(await right.run(), chromeForTestingExecutable(directory));
  assert.equal(existsSync(`${directory}.staging`), false);
  assert.equal(bundleDigest(chromeForTestingApp(directory)), expected);
  // The first run downloaded; the rest verified the cached archive again.
  assert.equal(served.requests(), 1);

  const again = fetchInto(
    directory,
    pin({ ...archivePinned, bundleSha256: expected }),
  );
  assert.equal(await again.run(), chromeForTestingExecutable(directory));
  assert.deepEqual(again.unpacked, []);
  assert.match(again.lines[0] ?? "", /^already installed: /);

  writeFileSync(chromeForTestingExecutable(directory), "tampered");
  const repair = fetchInto(
    directory,
    pin({ ...archivePinned, bundleSha256: expected }),
  );
  assert.equal(await repair.run(), chromeForTestingExecutable(directory));
  assert.equal(repair.unpacked.length, 1);
  assert.match(repair.lines[0] ?? "", /^replacing the cached build: /);
  assert.equal(bundleDigest(chromeForTestingApp(directory)), expected);
});

test("every screencast asks for three frames in flight and the last frame kept", async () => {
  const sent: [string, unknown][] = [];
  await startScreencast({
    send: (method: string, params?: unknown) => {
      sent.push([method, params]);
      return Promise.resolve({});
    },
  } as Pick<CDPSession, "send">);
  assert.deepEqual(sent, [
    [
      "Page.startScreencast",
      {
        format: "png",
        everyNthFrame: 1,
        maxFramesInFlight: 3,
        sendLastFrame: true,
      },
    ],
  ]);
  // The runner starts no screencast any other way. Its two calls are the
  // capture and the restart at a missed deadline.
  const here = dirname(fileURLToPath(import.meta.url));
  const runner = readdirSync(here)
    .filter(
      (name) =>
        /^bench(-.*)?\.ts$/u.test(name) &&
        !name.endsWith(".test.ts") &&
        name !== "bench-browser.ts",
    )
    .map((name) => readFileSync(join(here, name), "utf8"))
    .join("\n");
  assert.equal(runner.includes("Page.startScreencast"), false);
  assert.equal(runner.match(/\bstartScreencast\(/gu)?.length, 2);
});

test("the jetsam exits of a wave are counted from launchd's lines", () => {
  const log = [
    "Timestamp               Ty Process[PID:TID]",
    "2026-01-02 03:04:05.100 Df launchd[1:1a2b3c] [gui/502/com.example.first [4101]:] exited with exit reason (namespace: 1 code: 0x1) - OS_REASON_JETSAM, ran for 60000ms, time-to-relaunch: 1000ms",
    "2026-01-02 03:04:05.105 Df launchd[1:1a2b3d] [gui/502/com.example.second [4102]:] exited with exit reason (namespace: 1 code: 0x1) - OS_REASON_JETSAM, ran for 61000ms, time-to-relaunch: 0ms",
    "2026-01-02 03:04:05.170 Df launchd[1:1a2b3d] [gui/502/com.example.second [4102]:] exited due to exit(0)",
    "",
  ].join("\n");
  assert.deepEqual(parseJetsamKills(log), {
    count: 2,
    first: "2026-01-02 03:04:05.100",
    last: "2026-01-02 03:04:05.105",
  });
  assert.deepEqual(parseJetsamKills(log.split("\n")[0] ?? ""), {
    count: 0,
    first: null,
    last: null,
  });
});

test("a browser lost inside a jetsam wave or short of memory is the host's", () => {
  const wave = { count: 15, first: null, last: null };
  const none = { count: 0, first: null, last: null };
  assert.equal(browserExitCause(wave, 60), "host-memory");
  assert.equal(browserExitCause(none, 14), "host-memory");
  assert.equal(browserExitCause(none, 15), "unknown");
  assert.equal(browserExitCause(null, null), "unknown");
});

test("only errors that say the browser or page went away read as a loss", () => {
  assert.equal(
    isBrowserLoss(
      new Error(
        "browser.stopTracing: Target page, context or browser has been closed",
      ),
    ),
    true,
  );
  assert.equal(
    isBrowserLoss(new Error("page.evaluate: Target crashed ")),
    true,
  );
  assert.equal(isBrowserLoss(new Error("cold sample timed out")), false);
});

test("the unified log's start time is local and whole seconds", () => {
  assert.equal(
    unifiedLogTime(new Date(2026, 0, 2, 3, 4, 5, 833)),
    "2026-01-02 03:04:05",
  );
});
