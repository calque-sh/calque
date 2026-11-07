import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  CADDY,
  caddyArchivePath,
  caddyDirectory,
  type CaddyPin,
  fetchCaddy,
  requireCaddy,
} from "./bench-caddy.ts";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function scratch(t: test.TestContext): string {
  const directory = mkdtempSync(join(tmpdir(), "bench-caddy-"));
  t.after(() => {
    rmSync(directory, { recursive: true, force: true });
  });
  return directory;
}

function digest(algorithm: string, bytes: Buffer | string): string {
  return createHash(algorithm).update(bytes).digest("hex");
}

/** A release-shaped archive: `caddy` with `binary`, beside a license. */
function archive(directory: string, binary: string): Buffer {
  const contents = join(directory, "contents");
  mkdirSync(contents, { recursive: true });
  writeFileSync(join(contents, "caddy"), binary);
  writeFileSync(join(contents, "LICENSE"), "license");
  const path = join(directory, "release.tar.gz");
  const result = spawnSync(
    "tar",
    ["-c", "-z", "-f", path, "-C", contents, "caddy", "LICENSE"],
    { stdio: "inherit" },
  );
  assert.equal(result.status, 0);
  return readFileSync(path);
}

async function serve(
  t: test.TestContext,
  body: Buffer,
): Promise<{ url: string; requests: () => number }> {
  let requests = 0;
  const server = createServer((_request, response) => {
    requests += 1;
    response.writeHead(200, { "content-length": String(body.length) });
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
    url: `http://127.0.0.1:${String(port)}/caddy.tar.gz`,
    requests: () => requests,
  };
}

function pinFor(url: string, bytes: Buffer, binary: string): CaddyPin {
  return {
    ...CADDY,
    url,
    archiveBytes: bytes.length,
    archiveSha512: digest("sha512", bytes),
    binarySha256: digest("sha256", binary),
  };
}

test("the pin is the Caddy release the self-host composition runs", () => {
  const compose = readFileSync(
    join(ROOT, "docker-compose.selfhost.yml"),
    "utf8",
  );
  assert.deepEqual(
    [...compose.matchAll(/`caddy:(\d+\.\d+\.\d+)-alpine`/gu)].map(
      (match) => match[1],
    ),
    [CADDY.version],
  );
  assert.equal(
    CADDY.url,
    `https://github.com/caddyserver/caddy/releases/download/v${CADDY.version}/caddy_${CADDY.version}_${CADDY.platform}.tar.gz`,
  );
  assert.match(CADDY.archiveSha512, /^[0-9a-f]{128}$/u);
  assert.match(CADDY.binarySha256, /^[0-9a-f]{64}$/u);
});

test("Caddy is cached per version and platform, under an overridable root", () => {
  assert.equal(
    caddyDirectory(CADDY, { CALQUE_BENCH_CADDY_CACHE: "/cache" }),
    join("/cache", "2.11.4", "mac_arm64"),
  );
  assert.equal(
    caddyArchivePath(join("/cache", "2.11.4", "mac_arm64")),
    join("/cache", "2.11.4", "mac_arm64.tar.gz"),
  );
});

test("a binary is installed once its archive and itself match the pin, and reused after", async (t) => {
  const root = scratch(t);
  const bytes = archive(root, "the binary");
  const served = await serve(t, bytes);
  const directory = join(root, "cache", "2.11.4", "mac_arm64");
  const pin = pinFor(served.url, bytes, "the binary");
  const lines: string[] = [];

  const installed = await fetchCaddy({
    pin,
    directory,
    log: (line) => lines.push(line),
  });
  assert.deepEqual(installed, {
    executablePath: join(directory, "caddy"),
    version: "2.11.4",
    binarySha256: pin.binarySha256,
  });
  assert.equal(statSync(installed.executablePath).mode & 0o777, 0o755);
  assert.equal(existsSync(`${directory}.staging`), false);
  assert.deepEqual(lines, [
    `downloading ${served.url}`,
    `installed: ${join(directory, "caddy")}`,
  ]);

  const again: string[] = [];
  assert.deepEqual(
    await fetchCaddy({ pin, directory, log: (line) => again.push(line) }),
    installed,
  );
  assert.deepEqual(again, []);
  assert.equal(served.requests(), 1);

  // A changed binary is replaced from the cached archive, verified again.
  writeFileSync(installed.executablePath, "tampered");
  const repair: string[] = [];
  await fetchCaddy({ pin, directory, log: (line) => repair.push(line) });
  assert.match(
    repair[0] ?? "",
    /^replacing the cached Caddy: .* has SHA-256 /u,
  );
  assert.equal(readFileSync(installed.executablePath, "utf8"), "the binary");
  assert.equal(served.requests(), 1);
});

test("an archive that fails its SHA-512 pin is never unpacked, and is dropped", async (t) => {
  const root = scratch(t);
  const bytes = archive(root, "the binary");
  const served = await serve(t, bytes);
  const directory = join(root, "cache", "2.11.4", "mac_arm64");
  const pin = {
    ...pinFor(served.url, bytes, "the binary"),
    archiveSha512: digest("sha512", "other bytes"),
  };
  await assert.rejects(
    fetchCaddy({ pin, directory, log: () => undefined }),
    /with SHA-512 [0-9a-f]{128}; the pin is \d+ bytes with /u,
  );
  assert.equal(existsSync(caddyArchivePath(directory)), false);
  assert.equal(existsSync(`${directory}.staging`), false);
  assert.equal(existsSync(directory), false);
});

test("a binary that fails its SHA-256 pin is not installed", async (t) => {
  const root = scratch(t);
  const bytes = archive(root, "another binary");
  const served = await serve(t, bytes);
  const directory = join(root, "cache", "2.11.4", "mac_arm64");
  const pin = pinFor(served.url, bytes, "the binary");
  await assert.rejects(fetchCaddy({ pin, directory, log: () => undefined }), {
    message: `the extracted binary's SHA-256 is ${digest("sha256", "another binary")}; the pin is ${pin.binarySha256}`,
  });
  assert.equal(existsSync(`${directory}.staging`), false);
  assert.equal(existsSync(directory), false);
});

test("a run is refused a binary that is missing or not the pin's", (t) => {
  const directory = scratch(t);
  assert.throws(() => requireCaddy(CADDY, directory), {
    message: `Caddy 2.11.4 is not at ${join(directory, "caddy")}`,
  });
  writeFileSync(join(directory, "caddy"), "not caddy");
  assert.throws(() => requireCaddy(CADDY, directory), {
    message: `${join(directory, "caddy")} has SHA-256 ${digest("sha256", "not caddy")}; the pin is ${CADDY.binarySha256}`,
  });
});
