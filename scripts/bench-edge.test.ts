import assert from "node:assert/strict";
import { createHash, X509Certificate } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { parseBenchArguments } from "./bench-conditions.ts";
import { edgeCaddyfile, planEdge } from "./bench-edge.ts";

const PORTS = {
  page: 41001,
  calque: 41002,
  pageEdge: 45601,
  calqueEdge: 45602,
};

test("the edge offers HTTP/1.1 and HTTP/2 and forwards calque's own Host", () => {
  const caddyfile = edgeCaddyfile(PORTS, "/c/edge.crt", "/c/edge.key");
  assert.match(caddyfile, /^\t\tprotocols h1 h2$/m);
  assert.doesNotMatch(caddyfile, /h3/);
  assert.match(
    caddyfile,
    /^https:\/\/127\.0\.0\.1:45601 \{\n\ttls \/c\/edge\.crt \/c\/edge\.key\n\treverse_proxy 127\.0\.0\.1:41001\n\}$/m,
  );
  assert.match(
    caddyfile,
    /^https:\/\/127\.0\.0\.1:45602 \{\n\ttls \/c\/edge\.crt \/c\/edge\.key\n\treverse_proxy 127\.0\.0\.1:41002 \{\n\t\theader_up Host 127\.0\.0\.1:41002\n\t\}\n\}$/m,
  );
});

test("the edge's certificate names 127.0.0.1 and Chrome trusts its key alone", (t) => {
  const directory = mkdtempSync(join(tmpdir(), "calque-edge-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const plan = planEdge(directory, PORTS);
  const certificate = new X509Certificate(
    readFileSync(join(directory, "edge.crt")),
  );
  assert.equal(certificate.subjectAltName, "IP Address:127.0.0.1");
  const spki = createHash("sha256")
    .update(certificate.publicKey.export({ type: "spki", format: "der" }))
    .digest("base64");
  assert.deepEqual(plan.chromeArgs, [
    `--ignore-certificate-errors-spki-list=${spki}`,
  ]);
  assert.equal(plan.pageOrigin, "https://127.0.0.1:45601");
  assert.equal(plan.calqueBaseUrl, "https://127.0.0.1:45602");
  assert.equal(plan.environment.XDG_DATA_HOME, join(directory, "data"));
  assert.equal(plan.environment.XDG_CONFIG_HOME, join(directory, "config"));
  assert.equal(
    readFileSync(plan.caddyfile, "utf8"),
    edgeCaddyfile(
      PORTS,
      join(directory, "edge.crt"),
      join(directory, "edge.key"),
    ),
  );
});

test("every run has the edge unless --no-edge, given once, leaves it out", () => {
  assert.equal(parseBenchArguments([]).edge, true);
  assert.equal(parseBenchArguments(["--", "--no-edge"]).edge, false);
  assert.throws(
    () => parseBenchArguments(["--no-edge", "--no-edge"]),
    /usage: pnpm bench/,
  );
  assert.throws(
    () => parseBenchArguments(["--edge", "bin/caddy"]),
    /usage: pnpm bench/,
  );
});
