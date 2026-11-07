/// <reference lib="dom" />
// The callbacks below are serialised and evaluated in a browser, where
// `Worker` and `WorkerOptions` are in scope.
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { readFileSync, statSync } from "node:fs";
import { dirname, extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import { hasherCount } from "../client/packages/worker/src/columnar-hashers.ts";
import {
  consumerEvidenceFault,
  type ConsumerResult,
  type WorkerTrace,
} from "./lib/browser-consumer-evidence.ts";
import {
  ARTIFACT_ENCODINGS,
  buildColumnarConsumerFixture,
  type ArtifactEncoding,
  type ColumnarConsumerFixture,
} from "./lib/columnar-consumer-fixture.ts";

declare global {
  // `var` because `let`/`const` do not reach `globalThis`. These two are the
  // contract between this gate and
  // `client/packages/client/tests/browser-consumer`.
  var __calqueWorkerTrace: WorkerTrace | undefined;
  var __calqueConsumerResult: ConsumerResult | undefined;
}

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const clientRoot = join(root, "client/packages/client");
const output = join(root, "target/client-browser-consumer");
const requireFromClient = createRequire(join(clientRoot, "package.json"));
// The client package's own Playwright, so this gate runs the browser the
// client tests were written against.
const { chromium } = requireFromClient(
  "playwright",
) as typeof import("playwright");
const pageErrors: string[] = [];
/**
 * The direct-columnar bootstrap the page activates against, minted once the
 * server listens, because the plan names absolute URLs and the port is only
 * known after `listen`.
 */
let fixture: ColumnarConsumerFixture;
/** The representation the next bootstrap answer selects. */
let selectedEncoding: ArtifactEncoding = "identity";
/** Held change long-polls, released when the gate tears the server down. */
const heldChangePolls = new Set<import("node:http").ServerResponse>();
const server = createServer((request, response) => {
  try {
    const url = new URL(request.url ?? "/", "http://localhost");
    const pathname = url.pathname;
    if (pathname === "/v1/bootstrap") {
      const body = JSON.stringify(fixture.planFor(selectedEncoding));
      response.writeHead(200, {
        "content-type": "application/json",
        "content-length": String(Buffer.byteLength(body)),
        "cache-control": "no-store",
      });
      response.end(body);
      return;
    }
    if (pathname === "/__calque/descriptor.json") {
      const body = JSON.stringify(fixture.descriptor);
      response.writeHead(200, {
        "content-type": "application/json",
        "content-length": String(Buffer.byteLength(body)),
        "cache-control": "no-store",
      });
      response.end(body);
      return;
    }
    const artifactMatch = /^\/__calque\/artifact\/(identity|zstd)$/.exec(
      pathname,
    );
    if (artifactMatch) {
      const encoding = artifactMatch[1] as ArtifactEncoding;
      const bytes = fixture.bodies[encoding];
      response.writeHead(200, {
        "content-type": "application/vnd.calque.direct-columnar.v4",
        "content-length": String(bytes.byteLength),
        "cache-control": "no-store",
        // Chromium decodes zstd natively. The transport requires identity to
        // carry no header at all.
        ...(encoding === "identity" ? {} : { "content-encoding": encoding }),
      });
      response.end(bytes);
      return;
    }
    if (pathname === "/__calque/change") {
      // A long poll with nothing to report, never answered: the client aborts
      // it on reset and on close, and the rest are released when the server
      // closes.
      heldChangePolls.add(response);
      request.on("close", () => heldChangePolls.delete(response));
      return;
    }
    const relative = pathname === "/" ? "index.html" : pathname.slice(1);
    const path = normalize(join(output, relative));
    if (path !== output && !path.startsWith(output + sep)) {
      response.writeHead(403).end();
      return;
    }
    const stats = statSync(path);
    if (!stats.isFile()) throw new Error("not a file");
    response.writeHead(200, {
      "content-type": contentType(extname(path)),
      "cache-control": "no-store",
    });
    response.end(readFileSync(path));
  } catch {
    response.writeHead(404).end();
  }
});

await new Promise<void>((resolveListen, rejectListen) => {
  server.once("error", rejectListen);
  server.listen(0, "127.0.0.1", resolveListen);
});
const address = server.address();
if (!address || typeof address === "string") {
  throw new Error("browser consumer server has no TCP address");
}
fixture = await buildColumnarConsumerFixture(
  `http://127.0.0.1:${String(address.port)}`,
);
let browser: import("playwright").Browser | undefined;
/** The hashers one activation spawned in this browser. */
let hashersPerLoad = 0;
try {
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  page.on("pageerror", (error: Error) => pageErrors.push(String(error)));
  await page.addInitScript(() => {
    const NativeWorker = globalThis.Worker;
    const trace: WorkerTrace = { created: [], terminated: 0 };
    Object.defineProperty(globalThis, "__calqueWorkerTrace", {
      configurable: true,
      value: trace,
    });
    globalThis.Worker = class TracedApplicationWorker extends NativeWorker {
      constructor(url: string | URL, options?: WorkerOptions) {
        super(url, options);
        trace.created.push({
          name: options?.name ?? null,
          type: options?.type ?? null,
          url: String(url),
        });
      }

      terminate() {
        trace.terminated += 1;
        return super.terminate();
      }
    };
  });
  // The production client activates a real artifact once per encoding the
  // transport profile advertises.
  for (const encoding of ARTIFACT_ENCODINGS) {
    selectedEncoding = encoding;
    await page.goto(`http://127.0.0.1:${address.port}/?encoding=${encoding}`, {
      waitUntil: "load",
    });
    await page.waitForFunction(
      () => Object.hasOwn(globalThis, "__calqueConsumerResult"),
      undefined,
      { timeout: 60_000 },
    );
    const evidence = await page.evaluate(() => ({
      result: globalThis.__calqueConsumerResult,
      trace: globalThis.__calqueWorkerTrace,
      threads: globalThis.navigator.hardwareConcurrency,
    }));
    const fault = consumerEvidenceFault(encoding, evidence, pageErrors);
    if (fault !== undefined) throw new Error(fault);
    hashersPerLoad = hasherCount(evidence.threads);
  }
  process.stdout.write(
    [
      "Client browser consumer lifecycle: PASS (create -> ready -> reset -> ready -> close, x2)",
      "Client browser consumer native artifact decode: PASS (identity, zstd)",
      "Client browser consumer collection-less schema refusal: PASS (createSyncClient throws a TypeError)",
      `Client browser consumer worker ownership: PASS (2 generations, 2 owned engine workers and ${String(2 * hashersPerLoad)} hashers terminated, x2)`,
      `Client browser consumer verification: PASS (2 representations; create -> ready -> reset -> ready -> close; ${String(2 + 2 * hashersPerLoad)} owned workers terminated each)`,
      "",
    ].join("\n"),
  );
} finally {
  await browser?.close();
  for (const held of heldChangePolls) held.destroy();
  heldChangePolls.clear();
  await new Promise<void>((resolveClose, rejectClose) =>
    server.close((error) => (error ? rejectClose(error) : resolveClose())),
  );
}

function contentType(extension: string): string {
  switch (extension) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".js":
      return "text/javascript; charset=utf-8";
    default:
      return "application/octet-stream";
  }
}
