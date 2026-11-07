import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { BROTLI_PUBLISHED_LOG, holdFailure, holdText } from "./bench-calque.ts";
import { parseBenchArguments } from "./bench-conditions.ts";
import { redactedUrl } from "./bench-record.ts";
import {
  BROTLI_ENCODING,
  brotliSample,
  type BrotliServed,
  brotliSummary,
  calqueServed,
  decoderFiles,
  type DecoderRequest,
  DecoderRequests,
  PACKED_ENCODING,
  type PlannedArtifact,
  plannedArtifact,
  plannedObject,
} from "./bench-brotli.ts";
import { repositoryRoot } from "./lib/serve-e2e.ts";

function sha256(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

const HOLD = ["--hold-until-logged", "re-encoded artifact published"];

// The hold -----------------------------------------------------------------------

test("a run holds for calque's brotli object, packed or plain, unless it names another line", () => {
  assert.equal(BROTLI_PUBLISHED_LOG, "brotli-large-window artifact published");
  // calque logs one of two lines for a cut, and the text ends both.
  const engine = readFileSync(
    join(repositoryRoot, "server/crates/calque-engine/src/reencode.rs"),
    "utf8",
  );
  for (const line of [
    "packed-brotli-large-window artifact published",
    "brotli-large-window artifact published",
  ]) {
    assert.ok(engine.includes(`"${line}"`), line);
    assert.ok(line.endsWith(BROTLI_PUBLISHED_LOG), line);
  }
  assert.equal(holdText(parseBenchArguments([])), BROTLI_PUBLISHED_LOG);
  assert.equal(
    holdText(parseBenchArguments(["--smoke"])),
    BROTLI_PUBLISHED_LOG,
  );
  assert.equal(
    holdText(parseBenchArguments(HOLD)),
    "re-encoded artifact published",
  );
  // Its probes follow a page that loaded the artifact, as a full run's do.
  assert.equal(
    holdText(parseBenchArguments(["--realtime-only"])),
    BROTLI_PUBLISHED_LOG,
  );
  // The option itself stays unset, so a record says what the run was given.
  assert.equal(parseBenchArguments([]).holdUntilLogged, null);

  // A calque that never publishes the object stops the run that waited for it.
  const unlogged = {
    text: BROTLI_PUBLISHED_LOG,
    heldMs: 300_040,
    logged: false,
  };
  assert.match(
    holdFailure(parseBenchArguments([]), unlogged) ?? "",
    /^calque did not log "brotli-large-window artifact published" in 300 s, .* --hold-until-logged names another line to wait for$/u,
  );
  assert.equal(
    holdFailure(parseBenchArguments([]), { ...unlogged, logged: true }),
    null,
  );
  // A hold the run was given is recorded as it ended.
  assert.equal(holdFailure(parseBenchArguments(HOLD), unlogged), null);
});

// The plan -----------------------------------------------------------------------

/** The specification's example plan: one artifact, its zstd representation selected. */
const EXAMPLE_PLAN = readFileSync(
  join(repositoryRoot, "spec/bootstrap-plan.direct-columnar.example.json"),
  "utf8",
);

type Json = Record<string, unknown>;

function examplePlan(change: (artifact: Json, plan: Json) => void = () => {}) {
  const plan = JSON.parse(EXAMPLE_PLAN) as Json;
  const [artifact] = plan.artifacts as Json[];
  if (artifact === undefined) throw new Error("the example has no artifact");
  change(artifact, plan);
  return JSON.stringify(plan);
}

test("a plan's one artifact is read where it selects a compressed representation", () => {
  assert.deepEqual(plannedArtifact(EXAMPLE_PLAN), {
    artifactId: "artifact_01JZCOLUMNAR",
    decodedBytes: 24_746_441,
    decodedSha256:
      "4c10a234567890abcdef1234567890abcdef1234567890abcdef1234567890ab",
    representationId: `sha256:${"a".repeat(64)}`,
    encoding: "zstd",
    encodedBytes: 6_729_101,
    downloadUrl: "https://delivery.example.test/signed/columnar",
  });
  // calque's brotli objects, plain and packed, are read as its zstd one is.
  for (const encoding of [
    "brotli-large-window",
    "packed-brotli-large-window",
  ] as const)
    assert.deepEqual(
      plannedArtifact(
        examplePlan((artifact) => {
          (artifact.selectedRepresentation as Json).encoding = encoding;
        }),
      ),
      { ...plannedArtifact(EXAMPLE_PLAN), encoding },
    );
  assert.throws(
    () =>
      plannedArtifact(examplePlan((_artifact, plan) => (plan.artifacts = []))),
    /the plan names 0 artifacts, and the run expects exactly one/u,
  );
  assert.throws(
    () =>
      plannedArtifact(
        examplePlan(
          (artifact, plan) => (plan.artifacts = [artifact, artifact]),
        ),
      ),
    /the plan names 2 artifacts/u,
  );
  assert.throws(
    () => plannedArtifact(JSON.stringify({ planId: "p" })),
    /the plan names no artifacts/u,
  );
  assert.throws(
    () =>
      plannedArtifact(
        examplePlan((artifact) => {
          (artifact.selectedRepresentation as Json).encoding = "identity";
        }),
      ),
    /the plan selects the artifact's identity representation, not its zstd, brotli-large-window or packed-brotli-large-window one/u,
  );
  assert.throws(
    () =>
      plannedArtifact(
        examplePlan((artifact) => {
          delete artifact.decodedSha256;
        }),
      ),
    /the plan's artifact is not well formed/u,
  );
});

// The decoder's files ------------------------------------------------------------

test("the decoder's files are the build's copy of the client's module and the one script that names it", (t) => {
  const root = mkdtempSync(join(tmpdir(), "brotli-build-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const assets = join(root, "assets");
  mkdirSync(assets);
  const module = Buffer.from("\0asm decoder module");
  const worker =
    'fetch(new URL("brotli-decoder-Dd1.wasm", import.meta.url));\n';
  writeFileSync(join(assets, "brotli-decoder-Dd1.wasm"), module);
  writeFileSync(join(assets, "columnar-browser-decoder-entry-B1.js"), worker);
  writeFileSync(join(assets, "boot-1.js"), "new Worker('decoder-entry');\n");
  writeFileSync(join(assets, "main-1.css"), "body{margin:0}\n");
  assert.deepEqual(decoderFiles(root, module), {
    module: {
      path: "/assets/brotli-decoder-Dd1.wasm",
      bytes: module.byteLength,
      sha256: sha256(module),
    },
    worker: {
      path: "/assets/columnar-browser-decoder-entry-B1.js",
      bytes: Buffer.byteLength(worker),
      sha256: sha256(Buffer.from(worker)),
    },
  });
  // A build of another client carries another module, or none.
  assert.throws(
    () => decoderFiles(root, Buffer.from("\0asm another module")),
    /the served build holds 0 copies of the client's decoder module, not one/u,
  );
  writeFileSync(
    join(assets, "boot-1.js"),
    'import("brotli-decoder-Dd1.wasm");',
  );
  assert.throws(
    () => decoderFiles(root, module),
    /2 scripts of the served build name brotli-decoder-Dd1\.wasm, not the decoder worker's alone/u,
  );
  rmSync(join(assets, "boot-1.js"));
  rmSync(join(assets, "columnar-browser-decoder-entry-B1.js"));
  assert.throws(
    () => decoderFiles(root, module),
    /0 scripts of the served build name brotli-decoder-Dd1\.wasm/u,
  );
});

// A sample -----------------------------------------------------------------------

const SERVED: BrotliServed = {
  objectUrl:
    "https://127.0.0.1:45602/v1/artifact-objects/sha256:ab12?<redacted>",
  objectBytes: 7_642_956,
  artifactRoute: "https://127.0.0.1:45602/v1/artifact-objects/",
  moduleUrl: "https://127.0.0.1:45601/assets/brotli-decoder-Dd1.wasm",
  workerUrl:
    "https://127.0.0.1:45601/assets/columnar-browser-decoder-entry-B1.js",
};

/** A request as the ledger records it, `T_nav` at 1,000 ms on its clock. */
function ledgerRequest(
  url: string,
  response: {
    readonly contentEncoding?: string | null;
    readonly contentLength?: number | null;
    readonly state?: string;
  } = {},
) {
  return {
    url,
    contentEncoding:
      response.contentEncoding === undefined ? "br" : response.contentEncoding,
    contentLength:
      response.contentLength === undefined ? 100 : response.contentLength,
    state: response.state ?? "finished",
    sentMs: 1_040,
    respondedMs: 1_076,
    lastDataMs: 1_690,
    endedMs: 1_691,
  };
}

const OBJECT_REQUEST = ledgerRequest(SERVED.objectUrl, {
  contentEncoding: null,
  contentLength: 7_642_956,
  // Chrome reports a body read to its end through a stream as canceled.
  state: "failed",
});

const DECODER: DecoderRequest[] = [
  {
    url: SERVED.workerUrl,
    status: 200,
    contentEncoding: "br",
    contentLength: 1_252,
    transferBytes: 1_301,
    failure: null,
    requestMs: 91,
    responseStartMs: 95,
    responseEndMs: 96,
  },
  {
    url: SERVED.moduleUrl,
    status: 200,
    contentEncoding: "br",
    contentLength: 44_476,
    transferBytes: 44_530,
    failure: null,
    requestMs: 98,
    responseStartMs: 101,
    responseEndMs: 104,
  },
];

test("a sample is compared with calque's own brotli object, packed or plain, and with no zstd one", () => {
  const route = "https://127.0.0.1:45602/v1/artifact-objects/";
  const artifact: PlannedArtifact = {
    ...plannedArtifact(EXAMPLE_PLAN),
    encoding: BROTLI_ENCODING,
    encodedBytes: 7_642_956,
    downloadUrl: `${route}sha256:abc?cap=secret`,
  };
  const files = { moduleUrl: SERVED.moduleUrl, workerUrl: SERVED.workerUrl };
  const served = calqueServed(artifact, redactedUrl, files);
  assert.deepEqual(served, {
    objectUrl: redactedUrl(artifact.downloadUrl),
    objectBytes: 7_642_956,
    artifactRoute: route,
    ...files,
  });
  assert.ok(served);
  // The packed object is asked for and answered as the plain one is.
  assert.deepEqual(
    calqueServed(
      { ...artifact, encoding: PACKED_ENCODING },
      redactedUrl,
      files,
    ),
    served,
  );
  assert.equal(
    calqueServed({ ...artifact, encoding: "zstd" }, redactedUrl, files),
    null,
  );
  // The artifact witness reads a ledger for calque's zstd object as well.
  assert.deepEqual(
    plannedObject({ ...artifact, encoding: "zstd" }, redactedUrl),
    {
      objectUrl: redactedUrl(artifact.downloadUrl),
      objectBytes: 7_642_956,
      artifactRoute: route,
    },
  );
  const asked = (url: string, contentLength = 7_642_956) =>
    ledgerRequest(redactedUrl(url), {
      contentEncoding: null,
      contentLength,
      state: "failed",
    });
  const object = (requests: ReturnType<typeof ledgerRequest>[]) =>
    brotliSample(served, { requests, decoder: DECODER, navigationMs: 1_000 })
      .object;
  // Each plan carries its own capability; the object is the same one.
  assert.equal(object([asked(`${route}sha256:abc?cap=another`)]), true);
  // The level-19 object instead of it, or beside it, is not engagement.
  assert.equal(object([asked(`${route}sha256:def?cap=x`, 7_931_582)]), false);
  assert.equal(
    object([
      asked(`${route}sha256:abc?cap=x`),
      asked(`${route}sha256:def?cap=x`),
    ]),
    false,
  );
  assert.equal(object([]), false);
});

test("a sample received calque's brotli object when its only artifact request was the object's, uncoded, at its length", () => {
  const whole = brotliSample(SERVED, {
    requests: [
      ledgerRequest("https://127.0.0.1:45601/"),
      OBJECT_REQUEST,
      ledgerRequest("https://127.0.0.1:45602/v1/changes", {
        contentEncoding: null,
        contentLength: null,
      }),
    ],
    decoder: DECODER,
    navigationMs: 1_000,
  });
  assert.deepEqual(whole, {
    object: true,
    artifacts: [
      {
        url: SERVED.objectUrl,
        contentEncoding: null,
        contentLength: 7_642_956,
        state: "failed",
        // From T_nav: the request, its headers, its last bytes, its end.
        requestMs: 40,
        firstByteMs: 76,
        lastByteMs: 690,
        endedMs: 691,
      },
    ],
    module: [DECODER[1]],
    worker: [DECODER[0]],
  });

  const object = (requests: Parameters<typeof brotliSample>[1]["requests"]) =>
    brotliSample(SERVED, { requests, decoder: [], navigationMs: 1_000 });
  // calque's zstd object in the brotli one's place.
  const zstd = ledgerRequest(`${SERVED.artifactRoute}sha256:cd41?<redacted>`, {
    contentEncoding: "zstd",
    contentLength: 7_931_465,
  });
  assert.equal(object([zstd]).object, false);
  assert.equal(object([zstd]).artifacts[0]?.contentEncoding, "zstd");
  // The object beside the zstd one, after a plan asked for again.
  assert.equal(object([OBJECT_REQUEST, zstd]).object, false);
  assert.equal(object([OBJECT_REQUEST, zstd]).artifacts.length, 2);
  assert.equal(
    object([{ ...OBJECT_REQUEST, contentEncoding: "br" }]).object,
    false,
  );
  assert.equal(
    object([{ ...OBJECT_REQUEST, contentLength: 7_642_955 }]).object,
    false,
  );
  assert.equal(
    object([{ ...OBJECT_REQUEST, contentLength: null }]).object,
    false,
  );
  assert.equal(object([]).object, false);
  // The object asked for twice, after the network cut it, is still the object.
  assert.equal(object([OBJECT_REQUEST, OBJECT_REQUEST]).object, true);
  // A sample with no T_nav keeps what it received and no time.
  const untimed = brotliSample(SERVED, {
    requests: [OBJECT_REQUEST],
    decoder: [],
    navigationMs: null,
  });
  assert.equal(untimed.object, true);
  assert.equal(untimed.artifacts[0]?.requestMs, null);
  assert.equal(untimed.artifacts[0]?.endedMs, null);
});

/** A Playwright request, as far as `DecoderRequests` reads one. */
function playwrightRequest(
  url: string,
  options: {
    readonly status?: number;
    readonly headers?: Record<string, string>;
    readonly sizes?: { responseHeadersSize: number; responseBodySize: number };
    readonly failure?: string;
    readonly timing?: {
      startTime: number;
      responseStart: number;
      responseEnd: number;
    };
  } = {},
) {
  return {
    url: () => url,
    response: () =>
      Promise.resolve(
        options.status === undefined
          ? null
          : {
              status: () => options.status,
              headers: () => options.headers ?? {},
            },
      ),
    sizes: () =>
      options.sizes === undefined
        ? Promise.reject(new Error("no sizes"))
        : Promise.resolve(options.sizes),
    failure: () =>
      options.failure === undefined ? null : { errorText: options.failure },
    timing: () =>
      options.timing ?? { startTime: 0, responseStart: -1, responseEnd: -1 },
  };
}

test("the decoder's requests are read from the page's and its workers' ends, with their wire bytes and their times from T_nav", async () => {
  const listeners = new Map<string, (request: unknown) => void>();
  const page = {
    on: (event: string, listener: (request: unknown) => void) => {
      listeners.set(event, listener);
      return page;
    },
  };
  const requests = new DecoderRequests(
    page as unknown as ConstructorParameters<typeof DecoderRequests>[0],
    [SERVED.moduleUrl, SERVED.workerUrl],
  );
  assert.deepEqual([...listeners.keys()].sort(), [
    "requestfailed",
    "requestfinished",
  ]);
  assert.deepEqual(await requests.read(5_000), []);

  const finished = listeners.get("requestfinished");
  const failed = listeners.get("requestfailed");
  assert.ok(finished && failed);
  // Another file of the page's is not the decoder's.
  finished(playwrightRequest("https://127.0.0.1:45601/assets/main-1.js"));
  finished(
    playwrightRequest(SERVED.workerUrl, {
      status: 200,
      headers: { "content-encoding": "br", "content-length": "1252" },
      sizes: { responseHeadersSize: 49, responseBodySize: 1_252 },
      timing: { startTime: 5_091, responseStart: 4.5, responseEnd: 5.25 },
    }),
  );
  finished(
    playwrightRequest(SERVED.moduleUrl, {
      status: 200,
      headers: { "content-encoding": "br", "content-length": " 44476 " },
      sizes: { responseHeadersSize: 54, responseBodySize: 44_476 },
      timing: { startTime: 5_098, responseStart: 3, responseEnd: 6 },
    }),
  );
  // A request that failed has no response, no sizes and no later time.
  failed(
    playwrightRequest(SERVED.moduleUrl, {
      failure: "net::ERR_ABORTED",
      timing: { startTime: 5_200, responseStart: -1, responseEnd: -1 },
    }),
  );
  // A response whose sizes Chrome did not give keeps what it has.
  finished(
    playwrightRequest(SERVED.moduleUrl, {
      status: 404,
      timing: { startTime: 5_300, responseStart: 2, responseEnd: -1 },
    }),
  );
  assert.deepEqual(await requests.read(5_000), [
    {
      url: SERVED.workerUrl,
      status: 200,
      contentEncoding: "br",
      contentLength: 1_252,
      transferBytes: 1_301,
      failure: null,
      requestMs: 91,
      responseStartMs: 95.5,
      responseEndMs: 96.25,
    },
    {
      url: SERVED.moduleUrl,
      status: 200,
      contentEncoding: "br",
      contentLength: 44_476,
      transferBytes: 44_530,
      failure: null,
      requestMs: 98,
      responseStartMs: 101,
      responseEndMs: 104,
    },
    {
      url: SERVED.moduleUrl,
      status: null,
      contentEncoding: null,
      contentLength: null,
      transferBytes: null,
      failure: "net::ERR_ABORTED",
      requestMs: 200,
      responseStartMs: null,
      responseEndMs: null,
    },
    {
      url: SERVED.moduleUrl,
      status: 404,
      contentEncoding: null,
      contentLength: null,
      transferBytes: null,
      failure: null,
      requestMs: 300,
      responseStartMs: 302,
      responseEndMs: null,
    },
  ]);
});

test("a run's result lists the cold samples that received calque's brotli object and the decoder module", () => {
  const received = brotliSample(SERVED, {
    requests: [OBJECT_REQUEST],
    decoder: DECODER,
    navigationMs: 1_000,
  });
  assert.deepEqual(
    brotliSummary([
      { name: "cold-0", brotli: received },
      // calque's zstd object in the brotli one's place.
      {
        name: "cold-1",
        brotli: brotliSample(SERVED, {
          requests: [
            ledgerRequest(`${SERVED.artifactRoute}sha256:cd41?<redacted>`, {
              contentEncoding: "zstd",
            }),
          ],
          decoder: [],
          navigationMs: 1_000,
        }),
      },
      // The object, and a module that did not answer.
      {
        name: "cold-2",
        brotli: {
          ...received,
          module: [{ ...received.module[0], status: 404 } as DecoderRequest],
        },
      },
      { name: "cold-3", brotli: { ...received, module: [] } },
      // A sample with no reading at all.
      { name: "cold-4" },
      { name: "cold-5", brotli: received },
    ]),
    {
      received: ["cold-0", "cold-5"],
      notReceived: [
        {
          sample: "cold-1",
          lacks: [
            "the published object as its only artifact",
            "the decoder module",
          ],
        },
        { sample: "cold-2", lacks: ["the decoder module"] },
        { sample: "cold-3", lacks: ["the decoder module"] },
        {
          sample: "cold-4",
          lacks: [
            "the published object as its only artifact",
            "the decoder module",
          ],
        },
      ],
    },
  );
  assert.deepEqual(brotliSummary([]), { received: [], notReceived: [] });
});
