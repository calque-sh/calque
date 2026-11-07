/**
 * The artifact object calque plans for a run's pages, and, where that is
 * calque's brotli object, packed or plain, the client's decoder files the
 * page fetches to decode it and what each cold sample received of both.
 */
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { Page, Request } from "playwright";

/** The encoding of calque's plain brotli object. */
export const BROTLI_ENCODING = "brotli-large-window";

/**
 * The encoding of calque's packed object: the same encoding of the
 * artifact's packed body, which the client's decoder worker unpacks to the
 * artifact.
 */
export const PACKED_ENCODING = "packed-brotli-large-window";

/** The `Accept-Encoding` Chrome sends over TLS, which the run's own plan request sends. */
export const BROWSER_ACCEPT_ENCODING = "gzip, deflate, br, zstd";

function sha256(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** The one artifact of a plan and the compressed representation it selects. */
export interface PlannedArtifact {
  readonly artifactId: string;
  readonly decodedBytes: number;
  readonly decodedSha256: string;
  readonly representationId: string;
  readonly encoding: "zstd" | typeof BROTLI_ENCODING | typeof PACKED_ENCODING;
  readonly encodedBytes: number;
  readonly downloadUrl: string;
}

/**
 * The artifact of the plan in `planText`, which must name exactly one and
 * select one of its compressed representations: zstd, brotli-large-window
 * or packed-brotli-large-window. A run expects one object.
 */
export function plannedArtifact(planText: string): PlannedArtifact {
  const plan = JSON.parse(planText) as { artifacts?: unknown } | null;
  const artifacts = plan?.artifacts;
  if (!Array.isArray(artifacts) || artifacts.length !== 1)
    throw new Error(
      `the plan names ${Array.isArray(artifacts) ? String(artifacts.length) : "no"} artifacts, and the run expects exactly one`,
    );
  const artifact = artifacts[0] as Partial<Record<string, unknown>> | null;
  const representation = artifact?.selectedRepresentation as
    Partial<Record<string, unknown>> | null | undefined;
  const { artifactId, decodedBytes, decodedSha256 } = artifact ?? {};
  const { representationId, encoding, encodedBytes, downloadUrl } =
    representation ?? {};
  if (
    encoding !== "zstd" &&
    encoding !== BROTLI_ENCODING &&
    encoding !== PACKED_ENCODING
  )
    throw new Error(
      `the plan selects the artifact's ${String(encoding)} representation, not its zstd, brotli-large-window or packed-brotli-large-window one`,
    );
  if (
    typeof artifactId !== "string" ||
    typeof decodedBytes !== "number" ||
    typeof decodedSha256 !== "string" ||
    typeof representationId !== "string" ||
    typeof encodedBytes !== "number" ||
    typeof downloadUrl !== "string"
  )
    throw new Error("the plan's artifact is not well formed");
  return {
    artifactId,
    decodedBytes,
    decodedSha256,
    representationId,
    encoding,
    encodedBytes,
    downloadUrl,
  };
}

// The decoder's files and a sample's record ------------------------------------------

/** One file of the served build, as the summary names it. */
interface BuildFile {
  /** The file as the page requests it: `/assets/<name>`. */
  readonly path: string;
  readonly bytes: number;
  readonly sha256: string;
}

/** The client's decoder as the served build carries it. */
export interface DecoderFiles {
  /** The Wasm module the decoder worker fetches. */
  readonly module: BuildFile;
  /** The decoder worker's script, which names the module. */
  readonly worker: BuildFile;
}

/**
 * The decoder's two files in the build at `pageRoot`: the asset whose bytes
 * are `module`, the client's committed decoder module, and the one script
 * whose text names that asset, which is the worker's. A build without either,
 * or with several of either, is refused: the page it serves is not the
 * client whose decoder the run reads.
 */
export function decoderFiles(pageRoot: string, module: Buffer): DecoderFiles {
  const assets = join(pageRoot, "assets");
  const files = readdirSync(assets).map((name) => ({
    name,
    body: readFileSync(join(assets, name)),
  }));
  const file = ({ name, body }: (typeof files)[number]): BuildFile => ({
    path: `/assets/${name}`,
    bytes: body.byteLength,
    sha256: sha256(body),
  });
  const modules = files.filter(({ body }) => body.equals(module));
  const [found] = modules;
  if (modules.length !== 1 || found === undefined)
    throw new Error(
      `the served build holds ${String(modules.length)} copies of the client's decoder module, not one`,
    );
  const workers = files.filter(
    ({ name, body }) => name.endsWith(".js") && body.includes(found.name),
  );
  const [worker] = workers;
  if (workers.length !== 1 || worker === undefined)
    throw new Error(
      `${String(workers.length)} scripts of the served build name ${found.name}, not the decoder worker's alone`,
    );
  return { module: file(found), worker: file(worker) };
}

/**
 * One request for a decoder file, as Playwright reports it. The page target's
 * Network domain, which the ledger reads, sees a dedicated worker's script as
 * a request with no response and the worker's own requests not at all;
 * Playwright attaches to the worker's target as well. `transferBytes` is
 * `Network.loadingFinished`'s `encodedDataLength`: the whole response for the
 * module, but not always for the worker's own script, so `contentLength` gives
 * the body's length on the wire. The times are milliseconds from `T_nav`:
 * `requestMs` is when Chrome started the request, and the others add
 * Playwright's `responseStart` and `responseEnd` to it.
 */
export interface DecoderRequest {
  readonly url: string;
  readonly status: number | null;
  readonly contentEncoding: string | null;
  readonly contentLength: number | null;
  readonly transferBytes: number | null;
  readonly failure: string | null;
  readonly requestMs: number;
  readonly responseStartMs: number | null;
  readonly responseEndMs: number | null;
}

/** A decoder request before its times are taken from `T_nav`. */
interface TimedDecoderRequest extends Omit<
  DecoderRequest,
  "requestMs" | "responseStartMs" | "responseEndMs"
> {
  /** Milliseconds since the epoch. */
  readonly startedEpochMs: number;
  /** From the start, or negative where Playwright has none. */
  readonly responseStartMs: number;
  readonly responseEndMs: number;
}

/** How long a decoder request's sizes are waited for before it is recorded without them. */
const SIZES_TIMEOUT_MS = 2_000;

/**
 * The requests a page and its workers make for the decoder's files, from the
 * page's `requestfinished` and `requestfailed` events.
 */
export class DecoderRequests {
  readonly #reading: Promise<TimedDecoderRequest>[] = [];

  constructor(page: Pick<Page, "on">, urls: readonly string[]) {
    const take = (request: Request) => {
      if (urls.includes(request.url()))
        this.#reading.push(readDecoderRequest(request));
    };
    page.on("requestfinished", take);
    page.on("requestfailed", take);
  }

  /** Every request read so far, in the order they ended, from `timeOriginMs`. */
  async read(timeOriginMs: number): Promise<DecoderRequest[]> {
    return (await Promise.all(this.#reading)).map(
      ({ startedEpochMs, responseStartMs, responseEndMs, ...request }) => {
        const requestMs = startedEpochMs - timeOriginMs;
        return {
          ...request,
          requestMs,
          responseStartMs:
            responseStartMs < 0 ? null : requestMs + responseStartMs,
          responseEndMs: responseEndMs < 0 ? null : requestMs + responseEndMs,
        };
      },
    );
  }
}

async function readDecoderRequest(
  request: Request,
): Promise<TimedDecoderRequest> {
  const response = await request.response().catch(() => null);
  const sizes =
    response === null
      ? null
      : await Promise.race([
          request.sizes().catch(() => null),
          new Promise<null>((done) => {
            setTimeout(() => done(null), SIZES_TIMEOUT_MS).unref();
          }),
        ]);
  const headers = response?.headers() ?? {};
  const length = headers["content-length"];
  const timing = request.timing();
  return {
    url: request.url(),
    status: response?.status() ?? null,
    contentEncoding: headers["content-encoding"] ?? null,
    contentLength:
      length !== undefined && /^[0-9]+$/u.test(length.trim())
        ? Number(length.trim())
        : null,
    transferBytes:
      sizes === null
        ? null
        : sizes.responseHeadersSize + sizes.responseBodySize,
    failure: request.failure()?.errorText ?? null,
    startedEpochMs: timing.startTime,
    responseStartMs: timing.responseStart,
    responseEndMs: timing.responseEnd,
  };
}

/** calque's brotli object as a sample is compared with it. */
export interface BrotliServed {
  readonly objectUrl: string;
  readonly objectBytes: number;
  /** calque's own artifact route, as an absolute URL without an object. */
  readonly artifactRoute: string;
  /** The decoder module's and the worker script's URLs. */
  readonly moduleUrl: string;
  readonly workerUrl: string;
}

/**
 * calque's own object of a plan as a sample's ledger shows it, whatever its
 * encoding. `redact` is the ledger's redaction of a URL: each plan carries
 * its own capability.
 */
export function plannedObject(
  artifact: PlannedArtifact,
  redact: (url: string) => string,
): Pick<BrotliServed, "objectUrl" | "objectBytes" | "artifactRoute"> {
  const url = new URL(artifact.downloadUrl);
  return {
    objectUrl: redact(artifact.downloadUrl),
    objectBytes: artifact.encodedBytes,
    artifactRoute: `${url.origin}${url.pathname.slice(0, url.pathname.lastIndexOf("/") + 1)}`,
  };
}

/**
 * calque's own brotli object, packed or plain, as a sample is compared with
 * it, or null where the plan selects its zstd one, which the page loads
 * without the decoder.
 */
export function calqueServed(
  artifact: PlannedArtifact,
  redact: (url: string) => string,
  files: Pick<BrotliServed, "moduleUrl" | "workerUrl">,
): BrotliServed | null {
  return artifact.encoding === "zstd"
    ? null
    : { ...plannedObject(artifact, redact), ...files };
}

/** One request in a sample's network ledger, as far as this module reads it. */
interface LedgerRequest {
  readonly url: string;
  readonly contentEncoding: string | null;
  readonly contentLength: number | null;
  readonly state: string;
  /** In CDP monotonic milliseconds, the timebase of a sample's `T_nav`. */
  readonly sentMs: number;
  readonly respondedMs: number | null;
  readonly lastDataMs: number | null;
  readonly endedMs: number | null;
}

/**
 * What a sample received of calque's brotli object, as its record's
 * `brotli`. `artifacts` lists each request the ledger has for an artifact,
 * with its times from `T_nav`. `object` says whether the sample received the
 * planned object and no other: every artifact request was for its URL and
 * answered with no content coding and its `Content-Length`, and there was at
 * least one. `module` and `worker` are the requests for the decoder's two
 * files.
 */
export interface BrotliSample {
  readonly object: boolean;
  readonly artifacts: readonly {
    readonly url: string;
    readonly contentEncoding: string | null;
    readonly contentLength: number | null;
    readonly state: string;
    readonly requestMs: number | null;
    readonly firstByteMs: number | null;
    readonly lastByteMs: number | null;
    readonly endedMs: number | null;
  }[];
  readonly module: readonly DecoderRequest[];
  readonly worker: readonly DecoderRequest[];
}

/**
 * A sample's `BrotliSample`, from its ledger and its decoder requests.
 * `navigationMs` is `T_nav` on the ledger's clock, or null for a sample that
 * has none, whose ledger times are then null.
 */
export function brotliSample(
  served: BrotliServed,
  sample: {
    readonly requests: readonly LedgerRequest[];
    readonly decoder: readonly DecoderRequest[];
    readonly navigationMs: number | null;
  },
): BrotliSample {
  const fromNavigation = (ms: number | null) =>
    ms === null || sample.navigationMs === null
      ? null
      : ms - sample.navigationMs;
  const artifacts = sample.requests.filter((request) =>
    request.url.startsWith(served.artifactRoute),
  );
  return {
    object:
      artifacts.length > 0 &&
      artifacts.every(
        (request) =>
          request.url === served.objectUrl &&
          request.contentEncoding === null &&
          request.contentLength === served.objectBytes,
      ),
    artifacts: artifacts.map((request) => ({
      url: request.url,
      contentEncoding: request.contentEncoding,
      contentLength: request.contentLength,
      state: request.state,
      requestMs: fromNavigation(request.sentMs),
      firstByteMs: fromNavigation(request.respondedMs),
      lastByteMs: fromNavigation(request.lastDataMs),
      endedMs: fromNavigation(request.endedMs),
    })),
    module: sample.decoder.filter(
      (request) => request.url === served.moduleUrl,
    ),
    worker: sample.decoder.filter(
      (request) => request.url === served.workerUrl,
    ),
  };
}

/**
 * A run's `brotli` result, over its judged cold samples: the ones that
 * received calque's brotli object and the decoder module, and the ones that
 * did not, with what each lacked.
 */
export function brotliSummary(
  cold: readonly {
    readonly name: string;
    readonly brotli?: BrotliSample;
  }[],
) {
  const lacks = (sample: (typeof cold)[number]) => [
    ...(sample.brotli?.object === true
      ? []
      : ["the published object as its only artifact"]),
    ...(sample.brotli?.module.some((request) => request.status === 200) === true
      ? []
      : ["the decoder module"]),
  ];
  return {
    received: cold
      .filter((sample) => lacks(sample).length === 0)
      .map((sample) => sample.name),
    notReceived: cold
      .filter((sample) => lacks(sample).length > 0)
      .map((sample) => ({ sample: sample.name, lacks: lacks(sample) })),
  };
}
