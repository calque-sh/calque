import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { mutation, validateMutationBatchResponse } from "@calque/protocol";
import { stopReading } from "@calque/worker/columnar";
import { CONTROL_CHARACTER, MAX_ARTIFACT_CHUNK_BYTES } from "./limits.js";

import { timeoutSignal } from "./abort-signal.js";
import {
  acceptedArtifactEncodings,
  type ArtifactEncodings,
  BOOTSTRAP_PROTOCOL_VERSION,
  bootstrapInit,
  bootstrapUrl,
  CHROMIUM_NATIVE_ARTIFACT_PROFILE,
  chromiumNativeArtifactProfile,
} from "./bootstrap-start.js";
import { networkError, responseError, SyncError } from "./error.js";
import {
  freezeMutationResult,
  MutationSubmissionError,
  type MutationResult,
} from "./mutation.js";
import {
  parseBootstrapPlan,
  type BootstrapPlan,
  type PlanArtifact,
} from "./plan.js";
import type { SyncAuth } from "./client.js";

// The session's HTTP edge: the bootstrap request, the exact artifact
// download, and the sealing of what `auth()` returns.

export const DIRECT_COLUMNAR_ARTIFACT_MEDIA_TYPE =
  "application/vnd.calque.direct-columnar.v4";
const MAX_BOOTSTRAP_RESPONSE_BYTES = 2 * 1024 * 1024;
/** A refusal body is a small frozen document; anything larger is not one. */
const MAX_BOOTSTRAP_ERROR_BYTES = 8 * 1024;
/**
 * Bounds on the server's `Retry-After`, in milliseconds: the lower keeps a
 * zero or absent header from becoming a spin, and the upper is the most a
 * conforming server asks for.
 */
const MIN_PREPARING_RETRY_MS = 1_000;
const MAX_PREPARING_RETRY_MS = 300_000;
const MAX_BEARER_TOKEN_CODE_UNITS = 16_384;

/**
 * The encodings a plan's selected representation may be in, preferred first.
 * The bootstrap request names the brotli ones only where the page can run the
 * decoder worker, but a plan that selects one is read either way.
 */
const READABLE_ARTIFACT_ENCODINGS = Object.freeze([
  "packed-brotli-large-window",
  "brotli-large-window",
  "zstd",
  "identity",
] as const);

export interface ArtifactTransportProfile {
  readonly name: typeof CHROMIUM_NATIVE_ARTIFACT_PROFILE;
  /** What the bootstrap request names. */
  readonly acceptedEncodings: ArtifactEncodings;
  /** What a plan's selected representation may be. */
  readonly readableEncodings: readonly PlanArtifact["selectedRepresentation"]["encoding"][];
}

export function artifactTransportProfile(): ArtifactTransportProfile {
  if (!chromiumNativeArtifactProfile())
    throw new SyncError(
      "UNSUPPORTED_BROWSER",
      "client",
      "none",
      "this browser does not report the Chromium brand; Calque runs only in Chromium-based browsers",
    );
  return {
    name: CHROMIUM_NATIVE_ARTIFACT_PROFILE,
    acceptedEncodings: acceptedArtifactEncodings(),
    readableEncodings: READABLE_ARTIFACT_ENCODINGS,
  };
}

interface BootstrapRequest {
  readonly fetch: typeof fetch;
  readonly endpoint: URL;
  readonly bearerToken: string;
  readonly transport: ArtifactTransportProfile;
  readonly requiredTables: readonly string[];
  readonly memoryBudgetBytes: number;
  readonly clientHeapReserveBytes: number;
  readonly clientInstanceId: string;
  readonly deviceId: string;
  readonly signal?: AbortSignal | undefined;
}

/**
 * Posts one bootstrap request and returns its parsed, unexpired plan. Every
 * failure is a public `SyncError`; the raw response never reaches the caller.
 */
export function requestBootstrapPlan(
  request: BootstrapRequest,
): Promise<BootstrapPlan> {
  let response: Promise<Response>;
  try {
    response = request.fetch(
      bootstrapUrl(request.endpoint),
      bootstrapInit(
        {
          bearerToken: request.bearerToken,
          transportProfile: request.transport.name,
          protocolVersion: BOOTSTRAP_PROTOCOL_VERSION,
          requiredTables: request.requiredTables,
          acceptedEncodings: request.transport.acceptedEncodings,
          memoryBudgetBytes: request.memoryBudgetBytes,
          clientHeapReserveBytes: request.clientHeapReserveBytes,
          deviceId: request.deviceId,
        },
        request.clientInstanceId,
        request.signal,
      ),
    );
  } catch (error) {
    response = Promise.reject(error);
  }
  return readBootstrapPlan(response, request.endpoint);
}

/**
 * The parsed, unexpired plan a bootstrap request's response carries: the
 * session's own request's, or the one the page started and the session
 * adopted, through the same checks.
 */
export async function readBootstrapPlan(
  pending: Promise<Response>,
  endpoint: URL,
): Promise<BootstrapPlan> {
  let response: Response;
  try {
    response = await pending;
  } catch (error) {
    throw networkError(
      error,
      "sync",
      `could not reach ${endpoint.origin}: check that it is running, that this browser trusts its certificate, and that it allows this page's origin`,
    );
  }
  if (!response.ok) throw await bootstrapRefusal(response);
  const bytes = await responseBytes(
    response,
    MAX_BOOTSTRAP_RESPONSE_BYTES,
    "sync",
  );
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder().decode(bytes));
  } catch (error) {
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "bootstrap response is not valid JSON",
      { cause: error },
    );
  }
  let plan: BootstrapPlan;
  try {
    plan = parseBootstrapPlan(value);
  } catch (error) {
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "bootstrap response does not match the protocol",
      { cause: error },
    );
  }
  if (Date.now() >= Date.parse(plan.expiresAt))
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "retry",
      "bootstrap plan expired",
    );
  return plan;
}

/**
 * Fetches the representation the plan selected for one artifact and holds the
 * response headers to it. The body is untouched: the caller streams or
 * buffers it against `artifact.decodedBytes`.
 */
export async function fetchArtifact(
  fetchImplementation: typeof fetch,
  transport: ArtifactTransportProfile,
  artifact: PlanArtifact,
  expectedMediaType: string,
  signal?: AbortSignal,
): Promise<Response> {
  if (
    !transport.readableEncodings.includes(
      artifact.selectedRepresentation.encoding,
    )
  )
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "bootstrap selected an unsupported artifact encoding",
    );
  let response: Response;
  try {
    response = await fetchImplementation(
      artifact.selectedRepresentation.downloadUrl,
      { method: "GET", cache: "default", ...(signal ? { signal } : {}) },
    );
  } catch (error) {
    throw networkError(error, "sync", "artifact download failed");
  }
  if (!response.ok) {
    const message = `artifact download failed with HTTP ${response.status}`;
    throw response.status === 401 || response.status === 404
      ? new ArtifactCapabilityError(response.status, message)
      : responseError(response.status, "sync", message);
  }
  validateArtifactResponse(response, artifact, expectedMediaType);
  return response;
}

function validateArtifactResponse(
  response: Response,
  artifact: PlanArtifact,
  expectedMediaType: string,
): void {
  if (response.status !== 200 || response.headers.has("content-range"))
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact response must be a complete HTTP 200 response",
    );
  const expectedEncoding = artifact.selectedRepresentation.encoding;
  const actualEncoding = response.headers.get("content-encoding");
  // The browser removes only zstd, so only a zstd response names its coding.
  // A brotli body reaches the page as stored, for its decoder worker.
  if (
    expectedEncoding === "zstd"
      ? actualEncoding?.trim().toLowerCase() !== expectedEncoding
      : actualEncoding !== null
  )
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      `artifact Content-Encoding does not match selected ${expectedEncoding} representation`,
    );

  const contentLength = response.headers.get("content-length");
  if (!contentLength || !/^(0|[1-9][0-9]*)$/.test(contentLength))
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact response has no exact encoded Content-Length",
    );
  const encodedBytes = Number(contentLength);
  if (
    !Number.isSafeInteger(encodedBytes) ||
    encodedBytes !== artifact.selectedRepresentation.encodedBytes
  )
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact encoded Content-Length does not match the plan",
    );

  const mediaType = response.headers
    .get("content-type")
    ?.split(";", 1)[0]
    ?.trim()
    .toLowerCase();
  if (mediaType !== expectedMediaType)
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact response has an unexpected Content-Type",
    );
}

/**
 * An artifact download the server answered 401 or 404: the capability in the
 * plan's download URL is missing, malformed, expired or no longer held, as
 * after a restart. The session asks `auth()` and the bootstrap for a new plan
 * once before reporting it.
 */
export class ArtifactCapabilityError extends SyncError {
  constructor(status: 401 | 404, message: string) {
    const { code, scope, recovery } = responseError(status, "sync", message);
    super(code, scope, recovery, message);
  }
}

/**
 * A `503 ARTIFACT_PREPARING` refusal: the server is publishing the artifact
 * of a new authorization generation, as after a revocation, or a tenant's
 * first working set, and names when to ask again in `Retry-After`. The client
 * follows that schedule rather than a backoff of its own.
 */
export class ArtifactPreparingError extends SyncError {
  readonly retryAfterMs: number;

  constructor(message: string, retryAfterMs: number) {
    super("ARTIFACT_PREPARING", "sync", "retry", message);
    this.name = "ArtifactPreparingError";
    this.retryAfterMs = retryAfterMs;
  }
}

/**
 * How long the bootstrap follows the server's preparing schedule before
 * reporting `ARTIFACT_PREPARING`, so a server that never finishes publishing
 * still fails closed.
 */
export const MAX_PREPARING_WAIT_MS = 120_000;

/**
 * The error a bootstrap refusal reports, decided by its status and the `code`
 * its body names.
 *
 * The status alone is not enough: 503 is also how an ordinary unavailable
 * server answers, and that one is reported rather than waited out. A 400, 413
 * or 422 refuses the request itself, which the same request would meet again,
 * so none of them is reported as retryable.
 */
async function bootstrapRefusal(response: Response): Promise<SyncError> {
  const { status } = response;
  const code = await refusalCode(response);
  if (status === 503 && code === "ARTIFACT_PREPARING")
    return new ArtifactPreparingError(
      "the server has not published this grant's artifact yet",
      retryAfterMs(response.headers.get("retry-after")),
    );
  if (status === 422 && code === "CAPACITY_EXCEEDED")
    return new SyncError(
      "CAPACITY_EXCEEDED",
      "sync",
      "none",
      "the server refused the bootstrap: the working set does not fit memoryBudgetBytes",
    );
  const message = `bootstrap failed with HTTP ${status}${code === undefined ? "" : ` ${code}`}`;
  if (status === 400 || status === 413 || status === 422)
    return new SyncError("SERVER_ERROR", "sync", "none", message);
  return responseError(status, "sync", message);
}

/** The `code` a refusal body names, or nothing when it names none. */
async function refusalCode(response: Response): Promise<string | undefined> {
  let document: unknown;
  try {
    const bytes = await responseBytes(
      response,
      MAX_BOOTSTRAP_ERROR_BYTES,
      "sync",
    );
    document = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    return undefined;
  }
  if (typeof document !== "object" || document === null) return undefined;
  const { code } = document as { code?: unknown };
  return typeof code === "string" && /^[A-Z_]{1,64}$/u.test(code)
    ? code
    : undefined;
}

/** `Retry-After` in delta-seconds, clamped. A date form is not used here. */
function retryAfterMs(header: string | null): number {
  const seconds = header === null ? Number.NaN : Number(header.trim());
  if (!Number.isFinite(seconds)) return MIN_PREPARING_RETRY_MS;
  return Math.min(
    MAX_PREPARING_RETRY_MS,
    Math.max(MIN_PREPARING_RETRY_MS, Math.round(seconds * 1000)),
  );
}

async function responseBytes(
  response: Response,
  limit: number,
  scope: "sync" | "mutation",
): Promise<Uint8Array<ArrayBuffer>> {
  const bytes = new Uint8Array(limit);
  let length = 0;
  if (!response.body)
    throw new SyncError(
      "INVALID_RESPONSE",
      scope,
      scope === "mutation" ? "retry" : "reset",
      "HTTP response has no body",
    );
  const reader = response.body.getReader();
  try {
    while (true) {
      let next: ReadableStreamReadResult<Uint8Array>;
      try {
        next = await reader.read();
      } catch (error) {
        throw networkError(error, scope, "HTTP response stream failed");
      }
      if (next.done) break;
      length += next.value.byteLength;
      if (length > limit) {
        await stopReading(reader);
        throw new SyncError(
          "INVALID_RESPONSE",
          scope,
          scope === "mutation" ? "retry" : "reset",
          `HTTP response exceeds ${limit} bytes`,
        );
      }
      bytes.set(next.value, length - next.value.byteLength);
    }
  } finally {
    reader.releaseLock();
  }
  return bytes.subarray(0, length);
}

/**
 * Reads exactly `expectedBytes` from a validated artifact response into
 * consecutive fresh buffers of `bufferBytes`, the last holding what is left,
 * and yields each buffer once it is full. Every read is a bounded BYOB read
 * straight into the buffer it fills, so the browser writes each byte where the
 * caller takes it and no copy follows. Short, long, and non-transferable
 * responses fail closed.
 */
export async function* exactResponseBuffers(
  response: Response,
  expectedBytes: number,
  bufferBytes: number,
): AsyncGenerator<ArrayBuffer> {
  if (!Number.isSafeInteger(bufferBytes) || bufferBytes < 1)
    throw new RangeError("artifact buffers must hold at least one byte");
  if (!response.body)
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact response has no body",
    );
  let received = 0;
  let complete = false;
  let reader: ReadableStreamBYOBReader;
  try {
    reader = response.body.getReader({ mode: "byob" });
  } catch (error) {
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact response does not support bounded BYOB reads",
      { cause: error },
    );
  }
  let buffer: ArrayBuffer | undefined;
  let filled = 0;
  try {
    while (true) {
      // Once every expected byte is in, a read of one more byte must find the
      // end of the body.
      buffer ??= new ArrayBuffer(
        Math.max(1, Math.min(bufferBytes, expectedBytes - received)),
      );
      const capacity = buffer.byteLength;
      const requested = Math.min(MAX_ARTIFACT_CHUNK_BYTES, capacity - filled);
      let next: ReadableStreamReadResult<Uint8Array<ArrayBufferLike>>;
      try {
        next = await reader.read(new Uint8Array(buffer, filled, requested));
      } catch (error) {
        throw networkError(error, "sync", "artifact response stream failed");
      }
      if (next.done) {
        complete = true;
        break;
      }
      if (
        !(next.value instanceof Uint8Array) ||
        next.value.byteLength === 0 ||
        next.value.byteLength > requested ||
        next.value.byteOffset !== filled ||
        next.value.buffer.byteLength !== capacity
      ) {
        await stopReading(reader);
        throw new SyncError(
          "INVALID_RESPONSE",
          "sync",
          "reset",
          "artifact response violated the bounded BYOB contract",
        );
      }
      if (!(next.value.buffer instanceof ArrayBuffer)) {
        await stopReading(reader);
        throw new SyncError(
          "INVALID_RESPONSE",
          "sync",
          "reset",
          "artifact response returned a non-transferable buffer",
        );
      }
      // The read transferred `buffer`; the bytes live on in `next.value`.
      buffer = next.value.buffer;
      filled += next.value.byteLength;
      received += next.value.byteLength;
      if (received > expectedBytes) {
        await stopReading(reader);
        throw new SyncError(
          "INVALID_RESPONSE",
          "sync",
          "reset",
          "artifact response exceeds decoded length",
        );
      }
      if (filled === capacity) {
        yield buffer;
        buffer = undefined;
        filled = 0;
      }
    }
  } finally {
    if (!complete) await stopReading(reader);
    reader.releaseLock();
  }
  if (received !== expectedBytes)
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      `artifact response contained ${received} of ${expectedBytes} bytes`,
    );
}

/** A validated artifact response's body, read as the network delivers it. */
export interface ExactResponsePieces {
  /**
   * The body's next bytes, in a buffer that holds nothing else, with `end`
   * set on the ones that complete `expectedBytes`; undefined once the body
   * has ended there. A body that ends short or runs long fails closed.
   */
  read(): Promise<
    { readonly bytes: ArrayBuffer; readonly end: boolean } | undefined
  >;
  /** Cancels what is left of the body. */
  cancel(): Promise<void>;
}

/**
 * Reads exactly `expectedBytes` from a validated artifact response whose body
 * the page does not keep: each read's bytes are passed on in the buffer the
 * browser delivered them in, for the caller to transfer.
 */
export function exactResponsePieces(
  response: Response,
  expectedBytes: number,
): ExactResponsePieces {
  if (!response.body)
    throw new SyncError(
      "INVALID_RESPONSE",
      "sync",
      "reset",
      "artifact response has no body",
    );
  const reader = response.body.getReader();
  let received = 0;
  let ended = false;
  let cancelled = false;
  const cancel = async (): Promise<void> => {
    if (ended || cancelled) return;
    cancelled = true;
    await stopReading(reader);
  };
  return {
    cancel,
    async read() {
      for (;;) {
        let next: ReadableStreamReadResult<Uint8Array>;
        try {
          next = await reader.read();
        } catch (error) {
          throw networkError(error, "sync", "artifact response stream failed");
        }
        // A cancelled body ends wherever it was; its length says nothing.
        if (cancelled) return undefined;
        if (next.done) {
          ended = true;
          if (received !== expectedBytes)
            throw new SyncError(
              "INVALID_RESPONSE",
              "sync",
              "reset",
              `artifact response contained ${received} of ${expectedBytes} bytes`,
            );
          return undefined;
        }
        const { value } = next;
        if (value.byteLength === 0) continue;
        received += value.byteLength;
        if (received > expectedBytes) {
          await cancel();
          throw new SyncError(
            "INVALID_RESPONSE",
            "sync",
            "reset",
            "artifact response exceeds encoded length",
          );
        }
        // The browser gives each read a buffer of its own. A view of part of
        // one, which nothing here is known to produce, is copied so that the
        // caller transfers these bytes and no others.
        const whole =
          value.buffer instanceof ArrayBuffer &&
          value.byteOffset === 0 &&
          value.byteLength === value.buffer.byteLength;
        return {
          bytes: whole
            ? (value.buffer as ArrayBuffer)
            : new Uint8Array(value).buffer,
          end: received === expectedBytes,
        };
      }
    },
  };
}

export class InvalidSyncAuthError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidSyncAuthError";
  }
}

export function sealAuthSnapshot(value: unknown): SyncAuth {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    throw new InvalidSyncAuthError(
      "auth() must return an object with an own bearerToken data property",
    );
  }

  let bearerTokenDescriptor: PropertyDescriptor | undefined;
  try {
    bearerTokenDescriptor = Object.getOwnPropertyDescriptor(
      value,
      "bearerToken",
    );
  } catch {
    throw new InvalidSyncAuthError(
      "auth() must return an object whose own bearerToken property can be inspected",
    );
  }
  if (
    bearerTokenDescriptor === undefined ||
    !("value" in bearerTokenDescriptor)
  ) {
    throw new InvalidSyncAuthError(
      "auth() bearerToken must be an own data property",
    );
  }

  const bearerToken = bearerTokenDescriptor.value as unknown;
  if (typeof bearerToken !== "string" || bearerToken.length === 0) {
    throw new InvalidSyncAuthError(
      "auth() bearerToken must be a non-empty string",
    );
  }
  if (bearerToken.length > MAX_BEARER_TOKEN_CODE_UNITS) {
    throw new InvalidSyncAuthError(
      `auth() bearerToken exceeds its ${MAX_BEARER_TOKEN_CODE_UNITS} code-unit limit`,
    );
  }
  if (CONTROL_CHARACTER.test(bearerToken)) {
    throw new InvalidSyncAuthError(
      "auth() bearerToken must not contain control characters",
    );
  }
  return Object.freeze({ bearerToken });
}

const MUTATION_MEDIA_TYPE = "application/vnd.calque.mutation.v1+protobuf";
const MAX_MUTATION_RESPONSE_BYTES = 1024 * 1024;
const MUTATION_REQUEST_TIMEOUT_MS = 30_000;

interface MutationSubmission {
  readonly fetch: typeof fetch;
  /** The sync root; the mutation endpoint is `v1/mutations` under it. */
  readonly endpoint: URL;
  readonly bearerToken: string;
  /** Exactly one envelope: the outcome is matched to its mutation id. */
  readonly request: mutation.MutationBatchRequest;
}

/**
 * POSTs one mutation batch and returns the outcome the server named for it.
 * Every failure short of a decoded outcome, except a definitive 4xx, is
 * ambiguous: the request may have committed, so the caller keeps the
 * optimistic delta and the mutation id for a retry.
 */
export async function submitMutationBatch(
  submission: MutationSubmission,
): Promise<mutation.MutationOutcome> {
  const envelope = submission.request.mutations[0];
  if (submission.request.mutations.length !== 1 || !envelope)
    throw new SyncError(
      "INTERNAL_ERROR",
      "mutation",
      "none",
      "a mutation batch submits exactly one mutation",
    );
  const mutationId = envelope.mutationId;
  let response: Response;
  const timeout = timeoutSignal(MUTATION_REQUEST_TIMEOUT_MS);
  try {
    response = await submission.fetch(
      new URL("v1/mutations", submission.endpoint),
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${submission.bearerToken}`,
          "content-type": MUTATION_MEDIA_TYPE,
          accept: MUTATION_MEDIA_TYPE,
        },
        body: toBinary(mutation.MutationBatchRequestSchema, submission.request),
        signal: timeout.signal,
      },
    );
  } catch (error) {
    timeout.dispose();
    throw new MutationSubmissionError(
      mutationId,
      "MUTATION_AMBIGUOUS",
      "mutation submission did not receive an HTTP response; retry the same mutation ID",
      { cause: networkError(error, "mutation", "mutation request failed") },
    );
  }
  if (!response.ok) {
    timeout.dispose();
    // A caller without write capability gets the same 404 as an unmounted
    // route. No command ran either way, so the delta is dropped: kept for a
    // retry that cannot succeed, it would mask the row's canonical value.
    const definitive =
      response.status === 400 ||
      response.status === 404 ||
      response.status === 422;
    const authorization = response.status === 401 || response.status === 403;
    throw new MutationSubmissionError(
      mutationId,
      authorization
        ? "AUTH_FAILED"
        : definitive
          ? "MUTATION_REJECTED"
          : "MUTATION_AMBIGUOUS",
      authorization
        ? "mutation authorization failed; reset is required"
        : response.status === 404
          ? "this deployment serves no mutation route for these credentials"
          : definitive
            ? "mutation request was rejected"
            : "mutation submission failed; retry the same mutation ID",
    );
  }
  let bytes: Uint8Array<ArrayBuffer>;
  try {
    bytes = await responseBytes(
      response,
      MAX_MUTATION_RESPONSE_BYTES,
      "mutation",
    );
  } catch (error) {
    throw new MutationSubmissionError(
      mutationId,
      "MUTATION_AMBIGUOUS",
      "mutation response could not be read; retry the same mutation ID",
      { cause: error },
    );
  } finally {
    timeout.dispose();
  }
  let decoded: mutation.MutationBatchResponse;
  try {
    decoded = fromBinary(mutation.MutationBatchResponseSchema, bytes);
    validateMutationBatchResponse(decoded);
  } catch (error) {
    throw new MutationSubmissionError(
      mutationId,
      "MUTATION_AMBIGUOUS",
      "mutation response was invalid; retry the same mutation ID",
      { cause: error },
    );
  }
  const outcome = decoded.outcomes[0];
  if (decoded.outcomes.length !== 1 || outcome?.mutationId !== mutationId)
    throw new MutationSubmissionError(
      mutationId,
      "MUTATION_AMBIGUOUS",
      "mutation response did not match the submitted mutation ID",
    );
  return outcome;
}

/** Whether an outcome settles the mutation: nothing pending, nothing to retry. */
export function mutationOutcomeIsDefinitive(
  code: mutation.MutationOutcomeCode,
): boolean {
  return ![
    mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO,
    mutation.MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO,
    mutation.MutationOutcomeCode.RETRYABLE_FAILURE,
  ].includes(code);
}

/** The application-facing result one wire outcome publishes. */
export function mutationOutcomeResult(
  outcome: mutation.MutationOutcome,
): MutationResult {
  switch (outcome.code) {
    case mutation.MutationOutcomeCode.ACCEPTED_AWAITING_CANONICAL_ECHO:
    case mutation.MutationOutcomeCode.ALREADY_ACCEPTED_AWAITING_CANONICAL_ECHO:
      return freezeMutationResult({
        mutationId: outcome.mutationId,
        state: "awaiting_canonical_echo",
        message: outcome.message,
      });
    case mutation.MutationOutcomeCode.ALREADY_CANONICAL:
      return freezeMutationResult({
        mutationId: outcome.mutationId,
        state: "already_canonical",
        message: outcome.message,
      });
    case mutation.MutationOutcomeCode.CONFLICT: {
      if (!outcome.conflict)
        throw new MutationSubmissionError(
          outcome.mutationId,
          "MUTATION_AMBIGUOUS",
          "conflict mutation outcome omitted conflict details; retry the same mutation ID",
        );
      return freezeMutationResult({
        mutationId: outcome.mutationId,
        state: "conflict",
        message: outcome.message,
        conflict: {
          kind: outcome.conflict.kind,
          applicationPayload: outcome.conflict.applicationPayload.slice(),
        },
      });
    }
    case mutation.MutationOutcomeCode.RETRYABLE_FAILURE:
      if (outcome.retryAfterMs <= 0n)
        throw new MutationSubmissionError(
          outcome.mutationId,
          "MUTATION_AMBIGUOUS",
          "retryable mutation outcome omitted its retry delay; retry the same mutation ID",
        );
      return freezeMutationResult({
        mutationId: outcome.mutationId,
        state: "retryable",
        message: outcome.message,
        retryAfterMs: outcome.retryAfterMs,
      });
    default:
      return freezeMutationResult({
        mutationId: outcome.mutationId,
        state: "rejected",
        message: outcome.message,
      });
  }
}
