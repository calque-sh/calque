/**
 * The judgement `verify-client-browser-consumer.ts` passes on one load of the
 * packaged client in Chromium, kept apart from the browser so that a planted
 * failure can be shown to fail it.
 */
import { hasherCount } from "../../client/packages/worker/src/columnar-hashers.ts";

/**
 * What the consumer page records on two globals, read back through
 * `evaluate`.
 */
export interface CreatedWorkerRecord {
  readonly name: string | null;
  readonly type: string | null;
  readonly url: string;
}

export interface WorkerTrace {
  readonly created: CreatedWorkerRecord[];
  terminated: number;
}

export interface ConsumerResult {
  readonly ok: boolean;
  readonly states: readonly string[];
  readonly refusal: string;
}

/** What the gate reads from the page after one load. */
export interface ConsumerEvidence {
  readonly result: ConsumerResult | undefined;
  readonly trace: WorkerTrace | undefined;
  /** `navigator.hardwareConcurrency` in the page. */
  readonly threads: number;
}

/**
 * Why one load over `encoding` fails the gate, or undefined when the client
 * went create, ready, reset, ready, close; refused the collection-less
 * schema; and terminated every worker it created, which are one engine and
 * `hasherCount(threads)` hashers per activation and nothing else.
 */
export function consumerEvidenceFault(
  encoding: string,
  evidence: ConsumerEvidence,
  pageErrors: readonly string[],
): string | undefined {
  if (pageErrors.length > 0)
    return `browser consumer page errors (${encoding}): ${pageErrors.join(" | ")}`;
  if (
    !evidence.result?.ok ||
    JSON.stringify(evidence.result.states) !==
      JSON.stringify(["ready", "ready", "closed"])
  )
    return `browser consumer lifecycle failed (${encoding}): ${JSON.stringify(evidence.result)}`;
  if (evidence.result.refusal !== "the schema carries no generated collections")
    return `collection-less schema was not refused (${encoding}): ${JSON.stringify(evidence.result.refusal)}`;
  const hashersPerLoad = hasherCount(evidence.threads);
  const created = evidence.trace?.created ?? [];
  const engines = created.filter(
    (worker) =>
      worker.name === "calque-columnar" &&
      worker.type === "module" &&
      worker.url.includes("columnar-browser-worker-entry"),
  );
  const hashers = created.filter(
    (worker) =>
      worker.name === "calque-hasher" &&
      worker.type === "module" &&
      worker.url.includes("columnar-browser-hasher-entry"),
  );
  if (
    engines.length !== 2 ||
    hashers.length !== 2 * hashersPerLoad ||
    created.length !== engines.length + hashers.length ||
    evidence.trace?.terminated !== created.length
  )
    return `browser consumer worker lifecycle drifted (${encoding}, ${String(hashersPerLoad)} hashers a load): ${JSON.stringify(evidence.trace)}`;
  return undefined;
}
