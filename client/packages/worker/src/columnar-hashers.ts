import { awaitedLater } from "./async.js";
import type {
  HasherFailure,
  HasherRequest,
} from "./columnar-hasher-protocol.js";

/** The most hasher workers one load spawns. */
const MAX_HASHERS = 3;

/**
 * Chunks one hasher holds at most, from the page sending it one to the
 * engine worker crediting it: one being hashed, the next waiting behind it.
 */
export const CHUNKS_PER_HASHER = 2;

/** How long the engine worker may take to credit a chunk the page sent. */
export const HASHER_CREDIT_MS = 30_000;

/**
 * How many hashers a load spawns on a machine of `threads` threads: up to
 * `MAX_HASHERS`, leaving four threads to the page, the engine worker and the
 * browser, and never none.
 */
export function hasherCount(
  threads: number | undefined = globalThis.navigator?.hardwareConcurrency,
): number {
  return Math.min(MAX_HASHERS, Math.max(1, (threads ?? 1) - 4));
}

/**
 * A hasher worker as the page drives it; a DOM `Worker` fits. Its `message`
 * events carry a `HasherFailure`, the only message a hasher sends the page.
 */
export interface HasherWorker {
  addEventListener(
    type: "message" | "error" | "messageerror",
    listener: (event: Event) => void,
  ): void;
  postMessage(message: HasherRequest, transfer: Transferable[]): void;
  terminate(): void;
}

interface Hasher {
  readonly worker: HasherWorker;
  /** The chunks sent to it that the engine worker has not credited. */
  readonly held: Set<number>;
}

/**
 * The hasher workers of one load. The page spawns them before it requests
 * the plan, so they have loaded by the artifact's first byte; each gets one
 * end of a channel whose other end the engine worker's `open` takes
 * (`takePorts`). The page sends each chunk to the hasher holding the fewest,
 * and the engine worker's credit for it frees that hasher to take another.
 *
 * Any failure refuses the load and terminates every hasher: a hasher that
 * cannot be constructed, reports an error, sends a message that cannot be
 * decoded or could not digest a chunk, or a chunk the engine worker has not
 * credited `HASHER_CREDIT_MS` after it was sent. There is no other path to
 * hash a chunk on.
 */
export class ArtifactHashers {
  readonly #hashers: Hasher[] = [];
  #ports: MessagePort[] = [];
  readonly #sent = new Map<
    number,
    { readonly hasher: Hasher; readonly timer: ReturnType<typeof setTimeout> }
  >();
  /** `send` calls waiting for a hasher with room, in the order they came. */
  readonly #waiting: (() => void)[] = [];
  #failure: { readonly error: Error } | undefined;
  #closed = false;
  readonly #refused = Promise.withResolvers<never>();
  /** Rejects with the reason once the load is refused; never resolves. */
  readonly failed = awaitedLater(this.#refused.promise);

  /**
   * Spawns `count` hashers. A constructor that throws terminates the ones
   * already spawned and throws its error.
   */
  constructor(create: () => HasherWorker, count: number) {
    if (!Number.isSafeInteger(count) || count < 1)
      throw new RangeError("a load needs at least one hasher");
    for (let spawned = 0; spawned < count; spawned++) {
      let worker: HasherWorker;
      try {
        worker = create();
      } catch (error) {
        this.close();
        throw error;
      }
      this.#hashers.push({ worker, held: new Set() });
      worker.addEventListener("message", (event) => {
        const { index, message } = (event as MessageEvent<HasherFailure>).data;
        this.fail(
          new Error(
            `a hasher could not digest chunk ${String(index)}: ${message}`,
          ),
        );
      });
      worker.addEventListener("error", (event) => {
        this.fail(
          new Error(
            (event as Partial<ErrorEvent>).message || "a hasher worker failed",
          ),
        );
      });
      worker.addEventListener("messageerror", () => {
        this.fail(new Error("a hasher worker message could not be decoded"));
      });
      const channel = new MessageChannel();
      worker.postMessage({ type: "port", port: channel.port1 }, [
        channel.port1,
      ]);
      this.#ports.push(channel.port2);
    }
  }

  /** The engine worker's ends of the hashers' channels; taken once. */
  takePorts(): MessagePort[] {
    const ports = this.#ports;
    this.#ports = [];
    return ports;
  }

  /**
   * Sends chunk `index` to the hasher holding the fewest chunks, once one
   * holds fewer than `CHUNKS_PER_HASHER`, transferring `bytes`. It rejects
   * once the load is refused or the hashers are closed.
   */
  async send(index: number, bytes: ArrayBuffer): Promise<void> {
    for (;;) {
      if (this.#failure) throw this.#failure.error;
      if (this.#closed) throw new Error("the hashers are closed");
      let hasher: Hasher | undefined;
      for (const candidate of this.#hashers)
        if (!hasher || candidate.held.size < hasher.held.size)
          hasher = candidate;
      if (hasher && hasher.held.size < CHUNKS_PER_HASHER) {
        const timer = setTimeout(() => {
          this.fail(
            new Error(
              `chunk ${String(index)} was not accepted within ${String(HASHER_CREDIT_MS)} ms`,
            ),
          );
        }, HASHER_CREDIT_MS);
        hasher.held.add(index);
        this.#sent.set(index, { hasher, timer });
        hasher.worker.postMessage({ type: "chunk", index, bytes }, [bytes]);
        return;
      }
      await new Promise<void>((resolve) => {
        this.#waiting.push(resolve);
      });
    }
  }

  /** The engine worker accepted chunk `index`; its hasher may take another. */
  credit(index: number): void {
    const sent = this.#sent.get(index);
    if (!sent) return;
    this.#sent.delete(index);
    clearTimeout(sent.timer);
    sent.hasher.held.delete(index);
    this.#waiting.shift()?.();
  }

  /**
   * Refuses the load: every hasher is terminated, and `send` and `failed`
   * reject with `error`. Only the first failure counts.
   */
  fail(error: unknown): void {
    if (this.#failure || this.#closed) return;
    const failure = error instanceof Error ? error : new Error(String(error));
    this.#failure = { error: failure };
    this.close();
    this.#refused.reject(failure);
  }

  /** Terminates every hasher. A `send` still waiting rejects. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const { timer } of this.#sent.values()) clearTimeout(timer);
    this.#sent.clear();
    for (const { worker } of this.#hashers) worker.terminate();
    for (const port of this.takePorts()) port.close();
    for (const wake of this.#waiting.splice(0)) wake();
  }
}
