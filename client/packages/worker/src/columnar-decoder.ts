import { awaitedLater } from "./async.js";
import {
  DECODER_INPUT_WINDOW_BYTES,
  type DecoderRequest,
  type DecoderResponse,
} from "./columnar-decoder-protocol.js";

/**
 * A decoder worker as the page drives it; a DOM `Worker` fits. Its `message`
 * events carry a `DecoderResponse`.
 */
interface DecoderWorker {
  addEventListener(
    type: "message" | "error" | "messageerror",
    listener: (event: Event) => void,
  ): void;
  postMessage(message: DecoderRequest, transfer: Transferable[]): void;
  terminate(): void;
}

/** What one response body decodes to. */
interface DecodedArtifact {
  readonly decodedBytes: number;
  /** The length of every chunk but the last. */
  readonly chunkBytes: number;
  /** How many chunks, from the first, an earlier response delivered. */
  readonly skipChunks: number;
  /** Whether the body is the encoding of the artifact's packed body. */
  readonly packed: boolean;
}

/**
 * A body the decoder worker refused: it is not the `brotli-large-window`
 * encoding of the artifact its plan describes, or of that artifact's packed
 * body when the plan selects `packed-brotli-large-window`.
 */
export class ArtifactBodyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ArtifactBodyError";
  }
}

/**
 * The decoder worker of one response body. The page spawns it as soon as the
 * plan selects `brotli-large-window` or `packed-brotli-large-window`, so its
 * module loads while the artifact is requested. The body is written no faster
 * than the worker reads it (`room`), and the worker sends a chunk only against
 * a credit, so the page holds at most one decoded chunk a hasher has not
 * taken.
 *
 * Any failure terminates the worker and rejects `failed`, `room` and `next`
 * with one error: an `ArtifactBodyError` for a body the worker refused, and
 * an `Error` for a worker that cannot be constructed, reports an error, or
 * sends something that is not the next chunk.
 */
export class ArtifactDecoder {
  readonly #artifact: DecodedArtifact;
  #worker: DecoderWorker | undefined;
  /** Bytes written that the worker has not said it read. */
  #unread = 0;
  /** The index of the next chunk the worker sends. */
  #index: number;
  /** Chunks the worker sent that `next` has not returned. */
  readonly #arrived: ArrayBuffer[] = [];
  #taking: PromiseWithResolvers<ArrayBuffer> | undefined;
  /** `room` calls waiting for the worker to read on. */
  readonly #writing: PromiseWithResolvers<void>[] = [];
  #failure: { readonly error: Error } | undefined;
  #closed = false;
  readonly #refused = Promise.withResolvers<never>();
  /** Rejects with the reason once the body is refused; never resolves. */
  readonly failed = awaitedLater(this.#refused.promise);

  /** Spawns the worker and tells it what the body decodes to. */
  constructor(create: () => DecoderWorker, artifact: DecodedArtifact) {
    this.#artifact = artifact;
    this.#index = artifact.skipChunks;
    const worker = create();
    this.#worker = worker;
    worker.addEventListener("message", (event) => {
      this.#receive((event as MessageEvent<DecoderResponse>).data);
    });
    worker.addEventListener("error", (event) => {
      this.#fail(
        new Error(
          (event as Partial<ErrorEvent>).message || "the decoder worker failed",
        ),
      );
    });
    worker.addEventListener("messageerror", () => {
      this.#fail(new Error("a decoder worker message could not be decoded"));
    });
    worker.postMessage({ type: "start", ...artifact }, []);
  }

  #receive(response: DecoderResponse): void {
    if (this.#failure || this.#closed) return;
    if (response.type === "failed") {
      this.#fail(
        response.corrupt
          ? new ArtifactBodyError(response.message)
          : new Error(response.message),
      );
      return;
    }
    if (response.type === "taken") {
      this.#unread -= response.bytes;
      if (this.#unread < DECODER_INPUT_WINDOW_BYTES)
        for (const { resolve } of this.#writing.splice(0)) resolve();
      return;
    }
    const { decodedBytes, chunkBytes } = this.#artifact;
    if (
      response.index !== this.#index ||
      response.bytes.byteLength !==
        Math.min(chunkBytes, decodedBytes - response.index * chunkBytes)
    ) {
      this.#fail(
        new Error(
          "the decoder worker sent something other than the next chunk",
        ),
      );
      return;
    }
    this.#index += 1;
    const taking = this.#taking;
    this.#taking = undefined;
    if (taking) taking.resolve(response.bytes);
    else this.#arrived.push(response.bytes);
  }

  /**
   * Resolves once the worker holds fewer than `DECODER_INPUT_WINDOW_BYTES`
   * it has not read: when the page may read more of the body.
   */
  room(): Promise<void> {
    const ended = this.#ended();
    if (ended) return Promise.reject(ended);
    if (this.#unread < DECODER_INPUT_WINDOW_BYTES) return Promise.resolve();
    const writing: PromiseWithResolvers<void> = Promise.withResolvers();
    this.#writing.push(writing);
    return writing.promise;
  }

  /** Sends the body's next bytes, transferred; `end` with its last. */
  write(bytes: ArrayBuffer, end: boolean): void {
    if (!this.#worker) return;
    this.#unread += bytes.byteLength;
    this.#worker.postMessage({ type: "bytes", bytes, end }, [bytes]);
  }

  /** The next decoded chunk, in order. One call at a time. */
  next(): Promise<ArrayBuffer> {
    const ended = this.#ended();
    if (ended) return Promise.reject(ended);
    const arrived = this.#arrived.shift();
    if (arrived) return Promise.resolve(arrived);
    this.#taking = Promise.withResolvers();
    return this.#taking.promise;
  }

  /** A hasher took the chunk `next` returned; the worker may send another. */
  credit(): void {
    this.#worker?.postMessage({ type: "credit" }, []);
  }

  /** Terminates the worker. A `room` or `next` still waiting rejects. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#end(new Error("the decoder is closed"));
  }

  #ended(): Error | undefined {
    if (this.#failure) return this.#failure.error;
    return this.#closed ? new Error("the decoder is closed") : undefined;
  }

  #fail(error: Error): void {
    if (this.#failure || this.#closed) return;
    this.#failure = { error };
    this.#end(error);
    this.#refused.reject(error);
  }

  #end(error: Error): void {
    this.#worker?.terminate();
    this.#worker = undefined;
    this.#arrived.length = 0;
    this.#taking?.reject(error);
    this.#taking = undefined;
    for (const { reject } of this.#writing.splice(0)) reject(error);
  }
}
