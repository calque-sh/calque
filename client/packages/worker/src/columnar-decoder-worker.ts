/**
 * The decoder worker's behaviour, apart from where its module comes from: it
 * decodes one `brotli-large-window` body with brotli's reference decoder
 * (`wasm/brotli-decoder.wasm`) and sends the artifact back in the plan's chunk
 * sizes. When `start` says so, the stream decodes to a packed body, from which
 * `columnar-unpacker.ts` restores the artifact. Decoded bytes are copied once,
 * from the decoder's window into the outgoing chunk, except those a packed
 * body's streams are rebuilt from.
 *
 * The body is refused, and no further chunk sent, when the stream is
 * malformed, ends before or after the body does, decodes to more or fewer
 * bytes than the artifact, or decodes to a packed body the unpacker refuses.
 * The last chunk is sent only after all of that has held, so a page holding
 * every chunk has a body that decoded exactly.
 */
import {
  DECODER_TAKEN_BYTES,
  type DecoderRequest,
  type DecoderResponse,
} from "./columnar-decoder-protocol.js";
import {
  type ArtifactUnpacker,
  artifactUnpacker,
} from "./columnar-unpacker.js";

/** A dedicated worker's global scope, as much of it as the decoder uses. */
export interface DecoderScope {
  onmessage: ((event: MessageEvent<DecoderRequest>) => void) | null;
  postMessage(message: DecoderResponse, transfer: Transferable[]): void;
}

/** What `wasm/brotli-decoder.c` exports. */
interface DecoderModule {
  readonly memory: WebAssembly.Memory;
  readonly _initialize?: () => void;
  dec_begin(windowBytes: number): number;
  dec_input(): number;
  dec_input_capacity(): number;
  dec_feed(length: number): void;
  dec_pull(most: number): number;
  dec_output(): number;
  dec_output_length(): number;
}

const PULL_NEEDS_INPUT = 0;
const PULL_OUTPUT = 1;
const PULL_END = 2;
const PULL_INPUT_AFTER_END = -100;
const PULL_OVER_LIMIT = -101;
/** brotli's own codes for an allocation it could not make. */
const BROTLI_ALLOCATION_FAILURES = { first: -30, last: -21 };

/** The smallest and largest windows a large-window brotli stream declares. */
const MIN_WINDOW_BITS = 10;
const MAX_WINDOW_BITS = 30;

/**
 * The largest window a stream for an artifact of `decodedBytes` may declare:
 * the smallest that covers the artifact. A larger one buys the encoder
 * nothing, so a stream that declares it is not one a server made for this
 * artifact, and the decoder would allocate all of it.
 */
export function decoderWindowBytes(decodedBytes: number): number {
  let bits = MIN_WINDOW_BITS;
  while (bits < MAX_WINDOW_BITS && 2 ** bits < decodedBytes) bits += 1;
  return 2 ** bits;
}

/** A user-timing mark on the cold path, for a trace or a harness to read. */
function mark(name: string): void {
  globalThis.performance?.mark?.(`calque:${name}`);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Serves one body on `scope` with the module `instantiate` loads. The module
 * may arrive after the body's first bytes, or after all of them: they wait
 * here, in order, until it has.
 */
export function serveArtifactDecoder(
  scope: DecoderScope,
  instantiate: () => Promise<WebAssembly.Instance>,
): void {
  mark("decoder:evaluated");
  let decoder: DecoderModule | undefined;
  let plan:
    | {
        readonly decodedBytes: number;
        readonly chunkBytes: number;
        readonly skipChunks: number;
        readonly chunks: number;
      }
    | undefined;
  let begun = false;
  /** Set once the body is refused or its last chunk has gone. */
  let stopped = false;
  /** The body's bytes the decoder has not read, in order. */
  const pieces: Uint8Array[] = [];
  /** How much of `pieces[0]` the decoder has read. */
  let read = 0;
  /** Whether the body's last bytes have arrived. */
  let ended = false;
  let credits = 1;
  /** Restores the artifact from what the stream decodes to. */
  let unpacker: ArtifactUnpacker | undefined;
  /** The next chunk to send, or to pass over when it is one to skip. */
  let index = 0;
  /** Whether the stream, the body and what it decodes to ended together. */
  let exact = false;
  /** Bytes read since the last `taken`. */
  let untold = 0;
  let first = true;

  const fail = (corrupt: boolean, message: string): void => {
    if (stopped) return;
    stopped = true;
    pieces.length = 0;
    unpacker = undefined;
    scope.postMessage({ type: "failed", corrupt, message }, []);
  };

  /**
   * Gives the decoder the next bytes it has not read. False when there are
   * none: the body has ended short of its stream, which is refused, or the
   * rest of it has not arrived.
   */
  const feed = (module: DecoderModule): boolean => {
    const piece = pieces[0];
    if (!piece) {
      if (ended) fail(true, "the body ends before its brotli stream does");
      return false;
    }
    const length = Math.min(
      piece.byteLength - read,
      module.dec_input_capacity(),
    );
    // A grown memory has a new buffer, so the view is made for each piece.
    new Uint8Array(module.memory.buffer, module.dec_input(), length).set(
      piece.subarray(read, read + length),
    );
    module.dec_feed(length);
    read += length;
    if (read === piece.byteLength) {
      pieces.shift();
      read = 0;
    }
    untold += length;
    if (untold >= DECODER_TAKEN_BYTES) {
      scope.postMessage({ type: "taken", bytes: untold }, []);
      untold = 0;
    }
    return true;
  };

  const refuse = (status: number): void => {
    if (status === PULL_INPUT_AFTER_END)
      fail(true, "the body continues after its brotli stream ends");
    else if (status === PULL_OVER_LIMIT)
      fail(
        true,
        "the brotli stream declares a larger window than its artifact needs",
      );
    else if (
      status >= BROTLI_ALLOCATION_FAILURES.first &&
      status <= BROTLI_ALLOCATION_FAILURES.last
    )
      fail(false, "the decoder could not allocate its window");
    else
      fail(true, `the body is not a brotli stream (decoder error ${status})`);
  };

  /** Decodes as far as the bytes and the credits it holds allow. */
  const pump = (): void => {
    if (!decoder || !plan || !unpacker || stopped) return;
    if (!begun) {
      begun = true;
      decoder._initialize?.();
      if (decoder.dec_begin(decoderWindowBytes(plan.decodedBytes)) !== 0) {
        fail(false, "the decoder could not start");
        return;
      }
    }
    for (;;) {
      const last = index === plan.chunks - 1;
      // The last chunk goes only once the stream has ended, on the body's
      // last byte and on the last byte of what it decodes to.
      if (
        unpacker.ready() >=
          Math.min((index + 1) * plan.chunkBytes, plan.decodedBytes) &&
        (exact || !last)
      ) {
        if (index >= plan.skipChunks) {
          if (credits === 0) return;
          credits -= 1;
          const bytes = unpacker.take(index).buffer;
          scope.postMessage({ type: "chunk", index, bytes }, [bytes]);
        }
        if (last) {
          stopped = true;
          mark("decoder:last-chunk");
          return;
        }
        index += 1;
        continue;
      }
      // One byte, once every byte of the artifact is out: the stream has to
      // end here.
      const status = decoder.dec_pull(unpacker.room() || 1);
      if (status === PULL_OUTPUT) {
        // A grown memory has a new buffer, so the view is made for each pull.
        const refusal = unpacker.write(
          new Uint8Array(
            decoder.memory.buffer,
            decoder.dec_output(),
            decoder.dec_output_length(),
          ),
        );
        if (refusal !== undefined) {
          fail(true, refusal);
          return;
        }
      } else if (status === PULL_NEEDS_INPUT) {
        if (!feed(decoder)) return;
      } else if (status === PULL_END) {
        const refusal = unpacker.end();
        if (refusal !== undefined) {
          fail(true, refusal);
          return;
        }
        if (!ended || pieces.length > 0) {
          refuse(PULL_INPUT_AFTER_END);
          return;
        }
        exact = true;
      } else {
        refuse(status);
        return;
      }
    }
  };

  /** `pump`, with anything it throws reported as the worker's own failure. */
  const run = (): void => {
    try {
      pump();
    } catch (error) {
      fail(false, `the decoder failed: ${messageOf(error)}`);
    }
  };

  scope.onmessage = (event) => {
    const request = event.data;
    if (stopped) return;
    if (request.type === "start") {
      plan = {
        decodedBytes: request.decodedBytes,
        chunkBytes: request.chunkBytes,
        skipChunks: request.skipChunks,
        chunks: Math.ceil(request.decodedBytes / request.chunkBytes),
      };
      unpacker = artifactUnpacker(
        request.decodedBytes,
        request.chunkBytes,
        request.skipChunks,
        request.packed,
      );
    } else if (request.type === "bytes") {
      if (first) {
        first = false;
        mark("decoder:first-bytes");
      }
      if (request.bytes.byteLength > 0)
        pieces.push(new Uint8Array(request.bytes));
      if (request.end) ended = true;
    } else credits += 1;
    run();
  };

  instantiate().then(
    (instance) => {
      decoder = instance.exports as unknown as DecoderModule;
      mark("decoder:module");
      run();
    },
    (error: unknown) => {
      fail(false, `the decoder module did not load: ${messageOf(error)}`);
    },
  );
}
