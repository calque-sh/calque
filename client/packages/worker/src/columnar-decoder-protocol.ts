/**
 * The messages between the page and the decoder worker, which turns an
 * artifact that arrives as a `brotli-large-window` body, with no
 * `Content-Encoding`, into the decoded artifact's chunks. The stream is the
 * artifact's or, under `packed-brotli-large-window`, its packed body's. One
 * worker decodes one response body and is then terminated; a body the page
 * asks for again gets a worker of its own.
 *
 * Both directions are bounded. The page sends the body as the network
 * delivers it, and stops reading it while `DECODER_INPUT_WINDOW_BYTES` are
 * with the worker unread. The worker sends a chunk only against a credit, and
 * the page returns the credit once a hasher has taken the chunk.
 */

/** The most bytes of the body the page leaves with the worker unread. */
export const DECODER_INPUT_WINDOW_BYTES = 4 * 1024 * 1024;

/**
 * How many bytes the worker reads between two `taken` messages. A quarter of
 * the window, so the page never waits on a worker that has read all it was
 * sent.
 */
export const DECODER_TAKEN_BYTES = DECODER_INPUT_WINDOW_BYTES / 4;

/** The page's messages to the decoder worker: `start` first, once. */
export type DecoderRequest =
  | {
      readonly type: "start";
      /** The artifact's decoded length; the worker emits no byte past it. */
      readonly decodedBytes: number;
      /** The length of every chunk but the last. */
      readonly chunkBytes: number;
      /**
       * How many chunks, from the first, an earlier response already
       * delivered. The worker decodes them and sends none of them.
       */
      readonly skipChunks: number;
      /**
       * Whether the stream decodes to a packed body, which the worker
       * unpacks to the artifact, instead of to the artifact itself: true
       * for a `packed-brotli-large-window` representation.
       */
      readonly packed: boolean;
    }
  | {
      readonly type: "bytes";
      /** The body's next bytes, transferred. */
      readonly bytes: ArrayBuffer;
      /** Whether these are the last bytes of the body. */
      readonly end: boolean;
    }
  /** A hasher took a chunk, so the worker may send one more. */
  | { readonly type: "credit" };

/** The decoder worker's messages to the page. */
export type DecoderResponse =
  | {
      readonly type: "chunk";
      readonly index: number;
      /**
       * Exactly `chunkBytes` of the decoded artifact, or what is left of it
       * in the last chunk; transferred. The last chunk is sent only once the
       * stream has ended exactly where the body and the artifact do.
       */
      readonly bytes: ArrayBuffer;
    }
  /** The worker has read `bytes` more of what it was sent. */
  | { readonly type: "taken"; readonly bytes: number }
  | {
      readonly type: "failed";
      /**
       * True when the body is not the artifact's encoding: the stream is
       * malformed, ends before or after the body does, holds more or fewer
       * bytes than the artifact, declares a larger window than the artifact
       * needs, or holds a packed body the unpacker refuses. False when the
       * worker itself failed, as when its module did not load.
       */
      readonly corrupt: boolean;
      readonly message: string;
    };
