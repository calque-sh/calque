import {
  ArtifactBodyError,
  type ArtifactDecoder,
  type ArtifactHashers,
  awaitedLater,
  type ReceivingWorkspace,
} from "@calque/worker/columnar";

import { SyncError } from "../error.js";
import { exactResponseBuffers, exactResponsePieces } from "../transport.js";
import { mark } from "./user-timing.js";

/**
 * Sends a downloading artifact to the hashers, one transferred `chunkBytes`
 * buffer at a time, and ends once the response has delivered exactly
 * `expectedBytes`. The chunks in `handed` came from a response the network
 * cut and are skipped, since a retried request starts again at the first byte.
 */
export async function sendArtifact(
  response: Response,
  expectedBytes: number,
  chunkBytes: number,
  hashers: ArtifactHashers,
  receiving: ReceivingWorkspace,
  handed: HandedChunks,
): Promise<void> {
  const chunks = Math.ceil(expectedBytes / chunkBytes);
  let index = 0;
  for await (const buffer of exactResponseBuffers(
    response,
    expectedBytes,
    chunkBytes,
  )) {
    if (index < handed.count) {
      index += 1;
      continue;
    }
    const sent = hashers.send(index, buffer);
    index += 1;
    handed.count = index;
    if (index < chunks) await sent;
    else handed.last = awaitedLater(sent);
  }
  mark("artifact:downloaded");
  await handed.last;
  receiving.end(index);
}

/**
 * `sendArtifact` for a `brotli-large-window` or `packed-brotli-large-window`
 * body: the page passes the body to `decoder` as it reads it and sends the
 * hashers the chunks the decoder returns. The decoder returns its last chunk
 * only once the body ended with exactly `expectedBytes` decoded. It serves one
 * body and is closed on every way out; a body the network cut is fetched
 * again with a new decoder told to skip the chunks in `handed`.
 */
export async function sendDecodedArtifact(
  response: Response,
  encoding: DecodedEncoding,
  encodedBytes: number,
  expectedBytes: number,
  chunkBytes: number,
  decoder: ArtifactDecoder,
  hashers: ArtifactHashers,
  receiving: ReceivingWorkspace,
  handed: HandedChunks,
): Promise<void> {
  const chunks = Math.ceil(expectedBytes / chunkBytes);
  let body: ReturnType<typeof exactResponsePieces>;
  try {
    body = exactResponsePieces(response, encodedBytes);
  } catch (error) {
    decoder.close();
    throw error;
  }
  let failure: { readonly error: unknown } | undefined;
  // The first of the two loops below to fail ends the other wherever it
  // waits: on the decoder, which rejects once closed, or on the body, which
  // ends once cancelled.
  const run = async (loop: () => Promise<void>): Promise<void> => {
    try {
      await loop();
    } catch (error) {
      if (failure) return;
      failure = { error };
      decoder.close();
      void body.cancel();
    }
  };
  const reading = run(async () => {
    for (;;) {
      const piece = await body.read();
      if (failure) return;
      if (!piece) break;
      decoder.write(piece.bytes, piece.end);
      if (!piece.end) await decoder.room();
    }
    mark("artifact:downloaded");
  });
  const handing = run(async () => {
    while (handed.count < chunks) {
      const bytes = await decoder.next();
      if (failure) return;
      const sent = hashers.send(handed.count, bytes);
      handed.count += 1;
      if (handed.count < chunks) {
        await sent;
        decoder.credit();
      } else {
        mark("artifact:decoded");
        handed.last = awaitedLater(sent);
      }
    }
  });
  await Promise.all([reading, handing]);
  decoder.close();
  if (failure) throw artifactBodyRefusal(failure.error, encoding);
  await handed.last;
  receiving.end(chunks);
}

/** The encodings whose body the decoder worker turns into the artifact. */
export type DecodedEncoding =
  "brotli-large-window" | "packed-brotli-large-window";

/**
 * Reports a body the decoder refused as `INVALID_RESPONSE`, like a body of the
 * wrong length. Any other error is returned as it is.
 */
export function artifactBodyRefusal(
  error: unknown,
  encoding: DecodedEncoding,
): unknown {
  return error instanceof ArtifactBodyError
    ? new SyncError(
        "INVALID_RESPONSE",
        "sync",
        "reset",
        `artifact response is not its plan's ${encoding} body: ${error.message}`,
        { cause: error },
      )
    : error;
}

/**
 * The chunks one load has handed to the hashers, across every response it
 * read them from. The worker checks each chunk against the plan's digest as
 * it arrives, so a chunk handed over once is never needed again.
 */
export interface HandedChunks {
  /** How many, counted from the first. */
  count: number;
  /** The last chunk's hand-over, once it has been made. */
  last: Promise<void> | undefined;
}
