import {
  type ArtifactHashers,
  type ReceivingWorkspace,
  type StoredCopy,
  StoredRowsError,
} from "@calque/worker/columnar";
import { describe, expect, it } from "vitest";

import {
  type HandedChunks,
  STORED_READ_AHEAD,
  sendStoredArtifact,
} from "./artifact-download.js";

const CHUNKS = STORED_READ_AHEAD + 3;

/** A stored copy whose reads the test settles, chunk by chunk. */
function heldCopy(): {
  readonly copy: StoredCopy;
  readonly asked: number[];
  readonly reads: Map<number, PromiseWithResolvers<ArrayBuffer>>;
} {
  const asked: number[] = [];
  const reads = new Map<number, PromiseWithResolvers<ArrayBuffer>>();
  return {
    asked,
    reads,
    copy: {
      chunk: (index) => {
        asked.push(index);
        const read = Promise.withResolvers<ArrayBuffer>();
        reads.set(index, read);
        return read.promise;
      },
      release: () => undefined,
    },
  };
}

function recordingHashers(): {
  readonly hashers: ArtifactHashers;
  readonly sent: number[];
} {
  const sent: number[] = [];
  const hashers = {
    send: (index: number, bytes: ArrayBuffer): Promise<void> => {
      expect(new Uint8Array(bytes)[0]).toBe(index);
      sent.push(index);
      return Promise.resolve();
    },
  } as unknown as ArtifactHashers;
  return { hashers, sent };
}

function recordingReceiver(): {
  readonly receiving: ReceivingWorkspace;
  readonly ended: number[];
} {
  const ended: number[] = [];
  const receiving = {
    end: (chunks: number) => ended.push(chunks),
    opened: new Promise(() => undefined),
  } as unknown as ReceivingWorkspace;
  return { receiving, ended };
}

function chunkBytes(index: number): ArrayBuffer {
  return new Uint8Array([index]).buffer;
}

async function turns(count = 8): Promise<void> {
  for (let turn = 0; turn < count; turn += 1) await Promise.resolve();
}

describe("sendStoredArtifact", () => {
  it("reads chunks ahead of the hashers and hands them over in order", async () => {
    const { copy, asked, reads } = heldCopy();
    const { hashers, sent } = recordingHashers();
    const { receiving, ended } = recordingReceiver();
    const handed: HandedChunks = { count: 0, last: undefined };
    const done = sendStoredArtifact(copy, CHUNKS, hashers, receiving, handed);

    expect(asked).toEqual([0, 1, 2, 3]);
    reads.get(2)?.resolve(chunkBytes(2));
    reads.get(1)?.resolve(chunkBytes(1));
    await turns();
    // Chunk 0 has not opened, so nothing goes ahead of it.
    expect(sent).toEqual([]);
    expect(asked).toEqual([0, 1, 2, 3]);

    reads.get(0)?.resolve(chunkBytes(0));
    await turns();
    expect(sent).toEqual([0, 1, 2]);
    expect(asked).toEqual([0, 1, 2, 3, 4, 5, 6]);

    for (let index = 3; index < CHUNKS; index += 1)
      reads.get(index)?.resolve(chunkBytes(index));
    await done;
    expect(sent).toEqual([...Array(CHUNKS).keys()]);
    expect(asked).toEqual([...Array(CHUNKS).keys()]);
    expect(handed.count).toBe(CHUNKS);
    expect(ended).toEqual([CHUNKS]);
  });

  it("stops at a chunk that does not open, with only the chunks before it handed over", async () => {
    const { copy, reads } = heldCopy();
    const { hashers, sent } = recordingHashers();
    const { receiving, ended } = recordingReceiver();
    const handed: HandedChunks = { count: 0, last: undefined };
    const done = sendStoredArtifact(copy, CHUNKS, hashers, receiving, handed);

    const broken = new StoredRowsError("a stored chunk does not open");
    reads.get(3)?.resolve(chunkBytes(3));
    reads.get(2)?.reject(broken);
    reads.get(1)?.resolve(chunkBytes(1));
    reads.get(0)?.resolve(chunkBytes(0));
    await expect(done).rejects.toBe(broken);
    // A read already in flight that fails later is no unhandled rejection.
    for (const [index, read] of reads)
      if (index > 3) read.reject(new StoredRowsError("late"));
    await turns();

    expect(sent).toEqual([0, 1]);
    expect(handed.count).toBe(2);
    expect(ended).toEqual([]);
  });
});
