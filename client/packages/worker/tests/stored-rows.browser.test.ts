/**
 * The copy of an artifact a page keeps between visits, in the browser's own
 * origin private file system, with the real store worker
 * (`columnar-store-entry.ts`). The test stands in for the engine worker: it
 * sends the store worker each chunk on the writer's port, as the engine
 * worker does once it has accepted the chunk.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  STORED_ROWS_DIRECTORY,
  STORED_ROWS_RECORD,
  StoredRowsError,
  StoredRowsWriter,
  eraseStoredRows,
  importStoredRowsKey,
  openChunk,
  openStoredCopy,
  sealChunk,
  sealedChunkOffset,
  storedRowsAvailable,
  type StoreWorker,
  type StoredArtifact,
  type StoredRowsKey,
} from "../src/stored-rows.js";
import type { StoredRowsChunk } from "../src/stored-rows-protocol.js";

const CHUNK_BYTES = 4;
/** Three chunks: two whole ones and a short last one. */
const BODY = Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

let workers: Worker[] = [];

function storeWorker(): StoreWorker {
  const worker = new Worker(
    new URL("../src/columnar-store-entry.ts", import.meta.url),
    { type: "module" },
  );
  workers.push(worker);
  return worker;
}

function base64url(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replace(/=+$/u, "");
}

async function newKey(): Promise<StoredRowsKey> {
  return importStoredRowsKey(
    base64url(crypto.getRandomValues(new Uint8Array(32))),
  );
}

function hex(digest: ArrayBuffer): string {
  return Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}

function chunksOf(body: Uint8Array): Uint8Array<ArrayBuffer>[] {
  const chunks: Uint8Array<ArrayBuffer>[] = [];
  for (let start = 0; start < body.byteLength; start += CHUNK_BYTES)
    chunks.push(body.slice(start, start + CHUNK_BYTES));
  return chunks;
}

async function artifactOf(
  body: Uint8Array<ArrayBuffer>,
  artifactId = "artifact-a",
): Promise<StoredArtifact> {
  const chunkSha256 = await Promise.all(
    chunksOf(body).map(
      async (chunk) =>
        `sha256:${hex(await crypto.subtle.digest("SHA-256", chunk))}`,
    ),
  );
  return {
    artifactId,
    decodedSha256: hex(await crypto.subtle.digest("SHA-256", body)),
    decodedBytes: body.byteLength,
    chunkBytes: CHUNK_BYTES,
    chunkSha256,
  };
}

/** Sends the store worker `body`'s chunks, as the engine worker would. */
function sendChunks(
  writer: StoredRowsWriter,
  body: Uint8Array,
  indices?: readonly number[],
): void {
  const chunks = chunksOf(body);
  for (const index of indices ?? chunks.keys()) {
    const bytes = (chunks[index] as Uint8Array).slice().buffer;
    writer.port.postMessage({ index, bytes } satisfies StoredRowsChunk, [
      bytes,
    ]);
  }
}

async function storeBody(
  key: StoredRowsKey,
  artifact: StoredArtifact,
  body: Uint8Array,
): Promise<void> {
  const writer = new StoredRowsWriter(storeWorker, key, artifact);
  sendChunks(writer, body);
  expect(await writer.commit()).toEqual({ type: "committed" });
}

async function readBody(
  key: StoredRowsKey,
  artifact: StoredArtifact,
): Promise<Uint8Array | undefined> {
  const copy = await openStoredCopy(key, artifact);
  if (!copy) return undefined;
  try {
    const body = new Uint8Array(artifact.decodedBytes);
    for (let index = 0; index < artifact.chunkSha256.length; index += 1)
      body.set(new Uint8Array(await copy.chunk(index)), index * CHUNK_BYTES);
    return body;
  } finally {
    copy.release();
  }
}

async function storeFiles(): Promise<string[]> {
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(STORED_ROWS_DIRECTORY, {
    create: true,
  });
  const names: string[] = [];
  for await (const name of directory.keys()) names.push(name);
  return names.sort();
}

async function overwrite(
  name: string,
  at: number,
  bytes: Uint8Array<ArrayBuffer>,
) {
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(STORED_ROWS_DIRECTORY);
  const file = await directory.getFileHandle(name);
  const writable = await file.createWritable({ keepExistingData: true });
  await writable.write({ type: "write", position: at, data: bytes });
  await writable.close();
}

async function recordedDataFile(): Promise<string> {
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(STORED_ROWS_DIRECTORY);
  const record = JSON.parse(
    await (
      await (await directory.getFileHandle(STORED_ROWS_RECORD)).getFile()
    ).text(),
  ) as { dataFile: string };
  return record.dataFile;
}

beforeEach(async () => {
  const root = await navigator.storage.getDirectory();
  await root
    .removeEntry(STORED_ROWS_DIRECTORY, { recursive: true })
    .catch(() => undefined);
});

afterEach(() => {
  for (const worker of workers.splice(0)) worker.terminate();
  workers = [];
});

describe("a sealed chunk", () => {
  it("opens only at its own index, in its own artifact", async () => {
    const { key } = await newKey();
    // Chunks 0 and 1 are the same bytes, so they have the same digest, and
    // only the index tells them apart; `other` shares chunk 0 with it.
    const body = Uint8Array.from([1, 2, 3, 4, 1, 2, 3, 4, 9, 10]);
    const artifact = await artifactOf(body);
    expect(artifact.chunkSha256[1]).toBe(artifact.chunkSha256[0]);
    const plain = chunksOf(body)[0] as Uint8Array;
    const { iv, sealed } = await sealChunk(
      key,
      artifact,
      0,
      plain.slice().buffer,
    );
    const stored = new Uint8Array(iv.byteLength + sealed.byteLength);
    stored.set(iv);
    stored.set(sealed, iv.byteLength);

    expect(
      new Uint8Array(await openChunk(key, artifact, 0, stored.slice().buffer)),
    ).toEqual(plain);
    // Chunk 1 is as long as chunk 0, so only the binding refuses it.
    await expect(
      openChunk(key, artifact, 1, stored.slice().buffer),
    ).rejects.toBeInstanceOf(StoredRowsError);
    const other = await artifactOf(
      Uint8Array.from([1, 2, 3, 4, 1, 2, 3, 4, 9, 11]),
    );
    expect(other.chunkSha256[0]).toBe(artifact.chunkSha256[0]);
    await expect(
      openChunk(key, other, 0, stored.slice().buffer),
    ).rejects.toBeInstanceOf(StoredRowsError);
    const { key: otherKey } = await newKey();
    await expect(
      openChunk(otherKey, artifact, 0, stored.slice().buffer),
    ).rejects.toBeInstanceOf(StoredRowsError);
  });

  it("names its key by an id that is not the key", async () => {
    const raw = crypto.getRandomValues(new Uint8Array(32));
    const first = await importStoredRowsKey(base64url(raw));
    const again = await importStoredRowsKey(base64url(raw));
    const other = await newKey();
    expect(first.keyId).toMatch(/^[0-9a-f]{32}$/u);
    expect(first.keyId).toBe(again.keyId);
    expect(other.keyId).not.toBe(first.keyId);
    expect(first.keyId).not.toContain(hex(raw.buffer).slice(0, 32));
    await expect(importStoredRowsKey("c2hvcnQ")).rejects.toBeInstanceOf(
      StoredRowsError,
    );
  });
});

describe("the stored copy", () => {
  it("is available in this browser", () => {
    expect(storedRowsAvailable()).toBe(true);
  });

  it("reads back what a committed store worker kept, encrypted", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    await storeBody(key, artifact, BODY);

    expect(await readBody(key, artifact)).toEqual(BODY);
    const files = await storeFiles();
    expect(files).toContain(STORED_ROWS_RECORD);
    const dataFile = await recordedDataFile();
    const root = await navigator.storage.getDirectory();
    const sealed = new Uint8Array(
      await (
        await (
          await (
            await root.getDirectoryHandle(STORED_ROWS_DIRECTORY)
          ).getFileHandle(dataFile)
        ).getFile()
      ).arrayBuffer(),
    );
    // Each chunk is its IV, its ciphertext and its tag.
    expect(sealed.byteLength).toBe(BODY.byteLength + 3 * 28);
    const first = sealed.subarray(12, 12 + CHUNK_BYTES);
    expect(first).not.toEqual(BODY.subarray(0, CHUNK_BYTES));
  });

  it("keeps chunks that arrive in any order and after the commit", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    const writer = new StoredRowsWriter(storeWorker, key, artifact);
    sendChunks(writer, BODY, [2, 0]);
    const committed = writer.commit();
    sendChunks(writer, BODY, [1]);
    expect(await committed).toEqual({ type: "committed" });
    expect(await readBody(key, artifact)).toEqual(BODY);
  });

  it("is not read for another artifact or under another key", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    await storeBody(key, artifact, BODY);

    expect(await openStoredCopy(await newKey(), artifact)).toBeUndefined();
    expect(
      await openStoredCopy(key, { ...artifact, artifactId: "artifact-b" }),
    ).toBeUndefined();
    expect(
      await openStoredCopy(key, {
        ...artifact,
        chunkSha256: [
          ...artifact.chunkSha256.slice(0, 2),
          `sha256:${"0".repeat(64)}`,
        ],
      }),
    ).toBeUndefined();
    expect(
      await openStoredCopy(key, await artifactOf(BODY.slice(0, 9))),
    ).toBeUndefined();
    expect(await readBody(key, artifact)).toEqual(BODY);
  });

  it("does not exist until the page commits it", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    const writer = new StoredRowsWriter(storeWorker, key, artifact);
    sendChunks(writer, BODY);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(await openStoredCopy(key, artifact)).toBeUndefined();

    writer.abandon();
    expect(await writer.outcome).toMatchObject({ type: "abandoned" });
    expect(await openStoredCopy(key, artifact)).toBeUndefined();
    expect(await storeFiles()).toEqual([]);
  });

  it("is not kept by a store worker that began before an erasure", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    const writer = new StoredRowsWriter(storeWorker, key, artifact);
    sendChunks(writer, BODY);
    await new Promise((resolve) => setTimeout(resolve, 200));
    await eraseStoredRows();

    expect(await writer.commit()).toEqual({
      type: "abandoned",
      reason: "the store was erased",
    });
    expect(await openStoredCopy(key, artifact)).toBeUndefined();
    // A store worker that begins after the erasure keeps its copy.
    await storeBody(key, artifact, BODY);
    expect(await readBody(key, artifact)).toEqual(BODY);
  });

  it("is not kept by a store worker that began after the page activated and erased", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    // A store worker slow to start: the page's messages reach it once
    // `started` settles, in the order the page sent them.
    let start!: () => void;
    const started = new Promise<void>((resolve) => {
      start = resolve;
    });
    let delivered = started;
    const slow = (): StoreWorker => {
      const worker = storeWorker();
      return {
        addEventListener: (type, listener) => {
          worker.addEventListener(type, listener);
        },
        terminate: () => worker.terminate(),
        postMessage: (message, transfer) => {
          delivered = delivered.then(() => {
            worker.postMessage(message, transfer ?? []);
          });
        },
      };
    };
    const writer = new StoredRowsWriter(slow, key, artifact);
    sendChunks(writer, BODY);
    const outcome = writer.commit();
    // clearStoredRows: the load's writer is abandoned, then the store erased.
    writer.abandon();
    await eraseStoredRows();
    start();

    expect(await outcome).toMatchObject({ type: "abandoned" });
    expect(await openStoredCopy(key, artifact)).toBeUndefined();
  });

  it("is erased, with every file it wrote", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    await storeBody(key, artifact, BODY);
    await eraseStoredRows();

    expect(await openStoredCopy(key, artifact)).toBeUndefined();
    const files = await storeFiles();
    expect(files).toHaveLength(1);
    expect(files[0]).toMatch(/^epoch-/u);
  });

  it("replaces the one before it and deletes its data", async () => {
    const key = await newKey();
    const first = await artifactOf(BODY);
    await storeBody(key, first, BODY);
    const firstData = await recordedDataFile();
    const body = Uint8Array.from(BODY, (byte) => byte * 2);
    const second = await artifactOf(body, "artifact-b");
    await storeBody(key, second, body);

    expect(await openStoredCopy(key, first)).toBeUndefined();
    expect(await readBody(key, second)).toEqual(body);
    expect(await storeFiles()).not.toContain(firstData);
  });

  it("refuses a chunk whose stored bytes changed", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    await storeBody(key, artifact, BODY);
    const dataFile = await recordedDataFile();
    await overwrite(
      dataFile,
      sealedChunkOffset(CHUNK_BYTES, 1) + 12,
      Uint8Array.of(0xff),
    );

    const copy = await openStoredCopy(key, artifact);
    expect(copy).toBeDefined();
    try {
      await expect(copy?.chunk(0)).resolves.toBeInstanceOf(ArrayBuffer);
      await expect(copy?.chunk(1)).rejects.toBeInstanceOf(StoredRowsError);
    } finally {
      copy?.release();
    }
  });

  it("refuses a copy whose data file is gone or short", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    await storeBody(key, artifact, BODY);
    const dataFile = await recordedDataFile();
    const root = await navigator.storage.getDirectory();
    const directory = await root.getDirectoryHandle(STORED_ROWS_DIRECTORY);
    const writable = await (
      await directory.getFileHandle(dataFile)
    ).createWritable({ keepExistingData: true });
    await writable.truncate(20);
    await writable.close();
    await expect(openStoredCopy(key, artifact)).rejects.toBeInstanceOf(
      StoredRowsError,
    );

    await directory.removeEntry(dataFile);
    await expect(openStoredCopy(key, artifact)).rejects.toBeInstanceOf(
      StoredRowsError,
    );
  });

  it("holds an erasure back until the page has read it", async () => {
    const key = await newKey();
    const artifact = await artifactOf(BODY);
    await storeBody(key, artifact, BODY);
    const copy = await openStoredCopy(key, artifact);
    let erased = false;
    const erasing = eraseStoredRows().then(() => {
      erased = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(erased).toBe(false);
    expect(
      new Uint8Array((await copy?.chunk(2)) ?? new ArrayBuffer(0)),
    ).toEqual(BODY.subarray(8));
    copy?.release();
    await erasing;
    expect(await openStoredCopy(key, artifact)).toBeUndefined();
  });
});
