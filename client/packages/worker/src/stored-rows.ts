/**
 * The artifact a page keeps between visits, in the origin private file
 * system. Each chunk the engine worker accepted is sealed with AES-256-GCM
 * under the key the plan carries, which is never written to disk, so the
 * copy opens only for a page that holds a plan of the same scope.
 *
 * `calque-stored-rows-v1/` holds one stored working set per origin:
 * `record.json`, which names the artifact, its key id and its data file; the
 * data file, chunk `i` at `i * (chunkBytes + 28)` as a 12-byte IV followed by
 * the ciphertext and its 16-byte tag; and `epoch-*` files. Erasing adds an
 * epoch file before it deletes anything, and a store worker whose epochs
 * changed since it began writes no record, so a copy that was being written
 * when the application signed out never lands.
 *
 * A read takes `LOCK` shared, and erasing and a store worker's commit take it
 * exclusively.
 */
import { awaitedLater } from "./async.js";
import type { StoreOutcome, StoreRequest } from "./stored-rows-protocol.js";

export const STORED_ROWS_DIRECTORY = "calque-stored-rows-v1";
export const STORED_ROWS_LOCK = "calque-stored-rows-v1";
export const STORED_ROWS_RECORD = "record.json";
const EPOCH_PREFIX = "epoch-";
export const DATA_SUFFIX = ".chunks";
const IV_BYTES = 12;
const TAG_BYTES = 16;
/** What sealing adds to each chunk: its IV and its tag. */
export const SEALED_CHUNK_EXTRA_BYTES = IV_BYTES + TAG_BYTES;
const KEY_BYTES = 32;
const KEY_ID_LABEL = "calque.stored-rows.key-id.v1\0";
const SEAL_LABEL = "calque.stored-rows.v1\0";
const RECORD_VERSION = 1;
const SHA256_ID = /^sha256:[0-9a-f]{64}$/u;
const SHA256 = /^[0-9a-f]{64}$/u;
const KEY_ID = /^[0-9a-f]{32}$/u;
const DATA_FILE = /^[0-9a-f-]{36}\.chunks$/u;
/** How long the store worker may wait for the engine's last chunk. */
const CHUNKS_DUE_MS = 10_000;

/**
 * The artifact a stored copy holds, as the plan names it. A copy is read
 * only for a plan whose artifact equals it in every field.
 */
export interface StoredArtifact {
  readonly artifactId: string;
  /** The decoded artifact's SHA-256, lowercase hex. */
  readonly decodedSha256: string;
  readonly decodedBytes: number;
  readonly chunkBytes: number;
  /** Each chunk's digest from the plan, `sha256:` and lowercase hex. */
  readonly chunkSha256: readonly string[];
}

/** The plan's key, imported, and the id the record names it by. */
export interface StoredRowsKey {
  readonly key: CryptoKey;
  readonly keyId: string;
}

interface StoredRecord extends StoredArtifact {
  readonly version: typeof RECORD_VERSION;
  readonly keyId: string;
  readonly dataFile: string;
}

/** A stored copy that failed to read; the page erases it. */
export class StoredRowsError extends Error {
  override readonly name = "StoredRowsError";
}

/**
 * Whether this context can keep a copy: the origin private file system,
 * Web Locks and WebCrypto. A store worker also needs sync access handles,
 * which only a dedicated worker can see; one without them abandons its copy.
 */
export function storedRowsAvailable(): boolean {
  const scope = globalThis as {
    readonly navigator?: Partial<Pick<Navigator, "storage" | "locks">>;
    readonly crypto?: Partial<Crypto>;
  };
  return (
    typeof scope.navigator?.storage?.getDirectory === "function" &&
    typeof scope.navigator.locks?.request === "function" &&
    typeof scope.crypto?.subtle?.importKey === "function"
  );
}

/** Imports the plan's `storedRowsKey`: 32 bytes in unpadded base64url. */
export async function importStoredRowsKey(
  encoded: string,
): Promise<StoredRowsKey> {
  const raw = base64urlBytes(encoded);
  if (raw.byteLength !== KEY_BYTES)
    throw new StoredRowsError("the stored-rows key is not 32 bytes");
  const key = await crypto.subtle.importKey("raw", raw, "AES-GCM", false, [
    "encrypt",
    "decrypt",
  ]);
  const label = new TextEncoder().encode(KEY_ID_LABEL);
  const named = new Uint8Array(label.byteLength + raw.byteLength);
  named.set(label);
  named.set(raw, label.byteLength);
  const digest = await crypto.subtle.digest("SHA-256", named);
  return { key, keyId: hex(digest).slice(0, 32) };
}

/** Where chunk `index` starts in the data file. */
export function sealedChunkOffset(chunkBytes: number, index: number): number {
  return index * (chunkBytes + SEALED_CHUNK_EXTRA_BYTES);
}

/** The bytes of chunk `index`, sealed. */
function sealedChunkBytes(artifact: StoredArtifact, index: number): number {
  return (
    Math.min(
      artifact.chunkBytes,
      artifact.decodedBytes - index * artifact.chunkBytes,
    ) + SEALED_CHUNK_EXTRA_BYTES
  );
}

/** The data file's length once every chunk is written. */
export function sealedArtifactBytes(artifact: StoredArtifact): number {
  return (
    artifact.decodedBytes +
    artifact.chunkSha256.length * SEALED_CHUNK_EXTRA_BYTES
  );
}

/**
 * What a sealed chunk is bound to: this format, the artifact, the chunk's
 * index and the chunk's digest, so no chunk opens in another place.
 */
function associatedData(
  artifact: StoredArtifact,
  index: number,
): Uint8Array<ArrayBuffer> {
  const digest = artifact.chunkSha256[index];
  if (digest === undefined)
    throw new StoredRowsError("the chunk index is outside the artifact");
  return new TextEncoder().encode(
    `${SEAL_LABEL}${artifact.decodedSha256}\0${String(index)}\0${digest}`,
  );
}

/** Chunk `index`, sealed: its IV, then its ciphertext and tag. */
export async function sealChunk(
  key: CryptoKey,
  artifact: StoredArtifact,
  index: number,
  plain: ArrayBuffer,
): Promise<{
  readonly iv: Uint8Array<ArrayBuffer>;
  readonly sealed: Uint8Array<ArrayBuffer>;
}> {
  const iv = crypto.getRandomValues(new Uint8Array(IV_BYTES));
  const sealed = await crypto.subtle.encrypt(
    { name: "AES-GCM", iv, additionalData: associatedData(artifact, index) },
    key,
    plain,
  );
  return { iv, sealed: new Uint8Array(sealed) };
}

/** Opens sealed chunk `index`; rejects unless it was sealed there. */
export async function openChunk(
  key: CryptoKey,
  artifact: StoredArtifact,
  index: number,
  sealed: ArrayBuffer,
): Promise<ArrayBuffer> {
  if (sealed.byteLength !== sealedChunkBytes(artifact, index))
    throw new StoredRowsError("a stored chunk is short");
  try {
    return await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv: new Uint8Array(sealed, 0, IV_BYTES),
        additionalData: associatedData(artifact, index),
      },
      key,
      new Uint8Array(sealed, IV_BYTES),
    );
  } catch (error) {
    throw new StoredRowsError("a stored chunk does not open", {
      cause: error,
    });
  }
}

/**
 * A stored copy the page reads, holding the store's lock shared until
 * `release`.
 */
export interface StoredCopy {
  /** Chunk `index`, opened; rejects with `StoredRowsError` if it cannot be. */
  chunk(index: number): Promise<ArrayBuffer>;
  release(): void;
}

/**
 * The stored copy of `artifact` under `stored`, or undefined when the store
 * holds another artifact, another scope's, or nothing, or when another page
 * holds the store's lock. Rejects with `StoredRowsError` when the record
 * names this artifact and its data file is missing or short.
 */
export async function openStoredCopy(
  stored: StoredRowsKey,
  artifact: StoredArtifact,
): Promise<StoredCopy | undefined> {
  const release = await holdLock("shared");
  if (!release) return undefined;
  try {
    const directory = await storeDirectory(false);
    const record = directory && (await readRecord(directory));
    if (
      !directory ||
      !record ||
      record.keyId !== stored.keyId ||
      !sameArtifact(record, artifact)
    ) {
      release();
      return undefined;
    }
    let file: File;
    try {
      file = await (await directory.getFileHandle(record.dataFile)).getFile();
    } catch (error) {
      throw new StoredRowsError("the stored copy's data file is missing", {
        cause: error,
      });
    }
    if (file.size !== sealedArtifactBytes(artifact))
      throw new StoredRowsError("the stored copy's data file is short");
    return {
      chunk: async (index) => {
        const start = sealedChunkOffset(artifact.chunkBytes, index);
        const sealed = await file
          .slice(start, start + sealedChunkBytes(artifact, index))
          .arrayBuffer();
        return openChunk(stored.key, artifact, index, sealed);
      },
      release,
    };
  } catch (error) {
    release();
    throw error;
  }
}

/**
 * Erases every stored copy. A store worker still writing one writes no
 * record. Resolves once nothing readable is left; a file another page's
 * store worker holds open stays until a later store deletes it.
 */
export async function eraseStoredRows(): Promise<void> {
  if (!storedRowsAvailable()) return;
  await navigator.locks.request(
    STORED_ROWS_LOCK,
    { mode: "exclusive" },
    async () => {
      let directory: FileSystemDirectoryHandle | undefined;
      try {
        directory = await storeDirectory(false);
      } catch {
        // No origin private file system here, as in a private window, so
        // nothing was kept.
        return;
      }
      if (!directory) return;
      const epoch = `${EPOCH_PREFIX}${crypto.randomUUID()}`;
      await directory.getFileHandle(epoch, { create: true });
      // Only a store worker's commit opens the record, under this lock, so
      // failing to delete it is a failure to erase.
      try {
        await directory.removeEntry(STORED_ROWS_RECORD);
      } catch (error) {
        if (!isNotFound(error)) throw error;
      }
      const names: string[] = [];
      for await (const name of directory.keys())
        if (name !== epoch) names.push(name);
      for (const name of names) await removeQuietly(directory, name);
    },
  );
}

/**
 * The store's directory, created if `create`; undefined when it does not
 * exist and `create` is false.
 */
export async function storeDirectory(
  create: boolean,
): Promise<FileSystemDirectoryHandle | undefined> {
  const root = await navigator.storage.getDirectory();
  try {
    return await root.getDirectoryHandle(STORED_ROWS_DIRECTORY, { create });
  } catch (error) {
    if (!create && isNotFound(error)) return undefined;
    throw error;
  }
}

/** The store's epoch files, as one string a store worker compares. */
export async function storeEpochs(
  directory: FileSystemDirectoryHandle,
): Promise<string> {
  const names: string[] = [];
  for await (const name of directory.keys())
    if (name.startsWith(EPOCH_PREFIX)) names.push(name);
  return names.sort().join("/");
}

export async function readRecord(
  directory: FileSystemDirectoryHandle,
): Promise<StoredRecord | undefined> {
  let text: string;
  try {
    text = await (
      await directory.getFileHandle(STORED_ROWS_RECORD)
    )
      .getFile()
      .then((file) => file.text());
  } catch {
    return undefined;
  }
  try {
    return parseRecord(JSON.parse(text));
  } catch {
    return undefined;
  }
}

export function recordOf(
  artifact: StoredArtifact,
  keyId: string,
  dataFile: string,
): StoredRecord {
  return {
    version: RECORD_VERSION,
    keyId,
    dataFile,
    artifactId: artifact.artifactId,
    decodedSha256: artifact.decodedSha256,
    decodedBytes: artifact.decodedBytes,
    chunkBytes: artifact.chunkBytes,
    chunkSha256: [...artifact.chunkSha256],
  };
}

function parseRecord(value: unknown): StoredRecord | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const record = value as Record<string, unknown>;
  const digests = record["chunkSha256"];
  const {
    version,
    keyId,
    dataFile,
    artifactId,
    decodedSha256,
    decodedBytes,
    chunkBytes,
  } = record;
  if (
    version !== RECORD_VERSION ||
    typeof keyId !== "string" ||
    !KEY_ID.test(keyId) ||
    typeof dataFile !== "string" ||
    !DATA_FILE.test(dataFile) ||
    typeof artifactId !== "string" ||
    typeof decodedSha256 !== "string" ||
    !SHA256.test(decodedSha256) ||
    !Number.isSafeInteger(decodedBytes) ||
    !Number.isSafeInteger(chunkBytes) ||
    (chunkBytes as number) <= 0 ||
    !Array.isArray(digests) ||
    !digests.every(
      (digest): digest is string =>
        typeof digest === "string" && SHA256_ID.test(digest),
    ) ||
    digests.length !==
      Math.ceil((decodedBytes as number) / (chunkBytes as number))
  )
    return undefined;
  return {
    version,
    keyId,
    dataFile,
    artifactId,
    decodedSha256,
    decodedBytes: decodedBytes as number,
    chunkBytes: chunkBytes as number,
    chunkSha256: digests,
  };
}

function sameArtifact(left: StoredArtifact, right: StoredArtifact): boolean {
  return (
    left.artifactId === right.artifactId &&
    left.decodedSha256 === right.decodedSha256 &&
    left.decodedBytes === right.decodedBytes &&
    left.chunkBytes === right.chunkBytes &&
    left.chunkSha256.length === right.chunkSha256.length &&
    left.chunkSha256.every(
      (digest, index) => digest === right.chunkSha256[index],
    )
  );
}

/**
 * Takes the store's lock in `mode` if no page holds it in a way that blocks
 * it, without waiting; the release, or undefined if the lock was taken.
 */
async function holdLock(mode: LockMode): Promise<(() => void) | undefined> {
  const held = Promise.withResolvers<(() => void) | undefined>();
  navigator.locks
    .request(STORED_ROWS_LOCK, { mode, ifAvailable: true }, (lock) => {
      if (!lock) {
        held.resolve(undefined);
        return undefined;
      }
      const released: PromiseWithResolvers<void> = Promise.withResolvers();
      held.resolve(() => released.resolve());
      return released.promise;
    })
    .catch((error: unknown) => held.reject(error));
  return held.promise;
}

export async function removeQuietly(
  directory: FileSystemDirectoryHandle,
  name: string,
): Promise<void> {
  try {
    await directory.removeEntry(name, { recursive: true });
  } catch {
    // Missing already, or open in another page's store worker: a later
    // store deletes it.
  }
}

function isNotFound(error: unknown): boolean {
  return error instanceof DOMException && error.name === "NotFoundError";
}

function base64urlBytes(encoded: string): Uint8Array<ArrayBuffer> {
  if (!/^[A-Za-z0-9_-]*$/u.test(encoded))
    throw new StoredRowsError("the stored-rows key is not base64url");
  const binary = atob(encoded.replaceAll("-", "+").replaceAll("_", "/"));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1)
    bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function hex(digest: ArrayBuffer): string {
  return Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}

/** A store worker as the page drives it; a DOM `Worker` fits. */
export interface StoreWorker {
  addEventListener(
    type: "message" | "error" | "messageerror",
    listener: (event: Event) => void,
  ): void;
  postMessage(message: StoreRequest, transfer?: Transferable[]): void;
  terminate(): void;
}

/**
 * The page's side of one store: the port the engine worker sends accepted
 * chunks on, and the commit once the page has activated. Every failure,
 * including the worker's, ends as `abandoned`; the store never fails a load.
 */
export class StoredRowsWriter {
  /** For the engine worker's `open`, transferred with it. */
  readonly port: MessagePort;
  /** The id of the key this copy is sealed under. */
  readonly keyId: string;
  readonly #worker: StoreWorker | undefined;
  readonly #outcome = Promise.withResolvers<StoreOutcome>();
  /** What the store worker answered, or `abandoned` if it could not. */
  readonly outcome = awaitedLater(this.#outcome.promise);
  #sent = false;

  constructor(
    factory: () => StoreWorker,
    stored: StoredRowsKey,
    artifact: StoredArtifact,
  ) {
    const channel = new MessageChannel();
    this.port = channel.port1;
    this.keyId = stored.keyId;
    let worker: StoreWorker | undefined;
    try {
      worker = factory();
    } catch (error) {
      channel.port2.close();
      this.#settle({ type: "abandoned", reason: String(error) });
    }
    this.#worker = worker;
    if (!worker) return;
    worker.addEventListener("message", (event) => {
      const data = (event as MessageEvent<unknown>).data as
        Partial<StoreOutcome> | undefined;
      this.#settle(
        data?.type === "committed"
          ? { type: "committed" }
          : {
              type: "abandoned",
              reason:
                typeof data?.reason === "string" ? data.reason : "abandoned",
            },
      );
    });
    const fail = (): void =>
      this.#settle({ type: "abandoned", reason: "the store worker failed" });
    worker.addEventListener("error", fail);
    worker.addEventListener("messageerror", fail);
    worker.postMessage(
      {
        type: "begin",
        key: stored.key,
        keyId: stored.keyId,
        artifact,
        port: channel.port2,
      },
      [channel.port2],
    );
  }

  /** The page activated the artifact: keep it. */
  commit(): Promise<StoreOutcome> {
    this.#send("commit");
    return this.outcome;
  }

  /** The page did not activate it: delete it. */
  abandon(): void {
    this.#send("abandon");
  }

  #send(type: "commit" | "abandon"): void {
    if (this.#sent || !this.#worker) return;
    this.#sent = true;
    this.#worker.postMessage({ type });
  }

  #settle(outcome: StoreOutcome): void {
    this.#outcome.resolve(outcome);
    this.#worker?.terminate();
  }
}

export { CHUNKS_DUE_MS };
