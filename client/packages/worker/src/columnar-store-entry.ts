// A store worker: seals each chunk the engine worker accepted and writes it
// into a new data file in the origin private file system, then, once the page
// has activated, writes the record that makes it the stored working set. It
// runs beside the cold load and never holds it up: the engine worker sends it
// each chunk in the buffer it no longer needs, and a failure here only means
// the next visit downloads the artifact.
import {
  CHUNKS_DUE_MS,
  DATA_SUFFIX,
  STORED_ROWS_LOCK,
  STORED_ROWS_RECORD,
  readRecord,
  recordOf,
  removeQuietly,
  sealChunk,
  sealedChunkOffset,
  storeDirectory,
  storeEpochs,
  type StoredArtifact,
} from "./stored-rows.js";
import type {
  StoreOutcome,
  StoreRequest,
  StoredRowsChunk,
} from "./stored-rows-protocol.js";

interface StoreScope {
  onmessage: ((event: MessageEvent<StoreRequest>) => void) | null;
  postMessage(message: StoreOutcome): void;
}

/** What a worker's sync access handle does that this worker uses. */
interface SyncAccessHandle {
  write(buffer: BufferSource, options: { readonly at: number }): number;
  truncate(size: number): void;
  flush(): void;
  close(): void;
}

interface SyncFileHandle extends FileSystemFileHandle {
  createSyncAccessHandle(): Promise<SyncAccessHandle>;
}

/** A data file no record names is deleted once it is this old. */
const STALE_DATA_MS = 10 * 60_000;

const scope = globalThis as unknown as StoreScope;

interface Writing {
  readonly key: CryptoKey;
  readonly keyId: string;
  readonly artifact: StoredArtifact;
  readonly port: MessagePort;
  /** Settles once the data file is open, or rejects if it cannot be. */
  readonly opened: Promise<{
    readonly directory: FileSystemDirectoryHandle;
    readonly dataFile: string;
    readonly handle: SyncAccessHandle;
    readonly epochs: string;
  }>;
  /** Each chunk's write, in arrival order, so one is sealed at a time. */
  written: Promise<void>;
  count: number;
  failure: unknown;
  readonly seen: Set<number>;
  readonly complete: PromiseWithResolvers<void>;
}

let writing: Writing | undefined;
let finished = false;

function begin(request: Extract<StoreRequest, { type: "begin" }>): void {
  if (writing || finished) return;
  const opened = (async () => {
    // Under the lock shared, so an erasure is wholly before or after the
    // epochs this store compares at its commit.
    return navigator.locks.request(
      STORED_ROWS_LOCK,
      { mode: "shared" },
      async () => {
        const directory = await storeDirectory(true);
        if (!directory) throw new Error("the store has no directory");
        const epochs = await storeEpochs(directory);
        const dataFile = `${crypto.randomUUID()}${DATA_SUFFIX}`;
        const file = (await directory.getFileHandle(dataFile, {
          create: true,
        })) as SyncFileHandle;
        if (typeof file.createSyncAccessHandle !== "function")
          throw new Error("this browser has no sync access handles");
        const handle = await file.createSyncAccessHandle();
        return { directory, dataFile, handle, epochs };
      },
    );
  })();
  opened.catch(() => undefined);
  const state: Writing = {
    key: request.key,
    keyId: request.keyId,
    artifact: request.artifact,
    port: request.port,
    opened,
    written: Promise.resolve(),
    count: 0,
    failure: undefined,
    seen: new Set(),
    complete: Promise.withResolvers(),
  };
  state.complete.promise.catch(() => undefined);
  writing = state;
  state.port.onmessage = (event: MessageEvent<StoredRowsChunk>) => {
    receive(state, event.data);
  };
}

function receive(state: Writing, chunk: StoredRowsChunk): void {
  const { index, bytes } = chunk;
  if (
    state.failure !== undefined ||
    !Number.isSafeInteger(index) ||
    index < 0 ||
    index >= state.artifact.chunkSha256.length ||
    state.seen.has(index)
  ) {
    fail(state, new Error("the engine sent a chunk out of place"));
    return;
  }
  state.seen.add(index);
  state.written = state.written.then(async () => {
    if (state.failure !== undefined) return;
    try {
      const { handle } = await state.opened;
      const { iv, sealed } = await sealChunk(
        state.key,
        state.artifact,
        index,
        bytes,
      );
      const at = sealedChunkOffset(state.artifact.chunkBytes, index);
      handle.write(iv, { at });
      handle.write(sealed, { at: at + iv.byteLength });
      state.count += 1;
      if (state.count === state.artifact.chunkSha256.length)
        state.complete.resolve();
    } catch (error) {
      fail(state, error);
    }
  });
}

function fail(state: Writing, error: unknown): void {
  state.failure ??= error;
  state.complete.reject(state.failure);
}

async function commit(state: Writing): Promise<StoreOutcome> {
  const { directory, dataFile, handle, epochs } = await state.opened;
  const due = Promise.withResolvers<never>();
  const timer = setTimeout(
    () => due.reject(new Error("the engine did not send every chunk")),
    CHUNKS_DUE_MS,
  );
  try {
    await Promise.race([state.complete.promise, due.promise]);
  } finally {
    clearTimeout(timer);
  }
  handle.flush();
  handle.close();
  return navigator.locks.request(
    STORED_ROWS_LOCK,
    { mode: "exclusive" },
    async (): Promise<StoreOutcome> => {
      if ((await storeEpochs(directory)) !== epochs) {
        await removeQuietly(directory, dataFile);
        return { type: "abandoned", reason: "the store was erased" };
      }
      const previous = await readRecord(directory);
      const record = (await directory.getFileHandle(STORED_ROWS_RECORD, {
        create: true,
      })) as SyncFileHandle;
      const recordHandle = await record.createSyncAccessHandle();
      try {
        const bytes = new TextEncoder().encode(
          JSON.stringify(recordOf(state.artifact, state.keyId, dataFile)),
        );
        recordHandle.truncate(0);
        recordHandle.write(bytes, { at: 0 });
        recordHandle.flush();
      } finally {
        recordHandle.close();
      }
      if (previous && previous.dataFile !== dataFile)
        await removeQuietly(directory, previous.dataFile);
      await sweep(directory, dataFile);
      return { type: "committed" };
    },
  );
}

/** Deletes data files no record names that no store can still be writing. */
async function sweep(
  directory: FileSystemDirectoryHandle,
  kept: string,
): Promise<void> {
  const now = Date.now();
  for await (const [name, entry] of directory.entries()) {
    if (name === kept || !name.endsWith(DATA_SUFFIX) || entry.kind !== "file")
      continue;
    try {
      const file = await (entry as FileSystemFileHandle).getFile();
      if (now - file.lastModified > STALE_DATA_MS)
        await removeQuietly(directory, name);
    } catch {
      // Open in another page's store worker.
    }
  }
}

async function abandon(state: Writing | undefined): Promise<void> {
  if (!state) return;
  fail(state, new Error("abandoned"));
  try {
    const { directory, dataFile, handle } = await state.opened;
    await state.written;
    handle.close();
    await removeQuietly(directory, dataFile);
  } catch {
    // Nothing was opened, or it is already gone.
  }
}

function settle(outcome: StoreOutcome): void {
  finished = true;
  writing?.port.close();
  scope.postMessage(outcome);
}

scope.onmessage = (event) => {
  const request = event.data;
  if (request.type === "begin") {
    begin(request);
    return;
  }
  if (finished) return;
  const state = writing;
  if (request.type === "abandon" || !state) {
    finished = true;
    void abandon(state).then(() =>
      settle({ type: "abandoned", reason: "the page did not activate" }),
    );
    return;
  }
  finished = true;
  commit(state).then(settle, async (error: unknown) => {
    await abandon(state);
    settle({
      type: "abandoned",
      reason: error instanceof Error ? error.message : String(error),
    });
  });
};
