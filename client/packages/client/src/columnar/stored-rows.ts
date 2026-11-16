import {
  StoredRowsWriter,
  eraseStoredRows,
  importStoredRowsKey,
  openStoredCopy,
  storedRowsAvailable,
  type StoreOutcome,
  type StoredArtifact,
  type StoredCopy,
  type StoredRowsKey,
} from "@calque/worker/columnar";

import {
  heldCopy,
  hintStoredRows,
  type StoredRowsHint,
} from "../stored-rows-hint.js";
import type { ColumnarStoreWorker } from "./worker.js";

/** One load's store of what it downloads, as `StoredRowsWriter` is. */
export interface RowWriter {
  /** For the engine worker's `open`, transferred with it. */
  readonly port: MessagePort;
  /** The id of the key the copy is sealed under. */
  readonly keyId: string;
  commit(): Promise<StoreOutcome>;
  abandon(): void;
}

/**
 * Where a session keeps its working set between visits: the origin private
 * file system in a browser, a fake in a test.
 */
export interface RowStore {
  /** Whether this context can keep a copy at all. */
  available(): boolean;
  importKey(encoded: string): Promise<StoredRowsKey>;
  /**
   * The stored copy of `artifact`, or undefined on a miss; rejects with
   * `StoredRowsError` when the copy it names is broken.
   */
  open(
    key: StoredRowsKey,
    artifact: StoredArtifact,
  ): Promise<StoredCopy | undefined>;
  /** Keeps what this load downloads, once the session commits it. */
  writer(key: StoredRowsKey, artifact: StoredArtifact): RowWriter;
  /**
   * Erases every stored copy and the hint; a writer still writing keeps
   * nothing.
   */
  erase(): Promise<void>;
  /**
   * Tells the server, and the backend that renders the page, which copy the
   * next load can read: `copy`, or none.
   */
  hint(copy: StoredRowsHint | undefined): void;
  /** The copy the page's cookie names, if any. */
  heldCopy(): StoredRowsHint | undefined;
}

export function browserRowStore(factory: () => ColumnarStoreWorker): RowStore {
  return {
    available: storedRowsAvailable,
    importKey: importStoredRowsKey,
    open: openStoredCopy,
    writer: (key, artifact) => new StoredRowsWriter(factory, key, artifact),
    erase: async () => {
      hintStoredRows(undefined);
      await eraseStoredRows();
    },
    hint: hintStoredRows,
    heldCopy,
  };
}
