import type {
  QueryExecutionOptions,
  QuerySpecification,
  QueryWindow,
  QueryWindowResult,
} from "@calque/core";
import {
  awaitedLater,
  type FrameCompletion,
  type FrameMember,
} from "@calque/worker/columnar";

import type { SyncError } from "../error.js";
import {
  materializeRows,
  materializeWindow,
  type CellReaders,
  type QueryRow,
  type QueryWindowRows,
} from "../facade/port.js";
import { assertObserver, ObserverRegistry, once } from "../observer.js";
import type { LiveQueryResource, LiveQuerySnapshot } from "../resource.js";
import { queryError } from "./session-errors.js";
import type { ColumnarSession } from "./session.js";

export const LOADING: LiveQuerySnapshot<never> = Object.freeze({
  status: "loading",
});

/**
 * How long a resource no component listens to stays in the session after it
 * settles or is last handed out. A suspended render that React discards never
 * subscribes, and its resource would otherwise stay until the client closes.
 * React retries a suspended render as soon as the promise settles, so a few
 * seconds cover a retry; a component that commits later subscribes the
 * released resource again and reads it only if its rows are stale.
 */
export const UNLISTENED_GRACE_MS = 5_000;

export type SubscriptionResult = readonly QueryRow[] | QueryWindowRows;

export class QuerySubscription<
  Result extends SubscriptionResult = SubscriptionResult,
> implements LiveQueryResource<Result> {
  readonly #session: ColumnarSession;
  readonly key: string;
  readonly specification: QuerySpecification;
  /** `canonicalQuerySpecification(specification)`, which reuse compares. */
  readonly canonical: string;
  readonly dependencies: readonly string[];
  readonly options: QueryExecutionOptions;
  readonly window: QueryWindow | undefined;
  readonly readers: CellReaders;
  readonly #listeners = new ObserverRegistry<true>();
  #snapshot: LiveQuerySnapshot<Result> = LOADING;
  /** The generations `#snapshot` holds at; `undefined` before the first. */
  #seen: Readonly<Record<string, number>> | undefined;
  #ran = false;
  /** Settles once a frame has published this resource at the newest state. */
  #read: PromiseWithResolvers<void> | undefined;
  /** Set when a caller awaits `#read` through `promise`. */
  #awaited = false;
  #idle: ReturnType<typeof setTimeout> | undefined;

  constructor(
    session: ColumnarSession,
    key: string,
    specification: QuerySpecification,
    canonical: string,
    dependencies: readonly string[],
    options: QueryExecutionOptions,
    window: QueryWindow | undefined,
    readers: CellReaders,
  ) {
    this.#session = session;
    this.key = key;
    this.specification = specification;
    this.canonical = canonical;
    this.dependencies = dependencies;
    this.options = options;
    this.window = window;
    this.readers = readers;
    this.touch();
  }

  readonly getSnapshot = (): LiveQuerySnapshot<Result> => this.#snapshot;

  readonly getServerSnapshot = (): LiveQuerySnapshot<Result> => LOADING;

  readonly subscribe = (listener: () => void): (() => void) => {
    assertObserver(listener);
    if (this.#listeners.has(listener))
      throw new TypeError("listener is already subscribed to this resource");
    this.#listeners.set(listener, true);
    this.stopIdle();
    // The first listener since the last one left: publications skipped this
    // resource meanwhile.
    if (
      this.#listeners.size === 1 &&
      (this.#session.retain(this) || !this.#ran)
    )
      void this.run();
    return once(() => {
      this.#listeners.delete(listener);
      if (this.#listeners.size === 0) this.#session.release(this);
    });
  };

  get promise(): Promise<void> {
    if (!this.#ran) void this.run();
    if (!this.#read) return Promise.resolve();
    this.#awaited = true;
    return this.#read.promise;
  }

  get listenerCount(): number {
    return this.#listeners.size;
  }

  /**
   * Starts the grace period again for a resource nobody listens to; called
   * when it is created, handed out or settles.
   */
  touch(): void {
    this.stopIdle();
    if (this.#listeners.size > 0) return;
    this.#idle = setTimeout(() => {
      this.#idle = undefined;
      if (this.#listeners.size === 0 && !this.#read)
        this.#session.release(this);
    }, UNLISTENED_GRACE_MS);
  }

  /** Somebody paints these rows or awaits them, so a frame reads them. */
  get wanted(): boolean {
    return this.#listeners.size > 0 || this.#awaited;
  }

  /** True when a dependency moved past the rows this resource publishes. */
  isStale(generations: Readonly<Record<string, number>>): boolean {
    const seen = this.#seen;
    return (
      seen === undefined ||
      this.dependencies.some(
        (collection) =>
          (seen[collection] ?? -1) < (generations[collection] ?? -1),
      )
    );
  }

  /** Asks the next frame to bring these rows to the newest state. */
  run(): Promise<void> {
    this.#ran = true;
    if (!this.#read) {
      this.#read = Promise.withResolvers();
      // A failure is published as the snapshot; only `promise` rejects.
      awaitedLater(this.#read.promise);
    }
    this.#session.frame(this);
    return this.#read.promise;
  }

  /** This resource in a frame; a stale one is read whatever its `seen`. */
  member(stale: boolean): FrameMember {
    const seen =
      stale || this.#seen === undefined
        ? undefined
        : Object.fromEntries(
            this.dependencies.map((collection) => [
              collection,
              this.#seen?.[collection] ?? -1,
            ]),
          );
    const base = {
      specification: this.specification,
      ...(seen === undefined ? {} : { seen }),
    };
    return this.window === undefined
      ? { type: "query", ...base, options: this.options }
      : {
          type: "window",
          ...base,
          window: this.window,
          options:
            this.options.maxBytes === undefined
              ? {}
              : { maxBytes: this.options.maxBytes },
        };
  }

  /**
   * Takes a frame's answer: the state its rows now hold at, and new rows or
   * a failure, or `null` when the rows already published still hold. Returns
   * whether the snapshot changed; nobody is notified until `notify`.
   */
  adopt(
    generations: Readonly<Record<string, number>>,
    outcome: FrameCompletion["results"][number],
  ): boolean {
    this.#seen = generations;
    if (outcome === null) return false;
    if (!outcome.ok)
      this.#snapshot = Object.freeze({
        status: "error",
        error: queryError(outcome.error),
      });
    else
      this.#snapshot = Object.freeze({
        status: "ready",
        result: (this.window === undefined
          ? materializeRows(outcome.result, this.readers)
          : materializeWindow(
              outcome.result as QueryWindowResult,
              this.readers,
            )) as Result,
      });
    return true;
  }

  /**
   * Settles the read in flight unless a later state has moved these rows
   * again, and says whether it did.
   */
  settle(generations: Readonly<Record<string, number>>): boolean {
    if (this.isStale(generations)) return false;
    this.#end((read) => {
      if (this.#snapshot.status === "error") read.reject(this.#snapshot.error);
      else read.resolve();
    });
    return true;
  }

  /** Publishes a failure no frame could read past. */
  fail(error: SyncError): void {
    this.publish(Object.freeze({ status: "error", error }));
    this.#end((read) => read.reject(error));
  }

  /** Ends the read of rows nobody wants any more; the next listener reads. */
  drop(): void {
    this.#ran = false;
    this.#end((read) => read.resolve());
  }

  /** Forgets `#seen`, which another activation's generations do not order. */
  forget(): void {
    this.#seen = undefined;
  }

  notify(): void {
    this.#listeners.notify();
  }

  publish(snapshot: LiveQuerySnapshot<Result>): void {
    this.#snapshot = snapshot;
    this.#listeners.notify();
  }

  #end(settle: (read: PromiseWithResolvers<void>) => void): void {
    const read = this.#read;
    this.#read = undefined;
    this.#awaited = false;
    if (read) settle(read);
    this.touch();
  }

  /** Cancels the grace period; the session does when it closes. */
  stopIdle(): void {
    clearTimeout(this.#idle);
    this.#idle = undefined;
  }
}
