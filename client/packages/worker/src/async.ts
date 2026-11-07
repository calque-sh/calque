/**
 * Resolves after `milliseconds`, or as soon as `signal` aborts, and then
 * clears its timer: a wait that no longer has anything to resume leaves
 * nothing scheduled.
 */
export function delay(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const end = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", end);
      resolve();
    };
    const timer = setTimeout(end, milliseconds);
    signal?.addEventListener("abort", end);
  });
}

/**
 * Returns `promise`, started now and awaited later, if at all. Whoever awaits
 * it sees its rejection; until then the rejection is not reported unhandled.
 */
export function awaitedLater<T>(promise: Promise<T>): Promise<T> {
  promise.catch(() => undefined);
  return promise;
}

/**
 * Stops reading a body and cancels the rest of it. A body that already failed
 * refuses the cancel with that failure, which the reader is already handling.
 */
export async function stopReading(reader: {
  cancel(): Promise<void>;
}): Promise<void> {
  await reader.cancel().catch(() => undefined);
}

/**
 * Hands items from one producer to one consumer. The producer awaits `room()`
 * before it produces more, which holds it back while the queue holds more than
 * `highWaterMark`, each item counted by `size`.
 */
export class BoundedQueue<T> {
  readonly #highWaterMark: number;
  readonly #size: (item: T) => number;
  #items: T[] = [];
  #held = 0;
  #end: { readonly failure?: { readonly error: unknown } } | undefined;
  #cancelled: { readonly reason: unknown } | undefined;
  #taking: PromiseWithResolvers<T[] | undefined> | undefined;
  #waiting: PromiseWithResolvers<void> | undefined;

  constructor(highWaterMark: number, size: (item: T) => number = () => 1) {
    this.#highWaterMark = highWaterMark;
    this.#size = size;
  }

  /**
   * Queues `items` together, so a waiting consumer takes them in one call.
   * Dropped once the consumer has cancelled.
   */
  push(...items: readonly T[]): void {
    if (this.#cancelled) return;
    if (this.#end) throw new Error("the queue is closed");
    for (const item of items) {
      this.#items.push(item);
      this.#held += this.#size(item);
    }
    this.#hand();
  }

  /**
   * Resolves once the queue holds no more than its high-water mark, and
   * rejects with the consumer's reason once it has cancelled.
   */
  room(): Promise<void> {
    if (this.#cancelled) return Promise.reject(this.#cancelled.reason);
    if (this.#held <= this.#highWaterMark) return Promise.resolve();
    this.#waiting ??= Promise.withResolvers();
    return this.#waiting.promise;
  }

  /** The producer is done: `take` returns what is queued, then `undefined`. */
  close(): void {
    if (this.#end || this.#cancelled) return;
    this.#end = {};
    this.#hand();
  }

  /** The producer failed: `take` returns what is queued, then rejects. */
  fail(error: unknown): void {
    if (this.#end || this.#cancelled) return;
    this.#end = { failure: { error } };
    this.#hand();
  }

  /**
   * Every item queued, once there is one, or `undefined` once the producer
   * closed the queue and it is empty. One call at a time.
   */
  take(): Promise<T[] | undefined> {
    if (this.#cancelled) return Promise.reject(this.#cancelled.reason);
    if (this.#taking)
      return Promise.reject(new Error("the queue is already being taken from"));
    const taking = Promise.withResolvers<T[] | undefined>();
    this.#taking = taking;
    this.#hand();
    return taking.promise;
  }

  /**
   * The consumer stops: what is queued is dropped, and `room` and `take`
   * reject with `reason`.
   */
  cancel(reason: unknown): void {
    if (this.#cancelled) return;
    this.#cancelled = { reason };
    this.#items = [];
    this.#held = 0;
    this.#taking?.reject(reason);
    this.#taking = undefined;
    this.#waiting?.reject(reason);
    this.#waiting = undefined;
  }

  #hand(): void {
    const taking = this.#taking;
    if (!taking) return;
    if (this.#items.length > 0) {
      const items = this.#items;
      this.#items = [];
      this.#held = 0;
      this.#taking = undefined;
      taking.resolve(items);
      this.#waiting?.resolve();
      this.#waiting = undefined;
      return;
    }
    if (!this.#end) return;
    this.#taking = undefined;
    if (this.#end.failure) taking.reject(this.#end.failure.error);
    else taking.resolve(undefined);
  }
}
