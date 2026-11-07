type ObserverListener = () => void;

interface ObserverEntry<Value> {
  readonly listener: ObserverListener;
  readonly value: Value;
}

const MAX_REENTRANT_PUBLICATIONS = 64;
const INVALID_LISTENER_MESSAGE = "listener must be a function";

export function assertObserver(
  listener: unknown,
): asserts listener is ObserverListener {
  if (typeof listener !== "function") {
    throw new TypeError(INVALID_LISTENER_MESSAGE);
  }
}

/**
 * The listeners of one observable value. `notify` calls each listener
 * registered when its pass starts, skipping one removed or replaced since.
 * Calls to `notify` from inside a listener fold into one more pass after the
 * current one, up to 64 passes, after which the registry reports an error. A
 * listener that throws is reported and does not stop the others.
 */
export class ObserverRegistry<Value> {
  readonly #entries = new Map<ObserverListener, ObserverEntry<Value>>();
  #notifying = false;
  #pendingPublication = false;

  get size(): number {
    return this.#entries.size;
  }

  has(listener: ObserverListener): boolean {
    return this.#entries.has(listener);
  }

  set(listener: ObserverListener, value: Value): void {
    this.#entries.set(listener, { listener, value });
  }

  delete(listener: ObserverListener): boolean {
    return this.#entries.delete(listener);
  }

  notify(): void {
    if (this.#notifying) {
      this.#pendingPublication = true;
      return;
    }
    this.#notifying = true;
    let publications = 0;
    try {
      do {
        this.#pendingPublication = false;
        publications += 1;
        const eligible = Array.from(this.#entries.values());
        for (const entry of eligible) {
          if (this.#entries.get(entry.listener) !== entry) continue;
          try {
            entry.listener();
          } catch (error) {
            reportObserverError(error);
          }
        }
      } while (
        this.#pendingPublication &&
        publications < MAX_REENTRANT_PUBLICATIONS
      );
      if (this.#pendingPublication)
        reportObserverError(
          new Error("observer publication exceeded the reentrancy bound"),
        );
    } finally {
      this.#pendingPublication = false;
      this.#notifying = false;
    }
  }
}

/** Wraps `unsubscribe` so that every call after the first does nothing. */
export function once(unsubscribe: () => void): () => void {
  let subscribed = true;
  return () => {
    if (!subscribed) return;
    subscribed = false;
    unsubscribe();
  };
}

export function reportObserverError(error: unknown): void {
  try {
    const reporter = (
      globalThis as typeof globalThis & {
        readonly reportError?: (reported: unknown) => void;
      }
    ).reportError;
    if (typeof reporter === "function") {
      Reflect.apply(reporter, globalThis, [error]);
      return;
    }
    const logger = globalThis.console?.error;
    if (typeof logger === "function") {
      Reflect.apply(logger, globalThis.console, [error]);
    }
  } catch {
    // Observer reporting is advisory and cannot affect synchronization state.
  }
}
