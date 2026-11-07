const INVALID_ABORT_SIGNAL_MESSAGE = "signal must be a genuine AbortSignal";
const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const reflectApply = Reflect.apply;

const abortSignalPrototype = globalThis.AbortSignal?.prototype;
const abortedGetter = abortSignalPrototype
  ? getOwnPropertyDescriptor(abortSignalPrototype, "aborted")?.get
  : undefined;
const reasonGetter = abortSignalPrototype
  ? getOwnPropertyDescriptor(abortSignalPrototype, "reason")?.get
  : undefined;
const eventTargetPrototype = globalThis.EventTarget?.prototype;
const addEventListener = eventTargetPrototype?.addEventListener;
const removeEventListener = eventTargetPrototype?.removeEventListener;
const emptyArguments: readonly unknown[] = [];

export function sealOptionalAbortSignal(
  signal: unknown,
): AbortSignal | undefined {
  if (signal === undefined) return undefined;
  if (
    signal === null ||
    typeof signal !== "object" ||
    !abortedGetter ||
    !reasonGetter ||
    !addEventListener ||
    !removeEventListener
  ) {
    invalidAbortSignal();
  }
  try {
    reflectApply(abortedGetter, signal, emptyArguments);
    reflectApply(reasonGetter, signal, emptyArguments);
  } catch {
    invalidAbortSignal();
  }
  return signal as AbortSignal;
}

export function isAbortSignalAborted(signal: AbortSignal): boolean {
  if (!abortedGetter) invalidAbortSignal();
  return reflectApply(abortedGetter, signal, emptyArguments) as boolean;
}

/**
 * A signal that aborts after `timeoutMs`. The caller disposes it once the
 * guarded request settles so the timer never fires into a finished request.
 */
export function timeoutSignal(timeoutMs: number): {
  readonly signal: AbortSignal;
  readonly dispose: () => void;
} {
  const controller = new AbortController();
  const timer = setTimeout(
    () =>
      controller.abort(new DOMException("request timed out", "TimeoutError")),
    timeoutMs,
  );
  return {
    signal: controller.signal,
    dispose: () => clearTimeout(timer),
  };
}

function invalidAbortSignal(): never {
  throw new TypeError(INVALID_ABORT_SIGNAL_MESSAGE);
}
