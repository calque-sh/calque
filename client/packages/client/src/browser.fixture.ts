import { vi } from "vitest";

/**
 * A `Worker` that reads its options' `type` as a browser does and starts
 * nothing: every test that runs a session hands it worker factories of its
 * own, so this one serves only the client's check that module workers exist.
 */
const ModuleWorkerStub = vi.fn(function (
  _url: string | URL,
  options?: WorkerOptions,
) {
  void options?.type;
});
/**
 * A page in a browser that has every API the client calls and reports no
 * hardware thread count, so a session spawns one hasher unless `threads` says
 * otherwise.
 */
export function stubBrowserFeatures(threads?: number): void {
  vi.stubGlobal("Worker", ModuleWorkerStub);
  vi.stubGlobal(
    "navigator",
    threads === undefined ? {} : { hardwareConcurrency: threads },
  );
}

/**
 * A page whose Content-Security-Policy lacks `wasm-unsafe-eval`: compiling a
 * module throws, as in Chrome, and every other part of `WebAssembly` is the
 * real one.
 */
export function stubWebAssemblyPolicy(): void {
  vi.stubGlobal(
    "WebAssembly",
    Object.create(WebAssembly, {
      Module: {
        value: function Module(): never {
          throw new Error(
            "Refused to compile or instantiate WebAssembly module",
          );
        },
      },
    }),
  );
}
