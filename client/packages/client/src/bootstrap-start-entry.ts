/**
 * The entry a page imports to post its bootstrap request before the client
 * library has loaded: `@calque/client/bootstrap-start`. It and the modules it
 * re-exports import nothing else, so a bundler can put them in a chunk that
 * evaluates before the client library's.
 */
export {
  renderedConnection,
  startBootstrap,
  startedAuth,
} from "./bootstrap-start.js";
export type {
  BootstrapRequest,
  BootstrapStart,
  RenderedConnection,
} from "./bootstrap-start.js";
export {
  BOOTSTRAP_START_SCRIPT,
  BOOTSTRAP_START_SCRIPT_SHA256,
} from "./bootstrap-start-script.js";
