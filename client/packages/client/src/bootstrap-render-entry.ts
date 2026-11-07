/**
 * The entry an application's backend imports to render a page's bootstrap:
 * `@calque/client/bootstrap-render`. It imports only the starter's module, so
 * a backend that renders pages never loads the client library.
 */
export { renderBootstrap } from "./bootstrap-render.js";
export type { RenderBootstrapOptions } from "./bootstrap-render.js";
export type {
  BootstrapRequest,
  RenderedConnection,
} from "./bootstrap-start.js";
