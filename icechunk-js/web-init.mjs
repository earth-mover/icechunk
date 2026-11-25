// `@earthmover/icechunk/web/init`: the browser build (see web.mjs) without
// loading its .wasm on import. Call `init()` or `initSync()` once, before
// using anything else; until then every class throws. For bundlers that do not
// copy the .wasm next to the module (esbuild), service workers (no top-level
// await), server-side rendering, and strict CSP setups.
import init, {
  CompressionAlgorithm,
  initSync,
  Repository,
  Session,
  Storage,
  Store,
} from './wasm-bindgen/web/icechunk_js.js'
import { fetchStorageBackend } from './fetch-storage-backend.mjs'

export { init, initSync }
export { CompressionAlgorithm, Repository, Session, Storage, Store }
// the napi loader exports these names too
export { Repository as JsRepository, Session as JsSession, Storage as JsStorage, Store as JsStore }
export const __napiBindingTarget = 'wasm32-unknown-unknown'

/**
 * Read-only fetch storage for this build; see `@earthmover/icechunk/fetch-storage`.
 * @param {string} baseUrl - Base URL of the icechunk repository (no trailing slash)
 */
export function createFetchStorage(baseUrl) {
  return Storage.newCustom(fetchStorageBackend(baseUrl))
}

export { createHttpVirtualChunkFetcher } from './http-virtual-chunks.mjs'
