// The browser build: wasm-bindgen for wasm32-unknown-unknown. It runs on the
// page's (or worker's) event loop with no threads, so unlike the default
// browser build it needs no cross-origin isolation (COOP/COEP). It loads and
// initializes its .wasm on import.
import init, { CompressionAlgorithm, Repository, Session, Storage, Store } from './wasm-bindgen/web/icechunk_js.js'
import { fetchStorageBackend } from './fetch-storage-backend.mjs'

await init()

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
