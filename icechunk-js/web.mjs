// The browser build: wasm-bindgen for wasm32-unknown-unknown. It runs on the
// page's (or worker's) event loop with no threads, so it needs no
// cross-origin isolation (COOP/COEP). It loads its .wasm, from next to this
// module, on import; `@earthmover/icechunk/web/init` leaves that to you.
import { init } from './web-init.mjs'

await init()

export * from './web-init.mjs'
