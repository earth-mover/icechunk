import test from 'ava'

import { wasmBindgen } from './binding'

// `@earthmover/icechunk/web` (web.mjs). binding.ts has already loaded it in
// this lane, so importing it again reuses that instance.
const webOnly = wasmBindgen ? test : test.skip

webOnly('exports the napi names', async (t) => {
  const web = await import('../web.mjs')
  t.is(web.JsRepository, web.Repository)
  t.is(web.JsSession, web.Session)
  t.is(web.JsStorage, web.Storage)
  t.is(web.JsStore, web.Store)
  t.is(web.CompressionAlgorithm.Zstd, 0)
  t.is(web.__napiBindingTarget, 'wasm32-unknown-unknown')
})

webOnly("createFetchStorage builds this build's Storage", async (t) => {
  // a Storage from the default entry point would be rejected by this build's Repository
  const web = await import('../web.mjs')
  t.true(web.createFetchStorage('https://example.com/repo.icechunk') instanceof web.Storage)
  t.is(typeof web.createHttpVirtualChunkFetcher, 'function')
})
