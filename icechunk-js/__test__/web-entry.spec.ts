import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

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

webOnly("web/init shares web.mjs's classes", async (t) => {
  const [web, manual] = await Promise.all([import('../web.mjs'), import('../web-init.mjs')])
  t.is(manual.Repository, web.Repository)
  t.is(manual.Storage, web.Storage)
  t.is(typeof manual.init, 'function')
  t.is(typeof manual.initSync, 'function')
})

webOnly('web/init loads nothing on import, and works after initSync', async (t) => {
  // a fresh process, so the module is not already initialized
  const entry = new URL('../web-init.mjs', import.meta.url).href
  const wasm = new URL('../wasm-bindgen/web/icechunk_js_bg.wasm', import.meta.url).href
  const script = `
    globalThis.fetch = () => { throw new Error('fetched on import') }
    const icechunk = await import(${JSON.stringify(entry)})
    const { readFileSync } = await import('node:fs')
    icechunk.initSync({ module: readFileSync(new URL(${JSON.stringify(wasm)})) })
    const repo = await icechunk.Repository.create(await icechunk.Storage.newInMemory())
    const session = await repo.writableSession('main')
    await session.store.set('zarr.json', new TextEncoder().encode('{"zarr_format":3,"node_type":"group"}'))
    console.log(await session.commit('manual init'))
  `
  const { stdout } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script])
  t.regex(stdout.trim(), /^\w+$/)
})
