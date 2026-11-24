// The binding under test. By default this is the napi package (`../index`);
// ICECHUNK_JS_BINDING=wasm-bindgen runs the same specs against the browser
// entry point `../web.mjs` (built by `just js-build-browser`) under node.
import { readFile } from 'node:fs/promises'
import { createRequire } from 'node:module'

import type * as Napi from '../index'

const require = createRequire(import.meta.url)

export const wasmBindgen = process.env.ICECHUNK_JS_BINDING === 'wasm-bindgen'

async function loadWeb(): Promise<typeof Napi> {
  // web.mjs fetches its .wasm next to itself, as in a browser; node's fetch
  // cannot read file: URLs, so serve those from disk
  const fetch = globalThis.fetch
  globalThis.fetch = async (input, init) => {
    const url = input instanceof URL ? input : new URL(input instanceof Request ? input.url : input)
    if (url.protocol !== 'file:') return fetch(input, init)
    return new Response(await readFile(url), { headers: { 'Content-Type': 'application/wasm' } })
  }
  try {
    return (await import('../web.mjs')) as unknown as typeof Napi
  } finally {
    globalThis.fetch = fetch
  }
}

const binding: typeof Napi = wasmBindgen ? await loadWeb() : require('../index.js')

export const { Repository, Session, Storage, Store } = binding
export type Repository = Napi.Repository
export type Session = Napi.Session
export type Storage = Napi.Storage
export type Store = Napi.Store
export type * from '../index'
