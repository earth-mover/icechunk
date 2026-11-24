// The binding under test. By default this is the napi package (`../index`);
// ICECHUNK_JS_BINDING=wasm-bindgen runs the same specs against the browser
// build (wasm32-unknown-unknown, built by `just js-build-browser`) under node.
import { createRequire } from 'node:module'

import type * as Napi from '../index'

const require = createRequire(import.meta.url)

export const wasmBindgen = process.env.ICECHUNK_JS_BINDING === 'wasm-bindgen'

const binding: typeof Napi = wasmBindgen ? require('../wasm-bindgen/node/icechunk_js.js') : require('../index.js')

export const { Repository, Session, Storage, Store } = binding
export type Repository = Napi.Repository
export type Session = Napi.Session
export type Storage = Napi.Storage
export type Store = Napi.Store
export type * from '../index'
