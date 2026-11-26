// `@earthmover/icechunk/web/init`: `@earthmover/icechunk/web` without loading
// the .wasm on import. Call one of these once before using anything else.
// The package exports the .wasm as `@earthmover/icechunk/web/icechunk_js_bg.wasm`:
//
//   // a bundler (Vite shown; esbuild: `--loader:.wasm=file`, no `?url`)
//   import wasmUrl from '@earthmover/icechunk/web/icechunk_js_bg.wasm?url'
//   await init({ module_or_path: wasmUrl })
//
//   // Node.js
//   const wasm = new URL(import.meta.resolve('@earthmover/icechunk/web/icechunk_js_bg.wasm'))
//   initSync({ module: readFileSync(wasm) })
export * from './web'

/** Where to load the .wasm from; defaults to next to the module. */
export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module

export declare function init(options?: { module_or_path: InitInput | Promise<InitInput> }): Promise<unknown>
export declare function initSync(options: { module: BufferSource | WebAssembly.Module }): unknown
