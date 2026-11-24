// `@earthmover/icechunk/web/init`: `@earthmover/icechunk/web` without loading
// the .wasm on import. Call one of these once before using anything else.
export * from './web'

/** Where to load the .wasm from; defaults to next to the module. */
export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module

export declare function init(options?: { module_or_path: InitInput | Promise<InitInput> }): Promise<unknown>
export declare function initSync(options: { module: BufferSource | WebAssembly.Module }): unknown
