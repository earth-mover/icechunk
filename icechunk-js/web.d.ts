// `@earthmover/icechunk/web`: the same API as the default entry point, minus
// the native-only storage backends and virtual reference writes.
export * from './index'
export declare const __napiBindingTarget: 'wasm32-unknown-unknown'
export { createFetchStorage } from './fetch-storage'
export { createHttpVirtualChunkFetcher } from './http-virtual-chunks'
