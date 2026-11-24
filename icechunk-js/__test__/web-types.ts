// Compile-time check (`yarn check:web-types`, after `just js-build-browser`):
// the wasm-bindgen build has every class member that index.d.ts declares,
// except the native-only ones listed here. `web.d.ts` reuses index.d.ts, so
// this keeps those types honest for `@earthmover/icechunk/web`.
import type * as Napi from '../index'
import type * as Web from '../wasm-bindgen/web/icechunk_js'

type NativeOnly = {
  Repository: never
  Session: 'allVirtualChunkLocations'
  Store: 'setVirtualRef' | 'setVirtualRefs'
  Storage:
    | 'newLocalFilesystem'
    | 'newS3'
    | 'newR2'
    | 'newTigris'
    | 'newS3ObjectStore'
    | 'newGcs'
    | 'newAzureBlob'
    | 'newHttp'
    | 'newS3WithRefreshableCredentials'
    | 'newR2WithRefreshableCredentials'
    | 'newTigrisWithRefreshableCredentials'
    | 'newS3ObjectStoreWithRefreshableCredentials'
    | 'newGcsWithRefreshableCredentials'
}
type Class = keyof NativeOnly

type MissingStatics<C extends Class> = Exclude<
  keyof (typeof Napi)[C],
  keyof (typeof Web)[C] | NativeOnly[C] | 'prototype'
>
type MissingMembers<C extends Class> = Exclude<
  keyof (typeof Napi)[C]['prototype'],
  keyof (typeof Web)[C]['prototype'] | NativeOnly[C]
>
// Fails to compile, naming the member, if anything is missing.
type None<T extends never> = T

export type Checks = [
  None<MissingStatics<'Repository'>>,
  None<MissingMembers<'Repository'>>,
  None<MissingStatics<'Session'>>,
  None<MissingMembers<'Session'>>,
  None<MissingStatics<'Store'>>,
  None<MissingMembers<'Store'>>,
  None<MissingStatics<'Storage'>>,
  None<MissingMembers<'Storage'>>,
]

// and the native-only list stays accurate: each name exists in index.d.ts
type Stale<C extends Class> = Exclude<NativeOnly[C], keyof (typeof Napi)[C] | keyof (typeof Napi)[C]['prototype']>
export type NoStaleExceptions = [None<Stale<'Session'>>, None<Stale<'Store'>>, None<Stale<'Storage'>>]
