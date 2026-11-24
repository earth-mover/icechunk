# Icechunk JS

JavaScript/TypeScript library for Icechunk Zarr Stores. Works as a native Node.js addon and in the browser via WASM.

## Getting Started

Install with `npm`

```
npm install @earthmover/icechunk@alpha
```

Node.js loads a native addon (Linux x64/arm64 glibc, macOS arm64/x64, Windows x64).
Browsers get a WebAssembly build of the same API from the same package; see
[Using in the Browser](#using-in-the-browser).

```typescript
import { Repository, Storage } from '@earthmover/icechunk'
import * as zarr from 'zarrita'

const storage = await Storage.newInMemory()
const repo = await Repository.create(storage)

const session = await repo.writableSession('main')
const store = session.store

// zarrita uses the store directly via duck-typing
const root = zarr.root(store)
const arr = await zarr.create(root.resolve('/foo'), {
    shape: [100], dtype: '<i4', chunks: [10],
})

const snapshotId = await session.commit('Create foo array')
```

## Features

### Storage Backends

In-memory storage works everywhere. The following backends are available in Node.js only:

- **S3** — `Storage.newS3(bucket, prefix?, credentials?, options?)`
- **R2** — `Storage.newR2(bucket?, prefix?, accountId?, credentials?, options?)`
- **GCS** — `Storage.newGcs(bucket, prefix?, credentials?, config?)`
- **Azure Blob** — `Storage.newAzureBlob(account, container, prefix?, credentials?, config?)`
- **Tigris** — `Storage.newTigris(bucket, prefix?, credentials?, options?)`
- **HTTP** — `Storage.newHttp(baseUrl, config?)`
- **Local Filesystem** — `Storage.newLocalFilesystem(path)`

#### Fetch Storage (read-only, works everywhere)

For read-only access to a publicly hosted Icechunk repository, use the built-in fetch-based storage. This works on both native and WASM builds, making it the easiest way to open a repository in the browser:

```typescript
import { Repository } from '@earthmover/icechunk'
import { createFetchStorage } from '@earthmover/icechunk/fetch-storage'

const storage = createFetchStorage('https://my-bucket.s3.us-west-2.amazonaws.com/my-repo.icechunk')
const repo = await Repository.open(storage)
const session = await repo.readonlySession({ branch: 'main' })
const keys = await session.store.list()
```

The repository must be on S3-compatible storage with public read access and XML listing enabled. `createFetchStorage` uses the browser `fetch` API under the hood, so it works in any environment where `fetch` is available.

#### Custom Storage Backends

For WASM builds (or any environment where the built-in backends aren't suitable), you can provide your own storage implementation in JavaScript using `Storage.newCustom()`:

```typescript
const storage = Storage.newCustom({
  canWrite: async (_err, ) => true,
  getObjectRange: async (_err, { path, rangeStart, rangeEnd }) => {
    const headers: Record<string, string> = {}
    if (rangeStart != null && rangeEnd != null) {
      headers['Range'] = `bytes=${rangeStart}-${rangeEnd - 1}`
    }
    const resp = await fetch(`https://my-bucket.example.com/${path}`, { headers })
    return { data: new Uint8Array(await resp.arrayBuffer()), version: { etag: resp.headers.get('etag') ?? undefined } }
  },
  putObject: async (_err, { path, data, contentType }) => { /* ... */ },
  copyObject: async (_err, { from, to }) => { /* ... */ },
  listObjects: async (_err, prefix) => { /* return [{ id, createdAt, sizeBytes }] */ },
  deleteBatch: async (_err, { prefix, batch }) => { /* return { deletedObjects, deletedBytes } */ },
  getObjectLastModified: async (_err, path) => { /* return Date */ },
  getObjectConditional: async (_err, { path, previousVersion }) => { /* ... */ },
})
```

> **Note:** Callbacks use the Node.js error-first convention — the first argument is always `null` (reserved for errors) and the actual arguments follow. Use `_err` to skip it.

This is the primary way to use cloud storage in the browser, where native Rust networking is unavailable. Each callback method maps to an operation on the underlying `Storage` trait. See the exported `Storage*` TypeScript interfaces for the full type signatures.

#### Virtual Chunks

Node.js supports the native virtual-chunk backends. The browser build can read HTTP(S) virtual
chunks through an explicit JavaScript callback. This reads the referenced byte
ranges directly in the browser, without materializing the source data.

```typescript
const { Repository } = await import('@earthmover/icechunk')
const { createHttpVirtualChunkFetcher } = await import('@earthmover/icechunk/http-virtual-chunks')

const repo = await Repository.open(storage, undefined, {
  'https://data.example.com/tiles/': { type: 'HttpAccess' },
})
await repo.setHttpVirtualChunkFetcher(createHttpVirtualChunkFetcher())
const session = await repo.readonlySession({ branch: 'main' })
```

Register the callback before opening sessions. It is not persisted or serialized.
The repository configuration must define an HTTP virtual-chunk container whose
URL prefix matches each referenced HTTP source. For example, references to
`https://example.com/tiles/a.tif` can use a container configured for
`https://example.com/tiles/` with an HTTP store. You can configure this container
when creating the repository in Python.

Registering the callback only supplies the HTTP transport; it does not configure
containers or authorize access. Pass the allowed prefixes in
`authorizeVirtualChunkAccess` when opening the repository, as shown above.
The resolver checks that authorization, the response length, and the stored
`ETag`/`Last-Modified` checksums.

The provided helper requires HTTP 206 responses of exactly the requested length.
If the response exposes `Content-Range`, it must match the requested range exactly.
Cross-origin sources must allow CORS, and must expose `ETag` when references record
ETag checksums. Browsers can always read `Last-Modified`. Checksums are verified against the metadata of the
same response, without sending conditional headers, so range requests need no CORS preflight.
Redirects and ambient cookies are disabled.
Native HTTP transport options are rejected; use a custom callback when needed.
TIFF compression/predictor codecs still need support in your Zarr reader.

##### Reading a container from any Storage

To control how a container's chunks are fetched, register a `Storage` for its
URL prefix. A chunk at `{prefix}{path}` is read as `path` with the storage's
`getObjectRange`, so `Storage.newCustom(...)`, the fetch storage, or a native
backend can serve virtual chunks. This works for any container type on native
builds. WASM builds support HTTP and S3-style containers, but cannot load
repositories that configure GCS or Azure containers. For example, this reads
public `s3://` references in the browser over the bucket's HTTPS endpoint:

```typescript
const repo = await Repository.open(storage, undefined, {
  's3://my-public-bucket/data/': null, // authorized; the registered storage brings its own access
})
await repo.setVirtualChunkStorage(
  's3://my-public-bucket/data/',
  createFetchStorage('https://my-public-bucket.s3.us-west-2.amazonaws.com/data'),
)
const session = await repo.readonlySession({ branch: 'main' })
```

The prefix must match a virtual chunk container in the repository's
configuration, and the container must still be authorized when opening the
repository; any credential value works, including `null`. Register the storage
before creating sessions; it is not persisted. ETag checksums are verified against the version that
`getObjectRange` returns. Last-modified checksums cost one extra
`getObjectLastModified` call per chunk read. In the browser, the source must
allow CORS from your origin and expose `ETag` when references record ETag checksums.
`setHttpVirtualChunkFetcher` is a convenience built on the same mechanism: it
registers the callback for every HTTP container in the repository.

## Using in the Browser

Bundlers resolve `@earthmover/icechunk` to its browser build (the `browser`
export condition), a WebAssembly module built with wasm-bindgen. It has the
same API as the Node.js build, minus the native-only storage backends and
virtual reference writes. It needs no special install flags and no
cross-origin isolation (COOP/COEP) headers.

To open a public repository, use fetch storage:

```typescript
import { Repository, createFetchStorage } from '@earthmover/icechunk'

const repo = await Repository.open(createFetchStorage('https://my-bucket.s3.us-west-2.amazonaws.com/my-repo.icechunk'))
```

`@earthmover/icechunk/fetch-storage` works too. For more control, implement a
storage backend with `Storage.newCustom()` and any JS networking library.

Things to know:

- **It runs on the thread that imports it.** There are no worker threads, so
  decompressing and parsing large snapshots and manifests happens on that
  thread. For large repositories, import it in a
  [module Worker](https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker#module)
  to keep the page responsive.
- **It loads its `.wasm` on import**, from next to the module
  (`new URL('icechunk_js_bg.wasm', import.meta.url)`), using top-level `await`.
  Vite and webpack 5 handle this. With Vite, exclude the package from dependency
  optimization:

  ```typescript
  // vite.config.ts
  export default defineConfig({
    optimizeDeps: { exclude: ['@earthmover/icechunk'] },
  })
  ```

  A strict Content Security Policy needs `script-src 'wasm-unsafe-eval'`, and
  the server must send `.wasm` files as `application/wasm`.
- **To load the `.wasm` yourself**, import `@earthmover/icechunk/web/init` and
  call `init()` (or `initSync()`) once before anything else. Use it with esbuild
  (which does not copy the `.wasm` next to the bundle), in service workers (no
  top-level `await`), with server-side rendering, or to control when and from
  where the `.wasm` loads:

  ```typescript
  import { init, Repository, Storage } from '@earthmover/icechunk/web/init'

  await init({ module_or_path: '/assets/icechunk_js_bg.wasm' })
  ```

  The file is `wasm-bindgen/web/icechunk_js_bg.wasm` in the package.
- **Session getters never block.** While an operation such as a commit is using a
  session, getters like `session.snapshotId` return the state from before it
  started. The Node.js build waits for the operation instead.
- **Writing** works through `Storage.newCustom()`. A custom storage's `putObject`
  must honor `previousVersion` (create-if-missing, or write only if the ETag
  matches) and return `{ kind: 'not_on_latest_version' }` when the condition
  fails. icechunk relies on this to update branches safely.
- `@earthmover/icechunk/web` is the browser build under an explicit name, for
  tools that do not apply the `browser` condition. Import everything from one
  entry point: a `Storage` from one build does not work with another build's
  `Repository`.

## Examples

See [`examples/react-wasm`](examples/react-wasm) for an interactive React app demonstrating repository management, branching, tagging, and Zarr store operations in the browser.

## For Development

```bash
cd icechunk-js
yarn install
yarn build
yarn test
```

To build and test the browser build, run these from the repository root. They
need `clang`, `llvm-ar`, and the `wasm-bindgen` CLI at the version in `Cargo.lock`;
on macOS, Apple's `clang` works but its `ar` does not, so install Homebrew LLVM for
`llvm-ar` (see [WASM Compiler Setup (macOS)](../icechunk-python/docs/docs/reference/contributing.md#wasm-compiler-setup-macos)).

```bash
just js-build-browser   # or `just js-build-browser browser-release` for the published build
just js-test-browser    # the ava specs against web.mjs, under node
just wasm-browser-test  # Rust-side browser tests (needs wasm-bindgen-test-runner)
```

> [!IMPORTANT]
> Building requires the workspace's minimum Rust version (`rust-version` in the root `Cargo.toml`).
> The browser build needs the `wasm32-unknown-unknown` target installed for that toolchain.
