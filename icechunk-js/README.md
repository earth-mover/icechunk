# Icechunk JS

JavaScript/TypeScript library for Icechunk Zarr Stores. Works as a native Node.js addon and in the browser via WASM.

## Getting Started

Install with `npm`

```
npm install @earthmover/icechunk@alpha
```

> [!NOTE]
> When using in the browser, you must specify to install the wasm package with `--cpu=wasm32`

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

In-memory storage works on all platforms. The following backends are available on native (non-WASM) builds:

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

Node.js supports the native virtual-chunk backends. WASM can read HTTP(S) virtual
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

## Using in the Browser (WASM)

Install the package with the `--cpu=wasm32` flag to get the WASM binary:

```bash
npm install @earthmover/icechunk --cpu=wasm32
```

To open a public repository in the browser, use fetch storage:

```typescript
import { Repository } from '@earthmover/icechunk'
import { createFetchStorage } from '@earthmover/icechunk/fetch-storage'

const storage = createFetchStorage('https://my-bucket.s3.us-west-2.amazonaws.com/my-repo.icechunk')
const repo = await Repository.open(storage)
```

For more control, use `Storage.newCustom()` to implement your own storage backend with any JS networking library.

The WASM build uses `SharedArrayBuffer` for threading, which requires your server to send these headers:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

With Vite, you'll also need the `vite-plugin-wasm` and `vite-plugin-top-level-await` plugins, and must exclude the icechunk packages from dependency optimization:

```typescript
// vite.config.ts
import wasm from 'vite-plugin-wasm'
import topLevelAwait from 'vite-plugin-top-level-await'

export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  optimizeDeps: {
    exclude: ['@earthmover/icechunk', '@earthmover/icechunk-wasm32-wasi'],
    include: ['@earthmover/icechunk > @earthmover/icechunk-wasm32-wasi'],
  },
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
})
```

See the [NAPI-RS WebAssembly docs](https://napi.rs/docs/concepts/webassembly) for more details.

## Examples

See [`examples/react-wasm`](examples/react-wasm) for an interactive React app demonstrating repository management, branching, tagging, and Zarr store operations in the browser.

## For Development

```bash
cd icechunk-js
yarn install
yarn build
yarn test
```

To build and test the WASM target, run these from the repository root:

```bash
just js-build-wasi
just js-test-wasi
```

The WASM build needs a C compiler that targets WebAssembly. The `js` pixi environment provides one.
On macOS without pixi, Apple's `clang` cannot target WebAssembly, so install Homebrew LLVM first.
See [WASM Compiler Setup (macOS)](../icechunk-python/docs/docs/reference/contributing.md#wasm-compiler-setup-macos).
Use the `just` recipes rather than calling `yarn build --target wasm32-wasip1-threads` directly.
The recipes set `RUSTC` and clear `CFLAGS`. Without them the build fails with
`Failed to copy artifact`, or produces a module that hangs on load.

> [!IMPORTANT]
> Building requires the workspace's minimum Rust version (`rust-version` in the root `Cargo.toml`).
> The `wasm32-wasip1-threads` target must be installed for that toolchain.
