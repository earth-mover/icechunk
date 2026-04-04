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

### Fetch Storage (read-only, works everywhere)

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

### Custom Storage Backends

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

### Virtual Chunks

Virtual chunks are supported in node but not in WASM builds.

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

For WASM:

```bash
# Requires brew install llvm and env vars (see docs/docs/contributing.md)
yarn build --target wasm32-wasip1-threads
NAPI_RS_FORCE_WASI=1 yarn test
```

> [!IMPORTANT]
> Building the WASM target requires Rust 1.95+ (or a nightly after 2026-01-19).
> Rust 1.94 has a bug where `std::thread::spawn` is unconditionally disabled on all WASI targets,
> including `wasm32-wasip1-threads` which supports threading.
> See [rust-lang/rust#151309](https://github.com/rust-lang/rust/pull/151309) for the fix.
