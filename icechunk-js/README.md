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

### Virtual Chunks

Virtual chunks are supported in node but not in WASM builds.

## Using in the Browser (WASM)

Install the package with the `--cpu=wasm32` flag to get the WASM binary:

```bash
npm install @earthmover/icechunk --cpu=wasm32
```

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
