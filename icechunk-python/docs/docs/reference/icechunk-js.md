---
title: JavaScript / TypeScript
---

# Icechunk JavaScript / TypeScript

The official [`@earthmover/icechunk`](https://www.npmjs.com/package/@earthmover/icechunk) package provides full read-write access to Icechunk repositories from JavaScript and TypeScript. It uses native Rust bindings (via NAPI-RS) in Node.js and a WebAssembly build (via wasm-bindgen) of the same API in browsers.

## Installation

For your default platform:

```bash
npm install @earthmover/icechunk zarrita
```

Node.js loads a native addon on Linux (x64 and arm64, glibc), macOS (arm64 and x64) and Windows (x64).
The same package works in the browser: bundlers pick its WebAssembly build through the `browser`
export condition. No install flags or cross-origin isolation headers are needed; see [Browser](#browser).

## Quick start

### Create a repository and write data

```typescript
import { Repository, Storage } from '@earthmover/icechunk'
import * as zarr from 'zarrita'

// Create an in-memory repository
const storage = await Storage.newInMemory()
const repo = await Repository.create(storage)

// Open a writable session on the main branch
const session = await repo.writableSession('main')
const store = session.store
const root = zarr.root(store)

// Create a root group
const group = await zarr.create(root)

// Create a float32 array
const arr = await zarr.create(root.resolve('/temperature'), {
  shape: [4],
  chunk_shape: [4],
  data_type: 'float32',
})

// Write data
await zarr.set(arr, null, {
  data: new Float32Array([1.5, 2.5, 3.5, 4.5]),
  shape: [4],
  stride: [1],
})

// Commit changes
const snapshotId = await session.commit('Add temperature array')
```

### Read data

```typescript
const readSession = await repo.readonlySession({ branch: 'main' })
const readRoot = zarr.root(readSession.store)

// Open the array and read all values
const arr = await zarr.open(readRoot.resolve('/temperature'), { kind: 'array' })
console.log(arr.shape)  // [4]
console.log(arr.dtype)  // 'float32'

const data = await zarr.get(arr)
console.log(data.data)  // Float32Array [1.5, 2.5, 3.5, 4.5]

// Read a slice
const slice = await zarr.get(arr, [zarr.slice(1, 3)])
console.log(slice.data)  // Float32Array [2.5, 3.5]
```

## Version control

The JavaScript package supports the same version control features as the Python package.

### Branches and tags

```typescript
// Create a branch from the current snapshot
await repo.createBranch('dev', snapshotId)

// List branches
const branches = await repo.listBranches()

// Create a tag
await repo.createTag('v1.0', snapshotId)

// List tags
const tags = await repo.listTags()
```

### Commit history

```typescript
const history = await repo.ancestry({ branch: 'main' })
for (const snapshot of history) {
  console.log(`${snapshot.id} ${snapshot.message}`)
}
```

### Diff

```typescript
const diff = await repo.diff({
  fromBranch: 'main',
  toBranch: 'dev',
})
console.log('New arrays:', diff.newArrays)
console.log('Updated arrays:', diff.updatedArrays)
```

## Storage backends

### Node.js

In Node.js, all cloud storage backends are available natively:

```typescript
import { Storage } from '@earthmover/icechunk'

// Local filesystem
const local = await Storage.newLocalFilesystem('/path/to/repo')

// Amazon S3
const s3 = Storage.newS3('my-bucket', 'my-prefix', credentials, options)

// Google Cloud Storage
const gcs = Storage.newGcs('my-bucket', 'my-prefix', credentials, config)

// Azure Blob Storage
const azure = await Storage.newAzureBlob('account', 'container', 'prefix', credentials, config)

// Cloudflare R2
const r2 = Storage.newR2('my-bucket', 'my-prefix', accountId, credentials, options)

// Tigris
const tigris = Storage.newTigris('my-bucket', 'my-prefix', credentials, options)

// HTTP (read-only)
const http = Storage.newHttp('https://example.com/repo')
```

### Browser

In the browser, the native storage backends are not available. Instead, use:

- **In-memory storage** for ephemeral repositories
- **Fetch storage** for read-only access to publicly-accessible repositories over HTTP/HTTPS
- **Custom storage** with a JavaScript-provided backend (e.g., using `fetch`, `@aws-sdk/client-s3`, or a proxy you run)

The browser build runs on the thread that imports it, with no worker threads: decompressing and
parsing large snapshots and manifests happens there. For large repositories, use icechunk from a
[module Worker](https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker#module) to keep the
page responsive. While an operation such as a commit is using a session, getters like
`session.snapshotId` return the state from before it started instead of waiting.

#### Read-only fetch storage

The package includes a built-in fetch-based storage backend for read-only access to repositories hosted on S3-compatible storage with public access:

```typescript
import { Repository } from '@earthmover/icechunk'
import { createFetchStorage } from '@earthmover/icechunk/fetch-storage'
import * as zarr from 'zarrita'

const storage = createFetchStorage(
  'https://my-bucket.s3.amazonaws.com/my-repo.icechunk'
)
const repo = await Repository.open(storage)
const session = await repo.readonlySession({ branch: 'main' })

// Read data with zarrita
const root = zarr.root(session.store)
const arr = await zarr.open(root.resolve('/temperature'), { kind: 'array' })
const data = await zarr.get(arr)
```

#### Custom storage

For full control, implement the `StorageBackend` interface. Callbacks use the Node.js error-first
convention: the first argument is always `null`, and the actual arguments follow.

```typescript
import { Storage } from '@earthmover/icechunk'

const storage = Storage.newCustom({
  canWrite: async (_err) => true,
  getObjectRange: async (_err, { path, rangeStart, rangeEnd }) => {
    // Fetch bytes from your storage
    return { data, version: { etag } }
  },
  putObject: async (_err, { path, data, previousVersion }) => {
    // Write bytes to your storage, honoring previousVersion (see below)
    return { kind: 'updated', newVersion: { etag } }
  },
  copyObject: async (_err, { from, to, version }) => { /* ... */ },
  listObjects: async (_err, prefix) => [ /* { id, createdAt, sizeBytes } */ ],
  deleteBatch: async (_err, { prefix, batch }) => ({
    deletedObjects: 0,
    deletedBytes: 0,
  }),
  getObjectLastModified: async (_err, path) => new Date(),
  getObjectConditional: async (_err, { path, previousVersion }) => { /* ... */ },
})
```

To support writes, `putObject` must honor `previousVersion`: with no `previousVersion`, overwrite;
with one that has neither `etag` nor `generation`, create only if the object is missing; otherwise
write only if the object's ETag still matches. Return `{ kind: 'not_on_latest_version' }` when the
condition fails. Icechunk relies on this to update branches safely. Against S3, that means sending
`If-None-Match: *` or `If-Match` and mapping a `412` response to `not_on_latest_version`. A bucket
read from the browser must expose the `ETag` header through its CORS configuration.

### Bundler setup

The browser build loads its `.wasm` file on import, from next to its JavaScript module, using
top-level `await`. Vite and webpack 5 handle this. With Vite, exclude the package from dependency
optimization:

```typescript
// vite.config.ts
export default {
  optimizeDeps: { exclude: ['@earthmover/icechunk'] },
}
```

To load the `.wasm` yourself, for example with esbuild, in a service worker, with server-side
rendering, or under a strict Content Security Policy, import `@earthmover/icechunk/web/init` and
call `init()` (or `initSync()`) once before anything else:

```typescript
import { init, Repository, Storage } from '@earthmover/icechunk/web/init'

await init({ module_or_path: '/assets/icechunk_js_bg.wasm' })
```

A Content Security Policy needs `script-src 'wasm-unsafe-eval'`, and the server must send `.wasm`
files as `application/wasm`.

## Examples

- **[Precipitate](https://github.com/earth-mover/precipitate)** ([live demo](https://precipitate.earthmover-sandbox.workers.dev/)) — Browser app for viewing MRMS precipitation data from a [dynamical.org](https://dynamical.org/) Icechunk repository using WebAssembly, React, and Vite deployed to Cloudflare Workers.
- **[Node.js CLI](https://github.com/earth-mover/icechunk/tree/main/icechunk-js/examples/node-cli)** — Create repositories, manage branches/tags, and browse commit history from the command line.
- **[React + WASM](https://github.com/earth-mover/icechunk/tree/main/icechunk-js/examples/react-wasm)** — Interactive browser app demonstrating in-memory repository management with WebAssembly, React, and Vite
