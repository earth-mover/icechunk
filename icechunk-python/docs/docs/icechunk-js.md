---
title: JavaScript / TypeScript
---

# Icechunk JavaScript / TypeScript

The official [`@earthmover/icechunk`](https://www.npmjs.com/package/@earthmover/icechunk) package provides full read-write access to Icechunk repositories from JavaScript and TypeScript. It uses native Rust bindings (via NAPI-RS) for Node.js and WebAssembly for browser environments.

## Installation

For your default platform:

```bash
npm install @earthmover/icechunk zarrita
```

Explicitly for WASM:

```bash
npm install @earthmover/icechunk --cpu=wasm32 zarrita
```

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

### Node.js (or NAPI compatible runtimes)

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

### Browser (WASM)

In browser environments, native storage backends are not available. Instead, use:

- **In-memory storage** for ephemeral repositories
- **Custom storage** with a JavaScript-provided backend (e.g., using `fetch` or `@aws-sdk/client-s3`)
- **Fetch storage** for read-only access to publicly-accessible repositories over HTTP/HTTPS

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

For full control, implement the `StorageBackend` interface:

```typescript
import { Storage } from '@earthmover/icechunk'

const storage = Storage.newCustom({
  canWrite: async () => true,
  getObjectRange: async ({ path, rangeStart, rangeEnd }) => {
    // Fetch bytes from your storage
    return { data, version: { etag } }
  },
  putObject: async ({ path, data }) => {
    // Write bytes to your storage
    return { kind: 'updated', newVersion: { etag } }
  },
  copyObject: async ({ from, to }) => { /* ... */ },
  listObjects: async (prefix) => [ /* ... */ ],
  deleteBatch: async ({ prefix, batch }) => ({
    deletedObjects: 0,
    deletedBytes: 0,
  }),
  getObjectLastModified: async (path) => new Date(),
  getObjectConditional: async ({ path, previousVersion }) => { /* ... */ },
})
```

### WASM setup

When using the WASM build with a bundler like Vite, you will need the following plugins:

```bash
npm install vite-plugin-wasm vite-plugin-top-level-await
```

```typescript
// vite.config.ts
import wasm from 'vite-plugin-wasm'
import topLevelAwait from 'vite-plugin-top-level-await'

export default {
  plugins: [wasm(), topLevelAwait()],
}
```

## Examples

- **[Precipitate](https://github.com/earth-mover/precipitate)** ([live demo](https://precipitate.earthmover-sandbox.workers.dev/)) — Browser app for viewing MRMS precipitation data from a [dynamical.org](https://dynamical.org/) Icechunk repository using WASM, React, and Vite deployed to Cloudflare Workers.
- **[Node.js CLI](https://github.com/earth-mover/icechunk/tree/main/icechunk-js/examples/node-cli)** — Create repositories, manage branches/tags, and browse commit history from the command line.
- **[React + WASM](https://github.com/earth-mover/icechunk/tree/main/icechunk-js/examples/react-wasm)** — Interactive browser app demonstrating in-memory repository management with WASM, React, and Vite
