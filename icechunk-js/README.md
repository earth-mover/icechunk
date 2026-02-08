# Icechunk JS

JavaScript/TypeScript library for Icechunk Zarr Stores

**Status: Early development.** In-memory storage works. Cloud storage backends coming soon.

## Getting Started

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
