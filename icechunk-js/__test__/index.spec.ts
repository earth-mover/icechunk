import test from 'ava'
import assert from 'node:assert/strict'

import { Storage, Repository, RepositoryConfig } from '../index'

test('repository lifecycle', async (t) => {
  const storage = await Storage.newInMemory()

  // create
  const repo = await Repository.create(storage)
  assert(repo)

  // open
  const repo2 = await Repository.open(storage)
  assert(repo2)

  // openOrCreate on existing
  const repo3 = await Repository.openOrCreate(storage)
  assert(repo3)

  // openOrCreate on fresh storage
  const freshStorage = await Storage.newInMemory()
  const repo4 = await Repository.openOrCreate(freshStorage)
  assert(repo4)

  t.pass()
})

test('session and store operations', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  // writable session properties
  const session = await repo.writableSession('main')
  assert.equal(session.readOnly, false)
  assert.equal(session.branch, 'main')
  assert.equal(session.hasUncommittedChanges, false)
  assert(session.snapshotId)

  // store capabilities
  const store = session.store
  assert.equal(store.supportsWrites, true)
  assert.equal(store.supportsDeletes, true)
  assert.equal(store.supportsListing, true)

  // set, get, exists
  const data = Buffer.from('{"zarr_format":3,"node_type":"group"}')
  await store.set('zarr.json', data)
  const result = await store.get('zarr.json')
  assert(result)
  assert.equal(result!.toString(), '{"zarr_format":3,"node_type":"group"}')
  assert.equal(await store.exists('zarr.json'), true)

  // get nonexistent returns null
  assert.equal(await store.get('nogroup/zarr.json'), null)
  assert.equal(await store.exists('nogroup/zarr.json'), false)

  // list
  const keys = await store.list()
  assert(keys.length > 0)

  // delete
  await store.delete('zarr.json')
  assert.equal(await store.exists('zarr.json'), false)

  // discard changes
  await store.set('zarr.json', data)
  assert.equal(session.hasUncommittedChanges, true)
  await session.discardChanges()
  assert.equal(session.hasUncommittedChanges, false)

  t.pass()
})

test('commit, branches, and tags', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  // initial branches
  const branches = await repo.listBranches()
  assert(branches.includes('main'))

  // commit data
  const session = await repo.writableSession('main')
  await session.store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('initial commit')
  assert(snapshotId)
  assert.equal(typeof snapshotId, 'string')

  // create and list branches
  await repo.createBranch('dev', snapshotId)
  const branches2 = await repo.listBranches()
  assert(branches2.includes('main'))
  assert(branches2.includes('dev'))

  // create and list tags
  await repo.createTag('v1.0', snapshotId)
  const tags = await repo.listTags()
  assert(tags.includes('v1.0'))

  // reopen and verify data persisted
  const repo2 = await Repository.open(storage)
  const readSession = await repo2.readonlySession({ branch: 'main' })
  assert.equal(readSession.readOnly, true)
  const result = await readSession.store.get('zarr.json')
  assert(result)
  assert.equal(result!.toString(), '{"zarr_format":3,"node_type":"group"}')

  // readonly session from tag
  const tagSession = await repo2.readonlySession({ tag: 'v1.0' })
  assert.equal(tagSession.readOnly, true)
  const tagResult = await tagSession.store.get('zarr.json')
  assert(tagResult)
  assert.equal(tagResult!.toString(), '{"zarr_format":3,"node_type":"group"}')

  t.pass()
})

test('create with repository config', async (t) => {
  const storage = await Storage.newInMemory()
  const config: RepositoryConfig = {
    inlineChunkThresholdBytes: 1024,
    getPartialValuesConcurrency: 20,
    maxConcurrentRequests: 128,
    compression: { level: 3 },
    caching: {
      numSnapshotNodes: 100,
      numChunkRefs: 500,
      numTransactionChanges: 50,
      numBytesAttributes: 10000,
      numBytesChunks: 100000,
    },
    storage: {
      retries: { maxTries: 5, initialBackoffMs: 100, maxBackoffMs: 5000 },
    },
  }

  const repo = await Repository.create(storage, config)
  assert(repo)

  // write some data and commit to verify the repo is functional
  const session = await repo.writableSession('main')
  await session.store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('test config')
  assert(snapshotId)

  t.pass()
})

test('create with manifest preload config', async (t) => {
  const storage = await Storage.newInMemory()
  const config: RepositoryConfig = {
    manifest: {
      preload: {
        max_total_refs: 1000,
        preload_if: {
          and: [
            "true",
            { name_matches: { regex: "foo" } },
          ],
        },
        max_arrays_to_scan: 10,
      },
    },
  }

  const repo = await Repository.create(storage, config)
  assert(repo)

  const session = await repo.writableSession('main')
  await session.store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('with preload config')
  assert(snapshotId)

  t.pass()
})

test('create with manifest splitting config', async (t) => {
  const storage = await Storage.newInMemory()

  // Split every 3 chunks along any dimension for any array
  const config: RepositoryConfig = {
    inlineChunkThresholdBytes: 0,
    manifest: {
      splitting: {
        split_sizes: [
          [
            { path_matches: { regex: ".*" } },
            [{ condition: "Any", num_chunks: 3 }],
          ],
        ],
      },
    },
  }

  const repo = await Repository.create(storage, config)
  assert(repo)

  const session = await repo.writableSession('main')
  const store = session.store

  // Create a zarr v3 group
  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))

  // Create a 1D zarr v3 array with shape [10], chunks [1] => 10 chunks
  const arrayMeta = JSON.stringify({
    zarr_format: 3,
    node_type: 'array',
    shape: [10],
    data_type: 'int32',
    chunk_grid: { name: 'regular', configuration: { chunk_shape: [1] } },
    chunk_key_encoding: { name: 'default', configuration: { separator: '/' } },
    fill_value: 0,
    codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
  })
  await store.set('temperature/zarr.json', Buffer.from(arrayMeta))

  // Write 10 chunks (each 4 bytes for int32)
  const chunkData = Buffer.alloc(4)
  for (let i = 0; i < 10; i++) {
    chunkData.writeInt32LE(i)
    await store.set(`temperature/c/${i}`, chunkData)
  }

  const snapshotId = await session.commit('with splitting config')
  assert(snapshotId)

  // With 10 chunks split every 3, we expect ceil(10/3) = 4 manifests
  const manifests = await repo.lookupManifestFiles(snapshotId)
  assert.equal(manifests.length, 4)

  // Verify we can read back a chunk
  const readSession = await repo.readonlySession({ branch: 'main' })
  const result = await readSession.store.get('temperature/c/5')
  assert(result)

  t.pass()
})

test('create with full manifest config', async (t) => {
  const storage = await Storage.newInMemory()
  // Mirrors the Python test_can_change_deep_config_values
  const config: RepositoryConfig = {
    inlineChunkThresholdBytes: 5,
    getPartialValuesConcurrency: 42,
    maxConcurrentRequests: 10,
    compression: { level: 2 },
    caching: { numChunkRefs: 8 },
    manifest: {
      preload: {
        max_total_refs: 42,
        preload_if: {
          and: [
            "true",
            { name_matches: { regex: "foo" } },
          ],
        },
      },
      splitting: {
        split_sizes: [
          [
            { name_matches: { regex: "temperature" } },
            [{ condition: { DimensionName: "longitude" }, num_chunks: 3 }],
          ],
        ],
      },
    },
  }

  const repo = await Repository.create(storage, config)
  assert(repo)

  // verify the repo is functional with this config
  const session = await repo.writableSession('main')
  await session.store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('full config')
  assert(snapshotId)

  // reopen with the same config
  const repo2 = await Repository.open(storage, config)
  assert(repo2)
  const readSession = await repo2.readonlySession({ branch: 'main' })
  const result = await readSession.store.get('zarr.json')
  assert(result)
  assert.equal(result!.toString(), '{"zarr_format":3,"node_type":"group"}')

  t.pass()
})

test('reopen with different manifest config', async (t) => {
  // Mirrors test_manifest_overwrite_splitting_config_on_read
  const storage = await Storage.newInMemory()

  const config1: RepositoryConfig = {
    manifest: {
      splitting: {
        split_sizes: [
          [
            { name_matches: { regex: "temperature" } },
            [{ condition: { DimensionName: "longitude" }, num_chunks: 3 }],
          ],
        ],
      },
    },
  }

  const repo = await Repository.create(storage, config1)
  assert(repo)

  const session = await repo.writableSession('main')
  await session.store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  await session.commit('initial')

  // reopen with different splitting config
  const config2: RepositoryConfig = {
    manifest: {
      splitting: {
        split_sizes: [
          [
            { name_matches: { regex: "temperature" } },
            [{ condition: { DimensionName: "longitude" }, num_chunks: 10 }],
          ],
        ],
      },
    },
  }

  const repo2 = await Repository.open(storage, config2)
  assert(repo2)

  const session2 = await repo2.writableSession('main')
  await session2.store.set('data/zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  await session2.commit('with new config')

  t.pass()
})

test('invalid manifest config gives error', async (t) => {
  const storage = await Storage.newInMemory()

  const config: RepositoryConfig = {
    manifest: {
      preload: {
        // invalid: preload_if expects a condition, not a number
        preload_if: 12345 as any,
      },
    },
  }

  await t.throwsAsync(
    async () => { await Repository.create(storage, config) },
    { message: /.*/ },
  )
})
