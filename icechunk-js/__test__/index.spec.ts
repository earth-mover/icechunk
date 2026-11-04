import test from 'ava'
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { cp, mkdtemp, readFile, readdir, rename, rm, stat, unlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'
import * as zarr from 'zarrita'

import { Storage, Repository, RepositoryConfig } from '../index'

const decoder = new TextDecoder()
function decodeBytes(data: Uint8Array): string {
  return decoder.decode(data)
}

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
  assert.equal(decodeBytes(result!), '{"zarr_format":3,"node_type":"group"}')
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
  assert.equal(decodeBytes(result!), '{"zarr_format":3,"node_type":"group"}')

  // readonly session from tag
  const tagSession = await repo2.readonlySession({ tag: 'v1.0' })
  assert.equal(tagSession.readOnly, true)
  const tagResult = await tagSession.store.get('zarr.json')
  assert(tagResult)
  assert.equal(decodeBytes(tagResult!), '{"zarr_format":3,"node_type":"group"}')

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
  assert.equal(decodeBytes(result!), '{"zarr_format":3,"node_type":"group"}')

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

test('zarrita read and write', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  // Write a root group and a 1D float32 array via the store
  const session = await repo.writableSession('main')
  const store = session.store
  const enc = new TextEncoder()

  await store.set('zarr.json', enc.encode(JSON.stringify({
    zarr_format: 3,
    node_type: 'group',
  })))

  await store.set('temperature/zarr.json', enc.encode(JSON.stringify({
    zarr_format: 3,
    node_type: 'array',
    shape: [4],
    data_type: 'float32',
    chunk_grid: { name: 'regular', configuration: { chunk_shape: [4] } },
    chunk_key_encoding: { name: 'default', configuration: { separator: '/' } },
    fill_value: 0,
    codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
  })))

  // Write a chunk with 4 float32 values: [1.5, 2.5, 3.5, 4.5]
  const buf = new ArrayBuffer(16)
  const view = new Float32Array(buf)
  view.set([1.5, 2.5, 3.5, 4.5])
  await store.set('temperature/c/0', new Uint8Array(buf))

  await session.commit('add temperature array')

  // Read back via zarrita
  const readSession = await repo.readonlySession({ branch: 'main' })
  const root = zarr.root(readSession.store)

  const arr = await zarr.open(root.resolve('/temperature'), { kind: 'array' })
  assert.deepEqual(arr.shape, [4])
  assert.equal(arr.dtype, 'float32')

  const data = await zarr.get(arr)
  assert.deepEqual(Array.from(data.data as Float32Array), [1.5, 2.5, 3.5, 4.5])

  // Read a slice
  const slice = await zarr.get(arr, [zarr.slice(1, 3)])
  assert.deepEqual(Array.from(slice.data as Float32Array), [2.5, 3.5])

  t.pass()
})

// js-test-wasi always sets NAPI_RS_FORCE_WASI; filesystem constructors are native-only
const nativeOnly = process.env.NAPI_RS_FORCE_WASI ? test.skip : test
const nativeUnixOnly = process.platform === 'win32' ? test.skip : nativeOnly

nativeUnixOnly('filesystem storage lifecycle', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'icechunk-js-fs-'))
  try {
    const repo = await Repository.create(await Storage.newFilesystem(dir))
    const session = await repo.writableSession('main')
    const store = session.store
    const enc = new TextEncoder()

    await store.set('zarr.json', enc.encode(JSON.stringify({
      zarr_format: 3,
      node_type: 'group',
    })))
    await store.set('temperature/zarr.json', enc.encode(JSON.stringify({
      zarr_format: 3,
      node_type: 'array',
      shape: [4],
      data_type: 'float32',
      chunk_grid: { name: 'regular', configuration: { chunk_shape: [4] } },
      chunk_key_encoding: { name: 'default', configuration: { separator: '/' } },
      fill_value: 0,
      codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
    })))
    await store.set('temperature/c/0', new Uint8Array(new Float32Array([1.5, 2.5, 3.5, 4.5]).buffer))
    await session.commit('add temperature array')

    const reopened = await Repository.open(await Storage.newFilesystem(dir))
    const readSession = await reopened.readonlySession({ branch: 'main' })
    const arr = await zarr.open(zarr.root(readSession.store).resolve('/temperature'), { kind: 'array' })
    const data = await zarr.get(arr)
    assert.deepEqual(Array.from(data.data as Float32Array), [1.5, 2.5, 3.5, 4.5])
  } finally {
    await rm(dir, { recursive: true, force: true })
  }

  t.pass()
})

nativeUnixOnly('filesystem storage conversion changes nothing on its own repositories', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'icechunk-js-fs-'))
  try {
    const storage = await Storage.newFilesystem(dir)
    await t.throwsAsync(
      async () => { await storage.convertToFilesystemLayout() },
      { message: /object not found/ },
    )
    await Repository.create(storage)
    assert.equal(await storage.convertToFilesystemLayout(), false)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }

  t.pass()
})

nativeUnixOnly('filesystem storage converts between the object store and filesystem layouts', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'icechunk-js-fs-'))
  try {
    const legacy = await Repository.create(await Storage.newLocalFilesystem(dir))
    const main = await legacy.lookupBranch('main')

    const storage = await Storage.newFilesystem(dir)
    await t.throwsAsync(
      async () => { await Repository.open(storage) },
      { message: /convert_to_filesystem_layout/ },
    )
    assert.equal(await storage.convertToFilesystemLayout(), true)
    assert.equal(await storage.convertToFilesystemLayout(), false)
    const repo = await Repository.open(storage)
    assert.equal(await repo.lookupBranch('main'), main)

    assert.equal(await storage.convertToObjectStoreLayout(), true)
    assert.equal(await storage.convertToObjectStoreLayout(), false)
    await t.throwsAsync(stat(join(dir, 'repo.d')))
    const objectStoreRepo = await Repository.open(await Storage.newLocalFilesystem(dir))
    await objectStoreRepo.createBranch('dev', main)
    assert.deepEqual((await objectStoreRepo.listBranches()).sort(), ['dev', 'main'])
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

// The version file the marker of repo.d names.
async function currentVersion(dir: string): Promise<string> {
  const [marker] = (await readdir(join(dir, 'repo.d'))).filter((name) => name.startsWith('latest.'))
  const [, , n, h] = marker.split('.')
  return join(dir, 'repo.d', `${n}.${h}`)
}

nativeUnixOnly('filesystem storage publishes a missing mirror', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'icechunk-js-fs-'))
  try {
    const storage = await Storage.newFilesystem(dir)
    await Repository.create(storage)
    await unlink(join(dir, 'repo'))
    const outcome = await storage.publishMirror()
    assert.equal(outcome.current, true)
    assert.equal(outcome.left ?? null, null)
    assert.deepEqual(await readFile(join(dir, 'repo')), await readFile(await currentVersion(dir)))
  } finally {
    await rm(dir, { recursive: true, force: true })
  }

  t.pass()
})

nativeUnixOnly('filesystem storage reports forks, unreadable mirrors and durable commits', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'icechunk-js-fs-'))
  try {
    const root = join(dir, 'root')
    const other = join(dir, 'other')
    const storage = await Storage.newFilesystem(root)
    const repo = await Repository.create(storage)

    await writeFile(join(root, 'repo'), 'not a repo info')
    await t.throwsAsync(
      async () => { await storage.publishMirror() },
      { message: /couldn't be read or decoded/ },
    )

    await Repository.create(await Storage.newLocalFilesystem(other))
    await writeFile(join(root, 'repo'), await readFile(join(other, 'repo')))
    await t.throwsAsync(
      async () => { await storage.publishMirror() },
      { message: /repository forked/ },
    )

    const session = await repo.writableSession('main')
    await session.store.set('zarr.json', new TextEncoder().encode(JSON.stringify({
      zarr_format: 3,
      node_type: 'group',
    })))
    const err = await t.throwsAsync(async () => { await session.commit('lands, then finds the fork') })
    assert(err)
    const snapshot = await repo.lookupBranch('main')
    assert.match(err.message, new RegExp(`snapshot ${snapshot} is durable`))
    assert.match(err.message, /must not be retried/)
    assert.match(err.message, /repository is forked/)
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

nativeUnixOnly('filesystem storage refuses writes during an interrupted conversion to the object store layout', async (t) => {
  const dir = await mkdtemp(join(tmpdir(), 'icechunk-js-fs-'))
  try {
    const root = join(dir, 'root')
    const copy = join(dir, 'copy')
    const storage = await Storage.newFilesystem(root)
    await Repository.create(storage)
    await cp(root, copy, { recursive: true })
    assert.equal(await (await Storage.newFilesystem(copy)).convertToObjectStoreLayout(), true)

    // What a conversion to the object store layout interrupted after its change leaves.
    const featureless = await readFile(join(copy, 'repo'))
    const [marker] = (await readdir(join(root, 'repo.d'))).filter((name) => name.startsWith('latest.'))
    const n = BigInt(marker.split('.')[2]) + 1n
    const h = createHash('sha256').update(featureless).digest('hex')
    await writeFile(join(root, 'repo.d', `${n}.${h}`), featureless)
    const k = (999_999_999_999_999n - n).toString().padStart(15, '0')
    await rename(join(root, 'repo.d', marker), join(root, 'repo.d', `latest.${k}.${n}.${h}`))

    const repo = await Repository.open(await Storage.newFilesystem(root))
    await t.throwsAsync(
      async () => { await repo.createBranch('dev', await repo.lookupBranch('main')) },
      { message: /conversion to the object store layout was interrupted, rerun convert_to_object_store_layout/ },
    )
    await t.throwsAsync(
      async () => { await storage.publishMirror() },
      { message: /rerun convert_to_object_store_layout/ },
    )
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

nativeOnly('repository operations of filesystem storage fail on other storage', async (t) => {
  const storage = await Storage.newInMemory()
  for (const method of ['convertToFilesystemLayout', 'convertToObjectStoreLayout', 'publishMirror'] as const) {
    await t.throwsAsync(
      async () => { await storage[method]() },
      { message: `${method} is only available on storage created with Storage.newFilesystem` },
    )
  }
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

/** Read-only storage over a repository directory, through JS callbacks so it works under WASI. */
function directoryStorage(dir: string): Storage {
  const notFound = (err: unknown, path: string) => {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') throw new Error(`ObjectNotFound: ${path}`)
    throw err
  }
  const read = async (path: string) => {
    try {
      return await readFile(join(dir, path))
    } catch (err) {
      return notFound(err, path)
    }
  }
  const readOnly = async () => {
    throw new Error('read-only storage')
  }
  const version = { etag: 'fixture' }
  return Storage.newCustom({
    canWrite: async () => false,
    getObjectRange: async (_err, { path, rangeStart, rangeEnd }) => {
      const data = await read(path)
      return { data: data.subarray(rangeStart ?? 0, rangeEnd ?? data.length), version }
    },
    putObject: readOnly,
    copyObject: readOnly,
    listObjects: async (_err, prefix) => {
      const base = join(dir, prefix)
      let entries
      try {
        entries = await readdir(base, { recursive: true, withFileTypes: true })
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === 'ENOENT') return []
        throw err
      }
      return Promise.all(
        entries
          .filter((entry) => entry.isFile())
          .map(async (entry) => {
            const full = join(entry.parentPath, entry.name)
            const info = await stat(full)
            return { id: relative(base, full), createdAt: info.mtime, sizeBytes: info.size }
          }),
      )
    },
    deleteBatch: readOnly,
    getObjectLastModified: async (_err, path) => {
      try {
        return (await stat(join(dir, path))).mtime
      } catch (err) {
        return notFound(err, path)
      }
    },
    getObjectConditional: async (_err, { path }) => ({
      kind: 'modified',
      data: await read(path),
      newVersion: version,
    }),
  })
}

// Written by `generate_js_gcs_vcc_fixture` in icechunk/tests/test_config_compliance.rs.
// The WASM build has no GCS support, so it can't decode the container's store settings.
test('repository with a GCS virtual chunk container opens and reads non-virtual data', async (t) => {
  const dir = fileURLToPath(new URL('./data/gcs-vcc-repo', import.meta.url))
  const repo = await Repository.open(directoryStorage(dir))
  const session = await repo.readonlySession({ branch: 'main' })
  const root = zarr.root(session.store)

  const temperature = await zarr.open(root.resolve('/temperature'), { kind: 'array' })
  const data = await zarr.get(temperature)
  assert.deepEqual(Array.from(data.data as Float32Array), [1.5, 2.5, 3.5, 4.5])

  const virtual = await zarr.open(root.resolve('/virtual'), { kind: 'array' })
  // WASM: the store settings don't decode; native: the container isn't authorized
  await assert.rejects(
    async () => { await zarr.get(virtual) },
    /gs:\/\/some-bucket\/data\/ \(container: gcs-data\), (whose store settings this Icechunk library can't decode|to be able to fetch the chunk you need to authorize)/,
  )

  t.pass()
})
