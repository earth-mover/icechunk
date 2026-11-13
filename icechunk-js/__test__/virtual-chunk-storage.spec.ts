import test from 'ava'
import * as zarr from 'zarrita'

import { Repository, Storage } from '../index'

// WASM builds cannot configure virtual chunk containers, so these tests write
// the repository with the native binding only. Reading through a registered
// Storage works the same on both builds; the browser path is exercised manually.
const nativeOnly = process.env.NAPI_RS_FORCE_WASI ? test.skip : test

const PREFIX = 's3://source-bucket/data/'
const AUTH = { [PREFIX]: { type: 'S3' as const, field0: { type: 'Anonymous' as const } } }
const source = new Uint8Array(64).map((_, i) => i)

/** A read-only custom Storage serving `objects`, recording each range read. */
function sourceStorage(objects: Record<string, Uint8Array>, etag = '"v1"') {
  const reads: string[] = []
  const missing = (path: string) => new Error(`ObjectNotFound: ${path}`)
  const storage = Storage.newCustom({
    canWrite: async () => false,
    getObjectRange: async (_err, { path, rangeStart, rangeEnd }) => {
      const data = objects[path]
      if (!data) throw missing(path)
      reads.push(`${path}:${rangeStart}-${rangeEnd}`)
      return { data: data.slice(rangeStart ?? 0, rangeEnd ?? data.length), version: { etag } }
    },
    putObject: async () => { throw new Error('read-only') },
    copyObject: async () => { throw new Error('read-only') },
    listObjects: async () => [],
    deleteBatch: async () => { throw new Error('read-only') },
    getObjectLastModified: async () => new Date(0),
    getObjectConditional: async (_err, { path }) => {
      if (!objects[path]) throw missing(path)
      return { kind: 'modified', data: objects[path], newVersion: { etag } }
    },
  } as Parameters<typeof Storage.newCustom>[0])
  return { storage, reads }
}

async function makeRepo() {
  const storage = await Storage.newInMemory()
  const config = {
    virtualChunkContainers: {
      [PREFIX]: { urlPrefix: PREFIX, store: { type: 'S3' as const, field0: { anonymous: true, allowHttp: false, forcePathStyle: false } } },
    },
  }
  const repo = await Repository.create(storage, config, undefined, AUTH)
  const session = await repo.writableSession('main')
  await zarr.create(zarr.root(session.store).resolve('v'), {
    shape: [16],
    chunkShape: [4],
    dtype: 'uint8',
    codecs: [{ name: 'bytes', configuration: {} }],
  })
  for (let i = 0; i < 4; i++) {
    await session.store.setVirtualRef(`v/c/${i}`, `${PREFIX}nested/f.bin`, 8 + i * 4, 4, 'v1', null, true)
  }
  await session.commit('virtual refs')
  return storage
}

async function readAll(repo: Repository) {
  const session = await repo.readonlySession({ branch: 'main' })
  const arr = await zarr.open(zarr.root(session.store).resolve('v'), { kind: 'array' })
  return Array.from((await zarr.get(arr)).data as Uint8Array)
}

nativeOnly('reads a container from a registered Storage, relative to the prefix', async (t) => {
  const repo = await Repository.open(await makeRepo(), undefined, AUTH)
  const { storage, reads } = sourceStorage({ 'nested/f.bin': source })
  await repo.setVirtualChunkStorage(PREFIX, storage)
  t.deepEqual(await readAll(repo), Array.from(source.slice(8, 24)))
  t.deepEqual(reads.sort(), ['nested/f.bin:12-16', 'nested/f.bin:16-20', 'nested/f.bin:20-24', 'nested/f.bin:8-12'])
})

nativeOnly('registering a Storage does not authorize the container', async (t) => {
  const repo = await Repository.open(await makeRepo())
  const { storage, reads } = sourceStorage({ 'nested/f.bin': source })
  await repo.setVirtualChunkStorage(PREFIX, storage)
  await t.throwsAsync(readAll(repo), { message: /authorize/ })
  t.deepEqual(reads, [])
})

nativeOnly('verifies ETag checksums against the version getObjectRange returns', async (t) => {
  const repo = await Repository.open(await makeRepo(), undefined, AUTH)
  await repo.setVirtualChunkStorage(PREFIX, sourceStorage({ 'nested/f.bin': source }, '"v2"').storage)
  await t.throwsAsync(readAll(repo), { message: /checksum of the object owning the virtual chunk has changed/ })
})

nativeOnly('rejects a prefix that matches no container', async (t) => {
  const repo = await Repository.open(await makeRepo())
  await t.throwsAsync(repo.setVirtualChunkStorage('s3://other/', sourceStorage({}).storage), {
    message: /s3:\/\/other\//,
  })
})
