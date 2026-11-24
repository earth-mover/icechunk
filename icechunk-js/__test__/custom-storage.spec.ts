import test from 'ava'

import { Repository, Storage } from './binding'
import type { StorageVersionInfo } from './binding'

const GROUP = new TextEncoder().encode('{"zarr_format":3,"node_type":"group"}')

/**
 * An object store kept in a JS Map, implementing the conditional-write rules
 * icechunk relies on to update branches safely:
 *  - no `previousVersion`: overwrite
 *  - a `previousVersion` with no etag/generation: create only if missing
 *  - otherwise: write only if the etag still matches
 * A failed condition is `not_on_latest_version`. This is what a fetch backend
 * talking to S3 (or to a proxy in front of it) must reproduce.
 */
function memoryBackend({ raceUpdates = false } = {}) {
  const objects = new Map<string, { data: Uint8Array; etag: string; modified: Date }>()
  const stats = { conditionalPuts: 0, rejectedPuts: 0 }
  // With `raceUpdates`, the first conditional update waits for a second one,
  // so two writers both read the same version before either writes.
  let releaseFirstUpdate: (() => void) | undefined
  const barrier = async (previous: StorageVersionInfo | null | undefined) => {
    if (!raceUpdates || previous?.etag == null) return
    if (releaseFirstUpdate) return releaseFirstUpdate()
    await new Promise<void>((resolve) => {
      releaseFirstUpdate = resolve
      setTimeout(resolve, 5000)
    })
  }
  let nextEtag = 0
  const missing = (path: string) => new Error(`ObjectNotFound: ${path}`)
  const write = (path: string, data: Uint8Array) => {
    const etag = `"${++nextEtag}"`
    objects.set(path, { data: new Uint8Array(data), etag, modified: new Date() })
    return { kind: 'updated', newVersion: { etag } }
  }
  const conflicts = (path: string, previous: StorageVersionInfo | null | undefined) => {
    if (previous == null) return false
    stats.conditionalPuts++
    const current = objects.get(path)
    const failed =
      previous.etag == null && previous.generation == null ? current !== undefined : current?.etag !== previous.etag
    if (failed) stats.rejectedPuts++
    return failed
  }

  const backend = {
    canWrite: async () => true,
    getObjectRange: async (
      _err: null,
      { path, rangeStart, rangeEnd }: { path: string; rangeStart?: number | null; rangeEnd?: number | null },
    ) => {
      const object = objects.get(path)
      if (!object) throw missing(path)
      return {
        data: object.data.slice(rangeStart ?? 0, rangeEnd ?? object.data.length),
        version: { etag: object.etag },
      }
    },
    putObject: async (
      _err: null,
      { path, data, previousVersion }: { path: string; data: Uint8Array; previousVersion?: StorageVersionInfo | null },
    ) => {
      await barrier(previousVersion)
      return conflicts(path, previousVersion) ? { kind: 'not_on_latest_version' } : write(path, data)
    },
    copyObject: async (
      _err: null,
      { from, to, version }: { from: string; to: string; version: StorageVersionInfo },
    ) => {
      const source = objects.get(from)
      if (!source) throw missing(from)
      return source.etag !== version.etag ? { kind: 'not_on_latest_version' } : write(to, source.data)
    },
    listObjects: async (_err: null, prefix: string) =>
      [...objects.entries()]
        .filter(([path]) => path.startsWith(prefix))
        .map(([path, o]) => ({ id: path.slice(prefix.length), createdAt: o.modified, sizeBytes: o.data.length })),
    deleteBatch: async (
      _err: null,
      { prefix, batch }: { prefix: string; batch: Array<{ id: string; size: number }> },
    ) => {
      let deletedObjects = 0
      let deletedBytes = 0
      for (const { id, size } of batch) {
        if (objects.delete(prefix + id)) {
          deletedObjects++
          deletedBytes += size
        }
      }
      return { deletedObjects, deletedBytes }
    },
    getObjectLastModified: async (_err: null, path: string) => {
      const object = objects.get(path)
      if (!object) throw missing(path)
      return object.modified
    },
    getObjectConditional: async (
      _err: null,
      { path, previousVersion }: { path: string; previousVersion?: StorageVersionInfo | null },
    ) => {
      const object = objects.get(path)
      if (!object) throw missing(path)
      if (previousVersion?.etag === object.etag) return { kind: 'on_latest_version' }
      return { kind: 'modified', data: object.data, newVersion: { etag: object.etag } }
    },
  }
  const storage = () => Storage.newCustom(backend as unknown as Parameters<typeof Storage.newCustom>[0])
  return { storage, stats, objects }
}

test('writes, commits and reopens through a JS storage', async (t) => {
  const backend = memoryBackend()
  const repo = await Repository.create(backend.storage())
  const session = await repo.writableSession('main')
  await session.store.set('zarr.json', GROUP)
  const snapshot = await session.commit('from JS storage')
  t.true(backend.objects.size > 0)

  // a fresh Storage and Repository over the same objects
  const reopened = await Repository.open(backend.storage())
  t.is(await reopened.lookupBranch('main'), snapshot)
  const readonly = await reopened.readonlySession({ branch: 'main' })
  t.deepEqual(await readonly.store.get('zarr.json'), GROUP)
  const ancestry = await reopened.ancestry({ branch: 'main' })
  t.deepEqual(
    ancestry.map((s) => s.message),
    ['from JS storage', 'Repository initialized'],
  )
})

test('a commit on a stale session is a conflict, and rebasing resolves it', async (t) => {
  const backend = memoryBackend()
  await Repository.create(backend.storage())
  // two writers that do not share any in-memory state
  const [a, b] = await Promise.all([Repository.open(backend.storage()), Repository.open(backend.storage())])
  const sessionA = await a.writableSession('main')
  const sessionB = await b.writableSession('main')
  await sessionA.store.set('zarr.json', GROUP)
  await sessionA.store.set('a/zarr.json', GROUP)
  await sessionB.store.set('b/zarr.json', GROUP)

  await sessionA.commit('a')
  await t.throwsAsync(sessionB.commit('b'))
  await sessionB.rebase()
  await sessionB.commit('b')

  const latest = await (await Repository.open(backend.storage())).readonlySession({ branch: 'main' })
  t.deepEqual(await latest.store.get('a/zarr.json'), GROUP)
  t.deepEqual(await latest.store.get('b/zarr.json'), GROUP)
  t.true(backend.stats.conditionalPuts > 0, 'branch updates must be conditional writes')
})

test('when two commits race, the storage rejects one write and only one commit lands', async (t) => {
  const backend = memoryBackend({ raceUpdates: true })
  await Repository.create(backend.storage())
  const [a, b] = await Promise.all([Repository.open(backend.storage()), Repository.open(backend.storage())])
  const sessions = await Promise.all([a.writableSession('main'), b.writableSession('main')])
  await sessions[0].store.set('zarr.json', GROUP)
  await sessions[1].store.set('zarr.json', GROUP)

  const results = await Promise.allSettled(sessions.map((s, i) => s.commit(`writer ${i}`)))
  t.true(backend.stats.rejectedPuts > 0, 'the storage must have rejected the losing write')
  const landed = results.flatMap((r) => (r.status === 'fulfilled' ? [r.value] : []))
  t.is(landed.length, 1, JSON.stringify(results.map((r) => r.status === 'rejected' && String(r.reason))))
  t.is(await (await Repository.open(backend.storage())).lookupBranch('main'), landed[0])
})
