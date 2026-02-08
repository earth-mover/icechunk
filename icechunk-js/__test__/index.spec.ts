import test from 'ava'

import { Storage, Repository, type Session, type Store } from '../index'

test('create in-memory repository', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)
  t.truthy(repo)
})

test('writable session and commit', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  const session = await repo.writableSession('main')
  t.false(session.readOnly)
  t.is(session.branch, 'main')
  t.false(session.hasUncommittedChanges)
  t.truthy(session.snapshotId)
})

test('store get/set/delete', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)
  const session = await repo.writableSession('main')
  const store = session.store

  t.true(store.supportsWrites)
  t.true(store.supportsDeletes)
  t.true(store.supportsListing)

  // set and get
  const data = Buffer.from('{"zarr_format":3,"node_type":"group"}')
  await store.set('zarr.json', data)

  const result = await store.get('zarr.json')
  t.truthy(result)
  t.is(result!.toString(), '{"zarr_format":3,"node_type":"group"}')

  // exists
  t.true(await store.exists('zarr.json'))
  t.false(await store.exists('nogroup/zarr.json'))

  // get nonexistent returns null
  const missing = await store.get('nogroup/zarr.json')
  t.is(missing, null)
})

test('commit and open workflow', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  // write data and commit
  const session = await repo.writableSession('main')
  const store = session.store
  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('Initial commit')
  t.truthy(snapshotId)
  t.is(typeof snapshotId, 'string')

  // reopen and verify data persisted
  const repo2 = await Repository.open(storage)
  const session2 = await repo2.readonlySession({ branch: 'main' })
  t.true(session2.readOnly)
  const store2 = session2.store
  const result = await store2.get('zarr.json')
  t.truthy(result)
  t.is(result!.toString(), '{"zarr_format":3,"node_type":"group"}')
})

test('list operations', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)
  const session = await repo.writableSession('main')
  const store = session.store

  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))

  const keys = await store.list()
  t.true(keys.length > 0)
})

test('branch operations', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  const branches = await repo.listBranches()
  t.true(branches.includes('main'))

  // commit to get a snapshot id, then create branch
  const session = await repo.writableSession('main')
  const store = session.store
  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('first commit')

  await repo.createBranch('dev', snapshotId)
  const branches2 = await repo.listBranches()
  t.true(branches2.includes('dev'))
})

test('tag operations', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  // commit to get a snapshot id, then create tag
  const session = await repo.writableSession('main')
  const store = session.store
  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('first commit')

  await repo.createTag('v1.0', snapshotId)
  const tags = await repo.listTags()
  t.true(tags.includes('v1.0'))
})

test('openOrCreate creates new repo', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.openOrCreate(storage)
  t.truthy(repo)
})

test('discard changes', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)
  const session = await repo.writableSession('main')
  const store = session.store

  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  t.true(session.hasUncommittedChanges)

  await session.discardChanges()
  t.false(session.hasUncommittedChanges)
})

test('readonly session from tag', async (t) => {
  const storage = await Storage.newInMemory()
  const repo = await Repository.create(storage)

  const session = await repo.writableSession('main')
  const store = session.store
  await store.set('zarr.json', Buffer.from('{"zarr_format":3,"node_type":"group"}'))
  const snapshotId = await session.commit('tagged commit')

  await repo.createTag('v1', snapshotId)

  const readSession = await repo.readonlySession({ tag: 'v1' })
  t.true(readSession.readOnly)
  const readStore = readSession.store
  const result = await readStore.get('zarr.json')
  t.truthy(result)
})
