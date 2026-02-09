import test from 'ava'
import assert from 'node:assert/strict'

import { Storage, Repository } from '../index'

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
