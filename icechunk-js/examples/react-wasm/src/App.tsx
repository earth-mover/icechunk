import { useState, useCallback } from 'react'
import { Storage, Repository, type Session, type Store } from '@earthmover/icechunk'
import type { Snapshot, RepoEntry } from './types.ts'
import { Sidebar } from './Sidebar.tsx'
import { RepoView } from './RepoView.tsx'

const encoder = new TextEncoder()

function encode(value: string): Uint8Array {
  return encoder.encode(value)
}

function App() {
  const [repos, setRepos] = useState<Map<string, RepoEntry>>(new Map())
  const [selectedRepo, setSelectedRepo] = useState<string | null>(null)
  const [session, setSession] = useState<Session | null>(null)
  const [currentBranch, setCurrentBranch] = useState('main')
  const [branches, setBranches] = useState<string[]>([])
  const [tags, setTags] = useState<string[]>([])
  const [keys, setKeys] = useState<string[]>([])
  const [snapshots, setSnapshots] = useState<Snapshot[]>([])
  const [status, setStatus] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const showStatus = (msg: string) => {
    setStatus(msg)
    setError(null)
    setTimeout(() => setStatus(null), 3000)
  }

  const showError = (msg: string) => {
    setError(msg)
    setStatus(null)
  }

  const refreshState = useCallback(async (repo: Repository, sess: Session) => {
    const [branchList, tagList, keyList] = await Promise.all([
      repo.listBranches(),
      repo.listTags(),
      sess.store.list(),
    ])
    setBranches(branchList)
    setTags(tagList)
    setKeys(keyList.sort())
  }, [])

  const getEntry = () => selectedRepo ? repos.get(selectedRepo) : undefined

  const handleCreateRepo = async (name: string) => {
    if (repos.has(name)) {
      showError(`Repo "${name}" already exists`)
      return
    }
    try {
      const storage = await Storage.newInMemory()
      const repo = await Repository.create(storage)
      const sess = await repo.writableSession('main')
      const initialSnap: Snapshot = { id: sess.snapshotId, message: 'Repository created', branch: 'main' }
      const entry: RepoEntry = { storage, repo, snapshots: [initialSnap] }
      const next = new Map(repos)
      next.set(name, entry)
      setRepos(next)
      setSelectedRepo(name)
      setSession(sess)
      setCurrentBranch('main')
      setSnapshots([initialSnap])
      await refreshState(repo, sess)
      showStatus(`Created repo "${name}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleSelectRepo = useCallback(async (name: string) => {
    const entry = repos.get(name)
    if (!entry) return
    try {
      const sess = await entry.repo.writableSession('main')
      setSelectedRepo(name)
      setSession(sess)
      setCurrentBranch('main')
      setSnapshots(entry.snapshots)
      await refreshState(entry.repo, sess)
    } catch (e) {
      showError(String(e))
    }
  }, [repos, refreshState])

  const handleSwitchBranch = async (branch: string) => {
    const entry = getEntry()
    if (!entry) return
    try {
      const sess = await entry.repo.writableSession(branch)
      setSession(sess)
      setCurrentBranch(branch)
      await refreshState(entry.repo, sess)
      showStatus(`Switched to branch "${branch}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleCommit = async (message: string) => {
    const entry = getEntry()
    if (!entry || !session) return
    try {
      const snapId = await session.commit(message)
      const snap: Snapshot = { id: snapId, message, branch: currentBranch }
      const updated = [snap, ...entry.snapshots]
      entry.snapshots = updated
      setSnapshots(updated)
      const newSess = await entry.repo.writableSession(currentBranch)
      setSession(newSess)
      await refreshState(entry.repo, newSess)
      showStatus(`Committed: "${message}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleCreateBranch = async (name: string) => {
    const entry = getEntry()
    if (!entry || !session) return
    try {
      await entry.repo.createBranch(name, session.snapshotId)
      setBranches(await entry.repo.listBranches())
      showStatus(`Created branch "${name}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleCreateTag = async (name: string) => {
    const entry = getEntry()
    if (!entry || !session) return
    try {
      await entry.repo.createTag(name, session.snapshotId)
      setTags(await entry.repo.listTags())
      showStatus(`Created tag "${name}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleAddGroup = async (name: string) => {
    if (!session) return
    try {
      const meta = JSON.stringify({ zarr_format: 3, node_type: 'group' })
      await (session.store as Store).set(`${name}/zarr.json`, encode(meta) as never)
      setKeys((await session.store.list()).sort())
      showStatus(`Added group "${name}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleAddArray = async (name: string, shape: number[]) => {
    if (!session) return
    try {
      const meta = JSON.stringify({
        zarr_format: 3,
        node_type: 'array',
        shape,
        data_type: 'float64',
        chunk_grid: { name: 'regular', configuration: { chunk_shape: shape } },
        chunk_key_encoding: { name: 'default', configuration: { separator: '/' } },
        fill_value: 0,
        codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
      })
      await (session.store as Store).set(`${name}/zarr.json`, encode(meta) as never)
      setKeys((await session.store.list()).sort())
      showStatus(`Added array "${name}" with shape [${shape}]`)
    } catch (e) {
      showError(String(e))
    }
  }

  const handleDeleteKey = async (key: string) => {
    if (!session) return
    try {
      await session.store.delete(key)
      setKeys((await session.store.list()).sort())
      showStatus(`Deleted "${key}"`)
    } catch (e) {
      showError(String(e))
    }
  }

  return (
    <div className="flex h-screen bg-gray-950 text-gray-100">
      <Sidebar
        repoNames={Array.from(repos.keys())}
        selectedRepo={selectedRepo}
        onSelectRepo={handleSelectRepo}
        onCreateRepo={handleCreateRepo}
      />

      <div className="flex-1 flex flex-col overflow-hidden">
        {(status || error) && (
          <div className={`px-4 py-2 text-sm ${error ? 'bg-red-900/50 text-red-300' : 'bg-green-900/50 text-green-300'}`}>
            {error || status}
          </div>
        )}

        {!selectedRepo || !session ? (
          <div className="flex-1 flex items-center justify-center text-gray-500">
            Create or select a repo to get started
          </div>
        ) : (
          <RepoView
            name={selectedRepo}
            session={session}
            currentBranch={currentBranch}
            branches={branches}
            tags={tags}
            keys={keys}
            snapshots={snapshots}
            onSwitchBranch={handleSwitchBranch}
            onCommit={handleCommit}
            onCreateBranch={handleCreateBranch}
            onCreateTag={handleCreateTag}
            onAddGroup={handleAddGroup}
            onAddArray={handleAddArray}
            onDeleteKey={handleDeleteKey}
          />
        )}
      </div>
    </div>
  )
}

export default App
