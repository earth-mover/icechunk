import type { Storage, Repository } from '@earthmover/icechunk'

export type Snapshot = {
  id: string
  message: string
  branch: string
}

export type RepoEntry = {
  storage: Storage
  repo: Repository
  snapshots: Snapshot[]
}
