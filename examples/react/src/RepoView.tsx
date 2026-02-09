import type { Session } from '@earthmover/icechunk'
import type { Snapshot } from './types'
import { Toolbar } from './Toolbar.tsx'
import { StoreContents } from './StoreContents.tsx'
import { CommitHistory } from './CommitHistory.tsx'

export function RepoView({
  name,
  session,
  currentBranch,
  branches,
  tags,
  keys,
  snapshots,
  onSwitchBranch,
  onCommit,
  onCreateBranch,
  onCreateTag,
  onAddGroup,
  onAddArray,
  onDeleteKey,
}: {
  name: string
  session: Session
  currentBranch: string
  branches: string[]
  tags: string[]
  keys: string[]
  snapshots: Snapshot[]
  onSwitchBranch: (branch: string) => void
  onCommit: (message: string) => void
  onCreateBranch: (name: string) => void
  onCreateTag: (name: string) => void
  onAddGroup: (name: string) => void
  onAddArray: (name: string, shape: number[]) => void
  onDeleteKey: (key: string) => void
}) {
  return (
    <>
      <div className="p-4 border-b border-gray-800 flex items-center gap-4">
        <h2 className="text-xl font-bold">{name}</h2>
        <select
          value={currentBranch}
          onChange={(e) => onSwitchBranch(e.target.value)}
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none"
        >
          {branches.map((b) => (
            <option key={b} value={b}>{b}</option>
          ))}
        </select>
        {session.hasUncommittedChanges && (
          <span className="text-yellow-400 text-xs">uncommitted changes</span>
        )}
      </div>

      <Toolbar
        onCommit={onCommit}
        onCreateBranch={onCreateBranch}
        onCreateTag={onCreateTag}
      />

      <div className="flex-1 overflow-y-auto p-4 flex gap-6">
        <StoreContents
          keys={keys}
          onAddGroup={onAddGroup}
          onAddArray={onAddArray}
          onDeleteKey={onDeleteKey}
        />
        <CommitHistory snapshots={snapshots} tags={tags} />
      </div>
    </>
  )
}
