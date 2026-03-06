import { useState } from 'react'

export function Toolbar({
  onCommit,
  onCreateBranch,
  onCreateTag,
}: {
  onCommit: (message: string) => void
  onCreateBranch: (name: string) => void
  onCreateTag: (name: string) => void
}) {
  const [commitMessage, setCommitMessage] = useState('')
  const [branchName, setBranchName] = useState('')
  const [tagName, setTagName] = useState('')

  const handleCommit = () => {
    const msg = commitMessage.trim()
    if (!msg) return
    onCommit(msg)
    setCommitMessage('')
  }

  const handleBranch = () => {
    const name = branchName.trim()
    if (!name) return
    onCreateBranch(name)
    setBranchName('')
  }

  const handleTag = () => {
    const name = tagName.trim()
    if (!name) return
    onCreateTag(name)
    setTagName('')
  }

  return (
    <div className="p-4 border-b border-gray-800 flex flex-wrap gap-3">
      <div className="flex gap-2">
        <input
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500 w-48"
          placeholder="Commit message"
          value={commitMessage}
          onChange={(e) => setCommitMessage(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleCommit()}
        />
        <button onClick={handleCommit} className="bg-green-700 hover:bg-green-600 text-white px-3 py-1 rounded text-sm">
          Commit
        </button>
      </div>

      <div className="border-l border-gray-700" />

      <div className="flex gap-2">
        <input
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500 w-32"
          placeholder="Branch name"
          value={branchName}
          onChange={(e) => setBranchName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleBranch()}
        />
        <button onClick={handleBranch} className="bg-purple-700 hover:bg-purple-600 text-white px-3 py-1 rounded text-sm">
          Branch
        </button>
      </div>

      <div className="flex gap-2">
        <input
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500 w-32"
          placeholder="Tag name"
          value={tagName}
          onChange={(e) => setTagName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleTag()}
        />
        <button onClick={handleTag} className="bg-orange-700 hover:bg-orange-600 text-white px-3 py-1 rounded text-sm">
          Tag
        </button>
      </div>
    </div>
  )
}
