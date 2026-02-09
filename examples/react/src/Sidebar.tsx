import { useState } from 'react'

export function Sidebar({
  repoNames,
  selectedRepo,
  onSelectRepo,
  onCreateRepo,
}: {
  repoNames: string[]
  selectedRepo: string | null
  onSelectRepo: (name: string) => void
  onCreateRepo: (name: string) => void
}) {
  const [newName, setNewName] = useState('')

  const handleCreate = () => {
    const name = newName.trim()
    if (!name) return
    onCreateRepo(name)
    setNewName('')
  }

  return (
    <div className="w-64 border-r border-gray-800 flex flex-col">
      <div className="p-4 border-b border-gray-800">
        <h1 className="text-lg font-bold mb-3">Icechunk</h1>
        <div className="flex gap-2">
          <input
            className="flex-1 bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500"
            placeholder="Repo name"
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
          />
          <button
            onClick={handleCreate}
            className="bg-blue-600 hover:bg-blue-500 text-white px-3 py-1 rounded text-sm"
          >
            +
          </button>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto">
        {repoNames.length === 0 && (
          <p className="text-gray-500 text-sm p-4">No repos yet</p>
        )}
        {repoNames.map((name) => (
          <button
            key={name}
            onClick={() => onSelectRepo(name)}
            className={`w-full text-left px-4 py-2 text-sm hover:bg-gray-800 ${
              selectedRepo === name ? 'bg-gray-800 text-white' : 'text-gray-400'
            }`}
          >
            {name}
          </button>
        ))}
      </div>
    </div>
  )
}
