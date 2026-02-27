import { useState } from 'react'

export function StoreContents({
  keys,
  onAddGroup,
  onAddArray,
  onDeleteKey,
}: {
  keys: string[]
  onAddGroup: (name: string) => void
  onAddArray: (name: string, shape: number[]) => void
  onDeleteKey: (key: string) => void
}) {
  const [groupName, setGroupName] = useState('')
  const [arrayName, setArrayName] = useState('')
  const [arrayShape, setArrayShape] = useState('10')

  const handleAddGroup = () => {
    const name = groupName.trim()
    if (!name) return
    onAddGroup(name)
    setGroupName('')
  }

  const handleAddArray = () => {
    const name = arrayName.trim()
    if (!name) return
    const shape = arrayShape.split(',').map((s) => parseInt(s.trim(), 10))
    if (shape.some(isNaN)) return
    onAddArray(name, shape)
    setArrayName('')
  }

  return (
    <div className="flex-1">
      <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wide mb-3">Store Contents</h3>

      <div className="flex gap-2 mb-2">
        <input
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500 flex-1"
          placeholder="Group name"
          value={groupName}
          onChange={(e) => setGroupName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleAddGroup()}
        />
        <button onClick={handleAddGroup} className="bg-gray-700 hover:bg-gray-600 text-white px-3 py-1 rounded text-sm">
          + Group
        </button>
      </div>

      <div className="flex gap-2 mb-4">
        <input
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500 flex-1"
          placeholder="Array name"
          value={arrayName}
          onChange={(e) => setArrayName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleAddArray()}
        />
        <input
          className="bg-gray-900 border border-gray-700 rounded px-2 py-1 text-sm focus:outline-none focus:border-blue-500 w-24"
          placeholder="Shape"
          value={arrayShape}
          onChange={(e) => setArrayShape(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleAddArray()}
        />
        <button onClick={handleAddArray} className="bg-gray-700 hover:bg-gray-600 text-white px-3 py-1 rounded text-sm">
          + Array
        </button>
      </div>

      {keys.length === 0 ? (
        <p className="text-gray-500 text-sm">No keys in store</p>
      ) : (
        <ul className="space-y-1">
          {keys.map((key) => (
            <li key={key} className="flex items-center justify-between bg-gray-900 rounded px-3 py-1.5 text-sm font-mono group">
              <span>{key}</span>
              <button
                onClick={() => onDeleteKey(key)}
                className="text-gray-600 hover:text-red-400 opacity-0 group-hover:opacity-100 transition-opacity text-xs"
              >
                delete
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
