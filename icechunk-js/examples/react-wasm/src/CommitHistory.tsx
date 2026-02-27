import type { Snapshot } from './types'

export function CommitHistory({
  snapshots,
  tags,
}: {
  snapshots: Snapshot[]
  tags: string[]
}) {
  return (
    <div className="w-72">
      {tags.length > 0 && (
        <div className="mb-6">
          <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wide mb-2">Tags</h3>
          <div className="flex flex-wrap gap-1">
            {tags.map((t) => (
              <span key={t} className="bg-orange-900/50 text-orange-300 px-2 py-0.5 rounded text-xs">{t}</span>
            ))}
          </div>
        </div>
      )}

      <h3 className="text-sm font-semibold text-gray-400 uppercase tracking-wide mb-2">Commits</h3>
      {snapshots.length === 0 ? (
        <p className="text-gray-500 text-sm">No commits yet</p>
      ) : (
        <ul className="space-y-2">
          {snapshots.map((s) => (
            <li key={s.id} className="bg-gray-900 rounded px-3 py-2">
              <div className="text-sm">{s.message}</div>
              <div className="text-xs text-gray-500 font-mono mt-0.5">
                <span className="text-purple-400">{s.branch}</span> {s.id.slice(0, 16)}...
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
