import { useState } from 'react'
import { plus100 } from '@earthmover/icechunk'

function App() {
  const [count, setCount] = useState(0)

  return (
    <>
      <h1>Vite + React + Icechunk</h1>
      <p>count: {count}</p>
      <button onClick={() => setCount(plus100(count))}>Plus 100</button>
    </>
  )
}

export default App
