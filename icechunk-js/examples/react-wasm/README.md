# Icechunk React WASM Example

An interactive web UI demonstrating [Icechunk](https://github.com/earth-mover/icechunk) running entirely in the browser via WASM. Create in-memory repositories, manage Zarr groups and arrays, commit changes, and work with branches and tags — all client-side.

## Features

- Create and manage multiple in-memory Icechunk repositories
- Add/delete Zarr groups and arrays in the data store
- Commit changes, create branches, and tag snapshots
- View commit history and switch between branches

## Getting Started

```bash
npm install
npm run dev
```

## Tech Stack

- **React 19** + **TypeScript**
- **Vite** with WASM and top-level await plugins
- **Tailwind CSS v4**
- **@earthmover/icechunk** (WASM build)
