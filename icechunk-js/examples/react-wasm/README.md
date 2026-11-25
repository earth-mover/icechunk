# Icechunk React WASM Example

An interactive web UI demonstrating [Icechunk](https://github.com/earth-mover/icechunk) running entirely in the browser via WASM. Create in-memory repositories, manage Zarr groups and arrays, commit changes, and work with branches and tags — all client-side.

## Features

- Create and manage multiple in-memory Icechunk repositories
- Add/delete Zarr groups and arrays in the data store
- Commit changes, create branches, and tag snapshots
- View commit history and switch between branches

## Getting Started

The example uses the `icechunk-js` package in this repository, so build its
browser build first (from the repository root; see the package README for the
toolchain):

```bash
just js-build-browser
```

Then:

```bash
npm install
npm run dev
```

## Tech Stack

- **React 19** + **TypeScript**
- **Vite** (no plugins or cross-origin isolation headers needed)
- **Tailwind CSS v4**
- **@earthmover/icechunk** (its browser build)
