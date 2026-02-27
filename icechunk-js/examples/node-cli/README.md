# Icechunk Node Native Example

A CLI for inspecting Icechunk repositories stored in Minio (S3-compatible) on `localhost:9000`.

## Prerequisites

- Node.js >= 22
- Minio running on port 9000 (see `compose.yaml` in the repo root)

## Usage

```bash
npm install

# Create a new repo with sample groups and arrays
node --experimental-strip-types main.ts create

# List branches
node --experimental-strip-types main.ts list-branches <prefix>

# List tags
node --experimental-strip-types main.ts list-tags <prefix>

# List commits
node --experimental-strip-types main.ts list-commits <prefix>
```
