# Icechunk Node Native Example

An example CLI for creating and inspecting Icechunk repositories on the local filesystem.

## Prerequisites

- Node.js >= 22

## Usage

```bash
npm install

# Create a new repo with sample groups and arrays
node --experimental-strip-types main.ts create ./my-repo

# List branches
node --experimental-strip-types main.ts list-branches ./my-repo

# List tags
node --experimental-strip-types main.ts list-tags ./my-repo
```
