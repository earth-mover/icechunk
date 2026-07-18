# Icechunk Documentation Website

Built with [MkDocs](https://www.mkdocs.org/) using [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Developing

### Prerequisites

This repository uses [uv](https://docs.astral.sh/uv/) to manage dependencies.

**System dependencies**: The documentation build requires Cairo graphics library for image processing:

- **macOS**: `brew install cairo`
  - If MkDocs fails to find Cairo, set: `export DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib`
- **Ubuntu/Debian**: `sudo apt-get install libcairo2-dev`
- **Fedora/RHEL**: `sudo dnf install cairo-devel`
- **Windows**: Download from [cairographics.org](https://cairographics.org/download/)

### Building and Running

Please see the ["Building Documentation" section of the Icechunk contributor's guide](https://icechunk.io/en/stable/reference/contributing/#building-documentation) for the most up-to-date build and dev server instructions.


Builds output to: `docs/.site` directory.

### Deploying

Docs are automatically deployed upon commits to `main` branch via readthedocs
