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

### Running

From the `icechunk-python` directory:

```bash
# Install icechunk with docs dependencies
uv sync --group docs

# Start the MkDocs development server
cd ../docs
uv run mkdocs serve
```

> [!TIP]
> You can use the optional `--dirty` flag to only rebuild changed files, although you may need to restart if you make changes to `mkdocs.yaml`.

### Building

From the `docs` directory:

```bash
uv run mkdocs build
```

Builds output to: `docs/.site` directory.

### Deploying

Docs are automatically deployed upon commits to `main` branch via readthedocs
