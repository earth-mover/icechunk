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
uv sync --extra docs

# Start the MkDocs development server
cd ../docs
uv run mkdocs serve
```

Or install in editable mode:

```bash
cd icechunk-python
uv pip install -e ".[docs]"
cd ../docs
mkdocs serve
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

Docs are automatically deployed upon commits to `main` branch via the `./github/workflows/deploy-docs.yaml` action.

You can manually deploy by running the command `mkdocs gh-deploy --force` from the directory containing the `mkdocs.yml` file.

## Dev Notes

#### Symlinked Files

Several directories and files are symlinked into the MkDocs' `/docs`[^1] directory in order to be made available to MkDocs. Avoid modifying them directly:
- `/docs/icechunk-python/examples/`
- `/docs/icechunk-python/notebooks/`
- `/docs/spec.md`

These are also ignored in `.gitignore`

> [!TIP]
> See [icechunk-docs/macros.py](./macros.py) for more info.

[^1]: Disambiguation: `icechunk/docs/docs`
