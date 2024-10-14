# Icechunk Documentation Website

Built with [MkDocs](https://www.mkdocs.org/) using [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Developing

### Prerequisites

This repository uses [Poetry](https://python-poetry.org/) to manage dependencies

1. Install dependencies using `poetry install`

### Running

1. Run `poetry shell` from the `/docs` directory
2. Start the MkDocs development server: `mkdocs serve`

!!! tip 
    You can use the optional `--dirty` flag to only rebuild changed files, although you may need to restart if you make changes to `mkdocs.yaml`.

### Building

1. Run `mkdocs build`

Builds output to: `icechunk-docs/.site` directory.


### Deploying

Docs are automatically deployed upon commits to `main` branch via the `./github/workflows/deploy-docs.yaml` action.

You can manually deploy by running the command `mkdocs gh-deploy --force` from the directory containing the `mkdocs.yml` file.

## Dev Notes

#### Symlinked Files

Several directories and files are symlinked into the MkDocs' `/docs`[^1] directory in order to be made available to MkDocs. Avoid modifying them directly: 
    * `/docs/icechunk-python/examples/`
    * `/docs/icechunk-python/notebooks/`
    * `/docs/spec.md`

These are also ignored in `.gitignore`

!!! tip 
    See [icechunk-docs/macros.py](./macros.py) for more info.

[^1]: Disambiguation: `icechunk/docs/docs`