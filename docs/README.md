# Icechunk Documentation Website

Built with [MkDocs](https://www.mkdocs.org/) using [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Developing Locally

### Install Base Conda Environment

```
mamba env create -f docs/doc-env.yml  
mamba activate icechunk-docs
```

### Build Icechunk Python

```
cd icechunk-python
maturin build
pip install "$(ls ../target/wheels/*.whl | head -n 1)[docs]"
```

### Install MkDocs and Plugins

```
pip install \
mkdocs "mkdocs-material[imaging]" \
mkdocs-include-markdown-plugin mkdocs-open-in-new-tab mkdocs-breadcrumbs-plugin \
mkdocs-mermaid2-plugin mkdocs-minify-plugin mkdocs-awesome-pages-plugin \
mkdocs-macros-plugin mkdocs-git-revision-date-localized-plugin mkdocstrings \
mkdocstrings-python mkdocs-jupyter markdown-exec 
```

### Building

```
cd docs
mkdocs build
```

## Deploying

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
