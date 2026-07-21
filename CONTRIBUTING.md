# Contributing to Icechunk

👋 Hi! Thanks for your interest in contributing to Icechunk!

Icechunk is an open source (Apache 2.0) project and we welcome contributions of
all kinds. By participating, you agree to abide by our
[Code of Conduct](./CODE_OF_CONDUCT.md).

The authoritative, always-up-to-date contributor guide lives in the docs:
**[icechunk.io/en/latest/contributing](https://icechunk.io/en/latest/contributing/)**.
This file is a short pointer to get you started — see that page for the full
Python, Rust, and documentation workflows.

🤖 Please also review our
[AI Usage Policy](https://icechunk.io/en/latest/ai-policy/), which doubles as our
contribution guidelines, before submitting a pull request.

## Development environment

We use [pixi](https://pixi.prefix.dev/latest/) to manage both the Python and Rust
dependencies so your setup matches CI as closely as possible:

```bash
pixi shell -m icechunk-python/pyproject.toml
just develop
```

`just develop` sets up the `maturin-import-hook` for fast incremental Rust
compilation and does the first build. All build, test, and code-quality tasks are
run through [`just`](https://github.com/casey/just) — run `just --list` to see the
available recipes.

If you'd rather not use pixi, the docs describe alternatives using
[uv](https://docs.astral.sh/uv/), a virtualenv, or conda/mamba. With uv the
equivalent setup is:

```bash
uv sync
uv run -m maturin_import_hook site install
uv run maturin develop --uv
```

## Running the tests

```bash
# Python test suite (Rust changes trigger an incremental rebuild automatically)
just pytest

# Rust test suite
just test
```

Some tests require S3- and Azure-compatible object stores running locally via
Docker (`docker compose up -d` from the repo root). See the
[docs](https://icechunk.io/en/latest/contributing/) for the full details,
including Hypothesis profiles and cross-version tests.

## Code quality

We use a tiered [pre-commit](https://pre-commit.com/) setup:

```bash
just pre-commit-fast   # format + lint, ~3s
just pre-commit        # + compilation and dependency checks
```

## Reporting bugs & requesting features

Bug reports, feature requests, documentation issues, and usage questions all go
through [GitHub Issues](https://github.com/earth-mover/icechunk/issues). Please
use the provided
[issue templates](https://github.com/earth-mover/icechunk/issues/new/choose)
where they apply, and include a minimal reproducible example for bugs.

## Opening a pull request

1. Fork the repo and create a topic branch off `main`.
2. Make your change, adding or updating tests as appropriate.
3. Make sure the relevant test suite and pre-commit checks pass locally.
4. Open a pull request against `main` with a clear description of the change and
   the motivation behind it. Draft PRs are welcome if you'd like early feedback.

A maintainer will review your PR; please respond to review feedback in your own
words (see the AI Usage Policy). Keep changes focused and avoid bundling
unrelated work to ease the review burden.

## Where to ask questions

For discussion and help, join the
[Earthmover Community Slack](https://join.slack.com/t/earthmover-community/shared_invite/zt-2cwje92ir-xU3CfdG8BI~4CJOJy~sceQ),
or open a [GitHub Issue](https://github.com/earth-mover/icechunk/issues).
