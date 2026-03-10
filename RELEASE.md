# How to release new versions

## Update version metadata files

1. (Optional: Iff changes to core icechunk rust crate) Update `icechunk/Cargo.toml` by incrementing the version.
1. Update `icechunk-python/Cargo.toml` by incrementing the version.
1. Update the version of the dependency on the core Rust Icechunk library in `icechunk-python/Cargo.toml`, ensuring that it is consistent with the latest version of the core icechunk rust crate.
1. Update the `Cargo.lock` file, by running `cargo check` from the top-level icechunk directory.
1. Document changes in `Changelog.md`.
1. Commit all changes and make a PR.
1. Once CI passes on the PR merge it.

## (Optional) Issuing a icechunk rust library release

1. (Optional: Iff changes to core icechunk rust crate) Go to the [`publish rust library` github action](https://github.com/earth-mover/icechunk/actions/workflows/publish-rust-library.yml). Click `Run workflow`, select the branch (`main` for a normal release, `support/v1.x` for a V1 support release), and click the green `Run workflow` button to launch.

## Issuing an icechunk python release

1. Wait until any previous workflow completes.
1. Go to the [`Python CI and library release` github action](https://github.com/earth-mover/icechunk/actions/workflows/python-ci.yaml).
1. Check `Make a PyPI release`.
1. Uncheck `Use git-based version for nightly builds`.
1. Under `Branch to build/release (for manual builds only)` select the branch you want to release (`main` for a normal release, `support/v1.x` for a V1 support release). Where it asks which branch workflow you want to use the workflow from just use the one from `main`.
1. Click the green `Run workflow` button to launch.
1. After the previous point completes, [create a new release in GitHub](https://github.com/earth-mover/icechunk/releases/new). Ask it to create the tag, and generate the release notes. Once you hit release, this step will upload the new version to [PyPI](https://pypi.org/project/icechunk/), and notify the community slack of the new release.
1. After an hour or so an automated PR should appear to update the [conda-forge feedstock](https://github.com/conda-forge/icechunk-feedstock). Merge that and the new version will appear on conda-forge.

## Understanding the Python CI workflow options

The [`Python CI and library release` workflow](https://github.com/earth-mover/icechunk/actions/workflows/python-ci.yaml) has three inputs that control what gets built and where it goes:

| Input | Purpose |
|-------|---------|
| `pypi_release` | If checked, uploads wheels to PyPI |
| `use_git_version` | If checked, generates version from git tags (e.g., `2.0.0-alpha.0-dev123+gabc1234`). If unchecked, uses version from `Cargo.toml` |
| `branch` | Which branch to build (`main` or `support/v1.x`) |

### Where wheels are uploaded

| Trigger | Destination |
|---------|-------------|
| Scheduled (cron) | Scientific Python nightly only |
| Manual with `pypi_release: false` | Scientific Python nightly only |
| Manual with `pypi_release: true` | PyPI and Scientific Python nightly |

Note: Any manual `workflow_dispatch` trigger will upload to the Scientific Python nightly server, regardless of other settings.

### Common scenarios

**Standard release to PyPI** (e.g., `2.0.0`):

- `pypi_release`: ✅ checked
- `use_git_version`: ❌ unchecked (uses `Cargo.toml` version)

**Alpha/pre-release to PyPI** (e.g., `2.0.0-alpha.0`):

- Same as standard release - just ensure `Cargo.toml` has the alpha version
- `pypi_release`: ✅ checked
- `use_git_version`: ❌ unchecked

**Dev build to PyPI** (e.g., `2.0.0-alpha.0-dev123+gabc1234`):

- `pypi_release`: ✅ checked
- `use_git_version`: ✅ checked
