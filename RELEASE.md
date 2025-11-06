# How to release new versions

## Update version metadata files

1. (Optional: Iff changes to core icechunk rust crate) Update `icechunk/Cargo.toml` by incrementing the version.
1. Update `icechunk-python/Cargo.toml` by incrementing the version.
1. Update the version of the dependency on the core Rust Icechunk library in `icechunk-python/Cargo.toml`, ensuring that it is consistent with the latest version of the core icechunk rust crate.
1. Update the `Cargo.lock` file, by running `cargo check` from the top-level icechunk directory.
1. Document changes in `Changelog.md`.
1. Commit all changes and make a PR.

## (Optional) Issuing a icechunk rust library release

1. (Optional: Iff changes to core icechunk rust crate) Go to the [`publish rust library` github action](https://github.com/earth-mover/icechunk/actions/workflows/publish-rust-library.yml). Click `Run workflow`, select the branch (`main` for a normal release, `support/v1.x` for a V1 support release), and click the green `Run workflow` button to launch.

## Issuing an icechunk python release

1. Wait until any previous workflow completes.
1. Go to the [`Python CI and library release` github action](https://github.com/earth-mover/icechunk/actions/workflows/python-ci.yaml).
1. Check `Make a PyPI release`.
1. Uncheck `Use git-based version for nightly builds`.
1. Under `Branch to build/release (for manual builds only)` select the branch you want to release (`main` for a normal release, `support/v1.x` for a V1 support release).
1. Click the green `Run workflow` button to launch.
1. After the previous point completes, [create a new release in GitHub](https://github.com/earth-mover/icechunk/releases/new). Ask it to create the tag, and generate the release notes. This step will notify the community slack of the new release.