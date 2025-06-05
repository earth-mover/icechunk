# How to release new versions

1. If there are Rust changes, update `icechunk/Cargo.toml` increasing version.
1. Update `icechunk-python/Cargo.toml` increasing version. This version can be ahead of the one in `icechunk/Cargo.toml`, that's OK.
1. If there are Rust changes, update the version on the dependency on Rust Icechunk library in `icechunk-python/Cargo.toml`.
1. Run tests to ensure `Cargo.lock` updates and everything works.
1. Document changes in `Changelog.md` and commit.
1. Prepare the desired release on a branch or tag (not the official tag name, this ref can be temporary). We usually use `main` for the release point.
1. Run the [`publish rust library`](https://github.com/earth-mover/icechunk/actions/workflows/publish-rust-library.yml) workflow on the ref you want to release.
1. After the previous point completes, run the [`Python CI and library release`](https://github.com/earth-mover/icechunk/actions/workflows/python-ci.yaml) workflow on the ref you want to release.
1. After the previous point completes, [create a new release in GitHub](https://github.com/earth-mover/icechunk/releases/new). Ask it to create the tag, and generate the release notes. This step will notify the community slack of the new release.
