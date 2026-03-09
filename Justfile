# Cargo profile: override with `just profile=ci test` (default: dev)
profile := "dev"

set positional-arguments

alias fmt := format
alias pre := pre-commit

[doc("Run all Rust tests via cargo-nextest")]
test *args:
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --lib --bins --tests --examples "$@"

[doc("Run Rust doc tests only")]
doctest *args:
  cargo test --workspace --profile {{profile}} --doc "$@"

[doc("Run all Rust tests with RUST_LOG enabled (e.g. `just test-logs debug`)")]
test-logs level *args:
  shift && export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && RUST_LOG=icechunk={{level}} cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --lib --bins --tests --examples --nocapture "$@"

[doc("Compile tests without running them")]
compile-tests *args:
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-run --cargo-profile {{profile}} --workspace --all-targets "$@"

[doc("Build the Rust workspace (debug by default, override with `just profile=ci build`)")]
build *args:
  cargo build --profile {{profile}} "$@"

[doc("Build the Rust workspace in release mode")]
build-release *args:
  cargo build --release "$@"

[doc("Prepare environment for development")]
develop *args:
  cd icechunk-python && uv run -m maturin_import_hook site install && maturin develop --uv --profile {{profile}} "$@"

[doc("Run clippy lints on all features")]
lint *args:
  cargo clippy --profile {{profile}} --all-features "$@"

[doc("Format all Rust files (pass `--check` to verify only)")]
format *args:
  cargo fmt --all "$@"

[doc("Format all Nix files with alejandra")]
format-nix *args:
  alejandra .

[doc("Check dependencies for security/license issues via cargo-deny")]
check-deps *args:
  cargo deny --all-features check "$@"

[doc("Run all Rust examples (skips limits_chunk_refs, large_manifests)")]
run-all-examples:
  for example in icechunk/examples/*.rs; do case "$example" in *limits_chunk_refs*|*large_manifests*) continue;; esac; cargo run --profile {{profile}} --example "$(basename "${example%.rs}")"; done

[doc("Fast Rust pre-commit: format + lint (~3s)")]
pre-commit-fast:
  just format
  just lint "--workspace"

[doc("Medium Rust pre-commit: compile, build, format, lint, deps (~2-3min)")]
pre-commit $RUSTFLAGS="-D warnings -W unreachable-pub -W bare-trait-objects":
  just compile-tests "--locked"
  just build
  just format
  just lint "--workspace"
  just check-deps

[doc("Full Rust CI pre-commit: all checks including tests and examples (~5+min)")]
pre-commit-ci $RUSTFLAGS="-D warnings -W unreachable-pub -W bare-trait-objects":
  just profile=ci compile-tests "--locked"
  just profile=ci build
  just format "--check"
  just profile=ci lint "--workspace"
  just profile=ci doctest
  just profile=ci test
  just profile=ci run-all-examples
  just check-deps

[doc("Rust format + lint for the icechunk-python crate only")]
pre-commit-python:
  just format "-p icechunk-python"
  just lint "-p icechunk-python"

[doc("Compare pytest-benchmark results")]
bench-compare *args:
  pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=short "$@"

[doc("Run ruff formatter on Python code")]
ruff-format *args:
  ruff format "$@"

[doc("Run ruff linter on Python code (pass `--fix` for auto-fix)")]
ruff *args:
  ruff check --show-fixes icechunk-python/ "$@"

[doc("Run mypy type checking on Python code")]
mypy *args:
  cd icechunk-python && mypy python tests "$@"

[doc("Run mypy stub checking on type stubs")]
stubtest *args:
  cd icechunk-python && python -m mypy.stubtest --ignore-disjoint-bases icechunk._icechunk_python --allowlist stubtest_allowlist.txt "$@"

[doc("Run all Python pre-commit hooks (ruff, formatting, codespell, etc.)")]
py-pre-commit $SKIP="rust-pre-commit-fast,rust-pre-commit,rust-pre-commit-ci" *args:
  prek run --all-files

[doc("Run Python tests via pytest")]
pytest *args:
  cd icechunk-python && pytest "$@"

[doc("Start MkDocs dev server with live reload")]
docs-serve *args:
  mkdocs serve -f icechunk-python/docs/mkdocs.yml --livereload "$@"

[doc("Build MkDocs static site")]
docs-build *args:
  mkdocs build -f icechunk-python/docs/mkdocs.yml "$@"

[doc("Run all Python and Rust checks")]
all-checks:
  just pytest
  just py-pre-commit
  just mypy
  just ruff
  just ruff-format
  just pre-commit-python
  just pre-commit-ci
