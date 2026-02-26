# Cargo profile: override with `just profile=ci test` (default: dev)
profile := "dev"

alias fmt := format
alias pre := pre-commit

[doc("Run all Rust tests via cargo-nextest")]
test *args='':
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --all-targets {{args}}

[doc("Run Rust doc tests only")]
doctest *args='':
  cargo test --workspace --profile {{profile}} --doc {{args}}

[doc("Run all Rust tests with RUST_LOG enabled (e.g. `just test-logs debug`)")]
test-logs level *args='':
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && RUST_LOG=icechunk={{level}} cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --all-targets {{args}} -- --nocapture

[doc("Compile tests without running them")]
compile-tests *args='':
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-run --cargo-profile {{profile}} --workspace --all-targets {{args}}

[doc("Build the Rust workspace (debug by default, override with `just profile=ci build`)")]
build *args='':
  cargo build --profile {{profile}} {{args}}

[doc("Build the Rust workspace in release mode")]
build-release *args='':
  cargo build --release {{args}}

[doc("Run clippy lints on all features")]
lint *args='':
  cargo clippy --profile {{profile}} --all-features {{args}}

[doc("Format all Rust files (pass `--check` to verify only)")]
format *args='':
  cargo fmt --all {{args}}

[doc("Format all Nix files with alejandra")]
format-nix *args='':
  alejandra .

[doc("Check dependencies for security/license issues via cargo-deny")]
check-deps *args='':
  cargo deny --all-features check {{args}}

[doc("Run all Rust examples (skips limits_chunk_refs)")]
run-all-examples:
  for example in icechunk/examples/*.rs; do if [[ ${example} =~ "limits_chunk_refs" ]]; then continue; fi; cargo run --profile {{profile}} --example "$(basename "${example%.rs}")"; done

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
  pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=short {{args}}

[doc("Run ruff formatter on Python code")]
ruff-format *args:
  ruff format

[doc("Run ruff linter on Python code (pass `--fix` for auto-fix)")]
ruff *args:
  ruff check --show-fixes icechunk-python/ {{args}}

[doc("Run mypy type checking on Python code")]
mypy *args:
  cd icechunk-python && mypy python tests {{args}}

[doc("Run all Python pre-commit hooks (ruff, formatting, codespell, etc.)")]
py-pre-commit $SKIP="rust-pre-commit-fast,rust-pre-commit,rust-pre-commit-ci" *args:
  prek run --all-files

[doc("Run Python tests via pytest")]
pytest *args:
  cd icechunk-python && pytest {{args}}

[doc("Start MkDocs dev server with live reload")]
docs-serve *args:
  mkdocs serve -f icechunk-python/docs/mkdocs.yml --livereload {{args}}

[doc("Build MkDocs static site")]
docs-build *args:
  mkdocs build -f icechunk-python/docs/mkdocs.yml {{args}}

[doc("Run all Python and Rust checks")]
all-checks:
  just pytest
  just py-pre-commit
  just mypy
  just ruff
  just ruff-format
  just pre-commit-python
  just pre-commit-ci
