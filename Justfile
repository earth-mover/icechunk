# Cargo profile: override with `just profile=ci test` (default: dev)
profile := "dev"

alias fmt := format
alias pre := pre-commit

# run all tests
test *args='':
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --all-targets {{args}}

doctest *args='':
  cargo test --workspace --profile {{profile}} --doc {{args}}

# run all tests with logs enabled
test-logs level *args='':
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && RUST_LOG=icechunk={{level}} cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --all-targets {{args}} -- --nocapture

# compile but don't run all tests
compile-tests *args='':
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-run --cargo-profile {{profile}} --workspace --all-targets {{args}}

# build debug version
build *args='':
  cargo build --profile {{profile}} {{args}}

# build release version
build-release *args='':
  cargo build --release {{args}}

# run clippy
lint *args='':
  cargo clippy --profile {{profile}} --all-features {{args}}

# reformat all rust files
format *args='':
  cargo fmt --all {{args}}

# reformat all nix files
format-nix *args='':
  alejandra .

# run cargo deny to check dependencies
check-deps *args='':
  cargo deny --all-features check {{args}}

run-all-examples:
  for example in icechunk/examples/*.rs; do if [[ ${example} =~ "limits_chunk_refs" ]]; then continue; fi; cargo run --profile {{profile}} --example "$(basename "${example%.rs}")"; done

# fast pre-commit - format and lint only
pre-commit-fast:
  just format
  just lint "--workspace"

# medium pre-commit - includes compilation checks (~2-3 minutes)
pre-commit $RUSTFLAGS="-D warnings -W unreachable-pub -W bare-trait-objects":
  just compile-tests "--locked"
  just build
  just format
  just lint "--workspace"
  just check-deps

# full pre-commit for CI - runs all checks including tests
pre-commit-ci $RUSTFLAGS="-D warnings -W unreachable-pub -W bare-trait-objects":
  just profile=ci compile-tests "--locked"
  just profile=ci build
  just format "--check"
  just profile=ci lint "--workspace"
  just profile=ci doctest
  just profile=ci test
  just profile=ci run-all-examples
  just check-deps

pre-commit-python:
  just format "-p icechunk-python"
  just lint "-p icechunk-python"

bench-compare *args:
  pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=short {{args}}

ruff-format *args:
  ruff format

ruff *args:
  ruff check --show-fixes icechunk-python/ {{args}}

mypy *args:
  cd icechunk-python && mypy python tests {{args}}

py-pre-commit $SKIP="rust-pre-commit-fast,rust-pre-commit,rust-pre-commit-ci" *args:
  prek run --all-files

pytest *args:
  cd icechunk-python && pytest {{args}}

docs-serve *args:
  mkdocs serve -f icechunk-python/docs/mkdocs.yml --livereload {{args}}

docs-build *args:
  mkdocs build -f icechunk-python/docs/mkdocs.yml {{args}}

all-checks:
  just pytest
  just py-pre-commit
  just mypy
  just ruff
  just ruff-format
  just pre-commit-python
  just pre-commit-ci
