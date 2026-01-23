alias fmt := format
alias pre := pre-commit

# run all tests
test *args='':
  cargo test --all --all-targets {{args}}

doctest *args='':
  cargo test --doc {{args}}

# run all tests with logs enabled
test-logs level *args='':
  RUST_LOG=icechunk={{level}} cargo test --all --all-targets {{args}} -- --nocapture

# compile but don't run all tests
compile-tests *args='':
  cargo test --no-run {{args}}

# build debug version
build *args='':
  cargo build {{args}}

# build release version
build-release *args='':
  cargo build --release {{args}}

# run clippy
lint *args='':
  cargo clippy --all-features {{args}}

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
  for example in icechunk/examples/*.rs; do if [[ ${example} =~ "limits_chunk_refs" ]]; then continue; fi; cargo run --example "$(basename "${example%.rs}")"; done

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
  just compile-tests "--locked"
  just build
  just format "--check"
  just lint "--workspace"
  just doctest
  just test
  just run-all-examples
  just check-deps

pre-commit-python:
  just format "-p icechunk-python"
  just lint "-p icechunk-python"

bench-compare *args:
  pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=short {{args}}

create-deepak-env name:
  mamba create -y -n icechunk-{{name}} python=3.12 ipykernel ipdb
  mamba activate icechunk-{{name}}
  just coiled-ice-create {{name}}

coiled-ice-create version:
  pip install coiled arraylake icechunk=='{{version}}' watermark xarray bokeh
