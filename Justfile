alias fmt := format
alias pre := pre-commit

# run all tests
test *args='':
  AWS_ALLOW_HTTP=1 AWS_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=minio123 AWS_SECRET_ACCESS_KEY=minio123 cargo test {{args}}

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
  cargo clippy -p icechunk -p icechunk-python --all-features {{args}}

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
  for example in icechunk/examples/*.rs; do cargo run --example "$(basename "${example%.rs}")"; done

# run all checks that CI actions will run
pre-commit $RUSTFLAGS="-D warnings -W unreachable-pub -W bare-trait-objects": (compile-tests "--locked") build (format "--check") lint test run-all-examples check-deps
