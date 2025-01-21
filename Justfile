alias fmt := format
alias pre := pre-commit

# run all tests
test *args='':
  cargo test --all {{args}}

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
  for example in icechunk/examples/*.rs; do cargo run --example "$(basename "${example%.rs}")"; done

# run all checks that CI actions will run
pre-commit $RUSTFLAGS="-D warnings -W unreachable-pub -W bare-trait-objects":
  just compile-tests "--locked"
  just build
  just format "--check"
  just lint "-p icechunk -p icechunk-python"
  just test
  just run-all-examples
  just check-deps

pre-commit-python:
  just format "--check -p icechunk-python"
  just lint "-p icechunk-python"


bench-compare *args:
  pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=short {{args}}
