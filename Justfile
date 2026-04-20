# Cargo profile: override with `just profile=ci test` (default: dev)
profile := "dev"

set positional-arguments

alias fmt := format
alias pre := pre-commit

export PYTHON_VERSION := "3.14"

[doc("Run all Rust tests via cargo-nextest")]
test *args:
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --lib --bins --tests --examples "$@"

[doc("Run all Rust lib tests via cargo-nextest")]
unit-test *args:
  export DYLD_LIBRARY_PATH="${CONDA_PREFIX:-}/lib" && cargo nextest run --no-fail-fast --cargo-profile {{profile}} --lib "$@"

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
  cd icechunk-python && maturin develop --uv --profile {{profile}} "$@"

[doc("Install maturin import hook for more convenient development flow")]
import-hook *args:
  cd icechunk-python && python -m maturin_import_hook site install

[doc("Uninstall maturin import hook")]
import-hook-remove *args:
  cd icechunk-python && python -m maturin_import_hook site uninstall

# Use --all-features for the workspace but skip icechunk's `shuttle` feature,
# which swaps tokio for shuttle-tokio and is incompatible with other crates.
icechunk_features := "s3,object-store-s3,object-store-gcs,object-store-azure,object-store-http,object-store-fs,redirect,logs,cli,napi-send-contract"

[doc("Run clippy lints on all features")]
lint *args:
  cargo clippy --profile {{profile}} --all-features --exclude icechunk "$@"
  cargo clippy --profile {{profile}} -p icechunk --features {{icechunk_features}} "$@"

[doc("Run check on all features")]
check *args:
  cargo check --profile {{profile}} --workspace --all-features "$@"

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
  just lint "--workspace" "--all-targets"

[doc("Medium Rust pre-commit: compile, build, format, lint, deps (~2-3min)")]
pre-commit $RUSTFLAGS="-D warnings":
  just compile-tests "--locked"
  just build
  just format
  just lint "--workspace"
  just check-deps

[doc("Full Rust CI pre-commit: all checks including tests and examples (~5+min)")]
pre-commit-ci $RUSTFLAGS="-D warnings":
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

[doc("Profile benchmarks with cargo-samply (tracing spans become profiler markers)")]
samply *args:
  ICECHUNK_TRACE=samply cargo samply --features logs --bench main -- {{args}} --test

[doc("Run benchmarks and emit a Chrome trace JSON file (open in Perfetto UI)")]
chrome-trace *args:
  ICECHUNK_TRACE=chrome cargo bench --features logs --bench main -- {{args}} --test

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

[doc("Check compatibility with zarrs_icechunk")]
zarrs-upstream zarrs_dir="../zarrs_icechunk": zarrs-upstream-clone zarrs-upstream-patch zarrs-upstream-build zarrs-upstream-test
  @echo "zarrs_upstream check passed"
  rm -rf {{zarrs_dir}}

zarrs-upstream-clone zarrs_dir="../zarrs_icechunk":
  rm -rf {{zarrs_dir}}
  git clone https://github.com/zarrs/zarrs_icechunk {{zarrs_dir}}

[doc("Patch zarrs_icechunk Cargo.toml to use local icechunk crate")]
zarrs-upstream-patch zarrs_dir="../zarrs_icechunk":
  #!/usr/bin/env bash
  set -euo pipefail
  icechunk_path=$(realpath icechunk)
  sed -i '/^\[patch\.crates-io\]/a icechunk = { path = "'"$icechunk_path"'" }' {{zarrs_dir}}/Cargo.toml

[doc("Build zarrs_icechunk against local icechunk")]
zarrs-upstream-build zarrs_dir="../zarrs_icechunk":
  #!/usr/bin/env bash
  set -euo pipefail
  cd {{zarrs_dir}} && cargo build 2>&1 | tee build-output.log

[doc("Test zarrs_icechunk against local icechunk")]
zarrs-upstream-test zarrs_dir="../zarrs_icechunk":
  #!/usr/bin/env bash
  set -euo pipefail
  cd {{zarrs_dir}} && cargo test 2>&1 | tee test-output.log

[doc("Start RustFS via docker compose")]
rustfs-up:
  docker compose up -d rustfs_init

[doc("Wait for RustFS container to be ready")]
rustfs-wait:
  #!/usr/bin/env bash
  set -euo pipefail
  for _ in {1..10}; do
    if docker compose ps --status exited --filter status==0 | grep rustfs ; then
      break
    fi
    sleep 3
  done

[doc("Build Python wheels with maturin")]
build-wheels *args:
  cd icechunk-python && maturin build --release --out dist -i $PYTHON_VERSION "$@"

[doc("Run Python checks with upstream nightly dependencies")]
python-upstream: build-wheels python-upstream-setup python-upstream-mypy python-upstream-describe python-upstream-pytest
  echo "python upstream nightly checks passed"

[doc("Install Python upstream nightly dependencies")]
python-upstream-setup:
  #!/usr/bin/env bash
  set -euo pipefail
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  python --version
  PY_TAG="cp${PYTHON_VERSION//./}"
  WHEEL=$(ls dist/*-"${PY_TAG}"-*.whl)
  export UV_INDEX="https://pypi.anaconda.org/scientific-python-nightly-wheels/simple/"
  export UV_PRERELEASE=allow
  uv pip install "$WHEEL" --group dev \
    --resolution highest \
    --index-strategy unsafe-best-match 2>&1 | tee setup-output.log
  uv pip install "hypothesis @ git+https://github.com/ianhi/hypothesis.git@flaky-feedback#subdirectory=hypothesis-python"
  uv pip list

[doc("Run mypy against Python upstream nightly")]
python-upstream-mypy:
  #!/usr/bin/env bash
  set -euo pipefail
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  mypy --python-version "$PYTHON_VERSION" python 2>&1 | tee mypy-output.log

[doc("Describe Python upstream nightly environment")]
python-upstream-describe:
  #!/usr/bin/env bash
  set -euo pipefail
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  pip list

[doc("Run pytest with Python upstream nightly dependencies")]
python-upstream-pytest *args:
  #!/usr/bin/env bash
  set -euo pipefail
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  pytest -n 4 --hypothesis-profile=nightly --report-log output-pytest-log.jsonl "$@"

[doc("Run full xarray-upstream checks")]
xarray-upstream xarray_dir="../xarray": xarray-upstream-clone build-wheels xarray-upstream-setup xarray-upstream-pytest
  echo "xarray-upstream checks passed"
  rm -rf {{xarray_dir}}

[doc("Clone xarray from upstream")]
xarray-upstream-clone xarray_dir="../xarray":
  rm -rf {{xarray_dir}}
  git clone https://github.com/pydata/xarray {{xarray_dir}}

[doc("Install xarray upstream test dependencies")]
xarray-upstream-setup:
  #!/usr/bin/env bash
  set -euo pipefail
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  python --version
  PY_TAG="cp${PYTHON_VERSION//./}"
  WHEEL=$(ls dist/*-"${PY_TAG}"-*.whl)
  export UV_INDEX="https://pypi.anaconda.org/scientific-python-nightly-wheels/simple/"
  export UV_PRERELEASE=allow
  uv pip install "$WHEEL" --group test pytest-mypy-plugins \
    --resolution highest \
    --index-strategy unsafe-best-match
  uv pip list

[doc("Run xarray backend tests against local icechunk")]
xarray-upstream-pytest xarray_dir="../xarray":
  #!/usr/bin/env bash
  set -euo pipefail
  xarray_abs=$(realpath "{{xarray_dir}}")
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  pytest -c="$xarray_abs/pyproject.toml" -W ignore tests/run_xarray_backends_tests.py --report-log output-pytest-log.jsonl

[doc("Run all Python and Rust checks")]
all-checks:
  just pytest
  just py-pre-commit
  just mypy
  just ruff
  just ruff-format
  just pre-commit-python
  just pre-commit-ci
