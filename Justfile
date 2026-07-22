# Cargo profile: override with `just profile=ci test` (default: dev)
profile := "dev"

# Enable coverage instrumentation: override with `just coverage=true build-wheels` (default: false)
coverage := "false"

# Hypothesis profile for `just python-upstream-pytest`: override with `just hypothesis_profile=ci python-upstream-pytest` (default: nightly)
hypothesis_profile := "nightly"

# Settings can't interpolate variables, so the interpreter preamble below
# reads these through exported env vars.
export JUST_COVERAGE := coverage
export JUST_PROFILE := profile

# pytest-cov flags for `just pytest`, active only when coverage=true
pytest_cov_args := if coverage == "true" { "--cov=icechunk" } else { "" }

# Env preamble for every [script] recipe: conda lib path, uv-targets-pixi-env,
# plus llvm-cov instrumentation when coverage=true. Runs in-shell so macOS SIP
# can't strip DYLD_*.
set script-interpreter := ["bash", "-euo", "pipefail", "-c", '''
# fallback, not DYLD_LIBRARY_PATH: only otherwise-unresolvable libs (e.g. the
# rpath-less pyo3 lib-test's libpython) may come from conda; the venv numpy's
# BLAS must not be shadowed. Tail keeps the system defaults this var replaces.
export DYLD_FALLBACK_LIBRARY_PATH="${CONDA_PREFIX:-}/lib:/usr/local/lib:/usr/lib"
if [ -n "${CONDA_PREFIX:-}" ]; then
  # make uv target the active pixi env instead of syncing a project venv
  export VIRTUAL_ENV="$CONDA_PREFIX" UV_NO_SYNC=1
fi
if [ "$JUST_COVERAGE" = "true" ]; then
  # eval, not `source <(...)`: macOS /bin/bash 3.2 silently sources nothing from process substitution
  eval "$(cargo llvm-cov show-env --sh --profile "$JUST_PROFILE")"
  export CARGO_TARGET_DIR=$CARGO_LLVM_COV_TARGET_DIR
fi
source "$0"
''']

set positional-arguments

alias fmt := format
alias pre := pre-commit

export PYTHON_VERSION := env("PYTHON_VERSION", "3.12")

# First recipe = what bare `just` runs; --unsorted lists groups in file order
[private]
default:
  @just --list --unsorted

# One recipe per GitHub workflow, chaining the same `just` steps CI runs.
# Bodies re-invoke `just profile=ci ...` because recipe deps can't carry
# variable overrides (same pattern as pre-commit-ci).

[group('ci')]
[doc("Reproduce rust-ci.yaml's rust job (Linux lane); needs docker. Other jobs: shuttle-test, wasm-build + wasm-proxy-test")]
ci-rust $RUSTFLAGS="-D warnings":
  just check-msrv
  just contup
  just contwait
  just profile=ci coverage={{coverage}} compile-tests "--locked"
  just profile=ci coverage={{coverage}} test
  just profile=ci coverage={{coverage}} doctest
  just profile=ci coverage={{coverage}} run-all-examples
  just profile=ci coverage={{coverage}} develop
  just profile=ci coverage={{coverage}} pytest -n 4 -m "not hypothesis"
  [ "{{coverage}}" != "true" ] || just profile=ci coverage=true coverage-report

[group('ci')]
[doc("Reproduce python-check.yaml: wheel build + all test lanes (latest deps; free-threaded and hypothesis sharding excluded)")]
ci-python-check:
  just profile={{profile}} build-wheels
  just rustfs-up
  just install-test-wheel test
  just rustfs-wait
  just coverage={{coverage}} pytest-venv -n 4 -m "not hypothesis"
  [ "{{coverage}}" != "true" ] || just coverage-stash unit
  just coverage={{coverage}} pytest-venv -m hypothesis
  [ "{{coverage}}" != "true" ] || just coverage-stash hypothesis
  [ "{{coverage}}" != "true" ] || just coverage-report-python
  just install-ic-v1
  just pytest-venv tests/test_stateful_compat.py -v
  just install-test-wheel test "pytest-mypy-plugins<4"
  just xarray-clone-installed
  just xarray-backends-pytest
  just install-test-wheel dev
  just venv-run just stubtest
  just check-xarray-docs

[group('ci')]
[doc("Reproduce code-quality.yaml: develop, pixi version check, format check, clippy, doctests, mypy, pre-commit")]
ci-code-quality $RUSTFLAGS="-D warnings":
  just profile=ci develop
  just check-pixi-version
  just format "--check"
  just profile=ci lint
  just profile=ci doctest
  just mypy
  just py-pre-commit

[group('ci')]
[doc("Reproduce js-ci.yaml for the host target (WASI lane: js-build-wasi + js-test-wasi)")]
ci-js:
  just js-install
  just js-build
  just js-test

[group('ci')]
[doc("Run all Python and Rust checks")]
all-checks:
  just pytest
  just py-pre-commit
  just mypy
  just pre-commit-ci

[group('test')]
[script]
[doc("Run all Rust tests via cargo-nextest")]
test *args:
  cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --lib --bins --tests --examples "$@"

[group('test')]
[script]
[doc("Run integration tests against object stores (rooted=false skips the bucket-root rooted_roundtrip tests, which race across concurrent runs)")]
test-integration rooted="true" *args:
  shift
  # don't let a failure (or flake) in one group hide the other
  set +e
  rc=0
  # exclude icechunk-python: no Rust tests, but its PyO3 lib-test binary aborts loading libpython
  cargo test --profile {{profile}} --workspace --exclude icechunk-python --tests "$@" -- --ignored --skip rooted_roundtrip || rc=1
  if [ "{{rooted}}" = "true" ]; then
    cargo test --profile {{profile}} -p icechunk --test main rooted_roundtrip -- --ignored || rc=1
  fi
  exit $rc

[group('test')]
[script]
[doc("Run all Rust lib tests via cargo-nextest")]
unit-test *args:
  cargo nextest run --no-fail-fast --cargo-profile {{profile}} --lib "$@"

[group('test')]
[script]
[doc("Run Rust doc tests only")]
doctest *args:
  cargo test --workspace --profile {{profile}} --doc "$@"

[group('test')]
[script]
[doc("Run all Rust tests with RUST_LOG enabled (e.g. `just test-logs debug`)")]
test-logs level *args:
  shift && RUST_LOG=icechunk={{level}} cargo nextest run --no-fail-fast --cargo-profile {{profile}} --workspace --lib --bins --tests --examples --nocapture "$@"

[group('test')]
[script]
[doc("Compile tests without running them")]
compile-tests *args:
  cargo nextest run --no-run --cargo-profile {{profile}} --workspace --all-targets "$@"

[group('test')]
[script]
[doc("Run shuttle concurrency tests (pass --no-run to compile only)")]
shuttle-test *args:
  cargo test --profile {{profile}} -p icechunk --features shuttle --test test_shuttle "$@"

[group('build')]
[script]
[doc("Build the Rust workspace (dev by default, override with `just profile=ci build`)")]
build *args:
  cargo build --profile {{profile}} "$@"

[group('build')]
[doc("Build the Rust workspace in release mode")]
build-release *args:
  cargo build --release "$@"

# WASI toolchain for wasm-build / js-build-wasi; inert for other targets.
# wasi-sdk sysroots have a per-target include dir; Debian's wasi-libc doesn't.
export WASI_SYSROOT := env("WASI_SYSROOT", "/usr")
export CC_wasm32_wasip1_threads := env("CC_wasm32_wasip1_threads", "clang")
export CXX_wasm32_wasip1_threads := env("CXX_wasm32_wasip1_threads", "clang++")
export AR_wasm32_wasip1_threads := env("AR_wasm32_wasip1_threads", "llvm-ar")
wasi_include := if path_exists(WASI_SYSROOT / "include/wasm32-wasip1-threads") == "true" { WASI_SYSROOT / "include/wasm32-wasip1-threads" } else { WASI_SYSROOT / "include/wasm32-wasi" }
export CFLAGS_wasm32_wasip1_threads := "--sysroot=" + WASI_SYSROOT + " -isystem " + wasi_include
export CXXFLAGS_wasm32_wasip1_threads := CFLAGS_wasm32_wasip1_threads

[group('build')]
[script]
[doc("Compile-check icechunk for wasm (needs clang/llvm + wasi-libc; override sysroot with WASI_SYSROOT)")]
wasm-build:
  # compile smoke test: don't fail on existing warnings in no-default-features wasm cfgs
  export RUSTFLAGS=""
  cargo build -p icechunk --no-default-features --target wasm32-wasip1-threads

[group('test')]
[script]
[doc("Run icechunk lib tests with no default features (wasm feature-set proxy)")]
wasm-proxy-test *args:
  # don't fail on existing warnings in no-default-features wasm cfgs
  export RUSTFLAGS=""
  cargo test --profile {{profile}} -p icechunk --no-default-features --lib "$@"

[group('build')]
[doc("Regenerate src/flatbuffers/all_generated.rs from the FlatBuffers schemas")]
gen-flatbuffers:
  cd icechunk-format/flatbuffers && flatc --rust -o ../src/flatbuffers/ --gen-all all.fbs
  just format

[group('python')]
[script]
[doc("Prepare environment for development")]
develop *args:
  cd icechunk-python
  uv run --active maturin develop --uv --profile {{profile}} "$@"

[group('python')]
[script]
[doc("Install maturin import hook for more convenient development flow")]
import-hook:
  cd icechunk-python && python -m maturin_import_hook site install

[group('python')]
[script]
[doc("Uninstall maturin import hook")]
import-hook-remove:
  cd icechunk-python && python -m maturin_import_hook site uninstall

# Use --all-features for the workspace but skip icechunk's `shuttle` feature,
# which swaps tokio for shuttle-tokio and is incompatible with other crates.
icechunk_features := "s3,object-store-s3,object-store-gcs,object-store-azure,object-store-http,object-store-fs,redirect,logs,otel,cli,napi-send-contract"

[group('lint')]
[doc("Run clippy lints on all features and targets")]
lint *args:
  cargo clippy --profile {{profile}} --all-features --all-targets --workspace --exclude icechunk "$@"
  cargo clippy --profile {{profile}} --all-targets -p icechunk --features {{icechunk_features}} "$@"

[group('lint')]
[doc("Run check on all features")]
check *args:
  cargo check --profile {{profile}} --all-features --workspace --exclude icechunk "$@"
  cargo check --profile {{profile}} -p icechunk --features {{icechunk_features}} "$@"

[group('lint')]
[script]
[doc("Check Cargo.toml rust-version matches the contributing docs (pass an expected MSRV to pin it)")]
check-msrv expected="":
  msrv=$(sed -n 's/^rust-version = "\(.*\)"$/\1/p' Cargo.toml)
  pysemver check "$msrv"
  test -z "{{expected}}" || test "$(pysemver compare "$msrv" "{{expected}}")" = 0
  grep -F "The current MSRV is \`$msrv\`" icechunk-python/docs/docs/reference/contributing.md

[group('lint')]
[script]
[doc("Check pixi versions in CI workflows and docs satisfy requires-pixi in icechunk-python/pyproject.toml")]
check-pixi-version:
  min=$(sed -n 's/^requires-pixi = ">=\(.*\)"$/\1/p' icechunk-python/pyproject.toml)
  grep -h 'PIXI_VERSION:' .github/workflows/*.y*ml | sed 's/.*PIXI_VERSION: "v\(.*\)".*/\1/' | while read -r ver; do
    test "$(pysemver compare "$ver" "$min")" -ge 0
  done
  docver=$(sed -n 's/.*at least pixi version `\([0-9.]*\)`.*/\1/p' icechunk-python/docs/docs/reference/contributing.md)
  test "$(pysemver compare "$docver" "$min")" -ge 0

[group('lint')]
[doc("Format all Rust files (pass `--check` to verify only)")]
format *args:
  cargo fmt --all "$@"

[group('lint')]
[doc("Format all Nix files with alejandra")]
format-nix *args:
  alejandra .

[group('lint')]
[doc("Check dependencies for security/license issues via cargo-deny")]
check-deps *args:
  cargo deny --all-features check "$@"

[group('test')]
[script]
[doc("Run all Rust examples (skips limits_chunk_refs, large_manifests)")]
run-all-examples:
  # Allow failing examples, fix in the future
  set +e

  for example in icechunk/examples/*.rs; do case "$example" in *limits_chunk_refs*|*large_manifests*) continue;; esac; cargo run --profile {{profile}} --example "$(basename "${example%.rs}")"; done

[group('lint')]
[doc("Fast Rust pre-commit: format + lint + doctest (~3s)")]
pre-commit-fast:
  just format
  just lint
  just doctest

[group('lint')]
[doc("Medium Rust pre-commit: compile, build, format, lint, deps (~2-3min)")]
pre-commit $RUSTFLAGS="-D warnings":
  just compile-tests "--locked"
  just build
  just format
  just lint
  just check-deps

[group('lint')]
[doc("Full Rust CI pre-commit: all checks including tests and examples (~5+min)")]
pre-commit-ci $RUSTFLAGS="-D warnings":
  just profile=ci compile-tests "--locked"
  just profile=ci build
  just format "--check"
  just profile=ci lint
  just profile=ci doctest
  just profile=ci test
  just profile=ci run-all-examples
  just check-deps

[group('lint')]
[doc("Rust format + lint for the icechunk-python crate only")]
pre-commit-python:
  just format "-p icechunk-python"
  cargo clippy --profile {{profile}} --all-features --all-targets -p icechunk-python

[group('bench')]
[doc("Profile benchmarks with cargo-samply (tracing spans become profiler markers)")]
samply *args:
  ICECHUNK_TRACE=samply cargo samply --features logs --bench main -- {{args}} --test

[group('bench')]
[doc("Run benchmarks and emit a Chrome trace JSON file (open in Perfetto UI)")]
chrome-trace *args:
  ICECHUNK_TRACE=chrome cargo bench --features logs --bench main -- {{args}} --test

[group('bench')]
[doc("Compare pytest-benchmark results")]
bench-compare *args:
  pytest-benchmark compare --group=group,func,param --sort=fullname --columns=median --name=short "$@"

[group('lint')]
[doc("Run ruff formatter on Python code")]
ruff-format *args:
  ruff format "$@"

[group('lint')]
[doc("Run ruff linter on Python code (pass `--fix` for auto-fix)")]
ruff *args:
  ruff check --show-fixes icechunk-python/ "$@"

[group('lint')]
[script]
[doc("Run mypy type checking on Python code")]
mypy *args:
  cd icechunk-python
  uv run --active mypy python tests "$@"

[group('lint')]
[script]
[doc("Run mypy stub checking on type stubs")]
stubtest *args:
  cd icechunk-python && python -m mypy.stubtest --ignore-disjoint-bases icechunk._icechunk_python --allowlist stubtest_allowlist.txt "$@"

[group('lint')]
[doc("Run all Python pre-commit hooks (ruff, formatting, codespell, etc.)")]
py-pre-commit $SKIP="rust-pre-commit-fast,rust-pre-commit,rust-pre-commit-ci" *args:
  prek run --all-files

[group('python')]
[script]
[doc("Run Python tests via pytest (coverage=true also collects Python + FFI coverage)")]
pytest *args:
  cd icechunk-python
  uv run --active pytest {{pytest_cov_args}} "$@"

[group('python')]
[script]
[doc("Run the Python tests with OpenTelemetry export to local Jaeger (starts Jaeger; traces at http://localhost:16686)")]
pytest-otel *args: jaeger-up
  export ICECHUNK_OTLP_ENDPOINT="${ICECHUNK_OTLP_ENDPOINT:-http://localhost:4317}"
  echo "Exporting traces to $ICECHUNK_OTLP_ENDPOINT (filter ${ICECHUNK_OTEL_FILTER:-icechunk=info}) — view at http://localhost:16686"
  # test_logs.py asserts on exact console output, which races with the background
  # OTLP export: its gRPC transport (h2/tonic) emits debug logs into the same
  # tracing subscriber. Those tests are meaningless under telemetry, so skip them.
  just pytest --ignore=tests/test_logs.py "$@"

[group('python')]
[script]
[doc("Regenerate the post-expiration can_read_old fixtures (needs icechunk 1.1.21 + 2.0.5 wheels, installed via third-wheel)")]
gen-expired-fixtures *args:
  cd icechunk-python
  uv run --with third-wheel third-wheel sync --rename "icechunk==1.1.21=icechunk_v1" --rename "icechunk==2.0.5=icechunk_v2"
  uv run python tests/data_generation/generate_expired_repos.py "$@"

[group('docs')]
[script]
[doc("Start MkDocs dev server with live reload")]
docs-serve *args:
  cd icechunk-python
  uv run --active --group docs mkdocs serve -f docs/mkdocs.yml --livereload "$@"

[group('docs')]
[script]
[doc("Build MkDocs static site")]
docs-build *args:
  cd icechunk-python
  uv run --active --group docs mkdocs build -f docs/mkdocs.yml "$@"

[group('upstream')]
[doc("Check compatibility with zarrs_icechunk")]
zarrs-upstream zarrs_dir="zarrs_icechunk": zarrs-upstream-clone zarrs-upstream-patch zarrs-upstream-build zarrs-upstream-test
  @echo "zarrs_upstream check passed"
  rm -rf {{zarrs_dir}}

[private]
[doc("Clone zarrs_icechunk for local checks")]
zarrs-upstream-clone zarrs_dir="zarrs_icechunk":
  rm -rf {{zarrs_dir}}
  git clone https://github.com/zarrs/zarrs_icechunk {{zarrs_dir}}

[private]
[script]
[doc("Patch zarrs_icechunk Cargo.toml to use local icechunk crate")]
zarrs-upstream-patch zarrs_dir="zarrs_icechunk":
  icechunk_path=$(realpath icechunk)
  if ! grep -q 'icechunk = { path' {{zarrs_dir}}/Cargo.toml; then
    sed -i.bak $'/^\\[patch\\.crates-io\\]/a\\\nicechunk = { path = "'"$icechunk_path"'" }' {{zarrs_dir}}/Cargo.toml && rm {{zarrs_dir}}/Cargo.toml.bak
  fi
  # empty [workspace] table so cargo doesn't adopt the enclosing icechunk workspace
  if ! grep -q '^\[workspace\]' {{zarrs_dir}}/Cargo.toml; then
    printf '\n[workspace]\n' >> {{zarrs_dir}}/Cargo.toml
  fi

[private]
[script]
[doc("Build zarrs_icechunk against local icechunk")]
zarrs-upstream-build zarrs_dir="zarrs_icechunk": zarrs-upstream-patch
  cd {{zarrs_dir}} && cargo build 2>&1 | tee build-output.log

[private]
[script]
[doc("Test zarrs_icechunk against local icechunk")]
zarrs-upstream-test zarrs_dir="zarrs_icechunk": zarrs-upstream-patch zarrs-upstream-build
  cd {{zarrs_dir}} && cargo test 2>&1 | tee test-output.log

[group('services')]
[doc("Start all docker compose services")]
contup:
  docker compose up -d

[group('services')]
[doc("Start RustFS via docker compose")]
rustfs-up:
  docker compose up -d rustfs_init

[group('services')]
[script]
[doc("Wait for RustFS container to be ready")]
rustfs-wait:
  for _ in {1..10}; do
    if docker compose ps --status exited --filter status==0 | grep rustfs ; then
      exit 0
    fi
    sleep 3
  done
  echo "ERROR: RustFS did not become ready in time" >&2
  exit 1

[private]
[script]
[doc("Poll a URL until it responds successfully")]
wait-http name url:
  for _ in {1..60}; do
    if curl --silent --fail "{{url}}"; then
      exit 0
    fi
    sleep 1
  done
  echo "ERROR: {{name}} did not become ready in time" >&2
  exit 1

[group('services')]
[doc("Wait for all docker compose services to be ready")]
contwait: rustfs-wait \
  (wait-http "Azurite" "http://localhost:10000/devstoreaccount1/testcontainer?sv=2023-01-03&ss=btqf&srt=sco&spr=https%2Chttp&st=2025-01-06T14%3A53%3A30Z&se=2035-01-07T14%3A53%3A00Z&sp=rwdftlacup&sig=jclETGilOzONYp4Y0iK9SpVRLGyehaS5lg5booJ9VYA%3D&restype=container") \
  (wait-http "MinIO" "http://localhost:4202/minio/health/live")

[group('services')]
[doc("Start Jaeger for local OpenTelemetry tracing (UI http://localhost:16686, OTLP gRPC localhost:4317)")]
jaeger-up:
  docker compose up -d jaeger
  @echo "Jaeger UI: http://localhost:16686 — set ICECHUNK_OTLP_ENDPOINT=http://localhost:4317 to export traces"

[group('services')]
[doc("Stop and remove the Jaeger container")]
jaeger-down:
  docker compose rm --force --stop --volumes jaeger

[group('release')]
[doc("Publish workspace crates to crates.io via cargo-release")]
publish-crates:
  cargo release --workspace --unpublished --no-confirm --no-tag --no-push --execute

# wheels default to release; an explicit profile (e.g. `just profile=ci build-wheels`) wins
wheel_profile := if profile == "dev" { "release" } else { profile }

[group('python')]
[script]
[doc("Build Python wheels with maturin (coverage=true needs an explicit profile, e.g. profile=ci)")]
build-wheels *args:
  if [ "$JUST_COVERAGE" = "true" ] && [ "{{profile}}" = "dev" ]; then
    echo "ERROR: coverage=true build-wheels needs an explicit profile (e.g. profile=ci) so coverage-report can find the instrumented artifacts" >&2
    exit 1
  fi
  cd icechunk-python && maturin build --profile {{wheel_profile}} --out dist -i $PYTHON_VERSION "$@"

[group('python')]
[script]
[doc("Install built wheel and test dependencies into a venv")]
install-test-wheel group="test" *args:
  shift
  cd icechunk-python
  uv venv --clear --python=${PYTHON_VERSION}
  source .venv/bin/activate
  PY_TAG="cp${PYTHON_VERSION//./}"
  WHEEL=$(ls dist/*-"${PY_TAG}"-*.whl)
  uv pip install "$WHEEL" --group "{{group}}" "$@"

[private]
[script]
[doc("Install icechunk_v1 via third-wheel into the test venv")]
install-ic-v1:
  cd icechunk-python && source .venv/bin/activate
  # --installer uv: auto-detect targets the pixi env via CONDA_PREFIX, not .venv
  uv run third-wheel sync -v --installer uv

[group('python')]
[script]
[doc("Run a command inside the icechunk-python test venv")]
venv-run *args:
  cd icechunk-python
  source .venv/bin/activate
  "$@"

[group('python')]
[script]
[doc("Run pytest from the test venv (coverage=true also collects Python + FFI coverage)")]
pytest-venv *args:
  cd icechunk-python
  source .venv/bin/activate
  python -m pytest {{pytest_cov_args}} "$@"

[group('upstream')]
[doc("Run Python checks with upstream nightly dependencies")]
python-upstream: build-wheels python-upstream-setup python-upstream-mypy python-upstream-describe python-upstream-pytest
  echo "python upstream nightly checks passed"

[private]
[script]
[doc("Install Python upstream nightly dependencies")]
python-upstream-setup:
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  python --version
  WHEEL=$(ls dist/*-abi3-*.whl)
  export UV_INDEX="https://pypi.anaconda.org/scientific-python-nightly-wheels/simple/"
  export UV_PRERELEASE=allow
  uv pip install "$WHEEL" --group dev \
    --resolution highest \
    --index-strategy unsafe-best-match 2>&1 | tee setup-output.log
  uv pip install "hypothesis @ git+https://github.com/ianhi/hypothesis.git@flaky-feedback#subdirectory=hypothesis-python"
  uv pip list

[private]
[script]
[doc("Run mypy against Python upstream nightly")]
python-upstream-mypy: python-upstream-setup
  cd icechunk-python
  source .venv/bin/activate
  mypy --python-version "$PYTHON_VERSION" python 2>&1 | tee mypy-output.log

[private]
[script]
[doc("Describe Python upstream nightly environment")]
python-upstream-describe: python-upstream-setup
  cd icechunk-python
  source .venv/bin/activate
  pip list

[private]
[script]
[doc("Run pytest with Python upstream nightly dependencies (set coverage=true to capture FFI coverage)")]
python-upstream-pytest *args: python-upstream-setup
  cd icechunk-python
  source .venv/bin/activate
  pytest -n 4 --hypothesis-profile={{hypothesis_profile}} --report-log output-pytest-log.jsonl "$@"

[group('upstream')]
[doc("Run full xarray-upstream checks")]
xarray-upstream xarray_dir="xarray": xarray-upstream-clone build-wheels xarray-upstream-setup xarray-upstream-pytest
  echo "xarray-upstream checks passed"
  rm -rf {{xarray_dir}}

[private]
[doc("Clone xarray from upstream")]
xarray-upstream-clone xarray_dir="xarray":
  rm -rf {{xarray_dir}}
  git clone https://github.com/pydata/xarray {{xarray_dir}}

[private]
[script]
[doc("Install xarray upstream test dependencies")]
xarray-upstream-setup:
  cd icechunk-python
  python3 -m venv .venv
  source .venv/bin/activate
  python --version
  WHEEL=$(ls dist/*-abi3-*.whl)
  export UV_INDEX="https://pypi.anaconda.org/scientific-python-nightly-wheels/simple/"
  export UV_PRERELEASE=allow
  uv pip install "$WHEEL" --group test pytest-mypy-plugins \
    --resolution highest \
    --index-strategy unsafe-best-match
  uv pip list

[private]
[script]
[doc("Run xarray backend tests against local icechunk (set coverage=true to capture FFI coverage)")]
xarray-upstream-pytest xarray_dir="xarray": xarray-upstream-clone xarray-upstream-setup
  xarray_abs=$(realpath "{{xarray_dir}}")
  cd icechunk-python
  source .venv/bin/activate
  export ICECHUNK_XARRAY_BACKENDS_TESTS=1
  pytest -c="$xarray_abs/pyproject.toml" -W ignore tests/run_xarray_backends_tests.py --report-log output-pytest-log.jsonl

[group('docs')]
[script]
[doc("Check to_icechunk docstrings stay in sync with xarray's to_zarr (CI: check-xarray-docs)")]
check-xarray-docs xarray_dir="xarray": (xarray-upstream-clone xarray_dir)
  xarray_abs=$(realpath "{{xarray_dir}}")
  cd icechunk-python
  XARRAY_DIR="$xarray_abs" uv run scripts/check_xarray_docs_sync.py

[private]
[script]
[doc("Clone xarray at the tag matching the version installed in the test venv")]
xarray-clone-installed xarray_dir="xarray":
  cd icechunk-python
  source .venv/bin/activate
  # zero-pad the month: 2025.1.2 -> v2025.01.2
  tag=$(python -c "import xarray; p = xarray.__version__.split('.'); print(f'v{p[0]}.{p[1].zfill(2)}.{p[2]}')")
  echo "Checking out xarray $tag"
  cd ..
  rm -rf "{{xarray_dir}}"
  git clone --depth 1 --branch "$tag" https://github.com/pydata/xarray.git "{{xarray_dir}}"

[private]
[script]
[doc("Run xarray backends tests from the test venv against a pinned xarray checkout (CI: python-check xarray-backends)")]
xarray-backends-pytest xarray_dir="xarray":
  xarray_abs=$(realpath "{{xarray_dir}}")
  cd icechunk-python
  source .venv/bin/activate
  export ICECHUNK_XARRAY_BACKENDS_TESTS=1
  # xarray's pyproject.toml so pytest finds the `flaky` fixture
  python -m pytest -c="$xarray_abs/pyproject.toml" -W ignore --override-ini="strict_markers=false" tests/run_xarray_backends_tests.py

[group('coverage')]
[script]
[doc("code coverage report generation (Rust + FFI + Python)")]
coverage-report *args:
  cargo llvm-cov report --profile {{ profile }} --lcov --output-path coverage_rust.lcov
  echo "Coverage report: coverage_rust.lcov (Rust, unified FFI + native)"
  if [ -f icechunk-python/.coverage ]; then
    coverage lcov --data-file=icechunk-python/.coverage -o coverage_python.lcov
    echo "Coverage report: coverage_python.lcov (Python)"
  fi

[group('coverage')]
[script]
[doc("Python-line coverage lcov from .coverage fragments (remaps site-packages paths to the source tree)")]
coverage-report-python:
  [ ! -f icechunk-python/.venv/bin/activate ] || source icechunk-python/.venv/bin/activate
  mkdir -p icechunk-python/.coverage-fragments
  [ ! -f icechunk-python/.coverage ] || mv icechunk-python/.coverage icechunk-python/.coverage-fragments/.coverage.wheel
  coverage combine --rcfile=icechunk-python/pyproject.toml --data-file=icechunk-python/.coverage icechunk-python/.coverage-fragments
  coverage lcov --data-file=icechunk-python/.coverage -o coverage_python.lcov
  echo "Coverage report: coverage_python.lcov (Python)"

# fragments live in a subdir because pytest-cov's session-start erase
# deletes any sibling .coverage.* files
[group('coverage')]
[doc("Stash .coverage as a named fragment so later runs don't erase it (combined by coverage-report-python)")]
coverage-stash name:
  mkdir -p icechunk-python/.coverage-fragments
  mv icechunk-python/.coverage icechunk-python/.coverage-fragments/.coverage.{{name}}

[group('coverage')]
[doc("Delete coverage artifacts (profraw, lcov, .coverage)")]
coverage-clean:
  find . -iname "*.profraw" -delete
  rm -f coverage_rust.lcov coverage_python.lcov icechunk-python/.coverage
  rm -rf icechunk-python/.coverage-fragments

# corepack provisions yarn@4.12.0 per packageManager; suppress its download prompt
export COREPACK_ENABLE_DOWNLOAD_PROMPT := env("COREPACK_ENABLE_DOWNLOAD_PROMPT", "0")

[group('js')]
[doc("Install icechunk-js dependencies with yarn")]
js-install:
  # conda-forge nodejs ships corepack without yarn shims
  command -v yarn >/dev/null || corepack enable
  cd icechunk-js && yarn install

[group('js')]
[doc("Build icechunk-js native bindings, release profile (pass --target <triple> to cross-compile)")]
js-build *args: js-install
  cd icechunk-js && yarn build "$@"

[group('js')]
[doc("Build icechunk-js native bindings, debug profile (faster)")]
js-build-debug *args: js-install
  cd icechunk-js && yarn build:debug "$@"

[group('js')]
[doc("Run icechunk-js tests with ava (build with js-build/js-build-debug first)")]
js-test *args: js-install
  cd icechunk-js && yarn test "$@"

[group('js')]
[script]
[doc("Build icechunk-js for wasm32-wasip1-threads (same WASI toolchain env as wasm-build)")]
js-build-wasi *args: js-install
  cd icechunk-js
  yarn build --target wasm32-wasip1-threads "$@"

[group('js')]
[script]
[doc("Run icechunk-js tests under WASI like CI's test-wasi lane (needs js-build-wasi)")]
js-test-wasi *args:
  cd icechunk-js
  # `yarn config set` writes .yarnrc.yml; restore host setup on exit
  yarn config set supportedArchitectures.cpu "wasm32"
  trap 'yarn config unset supportedArchitectures.cpu; yarn install' EXIT
  yarn install
  NAPI_RS_FORCE_WASI=1 yarn test "$@"
