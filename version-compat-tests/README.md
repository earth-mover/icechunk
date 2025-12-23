# Icechunk Version Compatibility Tests

This directory contains regression tests to ensure compatibility between icechunk v1 and v2 formats.

## Purpose

These tests verify that:

1. **v2 can write v1 format**: The v2 library can create repositories using `spec_version=1`
2. **v1 can read v1 format from v2**: Repositories written in v1 format by the v2 library can be read by the actual v1 library
3. **Graceful upgrade errors**: After upgrading a repository to v2 format, the v1 library fails gracefully
4. **v2 can read upgraded repos**: The v2 library can read repositories after upgrading from v1 to v2

## Setup

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### Option 1: Using the Proxy Server (Recommended)

This approach uses `wheel-rename serve` to run a local proxy that renames packages on the fly.

**Terminal 1 - Start the proxy server:**
```bash
./scripts/start_proxy.sh
```

**Terminal 2 - Install and run tests:**
```bash
uv sync
uv run pytest -v
```

### Option 2: Manual Wheel Download

This approach downloads and renames wheels manually.

```bash
# Download and rename wheels
./scripts/setup_wheels.sh

# Create venv and install
uv venv
uv pip install ./wheels/icechunk_v1-*.whl
uv pip install ./wheels/icechunk-2*.whl
uv pip install zarr pytest numpy

# Run tests
uv run pytest -v
```

## How It Works

The tests use the [wheel-rename](https://github.com/earth-mover/rename-wheel) tool to install both icechunk v1 and v2 in the same Python environment:

- `icechunk` - The v2 library (latest)
- `icechunk_v1` - The v1 library (renamed from icechunk <2.0)

This allows the tests to import both versions and verify cross-version compatibility:

```python
import icechunk      # v2 library
import icechunk_v1   # v1 library (renamed)
```

## Test Scenarios

| Test | Description |
|------|-------------|
| `test_v1_can_read_v1_format_written_by_v2` | Create repo with v2 using spec_version=1, verify v1 can read |
| `test_v1_cannot_read_after_upgrade_to_v2` | Upgrade repo to v2 format, verify v1 fails gracefully |
| `test_v2_can_read_after_upgrade` | Verify v2 can read data after upgrade |
| `test_v2_can_write_and_read_v1_format` | Verify v2 can write and read v1 format repos |
| `test_version_info` | Print and verify version information |

## pyproject.toml Configuration

The `pyproject.toml` is configured to use multiple package indexes:

```toml
[tool.uv]
extra-index-url = [
    "https://pypi.anaconda.org/scientific-python-nightly-wheels/simple",  # v2
    "http://127.0.0.1:8123/simple/",  # proxy for renamed v1
]
prerelease = "allow"
index-strategy = "unsafe-best-match"
```

## Troubleshooting

### Proxy server not running

If `uv sync` fails with connection errors to `127.0.0.1:8123`, make sure the proxy server is running:
```bash
./scripts/start_proxy.sh
```

### Wheel download fails

If the manual wheel download fails:
1. Check network connectivity to `pypi.anaconda.org`
2. Verify nightly wheels are available for your platform
3. Try: `uvx --from "git+https://github.com/earth-mover/rename-wheel" wheel-rename --help`

### Import errors

If you get import errors for `icechunk` or `icechunk_v1`:
1. Verify both packages are installed: `uv pip list | grep icechunk`
2. Re-run the installation steps
3. Test imports: `uv run python -c "import icechunk; import icechunk_v1"`
