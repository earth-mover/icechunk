#!/bin/bash
set -e

# Version Compatibility Test Wheel Setup
# This script downloads icechunk v1 and v2 wheels and renames v1 to icechunk_v1
# so both versions can be installed in the same environment.
#
# Alternative: Use the proxy-based approach with start_proxy.sh and uv sync

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
WHEELS_DIR="$PROJECT_DIR/wheels"
INDEX_URL="https://pypi.anaconda.org/scientific-python-nightly-wheels/simple"

# wheel-rename is not on PyPI yet, install from GitHub
WHEEL_RENAME_PKG="git+https://github.com/earth-mover/rename-wheel"

echo "=== Icechunk Version Compatibility Test Setup ==="
echo "Wheels directory: $WHEELS_DIR"
echo "Index URL: $INDEX_URL"
echo ""

# Clean up old wheels
rm -rf "$WHEELS_DIR"
mkdir -p "$WHEELS_DIR"

# Download v1 wheel (version <2)
echo "=== Step 1: Downloading icechunk v1 (version <2) ==="
uvx --from "$WHEEL_RENAME_PKG" wheel-rename download icechunk --version "<2" \
    -i "$INDEX_URL" \
    -o "$WHEELS_DIR"

# Find the v1 wheel file
V1_WHEEL=$(ls "$WHEELS_DIR"/icechunk-1*.whl 2>/dev/null | head -n 1)
if [ -z "$V1_WHEEL" ]; then
    echo "ERROR: Failed to download v1 wheel"
    exit 1
fi
echo "Downloaded: $V1_WHEEL"

# Rename v1 wheel to icechunk_v1
echo ""
echo "=== Step 2: Renaming v1 wheel to icechunk_v1 ==="
uvx --from "$WHEEL_RENAME_PKG" wheel-rename "$V1_WHEEL" icechunk_v1 -o "$WHEELS_DIR"

# Remove the original v1 wheel (we only need the renamed one)
rm "$V1_WHEEL"

# Download v2 wheel
echo ""
echo "=== Step 3: Downloading icechunk v2 (version >=2.0.0.dev0) ==="
uvx --from "$WHEEL_RENAME_PKG" wheel-rename download icechunk --version ">=2.0.0.dev0" \
    -i "$INDEX_URL" \
    -o "$WHEELS_DIR"

echo ""
echo "=== Setup Complete ==="
echo "Wheels in $WHEELS_DIR:"
ls -la "$WHEELS_DIR"

echo ""
echo "To install the wheels, run:"
echo "  cd $PROJECT_DIR"
echo "  uv venv"
echo "  uv pip install $WHEELS_DIR/icechunk_v1-*.whl"
echo "  uv pip install $WHEELS_DIR/icechunk-2*.whl"
echo "  uv pip install zarr pytest numpy"
echo ""
echo "Then run tests with:"
echo "  uv run pytest"
