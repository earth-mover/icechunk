#!/bin/bash

# Start the wheel-rename proxy server
# This proxy serves icechunk v1 packages renamed to icechunk_v1

NIGHTLY_INDEX="https://pypi.anaconda.org/scientific-python-nightly-wheels/simple"
PORT=8123

echo "Starting wheel-rename proxy server on port $PORT..."
echo "Upstream index: $NIGHTLY_INDEX"
echo "Renaming: icechunk (<2) -> icechunk_v1"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# wheel-rename is not on PyPI yet, install from GitHub with server extras
uvx --from "wheel-rename[server] @ git+https://github.com/earth-mover/rename-wheel" \
    wheel-rename serve \
    -u "$NIGHTLY_INDEX" \
    -r "icechunk=icechunk_v1:<2" \
    --port $PORT
