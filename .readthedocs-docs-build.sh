#!/usr/bin/env bash
# Run docs-build with periodic stack dumps to locate build stalls on RTD
set -euo pipefail
if [ -n "${CONDA_PREFIX:-}" ]; then
  # make uv target the active pixi env instead of syncing a project venv
  export VIRTUAL_ENV="$CONDA_PREFIX" UV_NO_SYNC=1
fi
cd icechunk-python
exec uv run --active --group docs python -c '
import faulthandler, os, sys
from mkdocs.__main__ import cli
interval = float(os.environ.get("DOCS_BUILD_DUMP_INTERVAL", "60"))
faulthandler.dump_traceback_later(interval, repeat=True)
sys.argv = ["mkdocs", "build", "-f", "docs/mkdocs.yml", *sys.argv[1:]]
try:
    cli()
finally:
    faulthandler.cancel_dump_traceback_later()
' "$@"
