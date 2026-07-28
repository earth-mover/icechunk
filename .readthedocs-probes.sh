#!/usr/bin/env bash
# Egress probes: docs-build stalls in ~260s connect timeouts on RTD builders
probe() {
  curl "$1" -sS -m 5 -o /dev/null -w "probe $1 $2 %{http_code} %{time_total}s\n" "$2" || true
}
probe -4 https://fonts.googleapis.com/
probe -6 https://fonts.googleapis.com/
probe -4 https://fonts.gstatic.com/
probe -6 https://fonts.gstatic.com/
probe -4 https://pypi.org/
exit 0
