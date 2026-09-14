#!/usr/bin/env bash
# Does a file lock reach another host? Two clients answer it for one NFS export.
# README.md says what it measures and what it needs.
set -euo pipefail
cd "$(dirname "$0")"

HOLD_SECONDS=${HOLD_SECONDS:-8}
export HOLD_SECONDS

# Rootless podman cannot run Ganesha. Point this at a rootful engine there,
# in the way README.md shows.
DOCKER=${DOCKER:-docker}

# The clients mount NFS with the host kernel, so the host needs the nfs module.
preflight() {
  if $DOCKER run --rm alpine grep -qw nfs /proc/filesystems; then return 0; fi
  cat >&2 <<'EOF'
The host kernel has no NFS client. Load it first.

On Linux:
  sudo modprobe nfs
With podman machine:
  podman machine ssh 'sudo modprobe nfs'
With Docker Desktop, the LinuxKit VM usually has it already.
EOF
  return 1
}
preflight

rm -rf results && mkdir results
$DOCKER compose down -v >/dev/null 2>&1  # a stale container blocks the recreate
# Build both images: `up --build` only builds what it starts, and the client
# runs through `compose run`.
$DOCKER compose build
$DOCKER compose up -d --wait nfs

# One client first, on its own: two parallel `compose run` calls race to create
# the shared volume, and the loser fails.
$DOCKER compose run --rm --entrypoint true client >/dev/null

for mode in local nfs; do
  # Far enough ahead that both clients mount before it fires.
  start=$(( $(date +%s) + 15 ))
  $DOCKER compose run --rm -e "START=$start" -e "MODE=$mode" -e NAME=a client &
  $DOCKER compose run --rm -e "START=$start" -e "MODE=$mode" -e NAME=b client &
  wait
done

$DOCKER compose down -v >/dev/null 2>&1

report() {
  local mode=$1 rc=0
  echo
  echo "== $mode: $(cat "results/$mode.a.fstype") (filesystem, statfs magic)"
  echo "   mounted as: $(cat "results/$mode.a.mount")"

  unset holder_acquired holder_released nonblocking nonblocking_error \
        blocking blocking_error blocking_seconds
  # shellcheck disable=SC1090
  [ -s "results/$mode.a.log" ] && . "results/$mode.a.log"
  # shellcheck disable=SC1090
  [ -s "results/$mode.b.log" ] && . "results/$mode.b.log"

  if [ -z "${holder_acquired:-}" ]; then
    echo "   the holder never took the lock: nothing was tested" >&2
    return 1
  fi
  printf '   holder held the lock for %.1fs\n' \
    "$(echo "$holder_released - $holder_acquired" | bc)"

  case "${nonblocking:-missing}" in
    refused)
      echo "   the other host was refused the lock: it reached the server" ;;
    acquired)
      echo "   the other host TOOK THE SAME LOCK: it never reached the server."
      echo "   A compare-and-swap here can lose a commit."
      rc=1 ;;
    *)
      echo "   the non-blocking attempt failed: ${nonblocking_error:-unknown}" >&2
      rc=1 ;;
  esac

  # This is the form icechunk uses, so name what it did either way.
  case "${blocking:-missing}" in
    acquired)
      printf '   a blocking lock waited %.1fs and then got it\n' \
        "${blocking_seconds:-0}" ;;
    failed)
      printf '   a blocking lock FAILED after %.1fs: %s\n' \
        "${blocking_seconds:-0}" "${blocking_error:-unknown}"
      echo "   icechunk would surface this as an error, not as a lost commit." ;;
    *)
      echo "   the blocking attempt reported nothing" >&2 ;;
  esac
  return $rc
}

status=0
# The control must refuse, or the probe proves nothing.
report local || status=1
report nfs || status=1
exit $status
