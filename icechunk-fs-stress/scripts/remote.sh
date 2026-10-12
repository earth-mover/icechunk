#!/usr/bin/env bash
# Drives multi-host fs-stress runs over ssh from one control machine. The
# README's "Driving several hosts from one machine" has the details.
#
# Commands:
#   deploy BINARY             copy BINARY to ~/fs-stress on every host, check its sha256
#   preflight MOUNT           check on every host: same binary, MOUNT mounted with the
#                             same fs type, writable, clock within 100 ms of NTP
#   run MOUNT MODE [ARGS...]  preflight, `init` (with ARGS) on the first host, `workers`
#                             on every host, `verify` on the first host, fetch results;
#                             delete the run root if it passed, keep it otherwise
#   matrix MOUNT... [-- ARGS...]
#                             `run` for each MOUNT and mode, one at a time, then a table
#   selftest MOUNT [ARGS...]  `fs-stress selftest` on the first host
#   net-clean                 stop a leftover fault loop and remove its rules
#
# MOUNT is the mount point, the same path on every host; run roots go under
# MOUNT/fs-stress/. Results go to FS_STRESS_RESULTS/<mount>-<mode>-<timestamp>/.
#
# Environment:
#   FS_STRESS_HOSTS     required: ssh destinations, space-separated; the first runs
#                       init, verify and selftest
#   FS_STRESS_RESULTS   required: local directory for the fetched results
#   FS_STRESS_SSH_OPTS  ssh/scp options (default: -o BatchMode=yes -o ConnectTimeout=10)
#   FS_STRESS_MODES     modes for `matrix` (default: storage repository)
#
# Network faults, on one host while its workers run; on when RESET_EVERY or
# NETEM is set. They hit TCP from that host to the control machine's /24,
# except the control machine itself. The host needs passwordless sudo, nft, tc
# and ss.
#   FS_STRESS_NET_RESET_EVERY  seconds between resets, jittered 0.5-1.5x (try 15):
#                              live connections are killed with `ss -K`
#   FS_STRESS_NET_OUTAGE       seconds after each reset in which new connections are
#                              refused with a TCP reset (default: 0)
#   FS_STRESS_NETEM            `tc netem` arguments for outgoing traffic,
#                              e.g. "delay 5ms 2ms loss 1%"
#   FS_STRESS_NET_HOST         which host, 1-based (default: the last one)
set -euo pipefail

: "${FS_STRESS_HOSTS:?set FS_STRESS_HOSTS to the ssh destinations, space-separated}"
: "${FS_STRESS_RESULTS:?set FS_STRESS_RESULTS to a local results directory}"
read -r -a hosts <<<"$FS_STRESS_HOSTS"
read -r -a ssh_opts <<<"${FS_STRESS_SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=10}"
first=${hosts[0]}
# ssh starts remote commands in the home directory.
remote_bin=./fs-stress
# Bounds every remote command that only touches metadata: a mount whose server
# is down blocks I/O indefinitely.
quick_timeout=30
net_every=${FS_STRESS_NET_RESET_EVERY:-}
net_outage=${FS_STRESS_NET_OUTAGE:-0}
netem=${FS_STRESS_NETEM:-}
net_host=${hosts[$((${FS_STRESS_NET_HOST:-${#hosts[@]}} - 1))]}
# The loop stops at this file, or after this many seconds.
net_stop=/tmp/fs-stress-net.stop
net_limit=7200

# Runs as root on the fault host: `$0` names the process for `pkill`, then
# control address, reset period, outage, netem arguments, time limit.
# shellcheck disable=SC2016 # expanded on the fault host
net_loop='
set -u
ctl=$1 every=$2 outage=$3 netem=$4 limit=$5 stop=$6
net=${ctl%.*}
dev=$(ip -o route get "$ctl" | sed -n "s/.* dev \([^ ]*\).*/\1/p")
cleanup() {
  nft delete table inet fsstress 2>/dev/null
  [ -z "$netem" ] || tc qdisc del dev "$dev" root 2>/dev/null
  rm -f "$stop"
}
cleanup
trap cleanup EXIT
trap "exit 0" INT TERM HUP
nft add table inet fsstress
nft add chain inet fsstress out "{ type filter hook output priority 0; policy accept; }"
if [ -n "$netem" ]; then
  # Band 3 gets netem; the control machine goes to band 1, the rest by priomap.
  tc qdisc add dev "$dev" root handle 1: prio
  tc qdisc add dev "$dev" parent 1:3 handle 30: netem $netem
  tc filter add dev "$dev" parent 1: prio 1 protocol ip u32 match ip dst "$ctl/32" flowid 1:1
  tc filter add dev "$dev" parent 1: prio 2 protocol ip u32 match ip dst "$net.0/24" flowid 1:3
fi
echo "faults on $dev to $net.0/24 except $ctl: resets every ${every:-never}s (${outage}s outage), netem ${netem:-off}"
resets=0
end=$((SECONDS + limit))
while [ "$SECONDS" -lt "$end" ] && [ ! -e "$stop" ]; do
  if [ -z "$every" ]; then
    sleep 1
    continue
  fi
  ms=$((every * 500 + RANDOM * every * 1000 / 32768))
  sleep "$((ms / 1000)).$(printf %03d $((ms % 1000)))"
  [ -e "$stop" ] && break
  refuse=$(awk -v o="$outage" "BEGIN { print (o > 0) }")
  if [ "$refuse" = 1 ]; then
    nft add rule inet fsstress out ip daddr "$net.0/24" ip daddr != "$ctl" meta l4proto tcp reject with tcp reset
  fi
  ss -K -t state established "dst $net.0/24 and not dst $ctl" >/dev/null
  # IPv4-mapped sockets, e.g. BeeGFS'"'"'s.
  ss -K -t state established "dst ::ffff:$net.0/120" >/dev/null
  if [ "$refuse" = 1 ]; then
    sleep "$outage"
    nft flush chain inet fsstress out
  fi
  resets=$((resets + 1))
done
echo "resets: $resets"
'

die() {
  echo "error: $*" >&2
  exit 3
}

# Runs a shell command line on a host; quote remote arguments with `printf %q`.
# shellcheck disable=SC2029 # remote command lines are built and quoted locally
on() {
  local host=$1
  shift
  ssh "${ssh_opts[@]}" "$host" "$@"
}

# Shell-quotes each argument for a remote command line, space-separated.
quoted() {
  local a out=()
  for a in "$@"; do
    out+=("$(printf '%q' "$a")")
  done
  echo "${out[*]}"
}

net_faults() {
  [ -n "$net_every$netem" ]
}

# Kills a leftover fault loop and removes its rules.
cmd_net-clean() {
  on "$net_host" "sudo pkill -f 'fs-stress-net-loo[p]'; sudo nft delete table inet fsstress 2>/dev/null; \
    dev=\$(ip -o route get \${SSH_CONNECTION%% *} | sed -n 's/.* dev \([^ ]*\).*/\1/p'); \
    tc qdisc show dev \$dev | grep -q netem && sudo tc qdisc del dev \$dev root; sudo rm -f $net_stop; true"
}

# TCP and RPC counters on the fault host, to show the faults took effect.
net_counters() {
  on "$net_host" "grep '^Tcp:' /proc/net/snmp; nfsstat -rc 2>/dev/null; true"
}

remote_sha() {
  on "$1" "sha256sum $remote_bin 2>/dev/null | cut -d' ' -f1"
}

cmd_deploy() {
  [ $# -eq 1 ] || die "usage: deploy BINARY"
  local bin=$1 sha host
  [ -x "$bin" ] || die "not an executable: $bin"
  sha=$(sha256sum "$bin" | cut -d' ' -f1)
  for host in "${hosts[@]}"; do
    scp -q "${ssh_opts[@]}" "$bin" "$host:$remote_bin.new"
    on "$host" "chmod +x $remote_bin.new && mv $remote_bin.new $remote_bin"
    [ "$(remote_sha "$host")" = "$sha" ] || die "$host: checksum mismatch after copy"
    echo "$host: $sha"
  done
}

cmd_preflight() {
  [ $# -eq 1 ] || die "usage: preflight MOUNT"
  local mount=$1 host q sha first_sha="" fstype first_fstype="" offset failed=0
  q=$(quoted "$mount")
  for host in "${hosts[@]}"; do
    sha=$(remote_sha "$host")
    if [ -z "$sha" ]; then
      echo "$host: no ~/fs-stress, run deploy first" >&2
      failed=1
    elif [ -z "$first_sha" ]; then
      first_sha=$sha
    elif [ "$sha" != "$first_sha" ]; then
      echo "$host: ~/fs-stress differs from $first's" >&2
      failed=1
    fi

    if ! fstype=$(on "$host" "timeout $quick_timeout mountpoint -q $q && timeout $quick_timeout findmnt -no FSTYPE --mountpoint $q"); then
      echo "$host: $mount is not mounted, or doesn't answer within ${quick_timeout}s" >&2
      failed=1
      continue
    fi
    if [ -z "$first_fstype" ]; then
      first_fstype=$fstype
    elif [ "$fstype" != "$first_fstype" ]; then
      echo "$host: $mount is $fstype, but $first_fstype on $first" >&2
      failed=1
    fi

    if ! on "$host" "mkdir -p $q/fs-stress && f=$q/fs-stress/.preflight-\$\$ && timeout $quick_timeout touch \"\$f\" && rm \"\$f\""; then
      echo "$host: can't write to $mount/fs-stress" >&2
      failed=1
    fi

    # `System time : 0.000926122 seconds fast of NTP time`
    offset=$(on "$host" "chronyc -n tracking 2>/dev/null | awk '/^System time/ {print \$4}'") || true
    if [ -z "$offset" ]; then
      echo "$host: warning: no chrony; check that the clock is synchronised" >&2
    elif awk -v o="$offset" 'BEGIN { exit !(o > 0.1) }'; then
      echo "$host: clock is ${offset}s off NTP" >&2
      failed=1
    fi

    echo "$host: $fstype, clock ${offset:-?}s off, $(on "$host" "timeout $quick_timeout df -h --output=avail $q | tail -1 | tr -d ' '") free"
  done
  if net_faults; then
    if on "$net_host" "command -v nft tc ss >/dev/null && sudo -n true"; then
      cmd_net-clean
    else
      echo "$net_host: network faults need nft, tc, ss and passwordless sudo" >&2
      failed=1
    fi
  fi
  [ $failed -eq 0 ] || die "preflight failed for $mount"
}

cmd_run() {
  [ $# -ge 2 ] || die "usage: run MOUNT MODE [ARGS...]"
  local mount=$1 mode=$2
  shift 2
  local id root out q i pids=() host verify_rc=0 workers_rc=0 net_pid=
  id=$(basename "$mount")-$mode-$(date +%Y%m%d-%H%M%S)
  root=$mount/fs-stress/$id
  out=$FS_STRESS_RESULTS/$id
  q=$(quoted "$root")

  cmd_preflight "$mount"
  mkdir -p "$out"
  {
    echo "mount: $mount"
    echo "mode: $mode"
    echo "args: $*"
    echo "hosts: ${hosts[*]}"
    echo "run root: $root"
    echo "binary sha256: $(remote_sha "$first")"
    if net_faults; then
      echo "net faults: $net_host, resets every ${net_every:-never}s (${net_outage}s outage), netem ${netem:-off}"
    fi
  } >"$out/meta.txt"
  echo "run root: $root" >&2
  echo "results: $out" >&2

  on "$first" "$remote_bin init --root $q --hosts ${#hosts[@]} --mode $(quoted "$mode") $(quoted "$@")" 2>&1 |
    tee "$out/init.log"

  # Without a tty, ssh doesn't pass ^C on; tell the hosts to stop instead.
  trap 'echo "stopping $root" >&2; on "$first" "$remote_bin stop --root $q" || true' INT TERM
  if net_faults; then
    net_counters >"$out/net-before.txt"
    on "$net_host" "sudo bash -c $(quoted "$net_loop") fs-stress-net-loop \"\${SSH_CONNECTION%% *}\" \
      $(quoted "$net_every" "$net_outage" "$netem" "$net_limit" "$net_stop")" \
      >"$out/net-faults.log" 2>&1 &
    net_pid=$!
  fi
  for i in "${!hosts[@]}"; do
    host=${hosts[$i]}
    # A subshell, so that `wait` reports ssh's status rather than tee's.
    (on "$host" "$remote_bin workers --root $q" 2>&1 |
      sed -u "s/^/[$host] /" | tee "$out/workers-$i.log" >&2) &
    pids+=($!)
  done
  for i in "${!pids[@]}"; do
    if ! wait "${pids[$i]}"; then
      echo "${hosts[$i]}: workers failed" >&2
      workers_rc=1
    fi
  done
  trap - INT TERM
  if [ -n "$net_pid" ]; then
    on "$net_host" "sudo touch $net_stop" || true
    wait "$net_pid" || echo "$net_host: the network fault loop failed, see net-faults.log" >&2
    net_counters >"$out/net-after.txt"
    tail -1 "$out/net-faults.log" >&2
  fi

  on "$first" "$remote_bin verify --root $q" >"$out/summary.txt" 2> >(tee "$out/verify.log" >&2) || verify_rc=$?
  cat "$out/summary.txt"
  on "$first" "cd $q && tar -czf - run.json control go hosts done report.json 2>/dev/null" | tar -xzf - -C "$out" || true
  on "$first" "tar -C $q -czf - logs" >"$out/logs.tar.gz" || true

  if [ $verify_rc -eq 0 ] && [ $workers_rc -eq 0 ]; then
    on "$first" "rm -rf $q"
  else
    echo "run root kept: $root" >&2
  fi
  echo "verify exit: $verify_rc, workers exit: $workers_rc" >>"$out/meta.txt"
  if [ $verify_rc -ne 0 ]; then
    return $verify_rc
  fi
  return $workers_rc
}

cmd_matrix() {
  local mounts=() modes mount mode rc failed=0 lines=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do
    mounts+=("$1")
    shift
  done
  [ $# -gt 0 ] && shift
  [ ${#mounts[@]} -gt 0 ] || die "usage: matrix MOUNT... [-- ARGS...]"
  read -r -a modes <<<"${FS_STRESS_MODES:-storage repository}"
  for mount in "${mounts[@]}"; do
    for mode in "${modes[@]}"; do
      echo "=== $mount $mode" >&2
      rc=0
      (cmd_run "$mount" "$mode" "$@") || rc=$?
      [ $rc -eq 0 ] || failed=1
      lines+=("$(printf '%-20s %-11s %s' "$mount" "$mode" "$(result_word $rc)")")
    done
  done
  printf '%s\n' "${lines[@]}" | tee "$FS_STRESS_RESULTS/matrix-$(date +%Y%m%d-%H%M%S).txt"
  return $failed
}

result_word() {
  case $1 in
    0) echo PASS ;;
    1) echo FAIL ;;
    *) echo "ERROR (exit $1)" ;;
  esac
}

cmd_selftest() {
  [ $# -ge 1 ] || die "usage: selftest MOUNT [ARGS...]"
  local mount=$1 id parent out rc=0
  shift
  id=$(basename "$mount")-selftest-$(date +%Y%m%d-%H%M%S)
  parent=$mount/fs-stress/$id
  out=$FS_STRESS_RESULTS/$id
  cmd_preflight "$mount"
  mkdir -p "$out"
  on "$first" "$remote_bin selftest --root-parent $(quoted "$parent") $(quoted "$@")" \
    >"$out/summary.txt" 2> >(tee "$out/selftest.log" >&2) || rc=$?
  cat "$out/summary.txt"
  # Detected cases delete their run roots, so a pass leaves an empty parent.
  [ $rc -eq 0 ] && on "$first" "rmdir $(quoted "$parent")" || true
  return $rc
}

[ $# -ge 1 ] || die "usage: $0 deploy|preflight|run|matrix|selftest|net-clean ..."
command=$1
shift
case $command in
  deploy | preflight | run | matrix | selftest | net-clean) "cmd_$command" "$@" ;;
  *) die "unknown command: $command" ;;
esac
