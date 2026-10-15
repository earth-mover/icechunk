#!/usr/bin/env bash
# IBM Storage Scale (GPFS) Developer Edition: cluster "fs-stress-gpfs" with filesystem fs1 on data1
# (NSD nsd1), mounted at /gpfs/fs1. This node is the only quorum node, cluster manager, NSD
# server and admin node; the clients join as non-quorum nodes (phase join).
# install expects lab.sh provision --scale-installer to have put IBM's zip in /var/tmp/fs-stress-scale.
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

phase_install() {
  base_setup
  ensure_kernel "$KERNEL" kernel kernel-core kernel-modules-core kernel-modules kernel-devel
  install_gpfs
}

# mm* admin commands run as root over ssh/scp on every node, this one included. The clients
# get the public key from lab.sh provision.
known_host() { # known_host <name>: (re)record a node's current host key
  local n
  for n in "$1" "$1.fs-stress"; do ssh-keygen -R "$n" &>/dev/null || true; done
  ssh-keyscan "$1" "$1.fs-stress" 2>/dev/null >>/root/.ssh/known_hosts
}

phase_configure() {
  gpfs_hosts
  [[ -f /root/.ssh/id_ed25519 ]] || ssh-keygen -q -t ed25519 -N '' -f /root/.ssh/id_ed25519
  grep -qF "$(cat /root/.ssh/id_ed25519.pub)" /root/.ssh/authorized_keys 2>/dev/null ||
    cat /root/.ssh/id_ed25519.pub >>/root/.ssh/authorized_keys
  known_host gpfs-srv

  if ! mmlscluster &>/dev/null; then
    mmcrcluster -N gpfs-srv:quorum-manager -C fs-stress-gpfs -r /usr/bin/ssh -R /usr/bin/scp -A
    mmchlicense server --accept -N gpfs-srv
    # Default is 1G; this VM has 4G.
    mmchconfig pagepool=512M -N gpfs-srv
  fi

  [[ $(gpfs_state) == active ]] || mmstartup
  for _ in $(seq 60); do
    [[ $(gpfs_state) == active ]] && break
    sleep 2
  done

  local dev
  dev=$(data_disk 1)
  cat >/root/nsd.stanza <<EOS
%nsd: device=$dev nsd=nsd1 servers=gpfs-srv usage=dataAndMetadata failureGroup=1 pool=system
EOS
  # mmlsnsd exits 0 even when there are no disks.
  [[ $(mmlsnsd -Y) == *:nsd1:* ]] || mmcrnsd -F /root/nsd.stanza
  mmlsfs fs1 &>/dev/null || mmcrfs fs1 -F /root/nsd.stanza -T /gpfs/fs1 -A yes -B 1M -m 1 -r 1
  mountpoint -q /gpfs/fs1 || mmmount fs1
  for _ in $(seq 30); do
    mountpoint -q /gpfs/fs1 && break
    sleep 2
  done
  chmod 1777 /gpfs/fs1

  mmlscluster
  mmgetstate -a
  mmlsfs fs1 -T -B
  df -h /gpfs/fs1
}

# Captured, not piped to grep -q: that SIGPIPEs mmlscluster, which fails under pipefail.
is_member() { [[ $(mmlscluster -Y) == *":$1.fs-stress:"* ]]; }

# Add client nodes to the cluster, start GPFS on them and wait until they mount fs1.
# The clients must already have Storage Scale installed and this node's root key.
phase_join() { # phase_join <node...>
  local n
  shift
  gpfs_hosts
  for n in "$@"; do
    known_host "$n"
    if is_member "$n"; then
      # A rebuilt client is still a member but has lost its copy of the cluster config.
      ssh "$n" test -f /var/mmfs/gen/mmsdrfs || mmdelnode -N "$n"
    fi
    if ! is_member "$n"; then
      mmaddnode -N "$n"
      mmchlicense client --accept -N "$n"
      # Default is 1G; clients have 4G and run the other filesystems' clients too.
      mmchconfig pagepool=256M -N "$n"
    fi
  done
  local nodes want mounted=0
  nodes=$(IFS=,; echo "$*")
  want=$(IFS='|'; echo "$*")
  mmstartup -N "$nodes"
  for _ in $(seq 60); do
    mounted=$(mmlsmount fs1 -L -Y | grep -c -E ":($want)(\.fs-stress)?:" || true)
    [[ $mounted -eq $# ]] && break
    sleep 3
  done
  mmgetstate -a
  mmlsmount fs1 -L
  [[ $mounted -eq $# ]] || { log "fs1 is mounted on $mounted of: $*"; exit 1; }
}

"phase_$1" "$@"
