#!/usr/bin/env bash
# Ceph (cephadm, containerized) single-host cluster: mon+mgr+mds, one OSD on data1,
# CephFS "cephfs" with pool size 1. Client credentials: client.stress, written to
# /etc/ceph/ceph.client.stress.keyring next to a minimal ceph.conf (lab.sh provision copies both).
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

CEPHADM=/usr/local/sbin/cephadm
ceph() { "$CEPHADM" shell -- ceph "$@"; }

wait_for() { # wait_for <description> <cmd...>
  local what=$1
  shift
  for _ in $(seq 60); do
    "$@" &>/dev/null && return
    sleep 5
  done
  log "timed out waiting for $what"
  return 1
}

phase_install() {
  base_setup
  dnf install -y podman lvm2 chrony python3
  systemctl enable --now chronyd
  if [[ ! -x $CEPHADM ]]; then
    curl -fsSL -o "$CEPHADM" "https://download.ceph.com/rpm-$CEPH_RELEASE/el9/noarch/cephadm"
    chmod +x "$CEPHADM"
  fi
}

phase_configure() {
  if [[ ! -f /etc/ceph/ceph.conf ]]; then
    log "bootstrapping"
    "$CEPHADM" --image "quay.io/ceph/ceph:v$CEPH_RELEASE" bootstrap \
      --mon-ip "$CEPH_SRV_IP" --single-host-defaults \
      --skip-monitoring-stack --skip-dashboard
  fi
  wait_for "orchestrator" ceph orch status

  ceph config set global mon_allow_pool_size_one true
  ceph config set global osd_pool_default_size 1
  ceph config set global osd_pool_default_min_size 1
  ceph config set osd osd_memory_target_autotune false
  ceph config set osd osd_memory_target 1G

  local dev
  dev=$(data_disk 1)
  if [[ $(ceph osd stat) != *'1 up'* ]]; then
    ceph orch daemon add osd "$(hostname -s):$dev"
    wait_for "osd.0 up" sh -c "$CEPHADM shell -- ceph osd stat | grep -q '1 up'"
  fi

  [[ $(ceph fs ls) == *'name: cephfs'* ]] || ceph fs volume create cephfs
  # Pools created before osd_pool_default_size=1 took effect; .mgr may not exist yet.
  for pool in $(ceph osd pool ls); do
    ceph osd pool set "$pool" size 1 --yes-i-really-mean-it
  done
  ceph health mute POOL_NO_REDUNDANCY --sticky

  # Tentacle defaults to a new key type (2) that the EL9 kernel client can't decode
  # (mount.ceph fails with error 524), so the client key must be legacy AES. Caps are what
  # `ceph fs authorize cephfs client.stress / rw` grants, which has no --key_type option.
  ceph config set mon mon_auth_allow_insecure_key true
  ceph mon set auth_allowed_ciphers aes256k,aes
  ceph auth get client.stress &>/dev/null ||
    ceph auth get-or-create client.stress \
      mon 'allow r fsname=cephfs' mds 'allow rw fsname=cephfs' osd 'allow rw tag cephfs data=cephfs' \
      --key_type aes
  ceph config set mon mon_auth_allow_insecure_key false
  ceph auth get client.stress >/etc/ceph/ceph.client.stress.keyring
  # Expected on a single-OSD test cluster with a legacy client key.
  for w in TOO_FEW_OSDS AUTH_INSECURE_CLIENT_KEY_TYPE AUTH_INSECURE_KEYS_ALLOWED; do
    ceph health mute "$w" --sticky
  done
  ceph config generate-minimal-conf >/etc/ceph/ceph.minimal.conf
  wait_for "mds active" sh -c "$CEPHADM shell -- ceph fs status cephfs | grep -q active"
  ceph -s
}

"phase_$1"
