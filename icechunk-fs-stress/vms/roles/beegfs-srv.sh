#!/usr/bin/env bash
# BeeGFS 8.4 server: mgmtd + meta (ext4 on data1) + storage (xfs on data2).
# Connection auth uses a shared secret (/etc/beegfs/conn.auth, copied to clients by
# lab.sh provision). gRPC TLS is disabled; it only covers mgmtd <-> CLI traffic.
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

phase_install() {
  base_setup
  ensure_kernel "$KERNEL" kernel kernel-core kernel-modules-core
  curl -fsSL -o /etc/yum.repos.d/beegfs.repo "$BEEGFS_REPO_URL"
  dnf install -y beegfs-mgmtd libbeegfs-license beegfs-meta beegfs-storage beegfs-tools xfsprogs
}

mount_target() { # mount_target <disk-index> <fstype> <mountpoint> [mkfs-args...]
  local dev
  dev=$(data_disk "$1")
  blkid "$dev" &>/dev/null || "mkfs.$2" "${@:4}" "$dev"
  mkdir -p "$3"
  fstab_line "$3" "$dev $3 $2 defaults,noatime 0 0"
  mountpoint -q "$3" || mount "$3"
}

phase_configure() {
  # BeeGFS's recommended meta formatting. Default ext4 gives an 8G disk only 524k inodes,
  # which puts the target in the "emergency" capacity pool (<1M free) and caps file count.
  mount_target 1 ext4 /data/beegfs/meta -i 2048 -I 512 -J size=400 -O dir_index,filetype
  mount_target 2 xfs /data/beegfs/storage

  if [[ ! -f /etc/beegfs/conn.auth ]]; then
    dd if=/dev/random of=/etc/beegfs/conn.auth bs=128 count=1 status=none
    chmod 400 /etc/beegfs/conn.auth
  fi

  local toml=/etc/beegfs/beegfs-mgmtd.toml
  # Top-level TOML keys must precede the first [table], so prepend.
  sed -i '/^tls-disable\s*=/d' "$toml"
  sed -i '1i tls-disable = true' "$toml"
  echo 'export BEEGFS_TLS_DISABLE=true' >/etc/profile.d/beegfs.sh

  if [[ ! -f /etc/fs-stress-beegfs-init-done ]]; then
    /opt/beegfs/sbin/beegfs-mgmtd --init
    /opt/beegfs/sbin/beegfs-setup-meta -p /data/beegfs/meta -s 1 -m "$BEEGFS_SRV_IP"
    /opt/beegfs/sbin/beegfs-setup-storage -p /data/beegfs/storage -s 1 -i 101 -m "$BEEGFS_SRV_IP"
    touch /etc/fs-stress-beegfs-init-done
  fi
  systemctl enable --now beegfs-mgmtd
  systemctl enable --now beegfs-meta beegfs-storage
  sleep 5
  BEEGFS_TLS_DISABLE=true beegfs --mgmtd-addr "$BEEGFS_SRV_IP:8010" health check || true
}

"phase_$1"
