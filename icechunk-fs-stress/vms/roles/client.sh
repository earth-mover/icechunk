#!/usr/bin/env bash
# Client for all the filesystems. Mounts, all writable by the login user:
#   /mnt/lustre                       Lustre
#   /mnt/beegfs                       BeeGFS if $BEEGFS is 1; unlicensed BeeGFS allows 5 clients
#   /mnt/cephfs                       CephFS kernel client
#   /mnt/cephfs-fuse                  CephFS via ceph-fuse
#   /mnt/nfs{3,40,41,42}              NFS, one export per version
#   /gpfs/fs1                         GPFS (Storage Scale) if $GPFS is 1; one mount per node.
#                                     Mounted by gpfs-srv's join phase, which adds this node
#                                     to the cluster
# configure expects lab.sh provision to have copied the server artifacts into /tmp/fs-stress/artifacts.
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

phase_install() {
  base_setup
  # kernel-devel is for the BeeGFS client module autobuild.
  ensure_kernel "$KERNEL" kernel kernel-core kernel-modules-core kernel-modules kernel-devel
  enable_epel

  cat >/etc/yum.repos.d/lustre.repo <<EOF
[lustre-client]
name=Lustre client
baseurl=$LUSTRE_URL/client/
gpgcheck=0
EOF
  cat >/etc/yum.repos.d/ceph.repo <<EOF
[ceph]
name=Ceph
baseurl=https://download.ceph.com/rpm-$CEPH_RELEASE/el9/\$basearch
gpgcheck=1
gpgkey=https://download.ceph.com/keys/release.asc

[ceph-noarch]
name=Ceph noarch
baseurl=https://download.ceph.com/rpm-$CEPH_RELEASE/el9/noarch
gpgcheck=1
gpgkey=https://download.ceph.com/keys/release.asc
EOF
  curl -fsSL -o /etc/yum.repos.d/beegfs.repo "$BEEGFS_REPO_URL"

  dnf install -y kmod-lustre-client lustre-client \
    beegfs-client beegfs-tools elfutils-libelf-devel gcc make \
    ceph-common ceph-fuse \
    nfs-utils \
    python3.12 python3.12-pip git \
    nftables # just the nft tool: nftables.service stays disabled (no ruleset loaded at boot)
  [[ $GPFS == 0 ]] || install_gpfs
}

open_up() { # make a filesystem root writable by everyone (sticky, like /tmp)
  chmod 1777 "$1"
}

setup_lustre() {
  lnet_config
  modprobe lustre
  mkdir -p /mnt/lustre
  fstab_line /mnt/lustre "$LUSTRE_SRV_IP@tcp:/lfs /mnt/lustre lustre defaults,_netdev 0 0"
  mount_fstab /mnt/lustre
  open_up /mnt/lustre
}

setup_beegfs() {
  if [[ $BEEGFS == 0 ]]; then
    # The package's default mounts file names /mnt/beegfs, which fs-stress-mount-retry
    # would keep retrying.
    : >/etc/beegfs/beegfs-mounts.conf
    systemctl disable --now beegfs-client 2>/dev/null || true
    return
  fi
  install -m 400 /tmp/fs-stress/artifacts/conn.auth /etc/beegfs/conn.auth
  /opt/beegfs/sbin/beegfs-setup-client -m "$BEEGFS_SRV_IP"
  echo "/mnt/beegfs /etc/beegfs/beegfs-client.conf" >/etc/beegfs/beegfs-mounts.conf
  echo 'export BEEGFS_TLS_DISABLE=true' >/etc/profile.d/beegfs.sh
  mkdir -p /mnt/beegfs
  systemctl enable beegfs-client
  # First start builds the kernel module, which takes a while.
  mountpoint -q /mnt/beegfs || systemctl restart beegfs-client
  open_up /mnt/beegfs
}

setup_ceph() {
  install -m 644 /tmp/fs-stress/artifacts/ceph.conf /etc/ceph/ceph.conf
  install -m 600 /tmp/fs-stress/artifacts/ceph.client.stress.keyring /etc/ceph/ceph.client.stress.keyring
  mkdir -p /mnt/cephfs /mnt/cephfs-fuse
  fstab_line /mnt/cephfs "stress@.cephfs=/ /mnt/cephfs ceph _netdev 0 0"
  fstab_line /mnt/cephfs-fuse "none /mnt/cephfs-fuse fuse.ceph ceph.id=stress,_netdev 0 0"
  for m in cephfs cephfs-fuse; do
    mount_fstab "/mnt/$m"
  done
  open_up /mnt/cephfs
}

setup_nfs() {
  local v m
  for v in 3 4.0 4.1 4.2; do
    m=/mnt/nfs${v//./}
    mkdir -p "$m"
    fstab_line "$m" "$NFS_SRV_IP:/srv/nfs/v${v//./} $m nfs vers=$v,_netdev 0 0"
    mount_fstab "$m"
  done
}

# Joining the cluster happens from gpfs-srv (phase join); here we only let its root in.
setup_gpfs() {
  gpfs_hosts
  local key
  key=$(cat /tmp/fs-stress/artifacts/gpfs-admin.pub)
  grep -qF "$key" /root/.ssh/authorized_keys 2>/dev/null || echo "$key" >>/root/.ssh/authorized_keys
}

# Clients often boot before their servers (cephadm's containers and BeeGFS mgmtd are slow),
# and fstab mounts and beegfs-client don't retry. This oneshot does, for up to 10 minutes.
setup_mount_retry() {
  cat >/usr/local/sbin/fs-stress-mount-retry <<'EOF'
#!/bin/bash
for _ in $(seq 120); do
  missing=0
  # Through systemd, not mount(8): a plain mount while the unit's own attempt is still
  # in progress stacks a second mount on top of it.
  for m in $(awk '$3 ~ /^(lustre|ceph|fuse\.ceph|nfs|nfs4)$/ { print $2 }' /etc/fstab); do
    mountpoint -q "$m" || systemctl start "$(systemd-escape -p --suffix=mount "$m")" || missing=1
  done
  for m in $(awk '{ print $1 }' /etc/beegfs/beegfs-mounts.conf 2>/dev/null); do
    if ! mountpoint -q "$m"; then
      systemctl restart beegfs-client
      mountpoint -q "$m" || missing=1
      break
    fi
  done
  [[ $missing == 0 ]] && exit 0
  sleep 5
done
exit 1
EOF
  chmod 755 /usr/local/sbin/fs-stress-mount-retry
  cat >/etc/systemd/system/fs-stress-mount-retry.service <<'EOF'
[Unit]
Description=Retry distributed filesystem mounts that lost the race against their servers
Wants=network-online.target
After=network-online.target remote-fs.target beegfs-client.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/usr/local/sbin/fs-stress-mount-retry
TimeoutStartSec=15min

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable fs-stress-mount-retry.service
}

phase_configure() {
  setup_lustre
  setup_beegfs
  setup_ceph
  setup_nfs
  [[ $GPFS == 0 ]] || setup_gpfs
  setup_mount_retry
  findmnt -t lustre,beegfs,ceph,fuse.ceph-fuse,nfs,nfs4,gpfs
}

"phase_$1"
