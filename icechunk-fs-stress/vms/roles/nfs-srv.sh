#!/usr/bin/env bash
# Linux knfsd serving NFSv3, 4.0, 4.1 and 4.2 from one xfs disk (data1 at /srv/nfs).
# One export per version, /srv/nfs/{v3,v40,v41,v42}. The server doesn't tie exports to
# versions; clients mount each export only at its version so tests can't interfere.
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

EXPORTS=(v3 v40 v41 v42)

phase_install() {
  base_setup
  ensure_kernel "$KERNEL" kernel kernel-core kernel-modules-core
  dnf install -y nfs-utils xfsprogs
}

phase_configure() {
  local dev
  dev=$(data_disk 1)
  blkid "$dev" &>/dev/null || mkfs.xfs "$dev"
  mkdir -p /srv/nfs
  fstab_line /srv/nfs "$dev /srv/nfs xfs defaults,noatime 0 0"
  mountpoint -q /srv/nfs || mount /srv/nfs

  for v in 3 4.0 4.1 4.2; do
    nfsconf --set nfsd "vers$v" y
  done

  : >/etc/exports
  for e in "${EXPORTS[@]}"; do
    mkdir -p "/srv/nfs/$e"
    # Clients' root is squashed, so the export roots are opened up here.
    chmod 1777 "/srv/nfs/$e"
    echo "/srv/nfs/$e $SUBNET_CIDR(rw,sync,no_subtree_check)" >>/etc/exports
  done

  systemctl enable --now rpcbind nfs-server
  systemctl restart nfs-server
  systemctl start rpc-statd
  exportfs -ra
  exportfs -v
  cat /proc/fs/nfsd/versions
}

"phase_$1"
