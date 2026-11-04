#!/usr/bin/env bash
# Lustre 2.17.0 server: MGS+MDT on data1, one OST on data2, fsname "lfs".
# The prebuilt osd-ldiskfs needs Whamcloud's patched kernel (missing jbd2 symbols on stock).
# shellcheck source=common.sh
source /tmp/fs-stress/common.sh

phase_install() {
  base_setup
  cat >/etc/yum.repos.d/lustre.repo <<EOF
[lustre-server]
name=Lustre server
baseurl=$LUSTRE_URL/server/
gpgcheck=0

[e2fsprogs-wc]
name=Whamcloud e2fsprogs
baseurl=$LUSTRE_E2FSPROGS_URL/
gpgcheck=0
EOF
  # Keep dnf from replacing the patched kernel with a stock one.
  dnf config-manager --save --setopt='baseos.excludepkgs=kernel*' --setopt='appstream.excludepkgs=kernel*'
  ensure_kernel "$LUSTRE_SRV_KERNEL" kernel kernel-core kernel-modules-core kernel-modules
  dnf install -y e2fsprogs lustre kmod-lustre kmod-lustre-osd-ldiskfs lustre-osd-ldiskfs-mount
}

phase_configure() {
  lnet_config
  modprobe lnet
  local mdt ost
  mdt=$(data_disk 1) ost=$(data_disk 2)
  mkdir -p /mnt/mdt /mnt/ost0
  if ! blkid -o value -s LABEL "$mdt" | grep -q ^lfs; then
    log "formatting MGS/MDT on $mdt"
    mkfs.lustre --fsname=lfs --mgs --mdt --index=0 "$mdt"
  fi
  if ! blkid -o value -s LABEL "$ost" | grep -q ^lfs; then
    log "formatting OST on $ost"
    mkfs.lustre --fsname=lfs --ost --index=0 --mgsnode="$LUSTRE_SRV_IP@tcp" "$ost"
  fi
  fstab_line /mnt/mdt "$mdt /mnt/mdt lustre defaults,_netdev 0 0"
  fstab_line /mnt/ost0 "$ost /mnt/ost0 lustre defaults,_netdev 0 0"
  mountpoint -q /mnt/mdt || mount /mnt/mdt
  mountpoint -q /mnt/ost0 || mount /mnt/ost0
  lctl dl
}

"phase_$1"
