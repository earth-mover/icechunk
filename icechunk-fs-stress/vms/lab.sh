#!/usr/bin/env bash
# A lab of libvirt VMs serving Lustre, BeeGFS, CephFS, NFS and optionally GPFS to a number
# of client VMs, which also form a Slurm cluster, for running fs-stress across hosts. See
# README.md.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: lab.sh [--lab NAME] COMMAND [ARGS...]
       lab.sh --help

A lab of libvirt VMs (qemu:///system) serving Lustre, BeeGFS, CephFS, NFS and optionally
GPFS to client VMs, which also form a Slurm cluster, for fs-stress. See README.md next to
this script.

Options:
  --lab NAME   which lab (default: ic-fs-stress). Names the libvirt domains (NAME-<vm>),
               network, storage pool (/var/lib/libvirt/NAME) and cache directory.

Commands:
  up [--gpfs] [--no-slurm] [--subnet A.B.C] [--clients N] [--client-memory MIB]
     [--client-vcpus N] [VM...]
      Create the network, storage pools and VMs, and wait for ssh. Existing pieces are kept.
      Without VMs: the servers except gpfs-srv, and clients up to --clients (default 2);
      slurm-srv only when the lab is created.
      --gpfs           also create gpfs-srv, which turns GPFS on for the lab
      --no-slurm       don't create slurm-srv; Slurm is on for the lab while slurm-srv exists
      --subnet A.B.C   the lab's /24, only when its network is created (default 10.77.0)
      --clients N      create client1..clientN (at most 79)
      --client-memory  RAM of clients created now, in MiB (default 4096)
      --client-vcpus   vCPUs of clients created now (default 4)

  provision [--scale-installer ZIP] [VM...]
      Install and configure VMs (default: all of them). Install runs in parallel; then
      servers are configured, their credentials copied to the clients, and the clients
      configured: with Slurm on, after slurm-srv has added them as nodes, and with GPFS
      on, then added to the GPFS cluster. Re-runnable.
      --scale-installer ZIP   IBM Storage Scale Developer Edition zip; needed while a GPFS
                              node (gpfs-srv, clients with GPFS on) doesn't have it installed

  start [VM...]    Start VMs (default: all): servers, then clients; wait for every mount.
  stop [VM...]     Shut VMs down (default: all), clients first, keeping all state.
  down [VM...]     Destroy VMs and their disks. Without VMs: the whole lab, including its
                   network, storage pool and base image.
  ssh VM [CMD...]  ssh to a VM as rocky (passwordless sudo).
  status           The lab's network, and each VM's state, IP, ssh and client mounts.

  fs-stress [--results DIR] [--clients C1,C2,...] COMMAND [ARGS...]
      Run scripts/remote.sh COMMAND on the lab's clients (default: all of them), with the
      lab's ssh options. Results go to DIR (default: the lab's cache directory, results/).
      Run `lab.sh fs-stress --help` for remote.sh's commands.

VMs: ceph-srv lustre-srv beegfs-srv nfs-srv slurm-srv gpfs-srv client1 ... client79
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

here=$(cd "$(dirname "$0")" && pwd)
export LIBVIRT_DEFAULT_URI=qemu:///system

# Rocky 9.7, not 9.8: no Lustre release supports 9.8 kernels yet. 611.55.1 is the final
# 9.7 kernel; the Lustre 2.17.0 client kmods and BeeGFS 8.4 client both support it.
ROCKY_IMAGE_URL=https://dl.rockylinux.org/vault/rocky/9.7/images/x86_64/Rocky-9-GenericCloud-Base-9.7-20251123.2.x86_64.qcow2
KERNEL=5.14.0-611.55.1.el9_7
LUSTRE_SRV_KERNEL=5.14.0-611.13.1_lustre.el9
LUSTRE_URL=https://downloads.whamcloud.com/public/lustre/lustre-2.17.0/el9.7
LUSTRE_E2FSPROGS_URL=https://downloads.whamcloud.com/public/e2fsprogs/latest/el9
BEEGFS_REPO_URL=https://www.beegfs.io/release/beegfs_8.4/dists/beegfs-rhel9.repo
CEPH_RELEASE=20.2.4
OHPC_RELEASE_URL=https://repos.openhpc.community/OpenHPC/3/EL_9/x86_64/ohpc-release-3-1.el9.x86_64.rpm
# OpenHPC's updates repo keeps only the latest Slurm: once it's gone, provisioning fails and
# this needs bumping, for every VM (slurmctld mustn't be older than any slurmd).
SLURM_VERSION=25.11.8
BASE_VOL=rocky9-base.qcow2

VM_USER=rocky
DEFAULT_SUBNET=10.77.0
# Every lab's network uses this DNS domain, which also marks a network as a lab's.
DNS_DOMAIN=fs-stress
# Where virt-install puts the cloud-init seed ISO; shared by all labs.
BOOT_POOL=boot-scratch
BOOT_POOL_DIR=/var/lib/libvirt/boot

# server -> "last-octet ram-MiB vcpus os-disk-GiB [data-disk-GiB ...]"
declare -A SERVER_SPECS=(
  [ceph-srv]="10 6144 4 25 30"
  [lustre-srv]="11 4096 2 20 8 25"
  [beegfs-srv]="12 3072 2 20 8 25"
  [nfs-srv]="13 2048 2 20 25"
  [gpfs-srv]="14 4096 2 25 25"
  [slurm-srv]="15 1536 1 20"
)
SERVERS=(ceph-srv lustre-srv beegfs-srv nfs-srv slurm-srv gpfs-srv)
# clientK has last octet CLIENT_OCTET_BASE+K; DHCP's dynamic range starts at .100.
CLIENT_OCTET_BASE=20
MAX_CLIENTS=79
CLIENT_OS_DISK=20

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5)

lab=ic-fs-stress
subnet=

set_lab() {
  [[ $1 =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "lab name must be lowercase letters, digits and dashes: $1"
  lab=$1
  NET=$lab
  POOL=$lab
  POOL_DIR=/var/lib/libvirt/$lab
  CACHE_DIR=${XDG_CACHE_HOME:-$HOME/.cache}/$lab
  LOG_DIR=$CACHE_DIR/logs
  ART_DIR=$CACHE_DIR/artifacts
}

dom() { echo "$lab-$1"; }

client_index() { # client_index <vm>: K for clientK, fails for servers
  [[ $1 =~ ^client([1-9][0-9]*)$ ]] && echo "${BASH_REMATCH[1]}"
}
is_client() { [[ $1 =~ ^client[1-9][0-9]*$ ]]; }
is_server() { [[ -n ${SERVER_SPECS[$1]:-} ]]; }

check_vm_name() {
  local k
  if is_server "$1"; then
    return
  elif k=$(client_index "$1") && [[ $k -le $MAX_CLIENTS ]]; then
    return
  fi
  die "unknown VM: $1 (servers: ${SERVERS[*]}; clients: client1..client$MAX_CLIENTS)"
}

vm_octet() {
  if is_server "$1"; then
    local -a f
    read -ra f <<<"${SERVER_SPECS[$1]}"
    echo "${f[0]}"
  else
    echo $((CLIENT_OCTET_BASE + $(client_index "$1")))
  fi
}

# Commands call this before anything that runs vm_ip in a subshell, so the subnet is
# looked up once.
load_subnet() {
  [[ -n $subnet ]] && return
  local xml
  xml=$(virsh net-dumpxml "$NET" 2>/dev/null) || die "lab $lab doesn't exist; create it with: lab.sh --lab $lab up"
  [[ $xml =~ \<ip\ address=\'([0-9]+\.[0-9]+\.[0-9]+)\.1\' ]] || die "can't find the subnet of network $NET"
  subnet=${BASH_REMATCH[1]}
}

vm_ip() {
  load_subnet
  echo "$subnet.$(vm_octet "$1")"
}

# The MAC holds the IP's last three octets, so labs on different subnets don't share MACs.
vm_mac() {
  load_subnet
  local -a o
  IFS=. read -ra o <<<"$subnet"
  printf '52:54:00:%02x:%02x:%02x\n' "${o[1]}" "${o[2]}" "$(vm_octet "$1")"
}

vm_exists() { virsh dominfo "$(dom "$1")" &>/dev/null; }
vm_state() { virsh domstate "$(dom "$1")" 2>/dev/null || echo missing; }
gpfs_on() { vm_exists gpfs-srv; }
slurm_on() { vm_exists slurm-srv; }

# Existing VMs of the lab: servers in order, then clients by index.
existing_vms() {
  local names vm
  names=$(virsh list --all --name) || return 1
  for vm in "${SERVERS[@]}"; do
    if grep -qx "$(dom "$vm")" <<<"$names"; then echo "$vm"; fi
  done
  sed -n "s/^$lab-\(client[1-9][0-9]*\)\$/\1/p" <<<"$names" | sort -V
}

existing_clients() { existing_vms | grep '^client' || true; }

split_roles() { # split_roles <vm...>: sets the globals servers and clients
  servers=() clients=()
  local vm
  for vm in "$@"; do
    if is_client "$vm"; then clients+=("$vm"); else servers+=("$vm"); fi
  done
}

# Unlicensed BeeGFS registers at most 5 clients.
has_beegfs() { [[ $(client_index "$1") -le 5 ]]; }

client_mounts() { # client_mounts <client>: the mount points it should have
  echo /mnt/lustre /mnt/cephfs /mnt/cephfs-fuse /mnt/nfs3 /mnt/nfs40 /mnt/nfs41 /mnt/nfs42
  if has_beegfs "$1"; then echo /mnt/beegfs; fi
  if gpfs_on; then echo /gpfs/fs1; fi
}

MOUNT_TYPES=lustre,beegfs,ceph,fuse.ceph-fuse,nfs,nfs4,gpfs

# Prints a client's mount points that aren't mounted; fails if it can't tell. findmnt reads
# the mount table without touching the mounts, but the timeout still guards against a hung
# VM. findmnt exits with 1 when nothing is mounted.
missing_mounts() {
  local mounted m
  mounted=$(vm_ssh "$1" "timeout 10 findmnt -rn -t $MOUNT_TYPES -o TARGET; [ \$? -le 1 ]") || return 1
  for m in $(client_mounts "$1"); do
    grep -qx "$m" <<<"$mounted" || echo "$m"
  done
}

# shellcheck disable=SC2029 # remote command lines are built locally
vm_ssh() { # vm_ssh <vm> [cmd...]
  local vm=$1
  shift
  ssh "${SSH_OPTS[@]}" "$VM_USER@$(vm_ip "$vm")" "$@"
}

vm_scp() { # vm_scp <vm> <local> <remote>
  scp "${SSH_OPTS[@]}" "$2" "$VM_USER@$(vm_ip "$1"):$3"
}

wait_ssh() { # wait_ssh <vm> <seconds>
  local end=$((SECONDS + $2))
  until vm_ssh "$1" true 2>/dev/null; do
    if [[ $SECONDS -ge $end ]]; then
      echo "$1: no ssh after $2 s" >&2
      return 1
    fi
    sleep 3
  done
}

# Sets the global vms to the VMs named on the command line, which must exist, or to all
# existing VMs.
select_vms() {
  local vm out
  vms=()
  if [[ $# -eq 0 ]]; then
    out=$(existing_vms) || die "can't list libvirt domains"
    [[ -z $out ]] || mapfile -t vms <<<"$out"
    return
  fi
  for vm in "$@"; do
    check_vm_name "$vm"
    vm_exists "$vm" || die "$vm doesn't exist in lab $lab"
    vms+=("$vm")
  done
}

need_tools() {
  local t missing=()
  for t in "$@"; do
    command -v "$t" >/dev/null || missing+=("$t")
  done
  [[ ${#missing[@]} -eq 0 ]] || die "missing tools: ${missing[*]}"
}

# --- up ---------------------------------------------------------------------------------

ensure_network() {
  local defined=false
  if ! virsh net-info "$NET" &>/dev/null; then
    defined=true
    virsh net-define /dev/stdin <<EOF
<network>
  <name>$NET</name>
  <forward mode='nat'/>
  <domain name='$DNS_DOMAIN' localOnly='yes'/>
  <ip address='$subnet.1' netmask='255.255.255.0'>
    <dhcp>
      <range start='$subnet.100' end='$subnet.200'/>
    </dhcp>
  </ip>
</network>
EOF
  fi
  [[ $(virsh net-info "$NET") =~ Active:\ +yes ]] && return
  if ! virsh net-start "$NET"; then
    # Don't keep a network that can't start, so that up can try again with another subnet.
    $defined && virsh net-undefine "$NET" >/dev/null
    die "can't start network $NET; if $subnet.0/24 is in use, pick another with --subnet"
  fi
}

# A fixed address and DNS name for the VM, so the lab doesn't depend on DHCP lease order.
ensure_vm_address() {
  local vm=$1 xml
  xml=$(virsh net-dumpxml "$NET")
  [[ $xml == *"$(vm_mac "$vm")"* ]] && return
  virsh net-update "$NET" add ip-dhcp-host \
    "<host mac='$(vm_mac "$vm")' name='$vm' ip='$(vm_ip "$vm")'/>" --live --config >/dev/null
  virsh net-update "$NET" add dns-host \
    "<host ip='$(vm_ip "$vm")'><hostname>$vm</hostname></host>" --live --config >/dev/null
}

ensure_pool() { # ensure_pool <name> <dir>
  if ! virsh pool-info "$1" &>/dev/null; then
    virsh pool-define-as "$1" dir --target "$2" >/dev/null
    virsh pool-build "$1" >/dev/null
  fi
  [[ $(virsh pool-info "$1") =~ State:\ +running ]] || virsh pool-start "$1" >/dev/null
}

ensure_base_image() {
  virsh vol-info "$BASE_VOL" --pool "$POOL" &>/dev/null && return
  mkdir -p "$CACHE_DIR"
  local img
  img="$CACHE_DIR/$(basename "$ROCKY_IMAGE_URL")"
  if [[ ! -f $img ]]; then
    curl -fL# --retry 3 -o "$img.part" "$ROCKY_IMAGE_URL"
    mv "$img.part" "$img"
  fi
  virsh vol-create-as "$POOL" "$BASE_VOL" "$(stat -c %s "$img")" --format qcow2 >/dev/null
  virsh vol-upload "$BASE_VOL" "$img" --pool "$POOL"
}

ensure_vol() { # ensure_vol <name> <GiB> [backing]
  virsh vol-info "$1" --pool "$POOL" &>/dev/null && return
  if [[ -n ${3:-} ]]; then
    virsh vol-create-as "$POOL" "$1" "${2}G" --format qcow2 --backing-vol "$3" --backing-vol-format qcow2 >/dev/null
  else
    virsh vol-create-as "$POOL" "$1" "${2}G" --format qcow2 >/dev/null
  fi
}

create_vm() { # create_vm <vm> <client-ram-MiB> <client-vcpus>
  local vm=$1 d
  d=$(dom "$vm")
  if vm_exists "$vm"; then
    echo "$vm exists, skipping"
    return
  fi
  local ram vcpus os data=()
  if is_server "$vm"; then
    local -a f
    read -ra f <<<"${SERVER_SPECS[$vm]}"
    ram=${f[1]} vcpus=${f[2]} os=${f[3]} data=("${f[@]:4}")
  else
    ram=$2 vcpus=$3 os=$CLIENT_OS_DISK
  fi

  ensure_vm_address "$vm"
  ensure_vol "$vm-os.qcow2" "$os" "$BASE_VOL"
  local -a disks=(--disk "vol=$POOL/$vm-os.qcow2,bus=virtio")
  local i=1 size
  for size in "${data[@]}"; do
    ensure_vol "$vm-data$i.qcow2" "$size"
    disks+=(--disk "vol=$POOL/$vm-data$i.qcow2,bus=virtio,serial=data$i")
    i=$((i + 1))
  done

  local tmp
  tmp=$(mktemp -d)
  cat >"$tmp/meta-data" <<EOF
instance-id: $d
local-hostname: $vm
EOF
  local keys
  # ssh-add -L prints a message instead of keys when the agent has none.
  keys=$({ ssh-add -L 2>/dev/null || true; cat ~/.ssh/*.pub 2>/dev/null || true; } |
    grep -E '^(ssh-|ecdsa-|sk-)' | awk '!seen[$0]++') || true
  local -a keys_list
  mapfile -t keys_list <<<"$keys"
  if [[ -z $keys ]]; then
    rm -rf "$tmp"
    die "no ssh public keys in ssh-add -L or ~/.ssh/*.pub"
  fi
  {
    echo "#cloud-config"
    echo "ssh_authorized_keys:"
    printf '  - %s\n' "${keys_list[@]}"
  } >"$tmp/user-data"

  echo "$vm: creating ($ram MiB, $vcpus vCPUs, $(vm_ip "$vm"))"
  virt-install \
    --name "$d" \
    --memory "$ram" --vcpus "$vcpus" --cpu host-passthrough \
    --osinfo linux2022 \
    --boot uefi,firmware.feature0.name=secure-boot,firmware.feature0.enabled=no \
    --import "${disks[@]}" \
    --network "network=$NET,mac=$(vm_mac "$vm"),model=virtio" \
    --cloud-init "user-data=$tmp/user-data,meta-data=$tmp/meta-data" \
    --graphics none --noautoconsole >/dev/null
  rm -rf "$tmp"
  created+=("$vm")
}

cmd_up() {
  local gpfs=false slurm=true want_subnet="" nclients="" ram=4096 vcpus=4 vms=() vm k
  while [[ $# -gt 0 ]]; do
    case $1 in
      --gpfs) gpfs=true ;;
      --no-slurm) slurm=false ;;
      --subnet) want_subnet=${2:?--subnet needs a value}; shift ;;
      --clients) nclients=${2:?--clients needs a value}; shift ;;
      --client-memory) ram=${2:?--client-memory needs a value}; shift ;;
      --client-vcpus) vcpus=${2:?--client-vcpus needs a value}; shift ;;
      -*) die "up: unknown option: $1" ;;
      *) check_vm_name "$1"; vms+=("$1") ;;
    esac
    shift
  done
  [[ -z $nclients || ($nclients =~ ^[0-9]+$ && $nclients -le $MAX_CLIENTS) ]] ||
    die "--clients must be 0 to $MAX_CLIENTS"
  [[ $ram =~ ^[0-9]+$ && $vcpus =~ ^[0-9]+$ ]] || die "--client-memory and --client-vcpus take numbers"
  need_tools virsh virt-install xorriso curl ssh scp

  # Slurm is on by default only for a new lab, so that adding clients doesn't turn it on.
  if virsh net-info "$NET" &>/dev/null; then slurm=false; fi
  if [[ ${#vms[@]} -eq 0 ]]; then
    for vm in "${SERVERS[@]}"; do
      [[ $vm == gpfs-srv || ($vm == slurm-srv && $slurm == false) ]] || vms+=("$vm")
    done
    nclients=${nclients:-2}
  fi
  $gpfs && vms+=(gpfs-srv)
  for ((k = 1; k <= ${nclients:-0}; k++)); do vms+=("client$k"); done
  # Each VM once, in the order given.
  mapfile -t vms < <(printf '%s\n' "${vms[@]}" | awk '!seen[$0]++')

  if virsh net-info "$NET" &>/dev/null; then
    load_subnet
    [[ -z $want_subnet || $want_subnet == "$subnet" ]] ||
      die "lab $lab already uses $subnet.0/24; --subnet only applies when the lab is created"
  else
    subnet=${want_subnet:-$DEFAULT_SUBNET}
    [[ $subnet =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$ ]] &&
      [[ ${BASH_REMATCH[1]} -le 255 && ${BASH_REMATCH[2]} -le 255 && ${BASH_REMATCH[3]} -le 255 ]] ||
      die "--subnet takes the first three octets of a /24, e.g. 10.77.0"
  fi

  ensure_network
  ensure_pool "$POOL" "$POOL_DIR"
  ensure_pool "$BOOT_POOL" "$BOOT_POOL_DIR"
  ensure_base_image
  created=()
  for vm in "${vms[@]}"; do create_vm "$vm" "$ram" "$vcpus"; done
  for vm in "${created[@]}"; do
    printf '%s: waiting for ssh... ' "$vm"
    wait_ssh "$vm" 600
    echo ok
  done
}

# --- provision --------------------------------------------------------------------------

role_of() { if is_client "$1"; then echo client; else echo "$1"; fi; }

slurm_node() { # slurm_node <client>: name:CPUs:RealMemory (MiB)
  local info cpus kib mib crash
  info=$(virsh dominfo "$(dom "$1")")
  cpus=$(awk '/^CPU\(s\):/ { print $2 }' <<<"$info")
  kib=$(awk '/^Max memory:/ { print $3 }' <<<"$info")
  [[ $cpus =~ ^[1-9][0-9]*$ && $kib =~ ^[1-9][0-9]*$ ]] || die "can't read $1's CPUs and memory"
  mib=$((kib / 1024))
  # Below what slurmd detects: the RAM without the crash kernel (Rocky's crashkernel=
  # 1G-4G:192M,4G-64G:256M,64G-:512M), with margin for what the kernel takes; a 2048 MiB
  # client has 1766 MiB.
  if ((mib < 1024)); then
    crash=0
  elif ((mib < 4096)); then
    crash=192
  elif ((mib < 65536)); then
    crash=256
  else
    crash=512
  fi
  echo "$1:$cpus:$((mib - crash - mib / 16 - 64))"
}

write_role_env() { # write_role_env <vm> <file>
  local vm=$1 gpfs=0 slurm=0 nodes snodes="" c
  if gpfs_on; then gpfs=1; fi
  nodes="gpfs-srv:$(vm_ip gpfs-srv)"
  for c in $(existing_clients); do nodes+=" $c:$(vm_ip "$c")"; done
  if slurm_on; then slurm=1; fi
  if [[ $vm == slurm-srv ]]; then
    for c in $(existing_clients); do snodes+="${snodes:+ }$(slurm_node "$c")"; done
  fi
  cat >"$2" <<EOF
KERNEL=$KERNEL
LUSTRE_SRV_KERNEL=$LUSTRE_SRV_KERNEL
LUSTRE_URL=$LUSTRE_URL
LUSTRE_E2FSPROGS_URL=$LUSTRE_E2FSPROGS_URL
BEEGFS_REPO_URL=$BEEGFS_REPO_URL
CEPH_RELEASE=$CEPH_RELEASE
OHPC_RELEASE_URL=$OHPC_RELEASE_URL
SLURM_VERSION=$SLURM_VERSION
CEPH_SRV_IP=$(vm_ip ceph-srv)
LUSTRE_SRV_IP=$(vm_ip lustre-srv)
BEEGFS_SRV_IP=$(vm_ip beegfs-srv)
NFS_SRV_IP=$(vm_ip nfs-srv)
GPFS_SRV_IP=$(vm_ip gpfs-srv)
GPFS_NODES="$nodes"
SUBNET_CIDR=$subnet.0/24
GPFS=$gpfs
SLURM_NODES="$snodes"
SLURM=$slurm
EOF
  if is_client "$vm"; then
    if has_beegfs "$vm"; then echo BEEGFS=1; else echo BEEGFS=0; fi >>"$2"
  fi
}

push() {
  local vm=$1 env f
  vm_ssh "$vm" 'rm -rf /tmp/fs-stress && mkdir -p /tmp/fs-stress/artifacts'
  tar -C "$here/roles" -c common.sh "$(role_of "$vm").sh" | vm_ssh "$vm" tar -C /tmp/fs-stress -x
  env=$(mktemp)
  write_role_env "$vm" "$env"
  vm_scp "$vm" "$env" /tmp/fs-stress/role-env.sh
  rm -f "$env"
  if is_client "$vm"; then
    for f in "$ART_DIR"/*; do
      if [[ -f $f ]]; then vm_scp "$vm" "$f" "/tmp/fs-stress/artifacts/$(basename "$f")"; fi
    done
  fi
}

SCALE_DEST=/var/tmp/fs-stress-scale
# The uploaded zip's name on the VM, whatever it's called on the host.
SCALE_ZIP=$SCALE_DEST/Storage_Scale_installer.zip

needs_gpfs() { [[ $1 == gpfs-srv ]] || { is_client "$1" && gpfs_on; }; }
has_scale() { vm_ssh "$1" rpm -q gpfs.base &>/dev/null; }

# Copy the Storage Scale zip to the VM, outside /tmp/fs-stress so it survives push and
# reboots. The role script installs from it and deletes it.
upload_scale() { # upload_scale <vm> <zip>
  local vm=$1 zip=$2
  has_scale "$vm" && return
  vm_ssh "$vm" test -f "$SCALE_ZIP" && return
  echo "$vm: uploading $(basename "$zip")"
  vm_ssh "$vm" "sudo rm -rf $SCALE_DEST && sudo install -d -o $VM_USER $SCALE_DEST"
  vm_scp "$vm" "$zip" "$SCALE_ZIP.part"
  vm_ssh "$vm" mv "$SCALE_ZIP.part" "$SCALE_ZIP"
}

run_phase() { # run_phase <vm> <phase> [arg...]; reboots and reruns while the script asks for it
  local vm=$1 phase=$2 rc
  shift 2
  while true; do
    push "$vm"
    rc=0
    vm_ssh "$vm" sudo bash "/tmp/fs-stress/$(role_of "$vm").sh" "$phase" "$@" || rc=$?
    [[ $rc -eq 100 ]] || return $rc
    echo "[$vm] rebooting"
    vm_ssh "$vm" sudo systemctl reboot || true
    sleep 15
    # virt-install's first ("install") boot powers off on reboot instead of restarting.
    if [[ $(vm_state "$vm") == "shut off" ]]; then virsh start "$(dom "$vm")"; fi
    wait_ssh "$vm" 600
  done
}

parallel_phase() { # parallel_phase <phase> <vm...>
  local phase=$1 vm failed=()
  shift
  local -A pids
  for vm in "$@"; do
    echo "$vm: $phase (log: $LOG_DIR/$vm.log)"
    run_phase "$vm" "$phase" >>"$LOG_DIR/$vm.log" 2>&1 &
    pids[$vm]=$!
  done
  for vm in "$@"; do
    wait "${pids[$vm]}" || failed+=("$vm")
  done
  [[ ${#failed[@]} -eq 0 ]] || die "$phase failed on: ${failed[*]} (see $LOG_DIR)"
}

fetch_artifacts() {
  vm_ssh ceph-srv sudo cat /etc/ceph/ceph.minimal.conf >"$ART_DIR/ceph.conf"
  vm_ssh ceph-srv sudo cat /etc/ceph/ceph.client.stress.keyring >"$ART_DIR/ceph.client.stress.keyring"
  vm_ssh beegfs-srv sudo cat /etc/beegfs/conn.auth >"$ART_DIR/conn.auth"
  rm -f "$ART_DIR/gpfs-admin.pub"
  if gpfs_on; then
    vm_ssh gpfs-srv sudo cat /root/.ssh/id_ed25519.pub >"$ART_DIR/gpfs-admin.pub"
  fi
  rm -f "$ART_DIR/munge.key"
  if slurm_on; then
    vm_ssh slurm-srv sudo test -f /etc/munge/munge.key ||
      die "Slurm is on but slurm-srv isn't provisioned: run lab.sh provision slurm-srv"
    (umask 077 && vm_ssh slurm-srv sudo cat /etc/munge/munge.key >"$ART_DIR/munge.key")
  fi
}

cmd_provision() {
  local zip="" args=() vm
  while [[ $# -gt 0 ]]; do
    case $1 in
      --scale-installer)
        [[ -f ${2:-} ]] || die "--scale-installer: no such file: ${2:-}"
        zip=$(realpath "$2")
        shift
        ;;
      -*) die "provision: unknown option: $1" ;;
      *) args+=("$1") ;;
    esac
    shift
  done
  select_vms "${args[@]}"
  [[ ${#vms[@]} -gt 0 ]] || die "lab $lab has no VMs; create them with: lab.sh --lab $lab up"
  split_roles "${vms[@]}"
  if [[ ${#clients[@]} -gt 0 ]]; then
    for vm in ceph-srv lustre-srv beegfs-srv nfs-srv; do
      vm_exists "$vm" || die "clients need $vm; create it with: lab.sh --lab $lab up $vm"
    done
  fi
  mkdir -p "$LOG_DIR" "$ART_DIR"
  load_subnet

  for vm in "${vms[@]}"; do
    needs_gpfs "$vm" || continue
    if [[ -n $zip ]]; then
      upload_scale "$vm" "$zip"
    elif ! has_scale "$vm" && ! vm_ssh "$vm" test -f "$SCALE_ZIP"; then
      die "$vm needs IBM Storage Scale: pass the Developer Edition zip with --scale-installer (see README.md)"
    fi
  done

  parallel_phase install "${vms[@]}"
  [[ ${#servers[@]} -eq 0 ]] || parallel_phase configure "${servers[@]}"
  if [[ ${#clients[@]} -gt 0 ]]; then
    fetch_artifacts
    if slurm_on; then
      # A node has to be in slurm.conf before its slurmd starts.
      echo "slurm-srv: nodes (log: $LOG_DIR/slurm-srv.log)"
      run_phase slurm-srv nodes >>"$LOG_DIR/slurm-srv.log" 2>&1 &
      wait $! || die "updating slurm-srv's nodes failed (see $LOG_DIR/slurm-srv.log)"
    fi
    parallel_phase configure "${clients[@]}"
    if gpfs_on; then
      # GPFS clients are added to the cluster from gpfs-srv, the admin node.
      echo "gpfs-srv: join ${clients[*]} (log: $LOG_DIR/gpfs-srv.log)"
      # In the background, like parallel_phase, so that errexit applies inside run_phase.
      run_phase gpfs-srv join "${clients[@]}" >>"$LOG_DIR/gpfs-srv.log" 2>&1 &
      wait $! || die "gpfs join failed (see $LOG_DIR/gpfs-srv.log)"
    fi
  fi
  echo "all done"
}

# --- start, stop, down ------------------------------------------------------------------

cmd_start() {
  local vm n missing m
  select_vms "$@"
  load_subnet
  split_roles "${vms[@]}"
  # The network doesn't start with the host.
  [[ $(virsh net-info "$NET") =~ Active:\ +yes ]] || virsh net-start "$NET" >/dev/null
  # Servers get a head start; clients retry their mounts on their own anyway.
  for vm in "${servers[@]}" "${clients[@]}"; do
    if [[ $(vm_state "$vm") != running ]]; then
      virsh start "$(dom "$vm")" >/dev/null
      echo "$vm: started"
    fi
    is_client "$vm" || wait_ssh "$vm" 180
  done
  for vm in "${clients[@]}"; do
    wait_ssh "$vm" 180
    missing=$(client_mounts "$vm")
    for _ in $(seq 100); do
      if m=$(missing_mounts "$vm"); then
        missing=$m
        [[ -z $missing ]] && break
      fi
      sleep 3
    done
    n=$(client_mounts "$vm" | wc -w)
    echo "$vm: $((n - $(wc -w <<<"$missing")))/$n mounts${missing:+, missing: ${missing//$'\n'/ }}"
  done
}

shutdown_and_wait() {
  local vm
  for vm in "$@"; do
    if [[ $(vm_state "$vm") == running ]]; then virsh shutdown "$(dom "$vm")" >/dev/null; fi
  done
  for vm in "$@"; do
    for _ in $(seq 60); do
      [[ $(vm_state "$vm") == "shut off" ]] && break
      sleep 3
    done
    if [[ $(vm_state "$vm") != "shut off" ]]; then
      echo "$vm: didn't shut down in 3 minutes, forcing it off" >&2
      virsh destroy "$(dom "$vm")" >/dev/null
    fi
    echo "$vm: off"
  done
}

cmd_stop() {
  select_vms "$@"
  split_roles "${vms[@]}"
  # Clients first, so their unmounts don't hang on servers that are already gone.
  [[ ${#clients[@]} -eq 0 ]] || shutdown_and_wait "${clients[@]}"
  [[ ${#servers[@]} -eq 0 ]] || shutdown_and_wait "${servers[@]}"
}

# Other labs' networks carry the same DNS domain.
other_labs_exist() {
  local net
  for net in $(virsh net-list --all --name); do
    [[ $net == "$NET" ]] && continue
    [[ $(virsh net-dumpxml "$net") == *"<domain name='$DNS_DOMAIN'"* ]] && return 0
  done
  return 1
}

delete_pool() {
  local pool=$1 vol
  virsh pool-info "$pool" &>/dev/null || return 0
  for vol in $(virsh vol-list "$pool" | awk 'NR > 2 && NF { print $1 }'); do
    virsh vol-delete "$vol" --pool "$pool" >/dev/null
  done
  virsh pool-destroy "$pool" &>/dev/null || true
  virsh pool-delete "$pool" >/dev/null
  virsh pool-undefine "$pool" >/dev/null
}

cmd_down() {
  local vm everything=false d
  [[ $# -gt 0 ]] || everything=true
  select_vms "$@"
  for vm in "${vms[@]}"; do
    d=$(dom "$vm")
    # Unmounting unregisters the BeeGFS clients; otherwise they hold licence slots until
    # mgmtd drops them after 30 minutes.
    if is_client "$vm" && has_beegfs "$vm" && [[ $(vm_state "$vm") == running ]]; then
      load_subnet
      vm_ssh "$vm" sudo timeout 30 systemctl stop beegfs-client &>/dev/null || true
    fi
    virsh destroy "$d" &>/dev/null || true
    virsh undefine "$d" --nvram --remove-all-storage >/dev/null 2>&1 ||
      virsh undefine "$d" --remove-all-storage >/dev/null
    echo "$vm: destroyed"
  done
  $everything || return 0
  delete_pool "$POOL"
  other_labs_exist || delete_pool "$BOOT_POOL"
  if virsh net-info "$NET" &>/dev/null; then
    virsh net-destroy "$NET" &>/dev/null || true
    virsh net-undefine "$NET" >/dev/null
  fi
  echo "lab $lab destroyed; the downloaded image and logs are in $CACHE_DIR"
}

# --- ssh, status, fs-stress -------------------------------------------------------------

cmd_ssh() {
  [[ $# -ge 1 ]] || die "usage: lab.sh ssh VM [CMD...]"
  check_vm_name "$1"
  load_subnet
  vm_ssh "$@"
}

cmd_status() {
  [[ $# -eq 0 ]] || die "usage: lab.sh status"
  local active=no vm state ssh_ok mounts missing n
  if ! virsh net-info "$NET" &>/dev/null; then
    echo "lab $lab: no network"
    return
  fi
  load_subnet
  [[ $(virsh net-info "$NET") =~ Active:\ +yes ]] && active=yes
  echo "lab $lab: subnet $subnet.0/24, network active: $active," \
    "GPFS: $(gpfs_on && echo on || echo off), Slurm: $(slurm_on && echo on || echo off)"
  mapfile -t vms < <(existing_vms)
  printf '%-12s %-10s %-15s %-4s %s\n' VM STATE IP SSH MOUNTS
  for vm in "${vms[@]}"; do
    state=$(vm_state "$vm") ssh_ok=- mounts=""
    if [[ $state == running ]]; then
      ssh_ok=no
      vm_ssh "$vm" true 2>/dev/null && ssh_ok=ok
    fi
    if is_client "$vm" && [[ $ssh_ok == ok ]]; then
      n=$(client_mounts "$vm" | wc -w)
      if missing=$(missing_mounts "$vm"); then
        mounts="$((n - $(wc -w <<<"$missing")))/$n${missing:+, missing: ${missing//$'\n'/ }}"
      else
        mounts="?/$n (findmnt failed)"
      fi
    fi
    printf '%-12s %-10s %-15s %-4s %s\n' "$vm" "$state" "$(vm_ip "$vm")" "$ssh_ok" "$mounts"
  done
}

cmd_fs_stress() {
  local results=$CACHE_DIR/results picked="" hosts=() vm
  if [[ ${1:-} == -h || ${1:-} == --help ]]; then exec "$here/../scripts/remote.sh" --help; fi
  while [[ $# -gt 0 ]]; do
    case $1 in
      --results) results=${2:?--results needs a value}; shift ;;
      --clients) picked=${2:?--clients needs a value}; shift ;;
      *) break ;;
    esac
    shift
  done
  load_subnet
  if [[ -n $picked ]]; then
    for vm in ${picked//,/ }; do
      is_client "$vm" || die "--clients takes client VMs: $vm"
      vm_exists "$vm" || die "$vm doesn't exist in lab $lab"
      hosts+=("$VM_USER@$(vm_ip "$vm")")
    done
  else
    for vm in $(existing_clients); do hosts+=("$VM_USER@$(vm_ip "$vm")"); done
  fi
  [[ ${#hosts[@]} -gt 0 ]] || die "lab $lab has no clients"
  mkdir -p "$results"
  exec "$here/../scripts/remote.sh" \
    --hosts "${hosts[*]}" \
    --results "$results" \
    --ssh-opts "-o BatchMode=yes ${SSH_OPTS[*]}" \
    "$@"
}

set_lab "$lab"
while [[ $# -gt 0 ]]; do
  case $1 in
    --lab) set_lab "${2:?--lab needs a value}"; shift 2 ;;
    -h | --help) usage; exit 0 ;;
    -*) die "unknown option: $1 (see lab.sh --help)" ;;
    *) break ;;
  esac
done
[[ $# -ge 1 ]] || { usage >&2; exit 1; }
command=$1
shift
case $command in
  up | provision | start | stop | down | ssh | status)
    if [[ ${1:-} == -h || ${1:-} == --help ]]; then usage; exit 0; fi
    "cmd_$command" "$@"
    ;;
  fs-stress) cmd_fs_stress "$@" ;;
  *) die "unknown command: $command (see lab.sh --help)" ;;
esac
