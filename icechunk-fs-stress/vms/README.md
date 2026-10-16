# fs-stress VM lab

A set of throwaway Rocky Linux 9.7 VMs on libvirt that serves Lustre, BeeGFS,
CephFS, NFS and, optionally, IBM Storage Scale (GPFS) to any number of client
VMs, for running [fs-stress](../README.md) across hosts on real distributed
filesystems.

`lab.sh` is the entry point; `./lab.sh --help` lists its commands and
options. Commands below run from this directory. For a managed Lustre on AWS
instead, see [FSx for Lustre on AWS](#fsx-for-lustre-on-aws).

```sh
./lab.sh up              # create the VMs
./lab.sh provision       # install and configure them
./lab.sh status          # VMs, ssh, client mounts
./lab.sh fs-stress ...   # run scripts/remote.sh on the clients
./lab.sh stop            # shut everything down, keeping all state
./lab.sh start           # bring it back
./lab.sh down            # destroy the lab
```

On a 32-core host with a fast disk, `up` takes 2 to 3 minutes the first time,
mostly downloading the base image. `provision` takes about 8 minutes for the
default lab, and about 10 with GPFS and 5 clients; adding GPFS or a few
clients to a provisioned lab takes 5 to 7. `stop` takes 15 seconds, `start`
1 to 2 minutes.

## Requirements

- **An x86_64 Linux host with KVM** (`/dev/kvm`). Everything in the lab is
  pinned to x86_64: the Rocky image, Whamcloud's prebuilt Lustre packages,
  the Storage Scale installer and the static fs-stress binary.
- **libvirt**, with your user allowed to manage `qemu:///system` without
  sudo: usually membership in the `libvirt` group (`libvirtd` on some
  distributions). The lab never uses sudo on the host.
- **Tools on `PATH`:** `virsh`, `virt-install` (package `virtinst` on
  Debian and Ubuntu), `xorriso` (virt-install builds the cloud-init ISO with
  it), `curl`, `ssh`, `scp`.
- **UEFI firmware for VMs:** OVMF (package `ovmf` on Debian and Ubuntu,
  `edk2-ovmf` on Fedora). The VMs boot UEFI with Secure Boot off.
- **An ssh key.** `up` puts every key from `ssh-add -L` and `~/.ssh/*.pub`
  into the VMs' `rocky` user. Keys are baked in when a VM is created.
- **Memory:** about 15G for the servers (19G with GPFS) plus 4G per client
  by default, so 23G for the default lab.
- **Disk:** sparse qcow2 files in `/var/lib/libvirt/<lab>`; each OS disk is a
  copy-on-write overlay of one base image.

**macOS is not supported yet.**

## Layout

| VM | IP | vCPU / RAM | Disks | Serves |
| --- | --- | --- | --- | --- |
| ceph-srv | .10 | 4 / 6G | os 25G, data1 30G (OSD) | Ceph 20.2.4 via cephadm (podman): mon, mgr, mds, one OSD; CephFS `cephfs` |
| lustre-srv | .11 | 2 / 4G | os 20G, data1 8G (MGS+MDT), data2 25G (OST) | Lustre 2.17.0, fsname `lfs`, on Whamcloud's patched kernel |
| beegfs-srv | .12 | 2 / 3G | os 20G, data1 8G (meta, ext4), data2 25G (storage, xfs) | BeeGFS 8.4.1 mgmtd, meta and storage |
| nfs-srv | .13 | 2 / 2G | os 20G, data1 25G (xfs) | Linux knfsd, NFSv3, 4.0, 4.1 and 4.2, one `sync` export per version |
| gpfs-srv (optional) | .14 | 2 / 4G | os 25G, data1 25G (NSD) | Storage Scale 6.0.1.2 Developer Edition, cluster `fs-stress-gpfs`, filesystem `fs1` |
| client*K* | .(20+*K*) | 4 / 4G by default | os 20G | clients of all of the above |

IPs are in the lab's /24, `10.77.0.0/24` by default. The libvirt domains are
named `<lab>-<vm>`, for example `ic-fs-stress-client1`. Inside the lab, the
VMs resolve each other by name (`ceph-srv`, `client2.fs-stress`); the host
reaches them by IP.

### Client mounts

| Mount | Filesystem | Notes |
| --- | --- | --- |
| `/mnt/lustre` | Lustre | |
| `/mnt/beegfs` | BeeGFS | clients 1 to 5 only |
| `/mnt/cephfs` | CephFS, kernel client | `stress@.cephfs=/` |
| `/mnt/cephfs-fuse` | CephFS via ceph-fuse | userspace client (libcephfs), with its own caching and capability handling |
| `/mnt/nfs3` | NFSv3 | `vers=3` |
| `/mnt/nfs40` | NFSv4.0 | `vers=4.0` |
| `/mnt/nfs41` | NFSv4.1 | `vers=4.1` |
| `/mnt/nfs42` | NFSv4.2 | `vers=4.2` |
| `/gpfs/fs1` | GPFS | with GPFS on |

Every filesystem root is mode `1777`, so the `rocky` user writes without sudo.

Only clients 1 to 5 mount BeeGFS because of its license: without one, mgmtd
registers at most 5 clients, and every mount is a client.

## GPFS (optional)

GPFS is on when the lab has a gpfs-srv VM:

```sh
./lab.sh up --gpfs
./lab.sh provision --scale-installer /path/to/Storage_Scale_Developer-<version>-x86_64-Linux.zip
```

IBM doesn't allow scripted downloads, so you download the installer
yourself: sign up for an IBM ID through the "Try the free trial" button on
<https://www.ibm.com/products/storage-scale>, and download the Developer
Edition zip (about 1.9G). The Developer Edition is free, has all features and
no time limit, is limited to 12 TB per cluster, and is not for production
use. **`provision` installs it with `--silent`, which accepts IBM's license.**

`--scale-installer` is needed whenever a GPFS node doesn't have Storage
Scale installed yet: a new gpfs-srv, or new clients while GPFS is on. It
uploads the zip to those VMs, which install from it and delete it.

How GPFS differs from the other filesystems:

- gpfs-srv is the only quorum node, the cluster manager, the NSD server and
  the admin node. Clients join as non-quorum nodes: `provision` adds them
  from gpfs-srv (`mmaddnode`, `mmchlicense client`, `mmstartup`).
- Every client runs the full daemon (`mmfsd`) with a kernel module built on
  the VM (`mmbuildgpl`), and gpfs-srv runs admin commands on it as root over
  ssh.
- `pagepool` is 256M on the clients and 512M on the server, instead of 1G.
- At boot, GPFS starts on its own and mounts `fs1` once the server is
  reachable.

## More clients

```sh
./lab.sh up --clients 6 --client-memory 2048 --client-vcpus 2
./lab.sh provision client3 client4 client5 client6 --scale-installer ...   # zip only with GPFS on
```

`up --clients N` creates whichever of client1 to client*N* don't exist, up to
79 clients. `--client-memory` (MiB) and `--client-vcpus` apply to the clients
that `up` creates. Provisioning new clients needs the servers to be running.
Clients 6 and up have no BeeGFS mount.

## Several labs

`--lab NAME` (default `ic-fs-stress`) selects a lab, so several can exist
side by side. Give each its own subnet when creating it:

```sh
./lab.sh --lab second up --subnet 10.78.0
./lab.sh --lab second status
```

The subnet is fixed when the lab's network is created; later commands read it
from libvirt. Each lab has its own network, storage pool
(`/var/lib/libvirt/NAME`) and cache directory (`~/.cache/NAME`, with the base
image, provisioning logs, server credentials and fs-stress results). All labs
share the `boot-scratch` pool (`/var/lib/libvirt/boot`), where virt-install
puts the cloud-init ISOs; `down` removes it with the last lab.

## Running fs-stress on the lab

`lab.sh fs-stress` runs [`scripts/remote.sh`](../scripts/remote.sh) with the
lab's clients as hosts and the lab's ssh options. Results go to
`~/.cache/<lab>/results` unless you pass `--results DIR`. Build the static
binary as described in [Building](../README.md#building), then:

```sh
./lab.sh fs-stress deploy ../../target/x86_64-unknown-linux-musl/fs-stress-dist/fs-stress
./lab.sh fs-stress preflight /mnt/lustre
./lab.sh fs-stress run /mnt/nfs41 storage --duration 2m
./lab.sh fs-stress matrix /mnt/lustre /mnt/cephfs /mnt/nfs41 -- --duration 2m
./lab.sh fs-stress --clients client1,client2,client3,client4,client5 run /mnt/beegfs repository
./lab.sh fs-stress run --net-reset-every 15 /mnt/nfs40 storage
```

`--clients` picks a subset of clients; by default all of them run workers.
Runs on BeeGFS need a subset when the lab has more than 5 clients. See
[Driving several hosts from one machine](../README.md#driving-several-hosts-from-one-machine)
for `remote.sh`'s commands and network faults. The clients have `chronyc`,
`nft`, `tc` and `ss`, and passwordless sudo, so every preflight check and
network fault works.

## Starting and stopping

VMs don't start when the host boots. They keep all state across shutdowns;
only `down` destroys anything.

- `stop [VM...]` shuts VMs down cleanly, clients first, so that their
  unmounts don't hang on a server that's already gone. A VM that doesn't stop
  within 3 minutes is forced off.
- `start [VM...]` starts the lab's network if needed (it doesn't start with
  the host), then the servers, and waits for their ssh. Then it starts the
  clients and waits until each has all its mounts, printing for example
  `client1: 8/8 mounts`.
- Plain `virsh -c qemu:///system start|shutdown` works too. Keep the order:
  servers before clients when starting, clients before servers when
  stopping.
- `sudo reboot` inside a VM is fine. The exception is a VM's very first boot
  after `up`, which powers off on reboot; `provision` handles that.

## Rebuilding

```sh
./lab.sh down client2 && ./lab.sh up client2 && ./lab.sh provision client2
```

Rebuilding a server invalidates what the clients hold.

## Access

- `./lab.sh ssh VM [CMD...]` logs in as `rocky`, who has passwordless sudo.
  Host keys aren't checked, since VMs get rebuilt. Plain
  `ssh rocky@10.77.0.21` works too.
- SELinux is permissive and firewalld is off in every VM; Lustre and BeeGFS
  want it so, and the VMs live on a private network. `nftables.service` is
  not enabled on the clients: no ruleset is loaded at boot.
- The lab's network is NAT: the VMs reach the internet (dnf needs it) and the
  host at `.1`, and nothing outside the host reaches the VMs.

| Credential | Where | Notes |
| --- | --- | --- |
| Ceph client key `client.stress` | clients: `/etc/ceph/ceph.client.stress.keyring`, `/etc/ceph/ceph.conf` | the caps of `ceph fs authorize cephfs client.stress / rw` |
| Ceph admin | ceph-srv: `/etc/ceph/ceph.client.admin.keyring` | `sudo /usr/local/sbin/cephadm shell -- ceph ...` |
| BeeGFS shared secret | `/etc/beegfs/conn.auth` on all BeeGFS nodes | generated on beegfs-srv |
| GPFS admin key | gpfs-srv: `/root/.ssh/id_ed25519`, trusted by root on every GPFS node | `mm*` commands that touch other nodes only work on gpfs-srv |
| Copies on the host | `~/.cache/<lab>/artifacts/` | `provision` passes the server credentials to the clients through the host |

The Ceph client key is a legacy AES key. Ceph 20 creates keys of a newer
type by default, which the EL9 kernel client can't use (`mount.ceph` fails
with error 524). To allow it, the mon map has
`auth_allowed_ciphers aes256k,aes`, and `mon_auth_allow_insecure_key` is on
only while the key is created. That differs from a default Ceph 20 setup.

BeeGFS's gRPC TLS, which only covers mgmtd and the `beegfs` CLI, is off.
`/etc/profile.d/beegfs.sh` exports `BEEGFS_TLS_DISABLE=true` for the CLI;
under sudo, pass it explicitly.

## Versions

Pinned at the top of `lab.sh`:

- **Rocky 9.7, not 9.8.** No Lustre release supports 9.8 kernels yet. Kernel
  `5.14.0-611.55.1.el9_7`, the last 9.7 kernel, satisfies the Lustre 2.17.0
  client kmods and is in BeeGFS 8.4's tested set. The dnf repos point at the
  9.7 vault, so `dnf update` stays on 9.7. EPEL (clients, for ceph-common's
  dependencies) tracks the latest EL9.
- **Lustre server on Whamcloud's patched kernel**
  (`5.14.0-611.13.1_lustre.el9`): the prebuilt `kmod-lustre-osd-ldiskfs`
  needs jbd2 symbols that only the patched kernel exports. It's unsigned,
  hence Secure Boot off, and stock kernels are excluded from lustre-srv's
  repos so dnf can't replace it.
- **Lustre client:** prebuilt `kmod-lustre-client`, LNet on `tcp0`.
- **BeeGFS client:** the kernel module is built on the VM by `beegfs-client`
  at its first start or after a kernel change.
- **Ceph:** cephadm with `quay.io/ceph/ceph:v20.2.4`; clients use the
  `download.ceph.com/rpm-20.2.4/el9` repository.
- **NFS:** knfsd from the 9.7 kernel, versions 3, 4.0, 4.1 and 4.2 enabled
  in `/etc/nfs.conf`. Exports are `rw,sync,no_subtree_check` for the lab's
  /24, root squashed.
- **Storage Scale 6.0.1.2 Developer Edition**, or whichever version you
  download: nothing depends on the version or the zip's name. IBM supports kernel
  `5.14.0-611.55.1.el9_7` from 5.2.3.7 on. Rocky isn't an IBM-supported
  distribution, but the build and runtime work. Installed packages:
  `gpfs.base`, `gpl`, `gskit`, `msg.en_US`, `license.dev`, `adv` and
  `compression`.

## Checking health

```sh
./lab.sh status
./lab.sh ssh ceph-srv 'sudo /usr/local/sbin/cephadm shell -- ceph -s'       # HEALTH_OK, with mutes
./lab.sh ssh lustre-srv 'sudo lctl dl'                                     # all targets UP
./lab.sh ssh client1 'lfs df -h /mnt/lustre'
./lab.sh ssh nfs-srv 'sudo exportfs -v; sudo cat /proc/fs/nfsd/versions'   # +3 +4 +4.1 +4.2
./lab.sh ssh client1 'nfsstat -m'
./lab.sh ssh beegfs-srv 'sudo BEEGFS_TLS_DISABLE=true beegfs --mgmtd-addr 10.77.0.12:8010 health check'
./lab.sh ssh gpfs-srv 'sudo /usr/lpp/mmfs/bin/mmgetstate -a; sudo /usr/lpp/mmfs/bin/mmlsmount fs1 -L'
```

Ceph mutes `POOL_NO_REDUNDANCY`, `TOO_FEW_OSDS`,
`AUTH_INSECURE_CLIENT_KEY_TYPE` and `AUTH_INSECURE_KEYS_ALLOWED`, all
expected with one OSD and a legacy client key. Pools have size 1: no
replication, fine for semantics tests, not for durability.

`beegfs health check` always ends with "one or more checks failed": "No
valid license", and an "Available Capacity" warning for the small disks. What
matters is that the targets are Online, Good and Healthy and that the
connection checks pass. BeeGFS counts storage below 10GiB free and meta below
3GiB or 1M free inodes as "emergency", which is why the disks are 25G and 8G
and meta is formatted with BeeGFS's recommended
`mkfs.ext4 -i 2048 -I 512 -J size=400 -O dir_index,filetype`.

## Troubleshooting

`provision` appends each VM's output to `~/.cache/<lab>/logs/<vm>.log`.

- **A VM has no console output or DHCP lease:** BIOS boot hangs before the
  kernel; VMs must be UEFI, which needs OVMF on the host.
- **virt-install: "cannot open directory /var/lib/libvirt/boot":** the
  `boot-scratch` pool is missing; `up` creates it.
- **The VMs can't reach the internet, so dnf fails:** something on the host
  drops forwarded traffic from libvirt's bridge, for example Docker setting
  the `FORWARD` chain's policy to `DROP`.
- **A VM is off after a reboot:** its first boot after `up`;
  `./lab.sh start VM`.
- **`mount.ceph`: "adding ceph secret key to kernel failed: Unknown error
  524":** the client key is of the new type; see [Access](#access).
- **`mount.lustre ... Invalid argument`, dmesg "Unknown option 'nofail'":**
  the Lustre mount helper passes `nofail` to the kernel; don't use it in
  fstab.
- **`sudo: cephadm: command not found`:** sudo's `PATH` lacks
  `/usr/local/sbin`; use the full path.
- **CephFS or BeeGFS not mounted after boot:** the client won the race
  against its server. `fs-stress-mount-retry.service` fixes it within about
  20 s; see `systemctl status fs-stress-mount-retry` and
  `journalctl -b -u fs-stress-mount-retry`.
- **Ceph back to `HEALTH_WARN POOL_NO_REDUNDANCY`:** a non-sticky mute clears
  when the alert worsens; the role script mutes with `--sticky`.
- **A client stops answering ssh under load and its qemu process burns
  several CPUs:** the Lustre 2.17 client livelocks; its previous boot's kernel
  log (`journalctl -k -b -1`) floods with `Force grant RPC slot`. The guest
  ignores an ACPI reboot: `virsh -c qemu:///system reset ic-fs-stress-clientN`.
  If `/mnt/beegfs` then doesn't mount, delete the client's stale registration
  on `beegfs-srv` (`beegfs node list`, then `beegfs node delete c:<id> --yes`,
  with the options of the health check above) and run
  `sudo systemctl restart beegfs-client` on the client.
- **`/mnt/cephfs-fuse` is too slow for the stress matrix:** each lookup in a
  directory of a few thousand entries, such as a run's `overwritten/`, takes
  about 5 s, so verifying one storage run takes hours. Use `/mnt/cephfs`.

## FSx for Lustre on AWS

`aws-lustre.sh` builds a temporary Amazon FSx for Lustre file system and EC2
clients that mount it at `/mnt/fsx`, for testing against a managed Lustre
on a real network instead of the lab's. Everything is one CloudFormation
stack, from [`aws-lustre.yaml`](aws-lustre.yaml), in a VPC of its own. The
script uses the AWS CLI with your usual credentials and region
(`AWS_PROFILE`, `AWS_REGION`).

```sh
./aws-lustre.sh up                  # shows account, region and sizes, asks before creating
./aws-lustre.sh fs-stress deploy ../../target/x86_64-unknown-linux-musl/fs-stress-dist/fs-stress
./aws-lustre.sh fs-stress preflight /mnt/fsx
./aws-lustre.sh fs-stress run /mnt/fsx repository --duration 10m --gc
./aws-lustre.sh status
./aws-lustre.sh down                # deletes the stack; results stay in ~/.cache/<name>/results
```

- **File system:** Lustre 2.15 (the newest FSx offers), `PERSISTENT_2`, SSD,
  1.2 TiB at 125 MB/s/TiB by default (`--storage-gib`, `--throughput`), no
  backups.
- **Clients:** an Auto Scaling group of 5 × `c7i.large` by default
  (`--clients`, `--instance-type`), Amazon Linux 2023 with its
  `lustre-client` package, `ec2-user` with passwordless sudo and `chronyc`
  against Amazon Time Sync. Clients are numbered by private IP.
  `./aws-lustre.sh ssh N` logs in to client N with the stack's key
  (`~/.cache/<name>/id_ed25519`).
- **Creation:** 20-40 minutes, mostly the file system. Each client signals
  CloudFormation once it has mounted `/mnt/fsx`, so `up` returns only when
  all of them have. If anything fails, CloudFormation deletes the whole stack
  and `up` prints the failed events.
- **Progress:** `up` and `down` print the stack's events as they happen, and
  every 2 quiet minutes what is still in progress. `./aws-lustre.sh events`
  shows them from another terminal; `events -f` follows until the stack
  settles.
- **Where the stack is:** `up` records the account, region and stack ID in
  `~/.cache/<name>/stack` before creating anything. Every other command uses
  that region and refuses other accounts, so `down` can't miss the stack by
  looking in the wrong place. `down` deletes the file only once the stack is
  gone.
- **Network:** the clients have public IPs; ssh is open only to
  `--ssh-cidr` (default: this machine's public IP). Lustre traffic (988,
  1018-1023) is allowed only within the stack's security group.
- **Cost:** about $0.75/hour with the defaults in us-east-1, billed until
  `down`.
- **Network faults** (`--net-reset-every`, `--netem`) don't apply: they target
  the control machine's /24, which isn't where the file servers are.

## Cleanup

`./lab.sh down` removes the lab's VMs and disks, its network and storage
pool, and the shared `boot-scratch` pool if no other lab is left. It keeps
`~/.cache/<lab>` (base image of about 620M, logs, credentials, results) and
the Storage Scale zip, wherever you keep it. libvirt may also have created a
`default` storage pool (`/var/lib/libvirt/images`) the first time virt-install
ran; the lab doesn't use it.
