# Does a file lock reach another host?

`ConditionalLocalFileSystem` does a compare-and-swap under an exclusive
`flock`. That is only safe if the lock reaches every writer. This check answers
whether it does, on one NFS export, with two clients on separate hosts.

Run it:

    ./run.sh

Client `a` holds the lock. Client `b` then tries the same lock without blocking.
A refusal is the only proof that the lock reached the server. Client `b` then
tries the blocking form, which is what Rust's `File::lock` calls, so its result
is what icechunk would see.

A control run does the same on a shared local volume, where the lock must be
refused. If the control does not refuse, the probe proves nothing, so read that
line first.

Each run also prints the filesystem name and the `statfs` magic number, and the
mount options the kernel really applied. Those are the values
`network_filesystem` reads, so the check validates the detection table as well.

## Prerequisites

The clients mount NFS with the host kernel, so the host needs the `nfs` module.
`run.sh` checks and prints the fix. Inside a VM, load it in the VM:

    podman machine ssh 'sudo modprobe nfs'

The server is NFS-Ganesha, which serves NFSv4 from user space, so no kernel
`nfsd` is needed. It does need two capabilities, `SYS_RESOURCE` for
`prctl(PR_SET_IO_FLUSHER)` and `DAC_READ_SEARCH` for `open_by_handle_at`, both
in the initial user namespace. Rootless podman cannot give it those, so point
the script at a rootful engine:

    DOCKER="podman --connection podman-machine-default-root" ./run.sh

If that connection has no host socket, forward it. Keep the socket path short,
because macOS caps a socket path at 104 bytes:

    ssh -i ~/.local/share/containers/podman/machine/machine -p <port> \
        -f -N -L /tmp/podman-root.sock:/run/podman/podman.sock root@127.0.0.1
    DOCKER_HOST=unix:///tmp/podman-root.sock ./run.sh

Rootful Docker on Linux needs none of this.

## What it found here

Against Ganesha 6.5, NFSv4.1, Linux client 7.1.3:

- The second host was refused the lock. So `flock` did reach the server, and two
  hosts cannot hold one lock at the same time.
- A *blocking* lock failed at once with `EIO` instead of waiting for the holder.
  That is the form `File::lock` uses, so icechunk would report an error rather
  than lose a commit. Whether a hardware filer behaves the same way is untested.
- `local_lock=all` had no effect: the kernel mounted with `local_lock=none`.
  That option applies to NFSv3 through NLM, not to NFSv4, whose locking is part
  of the protocol.

Both results argue for what the code does, which is to turn conditional updates
off on a network filesystem. Locks there either work, or fail every contended
commit.

## Extending it

To test SMB, replace the server with Samba and mount `-t cifs`. To test NFSv3
and its `nolock` option, set `NFS_Protocols = 3,4` and `Enable_NLM = true` in
`server/ganesha.conf`. To drive icechunk itself, replace the probe with a
program that opens a repository on `/mnt/target` and commits. Then count how
many committers succeed.
