# fs-stress

A stress test for Icechunk's filesystem storage (`filesystem_storage` in
Python, `FilesystemStorage` in Rust), the experimental backend designed to make
concurrent commits safe on local and network filesystems.

Many processes, on one or more hosts, update one repository at the same time,
while the tool kills and pauses them in the middle of the commit protocol.
Afterwards it checks that no acknowledged update was lost and that the
repository recovers and cleans up.

We use it to qualify filesystems. If you want to run `filesystem_storage` on
your own filesystem, run it there and [send us the report](#what-to-send-us).

## What it tests

- **No lost updates, across processes and hosts.** Every acknowledged update
  must be in the final history, after the version it was based on, and the
  history may contain nothing that no worker attempted.
- **Crash and pause recovery.** Workers are killed with `SIGKILL` and paused
  at points inside the protocol, and each host also kills one of its workers
  from outside every few seconds. No writer holds anything across protocol
  steps, so the remaining writers must keep making progress whatever the dead
  and paused ones left behind. After the run, a fresh writer must succeed
  within 10 s.
- **Modes**. Support storage and repository mode. The former uses the filesystem
  storage directly to do high concurrency validation; the later uses the full
  Icechunk API and it provides a higher level check, at the price of reduced
  concurrency.
- **Sweeping.** After the run, `verify` sets the modification time of the
  protocol's leftovers 25 hours into the past and sweeps them: with the
  storage's own sweep in storage mode, with garbage collection in repository
  mode. Only `repo`, and `repo.d/` holding the marker and the one version it
  names, may be left.

Even with a passing run, remember the backend is experimental.

## Requirements

- **Linux** on every host. The tool is Unix-only; on other Unix systems its
  results are informative only (see below).
- **One filesystem, mounted on every host.** The mount path may differ from
  host to host: every command takes the run root's path as this host sees it
  (`--root`). The filesystem needs hard links, atomic rename by name and
  exclusive directory rename; the backend doesn't check for them.
- **Synchronised clocks** (NTP) for runs on several hosts: the stall and
  start-skew checks compare timestamps from different hosts.
- **The Rust toolchain** ([rustup](https://rustup.rs)), on every host or on a
  build machine with the same architecture and C library.
- **Disk and memory.** See [Safety notes](#safety-notes).

Expected to qualify: local POSIX filesystems, NFSv3 and NFSv4 with a Linux
client, Lustre, GPFS, CephFS. The [storage guide](../icechunk-python/docs/docs/guides/storage.md#requirements-and-limits-of-filesystem_storage)
lists the backend's requirements and limits.

## Building

On every host:

```sh
git clone https://github.com/earth-mover/icechunk
cd icechunk
cargo build --release -p icechunk-fs-stress
```

The binary is `target/release/fs-stress`. Build every host from the same
commit: the tool doesn't check that all hosts run the same build.

Alternatively, build one fully static binary and copy it to every x86_64
Linux host, whatever its C library. In the repository's Nix dev shell:

```sh
just fs-stress-static
```

It prints the binary's path and its sha256.

## Single host

```sh
fs-stress run --root /path/on/fs/stress-1 --mode storage
```

`run` creates the run root, runs this host's workers for `--duration`
(default 1 minute), waits for them to exit and verifies the result. It takes
the same [knobs](#knobs) as `init`, without `--hosts`.

Progress goes to stderr: the estimated data volume, a status line every
`--progress-every` (default `5s`, `0` turns it off), the wind-down, and the
phases of the check. Stdout
gets only the final summary. The exit code is 0 for a pass, 1 for a failure,
3 for an error of the tool itself or its setup.

## Several hosts

1. **On one host**, create the run root:

   ```sh
   fs-stress init --root /path/on/fs/stress-1 --hosts 3 --mode storage
   ```

   It prints the estimated data volume.

2. **On every host**, start the workers. Each host passes the run root's path
   as it sees it. In separate terminals:

   ```sh
   fs-stress workers --root /path/on/fs/stress-1
   ```

   With ssh:

   ```sh
   for h in a b c; do ssh $h /path/fs-stress workers --root /path/on/fs/stress-1 & done; wait
   ```

   With Slurm, one task per node:

   ```sh
   srun -N 3 --ntasks-per-node=1 /path/fs-stress workers --root /path/on/fs/stress-1
   ```

   Each host registers and waits. Once `--hosts` hosts have registered, the all
   start working at once. Every host
   runs for `--duration` from the moment it sees the go, then waits for its
   workers and writes its `done/` entry. `workers` and `verify` print progress
   to stderr, like `run`.

3. **On one host**, once the others finished, check the run:

   ```sh
   fs-stress verify --root /path/on/fs/stress-1
   ```

Details:

- **Late hosts are refused.** The run starts with the hosts registered when
  the `--hosts`-th one arrives. A host that registers after that exits with
  code 2, and the run goes on without it.
- **Host names.** A host's tag defaults to `<hostname>-<pid>`; `--tag`
  overrides it. `workers --workers` overrides the run's workers per host for
  this host.
- **Stopping early.** `fs-stress stop --root R` tells every host to stop; they
  notice within a second. Hosts still waiting for the go exit with code 2.
  `verify` still checks a stopped run.
- **Waiting for hosts.** `verify` takes the participants from `go` and waits
  up to `--done-timeout` (default `10m`) for each one's `done/` entry. If
  hosts are still missing, it runs only the read-only checks, skips the
  recovery write, aging and sweep, and says so in the report. Run it again
  once they finished. A host whose `workers` process died never writes its
  `done/` entry; send us that partial report.
- **One full check per run.** The recovery write and the sweep change the
  repository, so `verify` refuses a run root it already verified fully.
- **Exit codes.** `workers`: 0, 2 for a late host, 1 for other errors.
  `verify`: 0 for a pass, 1 for violations, 3 for errors of the tool or its
  setup. A report whose only violations are `Harness` ones exits with 3. With
  hosts still missing, every violation is provisional: a host that is still
  writing can make the others look wrong. The summary then says
  "provisional: hosts still missing (…)" and `verify` exits with 3; verify
  again once they finished.

## Driving several hosts from one machine

`scripts/remote.sh` runs the procedure above over ssh from a control machine
that doesn't need the filesystem: it copies the binary, checks the hosts,
creates the run root, starts `workers` on every host, verifies, and copies
the results back. It can also inject network faults on one host during the
run. `scripts/remote.sh --help` summarises its options and commands. To try it on
Lustre, BeeGFS, CephFS, NFS and GPFS without a cluster, the
[VM lab](vms/README.md) creates servers and client hosts on one Linux machine
and runs `remote.sh` on them.

### What it needs

- Bash on the control machine, and non-interactive ssh (keys, no password
  prompt) to every host.
- The filesystem mounted at **the same path on every host**.
- One binary that runs on every host: build it with `just fs-stress-static`.
- `chronyc` on the hosts, for the clock check (without it you only get a
  warning).
- For network faults, on the fault host only: passwordless `sudo`, `nft`
  (nftables), `tc` (iproute-tc) and `ss`.

### Options

These come before the command:

| Option | Default | Meaning |
| --- | --- | --- |
| `--hosts "HOST…"` | required | ssh destinations, space-separated: `"user@host1 user@host2"` |
| `--results DIR` | required by `run`, `matrix`, `selftest` | local directory for the fetched results |
| `--ssh-opts "OPTS"` | `-o BatchMode=yes -o ConnectTimeout=10` | options for every `ssh` and `scp` |

The first host in `--hosts` runs `init`, `verify` and `selftest`.

```sh
just fs-stress-static
r() { scripts/remote.sh --hosts "user@node1 user@node2" --results ~/fs-stress-results "$@"; }
r deploy target/x86_64-unknown-linux-musl/fs-stress-dist/fs-stress
r preflight /mnt/nfs
r run /mnt/nfs storage --duration 2m
r matrix /mnt/nfs /mnt/lustre -- --duration 2m
```

### Commands

- **`deploy BINARY`** copies `BINARY` to `~/fs-stress` on every host and
  checks its sha256 there.
- **`preflight MOUNT`** checks, on every host: the binary is there and the
  same as on the first host; `MOUNT` is a mount point of the same filesystem
  type as on the first host; `MOUNT/fs-stress/` is writable; the clock is
  within 100 ms of NTP. It prints each host's filesystem type, clock offset
  and free space. Each check gives up after 30 s, so a mount whose server is
  down fails the preflight instead of hanging it. When `run` or `matrix`
  preflights with network faults on, it also checks the fault host's tools
  and removes leftover faults.
- **`run [NET…] MOUNT MODE [ARGS…]`** runs `preflight`, then `init` on the first host
  with `--hosts` set to the number of hosts and `ARGS`, then `workers` on
  every host in parallel, then `verify` on the first host. It fetches the
  results, then deletes the run root if both the workers and `verify`
  succeeded, and keeps it otherwise. The run root is
  `MOUNT/fs-stress/<mount name>-<mode>-<timestamp>`. Progress goes to stderr,
  each line prefixed with its host; the summary goes to stdout. The exit code
  is `verify`'s, or 1 if a host's `workers` failed. `NET` are the
  [network fault](#network-faults) options; everything after `MODE` goes to
  `init`.
- **`matrix [--modes "MODE…"] [NET…] MOUNT… [-- ARGS…]`** runs `run` for each
  mount and each mode (default: `storage repository`), one at a time, and
  goes on after a failure. At the end it prints a table of `PASS`, `FAIL` or
  `ERROR (exit N)` per run, and saves it as `matrix-<timestamp>.txt` in the
  results directory. It exits with 1 if any run didn't pass.
- **`selftest MOUNT [ARGS…]`** runs `selftest` on the first host with its root
  parent under `MOUNT/fs-stress/`, and saves its output.
- **`net-clean [--net-host N]`** stops a leftover fault loop on the fault host
  and removes its rules and its netem queue.

`^C` during a `run` tells the hosts to stop (`fs-stress stop`), and `verify`
then checks the stopped run. Nothing bounds a run itself: if a server goes
down mid-run, the workers block with it. Run one `run` or `matrix` at a time:
runs on the same hosts slow each other down and make the stall and starvation
checks meaningless.

### Results

Each run writes `<results>/<mount name>-<mode>-<timestamp>/`:

| File | Content |
| --- | --- |
| `meta.txt` | mount, mode, arguments, hosts, run root, binary sha256, network faults, exit codes |
| `summary.txt` | the summary `verify` printed |
| `report.json`, `run.json`, `control`, `go`, `hosts/`, `done/` | copied from the run root |
| `logs.tar.gz` | the run root's `logs/`, for every run |
| `init.log`, `workers-<i>.log`, `verify.log` | output of each step; `workers-<i>.log` is host `i` (0-based) |
| `net-faults.log`, `net-before.txt`, `net-after.txt` | with network faults: the loop's output and the fault host's TCP and NFS client counters before and after |

### Network faults

They test the protocol's handling of lost replies: a client whose connection
breaks resends its outstanding requests, and a resent `link` or `rename` that
already happened on the server can come back as `EEXIST` or `ENOENT`. The
faults run on one host, from just after `init` until its workers finish, so
`verify` runs on a healthy network. `run` and `matrix` take these options
before their mounts:

| Option | Default | Meaning |
| --- | --- | --- |
| `--net-reset-every S` | off | seconds between connection resets, jittered between 0.5× and 1.5× |
| `--net-outage S` | `0` | seconds after each reset in which new connections are refused |
| `--netem "ARGS"` | off | `tc netem` arguments for outgoing traffic, e.g. `"delay 5ms 2ms loss 1%"` |
| `--net-host N` | the last host | which host gets the faults, 1-based |

Network faults are on when `--net-reset-every` or `--netem` is given:

```sh
scripts/remote.sh --hosts "…" --results … run --net-reset-every 15 /mnt/nfs storage
```

- **What they hit.** All TCP from the fault host to the control machine's
  /24, as the fault host sees the control machine's address, except the
  control machine itself, so the ssh sessions survive. The file servers must
  be in that /24. The faults hit every filesystem mounted on the fault host,
  not only the one under test, and its connections to the other hosts too:
  GPFS daemons, for example, talk to each other directly and log
  "Connection reset by peer" on the other hosts.
- **A reset** kills the fault host's live connections to them with `ss -K`;
  the kernel clients reconnect on their next request. With
  `--net-outage`, an `nft` rule also refuses new connections with a
  TCP reset for that long.
- **Netem** delays, drops or reorders outgoing packets. Over TCP, loss only
  slows the client down: TCP retransmits the packets, and no RPC reply is
  lost.
- **Cleanup.** The loop runs as root under a trap that removes its `nft`
  table and netem queue, stops when the run's workers are done, and in any
  case after 2 hours. `preflight` and `net-clean` remove what a killed loop
  left.

Start with resets every 15 s and nothing else. Kernel clients back off for
seconds after a refused reconnect, so frequent resets, an outage or packet
loss can keep the fault host from committing at all; the report then shows it
as `Starved`. Check the `observed:` line of the summary (see
[Reading the report](#reading-the-report)): `link_existed` or
`rename_enoent.landed` means a lost reply reached the protocol. The file
servers often hide lost replies: an NFS server's duplicate-request cache
answers a resent request with the reply it already sent, and Lustre and
CephFS reconstruct replies on resend.

## Knobs

`init` and `run` take these flags. Durations are written `500ms`, `5s`, `2m`
or `1h`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--mode` | required | `storage` or `repository`, see [Modes](#modes) |
| `--hosts` | required (`init` only) | how many hosts run `workers` |
| `--duration` | `1m` | how long each host runs its workers |
| `--no-fsync` | fsync on | don't fsync storage writes |
| `--workers` | `8` | worker processes per host |
| `--kill-p` | `0.005` | probability of a kill at each fault point an attempt passes |
| `--pause-p` | `0.005` | probability of a pause at each fault point |
| `--pause` | `2s` | length of a pause |
| `--fault-points` | `Linked,BeforeRename,BeforeTidyUp` | where kills and pauses happen; also `Staged`, `Renamed` |
| `--external-kill-every` | `3s` | how often each host kills one of its workers, or `off` |
| `--payload-bytes` | `1024` | storage mode: padding per update |
| `--chunk-bytes` | `1024` | repository mode: chunk size |
| `--gc` | off | repository mode: run garbage collection during the run |
| `--gc-every` | `5s` | how often GC runs |
| `--gc-margin` | `3 × pause + 30s` | GC deletes what is older than this |
| `--initial-backoff` | `10ms` | repository mode: first wait before retrying a conflicting update |
| `--max-backoff` | `100ms` | repository mode: longest wait before retrying a conflicting update |

The fault points are steps of a commit of `repo`, described in
[design doc 020](../design-docs/020-filesystem-storage.md#algorithm). A
commit writes the new version to a staging file in `repo.d/` (`Staged`),
links it as the next version (`Linked`), renames the marker that names the
current version to name the new one (`BeforeRename`, then `Renamed` once it
succeeded), and deletes the versions it made obsolete (`BeforeTidyUp`).
Only the rename decides whether the commit lands.

**`--pause`.** A paused writer holds nothing, so the others keep committing
and the paused one comes back on a stale base: that is the race the pauses
are for. A worker killed at `Linked` or `BeforeRename` leaves its next
version in `repo.d/` until a later commit or the sweep deletes it. The stall
threshold is 30 s.

**`--duration`.** Each kind of fault must fire a minimum number of times,
otherwise the run fails with `FaultFloor`: 3 in-window kills, 3 pauses, 10
external kills, and 1 kill or pause at each fault point. A kind you turn off
(`--kill-p 0`, `--pause-p 0`, `--external-kill-every off`) has no minimum.
With the defaults the external kills alone need about 35 seconds. Kills and
pauses inside the protocol depend on how many updates the filesystem
sustains, which is lower on network filesystems. Keep the default minute or
longer, and raise it when `FaultFloor` says so. Runs of an hour or more find
rarer races; check the data volume first.

**`--initial-backoff` and `--max-backoff`** (repository mode) are the retry
back-off of the repository's own conflicting updates (`repo_update_retries`):
each retry waits longer, from the initial value up to the maximum. Short
waits keep many writers racing for the same version, which is what the test
is after, but every retry costs metadata operations. On a shared production
filesystem you may raise `--max-backoff`, for example to `1s`, to reduce the
load on the file server, at the cost of fewer races. In storage mode a worker
that lost a race waits a random 1 to 10 ms before its next attempt.

## Modes

- **`storage`** exercises the backend's conditional update directly. Every
  worker updates the same key, `repo`, with a payload that names the version
  it replaces. Before each attempt it copies the current version to
  `storage/overwritten/<attempt id>`, so `verify` can rebuild the whole chain
  of versions from these backups.
- **`repository`** goes through Icechunk's repository API. Workers commit to
  their own branch (60 %), commit to `main` with rebase (30 %), or tag a
  snapshot (10 %); every commit overwrites one chunk. `verify` checks the
  ancestry of every branch and of `main`, every tag, and reads back the
  chunk of every acknowledged commit.

**`--gc`** (repository mode) adds a garbage-collection worker on one host, the
first participant in sorted order. It runs every `--gc-every` and deletes what
is older than `--gc-margin`, while the other workers commit. It is killed and
restarted like any other worker.

## Reading the report

`verify` and `run` print a summary and write the full report to
`report.json` in the run root. The first line says `PASS` or `FAIL`. Then
come the hosts with their filesystem type, local or network; the operation
counts and faults fired; violations; warnings; how long the recovery write
took; and notes.

Conflicts are normal: all workers race for the same key or branch, and most
attempts lose. The counts are in the stats. An attempt without an outcome
("unresolved") belongs to a killed worker. "Failed before intent" counts
storage-mode attempts that couldn't read or back up `repo`, so they never
tried to update it; see `AttemptFailures` below.

The `observed:` line counts how commits of `repo` resolved a step whose
outcome was in doubt:

- `rename_enoent.lost_race`, `rename_enoent.superseded`: the commit rename
  failed because another writer committed first. Most conflicts end here.
- `rename_enoent.unproven`: the listing after a failed rename didn't yet show
  the winner, so the attempt reported a conflict. A few per minute on network
  filesystems are listing lag.
- `rename_enoent.landed`: the rename failed, but our own marker is there, so
  our rename landed and its reply was lost. The commit is acknowledged.
- `link_existed`: linking our new version found the name taken. The name is
  the hash of the content, so it holds our bytes: our own link, resent after a
  lost reply.
- `rename_enoent.frozen`: the repository was frozen under the commit.

`landed` and `link_existed` only happen after lost replies. They show that a
run exercised that path; they aren't failures.

A filesystem counts as a network filesystem unless its type is a known local
one: ext2/3/4, XFS, Btrfs, ZFS, tmpfs, F2FS, bcachefs, overlay, vfat, exFAT,
JFS, NILFS2, APFS or HFS. An unknown type counts as network. If
any host is on a network filesystem, the whole run is judged as one.

### Violations: the run failed

- **`LostUpdate`**: an acknowledged update is missing from the final history
  or follows a version other than its base. This is what the tool exists to
  find.
- **`Phantom`**: the history contains a version, snapshot or tag that no
  worker attempted.
- **`FalseConflict`**: on a local filesystem, an update that reported a
  conflict landed anyway, or a commit to `main` landed twice because its
  rebase ran into its own first landing.
- **`BrokenHistory`**: the history can't be rebuilt: a missing backup, a
  cycle, an unparsable payload or commit message, a `repo` that can't be
  read, or a `repo.d/` without exactly one marker and its version.
- **`Stall`**: on a local filesystem, no worker got an update acknowledged for
  longer than the stall threshold.
- **`FaultFloor`**: a kind of fault fired fewer times than required, so the
  run didn't exercise recovery enough. The message says which flag to raise.
- **`Recovery`**: after the run, a fresh writer couldn't update within its
  deadline, or aging or sweeping the repository failed.
- **`Leftovers`**: after aging and sweeping, the repository's top-level
  directory holds files other than `repo` or a `repo.d.tmp.*` directory, or
  `repo.d/` holds anything but the marker and its version. `.nfs*` files,
  which an NFS client keeps for a while after a deletion, are ignored.
- **`DataMismatch`**: repository mode: the chunk of an acknowledged commit,
  on a worker branch or on `main`, reads back with the wrong content or can't
  be read.
- **`Harness`**: the tool itself failed: malformed logs, a missing `done/`
  entry, a run that never started. It says nothing about the filesystem.

### Warnings: worth a look, not a failure

- **`LostReplyConflict`**: on a network filesystem, an update that reported a
  conflict landed anyway. This is the documented limitation of NFSv3 and
  NFSv4.0, where a lost reply to the final rename can make a successful commit
  report a conflict; see "Spurious conflicts" in the
  [storage guide](../icechunk-python/docs/docs/guides/storage.md#requirements-and-limits-of-filesystem_storage)
  and the
  [design doc](../design-docs/020-filesystem-storage.md#known-limitation-spurious-conflict-after-a-lost-commit-reply).
  Nothing is lost. On a local filesystem the same thing is a `FalseConflict`.
- **`AmbiguousErrorLanded`**: an update that reported an error, rather than a
  conflict, landed anyway. An error doesn't say whether the update happened,
  so this is always a warning.
- **`Stall`**: on a network filesystem, no worker got an update acknowledged
  for longer than the stall threshold. Network filesystems can freeze for a
  while, for example during a server failover.
- **`StartSkew`**: hosts saw the go more than 2 s apart: slow polling of the
  shared directory, or clocks out of sync.
- **`Starved`**: one host went much longer without an acknowledged update than
  the run as a whole.
- **`SubdirStaging`**: staging files below the top-level directory, such as in
  `storage/overwritten/`. Killed writes leave them, and no sweep covers
  subdirectories yet, so they are expected after a run with kills.
- **`Stragglers`**: workers still running at their host's wind-down deadline,
  which were then killed.
- **`AttemptFailures`**: storage mode: attempts that failed reading or
  backing up `repo`, before logging their intent, with the first few messages.
  They change nothing, so no other check sees them. A version whose bytes
  don't hash to its name means a reader saw other bytes than the writer
  wrote; send us the report.

## Selftest

Optional. `selftest` shows that the checks would notice a broken protocol on
your filesystem, rather than passing because they can't see anything:

```sh
fs-stress selftest --root-parent /path/on/fs/selftest
```

On this host only, it runs one case per broken protocol step and one on the
old `local_filesystem_storage` backend, which overwrites `repo`
unconditionally, in each mode (`--mode storage|repository|both`, default
both). The broken steps:

- `skip-link`: commit without linking the new version's bytes;
- `rename-from-listed-marker`: rename the marker from wherever a listing
  shows it, not from the base, so there is no compare;
- `trust-rename-enoent`: treat a failed rename as a success without listing
  again;
- `ack-unproven`: treat a failed rename as a success whenever the listing
  after it doesn't prove the outcome;
- `tidy-up-deletes-current`: the tidy-up also deletes the version just
  committed;
- `delete-above-marker`: the tidy-up and the sweep also delete versions
  newer than the marker, which other writers are about to commit;
- `unlink-base-before-rename`: delete the base's version before the rename,
  with kills in between;
- `link-after-rename`: rename first and link the bytes after, with kills in
  between;
- `delete-winner-file`: a writer that lost a race deletes the winner's
  version instead of its own.

Writers pause before the rename, so that others commit in between, and the
supervisor restarts killed workers. Every case must be *detected*: a
broken-protocol case after the sabotage fired, with a `LostUpdate`, a
`BrokenHistory` or a repository too damaged to read; a legacy-backend case
with a `LostUpdate`. The self-test exercises only the tidy-up half of
`delete-above-marker`, since no sweep runs while writers are live; a unit
test in icechunk-fs `repo_d/sweep.rs` covers the sweep half. Cases run for
`--duration` (default `20s`); the legacy-backend cases run at most 10 s, with
64-byte payloads. The run roots of detected cases are deleted; the others are
kept and printed. The exit code is 0 if every case was detected, 1 if one was
missed, 3 if a case couldn't be run.

## What to send us

From the run root:

- `report.json`;
- the summary that `verify` or `run` printed;
- `hosts/*.json`;
- if the run failed, `logs/` as well (compressed);
- the commit you built, and anything unusual about the setup (mount options,
  server, load).

**These files identify your systems.** They contain hostnames, the
filesystem's mount line (which includes the file server's name and the export
path), kernel versions and the filesystem type. Remove what you can't share,
and don't attach them to a public issue unless that's fine with you.

## Cleanup

```sh
rm -rf /path/on/fs/stress-1
```

The tool writes nothing outside the run root, needs no privileges, and kills
only processes it started itself.

## Safety notes

- **Never point it at a real repository.** `init` and `run` refuse a run root
  that isn't empty, and create a new repository under `storage/`.
- **Disk.** `init` prints an upper bound on the data volume, assuming 1000
  operations per second per worker. In storage mode every update attempt
  first writes a backup to `storage/overwritten/`.
- **Memory.** In storage mode, `verify` reads the backups one at a time and
  keeps a few hundred bytes for each distinct backed-up version, never their
  padding. It does hold every worker's log records in memory.

## The run root

```
R/run.json          configuration, written by init
R/control           wait, go with the participants, or stop
R/go                participant list, decided by one host at the go
R/hosts/<tag>.json  each registered host: hostname, kernel, mount line, fs type
R/done/<tag>.json   each finished host
R/logs/             worker and supervisor logs, one JSON record per line
R/storage/          the repository
R/report.json       written by verify
```

Nothing in the run root records where the filesystem is mounted on each host,
apart from the mount line in `hosts/*.json`, which is informational.

## Development

From the repository root, with the dev profile:

- `just fs-stress [mode] [parent] [args…]`: a single-host `run` in a new run
  root under `parent` (default `$TMPDIR`), with `args` passed on to `run`.
  The run root is deleted if the run passes and kept, with its path printed,
  if it fails.
- `just fs-stress-selftest [parent] [args…]`: `selftest` under `parent`.
- `just fs-stress-process-tests`: the ignored tests that spawn stress
  processes. `just test` runs the others.
- `just fs-stress-static`: the static binary for other hosts.
- `scripts/remote.sh`: see
  [Driving several hosts from one machine](#driving-several-hosts-from-one-machine).
- `vms/lab.sh`: a lab of libvirt VMs serving Lustre, BeeGFS, CephFS, NFS
  and GPFS to client VMs; see [its README](vms/README.md).

The scheduled CI runs `just fs-stress-process-tests` and short storage and
repository mode `just fs-stress` runs; none of the others run in CI.
