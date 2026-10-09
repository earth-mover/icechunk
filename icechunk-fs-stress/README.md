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
  from outside every few seconds. The remaining writers must keep making
  progress, taking over the locks the dead and paused ones leave behind. After
  the run, a fresh writer must succeed within `2 × stale_after + 5 s`.
- **Sweeping.** After the run, `verify` sets the modification time of every
  other file in the repository's top-level directory 25 hours into the past and
  sweeps it: with the storage's own sweep in storage mode, with garbage
  collection in repository mode. Only `repo` may be left.

What it doesn't test:

- **Power loss and durability.** Killing a process leaves the kernel's and
  the file server's state intact; nothing here crashes a machine or a server.
  Whether `fsync` really makes data durable is out of scope.
- **Performance.** The numbers it prints are not benchmarks.

A passing run is evidence, not a guarantee: the backend is experimental.

## Requirements

- **Linux** on every host. The tool is Unix-only; on other Unix systems its
  results are informative only (see below).
- **One filesystem, mounted on every host.** The mount path may differ from
  host to host: every command takes the run root's path as this host sees it
  (`--root`). The filesystem needs exclusive hard links; the backend checks
  that before its first write.
- **Synchronised clocks** (NTP) for runs on several hosts: the stall and
  start-skew checks compare timestamps from different hosts.
- **The Rust toolchain** ([rustup](https://rustup.rs)), on every host or on a
  build machine with the same architecture and C library.
- **Disk and memory.** See [Safety notes](#safety-notes).

Expected to qualify: local POSIX filesystems, NFSv3 and NFSv4 with a Linux
client, Lustre, GPFS, CephFS, JuiceFS. NFS mounts with `nocto` are
unsupported. SMB and non-Linux NFS clients (macOS, FreeBSD) have not been
analysed: you can run the tool there, but its results are informative only,
and a pass doesn't make them supported. The
[storage guide](../icechunk-python/docs/docs/guides/storage.md#requirements-and-limits-of-filesystem_storage)
lists the backend's requirements and limits.

## Building

On every host, at the tag or commit we gave you:

```sh
git clone https://github.com/earth-mover/icechunk
cd icechunk
git checkout <tag or commit>
cargo build --release -p icechunk-fs-stress
```

The binary is `target/release/fs-stress`. Build every host from the same
commit: the tool doesn't check that all hosts run the same build.

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

   Each host registers and waits. Once `--hosts` hosts have registered, one of
   them writes the participant list to `go` and the run starts. Every host
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

## Knobs

`init` and `run` take these flags. Durations are written `500ms`, `5s`, `2m`
or `1h`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--mode` | required | `storage` or `repository`, see [Modes](#modes) |
| `--hosts` | required (`init` only) | how many hosts run `workers` |
| `--duration` | `1m` | how long each host runs its workers |
| `--stale-after` | `5s` | how old a lock must be before another writer may take it over |
| `--no-fsync` | fsync on | don't fsync storage writes |
| `--workers` | `8` | worker processes per host |
| `--kill-p` | `0.005` | probability of a kill at each fault point an attempt passes |
| `--pause-p` | `0.005` | probability of a pause at each fault point |
| `--pause` | `2 × stale-after` | length of a pause |
| `--fault-points` | `LockTaken,Fenced,Compared` | where kills and pauses happen; also `LockInfoWritten`, `TicketWritten`, `Renamed` |
| `--external-kill-every` | `3s` | how often each host kills one of its workers, or `off` |
| `--payload-bytes` | `1024` | storage mode: padding per update |
| `--chunk-bytes` | `1024` | repository mode: chunk size |
| `--gc` | off | repository mode: run garbage collection during the run |
| `--gc-every` | `5s` | how often GC runs |
| `--gc-margin` | `3 × pause + 30s` | GC deletes what is older than this |
| `--initial-backoff` | `10ms` | repository mode: first wait before retrying a conflicting update |
| `--max-backoff` | `100ms` | repository mode: longest wait before retrying a conflicting update |

The fault points are steps of the commit protocol, described in
[design doc 020](../design-docs/020-filesystem-storage.md#algorithm-detail).

**`--stale-after`** is the backend's `stale_after` setting for this run
(the library's default is 60 s). A lock older than this counts as abandoned,
and the next writer takes it over; until then, writers on top of the locked
version get conflicts. It must exceed how long a healthy writer holds the
lock, which is a few metadata operations. Too short a value causes spurious
conflicts, never lost updates. Pauses (`2 × stale_after`) are long enough for
other writers to take over the paused writer's lock. The stall threshold is
`4 × stale_after + pause`: 30 s with the defaults. Raise `--stale-after` if
metadata operations on your filesystem can take seconds.

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
waits keep many writers racing inside each other's lock windows, which is
what the test is after, but every retry costs metadata operations. On a
shared production filesystem you may raise `--max-backoff`, for example to
`1s`, to reduce the load on the file server, at the cost of fewer races in
the lock window. In storage mode a worker that lost a race waits a random 1
to 10 ms before its next attempt.

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
("unresolved") belongs to a killed worker.

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
  cycle, an unparsable payload or commit message.
- **`Stall`**: on a local filesystem, no worker got an update acknowledged for
  longer than the stall threshold.
- **`FaultFloor`**: a kind of fault fired fewer times than required, so the
  run didn't exercise recovery enough. The message says which flag to raise.
- **`Recovery`**: after the run, a fresh writer couldn't update within its
  deadline, or aging or sweeping the repository failed.
- **`Leftovers`**: after aging and sweeping, the repository's top-level
  directory holds files other than `repo`.
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

- `skip-fence`: don't delete the other writer's ticket before committing;
- `skip-compare`: commit even if the current version isn't the base;
- `trust-rename-enoent`: treat a failed final rename as a success.

Every case must be *detected*: its run must fail with a `LostUpdate`, or a
broken-protocol case must leave the repository too damaged to read. Cases run
for `--duration` (default `20s`); the legacy-backend cases run at most 10 s,
with 64-byte payloads. The run roots of detected cases are deleted; the others
are kept and printed. The exit code is 0 if every case was detected, 1 if one
was missed, 3 if a case couldn't be run.

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
  root under `parent` (default `$TMPDIR`), with `--stale-after 1s`; later
  arguments override it. The run root is deleted if the run passes and kept,
  with its path printed, if it fails.
- `just fs-stress-selftest [parent] [args…]`: `selftest` under `parent`.
- `just fs-stress-process-tests`: the ignored tests that spawn stress
  processes. `just test` runs the others.

None of these run in CI.
