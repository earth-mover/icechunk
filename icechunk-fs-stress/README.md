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

If you only want to test your filesystem and send us the results, follow the
shorter [testing guide](TESTING.md) instead of this README.

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
- **Modes**. Storage mode uses the filesystem storage directly, for high
  concurrency; repository mode uses the full Icechunk API, a higher level
  check at the price of less concurrency. See [Modes](#modes).
- **The mirror `repo`.** Every change publishes the version it made as
  `repo` (in the storage root, next to `repo.d/`), a byte-identical copy that
  Icechunk versions without the filesystem layout read. Every copy a
  publication installs, and every read of `repo`, must be a version that was
  current; after `verify`'s own publication, with no other writer, it must
  be the current version; after a change that sets the status `ReadOnly` it
  must hold that change's version; and every object it references must
  exist.
- **The writer feature.** Every version that was current lists
  `filesystem-layout-v1` with its gate, the `ReadOnly` status that keeps
  older Icechunk versions from writing, over the run's own status. The first
  version's number is its creation time in milliseconds.
- **Forks**, with `--fork-at`: a `repo` replaced as Icechunk 2.0 to 2.3 replace
  it when they set the status. Every change that landed afterwards must
  report the fork, no GC may delete while it is there, only a publication
  whose fork check came before it may erase it, and both histories must
  survive for a manual recovery.
- **Conversions**, with `convert-scenario`: both conversions between the
  object store layout and the filesystem layout, killed at every step and rerun, keep
  every acknowledged change and leave only the target layout. See
  [Conversion scenario](#conversion-scenario).
- **Sweeping.** After the run, `verify` plants entries the sweep and GC must
  never delete (canaries), sets the modification time of the protocol's
  leftovers 25 hours into the past and sweeps them: with the storage's own
  sweep in storage mode, with garbage collection in repository mode. Then
  only `repo`, `repo.d/` holding the marker and the one version it names,
  and the data directories (`overwritten/`, `snapshots/`, `manifests/`,
  `transactions/`, `chunks/`) may be left, and every canary must still be
  there.

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

The [storage guide](../icechunk-python/docs/docs/guides/storage.md#requirements-and-limits-of-filesystem_storage)
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
Linux host, whatever its C library. It needs the
[development environment](https://icechunk.io/en/latest/reference/contributing/#setting-up-your-development-environment),
Rust's `x86_64-unknown-linux-musl` target and a musl C compiler (with rustup on
Debian or Ubuntu: `rustup target add x86_64-unknown-linux-musl` and
`apt install musl-tools`; the repository's Nix dev shell has both):

```sh
CC_x86_64_unknown_linux_musl=musl-gcc just fs-stress-static
```

In the Nix dev shell, run `just fs-stress-static` without the variable.

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
phases of the check. Stdout gets only the final summary and what to do next.
The workers' own stderr, with Icechunk's warnings and any panic, goes to
`R/stderr/<tag>.txt`. The exit code is 0 for a pass, 1 for a failure, 3 for
an error of the tool itself or its setup.

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

- **Host names.** A host's tag defaults to `<hostname>-<pid>`; `--tag`
  overrides it. `workers --workers` overrides the run's workers per host for
  this host.
- **Stopping early.** `fs-stress stop --root R` tells every host to stop; they
  notice within a second. Hosts still waiting for the go exit with code 2.
  `verify` still checks a stopped run.
- **Exit codes.** `workers`: 0, 2 for a late host, 1 for other errors.
  `verify`: 0 for a pass, 1 for violations, 3 for errors of the tool or its
  setup. A report whose only violations are `Harness` ones exits with 3. With
  hosts still missing, every violation is provisional: a host that is still
  writing can make the others look wrong. The summary then says
  "provisional: hosts still missing (…)" and `verify` exits with 3; verify
  again once they finished.

## Qualification

`fs-stress qualify` runs the fixed test we use to qualify a filesystem: a
storage-mode run, then a repository-mode run, checked together into one
result. Please run it unchanged.

On one host:

```sh
fs-stress qualify run --root /path/on/fs/qualify-1
```

On several hosts, as in [Several hosts](#several-hosts):

```sh
fs-stress qualify init --root /path/on/fs/qualify-1 --hosts 3   # on one host
fs-stress qualify workers --root /path/on/fs/qualify-1          # on every host
fs-stress qualify verify --root /path/on/fs/qualify-1           # on one host, at the end
```

`qualify workers` takes part in the storage-mode run, then in the
repository-mode run, even if the first went wrong, with the same tag in both.
The repository-mode run starts once every host registered for it, or 10
minutes after a host registered, with the hosts registered by then; the
summary names the hosts missing from it. `qualify stop --root R` stops both
runs.

`qualify` keeps its output short, for people who don't know the test: on
stderr, a line every 10 seconds with the time left (`Run 1 of 2: 2m 30s of 5m
done, about 13 min left`) and the hosts it is waiting for, each line after
the host name for `qualify workers` (`[host2] Run 1 of 2: …`). The status lines
that `workers` and `verify` print go to `R/<run>/progress/<tag>.txt`, one
file per host, and `R/<run>/progress/verify.txt`, each line with its time.
The workers' warnings go to `R/<run>/stderr/<tag>.txt`.

| | Storage mode | Repository mode |
|---|---|---|
| Duration | 5m | 10m |
| GC | | on, default margin and interval |
| `--external-kill-every` | 10s | 10s |
| `--mirror-sample-every` | 25ms | 25ms |
| `--readonly-every` | | 10s |

Every other setting keeps its default. Only `--hosts` (`init`), `--workers`
(workers per host, default 8) and `--max-backoff` (default 100ms) can change.

The whole qualification needs up to about 10 GB of free space on the
filesystem under test, whatever the number of hosts and workers; usually
under 1 GB on network filesystems. Most of it is the backup of the repo info
that Icechunk keeps in `storage/overwritten/` for every change.

`qualify verify` (and `qualify run`) verifies each run as `verify` does, then
writes the combined summary to `R/summary.txt`: the overall result and the
build, each run's summary, the hosts missing from the second run, and what to
do next. It prints the result in short: the overall result, the build, a
line per run (passed, how many problems it found, or why it has no result),
the notes that matter to whoever runs it (a changed preset, other builds,
hosts that started seconds apart by their clocks), where the full summary is,
and what to do next. A run
that hasn't started yet makes the result provisional: run `qualify verify`
again once every host finished. A second `qualify verify` refuses a run
already verified fully, as `verify` does, and shows its earlier report.

Exit codes. `qualify workers`: 1 if either run hit an error, else 2 if this
host was too late for either run, else 0. `qualify verify` and `qualify run`:
1 if either run failed, else 3 if either is an error or provisional, else 0.

```
R/qualify.json      the preset, hosts, workers, max back-off, the build
R/storage-mode/     the storage-mode run's run root, with progress/ and stderr/
R/repository-mode/  the repository-mode run's run root, with progress/ and stderr/
R/summary.txt       the combined summary
```

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
| `report.json`, `run.json`, `control`, `go`, `hosts/`, `done/`, `stderr/` | copied from the run root |
| `logs.tar.gz` | the run root's `logs/`, for every run |
| `init.log`, `workers-<i>.log`, `verify.log` | output of each step; `workers-<i>.log` is host `i` (0-based), whose workers' stderr is in `stderr/` |
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
| `--fault-points` | `Linked,BeforeRename,BeforeTidyUp,BeforePublish,BeforeMirrorRename` | where kills and pauses happen; also `Staged`, `Renamed`, `MirrorStaged`, `MirrorRenamed` |
| `--external-kill-every` | `3s` | how often each host kills one of its workers (never the status toggler), or `off` |
| `--payload-bytes` | `1024` | storage mode: random padding per update |
| `--chunk-bytes` | `1024` | repository mode: chunk size |
| `--gc` | off | repository mode: run expiration and garbage collection during the run |
| `--gc-every` | `5s` | how often expiration and GC run |
| `--gc-margin` | `3 × pause + 3m` | expiration and GC remove what is older than this; must be shorter than `--duration` |
| `--initial-backoff` | `10ms` | repository mode: first wait before retrying a conflicting update |
| `--max-backoff` | `100ms` | repository mode: longest wait before retrying a conflicting update |
| `--readonly-every` | off | repository mode: how often the status toggler sets the status `ReadOnly` |
| `--readonly-hold` | `3s` | how long the status toggler holds `ReadOnly`; shorter than `--readonly-every` and the 30 s stall threshold |
| `--mirror-sample-every` | off | how often every host reads `repo` and logs what changed, e.g. `25ms` |
| `--fork-at` | off | when, after the go, the fork actor replaces `repo` with a fork |
| `--fork-repeat` | off | fork again 500 ms after each erasure of the fork |

The fault points are steps of a change of the repository, described in
[design doc 020](../design-docs/020-filesystem-storage.md#algorithm-overview).
A change writes the new version to a staging file in `repo.d/` (`Staged`),
links it as the next version (`Linked`), renames the marker that names the
current version to name the new one (`BeforeRename`, then `Renamed` once it
succeeded), and deletes the versions it made obsolete (`BeforeTidyUp`). Only
the rename decides whether the change lands. Then it publishes the new
version as `repo` (`BeforePublish`): it copies it to `repo.d/mirror.<id>`
(`MirrorStaged`), checks `repo` for a fork, renames the copy over `repo`
(`BeforeMirrorRename`, then `MirrorRenamed`) and lists `repo.d/` to see
whether a newer version needs publishing.

**`--pause`.** A paused writer holds nothing, so the others keep committing
and the paused one comes back on a stale base: that is the race the pauses
are for. A worker killed at `Linked` or `BeforeRename` leaves its next
version in `repo.d/` until a later commit or the sweep deletes it; one killed
at `BeforePublish` or `BeforeMirrorRename` leaves `repo` behind until the next
publication. The stall threshold is 30 s.

**`--duration`.** Each kind of fault must fire a minimum number of times,
otherwise the run fails with `FaultFloor`: 3 in-window kills, 3 pauses, 10
external kills, and 1 kill or pause at each fault point. A kind you turn off
(`--kill-p 0`, `--pause-p 0`, `--external-kill-every off`) has no minimum.
With the defaults the external kills alone need about 35 seconds, and a
point that fires only on some attempts, such as `BeforeMirrorRename`, needs
enough publications. Kills and
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
  worker updates the same key, `repo`, with a real repo info whose metadata
  holds the worker's payload, naming the version it replaces, and random
  padding. Before each attempt it copies the current version to
  `storage/overwritten/repo.<14 digits>.<id>`, the name the attempt's new
  repo info gives its predecessor, as Icechunk does, so `verify` can rebuild
  the whole chain of versions from these backups.
- **`repository`** goes through Icechunk's repository API. Workers commit to
  their own branch (55 %), commit to `main` with rebase (30 %), tag a
  snapshot of their branch (9 %), delete one of their tags (4 %), or delete
  their branch and create it again (2 %); every commit overwrites one chunk.
  `verify` checks the ancestry of every branch and of `main`, every tag, and
  reads back the chunk of every acknowledged commit that is still reachable.

The first participant, in sorted order, also runs these, killed and
restarted like any other worker, except that external kills spare the
status toggler:

- **`--gc`** (repository mode): a worker that, every `--gc-every`, expires
  the snapshots older than `--gc-margin` and then collects garbage, while
  the other workers commit. Expired and deleted commits are excused only
  where the expiration or deletion explains them. A commit that started
  before GC's cutoff and lands after it can reference what GC deleted, so the
  margin must exceed the longest commit: on a shared filesystem under load,
  over a minute. `init` refuses a `--duration` not longer than the margin,
  since GC would delete nothing.
- **`--readonly-every`** (repository mode only: storage-mode workers write
  repo infos below Icechunk's status check): the status toggler, which sets
  the status `ReadOnly`, holds it for `--readonly-hold`, checks `repo` and
  sets it `Online` again. Writers are refused meanwhile. If it dies during a
  hold, `verify` sets the status `Online` and notes it.
- **`--fork-at`**: the fork actor. At that time after the go it copies
  `repo` to `overwritten/`, logging the backup's name first, and replaces
  `repo` with a repo info without the writer feature and the gate, through a
  temporary file and a rename, as Icechunk 2.3's local backend writes a
  status change. It then watches
  `repo` and logs when the fork is erased; with `--fork-repeat` it forks again
  500 ms later.

`--mirror-sample-every` starts a sampler thread on every host that reads
`repo` at that interval and logs every change. On NFS, keeping `repo` open
that often makes the client's silly renames of a replaced open file more
likely, so a few `MirrorMissing` warnings may be the sampler's own doing.

`scripts/remote.sh matrix` turns on the sampler (`25ms`) and, in repository
mode, the status toggler (`10s`).

## Reading the report

`verify` and `run` print a summary and write the full report to
`report.json` in the run root, and what they printed to `summary.txt`. The
first line says `PASS` or `FAIL`. Then come the build (the Icechunk version
and the commit fs-stress was built from, as `fs-stress --version` shows it),
with a note if some hosts ran a different build; the hosts with their
filesystem type, local or network; the operation counts and faults fired;
violations; warnings; how long the recovery write took; and notes. Last comes
what to do next.

Conflicts are normal: all workers race for the same key or branch, and most
attempts lose. The counts are in the stats. "Refused" counts operations the
repository's status or layout refused: writers during a `ReadOnly` hold, and
the refusals of a fork run. An attempt without an outcome ("unresolved")
belongs to a killed worker. "Failed before intent" counts storage-mode
attempts that couldn't read or back up `repo`, so they never tried to update
it; see `AttemptFailures` below.

The `mirror:` line counts the publications' renames of `repo`, the changes the
samplers saw, the status toggler's holds, the holds that ended with an older
version renamed over `repo` by a publication still in flight (mirror lag, not
a failure), and the holds not judged because the storage logged an error
during the `ReadOnly` change.

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

`landed` and `link_existed` only happen after lost replies. They show that a
run exercised that path; they aren't failures.

A filesystem counts as a network filesystem unless its type is a known local
one: ext2/3/4, XFS, Btrfs, ZFS, tmpfs, F2FS, bcachefs, overlay, vfat, exFAT,
JFS, NILFS2, APFS or HFS. An unknown type counts as network. If
any host is on a network filesystem, the whole run is judged as one.

### Violations: the run failed

- **`LostUpdate`**: an acknowledged update is missing from the final history
  or follows a version other than its base, or an acknowledged deletion
  didn't remove what it deleted. This is what the tool exists to find.
- **`Phantom`**: the history contains a version, snapshot or tag that no
  worker attempted.
- **`FalseConflict`**: on a local filesystem, an update that reported a
  conflict landed anyway, or a commit to `main` landed twice because its
  rebase ran into its own first landing.
- **`BrokenHistory`**: the history can't be rebuilt: a missing backup, a
  cycle, an unparsable payload or commit message, a `repo` that can't be
  read or is missing, or a `repo.d/` without exactly one marker and its
  version.
- **`Stall`**: on a local filesystem, no worker got an update acknowledged for
  longer than the stall threshold.
- **`FaultFloor`**: a kind of fault fired fewer times than required, so the
  run didn't exercise recovery enough. The message says which flag to raise.
- **`Recovery`**: after the run, a fresh writer couldn't update within its
  deadline, setting the status `Online` failed, or aging or sweeping the
  repository failed.
- **`Leftovers`**: after aging and sweeping, the storage root holds a
  staging file (`<name>.staging.<id>`), `object_store`'s `repo#<n>` or a
  `repo.d.tmp.<id>/` directory, or `repo.d/` holds a version other than the
  marker's, a `stage.<id>`, a `mirror.<id>` or a staging file. `.nfs*` files,
  which an NFS client keeps for a while after a deletion, are ignored.
- **`DataMismatch`**: repository mode: the chunk of an acknowledged commit,
  on a worker branch or on `main`, reads back with the wrong content or can't
  be read.
- **`DeletedUnrecognized`**: the sweep or GC deleted a canary: an entry it
  doesn't recognize, such as an unknown name or an object id that isn't
  canonical.
- **`MirrorNotAVersion`**: a publication installed, or a sample read, a
  `repo` that was never a current version.
- **`MirrorStale`**: on a local filesystem, `repo` isn't the current version
  after `verify`'s own publication or its recovery write, with no other
  writer.
- **`ReadOnlyMirrorMismatch`**: at the end of a `ReadOnly` hold, on a local
  filesystem, `repo` wasn't the `ReadOnly` change's version, and no
  publication that started before it explains the difference; or, on any
  filesystem, a change landed during the hold.
- **`MirrorDangling`**: repository mode: `repo` references a snapshot,
  manifest or transaction log that doesn't exist. Expiration, deletions and
  GC make this possible, which is why GC publishes before it deletes. See
  `GcCutoffAfterSessionStart` for the one exception.
- **`FeatureMissing`**: a version that was current, or `repo`, doesn't list
  exactly `filesystem-layout-v1` with its gate over a status the run set.
- **`InitialVersion`**: the first version's number isn't within the
  milliseconds of its creation.
- **`UnreportedFork`**: on a local filesystem, an operation that started after
  a fork, and ended before the fork was erased, was acknowledged: its change
  landed and didn't report the fork. After a fork every change lands in
  `repo.d/` and reports the fork as a durable error.
- **`GcAfterFork`**: on a local filesystem, a GC that started after a fork, or
  with a fork written before its hook, was acknowledged: it deleted while the
  fork was in `repo`.
- **`SilentForkLoss`**: a fork was erased by something other than a
  publication whose fork check read `repo` before the fork was written: its
  last read of `repo` began by the time the fork's write returned (in logs
  without the read's time, the check passed by then). The check's listing of
  `repo.d/` after the read can take long on a network filesystem. On a
  network filesystem, see also `ForkErasedAfterKilledPublisher`.
- **`ForkDamaged`**: after a run whose fork persisted, the fork, its backup
  or (repository mode) the objects it references are gone, so a recovery
  that keeps `repo` is no longer possible.
- **`ConversionMismatch`**, **`ConversionEndState`**, **`OverwroteObjectStore`**,
  **`DeletedHistory`**, **`WriterDuringConversion`**, **`ConversionRetried`**:
  the conversion scenario's, see [Conversion scenario](#conversion-scenario).
- **`Harness`**: the tool itself failed: malformed logs, a missing `done/`
  entry, a run that never started, a storage root that doesn't exist or holds
  no repository, or a run in which no operation of the workers landed (they
  worked on another storage root, or none). It says nothing about the
  filesystem.

### Warnings

- **`LostReplyConflict`**: on a network filesystem, an update that reported a
  conflict landed anyway. This is the documented limitation of NFSv3 and
  NFSv4.0, where a lost reply to the final rename can make a successful commit
  report a conflict; see "Spurious conflicts" in the
  [storage guide](../icechunk-python/docs/docs/guides/storage.md#requirements-and-limits-of-filesystem_storage)
  and the
  [design doc](../design-docs/020-filesystem-storage.md#accepted-limits).
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
- **`UnknownEntries`**: names in the storage root or `repo.d/` that the layout
  never creates and GC never deletes: someone else's, leftovers of removed
  designs, or a bug.
- **`MirrorStale`**, **`ReadOnlyMirrorMismatch`** (the mirror mismatch only),
  **`UnreportedFork`**, **`GcAfterFork`**: the violations of the same name on a
  network filesystem, where reads of `repo` may be stale and `publish_mirror()`
  is best effort.
- **`MirrorMissing`**, **`MirrorUnreadable`**: samples that found no `repo`, or
  couldn't read it. The layout never removes `repo`, so on a local filesystem
  a replacement wasn't atomic.
- **`SamplerFailed`**: a host's sampler couldn't log; its samples end there.
- **`ForkErasedInWindow`**: a fork erased by a publication whose fork check
  read `repo` before the fork (see `SilentForkLoss`) and whose rename came
  after it: the accepted limit, with the milliseconds from the check passing
  until the rename returned, unknown if the rename was announced but never
  recorded done. A rename counts at any time from its announcement until it
  returned; one never recorded done, until its log's next record (a failed
  rename, as `ENOENT` after a lost reply, may hide one that landed) or, if
  that was the last, its process's death.
- **`ForkErasedAfterKilledPublisher`**: on a network filesystem only, a fork
  erased by bytes that a publication whose fork check read `repo` before the
  fork (see `SilentForkLoss`) announced renaming, its process seen dead
  before the fork was written, and that no publication whose fork check came
  after the fork, or that had none, may have renamed since. The kill cancels
  the wait for the rename RPC, not the RPC: the server may apply it after the
  process died. On a local
  filesystem this is a `SilentForkLoss`.
- **`ForkActorPartialBackup`**: a backup in `overwritten/` that doesn't
  decode, which a fork's write named in its log before copying `repo` and
  which no fork it wrote names: the emulated old writer's copy was
  interrupted (on NFS, `repo` renamed over during the copy fails it with
  `ESTALE`), and no ops log references the partial file. Any other backup
  that doesn't decode is a `BrokenHistory`.
- **`ConversionRetried`**: the conversion scenario's, on a network
  filesystem.
- **`GcCutoffAfterSessionStart`**: repository mode with `--gc`: a commit
  started before a GC's cutoff and was still running when that GC began, by
  more than the clock allowance on both counts, and the GC may have landed
  (its outcome isn't a conflict or a refusal). The run broke GC's
  precondition: the spec requires the cutoff to be earlier than the start of
  every session that may still commit. A storage stall longer than
  `--gc-margin` does this. That GC may have deleted the commit's objects, so
  the warning lists, and excuses, the missing snapshot file and transaction
  log of the commit's acknowledged snapshot, which would otherwise be
  `MirrorDangling`, `BrokenHistory` or `ForkDamaged` violations. It also
  lists the manifest or chunk, both written by the commit, that the
  read-back of its chunk found missing, and excuses only that read-back's
  `DataMismatch`: a missing manifest is still `MirrorDangling`, the commit's
  own as well as one a later commit inherited. Any other missing object, and
  wrong bytes, are still violations. A larger `--gc-margin` avoids this
  warning.
- **`StaleRootListing`**: the conversion scenario's, on a network filesystem
  only: a rerun of `convert_to_object_store_layout()` returned `NothingToConvert`
  and left `repo.d/`, with `repo` its current version without the feature,
  because its listing of the storage root missed `repo.d/`. That is an
  accepted limit (see "Rerun misses `repo.d`" in the
  [design doc](../design-docs/020-filesystem-storage.md#accepted-limits));
  the scenario runs the conversion again after the delay it names.

### Fork runs

With `--fork-at`, before `verify` checks anything, it logs a fork that
`repo` no longer holds although the fork actor didn't see it erased (a
publication of a worker winding down). If the last fork is still in `repo`,
`verify` checks both sides instead of cleaning up: the fork, its backup and,
in repository mode, every object it references must exist; its publication
must refuse the fork; its one recovery write must land in `repo.d/` and
report the fork. It skips the canaries, the aging, the sweep and the
clean-root check, since the storage refuses GC while the fork is there, and
says so in a note.

## Selftest

Optional. `selftest` shows that the checks would notice a broken protocol on
your filesystem, rather than passing because they can't see anything:

```sh
fs-stress selftest --root-parent /path/on/fs/selftest
```

On this host only, it runs one case per broken protocol step and one on the
old `local_filesystem_storage` backend, which overwrites `repo`
unconditionally, in each mode (`--mode storage|repository|both`, default
both) that can detect it. Each case names the violations that detect it:

| Case | Modes | Detected by |
| --- | --- | --- |
| `skip-link`: commit without linking the new version's bytes | both | `LostUpdate`, `BrokenHistory` |
| `rename-from-listed-marker`: rename the marker from wherever a listing shows it, not from the base, so there is no compare | both | `LostUpdate`, `BrokenHistory` |
| `trust-rename-enoent`: treat a failed rename as a success without listing again | both | `LostUpdate`, `BrokenHistory` |
| `ack-unproven`: treat a failed rename as a success whenever the listing after it doesn't prove the outcome | both | `LostUpdate`, `BrokenHistory` |
| `tidy-up-deletes-current`: the tidy-up also deletes the version just committed | both | `LostUpdate`, `BrokenHistory` |
| `delete-above-marker`: the tidy-up and the sweep also delete versions newer than the marker, which other writers are about to commit | both | `LostUpdate`, `BrokenHistory` |
| `unlink-base-before-rename`: delete the base's version before the rename, with kills in between | both | `LostUpdate`, `BrokenHistory` |
| `link-after-rename`: rename first and link the bytes after, with kills in between | both | `LostUpdate`, `BrokenHistory` |
| `delete-winner-file`: a writer that lost a race deletes the winner's version instead of its own | both | `LostUpdate`, `BrokenHistory` |
| `ack-superseded-without-proof`: acknowledge an attempt whose listing shows a marker past its own without the lost-reply check | both | `LostUpdate` |
| `publish-without-fork-check`: publish without reading `repo`, with the fork actor forking 30 % of the way in and again after each erasure | both | `SilentForkLoss` |
| `publish-non-current`: publish the version with the highest number in a listing, with frequent kills after linking, so that versions above the marker exist, and short frequent pauses before publications | both | `MirrorNotAVersion`, `MirrorStale` |
| `commit-without-publish`: a change returns without publishing, the status toggler's `ReadOnly` change included, every 3 s for 1 s | repository | `ReadOnlyMirrorMismatch` |
| `gc-without-fork-check`: GC's hook before its deletes doesn't check for a fork, with a fork written by the GC worker once GC's change is published, before the hook | repository | `GcAfterFork` |
| `gc-publishes-listed`: GC's hook publishes the version its listing shows, not the one GC's change made | repository | `MirrorStale`, `MirrorDangling` |
| `from-object-store-without-provenance`: the rerun of an interrupted conversion from the object store layout publishes without the provenance check | repository | `ConversionEndState`, `SilentForkLoss` |
| `to-object-store-without-change`: the conversion to the object store layout publishes the version it read, converted, without its change | repository | `LostUpdate` |
| `delete-dir-before-publish`: the conversion to the object store layout deletes `repo.d` before publishing | repository | `ConversionEndState`, `DeletedHistory`, `LostUpdate` |

Writers pause before the rename, so that others commit in between, and the
supervisor restarts killed workers. A broken-protocol case is *detected* when
the sabotage fired and the run failed with one of its violations, or the
repository is too damaged to read; a legacy-backend case when it lost an
update. Notes on some cases:

- `delete-above-marker`: the self-test exercises only the tidy-up half, since
  no sweep runs while writers are live; a unit test in icechunk-fs
  `repo_d/sweep.rs` covers the sweep half.
- `unlink-base-before-rename`: a writer killed after unlinking its base
  leaves the marker without bytes only until another writer that read that
  base renames past it. Under eight writers one almost always does, so the
  case runs at most two and kills before the rename with probability 0.3.
- `publish-non-current`: a version above the marker that never becomes
  current is one a writer killed after linking leaves, until the next commit's
  tidy-up deletes it. A publication right after its commit lists `repo.d/`
  just after that tidy-up and finds nothing above the marker, so the sabotage
  publishes the right version. The case therefore pauses publications before
  they start (`BeforePublish`, probability 0.15, 500 ms) and kills writers
  after linking and before the rename with probability 0.15, so that a paused
  publication lists while a killed writer's version waits.
- `commit-without-publish`: the status toggler's storage carries the
  sabotage like every worker's, so its own `ReadOnly` change returns without
  publishing, and `repo` at the end of the hold is whatever an earlier
  publication left. That is the failure M4 states. Without the sabotage on
  the toggler's storage, its change would publish and no hold could show it.
- `gc-without-fork-check`: GC's own change publishes first, and its fork check
  would find any fork written before it, so the case writes the fork between
  that publication and GC's hook, where only the hook's check can find it.
  The GC cases run one writer: GC updates the repo info once per attempt and
  rarely gets through under eight.
- `gc-publishes-listed` is **undetectable on a local filesystem**, and the
  case says so (`undetectable here`) instead of failing: a listing of
  `repo.d/` never lags there, so the version GC's hook lists is never older
  than the one GC's change made. On a network filesystem it is detected only
  if a listing lags in that window.
- The three conversion cases run the [conversion scenario](#conversion-scenario)
  for one round without a stress phase or kills, with what makes their
  sabotage matter: `from-object-store-without-provenance` kills the first run
  after its directory rename and replaces `repo` as Icechunk 2.3 writes a
  status change before the rerun, which must then stop; `to-object-store-without-change`
  lands a commit right after the conversion read the current version, a
  commit that publishes nothing, as a stale listing would hide it from the
  conversion on a network filesystem.

Cases run for `--duration` (default `20s`); the legacy-backend cases run at
most 10 s, with 64-byte payloads, and the conversion cases a few seconds. The
run roots of detected and undetectable cases are deleted; the others are kept
and printed. The exit code is 0 if every case was detected or is
undetectable here, 1 if one was missed, 3 if a case couldn't be run.

## Conversion scenario

```sh
fs-stress convert-scenario --root /path/on/fs/convert-1
```

On this host only, on one repository, in rounds (`--rounds`, default 2):

1. In round 1, it creates a repository in the object store layout through
   `local_filesystem_storage` and commits to it (`--object-store-commits`, default
   3).
2. It converts it to the filesystem layout with
   `convert_to_filesystem_layout()`.
3. A stress phase in repository mode with GC, `--workers` (default 8) for
   `--duration` (default `1m`; `0s` for none), checked by `verify` as any
   run, with every earlier round's operations.
4. It converts it to the object store layout with `convert_to_object_store_layout()`.
5. It commits to it in the object store layout again.

Each run of a conversion is a child process (`fs-stress conversion`), run
again after a kill until it finishes. In every round each conversion has one
guaranteed kill with `SIGKILL`: from the object store layout at one of its steps,
chosen at random; to the object store layout right after its change or its
publication, chosen at random, so that C7 runs. Once that kill landed, every
later run is killed at a random step with probability `--conversion-kill-p`
(default `0.3`). The
summary's notes list the steps runs were killed at and how many writers C7
tried. A run can end before its guaranteed step (a rerun that fails as under
stale listings); the next run of that conversion then gets the same step. A
scenario that finished in which no conversion from the object store layout was
killed or C7 never ran fails with a `Harness` violation; one that ended early
on a violation says so in a note instead. Only the conversions run
during a conversion, as they require. A stress phase's `run.json` names the
shared `storage/`, so its workers and `verify`, which get only the phase's
run root, use it. It checks:

- **C1**: no acknowledged change is lost: the stress phase's `verify`, and the
  repository oracle through `local_filesystem_storage` after each conversion
  to the object store layout, over the logs of every round so far.
- **C2** (`ConversionMismatch`): a conversion changes only the writer feature,
  the gate and the ops log: the repo info it wrote is the one it backed up,
  named on its new ops-log entry, with the feature added or removed and one
  status change on top.
- **C3** (`ConversionEndState`): a finished conversion leaves the target
  layout and nothing else: to the object store layout, a `repo` without writer
  features or gate, no `repo.d/` and no `repo.d.tmp.*`; from it, `repo.d/`
  with one marker whose version `repo` copies, and no `repo.d.tmp.*`. A
  conversion that can't finish is a `ConversionEndState` too, and ends the
  scenario.
- **C4** (`InitialVersion`): the first version a conversion from the object store
  layout builds is numbered by the time of the conversion.
- **C5** (`OverwroteObjectStore`): after the commits in the object store layout,
  nothing has made `repo` list the feature again or created `repo.d/`.
- **C6** (`DeletedHistory`): after every run of a conversion to the object store
  layout, killed or not, `repo.d/` is gone only if `repo` holds that
  conversion's version without the feature.
- **C7** (`WriterDuringConversion`): after a run of the conversion to the
  object store layout killed right after its change or its publication, while the
  current version lacks the feature, a writer through `filesystem_storage`
  must fail with the error that says to rerun `convert_to_object_store_layout()`.
  Runs killed at later steps, once `repo.d/` is renamed aside, try no writer.
- **Reruns** (`ConversionRetried`): a rerun that fails as only stale listings
  make it fail (relisting ran out, or a fork reported while `repo` holds the
  bytes of a version of `repo.d/`) is retried; that is a warning on a network
  filesystem and a violation on a local one.
- **Reruns that miss `repo.d/`** (`StaleRootListing`): on a network
  filesystem, a rerun of the conversion to the object store layout whose listing
  of the storage root misses `repo.d/` returns `NothingToConvert` and leaves
  it, with `repo` the conversion's version without the feature. That is an
  accepted limit, cured by running the conversion again once the listing
  refreshes, so the scenario warns and runs it again, up to 9 times, after
  0.5, 1, 2, 4 and 8 s, then 16 s each: 79.5 s in all, chosen to exceed the
  longest time NFS caches directory attributes (`acdirmax`, 60 s by default).
  These reruns don't count toward the conversion's 12 runs. If `repo.d/` is
  still missed after the last, that is a `ConversionEndState` saying so. On a
  local filesystem the same end state is a `ConversionEndState` (`repo.d
  exists`) right away.

It prints a summary and writes `report.json` and `summary.txt` in its root,
with every violation of the stress phases too; each phase's `verify` report
is in `phase-<k>/report.json`. The exit codes are `verify`'s.

## What to send us

```sh
fs-stress bundle --root /path/on/fs/stress-1
```

This packs the results into one file,
`fs-stress-<pass|fail>-<UTC time>.tar.gz` in the current directory, and
prints its path and size. `--output FILE` names another file; `bundle` never
overwrites one. It takes a run root, a qualification's root or a conversion
scenario's root. Send the file in the #icechunk channel of the
[Earthmover community Slack](https://join.slack.com/t/earthmover-community/shared_invite/zt-2cwje92ir-xU3CfdG8BI~4CJOJy~sceQ)
or in a direct message to an Icechunk developer there, or open an
[issue](https://github.com/earth-mover/icechunk/issues) without attaching it,
and we'll contact you with a private way to send it. Tell us anything unusual
about the setup (mount options, server, load).

The file holds every file under the root (`run.json`, `control`, `go`,
`hosts/`, `done/`, `logs/`, `progress/`, `stderr/`, `report.json`, `summary.txt`, `qualify.json`,
`scenario.json`), but none of the repository's data under `storage/`, and no
earlier bundles. If any run didn't pass or has no report, the file also holds
every repository's `repo`, `repo.d/` and a listing, `storage-listing.txt`:
every entry under `storage/` with its size and modification time. `bundle.json` gives the fs-stress build, when and
with which command the file was made, and the files left out: symbolic
links, other files that aren't regular files, and files that vanished or
failed to read.

**The file identifies your systems.** It contains hostnames, the
filesystem's mount line (which includes the file server's name and the
export path), kernel versions, the filesystem type and the path of the run
root. `bundle` removes none of it. If you'd rather not share these details
publicly, send the file privately: in a direct message to an Icechunk
developer on Slack, or open an issue without it and we'll contact you.

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
  operations per second per worker: per operation, in storage mode the
  padding plus 4 KiB for the rest of a repo info, in repository mode the
  chunk plus 8 KiB. In storage mode every update attempt first writes a
  backup to `storage/overwritten/`. Repo info files are compressed, so the
  random padding takes about three quarters of `--payload-bytes` on disk.
- **Memory.** In storage mode, `verify` reads the backups one at a time and
  keeps a few hundred bytes for each distinct backed-up version, never their
  padding. It does hold every worker's log records in memory.

## The run root

```
R/run.json          configuration, written by init
R/control           wait, go with the participants, or stop
R/go                participant list, decided by one host at the go
R/hosts/<tag>.json  each registered host: hostname, kernel, mount line, fs type,
                    the fs-stress build
R/done/<tag>.json   each finished host
R/logs/             logs, one JSON record per line:
  <tag>.<slot>.<n>.log   worker <slot>'s incarnation n: 0, 1, … for writers,
                         gc, toggler and fork for the GC worker, the status
                         toggler and the fork actor
  <tag>.supervisor.log   the host's supervisor: spawns and external kills
  <tag>.sampler.log      the host's mirror sampler
  init.init.0.log        init: the first version's number
  verify.recovery.0.log  verify's recovery write
  verify.publish.<k>.log verify's publications of repo, one per verify
  verify.fork.<k>.log    a fork erasure verify found
R/stderr/<tag>.txt  the stderr of the host's workers: warnings and panics
R/storage/          the repository
R/report.json       written by verify
R/summary.txt       what verify printed
```

The host tags `init`, `verify` and `scenario` are reserved, and so are the
slots `supervisor` and `sampler`.

The conversion scenario's root:

```
R/scenario.json     its configuration, which its converters read
R/storage/          the repository, shared by every round
R/phase-<k>/        round k: a run root on R/storage/ (run.json, control, go,
                    hosts/, done/, report.json and summary.txt for its
                    stress phase) whose logs/ also hold
                    scenario.<slot>.<n>.log: the converters
                    (from-object-store, to-object-store, n counting runs), the
                    commits in the object store layout (object-store), the
                    writers during an interrupted conversion (writer) and a
                    planted object-store-layout write (plant)
R/report.json       the scenario's report
R/summary.txt       what the scenario printed
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
- `cargo run --release -p icechunk-fs-stress -- convert-scenario --root R`:
  the conversion scenario (there is no `just` target); any `fs-stress` binary
  runs it as `fs-stress convert-scenario --root R`.
- `just fs-stress-process-tests`: the ignored tests that spawn stress
  processes. `just test` runs the others.
- `just fs-stress-static`: the static binary for other hosts.
- `scripts/remote.sh`: see
  [Driving several hosts from one machine](#driving-several-hosts-from-one-machine).
- `vms/lab.sh`: a lab of libvirt VMs serving Lustre, BeeGFS, CephFS, NFS
  and GPFS to client VMs; see [its README](vms/README.md).

The scheduled CI runs `just fs-stress-process-tests` and short storage and
repository mode `just fs-stress` runs; none of the others run in CI.
