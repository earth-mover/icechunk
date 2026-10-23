# Testing `filesystem_storage` on your filesystem

Thank you for helping us test Icechunk on your filesystem. This guide takes
you from downloading the test to sending us the results. The
[README](README.md) has the full details, but you shouldn't need them.

## What this is

`fs-stress` is the test we use to decide which filesystems
[`filesystem_storage`](https://icechunk.io/en/latest/guides/storage/#filesystem-storage)
is safe on. It starts many processes, on one or more hosts, that all update
one Icechunk repository on your filesystem at the same time. While they work,
it kills and pauses them at the worst moments. At the end it checks that no
update was lost and that the repository recovers and cleans up.

## What it costs

- **Time.** About 15 minutes of running, plus a few minutes to start and to
  check the result.
- **Disk.** Up to about 10 GB of free space on the filesystem under test, for
  the whole qualification; usually under 1 GB on network filesystems.
- **Load on the file server.** Each host runs 8 processes that do many small
  metadata operations: creates, renames, hard links, fsyncs and directory
  listings. Use a scratch directory, never one that holds real data.
- **The hosts.** The test needs little memory, but don't run it on hosts
  busy with other work, or two tests at once: slow hosts make its timing
  checks less meaningful.

## Get the binary

### Download it

Linux x86_64 and aarch64 binaries are attached to every
[Icechunk release](https://github.com/earth-mover/icechunk/releases), from
Icechunk 2.4.0 on. They are static, so they run on any Linux distribution.
Set `version` to the tag of the Icechunk release you plan to use:

```sh
version=v2.4.0
asset=fs-stress-$version-$(uname -m)-linux-musl
base=https://github.com/earth-mover/icechunk/releases/download/$version
curl -fL -o fs-stress "$base/$asset"
curl -fLO "$base/SHA256SUMS"
awk -v a="$asset" '$2 == a { print $1 "  fs-stress" }' SHA256SUMS | sha256sum --check
chmod +x fs-stress
./fs-stress --version
```

`sha256sum` must print `fs-stress: OK`. With the
[GitHub CLI](https://cli.github.com) you can also check that our release
workflow built it:

```sh
gh attestation verify fs-stress --repo earth-mover/icechunk
```

`--version` prints the Icechunk version and the commit it was built from,
for example `fs-stress (Icechunk 2.4.0, commit 0123abc…)`. The commands
below, and the ones the test prints, write `fs-stress`: put it in a directory
on your `PATH`, or type its path instead.

We sometimes publish test builds as pre-releases tagged `fs-stress-…`, for
example `fs-stress-2.4.0-rc1` with the binary
`fs-stress-2.4.0-rc1-x86_64-linux-musl`. Use one only if we ask you to.

### Or build it yourself

First set up the Icechunk development environment, as described in
[Setting up your development environment](https://icechunk.io/en/latest/reference/contributing/#setting-up-your-development-environment).
Then, on every host, from the same commit:

```sh
git clone https://github.com/earth-mover/icechunk
cd icechunk
cargo build --release -p icechunk-fs-stress
```

The binary is `target/release/fs-stress`.

To build instead one static binary that you copy to every x86_64 host, you
also need Rust's `x86_64-unknown-linux-musl` target and a musl C compiler (with
rustup on Debian or Ubuntu: `rustup target add x86_64-unknown-linux-musl` and
`apt install musl-tools`). Then run:

```sh
CC_x86_64_unknown_linux_musl=musl-gcc just fs-stress-static
```

It prints the binary's path.

## Run it on one host

Pick a path on the filesystem you want to test. It must not exist yet, or be
an empty directory:

```sh
fs-stress qualify run --root /path/on/your/fs/fs-stress-1
```

We call this path `R` below. Every 10 seconds the test prints how far it
got and about how long is left. At the end it prints the result. The exit
code is 0 for a pass, 1 for a failure, and 3 for an error or a provisional
result.

Then, whether it passed or failed, send us the results. In a directory
outside `R`, run:

```sh
fs-stress bundle --root /path/on/your/fs/fs-stress-1
```

and send the file it writes as described in
[Send us the results](#send-us-the-results).

## Run it on several hosts

This is the real test for a shared filesystem: each host sees the others'
changes only through the filesystem. We recommend at least 3 hosts; the
examples use 5. You need:

- the filesystem mounted on every host (the path may differ between hosts:
  each command takes `R` as that host sees it);
- the `fs-stress` binary on every host;
- clocks roughly in sync, as NTP or chrony keeps them. A few seconds of
  difference is fine, and the result notes it. If you aren't sure, run the
  test anyway and tell us when you send the results.

1. **On one host**, create the root, saying how many hosts take part. It
   returns at once and prints the next steps:

   ```sh
   fs-stress qualify init --root /path/on/your/fs/fs-stress-1 --hosts 5
   ```

2. **On every host**, start the workers. Each waits until every host has
   started, so you can start them one by one:

   ```sh
   fs-stress qualify workers --root /path/on/your/fs/fs-stress-1
   ```

   With ssh, from one machine:

   ```sh
   for h in host1 host2 host3 host4 host5; do
     {
       ssh -n $h /path/to/fs-stress qualify workers --root /path/on/your/fs/fs-stress-1 ||
         echo "[$h] failed with exit code $?"
     } &
   done
   wait
   ```

   Every line starts with the name of the host that printed it. `wait`
   returns once every host has stopped. Each host that ran prints `This host
   is done` at the end, and a host that failed prints `[host] failed with
   exit code N`. 255 means ssh itself failed; the other codes are below.

   With Slurm, one task per node:

   ```sh
   srun -N 5 --ntasks-per-node=1 --time=1:00:00 /path/to/fs-stress qualify workers --root /path/on/your/fs/fs-stress-1
   ```

   Add your cluster's partition and account options if it needs them, and a
   time limit of at least an hour.

3. **On one host**, once every host's `qualify workers` has exited, check the
   result:

   ```sh
   fs-stress qualify verify --root /path/on/your/fs/fs-stress-1
   ```

4. **On that host**, whether the test passed or failed, send us the results.
   In a directory outside `R`, run:

   ```sh
   fs-stress bundle --root /path/on/your/fs/fs-stress-1
   ```

   and send the file it writes as described in
   [Send us the results](#send-us-the-results).

The test has two runs, one after the other, and the hosts wait for each other
before each. The first run starts once every host has registered. The second
starts once every host has registered for it too, or 10 minutes after the
first one did, without the hosts still missing.

`qualify workers` exits with 0, with 2 if this host came too late for a run,
and with 1 if a run hit an error. `qualify verify` exits like `qualify run`.

### Options

`qualify init` and `qualify run` take two optional settings. Please leave
everything else as it is: the test has to be the same everywhere for us to
compare results.

- `--workers N`: processes per host (default 8). Change it only if a host
  can't run 8.
- `--max-backoff 1s`: how long the second run's processes wait, at most,
  before they retry an update that lost a race (default `100ms`). A longer wait
  means less load on the file server and fewer races.

## Reading the result

The first line of the result is one of:

- **`PASS`**: both runs passed.
- **`FAIL`**: a run found problems. That doesn't by itself mean your
  filesystem is unsafe: some problems need our review first. Please send us
  the results.
- **`PROVISIONAL`**: some hosts hadn't finished. Run `fs-stress qualify verify
  --root R` again once they have. If a host stopped for good, send us the
  results anyway.
- **`ERROR`**: the test itself hit an error, so the result says nothing about
  your filesystem. Please send us the results.

Then come a line for each run, any notes, and what to do next. The full
report, with each run's details, is in `R/summary.txt`.

## Send us the results

Whether it passed or failed, your results help us. On a host that has the
filesystem mounted, in a directory outside `R`, run:

```sh
fs-stress bundle --root /path/on/your/fs/fs-stress-1
```

It writes one file in the current directory, named
`fs-stress-pass-<time>.tar.gz` or `fs-stress-fail-<time>.tar.gz`, and prints
its name and size.

Send the file in the #icechunk channel of the
[Earthmover community Slack](https://join.slack.com/t/earthmover-community/shared_invite/zt-2cwje92ir-xU3CfdG8BI~4CJOJy~sceQ),
or in a direct message to an Icechunk developer there.
Or open an [issue](https://github.com/earth-mover/icechunk/issues) **without
attaching the file**, and we'll contact you with a private way to send it.

Please also tell us:

- the filesystem product and its version, on the servers and the clients;
- how the servers are set up (how many, what storage behind them, failover);
- mount options, if you changed them from the defaults;
- anything else that was running on the file server or the hosts during the
  test.

Don't worry if you don't know some of this: tell us what you can. Any
information helps more than none.

**What the file contains.** The test's own files: its settings, logs and
reports. It also contains your host names, kernel versions, the filesystem
type, the path of `R`, and each host's mount line, which includes the file
server's name and the export path. If the test didn't pass, it also has the
test repository's small metadata files and a list of its files. It contains
no other data of yours. `bundle` removes nothing. If you'd rather not share
these details publicly, send the file in a direct message to an Icechunk
developer on Slack, or open an issue without it and we'll contact you with a
private way to send it.

## Clean up

Once you have the bundle, delete the root:

```sh
rm -rf /path/on/your/fs/fs-stress-1
```

The test writes nothing outside `R` and needs no special privileges.

## More detail

The [README](README.md) covers everything else: the single runs behind
`qualify`, all their settings, the checks, and how to read a report line by
line.
