//! The `list` subcommand: listing throughput, measured over a fixed duration.
//!
//! Strictly read only. The only requests it makes are listings, which is why it
//! is the one subcommand that accepts a real S3 target.

use std::{
    num::NonZeroU16,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{StreamExt as _, stream::BoxStream};
use icechunk::{
    Storage,
    asset_manager::AssetManager,
    format::format_constants::SpecVersionBin,
    repository::RepositoryResult,
    storage::{ListInfo, Settings, StorageResult, metering::MeteringStorage},
};

use crate::{
    BoxError,
    cli::{Kind, ListArgs, ListMode, Target},
    report, storage,
};

/// How the run ended, and what it listed.
struct Run {
    /// Keys listed during each elapsed second.
    timeline: Vec<u64>,
    keys: u64,
    elapsed: Duration,
    /// Why the run stopped, and the first listing error if there was one.
    note: String,
    /// Whether a listing failed, so the process can exit non-zero.
    failed: bool,
}

// ----------------------------------------------------------------- single mode

/// Shared by the listings of one metered run.
#[derive(Debug, Default)]
struct Counters {
    keys: AtomicU64,
    /// Set when `--max-keys` is reached; the listing stops at its next key.
    stop: AtomicBool,
}

impl Counters {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

/// Count the keys of one stream, without ever holding on to a key. Returns the
/// count and whether the stream drained (rather than being cut by `--max-keys`).
async fn drain(
    mut stream: BoxStream<'_, StorageResult<ListInfo<String>>>,
    counters: &Counters,
    max_keys: Option<u64>,
) -> Result<(u64, bool), BoxError> {
    let mut listed = 0u64;
    while let Some(item) = stream.next().await {
        item?;
        listed += 1;
        let total = counters.keys.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(max) = max_keys
            && total >= max
        {
            counters.stop.store(true, Ordering::Relaxed);
        }
        if counters.stopped() {
            return Ok((listed, false));
        }
    }
    Ok((listed, true))
}

/// `single`: one `list_objects` stream over the whole prefix, driven by one
/// task, with the meter seeing each key as it is yielded.
async fn run_single(
    metering: &MeteringStorage,
    settings: &Settings,
    args: &ListArgs,
) -> Run {
    let counters = Counters::default();
    metering.reset();
    let started = Instant::now();
    let outcome = tokio::time::timeout(Duration::from_secs(args.duration_secs), async {
        let prefix = format!("{}/", args.kind.prefix());
        let stream = metering.list_objects(settings, &prefix).await?;
        drain(stream, &counters, args.max_keys).await?;
        Ok::<(), BoxError>(())
    })
    .await;
    let elapsed = started.elapsed();

    let (note, failed) = match outcome {
        // Dropping the stream abandons the in-flight request, which is fine
        // for a measurement that only reads.
        Err(_) => ("stopped at the --duration-secs limit".to_string(), false),
        Ok(Ok(())) if counters.stopped() => ("stopped at --max-keys".to_string(), false),
        Ok(Ok(())) => ("listing drained before the duration limit".to_string(), false),
        Ok(Err(err)) => (format!("listing failed: {err}"), true),
    };
    let report = metering.snapshot();
    Run {
        timeline: report.read_timeline.iter().map(|s| s.keys_listed).collect(),
        keys: counters.keys.load(Ordering::Relaxed),
        elapsed,
        note,
        failed,
    }
}

// ----------------------------------------------------------- asset manager mode

/// Keys counted locally before publishing them to the shared counter.
const FLUSH_EVERY: u64 = 1000;

#[derive(Debug, Default)]
struct TaskRun {
    keys: AtomicU64,
    stop: AtomicBool,
    /// The first listing error, if any.
    error: Mutex<Option<String>>,
}

impl TaskRun {
    fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    fn add(&self, keys: u64, max_keys: Option<u64>) {
        let total = self.keys.fetch_add(keys, Ordering::Relaxed) + keys;
        if let Some(max) = max_keys
            && total >= max
        {
            self.stop.store(true, Ordering::Relaxed);
        }
    }

    fn fail(&self, err: impl std::fmt::Display) {
        let mut slot = self.error.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(err.to_string());
        }
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Sample the shared counter once a second, as a per-second delta.
async fn tick(run: Arc<TaskRun>, timeline: Arc<Mutex<Vec<u64>>>) {
    let mut previous = 0u64;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let total = run.keys.load(Ordering::Relaxed);
        timeline.lock().unwrap_or_else(|p| p.into_inner()).push(total - previous);
        previous = total;
    }
}

/// Count the items of one `AssetManager` listing, publishing to the shared
/// counter once per [`FLUSH_EVERY`] items. Counting stays local so the shared
/// counter is touched once per batch instead of once per key.
async fn drain_infos<Id: std::fmt::Debug>(
    mut stream: BoxStream<'_, RepositoryResult<ListInfo<Id>>>,
    run: &TaskRun,
    max_keys: Option<u64>,
) {
    let mut local = 0u64;
    while let Some(item) = stream.next().await {
        if let Err(err) = item {
            run.add(local, max_keys);
            return run.fail(err);
        }
        local += 1;
        if local == FLUSH_EVERY {
            run.add(local, max_keys);
            local = 0;
            if run.stopped() {
                return;
            }
        }
    }
    run.add(local, max_keys);
}

/// `asset-manager`: the production listing path, over the raw storage. The
/// metering wrapper is deliberately skipped: at hundreds of thousands of keys
/// per second its per-key locks would be the bottleneck being measured.
async fn run_asset_manager(
    backend: &Arc<dyn Storage + Send + Sync>,
    settings: &Settings,
    args: &ListArgs,
) -> Run {
    let concurrency =
        NonZeroU16::new(u16::try_from(args.streams.max(1)).unwrap_or(u16::MAX))
            .unwrap_or(NonZeroU16::MIN);
    let am = AssetManager::new_no_cache(
        Arc::clone(backend),
        settings.clone(),
        SpecVersionBin::V2,
        1,
        100,
    );
    let run = Arc::new(TaskRun::default());
    let timeline = Arc::new(Mutex::new(Vec::new()));

    let started = Instant::now();
    let ticker = tokio::spawn(tick(Arc::clone(&run), Arc::clone(&timeline)));
    let drain = async {
        // every kind yields a different id type, so each drains on its own arm
        match args.kind {
            Kind::Chunks => match am.list_chunks_with_concurrency(concurrency).await {
                Ok(s) => drain_infos(s, &run, args.max_keys).await,
                Err(err) => run.fail(err),
            },
            Kind::Manifests => {
                match am.list_manifests_with_concurrency(concurrency).await {
                    Ok(s) => drain_infos(s, &run, args.max_keys).await,
                    Err(err) => run.fail(err),
                }
            }
            Kind::Snapshots => {
                match am.list_snapshots_with_concurrency(concurrency).await {
                    Ok(s) => drain_infos(s, &run, args.max_keys).await,
                    Err(err) => run.fail(err),
                }
            }
            Kind::TransactionLogs => {
                match am.list_transaction_logs_with_concurrency(concurrency).await {
                    Ok(s) => drain_infos(s, &run, args.max_keys).await,
                    Err(err) => run.fail(err),
                }
            }
        }
    };
    // dropping the stream at the deadline aborts its listing tasks, which is
    // fine for a measurement that only reads
    let drained = tokio::select! {
        () = drain => true,
        () = tokio::time::sleep(Duration::from_secs(args.duration_secs)) => false,
    };
    run.stop.store(true, Ordering::Relaxed);
    ticker.abort();
    let elapsed = started.elapsed();

    let error = run.error.lock().unwrap_or_else(|p| p.into_inner()).clone();
    let (note, failed) = match (error, drained) {
        (Some(err), _) => (format!("listing failed: {err}"), true),
        (None, true) if run.stopped() => ("stopped at --max-keys".to_string(), false),
        (None, true) => {
            ("the listing drained before the duration limit".to_string(), false)
        }
        (None, false) => ("stopped at the --duration-secs limit".to_string(), false),
    };
    Run {
        timeline: timeline.lock().unwrap_or_else(|p| p.into_inner()).clone(),
        keys: run.keys.load(Ordering::Relaxed),
        elapsed,
        note,
        failed,
    }
}

// ---------------------------------------------------------------------- report

fn median(sorted: &[u64]) -> f64 {
    match sorted.len() {
        0 => 0.0,
        n if n % 2 == 1 => sorted[n / 2] as f64,
        n => (sorted[n / 2 - 1] as f64 + sorted[n / 2] as f64) / 2.0,
    }
}

fn print_report(target: &str, credentials: &str, run: &Run) {
    println!();
    println!("{:>6} {:>12}", "second", "keys listed");
    for (second, keys) in run.timeline.iter().enumerate() {
        println!("{second:>6} {keys:>12}");
    }

    // The first second carries connection setup and the first round trips.
    let mut steady: Vec<u64> = run.timeline.iter().skip(1).copied().collect();
    steady.sort_unstable();
    println!();
    if steady.is_empty() {
        println!("keys/s after the first second: not enough seconds measured");
    } else {
        let sum: u64 = steady.iter().sum();
        println!(
            "keys/s over the {} seconds after the first: min {} median {:.0} max {} mean {:.0}",
            steady.len(),
            steady.first().copied().unwrap_or(0),
            median(&steady),
            steady.last().copied().unwrap_or(0),
            sum as f64 / steady.len() as f64,
        );
    }

    println!();
    println!("total keys listed: {}", run.keys);
    println!("elapsed: {:.2}s", run.elapsed.as_secs_f64());
    println!(
        "keys/s overall: {:.0}",
        run.keys as f64 / run.elapsed.as_secs_f64().max(1e-9)
    );
    println!("approximate pages (keys/1000): {:.1}", run.keys as f64 / 1000.0);

    println!();
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    println!("available cores: {cores}");
    match report::cpu_seconds() {
        Some((user, sys)) => {
            let busy = (user + sys) / run.elapsed.as_secs_f64().max(1e-9);
            println!("cpu time: {user:.1}s user, {sys:.1}s sys ({busy:.1} cores busy)");
        }
        None => println!("cpu time: n/a"),
    }

    println!();
    println!("target: {target}");
    println!("credentials: {credentials}");
}

pub(crate) async fn list(args: ListArgs) -> Result<(), BoxError> {
    let target = args.target()?;
    let (backend, target_label, credentials) = match &target {
        Target::Dataset(name) => (
            storage::rustfs_storage(name, storage::RUSTFS_PORT)?,
            format!("RustFS dataset {name}"),
            "local RustFS static credentials".to_string(),
        ),
        Target::S3 { bucket, prefix, region } => {
            let (backend, source) = storage::s3_storage(bucket, prefix, region)?;
            (backend, format!("s3://{bucket}/{prefix} ({region})"), source.to_string())
        }
    };
    println!(
        "list {} on {target_label} (mode={:?}, duration={}s)",
        args.kind.prefix(),
        args.mode,
        args.duration_secs
    );
    println!("credentials: {credentials}");

    let run = match args.mode {
        ListMode::AssetManager => {
            let settings = backend.default_settings().await?;
            run_asset_manager(&backend, &settings, &args).await
        }
        ListMode::Single => {
            let metering = MeteringStorage::new(backend);
            let settings = metering.default_settings().await?;
            run_single(&metering, &settings, &args).await
        }
    };

    println!("{}", run.note);
    print_report(&target_label, &credentials, &run);
    if run.failed { Err(run.note.into()) } else { Ok(()) }
}
