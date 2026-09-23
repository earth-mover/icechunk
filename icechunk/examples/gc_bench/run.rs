//! The `gc` and `stats` subcommands.

use std::{
    collections::HashMap,
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
    time::Instant,
};

use chrono::{TimeDelta, Utc};
use icechunk::{
    Repository, Storage,
    asset_manager::AssetManager,
    ops::{
        gc::{GCConfig, garbage_collect},
        stats::repo_chunks_storage,
    },
    storage::metering::MeteringStorage,
};

use crate::{
    BoxError,
    cli::{GcArgs, NetArgs, StatsArgs, WalkArgs},
    report, storage, toxi,
};

struct Opened {
    am: Arc<AssetManager>,
    metering: Arc<MeteringStorage>,
    num_updates_per_file: u16,
}

/// Set up toxics if requested, wrap storage in the meter, open the repo.
async fn open(name: &str, net: &NetArgs) -> Result<Opened, BoxError> {
    let port = if toxi::enabled(net) {
        toxi::setup(net).await?;
        storage::TOXIPROXY_PORT
    } else {
        storage::RUSTFS_PORT
    };
    let backend = storage::rustfs_storage(name, port)?;
    let metering = Arc::new(MeteringStorage::new(backend));
    let as_storage: Arc<dyn Storage + Send + Sync> =
        Arc::<MeteringStorage>::clone(&metering);
    let repo = Repository::open(None, as_storage, HashMap::new()).await?;
    Ok(Opened {
        am: Arc::clone(repo.asset_manager()),
        metering,
        num_updates_per_file: repo.config().num_updates_per_repo_info_file(),
    })
}

fn non_zero_u16(v: u16, flag: &str) -> Result<NonZeroU16, BoxError> {
    NonZeroU16::new(v).ok_or_else(|| format!("{flag} must be > 0").into())
}

fn walk(
    w: &WalkArgs,
) -> Result<(NonZeroU16, NonZeroUsize, NonZeroUsize, NonZeroU16), BoxError> {
    Ok((
        non_zero_u16(w.max_snapshots_in_memory, "--max-snapshots-in-memory")?,
        NonZeroUsize::new(w.max_manifest_mem_bytes)
            .ok_or("--max-manifest-mem-bytes must be > 0")?,
        NonZeroUsize::new(w.max_decoded_manifest_mem_bytes)
            .ok_or("--max-decoded-manifest-mem-bytes must be > 0")?,
        non_zero_u16(
            w.max_concurrent_manifest_fetches,
            "--max-concurrent-manifest-fetches",
        )?,
    ))
}

async fn finish(
    started: Instant,
    phases: &report::PhaseTimes,
    opened: &Opened,
    net: &NetArgs,
) {
    report::print_report(started.elapsed(), phases, &opened.metering.snapshot());
    if toxi::enabled(net) {
        toxi::teardown().await;
    }
}

pub(crate) async fn gc(args: GcArgs) -> Result<(), BoxError> {
    let phases = report::install_tracing();
    let opened = open(&args.name, &args.net).await?;
    let (snaps, mem, decoded_mem, fetches) = walk(&args.walk)?;
    let cutoff = Utc::now() - TimeDelta::seconds(args.cutoff_age_secs as i64);
    let deletes = non_zero_u16(args.max_concurrent_deletes, "--max-concurrent-deletes")?;
    let delete_failures = non_zero_u16(
        args.max_consecutive_delete_failures,
        "--max-consecutive-delete-failures",
    )?;
    let config = GCConfig::clean_all(
        cutoff,
        cutoff,
        None,
        snaps,
        mem,
        decoded_mem,
        fetches,
        deletes,
        delete_failures,
        args.max_concurrent_listings
            .map(|n| non_zero_u16(n, "--max-concurrent-listings"))
            .transpose()?,
        !args.delete,
    );
    println!(
        "gc on {} (dry_run={}, cutoff={})",
        args.name,
        !args.delete,
        cutoff.to_rfc3339()
    );

    // drop what `Repository::open` metered, so the tables below describe the
    // operation alone
    opened.metering.reset();
    let started = Instant::now();
    let result = garbage_collect(
        Arc::clone(&opened.am),
        &config,
        None,
        opened.num_updates_per_file,
    )
    .await;
    match &result {
        Ok(summary) => println!("{summary:#?}"),
        Err(err) => eprintln!("GC failed: {err}"),
    }
    finish(started, &phases, &opened, &args.net).await;
    result.map(|_| ()).map_err(Into::into)
}

pub(crate) async fn stats(args: StatsArgs) -> Result<(), BoxError> {
    let phases = report::install_tracing();
    let opened = open(&args.name, &args.net).await?;
    let (snaps, mem, decoded_mem, fetches) = walk(&args.walk)?;
    println!("stats on {}", args.name);

    opened.metering.reset();
    let started = Instant::now();
    let result =
        repo_chunks_storage(Arc::clone(&opened.am), snaps, mem, decoded_mem, fetches)
            .await;
    match &result {
        Ok(stats) => println!("{stats:#?}"),
        Err(err) => eprintln!("stats failed: {err}"),
    }
    finish(started, &phases, &opened, &args.net).await;
    result.map(|_| ()).map_err(Into::into)
}
