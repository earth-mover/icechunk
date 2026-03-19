#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_arguments)]

use chrono::{DateTime, TimeDelta, Utc};
use icechunk::{
    Storage,
    asset_manager::AssetManager,
    format::{
        IcechunkFormatError, REPO_INFO_FILE_PATH, SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::{RepoAvailability, RepoInfo, RepoStatus, UpdateInfo, UpdateType},
        snapshot::SnapshotInfo,
    },
    new_in_memory_storage, storage,
};
use rand::{Rng, RngExt};
use std::{sync::Arc, time::Instant};
use tokio::io::AsyncReadExt;

const NUM_SNAPSHOTS: usize = 10_000;
const S3_STANDARD_PER_GB_MONTH: f64 = 0.023;
const NUM_BRANCHES: usize = 100;
const NUM_TAGS: usize = 100;
const SAMPLE_STEP: usize = 200;

type UpdateTuple<'a> =
    Result<(UpdateType, DateTime<Utc>, Option<&'a str>), IcechunkFormatError>;

fn random_message(rng: &mut impl Rng) -> String {
    let len = rng.random_range(50..=300);
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..62u8);
            match idx {
                0..26 => (b'a' + idx) as char,
                26..52 => (b'A' + idx - 26) as char,
                _ => (b'0' + idx - 52) as char,
            }
        })
        .collect()
}

fn gib(bytes: f64) -> f64 {
    bytes / (1024.0 * 1024.0 * 1024.0)
}

struct SizeResult {
    raw_size: usize,
    compressed_size: usize,
    fetch_time: std::time::Duration,
}

async fn measure_size(
    tags: &[(&str, SnapshotId)],
    branches: &[(&str, SnapshotId)],
    snapshots: Vec<SnapshotInfo>,
    latest_update: UpdateType,
    latest_time: DateTime<Utc>,
    previous_updates: Vec<UpdateTuple<'_>>,
    num_updates_per_file: u16,
    status: &RepoStatus,
) -> Result<SizeResult, Box<dyn std::error::Error>> {
    let repo_info = RepoInfo::new(
        SpecVersionBin::V2dot0,
        tags.to_vec(),
        branches.to_vec(),
        std::iter::empty::<&str>(),
        snapshots,
        &Default::default(),
        UpdateInfo {
            update_type: latest_update,
            update_time: latest_time,
            previous_updates: previous_updates.into_iter(),
        },
        None,
        num_updates_per_file,
        None,
        None,
        None::<std::iter::Empty<u16>>,
        None::<std::iter::Empty<u16>>,
        status,
    )?;
    let raw_size = repo_info.bytes().len();

    let stor: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;
    let settings = storage::Settings::default();
    let am = AssetManager::new_no_cache(
        Arc::clone(&stor),
        settings.clone(),
        SpecVersionBin::V2dot0,
        3,
        16,
    );
    am.create_repo_info(Arc::new(repo_info)).await?;

    let (mut reader, _) = stor.get_object(&settings, REPO_INFO_FILE_PATH, None).await?;
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await?;
    let compressed_size = buf.len();

    let t_fetch = Instant::now();
    let _ = am.fetch_repo_info().await?;
    let fetch_time = t_fetch.elapsed();

    Ok(SizeResult { raw_size, compressed_size, fetch_time })
}

struct SamplePoint {
    snap_count: usize,
    raw_size: usize,
    compressed_size: usize,
    fetch_time: std::time::Duration,
}

/// Estimate cumulative storage by linearly interpolating between sample points.
///
/// Between consecutive samples (prev, cur), there are `step` repo info objects.
/// Object k (1..=step) has estimated size: prev_size + (cur_size - prev_size) * k / step.
/// Sum over k=1..=step: step * prev_size + delta * (step + 1) / 2.
fn interpolate_cumulative(samples: &[SamplePoint]) -> (f64, f64) {
    let mut total_raw: f64 = 0.0;
    let mut total_compressed: f64 = 0.0;

    for (i, cur) in samples.iter().enumerate() {
        let (prev_count, prev_raw, prev_comp) = if i == 0 {
            (0usize, 0.0, 0.0)
        } else {
            let p = &samples[i - 1];
            (p.snap_count, p.raw_size as f64, p.compressed_size as f64)
        };

        let n = (cur.snap_count - prev_count) as f64;
        let raw_delta = cur.raw_size as f64 - prev_raw;
        let comp_delta = cur.compressed_size as f64 - prev_comp;

        total_raw += n * prev_raw + raw_delta * (n + 1.0) / 2.0;
        total_compressed += n * prev_comp + comp_delta * (n + 1.0) / 2.0;
    }

    (total_raw, total_compressed)
}

fn print_table_header() {
    println!(
        "{:>10}  {:>14}  {:>14}  {:>8}",
        "Snapshots", "Raw (bytes)", "Compressed", "Ratio"
    );
    println!("{}", "-".repeat(52));
}

fn print_table_row(s: &SamplePoint) {
    let ratio = s.compressed_size as f64 / s.raw_size as f64 * 100.0;
    println!(
        "{:>10}  {:>14}  {:>14}  {:>7.1}%",
        s.snap_count, s.raw_size, s.compressed_size, ratio
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = rand::rng();
    let base_time = Utc::now();
    let mut time = base_time;
    let mut next_time = || {
        time += TimeDelta::microseconds(1);
        time
    };

    // Generate all snapshots upfront as a linear chain
    println!("Generating {NUM_SNAPSHOTS} snapshots...");
    // Generate IDs pre-sorted so snapshots[..count] is always a valid prefix
    // with intact parent chains.
    let mut ids: Vec<SnapshotId> =
        (0..NUM_SNAPSHOTS).map(|_| SnapshotId::random()).collect();
    ids.sort_by(|a, b| a.0.cmp(&b.0));

    let mut snapshots: Vec<SnapshotInfo> = Vec::with_capacity(NUM_SNAPSHOTS);
    for (i, id) in ids.into_iter().enumerate() {
        let parent_id = if i == 0 { None } else { Some(snapshots[i - 1].id.clone()) };
        snapshots.push(SnapshotInfo {
            id,
            parent_id,
            flushed_at: next_time(),
            message: random_message(&mut rng),
            metadata: Default::default(),
        });
    }
    // Sorted IDs matching the sorted snapshots order — used for refs and ops
    let sorted_ids: Vec<SnapshotId> = snapshots.iter().map(|s| s.id.clone()).collect();
    println!("  done");

    let branch_names: Vec<String> =
        (0..NUM_BRANCHES).map(|i| format!("branch-{i:04}")).collect();
    let tag_names: Vec<String> = (0..NUM_TAGS).map(|i| format!("tag-{i:04}")).collect();

    // Build branches and tags referencing IDs within a given subset
    let make_refs = |ids: &[SnapshotId],
                     names: &[String],
                     count: usize|
     -> Vec<(String, SnapshotId)> {
        let ref_at = |i: usize| (i + 1) * (ids.len() / (count + 1));
        names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), ids[ref_at(i)].clone()))
            .collect()
    };

    let status = RepoStatus {
        availability: RepoAvailability::Online,
        set_at: base_time,
        limited_availability_reason: None,
    };

    let sample_counts: Vec<usize> =
        (1..=(NUM_SNAPSHOTS / SAMPLE_STEP)).map(|i| i * SAMPLE_STEP).collect();

    // =========================================================
    //  Pass 1: WITH ops log
    // =========================================================
    println!("\n========================================");
    println!("  Pass 1: WITH ops log");
    println!("========================================\n");
    print_table_header();

    let mut samples_with: Vec<SamplePoint> = Vec::with_capacity(sample_counts.len());

    for &count in &sample_counts {
        let snaps = snapshots[..count].to_vec();
        let subset_ids = &sorted_ids[..count];

        // Branches and tags from this subset's IDs
        let branch_refs = make_refs(subset_ids, &branch_names, NUM_BRANCHES);
        let mut branches: Vec<(&str, SnapshotId)> =
            vec![("main", subset_ids.last().unwrap().clone())];
        branches.extend(branch_refs.iter().map(|(n, id)| (n.as_str(), id.clone())));
        let tag_refs = make_refs(subset_ids, &tag_names, NUM_TAGS);
        let tags: Vec<(&str, SnapshotId)> =
            tag_refs.iter().map(|(n, id)| (n.as_str(), id.clone())).collect();

        // Build ops: `count` commit ops + branch + tag creation ops
        let num_ops = count + NUM_BRANCHES + NUM_TAGS;
        let mut ops: Vec<(UpdateType, DateTime<Utc>, Option<&str>)> =
            Vec::with_capacity(num_ops);
        for id in subset_ids {
            ops.push((
                UpdateType::NewCommitUpdate {
                    branch: "main".to_string(),
                    new_snap_id: id.clone(),
                },
                next_time(),
                None,
            ));
        }
        for name in &branch_names {
            ops.push((
                UpdateType::BranchCreatedUpdate { name: name.clone() },
                next_time(),
                None,
            ));
        }
        for name in &tag_names {
            ops.push((
                UpdateType::TagCreatedUpdate { name: name.clone() },
                next_time(),
                None,
            ));
        }
        ops.reverse();

        let (latest_update, latest_time, _) = ops.remove(0);
        let previous_updates: Vec<UpdateTuple<'_>> = ops.into_iter().map(Ok).collect();

        let result = measure_size(
            &tags,
            &branches,
            snaps,
            latest_update,
            latest_time,
            previous_updates,
            (num_ops + 10) as u16,
            &status,
        )
        .await?;

        let sp = SamplePoint {
            snap_count: count,
            raw_size: result.raw_size,
            compressed_size: result.compressed_size,
            fetch_time: result.fetch_time,
        };
        print_table_row(&sp);
        samples_with.push(sp);
    }

    let (total_raw_with, total_comp_with) = interpolate_cumulative(&samples_with);

    println!();
    println!("Estimated cumulative storage across {NUM_SNAPSHOTS} commits (WITH ops):");
    println!("  Raw:        {:>10.2} GiB", gib(total_raw_with));
    println!("  Compressed: {:>10.2} GiB", gib(total_comp_with));

    // =========================================================
    //  Pass 2: WITHOUT ops log
    // =========================================================
    println!("\n========================================");
    println!("  Pass 2: WITHOUT ops log");
    println!("========================================\n");
    print_table_header();

    let mut samples_without: Vec<SamplePoint> = Vec::with_capacity(sample_counts.len());

    for &count in &sample_counts {
        let snaps = snapshots[..count].to_vec();
        let subset_ids = &sorted_ids[..count];

        let branch_refs = make_refs(subset_ids, &branch_names, NUM_BRANCHES);
        let mut branches: Vec<(&str, SnapshotId)> =
            vec![("main", subset_ids.last().unwrap().clone())];
        branches.extend(branch_refs.iter().map(|(n, id)| (n.as_str(), id.clone())));
        let tag_refs = make_refs(subset_ids, &tag_names, NUM_TAGS);
        let tags: Vec<(&str, SnapshotId)> =
            tag_refs.iter().map(|(n, id)| (n.as_str(), id.clone())).collect();

        let result = measure_size(
            &tags,
            &branches,
            snaps,
            UpdateType::RepoInitializedUpdate,
            next_time(),
            vec![],
            1,
            &status,
        )
        .await?;

        let sp = SamplePoint {
            snap_count: count,
            raw_size: result.raw_size,
            compressed_size: result.compressed_size,
            fetch_time: result.fetch_time,
        };
        print_table_row(&sp);
        samples_without.push(sp);
    }

    let (total_raw_without, total_comp_without) =
        interpolate_cumulative(&samples_without);

    println!();
    println!(
        "Estimated cumulative storage across {NUM_SNAPSHOTS} commits (WITHOUT ops):"
    );
    println!("  Raw:        {:>10.2} GiB", gib(total_raw_without));
    println!("  Compressed: {:>10.2} GiB", gib(total_comp_without));

    // =========================================================
    //  Comparison
    // =========================================================
    println!("\n============================================================");
    println!("                   CUMULATIVE COMPARISON");
    println!("============================================================\n");
    let gb = |bytes: f64| bytes / 1_000_000_000.0;
    let s3_cost = |bytes: f64| gb(bytes) * S3_STANDARD_PER_GB_MONTH;

    println!(
        "  {:>25}  {:>14}  {:>16}  {:>12}",
        "", "Raw (GiB)", "Compressed (GiB)", "S3 $/month"
    );
    println!(
        "  {:>25}  {:>14.2}  {:>16.2}  {:>12.2}",
        "WITH ops log",
        gib(total_raw_with),
        gib(total_comp_with),
        s3_cost(total_comp_with)
    );
    println!(
        "  {:>25}  {:>14.2}  {:>16.2}  {:>12.2}",
        "WITHOUT ops log",
        gib(total_raw_without),
        gib(total_comp_without),
        s3_cost(total_comp_without)
    );
    println!(
        "  {:>25}  {:>14.2}  {:>16.2}  {:>12.2}",
        "Ops log overhead",
        gib(total_raw_with - total_raw_without),
        gib(total_comp_with - total_comp_without),
        s3_cost(total_comp_with - total_comp_without)
    );

    let first_with = samples_with.first().unwrap();
    let last_with = samples_with.last().unwrap();
    let first_without = samples_without.first().unwrap();
    let last_without = samples_without.last().unwrap();

    println!(
        "\n  Single object sizes ({} snaps vs {} snaps):",
        SAMPLE_STEP, NUM_SNAPSHOTS
    );
    println!("  {:>25}  {:>18}  {:>18}", "", "First sample", "Last sample");
    println!(
        "  {:>25}  {:>16.2} MB  {:>16.2} MB",
        "WITH ops log",
        first_with.compressed_size as f64 / 1_000_000.0,
        last_with.compressed_size as f64 / 1_000_000.0
    );
    println!(
        "  {:>25}  {:>16.2} MB  {:>16.2} MB",
        "WITHOUT ops log",
        first_without.compressed_size as f64 / 1_000_000.0,
        last_without.compressed_size as f64 / 1_000_000.0
    );

    println!(
        "\n  Fetch (decompress) time ({} snaps vs {} snaps):",
        SAMPLE_STEP, NUM_SNAPSHOTS
    );
    println!("  {:>25}  {:>18}  {:>18}", "", "First sample", "Last sample");
    println!(
        "  {:>25}  {:>18?}  {:>18?}",
        "WITH ops log", first_with.fetch_time, last_with.fetch_time
    );
    println!(
        "  {:>25}  {:>18?}  {:>18?}",
        "WITHOUT ops log", first_without.fetch_time, last_without.fetch_time
    );

    Ok(())
}
