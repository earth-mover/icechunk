// NOTE: All expensive setup must go INSIDE the bench_with_input / iter_custom closure,
// not before it. Criterion's filter (the CLI argument) is only checked inside
// bench_with_input. Any work done before that call runs unconditionally for every
// benchmark in the group, even when filtered out.
//
// Environment variables:
//   ICECHUNK_BENCH_LATENCY_MS=<ms>  — Use S3 (MinIO) behind toxiproxy instead
//     of in-memory storage. The value sets the downstream latency in ms (e.g. 100).
//     The latency toxic is applied only during the timed get_chunk iterations.
//     Requires `docker compose up -d`.

use std::cell::OnceCell;

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
};
use futures::{StreamExt, stream};
use icechunk::config::{ManifestConfig, ManifestSplittingConfig, RepositoryConfig};
use icechunk::conflicts::detector::ConflictDetector;
use icechunk::format::snapshot::ArrayShape;
use icechunk::format::{ByteRange, ChunkIndices, Path};
use icechunk::repository::VersionInfo;
use icechunk::session::get_chunk;
use tokio::runtime::Runtime;

use crate::helpers::{
    ChunkKind, StorageKind, add_latency_toxic, default_storage_kind,
    remove_latency_toxic, set_chunks, setup_repo, toxiproxy_latency_ms,
};

/// Benchmarks setting of inline and virtual chunks
fn benchmark_set_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_chunks");

    let path: Path = "/temperature".try_into().unwrap();

    let rt = Runtime::new().unwrap();

    for kind in [ChunkKind::Inline, ChunkKind::Virtual, ChunkKind::VirtualWithPrefixes] {
        for num_chunks in [1_000, 10_000, 100_000, 1_000_000] {
            group.throughput(Throughput::Elements(num_chunks as u64));
            group.bench_with_input(
                BenchmarkId::new(kind.to_string(), num_chunks),
                &num_chunks,
                |b, &num_chunks| {
                    b.iter_batched(
                        || {
                            let path = path.clone();
                            let shape =
                                ArrayShape::new(vec![(num_chunks.into(), num_chunks)])
                                    .unwrap();
                            rt.block_on(async move {
                                let (repo, _tmpdir) = setup_repo(
                                    path.clone(),
                                    shape,
                                    None,
                                    None,
                                    default_storage_kind(),
                                )
                                .await
                                .unwrap();
                                repo.writable_session("main").await.unwrap()
                            })
                        },
                        |mut session| {
                            rt.block_on({
                                let path = path.clone();
                                async {
                                    set_chunks(
                                        path,
                                        &mut session,
                                        0..num_chunks,
                                        kind,
                                        default_storage_kind(),
                                    )
                                    .await
                                    .unwrap();
                                }
                            })
                        },
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
    group.finish();
}

/// Benchmarks getting of a random sequence of `nget` = 500 chunks
/// from a million chunks split across 1 or 500 manifests, for both native,
/// virtual, and virtual-with-prefixes chunk kinds.
/// We always get the first and last chunk; the rest are randomly sampled.
///
/// Each benchmark variant creates its own repo independently inside
/// `bench_with_input`, so setup only runs for benchmarks that match
/// the CLI filter.
fn benchmark_get_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_chunks");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);

    let path: Path = "/temperature".try_into().unwrap();
    let rt = Runtime::new().unwrap();

    let nget = 500;
    let num_chunks: u32 = 1_000_000;

    for storage_kind in [StorageKind::InMemory, StorageKind::Rustfs] {
        for kind in
            [ChunkKind::Native, ChunkKind::Virtual, ChunkKind::VirtualWithPrefixes]
        {
            for num_manifests in [1u32, 500] {
                let session_cell: OnceCell<_> = OnceCell::new();

                group.bench_with_input(
                    BenchmarkId::new(format!("{storage_kind}/{kind}"), num_manifests),
                    &num_manifests,
                    |b, _| {
                        // Lazy setup: only runs if criterion's filter matches.
                        let session = session_cell.get_or_init(|| {
                            let split_config = ManifestSplittingConfig::with_size(
                                num_chunks.div_ceil(num_manifests),
                            );

                            rt.block_on(async {
                                // Create repo and write all chunks (no splitting).
                                let shape = ArrayShape::new(vec![(
                                    num_chunks.into(),
                                    num_chunks,
                                )])
                                .unwrap();
                                let (repo, _tmpdir) = setup_repo(
                                    path.clone(),
                                    shape,
                                    None,
                                    None,
                                    storage_kind,
                                )
                                .await
                                .unwrap();
                                let mut session =
                                    repo.writable_session("main").await.unwrap();
                                set_chunks(
                                    path.clone(),
                                    &mut session,
                                    0..num_chunks,
                                    kind,
                                    storage_kind,
                                )
                                .await
                                .unwrap();
                                session
                                    .commit("data")
                                    .max_concurrent_nodes(8)
                                    .execute()
                                    .await
                                    .unwrap();

                                // Re-open with the splitting config and rewrite
                                // manifests. Using reopen() preserves VCC auth.
                                let man_config = ManifestConfig {
                                    splitting: Some(split_config),
                                    ..Default::default()
                                };
                                let config = RepositoryConfig {
                                    manifest: Some(man_config),
                                    ..RepositoryConfig::default()
                                };
                                let repo = repo.reopen(Some(config), None).await.unwrap();
                                if num_manifests > 1 {
                                    let mut session =
                                        repo.writable_session("main").await.unwrap();
                                    session
                                        .commit("rewrite")
                                        .max_concurrent_nodes(8)
                                        .rewrite_manifests()
                                        .amend()
                                        .execute()
                                        .await
                                        .unwrap();
                                }
                                repo.readonly_session(&VersionInfo::BranchTipRef(
                                    "main".into(),
                                ))
                                .await
                                .unwrap()
                            })
                        });

                        // Enable the latency toxic only around the timed iterations.
                        if let Some(ms) = toxiproxy_latency_ms() {
                            rt.block_on(add_latency_toxic(ms)).unwrap();
                        }

                        b.iter_batched(
                            || {
                                // Generate a fresh random sample per iteration
                                let mut rng = rand::rng();
                                let mut get_chunks: Vec<usize> = Vec::with_capacity(nget);
                                get_chunks.push(0);
                                get_chunks.append(
                                    &mut rand::seq::index::sample(
                                        &mut rng,
                                        (num_chunks - 1) as usize,
                                        nget - 2,
                                    )
                                    .into_vec(),
                                );
                                get_chunks.push((num_chunks - 1) as usize);
                                get_chunks
                            },
                            |get_chunks| {
                                rt.block_on(async {
                                    stream::iter(get_chunks.into_iter())
                                        .for_each(|idx| {
                                            let session = session.clone();
                                            let path = path.clone();
                                            async move {
                                                let reader = session
                                                    .get_chunk_reader(
                                                        &path,
                                                        &ChunkIndices(vec![idx as u32]),
                                                        &ByteRange::ALL,
                                                    )
                                                    .await
                                                    .unwrap();
                                                get_chunk(reader).await.unwrap().unwrap();
                                            }
                                        })
                                        .await
                                })
                            },
                            BatchSize::SmallInput,
                        );

                        if toxiproxy_latency_ms().is_some() {
                            rt.block_on(remove_latency_toxic()).unwrap();
                        }
                    },
                );
            }
        }
    }
    group.finish();
}

/// Benchmarks committing half a million chunks
/// split across 1-1000 manifests
fn benchmark_commit_split_manifests(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_split_manifests");
    group.sample_size(20).sampling_mode(SamplingMode::Flat);

    let path: Path = "/temperature".try_into().unwrap();

    let rt = Runtime::new().unwrap();

    let num_chunks = 500_000u32;
    for kind in [ChunkKind::Inline, ChunkKind::Virtual] {
        for num_manifests in [1, 10, 100, 1_000] {
            let split_config =
                ManifestSplittingConfig::with_size(num_chunks.div_ceil(num_manifests));
            group.bench_with_input(
                BenchmarkId::new(kind.to_string(), num_manifests),
                &num_chunks,
                |b, &num_chunks| {
                    b.iter_batched(
                        || {
                            let path = path.clone();
                            let shape =
                                ArrayShape::new(vec![(num_chunks.into(), num_chunks)])
                                    .unwrap();
                            let split_config = split_config.clone();
                            rt.block_on(async move {
                                let (repo, _tmpdir) = setup_repo(
                                    path.clone(),
                                    shape,
                                    None,
                                    Some(split_config),
                                    default_storage_kind(),
                                )
                                .await
                                .unwrap();
                                let mut session =
                                    repo.writable_session("main").await.unwrap();
                                set_chunks(
                                    path,
                                    &mut session,
                                    0..num_chunks,
                                    kind,
                                    default_storage_kind(),
                                )
                                .await
                                .unwrap();
                                session
                            })
                        },
                        |mut session| {
                            rt.block_on(async {
                                session
                                    .commit("foo")
                                    .max_concurrent_nodes(8)
                                    .execute()
                                    .await
                                    .unwrap();
                            })
                        },
                        // Make sure we run the set up before every iteration
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
    group.finish();
}

/// Benchmarks committing half a million chunks
/// by sequential appending such that each commit is a new manifest.
/// The sequential repeated commit really stresses `get_existing_node` and flatbuffer decoding.
fn benchmark_append_split_manifests(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_split_manifests");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);

    let rt = Runtime::new().unwrap();

    let path: Path = "/temperature".try_into().unwrap();
    let num_chunks = 500_000u32;
    let num_manifests = 50u32;

    let split_size = num_chunks.div_ceil(num_manifests);
    let split_config = ManifestSplittingConfig::with_size(split_size);

    for kind in [ChunkKind::Inline, ChunkKind::Virtual] {
        for phase in ["set", "commit"] {
            group.bench_with_input(
                BenchmarkId::new(phase, kind.to_string()),
                &kind,
                |b, &kind| {
                    b.iter_custom(|_iters| {
                        // Fresh repo per sample
                        let shape =
                            ArrayShape::new(vec![(num_chunks.into(), num_chunks)])
                                .unwrap();
                        let (repo, _tmpdir) = rt.block_on(async {
                            setup_repo(
                                path.clone(),
                                shape,
                                None,
                                Some(split_config.clone()),
                                default_storage_kind(),
                            )
                            .await
                            .unwrap()
                        });

                        let mut total = std::time::Duration::ZERO;
                        for batch in 0..num_manifests {
                            let start_set = std::time::Instant::now();
                            let mut session = rt.block_on(async {
                                let mut session =
                                    repo.writable_session("main").await.unwrap();
                                set_chunks(
                                    path.clone(),
                                    &mut session,
                                    batch * split_size..(batch + 1) * split_size,
                                    kind,
                                    default_storage_kind(),
                                )
                                .await
                                .unwrap();
                                session
                            });
                            let set_elapsed = start_set.elapsed();

                            let start_commit = std::time::Instant::now();
                            rt.block_on(async {
                                session
                                    .commit("commit")
                                    .max_concurrent_nodes(8)
                                    .execute()
                                    .await
                                    .unwrap();
                            });
                            let commit_elapsed = start_commit.elapsed();

                            total +=
                                if phase == "set" { set_elapsed } else { commit_elapsed };
                        }
                        total
                    })
                },
            );
        }
    }
    group.finish();
}

/// Benchmarks committing a million chunks with rebasing.
/// All sessions are opened from the same branch tip, then committed
/// sequentially — the first is a fast-forward, the rest must rebase.
fn benchmark_commit_rebase_split_manifests(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_rebase_split_manifests");
    group.sample_size(10).sampling_mode(SamplingMode::Flat);

    let rt = Runtime::new().unwrap();

    let path: Path = "/temperature".try_into().unwrap();
    let num_chunks = 1_000_000u32;
    let num_manifests = 50u32;

    let split_size = num_chunks.div_ceil(num_manifests);
    let split_config = ManifestSplittingConfig::with_size(split_size);

    for storage_kind in [StorageKind::InMemory, StorageKind::Rustfs] {
        for kind in [ChunkKind::Inline, ChunkKind::Virtual] {
            group.bench_with_input(
                BenchmarkId::new(format!("{storage_kind}/{kind}"), num_manifests),
                &kind,
                |b, &kind| {
                    b.iter_custom(|_iters| {
                        // Fresh repo per sample
                        let shape =
                            ArrayShape::new(vec![(num_chunks.into(), num_chunks)])
                                .unwrap();
                        let (repo, _tmpdir) = rt.block_on(async {
                            setup_repo(
                                path.clone(),
                                shape,
                                None,
                                Some(split_config.clone()),
                                storage_kind,
                            )
                            .await
                            .unwrap()
                        });

                        // Create all sessions from the same branch tip
                        let sessions: Vec<_> = (0..num_manifests)
                            .map(|batch| {
                                rt.block_on(async {
                                    let mut session =
                                        repo.writable_session("main").await.unwrap();
                                    set_chunks(
                                        path.clone(),
                                        &mut session,
                                        batch * split_size..(batch + 1) * split_size,
                                        kind,
                                        storage_kind,
                                    )
                                    .await
                                    .unwrap();
                                    session
                                })
                            })
                            .collect();

                        let mut total = std::time::Duration::ZERO;
                        let start = std::time::Instant::now();
                        rt.block_on(async {
                            for mut session in sessions {
                                session
                                    .commit("foo")
                                    .max_concurrent_nodes(8)
                                    .rebase(&ConflictDetector, 10)
                                    .execute()
                                    .await
                                    .unwrap();
                            }
                        });
                        total += start.elapsed();
                        total
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(
    manifest_benches,
    benchmark_append_split_manifests,
    benchmark_commit_split_manifests,
    benchmark_commit_rebase_split_manifests,
    benchmark_set_chunks,
    benchmark_get_chunks,
);

// TODO: open repo with large number of snapshots
// TODO: IFS style rebasing workload
