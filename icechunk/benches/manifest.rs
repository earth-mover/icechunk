#![allow(clippy::unwrap_used)]
// NOTE: All expensive setup must go INSIDE the bench_with_input / iter_custom closure,
// not before it. Criterion's filter (the CLI argument) is only checked inside
// bench_with_input. Any work done before that call runs unconditionally for every
// benchmark in the group, even when filtered out.

use std::collections::HashMap;
use std::error::Error;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use futures::{StreamExt, stream};
use icechunk::config::{ManifestConfig, ManifestPreloadConfig, ManifestSplittingConfig};
use icechunk::conflicts::detector::ConflictDetector;
use icechunk::format::manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef};
use icechunk::format::snapshot::{ArrayShape, DimensionName};
use icechunk::format::{ByteRange, ChunkIndices, Path};
#[cfg(feature = "logs")]
use icechunk::initialize_tracing;
use icechunk::repository::{Repository, VersionInfo};
use icechunk::session::{CommitMethod, Session, get_chunk};
use icechunk::storage::new_in_memory_storage;
use icechunk::{RepositoryConfig, Storage};
use tokio::runtime::Runtime;

#[derive(Clone, Copy)]
enum ChunkKind {
    Inline,
    Virtual,
}

impl std::fmt::Display for ChunkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkKind::Inline => write!(f, "inline"),
            ChunkKind::Virtual => write!(f, "virtual"),
        }
    }
}

async fn setup_repo(
    path: Path,
    shape: ArrayShape,
    dimension_names: Option<Vec<DimensionName>>,
    split_config: Option<ManifestSplittingConfig>,
) -> Result<Repository, Box<dyn Error>> {
    let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage().await?;

    let man_config = ManifestConfig {
        preload: Some(ManifestPreloadConfig {
            max_total_refs: None,
            preload_if: None,
            max_arrays_to_scan: None,
        }),
        splitting: split_config,
        virtual_chunk_location_compression: None,
    };
    let config =
        RepositoryConfig { manifest: Some(man_config), ..RepositoryConfig::default() };
    let repository =
        Repository::create(Some(config), storage, HashMap::new(), None, false).await?;

    let mut session = repository.writable_session("main").await?;

    let defn = Bytes::from_static(br#"{"this":"array"}"#);
    session.add_group(Path::root(), defn.clone()).await?;
    session.add_array(path, shape, dimension_names, defn.clone()).await?;
    session.commit("initialized", None).await?;

    Ok(repository)
}

async fn set_chunks(
    path: Path,
    session: &mut Session,
    chunks: Range<u32>,
    kind: ChunkKind,
) -> Result<(), Box<dyn Error>> {
    match kind {
        ChunkKind::Inline => {
            let bytes = Bytes::copy_from_slice(&42i8.to_be_bytes());
            for idx in chunks {
                let payload = session.get_chunk_writer().unwrap()(bytes.clone()).await?;
                session
                    .set_chunk_ref(path.clone(), ChunkIndices(vec![idx]), Some(payload))
                    .await?;
            }
        }
        ChunkKind::Virtual => {
            for idx in chunks {
                let payload = ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url(&format!(
                        "s3://bucket/chunk_{idx}"
                    ))?,
                    offset: 0,
                    length: 1,
                    checksum: None,
                });
                session
                    .set_chunk_ref(path.clone(), ChunkIndices(vec![idx]), Some(payload))
                    .await?;
            }
        }
    }
    Ok(())
}

/// Benchmarks setting of inline and virtual chunks
fn benchmark_set_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_chunks");

    let chunk_size = 1u32;
    let path: Path = "/temperature".try_into().unwrap();

    let rt = Runtime::new().unwrap();

    for kind in [ChunkKind::Inline, ChunkKind::Virtual] {
        for num_chunks in [1_000, 10_000, 100_000, 1_000_000] {
            group.throughput(Throughput::Elements(num_chunks as u64));
            group.bench_with_input(
                BenchmarkId::new(kind.to_string(), num_chunks),
                &num_chunks,
                |b, &num_chunks| {
                    b.iter_batched(
                        || {
                            let path = path.clone();
                            let shape = ArrayShape::new(vec![(
                                num_chunks.into(),
                                chunk_size.into(),
                            )])
                            .unwrap();
                            rt.block_on(async move {
                                let repo = setup_repo(path.clone(), shape, None, None)
                                    .await
                                    .unwrap();
                                repo.writable_session("main").await.unwrap()
                            })
                        },
                        |mut session| {
                            rt.block_on({
                                let path = path.clone();
                                async {
                                    set_chunks(path, &mut session, 0..num_chunks, kind)
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

/// Benchmarks getting of a random sequence of `nget` = 1000 chunks
/// from a million chunks split across 1-1000 manifests.
/// We always get the first and last chunk; the rest are randomly sampled.
///
/// The repo and chunks are created once, then `rewrite_manifests` is used
/// to re-split for each `num_manifests` value.
fn benchmark_get_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_chunks");

    let chunk_size = 1u32;
    let path: Path = "/temperature".try_into().unwrap();
    let rt = Runtime::new().unwrap();

    // grab 1000 chunks
    let nget = 1000;
    // form a total of a million chunks
    let num_chunks: u32 = 1_000_000;

    // One-time setup: create repo and write all chunks (no splitting).
    let mut storage_cache: Option<Arc<dyn Storage + Send + Sync>> = None;

    // split across this many manifests
    for num_manifests in [1u32, 10, 100, 1_000] {
        group.throughput(Throughput::Elements(num_manifests as u64));

        let split_size = num_chunks.div_ceil(num_manifests);
        let split_config = ManifestSplittingConfig::with_size(split_size);

        // Lazy init: write chunks once, then rewrite manifests for each split config.
        let storage = storage_cache.get_or_insert_with(|| {
            rt.block_on(async {
                let shape = ArrayShape::new(vec![(num_chunks.into(), chunk_size.into())])
                    .unwrap();
                let repo = setup_repo(path.clone(), shape, None, None).await.unwrap();
                let mut write_session = repo.writable_session("main").await.unwrap();
                set_chunks(
                    path.clone(),
                    &mut write_session,
                    0..num_chunks,
                    ChunkKind::Inline,
                )
                .await
                .unwrap();
                write_session.commit("data", None).await.unwrap();
                repo.storage().clone()
            })
        });

        // Re-open with the new splitting config and rewrite manifests.
        let session = rt.block_on(async {
            let man_config = ManifestConfig {
                preload: Some(ManifestPreloadConfig {
                    max_total_refs: None,
                    preload_if: None,
                    max_arrays_to_scan: None,
                }),
                splitting: Some(split_config),
                virtual_chunk_location_compression: None,
            };
            let config = RepositoryConfig {
                manifest: Some(man_config),
                ..RepositoryConfig::default()
            };
            let repo =
                Repository::open(Some(config), Arc::clone(storage), HashMap::new())
                    .await
                    .unwrap();
            let mut session = repo.writable_session("main").await.unwrap();
            session
                .rewrite_manifests("rewrite", None, CommitMethod::Amend)
                .await
                .unwrap();

            Arc::new(
                repo.readonly_session(&VersionInfo::BranchTipRef("main".into()))
                    .await
                    .unwrap(),
            )
        });

        group.bench_with_input(
            BenchmarkId::from_parameter(num_manifests),
            &num_manifests,
            |b, _| {
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
                                        get_chunk(
                                            session
                                                .get_chunk_reader(
                                                    &path,
                                                    &ChunkIndices(vec![idx as u32]),
                                                    &ByteRange::ALL,
                                                )
                                                .await
                                                .unwrap(),
                                        )
                                        .await
                                        .unwrap()
                                        .unwrap();
                                    }
                                })
                                .await
                        })
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

/// Benchmarks committing half a million chunks
/// split across 1-1000 manifests
fn benchmark_commit_split_manifests(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit_split_manifests");
    group.sample_size(20).sampling_mode(criterion::SamplingMode::Flat);

    let chunk_size = 1u32;

    let path: Path = "/temperature".try_into().unwrap();

    let rt = Runtime::new().unwrap();

    let num_chunks = 500_000u32;
    for kind in [ChunkKind::Inline, ChunkKind::Virtual] {
        for num_manifests in [1, 10, 100, 1_000] {
            let split_size = num_chunks.div_ceil(num_manifests);
            let split_config = ManifestSplittingConfig::with_size(split_size);
            group.throughput(Throughput::Elements(num_manifests as u64));
            group.bench_with_input(
                BenchmarkId::new(kind.to_string(), num_manifests),
                &num_chunks,
                |b, &num_chunks| {
                    b.iter_batched(
                        || {
                            let path = path.clone();
                            let shape = ArrayShape::new(vec![(
                                num_chunks.into(),
                                chunk_size.into(),
                            )])
                            .unwrap();
                            let split_config = split_config.clone();
                            rt.block_on(async move {
                                let repo = setup_repo(
                                    path.clone(),
                                    shape,
                                    None,
                                    Some(split_config),
                                )
                                .await
                                .unwrap();
                                let mut session =
                                    repo.writable_session("main").await.unwrap();
                                set_chunks(path, &mut session, 0..num_chunks, kind)
                                    .await
                                    .unwrap();
                                session
                            })
                        },
                        |mut session| {
                            rt.block_on(async {
                                session.commit("foo", None).await.unwrap();
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
    group.sample_size(10).sampling_mode(criterion::SamplingMode::Flat);

    let rt = Runtime::new().unwrap();

    let path: Path = "/temperature".try_into().unwrap();
    let chunk_size = 1u32;
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
                            ArrayShape::new(vec![(num_chunks.into(), chunk_size.into())])
                                .unwrap();
                        let repo = rt.block_on(async {
                            setup_repo(
                                path.clone(),
                                shape,
                                None,
                                Some(split_config.clone()),
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
                                )
                                .await
                                .unwrap();
                                session
                            });
                            let set_elapsed = start_set.elapsed();

                            let start_commit = std::time::Instant::now();
                            rt.block_on(async {
                                session.commit("commit", None).await.unwrap();
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
    #[cfg(feature = "logs")]
    initialize_tracing(None);
    let mut group = c.benchmark_group("commit_rebase_split_manifests");
    group.sample_size(10).sampling_mode(criterion::SamplingMode::Flat);

    let rt = Runtime::new().unwrap();

    let path: Path = "/temperature".try_into().unwrap();
    let chunk_size = 1u32;
    let num_chunks = 1_000_000u32;
    let num_manifests = 50u32;

    let split_size = num_chunks.div_ceil(num_manifests);
    let split_config = ManifestSplittingConfig::with_size(split_size);

    for kind in [ChunkKind::Inline, ChunkKind::Virtual] {
        group.bench_with_input(
            BenchmarkId::new("type", kind.to_string()),
            &kind,
            |b, &kind| {
                b.iter_custom(|_iters| {
                    // Fresh repo per sample
                    let shape =
                        ArrayShape::new(vec![(num_chunks.into(), chunk_size.into())])
                            .unwrap();
                    let repo = rt.block_on(async {
                        setup_repo(path.clone(), shape, None, Some(split_config.clone()))
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
                                .commit_rebasing(
                                    &ConflictDetector,
                                    10,
                                    "foo",
                                    None,
                                    |_| async {},
                                    |_| async {},
                                )
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
    group.finish();
}

criterion_group!(
    benches,
    benchmark_append_split_manifests,
    benchmark_commit_split_manifests,
    benchmark_commit_rebase_split_manifests,
    benchmark_set_chunks,
    benchmark_get_chunks,
);
criterion_main!(benches);

// TODO: open repo with large number of snapshots
// TODO: latency store
