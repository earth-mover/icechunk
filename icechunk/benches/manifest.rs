#![allow(clippy::unwrap_used)]
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
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
    criterion_main,
};
use futures::{StreamExt, stream};
use icechunk::ObjectStoreConfig;
use icechunk::config::{
    CachingConfig, ManifestConfig, ManifestPreloadCondition, ManifestPreloadConfig,
    ManifestSplittingConfig, S3Credentials, S3Options, S3StaticCredentials,
};
use icechunk::conflicts::detector::ConflictDetector;
use icechunk::format::manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef};
use icechunk::format::snapshot::{ArrayShape, DimensionName};
use icechunk::format::{ByteRange, ChunkIndices, Path};
#[cfg(feature = "logs")]
use icechunk::initialize_tracing;
use icechunk::new_s3_storage;
use icechunk::repository::{Repository, VersionInfo};
use icechunk::session::{CommitMethod, Session, get_chunk};
use icechunk::storage::new_in_memory_storage;
use icechunk::virtual_chunks::VirtualChunkContainer;
use icechunk::{RepositoryConfig, Storage};
use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};
use object_store::ObjectStoreExt;
use rand::RngExt;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const RUSTFS_PORT: u16 = 9000;
const TOXIPROXY_PORT: u16 = 9002;
const TOXIPROXY_PROXY_NAME: &str = "bench-latency";
const NUM_VCCS: usize = 20;
const CHUNK_SIZE_BYTES: u64 = 1024 * 1024;
const VCC_S3_BASE: &str = "s3://testbucket/bench-vcc/";
const VCC_LOCAL_DIR: &str = "/tmp/icechunk-bench-vcc";

#[derive(Clone, Copy)]
enum ChunkKind {
    Inline,
    Native,
    Virtual,
    VirtualWithPrefixes,
}

impl std::fmt::Display for ChunkKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkKind::Inline => write!(f, "inline"),
            ChunkKind::Native => write!(f, "native"),
            ChunkKind::Virtual => write!(f, "virtual"),
            ChunkKind::VirtualWithPrefixes => {
                write!(f, "virtual-{}-prefixes", NUM_VCCS)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum StorageKind {
    InMemory,
    Rustfs,
}

impl std::fmt::Display for StorageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageKind::InMemory => write!(f, "memory"),
            StorageKind::Rustfs => write!(f, "rustfs"),
        }
    }
}

fn default_storage_kind() -> StorageKind {
    match toxiproxy_latency_ms() {
        Some(_) => StorageKind::Rustfs,
        None => StorageKind::InMemory,
    }
}

/// Parse `ICECHUNK_BENCH_LATENCY_MS` env var; returns `Some(ms)` if set.
fn toxiproxy_latency_ms() -> Option<u64> {
    #[allow(clippy::expect_used)]
    std::env::var("ICECHUNK_BENCH_LATENCY_MS")
        .ok()
        .map(|v| v.parse::<u64>().expect("ICECHUNK_BENCH_LATENCY_MS must be an integer"))
}

/// Set up toxiproxy: create a proxy on `TOXIPROXY_PORT` forwarding to RustFS at
/// `rustfs:9000`. Does NOT add any toxics — those are managed separately so that
/// latency is only injected during the timed portion of benchmarks.
async fn setup_toxiproxy() -> Result<(), Box<dyn Error>> {
    let client = Client::new("http://localhost:8474");

    // Delete any proxy already bound to this port (leftover from a previous run).
    let port_suffix = format!(":{TOXIPROXY_PORT}");
    let proxies = client.proxies().await.unwrap_or_default();
    for (name, proxy) in proxies {
        if proxy.config.listen.ends_with(&port_suffix) {
            proxy.delete().await.ok();
            eprintln!("Deleted stale proxy on :{TOXIPROXY_PORT}: {name}");
        }
    }

    let listen = format!("0.0.0.0:{TOXIPROXY_PORT}");
    client.create_proxy(TOXIPROXY_PROXY_NAME, &listen, "rustfs:9000").await?;
    eprintln!("Toxiproxy: {TOXIPROXY_PROXY_NAME} ({listen}) -> rustfs:9000");

    Ok(())
}

/// Add a downstream latency toxic to the bench proxy.
async fn add_latency_toxic(latency_ms: u64) -> Result<(), Box<dyn Error>> {
    let client = Client::new("http://localhost:8474");
    let toxic = Toxic {
        kind: ToxicKind::Latency { latency: latency_ms, jitter: 0 },
        name: "latency".into(),
        toxicity: 1.0,
        direction: StreamDirection::Downstream,
    };
    client.proxy(TOXIPROXY_PROXY_NAME).await?.add_toxic(&toxic).await?;
    Ok(())
}

/// Remove the latency toxic from the bench proxy.
async fn remove_latency_toxic() -> Result<(), Box<dyn Error>> {
    let client = Client::new("http://localhost:8474");
    client.proxy(TOXIPROXY_PROXY_NAME).await?.remove_toxic("latency").await?;
    Ok(())
}

fn rustfs_s3_options(endpoint_port: u16) -> S3Options {
    S3Options {
        region: Some("us-east-1".to_string()),
        endpoint_url: Some(format!("http://localhost:{endpoint_port}")),
        allow_http: true,
        anonymous: false,
        force_path_style: true,
        network_stream_timeout_seconds: None,
        requester_pays: false,
    }
}

fn rustfs_credentials() -> S3Credentials {
    S3Credentials::Static(S3StaticCredentials {
        access_key_id: "modify".into(),
        secret_access_key: "modifydata".into(),
        session_token: None,
        expires_after: None,
    })
}

/// Create S3 storage pointing at RustFS, optionally through toxiproxy.
fn make_s3_storage(
    latency_ms: Option<u64>,
) -> Result<Arc<dyn Storage + Send + Sync>, Box<dyn Error>> {
    let port = if latency_ms.is_some() { TOXIPROXY_PORT } else { RUSTFS_PORT };
    let storage = new_s3_storage(
        rustfs_s3_options(port),
        "testbucket".to_string(),
        Some(format!("bench-{}", uuid::Uuid::new_v4())),
        Some(rustfs_credentials()),
    )?;
    Ok(storage)
}

type Type = icechunk::config::Credentials;

/// Returns `(Repository, TempDir)` — the `TempDir` must be kept alive by the caller
/// because it holds the on-disk files that virtual chunk containers reference.
/// Dropping it would delete the files and break virtual chunk reads.
async fn setup_repo(
    path: Path,
    shape: ArrayShape,
    dimension_names: Option<Vec<DimensionName>>,
    split_config: Option<ManifestSplittingConfig>,
    storage_kind: StorageKind,
) -> Result<(Repository, TempDir), Box<dyn Error>> {
    let latency_ms = toxiproxy_latency_ms();
    let storage: Arc<dyn Storage + Send + Sync> = match storage_kind {
        StorageKind::Rustfs => {
            if latency_ms.is_some() {
                setup_toxiproxy().await?;
            }
            make_s3_storage(latency_ms)?
        }
        StorageKind::InMemory => new_in_memory_storage().await?,
    };

    let man_config = ManifestConfig {
        // turn off preloading so the profiles show us where things are loaded.
        preload: Some(ManifestPreloadConfig {
            max_total_refs: None,
            preload_if: Some(ManifestPreloadCondition::False),
            max_arrays_to_scan: None,
        }),
        splitting: split_config,
        virtual_chunk_location_compression: None,
    };

    let mut config = RepositoryConfig {
        manifest: Some(man_config),
        caching: Some(CachingConfig {
            // we only write 1 native chunk
            // & point a million chunk refs at it.
            // so turn off chunk caching.
            num_bytes_chunks: Some(0),
            ..CachingConfig::default()
        }),
        ..RepositoryConfig::default()
    };

    // Create 20 VCCs, each with a chunk file.
    // When using toxiproxy/S3, files live in MinIO and reads go through the proxy.
    // Otherwise, files live at VCC_LOCAL_DIR (a fixed path so set_chunks can
    // construct matching file:// URLs without needing the path at runtime).
    let use_s3 = matches!(storage_kind, StorageKind::Rustfs);
    let mut auth: HashMap<String, Option<Type>> = HashMap::new();
    let tmpdir = TempDir::new().unwrap();
    let chunk_data = Bytes::from(vec![42u8; CHUNK_SIZE_BYTES as usize]);

    if use_s3 {
        // Upload chunk objects to RustFS (direct, not through toxiproxy).
        let s3_store = object_store::aws::AmazonS3Builder::new()
            .with_endpoint(format!("http://localhost:{RUSTFS_PORT}"))
            .with_bucket_name("testbucket")
            .with_region("us-east-1")
            .with_access_key_id("modify")
            .with_secret_access_key("modifydata")
            .with_allow_http(true)
            .build()?;
        for i in 0..NUM_VCCS {
            let obj_path: object_store::path::Path =
                format!("bench-vcc/prefix-{i}/chunk").into();
            s3_store.put(&obj_path, chunk_data.clone().into()).await?;
        }
    } else {
        for i in 0..NUM_VCCS {
            let subdir = PathBuf::from(format!("{VCC_LOCAL_DIR}/prefix-{i}"));
            std::fs::create_dir_all(&subdir)?;
            let mut f = std::fs::File::create(subdir.join("chunk"))?;
            f.write_all(&chunk_data)?;
        }
    }

    for i in 0..NUM_VCCS {
        let (url_prefix, obj_store_config, cred) = if use_s3 {
            // VCC reads go through toxiproxy when latency is configured, else direct.
            let vcc_port =
                if latency_ms.is_some() { TOXIPROXY_PORT } else { RUSTFS_PORT };
            let url_prefix = format!("{VCC_S3_BASE}prefix-{i}/");
            let obj_store_config =
                ObjectStoreConfig::S3Compatible(rustfs_s3_options(vcc_port));
            let cred = Some(icechunk::config::Credentials::S3(rustfs_credentials()));
            (url_prefix, obj_store_config, cred)
        } else {
            let subdir = PathBuf::from(format!("{VCC_LOCAL_DIR}/prefix-{i}"));
            let url_prefix = format!("file://{}/", subdir.display());
            let obj_store_config = ObjectStoreConfig::LocalFileSystem(subdir);
            (url_prefix, obj_store_config, None)
        };

        let cont = VirtualChunkContainer::new_named(
            format!("prefix-{i}"),
            url_prefix.clone(),
            obj_store_config,
        )
        .unwrap();
        config.set_virtual_chunk_container(cont).unwrap();
        auth.insert(url_prefix, cred);
    }

    let repository = Repository::create(Some(config), storage, auth, None, false).await?;

    let mut session = repository.writable_session("main").await?;

    let defn = Bytes::from_static(br#"{"this":"array"}"#);
    session.add_group(Path::root(), defn.clone()).await?;
    session.add_array(path, shape, dimension_names, defn.clone()).await?;
    session.commit("initialized", None).await?;

    Ok((repository, tmpdir))
}

fn vcc_chunk_url(storage_kind: StorageKind, prefix_idx: usize) -> String {
    match storage_kind {
        StorageKind::Rustfs => format!("{VCC_S3_BASE}prefix-{prefix_idx}/chunk"),
        StorageKind::InMemory => {
            format!("file://{VCC_LOCAL_DIR}/prefix-{prefix_idx}/chunk")
        }
    }
}

async fn set_chunks(
    path: Path,
    session: &mut Session,
    chunks: Range<u32>,
    kind: ChunkKind,
    storage_kind: StorageKind,
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
        ChunkKind::Native => {
            let bytes = Bytes::from(vec![42u8; CHUNK_SIZE_BYTES as usize]);
            let payload = session.get_chunk_writer().unwrap()(bytes).await?;
            for idx in chunks {
                session
                    .set_chunk_ref(
                        path.clone(),
                        ChunkIndices(vec![idx]),
                        Some(payload.clone()),
                    )
                    .await?;
            }
        }
        ChunkKind::Virtual => {
            let mut rng = rand::rng();
            for idx in chunks {
                let prefix_idx = rng.random_range(0..NUM_VCCS);
                let payload = ChunkPayload::Virtual(VirtualChunkRef {
                    location: VirtualChunkLocation::from_url(&vcc_chunk_url(
                        storage_kind,
                        prefix_idx,
                    ))?,
                    offset: 0,
                    length: CHUNK_SIZE_BYTES,
                    checksum: None,
                });
                session
                    .set_chunk_ref(path.clone(), ChunkIndices(vec![idx]), Some(payload))
                    .await?;
            }
        }
        ChunkKind::VirtualWithPrefixes => {
            let mut rng = rand::rng();
            for idx in chunks {
                let prefix_idx = rng.random_range(0..NUM_VCCS);
                let location = VirtualChunkLocation::from_vcc_path(
                    &format!("prefix-{prefix_idx}"),
                    "chunk",
                )
                .unwrap();
                let payload = ChunkPayload::Virtual(VirtualChunkRef {
                    location,
                    offset: 0,
                    length: CHUNK_SIZE_BYTES,
                    checksum: None,
                });
                session
                    .set_chunk_ref(path.clone(), ChunkIndices(vec![idx]), Some(payload))
                    .await
                    .unwrap();
            }
        }
    }
    Ok(())
}

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
                let session_cell: OnceCell<Session> = OnceCell::new();

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
                                session.commit("data", None).await.unwrap();

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
                                        .rewrite_manifests(
                                            "rewrite",
                                            None,
                                            CommitMethod::Amend,
                                        )
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
    group.sample_size(10).sampling_mode(SamplingMode::Flat);

    let rt = Runtime::new().unwrap();

    let path: Path = "/temperature".try_into().unwrap();
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
                        ArrayShape::new(vec![(num_chunks.into(), num_chunks)]).unwrap();
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
                                    default_storage_kind(),
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
// TODO: IFS style rebasing workload
