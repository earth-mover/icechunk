// Shared helpers for benchmarks: storage setup, repo creation, chunk writing, toxiproxy.

/// Initialize a tracing backend selected by the `ICECHUNK_TRACE` env var.
///
/// Supported values (case-insensitive):
/// - `samply`  — wire up `tracing-samply` so `#[instrument]` spans appear as
///   profiler markers in the samply UI.
/// - `chrome`  — emit a Chrome trace JSON file (viewable in Perfetto UI or
///   `chrome://tracing`). The file is written when the returned
///   `FlushGuard` is dropped.
///
/// If unset or unrecognized, no tracing layer is installed.
pub(crate) fn init_tracing() -> Option<tracing_chrome::FlushGuard> {
    #[cfg(not(feature = "logs"))]
    {
        if std::env::var("ICECHUNK_TRACE").is_ok() {
            eprintln!("ICECHUNK_TRACE is set but the `logs` feature is not enabled");
        }
        return None;
    }

    #[cfg(feature = "logs")]
    {
        use tracing_subscriber::{
            layer::SubscriberExt as _, util::SubscriberInitExt as _,
        };

        let trace_var = std::env::var("ICECHUNK_TRACE").unwrap_or_default();
        match trace_var.to_lowercase().as_str() {
            "samply" => {
                match tracing_samply::SamplyLayer::new() {
                    Ok(layer) => {
                        match tracing_subscriber::Registry::default()
                            .with(tracing_error::ErrorLayer::default())
                            .with(layer)
                            .try_init()
                        {
                            Ok(()) => eprintln!("tracing: samply layer installed"),
                            Err(e) => eprintln!("tracing: samply try_init failed: {e}"),
                        }
                    }
                    Err(e) => eprintln!("tracing: SamplyLayer::new() failed: {e}"),
                }
                None
            }
            "chrome" => {
                let trace_file =
                    std::env::current_dir().unwrap_or_default().join(format!(
                        "trace-{}.json",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                    ));
                let (chrome_layer, guard) =
                    tracing_chrome::ChromeLayerBuilder::new().file(&trace_file).build();
                match tracing_subscriber::Registry::default()
                    .with(tracing_error::ErrorLayer::default())
                    .with(chrome_layer)
                    .try_init()
                {
                    Ok(()) => eprintln!(
                        "tracing: chrome layer installed, will write to {}",
                        trace_file.display()
                    ),
                    Err(e) => eprintln!("tracing: chrome try_init failed: {e}"),
                }
                Some(guard)
            }
            "" => None,
            other => {
                eprintln!(
                    "ICECHUNK_TRACE={other}: unrecognized value (expected `samply` or `chrome`)"
                );
                None
            }
        }
    }
}

use std::collections::HashMap;
use std::error::Error;
use std::io::Write as _;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use icechunk::ObjectStoreConfig;
use icechunk::config::{
    CachingConfig, ManifestConfig, ManifestPreloadCondition, ManifestPreloadConfig,
    ManifestSplittingConfig, S3Credentials, S3Options, S3StaticCredentials,
};
use icechunk::format::manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef};
use icechunk::format::snapshot::{ArrayShape, DimensionName};
use icechunk::format::{ChunkIndices, Path};
use icechunk::new_s3_storage;
use icechunk::repository::Repository;
use icechunk::session::Session;
use icechunk::storage::new_in_memory_storage;
use icechunk::virtual_chunks::VirtualChunkContainer;
use icechunk::{RepositoryConfig, Storage};
use icechunk_arrow_object_store::object_store::ObjectStoreExt as _;
use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};
use rand::RngExt as _;
use tempfile::TempDir;

pub(crate) const RUSTFS_PORT: u16 = 4200;
pub(crate) const TOXIPROXY_PORT: u16 = 9002;
pub(crate) const TOXIPROXY_PROXY_NAME: &str = "bench-latency";
pub(crate) const NUM_VCCS: usize = 20;
pub(crate) const CHUNK_SIZE_BYTES: u64 = 1024 * 1024;
pub(crate) const VCC_S3_BASE: &str = "s3://testbucket/bench-vcc/";
pub(crate) const VCC_LOCAL_DIR: &str = "/tmp/icechunk-bench-vcc";

#[derive(Clone, Copy)]
pub(crate) enum ChunkKind {
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
                write!(f, "virtual-{NUM_VCCS}-prefixes")
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum StorageKind {
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

pub(crate) fn default_storage_kind() -> StorageKind {
    match toxiproxy_latency_ms() {
        Some(_) => StorageKind::Rustfs,
        None => StorageKind::InMemory,
    }
}

/// Parse `ICECHUNK_BENCH_LATENCY_MS` env var; returns `Some(ms)` if set.
pub(crate) fn toxiproxy_latency_ms() -> Option<u64> {
    #[expect(clippy::expect_used)]
    std::env::var("ICECHUNK_BENCH_LATENCY_MS")
        .ok()
        .map(|v| v.parse::<u64>().expect("ICECHUNK_BENCH_LATENCY_MS must be an integer"))
}

/// Set up toxiproxy: create a proxy on `TOXIPROXY_PORT` forwarding to `RustFS` at
/// `rustfs:9000`. Does NOT add any toxics — those are managed separately so that
/// latency is only injected during the timed portion of benchmarks.
pub(crate) async fn setup_toxiproxy() -> Result<(), Box<dyn Error>> {
    let client = Client::new("http://localhost:8474");

    // Delete any proxy already bound to this port (leftover from a previous run).
    let port_suffix = format!(":{TOXIPROXY_PORT}");
    let proxies = client.proxies().await.unwrap_or_default();
    for (name, proxy) in proxies {
        if proxy.config.listen.ends_with(&port_suffix) {
            let _ = proxy.delete().await;
            eprintln!("Deleted stale proxy on :{TOXIPROXY_PORT}: {name}");
        }
    }

    let listen = format!("0.0.0.0:{TOXIPROXY_PORT}");
    client.create_proxy(TOXIPROXY_PROXY_NAME, &listen, "rustfs:9000").await?;
    eprintln!("Toxiproxy: {TOXIPROXY_PROXY_NAME} ({listen}) -> rustfs:9000");

    Ok(())
}

/// Add a downstream latency toxic to the bench proxy.
pub(crate) async fn add_latency_toxic(latency_ms: u64) -> Result<(), Box<dyn Error>> {
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
pub(crate) async fn remove_latency_toxic() -> Result<(), Box<dyn Error>> {
    let client = Client::new("http://localhost:8474");
    client.proxy(TOXIPROXY_PROXY_NAME).await?.remove_toxic("latency").await?;
    Ok(())
}

pub(crate) fn rustfs_s3_options(endpoint_port: u16) -> S3Options {
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

pub(crate) fn rustfs_credentials() -> S3Credentials {
    S3Credentials::Static(S3StaticCredentials {
        access_key_id: "modify".into(),
        secret_access_key: "modifydata".into(),
        session_token: None,
        expires_after: None,
    })
}

/// Create S3 storage pointing at `RustFS`, optionally through toxiproxy.
pub(crate) fn make_s3_storage(
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

type Credentials = icechunk::config::Credentials;

/// Returns `(Repository, TempDir)` — the `TempDir` must be kept alive by the caller
/// because it holds the on-disk files that virtual chunk containers reference.
/// Dropping it would delete the files and break virtual chunk reads.
pub(crate) async fn setup_repo(
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
    let mut auth: HashMap<String, Option<Credentials>> = HashMap::new();
    let tmpdir = TempDir::new().unwrap();
    let chunk_data = Bytes::from(vec![42u8; CHUNK_SIZE_BYTES as usize]);

    if use_s3 {
        // Upload chunk objects to RustFS (direct, not through toxiproxy).
        let s3_store =
            icechunk_arrow_object_store::object_store::aws::AmazonS3Builder::new()
                .with_endpoint(format!("http://localhost:{RUSTFS_PORT}"))
                .with_bucket_name("testbucket")
                .with_region("us-east-1")
                .with_access_key_id("modify")
                .with_secret_access_key("modifydata")
                .with_allow_http(true)
                .build()?;
        for i in 0..NUM_VCCS {
            let obj_path: icechunk_arrow_object_store::object_store::path::Path =
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
    session.commit("initialized").max_concurrent_nodes(8).execute().await?;

    Ok((repository, tmpdir))
}

pub(crate) fn vcc_chunk_url(storage_kind: StorageKind, prefix_idx: usize) -> String {
    match storage_kind {
        StorageKind::Rustfs => format!("{VCC_S3_BASE}prefix-{prefix_idx}/chunk"),
        StorageKind::InMemory => {
            format!("file://{VCC_LOCAL_DIR}/prefix-{prefix_idx}/chunk")
        }
    }
}

pub(crate) async fn set_chunks(
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
