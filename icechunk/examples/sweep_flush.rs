//! Measures how much of a commit's wall time is CPU, and on how many cores it lands.
//!
//! The `cores = cpu_ms / wall_ms` column is the signal:
//!   ~1        => CPU bound on a single runtime worker
//!   ~workers  => the CPU work spreads across the runtime
//!   <<1       => IO bound
//!
//! Storage is in-memory, so IO is near free and the CPU share is an upper bound.
//!
//! Usage:
//!   cargo run --release --example `sweep_flush` -- [chunks] [reps]

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::print_stdout)]

use std::{collections::HashMap, time::Instant};

use bytes::Bytes;
use icechunk::{
    Repository, RepositoryConfig,
    config::{ManifestConfig, ManifestSplittingConfig},
    format::{
        ChunkIndices,
        format_constants::SpecVersionBin,
        manifest::{ChunkPayload, VirtualChunkLocation, VirtualChunkRef},
        snapshot::ArrayShape,
    },
    session::Session,
    storage::new_in_memory_storage,
};
use icechunk::{
    config::{S3Credentials, S3Options, S3StaticCredentials},
    storage::new_s3_storage,
};
use noxious_client::{Client, StreamDirection, Toxic, ToxicKind};
use tokio::runtime::Builder;

const RUSTFS_PORT: u16 = 4200;
// The integration tests share this port, so the run deletes its proxy when it ends.
const TOXIPROXY_PORT: u16 = 9002;
const PROXY_NAME: &str = "sweep-flush-latency";

/// Injected per-request latency in ms, from `SWEEP_LATENCY_MS`. `None` uses memory storage.
fn latency_ms() -> Option<u64> {
    std::env::var("SWEEP_LATENCY_MS").ok().map(|v| v.parse().unwrap())
}

/// Creates the toxiproxy proxy in front of `RustFS` and applies the latency toxic.
async fn setup_toxiproxy(ms: u64) {
    let client = Client::new("http://localhost:8474");
    // Clear both a proxy left on this port by another run and one left under this
    // name on a different port.
    let suffix = format!(":{TOXIPROXY_PORT}");
    for (name, proxy) in client.proxies().await.unwrap_or_default() {
        if name == PROXY_NAME || proxy.config.listen.ends_with(&suffix) {
            let _ = proxy.delete().await;
        }
    }
    client
        .create_proxy(PROXY_NAME, &format!("0.0.0.0:{TOXIPROXY_PORT}"), "rustfs:9000")
        .await
        .unwrap();
    let toxic = Toxic {
        kind: ToxicKind::Latency { latency: ms, jitter: 0 },
        name: "latency".into(),
        toxicity: 1.0,
        direction: StreamDirection::Downstream,
    };
    client.proxy(PROXY_NAME).await.unwrap().add_toxic(&toxic).await.unwrap();
}

/// Removes the proxy, so a later test run finds the port free.
async fn teardown_toxiproxy() {
    let client = Client::new("http://localhost:8474");
    if let Ok(proxy) = client.proxy(PROXY_NAME).await {
        let _ = proxy.delete().await;
    }
}

fn s3_storage(
    through_proxy: bool,
) -> std::sync::Arc<dyn icechunk::storage::Storage + Send + Sync> {
    let port = if through_proxy { TOXIPROXY_PORT } else { RUSTFS_PORT };
    let options = S3Options::default()
        .with_region("us-east-1")
        .with_endpoint_url(format!("http://localhost:{port}"))
        .with_allow_http(true)
        .with_force_path_style(true);
    let creds = S3Credentials::Static(S3StaticCredentials {
        access_key_id: "modify".into(),
        secret_access_key: "modifydata".into(),
        session_token: None,
        expires_after: None,
    });
    new_s3_storage(
        options,
        "testbucket".to_string(),
        Some(format!("sweep-{}", uuid::Uuid::new_v4())),
        Some(creds),
        Vec::new(),
        Vec::new(),
        None,
    )
    .unwrap()
}

#[expect(unsafe_code)]
/// Process CPU time (user + system) in milliseconds.
fn cpu_ms() -> f64 {
    // SAFETY: getrusage writes into a fully initialised, correctly sized struct.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        usage
    };
    let to_ms = |t: libc::timeval| t.tv_sec as f64 * 1e3 + t.tv_usec as f64 / 1e3;
    to_ms(usage.ru_utime) + to_ms(usage.ru_stime)
}

/// Builds a session that holds `num_chunks` pending virtual chunk refs on one array,
/// split into `num_manifests` manifests.
async fn pending_session(
    num_chunks: u32,
    num_manifests: u32,
    inline: bool,
) -> Result<Session, Box<dyn std::error::Error>> {
    let storage = match latency_ms() {
        Some(_) => s3_storage(true),
        None => new_in_memory_storage().await?,
    };
    let config = RepositoryConfig {
        manifest: Some(ManifestConfig {
            splitting: Some(ManifestSplittingConfig::with_size(
                num_chunks.div_ceil(num_manifests),
            )),
            ..Default::default()
        }),
        ..Default::default()
    };
    let repo = Repository::create(
        Some(config),
        storage,
        HashMap::new(),
        Some(SpecVersionBin::current()),
        true,
    )
    .await?;

    let mut session = repo.writable_session("main").await?;
    let path = "/temperature".try_into()?;
    let shape =
        ArrayShape::new(vec![(num_chunks as u64, num_chunks)]).expect("valid shape");
    session
        .add_array(
            path,
            shape,
            Some(vec!["t".into()]),
            Bytes::from_static(br#"{"this":"array"}"#),
        )
        .await?;

    let path: icechunk::format::Path = "/temperature".try_into()?;
    let inline_bytes = Bytes::from_static(&[42u8]);
    for i in 0..num_chunks {
        let payload = if inline {
            ChunkPayload::Inline(inline_bytes.clone())
        } else {
            ChunkPayload::Virtual(VirtualChunkRef {
                location: VirtualChunkLocation::from_url(
                    format!("s3://foo/bar/{i}").as_str(),
                )?,
                offset: 0,
                length: 1,
                checksum: None,
            })
        };
        session.set_chunk_ref(path.clone(), ChunkIndices(vec![i]), Some(payload)).await?;
    }
    Ok(session)
}

/// Runs one config `reps` times and returns the median (`wall_ms`, `cpu_ms`) of the commit.
fn measure(
    num_chunks: u32,
    num_manifests: u32,
    workers: usize,
    nodes: usize,
    reps: usize,
    inline: bool,
) -> (f64, f64) {
    let rt =
        Builder::new_multi_thread().worker_threads(workers).enable_all().build().unwrap();
    let mut samples: Vec<(f64, f64)> = Vec::with_capacity(reps);
    for _ in 0..reps {
        let mut session =
            rt.block_on(pending_session(num_chunks, num_manifests, inline)).unwrap();
        let cpu0 = cpu_ms();
        let t0 = Instant::now();
        rt.block_on(async {
            session.commit("bench").max_concurrent_nodes(nodes).execute().await.unwrap();
        });
        samples.push((t0.elapsed().as_secs_f64() * 1e3, cpu_ms() - cpu0));
    }
    samples.sort_by(|a, b| a.0.total_cmp(&b.0));
    samples[samples.len() / 2]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let num_chunks: u32 = args.next().map_or(200_000, |a| a.parse().unwrap());
    let reps: usize = args.next().map_or(3, |a| a.parse().unwrap());
    // A single manifest count and worker count run one config, for profiling.
    let only_manifests: Option<u32> = args.next().map(|a| a.parse().unwrap());
    let only_workers: Option<usize> = args.next().map(|a| a.parse().unwrap());
    let inline = args.next().is_some_and(|a| a == "inline");

    if let Some(ms) = latency_ms() {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(setup_toxiproxy(ms));
    }

    let manifest_counts: Vec<u32> = only_manifests.map_or(vec![1, 10, 100], |m| vec![m]);
    let worker_counts: Vec<usize> = only_workers.map_or(vec![1, 2, 4, 8], |w| vec![w]);

    let kind = if inline { "inline" } else { "virtual" };
    let store = latency_ms().map_or("memory".to_string(), |ms| format!("rustfs+{ms}ms"));
    println!("chunks={num_chunks} reps={reps} kind={kind} storage={store} nodes=1 array");
    println!(
        "{:>10} {:>8} {:>10} {:>10} {:>7}",
        "manifests", "workers", "wall_ms", "cpu_ms", "cores"
    );
    for num_manifests in manifest_counts {
        for workers in worker_counts.iter().copied() {
            let (wall, cpu) =
                measure(num_chunks, num_manifests, workers, 8, reps, inline);
            println!(
                "{num_manifests:>10} {workers:>8} {wall:>10.1} {cpu:>10.1} {:>7.2}",
                cpu / wall
            );
        }
    }

    if latency_ms().is_some() {
        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(teardown_toxiproxy());
    }
}
