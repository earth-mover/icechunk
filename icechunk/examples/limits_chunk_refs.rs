use std::path::PathBuf;
use std::sync::Arc;
use std::vec;

use icechunk::{
    Repository, RepositoryConfig, Storage,
    config::{
        ManifestConfig, ManifestSplitCondition, ManifestSplitDim,
        ManifestSplitDimCondition, ManifestSplittingConfig,
    },
    format::{ChunkIndices, Path, manifest::ChunkPayload, snapshot::ArrayShape},
    storage::ObjectStorage,
};

use bytes::Bytes;
use itertools::Itertools;
use tokio::sync::RwLock;

use clap::Parser;
use test_log::tracing_subscriber;
use test_log::tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use tracing::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Location for the new repo
    #[arg(default_value = "test_repo")]
    repo_dir: PathBuf,

    /// Number of chunk_refs to set
    #[arg(default_value_t = 1_000_000)]
    count: u32,

    /// Number of chunk_refs per manifest
    #[arg(default_value_t = 50_000_000)]
    num_chunks: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer().compact())
        .init();

    const CHUNK_SIZE: usize = 20_000;

    let args = Args::parse();
    let repo_dir = args.repo_dir;
    let repo_size = args.count;
    let num_chunks = args.num_chunks;

    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(repo_dir.as_path())
            .await
            .expect("Creating local storage failed"),
    );
    let creds = Default::default();

    let split_sizes = Some(vec![(
        ManifestSplitCondition::PathMatches { regex: r".*".to_string() },
        vec![ManifestSplitDim {
            condition: ManifestSplitDimCondition::Any,
            num_chunks: num_chunks,
        }],
    )]);
    let manifest_config = ManifestConfig {
        splitting: Some(ManifestSplittingConfig { split_sizes }),
        ..ManifestConfig::default()
    };

    let repo = Repository::create(
        Some(RepositoryConfig {
            inline_chunk_threshold_bytes: Some(0),
            manifest: Some(manifest_config),
            ..Default::default()
        }),
        storage,
        creds,
        None,
    )
    .await
    .expect("Failed to initialize repository");

    let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));

    let shape = ArrayShape::new([(repo_size as u64, 1)]).unwrap();
    let user_data = Bytes::new();

    let new_array_path: Path = "/array".try_into().unwrap();

    ds.write()
        .await
        .add_array(new_array_path.clone(), shape, None, user_data)
        .await
        .unwrap();

    let payload = ChunkPayload::Inline(Bytes::from_static(&[42u8]));

    let mut count = 0;
    for chnk in &(0..repo_size).chunks(CHUNK_SIZE) {
        let mut set = tokio::task::JoinSet::new();

        for idx in chnk {
            let new_array_path = new_array_path.clone();
            let payload = payload.clone();
            let ds = ds.clone();

            set.spawn(async move {
                ds.write()
                    .await
                    .set_chunk_ref(new_array_path, ChunkIndices(vec![idx]), Some(payload))
                    .await
            });
        }

        while let Some(res) = set.join_next().await {
            res.unwrap().unwrap();
        }

        count += CHUNK_SIZE;
        if count % 1_000_000 == 0 {
            info!("{count} chunk_ref written");
        }
    }

    info!("starting commit");
    ds.write().await.commit("really big changeset", None).await?;

    Ok(())
}
