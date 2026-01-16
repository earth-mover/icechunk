use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::HashMap, vec};

use icechunk::{
    Repository, Storage,
    config::Credentials,
    format::{ChunkIndices, Path, manifest::ChunkPayload, snapshot::ArrayShape},
    storage::ObjectStorage,
};

use bytes::Bytes;
use itertools::Itertools;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const CHUNK_SIZE: usize = 20_000;

    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        println!(
            "Error: Pass either\n --write path/to/repo\n or\n --read path/to/repo\n as command line argument."
        );
        return Err("Invalid arguments".into());
    }

    let repo_dir = std::path::PathBuf::from(args[2].as_str());
    let REPO_SIZE: u32 = args[1].parse()?;

    let storage: Arc<dyn Storage + Send + Sync> = Arc::new(
        ObjectStorage::new_local_filesystem(repo_dir.as_path())
            .await
            .expect("Creating local storage failed"),
    );
    let creds: HashMap<String, Option<Credentials>> = Default::default();

    let repo = Repository::create(None, storage, creds, None)
        .await
        .expect("Failed to initialize repository");

    let ds = Arc::new(RwLock::new(repo.writable_session("main").await?));

    let shape = ArrayShape::new([(REPO_SIZE as u64, 1)]).unwrap();
    let user_data = Bytes::new();

    let new_array_path: Path = "/array".try_into().unwrap();

    ds.write()
        .await
        .add_array(new_array_path.clone(), shape, None, user_data)
        .await
        .unwrap();

    let payload = ChunkPayload::Inline(Bytes::from_static(&[42u8]));

    for chnk in &(0..REPO_SIZE).chunks(CHUNK_SIZE) {
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
    }

    ds.write().await.commit("really big changeset", None).await?;

    Ok(())
}
