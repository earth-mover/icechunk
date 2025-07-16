use futures::{TryStream, TryStreamExt as _, future::ready, stream};
use std::{
    collections::HashSet,
    num::NonZeroU16,
    sync::{Arc, Mutex},
};

use crate::{
    Storage,
    asset_manager::AssetManager,
    format::{ChunkId, ManifestId, manifest::ChunkPayload},
    ops::pointed_snapshots,
    repository::{RepositoryErrorKind, RepositoryResult},
    storage,
};

async fn manifest_chunks_storage(
    manifest_id: ManifestId,
    manifest_size: u64,
    asset_manager: Arc<AssetManager>,
    seen_chunks: Arc<Mutex<HashSet<ChunkId>>>,
) -> RepositoryResult<u64> {
    let manifest = asset_manager.fetch_manifest(&manifest_id, manifest_size).await?;
    let mut size = 0;
    for payload in manifest.chunk_payloads() {
        match payload {
            Ok(ChunkPayload::Ref(chunk_ref)) => {
                if seen_chunks
                    .lock()
                    .map_err(|e| {
                        RepositoryErrorKind::Other(format!(
                            "Thread panic during manifest_chunk_storage: {e}"
                        ))
                    })?
                    .insert(chunk_ref.id)
                {
                    size += chunk_ref.length;
                }
            }
            Ok(_) => {}
            // TODO: don't skip errors
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "Error in chunk payload iterator"
                );
            }
        }
    }
    Ok(size)
}

pub fn try_unique_stream<S, T, E, F, V>(
    f: F,
    stream: S,
) -> impl TryStream<Ok = T, Error = E>
where
    F: Fn(&S::Ok) -> V,
    S: TryStream<Ok = T, Error = E>,
    V: Eq + std::hash::Hash,
{
    let mut seen = HashSet::new();
    stream.try_filter(move |item| {
        let v = f(item);
        if seen.insert(v) {
            futures::future::ready(true)
        } else {
            futures::future::ready(false)
        }
    })
}

/// Compute the total size in bytes of all committed repo chunks.
/// It doesn't include inline or virtual chunks.
pub async fn repo_chunks_storage(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    process_manifests_concurrently: NonZeroU16,
) -> RepositoryResult<u64> {
    let extra_roots = Default::default();
    let all_snaps = pointed_snapshots(
        storage,
        storage_settings,
        Arc::clone(&asset_manager),
        &extra_roots,
    )
    .await?;

    let all_manifest_infos = all_snaps
        // this could be slightly optimized by not collecting all manifest info records into a vec
        // but we don't expect too many, and they are small anyway
        .map_ok(|snap| stream::iter(snap.manifest_files().map(Ok).collect::<Vec<_>>()))
        .try_flatten();
    let unique_manifest_infos = try_unique_stream(|mi| mi.id.clone(), all_manifest_infos);

    let seen_chunks = &Arc::new(Mutex::new(HashSet::new()));
    let asset_manager = &asset_manager;

    let res = unique_manifest_infos
        .map_ok(|manifest_info| async move {
            let manifest_size = manifest_info.size_bytes;
            manifest_chunks_storage(
                manifest_info.id,
                manifest_size,
                Arc::clone(asset_manager),
                Arc::clone(seen_chunks),
            )
            .await
        })
        .try_buffered(process_manifests_concurrently.get() as usize)
        .try_fold(0, |total, partial| ready(Ok(total + partial)))
        .await?;

    Ok(res)
}
