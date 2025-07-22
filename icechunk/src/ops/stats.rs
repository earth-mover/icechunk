use futures::{StreamExt, TryStream, TryStreamExt, future::ready, stream};
use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroUsize},
    sync::{Arc, Mutex},
};
use tokio::task;
use tracing::trace;

use crate::{
    Storage,
    asset_manager::AssetManager,
    format::{
        ChunkId, SnapshotId,
        manifest::{ChunkPayload, Manifest},
        snapshot::ManifestFileInfo,
    },
    ops::pointed_snapshots,
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    storage,
    stream_utils::{StreamLimiter, try_unique_stream},
};

fn calculate_manifest_storage(
    manifest: Arc<Manifest>,
    seen_chunks: Arc<Mutex<HashSet<ChunkId>>>,
) -> RepositoryResult<u64> {
    trace!(manifest_id = %manifest.id(), "Processing manifest");
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
    trace!(manifest_id = %manifest.id(), "Manifest done");
    Ok(size)
}

async fn unique_manifest_infos<'a>(
    storage: &'a (dyn Storage + Send + Sync),
    storage_settings: &'a storage::Settings,
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
    max_snapshots_in_memory: NonZeroU16,
) -> RepositoryResult<impl TryStream<Ok = ManifestFileInfo, Error = RepositoryError> + 'a>
{
    let all_snaps =
        pointed_snapshots(storage, storage_settings, asset_manager, extra_roots)
            .await?
            .map(ready)
            .buffer_unordered(max_snapshots_in_memory.get() as usize);
    let all_manifest_infos = all_snaps
        // this could be slightly optimized by not collecting all manifest info records into a vec
        // but we don't expect too many, and they are small anyway
        .map_ok(|snap| {
            stream::iter(
                snap.manifest_files().map(Ok::<_, RepositoryError>).collect::<Vec<_>>(),
            )
        })
        .try_flatten();
    let res = try_unique_stream(|mi| mi.id.clone(), all_manifest_infos);
    Ok(res)
}

/// Compute the total size in bytes of all committed repo chunks.
/// It doesn't include inline or virtual chunks.
pub async fn repo_chunks_storage(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    max_snapshots_in_memory: NonZeroU16,
    max_compressed_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,
) -> RepositoryResult<u64> {
    let extra_roots = Default::default();
    let manifest_infos = unique_manifest_infos(
        storage,
        storage_settings,
        asset_manager.clone(),
        &extra_roots,
        max_snapshots_in_memory,
    )
    .await?;

    // we want to fetch many manifests in parallel, but not more than memory allows
    // for this we use the StreamLimiter using the manifest size in bytes for usage
    let limiter = &Arc::new(StreamLimiter::new(
        "repo_chunks_storage".to_string(),
        max_compressed_manifest_mem_bytes.get(),
    ));

    // We rate limit the stream of manifests to make sure we don't blow up memory
    let rate_limited_manifests =
        limiter.limit_stream(manifest_infos, |minfo| minfo.size_bytes as usize);

    let seen_chunks = &Arc::new(Mutex::new(HashSet::new()));
    let asset_manager = &asset_manager;

    let compute_stream = rate_limited_manifests
        .map_ok(|m| async move {
            let manifest =
                Arc::clone(asset_manager).fetch_manifest(&m.id, m.size_bytes).await?;
            Ok((manifest, m))
        })
        // Now we can buffer a bunch of fetch_manifest operations. Because we are using
        // StreamLimiter we know memory is not going to blow up
        .try_buffer_unordered(max_concurrent_manifest_fetches.get() as usize)
        .and_then(|(manifest, minfo)| async move {
            let seen_chunks = Arc::clone(seen_chunks);
            let size = task::spawn_blocking(|| {
                calculate_manifest_storage(manifest, seen_chunks)
            })
            .await??;
            Ok((size, minfo))
        });
    let (_, res) = limiter
        .unlimit_stream(compute_stream, |(_, minfo)| minfo.size_bytes as usize)
        .try_fold((0u64, 0), |(processed, total_size), (partial, _)| {
            //info!("Processed {processed} manifests");
            ready(Ok((processed + 1, total_size + partial)))
        })
        .await?;

    debug_assert_eq!(limiter.current_usage(), 0);

    Ok(res)
}
