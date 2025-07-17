use futures::{TryStreamExt, future::ready, stream};
use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroUsize},
    sync::{Arc, Mutex},
};
use tracing::trace;

use crate::{
    Storage,
    asset_manager::AssetManager,
    format::{
        ChunkId,
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

/// Compute the total size in bytes of all committed repo chunks.
/// It doesn't include inline or virtual chunks.
pub async fn repo_chunks_storage(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
    max_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,
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
        .map_ok(|snap| {
            stream::iter(
                snap.manifest_files().map(Ok::<_, RepositoryError>).collect::<Vec<_>>(),
            )
        })
        .try_flatten();

    // we don't want to check manifests more than once, so we unique them by their id
    let unique_manifest_infos = try_unique_stream(|mi| mi.id.clone(), all_manifest_infos);

    // we want to fetch many manifests in parallel, but not more than memory allows
    // for this we use the StreamLimiter using the manifest size in bytes for usage
    let limiter = &Arc::new(StreamLimiter::new(
        "repo_chunks_storage".to_string(),
        max_manifest_mem_bytes.get(),
        |m: &ManifestFileInfo| m.size_bytes as usize,
    ));

    // The StreamLimiter works by calling limit on every element before they are processed
    let rate_limited_manifests = unique_manifest_infos
        .and_then(|m| async move { Ok(limiter.clone().limit(m).await) });

    let seen_chunks = &Arc::new(Mutex::new(HashSet::new()));
    let asset_manager = &asset_manager;

    let (_, res) = rate_limited_manifests
        .map_ok(|m| async move {
            let manifest =
                Arc::clone(asset_manager).fetch_manifest(&m.id, m.size_bytes).await?;
            Ok((manifest, m))
        })
        // Now we can buffer a bunch of fetch_manifest operations. Because we are using
        // StreamLimiter we know memory is not going to blow up
        .try_buffer_unordered(max_concurrent_manifest_fetches.get() as usize)
        .map_ok(|(manifest, minfo)| async move {
            let size = calculate_manifest_storage(manifest, Arc::clone(seen_chunks))?;
            Ok((size, minfo))
        })
        // We do some more buffering to get some concurrency on the processing of the manifest file
        // TODO: this should actually happen in a CPU bounded worker pool
        .try_buffer_unordered(4)
        // Now StreamLimiter requires us to call free, this will make room for more manifests to be
        // fetched into the previous buffer
        .and_then(|(size, minfo)| async move {
            limiter.clone().free(minfo).await;
            Ok(size)
        })
        .try_fold((0u64, 0), |(processed, total_size), partial| {
            //info!("Processed {processed} manifests");
            ready(Ok((processed + 1, total_size + partial)))
        })
        .await?;

    debug_assert_eq!(limiter.current_usage().await, (0, 0));

    Ok(res)
}
