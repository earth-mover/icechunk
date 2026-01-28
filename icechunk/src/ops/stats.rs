//! Repository statistics (chunk counts, sizes, etc.).

use futures::{StreamExt, TryStream, TryStreamExt, future::ready, stream};
use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroUsize},
    ops::Add,
    sync::{Arc, Mutex},
};
use tokio::task;
use tracing::trace;

use crate::{
    asset_manager::AssetManager,
    format::{
        ChunkId, ChunkLength, ChunkOffset, SnapshotId,
        manifest::{ChunkPayload, Manifest, VirtualChunkLocation},
        snapshot::ManifestFileInfo,
    },
    ops::pointed_snapshots,
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
    stream_utils::{StreamLimiter, try_unique_stream},
};

/// Statistics about chunk storage across different chunk types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkStorageStats {
    /// Total bytes stored in native chunks (stored in icechunk's chunk storage)
    pub native_bytes: u64,
    /// Total bytes stored in virtual chunks (references to external data)
    pub virtual_bytes: u64,
    /// Total bytes stored in inline chunks (stored directly in manifests)
    pub inlined_bytes: u64,
}

impl ChunkStorageStats {
    /// Create a new ChunkStorageStats with the specified byte counts
    pub fn new(native_bytes: u64, virtual_bytes: u64, inlined_bytes: u64) -> Self {
        Self { native_bytes, virtual_bytes, inlined_bytes }
    }

    /// Get the total bytes excluding virtual chunks (this is ~= to the size of all objects in the icechunk repo)
    pub fn non_virtual_bytes(&self) -> u64 {
        self.native_bytes.saturating_add(self.inlined_bytes)
    }

    /// Get the total bytes across all chunk types
    pub fn total_bytes(&self) -> u64 {
        self.native_bytes
            .saturating_add(self.virtual_bytes)
            .saturating_add(self.inlined_bytes)
    }
}

impl Add for ChunkStorageStats {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            native_bytes: self.native_bytes.saturating_add(other.native_bytes),
            virtual_bytes: self.virtual_bytes.saturating_add(other.virtual_bytes),
            inlined_bytes: self.inlined_bytes.saturating_add(other.inlined_bytes),
        }
    }
}

/// Helper function to deduplicate chunks by inserting into a HashSet and counting if new
fn insert_and_increment_size_if_new<T: Eq + std::hash::Hash>(
    seen: &Arc<Mutex<HashSet<T>>>,
    key: T,
    size_increment: u64,
    size_counter: &mut u64,
) -> RepositoryResult<()> {
    if seen
        .lock()
        .map_err(|e| {
            RepositoryErrorKind::Other(format!(
                "Thread panic during manifest_chunk_storage: {e}"
            ))
        })?
        .insert(key)
    {
        *size_counter += size_increment;
    }
    Ok(())
}

fn calculate_manifest_storage(
    manifest: Arc<Manifest>,
    // Different types of chunks require using different types of ids to de-duplicate them when counting.
    seen_native_chunks: Arc<Mutex<HashSet<ChunkId>>>,
    // Virtual chunks don't necessarily have checksums, so we instead use the (url, offset, length) tuple as an identifier.
    // This is more expensive, but should work to de-duplicate because the only way that this identifier could be the same
    // for different chunks is if the data were entirely overwritten at that exact storage location.
    // In that scenario it makes sense not to count both chunks towards the storage total,
    // as the overwritten data is no longer accessible anyway.
    seen_virtual_chunks: Arc<
        Mutex<HashSet<(VirtualChunkLocation, ChunkOffset, ChunkLength)>>,
    >,
) -> RepositoryResult<ChunkStorageStats> {
    trace!(manifest_id = %manifest.id(), "Processing manifest");
    let mut native_bytes: u64 = 0;
    let mut virtual_bytes: u64 = 0;
    let mut inlined_bytes: u64 = 0;
    for payload in manifest.chunk_payloads() {
        match payload {
            Ok(ChunkPayload::Ref(chunk_ref)) => {
                // Deduplicate native chunks by ChunkId
                insert_and_increment_size_if_new(
                    &seen_native_chunks,
                    chunk_ref.id,
                    chunk_ref.length,
                    &mut native_bytes,
                )?;
            }
            Ok(ChunkPayload::Virtual(virtual_ref)) => {
                // Deduplicate by by (location, offset, length)
                let virtual_chunk_identifier = (
                    // TODO: Remove the need for this clone somehow?
                    // It could potentially save a lot of memory usage for large virtual stores with long urls...
                    virtual_ref.location.clone(),
                    virtual_ref.offset,
                    virtual_ref.length,
                );
                insert_and_increment_size_if_new(
                    &seen_virtual_chunks,
                    virtual_chunk_identifier,
                    virtual_ref.length,
                    &mut virtual_bytes,
                )?;
            }
            Ok(ChunkPayload::Inline(bytes)) => {
                // Inline chunks are stored in the manifest itself,
                // so count each occurrence since they're actually stored repeatedly across different manifests
                inlined_bytes += bytes.len() as u64;
            }
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

    let stats = ChunkStorageStats::new(native_bytes, virtual_bytes, inlined_bytes);
    Ok(stats)
}

async fn unique_manifest_infos<'a>(
    asset_manager: Arc<AssetManager>,
    extra_roots: &'a HashSet<SnapshotId>,
    max_snapshots_in_memory: NonZeroU16,
) -> RepositoryResult<impl TryStream<Ok = ManifestFileInfo, Error = RepositoryError> + 'a>
{
    let all_snaps = pointed_snapshots(asset_manager, extra_roots)
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
/// The total for each type of chunk is computed separately.
pub async fn repo_chunks_storage(
    asset_manager: Arc<AssetManager>,
    max_snapshots_in_memory: NonZeroU16,
    max_compressed_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,
) -> RepositoryResult<ChunkStorageStats> {
    let extra_roots = Default::default();
    let manifest_infos = unique_manifest_infos(
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

    let seen_native_chunks = &Arc::new(Mutex::new(HashSet::new()));
    let seen_virtual_chunks = &Arc::new(Mutex::new(HashSet::new()));
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
            let seen_native_chunks = Arc::clone(seen_native_chunks);
            let seen_virtual_chunks = Arc::clone(seen_virtual_chunks);
            let stats = task::spawn_blocking(|| {
                calculate_manifest_storage(
                    manifest,
                    seen_native_chunks,
                    seen_virtual_chunks,
                )
            })
            .await??;
            Ok((stats, minfo))
        });
    let (_, res) = limiter
        .unlimit_stream(compute_stream, |(_, minfo)| minfo.size_bytes as usize)
        .try_fold(
            (0u64, ChunkStorageStats::default()),
            |(processed, total_stats), (partial, _)| {
                //info!("Processed {processed} manifests");
                ready(Ok((processed + 1, total_stats + partial)))
            },
        )
        .await?;

    debug_assert_eq!(limiter.current_usage(), 0);

    Ok(res)
}
