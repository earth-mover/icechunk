//! Repository statistics (chunk counts, sizes, etc.).

use std::{
    collections::HashSet,
    num::{NonZeroU16, NonZeroUsize},
    ops::Add,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tracing::{instrument, trace};

use crate::{
    asset_manager::AssetManager,
    format::{
        ChunkLength, ChunkOffset,
        manifest::{ChunkPayload, Manifest, VirtualChunkLocation},
    },
    ops::{
        pointed_snapshots,
        sharded_set::{ChunkIdSet, ShardedSet},
        walk_peak_requests,
        walker::{ManifestConsumer, ManifestWalkBudget, walk_manifests},
        warn_on_low_fd_limit,
    },
    repository::{RepositoryError, RepositoryErrorKind, RepositoryResult},
};
use icechunk_types::error::ICResultCtxExt as _;

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
    /// Create a new `ChunkStorageStats` with the specified byte counts
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

#[derive(Default)]
struct ChunkStorage {
    seen_native: ChunkIdSet,
    // Virtual chunks have no id; (location, offset, length) identifies the bytes.
    seen_virtual: ShardedSet<(VirtualChunkLocation, ChunkOffset, ChunkLength)>,
    native_bytes: AtomicU64,
    virtual_bytes: AtomicU64,
    // Inline chunks live in the manifest, so every occurrence is stored.
    inlined_bytes: AtomicU64,
}

impl ChunkStorage {
    fn into_stats(self) -> ChunkStorageStats {
        ChunkStorageStats::new(
            self.native_bytes.into_inner(),
            self.virtual_bytes.into_inner(),
            self.inlined_bytes.into_inner(),
        )
    }
}

impl ManifestConsumer for ChunkStorage {
    type Output = ();
    type Acc = ();

    fn consume(&self, manifest: &Manifest) -> RepositoryResult<()> {
        trace!(manifest_id = %manifest.id(), "Processing manifest");
        // Native ids stream straight into their shards. Virtual keys and the
        // inlined sum come out of the same single pass.
        let mut virtual_ = Vec::new();
        let mut inlined = 0u64;
        let native =
            manifest.chunk_payloads().inject()?.filter_map(|payload| match payload {
                Ok(ChunkPayload::Ref(r)) => Some(Ok((r.id, r.length))),
                Ok(ChunkPayload::Virtual(v)) => {
                    virtual_.push(((v.location, v.offset, v.length), v.length));
                    None
                }
                Ok(ChunkPayload::Inline(bytes)) => {
                    inlined += bytes.len() as u64;
                    None
                }
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            });
        let new_native = self.seen_native.try_extend_weighted(native).inject()?;
        let new_virtual = self.seen_virtual.extend_weighted(virtual_);
        self.native_bytes.fetch_add(new_native, Ordering::Relaxed);
        self.virtual_bytes.fetch_add(new_virtual, Ordering::Relaxed);
        self.inlined_bytes.fetch_add(inlined, Ordering::Relaxed);
        Ok(())
    }

    fn fold(_acc: &mut (), _output: ()) {}

    fn progress(&self) -> Option<(&'static str, u64)> {
        Some(("native_bytes", self.native_bytes.load(Ordering::Relaxed)))
    }
}

/// Chunk storage statistics over every reachable snapshot. Build with [`repo_chunks_storage`].
///
/// ```no_run
/// # use std::sync::Arc;
/// # use icechunk::{asset_manager::AssetManager, ops::stats::repo_chunks_storage};
/// # async fn f(am: Arc<AssetManager>) -> Result<(), Box<dyn std::error::Error>> {
/// let stats = repo_chunks_storage(am).execute().await?;
/// # Ok(()) }
/// ```
pub struct ChunkStorageStatsBuilder {
    asset_manager: Arc<AssetManager>,
    walk: ManifestWalkBudget,
}

impl std::fmt::Debug for ChunkStorageStatsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStorageStatsBuilder")
            .field("walk", &self.walk)
            .finish_non_exhaustive()
    }
}

/// Start a chunk storage computation. Each type of chunk gets its own total.
pub fn repo_chunks_storage(asset_manager: Arc<AssetManager>) -> ChunkStorageStatsBuilder {
    ChunkStorageStatsBuilder { asset_manager, walk: ManifestWalkBudget::default() }
}

impl ChunkStorageStatsBuilder {
    /// Default: 50.
    pub fn max_snapshots_in_memory(mut self, value: NonZeroU16) -> Self {
        self.walk.max_snapshots_in_memory = value;
        self
    }

    /// Default: 512 MiB.
    pub fn max_compressed_manifest_mem_bytes(mut self, value: NonZeroUsize) -> Self {
        self.walk.max_compressed_manifest_mem_bytes = value;
        self
    }

    /// Default: 4 GiB.
    pub fn max_decoded_manifest_mem_bytes(mut self, value: NonZeroUsize) -> Self {
        self.walk.max_decoded_manifest_mem_bytes = value;
        self
    }

    /// Default: 500.
    pub fn max_concurrent_manifest_fetches(mut self, value: NonZeroU16) -> Self {
        self.walk.max_concurrent_manifest_fetches = value;
        self
    }

    /// Compute the total size in bytes of all committed repo chunks.
    #[instrument(skip_all)]
    pub async fn execute(self) -> RepositoryResult<ChunkStorageStats> {
        let Self { asset_manager, walk } = self;
        warn_on_low_fd_limit(
            walk_peak_requests(
                walk.max_concurrent_manifest_fetches,
                walk.max_snapshots_in_memory,
            ),
            "Chunk storage stats",
        );
        let extra_roots = HashSet::new();
        let snaps = pointed_snapshots(
            Arc::clone(&asset_manager),
            None,
            &extra_roots,
            walk.max_snapshots_in_memory,
        )
        .await?;
        let limits = walk.limits(
            NonZeroU16::new(asset_manager.max_concurrent_decodes())
                .unwrap_or(NonZeroU16::MIN),
        );
        let consumer = Arc::new(ChunkStorage::default());
        walk_manifests(asset_manager, limits, Arc::clone(&consumer), snaps).await?;
        let consumer = Arc::try_unwrap(consumer).map_err(|_| {
            RepositoryError::capture(RepositoryErrorKind::Other(
                "manifest walker still holds the consumer".to_string(),
            ))
        })?;
        Ok(consumer.into_stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        asset_manager::AssetManagerOptions, format::format_constants::SpecVersionBin,
    };

    // defaults and method wiring; the asset manager is never touched
    #[tokio::test]
    async fn chunk_storage_stats_builder_methods_set_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let storage = crate::storage::new_in_memory_storage().await?;
        let am = Arc::new(AssetManager::new(
            storage,
            crate::storage::Settings::default(),
            SpecVersionBin::current(),
            &AssetManagerOptions::no_cache(),
        ));
        let b = repo_chunks_storage(Arc::clone(&am));
        assert_eq!(b.walk, ManifestWalkBudget::default());
        let b = b
            .max_snapshots_in_memory(NonZeroU16::new(7).unwrap())
            .max_compressed_manifest_mem_bytes(NonZeroUsize::new(8).unwrap())
            .max_decoded_manifest_mem_bytes(NonZeroUsize::new(9).unwrap())
            .max_concurrent_manifest_fetches(NonZeroU16::new(10).unwrap());
        assert_eq!(b.walk.max_snapshots_in_memory.get(), 7);
        assert_eq!(b.walk.max_compressed_manifest_mem_bytes.get(), 8);
        assert_eq!(b.walk.max_decoded_manifest_mem_bytes.get(), 9);
        assert_eq!(b.walk.max_concurrent_manifest_fetches.get(), 10);
        Ok(())
    }
}
