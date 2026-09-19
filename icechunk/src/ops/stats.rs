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
        walker::{ManifestConsumer, WalkLimits, walk_manifests},
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
}

/// Compute the total size in bytes of all committed repo chunks.
/// The total for each type of chunk is computed separately.
#[instrument(skip_all)]
pub async fn repo_chunks_storage(
    asset_manager: Arc<AssetManager>,
    max_snapshots_in_memory: NonZeroU16,
    max_compressed_manifest_mem_bytes: NonZeroUsize,
    max_concurrent_manifest_fetches: NonZeroU16,
) -> RepositoryResult<ChunkStorageStats> {
    let extra_roots = HashSet::new();
    let snaps = pointed_snapshots(
        Arc::clone(&asset_manager),
        None,
        &extra_roots,
        max_snapshots_in_memory,
    )
    .await?;
    let limits = WalkLimits {
        max_concurrent_manifest_fetches,
        max_manifest_mem_bytes: max_compressed_manifest_mem_bytes,
        decode_workers: NonZeroU16::new(asset_manager.max_concurrent_decodes())
            .unwrap_or(NonZeroU16::MIN),
    };
    let consumer = Arc::new(ChunkStorage::default());
    walk_manifests(asset_manager, limits, Arc::clone(&consumer), snaps).await?;
    let consumer = Arc::try_unwrap(consumer).map_err(|_| {
        RepositoryError::capture(RepositoryErrorKind::Other(
            "manifest walker still holds the consumer".to_string(),
        ))
    })?;
    Ok(consumer.into_stats())
}
