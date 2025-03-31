use futures::TryStreamExt as _;
use std::{collections::HashSet, sync::Arc};
use tokio::pin;

use crate::{
    Storage,
    asset_manager::AssetManager,
    format::{IcechunkFormatError, IcechunkFormatErrorKind, manifest::ChunkPayload},
    repository::RepositoryResult,
    storage,
};

use super::pointed_snapshots;

/// Compute the total size in bytes of all committed repo chunks.
/// It doesn't include inline or virtual chunks.
pub async fn repo_chunks_storage(
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
    asset_manager: Arc<AssetManager>,
) -> RepositoryResult<u64> {
    let extra_roots = Default::default();
    let all_snaps = pointed_snapshots(
        storage,
        storage_settings,
        Arc::clone(&asset_manager),
        &extra_roots,
    )
    .await?;

    let mut seen_manifests = HashSet::new();
    let mut seen_chunks = HashSet::new();
    let mut size = 0;

    pin!(all_snaps);
    while let Some(snap_id) = all_snaps.try_next().await? {
        let snap = asset_manager.fetch_snapshot(&snap_id).await?;

        for manifest_id in snap.manifest_files().map(|mf| mf.id) {
            if !seen_manifests.contains(&manifest_id) {
                let manifest_info =
                    snap.manifest_info(&manifest_id).ok_or_else(|| {
                        IcechunkFormatError::from(
                            IcechunkFormatErrorKind::ManifestInfoNotFound {
                                manifest_id: manifest_id.clone(),
                            },
                        )
                    })?;
                let manifest = asset_manager
                    .fetch_manifest(&manifest_id, manifest_info.size_bytes)
                    .await?;
                for payload in manifest.chunk_payloads() {
                    match payload {
                        Ok(ChunkPayload::Ref(chunk_ref)) => {
                            if seen_chunks.insert(chunk_ref.id) {
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

                seen_manifests.insert(manifest_id);
            }
        }
    }
    Ok(size)
}
