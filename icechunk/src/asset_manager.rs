use bytes::Bytes;
use quick_cache::sync::Cache;
use serde::{Deserialize, Serialize};
use std::{io::BufReader, ops::Range, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::SyncIoBridge;

use crate::{
    config::CachingConfig,
    format::{
        format_constants, manifest::Manifest, snapshot::Snapshot,
        transaction_log::TransactionLog, ChunkId, ChunkOffset, IcechunkFormatError,
        ManifestId, SnapshotId,
    },
    private,
    repository::{RepositoryError, RepositoryResult},
    storage, Storage,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(from = "AssetManagerSerializer")]
pub struct AssetManager {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    num_snapshots: u16,
    num_manifests: u16,
    num_transactions: u16,
    num_attributes: u16,
    num_chunks: u16,
    #[serde(skip)]
    snapshot_cache: Cache<SnapshotId, Arc<Snapshot>>,
    #[serde(skip)]
    manifest_cache: Cache<ManifestId, Arc<Manifest>>,
    #[serde(skip)]
    transactions_cache: Cache<SnapshotId, Arc<TransactionLog>>,
    #[serde(skip)]
    chunk_cache: Cache<(ChunkId, Range<ChunkOffset>), Bytes>,
}

impl private::Sealed for AssetManager {}

#[derive(Debug, Deserialize)]
struct AssetManagerSerializer {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    num_snapshots: u16,
    num_manifests: u16,
    num_transactions: u16,
    num_attributes: u16,
    num_chunks: u16,
}

impl From<AssetManagerSerializer> for AssetManager {
    fn from(value: AssetManagerSerializer) -> Self {
        AssetManager::new(
            value.storage,
            value.storage_settings,
            value.num_snapshots,
            value.num_manifests,
            value.num_transactions,
            value.num_attributes,
            value.num_chunks,
        )
    }
}

impl AssetManager {
    pub fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        num_snapshots: u16,
        num_manifests: u16,
        num_transactions: u16,
        num_attributes: u16,
        num_chunks: u16,
    ) -> Self {
        Self {
            num_snapshots,
            num_manifests,
            num_transactions,
            num_attributes,
            num_chunks,
            storage,
            storage_settings,
            snapshot_cache: Cache::new(num_snapshots as usize),
            manifest_cache: Cache::new(num_manifests as usize),
            transactions_cache: Cache::new(num_transactions as usize),
            chunk_cache: Cache::new(num_chunks as usize),
        }
    }

    pub fn new_no_cache(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
    ) -> Self {
        Self::new(storage, storage_settings, 0, 0, 0, 0, 0)
    }

    pub fn new_with_config(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        config: &CachingConfig,
    ) -> Self {
        Self::new(
            storage,
            storage_settings,
            config.snapshots_cache_size,
            config.manifests_cache_size,
            config.transactions_cache_size,
            config.attributes_cache_size,
            config.chunks_cache_size,
        )
    }

    pub async fn write_manifest(
        &self,
        manifest: Arc<Manifest>,
        compression_level: u8,
    ) -> RepositoryResult<Option<(ManifestId, u64)>> {
        let manifest_c = Arc::clone(&manifest);
        let res = write_new_manifest(
            manifest_c,
            compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
        )
        .await?;
        if let Some((manifest_id, _)) = res.as_ref() {
            self.manifest_cache.insert(manifest_id.clone(), manifest);
        }
        Ok(res)
    }

    pub async fn fetch_manifest(
        &self,
        manifest_id: &ManifestId,
        manifest_size: u64,
    ) -> RepositoryResult<Arc<Manifest>> {
        match self.manifest_cache.get_value_or_guard_async(manifest_id).await {
            Ok(manifest) => Ok(manifest),
            Err(guard) => {
                let manifest = fetch_manifest(
                    manifest_id,
                    manifest_size,
                    self.storage.as_ref(),
                    &self.storage_settings,
                )
                .await?;
                let _fail_is_ok = guard.insert(Arc::clone(&manifest));
                Ok(manifest)
            }
        }
    }

    pub async fn fetch_manifest_unknown_size(
        &self,
        manifest_id: &ManifestId,
    ) -> RepositoryResult<Arc<Manifest>> {
        self.fetch_manifest(manifest_id, 0).await
    }

    pub async fn write_snapshot(
        &self,
        snapshot: Arc<Snapshot>,
        compression_level: u8,
    ) -> RepositoryResult<SnapshotId> {
        let snapshot_c = Arc::clone(&snapshot);
        let snapshot_id = write_new_snapshot(
            snapshot_c,
            compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
        )
        .await?;
        self.snapshot_cache.insert(snapshot_id.clone(), snapshot);
        Ok(snapshot_id)
    }

    pub async fn fetch_snapshot(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<Arc<Snapshot>> {
        match self.snapshot_cache.get_value_or_guard_async(snapshot_id).await {
            Ok(snapshot) => Ok(snapshot),
            Err(guard) => {
                let snapshot = fetch_snapshot(
                    snapshot_id,
                    self.storage.as_ref(),
                    &self.storage_settings,
                )
                .await?;
                let _fail_is_ok = guard.insert(Arc::clone(&snapshot));
                Ok(snapshot)
            }
        }
    }

    pub async fn write_transaction_log(
        &self,
        transaction_id: SnapshotId,
        log: Arc<TransactionLog>,
        compression_level: u8,
    ) -> RepositoryResult<()> {
        let log_c = Arc::clone(&log);
        write_new_tx_log(
            transaction_id.clone(),
            log_c,
            compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
        )
        .await?;
        self.transactions_cache.insert(transaction_id, log);
        Ok(())
    }

    pub async fn fetch_transaction_log(
        &self,
        transaction_id: &SnapshotId,
    ) -> RepositoryResult<Arc<TransactionLog>> {
        match self.transactions_cache.get_value_or_guard_async(transaction_id).await {
            Ok(transaction) => Ok(transaction),
            Err(guard) => {
                let transaction = fetch_transaction_log(
                    transaction_id,
                    self.storage.as_ref(),
                    &self.storage_settings,
                )
                .await?;
                let _fail_is_ok = guard.insert(Arc::clone(&transaction));
                Ok(transaction)
            }
        }
    }

    pub async fn write_chunk(
        &self,
        chunk_id: ChunkId,
        bytes: Bytes,
    ) -> RepositoryResult<()> {
        // we don't pre-populate the chunk cache, there are too many of them for this to be useful
        Ok(self.storage.write_chunk(&self.storage_settings, chunk_id, bytes).await?)
    }

    pub async fn fetch_chunk(
        &self,
        chunk_id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> RepositoryResult<Bytes> {
        let key = (chunk_id.clone(), range.clone());
        match self.chunk_cache.get_value_or_guard_async(&key).await {
            Ok(chunk) => Ok(chunk),
            Err(guard) => {
                // TODO: split and parallelize downloads
                let chunk = self
                    .storage
                    .fetch_chunk(&self.storage_settings, chunk_id, range)
                    .await?;
                let _fail_is_ok = guard.insert(chunk.clone());
                Ok(chunk)
            }
        }
    }
}

fn binary_file_header(
    spec_version: u8,
    file_type: u8,
    compression_algorithm: u8,
) -> Vec<u8> {
    use format_constants::*;
    // TODO: initialize capacity
    let mut buffer = Vec::with_capacity(1024);
    // magic numbers
    buffer.extend_from_slice(ICECHUNK_FORMAT_MAGIC_BYTES);
    // implementation name
    let implementation = format!("{:<24}", &*ICECHUNK_CLIENT_NAME);
    buffer.extend_from_slice(&implementation.as_bytes()[..24]);
    // spec version
    buffer.push(spec_version);
    buffer.push(file_type);
    // compression
    buffer.push(compression_algorithm);
    buffer
}

async fn check_header(
    read: &mut (dyn AsyncRead + Unpin + Send),
    file_type: u8,
) -> RepositoryResult<u8> {
    let mut buf = [0; 12];
    read.read_exact(&mut buf).await?;
    // Magic numbers
    if format_constants::ICECHUNK_FORMAT_MAGIC_BYTES != buf {
        return Err(RepositoryError::FormatError(
            IcechunkFormatError::InvalidMagicNumbers,
        ));
    }

    let mut buf = [0; 24];
    // ignore implementation name
    read.read_exact(&mut buf).await?;

    let spec_version = read.read_u8().await?;
    if spec_version > format_constants::LATEST_ICECHUNK_SPEC_VERSION_BINARY {
        return Err(RepositoryError::FormatError(
            IcechunkFormatError::InvalidSpecVersion,
        ));
    }

    let actual_file_type = read.read_u8().await?;

    if actual_file_type != file_type {
        return Err(RepositoryError::FormatError(IcechunkFormatError::InvalidFileType {
            expected: file_type,
            got: actual_file_type,
        }));
    }

    let compression = read.read_u8().await?;
    Ok(compression)
}

async fn write_new_manifest(
    new_manifest: Arc<Manifest>,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<Option<(ManifestId, u64)>> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            LATEST_ICECHUNK_SPEC_VERSION_BINARY.to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_MANIFEST.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let new_manifest_info = if new_manifest.len() > 0 {
        use format_constants::*;
        let id = ManifestId::random();
        // TODO: we should compress only when the manifest reaches a certain size
        // but then, we would need to include metadata to know if it's compressed or not
        let buffer = tokio::task::spawn_blocking(move || {
            let buffer = binary_file_header(
                LATEST_ICECHUNK_SPEC_VERSION_BINARY,
                ICECHUNK_FILE_TYPE_BINARY_MANIFEST,
                ICECHUNK_COMPRESSION_BINARY_ZSTD,
            );
            let mut compressor =
                zstd::stream::Encoder::new(buffer, compression_level as i32)?;
            rmp_serde::encode::write(&mut compressor, new_manifest.as_ref())?;
            compressor.finish().map_err(RepositoryError::IOError)
        })
        .await??;

        let len = buffer.len() as u64;

        storage
            .write_manifest(storage_settings, id.clone(), metadata, buffer.into())
            .await?;
        Some((id, len))
    } else {
        None
    };

    Ok(new_manifest_info)
}

async fn fetch_manifest(
    manifest_id: &ManifestId,
    manifest_size: u64,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<Arc<Manifest>> {
    let read =
        storage.fetch_manifest(storage_settings, manifest_id, manifest_size).await?;
    check_decompress_and_parse(read, format_constants::ICECHUNK_FILE_TYPE_BINARY_MANIFEST)
        .await
}

async fn check_decompress_and_parse<T>(
    mut read: Box<dyn AsyncRead + Unpin + Send>,
    file_type: u8,
) -> RepositoryResult<Arc<T>>
where
    for<'de> T: Send + Deserialize<'de> + 'static,
{
    let compression = check_header(read.as_mut(), file_type).await?;
    debug_assert_eq!(compression, 1);

    let object = tokio::task::spawn_blocking(move || {
        let sync_read = SyncIoBridge::new(read);
        // We find a performance impact if we don't buffer here
        let decompressor =
            BufReader::with_capacity(1_024, zstd::stream::Decoder::new(sync_read)?);
        rmp_serde::from_read(decompressor).map_err(RepositoryError::DeserializationError)
    })
    .await??;
    Ok(Arc::new(object))
}

async fn write_new_snapshot(
    new_snapshot: Arc<Snapshot>,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<SnapshotId> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            LATEST_ICECHUNK_SPEC_VERSION_BINARY.to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_SNAPSHOT.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let id = new_snapshot.metadata.id.clone();
    let buffer = tokio::task::spawn_blocking(move || {
        let buffer = binary_file_header(
            LATEST_ICECHUNK_SPEC_VERSION_BINARY,
            ICECHUNK_FILE_TYPE_BINARY_SNAPSHOT,
            ICECHUNK_COMPRESSION_BINARY_ZSTD,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;
        rmp_serde::encode::write(&mut compressor, new_snapshot.as_ref())?;
        compressor.finish().map_err(RepositoryError::IOError)
    })
    .await??;

    storage.write_snapshot(storage_settings, id.clone(), metadata, buffer.into()).await?;

    Ok(id)
}

async fn fetch_snapshot(
    snapshot_id: &SnapshotId,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<Arc<Snapshot>> {
    let read = storage.fetch_snapshot(storage_settings, snapshot_id).await?;
    check_decompress_and_parse(read, format_constants::ICECHUNK_FILE_TYPE_BINARY_SNAPSHOT)
        .await
}

async fn write_new_tx_log(
    transaction_id: SnapshotId,
    new_log: Arc<TransactionLog>,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<()> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            LATEST_ICECHUNK_SPEC_VERSION_BINARY.to_string(),
        ),
        (ICECHUNK_CLIENT_NAME_METADATA_KEY.to_string(), ICECHUNK_CLIENT_NAME.to_string()),
        (
            ICECHUNK_FILE_TYPE_METADATA_KEY.to_string(),
            ICECHUNK_FILE_TYPE_TRANSACTION_LOG.to_string(),
        ),
        (
            ICECHUNK_COMPRESSION_METADATA_KEY.to_string(),
            ICECHUNK_COMPRESSION_ZSTD.to_string(),
        ),
    ];

    let buffer = tokio::task::spawn_blocking(move || {
        let buffer = binary_file_header(
            LATEST_ICECHUNK_SPEC_VERSION_BINARY,
            ICECHUNK_FILE_TYPE_BINARY_TRANSACTION_LOG,
            ICECHUNK_COMPRESSION_BINARY_ZSTD,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;
        rmp_serde::encode::write(&mut compressor, new_log.as_ref())?;
        compressor.finish().map_err(RepositoryError::IOError)
    })
    .await??;

    storage
        .write_transaction_log(storage_settings, transaction_id, metadata, buffer.into())
        .await?;

    Ok(())
}

async fn fetch_transaction_log(
    transaction_id: &SnapshotId,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<Arc<TransactionLog>> {
    let read = storage.fetch_transaction_log(storage_settings, transaction_id).await?;

    check_decompress_and_parse(
        read,
        format_constants::ICECHUNK_FILE_TYPE_BINARY_TRANSACTION_LOG,
    )
    .await
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod test {

    use itertools::Itertools;

    use super::*;
    use crate::{
        format::{
            manifest::{ChunkInfo, ChunkPayload},
            ChunkIndices, NodeId,
        },
        storage::{logging::LoggingStorage, new_in_memory_storage, Storage},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_caches() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(backend.clone(), settings.clone());

        let ci1 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"b")),
        };
        let pre_exiting_manifest =
            Arc::new(Manifest::from_iter(vec![ci1].into_iter()).await?);
        let (pre_existing_id, pre_size) =
            manager.write_manifest(Arc::clone(&pre_exiting_manifest), 1).await?.unwrap();

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = AssetManager::new_with_config(
            Arc::clone(&logging_c),
            settings,
            &CachingConfig::default(),
        );

        let manifest = Arc::new(Manifest::from_iter(vec![ci2].into_iter()).await?);
        let (id, size) = caching.write_manifest(Arc::clone(&manifest), 1).await?.unwrap();

        assert_eq!(caching.fetch_manifest(&id, size).await?, manifest);
        assert_eq!(caching.fetch_manifest(&id, size).await?, manifest);
        // when we insert we cache, so no fetches
        assert_eq!(logging.fetch_operations(), vec![]);

        // first time it sees an ID it calls the backend
        assert_eq!(
            caching.fetch_manifest(&pre_existing_id, pre_size).await?,
            pre_exiting_manifest
        );
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifest_splitting".to_string(), pre_existing_id.0.to_vec())]
        );

        // only calls backend once
        assert_eq!(
            caching.fetch_manifest(&pre_existing_id, pre_size).await?,
            pre_exiting_manifest
        );
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifest_splitting".to_string(), pre_existing_id.0.to_vec())]
        );

        // other walues still cached
        assert_eq!(caching.fetch_manifest(&id, size).await?, manifest);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifest_splitting".to_string(), pre_existing_id.0.to_vec())]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_has_limit() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(backend.clone(), settings.clone());

        let ci1 = ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(vec![]),
            payload: ChunkPayload::Inline(Bytes::copy_from_slice(b"a")),
        };
        let ci2 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci3 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci4 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci5 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci6 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci7 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci8 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };
        let ci9 = ChunkInfo { node: NodeId::random(), ..ci1.clone() };

        let manifest1 = Arc::new(Manifest::from_iter(vec![ci1, ci2, ci3]).await?);
        let (id1, size1) =
            manager.write_manifest(Arc::clone(&manifest1), 1).await?.unwrap();
        let manifest2 = Arc::new(Manifest::from_iter(vec![ci4, ci5, ci6]).await?);
        let (id2, size2) =
            manager.write_manifest(Arc::clone(&manifest2), 1).await?.unwrap();
        let manifest3 = Arc::new(Manifest::from_iter(vec![ci7, ci8, ci9]).await?);
        let (id3, size3) =
            manager.write_manifest(Arc::clone(&manifest3), 1).await?.unwrap();

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = AssetManager::new_with_config(
            logging_c,
            settings,
            // the cache can only fit 2 manifests.
            &CachingConfig {
                snapshots_cache_size: 0,
                manifests_cache_size: 2,
                transactions_cache_size: 0,
                attributes_cache_size: 0,
                chunks_cache_size: 0,
            },
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            assert_eq!(caching.fetch_manifest(&id1, size1).await?, manifest1);
            assert_eq!(caching.fetch_manifest(&id2, size2).await?, manifest2);
            assert_eq!(caching.fetch_manifest(&id3, size3).await?, manifest3);
        }
        // after the initial warming requests, we only request the file that doesn't fit in the cache
        assert_eq!(logging.fetch_operations()[10..].iter().unique().count(), 1);

        Ok(())
    }
}
