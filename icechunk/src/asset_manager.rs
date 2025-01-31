use async_stream::try_stream;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use quick_cache::{sync::Cache, Weighter};
use serde::{Deserialize, Serialize};
use std::{
    io::{BufReader, Read},
    ops::Range,
    sync::Arc,
};

use crate::{
    config::CachingConfig,
    format::{
        format_constants::{self, CompressionAlgorithmBin, FileTypeBin, SpecVersionBin},
        manifest::Manifest,
        serializers::{
            deserialize_manifest, deserialize_snapshot, deserialize_transaction_log,
            serialize_manifest, serialize_snapshot, serialize_transaction_log,
        },
        snapshot::{Snapshot, SnapshotInfo},
        transaction_log::TransactionLog,
        ChunkId, ChunkOffset, IcechunkFormatError, ManifestId, SnapshotId,
    },
    private,
    repository::{RepositoryError, RepositoryResult},
    storage::{self, Reader},
    Storage,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(from = "AssetManagerSerializer")]
pub struct AssetManager {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    num_snapshot_nodes: u64,
    num_chunk_refs: u64,
    num_transaction_changes: u64,
    num_bytes_attributes: u64,
    num_bytes_chunks: u64,
    compression_level: u8,
    #[serde(skip)]
    snapshot_cache: Cache<SnapshotId, Arc<Snapshot>, FileWeighter>,
    #[serde(skip)]
    manifest_cache: Cache<ManifestId, Arc<Manifest>, FileWeighter>,
    #[serde(skip)]
    transactions_cache: Cache<SnapshotId, Arc<TransactionLog>, FileWeighter>,
    #[serde(skip)]
    chunk_cache: Cache<(ChunkId, Range<ChunkOffset>), Bytes, FileWeighter>,
}

impl private::Sealed for AssetManager {}

#[derive(Debug, Deserialize)]
struct AssetManagerSerializer {
    storage: Arc<dyn Storage + Send + Sync>,
    storage_settings: storage::Settings,
    num_snapshot_nodes: u64,
    num_chunk_refs: u64,
    num_transaction_changes: u64,
    num_bytes_attributes: u64,
    num_bytes_chunks: u64,
    compression_level: u8,
}

impl From<AssetManagerSerializer> for AssetManager {
    fn from(value: AssetManagerSerializer) -> Self {
        AssetManager::new(
            value.storage,
            value.storage_settings,
            value.num_snapshot_nodes,
            value.num_chunk_refs,
            value.num_transaction_changes,
            value.num_bytes_attributes,
            value.num_bytes_chunks,
            value.compression_level,
        )
    }
}

impl AssetManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        num_snapshot_nodes: u64,
        num_chunk_refs: u64,
        num_transaction_changes: u64,
        num_bytes_attributes: u64,
        num_bytes_chunks: u64,
        compression_level: u8,
    ) -> Self {
        Self {
            num_snapshot_nodes,
            num_chunk_refs,
            num_transaction_changes,
            num_bytes_attributes,
            num_bytes_chunks,
            compression_level,
            storage,
            storage_settings,
            snapshot_cache: Cache::with_weighter(1, num_snapshot_nodes, FileWeighter),
            manifest_cache: Cache::with_weighter(1, num_chunk_refs, FileWeighter),
            transactions_cache: Cache::with_weighter(
                0,
                num_transaction_changes,
                FileWeighter,
            ),
            chunk_cache: Cache::with_weighter(0, num_bytes_chunks, FileWeighter),
        }
    }

    pub fn new_no_cache(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        compression_level: u8,
    ) -> Self {
        Self::new(storage, storage_settings, 0, 0, 0, 0, 0, compression_level)
    }

    pub fn new_with_config(
        storage: Arc<dyn Storage + Send + Sync>,
        storage_settings: storage::Settings,
        config: &CachingConfig,
        compression_level: u8,
    ) -> Self {
        Self::new(
            storage,
            storage_settings,
            config.num_snapshot_nodes(),
            config.num_chunk_refs(),
            config.num_transaction_changes(),
            config.num_bytes_attributes(),
            config.num_bytes_chunks(),
            compression_level,
        )
    }

    pub async fn write_manifest(&self, manifest: Arc<Manifest>) -> RepositoryResult<u64> {
        let manifest_c = Arc::clone(&manifest);
        let res = write_new_manifest(
            manifest_c,
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
        )
        .await?;
        self.manifest_cache.insert(manifest.id.clone(), manifest);
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

    pub async fn write_snapshot(&self, snapshot: Arc<Snapshot>) -> RepositoryResult<()> {
        let snapshot_c = Arc::clone(&snapshot);
        write_new_snapshot(
            snapshot_c,
            self.compression_level,
            self.storage.as_ref(),
            &self.storage_settings,
        )
        .await?;
        let snapshot_id = snapshot.id().clone();
        self.snapshot_cache.insert(snapshot_id, snapshot);
        Ok(())
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
    ) -> RepositoryResult<()> {
        let log_c = Arc::clone(&log);
        write_new_tx_log(
            transaction_id.clone(),
            log_c,
            self.compression_level,
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

    /// Returns the sequence of parents of the current session, in order of latest first.
    /// Output stream includes snapshot_id argument
    pub async fn snapshot_ancestry(
        self: Arc<Self>,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<impl Stream<Item = RepositoryResult<SnapshotInfo>>> {
        let mut this = self.fetch_snapshot(snapshot_id).await?;
        let stream = try_stream! {
            yield this.as_ref().into();
            while let Some(parent) = this.parent_id() {
                let snap = self.fetch_snapshot(parent).await?;
                let info: SnapshotInfo = snap.as_ref().into();
                yield info;
                this = snap;
            }
        };
        Ok(stream)
    }

    pub async fn get_snapshot_last_modified(
        &self,
        snap: &SnapshotId,
    ) -> RepositoryResult<DateTime<Utc>> {
        Ok(self.storage.get_snapshot_last_modified(&self.storage_settings, snap).await?)
    }
}

fn binary_file_header(
    spec_version: SpecVersionBin,
    file_type: FileTypeBin,
    compression_algorithm: CompressionAlgorithmBin,
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
    buffer.push(spec_version as u8);
    buffer.push(file_type as u8);
    // compression
    buffer.push(compression_algorithm as u8);
    buffer
}

fn check_header(
    read: &mut (dyn Read + Unpin + Send),
    file_type: FileTypeBin,
) -> RepositoryResult<(SpecVersionBin, CompressionAlgorithmBin)> {
    let mut buf = [0; 12];
    read.read_exact(&mut buf)?;
    // Magic numbers
    if format_constants::ICECHUNK_FORMAT_MAGIC_BYTES != buf {
        return Err(RepositoryError::FormatError(
            IcechunkFormatError::InvalidMagicNumbers,
        ));
    }

    let mut buf = [0; 24];
    // ignore implementation name
    read.read_exact(&mut buf)?;

    let mut spec_version = 0;
    read.read_exact(std::slice::from_mut(&mut spec_version))?;

    let spec_version = spec_version.try_into().map_err(|_| {
        RepositoryError::FormatError(IcechunkFormatError::InvalidSpecVersion)
    })?;

    let mut actual_file_type_int = 0;
    read.read_exact(std::slice::from_mut(&mut actual_file_type_int))?;

    let actual_file_type: FileTypeBin =
        actual_file_type_int.try_into().map_err(|_| {
            RepositoryError::FormatError(IcechunkFormatError::InvalidFileType {
                expected: file_type,
                got: actual_file_type_int,
            })
        })?;

    if actual_file_type != file_type {
        return Err(RepositoryError::FormatError(IcechunkFormatError::InvalidFileType {
            expected: file_type,
            got: actual_file_type_int,
        }));
    }

    let mut compression = 0;
    read.read_exact(std::slice::from_mut(&mut compression))?;

    let compression = compression.try_into().map_err(|_| {
        RepositoryError::FormatError(IcechunkFormatError::InvalidCompressionAlgorithm)
    })?;

    Ok((spec_version, compression))
}

async fn write_new_manifest(
    new_manifest: Arc<Manifest>,
    compression_level: u8,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<u64> {
    use format_constants::*;
    let metadata = vec![
        (
            LATEST_ICECHUNK_FORMAT_VERSION_METADATA_KEY.to_string(),
            (SpecVersionBin::current() as u8).to_string(),
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

    let id = new_manifest.id.clone();
    // TODO: we should compress only when the manifest reaches a certain size
    // but then, we would need to include metadata to know if it's compressed or not
    let buffer = tokio::task::spawn_blocking(move || {
        let buffer = binary_file_header(
            SpecVersionBin::current(),
            FileTypeBin::Manifest,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;

        serialize_manifest(
            new_manifest.as_ref(),
            SpecVersionBin::current(),
            &mut compressor,
        )?;

        compressor.finish().map_err(RepositoryError::IOError)
    })
    .await??;

    let len = buffer.len() as u64;
    storage.write_manifest(storage_settings, id.clone(), metadata, buffer.into()).await?;
    Ok(len)
}

async fn fetch_manifest(
    manifest_id: &ManifestId,
    manifest_size: u64,
    storage: &(dyn Storage + Send + Sync),
    storage_settings: &storage::Settings,
) -> RepositoryResult<Arc<Manifest>> {
    let reader = if manifest_size > 0 {
        storage
            .fetch_manifest_known_size(storage_settings, manifest_id, manifest_size)
            .await?
    } else {
        Reader::Asynchronous(
            storage.fetch_manifest_unknown_size(storage_settings, manifest_id).await?,
        )
    };

    tokio::task::spawn_blocking(move || {
        let (spec_version, decompressor) =
            check_and_get_decompressor(reader, FileTypeBin::Manifest)?;
        deserialize_manifest(spec_version, decompressor)
            .map_err(RepositoryError::DeserializationError)
    })
    .await?
    .map(Arc::new)
}

fn check_and_get_decompressor(
    data: Reader,
    file_type: FileTypeBin,
) -> RepositoryResult<(SpecVersionBin, Box<dyn Read + Send>)> {
    let mut sync_read = data.into_read();
    let (spec_version, compression) = check_header(sync_read.as_mut(), file_type)?;
    debug_assert_eq!(compression, CompressionAlgorithmBin::Zstd);
    // We find a performance impact if we don't buffer here
    let decompressor =
        BufReader::with_capacity(1_024, zstd::stream::Decoder::new(sync_read)?);
    Ok((spec_version, Box::new(decompressor)))
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
            (SpecVersionBin::current() as u8).to_string(),
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

    let id = new_snapshot.id().clone();
    let buffer = tokio::task::spawn_blocking(move || {
        let buffer = binary_file_header(
            SpecVersionBin::current(),
            FileTypeBin::Snapshot,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;

        serialize_snapshot(
            new_snapshot.as_ref(),
            SpecVersionBin::current(),
            &mut compressor,
        )?;

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

    tokio::task::spawn_blocking(move || {
        let (spec_version, decompressor) = check_and_get_decompressor(
            Reader::Asynchronous(read),
            FileTypeBin::Snapshot,
        )?;
        deserialize_snapshot(spec_version, decompressor)
            .map_err(RepositoryError::DeserializationError)
    })
    .await?
    .map(Arc::new)
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
            (SpecVersionBin::current() as u8).to_string(),
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
            SpecVersionBin::current(),
            FileTypeBin::TransactionLog,
            CompressionAlgorithmBin::Zstd,
        );
        let mut compressor =
            zstd::stream::Encoder::new(buffer, compression_level as i32)?;
        serialize_transaction_log(
            new_log.as_ref(),
            SpecVersionBin::current(),
            &mut compressor,
        )?;
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

    tokio::task::spawn_blocking(move || {
        let (spec_version, decompressor) = check_and_get_decompressor(
            Reader::Asynchronous(read),
            FileTypeBin::TransactionLog,
        )?;
        deserialize_transaction_log(spec_version, decompressor)
            .map_err(RepositoryError::DeserializationError)
    })
    .await?
    .map(Arc::new)
}

#[derive(Debug, Clone)]
struct FileWeighter;

impl Weighter<ManifestId, Arc<Manifest>> for FileWeighter {
    fn weight(&self, _: &ManifestId, val: &Arc<Manifest>) -> u64 {
        val.len() as u64
    }
}

impl Weighter<SnapshotId, Arc<Snapshot>> for FileWeighter {
    fn weight(&self, _: &SnapshotId, val: &Arc<Snapshot>) -> u64 {
        val.len() as u64
    }
}

impl Weighter<(ChunkId, Range<ChunkOffset>), Bytes> for FileWeighter {
    fn weight(&self, _: &(ChunkId, Range<ChunkOffset>), val: &Bytes) -> u64 {
        val.len() as u64
    }
}

impl Weighter<SnapshotId, Arc<TransactionLog>> for FileWeighter {
    fn weight(&self, _: &SnapshotId, val: &Arc<TransactionLog>) -> u64 {
        val.len() as u64
    }
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
        let manager = AssetManager::new_no_cache(backend.clone(), settings.clone(), 1);

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
        let pre_existing_manifest =
            Manifest::from_iter(vec![ci1].into_iter()).await?.unwrap();
        let pre_existing_manifest = Arc::new(pre_existing_manifest);
        let pre_existing_id = &pre_existing_manifest.id;
        let pre_size = manager.write_manifest(Arc::clone(&pre_existing_manifest)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = AssetManager::new_with_config(
            Arc::clone(&logging_c),
            settings,
            &CachingConfig::default(),
            1,
        );

        let manifest =
            Arc::new(Manifest::from_iter(vec![ci2].into_iter()).await?.unwrap());
        let id = &manifest.id;
        let size = caching.write_manifest(Arc::clone(&manifest)).await?;

        assert_eq!(caching.fetch_manifest(id, size).await?, manifest);
        assert_eq!(caching.fetch_manifest(id, size).await?, manifest);
        // when we insert we cache, so no fetches
        assert_eq!(logging.fetch_operations(), vec![]);

        // first time it sees an ID it calls the backend
        assert_eq!(
            caching.fetch_manifest(pre_existing_id, pre_size).await?,
            pre_existing_manifest
        );
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifest_splitting".to_string(), pre_existing_id.to_string())]
        );

        // only calls backend once
        assert_eq!(
            caching.fetch_manifest(pre_existing_id, pre_size).await?,
            pre_existing_manifest
        );
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifest_splitting".to_string(), pre_existing_id.to_string())]
        );

        // other walues still cached
        assert_eq!(caching.fetch_manifest(id, size).await?, manifest);
        assert_eq!(
            logging.fetch_operations(),
            vec![("fetch_manifest_splitting".to_string(), pre_existing_id.to_string())]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_caching_storage_has_limit() -> Result<(), Box<dyn std::error::Error>> {
        let backend: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let settings = storage::Settings::default();
        let manager = AssetManager::new_no_cache(backend.clone(), settings.clone(), 1);

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

        let manifest1 =
            Arc::new(Manifest::from_iter(vec![ci1, ci2, ci3]).await?.unwrap());
        let id1 = &manifest1.id;
        let size1 = manager.write_manifest(Arc::clone(&manifest1)).await?;
        let manifest2 =
            Arc::new(Manifest::from_iter(vec![ci4, ci5, ci6]).await?.unwrap());
        let id2 = &manifest2.id;
        let size2 = manager.write_manifest(Arc::clone(&manifest2)).await?;
        let manifest3 =
            Arc::new(Manifest::from_iter(vec![ci7, ci8, ci9]).await?.unwrap());
        let id3 = &manifest3.id;
        let size3 = manager.write_manifest(Arc::clone(&manifest3)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&backend)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let caching = AssetManager::new_with_config(
            logging_c,
            settings,
            // the cache can only fit 6 refs.
            &CachingConfig {
                num_snapshot_nodes: Some(0),
                num_chunk_refs: Some(6),
                num_transaction_changes: Some(0),
                num_bytes_attributes: Some(0),
                num_bytes_chunks: Some(0),
            },
            1,
        );

        // we keep asking for all 3 items, but the cache can only fit 2
        for _ in 0..20 {
            assert_eq!(caching.fetch_manifest(id1, size1).await?, manifest1);
            assert_eq!(caching.fetch_manifest(id2, size2).await?, manifest2);
            assert_eq!(caching.fetch_manifest(id3, size3).await?, manifest3);
        }
        // after the initial warming requests, we only request the file that doesn't fit in the cache
        assert_eq!(logging.fetch_operations()[10..].iter().unique().count(), 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dont_fetch_asset_twice() -> Result<(), Box<dyn std::error::Error>> {
        // Test that two concurrent requests for the same manifest doesn't generate two
        // object_store requests, one of them must wait
        let storage: Arc<dyn Storage + Send + Sync> = new_in_memory_storage()?;
        let settings = storage::Settings::default();
        let manager =
            Arc::new(AssetManager::new_no_cache(storage.clone(), settings.clone(), 1));

        // some reasonable size so it takes some time to parse
        let manifest = Manifest::from_iter((0..5_000).map(|_| ChunkInfo {
            node: NodeId::random(),
            coord: ChunkIndices(Vec::from([rand::random(), rand::random()])),
            payload: ChunkPayload::Inline("hello".into()),
        }))
        .await
        .unwrap()
        .unwrap();
        let manifest_id = manifest.id.clone();
        let size = manager.write_manifest(Arc::new(manifest)).await?;

        let logging = Arc::new(LoggingStorage::new(Arc::clone(&storage)));
        let logging_c: Arc<dyn Storage + Send + Sync> = logging.clone();
        let manager = Arc::new(AssetManager::new_with_config(
            logging_c.clone(),
            logging_c.default_settings(),
            &CachingConfig::default(),
            1,
        ));

        let manager_c = Arc::new(manager);
        let manager_cc = Arc::clone(&manager_c);
        let manifest_id_c = manifest_id.clone();

        let res1 = tokio::task::spawn(async move {
            manager_c.fetch_manifest(&manifest_id_c, size).await
        });
        let res2 = tokio::task::spawn(async move {
            manager_cc.fetch_manifest(&manifest_id, size).await
        });

        assert!(res1.await?.is_ok());
        assert!(res2.await?.is_ok());
        assert_eq!(logging.fetch_operations().len(), 1);
        Ok(())
    }
}
