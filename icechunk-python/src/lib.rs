mod errors;
mod storage;
mod streams;

use std::{borrow::Cow, sync::Arc};

use ::icechunk::{format::ChunkOffset, Store};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use errors::{PyIcechunkStoreError, PyIcechunkStoreResult};
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    format::{manifest::VirtualChunkRef, ChunkLength},
    refs::Ref,
    repository::{ChangeSet, VirtualChunkLocation},
    storage::virtual_ref::ObjectStoreVirtualChunkResolverConfig,
    zarr::{
        ConsolidatedStore, ObjectId, RepositoryConfig, StorageConfig, StoreError,
        StoreOptions, VersionInfo,
    },
    Repository, SnapshotMetadata,
};
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{PyNone, PyString},
};
use storage::{PyS3Credentials, PyStorageConfig, PyVirtualRefConfig};
use streams::PyAsyncGenerator;
use tokio::{
    runtime::Runtime,
    sync::{Mutex, RwLock},
};

#[pyclass]
struct PyIcechunkStore {
    consolidated: ConsolidatedStore,
    store: Arc<RwLock<Store>>,
}

#[pyclass(name = "StoreConfig")]
#[derive(Clone, Debug, Default)]
struct PyStoreConfig {
    #[pyo3(get, set)]
    pub get_partial_values_concurrency: Option<u16>,
    #[pyo3(get, set)]
    pub inline_chunk_threshold_bytes: Option<u16>,
    #[pyo3(get, set)]
    pub unsafe_overwrite_refs: Option<bool>,
    #[pyo3(get, set)]
    pub virtual_ref_config: Option<PyVirtualRefConfig>,
}

impl From<&PyStoreConfig> for RepositoryConfig {
    fn from(config: &PyStoreConfig) -> Self {
        RepositoryConfig {
            version: None,
            inline_chunk_threshold_bytes: config.inline_chunk_threshold_bytes,
            unsafe_overwrite_refs: config.unsafe_overwrite_refs,
            change_set_bytes: None,
            virtual_ref_config: config
                .virtual_ref_config
                .as_ref()
                .map(ObjectStoreVirtualChunkResolverConfig::from),
        }
    }
}

impl From<&PyStoreConfig> for StoreOptions {
    fn from(config: &PyStoreConfig) -> Self {
        if let Some(get_partial_values_concurrency) =
            config.get_partial_values_concurrency
        {
            StoreOptions { get_partial_values_concurrency }
        } else {
            StoreOptions::default()
        }
    }
}

#[pymethods]
impl PyStoreConfig {
    #[new]
    #[pyo3(signature = (
        get_partial_values_concurrency = None,
        inline_chunk_threshold_bytes = None,
        unsafe_overwrite_refs = None,
        virtual_ref_config = None,
    ))]
    fn new(
        get_partial_values_concurrency: Option<u16>,
        inline_chunk_threshold_bytes: Option<u16>,
        unsafe_overwrite_refs: Option<bool>,
        virtual_ref_config: Option<PyVirtualRefConfig>,
    ) -> Self {
        PyStoreConfig {
            get_partial_values_concurrency,
            inline_chunk_threshold_bytes,
            unsafe_overwrite_refs,
            virtual_ref_config,
        }
    }
}
#[pyclass(name = "SnapshotMetadata")]
#[derive(Clone, Debug)]
pub struct PySnapshotMetadata {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    written_at: DateTime<Utc>,
    #[pyo3(get)]
    message: String,
}

impl From<SnapshotMetadata> for PySnapshotMetadata {
    fn from(val: SnapshotMetadata) -> Self {
        PySnapshotMetadata {
            id: val.id.to_string(),
            written_at: val.written_at,
            message: val.message,
        }
    }
}

type KeyRanges = Vec<(String, (Option<ChunkOffset>, Option<ChunkOffset>))>;

impl PyIcechunkStore {
    pub(crate) fn consolidated(&self) -> &ConsolidatedStore {
        &self.consolidated
    }

    async fn store_exists(storage: StorageConfig) -> PyIcechunkStoreResult<bool> {
        let storage = storage
            .make_cached_storage()
            .await
            .map_err(PyIcechunkStoreError::UnkownError)?;
        let exists = Repository::exists(storage.as_ref()).await?;
        Ok(exists)
    }

    async fn open_existing(
        storage: StorageConfig,
        read_only: bool,
        repository_config: RepositoryConfig,
        store_config: StoreOptions,
    ) -> Result<Self, String> {
        let repository = repository_config
            .with_version(VersionInfo::BranchTipRef(Ref::DEFAULT_BRANCH.to_string()));
        let consolidated =
            ConsolidatedStore { storage, repository, config: Some(store_config) };

        PyIcechunkStore::from_consolidated(consolidated, read_only).await
    }

    async fn create(
        storage: StorageConfig,
        repository_config: RepositoryConfig,
        store_config: StoreOptions,
    ) -> Result<Self, String> {
        let consolidated = ConsolidatedStore {
            storage,
            repository: repository_config,
            config: Some(store_config),
        };

        PyIcechunkStore::from_consolidated(consolidated, false).await
    }

    async fn from_consolidated(
        consolidated: ConsolidatedStore,
        read_only: bool,
    ) -> Result<Self, String> {
        let access_mode = if read_only {
            icechunk::zarr::AccessMode::ReadOnly
        } else {
            icechunk::zarr::AccessMode::ReadWrite
        };

        let store = Store::from_consolidated(&consolidated, access_mode).await?;
        let store = Arc::new(RwLock::new(store));
        Ok(Self { consolidated, store })
    }

    async fn as_consolidated(&self) -> PyIcechunkStoreResult<ConsolidatedStore> {
        let consolidated = self.consolidated.clone();

        let store = self.store.read().await;
        let version = store.current_version().await;
        let change_set = store.change_set_bytes().await?;

        let consolidated =
            consolidated.with_version(version).with_change_set_bytes(change_set)?;
        Ok(consolidated)
    }
}

fn mk_runtime() -> PyResult<Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| PyIcechunkStoreError::UnkownError(e.to_string()))?)
}

#[pyfunction]
fn pyicechunk_store_open_existing(
    storage: &PyStorageConfig,
    read_only: bool,
    config: PyStoreConfig,
) -> PyResult<PyIcechunkStore> {
    let storage = storage.into();
    let repository_config = (&config).into();
    let store_config = (&config).into();

    let rt = mk_runtime()?;
    rt.block_on(async move {
        PyIcechunkStore::open_existing(
            storage,
            read_only,
            repository_config,
            store_config,
        )
        .await
        .map_err(PyValueError::new_err)
    })
}

#[pyfunction]
fn async_pyicechunk_store_open_existing<'py>(
    py: Python<'py>,
    storage: &'py PyStorageConfig,
    read_only: bool,
    config: PyStoreConfig,
) -> PyResult<Bound<'py, PyAny>> {
    let storage = storage.into();
    let repository_config = (&config).into();
    let store_config = (&config).into();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        PyIcechunkStore::open_existing(
            storage,
            read_only,
            repository_config,
            store_config,
        )
        .await
        .map_err(PyValueError::new_err)
    })
}

#[pyfunction]
fn async_pyicechunk_store_exists<'py>(
    py: Python<'py>,
    storage: &'py PyStorageConfig,
) -> PyResult<Bound<'py, PyAny>> {
    let storage = storage.into();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        PyIcechunkStore::store_exists(storage).await.map_err(PyErr::from)
    })
}

#[pyfunction]
fn pyicechunk_store_exists(storage: &PyStorageConfig) -> PyResult<bool> {
    let storage = storage.into();
    let rt = mk_runtime()?;
    rt.block_on(async move {
        PyIcechunkStore::store_exists(storage).await.map_err(PyErr::from)
    })
}

#[pyfunction]
fn async_pyicechunk_store_create<'py>(
    py: Python<'py>,
    storage: &'py PyStorageConfig,
    config: PyStoreConfig,
) -> PyResult<Bound<'py, PyAny>> {
    let storage = storage.into();
    let repository_config = (&config).into();
    let store_config = (&config).into();
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        PyIcechunkStore::create(storage, repository_config, store_config)
            .await
            .map_err(PyValueError::new_err)
    })
}

#[pyfunction]
fn pyicechunk_store_create(
    storage: &PyStorageConfig,
    config: PyStoreConfig,
) -> PyResult<PyIcechunkStore> {
    let storage = storage.into();
    let repository_config = (&config).into();
    let store_config = (&config).into();
    let rt = mk_runtime()?;
    rt.block_on(async move {
        PyIcechunkStore::create(storage, repository_config, store_config)
            .await
            .map_err(PyValueError::new_err)
    })
}

#[pyfunction]
fn pyicechunk_store_from_bytes(
    bytes: Cow<[u8]>,
    read_only: bool,
) -> PyResult<PyIcechunkStore> {
    // FIXME: Use rmp_serde instead of serde_json to optimize performance
    let consolidated: ConsolidatedStore = serde_json::from_slice(&bytes)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    let rt = mk_runtime()?;
    let store = rt.block_on(async move {
        PyIcechunkStore::from_consolidated(consolidated, read_only)
            .await
            .map_err(PyValueError::new_err)
    })?;

    Ok(store)
}

#[pymethods]
impl PyIcechunkStore {
    fn __eq__(&self, other: &Self) -> bool {
        self.consolidated.storage == other.consolidated().storage
    }

    fn as_bytes(&self) -> PyResult<Cow<[u8]>> {
        let consolidated =
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.as_consolidated())?;

        // FIXME: Use rmp_serde instead of serde_json to optimize performance
        let serialized = serde_json::to_vec(&consolidated)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Cow::Owned(serialized))
    }

    fn set_read_only(&self, read_only: bool) -> PyResult<()> {
        let access_mode = if read_only {
            icechunk::zarr::AccessMode::ReadOnly
        } else {
            icechunk::zarr::AccessMode::ReadWrite
        };

        let mut writeable_store = self.store.blocking_write();
        writeable_store.set_mode(access_mode);
        Ok(())
    }

    fn with_read_only(&self, read_only: bool) -> PyResult<PyIcechunkStore> {
        let access_mode = if read_only {
            icechunk::zarr::AccessMode::ReadOnly
        } else {
            icechunk::zarr::AccessMode::ReadWrite
        };

        let readable_store = self.store.blocking_read();
        let consolidated =
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.as_consolidated())?;
        let store = Arc::new(RwLock::new(readable_store.with_access_mode(access_mode)));
        Ok(PyIcechunkStore { consolidated, store })
    }

    fn async_checkout_snapshot<'py>(
        &'py self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_checkout_snapshot(store, snapshot_id).await
        })
    }

    fn checkout_snapshot<'py>(
        &'py self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyNone>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_checkout_snapshot(store, snapshot_id).await?;
            Ok(PyNone::get(py).to_owned())
        })
    }

    fn async_checkout_branch<'py>(
        &'py self,
        py: Python<'py>,
        branch: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_checkout_branch(store, branch).await
        })
    }

    fn checkout_branch<'py>(
        &'py self,
        py: Python<'py>,
        branch: String,
    ) -> PyResult<Bound<'py, PyNone>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_checkout_branch(store, branch).await?;
            Ok(PyNone::get(py).to_owned())
        })
    }

    fn async_checkout_tag<'py>(
        &'py self,
        py: Python<'py>,
        tag: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_checkout_tag(store, tag).await
        })
    }

    fn checkout_tag<'py>(
        &'py self,
        py: Python<'py>,
        tag: String,
    ) -> PyResult<Bound<'py, PyNone>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_checkout_tag(store, tag).await?;
            Ok(PyNone::get(py).to_owned())
        })
    }

    #[getter]
    fn snapshot_id(&self) -> PyIcechunkStoreResult<String> {
        let store = self.store.blocking_read();
        let snapshot_id =
            pyo3_async_runtimes::tokio::get_runtime().block_on(store.snapshot_id());
        Ok(snapshot_id.to_string())
    }

    fn async_commit<'py>(
        &'py self,
        py: Python<'py>,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_commit(store, message).await
        })
    }

    fn commit<'py>(
        &'py self,
        py: Python<'py>,
        message: String,
    ) -> PyResult<Bound<'py, PyString>> {
        let store = Arc::clone(&self.store);

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let res = do_commit(store, message).await?;
            Ok(PyString::new(py, res.as_str()))
        })
    }

    fn async_merge<'py>(
        &self,
        py: Python<'py>,
        change_set_bytes: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_merge(store, change_set_bytes).await
        })
    }

    fn merge(&self, change_set_bytes: Vec<u8>) -> PyIcechunkStoreResult<()> {
        let store = Arc::clone(&self.store);

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_merge(store, change_set_bytes).await?;
            Ok(())
        })
    }

    fn change_set_bytes(&self) -> PyIcechunkStoreResult<Vec<u8>> {
        let store = self.store.blocking_read();
        let res = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(store.change_set_bytes())
            .map_err(PyIcechunkStoreError::from)?;
        Ok(res)
    }

    #[getter]
    fn branch(&self) -> PyIcechunkStoreResult<Option<String>> {
        let store = self.store.blocking_read();
        let current_branch = store.current_branch();
        Ok(current_branch.clone())
    }

    #[getter]
    fn has_uncommitted_changes(&self) -> PyIcechunkStoreResult<bool> {
        let store = self.store.blocking_read();
        let has_uncommitted_changes = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(store.has_uncommitted_changes());
        Ok(has_uncommitted_changes)
    }

    fn async_reset<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let changes = do_reset(store).await?;
            Ok(changes)
        })
    }

    fn reset(&self) -> PyIcechunkStoreResult<Cow<[u8]>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let changes = do_reset(store).await?;
            Ok(Cow::Owned(changes))
        })
    }

    fn async_new_branch<'py>(
        &'py self,
        py: Python<'py>,
        branch_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_new_branch(store, branch_name).await
        })
    }

    fn new_branch(&self, branch_name: String) -> PyIcechunkStoreResult<String> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let res = do_new_branch(store, branch_name).await?;
            Ok(res)
        })
    }

    fn async_reset_branch<'py>(
        &'py self,
        py: Python<'py>,
        to_snapshot: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_reset_branch(store, to_snapshot).await
        })
    }

    fn reset_branch(&self, to_snapshot: String) -> PyIcechunkStoreResult<()> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_reset_branch(store, to_snapshot).await?;
            Ok(())
        })
    }

    fn async_tag<'py>(
        &'py self,
        py: Python<'py>,
        tag: String,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        // The commit mechanism is async and calls tokio::spawn so we need to use the
        // pyo3_async_runtimes::tokio helper to run the async function in the tokio runtime
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_tag(store, tag, snapshot_id).await
        })
    }

    fn tag(&self, tag: String, snapshot_id: String) -> PyIcechunkStoreResult<()> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_tag(store, tag, snapshot_id).await?;
            Ok(())
        })
    }

    fn ancestry(&self) -> PyIcechunkStoreResult<Vec<PySnapshotMetadata>> {
        // TODO: this holds everything in memory
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let store = self.store.read().await;
            let ancestry = store
                .ancestry()
                .await?
                .map_ok(Into::<PySnapshotMetadata>::into)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(ancestry)
        })
    }

    fn async_ancestry(&self) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let store = self.store.read().await;
                store.ancestry().await
            })?
            .map_ok(|parent| {
                let parent = Into::<PySnapshotMetadata>::into(parent);
                #[allow(deprecated)]
                Python::with_gil(|py| parent.into_py(py))
            });
        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    fn is_empty<'py>(
        &'py self,
        py: Python<'py>,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let is_empty = store
                .read()
                .await
                .is_empty(&prefix)
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(is_empty)
        })
    }

    fn clear<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.write().await.clear().await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    fn sync_clear(&self) -> PyIcechunkStoreResult<()> {
        let store = Arc::clone(&self.store);

        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            store.write().await.clear().await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (key, byte_range = None))]
    fn get<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        byte_range: Option<(Option<ChunkOffset>, Option<ChunkOffset>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let byte_range = byte_range.unwrap_or((None, None)).into();
            let data = store.read().await.get(&key, &byte_range).await;
            // We need to distinguish the "safe" case of trying to fetch an uninitialized key
            // from other types of errors, we use PyKeyError exception for that
            match data {
                Ok(data) => Ok(Vec::from(data)),
                Err(StoreError::NotFound(_)) => Err(PyKeyError::new_err(key)),
                Err(err) => Err(PyIcechunkStoreError::StoreError(err).into()),
            }
        })
    }

    fn get_partial_values<'py>(
        &'py self,
        py: Python<'py>,
        key_ranges: KeyRanges,
    ) -> PyResult<Bound<'py, PyAny>> {
        let iter = key_ranges.into_iter().map(|r| (r.0, r.1.into()));
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let readable_store = store.read().await;
            let partial_values_stream = readable_store
                .get_partial_values(iter)
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;

            // FIXME: this processing is hiding errors in certain keys
            let result = partial_values_stream
                .into_iter()
                // If we want to error instead of returning None we can collect into
                // a Result<Vec<_>, _> and short circuit
                .map(|x| x.map(Vec::from).ok())
                .collect::<Vec<_>>();

            Ok(result)
        })
    }

    fn exists<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let exists = store
                .read()
                .await
                .exists(&key)
                .await
                .map_err(PyIcechunkStoreError::from)?;

            Ok(exists)
        })
    }

    #[getter]
    fn supports_deletes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_deletes = self.store.blocking_read().supports_deletes()?;
        Ok(supports_deletes)
    }

    #[getter]
    fn supports_writes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_writes = self.store.blocking_read().supports_writes()?;
        Ok(supports_writes)
    }

    fn set<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        // Most of our async functions use structured coroutines so they can be called directly from
        // the python event loop, but in this case downstream objectstore crate  calls tokio::spawn
        // when emplacing chunks into its storage backend. Calling tokio::spawn requires an active
        // tokio runtime so we use the pyo3_async_runtimes::tokio helper to do this
        // In the future this will hopefully not be necessary,
        // see this tracking issue: https://github.com/PyO3/pyo3/issues/1632
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let store = store.read().await;
            store
                .set(&key, Bytes::from(value))
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    fn set_if_not_exists<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let store = store.read().await;
            store
                .set_if_not_exists(&key, Bytes::from(value))
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    fn async_set_virtual_ref<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        location: String,
        offset: ChunkOffset,
        length: ChunkLength,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            do_set_virtual_ref(store, key, location, offset, length).await
        })
    }

    fn set_virtual_ref(
        &self,
        key: String,
        location: String,
        offset: ChunkOffset,
        length: ChunkLength,
    ) -> PyIcechunkStoreResult<()> {
        let store = Arc::clone(&self.store);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            do_set_virtual_ref(store, key, location, offset, length).await?;
            Ok(())
        })
    }

    fn delete<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.read().await.delete(&key).await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    #[getter]
    fn supports_partial_writes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_partial_writes =
            self.store.blocking_read().supports_partial_writes()?;
        Ok(supports_partial_writes)
    }

    fn set_partial_values<'py>(
        &'py self,
        py: Python<'py>,
        key_start_values: Vec<(String, ChunkOffset, Vec<u8>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // We need to get our own copy of the keys to pass to the downstream store function because that
        // function requires a Vec<&str, which we cannot borrow from when we are borrowing from the tuple
        //
        // There is a choice made here, to clone the keys. This is because the keys are small and the
        // alternative is to have to clone the bytes instead of a copy which is usually more expensive
        // depending on the size of the chunk.
        let keys =
            key_start_values.iter().map(|(key, _, _)| key.clone()).collect::<Vec<_>>();

        let store = Arc::clone(&self.store);

        // Most of our async functions use structured coroutines so they can be called directly from
        // the python event loop, but in this case downstream objectstore crate  calls tokio::spawn
        // when emplacing chunks into its storage backend. Calling tokio::spawn requires an active
        // tokio runtime so we use the pyo3_async_runtimes::tokio helper to do this
        // In the future this will hopefully not be necessary,
        // see this tracking issue: https://github.com/PyO3/pyo3/issues/1632
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let store = store.read().await;

            let mapped_to_bytes = key_start_values.into_iter().enumerate().map(
                |(i, (_key, offset, value))| {
                    (keys[i].as_str(), offset, Bytes::from(value))
                },
            );

            store
                .set_partial_values(mapped_to_bytes)
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    #[getter]
    fn supports_listing(&self) -> PyIcechunkStoreResult<bool> {
        let supports_listing = self.store.blocking_read().supports_listing()?;
        Ok(supports_listing)
    }

    fn list(&self) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        #[allow(deprecated)]
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let store = self.store.read().await;
                store.list().await
            })?
            .map_ok(|s| Python::with_gil(|py| s.to_object(py)));

        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    fn list_prefix(&self, prefix: String) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        #[allow(deprecated)]
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let store = self.store.read().await;
                store.list_prefix(prefix.as_str()).await
            })?
            .map_ok(|s| Python::with_gil(|py| s.to_object(py)));
        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    fn list_dir(&self, prefix: String) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        #[allow(deprecated)]
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move {
                let store = self.store.read().await;
                store.list_dir(prefix.as_str()).await
            })?
            .map_ok(|s| Python::with_gil(|py| s.to_object(py)));
        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }
}

async fn do_commit(store: Arc<RwLock<Store>>, message: String) -> PyResult<String> {
    let store = store.write().await;
    let oid = store.commit(&message).await.map_err(PyIcechunkStoreError::from)?;
    Ok(String::from(&oid))
}

async fn do_checkout_snapshot(
    store: Arc<RwLock<Store>>,
    snapshot_id: String,
) -> PyResult<()> {
    let snapshot_id = ObjectId::try_from(snapshot_id.as_str()).map_err(|e| {
        PyIcechunkStoreError::UnkownError(format!(
            "Error checking out snapshot {snapshot_id}: {e}"
        ))
    })?;

    let mut store = store.write().await;
    store
        .checkout(VersionInfo::SnapshotId(snapshot_id))
        .await
        .map_err(PyIcechunkStoreError::from)?;
    Ok(())
}

async fn do_checkout_branch(store: Arc<RwLock<Store>>, branch: String) -> PyResult<()> {
    let mut store = store.write().await;
    store
        .checkout(VersionInfo::BranchTipRef(branch))
        .await
        .map_err(PyIcechunkStoreError::from)?;
    Ok(())
}

async fn do_checkout_tag(store: Arc<RwLock<Store>>, tag: String) -> PyResult<()> {
    let mut store = store.write().await;
    store.checkout(VersionInfo::TagRef(tag)).await.map_err(PyIcechunkStoreError::from)?;
    Ok(())
}

async fn do_merge(
    store: Arc<RwLock<Store>>,
    other_change_set_bytes: Vec<u8>,
) -> PyResult<()> {
    let change_set = ChangeSet::import_from_bytes(&other_change_set_bytes)
        .map_err(PyIcechunkStoreError::from)?;

    let store = store.write().await;
    store.merge(change_set).await;
    Ok(())
}

async fn do_reset<'py>(store: Arc<RwLock<Store>>) -> PyResult<Vec<u8>> {
    let changes =
        store.write().await.reset().await.map_err(PyIcechunkStoreError::StoreError)?;
    let serialized_changes =
        changes.export_to_bytes().map_err(PyIcechunkStoreError::from)?;
    Ok(serialized_changes)
}

async fn do_new_branch<'py>(
    store: Arc<RwLock<Store>>,
    branch_name: String,
) -> PyResult<String> {
    let mut writeable_store = store.write().await;
    let (oid, _version) = writeable_store
        .new_branch(&branch_name)
        .await
        .map_err(PyIcechunkStoreError::from)?;
    Ok(String::from(&oid))
}

async fn do_reset_branch<'py>(
    store: Arc<RwLock<Store>>,
    to_snapshot: String,
) -> PyResult<()> {
    let to_snapshot = ObjectId::try_from(to_snapshot.as_str())
        .map_err(|e| PyIcechunkStoreError::UnkownError(e.to_string()))?;
    let mut writeable_store = store.write().await;
    writeable_store
        .reset_branch(to_snapshot)
        .await
        .map_err(PyIcechunkStoreError::from)?;
    Ok(())
}

async fn do_tag<'py>(
    store: Arc<RwLock<Store>>,
    tag: String,
    snapshot_id: String,
) -> PyResult<()> {
    let store = store.read().await;
    let oid = ObjectId::try_from(snapshot_id.as_str())
        .map_err(|e| PyIcechunkStoreError::UnkownError(e.to_string()))?;
    store.tag(&tag, &oid).await.map_err(PyIcechunkStoreError::from)?;
    Ok(())
}

async fn do_set_virtual_ref(
    store: Arc<RwLock<Store>>,
    key: String,
    location: String,
    offset: ChunkOffset,
    length: ChunkLength,
) -> PyResult<()> {
    let virtual_ref = VirtualChunkRef {
        location: VirtualChunkLocation::Absolute(location),
        offset,
        length,
    };
    let mut store = store.write().await;
    store.set_virtual_ref(&key, virtual_ref).await.map_err(PyIcechunkStoreError::from)?;
    Ok(())
}

/// The icechunk Python module implemented in Rust.
#[pymodule]
fn _icechunk_python(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyStorageConfig>()?;
    m.add_class::<PyIcechunkStore>()?;
    m.add_class::<PyS3Credentials>()?;
    m.add_class::<PyStoreConfig>()?;
    m.add_class::<PySnapshotMetadata>()?;
    m.add_class::<PyVirtualRefConfig>()?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_exists, m)?)?;
    m.add_function(wrap_pyfunction!(async_pyicechunk_store_exists, m)?)?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_create, m)?)?;
    m.add_function(wrap_pyfunction!(async_pyicechunk_store_create, m)?)?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_open_existing, m)?)?;
    m.add_function(wrap_pyfunction!(async_pyicechunk_store_open_existing, m)?)?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_from_bytes, m)?)?;
    Ok(())
}
