mod errors;
mod storage;
mod streams;

use std::{pin::Pin, sync::Arc};

use ::icechunk::{format::ChunkOffset, zarr::StoreError, Store};
use bytes::Bytes;
use errors::{PyIcechunkStoreError, PyIcechunkStoreResult};
use futures::Stream;
use icechunk::{
    refs::Ref,
    zarr::{DatasetConfig, ObjectId, StorageConfig, StoreConfig, VersionInfo},
};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use storage::PyStorage;
use streams::PyAsyncStringGenerator;
use tokio::sync::{Mutex, RwLock};

#[pyclass]
struct PyIcechunkStore {
    store: Arc<RwLock<Store>>,
    rt: tokio::runtime::Runtime,
}

impl PyIcechunkStore {
    async fn open_existing(
        storage: StorageConfig,
        read_only: bool,
    ) -> Result<Self, String> {
        let access_mode = if read_only {
            icechunk::zarr::AccessMode::ReadOnly
        } else {
            icechunk::zarr::AccessMode::ReadWrite
        };
        let cached_storage = storage.into_cached();
        let dataset = DatasetConfig::existing(VersionInfo::BranchTipRef(
            Ref::DEFAULT_BRANCH.to_string(),
        ));
        let config = StoreConfig {
            storage: cached_storage,
            dataset,
            get_partial_values_concurrency: None,
        };

        let store = Store::from_config(&config, access_mode).await?;
        let store = Arc::new(RwLock::new(store));
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        Ok(Self { store, rt })
    }

    async fn create(storage: StorageConfig) -> Result<Self, String> {
        let cached_storage = storage.into_cached();
        let dataset = DatasetConfig::new();
        let config = StoreConfig {
            storage: cached_storage,
            dataset,
            get_partial_values_concurrency: None,
        };

        let store =
            Store::from_config(&config, icechunk::zarr::AccessMode::ReadWrite).await?;
        let store = Arc::new(RwLock::new(store));
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        Ok(Self { store, rt })
    }

    async fn from_json_config(json: &[u8], read_only: bool) -> Result<Self, String> {
        let access_mode = if read_only {
            icechunk::zarr::AccessMode::ReadOnly
        } else {
            icechunk::zarr::AccessMode::ReadWrite
        };
        let store = Store::from_json_config(json, access_mode).await?;
        let store = Arc::new(RwLock::new(store));
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        Ok(Self { store, rt })
    }
}

#[pyfunction]
fn pyicechunk_store_from_json_config<'py>(
    json: String,
    read_only: bool,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let json = json.as_bytes().to_owned();

    // The commit mechanism is async and calls tokio::spawn so we need to use the
    // pyo3_asyncio_0_21::tokio helper to run the async function in the tokio runtime
    pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
        PyIcechunkStore::from_json_config(&json, read_only)
            .await
            .map_err(PyValueError::new_err)
    })
}

#[pyfunction]
fn pyicechunk_store_open_existing<'py>(
    storage: &'py PyStorage,
    read_only: bool,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let storage = storage.into();
    pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
        PyIcechunkStore::open_existing(storage, read_only)
            .await
            .map_err(PyValueError::new_err)
    })
}

#[pyfunction]
fn pyicechunk_store_create<'py>(
    storage: &'py PyStorage,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let storage = storage.into();
    pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
        PyIcechunkStore::create(storage).await.map_err(PyValueError::new_err)
    })
}

#[pymethods]
impl PyIcechunkStore {
    pub fn checkout_snapshot<'py>(
        &'py mut self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let snapshot_id = ObjectId::try_from(snapshot_id.as_str()).map_err(|e| {
            PyIcechunkStoreError::UnkownError(format!(
                "Error checking out snapshot {snapshot_id}: {e}"
            ))
        })?;

        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut store = store.write().await;
            store
                .checkout(VersionInfo::SnapshotId(snapshot_id))
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;
            Ok(())
        })
    }

    pub fn checkout_branch<'py>(
        &'py mut self,
        py: Python<'py>,
        branch: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut store = store.write().await;
            store
                .checkout(VersionInfo::BranchTipRef(branch))
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;
            Ok(())
        })
    }

    pub fn checkout_tag<'py>(
        &'py mut self,
        py: Python<'py>,
        tag: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut store = store.write().await;
            store
                .checkout(VersionInfo::TagRef(tag))
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;
            Ok(())
        })
    }

    #[getter]
    pub fn snapshot_id(&self) -> PyIcechunkStoreResult<String> {
        let store = self.store.blocking_read();
        let snapshot_id = store.snapshot_id();
        Ok(String::from(snapshot_id))
    }

    pub fn commit<'py>(
        &'py mut self,
        py: Python<'py>,
        message: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        // The commit mechanism is async and calls tokio::spawn so we need to use the
        // pyo3_asyncio_0_21::tokio helper to run the async function in the tokio runtime
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut writeable_store = store.write().await;
            let oid = writeable_store
                .commit(&message)
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(String::from(&oid))
        })
    }

    #[getter]
    pub fn branch(&self) -> PyIcechunkStoreResult<Option<String>> {
        let store = self.store.blocking_read();
        let current_branch = store.current_branch();
        Ok(current_branch.clone())
    }

    #[getter]
    pub fn has_uncommitted_changes(&self) -> PyIcechunkStoreResult<bool> {
        let store = self.store.blocking_read();
        let has_uncommitted_changes = store.has_uncommitted_changes();
        Ok(has_uncommitted_changes)
    }

    pub fn reset<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            store
                .write()
                .await
                .reset()
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;
            Ok(())
        })
    }

    pub fn new_branch<'py>(
        &'py self,
        py: Python<'py>,
        branch_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        // The commit mechanism is async and calls tokio::spawn so we need to use the
        // pyo3_asyncio_0_21::tokio helper to run the async function in the tokio runtime
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut writeable_store = store.write().await;
            let (oid, _version) = writeable_store
                .new_branch(&branch_name)
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(String::from(&oid))
        })
    }

    pub fn tag<'py>(
        &'py self,
        py: Python<'py>,
        tag: String,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        // The commit mechanism is async and calls tokio::spawn so we need to use the
        // pyo3_asyncio_0_21::tokio helper to run the async function in the tokio runtime
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut writeable_store = store.write().await;
            let oid = ObjectId::try_from(snapshot_id.as_str())
                .map_err(|e| PyIcechunkStoreError::UnkownError(e.to_string()))?;
            writeable_store.tag(&tag, &oid).await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    pub fn empty<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let is_empty =
                store.read().await.empty().await.map_err(PyIcechunkStoreError::from)?;
            Ok(is_empty)
        })
    }

    pub fn clear<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            store.write().await.clear().await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    pub fn get<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        byte_range: Option<(Option<ChunkOffset>, Option<ChunkOffset>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let byte_range = byte_range.unwrap_or((None, None)).into();
            let data = store
                .read()
                .await
                .get(&key, &byte_range)
                .await
                .map_err(PyIcechunkStoreError::from)?;
            let pybytes = Python::with_gil(|py| {
                let bound_bytes = PyBytes::new_bound(py, &data);
                bound_bytes.to_object(py)
            });
            Ok(pybytes)
        })
    }

    pub fn get_partial_values<'py>(
        &'py self,
        py: Python<'py>,
        key_ranges: Vec<(String, (Option<ChunkOffset>, Option<ChunkOffset>))>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let iter = key_ranges.into_iter().map(|r| (r.0, r.1.into()));
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let readable_store = store.read().await;
            let partial_values_stream = readable_store
                .get_partial_values(iter)
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;

            let result = partial_values_stream
                .into_iter()
                // If we want to error instead of returning None we can collect into
                // a Result<Vec<_>, _> and short circuit
                .map(|x| {
                    x.map(|x| {
                        Python::with_gil(|py| {
                            let bound_bytes = PyBytes::new_bound(py, &x);
                            bound_bytes.to_object(py)
                        })
                    })
                    .ok()
                })
                .collect::<Vec<_>>();

            Ok(result)
        })
    }

    pub fn exists<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let exists = store
                .read()
                .await
                .exists(&key)
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;
            Ok(exists)
        })
    }

    #[getter]
    pub fn supports_writes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_writes = self.store.blocking_read().supports_writes()?;
        Ok(supports_writes)
    }

    pub fn set<'py>(
        &'py mut self,
        py: Python<'py>,
        key: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        // Most of our async functions use structured coroutines so they can be called directly from
        // the python event loop, but in this case downstream objectstore crate  calls tokio::spawn
        // when emplacing chunks into its storage backend. Calling tokio::spawn requires an active
        // tokio runtime so we use the pyo3_asyncio_0_21::tokio helper to do this
        // In the future this will hopefully not be necessary,
        // see this tracking issue: https://github.com/PyO3/pyo3/issues/1632
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut writeable_store = store.write().await;
            let result = writeable_store
                .set(&key, Bytes::from(value))
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(result)
        })
    }

    pub fn delete<'py>(
        &'py mut self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.store);

        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            store
                .write()
                .await
                .delete(&key)
                .await
                .map_err(PyIcechunkStoreError::StoreError)?;
            Ok(())
        })
    }

    #[getter]
    pub fn supports_partial_writes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_partial_writes =
            self.store.blocking_read().supports_partial_writes()?;
        Ok(supports_partial_writes)
    }

    pub fn set_partial_values<'py>(
        &'py mut self,
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
        // tokio runtime so we use the pyo3_asyncio_0_21::tokio helper to do this
        // In the future this will hopefully not be necessary,
        // see this tracking issue: https://github.com/PyO3/pyo3/issues/1632
        pyo3_asyncio_0_21::tokio::future_into_py(py, async move {
            let mut writeable_store = store.write().await;

            let mapped_to_bytes = key_start_values.into_iter().enumerate().map(
                |(i, (_key, offset, value))| {
                    (keys[i].as_str(), offset, Bytes::from(value))
                },
            );

            let result = writeable_store
                .set_partial_values(mapped_to_bytes)
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(result)
        })
    }

    #[getter]
    pub fn supports_listing(&self) -> PyIcechunkStoreResult<bool> {
        let supports_listing = self.store.blocking_read().supports_listing()?;
        Ok(supports_listing)
    }

    pub fn list(&self) -> PyIcechunkStoreResult<PyAsyncStringGenerator> {
        let store = self.rt.block_on(self.store.read());
        let list = self.rt.block_on(store.list())?;
        let prepared_list = pin_extend_stream(list);
        Ok(PyAsyncStringGenerator::new(prepared_list))
    }

    pub fn list_prefix(
        &self,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncStringGenerator> {
        let store = self.rt.block_on(self.store.read());
        let list = self.rt.block_on(store.list_prefix(&prefix))?;
        let prepared_list = pin_extend_stream(list);
        Ok(PyAsyncStringGenerator::new(prepared_list))
    }

    pub fn list_dir(
        &self,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncStringGenerator> {
        let store = self.rt.block_on(self.store.read());
        let list = self.rt.block_on(store.list_dir(&prefix))?;
        let prepared_list = pin_extend_stream(list);
        Ok(PyAsyncStringGenerator::new(prepared_list))
    }
}

// Prepares a stream for use in a Python AsyncGenerator impl.
// This is possibly unsafe. We have to do this because the stream is pinned
// and the lifetime of the stream is tied to the lifetime of the store. So the return
// value is valid as long as the store is valid. Rust has no way to express this to
// the Python runtime so we have to do it manually.
fn pin_extend_stream<'a>(
    stream: impl Stream<Item = Result<String, StoreError>> + Send,
) -> Arc<Mutex<Pin<Box<dyn Stream<Item = Result<String, StoreError>> + 'a + Send>>>> {
    let pinned = Box::pin(stream);
    let extended_stream = unsafe {
        core::mem::transmute::<
            Pin<Box<dyn Stream<Item = Result<String, StoreError>> + Send>>,
            Pin<Box<dyn Stream<Item = Result<String, StoreError>> + 'a + Send>>,
        >(pinned)
    };

    let mutexed_stream = Arc::new(Mutex::new(extended_stream));
    mutexed_stream
}

/// The icechunk Python module implemented in Rust.
#[pymodule]
fn _icechunk_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStorage>()?;
    m.add_class::<PyIcechunkStore>()?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_from_json_config, m)?)?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_create, m)?)?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_open_existing, m)?)?;
    Ok(())
}
