mod errors;
mod streams;

use std::{pin::Pin, sync::Arc};

use ::icechunk::{
    format::ChunkOffset,
    zarr::{ByteRange, StoreError},
    Store,
};
use bytes::Bytes;
use errors::{PyIcechunkStoreError, PyIcechunkStoreResult};
use futures::Stream;
use icechunk::zarr::{ObjectId, VersionInfo};
use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};
use streams::PyAsyncStringGenerator;
use tokio::sync::{Mutex, RwLock};

#[pyclass]
struct PyIcechunkStore {
    store: Arc<RwLock<Store>>,
}

impl PyIcechunkStore {
    async fn from_json_config(json: &[u8]) -> Result<Self, String> {
        let store = Store::from_json_config(json).await?;
        let store = Arc::new(RwLock::new(store));
        Ok(Self { store })
    }
}

#[pyfunction]
async fn pyicechunk_store_from_json_config(json: String) -> PyResult<PyIcechunkStore> {
    let json = json.as_bytes();
    PyIcechunkStore::from_json_config(json).await.map_err(PyValueError::new_err)
}

#[pymethods]
impl PyIcechunkStore {
    pub async fn checkout_ref(
        &mut self,
        snapshot_id: String,
    ) -> PyIcechunkStoreResult<()> {
        let snapshot_id = ObjectId::try_from(snapshot_id.as_str())
            .map_err(|_| PyIcechunkStoreError::from("Invalid SnapshotId"))?;
        let mut store = self.store.write().await;
        store.checkout(VersionInfo::SnapshotId(snapshot_id)).await?;
        Ok(())
    }

    pub async fn commit(
        &mut self,
        update_branch_name: String,
        message: String,
    ) -> PyIcechunkStoreResult<String> {
        let (oid, _version) =
            self.store.write().await.commit(&update_branch_name, &message).await?;
        Ok(String::from(&oid))
    }

    pub async fn empty(&self) -> PyIcechunkStoreResult<bool> {
        let is_empty = self.store.read().await.empty().await?;
        Ok(is_empty)
    }

    pub async fn clear(&mut self) -> PyIcechunkStoreResult<()> {
        self.store.write().await.clear().await?;
        Ok(())
    }

    pub async fn get(
        &self,
        key: String,
        byte_range: Option<ByteRange>,
    ) -> PyIcechunkStoreResult<PyObject> {
        let byte_range = byte_range.unwrap_or((None, None));
        let data = self.store.read().await.get(&key, &byte_range).await?;
        let pybytes = Python::with_gil(|py| {
            let bound_bytes = PyBytes::new_bound(py, &data);
            bound_bytes.to_object(py)
        });
        Ok(pybytes)
    }

    pub async fn get_partial_values(
        &self,
        key_ranges: Vec<(String, ByteRange)>,
    ) -> PyIcechunkStoreResult<Vec<Option<PyObject>>> {
        let iter = key_ranges.into_iter();
        let result = self
            .store
            .read()
            .await
            .get_partial_values(iter)
            .await?
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
            .collect();

        Ok(result)
    }

    pub async fn exists<'a>(&self, key: String) -> PyIcechunkStoreResult<bool> {
        let exists = self.store.read().await.exists(&key).await?;
        Ok(exists)
    }

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
        let store = self.store.clone();

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

    pub async fn delete(&mut self, key: String) -> PyIcechunkStoreResult<()> {
        self.store.write().await.delete(&key).await?;
        Ok(())
    }

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

        let store = self.store.clone();

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

    pub fn supports_listing(&self) -> PyIcechunkStoreResult<bool> {
        let supports_listing = self.store.blocking_read().supports_listing()?;
        Ok(supports_listing)
    }

    pub async fn list(&self) -> PyIcechunkStoreResult<PyAsyncStringGenerator> {
        let store = self.store.read().await;
        let list = store.list().await?;
        let prepared_list = pin_extend_stream(list);
        Ok(PyAsyncStringGenerator::new(prepared_list))
    }

    pub async fn list_prefix(
        &self,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncStringGenerator> {
        let store = self.store.read().await;
        let list = store.list_prefix(&prefix).await?;
        let prepared_list = pin_extend_stream(list);
        Ok(PyAsyncStringGenerator::new(prepared_list))
    }

    pub async fn list_dir(
        &self,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncStringGenerator> {
        let store = self.store.read().await;
        let list = store.list_dir(&prefix).await?;
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
    m.add_class::<PyIcechunkStore>()?;
    m.add_function(wrap_pyfunction!(pyicechunk_store_from_json_config, m)?)?;
    Ok(())
}
