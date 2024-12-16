use std::{borrow::Cow, sync::Arc};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    format::{ChunkLength, ChunkOffset},
    store::{StoreConfig, StoreError},
    Store,
};
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::PyType,
};
use tokio::sync::Mutex;

use crate::{
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    session::PySession,
    streams::PyAsyncGenerator,
};

type KeyRanges = Vec<(String, (Option<ChunkOffset>, Option<ChunkOffset>))>;

#[pyclass(name = "StoreConfig")]
#[derive(Clone)]
pub struct PyStoreConfig(pub StoreConfig);

#[pymethods]
impl PyStoreConfig {
    #[new]
    #[pyo3(signature = (*, get_partial_values_concurrency = 10))]
    fn new(get_partial_values_concurrency: u16) -> Self {
        Self(StoreConfig { get_partial_values_concurrency })
    }

    #[classmethod]
    fn from_json(_cls: Bound<'_, PyType>, json: String) -> PyResult<Self> {
        let config = serde_json::from_str(&json).map_err(|e| {
            PyValueError::new_err(format!(
                "Failed to deserialize store config from json: {}",
                e
            ))
        })?;
        Ok(Self(config))
    }

    fn as_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.0).map_err(|e| {
            PyValueError::new_err(format!(
                "Failed to serialize store config to json: {}",
                e
            ))
        })
    }
}

#[pyclass(name = "PyStore")]
#[derive(Clone)]
pub struct PyStore(pub Arc<Store>);

#[pymethods]
impl PyStore {
    #[classmethod]
    #[pyo3(signature = (bytes, config = None))]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        bytes: Vec<u8>,
        config: Option<PyStoreConfig>,
    ) -> PyResult<Self> {
        let bytes = Bytes::from(bytes);
        let config = config.map(|c| c.0).unwrap_or_default();
        let store = Store::from_bytes(bytes, config).map_err(|e| {
            PyValueError::new_err(format!(
                "Failed to deserialize store from bytes: {}",
                e
            ))
        })?;
        Ok(Self(Arc::new(store)))
    }

    fn __eq__(&self, other: &PyStore) -> bool {
        if Arc::ptr_eq(&self.0, &other.0) {
            return true;
        }

        // If the stores were created from the same session they are equal
        let session = self.0.session();
        let other_session = other.0.session();
        Arc::ptr_eq(&session, &other_session)
    }

    #[getter]
    fn read_only(&self) -> PyIcechunkStoreResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let read_only = self.0.read_only().await;
            Ok(read_only)
        })
    }

    #[getter]
    fn config(&self) -> PyIcechunkStoreResult<PyStoreConfig> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let config = self.0.config().clone();
            Ok(PyStoreConfig(config))
        })
    }

    fn as_bytes(&self) -> PyResult<Cow<[u8]>> {
        // FIXME: Use rmp_serde instead of serde_json to optimize performance
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let serialized =
                self.0.as_bytes().await.map_err(PyIcechunkStoreError::from)?;
            Ok(Cow::Owned(serialized.to_vec()))
        })
    }

    #[getter]
    fn session(&self) -> PyResult<PySession> {
        let session = self.0.session();
        Ok(PySession(session))
    }

    fn is_empty<'py>(
        &'py self,
        py: Python<'py>,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let is_empty =
                store.is_empty(&prefix).await.map_err(PyIcechunkStoreError::from)?;
            Ok(is_empty)
        })
    }

    fn clear<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.clear().await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    fn sync_clear(&self) -> PyIcechunkStoreResult<()> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            store.clear().await.map_err(PyIcechunkStoreError::from)?;
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
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let byte_range = byte_range.unwrap_or((None, None)).into();
            let data = store.get(&key, &byte_range).await;
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
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let partial_values_stream = store
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
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let exists = store.exists(&key).await.map_err(PyIcechunkStoreError::from)?;
            Ok(exists)
        })
    }

    #[getter]
    fn supports_deletes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_deletes = self.0.supports_deletes()?;
        Ok(supports_deletes)
    }

    #[getter]
    fn supports_writes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_writes = self.0.supports_writes()?;
        Ok(supports_writes)
    }

    fn set<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);

        // Most of our async functions use structured coroutines so they can be called directly from
        // the python event loop, but in this case downstream objectstore crate  calls tokio::spawn
        // when emplacing chunks into its storage backend. Calling tokio::spawn requires an active
        // tokio runtime so we use the pyo3_async_runtimes::tokio helper to do this
        // In the future this will hopefully not be necessary,
        // see this tracking issue: https://github.com/PyO3/pyo3/issues/1632
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let store = Arc::clone(&self.0);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store
                .set_if_not_exists(&key, Bytes::from(value))
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    fn set_virtual_ref(
        &self,
        _key: String,
        _location: String,
        _offset: ChunkOffset,
        _length: ChunkLength,
    ) -> PyIcechunkStoreResult<()> {
        let _store = Arc::clone(&self.0);

        // pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
        //     let virtual_ref = VirtualChunkRef {
        //         location: VirtualChunkLocation::Absolute(location),
        //         offset,
        //         length,
        //     };
        //     store
        //         .write()
        //         .await
        //         .set_virtual_ref(&key, virtual_ref)
        //         .await
        //         .map_err(PyIcechunkStoreError::from)?;
        //     Ok(())
        // })

        Err(PyIcechunkStoreError::PyValueError("Not yet implemented".to_string()))
    }

    fn delete<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.delete(&key).await.map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    #[getter]
    fn supports_partial_writes(&self) -> PyIcechunkStoreResult<bool> {
        let supports_partial_writes = self.0.supports_partial_writes()?;
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

        let store = Arc::clone(&self.0);
        // Most of our async functions use structured coroutines so they can be called directly from
        // the python event loop, but in this case downstream objectstore crate  calls tokio::spawn
        // when emplacing chunks into its storage backend. Calling tokio::spawn requires an active
        // tokio runtime so we use the pyo3_async_runtimes::tokio helper to do this
        // In the future this will hopefully not be necessary,
        // see this tracking issue: https://github.com/PyO3/pyo3/issues/1632
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
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
        let supports_listing = self.0.supports_listing()?;
        Ok(supports_listing)
    }

    fn list(&self) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        let store = Arc::clone(&self.0);

        #[allow(deprecated)]
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move { store.list().await })?
            .map_ok(|s| Python::with_gil(|py| s.to_object(py)));

        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    fn list_prefix(&self, prefix: String) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        let store = Arc::clone(&self.0);

        #[allow(deprecated)]
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move { store.list_prefix(prefix.as_str()).await })?
            .map_ok(|s| Python::with_gil(|py| s.to_object(py)));
        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    fn list_dir(&self, prefix: String) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        let store = Arc::clone(&self.0);

        #[allow(deprecated)]
        let list = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async move { store.list_dir(prefix.as_str()).await })?
            .map_ok(|s| Python::with_gil(|py| s.to_object(py)));
        let prepared_list = Arc::new(Mutex::new(list.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }
}
