use std::{borrow::Cow, sync::Arc};

use bytes::Bytes;
use chrono::Utc;
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    Store,
    format::{
        ChunkIndices, ChunkLength, ChunkOffset, Path,
        manifest::{Checksum, SecondsSinceEpoch, VirtualChunkLocation, VirtualChunkRef},
    },
    storage::ETag,
    store::{SetVirtualRefsResult, StoreError, StoreErrorKind},
};
use itertools::Itertools as _;
use pyo3::{
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{PyTuple, PyType},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    impl_pickle,
    session::PySession,
    streams::PyAsyncGenerator,
};

type KeyRanges = Vec<(String, (Option<ChunkOffset>, Option<ChunkOffset>))>;

#[derive(FromPyObject, Clone, Debug)]
enum ChecksumArgument {
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "datetime.datetime")]
    Datetime(chrono::DateTime<Utc>),
}

impl From<ChecksumArgument> for Checksum {
    fn from(value: ChecksumArgument) -> Self {
        match value {
            ChecksumArgument::String(etag) => Checksum::ETag(ETag(etag)),
            ChecksumArgument::Datetime(date_time) => {
                Checksum::LastModified(SecondsSinceEpoch(date_time.timestamp() as u32))
            }
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VirtualChunkSpec {
    #[pyo3(get)]
    index: Vec<u32>,
    #[pyo3(get)]
    location: String,
    #[pyo3(get)]
    offset: ChunkOffset,
    #[pyo3(get)]
    length: ChunkLength,
    #[pyo3(get)]
    etag_checksum: Option<String>,
    #[pyo3(get)]
    last_updated_at_checksum: Option<chrono::DateTime<Utc>>,
}

impl VirtualChunkSpec {
    fn checksum(&self) -> Option<Checksum> {
        self.etag_checksum
            .as_ref()
            .map(|etag| Checksum::ETag(ETag(etag.clone())))
            .or(self
                .last_updated_at_checksum
                .map(|t| Checksum::LastModified(SecondsSinceEpoch(t.timestamp() as u32))))
    }
}

#[pymethods]
impl VirtualChunkSpec {
    #[new]
    #[pyo3(signature = (index, location, offset, length, etag_checksum = None, last_updated_at_checksum = None))]
    fn new(
        index: Vec<u32>,
        location: String,
        offset: ChunkOffset,
        length: ChunkLength,
        etag_checksum: Option<String>,
        last_updated_at_checksum: Option<chrono::DateTime<Utc>>,
    ) -> Self {
        Self { index, location, offset, length, etag_checksum, last_updated_at_checksum }
    }
}

impl_pickle!(VirtualChunkSpec);

#[pyclass(name = "PyStore")]
#[derive(Clone)]
pub struct PyStore(pub Arc<Store>);

#[pymethods]
impl PyStore {
    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: Vec<u8>,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.allow_threads(move || {
            let bytes = Bytes::from(bytes);
            let store = Store::from_bytes(bytes).map_err(|e| {
                PyValueError::new_err(format!(
                    "Failed to deserialize store from bytes: {e}"
                ))
            })?;
            Ok(Self(Arc::new(store)))
        })
    }

    fn __eq__(&self, other: &PyStore) -> bool {
        // If the stores were created from the same session they are equal
        Arc::ptr_eq(&self.0.session(), &other.0.session())
    }

    #[getter]
    fn read_only(&self, py: Python<'_>) -> PyIcechunkStoreResult<bool> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let read_only = self.0.read_only().await;
                Ok(read_only)
            })
        })
    }

    fn as_bytes(&self, py: Python<'_>) -> PyResult<Cow<[u8]>> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            // FIXME: Use rmp_serde instead of serde_json to optimize performance
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let serialized =
                    self.0.as_bytes().await.map_err(PyIcechunkStoreError::from)?;
                Ok(Cow::Owned(serialized.to_vec()))
            })
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

    fn sync_clear(&self, py: Python<'_>) -> PyIcechunkStoreResult<()> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let store = Arc::clone(&self.0);
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                store.clear().await.map_err(PyIcechunkStoreError::from)?;
                Ok(())
            })
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
                Err(StoreError { kind: StoreErrorKind::NotFound(_), .. }) => {
                    Err(PyKeyError::new_err(key))
                }
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
    fn supports_consolidated_metadata(&self) -> PyIcechunkStoreResult<bool> {
        let supports = self.0.supports_consolidated_metadata()?;
        Ok(supports)
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

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (key, location, offset, length, checksum = None, validate_container = false))]
    fn set_virtual_ref(
        &self,
        py: Python<'_>,
        key: String,
        location: String,
        offset: ChunkOffset,
        length: ChunkLength,
        checksum: Option<ChecksumArgument>,
        validate_container: bool,
    ) -> PyIcechunkStoreResult<()> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let store = Arc::clone(&self.0);

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let virtual_ref = VirtualChunkRef {
                    location: VirtualChunkLocation(location),
                    offset,
                    length,
                    checksum: checksum.map(|cs| cs.into()),
                };
                store
                    .set_virtual_ref(&key, virtual_ref, validate_container)
                    .await
                    .map_err(PyIcechunkStoreError::from)?;
                Ok(())
            })
        })
    }

    fn set_virtual_refs(
        &self,
        py: Python<'_>,
        array_path: String,
        chunks: Vec<VirtualChunkSpec>,
        validate_containers: bool,
    ) -> PyIcechunkStoreResult<Option<Vec<Py<PyTuple>>>> {
        py.allow_threads(move || {
            let store = Arc::clone(&self.0);

            let res = pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let vrefs = chunks.into_iter().map(|vcs| {
                    let checksum = vcs.checksum();
                    let index = ChunkIndices(vcs.index);
                    let vref = VirtualChunkRef {
                        location: VirtualChunkLocation(vcs.location),
                        offset: vcs.offset,
                        length: vcs.length,
                        checksum,
                    };
                    (index, vref)
                });

                let array_path = if !array_path.starts_with("/") {
                    format!("/{array_path}")
                } else {
                    array_path.to_string()
                };

                let path = Path::try_from(array_path).map_err(|e| {
                    PyValueError::new_err(format!("Invalid array path: {e}"))
                })?;

                let res = store
                    .set_virtual_refs(&path, validate_containers, vrefs)
                    .await
                    .map_err(PyIcechunkStoreError::from)?;
                Ok::<_, PyIcechunkStoreError>(res)
            })?;

            match res {
                SetVirtualRefsResult::Done => Ok(None),
                SetVirtualRefsResult::FailedRefs(vec) => Python::with_gil(|py| {
                    let res = vec
                        .into_iter()
                        .map(|ci| PyTuple::new(py, ci.0).map(|tup| tup.unbind()))
                        .try_collect()?;

                    Ok(Some(res))
                }),
            }
        })
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

    fn delete_dir<'py>(
        &'py self,
        py: Python<'py>,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store.delete_dir(&prefix).await.map_err(PyIcechunkStoreError::from)?;
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

    fn list(&self, py: Python<'_>) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let store = Arc::clone(&self.0);

            #[allow(deprecated)]
            let list = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { store.list().await })?
                .map_ok(|s| Python::with_gil(|py| s.to_object(py)))
                .map_err(PyIcechunkStoreError::StoreError)
                .err_into();

            let prepared_list = Arc::new(Mutex::new(list.boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
        })
    }

    fn list_prefix(
        &self,
        py: Python<'_>,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let store = Arc::clone(&self.0);

            #[allow(deprecated)]
            let list = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { store.list_prefix(prefix.as_str()).await })?
                .map_ok(|s| Python::with_gil(|py| s.to_object(py)))
                .map_err(PyIcechunkStoreError::StoreError)
                .err_into();
            let prepared_list = Arc::new(Mutex::new(list.boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
        })
    }

    fn list_dir(
        &self,
        py: Python<'_>,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncGenerator> {
        // This is blocking function, we need to release the Gil
        py.allow_threads(move || {
            let store = Arc::clone(&self.0);

            #[allow(deprecated)]
            let list = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { store.list_dir(prefix.as_str()).await })?
                .map_ok(|s| Python::with_gil(|py| s.to_object(py)))
                .map_err(PyIcechunkStoreError::StoreError)
                .err_into();
            let prepared_list = Arc::new(Mutex::new(list.boxed()));
            Ok(PyAsyncGenerator::new(prepared_list))
        })
    }

    fn getsize<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let size = store.getsize(&key).await.map_err(PyIcechunkStoreError::from)?;
            Ok(size)
        })
    }

    fn getsize_prefix<'py>(
        &self,
        py: Python<'py>,
        prefix: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let size = store
                .getsize_prefix(prefix.as_str())
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(size)
        })
    }
}
