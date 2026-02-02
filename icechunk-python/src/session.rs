use std::{borrow::Cow, ops::Deref, sync::Arc};

use async_stream::try_stream;
use futures::{StreamExt, TryStreamExt};
use icechunk::{
    Store,
    format::{ChunkIndices, Path, manifest::ChunkPayload},
    session::{Session, SessionErrorKind, SessionMode},
    store::{StoreError, StoreErrorKind},
};
use pyo3::{
    prelude::*,
    types::{PyFunction, PyType},
};
use tokio::sync::{Mutex, RwLock};

use crate::{
    config::PyRepositoryConfig,
    conflicts::PyConflictSolver,
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    repository::{PyDiff, PySnapshotProperties},
    store::PyStore,
    streams::PyAsyncGenerator,
};

#[pyclass]
#[derive(Clone)]
pub struct PySession(pub Arc<RwLock<Session>>);

#[pyclass(eq, eq_int, rename_all = "UPPERCASE")]
#[derive(PartialEq)]
pub enum ChunkType {
    Uninitialized = 0,
    Native = 1,
    Virtual = 2,
    Inline = 3,
}

/// The mode of a session, determining what operations are allowed.
#[pyclass(name = "SessionMode", module = "icechunk", eq, rename_all = "UPPERCASE")]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PySessionMode {
    Readonly,
    Writable,
    Rearrange,
}

impl From<SessionMode> for PySessionMode {
    fn from(mode: SessionMode) -> Self {
        match mode {
            SessionMode::Readonly => PySessionMode::Readonly,
            SessionMode::Writable => PySessionMode::Writable,
            SessionMode::Rearrange => PySessionMode::Rearrange,
        }
    }
}

#[pymethods]
/// Most functions in this class block, so they need to `detach` so other
/// python threads can make progress
impl PySession {
    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: Vec<u8>,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.detach(move || {
            let session =
                Session::from_bytes(bytes).map_err(PyIcechunkStoreError::SessionError)?;
            Ok(Self(Arc::new(RwLock::new(session))))
        })
    }

    fn __eq__(&self, other: &PySession) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn as_bytes(&self, py: Python<'_>) -> PyIcechunkStoreResult<Cow<'_, [u8]>> {
        // This is a compute intensive task, we need to release the Gil
        py.detach(move || {
            let bytes =
                self.0.blocking_read().as_bytes().map_err(PyIcechunkStoreError::from)?;
            Ok(Cow::Owned(bytes))
        })
    }

    #[getter]
    pub fn read_only(&self, py: Python<'_>) -> bool {
        // This is blocking function, we need to release the Gil
        py.detach(move || self.0.blocking_read().read_only())
    }

    #[getter]
    pub fn mode(&self, py: Python<'_>) -> PySessionMode {
        // This is blocking function, we need to release the Gil
        py.detach(move || self.0.blocking_read().mode().into())
    }

    #[getter]
    pub fn snapshot_id(&self, py: Python<'_>) -> String {
        // This is blocking function, we need to release the Gil
        py.detach(move || self.0.blocking_read().snapshot_id().to_string())
    }

    #[getter]
    pub fn branch(&self, py: Python<'_>) -> Option<String> {
        // This is blocking function, we need to release the Gil
        py.detach(move || self.0.blocking_read().branch().map(|b| b.to_string()))
    }

    #[getter]
    pub fn has_uncommitted_changes(&self, py: Python<'_>) -> bool {
        // This is blocking function, we need to release the Gil
        py.detach(move || self.0.blocking_read().has_uncommitted_changes())
    }

    pub fn status(&self, py: Python<'_>) -> PyResult<PyDiff> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let session = self.0.blocking_read();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res =
                    session.status().await.map_err(PyIcechunkStoreError::SessionError)?;
                Ok(res.into())
            })
        })
    }

    pub fn discard_changes(&self, py: Python<'_>) -> PyResult<()> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            self.0
                .blocking_write()
                .discard_changes()
                .map_err(PyIcechunkStoreError::SessionError)
        })?;
        Ok(())
    }

    pub fn move_node(
        &self,
        py: Python<'_>,
        from_path: String,
        to_path: String,
    ) -> PyResult<()> {
        let from = Path::new(from_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let to = Path::new(to_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut session = self.0.write().await;
                session
                    .move_node(from, to)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)
            })
        })?;
        Ok(())
    }

    pub fn move_node_async<'py>(
        &'py self,
        py: Python<'py>,
        from_path: String,
        to_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let from = Path::new(from_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let to = Path::new(to_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let session = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut session = session.write().await;
            session
                .move_node(from, to)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    pub fn reindex_array<'py>(
        &mut self,
        py: Python<'py>,
        array_path: String,
        shift_chunk: Bound<'py, PyFunction>,
    ) -> PyResult<()> {
        let array_path = Path::new(array_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let shift_chunk = |idx: &ChunkIndices| {
            let python_index = idx
                .0
                .clone()
                .into_pyobject(py)
                .map_err(|e| SessionErrorKind::Other(Box::new(e)))?;
            let new_index = shift_chunk
                .call1((python_index,))
                .map_err(|e| SessionErrorKind::Other(Box::new(e)))?;
            if new_index.is_none() {
                Ok(None)
            } else {
                let new_index: Vec<u32> = new_index
                    .extract()
                    .map_err(|e| SessionErrorKind::Other(Box::new(e)))?;
                Ok(Some(ChunkIndices(new_index)))
            }
        };

        // TODO: detach
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut session = self.0.write().await;
            session
                .reindex_array(&array_path, shift_chunk)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    pub fn shift_array(&mut self, array_path: String, offset: Vec<i64>) -> PyResult<()> {
        let array_path = Path::new(array_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;

        // TODO: detach
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut session = self.0.write().await;
            session
                .shift_array(&array_path, offset.as_slice())
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    #[getter]
    pub fn store(&self, py: Python<'_>) -> PyResult<PyStore> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let session = self.0.blocking_read();
            let conc = session.config().get_partial_values_concurrency();
            let store = Store::from_session_and_config(self.0.clone(), conc);

            let store = Arc::new(store);
            Ok(PyStore(store))
        })
    }

    #[getter]
    pub fn config(&self, py: Python<'_>) -> PyResult<PyRepositoryConfig> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let session = self.0.blocking_read();
            let config = session.config().clone().into();
            Ok(config)
        })
    }

    pub fn all_virtual_chunk_locations(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let session = self.0.blocking_read();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let res = session
                    .all_virtual_chunk_locations()
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?
                    .try_collect()
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;

                Ok(res)
            })
        })
    }

    pub fn all_virtual_chunk_locations_async<'py>(
        &'py self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py::<_, Vec<String>>(py, async move {
            let session = session.read().await;
            let res = session
                .all_virtual_chunk_locations()
                .await
                .map_err(PyIcechunkStoreError::SessionError)?
                .try_collect()
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;

            Ok(res)
        })
    }

    /// Return vectors of coordinates, up to batch_size in length.
    ///
    /// We batch the results to make it faster.
    pub fn chunk_coordinates(
        &self,
        array_path: String,
        batch_size: u32,
    ) -> PyResult<PyAsyncGenerator> {
        // This is blocking function, we need to release the Gil
        let session = self.0.clone();
        let res = try_stream! {
            let session = session.read_owned().await;
            let array_path = array_path.try_into().map_err(|e| PyIcechunkStoreError::PyValueError(format!("Invalid path: {e}")))?;

            let stream = session
                .chunk_coordinates(&array_path)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?
                .map_err(PyIcechunkStoreError::SessionError)
                .chunks(batch_size as usize);

            #[allow(unused_braces, deprecated)]
            { for await coords_vec in stream {
                let vec = coords_vec
                    .into_iter()
                    .map(|maybe_coord| {
                        maybe_coord.and_then(|coord| {
                            Python::attach(|py| {
                                coord.0.into_pyobject(py)
                                    .map(|obj| obj.unbind())
                                    .map_err(PyIcechunkStoreError::PyError)
                            })
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let vec = Python::attach(|py| {
                    vec.into_pyobject(py)
                        .map(|obj| obj.unbind())
                        .map_err(PyIcechunkStoreError::PyError)
                })?;
                yield vec
            } }
        };

        let prepared_list = Arc::new(Mutex::new(res.boxed()));
        Ok(PyAsyncGenerator::new(prepared_list))
    }

    pub fn chunk_type(
        &self,
        array_path: String,
        coords: Vec<u32>,
    ) -> PyResult<ChunkType> {
        let session = self.0.clone();
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(Self::chunk_type_inner(session, array_path, coords))
    }

    pub fn chunk_type_async<'py>(
        &'py self,
        py: Python<'py>,
        array_path: String,
        coords: Vec<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();
        pyo3_async_runtimes::tokio::future_into_py::<_, ChunkType>(
            py,
            Self::chunk_type_inner(session, array_path, coords),
        )
    }

    pub fn merge(&self, other: &PySession, py: Python<'_>) -> PyResult<()> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            // TODO: bad clone
            let other = other.0.blocking_read().deref().clone();

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                self.0
                    .write()
                    .await
                    .merge(other)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(())
            })
        })
    }

    pub fn merge_async<'py>(
        &'py self,
        other: &PySession,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();
        let other = other.0.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut session = session.write().await;
            let other = other.read().await.deref().clone();
            session.merge(other).await.map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    #[pyo3(signature = (message, metadata=None, rebase_with=None, rebase_tries=1_000))]
    pub fn commit(
        &self,
        py: Python<'_>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        rebase_with: Option<PyConflictSolver>,
        rebase_tries: Option<u16>,
    ) -> PyResult<String> {
        let metadata = metadata.map(|m| m.into());
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut session = self.0.write().await;
                let snapshot_id = if let Some(solver) = rebase_with {
                    session
                        .commit_rebasing(
                            solver.as_ref(),
                            rebase_tries.unwrap_or(1_000),
                            message,
                            metadata,
                            |_| async {},
                            |_| async {},
                        )
                        .await
                } else {
                    session.commit(message, metadata).await
                }
                .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(snapshot_id.to_string())
            })
        })
    }

    pub fn commit_async<'py>(
        &'py self,
        py: Python<'py>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        rebase_with: Option<PyConflictSolver>,
        rebase_tries: Option<u16>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();
        let message = message.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = metadata.map(|m| m.into());
            let mut session = session.write().await;
            let snapshot_id = if let Some(solver) = rebase_with {
                session
                    .commit_rebasing(
                        solver.as_ref(),
                        rebase_tries.unwrap_or(1_000),
                        &message,
                        metadata,
                        |_| async {},
                        |_| async {},
                    )
                    .await
            } else {
                session.commit(&message, metadata).await
            }
            .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(snapshot_id.to_string())
        })
    }

    pub fn amend(
        &self,
        py: Python<'_>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
    ) -> PyResult<String> {
        let metadata = metadata.map(|m| m.into());
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut session = self.0.write().await;
                let snapshot_id = session
                    .amend(message, metadata)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(snapshot_id.to_string())
            })
        })
    }

    pub fn amend_async<'py>(
        &'py self,
        py: Python<'py>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();
        let message = message.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = metadata.map(|m| m.into());
            let mut session = session.write().await;
            let snapshot_id = session
                .amend(&message, metadata)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(snapshot_id.to_string())
        })
    }

    #[pyo3(signature = (message, metadata=None))]
    pub fn flush(
        &self,
        py: Python<'_>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
    ) -> PyResult<String> {
        let metadata = metadata.map(|m| m.into());
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut session = self.0.write().await;
                let snapshot_id = session
                    .flush(message, metadata)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(snapshot_id.to_string())
            })
        })
    }

    pub fn flush_async<'py>(
        &'py self,
        py: Python<'py>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();
        let message = message.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = metadata.map(|m| m.into());
            let mut session = session.write().await;
            let snapshot_id = session
                .flush(&message, metadata)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(snapshot_id.to_string())
        })
    }

    pub fn rebase(&self, solver: PyConflictSolver, py: Python<'_>) -> PyResult<()> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let solver = solver.as_ref();
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                self.0
                    .write()
                    .await
                    .rebase(solver)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(())
            })
        })
    }

    pub fn rebase_async<'py>(
        &'py self,
        py: Python<'py>,
        solver: PyConflictSolver,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut session = session.write().await;
            let solver = solver.as_ref();
            session.rebase(solver).await.map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }
}

impl PySession {
    async fn chunk_type_inner(
        session: Arc<RwLock<Session>>,
        array_path: String,
        coords: Vec<u32>,
    ) -> PyResult<ChunkType> {
        let session = session.read().await;
        let array_path = Path::new(array_path.as_str())
            .map_err(|e| StoreError::from(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let res = session
            .get_chunk_ref(&array_path, &ChunkIndices(coords))
            .await
            .map_err(PyIcechunkStoreError::SessionError)?;

        match res {
            None => Ok(ChunkType::Uninitialized),
            Some(ChunkPayload::Inline(_)) => Ok(ChunkType::Inline),
            Some(ChunkPayload::Virtual(_)) => Ok(ChunkType::Virtual),
            Some(ChunkPayload::Ref(_)) => Ok(ChunkType::Native),
            Some(_) => {
                Err(PyIcechunkStoreError::PyValueError("Invalid Chunk Type".into()))?
            }
        }
    }
}
