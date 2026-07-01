use std::{borrow::Cow, ops::Deref as _, sync::Arc};

use async_stream::try_stream;
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use icechunk::{
    Store,
    format::{
        ChunkIndices, Path,
        manifest::{ChunkPayload, ChunkRef, VirtualChunkRef},
    },
    session::{
        ReindexMapping, ReindexOperationResult, Session, SessionError, SessionErrorKind,
        SessionMode,
    },
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
    display::{PyRepr, ReprMode, py_bool},
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult},
    repository::{PyDiff, PySnapshotProperties},
    store::PyStore,
    streams::PyAsyncCloseableIterator,
};

#[pyclass(skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PySession(pub Arc<RwLock<Session>>);

#[pyclass(eq, eq_int, rename_all = "snake_case", skip_from_py_object)]
#[derive(Debug, PartialEq, Clone)]
pub enum ChunkType {
    Uninitialized = 0,
    Native = 1,
    Virtual = 2,
    Inline = 3,
}

impl From<&ChunkPayload> for ChunkType {
    fn from(payload: &ChunkPayload) -> Self {
        match payload {
            ChunkPayload::Inline(_) => ChunkType::Inline,
            ChunkPayload::Virtual(_) => ChunkType::Virtual,
            ChunkPayload::Ref(_) => ChunkType::Native,
            // ChunkPayload is non_exhaustive; unknown future variants are
            // classified as Uninitialized for now.
            _ => ChunkType::Uninitialized,
        }
    }
}

/// A single resolved chunk reference, in the plain-data form the columnar
/// `resolve_chunk_refs` result is built from. `location`, `offset` and
/// `length` share the same meaning across kinds (see `array_chunk_iterator`):
/// virtual → resolved source URL, native → `chunk_id`, inline → empty +
/// `inline_data`. Uninitialized coords are represented with `kind == 0`
/// (`ChunkType::Uninitialized`).
pub(crate) struct ResolvedChunkRef {
    pub kind: u8,
    pub location: String,
    pub offset: u64,
    pub length: u64,
    pub inline_data: Option<Bytes>,
}

/// The mode of a session, determining what operations are allowed.
#[pyclass(
    skip_from_py_object,
    name = "SessionMode",
    module = "icechunk",
    eq,
    rename_all = "snake_case"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

impl PyRepr for PySession {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.session.Session"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        let session = self.0.blocking_read();
        let mut fields = vec![
            ("read_only", py_bool(session.read_only())),
            ("snapshot_id", session.snapshot_id().to_string()),
        ];
        // Only show branch and uncommitted changes for writable sessions
        if !session.read_only() {
            let branch = session
                .branch()
                .map(|b| b.to_string())
                .unwrap_or_else(|| "None".to_string());
            fields.push(("branch", branch));
            fields.push((
                "has_uncommitted_changes",
                py_bool(session.has_uncommitted_changes()),
            ));
        }
        fields
    }
}

#[pymethods]
/// Most functions in this class block, so they need to `detach` so other
/// python threads can make progress
impl PySession {
    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }

    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: Vec<u8>,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.detach(move || {
            let session = Session::from_bytes(&bytes)
                .map_err(PyIcechunkStoreError::SessionError)?;
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

    fn fork(&self, py: Python<'_>) -> PyResult<Self> {
        py.detach(move || {
            let session = self.0.blocking_read();
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let forked =
                    session.fork().await.map_err(PyIcechunkStoreError::SessionError)?;
                Ok(Self(Arc::new(RwLock::new(forked))))
            })
        })
    }

    #[getter]
    pub fn read_only(&self, py: Python<'_>) -> bool {
        // This is blocking function, we need to release the Gil
        py.detach(move || self.0.blocking_read().read_only())
    }

    #[getter]
    pub fn is_fork(&self, py: Python<'_>) -> bool {
        py.detach(move || self.0.blocking_read().is_fork())
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
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let to = Path::new(to_path.as_str())
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
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
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let to = Path::new(to_path.as_str())
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let session = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut session = session.write().await;
            session
                .move_node(from, to)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    /// Return the node ID for the node at the given path.
    pub fn get_node_id(&self, py: Python<'_>, path: String) -> PyResult<String> {
        let path = Path::new(path.as_str())
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let session = self.0.read().await;
                let node = session
                    .get_node(&path)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(node.id.to_string())
            })
        })
    }

    /// Return the node ID for the node at the given path.
    pub fn get_node_id_async<'py>(
        &'py self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let path = Path::new(path.as_str())
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let session = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let session = session.read().await;
            let node = session
                .get_node(&path)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(node.id.to_string())
        })
    }

    #[pyo3(signature = (array_path, forward, backward=None))]
    pub fn reindex_array<'py>(
        &mut self,
        py: Python<'py>,
        array_path: String,
        forward: Bound<'py, PyFunction>,
        backward: Option<Bound<'py, PyFunction>>,
        //TODO: add a backwards shift as an option
    ) -> PyResult<()> {
        let array_path = Path::new(array_path.as_str())
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        fn make_py_reindex_closure<'py>(
            py: Python<'py>,
            func: Bound<'py, PyFunction>,
        ) -> Box<dyn Fn(&ChunkIndices) -> ReindexOperationResult + 'py> {
            Box::new(move |idx: &ChunkIndices| {
                let python_index = idx.0.clone().into_pyobject(py).map_err(|e| {
                    SessionError::capture(SessionErrorKind::Other(Box::new(e)))
                })?;
                let new_index = func.call1((python_index,)).map_err(|e| {
                    SessionError::capture(SessionErrorKind::Other(Box::new(e)))
                })?;
                if new_index.is_none() {
                    Ok(None)
                } else {
                    let new_index: Vec<u32> = new_index.extract().map_err(|e| {
                        SessionError::capture(SessionErrorKind::Other(Box::new(e)))
                    })?;
                    Ok(Some(ChunkIndices(new_index)))
                }
            })
        }

        let forward = make_py_reindex_closure(py, forward);
        let mapping = match backward {
            Some(backward) => ReindexMapping::ForwardBackward {
                forward,
                backward: make_py_reindex_closure(py, backward),
            },
            None => ReindexMapping::ForwardOnly(forward),
        };
        // TODO: detach
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut session = self.0.write().await;
            session
                .reindex_array(&array_path, mapping)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    pub fn shift_array(
        &mut self,
        array_path: String,
        chunk_offset: Vec<i64>,
    ) -> PyResult<()> {
        let array_path = Path::new(array_path.as_str())
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;

        // TODO: detach
        pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
            let mut session = self.0.write().await;
            session
                .shift_array(&array_path, chunk_offset.as_slice())
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
            let store = Store::from_session_and_config(Arc::clone(&self.0), conc);

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
        let session = Arc::clone(&self.0);
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

    /// Return vectors of coordinates, up to `batch_size` in length.
    ///
    /// We batch the results to make it faster.
    pub fn chunk_coordinates(
        &self,
        array_path: String,
        batch_size: u32,
    ) -> PyResult<PyAsyncCloseableIterator> {
        // This is blocking function, we need to release the Gil
        let session = Arc::clone(&self.0);
        let res = try_stream! {
            let session = session.read_owned().await;
            let array_path = array_path.try_into().map_err(|e| PyIcechunkStoreError::PyValueError(format!("Invalid path: {e}")))?;

            let stream = session
                .chunk_coordinates(&array_path)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?
                .map_err(PyIcechunkStoreError::SessionError)
                .chunks(batch_size as usize);

            for await coords_vec in stream {
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
            }
        };

        let prepared_list = Arc::new(Mutex::new(res.boxed()));
        Ok(PyAsyncCloseableIterator::new(prepared_list))
    }

    pub fn chunk_type(
        &self,
        array_path: String,
        coords: Vec<u32>,
    ) -> PyResult<ChunkType> {
        let session = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(Self::chunk_type_inner(session, array_path, coords))
    }

    pub fn chunk_type_async<'py>(
        &'py self,
        py: Python<'py>,
        array_path: String,
        coords: Vec<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.0);
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
        let session = Arc::clone(&self.0);
        let other = Arc::clone(&other.0);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut session = session.write().await;
            let other = other.read().await.deref().clone();
            session.merge(other).await.map_err(PyIcechunkStoreError::SessionError)?;
            Ok(())
        })
    }

    #[pyo3(signature = (message, metadata=None, rebase_with=None, rebase_tries=1_000, allow_empty=false))]
    pub fn commit(
        &self,
        py: Python<'_>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        rebase_with: Option<PyConflictSolver>,
        rebase_tries: Option<u16>,
        allow_empty: bool,
    ) -> PyResult<String> {
        let metadata = metadata.map(|m| m.into());
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut session = self.0.write().await;
                let snapshot_id = if let Some(solver) = &rebase_with {
                    let mut builder = session
                        .commit(message)
                        .allow_empty(allow_empty)
                        .rebase(solver.as_ref(), rebase_tries.unwrap_or(1_000));
                    if let Some(props) = metadata {
                        builder = builder.properties(props);
                    }
                    builder.execute().await
                } else {
                    let mut builder = session.commit(message).allow_empty(allow_empty);
                    if let Some(props) = metadata {
                        builder = builder.properties(props);
                    }
                    builder.execute().await
                }
                .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(snapshot_id.to_string())
            })
        })
    }

    #[pyo3(signature = (message, metadata=None, rebase_with=None, rebase_tries=1_000, allow_empty=false))]
    pub fn commit_async<'py>(
        &'py self,
        py: Python<'py>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        rebase_with: Option<PyConflictSolver>,
        rebase_tries: Option<u16>,
        allow_empty: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.0);
        let message = message.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = metadata.map(|m| m.into());
            let mut session = session.write().await;
            let snapshot_id = if let Some(solver) = &rebase_with {
                let mut builder = session
                    .commit(&message)
                    .allow_empty(allow_empty)
                    .rebase(solver.as_ref(), rebase_tries.unwrap_or(1_000));
                if let Some(props) = metadata {
                    builder = builder.properties(props);
                }
                builder.execute().await
            } else {
                let mut builder = session.commit(&message).allow_empty(allow_empty);
                if let Some(props) = metadata {
                    builder = builder.properties(props);
                }
                builder.execute().await
            }
            .map_err(PyIcechunkStoreError::SessionError)?;
            Ok(snapshot_id.to_string())
        })
    }

    #[pyo3(signature = (message, metadata=None, allow_empty=false))]
    pub fn amend(
        &self,
        py: Python<'_>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        allow_empty: bool,
    ) -> PyResult<String> {
        let metadata = metadata.map(|m| m.into());
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let mut session = self.0.write().await;
                let mut builder =
                    session.commit(message).amend().allow_empty(allow_empty);
                if let Some(props) = metadata {
                    builder = builder.properties(props);
                }
                let snapshot_id = builder
                    .execute()
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok(snapshot_id.to_string())
            })
        })
    }

    #[pyo3(signature = (message, metadata=None, allow_empty=false))]
    pub fn amend_async<'py>(
        &'py self,
        py: Python<'py>,
        message: &str,
        metadata: Option<PySnapshotProperties>,
        allow_empty: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = Arc::clone(&self.0);
        let message = message.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = metadata.map(|m| m.into());
            let mut session = session.write().await;
            let mut builder = session.commit(&message).amend().allow_empty(allow_empty);
            if let Some(props) = metadata {
                builder = builder.properties(props);
            }
            let snapshot_id =
                builder.execute().await.map_err(PyIcechunkStoreError::SessionError)?;
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
                let mut builder = session.commit(message).anonymous();
                if let Some(props) = metadata {
                    builder = builder.properties(props);
                }
                let snapshot_id = builder
                    .execute()
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
        let session = Arc::clone(&self.0);
        let message = message.to_owned();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metadata = metadata.map(|m| m.into());
            let mut session = session.write().await;
            let mut builder = session.commit(&message).anonymous();
            if let Some(props) = metadata {
                builder = builder.properties(props);
            }
            let snapshot_id =
                builder.execute().await.map_err(PyIcechunkStoreError::SessionError)?;
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
        let session = Arc::clone(&self.0);

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
            .map_err(|e| StoreError::capture(StoreErrorKind::PathError(e)))
            .map_err(PyIcechunkStoreError::StoreError)?;
        let res = session
            .get_chunk_ref(&array_path, &ChunkIndices(coords))
            .await
            .map_err(PyIcechunkStoreError::SessionError)?;

        Ok(res.as_ref().map(ChunkType::from).unwrap_or(ChunkType::Uninitialized))
    }
}

/// Convert a resolved [`ChunkPayload`] into a [`ResolvedChunkRef`], expanding
/// virtual locations through the session's resolver. This is the same payload →
/// `(kind, location, offset, length, inline)` mapping `array_chunk_iterator`
/// performs per row.
fn payload_to_resolved(
    session: &Session,
    payload: ChunkPayload,
) -> PyResult<ResolvedChunkRef> {
    let kind = ChunkType::from(&payload) as u8;
    let cref = match payload {
        ChunkPayload::Virtual(VirtualChunkRef { location, offset, length, .. }) => {
            let url = session.resolve_virtual_location(&location).map_err(|e| {
                PyIcechunkStoreError::SessionError(SessionError::capture(
                    SessionErrorKind::VirtualReferenceError(e.kind),
                ))
            })?;
            ResolvedChunkRef { kind, location: url, offset, length, inline_data: None }
        }
        ChunkPayload::Ref(ChunkRef { id, offset, length }) => ResolvedChunkRef {
            kind,
            location: format!("{id}"),
            offset,
            length,
            inline_data: None,
        },
        ChunkPayload::Inline(bytes) => ResolvedChunkRef {
            kind,
            location: String::new(),
            offset: 0,
            length: bytes.len() as u64,
            inline_data: Some(bytes),
        },
        other => {
            return Err(PyIcechunkStoreError::PyValueError(format!(
                "resolve_chunk_refs encountered an unsupported ChunkPayload variant: {other:?}"
            ))
            .into());
        }
    };
    Ok(cref)
}

/// Resolve an explicit batch of chunk coordinates for one array to their
/// references, reusing the same lazy per-key lookup the read path uses. Returns
/// one [`ResolvedChunkRef`] per input coord in input order; uninitialized coords
/// get `kind == 0` (`ChunkType::Uninitialized`). The per-coord loop lives here
/// (in Rust) so callers cross the FFI boundary once.
pub(crate) async fn resolve_chunk_refs_impl(
    session: Arc<RwLock<Session>>,
    array_path: String,
    coords: Vec<Vec<u32>>,
) -> PyResult<Vec<ResolvedChunkRef>> {
    let session = session.read().await;
    let array_path = crate::store::parse_array_path(array_path)?;

    // Each coord is resolved through the same lazy per-key lookup the read path
    // uses (`get_chunk_ref`), so only the manifest pages the coords fall in get
    // fetched — never the whole manifest.
    //
    // We fan the lookups out concurrently, mirroring the read path
    // (`Store::get_partial_values`), so cold manifest-page fetches happen in
    // parallel instead of serializing one network round-trip per page. Lookups
    // that land in the same page dedupe through the asset manager's
    // single-flight cache, keeping the cost O(#pages touched). `buffered`
    // preserves input order.
    let conc = (session.config().get_partial_values_concurrency() as usize).max(1);
    let session = &session;
    let array_path = &array_path;

    futures::stream::iter(coords.into_iter())
        .map(|coord| async move {
            let payload = session
                .get_chunk_ref(array_path, &ChunkIndices(coord))
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;
            match payload {
                Some(p) => payload_to_resolved(session, p),
                // Uninitialized coord: kind 0, empty/zero columns.
                None => Ok(ResolvedChunkRef {
                    kind: ChunkType::Uninitialized as u8,
                    location: String::new(),
                    offset: 0,
                    length: 0,
                    inline_data: None,
                }),
            }
        })
        .buffered(conc)
        .try_collect::<Vec<_>>()
        .await
}
