use std::{borrow::Cow, sync::Arc};

use async_stream::try_stream;
use bytes::Bytes;
use chrono::Utc;
use futures::{StreamExt as _, TryStreamExt as _};
use icechunk::{
    Store,
    format::{
        ChunkIndices, ChunkLength, ChunkOffset, Path,
        manifest::{Checksum, SecondsSinceEpoch, VirtualChunkLocation, VirtualChunkRef},
    },
    session::CoalescingReport,
    storage::ETag,
    store::{SetVirtualRefsResult, StoreError, StoreErrorKind},
};
use itertools::Itertools as _;
use numpy::{IntoPyArray as _, PyArrayMethods as _, PyReadonlyArray1};
use pyo3::{
    conversion::IntoPyObjectExt as _,
    exceptions::{PyKeyError, PyValueError},
    prelude::*,
    types::{PyBytes as PyBytesType, PyDict, PyList, PyTuple, PyType},
};
use pyo3_bytes::PyBytes;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    display::{PyRepr, ReprMode, py_bool},
    errors::{PyIcechunkStoreError, PyIcechunkStoreResult, session_error_to_pyerr},
    impl_pickle,
    session::{
        PySession, ResolvedChunkRef, payload_to_resolved, resolve_chunk_refs_impl,
    },
    streams::PyAsyncCloseableIterator,
    virtualrefs::{build_vrefs_from_arrays, do_set_virtual_refs, vrefs_result_to_py},
};

type KeyRanges = Vec<(String, (Option<ChunkOffset>, Option<ChunkOffset>))>;

/// Normalize a user-supplied zarr array path string into an icechunk [`Path`].
///
/// Prepends a leading `/` if missing (icechunk paths are absolute), then runs
/// the standard path validator. Used by every `PyStore` method that takes a
/// caller-supplied array path.
pub(crate) fn parse_array_path(path: String) -> PyResult<Path> {
    let path = if path.starts_with('/') { path } else { format!("/{path}") };
    Path::try_from(path)
        .map_err(|e| PyValueError::new_err(format!("Invalid array path: {e}")))
}

/// Parse a list of `(array_path, coords)` requests into `(Path, ChunkIndices)`
/// pairs, as `get_many_chunks` / `coalescing_report` take them.
fn parse_requests(
    requests: Vec<(String, Vec<u32>)>,
) -> PyResult<Vec<(Path, ChunkIndices)>> {
    requests
        .into_iter()
        .map(|(array_path, coords)| {
            Ok((parse_array_path(array_path)?, ChunkIndices(coords)))
        })
        .collect()
}

/// Pack resolved chunk refs into the columnar tuple the chunk-reference APIs
/// return: `(kinds, paths, offsets, lengths, inlined)`, with row `i` describing
/// the `i`th ref. This avoids allocating one Python object per chunk — the
/// arrays are built in one pass.
///
/// `coords` prepends a `(n, ndim)` coords column, as `array_chunk_iterator`
/// yields; `resolve_chunk_refs` omits it because the caller already has the
/// coords it asked for, in order.
fn resolved_refs_to_columns(
    py: Python<'_>,
    resolved: Vec<ResolvedChunkRef>,
    coords: Option<(Vec<u32>, usize)>,
) -> PyResult<Py<PyAny>> {
    let n = resolved.len();
    let mut kinds: Vec<u8> = Vec::with_capacity(n);
    let mut paths: Vec<String> = Vec::with_capacity(n);
    let mut offsets: Vec<u64> = Vec::with_capacity(n);
    let mut lengths: Vec<u64> = Vec::with_capacity(n);
    let mut inlined: Vec<(usize, Bytes)> = Vec::new();

    for (i, r) in resolved.into_iter().enumerate() {
        kinds.push(r.kind);
        paths.push(r.location);
        offsets.push(r.offset);
        lengths.push(r.length);
        if let Some(b) = r.inline_data {
            inlined.push((i, b));
        }
    }

    let kinds_arr = kinds.into_pyarray(py).into_any().unbind();
    let offsets_arr = offsets.into_pyarray(py).into_any().unbind();
    let lengths_arr = lengths.into_pyarray(py).into_any().unbind();
    let paths_list: Py<PyAny> = PyList::new(py, paths)?.into_any().unbind();
    let inlined_dict = PyDict::new(py);
    for (i, b) in inlined {
        let slice: &[u8] = b.as_ref();
        inlined_dict.set_item(i, PyBytesType::new(py, slice))?;
    }
    let mut columns: Vec<Py<PyAny>> = Vec::with_capacity(6);
    if let Some((coords_flat, ndim)) = coords {
        columns
            .push(coords_flat.into_pyarray(py).reshape((n, ndim))?.into_any().unbind());
    }
    columns.extend([
        kinds_arr,
        paths_list,
        offsets_arr,
        lengths_arr,
        inlined_dict.into_any().unbind(),
    ]);
    Ok(PyTuple::new(py, columns)?.into_any().unbind())
}

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

#[pyclass(from_py_object)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct VirtualChunkSpec {
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

#[pyclass(skip_from_py_object, name = "PyStore")]
#[derive(Clone, Debug)]
pub struct PyStore(pub Arc<Store>);

impl PyRepr for PyStore {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.IcechunkStore"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        let session = self.0.session();
        let session_guard = session.blocking_read();
        let branch = session_guard
            .branch()
            .map(|b| b.to_string())
            .unwrap_or_else(|| "None".to_string());
        vec![
            ("read_only", py_bool(session_guard.read_only())),
            ("snapshot_id", session_guard.snapshot_id().to_string()),
            ("branch", branch),
        ]
    }
}

#[pymethods]
impl PyStore {
    #[classmethod]
    fn from_bytes(
        _cls: Bound<'_, PyType>,
        py: Python<'_>,
        bytes: PyBytes,
    ) -> PyResult<Self> {
        // This is a compute intensive task, we need to release the Gil
        py.detach(move || {
            let bytes = bytes.into_inner();
            let store = Store::from_bytes(&bytes).map_err(|e| {
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

    pub(crate) fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    pub(crate) fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    pub(crate) fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }

    #[getter]
    fn read_only(&self, py: Python<'_>) -> PyIcechunkStoreResult<bool> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let read_only = self.0.read_only().await;
                Ok(read_only)
            })
        })
    }

    fn as_bytes(&self, py: Python<'_>) -> PyResult<Cow<'_, [u8]>> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
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
        py.detach(move || {
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
                Ok(data) => Ok(PyBytes::new(data)),
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
        value: PyBytes,
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
                .set(&key, value.into_inner())
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    fn set_if_not_exists<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        value: PyBytes,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            store
                .set_if_not_exists(&key, value.into_inner())
                .await
                .map_err(PyIcechunkStoreError::from)?;
            Ok(())
        })
    }

    #[expect(clippy::too_many_arguments)]
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
        py.detach(move || {
            let store = Arc::clone(&self.0);

            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let location = VirtualChunkLocation::from_url(location.as_str())
                    .map_err(PyIcechunkStoreError::from)?;
                let virtual_ref = VirtualChunkRef {
                    location,
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

    #[expect(clippy::too_many_arguments)]
    fn set_virtual_ref_async<'py>(
        &'py self,
        py: Python<'py>,
        key: String,
        location: String,
        offset: ChunkOffset,
        length: ChunkLength,
        checksum: Option<ChecksumArgument>,
        validate_container: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let location = VirtualChunkLocation::from_url(location.as_str())
                .map_err(PyIcechunkStoreError::from)?;
            let virtual_ref = VirtualChunkRef {
                location,
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
    }

    fn set_virtual_refs(
        &self,
        py: Python<'_>,
        array_path: String,
        chunks: Vec<VirtualChunkSpec>,
        validate_containers: bool,
    ) -> PyIcechunkStoreResult<Option<Vec<Py<PyTuple>>>> {
        py.detach(move || {
            let store = Arc::clone(&self.0);

            let res = pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let vrefs: Vec<(ChunkIndices, VirtualChunkRef)> = chunks
                    .into_iter()
                    .map(|vcs| {
                        let checksum = vcs.checksum();
                        let index = ChunkIndices(vcs.index);
                        let location =
                            VirtualChunkLocation::from_url(vcs.location.as_str())
                                .map_err(PyIcechunkStoreError::from)?;
                        let vref = VirtualChunkRef {
                            offset: vcs.offset,
                            length: vcs.length,
                            location,
                            checksum,
                        };
                        Ok::<_, PyIcechunkStoreError>((index, vref))
                    })
                    .try_collect()?;

                let path = parse_array_path(array_path.clone())?;

                let res = store
                    .set_virtual_refs(&path, validate_containers, vrefs)
                    .await
                    .map_err(PyIcechunkStoreError::from)?;
                Ok::<_, PyIcechunkStoreError>(res)
            })?;

            match res {
                SetVirtualRefsResult::Done => Ok(None),
                SetVirtualRefsResult::FailedRefs(vec) => Python::attach(|py| {
                    let res = vec
                        .into_iter()
                        .map(|ci| PyTuple::new(py, ci.0).map(|tup| tup.unbind()))
                        .try_collect()?;

                    Ok(Some(res))
                }),
            }
        })
    }

    fn set_virtual_refs_async<'py>(
        &'py self,
        py: Python<'py>,
        array_path: String,
        chunks: Vec<VirtualChunkSpec>,
        validate_containers: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, Option<Vec<Py<PyTuple>>>>(
            py,
            async move {
                let vrefs: Vec<(ChunkIndices, VirtualChunkRef)> = chunks
                    .into_iter()
                    .map(|vcs| {
                        let checksum = vcs.checksum();
                        let index = ChunkIndices(vcs.index);
                        let location =
                            VirtualChunkLocation::from_url(vcs.location.as_str())
                                .map_err(PyIcechunkStoreError::from)?;
                        let vref = VirtualChunkRef {
                            offset: vcs.offset,
                            length: vcs.length,
                            location,
                            checksum,
                        };
                        Ok::<_, PyIcechunkStoreError>((index, vref))
                    })
                    .try_collect()?;

                let path = parse_array_path(array_path.clone())?;

                let res = store
                    .set_virtual_refs(&path, validate_containers, vrefs)
                    .await
                    .map_err(PyIcechunkStoreError::from)?;

                match res {
                    SetVirtualRefsResult::Done => Ok(None),
                    SetVirtualRefsResult::FailedRefs(vec) => Python::attach(|py| {
                        let res = vec
                            .into_iter()
                            .map(|ci| PyTuple::new(py, ci.0).map(|tup| tup.unbind()))
                            .try_collect()?;

                        Ok(Some(res))
                    }),
                }
            },
        )
    }

    /// Set virtual references from columnar data (list of locations + numpy arrays).
    ///
    /// More efficient than `set_virtual_refs` as it avoids creating per-chunk
    /// Python `VirtualChunkSpec` objects.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (array_path, chunk_grid_shape, locations, offsets, lengths, *, validate_containers = true, arr_offset = None, checksum = None))]
    fn set_virtual_refs_arr(
        &self,
        py: Python<'_>,
        array_path: String,
        chunk_grid_shape: Vec<u32>,
        locations: &Bound<'_, PyList>,
        offsets: PyReadonlyArray1<'_, u64>,
        lengths: PyReadonlyArray1<'_, u64>,
        validate_containers: bool,
        arr_offset: Option<Vec<u32>>,
        checksum: Option<ChecksumArgument>,
    ) -> PyResult<Option<Vec<Py<PyTuple>>>> {
        let vrefs = build_vrefs_from_arrays(
            locations,
            offsets.as_slice().map_err(|e| {
                PyValueError::new_err(format!("offsets array must be contiguous: {e}"))
            })?,
            lengths.as_slice().map_err(|e| {
                PyValueError::new_err(format!("lengths array must be contiguous: {e}"))
            })?,
            &chunk_grid_shape,
            arr_offset.as_deref(),
            checksum.map(|c| c.into()),
        )?;

        let store = Arc::clone(&self.0);
        py.detach(move || {
            let res = pyo3_async_runtimes::tokio::get_runtime().block_on(
                do_set_virtual_refs(store, array_path, validate_containers, vrefs),
            )?;
            vrefs_result_to_py(res)
        })
    }

    /// Async variant of `set_virtual_refs_arr`.
    ///
    /// The vref construction still requires the GIL (to borrow strings from
    /// the Python list), but the store insertion releases it. Use
    /// `asyncio.gather()` to overlap vref building for one array with store
    /// insertion for another.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (array_path, chunk_grid_shape, locations, offsets, lengths, *, validate_containers = true, arr_offset = None, checksum = None))]
    fn set_virtual_refs_arr_async<'py>(
        &'py self,
        py: Python<'py>,
        array_path: String,
        chunk_grid_shape: Vec<u32>,
        locations: &Bound<'_, PyList>,
        offsets: PyReadonlyArray1<'_, u64>,
        lengths: PyReadonlyArray1<'_, u64>,
        validate_containers: bool,
        arr_offset: Option<Vec<u32>>,
        checksum: Option<ChecksumArgument>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let vrefs = build_vrefs_from_arrays(
            locations,
            offsets.as_slice().map_err(|e| {
                PyValueError::new_err(format!("offsets array must be contiguous: {e}"))
            })?,
            lengths.as_slice().map_err(|e| {
                PyValueError::new_err(format!("lengths array must be contiguous: {e}"))
            })?,
            &chunk_grid_shape,
            arr_offset.as_deref(),
            checksum.map(|c| c.into()),
        )?;

        let store = Arc::clone(&self.0);
        pyo3_async_runtimes::tokio::future_into_py::<_, Option<Vec<Py<PyTuple>>>>(
            py,
            async move {
                let res =
                    do_set_virtual_refs(store, array_path, validate_containers, vrefs)
                        .await?;
                vrefs_result_to_py(res)
            },
        )
    }

    /// Async iterator yielding columnar batches of chunk references for one array.
    ///
    /// Each batch is a 6-tuple; row `i` across the columns describes one
    /// chunk (columns are aligned in lock-step):
    ///
    /// ```text
    ///   coords   : np.ndarray[uint32, (n, ndim)]  chunk grid coordinates
    ///   kinds    : np.ndarray[uint8]              values of icechunk.ChunkType
    ///                                              (native=1, virtual=2, inline=3)
    ///   paths    : list[str]                      URL (virtual) | chunk_id (native) | "" (inline)
    ///   offsets  : np.ndarray[uint64]             byte offset within the source
    ///                                              URL (virtual) or chunk file
    ///                                              (native); 0 for inline
    ///   lengths  : np.ndarray[uint64]             byte length; equals
    ///                                              len(bytes) for inline
    ///   inlined  : dict[int, bytes]               *Inline rows only*. Keyed by
    ///                                              the row index `i` in this
    ///                                              batch. Rows whose kind is
    ///                                              virtual, native, or missing
    ///                                              are NOT present in this dict.
    /// ```
    ///
    /// Native and virtual rows share the same `(path, offset, length)` shape;
    /// the only structural difference is that `paths[i]` holds a bare
    /// `chunk_id` for native rows vs a fully-resolved URL for virtual rows.
    /// Consumers can therefore treat both uniformly as virtual references
    /// after prepending a URL prefix to the `chunk_id`.
    ///
    /// Virtual locations are passed through the session's resolver before
    /// yielding — relative `vcc://name/path` forms expand to absolute URLs;
    /// absolute URLs pass through unchanged. Missing chunks are not yielded.
    fn array_chunk_iterator(
        &self,
        array_path: String,
        batch_size: u32,
    ) -> PyResult<PyAsyncCloseableIterator> {
        let store = Arc::clone(&self.0);
        let res = try_stream! {
            let session_lock = store.session();
            let session = session_lock.read_owned().await;
            let path = parse_array_path(array_path).map_err(|e| {
                PyIcechunkStoreError::PyError(e)
            })?;
            let stream = session
                .array_chunk_iterator(&path)
                .await
                .map_err(PyIcechunkStoreError::SessionError)
                .chunks(batch_size as usize);

            for await infos in stream {
                let mut coords_flat: Vec<u32> = Vec::new();
                let mut ndim: usize = 0;
                let mut resolved: Vec<ResolvedChunkRef> =
                    Vec::with_capacity(infos.len());

                for ci_res in infos {
                    let ci = ci_res?;
                    ndim = ci.coord.0.len();
                    coords_flat.extend_from_slice(&ci.coord.0);
                    resolved.push(payload_to_resolved(&session, ci.payload)?);
                }

                let batch = Python::attach(|py| {
                    resolved_refs_to_columns(py, resolved, Some((coords_flat, ndim)))
                })?;

                yield batch;
            }
        };
        let prepared = Arc::new(Mutex::new(res.boxed()));
        Ok(PyAsyncCloseableIterator::new(prepared))
    }

    /// Resolve an explicit set of chunk coordinates for one array to their
    /// references, without scanning the whole manifest.
    ///
    /// Unlike `array_chunk_iterator`, which walks the entire array manifest,
    /// this only reads the manifest pages the requested coordinates fall in —
    /// reusing the same lazy per-key lookup the read path uses.
    ///
    /// Returns a columnar 5-tuple aligned with the input coords (row `i`
    /// describes `coords[i]`), matching `array_chunk_iterator`'s column layout
    /// minus the coords column:
    ///
    /// ```text
    /// kinds    : np.ndarray[uint8]    values of icechunk.ChunkType
    ///                                  (0 = uninitialized/missing)
    /// paths    : list[str]            URL (virtual) | chunk_id (native) | "" otherwise
    /// offsets  : np.ndarray[uint64]
    /// lengths  : np.ndarray[uint64]
    /// inlined  : dict[int, bytes]     inline rows only, keyed by row index
    /// ```
    fn resolve_chunk_refs(
        &self,
        py: Python<'_>,
        array_path: String,
        coords: Vec<Vec<u32>>,
    ) -> PyResult<Py<PyAny>> {
        let session = self.0.session();
        let resolved = py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(resolve_chunk_refs_impl(session, array_path, coords))
        })?;
        resolved_refs_to_columns(py, resolved, None)
    }

    fn resolve_chunk_refs_async<'py>(
        &'py self,
        py: Python<'py>,
        array_path: String,
        coords: Vec<Vec<u32>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.0.session();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let resolved = resolve_chunk_refs_impl(session, array_path, coords).await?;
            Python::attach(|py| resolved_refs_to_columns(py, resolved, None))
        })
    }

    /// Bulk-read many chunks with read coalescing (see `Session::get_many_chunks`).
    ///
    /// `requests` is a list of `(array_path, coords)` and may span multiple
    /// arrays. Returns an async iterator yielding a non-empty
    /// `list[(request_index, bytes, error)]` per completed span, in completion
    /// order, where `request_index` is the chunk's position in `requests`.
    /// `bytes` is that chunk's exact bytes (a zero-copy view of its coalesced
    /// span); `None` means the chunk is uninitialized.
    ///
    /// Coalescing knobs `max_gap` / `max_coalesced_bytes` are per call. Chunks
    /// are grouped and coalesced per manifest, and manifests are pipelined.
    #[pyo3(signature = (requests, max_gap, max_coalesced_bytes=None))]
    fn get_many_chunks(
        &self,
        requests: Vec<(String, Vec<u32>)>,
        max_gap: u64,
        max_coalesced_bytes: Option<u64>,
    ) -> PyResult<PyAsyncCloseableIterator> {
        let store = Arc::clone(&self.0);
        // Parsed before the stream is built so a malformed path is raised by this
        // call rather than deferred to the first `__anext__`. Resolving the array
        // itself needs the session lock, so a path that parses but names no array
        // (or names a group) can only surface on first iteration.
        let parsed = parse_requests(requests)?;
        let res = try_stream! {
            let session_lock = store.session();
            let session = session_lock.read_owned().await;

            let stream = session
                .get_many_chunks(parsed, max_gap, max_coalesced_bytes)
                .await
                .map_err(PyIcechunkStoreError::SessionError)?;

            // One `Python::attach` (hence one GIL acquire and one asyncio
            // round-trip) per batch rather than per chunk. At small chunk sizes
            // that delivery cost otherwise dominates the I/O the coalescing saves.
            for await outcomes in stream {
                let batch = Python::attach(|py| -> PyResult<Py<PyAny>> {
                    let none = || py.None().into_bound(py);
                    let mut rows: Vec<Bound<'_, PyAny>> =
                        Vec::with_capacity(outcomes.len());
                    for (index, outcome) in outcomes {
                        let (data, error): (Bound<'_, PyAny>, Bound<'_, PyAny>) = match outcome {
                            // Zero-copy: wrap the chunk's Bytes via the buffer protocol.
                            Ok(Some(b)) => (PyBytes::new(b).into_bound_py_any(py)?, none()),
                            Ok(None) => (none(), none()),
                            // Per-chunk failure: the rest of the batch still streams.
                            Err(err) => (
                                none(),
                                session_error_to_pyerr(&err).into_value(py).into_bound(py).into_any(),
                            ),
                        };
                        rows.push(
                            PyTuple::new(py, [index.into_bound_py_any(py)?, data, error])?
                                .into_any(),
                        );
                    }
                    Ok(PyList::new(py, rows)?.into_any().unbind())
                })?;
                yield batch;
            }
        };
        let prepared = Arc::new(Mutex::new(res.boxed()));
        Ok(PyAsyncCloseableIterator::new(prepared))
    }

    /// Diagnostic: resolve `requests` and plan spans without fetching, returning
    /// the coalescing stats (span count, over-read, kind breakdown) as a dict.
    /// Use it to see whether coalescing actually merges for an access pattern
    /// and how much it over-reads at a given `max_gap`. Timing this call also
    /// isolates the resolve phase (no download).
    #[pyo3(signature = (requests, max_gap, max_coalesced_bytes=None))]
    fn coalescing_report(
        &self,
        py: Python<'_>,
        requests: Vec<(String, Vec<u32>)>,
        max_gap: u64,
        max_coalesced_bytes: Option<u64>,
    ) -> PyResult<Py<PyAny>> {
        let store = Arc::clone(&self.0);
        let report: CoalescingReport = py.detach(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let session_lock = store.session();
                let session = session_lock.read_owned().await;
                let parsed = parse_requests(requests)?;
                let report = session
                    .coalescing_report(parsed, max_gap, max_coalesced_bytes)
                    .await
                    .map_err(PyIcechunkStoreError::SessionError)?;
                Ok::<CoalescingReport, PyErr>(report)
            })
        })?;

        let d = PyDict::new(py);
        d.set_item("requested", report.requested)?;
        d.set_item("virtual_chunks", report.virtual_chunks)?;
        d.set_item("native_chunks", report.native_chunks)?;
        d.set_item("inline_chunks", report.inline_chunks)?;
        d.set_item("missing_chunks", report.missing_chunks)?;
        d.set_item("spans", report.spans)?;
        d.set_item("useful_bytes", report.useful_bytes)?;
        d.set_item("over_read_bytes", report.over_read_bytes)?;
        d.set_item("max_span_bytes", report.max_span_bytes)?;
        d.set_item("concurrency", report.concurrency)?;
        Ok(d.into_any().unbind())
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
        key_start_values: Vec<(String, ChunkOffset, PyBytes)>,
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
                    (keys[i].as_str(), offset, value.into_inner())
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

    fn list(&self, py: Python<'_>) -> PyIcechunkStoreResult<PyAsyncCloseableIterator> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let store = Arc::clone(&self.0);

            let list = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { store.list().await })?
                .map_err(PyIcechunkStoreError::StoreError)
                .and_then(|s| {
                    futures::future::ready(Python::attach(|py| {
                        s.into_py_any(py).map_err(PyIcechunkStoreError::PyError)
                    }))
                })
                .err_into();

            let prepared_list = Arc::new(Mutex::new(list.boxed()));
            Ok(PyAsyncCloseableIterator::new(prepared_list))
        })
    }

    fn list_prefix(
        &self,
        py: Python<'_>,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncCloseableIterator> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let store = Arc::clone(&self.0);

            let list = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { store.list_prefix(prefix.as_str()).await })?
                .map_err(PyIcechunkStoreError::StoreError)
                .and_then(|s| {
                    futures::future::ready(Python::attach(|py| {
                        s.into_py_any(py).map_err(PyIcechunkStoreError::PyError)
                    }))
                })
                .err_into();
            let prepared_list = Arc::new(Mutex::new(list.boxed()));
            Ok(PyAsyncCloseableIterator::new(prepared_list))
        })
    }

    fn list_dir(
        &self,
        py: Python<'_>,
        prefix: String,
    ) -> PyIcechunkStoreResult<PyAsyncCloseableIterator> {
        // This is blocking function, we need to release the Gil
        py.detach(move || {
            let store = Arc::clone(&self.0);

            let list = pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { store.list_dir(prefix.as_str()).await })?
                .map_err(PyIcechunkStoreError::StoreError)
                .and_then(|s| {
                    futures::future::ready(Python::attach(|py| {
                        s.into_py_any(py).map_err(PyIcechunkStoreError::PyError)
                    }))
                })
                .err_into();
            let prepared_list = Arc::new(Mutex::new(list.boxed()));
            Ok(PyAsyncCloseableIterator::new(prepared_list))
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
