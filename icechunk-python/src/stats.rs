use pyo3::{pyclass, pymethods};

use crate::display::{PyRepr, ReprMode};
use icechunk::ops::stats::ChunkStorageStats;

/// Statistics about chunk storage across different chunk types.
#[pyclass(name = "ChunkStorageStats")]
#[derive(Clone, Debug)]
pub(crate) struct PyChunkStorageStats {
    inner: ChunkStorageStats,
}

impl From<ChunkStorageStats> for PyChunkStorageStats {
    fn from(stats: ChunkStorageStats) -> Self {
        Self { inner: stats }
    }
}

impl PyRepr for PyChunkStorageStats {
    const EXECUTABLE: bool = false;
    fn cls_name() -> &'static str {
        "icechunk.ChunkStorageStats"
    }
    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("native_bytes", self.inner.native_bytes.to_string()),
            ("virtual_bytes", self.inner.virtual_bytes.to_string()),
            ("inlined_bytes", self.inner.inlined_bytes.to_string()),
        ]
    }
}

#[pymethods]
impl PyChunkStorageStats {
    /// Total bytes stored in native chunks (stored in icechunk's chunk storage)
    #[getter]
    pub(crate) fn native_bytes(&self) -> u64 {
        self.inner.native_bytes
    }

    /// Total bytes stored in virtual chunks (references to external data)
    #[getter]
    pub(crate) fn virtual_bytes(&self) -> u64 {
        self.inner.virtual_bytes
    }

    /// Total bytes stored in inline chunks (stored directly in manifests)
    #[getter]
    pub(crate) fn inlined_bytes(&self) -> u64 {
        self.inner.inlined_bytes
    }

    /// Total bytes excluding virtual chunks.
    ///
    /// This represents the approximate size of all objects stored in the
    /// icechunk repository itself (native chunks plus inline chunks).
    /// Virtual chunks are not included since they reference external data.
    ///
    /// Returns:
    ///     int: The sum of `native_bytes` and `inlined_bytes`
    pub(crate) fn non_virtual_bytes(&self) -> u64 {
        self.inner.non_virtual_bytes()
    }

    /// Total bytes across all chunk types.
    ///
    /// Returns the sum of `native_bytes`, `virtual_bytes`, and `inlined_bytes`.
    /// This represents the total size of all data referenced by the repository,
    /// including both data stored in icechunk and external virtual references.
    ///
    /// Returns:
    ///     int: The sum of all chunk storage bytes
    pub(crate) fn total_bytes(&self) -> u64 {
        self.inner.total_bytes()
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

    pub(crate) fn __add__(&self, other: &PyChunkStorageStats) -> PyChunkStorageStats {
        PyChunkStorageStats { inner: self.inner + other.inner }
    }
}
