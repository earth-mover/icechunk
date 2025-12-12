use pyo3::{pyclass, pymethods};

use icechunk::ops::stats::ChunkStorageStats;

/// Statistics about chunk storage across different chunk types.
#[pyclass(name = "ChunkStorageStats")]
#[derive(Clone, Debug)]
pub struct PyChunkStorageStats {
    /// Total bytes stored in native chunks (stored in icechunk's chunk storage)
    #[pyo3(get)]
    pub native_bytes: u64,
    /// Total bytes stored in virtual chunks (references to external data)
    #[pyo3(get)]
    pub virtual_bytes: u64,
    /// Total bytes stored in inline chunks (stored directly in manifests)
    #[pyo3(get)]
    pub inlined_bytes: u64,
}

impl From<ChunkStorageStats> for PyChunkStorageStats {
    fn from(stats: ChunkStorageStats) -> Self {
        Self {
            native_bytes: stats.native_bytes,
            virtual_bytes: stats.virtual_bytes,
            inlined_bytes: stats.inlined_bytes,
        }
    }
}

#[pymethods]
impl PyChunkStorageStats {
    /// Total bytes excluding virtual chunks.
    ///
    /// This represents the approximate size of all objects stored in the
    /// icechunk repository itself (native chunks plus inline chunks).
    /// Virtual chunks are not included since they reference external data.
    ///
    /// Returns:
    ///     int: The sum of native_bytes and inlined_bytes
    pub fn non_virtual_bytes(&self) -> u64 {
        self.native_bytes
            .saturating_add(self.native_bytes)
            .saturating_add(self.inlined_bytes)
    }

    /// Total bytes across all chunk types.
    ///
    /// Returns the sum of native_bytes, virtual_bytes, and inlined_bytes.
    /// This represents the total size of all data referenced by the repository,
    /// including both data stored in icechunk and external virtual references.
    ///
    /// Returns:
    ///     int: The sum of all chunk storage bytes
    pub fn total_bytes(&self) -> u64 {
        self.native_bytes
            .saturating_add(self.virtual_bytes)
            .saturating_add(self.inlined_bytes)
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ChunkStorageStats(native_bytes={}, virtual_bytes={}, inlined_bytes={})",
            self.native_bytes, self.virtual_bytes, self.inlined_bytes
        )
    }
}
