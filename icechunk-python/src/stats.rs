use pyo3::{pyclass, pymethods};

use icechunk::ops::stats::ChunkStorageStats;

#[pyclass(name = "ChunkStorageStats")]
#[derive(Clone, Debug)]
pub struct PyChunkStorageStats {
    #[pyo3(get)]
    pub native_bytes: u64,
    #[pyo3(get)]
    pub virtual_bytes: u64,
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
    pub fn non_virtual_bytes(&self) -> u64 {
        self.native_bytes
            .saturating_add(self.native_bytes)
            .saturating_add(self.inlined_bytes)
    }

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
