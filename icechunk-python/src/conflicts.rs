use std::{fmt::Display, sync::Arc};

use icechunk::conflicts::{
    Conflict, ConflictSolver,
    basic_solver::{BasicConflictSolver, VersionSelection},
    detector::ConflictDetector,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::impl_pickle;

#[pyclass(name = "ConflictType", eq)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PyConflictType {
    NewNodeConflictsWithExistingNode = 1,
    NewNodeInInvalidGroup = 2,
    ZarrMetadataDoubleUpdate = 3,
    ZarrMetadataUpdateOfDeletedArray = 4,
    ZarrMetadataUpdateOfDeletedGroup = 5,
    ChunkDoubleUpdate = 6,
    ChunksUpdatedInDeletedArray = 7,
    ChunksUpdatedInUpdatedArray = 8,
    DeleteOfUpdatedArray = 9,
    DeleteOfUpdatedGroup = 10,
}

impl Display for PyConflictType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant_str = match self {
            PyConflictType::NewNodeConflictsWithExistingNode => {
                "New node conflicts with existing node"
            }
            PyConflictType::NewNodeInInvalidGroup => "New node in invalid group",
            PyConflictType::ZarrMetadataDoubleUpdate => "Zarr metadata double update",
            PyConflictType::ZarrMetadataUpdateOfDeletedGroup => {
                "Zarr metadata update of deleted group"
            }
            PyConflictType::ZarrMetadataUpdateOfDeletedArray => {
                "Zarr metadata update of deleted array"
            }
            PyConflictType::ChunkDoubleUpdate => "Chunk double update",
            PyConflictType::ChunksUpdatedInDeletedArray => {
                "Chunks updated in deleted array"
            }
            PyConflictType::ChunksUpdatedInUpdatedArray => {
                "Chunks updated in updated array"
            }
            PyConflictType::DeleteOfUpdatedArray => "Delete of updated array",
            PyConflictType::DeleteOfUpdatedGroup => "Delete of updated group",
        };
        write!(f, "{variant_str}")
    }
}

#[pymethods]
impl PyConflictType {
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    fn __str__(&self) -> String {
        format!("{self}")
    }
}

impl_pickle!(PyConflictType);

#[pyclass(name = "Conflict")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyConflict {
    #[pyo3(get)]
    conflict_type: PyConflictType,
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    conflicted_chunks: Option<Vec<Vec<u32>>>,
}

#[pymethods]
impl PyConflict {
    fn __repr__(&self) -> String {
        format!("Conflict({:?}, path={})", self.conflict_type, self.path)
    }

    fn __str__(&self) -> String {
        format!("{}: {}", self.path, self.conflict_type)
    }
}

impl_pickle!(PyConflict);

impl From<&Conflict> for PyConflict {
    fn from(conflict: &Conflict) -> Self {
        match conflict {
            Conflict::NewNodeConflictsWithExistingNode(path) => PyConflict {
                conflict_type: PyConflictType::NewNodeConflictsWithExistingNode,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::NewNodeInInvalidGroup(path) => PyConflict {
                conflict_type: PyConflictType::NewNodeInInvalidGroup,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::ZarrMetadataDoubleUpdate(path) => PyConflict {
                conflict_type: PyConflictType::ZarrMetadataDoubleUpdate,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::ZarrMetadataUpdateOfDeletedArray(path) => PyConflict {
                conflict_type: PyConflictType::ZarrMetadataUpdateOfDeletedArray,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::ZarrMetadataUpdateOfDeletedGroup(path) => PyConflict {
                conflict_type: PyConflictType::ZarrMetadataUpdateOfDeletedGroup,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::ChunkDoubleUpdate { path, node_id: _, chunk_coordinates } => {
                let mut chunk_coordinates =
                    chunk_coordinates.iter().map(|c| c.0.clone()).collect::<Vec<_>>();
                chunk_coordinates.sort();

                PyConflict {
                    conflict_type: PyConflictType::ChunkDoubleUpdate,
                    path: path.to_string(),
                    conflicted_chunks: Some(chunk_coordinates),
                }
            }
            Conflict::ChunksUpdatedInDeletedArray { path, node_id: _ } => PyConflict {
                conflict_type: PyConflictType::ChunksUpdatedInDeletedArray,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::ChunksUpdatedInUpdatedArray { path, node_id: _ } => PyConflict {
                conflict_type: PyConflictType::ChunksUpdatedInUpdatedArray,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::DeleteOfUpdatedArray { path, node_id: _ } => PyConflict {
                conflict_type: PyConflictType::DeleteOfUpdatedArray,
                path: path.to_string(),
                conflicted_chunks: None,
            },
            Conflict::DeleteOfUpdatedGroup { path, node_id: _ } => PyConflict {
                conflict_type: PyConflictType::DeleteOfUpdatedGroup,
                path: path.to_string(),
                conflicted_chunks: None,
            },
        }
    }
}

#[pyclass(name = "VersionSelection", eq)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PyVersionSelection {
    Fail = 0,
    UseOurs = 1,
    UseTheirs = 2,
}

impl From<PyVersionSelection> for VersionSelection {
    fn from(value: PyVersionSelection) -> Self {
        match value {
            PyVersionSelection::Fail => VersionSelection::Fail,
            PyVersionSelection::UseOurs => VersionSelection::UseOurs,
            PyVersionSelection::UseTheirs => VersionSelection::UseTheirs,
        }
    }
}

impl_pickle!(PyVersionSelection);

#[pyclass(subclass, name = "ConflictSolver")]
#[derive(Clone)]
pub struct PyConflictSolver(Arc<dyn ConflictSolver + Send + Sync>);

impl<'a> AsRef<dyn ConflictSolver + 'a> for PyConflictSolver {
    fn as_ref(&self) -> &(dyn ConflictSolver + 'a) {
        &*self.0
    }
}

#[pyclass(name = "BasicConflictSolver", extends=PyConflictSolver)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyBasicConflictSolver;

#[pymethods]
impl PyBasicConflictSolver {
    #[new]
    #[pyo3(signature = (*, on_chunk_conflict=PyVersionSelection::UseOurs, fail_on_delete_of_updated_array = false, fail_on_delete_of_updated_group = false))]
    fn new(
        on_chunk_conflict: PyVersionSelection,
        fail_on_delete_of_updated_array: bool,
        fail_on_delete_of_updated_group: bool,
    ) -> (Self, PyConflictSolver) {
        (
            Self,
            PyConflictSolver(Arc::new(BasicConflictSolver {
                on_chunk_conflict: on_chunk_conflict.into(),
                fail_on_delete_of_updated_array,
                fail_on_delete_of_updated_group,
            })),
        )
    }
}

impl_pickle!(PyBasicConflictSolver);

#[pyclass(name = "ConflictDetector", extends=PyConflictSolver)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PyConflictDetector;

#[pymethods]
impl PyConflictDetector {
    #[new]
    fn new() -> (Self, PyConflictSolver) {
        (Self, PyConflictSolver(Arc::new(ConflictDetector)))
    }
}

impl_pickle!(PyConflictDetector);
