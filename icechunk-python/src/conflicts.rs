use std::sync::Arc;

use icechunk::conflicts::{
    basic_solver::{BasicConflictSolver, VersionSelection},
    detector::ConflictDetector,
    ConflictSolver,
};
use pyo3::prelude::*;

#[pyclass(name = "VersionSelection", eq)]
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[pyclass(subclass, name = "ConflictSolver")]
#[derive(Clone)]
pub struct PyConflictSolver(Arc<dyn ConflictSolver + Send + Sync>);

impl<'a> AsRef<dyn ConflictSolver + 'a> for PyConflictSolver {
    fn as_ref(&self) -> &(dyn ConflictSolver + 'a) {
        &*self.0
    }
}

#[pyclass(name = "BasicConflictSolver", extends=PyConflictSolver)]
#[derive(Debug, Clone)]
pub struct PyBasicConflictSolver;

#[pymethods]
impl PyBasicConflictSolver {
    #[new]
    #[pyo3(signature = (*, on_user_attributes_conflict=PyVersionSelection::UseOurs, on_chunk_conflict=PyVersionSelection::UseOurs, fail_on_delete_of_updated_array = false, fail_on_delete_of_updated_group = false))]
    fn new(
        on_user_attributes_conflict: PyVersionSelection,
        on_chunk_conflict: PyVersionSelection,
        fail_on_delete_of_updated_array: bool,
        fail_on_delete_of_updated_group: bool,
    ) -> (Self, PyConflictSolver) {
        (
            Self,
            PyConflictSolver(Arc::new(BasicConflictSolver {
                on_user_attributes_conflict: on_user_attributes_conflict.into(),
                on_chunk_conflict: on_chunk_conflict.into(),
                fail_on_delete_of_updated_array,
                fail_on_delete_of_updated_group,
            })),
        )
    }
}

#[pyclass(name = "ConflictDetector", extends=PyConflictSolver)]
#[derive(Debug, Clone)]
pub struct PyConflictDetector;

#[pymethods]
impl PyConflictDetector {
    #[new]
    fn new() -> (Self, PyConflictSolver) {
        (Self, PyConflictSolver(Arc::new(ConflictDetector)))
    }
}
