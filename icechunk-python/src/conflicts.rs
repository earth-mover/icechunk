use icechunk::conflicts::{basic_solver::{BasicConflictSolver, VersionSelection}, detector::ConflictDetector};
use pyo3::{prelude::*, types::PyType};

#[pyclass(name = "VersionSelection")]
#[derive(Clone, Debug)]
pub struct PyVersionSelection(VersionSelection);

impl Default for PyVersionSelection {
    fn default() -> Self {
        PyVersionSelection(VersionSelection::UseOurs)
    }
}

#[pymethods]
impl PyVersionSelection {
    #[classmethod]
    fn use_ours(_cls: &Bound<'_, PyType>) -> Self {
        PyVersionSelection(VersionSelection::UseOurs)
    }

    #[classmethod]
    fn use_theirs(_cls: &Bound<'_, PyType>) -> Self {
        PyVersionSelection(VersionSelection::UseTheirs)
    }

    #[classmethod]
    fn fail(_cls: &Bound<'_, PyType>) -> Self {
        PyVersionSelection(VersionSelection::Fail)
    }
}

impl From<PyVersionSelection> for VersionSelection {
    fn from(value: PyVersionSelection) -> Self {
        value.0
    }
}

impl From<&PyVersionSelection> for VersionSelection {
    fn from(value: &PyVersionSelection) -> Self {
        value.0.clone()
    }
}

impl AsRef<VersionSelection> for PyVersionSelection {
    fn as_ref(&self) -> &VersionSelection {
        &self.0
    }
}

#[pyclass(name = "ConflictDetector")]
pub struct PyConflictDetector(ConflictDetector);

#[pymethods]
impl PyConflictDetector {
    #[new]
    fn new() -> Self {
        PyConflictDetector(ConflictDetector)
    }
}

#[pyclass(name = "BasicConflictSolver")]
#[derive(Clone, Debug)]
pub struct PyBasicConflictSolver(BasicConflictSolver);

#[pymethods]
impl PyBasicConflictSolver {
    #[new]
    #[pyo3(signature = (*, on_user_attributes_conflict=PyVersionSelection::default(), on_chunk_conflict=PyVersionSelection::default(), fail_on_delete_of_updated_array = false, fail_on_delete_of_updated_group = false))]
    fn new(
        on_user_attributes_conflict: PyVersionSelection,
        on_chunk_conflict: PyVersionSelection,
        fail_on_delete_of_updated_array: bool,
        fail_on_delete_of_updated_group: bool,
    ) -> Self {
        PyBasicConflictSolver(BasicConflictSolver {
            on_user_attributes_conflict: on_user_attributes_conflict.into(),
            on_chunk_conflict: on_chunk_conflict.into(),
            fail_on_delete_of_updated_array,
            fail_on_delete_of_updated_group,
        })
    }
}

impl From<PyBasicConflictSolver> for BasicConflictSolver {
    fn from(value: PyBasicConflictSolver) -> Self {
        value.0
    }
}

impl From<&PyBasicConflictSolver> for BasicConflictSolver {
    fn from(value: &PyBasicConflictSolver) -> Self {
        value.0.clone()
    }
}

impl AsRef<BasicConflictSolver> for PyBasicConflictSolver {
    fn as_ref(&self) -> &BasicConflictSolver {
        &self.0
    }
}
