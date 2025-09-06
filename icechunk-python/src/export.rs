use pyo3::{pyclass, pymethods};

#[pyclass(name = "ExportVersionSelection", eq, subclass)]
#[derive(Debug, PartialEq, Eq)]
pub struct PyVersionSelection;

#[pyclass(name = "ExportSingleSnapshot", eq, extends=PyVersionSelection)]
#[derive(Debug, PartialEq, Eq)]
pub struct PySingleSnapshot {
    #[pyo3(get)]
    pub snapshot_id: String,
}

#[pymethods]
impl PySingleSnapshot {
    #[new]
    fn new(snapshot_id: String) -> (Self, PyVersionSelection) {
        (PySingleSnapshot { snapshot_id }, PyVersionSelection)
    }
}

#[pyclass(name = "ExportAllHistory", eq, extends=PyVersionSelection)]
#[derive(Debug, PartialEq, Eq)]
pub struct PyAllHistory;

#[pymethods]
impl PyAllHistory {
    #[new]
    fn new() -> (Self, PyVersionSelection) {
        (PyAllHistory, PyVersionSelection)
    }
}

#[pyclass(name = "ExportRefsHistory", eq, extends=PyVersionSelection)]
#[derive(Debug, PartialEq, Eq)]
pub struct PyRefsHistory {
    #[pyo3(get)]
    pub branches: Vec<String>,
    #[pyo3(get)]
    pub tags: Vec<String>,
}

#[pymethods]
impl PyRefsHistory {
    #[new]
    fn new(branches: Vec<String>, tags: Vec<String>) -> (Self, PyVersionSelection) {
        (PyRefsHistory { branches, tags }, PyVersionSelection)
    }
}
