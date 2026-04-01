use icechunk::display::PyRepr;
use pyo3::{Py, PyClass, Python};

/// Get the repr of an optional `Py<T>` where `T` implements `PyRepr`.
///
/// Pyclass structs that hold other pyclass structs as attributes store them as
/// `Py<T>` (a Python-managed reference-counted pointer). To access the underlying
/// Rust struct and call the `PyRepr` trait method on it, we need to borrow it
/// via `Py::get(py)`, which requires the GIL and the `PyClass` bound.
pub(crate) fn py_option_nested_repr<T: PyRepr + PyClass>(opt: &Option<Py<T>>) -> String {
    match opt {
        None => "None".to_string(),
        Some(py_obj) => Python::attach(|py| <T as PyRepr>::__repr__(&*py_obj.borrow(py))),
    }
}
