use pyo3::{
    PyResult, Python,
    exceptions::{PyRuntimeError, PyValueError},
};

pub(crate) fn ensure_not_running_event_loop(py: Python<'_>) -> PyResult<()> {
    match pyo3_async_runtimes::get_running_loop(py) {
        Ok(_) => Err(PyValueError::new_err(
            "deadlock: synchronous API called from a running event loop thread; use the async API or run the sync call in a worker thread",
        )),
        Err(err) if err.is_instance_of::<PyRuntimeError>(py) => Ok(()),
        Err(err) => Err(err),
    }
}
