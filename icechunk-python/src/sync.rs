use pyo3::{
    Bound, PyErr, PyResult, Python,
    exceptions::{PyRuntimeError, PyValueError},
    types::{PyAny, PyAnyMethods, PyModule},
};

fn object_id(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<usize, PyErr> {
    let builtins = PyModule::import(py, "builtins")?;
    builtins.getattr("id")?.call1((obj,))?.extract()
}

pub(crate) fn would_deadlock_current_loop(
    task_locals: &pyo3_async_runtimes::TaskLocals,
) -> Result<bool, PyErr> {
    Python::attach(|py| {
        let running_loop = match pyo3_async_runtimes::get_running_loop(py) {
            Ok(loop_ref) => loop_ref,
            Err(err) if err.is_instance_of::<PyRuntimeError>(py) => return Ok(false),
            Err(err) => return Err(err),
        };

        let target_loop = task_locals.event_loop(py);
        Ok(object_id(py, &running_loop)? == object_id(py, &target_loop)?)
    })
}

pub(crate) fn ensure_not_running_event_loop(py: Python<'_>) -> PyResult<()> {
    match pyo3_async_runtimes::get_running_loop(py) {
        Ok(_) => Err(PyValueError::new_err(
            "deadlock: synchronous API called from a running event loop thread; use the async API or run the sync call in a worker thread",
        )),
        Err(err) if err.is_instance_of::<PyRuntimeError>(py) => Ok(()),
        Err(err) => Err(err),
    }
}
