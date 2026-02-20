use pyo3::{
    PyErr, PyResult, Python,
    exceptions::PyRuntimeError,
    types::{PyAnyMethods, PyModule},
};
use std::sync::OnceLock;

pub(crate) fn current_task_locals() -> Option<pyo3_async_runtimes::TaskLocals> {
    Python::attach(|py| pyo3_async_runtimes::tokio::get_current_locals(py).ok())
}

static FALLBACK_TASK_LOCALS: OnceLock<Result<pyo3_async_runtimes::TaskLocals, String>> =
    OnceLock::new();

fn create_fallback_task_locals() -> Result<pyo3_async_runtimes::TaskLocals, String> {
    let (tx, rx) = std::sync::mpsc::sync_channel::<
        Result<pyo3_async_runtimes::TaskLocals, String>,
    >(1);

    std::thread::Builder::new()
        .name("icechunk-python-asyncio-fallback".to_string())
        .spawn(move || {
            let mut tx = Some(tx);
            let run_result = Python::attach(|py| -> PyResult<()> {
                let asyncio = PyModule::import(py, "asyncio")?;
                let event_loop = asyncio.getattr("new_event_loop")?.call0()?;
                asyncio.getattr("set_event_loop")?.call1((event_loop.clone(),))?;

                let locals = pyo3_async_runtimes::TaskLocals::new(event_loop.clone());
                if let Some(sender) = tx.take() {
                    let _ = sender.send(Ok(locals));
                } else {
                    return Err(PyRuntimeError::new_err(
                        "fallback asyncio loop sender was unexpectedly missing",
                    ));
                }

                event_loop.call_method0("run_forever")?;
                Ok(())
            });

            if let Err(err) = run_result
                && let Some(sender) = tx
            {
                let _ = sender.send(Err(err.to_string()));
            }
        })
        .map_err(|err| format!("failed to start fallback asyncio loop thread: {err}"))?;

    match rx.recv() {
        Ok(Ok(locals)) => Ok(locals),
        Ok(Err(err)) => Err(format!("failed to initialize fallback asyncio loop: {err}")),
        Err(err) => Err(format!(
            "fallback asyncio loop thread terminated before initialization: {err}"
        )),
    }
}

pub(crate) fn fallback_task_locals() -> Result<pyo3_async_runtimes::TaskLocals, PyErr> {
    match FALLBACK_TASK_LOCALS.get_or_init(create_fallback_task_locals) {
        Ok(locals) => Ok(locals.clone()),
        Err(err) => Err(PyRuntimeError::new_err(err.clone())),
    }
}
