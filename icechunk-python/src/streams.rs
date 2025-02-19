use std::{pin::Pin, sync::Arc};

use futures::{Stream, StreamExt};
use pyo3::{
    exceptions::{PyStopAsyncIteration, PyStopIteration},
    prelude::*,
};
use tokio::sync::Mutex;

type PyObjectStream = Arc<Mutex<Pin<Box<dyn Stream<Item = PyResult<Py<PyAny>>> + Send>>>>;

/// An async generator that yields strings from a rust stream of strings
///
/// Python class objects cannot be generic, so this stream takes PyObjects
///
/// Inspired by https://gist.github.com/s3rius/3bf4a0bd6b28ca1ae94376aa290f8f1c
#[pyclass]
pub struct PyAsyncGenerator {
    stream: PyObjectStream,
}

impl PyAsyncGenerator {
    pub(crate) fn new(stream: PyObjectStream) -> Self {
        Self { stream }
    }
}

#[pymethods]
impl PyAsyncGenerator {
    /// We don't want to create another classes, we want this
    /// class to be iterable. Since we implemented __anext__ method,
    /// we can return self here.
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// This is an anext implementation.
    ///
    /// Notable thing here is that we return PyResult<Option<PyObject>>.
    /// We cannot return &PyAny directly here, because of pyo3 limitations.
    /// Here's the issue about it: https://github.com/PyO3/pyo3/issues/3190
    fn __anext__<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Arc::clone is cheap, so we can clone the Arc here because we move into the
        // future block
        let stream = slf.stream.clone();

        let future = async move {
            let mut unlocked = stream.lock().await;
            let next = unlocked.next().await;

            // Release the lock as soon we're done
            drop(unlocked);

            match next {
                Some(Ok(val)) => Ok(Some(val)),
                Some(Err(err)) => Err(err),
                None => Err(PyStopAsyncIteration::new_err("The iterator is exhausted")),
            }
        };

        // TODO: Can we convert this is an async function or a coroutine in the next versions
        // of pyo3?
        pyo3_async_runtimes::tokio::future_into_py(py, future)
    }

    fn __next__<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Option<PyObject>> {
        // Arc::clone is cheap, so we can clone the Arc here because we move into the
        // future block
        let stream = slf.stream.clone();

        py.allow_threads(move || {
            let next = pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let mut unlocked = stream.lock().await;
                unlocked.next().await
            });
            match next {
                Some(Ok(val)) => Ok(Some(val)),
                Some(Err(err)) => Err(err),
                None => Err(PyStopIteration::new_err("The iterator is exhausted")),
            }
        })
    }
}
