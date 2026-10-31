use std::{pin::Pin, sync::Arc};

use futures::{Stream, StreamExt as _};
use pyo3::{
    exceptions::{PyStopAsyncIteration, PyStopIteration},
    prelude::*,
    types::PyType,
};
use tokio::sync::Mutex;

type PyObjectStream = Arc<Mutex<Pin<Box<dyn Stream<Item = PyResult<Py<PyAny>>> + Send>>>>;

/// An async iterator that yields items from a rust stream.
///
/// This is an async *iterator*, not an async generator: it supports
/// `__aiter__`/`__anext__` (and the sync `__iter__`/`__next__`) plus `aclose`
/// for deterministic cleanup, but deliberately not `asend`/`athrow`. The
/// underlying rust stream is producer-only, so the bidirectional generator
/// contract has no meaning here.
///
/// Python class objects cannot be generic, so this stream takes `PyObjects`
///
/// Inspired by <https://gist.github.com/s3rius/3bf4a0bd6b28ca1ae94376aa290f8f1c>
#[pyclass(name = "AsyncCloseableIterator", module = "icechunk._icechunk_python")]
pub struct PyAsyncCloseableIterator {
    stream: PyObjectStream,
}

impl std::fmt::Debug for PyAsyncCloseableIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyAsyncCloseableIterator").finish_non_exhaustive()
    }
}

impl PyAsyncCloseableIterator {
    pub(crate) fn new(stream: PyObjectStream) -> Self {
        Self { stream }
    }
}

#[pymethods]
impl PyAsyncCloseableIterator {
    /// We don't want to create another classes, we want this
    /// class to be iterable. Since we implemented __anext__ method,
    /// we can return self here.
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Support runtime subscription (`AsyncCloseableIterator[str]`) like `list[str]`,
    /// so the generic type advertised in the stubs is usable in
    /// runtime-evaluated annotations and in user code.
    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        cls.py().import("types")?.getattr("GenericAlias")?.call1((cls, item))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// This is an anext implementation.
    ///
    /// Notable thing here is that we return `PyResult`<Option<PyObject>>.
    /// We cannot return &`PyAny` directly here, because of pyo3 limitations.
    /// Here's the issue about it: <https://github.com/PyO3/pyo3/issues/3190>
    fn __anext__<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Arc::clone is cheap, so we can clone the Arc here because we move into the
        // future block
        let stream = Arc::clone(&slf.stream);

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
    ) -> PyResult<Option<Py<PyAny>>> {
        // Arc::clone is cheap, so we can clone the Arc here because we move into the
        // future block
        let stream = Arc::clone(&slf.stream);

        py.detach(move || {
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

    /// Finalize the iterator, releasing any resources the underlying stream
    /// holds (for example, the session read lock held while iterating chunks).
    ///
    /// Returns a coroutine so callers can `await it.aclose()` directly or use
    /// `contextlib.aclosing(it)` to clean up deterministically on early exit,
    /// rather than waiting for the garbage collector to drop the stream on an
    /// arbitrary (possibly already closed) event loop. Iterating after closing
    /// raises `StopAsyncIteration`.
    fn aclose<'py>(
        slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let stream = Arc::clone(&slf.stream);

        let future = async move {
            let mut unlocked = stream.lock().await;
            // Replacing the stream drops the original, releasing whatever it
            // borrowed, and leaves an exhausted stream so further `__anext__`
            // calls report completion instead of panicking.
            *unlocked = futures::stream::empty().boxed();
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, future)
    }
}
