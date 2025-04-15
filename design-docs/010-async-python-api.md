# Async Python API

The Icechunk rust API for `Repository` and `Session` are both async using `tokio`. Originally, the python API was also async before the transition to separate `Repository`,` Session`, and `Store` classes. 

These changes were originally made to ease the typical python developer experience which may not be running from within an async context. However, Icechunk has many applications that may require an async runtime such as use within web servers. In these cases, blocking the main thread for 200 ms to perform IO is not acceptabl. 

This design document seeks to plan out the ability to perform async lifecycle functions from python, specifically in the `Repository` and `Session` interfaces. 

## API Options

There are a few different ways this interface can be achieved, we will iterate them here and add links to external references where appropriate. 

### Separate Classes

We will have a separate `asyn` module within the `icechunk` python module, leaving an API listing like this: 

```
icechunk
├── asyn
│   ├── async_repository.py
│   ├── async_session.py
│   └── __init__.py
├── repository.py
├── session.py
└── __init__.py
```

Looking at the `async_repository.py` file, it would have the following structure: 

```python
class AsyncRepository:
    async def fetch_config(self) -> RepositoryConfig | None:
        ...

...
class Repository:
    def fetch_config(self) -> RepositoryConfig | None:
        ...
```

#### Alternative Implementation

This approach leaves a few alternatives to maaximize reuse of code between the `Repository` and `AsyncRepository` classes. 

```python
class AsyncRepository:
    async def fetch_config(self) -> RepositoryConfig | None:
        ...

class Repository:

    _async_repo: AsyncRepository
    def fetch_config(self) -> RepositoryConfig | None:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self._async_repo.fetch_config())
```

We could do similar on the rust layer instead. looking like this. The performance and GIL impact of this apprach is unknown at this time:

```rust

#[pyclass]
pub struct PyAsyncRepository(Arc<RwLock<Repository>>);

#[pymethods]
impl PyAsyncRepository {
    #[staticmethod]
    fn fetch_config(py: Python<'_>, storage: PyStorage) -> PyResult<Bound<PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = Repository::fetch_config(storage.0.as_ref())
                .await
                .map_err(PyIcechunkStoreError::RepositoryError)?;
            let res: Option<PyRepositoryConfig> = res.map(|res| res.0.into());
            Ok(res)
        })
    }
}

#[pyclass]
pub struct PyRepository(PyAsyncRepository);

#[pymethods]
impl PyRepository {
    #[staticmethod]
    fn fetch_config(py: Python<'_>, storage: PyStorage) -> PyResult<Bound<PyAny>> {
        // This function calls block_on, so we need to allow other thread python to make progress
        py.allow_threads(move || {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                let coro = PyAsyncRepository::fetch_config(py, storage)?;
                let res = pyo3_async_runtimes::tokio::into_future(coro)?.await?
                Ok(res)
            })
        })    
    }
}

```

### Classes with Async Methods

We will add async methods to the existing classes, keeping the existing API structure. Taking the existing `Repository` class as an example, it would be extended to include the following: 

```python
class Repository:
    def fetch_config(self) -> RepositoryConfig | None:
        ...

    async def async_fetch_config(self) -> RepositoryConfig | None:
        ...
```

This is certainly similar but it will make the API more cluttered.