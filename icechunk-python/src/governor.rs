use std::{sync::Arc, time::Duration};

use icechunk::{
    governors::{
        BandwidthGovernor, BandwidthGovernorConfig, CompatGovernor, CompatGovernorConfig,
        DirectionConfig, DirectionMetrics, GovernorMetrics, MemoryMetrics,
        default_unknown_object_bytes, default_unknown_request_bytes,
    },
    storage::{Direction, IoGovernor},
};
use pyo3::{
    IntoPyObjectExt as _,
    prelude::*,
    types::{PyAny, PyDict, PyTuple},
};

use crate::display::{PyRepr, ReprMode, py_nested_repr};

/// Opaque handle to an injected I/O governor.
///
/// Not constructible from Python: build one of the concrete subclasses
/// instead. Equality is identity of the underlying governor instance.
#[pyclass(
    from_py_object,
    subclass,
    name = "IoGovernor",
    module = "icechunk.experimental",
    eq
)]
#[derive(Clone)]
pub(crate) struct PyIoGovernor(pub(crate) Arc<dyn IoGovernor>);

impl std::fmt::Debug for PyIoGovernor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyIoGovernor").finish_non_exhaustive()
    }
}

impl PartialEq for PyIoGovernor {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.0), Arc::as_ptr(&other.0))
    }
}

impl Eq for PyIoGovernor {}

#[pymethods]
impl PyIoGovernor {
    fn __repr__(&self) -> String {
        "<icechunk.experimental.IoGovernor>".to_string()
    }
}

/// Wrap a governor in the most specific Python class we know about.
pub(crate) fn governor_to_py(
    py: Python<'_>,
    governor: Arc<dyn IoGovernor>,
) -> PyResult<Py<PyAny>> {
    let any = Arc::clone(&governor).as_arc_any();
    let any = match any.downcast::<BandwidthGovernor>() {
        Ok(concrete) => {
            let init = PyClassInitializer::from(PyIoGovernor(governor))
                .add_subclass(PyBandwidthGovernor { governor: concrete });
            return Py::new(py, init)?.into_py_any(py);
        }
        Err(any) => any,
    };
    if let Ok(concrete) = any.downcast::<CompatGovernor>() {
        let init = PyClassInitializer::from(PyIoGovernor(governor))
            .add_subclass(PyCompatGovernor { governor: concrete });
        return Py::new(py, init)?.into_py_any(py);
    }
    Py::new(py, PyIoGovernor(governor))?.into_py_any(py)
}

#[pyclass(from_py_object, name = "DirectionConfig", module = "icechunk.experimental", eq)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PyDirectionConfig {
    #[pyo3(get, set)]
    pub target_bandwidth: u64,
    #[pyo3(get, set)]
    pub max_connection_bandwidth: u64,
    #[pyo3(get, set)]
    pub min_connection_bandwidth: u64,
    #[pyo3(get, set)]
    pub request_latency_us: u64,
    #[pyo3(get, set)]
    pub min_request_bytes: u64,
    #[pyo3(get, set)]
    pub unknown_request_bytes: u64,
}

#[pymethods]
impl PyDirectionConfig {
    #[new]
    #[pyo3(signature = (*, target_bandwidth, max_connection_bandwidth, min_connection_bandwidth, request_latency_us, min_request_bytes, unknown_request_bytes = default_unknown_request_bytes()))]
    fn new(
        target_bandwidth: u64,
        max_connection_bandwidth: u64,
        min_connection_bandwidth: u64,
        request_latency_us: u64,
        min_request_bytes: u64,
        unknown_request_bytes: u64,
    ) -> Self {
        Self {
            target_bandwidth,
            max_connection_bandwidth,
            min_connection_bandwidth,
            request_latency_us,
            min_request_bytes,
            unknown_request_bytes,
        }
    }

    /// Pickle support. Pyo3 classes aren't picklable by default: pickle's
    /// object-creation step calls `cls.__new__(cls)` with no arguments,
    /// which fails because our `#[new]` has required parameters. This hook
    /// makes pickle pass these keyword arguments to `__new__` instead
    /// (protocol 2+, `NEWOBJ_EX` opcode). Since the constructor arguments
    /// are the object's entire state, the round-trip needs no separate
    /// `__getstate__`/`__setstate__`. The keyword (not positional) form
    /// matters: the constructor is keyword-only.
    fn __getnewargs_ex__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("target_bandwidth", self.target_bandwidth)?;
        kwargs.set_item("max_connection_bandwidth", self.max_connection_bandwidth)?;
        kwargs.set_item("min_connection_bandwidth", self.min_connection_bandwidth)?;
        kwargs.set_item("request_latency_us", self.request_latency_us)?;
        kwargs.set_item("min_request_bytes", self.min_request_bytes)?;
        kwargs.set_item("unknown_request_bytes", self.unknown_request_bytes)?;
        Ok((PyTuple::empty(py), kwargs))
    }

    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PyDirectionConfig {
    const EXECUTABLE: bool = true;

    fn cls_name() -> &'static str {
        "icechunk.experimental.DirectionConfig"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("target_bandwidth", self.target_bandwidth.to_string()),
            ("max_connection_bandwidth", self.max_connection_bandwidth.to_string()),
            ("min_connection_bandwidth", self.min_connection_bandwidth.to_string()),
            ("request_latency_us", self.request_latency_us.to_string()),
            ("min_request_bytes", self.min_request_bytes.to_string()),
            ("unknown_request_bytes", self.unknown_request_bytes.to_string()),
        ]
    }
}

impl From<&PyDirectionConfig> for DirectionConfig {
    fn from(value: &PyDirectionConfig) -> Self {
        Self {
            target_bandwidth: value.target_bandwidth,
            max_connection_bandwidth: value.max_connection_bandwidth,
            min_connection_bandwidth: value.min_connection_bandwidth,
            request_latency: Duration::from_micros(value.request_latency_us),
            min_request_bytes: value.min_request_bytes,
            unknown_request_bytes: value.unknown_request_bytes,
        }
    }
}

impl From<DirectionConfig> for PyDirectionConfig {
    fn from(value: DirectionConfig) -> Self {
        Self {
            target_bandwidth: value.target_bandwidth,
            max_connection_bandwidth: value.max_connection_bandwidth,
            min_connection_bandwidth: value.min_connection_bandwidth,
            request_latency_us: value
                .request_latency
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
            min_request_bytes: value.min_request_bytes,
            unknown_request_bytes: value.unknown_request_bytes,
        }
    }
}

#[pyclass(
    from_py_object,
    name = "BandwidthGovernorConfig",
    module = "icechunk.experimental",
    eq
)]
#[derive(Debug)]
pub(crate) struct PyBandwidthGovernorConfig {
    #[pyo3(get, set)]
    pub label: String,
    #[pyo3(get, set)]
    pub read: Py<PyDirectionConfig>,
    #[pyo3(get, set)]
    pub write: Py<PyDirectionConfig>,
    #[pyo3(get, set)]
    pub memory_budget: u64,
    #[pyo3(get, set)]
    pub unknown_object_bytes: u64,
}

#[pymethods]
impl PyBandwidthGovernorConfig {
    #[new]
    #[pyo3(signature = (*, label = BandwidthGovernorConfig::random_label(), read, write, memory_budget, unknown_object_bytes = default_unknown_object_bytes()))]
    fn new(
        label: String,
        read: Py<PyDirectionConfig>,
        write: Py<PyDirectionConfig>,
        memory_budget: u64,
        unknown_object_bytes: u64,
    ) -> Self {
        Self { label, read, write, memory_budget, unknown_object_bytes }
    }

    /// A config with S3 backend constants; bandwidths and budget in bytes.
    #[staticmethod]
    #[pyo3(signature = (label = BandwidthGovernorConfig::random_label(), *, read_bandwidth, write_bandwidth, memory_budget))]
    fn s3_defaults(
        label: String,
        read_bandwidth: u64,
        write_bandwidth: u64,
        memory_budget: u64,
    ) -> Self {
        BandwidthGovernorConfig::s3_defaults(
            label,
            read_bandwidth,
            write_bandwidth,
            memory_budget,
        )
        .into()
    }

    /// Pickle support: reconstruct through `__new__`'s keyword arguments;
    /// see [`PyDirectionConfig::__getnewargs_ex__`] for why. The nested
    /// `DirectionConfig`s in the kwargs pickle recursively.
    fn __getnewargs_ex__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("label", &self.label)?;
        kwargs.set_item("read", &self.read)?;
        kwargs.set_item("write", &self.write)?;
        kwargs.set_item("memory_budget", self.memory_budget)?;
        kwargs.set_item("unknown_object_bytes", self.unknown_object_bytes)?;
        Ok((PyTuple::empty(py), kwargs))
    }

    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PyBandwidthGovernorConfig {
    const EXECUTABLE: bool = true;

    fn cls_name() -> &'static str {
        "icechunk.experimental.BandwidthGovernorConfig"
    }

    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("label", format!("{:?}", self.label)),
            ("read", py_nested_repr(&self.read, mode)),
            ("write", py_nested_repr(&self.write, mode)),
            ("memory_budget", self.memory_budget.to_string()),
            ("unknown_object_bytes", self.unknown_object_bytes.to_string()),
        ]
    }
}

impl Clone for PyBandwidthGovernorConfig {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            label: self.label.clone(),
            read: self.read.clone_ref(py),
            write: self.write.clone_ref(py),
            memory_budget: self.memory_budget,
            unknown_object_bytes: self.unknown_object_bytes,
        })
    }
}

impl PartialEq for PyBandwidthGovernorConfig {
    fn eq(&self, other: &Self) -> bool {
        let x: BandwidthGovernorConfig = self.into();
        let y: BandwidthGovernorConfig = other.into();
        x == y
    }
}

impl Eq for PyBandwidthGovernorConfig {}

impl From<&PyBandwidthGovernorConfig> for BandwidthGovernorConfig {
    fn from(value: &PyBandwidthGovernorConfig) -> Self {
        Python::attach(|py| Self {
            label: value.label.clone(),
            read: (&*value.read.borrow(py)).into(),
            write: (&*value.write.borrow(py)).into(),
            memory_budget: value.memory_budget,
            unknown_object_bytes: value.unknown_object_bytes,
        })
    }
}

impl From<BandwidthGovernorConfig> for PyBandwidthGovernorConfig {
    fn from(value: BandwidthGovernorConfig) -> Self {
        #[expect(clippy::expect_used)]
        Python::attach(|py| Self {
            label: value.label,
            read: Py::new(py, Into::<PyDirectionConfig>::into(value.read))
                .expect("Cannot create instance of DirectionConfig"),
            write: Py::new(py, Into::<PyDirectionConfig>::into(value.write))
                .expect("Cannot create instance of DirectionConfig"),
            memory_budget: value.memory_budget,
            unknown_object_bytes: value.unknown_object_bytes,
        })
    }
}

#[pyclass(
    from_py_object,
    name = "CompatGovernorConfig",
    module = "icechunk.experimental",
    eq
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PyCompatGovernorConfig {
    #[pyo3(get, set)]
    pub max_concurrent_requests: u16,
}

#[pymethods]
impl PyCompatGovernorConfig {
    #[new]
    #[pyo3(signature = (*, max_concurrent_requests = CompatGovernorConfig::default().max_concurrent_requests))]
    fn new(max_concurrent_requests: u16) -> Self {
        Self { max_concurrent_requests }
    }

    /// Pickle support: reconstruct through `__new__`'s keyword arguments;
    /// see [`PyDirectionConfig::__getnewargs_ex__`] for why.
    fn __getnewargs_ex__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("max_concurrent_requests", self.max_concurrent_requests)?;
        Ok((PyTuple::empty(py), kwargs))
    }

    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PyCompatGovernorConfig {
    const EXECUTABLE: bool = true;

    fn cls_name() -> &'static str {
        "icechunk.experimental.CompatGovernorConfig"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![("max_concurrent_requests", self.max_concurrent_requests.to_string())]
    }
}

impl From<&PyCompatGovernorConfig> for CompatGovernorConfig {
    fn from(value: &PyCompatGovernorConfig) -> Self {
        Self { max_concurrent_requests: value.max_concurrent_requests }
    }
}

impl From<CompatGovernorConfig> for PyCompatGovernorConfig {
    fn from(value: CompatGovernorConfig) -> Self {
        Self { max_concurrent_requests: value.max_concurrent_requests }
    }
}

/// Live handle to a `BandwidthGovernor`: runtime knobs plus metrics.
#[pyclass(skip_from_py_object, name = "BandwidthGovernor", module = "icechunk.experimental", extends = PyIoGovernor)]
pub(crate) struct PyBandwidthGovernor {
    governor: Arc<BandwidthGovernor>,
}

#[pymethods]
impl PyBandwidthGovernor {
    #[new]
    fn new(config: &PyBandwidthGovernorConfig) -> PyClassInitializer<Self> {
        let config: BandwidthGovernorConfig = config.into();
        let governor = Arc::new(BandwidthGovernor::new(&config));
        let base = PyIoGovernor(Arc::clone(&governor) as Arc<dyn IoGovernor>);
        PyClassInitializer::from(base).add_subclass(Self { governor })
    }

    #[getter]
    fn label(&self) -> String {
        self.governor.label().to_string()
    }

    #[getter]
    fn read_bandwidth(&self) -> u64 {
        self.governor.metrics().read.target_bandwidth
    }

    #[setter]
    fn set_read_bandwidth(&self, bytes_per_sec: u64) {
        self.governor.set_bandwidth(Direction::Read, bytes_per_sec);
    }

    #[getter]
    fn write_bandwidth(&self) -> u64 {
        self.governor.metrics().write.target_bandwidth
    }

    #[setter]
    fn set_write_bandwidth(&self, bytes_per_sec: u64) {
        self.governor.set_bandwidth(Direction::Write, bytes_per_sec);
    }

    #[getter]
    fn memory_budget(&self) -> u64 {
        self.governor.metrics().memory.budget
    }

    #[setter]
    fn set_memory_budget(&self, bytes: u64) {
        self.governor.set_memory_budget(bytes);
    }

    fn metrics(&self) -> PyGovernorMetrics {
        self.governor.metrics().into()
    }

    fn __repr__(&self) -> String {
        let m = self.governor.metrics();
        format!(
            "<icechunk.experimental.BandwidthGovernor label={:?} read_bandwidth={} write_bandwidth={} memory_budget={}>",
            self.governor.label(),
            m.read.target_bandwidth,
            m.write.target_bandwidth,
            m.memory.budget,
        )
    }
}

/// Live handle to a `CompatGovernor` (the fixed-concurrency default).
#[pyclass(skip_from_py_object, name = "CompatGovernor", module = "icechunk.experimental", extends = PyIoGovernor)]
pub(crate) struct PyCompatGovernor {
    governor: Arc<CompatGovernor>,
}

#[pymethods]
impl PyCompatGovernor {
    #[new]
    fn new(config: &PyCompatGovernorConfig) -> PyClassInitializer<Self> {
        let config: CompatGovernorConfig = config.into();
        let governor = Arc::new(CompatGovernor::new(&config));
        let base = PyIoGovernor(Arc::clone(&governor) as Arc<dyn IoGovernor>);
        PyClassInitializer::from(base).add_subclass(Self { governor })
    }

    #[getter]
    fn max_concurrent_requests(&self) -> u16 {
        self.governor.max_concurrent_requests()
    }

    fn __repr__(&self) -> String {
        format!(
            "<icechunk.experimental.CompatGovernor max_concurrent_requests={}>",
            self.governor.max_concurrent_requests(),
        )
    }
}

#[pyclass(
    skip_from_py_object,
    name = "DirectionMetrics",
    module = "icechunk.experimental",
    frozen,
    eq
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PyDirectionMetrics {
    #[pyo3(get)]
    pub target_bandwidth: u64,
    #[pyo3(get)]
    pub effective_bandwidth: u64,
    #[pyo3(get)]
    pub observed_connection_bandwidth: u64,
    #[pyo3(get)]
    pub in_flight_cost: u64,
    #[pyo3(get)]
    pub in_flight_requests: u64,
    #[pyo3(get)]
    pub queued_requests: u64,
    #[pyo3(get)]
    pub throttles_total: u64,
}

#[pymethods]
impl PyDirectionMetrics {
    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PyDirectionMetrics {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.experimental.DirectionMetrics"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("target_bandwidth", self.target_bandwidth.to_string()),
            ("effective_bandwidth", self.effective_bandwidth.to_string()),
            (
                "observed_connection_bandwidth",
                self.observed_connection_bandwidth.to_string(),
            ),
            ("in_flight_cost", self.in_flight_cost.to_string()),
            ("in_flight_requests", self.in_flight_requests.to_string()),
            ("queued_requests", self.queued_requests.to_string()),
            ("throttles_total", self.throttles_total.to_string()),
        ]
    }
}

impl From<DirectionMetrics> for PyDirectionMetrics {
    fn from(value: DirectionMetrics) -> Self {
        Self {
            target_bandwidth: value.target_bandwidth,
            effective_bandwidth: value.effective_bandwidth,
            observed_connection_bandwidth: value.observed_connection_bandwidth,
            in_flight_cost: value.in_flight_cost,
            in_flight_requests: value.in_flight_requests,
            queued_requests: value.queued_requests,
            throttles_total: value.throttles_total,
        }
    }
}

#[pyclass(
    skip_from_py_object,
    name = "MemoryMetrics",
    module = "icechunk.experimental",
    frozen,
    eq
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PyMemoryMetrics {
    #[pyo3(get)]
    pub budget: u64,
    #[pyo3(get)]
    pub reserved: u64,
    #[pyo3(get)]
    pub queued_fetches: u64,
}

#[pymethods]
impl PyMemoryMetrics {
    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PyMemoryMetrics {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.experimental.MemoryMetrics"
    }

    fn fields(&self, _mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("budget", self.budget.to_string()),
            ("reserved", self.reserved.to_string()),
            ("queued_fetches", self.queued_fetches.to_string()),
        ]
    }
}

impl From<MemoryMetrics> for PyMemoryMetrics {
    fn from(value: MemoryMetrics) -> Self {
        Self {
            budget: value.budget,
            reserved: value.reserved,
            queued_fetches: value.queued_fetches,
        }
    }
}

#[pyclass(
    skip_from_py_object,
    name = "GovernorMetrics",
    module = "icechunk.experimental",
    frozen,
    eq
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PyGovernorMetrics {
    #[pyo3(get)]
    pub read: PyDirectionMetrics,
    #[pyo3(get)]
    pub write: PyDirectionMetrics,
    #[pyo3(get)]
    pub memory: PyMemoryMetrics,
}

#[pymethods]
impl PyGovernorMetrics {
    fn __repr__(&self) -> String {
        <Self as PyRepr>::__repr__(self)
    }

    fn __str__(&self) -> String {
        <Self as PyRepr>::__str__(self)
    }

    fn _repr_html_(&self) -> String {
        <Self as PyRepr>::_repr_html_(self)
    }
}

impl PyRepr for PyGovernorMetrics {
    const EXECUTABLE: bool = false;

    fn cls_name() -> &'static str {
        "icechunk.experimental.GovernorMetrics"
    }

    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)> {
        vec![
            ("read", self.read.render(mode)),
            ("write", self.write.render(mode)),
            ("memory", self.memory.render(mode)),
        ]
    }
}

impl From<GovernorMetrics> for PyGovernorMetrics {
    fn from(value: GovernorMetrics) -> Self {
        Self {
            read: value.read.into(),
            write: value.write.into(),
            memory: value.memory.into(),
        }
    }
}
