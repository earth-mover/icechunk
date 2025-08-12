//! Python bindings for ZEP 8 URL specification support.

use icechunk::zep8::{IcechunkPathSpec, ReferenceType};
use pyo3::prelude::*;

/// Python wrapper for ReferenceType enum.
#[pyclass(name = "ReferenceType", module = "icechunk._icechunk_python")]
#[derive(Clone)]
pub struct PyReferenceType {
    pub inner: ReferenceType,
}

#[pymethods]
impl PyReferenceType {
    #[getter]
    fn name(&self) -> &'static str {
        match self.inner {
            ReferenceType::Branch => "branch",
            ReferenceType::Tag => "tag",
            ReferenceType::Snapshot => "snapshot",
        }
    }

    fn __str__(&self) -> String {
        self.name().to_string()
    }

    fn __repr__(&self) -> String {
        format!("ReferenceType.{}", self.name())
    }
}

/// Python wrapper for IcechunkPathSpec.
#[pyclass(name = "IcechunkPathSpec", module = "icechunk._icechunk_python")]
#[derive(Clone)]
pub struct PyIcechunkPathSpec {
    pub inner: IcechunkPathSpec,
}

#[pymethods]
impl PyIcechunkPathSpec {
    #[new]
    fn new(
        reference_type: &str,
        reference_value: String,
        path: String,
    ) -> PyResult<Self> {
        let ref_type = match reference_type {
            "branch" => ReferenceType::Branch,
            "tag" => ReferenceType::Tag,
            "snapshot" => ReferenceType::Snapshot,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid reference type: {reference_type}"
                )));
            }
        };

        Ok(PyIcechunkPathSpec {
            inner: IcechunkPathSpec { reference_type: ref_type, reference_value, path },
        })
    }

    #[staticmethod]
    fn parse(segment_path: &str) -> PyResult<Self> {
        match IcechunkPathSpec::parse(segment_path) {
            Ok(spec) => Ok(PyIcechunkPathSpec { inner: spec }),
            Err(e) => {
                Err(pyo3::exceptions::PyValueError::new_err(format!("Parse error: {e}")))
            }
        }
    }

    #[getter]
    fn reference_type(&self) -> PyReferenceType {
        PyReferenceType { inner: self.inner.reference_type.clone() }
    }

    #[getter]
    fn reference_value(&self) -> String {
        self.inner.reference_value.clone()
    }

    #[getter]
    fn path(&self) -> String {
        self.inner.path.clone()
    }

    #[getter]
    fn reference_type_str(&self) -> String {
        self.inner.reference_type_str().to_string()
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "IcechunkPathSpec(reference_type='{}', reference_value='{}', path='{}')",
            self.reference_type_str(),
            self.reference_value(),
            self.path()
        )
    }
}

/// Parse icechunk path specification (standalone function).
#[pyfunction]
pub(crate) fn parse_icechunk_path_spec(
    segment_path: &str,
) -> PyResult<PyIcechunkPathSpec> {
    PyIcechunkPathSpec::parse(segment_path)
}

pub(crate) fn register_zep8_module(
    py: Python,
    parent_module: &Bound<'_, PyModule>,
) -> PyResult<()> {
    let zep8_module = PyModule::new(py, "zep8")?;

    zep8_module.add_class::<PyReferenceType>()?;
    zep8_module.add_class::<PyIcechunkPathSpec>()?;
    zep8_module
        .add_function(wrap_pyfunction!(parse_icechunk_path_spec, &zep8_module)?)?;

    parent_module.add_submodule(&zep8_module)?;

    Ok(())
}
