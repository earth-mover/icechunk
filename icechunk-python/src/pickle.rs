/// Makes a pyo3 class picklable.
///
/// Generates `__reduce__`, which pickles the struct's serde state together with a
/// reference to the generated `_from_pickle` classmethod, and `_from_pickle`, which
/// rebuilds the object from that state. Unpickling doesn't call the class's
/// constructor, so classes without `#[new]` or with required arguments work too.
///
/// The struct must implement `Serialize` and `Deserialize`, and the class must set
/// `module = "icechunk"` and be exported from `icechunk`: pickle finds classes by
/// module and name. The state format is an internal implementation detail and is
/// subject to change.
///
/// `impl_pickle!(T, init = f)` builds the object from `f(state)`, a
/// `PyClassInitializer<T>`, for classes that extend another pyo3 class.
#[macro_export]
macro_rules! impl_pickle {
    ($struct_name:ident) => {
        $crate::impl_pickle!($struct_name, init = ::core::convert::identity);
    };
    ($struct_name:ident, init = $init:expr) => {
        #[pymethods]
        impl $struct_name {
            /// Rebuilds the object from the state `__reduce__` pickles.
            #[classmethod]
            fn _from_pickle(
                cls: &pyo3::Bound<'_, pyo3::types::PyType>,
                state: &pyo3::Bound<'_, pyo3::types::PyBytes>,
            ) -> pyo3::PyResult<pyo3::Py<Self>> {
                let value: Self =
                    rmp_serde::from_slice(state.as_bytes()).map_err(|e| {
                        $crate::errors::PyIcechunkStoreError::PickleError(format!(
                            "Failed to unpickle {}: {e}",
                            stringify!($struct_name),
                        ))
                    })?;
                pyo3::Py::new(cls.py(), ($init)(value))
            }

            fn __reduce__<'py>(
                slf: &pyo3::Bound<'py, Self>,
            ) -> pyo3::PyResult<(
                pyo3::Bound<'py, pyo3::PyAny>,
                (pyo3::Bound<'py, pyo3::types::PyBytes>,),
            )> {
                let py = slf.py();
                let state = rmp_serde::to_vec(&*slf.borrow()).map_err(|e| {
                    $crate::errors::PyIcechunkStoreError::PickleError(format!(
                        "Failed to pickle {}: {e}",
                        stringify!($struct_name),
                    ))
                })?;
                let rebuild =
                    py.get_type::<Self>().getattr(pyo3::intern!(py, "_from_pickle"))?;
                Ok((rebuild, (pyo3::types::PyBytes::new(py, &state),)))
            }
        }
    };
}

/// The `__getstate__` and `__setstate__` of the exception classes, which pickle
/// through their own `__reduce__` (standard exception pickling, which tblib relies
/// on) rather than [`impl_pickle!`].
#[macro_export]
macro_rules! impl_exception_pickle_state {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            pub(crate) fn __setstate__(
                &mut self,
                state: &pyo3::Bound<'_, pyo3::types::PyBytes>,
            ) -> pyo3::PyResult<()> {
                *self = rmp_serde::from_slice(state.as_bytes()).map_err(|e| {
                    $crate::errors::PyIcechunkStoreError::PickleError(format!(
                        "Failed to unpickle {}: {}",
                        stringify!($struct_name),
                        e.to_string()
                    ))
                })?;
                Ok(())
            }

            pub(crate) fn __getstate__<'py>(
                &self,
                py: pyo3::Python<'py>,
            ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::types::PyBytes>> {
                let state = rmp_serde::to_vec(&self).map_err(|e| {
                    $crate::errors::PyIcechunkStoreError::PickleError(format!(
                        "Failed to pickle {}: {}",
                        stringify!($struct_name),
                        e.to_string()
                    ))
                })?;
                let bytes = pyo3::types::PyBytes::new(py, &state);
                Ok(bytes)
            }
        }
    };
}
