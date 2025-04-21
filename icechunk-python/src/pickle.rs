#[macro_export]
macro_rules! impl_pickle {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            pub fn __setstate__(
                &mut self,
                state: &pyo3::Bound<'_, pyo3::types::PyBytes>,
            ) -> pyo3::PyResult<()> {
                *self = serde_json::from_slice(state.as_bytes()).map_err(|e| {
                    $crate::errors::PyIcechunkStoreError::PickleError(format!(
                        "Failed to unpickle {}: {}",
                        stringify!($struct_name),
                        e.to_string()
                    ))
                })?;
                Ok(())
            }

            pub fn __getstate__<'py>(
                &self,
                py: pyo3::Python<'py>,
            ) -> pyo3::PyResult<pyo3::Bound<'py, pyo3::types::PyBytes>> {
                let state = serde_json::to_vec(&self).map_err(|e| {
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
