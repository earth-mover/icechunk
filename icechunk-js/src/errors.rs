pub(crate) trait IntoNapiResult<T> {
    fn map_napi_err(self) -> napi::Result<T>;
}

impl<T, E: std::fmt::Display> IntoNapiResult<T> for Result<T, E> {
    fn map_napi_err(self) -> napi::Result<T> {
        self.map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}
