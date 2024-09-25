use std::path::PathBuf;

use icechunk::{storage::object_store::S3Credentials, zarr::StorageConfig};
use pyo3::{prelude::*, types::PyType};

#[pyclass(name = "S3Credentials")]
#[derive(Clone, Debug)]
pub struct PyS3Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl From<&PyS3Credentials> for S3Credentials {
    fn from(credentials: &PyS3Credentials) -> Self {
        S3Credentials {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
        }
    }
}

#[pymethods]
impl PyS3Credentials {
    #[new]
    fn new(
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    ) -> Self {
        PyS3Credentials { access_key_id, secret_access_key, session_token }
    }
}

#[pyclass(name = "Storage")]
pub enum PyStorage {
    Memory {
        prefix: Option<String>,
    },
    Filesystem {
        root: String,
    },
    S3 {
        bucket: String,
        prefix: String,
        credentials: Option<PyS3Credentials>,
        endpoint_url: Option<String>,
    },
}

#[pymethods]
impl PyStorage {
    #[classmethod]
    fn memory(_cls: &Bound<'_, PyType>, prefix: Option<String>) -> Self {
        PyStorage::Memory { prefix }
    }

    #[classmethod]
    fn filesystem(_cls: &Bound<'_, PyType>, root: String) -> Self {
        PyStorage::Filesystem { root }
    }

    #[classmethod]
    fn s3_from_env(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        endpoint_url: Option<String>,
    ) -> Self {
        PyStorage::S3 { bucket, prefix, credentials: None, endpoint_url }
    }

    #[classmethod]
    fn s3_from_credentials(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        credentials: PyS3Credentials,
        endpoint_url: Option<String>,
    ) -> Self {
        PyStorage::S3 { bucket, prefix, credentials: Some(credentials), endpoint_url }
    }
}

impl From<&PyStorage> for StorageConfig {
    fn from(storage: &PyStorage) -> Self {
        match storage {
            PyStorage::Memory { prefix } => {
                StorageConfig::InMemory { prefix: prefix.clone() }
            }
            PyStorage::Filesystem { root } => {
                StorageConfig::LocalFileSystem { root: PathBuf::from(root.clone()) }
            }
            PyStorage::S3 { bucket, prefix, credentials, endpoint_url } => {
                StorageConfig::S3ObjectStore {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    credentials: credentials.as_ref().map(S3Credentials::from),
                    endpoint: endpoint_url.clone(),
                }
            }
        }
    }
}
