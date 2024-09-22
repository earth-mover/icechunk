use std::path::PathBuf;

use icechunk::zarr::StorageConfig;
use pyo3::{prelude::*, types::PyType};

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
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
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
    fn s3_from_env(_cls: &Bound<'_, PyType>, bucket: String, prefix: String) -> Self {
        PyStorage::S3 {
            bucket,
            prefix,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            endpoint_url: None,
        }
    }

    #[classmethod]
    fn s3_from_credentials(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        endpoint_url: Option<String>,
    ) -> Self {
        PyStorage::S3 {
            bucket,
            prefix,
            access_key_id: Some(access_key_id),
            secret_access_key: Some(secret_access_key),
            session_token,
            endpoint_url,
        }
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
            PyStorage::S3 {
                bucket,
                prefix,
                access_key_id,
                secret_access_key,
                session_token,
                endpoint_url,
            } => StorageConfig::S3ObjectStore {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: session_token.clone(),
                endpoint: endpoint_url.clone(),
            },
        }
    }
}
