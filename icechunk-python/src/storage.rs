#![allow(clippy::too_many_arguments)]
// TODO: we only need that allow for PyStorageConfig, but i don't know how to set it

use std::path::PathBuf;

use icechunk::{
    storage::{
        s3::{S3Config, S3Credentials, StaticS3Credentials},
        virtual_ref::ObjectStoreVirtualChunkResolverConfig,
    },
    zarr::StorageConfig,
};
use pyo3::{prelude::*, types::PyType};

#[pyclass(name = "S3Credentials")]
#[derive(Clone, Debug)]
pub struct PyS3Credentials {
    #[pyo3(get, set)]
    access_key_id: String,
    #[pyo3(get, set)]
    secret_access_key: String,
    #[pyo3(get, set)]
    session_token: Option<String>,
}

impl From<&PyS3Credentials> for StaticS3Credentials {
    fn from(credentials: &PyS3Credentials) -> Self {
        StaticS3Credentials {
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

#[pyclass(name = "StorageConfig")]
pub enum PyStorageConfig {
    Memory {
        prefix: Option<String>,
    },
    Filesystem {
        root: String,
    },
    S3 {
        bucket: String,
        prefix: String,
        anon: bool,
        credentials: Option<PyS3Credentials>,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
    },
}

#[pymethods]
impl PyStorageConfig {
    #[classmethod]
    fn memory(_cls: &Bound<'_, PyType>, prefix: Option<String>) -> Self {
        PyStorageConfig::Memory { prefix }
    }

    #[classmethod]
    fn filesystem(_cls: &Bound<'_, PyType>, root: String) -> Self {
        PyStorageConfig::Filesystem { root }
    }

    #[classmethod]
    fn s3_from_env(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
    ) -> Self {
        PyStorageConfig::S3 {
            bucket,
            prefix,
            anon: false,
            credentials: None,
            endpoint_url,
            allow_http,
            region,
        }
    }

    #[classmethod]
    fn s3_from_config(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        credentials: PyS3Credentials,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
    ) -> Self {
        PyStorageConfig::S3 {
            bucket,
            prefix,
            anon: false,
            credentials: Some(credentials),
            endpoint_url,
            allow_http,
            region,
        }
    }

    #[classmethod]
    fn s3_anonymous(
        _cls: &Bound<'_, PyType>,
        bucket: String,
        prefix: String,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
    ) -> Self {
        PyStorageConfig::S3 {
            bucket,
            prefix,
            anon: true,
            credentials: None,
            endpoint_url,
            allow_http,
            region,
        }
    }
}

fn mk_credentials(config: Option<&PyS3Credentials>, anon: bool) -> S3Credentials {
    if anon {
        S3Credentials::Anonymous
    } else {
        match config {
            None => S3Credentials::FromEnv,
            Some(credentials) => S3Credentials::Static(credentials.into()),
        }
    }
}

impl From<&PyStorageConfig> for StorageConfig {
    fn from(storage: &PyStorageConfig) -> Self {
        match storage {
            PyStorageConfig::Memory { prefix } => {
                StorageConfig::InMemory { prefix: prefix.clone() }
            }
            PyStorageConfig::Filesystem { root } => {
                StorageConfig::LocalFileSystem { root: PathBuf::from(root.clone()) }
            }
            PyStorageConfig::S3 {
                bucket,
                prefix,
                anon,
                credentials,
                endpoint_url,
                allow_http,
                region,
            } => {
                let s3_config = S3Config {
                    region: region.clone(),
                    credentials: mk_credentials(credentials.as_ref(), *anon),
                    endpoint: endpoint_url.clone(),
                    allow_http: allow_http.unwrap_or(false),
                };

                StorageConfig::S3ObjectStore {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    config: Some(s3_config),
                }
            }
        }
    }
}

#[pyclass(name = "VirtualRefConfig")]
#[derive(Clone, Debug)]
pub enum PyVirtualRefConfig {
    S3 {
        credentials: Option<PyS3Credentials>,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
        anon: bool,
    },
}

#[pymethods]
impl PyVirtualRefConfig {
    #[classmethod]
    fn s3_from_env(_cls: &Bound<'_, PyType>) -> Self {
        PyVirtualRefConfig::S3 {
            credentials: None,
            endpoint_url: None,
            allow_http: None,
            region: None,
            anon: false,
        }
    }

    #[classmethod]
    fn s3_from_config(
        _cls: &Bound<'_, PyType>,
        credentials: PyS3Credentials,
        endpoint_url: Option<String>,
        allow_http: Option<bool>,
        region: Option<String>,
        anon: Option<bool>,
    ) -> Self {
        PyVirtualRefConfig::S3 {
            credentials: Some(credentials),
            endpoint_url,
            allow_http,
            region,
            anon: anon.unwrap_or(false),
        }
    }
}

impl From<&PyVirtualRefConfig> for ObjectStoreVirtualChunkResolverConfig {
    fn from(config: &PyVirtualRefConfig) -> Self {
        match config {
            PyVirtualRefConfig::S3 {
                credentials,
                endpoint_url,
                allow_http,
                region,
                anon,
            } => ObjectStoreVirtualChunkResolverConfig::S3(S3Config {
                region: region.clone(),
                endpoint: endpoint_url.clone(),
                credentials: mk_credentials(credentials.as_ref(), *anon),
                allow_http: allow_http.unwrap_or(false),
            }),
        }
    }
}
