//! Integration with Obstore, via pyo3-object_store

use icechunk::config::S3StaticCredentials;
use object_store::aws::AmazonS3ConfigKey;
use pyo3::PyTypeInfo;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_object_store::aws::{PyAWSCredentialProvider, PyAmazonS3Config};
use pyo3_object_store::{PyS3Store, PyTypedObjectStore};

use crate::config::{PyChecksumAlgorithm, PyS3Credentials, PyS3Options, PyStorage};

/// Top-level function to create a PyStorage from an obstore Store
pub(crate) fn new_obstore(
    py: Python<'_>,
    store: Bound<'_, PyAny>,
) -> PyResult<PyStorage> {
    let typed_store = PyTypedObjectStore::from_external_store(store.as_borrowed())?;
    match typed_store {
        PyTypedObjectStore::S3(inner) => new_s3(py, inner),
        _ => todo!(),
    }
}

/// Create PyS3Options from an obstore AmazonS3Config
fn create_s3_options(upstream_config: &PyAmazonS3Config) -> PyResult<PyS3Options> {
    let mut config = PyS3Options::default();

    for (key, value) in upstream_config.as_ref() {
        match key.as_ref() {
            AmazonS3ConfigKey::Region => {
                config.region = Some(value.0.to_string());
            }
            AmazonS3ConfigKey::Endpoint | AmazonS3ConfigKey::S3Endpoint => {
                config.endpoint_url = Some(value.0.to_string());
            }
            AmazonS3ConfigKey::SkipSignature => {
                config.anonymous = value.0.parse()?;
            }
            AmazonS3ConfigKey::VirtualHostedStyleRequest => {
                config.force_path_style = value.0.parse()?;
            }
            AmazonS3ConfigKey::RequestPayer => {
                config.requester_pays = value.0.parse()?;
            }
            AmazonS3ConfigKey::Checksum => {
                let val = value.as_ref().to_ascii_lowercase();
                config.checksum_algorithm = match val.as_str() {
                    "crc32" => Some(PyChecksumAlgorithm::Crc32),
                    "crc64nvme" => Some(PyChecksumAlgorithm::Crc64Nvme),
                    "sha1" => Some(PyChecksumAlgorithm::Sha1),
                    "sha256" => Some(PyChecksumAlgorithm::Sha256),
                    _ => None,
                };
            }
            // Skip keys that don't have a clear mapping to PyS3Options
            _ => (),
        }
    }

    Ok(config)
}

/// Intermediate builder for PyS3StaticCredentials, since we scan config keys in arbitrary order but need to ensure required fields are present
#[derive(Default)]
struct S3StaticCredentialsBuilder {
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
}

impl S3StaticCredentialsBuilder {
    fn build(self) -> PyS3Credentials {
        if self.access_key_id.is_none() && self.secret_access_key.is_none() {
            // If no credentials were provided, but skip_signature was not set, credentials are
            // inferred from the environment
            PyS3Credentials::FromEnv()
        } else {
            PyS3Credentials::Static(
                S3StaticCredentials {
                    access_key_id: self.access_key_id.unwrap_or_default(),
                    secret_access_key: self.secret_access_key.unwrap_or_default(),
                    session_token: self.session_token,
                    expires_after: None,
                }
                .into(),
            )
        }
    }
}

/// Create PyS3Credentials from an obstore config
fn create_s3_credentials(
    upstream_config: &PyAmazonS3Config,
    credential_provider: Option<&PyAWSCredentialProvider>,
) -> PyResult<PyS3Credentials> {
    if credential_provider.is_some() {
        return Err(PyValueError::new_err(
            "Refreshable credentials not currently supported",
        ));
    }

    let mut static_creds_builder = S3StaticCredentialsBuilder::default();

    for (key, value) in upstream_config.as_ref() {
        match key.as_ref() {
            AmazonS3ConfigKey::SkipSignature => {
                return Ok(PyS3Credentials::Anonymous());
            }
            AmazonS3ConfigKey::AccessKeyId => {
                static_creds_builder.access_key_id = Some(value.0.to_string());
            }
            AmazonS3ConfigKey::SecretAccessKey => {
                static_creds_builder.secret_access_key = Some(value.0.to_string());
            }
            AmazonS3ConfigKey::Token => {
                static_creds_builder.session_token = Some(value.0.to_string());
            }
            AmazonS3ConfigKey::ImdsV1Fallback
            | AmazonS3ConfigKey::ContainerCredentialsFullUri
            | AmazonS3ConfigKey::ContainerCredentialsRelativeUri
            | AmazonS3ConfigKey::ContainerAuthorizationTokenFile
            | AmazonS3ConfigKey::WebIdentityTokenFile
            | AmazonS3ConfigKey::RoleArn
            | AmazonS3ConfigKey::RoleSessionName
            | AmazonS3ConfigKey::StsEndpoint => {
                return Err(PyValueError::new_err(
                    "Complex object_store credentials not currently supported",
                ));
            }
            // Skip keys that don't have a clear mapping to PyS3Credentials
            _ => (),
        }
    }

    Ok(static_creds_builder.build())
}

/// Create a new ObjectStore-backed PyStorage from an obstore S3Store
fn new_s3(py: Python<'_>, store: PyS3Store) -> PyResult<PyStorage> {
    let upstream_config = store.config();
    let bucket = upstream_config.bucket().to_string();
    let prefix = upstream_config.prefix().map(|s| s.as_ref().to_string());
    let config = create_s3_options(upstream_config.config())?;
    let credentials = create_s3_credentials(
        upstream_config.config(),
        upstream_config.credential_provider(),
    )?;
    PyStorage::new_s3_object_store(
        &PyStorage::type_object(py),
        py,
        &config,
        bucket,
        prefix,
        Some(credentials),
    )
}

/// Parse a ConfigValue
///
/// This is vendored from upstream object_store internals
/// https://github.com/apache/arrow-rs-object-store/blob/b05890317a2e8b3c4ac5a401483ed8e7eb672ec0/src/config.rs#L69-L129
trait Parse: Sized {
    fn parse(v: &str) -> PyResult<Self>;
}

impl Parse for bool {
    fn parse(v: &str) -> PyResult<Self> {
        let lower = v.to_ascii_lowercase();
        match lower.as_str() {
            "1" | "true" | "on" | "yes" | "y" => Ok(true),
            "0" | "false" | "off" | "no" | "n" => Ok(false),
            _ => {
                Err(PyValueError::new_err(format!("failed to parse \"{v}\" as boolean")))
            }
        }
    }
}
