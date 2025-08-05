use std::ffi::OsString;
use thiserror::Error;

use crate::error::ICError;

#[cfg(not(target_arch = "wasm32"))]
use aws_sdk_s3::{
    config::http::HttpResponse,
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        create_multipart_upload::CreateMultipartUploadError,
        delete_objects::DeleteObjectsError, get_object::GetObjectError,
        head_object::HeadObjectError, list_objects_v2::ListObjectsV2Error,
        put_object::PutObjectError, upload_part::UploadPartError,
    },
    primitives::ByteStreamError,
};

#[derive(Debug, Error)]
pub enum StorageErrorKind {
    #[cfg(not(target_arch = "wasm32"))]
    #[error("object store error {0}")]
    ObjectStore(#[from] Box<::object_store::Error>),
    #[error("bad object store prefix {0:?}")]
    BadPrefix(OsString),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error getting object from object store {0}")]
    S3GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error writing object to object store {0}")]
    S3PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error creating multipart upload {0}")]
    S3CreateMultipartUploadError(
        #[from] Box<SdkError<CreateMultipartUploadError, HttpResponse>>,
    ),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error uploading multipart part {0}")]
    S3UploadPartError(#[from] Box<SdkError<UploadPartError, HttpResponse>>),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error completing multipart upload {0}")]
    S3CompleteMultipartUploadError(
        #[from] Box<SdkError<CompleteMultipartUploadError, HttpResponse>>,
    ),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error getting object metadata from object store {0}")]
    S3HeadObjectError(#[from] Box<SdkError<HeadObjectError, HttpResponse>>),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error listing objects in object store {0}")]
    S3ListObjectError(#[from] Box<SdkError<ListObjectsV2Error, HttpResponse>>),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error deleting objects in object store {0}")]
    S3DeleteObjectError(#[from] Box<SdkError<DeleteObjectsError, HttpResponse>>),
    #[cfg(not(target_arch = "wasm32"))]
    #[error("error streaming bytes from object store {0}")]
    S3StreamError(#[from] Box<ByteStreamError>),
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("storage configuration error: {0}")]
    R2ConfigurationError(String),
    #[error("storage error: {0}")]
    Other(String),
}

pub type StorageError = ICError<StorageErrorKind>;

// it would be great to define this impl in error.rs, but it conflicts with the blanket
// `impl From<T> for T`
impl<E> From<E> for StorageError
where
    E: Into<StorageErrorKind>,
{
    fn from(value: E) -> Self {
        Self::new(value.into())
    }
}

pub type StorageResult<A> = Result<A, StorageError>;