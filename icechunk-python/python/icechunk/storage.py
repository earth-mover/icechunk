from collections.abc import Callable
from datetime import datetime

from icechunk._icechunk_python import (
    ObjectStoreConfig,
    S3Options,
    S3StaticCredentials,
    Storage,
)
from icechunk.credentials import (
    gcs_credentials,
    s3_credentials,
)

AnyObjectStoreConfig = (
    ObjectStoreConfig.InMemory
    | ObjectStoreConfig.LocalFileSystem
    | ObjectStoreConfig.S3
    | ObjectStoreConfig.S3Compatible
    | ObjectStoreConfig.Gcs
    | ObjectStoreConfig.Azure
    | ObjectStoreConfig.Tigris
)


def in_memory_storage() -> Storage:
    """Create a Storage instance that saves data in memory.

    This Storage implementation is used for tests. Data will be lost after the process finishes, and can only be accesses through the Storage instance returned. Different instances don't share data."""
    return Storage.new_in_memory()


def local_filesystem_storage(path: str) -> Storage:
    """Create a Storage instance that saves data in the local file system.

    This Storage instance is not recommended for production data
    """
    return Storage.new_local_filesystem(path)


def s3_store(
    region: str | None = None,
    endpoint_url: str | None = None,
    allow_http: bool = False,
    anonymous: bool = False,
    s3_compatible: bool = False,
) -> ObjectStoreConfig.S3Compatible | ObjectStoreConfig.S3:
    """Build an ObjectStoreConfig instance for S3 or S3 compatible object stores."""
    options = S3Options(region=region, endpoint_url=endpoint_url, allow_http=allow_http)
    return (
        ObjectStoreConfig.S3Compatible(options)
        if s3_compatible
        else ObjectStoreConfig.S3(options)
    )


def s3_storage(
    *,
    bucket: str,
    prefix: str | None,
    region: str | None = None,
    endpoint_url: str | None = None,
    allow_http: bool = False,
    max_concurrent_requests_for_object: int | None = None,
    min_concurrent_request_size: int | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    anonymous: bool | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], S3StaticCredentials] | None = None,
) -> Storage:
    """Create a Storage instance that saves data in S3 or S3 compatible object stores.

    Parameters
    ----------
    bucket: str
        The bucket where the repository will store its data
    prefix: str | None
        The prefix within the bucket that is the root directory of the repository
    region: str | None
        The region to use in the object store, if `None` a default region will be used
    endpoint_url: str | None
        Optional endpoint where the object store serves data, example: http://localhost:9000
    allow_http: bool
        If the object store can be accessed using http protocol instead of https
    max_concurrent_requests_for_object: int | None
        Try to use this many concurrent requests to fetch an object from object store
    min_concurrent_request_size: int | None
        Try not to use requests smaller than min_concurrent_request_size bytes
    access_key_id: str | None
        S3 credential access key
    secret_access_key: str | None
        S3 credential secret access key
    session_token: str | None
        Optional S3 credential session token
    expires_after: datetime | None
        Optional expiration for the object store credentials
    anonymous: bool | None
        If set to True requests to the object store will not be signed
    from_env: bool | None
        Fetch credentials from the operative system environment
    get_credentials: Callable[[], S3StaticCredentials] | None
        Use this function to get and refresh object store credentials
    """
    credentials = s3_credentials(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        expires_after=expires_after,
        anonymous=anonymous,
        from_env=from_env,
        get_credentials=get_credentials,
    )
    options = S3Options(region=region, endpoint_url=endpoint_url, allow_http=allow_http)
    return Storage.new_s3(
        config=options,
        bucket=bucket,
        prefix=prefix,
        credentials=credentials,
        max_concurrent_requests_for_object=max_concurrent_requests_for_object,
        min_concurrent_request_size=min_concurrent_request_size,
    )


def gcs_storage(
    *,
    bucket: str,
    prefix: str | None,
    max_concurrent_requests_for_object: int | None = None,
    min_concurrent_request_size: int | None = None,
    service_account_file: str | None = None,
    service_account_key: str | None = None,
    application_credentials: str | None = None,
    from_env: bool | None = None,
    config: dict[str, str] | None = None,
) -> Storage:
    """Create a Storage instance that saves data in Google Cloud Storage object store.

    Parameters
    ----------
    bucket: str
        The bucket where the repository will store its data
    prefix: str | None
        The prefix within the bucket that is the root directory of the repository
    max_concurrent_requests_for_object: int | None
        Try to use this many concurrent requests to fetch an object from object store
    min_concurrent_request_size: int | None
        Try not to use requests smaller than min_concurrent_request_size bytes
    from_env: bool | None
        Fetch credentials from the operative system environment
    """
    credentials = gcs_credentials(
        service_account_file=service_account_file,
        service_account_key=service_account_key,
        application_credentials=application_credentials,
        from_env=from_env,
    )
    return Storage.new_gcs(
        bucket=bucket,
        prefix=prefix,
        credentials=credentials,
        config=config,
        max_concurrent_requests_for_object=max_concurrent_requests_for_object,
        min_concurrent_request_size=min_concurrent_request_size,
    )
