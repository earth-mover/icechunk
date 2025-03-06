from collections.abc import Callable
from datetime import datetime

from icechunk._icechunk_python import (
    GcsBearerCredential,
    ObjectStoreConfig,
    S3Options,
    S3StaticCredentials,
    Storage,
)
from icechunk.credentials import (
    azure_credentials,
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
    force_path_style: bool = False,
) -> ObjectStoreConfig.S3Compatible | ObjectStoreConfig.S3:
    """Build an ObjectStoreConfig instance for S3 or S3 compatible object stores."""
    options = S3Options(
        region=region,
        endpoint_url=endpoint_url,
        allow_http=allow_http,
        force_path_style=force_path_style,
    )
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
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    anonymous: bool | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], S3StaticCredentials] | None = None,
    force_path_style: bool = False,
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
    force_path_style: bool
        Whether to force using path-style addressing for buckets
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
    options = S3Options(
        region=region,
        endpoint_url=endpoint_url,
        allow_http=allow_http,
        force_path_style=force_path_style,
    )
    return Storage.new_s3(
        config=options,
        bucket=bucket,
        prefix=prefix,
        credentials=credentials,
    )


def s3_object_store_storage(
    *,
    bucket: str,
    prefix: str | None,
    region: str | None = None,
    endpoint_url: str | None = None,
    allow_http: bool = False,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    anonymous: bool | None = None,
    from_env: bool | None = None,
    force_path_style: bool = False,
) -> Storage:
    credentials = s3_credentials(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        expires_after=expires_after,
        anonymous=anonymous,
        from_env=from_env,
        get_credentials=None,
    )
    options = S3Options(
        region=region,
        endpoint_url=endpoint_url,
        allow_http=allow_http,
        force_path_style=force_path_style,
    )
    return Storage.new_s3_object_store(
        config=options,
        bucket=bucket,
        prefix=prefix,
        credentials=credentials,
    )


def tigris_storage(
    *,
    bucket: str,
    prefix: str | None,
    region: str | None = None,
    endpoint_url: str | None = None,
    use_weak_consistency: bool = False,
    allow_http: bool = False,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    anonymous: bool | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], S3StaticCredentials] | None = None,
) -> Storage:
    """Create a Storage instance that saves data in Tigris object store.

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
    use_weak_consistency: bool
        If set to True it will return a Storage instance that is read only, and can read from the
        the closest Tigris region. Behavior is undefined if objects haven't propagated to the region yet.
        This option is for experts only.
    allow_http: bool
        If the object store can be accessed using http protocol instead of https
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
    return Storage.new_tigris(
        config=options,
        bucket=bucket,
        prefix=prefix,
        use_weak_consistency=use_weak_consistency,
        credentials=credentials,
    )


def gcs_storage(
    *,
    bucket: str,
    prefix: str | None,
    service_account_file: str | None = None,
    service_account_key: str | None = None,
    application_credentials: str | None = None,
    bearer_token: str | None = None,
    from_env: bool | None = None,
    config: dict[str, str] | None = None,
    get_credentials: Callable[[], GcsBearerCredential] | None = None,
) -> Storage:
    """Create a Storage instance that saves data in Google Cloud Storage object store.

    Parameters
    ----------
    bucket: str
        The bucket where the repository will store its data
    prefix: str | None
        The prefix within the bucket that is the root directory of the repository
    from_env: bool | None
        Fetch credentials from the operative system environment
    bearer_token: str | None
        The bearer token to use for the object store
    get_credentials: Callable[[], GcsBearerCredential] | None
        Use this function to get and refresh object store credentials
    """
    credentials = gcs_credentials(
        service_account_file=service_account_file,
        service_account_key=service_account_key,
        application_credentials=application_credentials,
        bearer_token=bearer_token,
        from_env=from_env,
        get_credentials=get_credentials,
    )
    return Storage.new_gcs(
        bucket=bucket,
        prefix=prefix,
        credentials=credentials,
        config=config,
    )


def azure_storage(
    *,
    account: str,
    container: str,
    prefix: str,
    access_key: str | None = None,
    sas_token: str | None = None,
    bearer_token: str | None = None,
    from_env: bool | None = None,
    config: dict[str, str] | None = None,
) -> Storage:
    """Create a Storage instance that saves data in Azure Blob Storage object store.

    Parameters
    ----------
    account: str
        The account to which the caller must have access privileges
    container: str
        The container where the repository will store its data
    prefix: str
        The prefix within the container that is the root directory of the repository
    access_key: str | None
        Azure Blob Storage credential access key
    sas_token: str | None
        Azure Blob Storage credential SAS token
    bearer_token: str | None
        Azure Blob Storage credential bearer token
    from_env: bool | None
        Fetch credentials from the operative system environment
    """
    credentials = azure_credentials(
        access_key=access_key,
        sas_token=sas_token,
        bearer_token=bearer_token,
        from_env=from_env,
    )
    return Storage.new_azure_blob(
        account=account,
        container=container,
        prefix=prefix,
        credentials=credentials,
        config=config,
    )
