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
    return Storage.new_in_memory()


def local_filesystem_storage(path: str) -> Storage:
    return Storage.new_local_filesystem(path)


def s3_store(
    region: str | None = None,
    endpoint_url: str | None = None,
    allow_http: bool = False,
    anonymous: bool = False,
    s3_compatible: bool = False,
) -> ObjectStoreConfig.S3Compatible | ObjectStoreConfig.S3:
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
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    anonymous: bool | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], S3StaticCredentials] | None = None,
) -> Storage:
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
        config=options, bucket=bucket, prefix=prefix, credentials=credentials
    )


def gcs_storage(
    *,
    bucket: str,
    prefix: str | None,
    service_account_file: str | None = None,
    service_account_key: str | None = None,
    application_credentials: str | None = None,
    from_env: bool | None = None,
    config: dict[str, str] | None = None,
) -> Storage:
    credentials = gcs_credentials(
        service_account_file=service_account_file,
        service_account_key=service_account_key,
        application_credentials=application_credentials,
        from_env=from_env,
    )
    return Storage.new_gcs(
        bucket=bucket, prefix=prefix, credentials=credentials, config=config
    )
