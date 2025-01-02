import pickle
from collections.abc import Callable, Mapping
from datetime import datetime

from icechunk._icechunk_python import (
    Credentials,
    GcsCredentials,
    GcsStaticCredentials,
    S3Credentials,
    S3StaticCredentials,
)

AnyS3Credential = (
    S3Credentials.Static
    | S3Credentials.Anonymous
    | S3Credentials.FromEnv
    | S3Credentials.Refreshable
)

AnyGcsStaticCredential = (
    GcsStaticCredentials.ServiceAccount
    | GcsStaticCredentials.ServiceAccountKey
    | GcsStaticCredentials.ApplicationCredentials
)

AnyGcsCredential = GcsCredentials.FromEnv | GcsCredentials.Static

AnyCredential = Credentials.S3 | Credentials.Gcs


def s3_refreshable_credentials(
    get_credentials: Callable[[], S3StaticCredentials],
) -> S3Credentials.Refreshable:
    """Create refreshable credentials for S3 and S3 compatible object stores.


    Parameters
    ----------
    get_credentials: Callable[[], S3StaticCredentials]
        Use this function to get and refresh the credentials. The function must be pickable.
    """
    return S3Credentials.Refreshable(pickle.dumps(get_credentials))


def s3_static_credentials(
    *,
    access_key_id: str,
    secret_access_key: str,
    session_token: str | None = None,
    expires_after: datetime | None = None,
) -> S3Credentials.Static:
    """Create static credentials for S3 and S3 compatible object stores.

    Parameters
    ----------
    access_key_id: str | None
        S3 credential access key
    secret_access_key: str | None
        S3 credential secret access key
    session_token: str | None
        Optional S3 credential session token
    expires_after: datetime | None
        Optional expiration for the object store credentials
    """
    return S3Credentials.Static(
        S3StaticCredentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            expires_after=expires_after,
        )
    )


def s3_anonymous_credentials() -> S3Credentials.Anonymous:
    """Create no-signature credentials for S3 and S3 compatible object stores."""
    return S3Credentials.Anonymous()


def s3_from_env_credentials() -> S3Credentials.FromEnv:
    """Instruct S3 and S3 compatible object stores to gather credentials from the operative system environment."""
    return S3Credentials.FromEnv()


def s3_credentials(
    *,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    session_token: str | None = None,
    expires_after: datetime | None = None,
    anonymous: bool | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], S3StaticCredentials] | None = None,
) -> AnyS3Credential:
    """Create credentials for S3 and S3 compatible object stores.

    If all arguments are None, credentials are fetched from the environment.

    Parameters
    ----------
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
    if (
        (from_env is None or from_env)
        and access_key_id is None
        and secret_access_key is None
        and session_token is None
        and expires_after is None
        and not anonymous
        and get_credentials is None
    ):
        return s3_from_env_credentials()

    if (
        anonymous
        and access_key_id is None
        and secret_access_key is None
        and session_token is None
        and expires_after is None
        and not from_env
        and get_credentials is None
    ):
        return s3_anonymous_credentials()

    if (
        get_credentials is not None
        and access_key_id is None
        and secret_access_key is None
        and session_token is None
        and expires_after is None
        and not from_env
        and not anonymous
    ):
        return s3_refreshable_credentials(get_credentials)

    if (
        access_key_id
        and secret_access_key
        and not from_env
        and not anonymous
        and get_credentials is None
    ):
        return s3_static_credentials(
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            expires_after=expires_after,
        )

    raise ValueError("Conflicting arguments to s3_credentials function")


def gcs_static_credentials(
    *,
    service_account_file: str | None = None,
    service_account_key: str | None = None,
    application_credentials: str | None = None,
) -> AnyGcsStaticCredential:
    """Create static credentials Google Cloud Storage object store."""
    if service_account_file is not None:
        return GcsStaticCredentials.ServiceAccount(service_account_file)
    if service_account_key is not None:
        return GcsStaticCredentials.ServiceAccountKey(service_account_key)
    if application_credentials is not None:
        return GcsStaticCredentials.ApplicationCredentials(application_credentials)
    raise ValueError("Conflicting arguments to gcs_static_credentials function")


def gcs_from_env_credentials() -> GcsCredentials.FromEnv:
    """Instruct Google Cloud Storage object store to fetch credentials from the operative system environment."""
    return GcsCredentials.FromEnv()


def gcs_credentials(
    *,
    service_account_file: str | None = None,
    service_account_key: str | None = None,
    application_credentials: str | None = None,
    from_env: bool | None = None,
) -> AnyGcsCredential:
    """Create credentials Google Cloud Storage object store.

    If all arguments are None, credentials are fetched from the operative system environment.
    """
    if (from_env is None or from_env) and (
        service_account_file is None
        and service_account_key is None
        and application_credentials is None
    ):
        return gcs_from_env_credentials()

    if (
        service_account_file is not None
        or service_account_key is not None
        or application_credentials is not None
    ) and (from_env is None or not from_env):
        return GcsCredentials.Static(
            gcs_static_credentials(
                service_account_file=service_account_file,
                service_account_key=service_account_key,
                application_credentials=application_credentials,
            )
        )

    raise ValueError("Conflicting arguments to gcs_credentials function")


def containers_credentials(
    m: Mapping[str, AnyS3Credential] = {}, **kwargs: AnyS3Credential
) -> dict[str, Credentials.S3]:
    """Build a map of credentials for virtual chunk containers.

    Example usage:
    ```
    import icechunk as ic

    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 512

    virtual_store_config = ic.s3_store(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        s3_compatible=True,
    )
    container = ic.VirtualChunkContainer("s3", "s3://", virtual_store_config)
    config.set_virtual_chunk_container(container)
    credentials = ic.containers_credentials(
        s3=ic.s3_credentials(access_key_id="ACCESS_KEY", secret_access_key="SECRET")
    )

    repo = ic.Repository.create(
        storage=ic.local_filesystem_storage(store_path),
        config=config,
        virtual_chunk_credentials=credentials,
    )
    ```

    Parameters
    ----------
    m: Mapping[str, AnyS3Credential]
        A mapping from container name to credentials.
    """
    res = {}
    for name, cred in {**m, **kwargs}.items():
        if isinstance(cred, AnyS3Credential):
            res[name] = Credentials.S3(cred)
        else:
            raise ValueError(f"Unknown credential type {type(cred)}")
    return res
