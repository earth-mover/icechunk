import pickle
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import cast

from icechunk._icechunk_python import (
    AzureCredentials,
    AzureStaticCredentials,
    Credentials,
    GcsBearerCredential,
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
    | GcsStaticCredentials.BearerToken
)

AnyGcsCredential = (
    GcsCredentials.FromEnv | GcsCredentials.Static | GcsCredentials.Refreshable
)

AnyAzureStaticCredential = (
    AzureStaticCredentials.AccessKey
    | AzureStaticCredentials.SasToken
    | AzureStaticCredentials.BearerToken
)

AnyAzureCredential = AzureCredentials.FromEnv | AzureCredentials.Static


AnyCredential = Credentials.S3 | Credentials.Gcs | Credentials.Azure


def s3_refreshable_credentials(
    get_credentials: Callable[[], S3StaticCredentials],
    scatter_initial_credentials: bool = False,
) -> S3Credentials.Refreshable:
    """Create refreshable credentials for S3 and S3 compatible object stores.

    Parameters
    ----------
    get_credentials: Callable[[], S3StaticCredentials]
        Use this function to get and refresh the credentials. The function must be pickable.
    scatter_initial_credentials: bool, optional
        Immediately call and store the value returned by get_credentials. This is useful if the
        repo or session will be pickled to generate many copies. Passing scatter_initial_credentials=True will
        ensure all those copies don't need to call get_credentials immediately. After the initial
        set of credentials has expired, the cached value is no longer used. Notice that credentials
        obtained are stored, and they can be sent over the network if you pickle the session/repo.
    """
    current = get_credentials() if scatter_initial_credentials else None
    return S3Credentials.Refreshable(pickle.dumps(get_credentials), current)


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
    scatter_initial_credentials: bool = False,
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
    scatter_initial_credentials: bool, optional
        Immediately call and store the value returned by get_credentials. This is useful if the
        repo or session will be pickled to generate many copies. Passing scatter_initial_credentials=True will
        ensure all those copies don't need to call get_credentials immediately. After the initial
        set of credentials has expired, the cached value is no longer used. Notice that credentials
        obtained are stored, and they can be sent over the network if you pickle the session/repo.
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
        return s3_refreshable_credentials(
            get_credentials, scatter_initial_credentials=scatter_initial_credentials
        )

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
    bearer_token: str | None = None,
) -> AnyGcsStaticCredential:
    """Create static credentials Google Cloud Storage object store."""
    if service_account_file is not None:
        return GcsStaticCredentials.ServiceAccount(service_account_file)
    if service_account_key is not None:
        return GcsStaticCredentials.ServiceAccountKey(service_account_key)
    if application_credentials is not None:
        return GcsStaticCredentials.ApplicationCredentials(application_credentials)
    if bearer_token is not None:
        return GcsStaticCredentials.BearerToken(bearer_token)
    raise ValueError("Conflicting arguments to gcs_static_credentials function")


def gcs_refreshable_credentials(
    get_credentials: Callable[[], GcsBearerCredential],
    scatter_initial_credentials: bool = False,
) -> GcsCredentials.Refreshable:
    """Create refreshable credentials for Google Cloud Storage object store.

    Parameters
    ----------
    get_credentials: Callable[[], S3StaticCredentials]
        Use this function to get and refresh the credentials. The function must be pickable.
    scatter_initial_credentials: bool, optional
        Immediately call and store the value returned by get_credentials. This is useful if the
        repo or session will be pickled to generate many copies. Passing scatter_initial_credentials=True will
        ensure all those copies don't need to call get_credentials immediately. After the initial
        set of credentials has expired, the cached value is no longer used. Notice that credentials
        obtained are stored, and they can be sent over the network if you pickle the session/repo.
    """

    current = get_credentials() if scatter_initial_credentials else None
    return GcsCredentials.Refreshable(pickle.dumps(get_credentials), current)


def gcs_from_env_credentials() -> GcsCredentials.FromEnv:
    """Instruct Google Cloud Storage object store to fetch credentials from the operative system environment."""
    return GcsCredentials.FromEnv()


def gcs_credentials(
    *,
    service_account_file: str | None = None,
    service_account_key: str | None = None,
    application_credentials: str | None = None,
    bearer_token: str | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], GcsBearerCredential] | None = None,
    scatter_initial_credentials: bool = False,
) -> AnyGcsCredential:
    """Create credentials Google Cloud Storage object store.

    If all arguments are None, credentials are fetched from the operative system environment.
    """
    if (from_env is None or from_env) and (
        service_account_file is None
        and service_account_key is None
        and application_credentials is None
        and bearer_token is None
    ):
        return gcs_from_env_credentials()

    if (
        service_account_file is not None
        or service_account_key is not None
        or application_credentials is not None
        or bearer_token is not None
    ) and (from_env is None or not from_env):
        return GcsCredentials.Static(
            gcs_static_credentials(
                service_account_file=service_account_file,
                service_account_key=service_account_key,
                application_credentials=application_credentials,
                bearer_token=bearer_token,
            )
        )

    if get_credentials is not None:
        return gcs_refreshable_credentials(
            get_credentials, scatter_initial_credentials=scatter_initial_credentials
        )

    raise ValueError("Conflicting arguments to gcs_credentials function")


def azure_static_credentials(
    *,
    access_key: str | None = None,
    sas_token: str | None = None,
    bearer_token: str | None = None,
) -> AnyAzureStaticCredential:
    """Create static credentials Azure Blob Storage object store."""
    if [access_key, sas_token, bearer_token].count(None) != 2:
        raise ValueError("Conflicting arguments to azure_static_credentials function")
    if access_key is not None:
        return AzureStaticCredentials.AccessKey(access_key)
    if sas_token is not None:
        return AzureStaticCredentials.SasToken(sas_token)
    if bearer_token is not None:
        return AzureStaticCredentials.BearerToken(bearer_token)
    raise ValueError(
        "No valid static credential provided for Azure Blob Storage object store"
    )


def azure_from_env_credentials() -> AzureCredentials.FromEnv:
    """Instruct Azure Blob Storage object store to fetch credentials from the operative system environment."""
    return AzureCredentials.FromEnv()


def azure_credentials(
    *,
    access_key: str | None = None,
    sas_token: str | None = None,
    bearer_token: str | None = None,
    from_env: bool | None = None,
) -> AnyAzureCredential:
    """Create credentials Azure Blob Storage object store.

    If all arguments are None, credentials are fetched from the operative system environment.
    """
    if (from_env is None or from_env) and (
        access_key is None and sas_token is None and bearer_token is None
    ):
        return azure_from_env_credentials()

    if (access_key is not None or sas_token is not None or bearer_token is not None) and (
        from_env is None or not from_env
    ):
        return AzureCredentials.Static(
            azure_static_credentials(
                access_key=access_key,
                sas_token=sas_token,
                bearer_token=bearer_token,
            )
        )

    raise ValueError("Conflicting arguments to azure_credentials function")


def containers_credentials(
    m: Mapping[str, AnyS3Credential | AnyGcsCredential | AnyAzureCredential | None],
) -> dict[str, AnyCredential | None]:
    """Build a map of credentials for virtual chunk containers.

    Parameters
    ----------
    m: Mapping[str, AnyS3Credential | AnyGcsCredential | AnyAzureCredential ]
        A mapping from container url prefixes to credentials.

    Examples
    --------
    ```python
    import icechunk as ic

    config = ic.RepositoryConfig.default()
    config.inline_chunk_threshold_bytes = 512

    virtual_store_config = ic.s3_store(
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        allow_http=True,
        s3_compatible=True,
        force_path_style=True,
    )
    container = ic.VirtualChunkContainer("s3://somebucket", virtual_store_config)
    config.set_virtual_chunk_container(container)
    credentials = ic.containers_credentials(
        {"s3://somebucket": ic.s3_credentials(access_key_id="ACCESS_KEY", secret_access_key="SECRET"}
    )

    repo = ic.Repository.create(
        storage=ic.local_filesystem_storage(store_path),
        config=config,
        authorize_virtual_chunk_access=credentials,
    )
    ```

    """
    res: dict[str, AnyCredential | None] = {}
    for name, cred in m.items():
        if cred is None:
            res[name] = None
        elif isinstance(cred, AnyS3Credential):
            res[name] = Credentials.S3(cred)
        elif (
            isinstance(cred, GcsCredentials.FromEnv)
            or isinstance(cred, GcsCredentials.Static)
            or isinstance(cred, GcsCredentials.Refreshable)
        ):
            res[name] = Credentials.Gcs(cast(GcsCredentials, cred))
        elif isinstance(cred, AzureCredentials.FromEnv) or isinstance(
            cred, AzureCredentials.Static
        ):
            res[name] = Credentials.Azure(cast(AzureCredentials, cred))
        else:
            raise ValueError(f"Unknown credential type {type(cred)}")
    return res
