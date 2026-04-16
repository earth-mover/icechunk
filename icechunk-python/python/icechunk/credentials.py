from __future__ import annotations

import http.cookiejar
import json
import netrc as _netrc_module
import os
import pickle
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import cast
from urllib.parse import urlparse
from urllib.request import (
    HTTPCookieProcessor,
    HTTPRedirectHandler,
    Request,
    build_opener,
)

from icechunk._icechunk_python import (
    AzureCredentials,
    AzureRefreshableCredential,
    AzureStaticCredentials,
    Credentials,
    GcsBearerCredential,
    GcsCredentials,
    GcsStaticCredentials,
    S3Credentials,
    S3StaticCredentials,
)

__all__ = [
    "AnyAzureCredential",
    "AnyAzureStaticCredential",
    "AnyCredential",
    "AnyGcsCredential",
    "AnyGcsStaticCredential",
    "AnyS3Credential",
    "AzureCredentials",
    "AzureRefreshableCredential",
    "AzureStaticCredentials",
    "Credentials",
    "GcsBearerCredential",
    "GcsCredentials",
    "GcsStaticCredentials",
    "S3Credentials",
    "S3StaticCredentials",
    "azure_credentials",
    "azure_from_env_credentials",
    "azure_refreshable_credentials",
    "azure_static_credentials",
    "containers_credentials",
    "gcs_anonymous_credentials",
    "gcs_credentials",
    "gcs_from_env_credentials",
    "gcs_refreshable_credentials",
    "gcs_static_credentials",
    "s3_anonymous_credentials",
    "s3_credentials",
    "s3_earthdata_credentials",
    "s3_from_env_credentials",
    "s3_refreshable_credentials",
    "s3_static_credentials",
]

DEFAULT_EARTHDATA_HOST = "urs.earthdata.nasa.gov"
"""Hostname of the NASA Earthdata Login operational system."""

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
    GcsCredentials.Anonymous
    | GcsCredentials.FromEnv
    | GcsCredentials.Static
    | GcsCredentials.Refreshable
)

AnyAzureStaticCredential = (
    AzureStaticCredentials.AccessKey
    | AzureStaticCredentials.SasToken
    | AzureStaticCredentials.BearerToken
)

AnyAzureCredential = (
    AzureCredentials.FromEnv | AzureCredentials.Static | AzureCredentials.Refreshable
)


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


def gcs_anonymous_credentials() -> GcsCredentials.Anonymous:
    """Create anonymous credentials for Google Cloud Storage object store."""
    return GcsCredentials.Anonymous()


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
    anonymous: bool | None = None,
    get_credentials: Callable[[], GcsBearerCredential] | None = None,
    scatter_initial_credentials: bool = False,
) -> AnyGcsCredential:
    """Create credentials Google Cloud Storage object store.

    If all arguments are None, credentials are fetched from the operative system environment.
    """
    if anonymous is not None and anonymous:
        return gcs_anonymous_credentials()

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


def azure_refreshable_credentials(
    get_credentials: Callable[[], AzureRefreshableCredential],
    scatter_initial_credentials: bool = False,
) -> AzureCredentials.Refreshable:
    """Create refreshable credentials for Azure Blob Storage object store.

    Parameters
    ----------
    get_credentials: Callable[[], AzureRefreshableCredential]
        Use this function to get and refresh the credentials. The function must be picklable.
    scatter_initial_credentials: bool, optional
        Immediately call and store the value returned by get_credentials. This is useful if the
        repo or session will be pickled to generate many copies. Passing scatter_initial_credentials=True will
        ensure all those copies don't need to call get_credentials immediately. After the initial
        set of credentials has expired, the cached value is no longer used. Notice that credentials
        obtained are stored, and they can be sent over the network if you pickle the session/repo.
    """

    current = get_credentials() if scatter_initial_credentials else None
    return AzureCredentials.Refreshable(pickle.dumps(get_credentials), current)


def azure_from_env_credentials() -> AzureCredentials.FromEnv:
    """Instruct Azure Blob Storage object store to fetch credentials from the operative system environment."""
    return AzureCredentials.FromEnv()


def azure_credentials(
    *,
    access_key: str | None = None,
    sas_token: str | None = None,
    bearer_token: str | None = None,
    from_env: bool | None = None,
    get_credentials: Callable[[], AzureRefreshableCredential] | None = None,
    scatter_initial_credentials: bool = False,
) -> AnyAzureCredential:
    """Create credentials Azure Blob Storage object store.

    If all arguments are None, credentials are fetched from the operative system environment.
    """
    if get_credentials is not None:
        return azure_refreshable_credentials(
            get_credentials,
            scatter_initial_credentials=scatter_initial_credentials,
        )

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
        endpoint_url="http://localhost:4200",
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
            or isinstance(cred, GcsCredentials.Anonymous)
        ):
            res[name] = Credentials.Gcs(cast(GcsCredentials, cred))
        elif (
            isinstance(cred, AzureCredentials.FromEnv)
            or isinstance(cred, AzureCredentials.Static)
            or isinstance(cred, AzureCredentials.Refreshable)
        ):
            res[name] = Credentials.Azure(cast(AzureCredentials, cred))
        else:
            raise ValueError(f"Unknown credential type {type(cred)}")
    return res


# ---------------------------------------------------------------------------
# NASA Earthdata credential provider
# ---------------------------------------------------------------------------


class _Redirect(Exception):
    def __init__(self, url: str) -> None:
        self.url = url


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self, req, fp, code, msg, headers, newurl  # noqa: ANN001
    ) -> None:
        raise _Redirect(newurl)


def _read_earthdata_auth_from_env() -> str | tuple[str, str] | None:
    if token := os.environ.get("EARTHDATA_TOKEN"):
        return token
    if (username := os.environ.get("EARTHDATA_USERNAME")) and (
        password := os.environ.get("EARTHDATA_PASSWORD")
    ):
        return (username, password)
    return None


def _read_auth_from_netrc(host: str) -> tuple[str, str] | None:
    try:
        netrc_path = os.environ.get("NETRC")
        nrc = _netrc_module.netrc(netrc_path)
        auth_tuple = nrc.authenticators(host)
        if auth_tuple:
            return (auth_tuple[0], auth_tuple[2])
    except (FileNotFoundError, _netrc_module.NetrcParseError):
        pass
    return None


class _EarthdataCredentialFetcher:
    """Picklable credential fetcher for NASA Earthdata S3 access."""

    def __init__(
        self,
        credentials_url: str,
        host: str,
        auth: str | tuple[str, str] | None,
    ) -> None:
        self.credentials_url = credentials_url
        self.host = host
        self.auth = auth

    def __call__(self) -> S3StaticCredentials:
        if isinstance(self.auth, str):
            creds = self._fetch_with_token(self.auth)
        else:
            creds = self._fetch_with_basic_auth(self.auth)

        return S3StaticCredentials(
            access_key_id=creds["accessKeyId"],
            secret_access_key=creds["secretAccessKey"],
            session_token=creds["sessionToken"],
            expires_after=datetime.fromisoformat(creds["expiration"]),
        )

    def _fetch_with_token(self, token: str) -> dict[str, str]:
        opener = build_opener(_NoRedirectHandler)
        req = Request(
            self.credentials_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            response = opener.open(req)
            return json.loads(response.read().decode())  # type: ignore[no-any-return]
        except _Redirect:
            msg = (
                "NASA Earthdata bearer token was rejected. "
                "The token may be expired or invalid."
            )
            raise PermissionError(msg)

    def _fetch_with_basic_auth(
        self, auth: tuple[str, str] | None
    ) -> dict[str, str]:
        """Fetch credentials via the Basic Auth redirect flow.

        1. GET /s3credentials → 307 to EDL authorize
        2. GET EDL with Basic Auth → 302 back to DAAC /redirect?code=...
        3. Follow remaining redirects with cookies → returns JSON
        """
        import base64

        # Step 1: capture the redirect to EDL
        no_redirect_opener = build_opener(_NoRedirectHandler)
        req = Request(self.credentials_url)

        try:
            response = no_redirect_opener.open(req)
            return json.loads(response.read().decode())  # type: ignore[no-any-return]
        except _Redirect as r:
            redirect_url = r.url

        # Resolve auth credentials
        redirect_host = urlparse(redirect_url).hostname
        cred_auth = auth if redirect_host == self.host else None
        if cred_auth is None and redirect_host:
            cred_auth = _read_auth_from_netrc(redirect_host)
        if cred_auth is None:
            cred_auth = _read_auth_from_netrc(self.host)

        if cred_auth is None:
            msg = (
                "NASA Earthdata Login credentials not found. "
                f"Set EARTHDATA_USERNAME and EARTHDATA_PASSWORD environment "
                f"variables, or add an entry for '{self.host}' in ~/.netrc."
            )
            raise PermissionError(msg)

        encoded = base64.b64encode(
            f"{cred_auth[0]}:{cred_auth[1]}".encode()
        ).decode()

        # Steps 2-3: authenticate with EDL, then follow the remaining
        # redirect chain back to the DAAC with cookie handling
        cookie_opener = build_opener(
            HTTPCookieProcessor(http.cookiejar.CookieJar())
        )
        req = Request(
            redirect_url,
            headers={"Authorization": f"Basic {encoded}"},
        )
        response = cookie_opener.open(req)
        data = response.read().decode()
        try:
            return json.loads(data)  # type: ignore[no-any-return]
        except json.JSONDecodeError:
            msg = "NASA Earthdata Login credentials were rejected."
            raise PermissionError(msg)


def s3_earthdata_credentials(
    credentials_url: str,
    *,
    host: str | None = None,
    auth: str | tuple[str, str] | None = None,
    scatter_initial_credentials: bool = False,
) -> S3Credentials.Refreshable:
    """Create refreshable S3 credentials for NASA Earthdata cloud data access.

    Obtains temporary S3 credentials from a NASA DAAC endpoint, with
    automatic refresh when credentials expire (~1 hour). Authentication
    is handled via NASA Earthdata Login (EDL).

    Parameters
    ----------
    credentials_url : str
        The DAAC S3 credentials endpoint URL. Common endpoints:

        - NSIDC: ``"https://data.nsidc.earthdatacloud.nasa.gov/s3credentials"``
        - PO.DAAC: ``"https://archive.podaac.earthdata.nasa.gov/s3credentials"``
        - LP DAAC: ``"https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials"``
        - GES DISC: ``"https://data.gesdisc.earthdata.nasa.gov/s3credentials"``
        - ORNL DAAC: ``"https://data.ornldaac.earthdata.nasa.gov/s3credentials"``
        - GHRC DAAC: ``"https://data.ghrc.earthdata.nasa.gov/s3credentials"``
        - ASF: ``"https://sentinel1.asf.alaska.edu/s3credentials"``
    host : str, optional
        Hostname for NASA Earthdata Login authentication.
        Precedence: this argument, then the ``EARTHDATA_HOST`` environment
        variable, then ``"urs.earthdata.nasa.gov"``.
    auth : str or tuple[str, str], optional
        Authentication credentials. Can be:

        - A NASA Earthdata bearer token (``str``)
        - A ``(username, password)`` tuple
        - ``None`` (default) to read from environment or netrc

        When ``None``, precedence is:

        1. ``EARTHDATA_TOKEN`` environment variable
        2. ``EARTHDATA_USERNAME`` and ``EARTHDATA_PASSWORD`` environment variables
        3. ``~/.netrc`` entry for the EDL host
    scatter_initial_credentials : bool, optional
        If True, immediately fetch credentials and cache them. Useful
        when the repo or session will be pickled to create many copies,
        so they don't all need to fetch credentials at the same time.

    Returns
    -------
    S3Credentials.Refreshable
        Refreshable credentials for use with Icechunk S3 storage.
        Credentials are automatically refreshed when they expire.

    Notes
    -----
    - Temporary S3 credentials are valid for approximately 1 hour.
    - Direct S3 access requires running in the **us-west-2** AWS region.
    - Credentials are scoped to a single DAAC. If accessing data from
      multiple DAACs, create separate credentials for each.
    - A free NASA Earthdata account is required:
      https://urs.earthdata.nasa.gov/users/new

    Examples
    --------
    Access NSIDC data using environment variables or netrc for auth:

    >>> import icechunk as ic
    >>> creds = ic.s3_earthdata_credentials(
    ...     "https://data.nsidc.earthdatacloud.nasa.gov/s3credentials"
    ... )

    Access PO.DAAC data using an explicit bearer token:

    >>> creds = ic.s3_earthdata_credentials(
    ...     "https://archive.podaac.earthdata.nasa.gov/s3credentials",
    ...     auth="my-earthdata-token",
    ... )

    Use with a virtual chunk container:

    >>> config = ic.RepositoryConfig.default()
    >>> config.set_virtual_chunk_container(
    ...     ic.VirtualChunkContainer(
    ...         "s3://nsidc-cumulus-prod-protected/",
    ...         ic.s3_store(region="us-west-2"),
    ...     )
    ... )
    >>> credentials = ic.containers_credentials({
    ...     "s3://nsidc-cumulus-prod-protected/": creds,
    ... })
    """
    resolved_host = host or os.environ.get(
        "EARTHDATA_HOST", DEFAULT_EARTHDATA_HOST
    )
    resolved_auth = auth or _read_earthdata_auth_from_env()

    fetcher = _EarthdataCredentialFetcher(
        credentials_url, resolved_host, resolved_auth
    )
    return s3_refreshable_credentials(
        fetcher,
        scatter_initial_credentials=scatter_initial_credentials,
    )
