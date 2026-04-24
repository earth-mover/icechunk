"""Credential provider for accessing NASA Earthdata S3 data.

NASA Earthdata supports in-region direct S3 access (us-west-2 only).
This module provides a credential provider that obtains temporary S3
credentials from NASA DAAC endpoints, handling the Earthdata Login
authentication flow automatically.

See https://www.earthdata.nasa.gov/ for more information.
"""

from __future__ import annotations

import base64
import http.cookiejar
import json
import netrc as _netrc_module
import os
from datetime import datetime
from urllib.parse import urlparse
from urllib.request import (
    HTTPCookieProcessor,
    HTTPRedirectHandler,
    Request,
    build_opener,
)

from icechunk._icechunk_python import S3StaticCredentials
from icechunk.credentials import S3Credentials, s3_refreshable_credentials

DEFAULT_EARTHDATA_HOST = "urs.earthdata.nasa.gov"
"""Hostname of the NASA Earthdata Login operational system."""


class _Redirect(Exception):
    def __init__(self, url: str) -> None:
        self.url = url


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str
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
            raise PermissionError(msg) from None

    def _fetch_with_basic_auth(self, auth: tuple[str, str] | None) -> dict[str, str]:
        """Fetch credentials via the Basic Auth redirect flow.

        1. GET /s3credentials → 307 to EDL authorize
        2. GET EDL with Basic Auth → 302 back to DAAC /redirect?code=...
        3. Follow remaining redirects with cookies → returns JSON
        """
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
            raise PermissionError(msg) from None

        encoded = base64.b64encode(f"{cred_auth[0]}:{cred_auth[1]}".encode()).decode()

        # Steps 2-3: authenticate with EDL, then follow the remaining
        # redirect chain back to the DAAC with cookie handling
        cookie_opener = build_opener(HTTPCookieProcessor(http.cookiejar.CookieJar()))
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
            raise PermissionError(msg) from None


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
    - Direct S3 access to NASA Earthdata buckets requires running in the
      **us-west-2** AWS region. The temporary credentials are scoped to
      same-region access only; reads from outside us-west-2 will fail
      with ``AccessDenied``.
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
    resolved_host = host or os.environ.get("EARTHDATA_HOST", DEFAULT_EARTHDATA_HOST)
    resolved_auth = auth or _read_earthdata_auth_from_env()

    fetcher = _EarthdataCredentialFetcher(credentials_url, resolved_host, resolved_auth)
    return s3_refreshable_credentials(
        fetcher,
        scatter_initial_credentials=scatter_initial_credentials,
    )
