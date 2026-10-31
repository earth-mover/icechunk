"""
Helpers for Icechunk's [URL pipeline](https://github.com/jbms/url-pipeline) support (ZEP 8).

This module does not depend on zarr. The zarr adapter for the `icechunk:`
scheme lives in [`icechunk.zarr_url_adapter`][icechunk.zarr_url_adapter],
which requires a zarr release with URL pipeline support.
"""

from __future__ import annotations

from typing import cast

from icechunk._icechunk_python import (
    Credentials,
    GcsCredentials,
    S3Credentials,
    S3StaticCredentials,
    Storage,
    _storage_from_url,
)
from icechunk.credentials import AnyGcsCredential, AnyS3Credential

__all__ = ["storage_from_url"]


def _to_storage_credentials(
    credentials: AnyS3Credential
    | AnyGcsCredential
    | S3StaticCredentials
    | Credentials.S3
    | Credentials.Gcs
    | None,
) -> Credentials.S3 | Credentials.Gcs | None:
    if credentials is None or isinstance(credentials, Credentials.S3 | Credentials.Gcs):
        return credentials
    if isinstance(credentials, S3StaticCredentials):
        return Credentials.S3(S3Credentials.Static(credentials))
    if isinstance(credentials, AnyS3Credential):
        return Credentials.S3(credentials)
    if isinstance(
        credentials,
        GcsCredentials.FromEnv
        | GcsCredentials.Static
        | GcsCredentials.Refreshable
        | GcsCredentials.Anonymous,
    ):
        return Credentials.Gcs(cast(GcsCredentials, credentials))
    raise TypeError(
        f"unsupported credentials type {type(credentials).__name__}; expected "
        "S3 or GCS credentials, e.g. icechunk.s3_static_credentials(...)"
    )


def storage_from_url(
    url: str,
    *,
    credentials: AnyS3Credential
    | AnyGcsCredential
    | S3StaticCredentials
    | Credentials.S3
    | Credentials.Gcs
    | None = None,
    anonymous: bool = False,
    region: str | None = None,
    endpoint_url: str | None = None,
) -> Storage:
    """
    Create a `Storage` from a URL pipeline root URL.

    This uses the same mapping as the `icechunk:` URL pipeline adapter:

    | URL | Storage |
    |-----|---------|
    | `file:/abs/path`, `file:///abs/path` | local filesystem |
    | `memory:NAME` | in-memory, shared per `NAME` for the life of the process |
    | `s3://bucket/prefix` | S3 |
    | `s3+http(s)://endpoint/bucket/prefix` | S3-compatible endpoint (path-style) |
    | `gs://bucket/prefix` | Google Cloud Storage |
    | `http(s)://host/path` | HTTP, read-only |

    Icechunk's `memory:` root differs from zarr's: the whole body is the name,
    so `memory:a/b` is a storage named `a/b`, not the path `a/b` inside one
    shared store.

    Parameters
    ----------
    url : str
        The root URL.
    credentials : optional
        S3 credentials for `s3:`/`s3+http(s):` roots, or GCS credentials for
        `gs:` roots. When neither `credentials` nor `anonymous` is given,
        credentials are read from the environment, as for `s3_storage()`.
    anonymous : bool
        Use anonymous (unsigned) access.
    region : str, optional
        The S3 region.
    endpoint_url : str, optional
        An S3-compatible endpoint for an `s3:` root.

    Raises
    ------
    ValueError
        If the URL or the options are invalid.
    """
    return _storage_from_url(
        url,
        credentials=_to_storage_credentials(credentials),
        anonymous=anonymous,
        region=region,
        endpoint_url=endpoint_url,
    )
