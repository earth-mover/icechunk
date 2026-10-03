"""
The `icechunk:` [URL pipeline](https://github.com/jbms/url-pipeline) adapter for zarr (ZEP 8).

With this module registered under zarr's `zarr.url_adapters` entry-point
group, zarr resolves URLs such as

    s3://bucket/repo|icechunk://tag.v1/path/to/group|zarr3:

to an `IcechunkStore` on a read-only session. This module requires a zarr
release with URL pipeline support (zarr-developers/zarr-python#4192);
`import icechunk` does not import it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

try:
    from zarr.abc.url_pipeline import (
        AdapterResolution,
        PipelineContext,
        PipelineSegment,
        URLPipelineAdapter,
    )
    from zarr.errors import URLPipelineError
except ImportError as exc:  # pragma: no cover - exercised only with older zarr
    raise ImportError(
        "icechunk.zarr_url_adapter requires a zarr version with URL pipeline "
        "support (zarr.abc.url_pipeline, zarr-developers/zarr-python#4192)"
    ) from exc

from icechunk._icechunk_python import (
    _open_session_from_url_async,
    _url_root_schemes,
)
from icechunk.config import RepositoryConfig
from icechunk.credentials import containers_credentials, s3_static_credentials
from icechunk.session import Session
from icechunk.url_pipeline import _to_storage_credentials

__all__ = ["IcechunkURLAdapter"]

#: Root schemes resolved natively by Icechunk.
BUILTIN_ROOT_SCHEMES: frozenset[str] = frozenset(_url_root_schemes())

_WRITE_MODES = frozenset({"w", "w-", "r+"})


# storage_options


@dataclass
class _Options:
    credentials: Any = None
    anonymous: bool = False
    region: str | None = None
    endpoint_url: str | None = None
    config: RepositoryConfig | None = None
    virtual_chunk_credentials: Mapping[str, Any] | None = None
    # keys not addressed to icechunk
    passthrough: dict[str, Any] = field(default_factory=dict)

    @property
    def has_storage_options(self) -> bool:
        return (
            self.credentials is not None
            or self.anonymous
            or self.region is not None
            or self.endpoint_url is not None
        )


#: The `storage_options` keys the adapter consumes.
ICECHUNK_STORAGE_OPTIONS = frozenset(
    {
        "icechunk_credentials",
        "icechunk_anonymous",
        "icechunk_region",
        "icechunk_endpoint_url",
        "icechunk_config",
        "icechunk_virtual_chunk_credentials",
    }
)

#: fsspec-style keys accepted for built-in roots, as aliases.
FSSPEC_STORAGE_OPTIONS = frozenset(
    {"anon", "key", "secret", "token", "endpoint_url", "region"}
)


def _static_credentials_from_dict(creds: Mapping[str, Any]) -> Any:
    allowed = {"access_key_id", "secret_access_key", "session_token", "expires_after"}
    unknown = set(creds) - allowed
    if unknown or not {"access_key_id", "secret_access_key"} <= set(creds):
        raise URLPipelineError(
            "icechunk: a dict 'icechunk_credentials' must hold S3 static credentials: "
            "'access_key_id' and 'secret_access_key', optionally 'session_token' and "
            f"'expires_after'; got keys {sorted(creds)}"
        )
    return s3_static_credentials(**creds)


def _parse_storage_options(storage_options: Mapping[str, Any] | None) -> _Options:
    opts = _Options()
    if not storage_options:
        return opts
    remaining = dict(storage_options)
    unknown_icechunk = sorted(
        k
        for k in remaining
        if k.startswith("icechunk_") and k not in ICECHUNK_STORAGE_OPTIONS
    )
    if unknown_icechunk:
        raise URLPipelineError(
            f"icechunk: unknown storage_options {unknown_icechunk}; supported keys are "
            f"{sorted(ICECHUNK_STORAGE_OPTIONS)}"
        )

    creds = remaining.pop("icechunk_credentials", None)
    if isinstance(creds, Mapping):
        creds = _static_credentials_from_dict(creds)
    try:
        opts.credentials = _to_storage_credentials(creds)
    except TypeError as exc:
        raise URLPipelineError(
            f"icechunk: invalid 'icechunk_credentials': {exc}"
        ) from exc
    opts.anonymous = bool(remaining.pop("icechunk_anonymous", False))
    opts.region = remaining.pop("icechunk_region", None)
    opts.endpoint_url = remaining.pop("icechunk_endpoint_url", None)
    opts.config = remaining.pop("icechunk_config", None)
    if opts.config is not None and not isinstance(opts.config, RepositoryConfig):
        raise URLPipelineError(
            "icechunk: 'icechunk_config' must be an icechunk.RepositoryConfig, "
            f"not {type(opts.config).__name__}"
        )
    opts.virtual_chunk_credentials = remaining.pop(
        "icechunk_virtual_chunk_credentials", None
    )
    opts.passthrough = remaining
    return opts


def _apply_fsspec_aliases(opts: _Options) -> None:
    """Map the small set of fsspec-style keys onto icechunk options, in place."""
    p = opts.passthrough

    def conflict(fsspec_key: str, icechunk_key: str) -> URLPipelineError:
        return URLPipelineError(
            f"icechunk: storage_options {fsspec_key!r} and {icechunk_key!r} "
            "both given; pass only one"
        )

    if "anon" in p:
        if opts.anonymous:
            raise conflict("anon", "icechunk_anonymous")
        opts.anonymous = bool(p.pop("anon"))
    if "key" in p or "secret" in p:
        if opts.credentials is not None:
            raise conflict("key", "icechunk_credentials")
        if "key" not in p or "secret" not in p:
            raise URLPipelineError(
                "icechunk: storage_options 'key' and 'secret' must be given together"
            )
        opts.credentials = _to_storage_credentials(
            s3_static_credentials(
                access_key_id=p.pop("key"),
                secret_access_key=p.pop("secret"),
                session_token=p.pop("token", None),
            )
        )
    if "token" in p:
        raise URLPipelineError(
            "icechunk: storage_options 'token' is only accepted as an S3 session token "
            "together with 'key' and 'secret'; use 'icechunk_credentials' for other "
            "credentials"
        )
    for fsspec_key, attr in (("endpoint_url", "endpoint_url"), ("region", "region")):
        if fsspec_key in p:
            if getattr(opts, attr) is not None:
                raise conflict(fsspec_key, f"icechunk_{attr}")
            setattr(opts, attr, p.pop(fsspec_key))


# pipeline validation


def _validate_preceding(
    segment: PipelineSegment, context: PipelineContext
) -> PipelineSegment:
    if not context.preceding:
        raise URLPipelineError(
            "icechunk: needs a root URL locating the repository, e.g. "
            "'s3://bucket/repo|icechunk:'"
        )
    root, *rest = context.preceding
    if not root.scheme:
        raise URLPipelineError(
            f"icechunk: the root {root.raw!r} has no URL scheme; spell local paths as "
            "'file:///absolute/path'"
        )
    for seg in rest:
        if seg.scheme == "zarr3" and not seg.body and seg.query is None:
            continue
        if seg.scheme in ("zarr2", "zarr", "zarr3"):
            raise URLPipelineError(
                f"icechunk: unsupported segment {seg.raw!r} before 'icechunk:'; "
                "Icechunk repositories are Zarr v3 only and the format segment "
                f"belongs after the icechunk: segment (in {context.preceding_url}|"
                f"{segment.raw})"
            )
        raise URLPipelineError(
            f"icechunk: unsupported segment {seg.raw!r} before 'icechunk:'; the "
            "icechunk: adapter only supports a root URL that locates the repository "
            "directly (repositories inside other containers, such as "
            "'repo.zip|zip:|icechunk:', are not supported)"
        )
    return root


def _write_mode_error(mode: str) -> URLPipelineError:
    return URLPipelineError(
        f"icechunk: mode {mode!r} is not supported; icechunk: URLs open read-only "
        "sessions because zarr has no way to commit a writable session. To write, "
        "open the repository and use `repo.writable_session(branch)`, then "
        "`session.commit(...)`"
    )


class IcechunkURLAdapter(URLPipelineAdapter):
    """
    zarr URL pipeline adapter for the `icechunk:` scheme.

    Registered under zarr's `zarr.url_adapters` entry-point group as
    `icechunk`. It opens a read-only session on the ref selected by the
    segment (default `branch.main`) and returns its store, the residual path
    and `zarr_format=3`.

    Supported `storage_options` (all other keys are rejected for built-in
    root schemes):

    - `icechunk_credentials`: S3 or GCS credentials for the root storage
      (e.g. `icechunk.s3_static_credentials(...)`), or a dict of S3 static
      credentials (`access_key_id`, `secret_access_key`, `session_token`).
    - `icechunk_anonymous`: use anonymous access.
    - `icechunk_region`, `icechunk_endpoint_url`: S3 region and endpoint.
    - `icechunk_config`: an `icechunk.RepositoryConfig`.
    - `icechunk_virtual_chunk_credentials`: a mapping of virtual chunk
      container URL prefixes to credentials, passed to
      `authorize_virtual_chunk_access`.
    - fsspec-style aliases: `anon`, `key`, `secret`, `token` (an S3 session
      token, only with `key` and `secret`), `endpoint_url`, `region`.

    Without credentials or `anonymous`, S3 and GCS credentials are read from
    the environment, as for `icechunk.s3_storage()`.
    """

    @classmethod
    async def open_pipeline_segment(
        cls, segment: PipelineSegment, context: PipelineContext
    ) -> AdapterResolution:
        if segment.query is not None:
            raise URLPipelineError(
                f"icechunk: segments do not accept a query string: {segment.raw!r}"
            )
        root = _validate_preceding(segment, context)
        if context.mode in _WRITE_MODES:
            raise _write_mode_error(context.mode)
        # "r", "a" (zarr's open-or-create default) and None open read-only;
        # for "a", zarr opens an existing node and any write fails at the store.
        opts = _parse_storage_options(context.storage_options)
        virtual_credentials = (
            containers_credentials(opts.virtual_chunk_credentials)
            if opts.virtual_chunk_credentials is not None
            else None
        )

        if root.scheme not in BUILTIN_ROOT_SCHEMES:
            raise URLPipelineError(
                f"icechunk: unsupported root scheme {root.scheme!r} in {root.raw!r}; "
                f"supported schemes are {sorted(BUILTIN_ROOT_SCHEMES)}"
            )
        _apply_fsspec_aliases(opts)
        if opts.passthrough:
            raise URLPipelineError(
                f"icechunk: unknown storage_options {sorted(opts.passthrough)}; "
                f"supported keys are {sorted(ICECHUNK_STORAGE_OPTIONS)} and the "
                f"fsspec-style aliases {sorted(FSSPEC_STORAGE_OPTIONS)}"
            )
        try:
            pending = _open_session_from_url_async(
                root.raw,
                segment.body,
                credentials=opts.credentials,
                anonymous=opts.anonymous,
                region=opts.region,
                endpoint_url=opts.endpoint_url,
                config=opts.config,
                authorize_virtual_chunk_access=virtual_credentials,
                writable=False,
            )
        except ValueError as exc:
            raise URLPipelineError(f"icechunk: {exc}") from exc
        py_session, path = await pending
        session = Session(py_session)

        return AdapterResolution(store=session.store, path=path, zarr_format=3)
