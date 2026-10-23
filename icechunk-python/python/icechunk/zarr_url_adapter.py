"""
The `icechunk:` [URL pipeline](https://github.com/jbms/url-pipeline) adapter for zarr (ZEP 8).

With this module registered under zarr's `zarr.url_adapters` entry-point
group, zarr resolves URLs such as

    s3://bucket/repo|icechunk://tag.v1/path/to/group|zarr3:

to an `IcechunkStore` on a read-only session. This module requires a zarr
release with URL pipeline support (zarr-developers/zarr-python#4192);
`import icechunk` does not import it.

Other packages can teach the adapter new root schemes (for example `arraylake:`
for Arraylake) through the `icechunk.url_repository_openers` entry-point
group; see [`register_url_repository_opener`][icechunk.zarr_url_adapter.register_url_repository_opener].
"""

from __future__ import annotations

import inspect
import threading
import warnings
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from typing import Any, Protocol

try:
    from zarr.abc.url_pipeline import URLPipelineAdapter
    from zarr.errors import URLPipelineError
    from zarr.storage import PipelineContext, PipelineSegment, StorePath
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
from icechunk.repository import Repository
from icechunk.session import Session
from icechunk.url_pipeline import (
    _check_root_query,
    _redact_query,
    _to_storage_credentials,
    parse_url_ref,
)

__all__ = [
    "OPENER_ENTRY_POINT_GROUP",
    "IcechunkURLAdapter",
    "URLRepositoryOpener",
    "register_url_repository_opener",
]

#: Entry-point group for repository openers. The entry-point name is the
#: root URL scheme; the value is an async callable, see `URLRepositoryOpener`.
OPENER_ENTRY_POINT_GROUP = "icechunk.url_repository_openers"

#: Root schemes resolved natively by Icechunk. Openers cannot override them.
BUILTIN_ROOT_SCHEMES: frozenset[str] = frozenset(_url_root_schemes())

# Every mode that may write. zarr rejects a read-only store in mode "a" (its
# open-or-create default), so "a" is refused here too, before any I/O.
_WRITE_MODES = frozenset({"a", "w", "w-", "r+"})


class URLRepositoryOpener(Protocol):
    """
    Opens an Icechunk repository for a root URL scheme Icechunk doesn't know.

    Register an opener under the `icechunk.url_repository_openers` entry-point
    group, named for the root scheme:

    ```toml
    [project.entry-points."icechunk.url_repository_openers"]
    arraylake = "arraylake.url_pipeline:open_repository"
    ```

    The `icechunk:` adapter calls it with the pipeline's root segment and
    then applies the `icechunk:` ref and path itself, so
    `arraylake://org/repo|icechunk://tag.v1/path|zarr3:` works as the spec intends.

    Contract:

    - It must be an `async` function and must not block the event loop (it
      runs on zarr's IO loop), nor call zarr's synchronous API.
    - It returns an opened `icechunk.Repository`. It must not select a ref
      or path; those belong to the `icechunk:` segment.
    - It owns validation of `root.body` and `root.query`, and raises
      `zarr.errors.URLPipelineError` for anything it doesn't understand.
    - It owns its credentials. `storage_options` is a fresh dict with every
      `icechunk_*` key removed; the opener consumes its own namespaced keys
      (e.g. `arraylake_token`) and raises for keys it doesn't recognize.
    - `read_only` is always `True` for now: the adapter only opens
      read-only sessions.
    """

    def __call__(
        self,
        root: PipelineSegment,
        /,
        *,
        read_only: bool,
        storage_options: dict[str, Any],
    ) -> Awaitable[Repository]: ...


_openers: dict[str, URLRepositoryOpener] = {}
_entry_points_by_scheme: dict[str, Any] | None = None
_openers_lock = threading.Lock()


def register_url_repository_opener(
    scheme: str, opener: URLRepositoryOpener, *, override: bool = False
) -> None:
    """
    Register an opener for the root URL scheme `scheme`.

    This is the programmatic equivalent of an entry point in the
    `icechunk.url_repository_openers` group, and takes precedence over one.

    Parameters
    ----------
    scheme : str
        The root URL scheme, e.g. `"arraylake"`. Case-insensitive.
    opener : URLRepositoryOpener
        An async callable `(root, *, read_only, storage_options) -> Repository`.
    override : bool
        Replace an existing registration without warning.

    Raises
    ------
    ValueError
        If `scheme` is one of Icechunk's built-in root schemes.
    """
    key = scheme.lower()
    if key in BUILTIN_ROOT_SCHEMES:
        raise ValueError(
            f"cannot register a repository opener for the built-in root scheme {key!r}"
        )
    with _openers_lock:
        if key in _openers and not override:
            warnings.warn(
                f"replacing the icechunk URL repository opener for scheme {key!r}",
                RuntimeWarning,
                stacklevel=2,
            )
        _openers[key] = opener


def _scan_entry_points() -> dict[str, Any]:
    """Index opener entry points by scheme, without importing any of them."""
    by_scheme: dict[str, Any] = {}
    for ep in entry_points(group=OPENER_ENTRY_POINT_GROUP):
        key = ep.name.lower()
        if key in BUILTIN_ROOT_SCHEMES:
            warnings.warn(
                f"ignoring icechunk URL repository opener {ep.value!r}: {key!r} is a "
                "built-in root scheme and cannot be overridden",
                RuntimeWarning,
                stacklevel=4,
            )
        elif key in by_scheme:
            warnings.warn(
                f"several icechunk URL repository openers are registered for scheme "
                f"{key!r}; using {by_scheme[key].value!r} and ignoring {ep.value!r}",
                RuntimeWarning,
                stacklevel=4,
            )
        else:
            by_scheme[key] = ep
    return by_scheme


def _get_opener(scheme: str) -> URLRepositoryOpener | None:
    global _entry_points_by_scheme
    with _openers_lock:
        if scheme in _openers:
            return _openers[scheme]
        if _entry_points_by_scheme is None:
            _entry_points_by_scheme = _scan_entry_points()
        ep = _entry_points_by_scheme.get(scheme)
    if ep is None:
        return None
    try:
        opener = ep.load()
    except Exception as exc:
        raise URLPipelineError(
            f"icechunk: failed to load the repository opener {ep.value!r} for root "
            f"scheme {scheme!r}: {exc}"
        ) from exc
    if not callable(opener):
        raise URLPipelineError(
            f"icechunk: the repository opener {ep.value!r} for root scheme "
            f"{scheme!r} is not callable"
        )
    with _openers_lock:
        return _openers.setdefault(scheme, opener)


# storage_options


@dataclass
class _Options:
    credentials: Any = None
    anonymous: bool = False
    region: str | None = None
    endpoint_url: str | None = None
    config: RepositoryConfig | None = None
    virtual_chunk_credentials: Mapping[str, Any] | None = None
    # keys not addressed to icechunk, only allowed for opener roots
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
            f"icechunk: the root {_redact_query(root.raw)!r} has no URL scheme; spell "
            "local paths as 'file:///absolute/path'"
        )
    # zarr consumes `zarr:`/`zarr2:`/`zarr3:` format segments itself and only
    # accepts them last, so anything here is a container or another adapter.
    if rest:
        raise URLPipelineError(
            f"icechunk: unsupported segment {_redact_query(rest[0].raw)!r} before "
            "'icechunk:'; the icechunk: adapter only supports a root URL that locates "
            "the repository directly (repositories inside other containers, such as "
            "'repo.zip|zip:|icechunk:', are not supported)"
        )
    return root


def _write_mode_error(mode: str) -> URLPipelineError:
    hint = (
        " (it is the default mode of zarr.open and zarr.open_group; pass mode='r')"
        if mode == "a"
        else ""
    )
    return URLPipelineError(
        f"icechunk: mode {mode!r} is not supported{hint}; icechunk: URLs open "
        "read-only sessions because zarr has no way to commit a writable session. "
        "To write, open the repository and use `repo.writable_session(branch)`, "
        "then `session.commit(...)`"
    )


class IcechunkURLAdapter(URLPipelineAdapter):
    """
    zarr URL pipeline adapter for the `icechunk:` scheme.

    Registered under zarr's `zarr.url_adapters` entry-point group as
    `icechunk`. It opens a read-only session on the ref selected by the
    segment (default `branch.main`) and returns its store and the residual
    path. Only modes `"r"` and `None` are accepted.

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
    ) -> StorePath:
        if segment.query is not None:
            raise URLPipelineError(
                "icechunk: segments do not accept a query string: "
                f"{_redact_query(segment.raw)!r}"
            )
        root = _validate_preceding(segment, context)
        if context.mode in _WRITE_MODES:
            raise _write_mode_error(context.mode)
        opts = _parse_storage_options(context.storage_options)
        virtual_credentials = (
            containers_credentials(opts.virtual_chunk_credentials)
            if opts.virtual_chunk_credentials is not None
            else None
        )

        if root.scheme in BUILTIN_ROOT_SCHEMES:
            _apply_fsspec_aliases(opts)
            if opts.passthrough:
                raise URLPipelineError(
                    f"icechunk: unknown storage_options {sorted(opts.passthrough)}; "
                    f"supported keys are {sorted(ICECHUNK_STORAGE_OPTIONS)} and the "
                    f"fsspec-style aliases {sorted(FSSPEC_STORAGE_OPTIONS)}"
                )
            try:
                # Rust would echo the whole root, query included, in its error
                _check_root_query(root.raw)
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
        else:
            session, path = await cls._open_with_opener(
                root, segment, opts, virtual_credentials
            )

        return StorePath(session.store, path)

    @staticmethod
    async def _open_with_opener(
        root: PipelineSegment,
        segment: PipelineSegment,
        opts: _Options,
        virtual_credentials: dict[str, Any] | None,
    ) -> tuple[Session, str]:
        opener = _get_opener(root.scheme)
        if opener is None:
            raise URLPipelineError(
                f"icechunk: unsupported root scheme {root.scheme!r} in "
                f"{_redact_query(root.raw)!r}; "
                f"supported schemes are {sorted(BUILTIN_ROOT_SCHEMES)}, plus any "
                f"registered through the {OPENER_ENTRY_POINT_GROUP!r} entry-point group"
            )
        if opts.has_storage_options:
            raise URLPipelineError(
                "icechunk: 'icechunk_credentials', 'icechunk_anonymous', "
                "'icechunk_region' and 'icechunk_endpoint_url' only apply to built-in "
                f"root schemes; the {root.scheme!r} opener owns its own credentials"
            )
        try:
            ref = parse_url_ref(segment.body)
        except ValueError as exc:
            raise URLPipelineError(f"icechunk: {exc}") from exc

        pending = opener(root, read_only=True, storage_options=dict(opts.passthrough))
        if not inspect.isawaitable(pending):
            raise URLPipelineError(
                f"icechunk: the repository opener for {root.scheme!r} must be an async "
                f"function, but it returned {type(pending).__name__}"
            )
        repo = await pending
        if not isinstance(repo, Repository):
            raise URLPipelineError(
                f"icechunk: the repository opener for {root.scheme!r} returned "
                f"{type(repo).__name__}, not an icechunk.Repository"
            )
        if opts.config is not None or virtual_credentials is not None:
            repo = await repo.reopen_async(
                config=opts.config, authorize_virtual_chunk_access=virtual_credentials
            )
        if ref.kind == "branch":
            session = await repo.readonly_session_async(branch=ref.name)
        elif ref.kind == "tag":
            session = await repo.readonly_session_async(tag=ref.name)
        else:
            session = await repo.readonly_session_async(snapshot_id=ref.name)
        return session, ref.path
