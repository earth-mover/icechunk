"""
Icechunk store adapter for ZEP 8 URL syntax support.

This module provides integration between Icechunk and zarr-python's ZEP 8 URL syntax,
enabling Icechunk stores to be used in URL chains. It also includes shared utilities
for parsing ZEP 8 URL path specifications and creating readonly sessions.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlparse

from icechunk import (
    IcechunkStore,
    Repository,
    Storage,
    gcs_storage,
    in_memory_storage,
    local_filesystem_storage,
    s3_storage,
)
from zarr.abc.store_adapter import StoreAdapter  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from zarr.abc.store_adapter import URLSegment

# Re-export shared utilities for external use
__all__ = [
    "ICStoreAdapter",
    "IcechunkPathSpec",
    "IcechunkStoreAdapter",
    "create_readonly_session_from_path",
    "parse_icechunk_path_spec",
]

# =============================================================================
# Shared Session Utilities
# =============================================================================


class IcechunkPathSpec:
    """Parsed icechunk path specification from ZEP 8 URL syntax.

    Represents the parsed components of a path specification like:
    - @branch.main/data/array -> branch='main', path='data/array'
    - @tag.v1.0 -> tag='v1.0', path=''
    - @abc123def456/nested -> snapshot_id='abc123def456', path='nested'
    - data/array -> branch='main' (default), path='data/array'
    """

    def __init__(
        self,
        branch: Optional[str] = None,
        tag: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        path: str = "",
    ):
        """Initialize path specification.

        Args:
            branch: Branch name (defaults to 'main' if no other ref specified)
            tag: Tag name
            snapshot_id: Snapshot ID
            path: Path within the repository after version specification
        """
        # Ensure only one reference type is specified
        ref_count = sum(1 for ref in [branch, tag, snapshot_id] if ref is not None)
        if ref_count > 1:
            raise ValueError("Only one of branch, tag, or snapshot_id can be specified")

        # Default to main branch if no reference specified
        if ref_count == 0:
            branch = "main"

        self.branch = branch
        self.tag = tag
        self.snapshot_id = snapshot_id
        self.path = path

    @property
    def reference_type(self) -> str:
        """Get the type of reference specified."""
        if self.branch is not None:
            return "branch"
        elif self.tag is not None:
            return "tag"
        elif self.snapshot_id is not None:
            return "snapshot"
        else:
            return "branch"  # Default

    @property
    def reference_value(self) -> str:
        """Get the reference value."""
        if self.branch is not None:
            return self.branch
        elif self.tag is not None:
            return self.tag
        elif self.snapshot_id is not None:
            return self.snapshot_id
        else:
            return "main"  # Default

    def __repr__(self) -> str:
        return (
            f"IcechunkPathSpec(branch={self.branch!r}, tag={self.tag!r}, "
            f"snapshot_id={self.snapshot_id!r}, path={self.path!r})"
        )


def parse_icechunk_path_spec(segment_path: str) -> IcechunkPathSpec:
    """Parse ZEP 8 URL path specification into structured components.

    Args:
        segment_path: Path specification using ZEP 8 format:
            - @branch.main/path  -> branch 'main'
            - @tag.v123/path     -> tag 'v123'
            - @SNAPSHOT_ID/path  -> snapshot 'SNAPSHOT_ID'
            - empty/path         -> default to main branch
            - just/path          -> default to main branch, path='just/path'

    Returns:
        IcechunkPathSpec with parsed components

    Examples:
        >>> parse_icechunk_path_spec("@branch.main/data/temp")
        IcechunkPathSpec(branch='main', tag=None, snapshot_id=None, path='data/temp')

        >>> parse_icechunk_path_spec("@tag.v1.0")
        IcechunkPathSpec(branch=None, tag='v1.0', snapshot_id=None, path='')

        >>> parse_icechunk_path_spec("data/array")
        IcechunkPathSpec(branch='main', tag=None, snapshot_id=None, path='data/array')
    """
    if not segment_path:
        # Empty path -> default to main branch
        return IcechunkPathSpec(branch="main", path="")

    if not segment_path.startswith("@"):
        # No @ prefix -> treat entire string as path, default to main branch
        return IcechunkPathSpec(branch="main", path=segment_path)

    # Remove @ prefix and split on first /
    ref_spec = segment_path[1:]
    if "/" in ref_spec:
        ref_part, path_part = ref_spec.split("/", 1)
    else:
        ref_part = ref_spec
        path_part = ""

    # Parse reference specification
    if ref_part.startswith("branch."):
        branch_name = ref_part[7:]  # Remove 'branch.' prefix
        if not branch_name:
            raise ValueError("Branch name cannot be empty in @branch.name format")
        return IcechunkPathSpec(branch=branch_name, path=path_part)
    elif ref_part.startswith("tag."):
        tag_name = ref_part[4:]  # Remove 'tag.' prefix
        if not tag_name:
            raise ValueError("Tag name cannot be empty in @tag.name format")
        return IcechunkPathSpec(tag=tag_name, path=path_part)
    else:
        # Assume it's a snapshot ID (no prefix, must be a valid ID format)
        if not re.match(r"^[a-fA-F0-9]{12,}$", ref_part):
            # If it doesn't look like a snapshot ID, treat as invalid
            raise ValueError(
                f"Invalid reference specification: '{ref_part}'. "
                "Expected @branch.name, @tag.name, or @SNAPSHOT_ID format"
            )
        return IcechunkPathSpec(snapshot_id=ref_part, path=path_part)


async def create_readonly_session_from_path(
    repo: Repository, path_spec: IcechunkPathSpec
) -> IcechunkStore:
    """Create readonly Icechunk session from parsed path specification.

    Args:
        repo: Icechunk repository instance
        path_spec: Parsed path specification

    Returns:
        IcechunkStore instance for the specified version

    Raises:
        ValueError: If branch, tag, or snapshot doesn't exist
    """
    try:
        if path_spec.branch is not None:
            session = await repo.readonly_session_async(branch=path_spec.branch)
        elif path_spec.tag is not None:
            session = await repo.readonly_session_async(tag=path_spec.tag)
        elif path_spec.snapshot_id is not None:
            session = await repo.readonly_session_async(snapshot_id=path_spec.snapshot_id)
        else:
            # Fallback to main branch (should not happen due to IcechunkPathSpec validation)
            session = await repo.readonly_session_async(branch="main")

        return session.store
    except Exception as e:
        ref_desc = f"{path_spec.reference_type} '{path_spec.reference_value}'"
        raise ValueError(f"Could not create readonly session for {ref_desc}: {e}") from e


# =============================================================================
# Storage and URL Parsing Utilities
# =============================================================================


def _parse_s3_url(url: str) -> tuple[str, str]:
    """Parse s3:// URL into bucket and prefix."""
    parsed = urlparse(url)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URL, got: {url}")

    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def _parse_gcs_url(url: str) -> tuple[str, str]:
    """Parse gcs:// or gs:// URL into bucket and prefix."""
    parsed = urlparse(url)
    if parsed.scheme not in ("gcs", "gs"):
        raise ValueError(f"Expected gcs:// or gs:// URL, got: {url}")

    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def _create_icechunk_storage(preceding_url: str) -> Storage:
    """Create appropriate Icechunk storage from URL."""
    if preceding_url == "memory:":
        return in_memory_storage()
    elif preceding_url.startswith("file:"):
        path = preceding_url[5:]  # Remove 'file:' prefix
        return local_filesystem_storage(path)
    elif preceding_url.startswith("s3://"):
        bucket, prefix = _parse_s3_url(preceding_url)
        return s3_storage(
            bucket=bucket,
            prefix=prefix,
            from_env=True,  # Use environment variables for credentials
        )
    elif preceding_url.startswith(("gcs://", "gs://")):
        bucket, prefix = _parse_gcs_url(preceding_url)
        return gcs_storage(
            bucket=bucket,
            prefix=prefix,
            from_env=True,  # Use environment variables for credentials
        )
    else:
        raise ValueError(f"Unsupported storage URL for icechunk: {preceding_url}")


async def _create_icechunk_store(repo: Repository, segment_path: str) -> IcechunkStore:
    """Create appropriate Icechunk session based on path specification.

    Uses consolidated session utilities for consistent parsing and session creation.

    Expected path formats:
    - @branch.main/path  -> branch 'main'
    - @tag.v123/path     -> tag 'v123'
    - @SNAPSHOT_ID/path  -> snapshot 'SNAPSHOT_ID'
    - empty/path         -> default to main branch
    """
    # Use consolidated utilities for parsing and session creation
    path_spec = parse_icechunk_path_spec(segment_path)
    return await create_readonly_session_from_path(repo, path_spec)


# =============================================================================
# Store Adapter Implementation
# =============================================================================


class IcechunkStoreAdapter(StoreAdapter):  # type: ignore[misc]
    """Store adapter for Icechunk repositories in ZEP 8 URL chains."""

    adapter_name = "icechunk"

    @classmethod
    async def from_url_segment(
        cls,
        segment: URLSegment,
        preceding_url: str,
        **kwargs: Any,
    ) -> IcechunkStore:
        """
        Create an IcechunkStore from a URL segment and preceding URL.

        Parameters
        ----------
        segment : URLSegment
            The URL segment with adapter='icechunk' and optional path.
        preceding_url : str
            The full URL before this adapter segment (e.g., 's3://bucket/repo', 'file:/path').
            This is used to determine the appropriate Icechunk storage backend.
        **kwargs : Any
            Additional arguments including storage_options.

        Returns
        -------
        Store
            A configured IcechunkStore instance.

        Raises
        ------
        ValueError
            If write mode is requested or repository cannot be opened.

        Examples
        --------
        For URL "s3://mybucket/repo|icechunk:@branch.main":
        - segment.adapter = "icechunk"
        - segment.path = "@branch.main"
        - preceding_url = "s3://mybucket/repo"
        - Uses icechunk.s3_storage(bucket="mybucket", prefix="repo")

        For URL "file:/tmp/repo|icechunk:@tag.v1.0/data":
        - segment.adapter = "icechunk"
        - segment.path = "@tag.v1.0/data"
        - Opens tag "v1.0" and accesses path "data"
        """
        # Icechunk adapter is read-only via ZEP 8 URLs
        # For writing, use the native Icechunk API directly
        mode = kwargs.get("mode", "r")
        if mode in ("w", "w-", "a"):
            raise ValueError(
                f"Write mode '{mode}' not supported for icechunk: URLs. "
                "Use the native Icechunk API to create and write data, "
                "then read it back using ZEP 8 URL syntax."
            )

        # Check storage_options for read_only setting
        storage_options = kwargs.get("storage_options", {})
        if not storage_options.get("read_only", True):
            raise ValueError(
                "Icechunk adapter only supports read-only access via ZEP 8 URLs. "
                "Set storage_options={'read_only': True} or omit for default."
            )

        # Check for unsupported memory: URLs
        if preceding_url == "memory:":
            raise ValueError(
                "memory:|icechunk: URLs are not supported. In-memory Icechunk repositories "
                "cannot be shared across different URL resolution calls. Each call creates "
                "a new empty repository instance. Use file-based repositories instead: "
                "file:/path/to/repo|icechunk:"
            )

        # Create appropriate Icechunk storage from preceding URL
        try:
            storage = _create_icechunk_storage(preceding_url)
        except Exception as e:
            raise ValueError(
                f"Could not create Icechunk storage from URL '{preceding_url}': {e}"
            ) from e

        # Open existing repository (read-only)
        try:
            repo = await Repository.open_async(storage)
        except Exception:
            raise ValueError(
                f"Could not open existing Icechunk repository at '{preceding_url}'. "
                "Create the repository first using the native Icechunk API."
            ) from None

        # Create appropriate session / store based on segment path
        store = await _create_icechunk_store(repo, segment.path)

        return store

    @classmethod
    def can_handle_scheme(cls, scheme: str) -> bool:
        return scheme in ("icechunk", "ic")

    @classmethod
    def get_supported_schemes(cls) -> list[str]:
        return ["icechunk", "ic"]

    @classmethod
    def _extract_zarr_path_from_segment(cls, segment_path: str) -> str:
        """Extract the zarr path component from an icechunk segment path.

        Args:
            segment_path: Path like "@branch.main/data/array" or "@tag.v1.0"

        Returns:
            The zarr path component, e.g. "data/array" or ""
        """
        try:
            path_spec = parse_icechunk_path_spec(segment_path)
            return path_spec.path
        except Exception:
            return ""


# Additional short alias
class ICStoreAdapter(IcechunkStoreAdapter):
    """Short alias for IcechunkStoreAdapter."""

    adapter_name = "ic"
