"""
Icechunk store adapter for ZEP 8 URL syntax support.

This module provides integration between Icechunk and zarr-python's ZEP 8 URL syntax,
leveraging Rust-based parsing and session creation for high performance.

Requires the Rust-based Python bindings to be built and available.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from icechunk import IcechunkStore, Repository
from icechunk.zep8 import (
    IcechunkPathSpec,
    create_readonly_session_from_path,
    parse_icechunk_path_spec,
)
from zarr.abc.store_adapter import StoreAdapter

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
# Storage Creation Utilities
# =============================================================================


def _create_icechunk_storage(preceding_url: str):
    """Create appropriate Icechunk storage from URL."""
    if preceding_url == "memory:":
        raise ValueError(
            "memory:|icechunk: URLs are not supported. In-memory Icechunk repositories "
            "cannot be shared across different URL resolution calls."
        )
    elif preceding_url.startswith("file:"):
        path = preceding_url[5:]  # Remove 'file:' prefix
        from icechunk import local_filesystem_storage

        return local_filesystem_storage(path)
    elif preceding_url.startswith("s3://"):
        from icechunk import s3_storage

        parsed = urlparse(preceding_url)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return s3_storage(
            bucket=bucket,
            prefix=prefix,
            from_env=True,  # Use environment variables for credentials
        )
    elif preceding_url.startswith(("gcs://", "gs://")):
        from icechunk import gcs_storage

        parsed = urlparse(preceding_url)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        return gcs_storage(
            bucket=bucket,
            prefix=prefix,
            from_env=True,  # Use environment variables for credentials
        )
    else:
        raise ValueError(f"Unsupported storage URL for icechunk: {preceding_url}")


async def _create_icechunk_store(repo: Repository, segment_path: str) -> IcechunkStore:
    """Create appropriate Icechunk session based on path specification.

    Uses Rust-based parsing and session creation utilities.
    """
    # Use Rust-based parsing for performance
    path_spec = parse_icechunk_path_spec(segment_path)
    return await create_readonly_session_from_path(repo, path_spec)


# =============================================================================
# Store Adapter Implementation
# =============================================================================


class IcechunkStoreAdapter(StoreAdapter):
    """Store adapter for Icechunk repositories in ZEP 8 URL chains.

    Leverages Rust-based parsing and session creation for high performance.
    """

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
        """
        # Icechunk adapter is read-only via ZEP 8 URLs
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
        # Uses the Rust-based implementation for high performance
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
            # Use Rust-based parsing for consistency and performance
            path_spec = parse_icechunk_path_spec(segment_path)
            return path_spec.path
        except Exception:
            return ""


# Additional short alias
class ICStoreAdapter(IcechunkStoreAdapter):
    """Short alias for IcechunkStoreAdapter."""

    adapter_name = "ic"
