"""ZEP 8 URL specification support for Python.

This module provides Python wrappers for the Rust-based ZEP 8 URL parsing
and session creation utilities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from icechunk import IcechunkStore, Repository

# Import Rust implementations - these are required
import icechunk._icechunk_python

# Access ZEP 8 functionality as attributes (submodule registration in PyO3)
_zep8_module = icechunk._icechunk_python.zep8
_RustIcechunkPathSpec = _zep8_module.IcechunkPathSpec
_rust_parse_icechunk_path_spec = _zep8_module.parse_icechunk_path_spec
_RustReferenceType = _zep8_module.ReferenceType


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
        branch: str | None = None,
        tag: str | None = None,
        snapshot_id: str | None = None,
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
    # Use Rust implementation for performance
    rust_spec = _rust_parse_icechunk_path_spec(segment_path)
    return IcechunkPathSpec(
        branch=rust_spec.reference_value
        if rust_spec.reference_type_str == "branch"
        else None,
        tag=rust_spec.reference_value if rust_spec.reference_type_str == "tag" else None,
        snapshot_id=rust_spec.reference_value
        if rust_spec.reference_type_str == "snapshot"
        else None,
        path=rust_spec.path,
    )


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


# Re-export shared utilities
__all__ = [
    "IcechunkPathSpec",
    "create_readonly_session_from_path",
    "parse_icechunk_path_spec",
]
