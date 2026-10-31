"""Tests for empty-`prefix` (bucket-root) S3 repositories.

Empty-prefix repositories triggered the #2239 key-layout bug and are hard to test
in isolation, so creating *new* ones on an object store is no longer allowed;
pre-existing ones can still be opened. These tests verify that creation is refused
through the bindings and that the `legacy_rooted_keys` escape hatch keeps
validating its arguments.
"""

import pytest

import icechunk as ic

ENDPOINT = "http://localhost:4200"
REGION = "us-east-1"
ROOT_KEY, ROOT_SECRET = "test123", "test123"


def empty_prefix_storage(prefix: str | None = None) -> ic.Storage:
    return ic.s3_storage(
        bucket="testbucket",
        prefix=prefix,
        region=REGION,
        endpoint_url=ENDPOINT,
        allow_http=True,
        access_key_id=ROOT_KEY,
        secret_access_key=ROOT_SECRET,
        force_path_style=True,
    )


def test_empty_prefix_create_refused() -> None:
    # Creating a new empty-prefix (bucket-root) repository is no longer supported.
    # The check runs before any network I/O, so no live store is needed.
    with pytest.raises(Exception, match="empty prefix"):
        ic.Repository.create(storage=empty_prefix_storage())


def test_legacy_rooted_keys_rejected_with_nonempty_prefix() -> None:
    # The escape hatch is only valid for an empty prefix.
    with pytest.raises(Exception, match="empty prefix"):
        ic.s3_storage(
            bucket="testbucket",
            prefix="some/prefix",
            region=REGION,
            endpoint_url=ENDPOINT,
            allow_http=True,
            access_key_id=ROOT_KEY,
            secret_access_key=ROOT_SECRET,
            force_path_style=True,
            legacy_rooted_keys=True,
        )
