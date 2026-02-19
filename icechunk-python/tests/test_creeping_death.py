"""CreepingDeathStateMachine — VersionControlStateMachine running against S3 (rustfs/minio)."""

import time

import pytest
from hypothesis import settings

import icechunk
from icechunk import Storage, s3_storage
from tests.test_stateful_repo_ops import VersionControlStateMachine


def _minio_available() -> bool:
    try:
        repo = icechunk.Repository.create(
            storage=s3_storage(
                endpoint_url="http://localhost:9000",
                allow_http=True,
                force_path_style=True,
                region="us-east-1",
                bucket="testbucket",
                prefix=f"_health_check_{time.time()}",
                access_key_id="minio123",
                secret_access_key="minio123",
            ),
        )
        # If we get here, minio is reachable
        del repo
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _minio_available(),
    reason="rustfs/minio not available (run `docker compose up -d`)",
)


class CreepingDeathStateMachine(VersionControlStateMachine):
    def _make_storage(self) -> Storage:
        prefix = f"creeping-death-{time.time()}"
        return s3_storage(
            endpoint_url="http://localhost:9000",
            allow_http=True,
            force_path_style=True,
            region="us-east-1",
            bucket="testbucket",
            prefix=prefix,
            access_key_id="minio123",
            secret_access_key="minio123",
        )


CreepingDeathStateMachine.TestCase.settings = settings(
    deadline=None,
    max_examples=50,
)
CreepingDeathTest = CreepingDeathStateMachine.TestCase
