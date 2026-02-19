"""CreepingDeathStateMachine — VersionControlStateMachine running against S3 (rustfs/minio)."""

# TODO:
# 1. Be careful about retry settings
# 2. Add some base latency to make sure we trigger stuff?
# 3. write some chunks too!

import time

from hypothesis import note, settings

from icechunk import Storage, s3_storage
from icechunk.testing import toxiproxy
from tests.test_stateful_repo_ops import VersionControlStateMachine


class CreepingDeathStateMachine(VersionControlStateMachine):
    """
    This state machine test adds the complexity of an unreliable network.

    1. We use a toxiproxy+rustfs to simulate an unreliable object store.
    """

    toxi: toxiproxy.ToxiproxyClient
    proxy_name: str

    def __init__(self) -> None:
        self.toxi = toxiproxy.ToxiproxyClient("http://localhost:8474")
        self.proxy_name = f"creeping-death-{time.time()}"
        note(f"creating proxy {self.proxy_name}")
        self.toxi.create_proxy(
            self.proxy_name, listen="0.0.0.0:9002", upstream="rustfs:9000"
        )

        # Add some base latency to make sure we can do stuff
        base_dn = toxiproxy.Latency(
            name="base latency dn", latency=10, jitter=0, stream="downstream"
        )
        base_up = toxiproxy.Latency(
            name="base latency up", latency=10, jitter=0, stream="upstream"
        )
        self.toxi.add_toxic(self.proxy_name, base_dn)
        self.toxi.add_toxic(self.proxy_name, base_up)
        super().__init__()

    def teardown(self) -> None:
        note(f"Deleting proxy {self.proxy_name}")
        self.toxi.delete_proxy(self.proxy_name)

    def _make_storage(self) -> Storage:
        prefix = f"creeping-death-{time.time()}"
        return s3_storage(
            endpoint_url="http://localhost:9002",
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
