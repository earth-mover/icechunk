"""CreepingDeathStateMachine — VersionControlStateMachine running against S3 (rustfs/minio)."""

# TODO:
# 1. Be careful about retry settings
# 2. Add some base latency to make sure we trigger stuff?
# 3. write some chunks too!
# 4. draw values for toxics
# 5. I need to test the repo backup stuff, make sure we can reproduce some retries there; might need extra logging in IC rust
# 6. Hyper seems to be trying more than `max_tries`!

import threading
import time
from collections.abc import Callable
from typing import Any

import hypothesis.strategies as st
from hypothesis import note, settings
from hypothesis.stateful import invariant

from icechunk import Storage, StorageRetriesSettings, StorageSettings, s3_storage
from icechunk.testing import toxiproxy
from icechunk.testing.strategies import repository_configs
from tests.test_stateful_repo_ops import VersionControlStateMachine

NETWORK_STREAM_TIMEOUT_SECONDS = 1
STORAGE_RETRIES = StorageRetriesSettings(
    max_tries=1,
    initial_backoff_ms=10,
    max_backoff_ms=4000,
)


class CreepingDeathStateMachine(VersionControlStateMachine):
    """
    This state machine test adds the complexity of an unreliable network.

    1. We use a toxiproxy+rustfs to simulate an unreliable object store.
    2. We apply a randomly generated set of Toxics just before firing a @rule of the superclass.
    3. We remove the Toxic *during* the execution of the @rule, thus "healing" the network.
       Parameters of the Toxic are tweaked to test retries (:crossed_fingers:).
    More details are in the docstrings for functions below, particularly schedule_toxics & _rule_advise
    """

    toxi: toxiproxy.ToxiproxyClient
    proxy_name: str
    pending_toxics: tuple[toxiproxy.Toxic, ...]

    def __init__(self) -> None:
        self.pending_toxics = ()
        self.toxi = toxiproxy.ToxiproxyClient("http://localhost:8474")
        self.proxy_name = f"creeping-death-{time.time()}"
        note(f"creating proxy {self.proxy_name}")
        self.toxi.create_proxy(
            self.proxy_name, listen="0.0.0.0:9002", upstream="rustfs:9000"
        )

        # Add some base latency to make sure we can do stuff
        base_dn = toxiproxy.Latency(
            name="base latency dn", latency_ms=10, jitter_ms=0, stream="downstream"
        )
        base_up = toxiproxy.Latency(
            name="base latency up", latency_ms=10, jitter_ms=0, stream="upstream"
        )
        self.toxi.add_toxic(self.proxy_name, base_dn)
        self.toxi.add_toxic(self.proxy_name, base_up)
        super().__init__()

    def teardown(self) -> None:
        note(f"Deleting proxy {self.proxy_name}")
        self.toxi.delete_proxy(self.proxy_name)

    def _rule_advice(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        The challenge:
          > Hypothesis calls rule.function(machine, **data) directly —
          > it caches Rule objects from inspect.getmembers(cls) at class setup time.
        Thus simply overriding __get__ or __getattribute__ will not do :(.

        So instead we define an "advice" that runs before every rule.
        Here, this advice schedules the *setting* of Toxics and their *removal* using threading.Timer.
        This is nicely sync (plays well with hypothesis), and simulates the effect of a bad
        connection healing itself. Time durations are tweaked so that we retry at least once.

        See `schedule_toxics` for more details about the approach to setting Toxics.
        """
        if not self.pending_toxics:
            return fn(self, *args, **kwargs)

        for toxic in self.pending_toxics:
            self.toxi.add_toxic(self.proxy_name, toxic)
            # set the remove toxic action to run after a bit
            threading.Timer(
                7,
                self.toxi.remove_toxic,
                kwargs={"proxy_name": self.proxy_name, "toxic_name": toxic.name},
            ).start()

        ret = fn(self, *args, **kwargs)

        self.pending_toxics = ()

        return ret

    @invariant()
    def schedule_toxics(self):
        """
        This rule sets up Toxics to be set just before a rule is fired.
        It is an "invariant" purely so that is run prior to every other @rule.

        We cannot simply apply the Toxics here because the Hypothesis may run an invariant rule next.
        Thus if applied now, the Toxic will only affect the invariants.

        Instead, we generate some Toxics to be applied, store that state, and then apply them just before
        a @rule is fired using the __init_subclass__ approach.
        """
        assert not self.pending_toxics

        # TODO: we could set toxicity to some fractional number but for simplicity we just apply it always
        # TODO: generalize, this particular combo replicates the stalled stream error
        # TODO: tweak values so we trigger this on JSON sets as well as repo_info stuff?
        self.pending_toxics = (
            toxiproxy.LimitData(name="data_limit_dn", bytes=10, stream="downstream"),
            toxiproxy.LimitData(name="data_limit_up", bytes=10, stream="upstream"),
            toxiproxy.SlowClose(name="slow_close_dn", delay_ms=2000, stream="downstream"),
            toxiproxy.SlowClose(name="slow_close_up", delay_ms=2000, stream="downstream"),
        )

    def _repository_configs(self) -> st.SearchStrategy:
        return repository_configs(
            storage_settings=st.just(StorageSettings(retries=STORAGE_RETRIES)),
        )

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
            network_stream_timeout_seconds=NETWORK_STREAM_TIMEOUT_SECONDS,
        )


CreepingDeathStateMachine.TestCase.settings = settings(deadline=None, max_examples=50)
CreepingDeathTest = CreepingDeathStateMachine.TestCase
