"""Tests for the experimental I/O governor API."""

import pickle
import subprocess
import sys
from pathlib import Path

import zarr
from icechunk import Repository, RepositoryConfig, in_memory_storage
from icechunk.experimental import (
    BandwidthGovernor,
    BandwidthGovernorConfig,
    CompatGovernor,
    CompatGovernorConfig,
    DirectionConfig,
    DirectionMetrics,
    GovernorMetrics,
    IoGovernor,
    MemoryMetrics,
)


def direction_config(target_bandwidth: int = 100_000_000) -> DirectionConfig:
    return DirectionConfig(
        target_bandwidth=target_bandwidth,
        max_connection_bandwidth=90_000_000,
        min_connection_bandwidth=7_500_000,
        request_latency_us=30_000,
        min_request_bytes=30_000,
    )


def bandwidth_config(label: str = "test") -> BandwidthGovernorConfig:
    return BandwidthGovernorConfig.s3_defaults(
        label,
        read_bandwidth=100_000_000,
        write_bandwidth=50_000_000,
        memory_budget=512 * 1024 * 1024,
    )


def test_direction_config_construct_eq_repr_pickle() -> None:
    cfg = direction_config()
    assert cfg.target_bandwidth == 100_000_000
    assert cfg.request_latency_us == 30_000
    # the default value
    assert cfg.unknown_request_bytes == 1024 * 1024

    assert cfg == direction_config()
    assert cfg != direction_config(target_bandwidth=1)

    import icechunk

    assert eval(repr(cfg), {"icechunk": icechunk}) == cfg
    assert pickle.loads(pickle.dumps(cfg)) == cfg


def test_bandwidth_config_construct_eq_repr_pickle() -> None:
    cfg = BandwidthGovernorConfig(
        label="etl",
        read=direction_config(),
        write=direction_config(target_bandwidth=50_000_000),
        memory_budget=512 * 1024 * 1024,
    )
    assert cfg.label == "etl"
    assert cfg.read.target_bandwidth == 100_000_000
    assert cfg.write.target_bandwidth == 50_000_000
    # the default value
    assert cfg.unknown_object_bytes == 4 * 1024 * 1024

    # nested attribute assignment mutates the config, not a copy
    cfg.read.target_bandwidth = 1_000_000
    assert cfg.read.target_bandwidth == 1_000_000
    cfg.read.target_bandwidth = 100_000_000

    import icechunk

    assert eval(repr(cfg), {"icechunk": icechunk}) == cfg
    assert pickle.loads(pickle.dumps(cfg)) == cfg


def test_omitted_label_is_random_and_unique() -> None:
    a = BandwidthGovernorConfig.s3_defaults(
        read_bandwidth=100_000_000,
        write_bandwidth=50_000_000,
        memory_budget=512 * 1024 * 1024,
    )
    b = BandwidthGovernorConfig.s3_defaults(
        read_bandwidth=100_000_000,
        write_bandwidth=50_000_000,
        memory_budget=512 * 1024 * 1024,
    )
    assert a.label.startswith("governor-")
    assert a.label != b.label
    # same numbers, different identities
    assert a != b

    c = BandwidthGovernorConfig(
        read=direction_config(),
        write=direction_config(),
        memory_budget=1024,
    )
    assert c.label.startswith("governor-")


def test_distinct_unlabeled_governors_do_not_intern_together() -> None:
    """Two same-numbers governors built without labels keep separate
    identities when their sessions are unpickled in one process."""

    def session_bytes() -> bytes:
        gov = BandwidthGovernor(
            BandwidthGovernorConfig.s3_defaults(
                read_bandwidth=100_000_000,
                write_bandwidth=50_000_000,
                memory_budget=512 * 1024 * 1024,
            )
        )
        repo = Repository.create(in_memory_storage(), governor=gov)
        return pickle.dumps(repo.readonly_session(branch="main"))

    s1 = pickle.loads(session_bytes())
    s2 = pickle.loads(session_bytes())
    assert s1.governor != s2.governor


def test_bandwidth_config_s3_defaults() -> None:
    cfg = bandwidth_config("etl")
    assert cfg.label == "etl"
    assert cfg.read.target_bandwidth == 100_000_000
    assert cfg.read.max_connection_bandwidth == 90_000_000
    assert cfg.write.target_bandwidth == 50_000_000
    assert cfg.write.max_connection_bandwidth == 20_000_000
    assert cfg.read.request_latency_us == 30_000
    assert cfg.memory_budget == 512 * 1024 * 1024


def test_compat_config_construct_eq_repr_pickle() -> None:
    cfg = CompatGovernorConfig()
    assert cfg.max_concurrent_requests == 256
    cfg = CompatGovernorConfig(max_concurrent_requests=7)
    assert cfg.max_concurrent_requests == 7

    assert cfg == CompatGovernorConfig(max_concurrent_requests=7)
    assert cfg != CompatGovernorConfig()

    import icechunk

    assert eval(repr(cfg), {"icechunk": icechunk}) == cfg
    assert pickle.loads(pickle.dumps(cfg)) == cfg


def test_bandwidth_governor_knobs_and_metrics() -> None:
    gov = BandwidthGovernor(bandwidth_config())
    assert isinstance(gov, IoGovernor)
    assert gov.label == "test"
    assert gov.read_bandwidth == 100_000_000
    assert gov.write_bandwidth == 50_000_000
    assert gov.memory_budget == 512 * 1024 * 1024

    gov.read_bandwidth = 10_000_000
    gov.write_bandwidth = 5_000_000
    gov.memory_budget = 1024 * 1024
    assert gov.read_bandwidth == 10_000_000
    assert gov.write_bandwidth == 5_000_000
    assert gov.memory_budget == 1024 * 1024

    metrics = gov.metrics()
    assert isinstance(metrics, GovernorMetrics)
    assert isinstance(metrics.read, DirectionMetrics)
    assert isinstance(metrics.write, DirectionMetrics)
    assert isinstance(metrics.memory, MemoryMetrics)
    assert metrics.read.target_bandwidth == 10_000_000
    assert metrics.write.target_bandwidth == 5_000_000
    assert metrics.memory.budget == 1024 * 1024


def test_governor_shared_across_repositories() -> None:
    gov = BandwidthGovernor(bandwidth_config())
    repo_a = Repository.create(in_memory_storage(), governor=gov)
    repo_b = Repository.create(in_memory_storage(), governor=gov)

    assert repo_a.governor == gov
    assert repo_a.governor == repo_b.governor

    # a knob change is visible through every handle
    gov.read_bandwidth = 12_345_678
    assert isinstance(repo_a.governor, BandwidthGovernor)
    assert repo_a.governor.read_bandwidth == 12_345_678

    # drive some I/O through the governor and drain back to idle
    session = repo_a.writable_session("main")
    root = zarr.group(store=session.store)
    array = root.ones(name="ones", shape=(10, 10), chunks=(5, 5), dtype="float32")
    array[:] = 20
    session.commit("first")

    metrics = gov.metrics()
    assert metrics.read.in_flight_requests == 0
    assert metrics.write.in_flight_requests == 0
    assert metrics.read.queued_requests == 0
    assert metrics.memory.reserved == 0
    assert metrics.read.throttles_total == 0
    assert metrics.write.throttles_total == 0


def test_default_governor_is_compat() -> None:
    repo = Repository.create(in_memory_storage())
    gov = repo.governor
    assert isinstance(gov, CompatGovernor)
    assert gov.max_concurrent_requests == 256

    config = RepositoryConfig(max_concurrent_requests=11)
    repo = Repository.create(in_memory_storage(), config=config)
    gov = repo.governor
    assert isinstance(gov, CompatGovernor)
    assert gov.max_concurrent_requests == 11


def test_explicit_compat_governor_shared() -> None:
    gov = CompatGovernor(CompatGovernorConfig(max_concurrent_requests=17))
    repo_a = Repository.create(in_memory_storage(), governor=gov)
    repo_b = Repository.create(in_memory_storage(), governor=gov)
    assert repo_a.governor == repo_b.governor == gov
    assert isinstance(repo_a.governor, CompatGovernor)
    assert repo_a.governor.max_concurrent_requests == 17


def test_governor_identity_semantics() -> None:
    cfg = bandwidth_config()
    gov_a = BandwidthGovernor(cfg)
    gov_b = BandwidthGovernor(cfg)
    # equality is instance identity, not config equality
    assert gov_a != gov_b

    repo = Repository.create(in_memory_storage(), governor=gov_a)
    session = repo.readonly_session(branch="main")
    assert session.governor == gov_a
    assert session.governor != gov_b


def test_unpickled_sessions_intern_to_one_governor() -> None:
    gov = BandwidthGovernor(bandwidth_config("intern-test"))
    repo = Repository.create(in_memory_storage(), governor=gov)

    b1 = pickle.dumps(repo.readonly_session(branch="main"))
    b2 = pickle.dumps(repo.readonly_session(branch="main"))

    s1 = pickle.loads(b1)
    s2 = pickle.loads(b2)
    # equal factory recipes rebind to one process-wide instance...
    assert s1.governor == s2.governor
    # ...which is a fresh instance, not the injected one (that never
    # entered the intern table)
    assert s1.governor != gov
    # the rebound governor is concrete, knobs and metrics work
    rebound = s1.governor
    assert isinstance(rebound, BandwidthGovernor)
    assert rebound.label == "intern-test"
    assert rebound.read_bandwidth == 100_000_000


def test_knob_change_interns_to_different_governor() -> None:
    gov = BandwidthGovernor(bandwidth_config("knob-intern-test"))
    repo = Repository.create(in_memory_storage(), governor=gov)

    before = pickle.dumps(repo.readonly_session(branch="main"))
    gov.read_bandwidth = 1_000_000
    after = pickle.dumps(repo.readonly_session(branch="main"))

    s_before = pickle.loads(before)
    s_after = pickle.loads(after)
    # the recipes differ, so they rebind to different instances
    assert s_before.governor != s_after.governor
    assert isinstance(s_before.governor, BandwidthGovernor)
    assert isinstance(s_after.governor, BandwidthGovernor)
    assert s_before.governor.read_bandwidth == 100_000_000
    assert s_after.governor.read_bandwidth == 1_000_000


_CHILD_SCRIPT = """
import pickle
import sys

from icechunk.experimental import BandwidthGovernor

with open(sys.argv[1], "rb") as f:
    s1 = pickle.load(f)
with open(sys.argv[2], "rb") as f:
    s2 = pickle.load(f)

assert s1.governor == s2.governor
assert isinstance(s1.governor, BandwidthGovernor)
assert s1.governor.label == "child-test"
"""


def test_unpickled_sessions_share_governor_in_child_process(tmp_path: Path) -> None:
    """Sessions shipped to another process rebind to one governor there."""
    gov = BandwidthGovernor(bandwidth_config("child-test"))
    repo = Repository.create(in_memory_storage(), governor=gov)

    p1 = tmp_path / "s1.pickle"
    p2 = tmp_path / "s2.pickle"
    p1.write_bytes(pickle.dumps(repo.readonly_session(branch="main")))
    p2.write_bytes(pickle.dumps(repo.readonly_session(branch="main")))

    subprocess.run(
        [sys.executable, "-c", _CHILD_SCRIPT, str(p1), str(p2)],
        check=True,
    )
