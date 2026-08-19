"""Experimental Icechunk features.

Everything in this module is experimental: APIs may change or disappear in
any release, without a deprecation cycle. Import from here deliberately.

Currently this module hosts I/O governors, which control how Icechunk
performs I/O against object storage. A governor is injected when a
repository is created or opened, and one instance can be shared by several
repositories to bound their combined resource usage:

```python
from icechunk.experimental import BandwidthGovernor, BandwidthGovernorConfig

governor = BandwidthGovernor(
    BandwidthGovernorConfig.s3_defaults(
        "my-workload",
        read_bandwidth=100_000_000,  # bytes/s
        write_bandwidth=50_000_000,
        memory_budget=512 * 1024 * 1024,
    )
)
repo = icechunk.Repository.open(storage, governor=governor)
```
"""

from icechunk._icechunk_python import (
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

__all__ = [
    "BandwidthGovernor",
    "BandwidthGovernorConfig",
    "CompatGovernor",
    "CompatGovernorConfig",
    "DirectionConfig",
    "DirectionMetrics",
    "GovernorMetrics",
    "IoGovernor",
    "MemoryMetrics",
]
