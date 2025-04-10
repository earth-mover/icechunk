#!/usr/bin/env python3
#
# This is just a scratch script for testing purposes
# coiled notebook start --sync --software icechunk-alpha-12 --vm-type m5.4xlarge
import subprocess

# software = "icechunk-alpha-12"
vm_type = {
    "s3": "m5.4xlarge",
    "gcs": None,
    "tigris": None,
}
ref = "icechunk-v0.1.0-alpha.12"

COILED_SOFTWARE = {
    "icechunk-v0.1.0-alpha.1": "icechunk-alpha-release",
    "icechunk-v0.1.0-alpha.12": "icechunk-alpha-12",
}
software = COILED_SOFTWARE[ref]

cmd = f'python benchmarks/runner.py --where coiled --pytest "-k zarr_open" {ref}'
subprocess.run(
    [
        "coiled",
        "run",
        "--name",
        "icebench-712f1eb2",
        "--sync",
        "--sync-ignore='python/ reports/ profiling/'",
        "--keepalive",
        "5m",
        "--workspace=earthmover-devs",
        "--vm-type=m5.4xlarge",
        "--software=icechunk-bench-712f1eb2",
        "--region=us-east-1",
        "pytest -v benchmarks/",
    ]
)
