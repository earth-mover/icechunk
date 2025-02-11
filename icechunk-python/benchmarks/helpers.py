import logging
import os
import subprocess


def setup_logger():
    logger = logging.getLogger("icechunk-bench")
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)
    return logger


def get_coiled_kwargs(*, store: str, region: str | None = None) -> str:
    DEFAULT_REGIONS = {
        "s3": "us-east-1",
        "gcs": "us-east1",
        "tigris": "us-east-1",
        "az": "FOO",
    }
    WORKSPACES = {
        "s3": "earthmover-devs",
        "tigris": "earthmover-devs",
        "gcs": "earthmover-devs-gcp",
        "az": "earthmover-devs-azure",
    }
    TIGRIS_REGIONS = {"iad": "us-east-1"}

    if region is None:
        region = DEFAULT_REGIONS[store]
    else:
        region = TIGRIS_REGIONS[region] if store == "tigris" else region
    return {"workspace": WORKSPACES[store], "region": region}


def assert_cwd_is_icechunk_python():
    CURRENTDIR = os.getcwd()
    if not CURRENTDIR.endswith("icechunk-python"):
        raise ValueError(
            "Running in the wrong directory. Please run from the `icechunk-python` directory."
        )


def get_commit(ref: str) -> str:
    return subprocess.run(
        ["git", "rev-parse", ref], capture_output=True, text=True, check=True
    ).stdout.strip()[:8]
