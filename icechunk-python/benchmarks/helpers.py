import os
import subprocess


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
