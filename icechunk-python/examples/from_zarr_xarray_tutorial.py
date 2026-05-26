# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "icechunk",
#     "numpy",
#     "xarray",
#     "pooch",
#     "netcdf4",
# ]
# ///
"""Load xarray's `air_temperature` tutorial dataset, write it to a
local Zarr v3 store, then ingest into a fresh icechunk repository.

Run with uv (PEP 723 inline metadata pulls deps automatically):

    uv run examples/from_zarr_xarray_tutorial.py

To run against an in-tree development build of icechunk instead of the
released PyPI version:

    uv run --with-editable . examples/from_zarr_xarray_tutorial.py

What it does:
1. Downloads `xarray.tutorial.open_dataset("air_temperature")` (cached
   by pooch after the first run).
2. Writes the xarray Dataset to a local Zarr v3 store.
3. Ingests that store into an icechunk repository via `from_zarr`.
4. Opens the icechunk repo as an xarray Dataset and compares against
   the original to confirm the round-trip is value-faithful.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np

import icechunk
import xarray as xr


def main() -> None:
    with tempfile.TemporaryDirectory() as workdir:
        zarr_dir = Path(workdir) / "air_temperature.zarr"
        repo_dir = Path(workdir) / "air_temperature.icechunk"

        print("Loading xarray tutorial dataset 'air_temperature'")
        ds = xr.tutorial.open_dataset("air_temperature")
        print(ds)

        print(f"\nWriting Zarr v3 store to {zarr_dir}")
        ds.to_zarr(zarr_dir, mode="w", zarr_format=3, consolidated=False)

        print(f"\nCreating icechunk repo at {repo_dir}")
        repo = icechunk.Repository.create(
            icechunk.local_filesystem_storage(str(repo_dir))
        )

        source = icechunk.local_filesystem_storage(str(zarr_dir))

        def on_progress(stats: icechunk.IngestStats) -> None:
            print(
                f"  durable: {stats.keys} keys, "
                f"{stats.bytes / 1e6:.2f} MB, "
                f"{stats.arrays_done} arrays done"
            )

        print("\nIngesting Zarr store into icechunk:")
        result = icechunk.from_zarr(
            source,
            repo,
            message="import xarray air_temperature tutorial dataset",
            on_progress=on_progress,
        )
        print(f"\nFinal snapshot: {result.snapshot_id}")
        print(f"Copied {result.stats.keys} keys, {result.stats.bytes / 1e6:.2f} MB")

        print("\nReading back from icechunk as xarray:")
        session = repo.readonly_session(branch="main")
        ds_icechunk = xr.open_zarr(session.store, consolidated=False)
        print(ds_icechunk)

        print("\nVerifying round-trip:")
        for name in ds.data_vars:
            np.testing.assert_array_equal(ds[name].values, ds_icechunk[name].values)
            print(f"  {name}: OK")
        for name in ds.coords:
            np.testing.assert_array_equal(ds[name].values, ds_icechunk[name].values)
            print(f"  {name}: OK")


if __name__ == "__main__":
    main()
