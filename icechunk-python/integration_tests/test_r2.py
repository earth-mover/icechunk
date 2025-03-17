import pytest

import icechunk as ic
import xarray as xr


@pytest.mark.parametrize(
    "storage",
    [
        pytest.param(
            ic.r2_storage(
                prefix="v1/era5_weatherbench2",
                endpoint_url="https://data.icechunk.cloud",
                anonymous=True,
            ),
            id="public-url",
        ),
        pytest.param(
            ic.s3_storage(
                bucket="icechunk-public-data-r2",
                prefix="v1/era5_weatherbench2",
                endpoint_url="https://caa3022c13c9823de0d22b3b6c249494.r2.cloudflarestorage.com",
                region="auto",
                force_path_style=True,
            ),
            id="full-s3",
            marks=pytest.mark.skip(),
        ),
        pytest.param(
            ic.r2_storage(
                bucket="icechunk-public-data-r2",
                prefix="v1/era5_weatherbench2",
                endpoint_url="https://caa3022c13c9823de0d22b3b6c249494.r2.cloudflarestorage.com",
            ),
            id="endpoint-bucket",
            marks=pytest.mark.skip(),
        ),
        pytest.param(
            ic.r2_storage(
                bucket="icechunk-public-data-r2",
                prefix="v1/era5_weatherbench2",
                account_id="caa3022c13c9823de0d22b3b6c249494",
            ),
            id="account-bucket",
            marks=pytest.mark.skip(),
        ),
    ],
)
def test_r2(storage):
    repo = ic.Repository.open(storage=storage)
    session = repo.readonly_session("main")

    # simply test that we can open
    xr.open_dataset(
        session.store,
        group="1x721x1440",
        engine="zarr",
        chunks=None,
        consolidated=False,
    )
