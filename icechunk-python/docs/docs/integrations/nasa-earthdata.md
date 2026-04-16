# NASA Earthdata

Many NASA datasets are hosted in AWS S3 but require authentication through [NASA Earthdata Login](https://urs.earthdata.nasa.gov/) (EDL) to access. Icechunk provides built-in support for obtaining and automatically refreshing the temporary S3 credentials needed to read this data.

## Prerequisites

1. A free [NASA Earthdata Login account](https://urs.earthdata.nasa.gov/users/new)
2. Your credentials available via one of:
    - Environment variables: `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD`
    - A `~/.netrc` entry: `machine urs.earthdata.nasa.gov login <user> password <pass>`
    - Or pass them directly to `s3_earthdata_credentials(auth=("user", "pass"))`

!!! warning

    Direct S3 access to [NASA Earthdata](https://www.earthdata.nasa.gov/) buckets **only works from the us-west-2 AWS region**.
    The temporary credentials are scoped to same-region access only — attempting
    to read data from outside us-west-2 will result in an `AccessDenied` error.

## Example: Reading a virtual dataset

This example opens the [MUR Sea Surface Temperature](https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1) dataset, which is stored as an Icechunk repository with virtual chunk references pointing to data in PO.DAAC's S3 buckets.

```python
import icechunk as ic
import xarray as xr

# The Icechunk repo itself is in a public S3 bucket
storage = ic.s3_storage(
    bucket="nasa-eodc-public",
    prefix="icechunk/MUR-JPL-L4-GLOB-v4.1-virtual-v2-p2",
    region="us-west-2",
    anonymous=True,
)

# Create refreshable credentials for the PO.DAAC virtual chunk container.
# Icechunk will automatically call the /s3credentials endpoint and
# refresh the temporary AWS credentials whenever they expire (~1 hour).
podaac_creds = ic.s3_earthdata_credentials(
    "https://archive.podaac.earthdata.nasa.gov/s3credentials"
)

# Open the repo with virtual chunk access authorized
repo = ic.Repository.open(
    storage=storage,
    authorize_virtual_chunk_access=ic.containers_credentials({
        "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/": podaac_creds,
    }),
)

# Read with xarray
session = repo.readonly_session("main")
ds = xr.open_zarr(session.store, zarr_format=3, consolidated=False)
ds
```

## How it works

`s3_earthdata_credentials` returns an Icechunk `Refreshable` credential that:

1. Calls the DAAC's `/s3credentials` endpoint with your EDL credentials
2. Receives temporary AWS STS credentials (valid ~1 hour)
3. Automatically re-fetches credentials when they expire

No additional dependencies are needed beyond `icechunk` itself.

## Common DAAC credential endpoints

Each NASA DAAC has its own S3 credential endpoint. Use the one that corresponds to the DAAC hosting the data your virtual chunks reference:

| DAAC | Endpoint |
|------|----------|
| PO.DAAC | `https://archive.podaac.earthdata.nasa.gov/s3credentials` |
| NSIDC | `https://data.nsidc.earthdatacloud.nasa.gov/s3credentials` |
| LP DAAC | `https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials` |
| GES DISC | `https://data.gesdisc.earthdata.nasa.gov/s3credentials` |
| ORNL DAAC | `https://data.ornldaac.earthdata.nasa.gov/s3credentials` |
| GHRC DAAC | `https://data.ghrc.earthdata.nasa.gov/s3credentials` |
| ASF | `https://sentinel1.asf.alaska.edu/s3credentials` |

!!! warning

    Credentials from one DAAC cannot access another DAAC's buckets. If your virtual dataset references data from multiple DAACs, create separate credentials for each and map them to the appropriate virtual chunk containers.
