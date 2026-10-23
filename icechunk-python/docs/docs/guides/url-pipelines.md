---
title: URL Pipelines
---
# Opening repositories by URL

!!! warning "Experimental"
    URL pipeline support needs a zarr version with
    [URL pipelines](https://github.com/zarr-developers/zarr-python/pull/4192),
    which is not released yet.

Icechunk implements the `icechunk:` scheme of the
[URL pipeline specification](https://github.com/jbms/url-pipeline) (ZEP 8).
A URL pipeline names a root location, the Icechunk ref to read and a path
inside the repository, separated by `|`:

```text
s3://bucket/repo|icechunk://tag.v1/path/to/group|zarr3:
```

With Icechunk installed, wrap the URL in a `zarr.storage.URLPipeline` and pass
it anywhere zarr or Xarray accept a store:

```python
import xarray as xr
import zarr
from zarr.storage import URLPipeline

group = zarr.open_group(
    URLPipeline.from_url("s3://bucket/repo|icechunk://tag.v1/path/to/group"), mode="r"
)
ds = xr.open_zarr(URLPipeline.from_url("s3://bucket/repo|icechunk://tag.v1/path/to/group"))
array = zarr.open_array(
    URLPipeline.from_url("gs://bucket/repo|icechunk://branch.dev/temperature"), mode="r"
)
```

zarr reads only `URLPipeline` objects as pipelines; a plain string such as
`"s3://bucket/repo|icechunk:"` is not one.

The same URLs work in other tools that implement the specification, such as
TensorStore and Neuroglancer.

## Selecting a ref

| Segment | Opens |
|---|---|
| `icechunk:` or `icechunk:path` | the tip of `main` |
| `icechunk://branch.NAME/path` | the tip of branch `NAME` |
| `icechunk://tag.NAME/path` | tag `NAME` |
| `icechunk://SNAPSHOT_ID/path` | a snapshot, e.g. `icechunk://FWWFQGAW742XMX0F5MF0/` |

Branch and tag names may be percent-encoded, but must not contain `/`.

## Root URLs

| Root | Storage |
|---|---|
| `file:///abs/path` | local filesystem (absolute paths only) |
| `memory:NAME` | in-memory, shared per `NAME` within the process (see below) |
| `s3://bucket/prefix` | S3 |
| `s3+https://endpoint/bucket/prefix` | an S3-compatible endpoint (`s3+http:` allows plain HTTP) |
| `gs://bucket/prefix` | Google Cloud Storage |
| `https://host/path` | HTTP, read-only |

`icechunk.storage_from_url` uses the same mapping, which is handy for
creating a repository that you then open by URL:

```python
import icechunk

repo = icechunk.Repository.create(icechunk.storage_from_url("memory:scratch"))
# ... write and commit ...
zarr.open_group(URLPipeline.from_url("memory:scratch|icechunk:"), mode="r")
```

Icechunk resolves these roots itself, so its rules can differ from zarr's own
roots. In particular, Icechunk's `memory:NAME` is not zarr's `memory:` root:
zarr keeps one process-wide store and reads the body as a path inside it, so
`memory:a/b` is the key prefix `a/b`. Icechunk keeps a separate in-memory
storage for each name, and the whole body is the name: `memory:a/b` is a
storage named `a/b`, unrelated to `memory:a`. Icechunk's in-memory storages
are not visible to zarr's `memory:` store, or the reverse.

## Credentials and configuration

Pass credentials and configuration through `storage_options`:

```python
xr.open_zarr(
    URLPipeline.from_url("s3://bucket/repo|icechunk://tag.v1/"),
    storage_options={
        "icechunk_credentials": icechunk.s3_static_credentials(
            access_key_id="...", secret_access_key="..."
        ),
        "icechunk_region": "us-west-2",
        "icechunk_config": icechunk.RepositoryConfig.default(),
        "icechunk_virtual_chunk_credentials": {
            "s3://source-bucket/": icechunk.s3_anonymous_credentials(),
        },
    },
)
```

| Key | Meaning |
|---|---|
| `icechunk_credentials` | S3 or GCS credentials, or a dict with `access_key_id`, `secret_access_key` and optionally `session_token` |
| `icechunk_anonymous` | use anonymous access |
| `icechunk_region`, `icechunk_endpoint_url` | S3 region and endpoint |
| `icechunk_config` | an `icechunk.RepositoryConfig` |
| `icechunk_virtual_chunk_credentials` | credentials for virtual chunk containers, as for `authorize_virtual_chunk_access` |

The fsspec-style keys `anon`, `key`, `secret`, `token` (an S3 session token),
`endpoint_url` and `region` are accepted as aliases. Without credentials or
`anonymous`, S3 and GCS credentials are read from the environment. Other keys
are rejected. Error messages name the offending keys but never include their
values, and they show a URL's query string as `?…`.

## Read-only access

URLs always open read-only sessions, because zarr has no step at which to
commit. When passing a `URLPipeline` to `zarr.open`, `zarr.open_group` or
`zarr.open_array`, pass `mode="r"`. Every other mode raises an error,
including `"a"`, the default of `zarr.open` and `zarr.open_group`. Xarray opens stores read-only, so
`xr.open_zarr` needs no extra argument. To write, open the repository and
use a writable session:

```python
repo = icechunk.Repository.open(icechunk.storage_from_url("s3://bucket/repo"))
session = repo.writable_session("main")
# ... write ...
session.commit("my changes")
```

## Other root schemes

Packages can add root schemes, such as `arraylake:` for Arraylake, by registering a
repository opener in the `icechunk.url_repository_openers` entry-point group.
See `icechunk.zarr_url_adapter.URLRepositoryOpener` for the contract.
