---
title: Languages
---

# Languages

Icechunk is a format with a Rust core.
Most libraries bind to that core, so every one of them reads and writes the same repositories.
Independent readers implement the [spec](reference/spec-v2-1.md) directly.

## Official

Maintained in the [icechunk repository](https://github.com/earth-mover/icechunk) and supported by Earthmover.

| Language | Package | Access | Notes |
|---|---|---|---|
| Rust | [`icechunk`](https://crates.io/crates/icechunk) | Read and write | The core library. [Rust docs](reference/icechunk-rust.md) |
| Python | [`icechunk`](https://pypi.org/project/icechunk) | Read and write | Works with Zarr Python, Xarray and Dask. [API reference](reference/index.md) |
| JavaScript / TypeScript | [`@earthmover/icechunk`](https://www.npmjs.com/package/@earthmover/icechunk) | Read and write | Node.js through native bindings, the browser through WebAssembly. Use with zarrita. [JS docs](reference/icechunk-js.md) |

## Experimental

Earthmover projects under the [earth-mover](https://github.com/earth-mover) GitHub organization.
They bind to the Rust core but are not officially supported yet, and their APIs change without notice.

| Language | Package | Access | Notes |
|---|---|---|---|
| Java | [icechunk-java](https://github.com/earth-mover/icechunk-java) | Read and write | Bindings over JNI, a zarr-java store, and connectors for N5, Fiji and BigDataViewer. Not on Maven Central. |
| Julia | [Zarrs.jl](https://github.com/earth-mover/Zarrs.jl) | Read and write | Julia bindings to the zarrs Rust crate, with Icechunk support. |

## Community

Built and maintained outside Earthmover.

| Language | Project | Access | Notes |
|---|---|---|---|
| C++ | [GDAL Icechunk driver](https://gdal.org/en/latest/drivers/raster/icechunk.html) | Read only | Independent implementation of the spec. |
| TypeScript | [EarthyScience/icechunk-js](https://github.com/EarthyScience/icechunk-js) | Read only | Independent implementation, designed for zarrita. |
| JavaScript | [Neuroglancer](https://neuroglancer-docs.web.app/datasource/icechunk/index.html) | Read only | Independent implementation in Google's volumetric viewer. |
| Rust | [zarrs_icechunk](https://github.com/zarrs/zarrs_icechunk) | Read and write | Icechunk store for the zarrs crate, built on the `icechunk` crate. |

Independent readers can lag behind new spec versions, so check which versions each one reads.

## R

Native R bindings are in progress.
Today, R can use the Python library through [reticulate](https://rstudio.github.io/reticulate/):

```r
library(reticulate)
py_require(c("icechunk", "xarray", "pooch", "scipy"))

ic <- import("icechunk")
xr <- import("xarray")

repo <- ic$Repository$create(ic$local_filesystem_storage("demo-repo"))

session <- repo$writable_session("main")
ds <- xr$tutorial$open_dataset("air_temperature")
ds$to_zarr(session$store, consolidated = FALSE)
session$commit("Add air temperature from R")

session <- repo$readonly_session(branch = "main")
ds <- xr$open_zarr(session$store, consolidated = FALSE)
air <- py_to_r(ds$air$isel(time = 0L)$values)
```

The Arraylake docs have [more R examples](https://docs.earthmover.io/cookbooks/R), including plotting.

Building something with Icechunk in another language? [Open an issue](https://github.com/earth-mover/icechunk/issues) and we'll add it here.
