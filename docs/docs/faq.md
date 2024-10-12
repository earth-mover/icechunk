---
title: Icechunk - Frequenctly Asked Questions 
---

# FAQ

## Why was Icechunk created?

Icechunk was created by [Earthmover](https://earthmover.io/) as the open-source format for its cloud data platform Arraylake.

Icechunk builds on the successful [Zarr](https://zarr.dev) project.
Zarr is a great foundation for storing and querying large multidimensional array data in a flexible, scalable way.
But when people started using Zarr together with cloud object storage in a collaborative way, it became clear that Zarr alone could not offer the sort of consistency many users desired.
Icechunk makes Zarr work a little bit more like a database, enabling different users / processes to safely read and write concurrently, while still only using object storage as a persistence layer.

Another motivation for Icechunk was the success of [Kerchunk](https://github.com/fsspec/kerchunk/).
The Kerchunk project showed that it was possible to map many existing archival formats (e.g. HDF5, NetCDF, GRIB) to the Zarr data model without actually rewriting any bytes, by creating "virtual" Zarr datasets referencing binary chunks inside other files.
Doing this at scale requires tracking millions of "chunk references."
Icechunk's storage model enables allows for these virtual chunks to be stored seamlessly alongside native Zarr chunks.

Finally, Icechunk provides a universal I/O layer for cloud object storage, implementing numerous performance optimizations designed to accelerate data-intensive applications.

Solving these problems in one go via a powerful, open-source, Rust-based library will bring massive benefits
to the cloud-native scientific data community.

## When should I use Icechunk?

Here are the scenarios where it makes sense to use Icechunk:

- You want to store large, dynamically evolving multi-dimensional array (a.k.a. tensor) in cloud object storage.
- You want to allow multiple uncoordinated processes to access your data at the same time (like a database).
- You want to be able to safely roll back failed updates or revert Zarr data to an earlier state.
- You want to use concepts from data version control (e.g. tagging, branching, snapshots) with Zarr data.
- You want to achieve cloud-native performance on archival file formats (HDF5, NetCDF, GRIB) by exposing them as virtual Zarr datasets and need to store chunk references in a a robust, scalable, interoperable way.
- You want to get the best possible performance for reading / writing tensor data in AI / ML workflows.

## What are the downsides to using Icechunk?

As with all things in technology, the benefits of Icechunk come with some tradeoffs:

- There may be slightly higher cold-start latency to opening Icechunk datasets compared with regular Zarr.
- The on-disk format is less transparent than regular Zarr.
- The process for distributed writes is more complex to coordinate.

!!! warning 
    Another downside of Icechunk in its current state is its immaturity.
    The library is very new, likely contains bugs, and is not recommended
    for production usage at this point in time.


## What is Icechunk's relationship to Zarr?

The Zarr format and protocol is agnostic to the underlying storage system ("store" in Zarr terminology)
and communicates with the store via a simple key / value interface.
Zarr tells the store which keys and values it wants to get or set, and it's the store's job
to figure out how to persist or retrieve the required bytes.

Most existing Zarr stores have a simple 1:1 mapping between Zarr's keys and the underlying file / object names.
For example, if Zarr asks for a key call `myarray/c/0/0`, the store may just look up a key of the same name
in an underlying cloud object storage bucket.

Icechunk is a storage engine designed specifically for Zarr which creates a layer of indirection between the
Zarr keys and the actual files in storage.
A Zarr library doesn't have to know explicitly how Icechunk works or how it's storing data on disk.
It just gets / sets keys as it would with any store.
Icechunk figures out how to materialize these keys based on its [storage schema](./spec.md).

```mermaid
flowchart TD
    zarr-python[Zarr Library] <-- key / value--> icechunk[Icechunk Library]
    icechunk <-- data / metadata files --> storage[(Object Storage)]
```

Implementing Icechunk this way allows Icechunk's specification to evolve independently from Zarr's,
maintaining interoperability while enabling rapid iteration and promoting innovation on the I/O layer.

## Is Icechunk part of the Zarr Spec?

No. At the moment, the Icechunk spec is completely independent of the Zarr spec.

In the future, we may choose to propose Icechunk as a Zarr extension.
However, because it sits _below_ Zarr in the stack, it's not immediately clear how to do that. 

## Should I implement Icechunk on my own based on the spec?

No, we do not recommend implementing Icechunk independently of the existing Rust library.
There are two reasons for this:

1. The spec has not yet been stabilized and is still evolving rapidly.
1. It's probably much easier to bind to the Rust library from your language of choice,
   rather than re-implement from scratch.

We welcome contributions from folks interested in developing Icechunk bindings for other languages!

## Is Icechunk stable?

The Icechunk library is reasonably well-tested and performant.
The Rust-based core library provides a solid foundation of correctness, safety, and speed.

However, the actual on disk format is still evolving and may change from one release to the next.
Until Icechunk reaches v1.0, we can't commit to long-term stability of the on-disk format.
This means Icechunk can't yet be used for production uses which require long-term persistence of data.

😅 Don't worry! We are working as fast as we can and aim to release v1.0 as soon as possible!

## Is Icechunk fast?

We have not yet begun the process of optimizing Icechunk for performance.
Our focus so far has been on correctness and delivering the features needed for full interoperability with Zarr and Xarray.

However, preliminary investigations indicate that Icechunk is at least as fast as the existing Zarr / Dask / fsspec stack
and in many cases achieves significantly lower latency and higher throughput.
Furthermore, Icechunk achieves this without using Dask, by implementing its own asynchronous multithreaded I/O pipeline.

## Does Icechunk use Apache Arrow?

## How does Icechunk compare to X?

- Array Formats
  - HDF
  - NetCDF
  - Cloud Optimized GeoTiff (CoG)
  - TileDB
  - SafeTensors
- Tabular Formats
  - Apache Parquet
  - Apache Iceberg
  - Delta Lake
  - LanceDB
- Other Related projects
  - LakeFS
  - Kerchunk
  - fsspec
  - OCTDB
