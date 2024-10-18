---
title: Frequently Asked Questions 
---

# FAQ

## Why was Icechunk created?

Icechunk was created by [Earthmover](https://earthmover.io/) as the open-source format for its cloud data platform [Arraylake](https://docs.earthmover.io).

Icechunk builds on the successful [Zarr](https://zarr.dev) project.
Zarr is a great foundation for storing and querying large multidimensional array data in a flexible, scalable way.
But when people started using Zarr together with cloud object storage in a collaborative way, it became clear that Zarr alone could not offer the sort of consistency many users desired.
Icechunk makes Zarr work a little bit more like a database, enabling different users / processes to safely read and write concurrently, while still only using object storage as a persistence layer.

Another motivation for Icechunk was the success of [Kerchunk](https://github.com/fsspec/kerchunk/).
The Kerchunk project showed that it was possible to map many existing archival formats (e.g. HDF5, NetCDF, GRIB) to the Zarr data model without actually rewriting any bytes, by creating "virtual" Zarr datasets referencing binary chunks inside other files.
Doing this at scale requires tracking millions of "chunk references."
Icechunk's storage model allows for these virtual chunks to be stored seamlessly alongside native Zarr chunks.

Finally, Icechunk provides a universal I/O layer for cloud object storage, implementing numerous performance optimizations designed to accelerate data-intensive applications.

Solving these problems in one go via a powerful, open-source, Rust-based library will bring massive benefits
to the cloud-native scientific data community.

## Where does the name "Icechunk" come from?

Icechunk was inspired partly by [Apache Iceberg](https://iceberg.apache.org/), a popular cloud-native table format.
However, instead of storing tabular data, Icechunk stores multidimensional arrays, for which the individual unit of
storage is the _chunk_.

## When should I use Icechunk?

Here are some scenarios where it makes sense to use Icechunk:

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

Icechunk is a storage engine which creates a layer of indirection between the
Zarr keys and the actual files in storage.
A Zarr library doesn't have to know explicitly how Icechunk works or how it's storing data on disk.
It just gets / sets keys as it would with any store.
Icechunk figures out how to materialize these keys based on its [storage schema](./spec.md).

<div class="grid cards" markdown>

-   __Standard Zarr + Fsspec__

    ---

    In standard Zarr usage (without Icechunk), [fsspec](https://filesystem-spec.readthedocs.io/) sits
    between the Zarr library and the object store, translating Zarr keys directly to object store keys.

    ```mermaid
    flowchart TD
        zarr-python[Zarr Library] <-- key / value--> icechunk[fsspec]
        icechunk <-- key / value --> storage[(Object Storage)]
    ```

-   __Zarr + Icechunk__

    ---

    With Icechunk, the Icechunk library intercepts the Zarr keys and translates them to the
    Icechunk schema, storing data in object storage using its own format. 

    ```mermaid
    flowchart TD
        zarr-python[Zarr Library] <-- key / value--> icechunk[Icechunk Library]
        icechunk <-- icechunk data / metadata files --> storage[(Object Storage)]
    ```

</div>


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

However, the actual on disk format is still evolving and may change from one alpha release to the next.
Until Icechunk reaches v1.0, we can't commit to long-term stability of the on-disk format.
This means Icechunk can't yet be used for production uses which require long-term persistence of data.

😅 Don't worry! We are working as fast as we can and aim to release v1.0 soon!

## Is Icechunk fast?

We have not yet begun the process of optimizing Icechunk for performance.
Our focus so far has been on correctness and delivering the features needed for full interoperability with Zarr and Xarray.

However, preliminary investigations indicate that Icechunk is at least as fast as the existing Zarr / Dask / fsspec stack
and in many cases achieves significantly lower latency and higher throughput.
Furthermore, Icechunk achieves this without using Dask, by implementing its own asynchronous multithreaded I/O pipeline.

## How does Icechunk compare to X?

### Array Formats

Array formats are file formats for storing multi-dimensional array (tensor) data.
Icechunk is an array format.
Here is how Icechunk compares to other popular array formats.

#### [HDF5](https://www.hdfgroup.org/solutions/hdf5/)

HDF5 (Hierarchical Data Format version 5) is a popular format for storing scientific data.
HDF is widely used in high-performance computing.

<div class="grid cards" markdown>

-   __Similarities__
    
    ---
    
    Icechunk and HDF5 share the same data model: multidimensional arrays and metadata organized into a hierarchical tree structure.
    This data model can accommodate a wide range of different use cases and workflows.

    Both Icechunk and HDF5 use the concept of "chunking" to split large arrays into smaller storage units.

-   __Differences__
    
    ---
  
    HDF5 is a monolithic file format designed first and foremost for POSIX filesystems.
    All of the chunks in an HDF5 dataset live within a single file.
    The size of an HDF5 dataset is limited to the size of a single file.
    HDF5 relies on the filesystem for consistency and is not designed for multiple concurrent yet uncoordinated readers and writers.
    
    Icechunk spreads chunks over many files and is designed first and foremost for cloud object storage.
    Icechunk can accommodate datasets of arbitrary size.
    Icechunk's optimistic concurrency design allows for safe concurrent access for uncoordinated readers and writers.

</div>

#### [NetCDF](https://www.unidata.ucar.edu/software/netcdf/)

> NetCDF (Network Common Data Form) is a set of software libraries and machine-independent data formats that support the creation, access, and sharing of array-oriented scientific data.

NetCDF4 uses HDF5 as its underlying file format.
Therefore, the similarities and differences with Icechunk are fundamentally the same.

Icechunk can accommodate the NetCDF data model.
It's possible to write NetCDF compliant data in Icechunk using [Xarray](https://xarray.dev/).

#### [Zarr](https://zarr.dev)

Icechunk works together with Zarr.
(See [What is Icechunk's relationship to Zarr?](#what-is-icechunks-relationship-to-zarr) for more detail.)

Compared to regular Zarr (without Icechunk), Icechunk offers many benefits, including

- Serializable isolation of updates via transactions
- Data version control (snapshots, branches, tags)
- Ability to store references to chunks in external datasets (HDF5, NetCDF, GRIB, etc.)
- A Rust-optimized I/O layer for communicating with object storage

#### [Cloud Optimized GeoTiff](http://cogeo.org/) (CoG)

> A Cloud Optimized GeoTIFF (COG) is a regular GeoTIFF file, aimed at being hosted on a HTTP file server, with an internal organization that enables more efficient workflows on the cloud.
> It does this by leveraging the ability of clients issuing ​HTTP GET range requests to ask for just the parts of a file they need.

CoG has become very popular in the geospatial community as a cloud-native format for raster data.
A CoG file contains a single image (possibly with multiple bands), sharded into chunks of an appropriate size.
A CoG also contains "overviews," lower resolution versions of the same data.
Finally, a CoG contains relevant geospatial metadata regarding projection, CRS, etc. which allow georeferencing of the data.

Data identical to what is found in a CoG can be stored in the Zarr data model and therefore in an Icechunk repo.
Furthermore, Zarr / Icechunk can accommodate rasters of arbitrarily large size and facilitate massive-scale concurrent writes (in addition to reads);
A CoG, in contrast, is limited to a single file and thus has limitations on scale and write concurrency.

However, Zarr and Icechunk currently do not offer the same level of broad geospatial interoperability that CoG does.
The [GeoZarr](https://github.com/zarr-developers/geozarr-spec) project aims to change that.

#### [TileDB Embedded](https://docs.tiledb.com/main/background/key-concepts-and-data-format)

TileDB Embedded is an innovative array storage format that bears many similarities to both Zarr and Icechunk.
Like TileDB Embedded, Icechunk aims to provide database-style features on top of the array data model.
Both technologies use an embedded / serverless architecture, where client processes interact directly with
data files in storage, rather than through a database server.
However, there are a number of difference, enumerated below.

The following table compares Zarr + Icechunk with TileDB Embedded in a few key areas

| feature | **Zarr + Icechunk** | **TileDB Embedded** | Comment |
|---------|---------------------|---------------------|---------|
| *atomicity* | atomic updates can span multiple arrays and groups | _array fragments_ limited to a single array  |  Icechunk's model allows a writer to stage many updates across interrelated arrays into a single transaction. |
| *concurrency and isolation* | serializable isolation of transactions | [eventual consistency](https://docs.tiledb.com/main/background/internal-mechanics/consistency) | While both formats enable lock-free concurrent reading and writing, Icechunk can catch (and potentially reject) inconsistent, out-of order updates. |
| *versioning* | snapshots, branches, tags | linear version history | Icechunk's data versioning model is closer to Git's. |
| *unit of storage* | chunk | tile | (basically the same thing) |
| *minimum write* | chunk | cell | TileDB allows atomic updates to individual cells, while Zarr requires writing an entire chunk. |
| *sparse arrays* | :material-close: | :material-check: | Zarr + Icechunk do not currently support sparse arrays. |
| *virtual chunk references* |  :material-check: |  :material-close: | Icechunk enables references to chunks in other file formats (HDF5, NetCDF, GRIB, etc.), while TileDB does not. |

Beyond this list, there are numerous differences in the design, file layout, and implementation of Icechunk and TileDB embedded
which may lead to differences in suitability and performance for different workfloads.

#### [SafeTensors](https://github.com/huggingface/safetensors)

SafeTensors is a format developed by HuggingFace for storing tensors (arrays) safely, in contrast to Python pickle objects.

By the same criteria Icechunk and Zarr are also "safe", in that it is impossible to trigger arbitrary code execution when reading data.

SafeTensors is a single-file format, like HDF5,
SafeTensors optimizes for a simple on-disk layout that facilitates mem-map-based zero-copy reading in ML training pipelines,
assuming that the data are being read from a local POSIX filesystem
Zarr and Icechunk instead allow for flexible chunking and compression to optimize I/O against object storage.

### Tabular Formats

Tabular formats are for storing tabular data.
Tabular formats are extremely prevalent in general-purpose data analytics but are less widely used in scientific domains. 
The tabular data model is different from Icechunk's multidimensional array data model, and so a direct comparison is not always apt.
However, Icechunk is inspired by many tabular file formats, and there are some notable similarities.

#### [Apache Parquet](https://parquet.apache.org/)
  
> Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.
> It provides high performance compression and encoding schemes to handle complex data in bulk and is supported in many programming language and analytics tools.

Parquet employs many of the same core technological concepts used in Zarr + Icechunk such as chunking, compression, and efficient metadata access in a cloud context.
Both formats support a range of different numerical data types.
Both are "columnar" in the sense that different columns / variables / arrays can be queried efficiently without having to fetch unwanted data from other columns.
Both also support attaching arbitrary key-value metadata to variables.
Parquet supports "nested" types like variable-length lists, dicts, etc. that are currently unsupported in Zarr (but may be possible in the future).

In general, Parquet and other tabular formats can't be substituted for Zarr / Icechunk, due to the lack of multidimensional array support.
On the other hand, tabular data can be modeled in Zarr / Icechunk in a relatively straightforward way: each column as a 1D array, and a table / dataframe as a group of same-sized 1D arrays.

#### [Apache Iceberg](https://iceberg.apache.org/)

> Iceberg is a high-performance format for huge analytic tables.
> Iceberg brings the reliability and simplicity of SQL tables to big data, while making it possible for engines like Spark, Trino, Flink, Presto, Hive and Impala to safely work with the same tables, at the same time.

Iceberg is commonly used to manage many Parquet files as a single table in object storage.

Iceberg was influential in the design of Icechunk.
Many of the [spec](./spec.md) core requirements are similar to Iceberg.
Specifically, both formats share the following properties:

- Files written to object storage immutably
- All data and metadata files are tracked explicitly by manifests
- Similar mechanism for staging snapshots and committing transactions
- Support for branches and tags

However, unlike Iceberg, Icechunk _does not require an external catalog_ to commit transactions; it relies solely on the consistency of the object store.

#### [Delta Lake](https://delta.io/)

Delta is another popular table format based on a log of updates to the table state.
Its functionality and design is quite similar to Iceberg, as is its comparison to Icechunk.

#### [Lance](https://lancedb.github.io/lance/index.html)

> Lance is a modern columnar data format that is optimized for ML workflows and datasets.

Despite its focus on multimodal data, as a columnar format, Lance can't accommodate large arrays / tensors chunked over arbitrary dimensions, making it fundamentally different from Icechunk.

However, the modern design of Lance was very influential on Icechunk.
Icechunk's commit and conflict resolution mechanism is partly inspired by Lance.

### Other Related projects

#### [Xarray](https://xarray.dev/)

> Xarray is an open source project and Python package that introduces labels in the form of dimensions, coordinates, and attributes on top of raw NumPy-like arrays, which allows for more intuitive, more concise, and less error-prone user experience.
>
> Xarray includes a large and growing library of domain-agnostic functions for advanced analytics and visualization with these data structures.

Xarray and Zarr / Icechunk work great together!
Xarray is the recommended way to read and write Icechunk data for Python users in geospatial, weather, climate, and similar domains.

#### [Kerchunk](https://fsspec.github.io/kerchunk/)

> Kerchunk is a library that provides a unified way to represent a variety of chunked, compressed data formats (e.g. NetCDF/HDF5, GRIB2, TIFF, …), allowing efficient access to the data from traditional file systems or cloud object storage.
> It also provides a flexible way to create virtual datasets from multiple files. It does this by extracting the byte ranges, compression information and other information about the data and storing this metadata in a new, separate object.
> This means that you can create a virtual aggregate dataset over potentially many source files, for efficient, parallel and cloud-friendly in-situ access without having to copy or translate the originals.
> It is a gateway to in-the-cloud massive data processing while the data providers still insist on using legacy formats for archival storage

Kerchunk emerged from the [Pangeo](https://www.pangeo.io/) community as an experimental
way of reading archival files, allowing those files to be accessed "virtually" using the Zarr protocol.
Kerchunk pioneered the concept of a "chunk manifest", a file containing references to compressed binary chunks in other files in the form of the tuple `(uri, offset, size)`.
Kerchunk has experimented with different ways of serializing chunk manifests, including JSON and Parquet.

Icechunk provides a highly efficient and scalable mechanism for storing and tracking the references generated by Kerchunk.
Kerchunk and Icechunk are highly complimentary.

#### [VirtualiZarr](https://virtualizarr.readthedocs.io/en/latest/)

> VirtualiZarr creates virtual Zarr stores for cloud-friendly access to archival data, using familiar Xarray syntax.

VirtualiZarr provides another way of generating and manipulating Kerchunk-style references.
VirtualiZarr first uses Kerchunk to generate virtual references, but then provides a simple Xarray-based interface for manipulating those references. 
As VirtualiZarr can also write virtual references into an Icechunk Store directly, together they form a complete pipeline for generating and storing references to multiple pre-existing files.

#### [LakeFS](https://lakefs.io/)

LakeFS is a solution git-style version control on top of cloud object storage.
LakeFS enables git-style commits, tags, and branches representing the state of an entire object storage bucket.

LakeFS is format agnostic and can accommodate any type of data, including Zarr.
LakeFS can therefore be used to create a versioned Zarr store, similar to Icechunk.

Icechunk, however, is designed specifically for array data, based on the Zarr data model.
This specialization enables numerous optimizations and user-experience enhancements not possible with LakeFS.

LakeFS also requires a server to operate.
Icechunk, in contrast, works with just object storage.

#### [TensorStore](https://google.github.io/tensorstore/index.html)

> TensorStore is a library for efficiently reading and writing large multi-dimensional arrays.

TensorStore can read and write a variety of different array formats, including Zarr.

While TensorStore is not yet compatible with Icechunk, it should be possible to implement Icechunk support in TensorStore.

TensorStore implements an [ocdbt](https://google.github.io/tensorstore/kvstore/ocdbt/index.html#ocdbt-key-value-store-driver):

> The ocdbt driver implements an Optionally-Cooperative Distributed B+Tree (OCDBT) on top of a base key-value store.

Ocdbt implements a transactional, versioned key-value store suitable for storing Zarr data, thereby supporting some of the same features as Icechunk.
Unlike Icechunk, the ocdbt key-value store is not specialized to Zarr, does not differentiate between chunk or metadata keys, and does not store any metadata about chunks.


