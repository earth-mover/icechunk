---
title: Concepts
---
# Icechunk Concepts

## What is Icechunk?

Icechunk is an open-source (Apache 2.0), transactional storage engine for tensor / ND-array data designed for use on cloud object storage.
Icechunk works together with **[Zarr](https://zarr.dev/)**, augmenting the Zarr core data model with features
that enhance performance, collaboration, and safety in a cloud-computing context.

Let's break down what "transactional storage engine for Zarr" actually means:

- **[Zarr](https://zarr.dev/)** is an open source specification for the storage of multidimensional array (a.k.a. tensor) data.
  Zarr defines the metadata for describing arrays (shape, dtype, etc.) and the way these arrays are chunked, compressed, and converted to raw bytes for storage. Zarr can store its data in any key-value store.
  There are many different implementations of Zarr in different languages. _Right now, Icechunk only supports
  [Zarr Python](https://zarr.readthedocs.io/en/stable/)._
  If you're interested in implementing Icechunk support, please [open an issue](https://github.com/earth-mover/icechunk/issues) so we can help you.
- **Storage engine** - Icechunk exposes a key-value interface to Zarr and manages all of the actual I/O for getting, setting, and updating both metadata and chunk data in cloud object storage.
  Zarr libraries don't have to know exactly how icechunk works under the hood in order to use it.
- **Transactional** - The key improvement that Icechunk brings on top of regular Zarr is to provide consistent serializable isolation between transactions.
  This means that Icechunk data are safe to read and write in parallel from multiple uncoordinated processes.
  This allows Zarr to be used more like a database.

The core entity in Icechunk is a repository or **repo**.
A repo is defined as a Zarr hierarchy containing one or more Arrays and Groups, and a repo functions as
self-contained _Zarr Store_.
The most common scenario is for an Icechunk repo to contain a single Zarr group with multiple arrays, each corresponding to different physical variables but sharing common spatiotemporal coordinates.
However, formally a repo can be any valid Zarr hierarchy, from a single Array to a deeply nested structure of Groups and Arrays.
Users of Icechunk should aim to scope their repos only to related arrays and groups that require consistent transactional updates.

Icechunk supports the following core requirements:

1. **Object storage** - the format is designed around the consistency features and performance characteristics available in modern cloud object storage. No external database or catalog is required to maintain a repo.
(It also works with file storage.)
1. **Serializable isolation** - Reads are isolated from concurrent writes and always use a committed snapshot of a repo. Writes are committed atomically and are never partially visible. No locks are required for reading.
1. **Time travel** - Previous snapshots of a repo remain accessible while and after new snapshots are written.
1. **Data version control** - Repos support both _tags_ (immutable references to snapshots) and _branches_ (mutable references to snapshots).
1. **Chunk shardings** - Chunk storage is decoupled from specific file names. Multiple chunks can be packed into a single object (sharding).
1. **Chunk references** - Zarr-compatible chunks within other file formats (e.g. HDF5, NetCDF) can be referenced.
1. **Schema evolution** - Arrays and Groups can be added, renamed, and removed from the hierarchy with minimal overhead.

## Key Concepts

### Groups, Arrays, and Chunks

Icechunk is designed around the Zarr data model, widely used in scientific computing, data science, and AI / ML.
(The Zarr high-level data model is effectively the same as HDF5.)
The core data structure in this data model is the **array**.
Arrays have two fundamental properties:

- **shape** - a tuple of integers which specify the dimensions of each axis of the array. A 10 x 10 square array would have shape (10, 10)
- **data type** - a specification of what type of data is found in each element, e.g. integer, float, etc. Different data types have different precision (e.g. 16-bit integer, 64-bit float, etc.)

In Zarr / Icechunk, arrays are split into **chunks**,
A chunk is the minimum unit of data that must be read / written from storage, and thus choices about chunking have strong implications for performance.
Zarr leaves this completely up to the user.
Chunk shape should be chosen based on the anticipated data access pattern for each array
An Icechunk array is not bounded by an individual file and is effectively unlimited in size.

For further organization of data, Icechunk supports **groups** within a single repo.
Group are like folders which contain multiple arrays and or other groups.
Groups enable data to be organized into hierarchical trees.
A common usage pattern is to store multiple arrays in a group representing a NetCDF-style dataset.

Arbitrary JSON-style key-value metadata can be attached to both arrays and groups.

### Transactions

A **transaction** is a group of related operations that are treated as a single unit of work. Think of it like a batch of changes that either all succeed together or all fail together—there's no in-between state.

In everyday terms, a transaction is like making multiple edits to a document and then clicking "Save". While you're editing, nobody else sees your changes. Only when you click "Save" do all your edits become visible to others at once. If your computer crashes before you save, none of your edits are applied—you don't end up with half-edited content.

#### Example: Updating a Weather Forecast

Imagine you're updating a weather forecast dataset that contains multiple related variables:

```python
with repo.transaction("main", message="Add 6-hour forecast") as store:
    group = zarr.open_group(store)

    # All these operations happen together as one transaction
    group["temperature"][:, 6] = temp_forecast
    group["pressure"][:, 6] = pressure_forecast
    group["humidity"][:, 6] = humidity_forecast
    group["wind_speed"][:, 6] = wind_forecast

# Transaction commits here - all changes become visible at once
```

While the transaction is running (inside the `with` block), these changes exist in a kind of "scratch space" that only you can see. Other users reading the data see the previous committed version. When the transaction completes successfully:

- All four variables get updated together
- A new snapshot is created with all changes
- Other users can now see the complete update

If something goes wrong (power outage, bug in your code, disk full), the transaction fails and none of the changes are applied. You never end up with temperature updated but pressure missing—the data stays in its previous valid state.

#### Why Transactions Matter

Without transactions, file-based formats typically write each piece of data independently:

```python
# Without transactions (regular Zarr/NetCDF/HDF5)
group["temperature"][:, 6] = temp_forecast      # Written immediately to disk
group["pressure"][:, 6] = pressure_forecast     # Written immediately to disk
# ... computer crashes here ...
group["humidity"][:, 6] = humidity_forecast     # Never gets written!
```

Now your dataset has temperature and pressure for hour 6, but no humidity or wind speed. Anyone reading this data will get inconsistent results, and there's no automatic way to detect or fix this problem.

Transactions solve this by making all the writes atomic—they happen together or not at all. This is the foundation that makes Icechunk reliable enough to use as a database.

### ACID Transactions

Now that we understand what transactions are, let's explore **ACID transactions**—a specific set of guarantees that make transactions truly reliable. ACID is an acronym that describes four critical properties:

**Atomicity** - "All or nothing"

Imagine you're updating a weather forecast dataset with 10 different variables (temperature, pressure, humidity, etc.). With atomicity, either all 10 variables get updated successfully, or none of them do. You'll never end up in a broken state where 7 variables are updated but 3 failed—leaving your dataset internally inconsistent.

```python
# This entire block either succeeds completely or fails completely
with repo.transaction("main", message="Update forecast") as store:
    group = zarr.open_group(store)
    group["temperature"][:] = new_temp_data
    group["pressure"][:] = new_pressure_data
    group["humidity"][:] = new_humidity_data
    # If any write fails, none of the changes are saved
```

**Consistency** - "Data stays valid"

Your data has rules—for example, all arrays in a forecast must have the same time dimension. Consistency ensures that every transaction takes your data from one valid state to another valid state. You can't accidentally commit changes that violate your data's fundamental structure.

**Isolation** - "No interference"

Multiple data scientists can work with the same Icechunk repository simultaneously without interfering with each other. When you're reading data, you see a stable snapshot—even if someone else is writing updates at the exact same moment. Your analysis won't suddenly break because someone committed new data halfway through your computation.

```python
# Reader sees a consistent snapshot, unaffected by concurrent writers
session = repo.readonly_session("main")
data = zarr.open_group(session.store)["temperature"][:]
# This data won't change even if someone commits updates right now
```

**Durability** - "Commits are permanent"

Once your transaction is committed, it's saved permanently. Even if your computer crashes immediately after committing, your data is safe. The commit won't be lost or partially applied.

#### Why ACID Matters for Array Data

Traditional file-based workflows (like regular Zarr, NetCDF, or HDF5) don't provide ACID guarantees. This creates serious problems:

- **Partial writes**: A process crashes while updating multiple arrays, leaving some updated and others not
- **Read inconsistency**: An analysis reads data while another process is writing, getting a mix of old and new values
- **Lost updates**: Two processes try to update data simultaneously, and one person's changes get silently overwritten
- **Unrecoverable errors**: Someone accidentally corrupts data and there's no way to undo it

Icechunk's ACID transactions solve all of these problems, making array data as reliable as a traditional database. This is essential for production systems where data correctness isn't optional—like weather forecasting, financial modeling, or scientific research that needs to be reproducible.

### Snapshots

Every update to an Icechunk store creates a new **snapshot** with a unique ID.
Icechunk users must organize their updates into groups of related operations called **transactions**.
For example, appending a new time slice to multiple arrays should be done as a single transaction, comprising the following steps

1. Update the array metadata to resize the array to accommodate the new elements.
2. Write new chunks for each array in the group.

While the transaction is in progress, none of these changes will be visible to other users of the store.
Once the transaction is committed, a new snapshot is generated.
Readers can only see and use committed snapshots.

### Branches and Tags

Additionally, snapshots occur in a specific linear (i.e. serializable) order within a  **branch**.
A branch is a mutable reference to a snapshot: a pointer that maps the branch name to a snapshot ID.
The default branch is `main`.
Every commit to the main branch updates this reference.
Icechunk's design protects against the race condition in which two uncoordinated sessions attempt to update the branch at the same time; only one can succeed.

Icechunk also defines **tags**--_immutable_ references to a snapshot.
Tags are appropriate for publishing specific releases of a repository or for any application which requires a persistent, immutable identifier to the store state.

### Chunk References

Chunk references are "pointers" to chunks that exist in other files--HDF5, NetCDF, GRIB, etc.
Icechunk can store these references alongside native Zarr chunks as "virtual datasets".
You can then update these virtual datasets incrementally (overwrite chunks, change metadata, etc.) without touching the underlying files.

## How Does It Work?

!!! note
    For more detailed explanation, have a look at the [Icechunk spec](./spec.md)

Zarr itself works by storing both metadata and chunk data into a abstract store according to a specified system of "keys".
For example, a 2D Zarr array called `myarray`, within a group called `mygroup`, would generate the following keys:

```
mygroup/zarr.json
mygroup/myarray/zarr.json
mygroup/myarray/c/0/0
mygroup/myarray/c/0/1
```

In standard regular Zarr stores, these key map directly to filenames in a filesystem or object keys in an object storage system.
When writing data, a Zarr implementation will create these keys and populate them with data. When modifying existing arrays or groups, a Zarr implementation will potentially overwrite existing keys with new data.

This is generally not a problem, as long there is only one person or process coordinating access to the data.
However, when multiple uncoordinated readers and writers attempt to access the same Zarr data at the same time, [various consistency problems](https://docs.earthmover.io/concepts/version-control-system#consistency-problems-with-zarr) problems emerge.
These consistency problems can occur in both file storage and object storage; they are particularly severe in a cloud setting where Zarr is being used as an active store for data that are frequently changed while also being read.

With Icechunk, we keep the same core Zarr data model, but add a layer of indirection between the Zarr keys and the on-disk storage.
The Icechunk library translates between the Zarr keys and the actual on-disk data given the particular context of the user's state.
Icechunk defines a series of interconnected metadata and data files that together enable efficient isolated reading and writing of metadata and chunks.
Once written, these files are immutable.
Icechunk keeps track of every single chunk explicitly in a "chunk manifest".

```mermaid
flowchart TD
    zarr-python[Zarr Library] <-- key / value--> icechunk[Icechunk Library]
    icechunk <-- data / metadata files --> storage[(Object Storage)]
```
