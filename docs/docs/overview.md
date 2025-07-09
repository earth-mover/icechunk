---
title: Overview
---
# Icechunk

Icechunk is an open-source (Apache 2.0), transactional storage engine for tensor / ND-array data designed for use on cloud object storage.
Icechunk works together with **[Zarr](https://zarr.dev/)**, augmenting the Zarr core data model with features
that enhance performance, collaboration, and safety in a cloud-computing context.

## Docs Organization

This is the Icechunk documentation. It's organized into the following parts.

- This page: a general overview of the project's goals and components.
- [Frequently Asked Questions](./faq.md)
- Documentation for [Icechunk Python](./icechunk-python/quickstart.md), the main user-facing
  library
- Documentation for the [Icechunk Rust Crate](./icechunk-rust.md)
- The [Icechunk Spec](./spec.md)

## Icechunk Overview

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

The core entity in Icechunk is a Repository or **repo**.
A repo is defined as a Zarr hierarchy containing one or more Arrays and Groups, and a repo functions as
self-contained _Zarr Store_.
The most common scenario is for an Icechunk repo to contain a single Zarr group with multiple arrays, each corresponding to different physical variables but sharing common spatiotemporal coordinates.
However, formally a repo can be any valid Zarr hierarchy, from a single Array to a deeply nested structure of Groups and Arrays.
Users of Icechunk should aim to scope their repos only to related arrays and groups that require consistent transactional updates.

## Icechunk Design Requirements

Icechunk supports the following core requirements:

1. **Object storage** - the format is designed around the consistency features and performance characteristics available in modern cloud object storage. No external database or catalog is required to maintain a repo.
(It also works with file storage.)
1. **Serializable isolation** - Reads are isolated from concurrent writes and always use a committed snapshot of a repo. Writes are committed atomically and are never partially visible. No locks are required for reading.
1. **Time travel** - Previous snapshots of a repo remain accessible after new ones have been written.
1. **Data version control** - Repos support both _tags_ (immutable references to snapshots) and _branches_ (mutable references to snapshots).
1. **Chunk shardings** - Chunk storage is decoupled from specific file names. Multiple chunks can be packed into a single object (sharding).
1. **Chunk references** - Zarr-compatible chunks within other file formats (e.g. HDF5, NetCDF) can be referenced.
1. **Schema evolution** - Arrays and Groups can be added, renamed, and removed from the hierarchy with minimal overhead.
