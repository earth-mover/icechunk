# Icechunk Version Policy

There are three versions referred to in this document:

1. Icechunk Spec Version
2. `icechunk` Python Library Version
3. `icechunk` Rust Crate Version

## Icechunk Spec

The Icechunk format specification versions are identified by a single integer. The spec version will increment with any revisions as defined in the [Spec document](./spec.md).

## `icechunk` Python Library

Versions of this library are identified by a triplet of integers with the form `<major>.<minor>.<patch>`.

### Major Versions

The `icechunk` Python library major version will increment when and only when there is an on-disk incompatible change in the Icechunk Spec.

#### Reading

The latest release of `icechunk` Python will always be able to read data written against any previous version of the Icechunk Spec.

#### Writing

`icechunk` Python will be able to, at minimum, write to the last major version of the Icechunk Spec. This means that `icechunk` Python 2 can write to Icechunk Spec version 1 format. `icechunk` Python 3 will be able write to Icechunk Spec 2, but not Icechunk Spec 1.

##### Forward Compatibility

`icechunk` Python will **not** implement forward compatible reading and writing. For example `icechunk` Python version 1.x.x will not be able to read or write data using the Icechunk format 2 (or later).

## Minor Versions

Minor releases will require at most minor effort from users to update their code. Minor versions will increment with new features, significant bug fixes, or for small changes in python library api compatibility.

## Patch Versions

Patch releases will require no effort on the part of users of `icechunk` Python. They will contain bugfixes or documentation improvements.

## `icechunk` Rust Crate

No explicit versioning scheme followed here.
