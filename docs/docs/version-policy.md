# Icechunk Version Policy


There are three versions referred to in this document:

1. Icechunk Spec Version (file format maybe?)
2. Icechunk-Python library
3. Icechunk-Rust Library


## Icechunk Spec

The spec will advance with any revisions as defined in the [Spec document](./spec.md).

## Icechunk-Python

Versions of this library are identified by a triplet of integers with the form `<major>.<minor>.<patch>`

### Library Version

#### Major Versions

The Icechunk-Python library will change when there is an on-disk incompatible change in the Icechunk-Spec.

##### Reading

The latest release of Icechunk-Python will always be able to read data written against any version of the Icechunk-Spec.

##### Writing

Icechunk-Python will be able to, at minimum, write to the last major version of the Icechunk-Spec. This means that Icechunk-Python  2 can write to Icechunk-Spec version 1 format. Icechunk-Python 3 will be able write to Icehcunk-Spec 2, but not Icechunk-Spec 1.


###### Forward Compatibility

Icechunk-Python will **not** implement forward compatible reading and writing. Icechunk-Python version 1.x.x will not be able to read or write the Icechunk 2 format.

#### Minor Versions

Minor releases will require at most minor effort from users to update their code. Minor versions will increment with new features, significant bug fixes, or for small changes in python library api compatibility.

#### Patch Versions

Patch releases will require no effort on the part of users of Icechunk-Python. They will contain bugfixes or documentation improvements.

### Bug and Security Fixes

When a new major version of Icechunk-Python is published the last minor release of the previous major version will receive bug and security fixes for six months from the date of publishing of the new major version.

### Dependency Policy

Icechunk-Python has only one required dependency [zarr-python](https://zarr.readthedocs.io). Icechunk's functionality requires Zarr-python version 3 at a minimum. Going forward from version 3.0 Icehunk-python will make a best effort to follow SPEC 0 for what versions of core dependencies we allow. Support for core dependencies for 24 months from their initial release.
However, some Icechunk have, and will, require implementing fixes or new features upstream in Zarr-Python. This means that there will necessarily be tighter version constraints than the 24 months defined in Spec-0.

## Icechunk-Rust

Chaos - TODO: describe better
