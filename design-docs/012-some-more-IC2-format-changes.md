# More format changes in IC2

Icechunk library version 2.0 will support IC1 format repos. This means that
the following changes to the on disk format apply only to IC2 repositories.
IC1 repos shouldn't be affected. The Icechunk 2 library will keep writing
to IC1-format repos using the existing IC1-spec format.

There will be repos that have a mix of certain objects in IC1 format and
others (more recent ones) in IC2 format. This happens because the migration from
IC1 to IC2 format doesn't rewrite all objects in the repo. This is all good,
expected, and even documented behavior.

We list the remaining format changes we need, with brief design summaries and
links to longer design documents when needed.

## Allow rectilinear chunk grids

We would like to implement this feature in version 2, but even if we don't get
to it we certainly want the IC2 format to support it.

## Feature flags

We want to add to the format the ability to soft-block the repo from executing
certain operations.

See [design document](./013-feature-flags.md).

## More efficient virtual chunk references

Currently the manifest stores a full absolute URL per virtual chunk. This is
inefficient in terms of memory once the manifest is decompressed, and it's
inflexible under chunks movement.

See [design document](./014-virtual-chunk-ref-efficiency.md).

## Extra data in manifests and snapshots

We have several candidates to add "extra information" to snapshots,
manifests and manifest refs. We currently don't have a natural place to put this
information. Examples are statistics, pre computed quantities, and others.

This could also give us more flexibility as a way to create new features without
having to change the format.
