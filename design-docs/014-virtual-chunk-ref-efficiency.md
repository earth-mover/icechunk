# More efficient virtual chunk references

Currently the manifest stores a full absolute URL per virtual chunk. This is
inefficient in terms of memory once the manifest is decompressed, and it's
inflexible under chunks movement.

## Design

### Virtual Chunk Container (VCC) names

We re introduce virtual chunk container names. We had them at the beginning,
but we drop them for security reasons. Most of the code still exists.
Authorization of VCCs will still be based on prefixes and not names, to avoid
security issues. VCC names will be used as a way to refer to a VCC in manifests.
So VCC names must be unique per repo (except when name is not present).

To maintain compatibility, we still allow null names, but those VCCs without
name cannot be used for relative virtual chunk resolution.

### Reinterpretation of `ChunkRef::location`

Now, the `location` field in `ChunkRef` can be a url with protocol `vcc://`.
For this URLs, the hostname part will be interpreted as the name of a VCC.
The rest of the url (path) will be interpreted as relative to the `prefix`
in the matching VCC.

For "normal" location urls, those not having protocol `vcc://` like,
for example, `s3://bucket/prefix/foo.nc`, the interpretation is
unchanged, this is an absolute url to the chunk, and it must much one
of the defined VCC prefixes.

As an example, if the repo has a VCC like

```yaml
  s3:
    name: my-virtual-icechunk
    url_prefix: s3://testbucket/my-repo/chunks
    store: !s3_compatible
      region: us-east-1
      anonymous: false
```

A valid `ChunkRef` could have location: `vcc://my-virtual-icechunk/4K2JE645QXEXJ8BFDX70`.
This would resolve to an object in the S3 compatible store, with bucket name
`testbucket` and key `my-repo/chunks/4K2JE645QXEXJ8BFDX70`.

Hostnames in urls with protocol `vcc` must much the name of a VCC as defined in
the repo configuration, and if it doesn't those relative chunk refs
will generate an error when dereferenced.

### Compression of `location` field

The `location` field in `ChunkRef` will be compressed using
[Zstandart dictionary compression](https://facebook.github.io/zstd/#small-data),
globally across each whole manifest. Other compression options are available
but Icechunk already uses zstd.

To train the compression dictionary a small random number of references
in the manifest will be used at manifest write time. The compression level,
size of the dictionary, and number of training samples can be configured
in the repository config. Default values will be obtained experimenting
with real world manifets. We expect to use hundreds of samples and a few
KB as dictionary size.

Compression will be turned on by default only if we verify it's impact
in manifest generation time is minimal in real world scenarios.

Ideally compression parameters should be enabled on an array per array
basis, as we do with preloading or splitting.

To store the compression dictionary and the compressed location we add in
the manifest flatbuffer:

```flatbuffers
table Manifest {
  // existing field
  id: ObjectId12 (required);

  // existing field
  arrays: [ArrayManifest] (required);

  // NEW OPTIONAL FIELD
  location_dictionary: [uint8]
}

table ChunkRef {
  ...

  // existing field
  location: string;

  // NEW OPTIONAL FIELD
  compressed_location: [uint8]
}
```

Notice in the end, this will be zstd compression of zstd data, because the full
manifest is compressed with zstd before writing it. We should evaluate if the
parameters of this second compression should change.

### Changes to the API

- No important changes are needed.
- We may add some flags to help integration with VirtualiZarr.
- We may add some usability methods, like listing VCC names, etc.

### Changes to the implementation

Beyond the format changes, we have to:

- Compression, uncompression and configuration
- Make virtual chunk resolution work for both absolute and relative locations
- Make `Store::set_virtual_ref` use relative locations for IC2 repositories, if
there is a matching VCC. Potentially we may want to add explicit flags to force
use of relative or absolute urls only.
- Make `Session.all_virtual_chunk_locations` work for absolute and relative
locations
- Review tests and error messages to use the right combination of relative and
absolute locations.
