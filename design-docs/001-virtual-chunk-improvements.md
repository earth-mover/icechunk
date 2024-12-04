# Improvements to virtual chunks handling

## What we need

1. Virtual chunks can be stored in all supported object stores
2. Multiple credentials sets for virtual chunk resolution
3. Optimized manifests where the same prefix doesn't need to be repeated for every virtual chunk
4. Be able to know all the "places" pointed by a repo, without having to load every manifest in every version
5. Object level change detection (not serving a virtual chunk if the containing file changed)

## Discussion

### Chunks url styles

* Many chunks from a few objects, the prefix can be the file
  * Example: `s3://some-bucket/some-prefix/some-file.nc` is pointed to by 10k chunks.
  * Having between 10 and 10k chunks from the same file is not unusual
* Virtual zarr: each chunk comes from a different object, the prefix is not the object, we still need an extra string
  * Example: `s3://some-bucket/some-prefix/c/0/0/1` is pointed to by only 1 chunk. But there are many objects like it.
  * There could be tens of millions of chunks (so different urls)

### Prefixes

Storing the full URL for each chunk duplicates a lot of data in the manifest, making it large. The idea is to try to extract common _prefixes_ and identify them with a short id. Then the manifest only needs to identify the prefix id + maybe an extra string + length + offset.

In the first url style above, the prefix could be the full file, `s3://some-bucket/some-prefix/some-file.nc`. In the second example, there are options for the prefix, it could be `s3://some-bucket/some-prefix/c/0/0/1`, or `s3://some-bucket/some-prefix/c/0/0` or `s3://some-bucket/some-prefix/c/0` or `s3://some-bucket/some-prefix/c`, with different numbers of chunks under it in each case.

Intelligence is needed to optimize what is consider a good prefix. If done badly, we could have:

* as many prefixes as chunks, for example by selecting `s3://some-bucket/some-prefix/c/0/0/1` in the second example above
* too large of an extra string in each chunk, for example, if we select `s3://some-bucket` as the prefix each chunk needs to add something like `some-prefix/c/0/0`.

### Should prefixes be per snapshot or global?

The advantage of per snapshot is that we don't need to load/parse all the prefixes if the current snapshot uses only a few. A case for this would be a virtual dataset that has been materialized in a newer version. If prefixes are per-snapshot we wouldn't need to load any prefixes.

On the other hand, requirement 4 asks for the full list of "dependencies" for the dataset, and that means for all versions, which requires a global list of prefixes. Without a global list, we would need to fetch the list of prefixes of each snapshot.

### Who creates/decides the prefixes

Obviously, generating the list of prefixes given the list of virtual chunks is doable automatically. But it's a tricky optimization problem. One strategy could be to generate a trie with all chunk urls, and then use some heuristic to select prefixes using heavily populated nodes. It's tricky and potentially computationally expensive for large repos.

An alternative is to let the user dictate the prefixes. This way, they can optimize them knowing beforehand what virtual chunks they will add. If they don't optimize, they get large manifests without prefix extraction. We still need to satisfy requirements 2 and 4, which would require automatic extraction of at least the bucket and platform.

Additionally, the optimization problem can be treated as an extra process (similar to compaction or garbage collection), and ran offline on demand.

### Templates instead of prefixes

Using templates would be more efficient, example:

* `s3://some-bucket/some-prefix/1-analysis-output-for-model-abc.nc`
* `s3://some-bucket/some-prefix/2-analysis-output-for-model-abc.nc`

If we use the template `s3://some-bucket/some-prefix/{}-analysis-output-for-model-abc.nc` each chunk could have a couple of extra characters only, filling the placeholders in the template.

### Supporting multiple clouds

When the user adds a virtual ref, they need to indicate the details of the platform to retrieve it. These include what cloud it is (so we know how to set up credentials), region, endpoint url, port and other settings. It looks a lot like our `S3Config` type (or an expanded version of that).

### Supporting multiple credentials

Credentials are a read time concern. When the user writes the virtual chunk, they don't necessarily know what credentials set will be used to read it.

Credentials need to be specified later, when the chunks are going to be read. For example, by indicating in runtime configuration a set of credentials for each virtual chunk bucket/prefix/template.

## Design

### Explicit containers for virtual chunks

* Each repo has a global list of `VirtualChunkContainers`

```rust
// this is pseudo-code
struct VirtualChunkContainer {
  name: String,  // something like model-output
  platform: Platform,  // something like S3
  url_template: String, // something like some-bucket/some-prefix/output/{}/{}.nc
  default_template_arguments: Vec<String>,
  cloud_client_config: Map<String, Any>,  // something like {endpoint_url: 8080}
}
```

* Users can add to the list and edit members but not delete.

### Global config

* The list of chunk containers is stored in a new global `Config` object. There is a single configuration per repo, and all reads and writes are done reading the latest configuration. In the initial implementation, the "history" of configuration is present only for reference and user inspection by hand.

TBD: how to store this information. One approach would be using the new PUT if-match S3 functionality, but this would be the first place where we overwrite objects.

```rust
// this is pseudo-code
struct Config {
  inline_chunk_threshold_bytes: u16,
  virtual_chunk_containers: Vec<VirtualChunkContainer>,
}
```

* The `Config` object can act as a configuration default when users don't override. But different fields of configuration may have different behaviors.

### New virtual chunk ref

We change from

```rust
pub struct VirtualChunkRef {
    pub location: VirtualChunkLocation, // this is basically a String for the URL today
    pub offset: ChunkOffset,
    pub length: ChunkLength,
}
```

To

```rust
pub struct VirtualChunkRef {
    pub container_index: u16,
    pub container_template_args: Vec<Option<String>>,
    pub offset: ChunkOffset,
    pub length: ChunkLength,
    pub last_modified: Option<u32>,
}
```

* The `last_modified` field contains an optional unix epoch in seconds. If set for a chunk, retrieval will fail if the file was modified after this time. We use this instead of an ETag or checksum to save space on the manifests. `u32` should be enough until year ~ 2100.

### Reading virtual chunks

* When a user create a repository instance, they can specify credentials for each `VirtualChunkContainer`, pointing to them by name.
* They can also set default `VirtualChunkContainer` credentials, that will be used for any container not specifically set
* If no credentials are specified the same as for the main `Storage` will be used (if platforms are the same)
* On read of a virtual chunk:
  * The `VirtualChunkContainer` is resolved from configuration.
  * An object storage client is instantiated. We'll need a cache here, so we don't keep creating new instances for each chunk
  * The client is initialized with the credentials passed (or defaulted).
  * The container template is expanded with the `template_args` in the virtual chunks. If there are no enough template arguments or some are None, they are replaced by the default template arguments in the container. If there are too many, last ones are ignored.
  * The chunk is fetched from the results URL.

### Writing virtual chunks

* Users must first update the repo configuration to include the needed `VirtualChunkContainers`.
* When the user writes a virtual chunk ref, they pass in the `VirtualChunkRef` the index for the container and the template expansion arguments. An error is produced if the container is not found.
* This is a bit inconvenient, having to declare the container before being able to write. An alternative would be to automatically create a container that includes only the bucket name as template. On writes we try to match all templates against the url, and use the first one that matches, if non does, we create a new container. We could add that as a new function in the future if it's not required on first version.

## Check requirements are satisfied

1. > Virtual chunks can be stored in all supported object stores

   Now the virtual chunk containers inform what is the platform, and we can create the appropriate object store client at read time. These clients are cached for performance.
2. > Multiple credentials sets for virtual chunk resolution

    Now the user initializes a Repo instance by passing, if needed, a credential set for each virtual chunk container. If a chunk is read for which there are no credentials, we default to a common set of credentials for virtual chunks, and ultimately, to the Storage credentials if on the same platform.
3. > Optimized manifests where the same prefix doesn't need to be repeated for every virtual chunk
  
    For large prefixes, the template + arguments creates some savings. For example, a template for virtual zarr like `some-long-bucket-name/with-some-even-longer-prefix/and-more-nesting/some-long-array-name-for-the-dataset/c/{}/{}/{}`.
   On the other hand, the template arguments vector has a 24 character overhead, on top of the values themselves. So it's not a lot of savings for smaller prefixes.
5. > Be able to know all the "places" pointed by a repo, without having to load every manifest in every version

    This can now be retrieved directly from the repository config. It's basically the set of virtual chunk containers (or a subset at least)

6. > Object level change detection (not serving a virtual chunk if the containing file changed)

    We optionally store the last-modified timestamp of the object containing each chunk. This is not optimal, a hash would be better, but it takes more space. Also not optimal, storing it per chunk, when many chunks come from the same object, but we don't have other easy place to store it if they are using containers that include more than one object. We could add more configuration to optimize this in the future.
