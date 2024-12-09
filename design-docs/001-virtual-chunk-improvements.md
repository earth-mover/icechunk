# Improvements to virtual chunks handling

## What we need

1. Virtual chunks can be stored in all supported object stores. The same repo can have virtual chunks hosted in different object stores.
2. Multiple credentials sets for virtual chunk resolution, accessing some chunks may require a different set than others.
3. A mechanism to know all the "dependencies" of a repo. That is, all the "places" where its virtual chunks are hosted. This is important for users to know if they can delete old data, or if there are virtual datasets that depend on it.
4. If the file hosting a virtual chunk changed after the chunk ref was written, we want to fail with a runtime error instead of serving corrupted data. This is because offset and length are usually not maintained during updates.

## Discussion

### Current virtual chunk url styles

* Many chunks from a few objects, the prefix can be the file
  * Example: `s3://some-bucket/some-prefix/some-file.nc` is pointed to by 10k chunks.
  * Having between 10 and 10k chunks from the same file is not unusual
* Virtual zarr dataset: each chunk comes from a different object, the prefix is not the object, we still need an extra string
  * Example: `s3://some-bucket/some-prefix/c/0/0/1` is pointed to by only 1 chunk. But there are many objects like it.
  * There could be tens of millions of chunks (so tens of millions of different urls)

### Supporting multiple virtual chunk locations

Because of requirement 1, the same repo can contain virtual chunks in multiple clouds and buckets, potentially even bucket with the same name in different platforms.

When the user adds a virtual ref, they will need to indicate the details of the platform to retrieve it. These include what cloud it is, region, endpoint url, etc. This means, the current approach of pointing to a chunk with a simple URL is not going to be enough. Not without encoding several new attributes in the URL, which seems hacky.

This all means, the following function may have to change:

```python
def set_virtual_ref(
    self, key: str, location: str, offset: int, length: int
) -> None: ...
```

`location` as a `str` is not enough.

### Read time setup

Credentials are a read time concern. When the user writes the virtual chunk, they don't necessarily know what credentials set will be used to read it.

Because of requirements 1 and 2, when users open a repo they need to know what sets of credentials they need to provide to be able to access the chunks. This is not immediately obvious, Icechunk should provide a way to introspect a repo and inform the user what they need to provide. Then the user can setup the Repo instance appropriately with the credentials.

This also applies to requirement 3, users need to know if a repo is going to be affected by, for example, the delete of an unrelated repo.

## Design

### Virtual chunk containers

We introduce a new datastructure, the `VirtualChunkContainer`:

```rust
// this is pseudo-code
struct VirtualChunkContainer {
  name: String,                           // something like model
  protocol: String,                       // something like s3
  platform: Platform,                     // something like S3
  region: Option<String>,                 // something like us-east-2
  bucket_prefix: Option<String>,          // something like model-outputs/netcdf
  cloud_client_config: Map<String, Any>,  // something like {endpoint_url: http://localhost:8080}
}
```

* Each repo has a list of `VirtualChunkContainers` that is global (applies to all snapshots)
* Users can add to the list and edit members but not delete. Names are unique within the repo.
* Adding containers to a repo is optional but useful when there are virtual chunks.
* `VirtualChunkContainers` serve several purposes:
  * Using their `name` a user can setup credentials for a specific set of chunks, in a given object store and bucket prefix.
  * Using their `protocol` and other properties, Icechunk knows how to build an object store client that can retrieve the virtual chunks.
  * Users can review the list of `VirtualChunkContainers` for a repo to quickly understand what other buckets and prefixes contain data used by this repo.
  * Users can review the list of `VirtualChunkContainers` for a repo to quickly understand what credentials they need to provide if they want to access all virtual chunks. They can still use the repo without providing those credentials, but they virtual chunk requests may fail.

### Global config

The list of chunk containers is stored in a new global `Config` object. There is a single configuration per repo, and all operations use the last version of the configuration.

The configuration is stored as a JSON file in `repo-root/config.json`, and it's written using PUT if-match to ensure writers are aware of the latest updates.

The global configuration solves also [other requirements](https://github.com/earth-mover/icechunk/issues/440) outside of this design doc. Icechunk repos need, also for other reasons, a way to store persistent default configuration.

```rust
// this is pseudo-code
struct Config {
  inline_chunk_threshold_bytes: u16,
  ... more fields

  virtual_chunk_containers: Vec<VirtualChunkContainer>,
}
```

The `Config` object acts as a default, overrideable by users at runtime. But different fields of configuration may have different behaviors on overrides.

### New mechanism to set virtual references

We add an optional `checksum` argument to `set_virtual_ref` (we need a better name for this argument).

```python
# this is pseudo-code

class ETag:
  value: str

class LastModified:
  value: datetime

class Repository:  # the class with this method is going to change soon to be Session, for an unrelated reason
  def set_virtual_ref(
      self,
      key: str,
      location: str,
      offset: int,
      length: int,
      checksum: ETag | LastModified | None = None,
      validate_containers: bool = False
  ) -> None: ...
```

`checksum` can be set to the in-object-store ETag of the file containing the virtual chunk, or to its last modified time. In the case of last modified time, the value
doesn't need to be precise. The system will only fail to provide the chunk if the object's last-modified-at is larger than the passed value. So, passing `checksum = LastModified(datetime.now())` will usually be enough.

This new argument will help us satisfy requirement 4.

The interpretation of the `location` argument also changes. `location` must be an absolute url that includes a protocol. The protocol can be:

* s3://
* gcs://
* tigris://  (is this the right protocol)
* fs://  (is this right for local file system)
* etc. for other supported object stores
* arbitrary-virtual-chunk-container-name://  (example: models://)

Depending on the protocol used, Icechunk will use a different mechanism to resolve the chunk, as explained below.

If `validate_containers` is set to `True`, the function will fail if the protocol passed is not registered in a virtual chunk container. Using `validate_containers=False` is useful, if containers will be declared after the write.

### Reading virtual chunks

When a virtual chunk is requested, Icechunk will find in the `location` field of the stored reference, an absolute URL with a specific protocol. It will need to map this location to a given object store, bucket, path and set of credentials.

The mapping is done by comparing the protocol and path to the set of configured virtual chunk containers, plus a set of default virtual chunk containers.

This is how the mapping from a chunk location (url) to cloud+bucket+prefix+credentials works:

* Using the chunk location's protocol, we find all containers that have a matching `protocol` field
* We sort the matching containers in order of descending prefix length
* We find the first one that has a `bucket_prefix` that is a prefix of the `location` path (this will be the most specific container that matches)
* If no containers match, the chunk request cannot be fulfilled and we fail with an error message indicating the missing config.
* We use the rest of the data in the matched container to instantiate an object store client (or retrieve it from cache)
* Credentials for the client must be passed by the user when the repository is instantiated, by indicating the container name to which they apply

We have one default container for each supported object store, the container has protocol and name s3, gcs, tigris, etc. and has an empty `bucket_prefix` and no region configured. These default containers, just like any others set in the persistent repo configuration, can be overriden when the repository is instantiated.

When a region is not indicated and the platform needs it, the logic is:

* For the platform in which the Icechunk repo is stored, we use the same region as the repo
* For all other platforms we use the default region for that platform

#### Use case: basic usage

All virtual chunks point to S3 us-east-1, single credential set.

* When calling `set_virtual_ref` locations are passed with `s3://` protocol.
* No persistent configuration needs to be set in the repo.
* At runtime, the repo instance can be configured passing virtual chunk credentials for container "s3", the default container.
* If Icechunk native data is also in s3 us-east-1, and the same credentials can be used, the previous point is not needed, the default will be using the same credentials as the ones for the repo.

#### Use case: non default region

All virtual chunks point to S3 sa-east-1, single credential set.

* When calling `set_virtual_ref` locations are passed with `s3://` protocol.
* A persistent configuration can be set creating a virtual chunk container with protocol s3 and region "sa-east-1", this overrides the default s3 container.
* Alternatively, this config can also be set at runtime, but every reader needs to remember to do it if they want to access virtual chunks
* At runtime, the repo instance can be configured passing virtual chunk credentials for container "s3".
* If Icechunk native data is also in s3 sa-east-1, and the same credentials can be used, the previous point is not needed, the default will be using the same credentials as the ones for the repo.

#### Use case: repo on s3 virtual chunks on gcs

The repo is stored in S3 us-east-1, all virtual chunks point to Google cloud storage us-central1

* When calling `set_virtual_ref` locations are passed with `gcs://` protocol.
* A persistent configuration can be set creating a virtual chunk container with name gcs and region "us-central1"
* Credentials and runtime config work as before

#### Use case: go to local MinIO for testing

All virtual chunks point to S3 us-east-1, for testing purposes the chunks for an array in `s3://bucket/prefix/array` are also copied locally and served with MinIO on port 8080.

* When calling `set_virtual_ref` locations are passed with `s3://` protocol.
* The icechunk repo instance is initialized with a configuration that creates a container with name `protocol=s3`, `buket_prefix=bucket/prefix/array` and `endpoint_url=localhost:9000`

#### Use case: multiple clouds and credential sets

Ichecunk repo stored in S3 bucket `repo-bucket`. Virtual array `A` on Google cloud, bucket `virtual-a`. Virtual array `B` on Google cloud, bucket `virtual-b`. Virtual array `C` on s3 bucket `virtual-c`. All buckets have different credential sets.

* When ingesting `virtual-a` VirtualiZarr is instructed to use protocol `modela`
* When ingesting `virtual-b` VirtualiZarr is instructed to use protocol `modelb`
* When ingesting `virtual-c` VirtualiZarr is instructed to use protocol `modelc`
* Persistent config is set up in the repo to create the following virtual chunk containers:
  * `modela`: on Google storage platform with prefix `virtual-a`
  * `modelb`: on Google storage platform with prefix `virtual-b`
  * `modelc`: on S3 platform with prefix `virtual-c`
  * These containers can be set before or after the ingest (we recommend before and to pass `validate_containers=True`)
* To open the repository users pass credentials associated with each virtual chunk container, in addition to the repository credentials.

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
pub enum Checksum {
  LastModified(chrono::Utc),
  Etag(String),
}

pub struct VirtualChunkRef {
    pub offset: ChunkOffset,
    pub length: ChunkLength,
    pub checksum: Option<Checksum>,
}
```

### New functions

* Ways to read and update the config under the put-if-matches semantics.
* A way to list all virtual chunk containers, this will help users know what they need to provide credentials for.
* A way to list the locations of all virtual chunks, potentially matching some query. This will help users of a badly configured repository improve the configuration or use it without a configuration.

### New integrations

* We need to teach VirtualiZarr to pass arbitrary protocols in the URLs.
* We need to teach VirtualiZarr (something else?) to obtain and pass the Etag or last-modified value.
* We need to teach VirtualiZarr to optionally pass `validate_containers=True`

## Check requirements are satisfied

1. > Virtual chunks can be stored in all supported object stores

   Now the virtual chunk containers inform what is the platform, and we can create the appropriate object store client at read time. These clients are cached for performance.
2. > Multiple credentials sets for virtual chunk resolution

    Now the user initializes a Repo instance by passing, if needed, a credential set for each virtual chunk container. If a chunk is read for which there are no credentials, we default to the Storage credentials if on the same platform. Users can figure out the credentials they need to pass by checking the list of containers.

3. > A mechanism to know all the "dependencies" of a repo
  
    This can now be retrieved directly from the repository config. It's basically the set of virtual chunk containers (or a subset at least, if badly configured). To get more detail, there is a function that allows to query the list of all virtual chunk locations.

6. > Fail for modified virtual chunks

    We optionally store the last-modified timestamp or etag of the object containing the chunk.
