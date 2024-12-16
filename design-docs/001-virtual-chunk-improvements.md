# Improvements to virtual chunks handling

## What we need

1. Virtual chunks can be stored in all supported object stores. The same repo can have virtual chunks hosted in different object stores.
2. Multiple credentials sets for virtual chunk resolution, accessing some chunks may require a different set than others.
3. A mechanism to know all the "dependencies" of a repo. That is, all the "places" where its virtual chunks are hosted. This is important for users to know if they can delete old data, or if there are virtual datasets that depend on it.
4. If the file hosting a virtual chunk changed after the chunk ref was written, we want to fail with a runtime error instead of serving corrupted data. This is because offset and length are usually not maintained during updates.

## Discussion

### Current virtual chunk URL styles

* Existing packages for reference generation (VirtualiZarr, Kerchunk, etc.) uses URLs to identify virtual chunk locations.
* One common case is many virtual chunks from a few objects.
  * Example: `s3://some-bucket/some-prefix/some-file.nc` is pointed to by 10k chunks.
  * Having between 10 and 10k chunks from the same object is not unusual
* Other common use case for virtual zarr datasets: each chunk comes from a different object. This can happen, for example, with a virtual zarr dataset.
  * Example: `s3://some-bucket/some-prefix/c/0/0/1` is pointed to by only 1 chunk. But there are many objects like it.
  * There could be tens of millions of chunks (so tens of millions of different urls)

### Supporting multiple virtual chunk locations

Because of requirement 1, the same repo can contain virtual chunks in multiple buckets, object stores, and platforms, potentially even buckets with the same name in different platforms.

When the user adds a virtual ref, they will need to indicate the details of the platform for Icechunk to be able to retrieve it. These includes what cloud it is, region, endpoint url, etc. This means, the current approach of pointing to a chunk with a simple URL is not going to be enough. Not without encoding several new attributes in the URL, which seems hacky. A URL such as `s3://foo/bar.nc`, in itself doesn't have enough details to reach the object. Even if the protocol part of that URL is identified with the wire protocol needed to talk to the object store, there are missing details such as the region, the endpoint, or the authentication mechanism.

This all means, the following function may have to change:

```python
def set_virtual_ref(
    self, key: str, location: str, offset: int, length: int
) -> None: ...
```

### Credentials

Credentials are a read time concern. When the user writes the virtual chunk, they don't necessarily know what credentials set will be used to read it.

Because of requirements 1 and 2, when users open a repo they need to know what sets of credentials they have to provide to be able to access all the chunks. This is not immediately obvious, Icechunk should provide a way to introspect a repo and inform the user what credentials they need. Then the user can setup the Repo instance appropriately with the credentials.

### Differences between object stores

Many object stores support a subset of the S3 wire protocol to read and write objects. This seems to simplify the task, because Icechunk could talk to all those platforms with the same "client code". But there are many details that make this theoretical approach not work very well in practice.

* The level of conformity with the protocol varies widely. Icechunk doesn't use very advanced features of the S3 protocol but this could change in the future. Even today, Icechunk already uses, for example, conditional requests (both writes and reads).
* The ideal client settings for different platforms is different. Some object stores allow more aggressive retries than others, or more concurrency. In general, performance optimization needs to be targeted to a specific platform sometimes.
* Encryption and checksumming capabilities changes widely too between platforms.
* Error responses are not uniform, some tweaking is needed sometimes to extract the most relevant information.
* Authentication protocols, and credential refresh varies widely.

The more Icechunk knows about the specifics of the object store platform the better job it can do at fetching chunks. Since all the information for the virtual chunks is stored in the dataset at write time, it's important we store enough, to future proof for changes and improvements to the different platforms.

All that said, good defaults and usability are very important. The ideal situation is: virtual chunks that work by default, and can be made as efficient as possible with a small amount of work.

## Design

### Checksum

In order to support requirement 4, we add an optional `checksum` argument to `set_virtual_ref`.

```python
 def set_virtual_ref(
        self,
        key: str,
        location: str,
        offset: int,
        length: int,
        validate_containers: bool = True,  # this new argument will be explained below
        checksum: int | str | datetime.datetime | None = None,
    ) -> None: ...
```

`checksum` can be set to:

* a `str` for the in-object-store ETag of the file containing the virtual chunk,
* the last modified time in seconds since Unix epoch,
* the last modified `datetime`

The system will fail to provide the chunk if the object's last-modified-at is larger than the passed value or if the ETag has changed since the chunk ref was written. The result of the chunk fetch operation will clearly indicate (with an exception in Python) that a mapped virtual chunk has changed and it is no longer reliable.

Seconds are used as the resolution for last modified, it's consider granular enough and it provides some space savings compared to higher resolutions.

ETag could be considered more powerful than last modified, for example, in many cases it remains unchanged if the object is overwritten with the same contents. But last modified is much easier to use. To be able to pass ETag, client code will probably have to list from the store every single object it wants to add a reference to. While, for last modified, it's usually enough to pass `checksum = datetime.now()`.

If no `checksum` is provided, Icechunk will always attempt to retrieve the chunk, which is a potentially dangerous operation because there is no way to validate correctness.

### Virtual chunk containers

As discussed above, to correctly support requirements 1 and 2, we need to store information about the object store platform in which the chunk owner object is stored. Icechunk needs to link each virtual chunk ref to a platform, potentially a region, potentially an endpoint, etc. In contraposition, we want to maintain the rest of the open source stack as simple and unchanged as possible. The stack "speaks" URLs for virtual chunks, we maintain that, but we reinterpret what those URLs mean as explained in the following sections.

We first introduce a new datastructure that maintain information about the different object stores used by the dataset, the `VirtualChunkContainer`:

```rust
type ContainerName = String;

pub enum ObjectStorePlatform {
    S3,
    GoogleCloudStorage,
    Azure,
    Tigris,
    MinIO,
    S3Compatible,
    LocalFileSystem,
    ...
}

pub struct VirtualChunkContainer {
    pub name: ContainerName,
    pub prefix: String,
    pub object_store: ObjectStorePlatform,

    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub anonymous: bool,
    pub allow_http: bool,
}
```

* `name`: is used exclusively for user interaction and it's not part of the chunk resolution algorithm, it gives a human readable way to refer to the container. Names must be unique per repo.
* `object_store`: it gives Icechunk information on how to talk to the object store. Using this field Icechunk will be able to:
  * Determine what protocol to use to fetch chunks from to the object store.
  * Configure and optimize the client for the object store
  * Provide defaults for other fields such as the `endpoin_url`.
* `prefix`: it will be used to find a matching container for a virtual chunk ref (see below). Must be unique per repo.
* `region`, `endpoint_url`, `anonymous`, `allow_http`: the usual details to contact an object store (more fields could be needed and added with defaults)

Each repo has a list of `VirtualChunkContainers` that is global (applies to all snapshots). Users can add and edit the list of containers. Adding containers to a repo is optional but useful when there are virtual chunks.

### Global config

The list of chunk containers is stored in a global `RepositoryConfig` object. There is a single configuration per repo, and all operations use the latest version of the configuration.

The configuration is stored as a yaml file in `repo-root/config.yaml`, and it's written using PUT if-match to ensure writers are aware of the latest updates before overwriting.

The global configuration solves also [other requirements](https://github.com/earth-mover/icechunk/issues/440) outside of this design doc.

```rust
pub struct RepositoryConfig {
    pub inline_chunk_threshold_bytes: u16,
    pub unsafe_overwrite_refs: bool,
    // ... more fields

    virtual_chunk_containers: Vec<VirtualChunkContainer>,
}
```

The `RepositoryConfig` object acts as a default, read during repository instantiation, and overridable by users at runtime. Different fields of configuration may have different behaviors when overridden.

A user can modify the `RepositoryConfig` for their current repo instance, but decide not to save the change to storage. This will be very common as users tweak the configuration during experimentation.

### Setting virtual references

As we mentioned, we want to make as little changes as possible to the way virtual chunks are written to Icechunk by the open source stack. The new `set_virtual_ref` function still takes a `location` url as the main data, adds an optional `checksum` and an optional `validate_containers` argument.

If `validate_containers` is set to `True`, Icechunk will verify in `set_virtual_ref` that there is a `VirtualChunkContainer` with a `prefix` field  that is a prefix of the `location` url passed. An exception is raised if that's not the case and the reference is not stored.

The following examples will work for `set_virtual_ref` called with the given `location` and `validate_containers=True`:

* `location=s3://foo/bar.nc` if there is a `VirtualChunkContainer` with `prefix=s3`.
* `location=s3://foo/bar.nc` if there is a `VirtualChunkContainer` with `prefix=s3://`.
* `location=s3://foo/bar.nc` if there is a `VirtualChunkContainer` with `prefix=s3://foo`.
* `location=s3://foo/bar.nc` if there is a `VirtualChunkContainer` with `prefix=s3://foo/bar.nc`.
* `location=tigris://foo/bar.nc` if there is a `VirtualChunkContainer` with `prefix=tigris`.
* `location=models://foo/bar.nc` if there is a `VirtualChunkContainer` with `prefix=models`.

The following examples will now work for `set_virtual_ref` called with the given `location` and `validate_containers=True`:

* `location=s3://foo/bar.nc` if there are no `VirtualChunkContainers`
* `location=gcs://foo/bar.nc` if the only `VirtualChunkContainers` has `prefix=s3`

But calling `set_virtual_ref` in the two last examples will still work with `validate_containers=False`.

The exact algorithm to match a `location` URL to one specific container is explained in a section below.

### New virtual chunk datastructures

We change from

```rust
pub enum VirtualChunkLocation {
    Absolute(String),
    // Relative(prefix_id, String)  // this was a planned approach that remained  unimplemented
}

pub struct VirtualChunkRef {
    pub location: VirtualChunkLocation, // this is basically a String for the URL today
    pub offset: ChunkOffset,
    pub length: ChunkLength,
}
```

To

```rust
pub struct VirtualChunkLocation(pub String);

type ETag = String;

pub struct SecondsSinceEpoch(pub u32);

pub enum Checksum {
    LastModified(SecondsSinceEpoch),
    ETag(ETag),
}

pub struct VirtualChunkRef {
    pub location: VirtualChunkLocation,
    pub offset: ChunkOffset,
    pub length: ChunkLength,
    pub checksum: Option<Checksum>,
}
```

Using a `u32` for `SecondsSinceEpoch` gives us at least until year 2100.

Virtual chunk refs still store only the `location` URL as passed to `set_virtual_ref`. The resolution from URL to `VirtualChunkContainer` is done at read time. Why?

* It's cheap compared to pulling a chunk
* It gives flexibility on when the containers are created (for example after the writes by using `validate_containers=False`)
* It allows runtime changes to the containers (for example pointing the virtual chunks to a different place during tests)

`VirtualChunkContainer` is a `String` and not a `Url` to future proof the format. Some day we may want to do something different for other types of virtual refs, or to support new open source libraries.

### Reading virtual chunks

When a virtual chunk is requested we need to map the `VirtualChunkLocation` URL to one and only one `VirtualChunkContainer`. Then, from that container, we can instantiate an object store client, potentially with some user provided credentials.

These are the steps to find the `VirtualChunkContainer` that corresponds to a given `VirtualChunkLocation`

* Sort all containers by their `prefix` length, in descending order (longest `prefix` first).
* Find the first container in the sorted list for which its `prefix` field is a prefix of the `VirtualChunkLocation` string.
* If no containers satisfy the condition the fetch virtual chunk operation fails indicating the missing configuration.

These algorithm ensures that we find the single most specific container that matches the chunk location. This allows for very flexible matching, for example, three containers with prefixes `models` and `models/dev` `models/prod` can coexist and be hosted in different object store platforms.

Once the matching `VirtualChunkContainer` is found, its fields are used to build an object store client. Some smart defaults can be provided, for example:

* Each `ObjectStorePlatform` has a default `endpoint_url`.
* If the `ObjectStorePlatform` is the same as the repo is hosted on, we can use the same `region`.
* `ObjectStorePlatform` have defaults regions, or may not need them.

### Default `VirtualChunkContainers`

To optimize for usability for the most common use cases, a set of default `VirtualChunkContainers` is created in every repo. There will be default containers for the common supported platforms (s3, gcs, azure, tigris, etc.). The names and prefixes of these containers will be (`s3`, `gcs`, `azure`, `tigris`, etc). They will include no `region`, which in practice means they will use the same region as the repo or the platform default region.

This means that a user who has their repo and virtual chunks in Google Cloud Storage, only needs to instruct VirtualiZarr to create URLs with protocol gcs, as in `gcs://foo/bar.nc` and things will just work in Icechunk, without the need to tweak any configuration.

### Object store client infrastructure

A new trait takes care of the "fetching virtual chunks abstraction":

```rust
#[async_trait]
pub trait ChunkFetcher: std::fmt::Debug + private::Sealed + Send + Sync {
    async fn fetch_chunk(
        &self,
        chunk_location: &str,
        range: &ByteRange,
        checksum: Option<&Checksum>,
    ) -> Result<Bytes, VirtualReferenceError>;
}

```

We will have one implementation for each `ObjectStorePlatform` where we want to "speak" a different protocol or optimize differently. For example, we can have one implementation that uses the Rust native S3 client for `s3` and other one that uses object_store for `LocalFileSystem`.

The `Repository` now owns a `VirtualChunkResolver`. This struct is in charge of hosting the containers, matching them and building the `ChunkFetchers` needed.

```rust
pub struct VirtualChunkResolver {
    containers: Vec<VirtualChunkContainer>,
    credentials: HashMap<ContainerName, ObjectStoreCredentials>,
    fetchers: RwLock<HashMap<ContainerName, Arc<dyn ChunkFetcher>>>,
}
```

Creating a new object store client for each virtual chunk request would obviously not work well. We'll have a cache of clients, one per `VirtualChunkContainer` in use. This is not the most efficient thing, we could share the same client across containers that share the platform for example, but it's good enough and simpler. We can always optimize later.

The cached `ChunkFetchers` are maintained by the `VirtualChunkResolver` in its `fetchers` field. Fetchers are instantiated on demand, on the first chunk that needs them and then cached without invalidation. Careful coding is required to avoid a herd effect when multiple chunks for the same container are requested concurrently (a very common situation)

### Credentials

We haven't discussed the third field in the `VirtualChunkResolver`: `credentials`. This field gets populated when the `Repository` is initialized and it holds the credentials passed by the user for each container. Users don't necessarily need to pass credentials for every container, but if they don't, their requests for those chunks may fail.

The `ContainerName` field in the `VirtualChunkContainer` is how the users refer to them to pass credentials.

Several types of credentials are possible.

```rust
pub enum ObjectStoreCredentials {
    FromEnv,
    Anonymous,
    Static {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
}
```

An we will have to add at least one more variant once we implement credentials refresh.

### Use case examples

#### Basic usage

All virtual chunks point to S3 us-east-1, single credential set.

* When calling `set_virtual_ref` locations are passed with `s3://` protocol.
* No persistent configuration needs to be set in the repo.
* At runtime, the repo instance can be configured passing credentials for container "s3", the default container.
* If the repo data is also in s3 us-east-1, and the same credentials can be used, the previous point is not needed, the default will be using the same credentials as the ones for the repo.

#### Non default region

All virtual chunks point to S3 sa-east-1, single credential set.

* When calling `set_virtual_ref` locations are passed with `s3://` protocol.
* A persistent configuration can be set creating a virtual chunk container with prefix `s3://` and region "sa-east-1", this overrides the default s3 container.
* Alternatively, this config can also be set at runtime, but every reader needs to remember to do it if they want to access virtual chunks
* At runtime, the repo instance can be configured passing virtual chunk credentials for the container with prefix "s3://".
* If Icechunk native data is also in s3 sa-east-1, and the same credentials can be used, the previous point is not needed, the default will be using the same credentials as the ones for the repo.

#### Repo on s3 virtual chunks on gcs

The repo is stored in S3 us-east-1, all virtual chunks point to Google cloud storage africa-south1

* When calling `set_virtual_ref` locations are passed with `gcs://` protocol.
* A persistent configuration can be set creating a virtual chunk container with name gcs and region "africa-south1"
* Credentials must be set at runtime for the custom protocol

#### Go to local MinIO for testing

All virtual chunks point to S3 us-east-1, for testing purposes the chunks for an array in `s3://bucket/prefix/array` are also copied locally and served with MinIO on port 8080.

* When calling `set_virtual_ref` locations are passed with `s3://` protocol.
* The icechunk repo instance is initialized with a configuration that creates a container with `name=minio-override`, `prefix=s3://bucket/prefix/array` and `endpoint_url=localhost:9000`

#### Multiple clouds and credential sets

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

### New functions

* Ways to read and update the config under the put-if-matches semantics.
* A way to list all virtual chunk containers, this will help users know what they need to provide credentials for.
* A way to list the locations of all virtual chunks, potentially matching some query. This will help users of a badly configured repository improve the configuration or use it without a configuration.

### New integrations

* We need to teach VirtualiZarr to pass arbitrary protocols in the URLs.
* We need to teach VirtualiZarr to obtain and pass the Etag or last-modified value.
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
