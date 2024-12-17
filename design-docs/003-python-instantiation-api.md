# Python API for Repository instantiation and configuration

## What we need

1. A good interface to instantiate existing and new `Repositories`, including different types of `Storage` instances.
    * For the case of existing repos, reading of configuration and snapshot should happen concurrently.
1. A good interface to tune the configuration parameters of a `Repository`, both at runtime and in storage.
1. A good interface to tune the configuration parameters of a `Store`.
1. A good way to pass credentials, both for the `Storage` instance and for the `VirtualChunkContainers`.

## Discussion

* We currently don't have a way to modify (or retrieve) the repo config from Python
* Instantiating a repo requires a `Storage`, which in itself is not easy to instantiate:
  * `Storages` can be for different object stores
  * They need credentials
  * They are async and can fail
* In the Rust code `Repository` configuration is immutable. It can be set during instantiation but it cannot be changed later. This is intentional, changing the configuration while the repo is "alive" is tricky:
  * What happens to the already opened sessions and stores for that repo if config changes?
  * If the config changes, impact the open sessions things can get very confusing for the user.
  * Things get also hard to implement, when config changes happen concurrently in the middle of operations.
* Currently in the Rust code, a repo is instantiated with its persistent configuration by default, and the config can be overwritten by passing the full configuration object.
  * This is not ideal. To alter a single parameter of the config, users need to retrieve the full configuration, alter it, and overwrite the full config object
* Persistent configuration can be updated by calling `save_config` on a `Repository` instance. This is a destructive operation that requires no `commit`, so it must be executed carefully. The workflow is:
  * Decide on the full configuration object you want
  * Instantiate a repo with that config
  * Test it
  * Call `save_config`
* Should we move the `Store` config to the `Repository`?
  * That may simplify things, and it would allow `store` to be a property instead of a function
  * Of course, it would mean a `Repository` instance can only create `Stores` with the same config
* Is a common `Credentials` type enough? Should every object store have its own `Credentials` type?
  * For example, we could support refreshing for certain object stores and not others, or from environment.

## Design

* We unify repo and store config. The extra flexibility is not worth the complexity
* We use the same types for Storage config and virtual chunk container store config
* We use the same types for repo credentials and virtual chunk container credentials
* We don't offer different config types for different object stores

```python
ObjectStorePlatform = Literal["S3", "GoogleCloudStorage", "Azure", "Tigris", "S3Compatible", "LocalFileSystem", "InMemory"]

@dataclass
class ObjectStoreConfig:
    object_store: ObjectStorePlatform
    region: str | None
    endpoint_url: str | None
    anonymous: bool
    allow_http: bool
    extra: Mapping[str, Any] # this will initially be empty, but it could in the future include fine tuning parameters

@dataclass
class VirtualChunkContainer:
    name: str
    prefix: str
    store: ObjectStoreConfig

@dataclass
class RepositoryConfig:
    inline_chunk_threshold_bytes: int
    unsafe_overwrite_refs: bool
    virtual_chunk_containers: List[VirtualChunkContainer]

    # gathering together the config for the repo and the store
    get_partial_values_concurrency: int


def default_repository_config() -> RepositoryConfig:
  ...



@dataclass
class FromEnvCredentials:
  pass

@dataclass
class AnonymousCredentials:
  pass

@dataclass
class StaticCredentials:
  access_key_id: str
  secret_access_key: str
  session_token: str | None

@dataclass
class ExpiringCredentials(StaticCredentials):
  expiration: datetime

@dataclass
class RefreshableCredentials:
  fetch_with: Callable[[], ExpiringCredentials]
  refresh_with: Callable[[ExpiringCredentials], ExpiringCredentials]

ObjectStoreCredentials = FromEnvCredentials | AnonymousCredentials | StaticCredentials | RefreshableCredentials

class Repository:
    @classmethod
    def open(
        store: ObjectStoreConfig,
        store_credentials: ObjectStoreCredentials | None = None,
        config: RepositoryConfig | None = None,
        virtual_chunk_credentials: Mapping[str, ObjectStoreCredentials] | None = None
    ) -> Repository:
      ...

    @classmethod
    def create(...):
      ...

    @classmethod
    def open_or_create(...):
      ...

    @staticmethod
    def exists(
        store: ObjectStoreConfig,
        store_credentials: ObjectStoreCredentials | None = None,
    ) -> bool:
      ...

    @staticmethod
    def get_repository_config(
        store: ObjectStoreConfig,
        store_credentials: ObjectStoreCredentials | None = None,
    ) -> RepositoryConfig:
      ...

    # Changes the persistent configuration of this repo to match the current runtime config
    def save_config(self) -> None:
      ...

    @property
    def config(self) -> RepositoryConfig:
      ...
```
