import abc
import datetime
from collections.abc import AsyncGenerator, AsyncIterator
from enum import Enum
from typing import Any, TypeAlias

class S3Options:
    """Options for accessing an S3-compatible storage backend"""
    def __init__(
        self,
        region: str | None = None,
        endpoint_url: str | None = None,
        allow_http: bool = False,
        anonymous: bool = False,
        force_path_style: bool = False,
    ) -> None:
        """
        Create a new `S3Options` object

        Parameters
        ----------
        region: str | None
            Optional, the region to use for the storage backend.
        endpoint_url: str | None
            Optional, the endpoint URL to use for the storage backend.
        allow_http: bool
            Whether to allow HTTP requests to the storage backend.
        anonymous: bool
            Whether to use anonymous credentials to the storage backend. When `True`, the s3 requests will not be signed.
        force_path_style: bool
            Whether to force use of path-style addressing for buckets.
        """

class ObjectStoreConfig:
    class InMemory:
        def __init__(self) -> None: ...

    class LocalFileSystem:
        def __init__(self, path: str) -> None: ...

    class S3Compatible:
        def __init__(self, options: S3Options) -> None: ...

    class S3:
        def __init__(self, options: S3Options) -> None: ...

    class Gcs:
        def __init__(self, opts: dict[str, str] | None = None) -> None: ...

    class Azure:
        def __init__(self, opts: dict[str, str] | None = None) -> None: ...

    class Tigris:
        def __init__(self, opts: S3Options) -> None: ...

    class Http:
        def __init__(self, opts: dict[str, str] | None = None) -> None: ...

AnyObjectStoreConfig = (
    ObjectStoreConfig.InMemory
    | ObjectStoreConfig.LocalFileSystem
    | ObjectStoreConfig.S3
    | ObjectStoreConfig.S3Compatible
    | ObjectStoreConfig.Gcs
    | ObjectStoreConfig.Azure
    | ObjectStoreConfig.Tigris
    | ObjectStoreConfig.Http
)

class VirtualChunkContainer:
    """A virtual chunk container is a configuration that allows Icechunk to read virtual references from a storage backend.

    Attributes
    ----------
    url_prefix: str
        The prefix of urls that will use this containers configuration for reading virtual references.
    store: ObjectStoreConfig
        The storage backend to use for the virtual chunk container.
    """

    name: str
    url_prefix: str
    store: ObjectStoreConfig

    def __init__(self, url_prefix: str, store: AnyObjectStoreConfig):
        """
        Create a new `VirtualChunkContainer` object

        Parameters
        ----------
        url_prefix: str
            The prefix of urls that will use this containers configuration for reading virtual references.
        store: ObjectStoreConfig
            The storage backend to use for the virtual chunk container.
        """

class VirtualChunkSpec:
    """The specification for a virtual chunk reference."""
    @property
    def index(self) -> list[int]:
        """The chunk index, in chunk coordinates space"""
        ...
    @property
    def location(self) -> str:
        """The URL to the virtual chunk data, something like 's3://bucket/foo.nc'"""
        ...
    @property
    def offset(self) -> int:
        """The chunk offset within the pointed object, in bytes"""
        ...
    @property
    def length(self) -> int:
        """The length of the chunk in bytes"""
        ...
    @property
    def etag_checksum(self) -> str | None:
        """Optional object store e-tag for the containing object.

        Icechunk will refuse to serve data from this chunk if the etag has changed.
        """
        ...
    @property
    def last_updated_at_checksum(self) -> datetime.datetime | None:
        """Optional timestamp for the containing object.

        Icechunk will refuse to serve data from this chunk if it has been modified in object store after this time.
        """
        ...

    def __init__(
        self,
        index: list[int],
        location: str,
        offset: int,
        length: int,
        etag_checksum: str | None = None,
        last_updated_at_checksum: datetime.datetime | None = None,
    ) -> None: ...

class CompressionAlgorithm(Enum):
    """Enum for selecting the compression algorithm used by Icechunk to write its metadata files

    Attributes
    ----------
    Zstd: int
        The Zstd compression algorithm.
    """

    Zstd = 0

    def __init__(self) -> None: ...
    @staticmethod
    def default() -> CompressionAlgorithm:
        """
        The default compression algorithm used by Icechunk to write its metadata files.

        Returns
        -------
        CompressionAlgorithm
            The default compression algorithm.
        """
        ...

class CompressionConfig:
    """Configuration for how Icechunk compresses its metadata files"""

    def __init__(
        self, algorithm: CompressionAlgorithm | None = None, level: int | None = None
    ) -> None:
        """
        Create a new `CompressionConfig` object

        Parameters
        ----------
        algorithm: CompressionAlgorithm | None
            The compression algorithm to use.
        level: int | None
            The compression level to use.
        """
        ...
    @property
    def algorithm(self) -> CompressionAlgorithm | None:
        """
        The compression algorithm used by Icechunk to write its metadata files.

        Returns
        -------
        CompressionAlgorithm | None
            The compression algorithm used by Icechunk to write its metadata files.
        """
        ...
    @algorithm.setter
    def algorithm(self, value: CompressionAlgorithm | None) -> None:
        """
        Set the compression algorithm used by Icechunk to write its metadata files.

        Parameters
        ----------
        value: CompressionAlgorithm | None
            The compression algorithm to use.
        """
        ...
    @property
    def level(self) -> int | None:
        """
        The compression level used by Icechunk to write its metadata files.

        Returns
        -------
        int | None
            The compression level used by Icechunk to write its metadata files.
        """
        ...
    @level.setter
    def level(self, value: int | None) -> None:
        """
        Set the compression level used by Icechunk to write its metadata files.

        Parameters
        ----------
        value: int | None
            The compression level to use.
        """
        ...
    @staticmethod
    def default() -> CompressionConfig:
        """
        The default compression configuration used by Icechunk to write its metadata files.

        Returns
        -------
        CompressionConfig
        """

class CachingConfig:
    """Configuration for how Icechunk caches its metadata files"""

    def __init__(
        self,
        num_snapshot_nodes: int | None = None,
        num_chunk_refs: int | None = None,
        num_transaction_changes: int | None = None,
        num_bytes_attributes: int | None = None,
        num_bytes_chunks: int | None = None,
    ) -> None:
        """
        Create a new `CachingConfig` object

        Parameters
        ----------
        num_snapshot_nodes: int | None
            The number of snapshot nodes to cache.
        num_chunk_refs: int | None
            The number of chunk references to cache.
        num_transaction_changes: int | None
            The number of transaction changes to cache.
        num_bytes_attributes: int | None
            The number of bytes of attributes to cache.
        num_bytes_chunks: int | None
            The number of bytes of chunks to cache.
        """
    @property
    def num_snapshot_nodes(self) -> int | None:
        """
        The number of snapshot nodes to cache.

        Returns
        -------
        int | None
            The number of snapshot nodes to cache.
        """
        ...
    @num_snapshot_nodes.setter
    def num_snapshot_nodes(self, value: int | None) -> None:
        """
        Set the number of snapshot nodes to cache.

        Parameters
        ----------
        value: int | None
            The number of snapshot nodes to cache.
        """
        ...
    @property
    def num_chunk_refs(self) -> int | None:
        """
        The number of chunk references to cache.

        Returns
        -------
        int | None
            The number of chunk references to cache.
        """
        ...
    @num_chunk_refs.setter
    def num_chunk_refs(self, value: int | None) -> None:
        """
        Set the number of chunk references to cache.

        Parameters
        ----------
        value: int | None
            The number of chunk references to cache.
        """
        ...
    @property
    def num_transaction_changes(self) -> int | None:
        """
        The number of transaction changes to cache.

        Returns
        -------
        int | None
            The number of transaction changes to cache.
        """
        ...
    @num_transaction_changes.setter
    def num_transaction_changes(self, value: int | None) -> None:
        """
        Set the number of transaction changes to cache.

        Parameters
        ----------
        value: int | None
            The number of transaction changes to cache.
        """
        ...
    @property
    def num_bytes_attributes(self) -> int | None:
        """
        The number of bytes of attributes to cache.

        Returns
        -------
        int | None
            The number of bytes of attributes to cache.
        """
        ...
    @num_bytes_attributes.setter
    def num_bytes_attributes(self, value: int | None) -> None:
        """
        Set the number of bytes of attributes to cache.

        Parameters
        ----------
        value: int | None
            The number of bytes of attributes to cache.
        """
        ...
    @property
    def num_bytes_chunks(self) -> int | None:
        """
        The number of bytes of chunks to cache.

        Returns
        -------
        int | None
            The number of bytes of chunks to cache.
        """
        ...
    @num_bytes_chunks.setter
    def num_bytes_chunks(self, value: int | None) -> None:
        """
        Set the number of bytes of chunks to cache.

        Parameters
        ----------
        value: int | None
            The number of bytes of chunks to cache.
        """
        ...

class ManifestPreloadCondition:
    """Configuration for conditions under which manifests will preload on session creation"""

    @staticmethod
    def or_conditions(
        conditions: list[ManifestPreloadCondition],
    ) -> ManifestPreloadCondition:
        """Create a preload condition that matches if any of `conditions` matches"""
        ...
    @staticmethod
    def and_conditions(
        conditions: list[ManifestPreloadCondition],
    ) -> ManifestPreloadCondition:
        """Create a preload condition that matches only if all passed `conditions` match"""
        ...
    @staticmethod
    def path_matches(regex: str) -> ManifestPreloadCondition:
        """Create a preload condition that matches if the full path to the array matches the passed regex.

        Array paths are absolute, as in `/path/to/my/array`
        """
        ...
    @staticmethod
    def name_matches(regex: str) -> ManifestPreloadCondition:
        """Create a preload condition that matches if the array's name matches the passed regex.

        Example, for an array  `/model/outputs/temperature`, the following will match:
        ```
        name_matches(".*temp.*")
        ```
        """
        ...
    @staticmethod
    def num_refs(from_refs: int | None, to_refs: int | None) -> ManifestPreloadCondition:
        """Create a preload condition that matches only if the number of chunk references in the manifest is within the given range.

        from_refs is inclusive, to_refs is exclusive.
        """
        ...
    @staticmethod
    def true() -> ManifestPreloadCondition:
        """Create a preload condition that always matches any manifest"""
        ...
    @staticmethod
    def false() -> ManifestPreloadCondition:
        """Create a preload condition that never matches any manifests"""
        ...
    def __and__(self, other: ManifestPreloadCondition) -> ManifestPreloadCondition:
        """Create a preload condition that matches if both this condition and `other` match."""
        ...
    def __or__(self, other: ManifestPreloadCondition) -> ManifestPreloadCondition:
        """Create a preload condition that matches if either this condition or `other` match."""
        ...

class ManifestPreloadConfig:
    """Configuration for how Icechunk manifest preload on session creation"""

    def __init__(
        self,
        max_total_refs: int | None = None,
        preload_if: ManifestPreloadCondition | None = None,
    ) -> None:
        """
        Create a new `ManifestPreloadConfig` object

        Parameters
        ----------
        max_total_refs: int | None
            The maximum number of references to preload.
        preload_if: ManifestPreloadCondition | None
            The condition under which manifests will be preloaded.
        """
        ...
    @property
    def max_total_refs(self) -> int | None:
        """
        The maximum number of references to preload.

        Returns
        -------
        int | None
            The maximum number of references to preload.
        """
        ...
    @max_total_refs.setter
    def max_total_refs(self, value: int | None) -> None:
        """
        Set the maximum number of references to preload.

        Parameters
        ----------
        value: int | None
            The maximum number of references to preload.
        """
        ...
    @property
    def preload_if(self) -> ManifestPreloadCondition | None:
        """
        The condition under which manifests will be preloaded.

        Returns
        -------
        ManifestPreloadCondition | None
            The condition under which manifests will be preloaded.
        """
        ...
    @preload_if.setter
    def preload_if(self, value: ManifestPreloadCondition | None) -> None:
        """
        Set the condition under which manifests will be preloaded.

        Parameters
        ----------
        value: ManifestPreloadCondition | None
            The condition under which manifests will be preloaded.
        """
        ...

class ManifestSplitCondition:
    """Configuration for conditions under which manifests will be split into splits"""

    @staticmethod
    def or_conditions(
        conditions: list[ManifestSplitCondition],
    ) -> ManifestSplitCondition:
        """Create a splitting condition that matches if any of `conditions` matches"""
        ...
    @staticmethod
    def and_conditions(
        conditions: list[ManifestSplitCondition],
    ) -> ManifestSplitCondition:
        """Create a splitting condition that matches only if all passed `conditions` match"""
        ...
    @staticmethod
    def path_matches(regex: str) -> ManifestSplitCondition:
        """Create a splitting condition that matches if the full path to the array matches the passed regex.

        Array paths are absolute, as in `/path/to/my/array`
        """
        ...
    @staticmethod
    def name_matches(regex: str) -> ManifestSplitCondition:
        """Create a splitting condition that matches if the array's name matches the passed regex.

        Example, for an array  `/model/outputs/temperature`, the following will match:
        ```
        name_matches(".*temp.*")
        ```
        """
        ...

    @staticmethod
    def AnyArray() -> ManifestSplitCondition:
        """Create a splitting condition that matches any array."""
        ...

    def __or__(self, other: ManifestSplitCondition) -> ManifestSplitCondition:
        """Create a splitting condition that matches if either this condition or `other` matches"""
        ...

    def __and__(self, other: ManifestSplitCondition) -> ManifestSplitCondition:
        """Create a splitting condition that matches if both this condition and `other` match"""
        ...

class ManifestSplitDimCondition:
    """Conditions for specifying dimensions along which to shard manifests."""
    class Axis:
        """Split along specified integer axis."""
        def __init__(self, axis: int) -> None: ...

    class DimensionName:
        """Split along specified named dimension."""
        def __init__(self, regex: str) -> None: ...

    class Any:
        """Split along any other unspecified dimension."""
        def __init__(self) -> None: ...

DimSplitSize: TypeAlias = int
SplitSizes: TypeAlias = tuple[
    tuple[
        ManifestSplitCondition,
        tuple[
            tuple[
                ManifestSplitDimCondition.Axis
                | ManifestSplitDimCondition.DimensionName
                | ManifestSplitDimCondition.Any,
                DimSplitSize,
            ],
            ...,
        ],
    ],
    ...,
]

class ManifestSplittingConfig:
    """Configuration for manifest splitting."""

    @staticmethod
    def from_dict(
        split_sizes: dict[
            ManifestSplitCondition,
            dict[
                ManifestSplitDimCondition.Axis
                | ManifestSplitDimCondition.DimensionName
                | ManifestSplitDimCondition.Any,
                int,
            ],
        ],
    ) -> ManifestSplittingConfig: ...
    def __init__(self, split_sizes: SplitSizes) -> None:
        """Configuration for how Icechunk manifests will be split.

        Parameters
        ----------
        split_sizes: tuple[tuple[ManifestSplitCondition, tuple[tuple[ManifestSplitDimCondition, int], ...]], ...]
            The configuration for how Icechunk manifests will be preloaded.

        Examples
        --------

        Split manifests for the `temperature` array, with 3 chunks per shard along the `longitude` dimension.
        >>> ManifestSplittingConfig.from_dict(
        ...     {
        ...         ManifestSplitCondition.name_matches("temperature"): {
        ...             ManifestSplitDimCondition.DimensionName("longitude"): 3
        ...         }
        ...     }
        ... )
        """
        pass

    @property
    def split_sizes(self) -> SplitSizes:
        """
        Configuration for how Icechunk manifests will be split.

        Returns
        -------
        tuple[tuple[ManifestSplitCondition, tuple[tuple[ManifestSplitDimCondition, int], ...]], ...]
            The configuration for how Icechunk manifests will be preloaded.
        """
        ...

    @split_sizes.setter
    def split_sizes(self, value: SplitSizes) -> None:
        """
        Set the sizes for how Icechunk manifests will be split.

        Parameters
        ----------
        value: tuple[tuple[ManifestSplitCondition, tuple[tuple[ManifestSplitDimCondition, int], ...]], ...]
            The configuration for how Icechunk manifests will be preloaded.
        """
        ...

class ManifestConfig:
    """Configuration for how Icechunk manifests"""

    def __init__(
        self,
        preload: ManifestPreloadConfig | None = None,
        splitting: ManifestSplittingConfig | None = None,
    ) -> None:
        """
        Create a new `ManifestConfig` object

        Parameters
        ----------
        preload: ManifestPreloadConfig | None
            The configuration for how Icechunk manifests will be preloaded.
        splitting: ManifestSplittingConfig | None
            The configuration for how Icechunk manifests will be split.
        """
        ...
    @property
    def preload(self) -> ManifestPreloadConfig | None:
        """
        The configuration for how Icechunk manifests will be preloaded.

        Returns
        -------
        ManifestPreloadConfig | None
            The configuration for how Icechunk manifests will be preloaded.
        """
        ...
    @preload.setter
    def preload(self, value: ManifestPreloadConfig | None) -> None:
        """
        Set the configuration for how Icechunk manifests will be preloaded.

        Parameters
        ----------
        value: ManifestPreloadConfig | None
            The configuration for how Icechunk manifests will be preloaded.
        """
        ...

    @property
    def splitting(self) -> ManifestSplittingConfig | None:
        """
        The configuration for how Icechunk manifests will be split.

        Returns
        -------
        ManifestSplittingConfig | None
            The configuration for how Icechunk manifests will be split.
        """
        ...

    @splitting.setter
    def splitting(self, value: ManifestSplittingConfig | None) -> None:
        """
        Set the configuration for how Icechunk manifests will be split.

        Parameters
        ----------
        value: ManifestSplittingConfig | None
            The configuration for how Icechunk manifests will be split.
        """
        ...

class StorageRetriesSettings:
    """Configuration for how Icechunk retries requests.

    Icechunk retries failed requests with an exponential backoff algorithm."""

    def __init__(
        self,
        max_tries: int | None = None,
        initial_backoff_ms: int | None = None,
        max_backoff_ms: int | None = None,
    ) -> None:
        """
        Create a new `StorageRetriesSettings` object

        Parameters
        ----------
        max_tries: int | None
            The maximum number of tries, including the initial one. Set to 1 to disable retries
        initial_backoff_ms: int | None
            The initial backoff duration in milliseconds
        max_backoff_ms: int | None
            The limit to backoff duration in milliseconds
        """
        ...
    @property
    def max_tries(self) -> int | None:
        """
        The maximum number of tries, including the initial one.

        Returns
        -------
        int | None
            The maximum number of tries.
        """
        ...
    @max_tries.setter
    def max_tries(self, value: int | None) -> None:
        """
        Set the maximum number of tries. Set to 1 to disable retries.

        Parameters
        ----------
        value: int | None
            The maximum number of tries
        """
        ...
    @property
    def initial_backoff_ms(self) -> int | None:
        """
        The initial backoff duration in milliseconds.

        Returns
        -------
        int | None
            The initial backoff duration in milliseconds.
        """
        ...
    @initial_backoff_ms.setter
    def initial_backoff_ms(self, value: int | None) -> None:
        """
        Set the initial backoff duration in milliseconds.

        Parameters
        ----------
        value: int | None
            The initial backoff duration in milliseconds.
        """
        ...
    @property
    def max_backoff_ms(self) -> int | None:
        """
        The maximum backoff duration in milliseconds.

        Returns
        -------
        int | None
            The maximum backoff duration in milliseconds.
        """
        ...
    @max_backoff_ms.setter
    def max_backoff_ms(self, value: int | None) -> None:
        """
        Set the maximum backoff duration in milliseconds.

        Parameters
        ----------
        value: int | None
            The maximum backoff duration in milliseconds.
        """
        ...

class StorageConcurrencySettings:
    """Configuration for how Icechunk uses its Storage instance"""

    def __init__(
        self,
        max_concurrent_requests_for_object: int | None = None,
        ideal_concurrent_request_size: int | None = None,
    ) -> None:
        """
        Create a new `StorageConcurrencySettings` object

        Parameters
        ----------
        max_concurrent_requests_for_object: int | None
            The maximum number of concurrent requests for an object.
        ideal_concurrent_request_size: int | None
            The ideal concurrent request size.
        """
        ...
    @property
    def max_concurrent_requests_for_object(self) -> int | None:
        """
        The maximum number of concurrent requests for an object.

        Returns
        -------
        int | None
            The maximum number of concurrent requests for an object.
        """
        ...
    @max_concurrent_requests_for_object.setter
    def max_concurrent_requests_for_object(self, value: int | None) -> None:
        """
        Set the maximum number of concurrent requests for an object.

        Parameters
        ----------
        value: int | None
            The maximum number of concurrent requests for an object.
        """
        ...
    @property
    def ideal_concurrent_request_size(self) -> int | None:
        """
        The ideal concurrent request size.

        Returns
        -------
        int | None
            The ideal concurrent request size.
        """
        ...
    @ideal_concurrent_request_size.setter
    def ideal_concurrent_request_size(self, value: int | None) -> None:
        """
        Set the ideal concurrent request size.

        Parameters
        ----------
        value: int | None
            The ideal concurrent request size.
        """
        ...

class StorageSettings:
    """Configuration for how Icechunk uses its Storage instance"""

    def __init__(
        self,
        concurrency: StorageConcurrencySettings | None = None,
        retries: StorageRetriesSettings | None = None,
        unsafe_use_conditional_create: bool | None = None,
        unsafe_use_conditional_update: bool | None = None,
        unsafe_use_metadata: bool | None = None,
        storage_class: str | None = None,
        metadata_storage_class: str | None = None,
        chunks_storage_class: str | None = None,
        minimum_size_for_multipart_upload: int | None = None,
    ) -> None:
        """
        Create a new `StorageSettings` object

        Parameters
        ----------
        concurrency: StorageConcurrencySettings | None
            The configuration for how Icechunk uses its Storage instance.

        retries: StorageRetriesSettings | None
            The configuration for how Icechunk retries failed requests.

        unsafe_use_conditional_update: bool | None
            If set to False, Icechunk loses some of its consistency guarantees.
            This is only useful in object stores that don't support the feature.
            Use it at your own risk.

        unsafe_use_conditional_create: bool | None
            If set to False, Icechunk loses some of its consistency guarantees.
            This is only useful in object stores that don't support the feature.
            Use at your own risk.

        unsafe_use_metadata: bool | None
            Don't write metadata fields in Icechunk files.
            This is only useful in object stores that don't support the feature.
            Use at your own risk.

        storage_class: str | None
            Store all objects using this object store storage class
            If None the object store default will be used.
            Currently not supported in GCS.
            Example: STANDARD_IA

        metadata_storage_class: str | None
            Store metadata objects using this object store storage class.
            Currently not supported in GCS.
            Defaults to storage_class.

        chunks_storage_class: str | None
            Store chunk objects using this object store storage class.
            Currently not supported in GCS.
            Defaults to storage_class.

        minimum_size_for_multipart_upload: int | None
            Use object store's multipart upload for objects larger than this size in bytes.
            Default: 100 MB if None is passed.
        """
        ...
    @property
    def concurrency(self) -> StorageConcurrencySettings | None:
        """
        The configuration for how much concurrency Icechunk store uses

        Returns
        -------
        StorageConcurrencySettings | None
            The configuration for how Icechunk uses its Storage instance.
        """

    @concurrency.setter
    def concurrency(self, value: StorageConcurrencySettings | None) -> None: ...
    @property
    def retries(self) -> StorageRetriesSettings | None:
        """
        The configuration for how Icechunk retries failed requests.

        Returns
        -------
        StorageRetriesSettings | None
            The configuration for how Icechunk retries failed requests.
        """

    @retries.setter
    def retries(self, value: StorageRetriesSettings | None) -> None: ...
    @property
    def unsafe_use_conditional_update(self) -> bool | None:
        """True if Icechunk will use conditional PUT operations for updates in the object store"""
        ...

    @unsafe_use_conditional_update.setter
    def unsafe_use_conditional_update(self, value: bool) -> None: ...
    @property
    def unsafe_use_conditional_create(self) -> bool | None:
        """True if Icechunk will use conditional PUT operations for creation in the object store"""
        ...

    @unsafe_use_conditional_create.setter
    def unsafe_use_conditional_create(self, value: bool) -> None: ...
    @property
    def unsafe_use_metadata(self) -> bool | None:
        """True if Icechunk will write object metadata in the object store"""
        ...

    @unsafe_use_metadata.setter
    def unsafe_use_metadata(self, value: bool) -> None: ...
    @property
    def storage_class(self) -> str | None:
        """All objects in object store will use this storage class or the default if None"""
        ...

    @storage_class.setter
    def storage_class(self, value: str) -> None: ...
    @property
    def metadata_storage_class(self) -> str | None:
        """Metadata objects in object store will use this storage class or self.storage_class if None"""
        ...

    @metadata_storage_class.setter
    def metadata_storage_class(self, value: str) -> None: ...
    @property
    def chunks_storage_class(self) -> str | None:
        """Chunk objects in object store will use this storage class or self.storage_class if None"""
        ...

    @chunks_storage_class.setter
    def chunks_storage_class(self, value: str) -> None: ...
    @property
    def minimum_size_for_multipart_upload(self) -> int | None:
        """Use object store's multipart upload for objects larger than this size in bytes"""
        ...

    @minimum_size_for_multipart_upload.setter
    def minimum_size_for_multipart_upload(self, value: int) -> None: ...

class RepositoryConfig:
    """Configuration for an Icechunk repository"""

    def __init__(
        self,
        inline_chunk_threshold_bytes: int | None = None,
        get_partial_values_concurrency: int | None = None,
        compression: CompressionConfig | None = None,
        caching: CachingConfig | None = None,
        storage: StorageSettings | None = None,
        virtual_chunk_containers: dict[str, VirtualChunkContainer] | None = None,
        manifest: ManifestConfig | None = None,
    ) -> None:
        """
        Create a new `RepositoryConfig` object

        Parameters
        ----------
        inline_chunk_threshold_bytes: int | None
            The maximum size of a chunk that will be stored inline in the repository.
        get_partial_values_concurrency: int | None
            The number of concurrent requests to make when getting partial values from storage.
        compression: CompressionConfig | None
            The compression configuration for the repository.
        caching: CachingConfig | None
            The caching configuration for the repository.
        storage: StorageSettings | None
            The storage configuration for the repository.
        virtual_chunk_containers: dict[str, VirtualChunkContainer] | None
            The virtual chunk containers for the repository.
        manifest: ManifestConfig | None
            The manifest configuration for the repository.
        """
        ...
    @staticmethod
    def default() -> RepositoryConfig:
        """Create a default repository config instance"""
        ...
    @property
    def inline_chunk_threshold_bytes(self) -> int | None:
        """
        The maximum size of a chunk that will be stored inline in the repository. Chunks larger than this size will be written to storage.
        """
        ...
    @inline_chunk_threshold_bytes.setter
    def inline_chunk_threshold_bytes(self, value: int | None) -> None:
        """
        Set the maximum size of a chunk that will be stored inline in the repository. Chunks larger than this size will be written to storage.
        """
        ...
    @property
    def get_partial_values_concurrency(self) -> int | None:
        """
        The number of concurrent requests to make when getting partial values from storage.

        Returns
        -------
        int | None
            The number of concurrent requests to make when getting partial values from storage.
        """
        ...
    @get_partial_values_concurrency.setter
    def get_partial_values_concurrency(self, value: int | None) -> None:
        """
        Set the number of concurrent requests to make when getting partial values from storage.

        Parameters
        ----------
        value: int | None
            The number of concurrent requests to make when getting partial values from storage.
        """
        ...
    @property
    def compression(self) -> CompressionConfig | None:
        """
        The compression configuration for the repository.

        Returns
        -------
        CompressionConfig | None
            The compression configuration for the repository.
        """
        ...
    @compression.setter
    def compression(self, value: CompressionConfig | None) -> None:
        """
        Set the compression configuration for the repository.

        Parameters
        ----------
        value: CompressionConfig | None
            The compression configuration for the repository.
        """
        ...
    @property
    def caching(self) -> CachingConfig | None:
        """
        The caching configuration for the repository.

        Returns
        -------
        CachingConfig | None
            The caching configuration for the repository.
        """
        ...
    @caching.setter
    def caching(self, value: CachingConfig | None) -> None:
        """
        Set the caching configuration for the repository.

        Parameters
        ----------
        value: CachingConfig | None
            The caching configuration for the repository.
        """
        ...
    @property
    def storage(self) -> StorageSettings | None:
        """
        The storage configuration for the repository.

        Returns
        -------
        StorageSettings | None
            The storage configuration for the repository.
        """
        ...
    @storage.setter
    def storage(self, value: StorageSettings | None) -> None:
        """
        Set the storage configuration for the repository.

        Parameters
        ----------
        value: StorageSettings | None
            The storage configuration for the repository.
        """
        ...
    @property
    def manifest(self) -> ManifestConfig | None:
        """
        The manifest configuration for the repository.

        Returns
        -------
        ManifestConfig | None
            The manifest configuration for the repository.
        """
        ...
    @manifest.setter
    def manifest(self, value: ManifestConfig | None) -> None:
        """
        Set the manifest configuration for the repository.

        Parameters
        ----------
        value: ManifestConfig | None
            The manifest configuration for the repository.
        """
        ...
    @property
    def virtual_chunk_containers(self) -> dict[str, VirtualChunkContainer] | None:
        """
        The virtual chunk containers for the repository.

        Returns
        -------
        dict[str, VirtualChunkContainer] | None
            The virtual chunk containers for the repository.
        """
        ...
    def get_virtual_chunk_container(self, name: str) -> VirtualChunkContainer | None:
        """
        Get the virtual chunk container for the repository associated with the given name.

        Parameters
        ----------
        name: str
            The name of the virtual chunk container to get.

        Returns
        -------
        VirtualChunkContainer | None
            The virtual chunk container for the repository associated with the given name.
        """
        ...
    def set_virtual_chunk_container(self, cont: VirtualChunkContainer) -> None:
        """
        Set the virtual chunk container for the repository.

        Parameters
        ----------
        cont: VirtualChunkContainer
            The virtual chunk container to set.
        """
        ...
    def clear_virtual_chunk_containers(self) -> None:
        """
        Clear all virtual chunk containers from the repository.
        """
        ...

class Diff:
    """The result of comparing two snapshots"""
    @property
    def new_groups(self) -> set[str]:
        """
        The groups that were added to the target ref.
        """
        ...
    @property
    def new_arrays(self) -> set[str]:
        """
        The arrays that were added to the target ref.
        """
        ...
    @property
    def deleted_groups(self) -> set[str]:
        """
        The groups that were deleted in the target ref.
        """
        ...
    @property
    def deleted_arrays(self) -> set[str]:
        """
        The arrays that were deleted in the target ref.
        """
        ...
    @property
    def updated_groups(self) -> set[str]:
        """
        The groups that were updated via zarr metadata in the target ref.
        """
        ...
    @property
    def updated_arrays(self) -> set[str]:
        """
        The arrays that were updated via zarr metadata in the target ref.
        """
        ...
    @property
    def updated_chunks(self) -> dict[str, list[list[int]]]:
        """
        The chunks indices that had data updated in the target ref, keyed by the path to the array.
        """
        ...

class GCSummary:
    """Summarizes the results of a garbage collection operation on an icechunk repo"""
    @property
    def bytes_deleted(self) -> int:
        """
        How many bytes were deleted.
        """
        ...
    @property
    def chunks_deleted(self) -> int:
        """
        How many chunks were deleted.
        """
        ...
    @property
    def manifests_deleted(self) -> int:
        """
        How many manifests were deleted.
        """
        ...
    @property
    def snapshots_deleted(self) -> int:
        """
        How many snapshots were deleted.
        """
        ...
    @property
    def attributes_deleted(self) -> int:
        """
        How many attributes were deleted.
        """
        ...
    @property
    def transaction_logs_deleted(self) -> int:
        """
        How many transaction logs were deleted.
        """
        ...

class PyRepository:
    @classmethod
    def create(
        cls,
        storage: Storage,
        *,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> PyRepository: ...
    @classmethod
    def open(
        cls,
        storage: Storage,
        *,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> PyRepository: ...
    @classmethod
    def open_or_create(
        cls,
        storage: Storage,
        *,
        config: RepositoryConfig | None = None,
        authorize_virtual_chunk_access: dict[str, AnyCredential | None] | None = None,
    ) -> PyRepository: ...
    @staticmethod
    def exists(storage: Storage) -> bool: ...
    @classmethod
    def from_bytes(cls, data: bytes) -> PyRepository: ...
    def as_bytes(self) -> bytes: ...
    @staticmethod
    def fetch_config(storage: Storage) -> RepositoryConfig | None: ...
    def save_config(self) -> None: ...
    def config(self) -> RepositoryConfig: ...
    def storage(self) -> Storage: ...
    def set_default_commit_metadata(self, metadata: dict[str, Any]) -> None: ...
    def default_commit_metadata(self) -> dict[str, Any]: ...
    def async_ancestry(
        self,
        *,
        branch: str | None = None,
        tag: str | None = None,
        snapshot_id: str | None = None,
    ) -> AsyncIterator[SnapshotInfo]: ...
    def create_branch(self, branch: str, snapshot_id: str) -> None: ...
    def list_branches(self) -> set[str]: ...
    def lookup_branch(self, branch: str) -> str: ...
    def lookup_snapshot(self, snapshot_id: str) -> SnapshotInfo: ...
    def reset_branch(self, branch: str, snapshot_id: str) -> None: ...
    def delete_branch(self, branch: str) -> None: ...
    def delete_tag(self, tag: str) -> None: ...
    def create_tag(self, tag: str, snapshot_id: str) -> None: ...
    def list_tags(self) -> set[str]: ...
    def lookup_tag(self, tag: str) -> str: ...
    def diff(
        self,
        from_branch: str | None = None,
        from_tag: str | None = None,
        from_snapshot_id: str | None = None,
        to_branch: str | None = None,
        to_tag: str | None = None,
        to_snapshot_id: str | None = None,
    ) -> Diff: ...
    def readonly_session(
        self,
        branch: str | None = None,
        *,
        tag: str | None = None,
        snapshot_id: str | None = None,
        as_of: datetime.datetime | None = None,
    ) -> PySession: ...
    def writable_session(self, branch: str) -> PySession: ...
    def expire_snapshots(
        self,
        older_than: datetime.datetime,
        *,
        delete_expired_branches: bool = False,
        delete_expired_tags: bool = False,
    ) -> set[str]: ...
    def garbage_collect(
        self, delete_object_older_than: datetime.datetime
    ) -> GCSummary: ...
    def rewrite_manifests(
        self, message: str, *, branch: str, metadata: dict[str, Any] | None = None
    ) -> str: ...
    def total_chunks_storage(self) -> int: ...

class PySession:
    @classmethod
    def from_bytes(cls, data: bytes) -> PySession: ...
    def __eq__(self, value: object) -> bool: ...
    def as_bytes(self) -> bytes: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def snapshot_id(self) -> str: ...
    @property
    def branch(self) -> str | None: ...
    @property
    def has_uncommitted_changes(self) -> bool: ...
    def status(self) -> Diff: ...
    def discard_changes(self) -> None: ...
    def all_virtual_chunk_locations(self) -> list[str]: ...
    def chunk_coordinates(
        self, array_path: str, batch_size: int
    ) -> AsyncIterator[list[list[int]]]: ...
    @property
    def store(self) -> PyStore: ...
    def merge(self, other: PySession) -> None: ...
    def commit(
        self,
        message: str,
        metadata: dict[str, Any] | None = None,
        rebase_with: ConflictSolver | None = None,
        rebase_tries: int = 1_000,
    ) -> str: ...
    def rebase(self, solver: ConflictSolver) -> None: ...

class PyStore:
    @classmethod
    def from_bytes(cls, data: bytes) -> PyStore: ...
    def __eq__(self, value: object) -> bool: ...
    @property
    def read_only(self) -> bool: ...
    @property
    def session(self) -> PySession: ...
    def as_bytes(self) -> bytes: ...
    async def is_empty(self, prefix: str) -> bool: ...
    async def clear(self) -> None: ...
    def sync_clear(self) -> None: ...
    async def get(
        self, key: str, byte_range: tuple[int | None, int | None] | None = None
    ) -> bytes: ...
    async def get_partial_values(
        self, key_ranges: list[tuple[str, tuple[int | None, int | None]]]
    ) -> list[bytes]: ...
    async def exists(self, key: str) -> bool: ...
    @property
    def supports_writes(self) -> bool: ...
    @property
    def supports_consolidated_metadata(self) -> bool: ...
    @property
    def supports_deletes(self) -> bool: ...
    async def set(self, key: str, value: bytes) -> None: ...
    async def set_if_not_exists(self, key: str, value: bytes) -> None: ...
    def set_virtual_ref(
        self,
        key: str,
        location: str,
        offset: int,
        length: int,
        checksum: str | datetime.datetime | None = None,
        validate_container: bool = False,
    ) -> None: ...
    def set_virtual_refs(
        self,
        array_path: str,
        chunks: list[VirtualChunkSpec],
        validate_containers: bool,
    ) -> list[tuple[int, ...]] | None: ...
    async def delete(self, key: str) -> None: ...
    async def delete_dir(self, prefix: str) -> None: ...
    @property
    def supports_partial_writes(self) -> bool: ...
    async def set_partial_values(
        self, key_start_values: list[tuple[str, int, bytes]]
    ) -> None: ...
    @property
    def supports_listing(self) -> bool: ...
    def list(self) -> PyAsyncStringGenerator: ...
    def list_prefix(self, prefix: str) -> PyAsyncStringGenerator: ...
    def list_dir(self, prefix: str) -> PyAsyncStringGenerator: ...
    async def getsize(self, key: str) -> int: ...
    async def getsize_prefix(self, prefix: str) -> int: ...

class PyAsyncStringGenerator(AsyncGenerator[str, None], metaclass=abc.ABCMeta):
    def __aiter__(self) -> PyAsyncStringGenerator: ...
    async def __anext__(self) -> str: ...

class ManifestFileInfo:
    """Manifest file metadata"""

    @property
    def id(self) -> str:
        """The manifest id"""
        ...
    @property
    def size_bytes(self) -> int:
        """The size in bytes of the"""
        ...
    @property
    def num_chunk_refs(self) -> int:
        """The number of chunk references contained in this manifest"""
        ...

class SnapshotInfo:
    """Metadata for a snapshot"""
    @property
    def id(self) -> str:
        """The snapshot ID"""
        ...
    @property
    def parent_id(self) -> str | None:
        """The snapshot ID"""
        ...
    @property
    def written_at(self) -> datetime.datetime:
        """
        The timestamp when the snapshot was written
        """
        ...
    @property
    def message(self) -> str:
        """
        The commit message of the snapshot
        """
        ...
    @property
    def metadata(self) -> dict[str, Any]:
        """
        The metadata of the snapshot
        """
        ...
    @property
    def manifests(self) -> list[ManifestFileInfo]:
        """
        The manifests linked to this snapshot
        """
        ...

class PyAsyncSnapshotGenerator(AsyncGenerator[SnapshotInfo, None], metaclass=abc.ABCMeta):
    def __aiter__(self) -> PyAsyncSnapshotGenerator: ...
    async def __anext__(self) -> SnapshotInfo: ...

class S3StaticCredentials:
    """Credentials for an S3 storage backend

    Attributes:
        access_key_id: str
            The access key ID to use for authentication.
        secret_access_key: str
            The secret access key to use for authentication.
        session_token: str | None
            The session token to use for authentication.
        expires_after: datetime.datetime | None
            Optional, the expiration time of the credentials.
    """

    access_key_id: str
    secret_access_key: str
    session_token: str | None
    expires_after: datetime.datetime | None

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        session_token: str | None = None,
        expires_after: datetime.datetime | None = None,
    ):
        """
        Create a new `S3StaticCredentials` object

        Parameters
        ----------
        access_key_id: str
            The access key ID to use for authentication.
        secret_access_key: str
            The secret access key to use for authentication.
        session_token: str | None
            Optional, the session token to use for authentication.
        expires_after: datetime.datetime | None
            Optional, the expiration time of the credentials.
        """
        ...

class S3Credentials:
    """Credentials for an S3 storage backend"""
    class FromEnv:
        """Uses credentials from environment variables"""
        def __init__(self) -> None: ...

    class Anonymous:
        """Does not sign requests, useful for public buckets"""
        def __init__(self) -> None: ...

    class Static:
        """Uses s3 credentials without expiration

        Parameters
        ----------
        credentials: S3StaticCredentials
            The credentials to use for authentication.
        """
        def __init__(self, credentials: S3StaticCredentials) -> None: ...

    class Refreshable:
        """Allows for an outside authority to pass in a function that can be used to provide credentials.

        This is useful for credentials that have an expiration time, or are otherwise not known ahead of time.

        Parameters
        ----------
        pickled_function: bytes
            The pickled function to use to provide credentials.
        current: S3StaticCredentials
            The initial credentials. They will be returned the first time credentials
            are requested and then deleted.
        """
        def __init__(
            self, pickled_function: bytes, current: S3StaticCredentials | None = None
        ) -> None: ...

AnyS3Credential = (
    S3Credentials.Static
    | S3Credentials.Anonymous
    | S3Credentials.FromEnv
    | S3Credentials.Refreshable
)

class GcsBearerCredential:
    """Credentials for a google cloud storage backend

    This is a bearer token that has an expiration time.
    """

    bearer: str
    expires_after: datetime.datetime | None

    def __init__(
        self, bearer: str, *, expires_after: datetime.datetime | None = None
    ) -> None:
        """Create a GcsBearerCredential object

        Parameters
        ----------
        bearer: str
            The bearer token to use for authentication.
        expires_after: datetime.datetime | None
            The expiration time of the bearer token.
        """

class GcsStaticCredentials:
    """Credentials for a google cloud storage backend"""
    class ServiceAccount:
        """Credentials for a google cloud storage backend using a service account json file

        Parameters
        ----------
        path: str
            The path to the service account json file.
        """
        def __init__(self, path: str) -> None: ...

    class ServiceAccountKey:
        """Credentials for a google cloud storage backend using a a serialized service account key

        Parameters
        ----------
        key: str
            The serialized service account key.
        """
        def __init__(self, key: str) -> None: ...

    class ApplicationCredentials:
        """Credentials for a google cloud storage backend using application default credentials

        Parameters
        ----------
        path: str
            The path to the application default credentials (ADC) file.
        """
        def __init__(self, path: str) -> None: ...

    class BearerToken:
        """Credentials for a google cloud storage backend using a bearer token

        Parameters
        ----------
        token: str
            The bearer token to use for authentication.
        """
        def __init__(self, token: str) -> None: ...

AnyGcsStaticCredential = (
    GcsStaticCredentials.ServiceAccount
    | GcsStaticCredentials.ServiceAccountKey
    | GcsStaticCredentials.ApplicationCredentials
    | GcsStaticCredentials.BearerToken
)

class GcsCredentials:
    """Credentials for a google cloud storage backend

    This can be used to authenticate with a google cloud storage backend.
    """
    class FromEnv:
        """Uses credentials from environment variables"""
        def __init__(self) -> None: ...

    class Static:
        """Uses gcs credentials without expiration"""
        def __init__(self, credentials: AnyGcsStaticCredential) -> None: ...

    class Refreshable:
        """Allows for an outside authority to pass in a function that can be used to provide credentials.

        This is useful for credentials that have an expiration time, or are otherwise not known ahead of time.
        """
        def __init__(
            self, pickled_function: bytes, current: GcsBearerCredential | None = None
        ) -> None: ...

AnyGcsCredential = (
    GcsCredentials.FromEnv | GcsCredentials.Static | GcsCredentials.Refreshable
)

class AzureStaticCredentials:
    """Credentials for an azure storage backend"""
    class AccessKey:
        """Credentials for an azure storage backend using an access key

        Parameters
        ----------
        key: str
            The access key to use for authentication.
        """
        def __init__(self, key: str) -> None: ...

    class SasToken:
        """Credentials for an azure storage backend using a shared access signature token

        Parameters
        ----------
        token: str
            The shared access signature token to use for authentication.
        """
        def __init__(self, token: str) -> None: ...

    class BearerToken:
        """Credentials for an azure storage backend using a bearer token

        Parameters
        ----------
        token: str
            The bearer token to use for authentication.
        """
        def __init__(self, token: str) -> None: ...

AnyAzureStaticCredential = (
    AzureStaticCredentials.AccessKey
    | AzureStaticCredentials.SasToken
    | AzureStaticCredentials.BearerToken
)

class AzureCredentials:
    """Credentials for an azure storage backend

    This can be used to authenticate with an azure storage backend.
    """
    class FromEnv:
        """Uses credentials from environment variables"""
        def __init__(self) -> None: ...

    class Static:
        """Uses azure credentials without expiration"""
        def __init__(self, credentials: AnyAzureStaticCredential) -> None: ...

AnyAzureCredential = AzureCredentials.FromEnv | AzureCredentials.Static

class Credentials:
    class S3:
        def __init__(self, credentials: AnyS3Credential) -> None: ...

    class Gcs:
        def __init__(self, credentials: GcsCredentials) -> None: ...

    class Azure:
        def __init__(self, credentials: AzureCredentials) -> None: ...

AnyCredential = Credentials.S3 | Credentials.Gcs | Credentials.Azure

class Storage:
    """Storage configuration for an IcechunkStore

    Currently supports memory, filesystem S3, azure blob, and google cloud storage backends.
    Use the following methods to create a Storage object with the desired backend.

    Ex:
    ```
    storage = icechunk.in_memory_storage()
    storage = icechunk.local_filesystem_storage("/path/to/root")
    storage = icechunk.s3_storage("bucket", "prefix", ...)
    storage = icechunk.gcs_storage("bucket", "prefix", ...)
    storage = icechunk.azure_storage("container", "prefix", ...)
    ```
    """

    @classmethod
    def new_s3(
        cls,
        config: S3Options,
        bucket: str,
        prefix: str | None,
        credentials: AnyS3Credential | None = None,
    ) -> Storage: ...
    @classmethod
    def new_s3_object_store(
        cls,
        config: S3Options,
        bucket: str,
        prefix: str | None,
        credentials: AnyS3Credential | None = None,
    ) -> Storage: ...
    @classmethod
    def new_tigris(
        cls,
        config: S3Options,
        bucket: str,
        prefix: str | None,
        use_weak_consistency: bool,
        credentials: AnyS3Credential | None = None,
    ) -> Storage: ...
    @classmethod
    def new_in_memory(cls) -> Storage: ...
    @classmethod
    def new_local_filesystem(cls, path: str) -> Storage: ...
    @classmethod
    def new_gcs(
        cls,
        bucket: str,
        prefix: str | None,
        credentials: AnyGcsCredential | None = None,
        *,
        config: dict[str, str] | None = None,
    ) -> Storage: ...
    @classmethod
    def new_r2(
        cls,
        bucket: str | None,
        prefix: str | None,
        account_id: str | None,
        credentials: AnyS3Credential | None = None,
        *,
        config: S3Options,
    ) -> Storage: ...
    @classmethod
    def new_azure_blob(
        cls,
        account: str,
        container: str,
        prefix: str,
        credentials: AnyAzureCredential | None = None,
        *,
        config: dict[str, str] | None = None,
    ) -> Storage: ...
    def __repr__(self) -> str: ...
    def default_settings(self) -> StorageSettings: ...

class VersionSelection(Enum):
    """Enum for selecting the which version of a conflict

    Attributes
    ----------
    Fail: int
        Fail the rebase operation
    UseOurs: int
        Use the version from the source store
    UseTheirs: int
        Use the version from the target store
    """

    Fail = 0
    UseOurs = 1
    UseTheirs = 2

class ConflictSolver:
    """An abstract conflict solver that can be used to detect or resolve conflicts between two stores

    This should never be used directly, but should be subclassed to provide specific conflict resolution behavior
    """

    ...

class BasicConflictSolver(ConflictSolver):
    """A basic conflict solver that allows for simple configuration of resolution behavior

    This conflict solver allows for simple configuration of resolution behavior for conflicts that may occur during a rebase operation.
    It will attempt to resolve a limited set of conflicts based on the configuration options provided.

    - When a chunk conflict is encountered, the behavior is determined by the `on_chunk_conflict` option
    - When an array is deleted that has been updated, `fail_on_delete_of_updated_array` will determine whether to fail the rebase operation
    - When a group is deleted that has been updated, `fail_on_delete_of_updated_group` will determine whether to fail the rebase operation
    """

    def __init__(
        self,
        *,
        on_chunk_conflict: VersionSelection = VersionSelection.UseOurs,
        fail_on_delete_of_updated_array: bool = False,
        fail_on_delete_of_updated_group: bool = False,
    ) -> None:
        """Create a BasicConflictSolver object with the given configuration options

        Parameters
        ----------
        on_chunk_conflict: VersionSelection
            The behavior to use when a chunk conflict is encountered, by default VersionSelection.use_theirs()
        fail_on_delete_of_updated_array: bool
            Whether to fail when a chunk is deleted that has been updated, by default False
        fail_on_delete_of_updated_group: bool
            Whether to fail when a group is deleted that has been updated, by default False
        """
        ...

class ConflictDetector(ConflictSolver):
    """A conflict solver that can be used to detect conflicts between two stores, but does not resolve them

    Where the `BasicConflictSolver` will attempt to resolve conflicts, the `ConflictDetector` will only detect them. This means
    that during a rebase operation the `ConflictDetector` will raise a `RebaseFailed` error if any conflicts are detected, and
    allow the rebase operation to be retried with a different conflict resolution strategy. Otherwise, if no conflicts are detected
    the rebase operation will succeed.
    """

    def __init__(self) -> None: ...

class IcechunkError(Exception):
    """Base class for all Icechunk errors"""

    @property
    def message(self) -> str: ...

class ConflictError(Exception):
    """An error that occurs when a conflict is detected"""

    @property
    def expected_parent(self) -> str:
        """The expected parent snapshot ID.

        This is the snapshot ID that the session was based on when the
        commit operation was called.
        """
        ...
    @property
    def actual_parent(self) -> str:
        """
        The actual parent snapshot ID of the branch that the session attempted to commit to.

        When the session is based on a branch, this is the snapshot ID of the branch tip. If this
        error is raised, it means the branch was modified and committed by another session after
        the session was created.
        """
        ...
    ...

__version__: str

class ConflictType(Enum):
    """Type of conflict detected

    Attributes:
        NewNodeConflictsWithExistingNode: int
            A new node conflicts with an existing node
        NewNodeInInvalidGroup: tuple[int]
            A new node is in an invalid group
        ZarrMetadataDoubleUpdate: tuple[int]
            A zarr metadata update conflicts with an existing zarr metadata update
        ZarrMetadataUpdateOfDeletedArray: tuple[int]
            A zarr metadata update is attempted on a deleted array
        ZarrMetadataUpdateOfDeletedGroup: tuple[int]
            A zarr metadata update is attempted on a deleted group
        ChunkDoubleUpdate: tuple[int]
            A chunk update conflicts with an existing chunk update
        ChunksUpdatedInDeletedArray: tuple[int]
            Chunks are updated in a deleted array
        ChunksUpdatedInUpdatedArray: tuple[int]
            Chunks are updated in an updated array
        DeleteOfUpdatedArray: tuple[int]
            A delete is attempted on an updated array
        DeleteOfUpdatedGroup: tuple[int]
            A delete is attempted on an updated group
    """

    NewNodeConflictsWithExistingNode = (1,)
    NewNodeInInvalidGroup = (2,)
    ZarrMetadataDoubleUpdate = (3,)
    ZarrMetadataUpdateOfDeletedArray = (4,)
    ZarrMetadataUpdateOfDeletedGroup = (5,)
    ChunkDoubleUpdate = (6,)
    ChunksUpdatedInDeletedArray = (7,)
    ChunksUpdatedInUpdatedArray = (8,)
    DeleteOfUpdatedArray = (9,)
    DeleteOfUpdatedGroup = (10,)

class Conflict:
    """A conflict detected between snapshots"""

    @property
    def conflict_type(self) -> ConflictType:
        """The type of conflict detected

        Returns:
            ConflictType: The type of conflict detected
        """
        ...

    @property
    def path(self) -> str:
        """The path of the node that caused the conflict

        Returns:
            str: The path of the node that caused the conflict
        """
        ...

    @property
    def conflicted_chunks(self) -> list[list[int]] | None:
        """If the conflict is a chunk conflict, this will return the list of chunk indices that are in conflict

        Returns:
            list[list[int]] | None: The list of chunk indices that are in conflict
        """
        ...

class RebaseFailedError(IcechunkError):
    """An error that occurs when a rebase operation fails"""

    @property
    def snapshot(self) -> str:
        """The snapshot ID that the session was rebased to"""
        ...

    @property
    def conflicts(self) -> list[Conflict]:
        """The conflicts that occurred during the rebase operation

        Returns:
            list[Conflict]: The conflicts that occurred during the rebase operation
        """
    ...

def initialize_logs() -> None:
    """
    Initialize the logging system for the library.

    Reads the value of the environment variable ICECHUNK_LOG to obtain the filters.
    This is autamtically called on `import icechunk`.
    """
    ...

def set_logs_filter(log_filter_directive: str | None) -> None:
    """
    Set filters and log levels for the different modules.

    Examples:
      - set_logs_filter("trace")  # trace level for all modules
      - set_logs_filter("error")  # error level for all modules
      - set_logs_filter("icechunk=debug,info")  # debug level for icechunk, info for everything else

    Full spec for the log_filter_directive syntax is documented in
    https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives

    Parameters
    ----------
    log_filter_directive: str | None
        The comma separated list of directives for modules and log levels.
        If None, the directive will be read from the environment variable
        ICECHUNK_LOG
    """
    ...

def spec_version() -> int:
    """
    The version of the Icechunk specification that the library is compatible with.

    Returns:
        int: The version of the Icechunk specification that the library is compatible with
    """
    ...
