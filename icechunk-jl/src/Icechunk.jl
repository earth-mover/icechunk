module Icechunk

include("libicechunk.jl")
using .LibIcechunk

export IcechunkStorage, IcechunkStore
export in_memory_storage, local_filesystem_storage
export store_open, isreadonly, list_prefix, list_dir

# =============================================================================
# IcechunkStorage — wraps an opaque storage handle with a finalizer.
# =============================================================================

"""
    IcechunkStorage

Opaque handle to an Icechunk storage backend (in-memory, local filesystem, S3, etc.).
Automatically freed when garbage collected.
"""
mutable struct IcechunkStorage
    ptr::Ptr{Cvoid}
    function IcechunkStorage(ptr::Ptr{Cvoid})
        obj = new(ptr)
        finalizer(obj) do s
            if s.ptr != C_NULL
                LibIcechunk.storage_free(s.ptr)
                s.ptr = C_NULL
            end
        end
        obj
    end
end

"""
    in_memory_storage() -> IcechunkStorage

Create an in-memory storage backend (useful for testing).
"""
in_memory_storage() = IcechunkStorage(LibIcechunk.storage_new_in_memory())

"""
    local_filesystem_storage(path::AbstractString) -> IcechunkStorage

Create a local filesystem storage backend rooted at `path`.
"""
local_filesystem_storage(path::AbstractString) = IcechunkStorage(LibIcechunk.storage_new_local_filesystem(path))

# =============================================================================
# IcechunkStore — the Zarr-compatible key-value store.
# =============================================================================

"""
    IcechunkStore

A Zarr-compatible key-value store backed by an Icechunk repository session.
Automatically freed when garbage collected.

# Usage
```julia
storage = in_memory_storage()
store = store_open(storage, "main")
store["myarray/zarr.json"] = Vector{UInt8}(metadata_json)
data = store["myarray/c/0"]
```
"""
mutable struct IcechunkStore
    ptr::Ptr{Cvoid}
    function IcechunkStore(ptr::Ptr{Cvoid})
        obj = new(ptr)
        finalizer(obj) do s
            if s.ptr != C_NULL
                LibIcechunk.store_free(s.ptr)
                s.ptr = C_NULL
            end
        end
        obj
    end
end

"""
    store_open(storage::IcechunkStorage, branch::AbstractString="main") -> IcechunkStore

Create a new repository on `storage` and open a writable session on `branch`.

**Note:** `storage` is consumed by this call — do not use it afterwards.
"""
function store_open(storage::IcechunkStorage, branch::AbstractString="main")
    ptr = LibIcechunk.store_open(storage.ptr, branch)
    # The C side consumed the storage; null it out so the finalizer doesn't double-free.
    storage.ptr = C_NULL
    IcechunkStore(ptr)
end

"""
    isreadonly(store::IcechunkStore) -> Bool

Return `true` if the store is read-only.
"""
isreadonly(store::IcechunkStore) = LibIcechunk.store_is_read_only(store.ptr)

# --- Dict-like interface -----------------------------------------------------

"""
    store[key::AbstractString] -> Vector{UInt8}

Get the raw bytes for a Zarr key.  Returns `nothing` if the key does not exist.
Throws on other errors.
"""
function Base.getindex(store::IcechunkStore, key::AbstractString)
    result = LibIcechunk.store_get(store.ptr, key)
    result === nothing && throw(KeyError(key))
    result
end

"""
    get(store::IcechunkStore, key::AbstractString, default) -> Vector{UInt8}

Get the raw bytes for a Zarr key, or return `default` if it does not exist.
"""
function Base.get(store::IcechunkStore, key::AbstractString, default)
    result = LibIcechunk.store_get(store.ptr, key)
    result === nothing ? default : result
end

"""
    store[key::AbstractString] = data::Vector{UInt8}

Set the raw bytes for a Zarr key.
"""
function Base.setindex!(store::IcechunkStore, data::Vector{UInt8}, key::AbstractString)
    LibIcechunk.store_set(store.ptr, key, data)
    data
end

"""
    haskey(store::IcechunkStore, key::AbstractString) -> Bool

Check if a key exists in the store.
"""
Base.haskey(store::IcechunkStore, key::AbstractString) = LibIcechunk.store_exists(store.ptr, key)

"""
    delete!(store::IcechunkStore, key::AbstractString) -> IcechunkStore

Delete a key from the store.  No-op if the key does not exist.
"""
function Base.delete!(store::IcechunkStore, key::AbstractString)
    LibIcechunk.store_delete(store.ptr, key)
    store
end

# --- Listing / iteration -----------------------------------------------------

"""
    keys(store::IcechunkStore) -> Vector{String}

Return all keys in the store.
"""
function Base.keys(store::IcechunkStore)
    _collect_iter(LibIcechunk.store_list(store.ptr))
end

"""
    list_prefix(store::IcechunkStore, prefix::AbstractString) -> Vector{String}

Return all keys that start with `prefix`.
"""
function list_prefix(store::IcechunkStore, prefix::AbstractString)
    _collect_iter(LibIcechunk.store_list_prefix(store.ptr, prefix))
end

"""
    list_dir(store::IcechunkStore, prefix::AbstractString) -> Vector{String}

Return directory-style entries under `prefix` (immediate children only).
"""
function list_dir(store::IcechunkStore, prefix::AbstractString)
    _collect_iter(LibIcechunk.store_list_dir(store.ptr, prefix))
end

# Shared helper: drain a C iterator into a Vector{String}.
function _collect_iter(iter_ptr::Ptr{Cvoid})
    result = String[]
    try
        while true
            key = LibIcechunk.store_list_next(iter_ptr)
            key === nothing && break
            push!(result, key)
        end
    finally
        LibIcechunk.store_list_free(iter_ptr)
    end
    result
end

end # module Icechunk
