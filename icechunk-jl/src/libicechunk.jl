# Low-level ccall wrappers for the icechunk C API.
# These map 1:1 to the functions declared in icechunk.h.

module LibIcechunk

using Libdl

# --- Library path -----------------------------------------------------------

# Resolve the path to libicechunk_c at module load time.  We look relative to
# the repository root so the package works from a local checkout without any
# JLL plumbing.
const _REPO_ROOT = abspath(joinpath(@__DIR__, "..", ".."))

function _find_lib()
    for profile in ("debug", "release")
        p = joinpath(_REPO_ROOT, "target", profile, "libicechunk_c")
        handle = Libdl.dlopen(p; throw_error=false)
        handle !== nothing && return p
    end
    error("""
        Could not find libicechunk_c.  Build it first with:
            cargo build -p icechunk-c
        from $(_REPO_ROOT)
    """)
end

# Module-level String variable, set in __init__. @ccall references it as a
# bare identifier (same pattern as JLL packages like GEOS_jll).
libicechunk::String = ""

function __init__()
    global libicechunk = _find_lib()
    Libdl.dlopen(libicechunk)
end

# --- Return codes ------------------------------------------------------------

const SUCCESS       = Int32(0)
const ERROR         = Int32(-1)
const NOT_FOUND     = Int32(-2)
const READONLY      = Int32(-3)
const NULL_ARGUMENT = Int32(-4)

# --- Error handling ----------------------------------------------------------

function last_error()
    ptr = @ccall libicechunk.icechunk_last_error()::Ptr{Cchar}
    ptr == C_NULL ? nothing : unsafe_string(ptr)
end

# --- Storage -----------------------------------------------------------------

function storage_new_in_memory()
    ptr = @ccall libicechunk.icechunk_storage_new_in_memory()::Ptr{Cvoid}
    ptr == C_NULL && error("icechunk: failed to create in-memory storage: $(something(last_error(), "unknown"))")
    ptr
end

function storage_new_local_filesystem(path::AbstractString)
    ptr = @ccall libicechunk.icechunk_storage_new_local_filesystem(path::Cstring)::Ptr{Cvoid}
    ptr == C_NULL && error("icechunk: failed to create local filesystem storage: $(something(last_error(), "unknown"))")
    ptr
end

function storage_free(ptr::Ptr{Cvoid})
    @ccall libicechunk.icechunk_storage_free(ptr::Ptr{Cvoid})::Cvoid
end

# --- Store -------------------------------------------------------------------

function store_open(storage::Ptr{Cvoid}, branch::AbstractString)
    ptr = @ccall libicechunk.icechunk_store_open(storage::Ptr{Cvoid}, branch::Cstring)::Ptr{Cvoid}
    ptr == C_NULL && error("icechunk: failed to open store: $(something(last_error(), "unknown"))")
    ptr
end

function store_free(ptr::Ptr{Cvoid})
    @ccall libicechunk.icechunk_store_free(ptr::Ptr{Cvoid})::Cvoid
end

function store_is_read_only(store::Ptr{Cvoid})
    rc = @ccall libicechunk.icechunk_store_is_read_only(store::Ptr{Cvoid})::Int32
    rc < 0 && error("icechunk: is_read_only failed: $(something(last_error(), "unknown"))")
    rc == 1
end

# --- Key-value operations ----------------------------------------------------

function store_get(store::Ptr{Cvoid}, key::AbstractString)
    out_data = Ref{Ptr{UInt8}}(C_NULL)
    out_len = Ref{Csize_t}(0)
    rc = @ccall libicechunk.icechunk_store_get(
        store::Ptr{Cvoid}, key::Cstring,
        out_data::Ptr{Ptr{UInt8}}, out_len::Ptr{Csize_t}
    )::Int32
    if rc == NOT_FOUND
        return nothing
    elseif rc < 0
        error("icechunk: get failed: $(something(last_error(), "unknown"))")
    end
    data_ptr = out_data[]
    len = out_len[]
    if data_ptr == C_NULL || len == 0
        return UInt8[]
    end
    # Copy into Julia-managed memory, then free the C buffer.
    result = Vector{UInt8}(undef, len)
    unsafe_copyto!(pointer(result), data_ptr, len)
    Libc.free(data_ptr)
    result
end

function store_set(store::Ptr{Cvoid}, key::AbstractString, data::Vector{UInt8})
    rc = @ccall libicechunk.icechunk_store_set(
        store::Ptr{Cvoid}, key::Cstring,
        data::Ptr{UInt8}, length(data)::Csize_t
    )::Int32
    rc < 0 && error("icechunk: set failed: $(something(last_error(), "unknown"))")
    nothing
end

function store_exists(store::Ptr{Cvoid}, key::AbstractString)
    rc = @ccall libicechunk.icechunk_store_exists(store::Ptr{Cvoid}, key::Cstring)::Int32
    rc < 0 && error("icechunk: exists failed: $(something(last_error(), "unknown"))")
    rc == 1
end

function store_delete(store::Ptr{Cvoid}, key::AbstractString)
    rc = @ccall libicechunk.icechunk_store_delete(store::Ptr{Cvoid}, key::Cstring)::Int32
    rc < 0 && error("icechunk: delete failed: $(something(last_error(), "unknown"))")
    nothing
end

# --- Listing -----------------------------------------------------------------

function store_list(store::Ptr{Cvoid})
    ptr = @ccall libicechunk.icechunk_store_list(store::Ptr{Cvoid})::Ptr{Cvoid}
    ptr == C_NULL && error("icechunk: list failed: $(something(last_error(), "unknown"))")
    ptr
end

function store_list_prefix(store::Ptr{Cvoid}, prefix::AbstractString)
    ptr = @ccall libicechunk.icechunk_store_list_prefix(store::Ptr{Cvoid}, prefix::Cstring)::Ptr{Cvoid}
    ptr == C_NULL && error("icechunk: list_prefix failed: $(something(last_error(), "unknown"))")
    ptr
end

function store_list_dir(store::Ptr{Cvoid}, prefix::AbstractString)
    ptr = @ccall libicechunk.icechunk_store_list_dir(store::Ptr{Cvoid}, prefix::Cstring)::Ptr{Cvoid}
    ptr == C_NULL && error("icechunk: list_dir failed: $(something(last_error(), "unknown"))")
    ptr
end

function store_list_next(iter::Ptr{Cvoid})
    out_key = Ref{Ptr{Cchar}}(C_NULL)
    rc = @ccall libicechunk.icechunk_store_list_next(
        iter::Ptr{Cvoid}, out_key::Ptr{Ptr{Cchar}}
    )::Int32
    rc < 0 && error("icechunk: list_next failed: $(something(last_error(), "unknown"))")
    key_ptr = out_key[]
    if key_ptr == C_NULL
        return nothing  # iteration exhausted
    end
    key = unsafe_string(key_ptr)
    Libc.free(key_ptr)
    key
end

function store_list_free(iter::Ptr{Cvoid})
    @ccall libicechunk.icechunk_store_list_free(iter::Ptr{Cvoid})::Cvoid
end

end # module LibIcechunk
