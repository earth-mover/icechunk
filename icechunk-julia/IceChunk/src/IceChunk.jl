module IceChunk
export icechunk_add_group, icechunk_add_root_group, icechunk_create_inmemory_repository, icechunk_free_repository
macro icechunk_lib()
    if Sys.islinux()
        joinpath(@__DIR__, "..", "..", "..", "target", "release", "libicechunk.so")
    elseif Sys.isapple()
        joinpath(@__DIR__, "..", "..", "..", "target", "release", "libicechunk.so")
    end
end

mutable struct Repository
    ptr::Ptr{Cvoid}
end

function icechunk_create_inmemory_repository()
    ptr = Ref{Ptr{Cvoid}}()
    ret = @ccall @icechunk_lib().create_inmemory_repository(ptr::Ptr{Ptr{Cvoid}})::Cint
    ret !== Cint(0) && error("Somthing gone wrong")
    Repository(ptr[])
end

function icechunk_free_repository(r::Repository)
    ret = @ccall @icechunk_lib().icechunk_free_repository(r.ptr::Ptr{Cvoid})::Cint
    ret !== Cint(0) && error("Somthing gone wrong")
    r.ptr = C_NULL
end

function icechunk_add_root_group(repository::Repository)
    ret = @ccall @icechunk_lib().icechunk_add_root_group(repository.ptr::Ptr{Cvoid})::Cint
    ret !== Cint(0) && error("Somthing gone wrong")
    ret
end

function icechunk_add_group(repository::Repository, name::String)
    ret = @ccall @icechunk_lib().icechunk_add_group(repository.ptr::Ptr{Cvoid}, name::Cstring)::Cint
    ret !== Cint(0) && error("Somthing gone wrong")
    ret
end

end