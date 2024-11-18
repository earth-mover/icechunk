#using IceChunk

macro icechunk_lib()
    joinpath(@__DIR__, "..", "target", "release", "libicechunk.dylib")
end


ptr = Ref{Ptr{Cvoid}}()
ret2 = @ccall @icechunk_lib().create_inmemory_repository(ptr::Ptr{Ptr{Cvoid}})::Cint
@show ret2

ptr[]

@ccall @icechunk_lib().icechunk_add_root_group(ptr[]::Ptr{Cvoid})::Cint




name = "/group_a"
ret = @ccall @icechunk_lib().icechunk_add_group(ptr[]::Ptr{Cvoid}, name::Cstring)::Cint
#repo = IceChunk.icechunk_create_inmemory_repository()
@show ret
@show ptr[]

@ccall @icechunk_lib().icechunk_free_repository(ptr[]::Ptr{Cvoid})::Cint
