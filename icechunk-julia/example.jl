#using IceChunk

macro icechunk_lib()
    joinpath(@__DIR__, "..", "target", "release", "libicechunk.dylib")
end


ptr = Ref{Ptr{Cvoid}}()
@show ptr[]
ret = @ccall @icechunk_lib().create_inmemory_repository(ptr::Ptr{Ptr{Cvoid}})::Cint
@show ptr[]
@assert ret===Cint(0) 


ret = @ccall @icechunk_lib().icechunk_add_root_group(ptr[]::Ptr{Cvoid})::Cint
@assert ret === Cint(0)



name = "/group_a"
ret = @ccall @icechunk_lib().icechunk_add_group(ptr[]::Ptr{Cvoid}, name::Cstring)::Cint
#repo = IceChunk.icechunk_create_inmemory_repository()
@show ret


@ccall @icechunk_lib().icechunk_free_repository(ptr[]::Ptr{Cvoid})::Cint