using IceChunk

macro icechunk_lib()
    joinpath(@__DIR__, "..", "target", "release", "libicechunk.so")
end

x = Ref(Cint(2))
ret = @ccall @icechunk_lib().icechunk_hello_plus(x::Ptr{Cint})::Cint
@show ret
@show x

ptr = Ref{Ptr{Cvoid}}()
ret2 = @ccall @icechunk_lib().create_inmemory_repository(ptr::Ptr{Ptr{Cvoid}})::Cint
@show ret2

# repo = IceChunk.icechunk_create_inmemory_repository()
# @show repo.ptr

# IceChunk.icechunk_free_repository(repo)
