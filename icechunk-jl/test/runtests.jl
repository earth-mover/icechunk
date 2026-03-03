using Test
using Icechunk

const ZARR_METADATA = Vector{UInt8}("""
{"zarr_format":3,"node_type":"array","shape":[4],"data_type":"float32",
 "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
 "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
 "fill_value":0.0,
 "codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}
""")

# Metadata for an array with 2 chunks (shape [8], chunk_shape [4])
const ZARR_METADATA_2CHUNKS = Vector{UInt8}("""
{"zarr_format":3,"node_type":"array","shape":[8],"data_type":"int32",
 "chunk_grid":{"name":"regular","configuration":{"chunk_shape":[4]}},
 "chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},
 "fill_value":0,
 "codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}
""")

@testset "Icechunk.jl" begin

    @testset "Storage creation" begin
        storage = in_memory_storage()
        @test storage.ptr != C_NULL
    end

    @testset "Store open" begin
        storage = in_memory_storage()
        store = store_open(storage, "main")
        @test store.ptr != C_NULL
        @test storage.ptr == C_NULL  # consumed
        @test !Icechunk.isreadonly(store)
    end

    @testset "Store open (default branch)" begin
        storage = in_memory_storage()
        store = store_open(storage)
        @test store.ptr != C_NULL
    end

    @testset "Set and get round-trip" begin
        store = store_open(in_memory_storage())

        # Set array metadata
        store["myarray/zarr.json"] = ZARR_METADATA

        # Set chunk data (4 float32s: 1.0, 2.0, 3.0, 4.0 in little-endian)
        chunk = UInt8[0x00, 0x00, 0x80, 0x3f,   # 1.0f
                      0x00, 0x00, 0x00, 0x40,   # 2.0f
                      0x00, 0x00, 0x40, 0x40,   # 3.0f
                      0x00, 0x00, 0x80, 0x40]   # 4.0f
        store["myarray/c/0"] = chunk

        # Read it back
        result = store["myarray/c/0"]
        @test result == chunk
        @test length(result) == 16

        # Also verify metadata round-trips
        meta_result = store["myarray/zarr.json"]
        @test meta_result == ZARR_METADATA
    end

    @testset "haskey / exists" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA

        chunk = UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        store["arr/c/0"] = chunk

        @test haskey(store, "arr/c/0")
        @test haskey(store, "arr/zarr.json")
        @test !haskey(store, "arr/c/99")
    end

    @testset "delete!" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA
        store["arr/c/0"] = UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

        @test haskey(store, "arr/c/0")
        delete!(store, "arr/c/0")
        @test !haskey(store, "arr/c/0")

        # Metadata should still exist
        @test haskey(store, "arr/zarr.json")
    end

    @testset "get with default" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA

        # Chunk does not exist — should return default
        result = get(store, "arr/c/0", UInt8[])
        @test result == UInt8[]

        # Metadata exists — should return actual data
        result = get(store, "arr/zarr.json", UInt8[])
        @test result == ZARR_METADATA
    end

    @testset "getindex throws on missing key" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA
        # Use a valid Zarr key path that just doesn't have data
        @test_throws KeyError store["arr/c/0"]
    end

    @testset "keys / list" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA_2CHUNKS
        store["arr/c/0"] = UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        store["arr/c/1"] = UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

        k = keys(store)
        @test length(k) >= 3
        @test "arr/zarr.json" in k
        @test "arr/c/0" in k
        @test "arr/c/1" in k
    end

    @testset "list_prefix" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA_2CHUNKS
        store["arr/c/0"] = UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

        # list_prefix must point to a group or array node, not "arr/c"
        k = Icechunk.list_prefix(store, "arr")
        @test "arr/zarr.json" in k
        @test "arr/c/0" in k
    end

    @testset "list_dir" begin
        store = store_open(in_memory_storage())
        store["arr/zarr.json"] = ZARR_METADATA
        store["arr/c/0"] = UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

        entries = Icechunk.list_dir(store, "arr")
        @test "zarr.json" in entries || "arr/zarr.json" in entries
        @test "c" in entries || "arr/c" in entries
    end

    @testset "Empty store" begin
        store = store_open(in_memory_storage())

        k = keys(store)
        @test isempty(k)
    end

    @testset "Local filesystem storage" begin
        dir = mktempdir()
        storage = local_filesystem_storage(dir)
        store = store_open(storage, "main")

        store["data/zarr.json"] = ZARR_METADATA
        store["data/c/0"] = UInt8[42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57]

        result = store["data/c/0"]
        @test result == UInt8[42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57]
    end

end
