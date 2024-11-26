using IceChunk

repo = IceChunk.icechunk_create_inmemory_repository()

IceChunk.icechunk_add_root_group(repo)

IceChunk.icechunk_add_group(repo, "/group_a")

IceChunk.icechunk_add_group(repo, "/group_b")

IceChunk.icechunk_free_repository(repo)
name = "/group_a"
