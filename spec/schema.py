import pyarrow as pa

structure_schema = pa.schema(
    [
        pa.field("id", pa.uint16(), nullable=False, metadata={"description": "unique identifier for the node"}),
        pa.field("type", pa.string(), nullable=False, metadata={"description": "array or group"}),
        pa.field("path", pa.string(), nullable=False, metadata={"description": "path to the node within the store"}),
        pa.field(
            "array_metadata",
            pa.struct(
                [
                    pa.field("shape", pa.list_(pa.uint16()), nullable=False),
                    pa.field("data_type", pa.string(), nullable=False),
                    pa.field("fill_value", pa.binary(), nullable=True),
                    pa.field("dimension_names", pa.list_(pa.string())),
                    pa.field(
                        "chunk_grid",
                        pa.struct(
                            [
                                pa.field("name", pa.string(), nullable=False),
                                pa.field(
                                    "configuration",
                                    pa.struct(
                                        [
                                             pa.field("chunk_shape", pa.list_(pa.uint16()), nullable=False),
                                        ]
                                    ),
                                    nullable=False
                                )
                            ]
                        )
                    ),
                    pa.field(
                        "chunk_key_encoding",
                        pa.struct(
                            [
                                pa.field("name", pa.string(), nullable=False),
                                pa.field(
                                    "configuration",
                                    pa.struct(
                                        [
                                             pa.field("separator", pa.string(), nullable=False),
                                        ]
                                    ),
                                    nullable=False
                                )
                            ]
                        )
                    ),
                    pa.field(
                        "codecs",
                        pa.list_(
                            pa.struct(
                                [
                                    pa.field("name", pa.string(), nullable=False),
                                    pa.field("configuration", pa.binary(), nullable=True)
                                ]
                            )
                        )
                    )
                ]
            ),
            nullable=True,
            metadata={"description": "All the Zarr array metadata"}
        ),
        pa.field("inline_attrs", pa.binary(), nullable=True, metadata={"description": "user-defined attributes, stored inline with this entry"}),
        pa.field(
            "attrs_reference",
            pa.struct(
                [
                    pa.field("attrs_file", pa.string(), nullable=False),
                    pa.field("row", pa.uint16(), nullable=False),
                    pa.field("flags", pa.uint16(), nullable=True)
                ]
            ),
            nullable=True,
            metadata={"description": "user-defined attributes, stored in a separate attributes file"}
        ),
        pa.field(
            "inventories",
            pa.list_(
                pa.struct(
                    [
                        pa.field("inventory_file", pa.string(), nullable=False),
                        pa.field("row", pa.uint16(), nullable=False),
                        pa.field("extent", pa.list_(pa.list_(pa.uint16(), 2)), nullable=False),
                        pa.field("flags", pa.uint16(), nullable=True)
                    ]
                )
            ),
            nullable=True
        ),
    ]
)

print(structure_schema)

manifest_schema = pa.schema(
    [
        pa.field("id", pa.uint32(), nullable=False),
        pa.field("array_id", pa.uint32(), nullable=False),
        pa.field("coord", pa.binary(), nullable=False),
        pa.field("inline_data", pa.binary(), nullable=True),
        pa.field("chunk_file", pa.string(), nullable=True),
        pa.field("offset", pa.uint64(), nullable=True),
        pa.field("length", pa.uint32(), nullable=False)
    ]
)

print(manifest_schema)