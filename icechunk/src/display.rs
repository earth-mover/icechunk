#[macro_export]
macro_rules! write_dataclass_repr {
    // Writes a python-like (non-executable) repr, given a class name and set of attributes.
    //
    // Result of:
    //
    // write_dataclass_repr!(
    //     f,
    //     "icechunk.Session",
    //     "read_only": self.read_only(),
    //     "snapshot_id": self.snapshot_id(),
    // )
    //
    // Looks like:
    //
    // <icechunk.Session>
    // read_only: true
    // snapshot_id: 1CECHNKREP0F1RSTCMT0

    ($f:expr, $class_name:literal, $($field_name:literal: $field_value:expr),+ $(,)?) => {
        write!(
            $f,
            "<{}>\n{}",
            $class_name,
            [$(format!("{}: {}", $field_name, $field_value)),+].join("\n"),
        )
    };
}
