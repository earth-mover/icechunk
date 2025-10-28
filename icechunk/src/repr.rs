pub trait PyRepr {
    fn __str__(&self) -> String;

    fn __repr__(&self) -> String;

    // TODO fn _repr_html_(&self) -> String;
}

pub fn dataclass_str(class_name: &str, attributes: &[(&str, &str)]) -> String {
    // Writes a python-like (non-executable) multi-line repr, given a class name and an (ordered) mapping of name, attribute pairs.
    //
    // Result of:
    //
    // dataclass_str(
    //     "icechunk.Session",
    //     &[
    //         ("read_only", &self.read_only().to_string()),
    //         ("snapshot_id", &self.snapshot_id().to_string()),
    //     ]
    // )
    //
    // Looks like:
    //
    // <icechunk.Session>
    // read_only: true
    // snapshot_id: 1CECHNKREP0F1RSTCMT0

    let attrs = attributes
        .iter()
        .map(|(name, value)| format!("\n{}: {}", name, value))
        .collect::<String>();

    format!("<{}>{}", class_name, attrs)
}

pub fn dataclass_repr(class_name: &str, attributes: &[(&str, &str)]) -> String {
    // Writes a python-like (executable) multi-line repr, given a class name and an (ordered) mapping of name, attribute pairs.
    //
    // Result of:
    //
    // dataclass_repr(
    //     "icechunk.Config",
    //     &[
    //         ("field1", &self.field1().to_string()),
    //         ("field2", &self.field2().to_string()),
    //     ]
    // )
    //
    // Looks like:
    //
    // icechunk.Config(
    //     field1=value,
    //     field2=value,
    // )

    let attrs = attributes
        .iter()
        .map(|(name, value)| format!("\n    {}={}", name, value))
        .collect::<String>();

    format!("{}({}\n)", class_name, attrs)
}

// TODO: consolidate these two functions?
