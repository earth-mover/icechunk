pub fn dataclass_repr(class_name: &str, attributes: &[(&str, &str)]) -> String {
    // Writes a python-like (non-executable) repr, given a class name and an (ordered) mapping of name, attribute pairs.
    //
    // Result of:
    //
    // dataclass_repr(
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
