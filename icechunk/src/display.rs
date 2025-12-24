use std::fmt::Write as _;

/// Render a non-executable, human-readable string for a dataclass-like struct.
///
/// `type_name` is the public Python/Rust-facing type name, e.g. "icechunk.RepositoryConfig".
/// `fields` are pairs of (field_name, rendered_value), where `rendered_value` should
/// already be a string representation appropriate for the nested type.
///
/// This function returns a multi-line string like:
/// <icechunk.RepositoryConfig>\n
/// caching: \n
///     <icechunk.CachingConfig>\n
///     num_snapshot_nodes: 100\n
pub fn dataclass_str(type_name: &str, fields: &[(&str, String)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "<{}>", type_name);
    for (key, value) in fields {
        // If the value contains newlines, indent the nested block
        if value.contains('\n') {
            let _ = writeln!(out, "{}:", key);
            for line in value.lines() {
                let _ = writeln!(out, "    {}", line);
            }
        } else {
            let _ = writeln!(out, "{}: {}", key, value);
        }
    }
    out
}

/// Render an executable-style Python repr for a dataclass-like struct.
///
/// Produces something like:
/// icechunk.RepositoryConfig(caching=icechunk.CachingConfig(num_snapshot_nodes=100), ...)
///
/// Values should already be provided as executable string fragments for nested types.
pub fn dataclass_repr(type_name: &str, fields: &[(&str, String)]) -> String {
    let mut out = String::new();
    let _ = write!(out, "{}(", type_name);
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            let _ = write!(out, ", ");
        }
        let _ = write!(out, "{}={}", key, value);
    }
    let _ = write!(out, ")");
    out
}

/// Render a static HTML representation suitable for Jupyter `_repr_html_`.
///
/// Uses a `<details><summary>` pattern for nested values that contain newlines.
/// Nested HTML fragments should be passed in via `fields` values when available; otherwise
/// plain text will be HTML-escaped minimally.
pub fn dataclass_html_repr(type_name: &str, fields: &[(&str, String)]) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "<div class=\"icechunk-repr\">\n  <code>&lt;{}&gt;</code>",
        type_name
    );
    for (key, value) in fields {
        if value.contains('\n') {
            let _ = writeln!(
                out,
                "  <div class=\"field\">\n    <details>\n      <summary><strong>{}</strong></summary>",
                key
            );
            // Indent nested lines inside a pre/code block for readability
            let _ = writeln!(out, "      <pre><code>");
            for line in value.lines() {
                // Basic escape for angle brackets to avoid HTML injection in plain text
                let escaped = line.replace('<', "&lt;").replace('>', "&gt;");
                let _ = writeln!(out, "{}", escaped);
            }
            let _ = writeln!(out, "      </code></pre>\n    </details>\n  </div>");
        } else {
            let escaped = value.replace('<', "&lt;").replace('>', "&gt;");
            let _ = writeln!(
                out,
                "  <div class=\"field\"><strong>{}</strong>: <code>{}</code></div>",
                key, escaped
            );
        }
    }
    let _ = writeln!(out, "</div>");
    out
}
