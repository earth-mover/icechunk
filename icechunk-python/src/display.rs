use std::fmt::{Display, Write as _};

use pyo3::{Py, PyClass, Python};

/// Format an `Option<T>` as a Python repr: `"None"` or the Display string of the value.
pub(crate) fn py_option<T: Display>(o: &Option<T>) -> String {
    match o {
        None => "None".to_string(),
        Some(s) => s.to_string(),
    }
}

/// Format a bool as a Python literal (`True` / `False`).
pub(crate) fn py_bool(b: bool) -> String {
    if b { "True" } else { "False" }.to_string()
}

/// Trait for Python repr/str/html methods on icechunk classes.
///
/// Implementers provide `cls_name`, `fields`, and `EXECUTABLE`.
/// Default implementations of `__str__`, `__repr__`, and `_repr_html_` are provided.
///
/// The `fields(executable)` parameter controls how nested objects render themselves:
/// - `__repr__` passes `Self::EXECUTABLE`, so an executable class gets executable
///   nested reprs, producing output that `eval()` can reconstruct.
/// - `__str__` and `_repr_html_` always pass `false`, because these are meant to be
///   human-readable — embedding constructor syntax inside a human-readable display
///   would be confusing, and a half-executable repr is useless for round-tripping.
pub(crate) trait PyRepr {
    /// Whether this class has an executable `__repr__` (constructor-style).
    /// If false, `__repr__` uses the same non-executable format as `__str__`.
    const EXECUTABLE: bool;

    /// Fully-qualified Python class name, e.g. `"icechunk.Session"`.
    fn cls_name() -> &'static str;

    /// Field name/value pairs. Values should be in Python repr form
    /// (quoted strings, `True`/`False`, `None`).
    ///
    /// `executable` is true when building an executable repr, false for str/html.
    /// Pass this through to nested repr calls (e.g. `py_option_nested_repr`) so
    /// the recursion pattern is consistent.
    fn fields(&self, executable: bool) -> Vec<(&str, String)>;

    fn __str__(&self) -> String {
        let fields = self.fields(false);
        let refs: Vec<(&str, &str)> =
            fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
        dataclass_str(Self::cls_name(), &refs)
    }

    fn __repr__(&self) -> String {
        let fields = self.fields(Self::EXECUTABLE);
        let refs: Vec<(&str, &str)> =
            fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
        if Self::EXECUTABLE {
            dataclass_repr(Self::cls_name(), &refs)
        } else {
            dataclass_str(Self::cls_name(), &refs)
        }
    }

    fn _repr_html_(&self) -> String {
        let fields = self.fields(false);
        let refs: Vec<(&str, &str)> =
            fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
        dataclass_html_repr(Self::cls_name(), &refs)
    }
}

/// Get the repr of an optional `Py<T>` where `T` implements `PyRepr`.
///
/// Pyclass structs that hold other pyclass structs as attributes store them as
/// `Py<T>` (a Python-managed reference-counted pointer). To access the underlying
/// Rust struct and call the `PyRepr` trait method on it, we need to borrow it
/// via `Py::borrow(py)`, which requires the GIL and the `PyClass` bound.
///
/// If `executable` is true, calls `__repr__` (constructor-style); otherwise `__str__`.
pub(crate) fn py_option_nested_repr<T: PyRepr + PyClass>(
    opt: &Option<Py<T>>,
    executable: bool,
) -> String {
    match opt {
        None => "None".to_string(),
        Some(py_obj) => Python::attach(|py| {
            let inner = &*py_obj.borrow(py);
            if executable {
                <T as PyRepr>::__repr__(inner)
            } else {
                <T as PyRepr>::__str__(inner)
            }
        }),
    }
}

/// Render a non-executable, human-readable string for a dataclass-like struct.
///
/// `cls_name` is the public Python class name.
/// `fields` are `(field_name, rendered_value)` pairs.
fn dataclass_str(cls_name: &str, fields: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "<{cls_name}>");
    for (key, value) in fields {
        // If the value contains newlines, indent the nested block
        if value.contains('\n') {
            let _ = writeln!(out, "{key}:");
            for line in value.lines() {
                let _ = writeln!(out, "    {line}");
            }
        } else {
            let _ = writeln!(out, "{key}: {value}");
        }
    }
    out
}

/// Render an executable-style Python repr for a dataclass-like struct.
///
/// Produces multi-line output with indentation for readability.
fn dataclass_repr(cls_name: &str, fields: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{cls_name}(");
    for (key, value) in fields {
        if value.contains('\n') {
            // Indent all lines of a multi-line value
            let _ = write!(out, "    {key}=");
            for (i, line) in value.lines().enumerate() {
                if i == 0 {
                    let _ = writeln!(out, "{line}");
                } else {
                    let _ = writeln!(out, "    {line}");
                }
            }
        } else {
            let _ = writeln!(out, "    {key}={value},");
        }
    }
    let _ = write!(out, ")");
    out
}

/// Render a static HTML representation suitable for Jupyter `_repr_html_`.
///
/// Uses a `<details><summary>` pattern for nested values that contain newlines.
/// Nested HTML fragments should be passed in via `fields` values when available; otherwise
/// plain text will be HTML-escaped minimally.
fn dataclass_html_repr(cls_name: &str, fields: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "<div class=\"icechunk-repr\">\n  <code>&lt;{cls_name}&gt;</code>",
    );
    for (key, value) in fields {
        if value.contains('\n') {
            let _ = writeln!(
                out,
                "  <div class=\"field\">\n    <details>\n      <summary><strong>{key}</strong></summary>",
            );
            // Indent nested lines inside a pre/code block for readability
            let _ = writeln!(out, "      <pre><code>");
            for line in value.lines() {
                // Basic escape for angle brackets to avoid HTML injection in plain text
                let escaped = line.replace('<', "&lt;").replace('>', "&gt;");
                let _ = writeln!(out, "{escaped}");
            }
            let _ = writeln!(out, "      </code></pre>\n    </details>\n  </div>");
        } else {
            let escaped = value.replace('<', "&lt;").replace('>', "&gt;");
            let _ = writeln!(
                out,
                "  <div class=\"field\"><strong>{key}</strong>: <code>{escaped}</code></div>",
            );
        }
    }
    let _ = writeln!(out, "</div>");
    out
}
