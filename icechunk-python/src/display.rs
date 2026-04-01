use std::fmt::{Display, Write as _};

use pyo3::{Py, PyClass, Python};

/// The rendering mode, controlling how nested objects format themselves.
///
/// - `Str`: human-readable, non-executable (`__str__`)
/// - `Repr`: executable constructor syntax if the class supports it (`__repr__`)
/// - `Html`: styled HTML for Jupyter notebooks (`_repr_html_`)
///
/// The recursion rule is: each mode nests the same mode. A `Str` parent renders
/// children as `Str`, a `Repr` parent renders children as `Repr`, and an `Html`
/// parent renders children as `Html`. Non-executable classes always use `Str`
/// format for `Repr` mode (since they can't produce constructor syntax).
#[derive(Clone, Copy)]
pub(crate) enum ReprMode {
    Str,
    Repr,
    Html,
}

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
/// The `fields(mode)` parameter controls how nested objects render themselves:
/// - `__repr__` passes `Repr` (or `Str` for non-executable classes, since they
///   can't produce constructor syntax and a half-executable repr is useless).
/// - `__str__` passes `Str`.
/// - `_repr_html_` passes `Html`, so nested objects produce their own styled HTML
///   fragments rather than plain text.
pub(crate) trait PyRepr {
    /// Whether this class has an executable `__repr__` (constructor-style).
    /// If false, `__repr__` uses the same non-executable format as `__str__`.
    const EXECUTABLE: bool;

    /// Fully-qualified Python class name, e.g. `"icechunk.Session"`.
    fn cls_name() -> &'static str;

    /// Field name/value pairs.
    ///
    /// Values should be pre-formatted strings appropriate for the given mode.
    /// Pass the mode through to nested repr calls (e.g. `py_option_nested_repr`)
    /// so the recursion pattern is consistent.
    fn fields(&self, mode: ReprMode) -> Vec<(&str, String)>;

    fn __str__(&self) -> String {
        let fields = self.fields(ReprMode::Str);
        let refs: Vec<(&str, &str)> =
            fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
        dataclass_str(Self::cls_name(), &refs)
    }

    fn __repr__(&self) -> String {
        if Self::EXECUTABLE {
            let fields = self.fields(ReprMode::Repr);
            let refs: Vec<(&str, &str)> =
                fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
            dataclass_repr(Self::cls_name(), &refs)
        } else {
            let fields = self.fields(ReprMode::Str);
            let refs: Vec<(&str, &str)> =
                fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
            dataclass_str(Self::cls_name(), &refs)
        }
    }

    fn _repr_html_(&self) -> String {
        let fields = self.fields(ReprMode::Html);
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
pub(crate) fn py_option_nested_repr<T: PyRepr + PyClass>(
    opt: &Option<Py<T>>,
    mode: ReprMode,
) -> String {
    match opt {
        None => "None".to_string(),
        Some(py_obj) => Python::attach(|py| {
            let inner = &*py_obj.borrow(py);
            match mode {
                ReprMode::Str => <T as PyRepr>::__str__(inner),
                ReprMode::Repr => <T as PyRepr>::__repr__(inner),
                ReprMode::Html => <T as PyRepr>::_repr_html_(inner),
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

/// Inline CSS for icechunk HTML reprs.
///
/// Uses Jupyter CSS custom properties (`--jp-*`) with sensible fallbacks,
/// following the same pattern as xarray. This ensures the repr inherits the
/// notebook's actual theme (light or dark) rather than guessing from OS settings.
const ICECHUNK_CSS: &str = r#"<style>
.icechunk-repr {
    font-family: var(--jp-ui-font-family, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif);
    font-size: var(--jp-ui-font-size1, 13px);
    line-height: 1.6;
    border: 1px solid var(--jp-border-color2, #ddd);
    border-radius: 4px;
    padding: 10px 14px;
    background: var(--jp-layout-color1, #f7f7f7);
    color: var(--jp-content-font-color0, rgba(0, 0, 0, 1));
    max-width: 600px;
}
.icechunk-repr .icechunk-header {
    font-weight: 600;
    font-size: var(--jp-ui-font-size2, 14px);
    margin-bottom: 6px;
}
.icechunk-repr .icechunk-field {
    padding: 2px 0;
    display: flex;
    gap: 6px;
}
.icechunk-repr .icechunk-field-name {
    font-weight: 600;
    color: var(--jp-content-font-color2, rgba(0, 0, 0, 0.54));
    min-width: fit-content;
}
.icechunk-repr .icechunk-field-value {
    color: var(--jp-content-font-color0, rgba(0, 0, 0, 1));
}
.icechunk-repr details {
    margin: 2px 0;
}
.icechunk-repr summary {
    cursor: pointer;
    font-weight: 600;
    color: var(--jp-content-font-color2, rgba(0, 0, 0, 0.54));
}
.icechunk-repr details > .icechunk-repr {
    margin-top: 4px;
    margin-left: 12px;
    background: var(--jp-layout-color0, white);
}
</style>"#;

/// Render a static HTML representation suitable for Jupyter `_repr_html_`.
///
/// Uses a `<details>` pattern for nested values that contain HTML (detected by
/// checking for `<div`). Plain values are rendered inline. Includes scoped CSS
/// for styling in notebooks.
fn dataclass_html_repr(cls_name: &str, fields: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{ICECHUNK_CSS}");
    let _ = writeln!(out, "<div class=\"icechunk-repr\">");
    let _ = writeln!(out, "  <div class=\"icechunk-header\">{cls_name}</div>",);
    for (key, value) in fields {
        // If the value contains HTML (from a nested _repr_html_ call), render
        // it inside a collapsible <details> element.
        if value.contains("<div") {
            let _ = writeln!(out, "  <details>");
            let _ = writeln!(
                out,
                "    <summary class=\"icechunk-field-name\">{key}</summary>",
            );
            let _ = writeln!(out, "    {value}");
            let _ = writeln!(out, "  </details>");
        } else {
            let escaped = value.replace('<', "&lt;").replace('>', "&gt;");
            let _ = writeln!(
                out,
                "  <div class=\"icechunk-field\"><span class=\"icechunk-field-name\">{key}:</span> <span class=\"icechunk-field-value\">{escaped}</span></div>",
            );
        }
    }
    let _ = writeln!(out, "</div>");
    out
}
