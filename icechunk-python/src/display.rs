use std::fmt::{Display, Write as _};

use icechunk::display::AncestryGraph;
use pyo3::prelude::*;
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

/// Format an `Option<String>` as a quoted Python string repr: `None` or `"value"`.
pub(crate) fn py_option_str(o: &Option<String>) -> String {
    match o {
        None => "None".to_string(),
        Some(s) => format!("\"{s}\""),
    }
}

/// Format an `Option<T>` showing the effective default in human-readable modes.
///
/// - `Repr` mode: `"None"` (preserves round-trip fidelity for `eval(repr(x))`)
/// - `Str` / `Html` mode: `"<default> (default)"` when `None`, or the value when `Some`
pub(crate) fn py_option_or_default<T: Display>(
    o: &Option<T>,
    default: &str,
    mode: ReprMode,
) -> String {
    match o {
        Some(v) => v.to_string(),
        None => match mode {
            ReprMode::Repr => "None".to_string(),
            ReprMode::Str | ReprMode::Html => format!("{default} (default)"),
        },
    }
}

/// Format a bool as a Python literal (`True` / `False`).
pub(crate) fn py_bool(b: bool) -> String {
    if b { "True" } else { "False" }.to_string()
}

/// Format an `Option<bool>` showing the effective default in human-readable modes.
pub(crate) fn py_option_bool_or_default(
    o: &Option<bool>,
    default: bool,
    mode: ReprMode,
) -> String {
    match o {
        Some(b) => py_bool(*b),
        None => match mode {
            ReprMode::Repr => "None".to_string(),
            ReprMode::Str | ReprMode::Html => {
                format!("{} (default)", py_bool(default))
            }
        },
    }
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

    /// Dispatch to the appropriate repr method for the given mode.
    fn render(&self, mode: ReprMode) -> String {
        match mode {
            ReprMode::Str => self.__str__(),
            ReprMode::Repr => self.__repr__(),
            ReprMode::Html => self._repr_html_(),
        }
    }

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
        Some(py_obj) => Python::attach(|py| py_obj.borrow(py).render(mode)),
    }
}

/// Like [`py_option_nested_repr`], but when the value is `None`, renders the
/// effective default in human-readable modes (`Str` / `Html`).
///
/// The `make_default` closure constructs a default instance of the Python wrapper
/// type (typically via `RustConfig::default().into()`). The default instance's own
/// `fields()` method then shows per-field defaults recursively.
///
/// `Repr` mode still returns `"None"` to preserve `eval(repr(x))` round-trips.
pub(crate) fn py_option_nested_repr_or_default<T: PyRepr + PyClass>(
    opt: &Option<Py<T>>,
    mode: ReprMode,
    make_default: impl FnOnce() -> T,
) -> String {
    match opt {
        Some(py_obj) => Python::attach(|py| py_obj.borrow(py).render(mode)),
        None => match mode {
            ReprMode::Repr => "None".to_string(),
            _ => make_default().render(mode),
        },
    }
}

/// Render a non-executable, human-readable string for a dataclass-like struct.
///
/// `cls_name` is the public Python class name.
/// `fields` are `(field_name, rendered_value)` pairs.
pub(crate) fn dataclass_str(cls_name: &str, fields: &[(&str, &str)]) -> String {
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
pub(crate) fn dataclass_repr(cls_name: &str, fields: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{cls_name}(");
    for (key, value) in fields {
        if value.contains('\n') {
            // Indent all lines of a multi-line value
            let _ = write!(out, "    {key}=");
            let lines: Vec<&str> = value.lines().collect();
            for (i, line) in lines.iter().enumerate() {
                if i == 0 {
                    let _ = writeln!(out, "{line}");
                } else if i == lines.len() - 1 {
                    // Add trailing comma after the closing line
                    let _ = writeln!(out, "    {line},");
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

/// CSS for icechunk HTML reprs.
///
/// Follows xarray's pattern: define custom properties (`--ic-*`) on `:root`
/// that inherit from Jupyter's `--jp-*` variables with sensible fallbacks.
/// Using `:root` ensures the variables cascade into output areas where
/// class-scoped `<style>` blocks may not inherit theme variables directly.
const ICECHUNK_CSS: &str = r#"<style>
:root {
    --ic-font-color: var(--jp-content-font-color0, rgba(0, 0, 0, 1));
    --ic-font-color-muted: var(--jp-content-font-color2, rgba(0, 0, 0, 0.54));
    --ic-border-color: var(--jp-border-color2, #ddd);
    --ic-background-nested: var(--jp-layout-color1, #f7f7f7);
}
html[theme="dark"],
html[data-theme="dark"],
body[data-theme="dark"],
body.vscode-dark {
    --ic-font-color: var(--jp-content-font-color0, rgba(255, 255, 255, 1));
    --ic-font-color-muted: var(--jp-content-font-color2, rgba(255, 255, 255, 0.54));
    --ic-border-color: var(--jp-border-color2, #555);
    --ic-background-nested: var(--jp-layout-color1, #333);
}
.icechunk-repr {
    line-height: 1.6;
    max-width: 700px;
    color: var(--ic-font-color);
}
.icechunk-repr .icechunk-header {
    font-weight: 500;
    padding-bottom: 6px;
    margin-bottom: 4px;
    border-bottom: solid 1px var(--ic-border-color);
}
.icechunk-repr .icechunk-field {
    padding: 2px 0;
}
.icechunk-repr .icechunk-field-name {
    font-weight: 500;
    color: var(--ic-font-color-muted);
}
.icechunk-repr .icechunk-field-value {
    color: var(--ic-font-color);
    padding-left: 0.3em;
}
.icechunk-repr details {
    margin: 2px 0;
}
.icechunk-repr summary {
    cursor: pointer;
    font-weight: 500;
    color: var(--ic-font-color-muted);
}
.icechunk-repr details > .icechunk-repr {
    margin-top: 4px;
    margin-left: 12px;
    padding: 6px 10px;
    background: var(--ic-background-nested);
    border-radius: 4px;
}
</style>"#;

/// Render a static HTML representation suitable for Jupyter `_repr_html_`.
///
/// Uses a `<details>` pattern for nested values that contain HTML (detected by
/// checking for `<div`). Plain values are rendered inline. Includes CSS with
/// `:root` custom properties for theme-aware styling in notebooks.
pub(crate) fn dataclass_html_repr(cls_name: &str, fields: &[(&str, &str)]) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{ICECHUNK_CSS}");
    let _ = writeln!(out, "<div class=\"icechunk-repr\">");
    let _ = writeln!(out, "  <div class=\"icechunk-header\">{cls_name}</div>");
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

#[pyclass(name = "AncestryGraph")]
#[derive(Debug, Clone)]
pub(crate) struct PyAncestryGraph {
    inner: AncestryGraph,
}

impl From<AncestryGraph> for PyAncestryGraph {
    fn from(inner: AncestryGraph) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyAncestryGraph {
    pub(crate) fn __repr__(&self) -> String {
        self.inner.to_string()
    }

    pub(crate) fn __str__(&self) -> String {
        self.inner.to_string()
    }

    /// Return a raw SVG string for Jupyter notebooks.
    pub(crate) fn _repr_svg_(&self) -> String {
        self.inner.to_svg()
    }
}
