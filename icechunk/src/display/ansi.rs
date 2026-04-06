//! ANSI-colored Unicode text rendering for [`AncestryGraph`].

use std::collections::HashMap;
use std::fmt;

use super::ancestry_graph::{
    AncestryGraph, AncestryNode, LayoutElement, TAG_COLOR_ANSI, palette_ansi,
};

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const ITALIC: &str = "\x1b[3m";
const DIM: &str = "\x1b[2m";

fn truncate_message(msg: &str, max_len: usize) -> String {
    let first_line = msg.lines().next().unwrap_or("");
    if first_line.len() <= max_len {
        first_line.to_string()
    } else {
        format!("{}...", &first_line[..max_len - 3])
    }
}

fn format_labels(node: &AncestryNode, col_colors: &[usize], plain: bool) -> String {
    let mut parts = Vec::new();
    for b in &node.branches {
        if plain {
            parts.push(b.clone());
        } else {
            let color = palette_ansi(col_colors[node.column]);
            parts.push(format!("{BOLD}{color}{b}{RESET}"));
        }
    }
    for t in &node.tags {
        if plain {
            parts.push(t.clone());
        } else {
            parts.push(format!("{BOLD}{ITALIC}{TAG_COLOR_ANSI}{t}{RESET}"));
        }
    }
    if parts.is_empty() { String::new() } else { format!(" ({})", parts.join(", ")) }
}

/// Render a graph line prefix: for each column, draw a (optionally colored) glyph or blank.
fn render_prefix(
    col_colors: &[usize],
    plain: bool,
    glyph_for: impl Fn(usize) -> Option<char>,
) -> String {
    let mut out = String::with_capacity(col_colors.len() * 8);
    for (c, &color_idx) in col_colors.iter().enumerate() {
        if let Some(ch) = glyph_for(c) {
            if plain {
                out.push(ch);
                out.push(' ');
            } else {
                let color = palette_ansi(color_idx);
                out.push_str(&format!("{color}{ch}{RESET} "));
            }
        } else {
            out.push_str("  ");
        }
    }
    out
}

impl fmt::Display for AncestryGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nodes.is_empty() {
            return writeln!(f, "(empty history)");
        }

        let col_colors = self.column_colors();
        let elements = self.layout();

        // Group connector elements (Lines, Forks) by the node row they follow.
        let mut connector_lines: HashMap<usize, Vec<&LayoutElement>> = HashMap::new();
        for elem in &elements {
            match elem {
                LayoutElement::Line { from_row, .. }
                | LayoutElement::Fork { from_row, .. } => {
                    connector_lines.entry(*from_row).or_default().push(elem);
                }
                LayoutElement::Node { .. } => {}
            }
        }

        for (row, node) in self.nodes.iter().enumerate() {
            // Node line
            let prefix = render_prefix(&col_colors, self.plain, |c| {
                if c == node.column {
                    Some('●')
                } else {
                    // Show │ for active columns (those with a line on this or previous row).
                    let has_line = connector_lines.get(&row).is_some_and(|elems| {
                        elems.iter().any(
                            |e| matches!(e, LayoutElement::Line { col, .. } if *col == c),
                        )
                    });
                    let had_line = row > 0
                        && connector_lines.get(&(row - 1)).is_some_and(|elems| {
                            elems.iter().any(|e| {
                                matches!(e, LayoutElement::Line { col, .. } if *col == c)
                                    || matches!(e, LayoutElement::Fork { to_col, .. } if *to_col == c)
                            })
                        });
                    if has_line || had_line { Some('│') } else { None }
                }
            });

            let short_id = &node.info.id.to_string()[..8];
            let labels = format_labels(node, &col_colors, self.plain);
            let msg = truncate_message(&node.info.message, 60);
            if self.plain {
                writeln!(f, "{prefix}{short_id}{labels} {msg}")?;
            } else {
                writeln!(f, "{prefix}{DIM}{short_id}{RESET}{labels} {msg}")?;
            }

            // Connector line (if any elements follow this row)
            if let Some(elems) = connector_lines.get(&row) {
                let line = render_prefix(&col_colors, self.plain, |c| {
                    let is_fork = elems.iter().any(|e| {
                        matches!(e, LayoutElement::Fork { from_col, .. } if *from_col == c)
                    });
                    if is_fork {
                        return Some('╱');
                    }
                    let is_pipe = elems.iter().any(
                        |e| matches!(e, LayoutElement::Line { col, .. } if *col == c),
                    );
                    if is_pipe { Some('│') } else { None }
                });
                writeln!(f, "{line}")?;
            }
        }

        if self.total_snapshots > self.nodes.len() {
            if self.plain {
                writeln!(
                    f,
                    "  ... (showing {} of {} snapshots)",
                    self.nodes.len(),
                    self.total_snapshots
                )?;
            } else {
                writeln!(
                    f,
                    "{DIM}  ... (showing {} of {} snapshots){RESET}",
                    self.nodes.len(),
                    self.total_snapshots
                )?;
            }
        }
        Ok(())
    }
}
