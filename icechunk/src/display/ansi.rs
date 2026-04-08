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

/// A single column's rendering: glyph + trailing character, each with its own color.
struct ColCell {
    glyph: char,
    glyph_color: usize,
    trail: char,
    trail_color: usize,
}

/// Render a graph line prefix: for each column, draw a glyph + trailing char.
/// The closure returns a `ColCell` or `None` for blank (`  `).
fn render_prefix(
    col_colors: &[usize],
    plain: bool,
    cell_for: impl Fn(usize) -> Option<ColCell>,
) -> String {
    let mut out = String::with_capacity(col_colors.len() * 8);
    for c in 0..col_colors.len() {
        if let Some(cell) = cell_for(c) {
            if plain {
                out.push(cell.glyph);
                out.push(cell.trail);
            } else {
                let gc = palette_ansi(cell.glyph_color);
                if cell.trail == ' ' {
                    out.push_str(&format!("{gc}{}{RESET} ", cell.glyph));
                } else {
                    let tc = palette_ansi(cell.trail_color);
                    out.push_str(&format!(
                        "{gc}{}{RESET}{tc}{}{RESET}",
                        cell.glyph, cell.trail
                    ));
                }
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

        // Group Line elements by their from_row (connector rows between nodes).
        let mut connector_lines: HashMap<usize, Vec<&LayoutElement>> = HashMap::new();
        // Group Fork elements by their to_row (the fork point node they arrive at).
        let mut forks_at_node: HashMap<usize, Vec<&LayoutElement>> = HashMap::new();
        for elem in &elements {
            match elem {
                LayoutElement::Line { from_row, .. } => {
                    connector_lines.entry(*from_row).or_default().push(elem);
                }
                LayoutElement::Fork { to_row, .. } => {
                    forks_at_node.entry(*to_row).or_default().push(elem);
                }
                LayoutElement::Node { .. } => {}
            }
        }

        for (row, node) in self.nodes.iter().enumerate() {
            // Node line: show ● for this node's column, ╯ for merging columns,
            // │ for other active columns.
            // Collect fork columns sorted, for computing colors and trailing chars.
            let mut fork_cols: Vec<usize> = forks_at_node
                .get(&row)
                .map(|elems| {
                    elems
                        .iter()
                        .filter_map(|e| match e {
                            LayoutElement::Fork { from_col, .. } => Some(*from_col),
                            _ => None,
                        })
                        .collect()
                })
                .unwrap_or_default();
            fork_cols.sort();
            let rightmost_fork = fork_cols.last().copied();

            // Find the nearest fork column to the right of `c` and its color.
            let next_fork_color = |c: usize| -> usize {
                fork_cols
                    .iter()
                    .find(|&&fc| fc >= c)
                    .map(|&fc| col_colors[fc])
                    .unwrap_or(0)
            };

            let prefix = render_prefix(&col_colors, self.plain, |c| {
                // On a fork row, columns between ● and the rightmost ╯ use ─ as
                // the trailing char, colored to match the nearest ╯ to the right.
                let on_fork_row =
                    rightmost_fork.is_some_and(|rf| c >= node.column && c < rf);
                let trail = if on_fork_row { '─' } else { ' ' };
                let trail_color = if on_fork_row { next_fork_color(c + 1) } else { 0 };

                if c == node.column {
                    return Some(ColCell {
                        glyph: '●',
                        glyph_color: col_colors[c],
                        trail,
                        trail_color,
                    });
                }
                // Check if a fork arrives at this node from column c.
                let is_merging = forks_at_node.get(&row).is_some_and(|elems| {
                    elems.iter().any(
                        |e| matches!(e, LayoutElement::Fork { from_col, .. } if *from_col == c),
                    )
                });
                if is_merging {
                    return Some(ColCell {
                        glyph: '╯',
                        glyph_color: col_colors[c],
                        trail,
                        trail_color,
                    });
                }
                // Show │ for active columns (those with a line on this row).
                let has_line = connector_lines.get(&row).is_some_and(|elems| {
                    elems.iter().any(
                        |e| matches!(e, LayoutElement::Line { col, .. } if *col == c),
                    )
                });
                // Also check previous row's lines, excluding merged columns.
                let had_line = row > 0
                    && connector_lines.get(&(row - 1)).is_some_and(|elems| {
                        elems.iter().any(
                            |e| matches!(e, LayoutElement::Line { col, .. } if *col == c),
                        )
                    })
                    && !is_merging;
                if has_line || had_line {
                    return Some(ColCell {
                        glyph: '│',
                        glyph_color: col_colors[c],
                        trail: ' ',
                        trail_color: 0,
                    });
                }
                // Fill blank columns between ● and rightmost ╯ with ─.
                if c > node.column && rightmost_fork.is_some_and(|rf| c < rf) {
                    let fill_color = next_fork_color(c);
                    return Some(ColCell {
                        glyph: '─',
                        glyph_color: fill_color,
                        trail,
                        trail_color,
                    });
                }
                None
            });

            let short_id = &node.info.id.to_string()[..8];
            let labels = format_labels(node, &col_colors, self.plain);
            let msg = truncate_message(&node.info.message, 60);
            if self.plain {
                writeln!(f, "{prefix}{short_id}{labels} {msg}")?;
            } else {
                writeln!(f, "{prefix}{DIM}{short_id}{RESET}{labels} {msg}")?;
            }

            // Connector line: │ for active columns (no more ╱ here, forks are on node lines)
            if let Some(elems) = connector_lines.get(&row) {
                let line = render_prefix(&col_colors, self.plain, |c| {
                    let is_pipe = elems.iter().any(
                        |e| matches!(e, LayoutElement::Line { col, .. } if *col == c),
                    );
                    if is_pipe {
                        Some(ColCell {
                            glyph: '│',
                            glyph_color: col_colors[c],
                            trail: ' ',
                            trail_color: 0,
                        })
                    } else {
                        None
                    }
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
