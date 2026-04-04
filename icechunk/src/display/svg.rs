//! SVG rendering for [`AncestryGraph`], used by `_repr_html_()` in Jupyter notebooks.

use std::collections::HashMap;
use std::fmt::Write as _;

use super::ancestry_graph::{
    AncestryGraph, AncestryNode, LayoutElement, TAG_COLOR_HEX, palette_hex,
};

// Layout constants (pixels)
const COL_SPACING: f64 = 20.0;
const ROW_HEIGHT: f64 = 28.0;
const NODE_RADIUS: f64 = 5.0;
const TEXT_X_OFFSET: f64 = 14.0;
const PADDING_LEFT: f64 = 15.0;
const PADDING_TOP: f64 = 10.0;
const LINE_WIDTH: f64 = 2.0;
const FONT_SIZE: f64 = 13.0;

fn col_x(col: usize) -> f64 {
    PADDING_LEFT + col as f64 * COL_SPACING
}

fn row_y(row: usize) -> f64 {
    PADDING_TOP + row as f64 * ROW_HEIGHT + NODE_RADIUS
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn truncate_message(msg: &str, max_len: usize) -> String {
    let first_line = msg.lines().next().unwrap_or("");
    if first_line.len() <= max_len {
        first_line.to_string()
    } else {
        format!("{}...", &first_line[..max_len - 3])
    }
}

fn format_labels_svg(node: &AncestryNode, col_colors: &[usize]) -> String {
    let mut parts = Vec::new();
    for b in &node.branches {
        let color = palette_hex(col_colors[node.column]);
        parts.push(format!(
            "<tspan font-weight=\"bold\" fill=\"{color}\">{}</tspan>",
            escape_html(b)
        ));
    }
    for t in &node.tags {
        parts.push(format!(
            "<tspan font-weight=\"bold\" font-style=\"italic\" fill=\"{TAG_COLOR_HEX}\">{}</tspan>",
            escape_html(t)
        ));
    }
    if parts.is_empty() { String::new() } else { format!(" ({})", parts.join(", ")) }
}

impl AncestryGraph {
    /// Render the graph as an SVG string wrapped in an HTML div.
    ///
    /// Uses the same layout pass as the ANSI renderer for consistent structure.
    pub fn to_svg(&self) -> String {
        if self.nodes.is_empty() {
            return "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"200\" height=\"30\" \
                    style=\"font-family: monospace; font-size: 13px;\">\
                    <text x=\"10\" y=\"20\" fill=\"#888\">(empty history)</text></svg>"
                .to_string();
        }

        let col_colors = self.column_colors();
        let elements = self.layout();

        let graph_width = PADDING_LEFT + (self.num_columns.max(1)) as f64 * COL_SPACING;
        let text_x = graph_width + TEXT_X_OFFSET;
        let total_height = PADDING_TOP * 2.0 + self.nodes.len() as f64 * ROW_HEIGHT;
        let total_width = text_x + 500.0; // enough room for text

        let mut svg = String::new();
        let _ = writeln!(
            svg,
            "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{total_width}\" height=\"{total_height}\" \
             style=\"font-family: monospace; font-size: {FONT_SIZE}px;\">"
        );

        // Merge consecutive Line segments in the same column into single lines,
        // so the SVG draws one continuous line between actual nodes rather than
        // many short segments with potential subpixel gaps.
        let mut col_line_spans: HashMap<usize, (usize, usize)> = HashMap::new();
        let mut merged_lines: Vec<(usize, usize, usize)> = Vec::new(); // (col, from_row, to_row)

        for elem in &elements {
            if let LayoutElement::Line { from_row, to_row, col } = elem {
                let entry = col_line_spans.entry(*col);
                match entry {
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        let span = e.get_mut();
                        if *from_row <= span.1 {
                            // Extend the current span
                            span.1 = span.1.max(*to_row);
                        } else {
                            // Gap — flush and start new span
                            merged_lines.push((*col, span.0, span.1));
                            *span = (*from_row, *to_row);
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert((*from_row, *to_row));
                    }
                }
            }
        }
        // Flush remaining spans
        for (col, (from_row, to_row)) in &col_line_spans {
            merged_lines.push((*col, *from_row, *to_row));
        }

        // Render merged lines (behind nodes)
        for (col, from_row, to_row) in &merged_lines {
            let color = palette_hex(col_colors[*col]);
            let x = col_x(*col);
            let y1 = row_y(*from_row);
            let y2 = row_y(*to_row);
            let _ = writeln!(
                svg,
                "  <line x1=\"{x}\" y1=\"{y1}\" x2=\"{x}\" y2=\"{y2}\" \
                 stroke=\"{color}\" stroke-width=\"{LINE_WIDTH}\" />"
            );
        }

        // Render fork/merge curves
        for elem in &elements {
            if let LayoutElement::Fork { from_row, from_col, to_row, to_col } = elem {
                let color = palette_hex(col_colors[*from_col]);
                let x1 = col_x(*from_col);
                let y1 = row_y(*from_row);
                let x2 = col_x(*to_col);
                let y2 = row_y(*to_row);
                let mid_y = (y1 + y2) / 2.0;
                let _ = writeln!(
                    svg,
                    "  <path d=\"M {x1} {y1} C {x1} {mid_y}, {x2} {mid_y}, {x2} {y2}\" \
                     fill=\"none\" stroke=\"{color}\" stroke-width=\"{LINE_WIDTH}\" />"
                );
            }
        }

        // Render nodes (circles) and text labels
        for (row, node) in self.nodes.iter().enumerate() {
            let cx = col_x(node.column);
            let cy = row_y(row);
            let color = palette_hex(col_colors[node.column]);

            // Circle
            let _ = writeln!(
                svg,
                "  <circle cx=\"{cx}\" cy=\"{cy}\" r=\"{NODE_RADIUS}\" fill=\"{color}\" />"
            );

            // Text: short ID + labels + message
            let short_id = &node.info.id.to_string()[..8];
            let labels = format_labels_svg(node, &col_colors);
            let msg = escape_html(&truncate_message(&node.info.message, 60));
            let text_y = cy + FONT_SIZE / 3.0; // vertical center

            let _ = writeln!(
                svg,
                "  <text x=\"{text_x}\" y=\"{text_y}\">\
                 <tspan fill=\"#888\">{short_id}</tspan>\
                 {labels} {msg}</text>"
            );
        }

        // Truncation message
        if self.total_snapshots > self.nodes.len() {
            let y = row_y(self.nodes.len()) + FONT_SIZE;
            let _ = writeln!(
                svg,
                "  <text x=\"{text_x}\" y=\"{y}\" fill=\"#888\">... (showing {} of {} snapshots)</text>",
                self.nodes.len(),
                self.total_snapshots
            );
        }

        let _ = writeln!(svg, "</svg>");

        svg
    }
}
