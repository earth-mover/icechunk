use std::collections::HashMap;
use std::fmt;

use icechunk_format::SnapshotId;
use icechunk_format::snapshot::SnapshotInfo;

/// A single node in the ancestry graph, combining snapshot info with display metadata.
#[derive(Debug, Clone)]
pub struct AncestryNode {
    pub info: SnapshotInfo,
    pub branches: Vec<String>,
    pub tags: Vec<String>,
    /// Column index for multi-branch graph rendering (0 = leftmost).
    pub column: usize,
}

/// A materialized view of repository commit history, suitable for rendering.
///
/// Built via [`Repository::ancestry_graph()`](crate::Repository) — either for a single
/// ref (linear history) or all branches (tree view).
#[derive(Debug, Clone)]
pub struct AncestryGraph {
    /// Nodes in display order (newest first).
    pub nodes: Vec<AncestryNode>,
    /// Number of columns needed for rendering (1 for single-branch linear view).
    pub num_columns: usize,
}

impl AncestryGraph {
    /// Build a linear (single-branch) graph from an ordered list of snapshots.
    ///
    /// `snapshots` should be in ancestry order (newest first).
    /// `branch_map` and `tag_map` provide label decorations keyed by snapshot ID.
    pub fn from_linear(
        snapshots: Vec<SnapshotInfo>,
        branch_map: &HashMap<SnapshotId, Vec<String>>,
        tag_map: &HashMap<SnapshotId, Vec<String>>,
    ) -> Self {
        let nodes = snapshots
            .into_iter()
            .map(|info| {
                let branches = branch_map
                    .get(&info.id)
                    .cloned()
                    .unwrap_or_default();
                let tags = tag_map
                    .get(&info.id)
                    .cloned()
                    .unwrap_or_default();
                AncestryNode {
                    info,
                    branches,
                    tags,
                    column: 0,
                }
            })
            .collect();

        Self {
            nodes,
            num_columns: 1,
        }
    }

    /// Build a tree graph from multiple branches.
    ///
    /// `branch_ancestries` is a list of `(branch_name, snapshots)` where each snapshot
    /// list is in ancestry order (newest first). Snapshots shared across branches are
    /// deduplicated — the first branch to claim a snapshot wins.
    ///
    /// `tag_map` provides tag label decorations keyed by snapshot ID.
    pub fn from_tree(
        branch_ancestries: Vec<(String, Vec<SnapshotInfo>)>,
        tag_map: &HashMap<SnapshotId, Vec<String>>,
    ) -> Self {
        if branch_ancestries.is_empty() {
            return Self {
                nodes: Vec::new(),
                num_columns: 0,
            };
        }

        // Track which snapshots we've already seen and their assigned column
        let mut seen: HashMap<SnapshotId, usize> = HashMap::new();
        // Branch label map: snapshot_id → list of branch names pointing at it
        let mut branch_map: HashMap<SnapshotId, Vec<String>> = HashMap::new();
        // All nodes collected, keyed by snapshot_id for dedup
        let mut node_map: HashMap<SnapshotId, AncestryNode> = HashMap::new();
        // Track fork points: snapshot where a branch joins an already-seen column
        let mut fork_points: HashMap<SnapshotId, Vec<usize>> = HashMap::new();

        let num_columns = branch_ancestries.len();

        for (col, (branch_name, snapshots)) in branch_ancestries.iter().enumerate() {
            // The tip of each branch gets a branch label
            if let Some(tip) = snapshots.first() {
                branch_map
                    .entry(tip.id.clone())
                    .or_default()
                    .push(branch_name.clone());
            }

            for info in snapshots {
                if let Some(&existing_col) = seen.get(&info.id) {
                    // This snapshot already belongs to another branch — it's a fork point
                    fork_points
                        .entry(info.id.clone())
                        .or_default()
                        .push(col);
                    // Don't re-insert, but we already recorded the branch label above
                    let _ = existing_col;
                    break; // All further ancestors are shared, stop walking this branch
                }

                seen.insert(info.id.clone(), col);
                let tags = tag_map
                    .get(&info.id)
                    .cloned()
                    .unwrap_or_default();
                let branches = branch_map
                    .get(&info.id)
                    .cloned()
                    .unwrap_or_default();

                node_map.insert(
                    info.id.clone(),
                    AncestryNode {
                        info: info.clone(),
                        branches,
                        tags,
                        column: col,
                    },
                );
            }
        }

        // Sort nodes by timestamp descending (newest first)
        let mut nodes: Vec<AncestryNode> = node_map.into_values().collect();
        nodes.sort_by(|a, b| b.info.flushed_at.cmp(&a.info.flushed_at));

        Self {
            nodes,
            num_columns,
        }
    }
}

// ANSI color codes
const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";

/// Rotating palette for branch graph lines and nodes.
const BRANCH_COLORS: &[&str] = &[
    "\x1b[31m", // red
    "\x1b[32m", // green
    "\x1b[33m", // yellow
    "\x1b[34m", // blue
    "\x1b[35m", // magenta
    "\x1b[36m", // cyan
];

fn branch_color(column: usize) -> &'static str {
    BRANCH_COLORS[column % BRANCH_COLORS.len()]
}

/// Truncate a string to at most `max_len` characters, appending "..." if truncated.
fn truncate_message(msg: &str, max_len: usize) -> String {
    let first_line = msg.lines().next().unwrap_or("");
    if first_line.len() <= max_len {
        first_line.to_string()
    } else {
        format!("{}...", &first_line[..max_len - 3])
    }
}

impl AncestryGraph {
    /// Format the labels (branches + tags) for a node with ANSI colors.
    fn format_labels(node: &AncestryNode) -> String {
        let mut parts = Vec::new();
        for b in &node.branches {
            parts.push(format!("{BOLD}{GREEN}{b}{RESET}"));
        }
        for t in &node.tags {
            parts.push(format!("{BOLD}{YELLOW}{t}{RESET}"));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(" ({})", parts.join(", "))
        }
    }

    /// Render a single-column linear graph.
    fn fmt_linear(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, node) in self.nodes.iter().enumerate() {
            let color = branch_color(0);
            let short_id = &node.info.id.to_string()[..8];
            let labels = Self::format_labels(node);
            let msg = truncate_message(&node.info.message, 60);

            writeln!(
                f,
                "{color}*{RESET} {DIM}{short_id}{RESET}{labels} {msg}"
            )?;

            // Draw connector line between nodes (except after last)
            if i < self.nodes.len() - 1 {
                writeln!(f, "{color}|{RESET}")?;
            }
        }
        Ok(())
    }

    /// Render a multi-column tree graph.
    fn fmt_tree(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Track which columns are currently active (have a branch line running through)
        let mut active_columns: Vec<bool> = vec![false; self.num_columns];

        for (i, node) in self.nodes.iter().enumerate() {
            let col = node.column;

            // Activate this column if not already active
            active_columns[col] = true;

            // Build the graph prefix: show `|` for active columns, `*` for current node
            let mut prefix = String::new();
            for c in 0..self.num_columns {
                if c == col {
                    let color = branch_color(c);
                    prefix.push_str(&format!("{color}*{RESET} "));
                } else if active_columns[c] {
                    let color = branch_color(c);
                    prefix.push_str(&format!("{color}|{RESET} "));
                } else {
                    prefix.push_str("  ");
                }
            }

            let short_id = &node.info.id.to_string()[..8];
            let labels = Self::format_labels(node);
            let msg = truncate_message(&node.info.message, 60);

            writeln!(f, "{prefix}{DIM}{short_id}{RESET}{labels} {msg}")?;

            // Check if this node is a fork point — the next node on this column's
            // parent is in a different column. If so, draw merge lines.
            // For now, draw connector lines for all active columns.
            if i < self.nodes.len() - 1 {
                // Check if this column should deactivate (no more nodes in this column after this one)
                let more_in_column = self.nodes[i + 1..]
                    .iter()
                    .any(|n| n.column == col);

                // Check if next node is a fork point (it's in a different column and
                // this column merges into it)
                let next = &self.nodes[i + 1];
                let is_fork_point = !more_in_column
                    && node.info.parent_id.as_ref() == Some(&next.info.id)
                    && next.column != col;

                if is_fork_point {
                    // Draw a merge line: this column merges into the next node's column
                    let mut merge_line = String::new();
                    for c in 0..self.num_columns {
                        if c == next.column {
                            let color = branch_color(c);
                            merge_line.push_str(&format!("{color}|{RESET}"));
                        } else if c == col {
                            let color = branch_color(col);
                            if col > next.column {
                                merge_line.push_str(&format!("{color}/{RESET}"));
                            } else {
                                merge_line.push_str(&format!("{color}\\{RESET}"));
                            }
                        } else if active_columns[c] {
                            let color = branch_color(c);
                            merge_line.push_str(&format!("{color}|{RESET}"));
                        } else {
                            merge_line.push(' ');
                        }
                        merge_line.push(' ');
                    }
                    writeln!(f, "{merge_line}")?;
                    active_columns[col] = false;
                } else {
                    // Normal connector line
                    if !more_in_column {
                        active_columns[col] = false;
                    }
                    let mut line = String::new();
                    for c in 0..self.num_columns {
                        if active_columns[c] {
                            let color = branch_color(c);
                            line.push_str(&format!("{color}|{RESET} "));
                        } else {
                            line.push_str("  ");
                        }
                    }
                    writeln!(f, "{line}")?;
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for AncestryGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nodes.is_empty() {
            return writeln!(f, "(empty history)");
        }

        if self.num_columns <= 1 {
            self.fmt_linear(f)
        } else {
            self.fmt_tree(f)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn make_snapshot(id_byte: u8, parent_id_byte: Option<u8>) -> SnapshotInfo {
        let mut id_bytes = [0u8; 12];
        id_bytes[0] = id_byte;
        let parent_id = parent_id_byte.map(|b| {
            let mut p = [0u8; 12];
            p[0] = b;
            SnapshotId::new(p)
        });
        SnapshotInfo {
            id: SnapshotId::new(id_bytes),
            parent_id,
            flushed_at: Utc::now(),
            message: format!("Commit {id_byte}"),
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn test_from_linear_empty() {
        let graph = AncestryGraph::from_linear(vec![], &HashMap::new(), &HashMap::new());
        assert_eq!(graph.nodes.len(), 0);
        assert_eq!(graph.num_columns, 1);
    }

    #[test]
    fn test_from_linear_with_labels() {
        let s1 = make_snapshot(1, None);
        let s2 = make_snapshot(2, Some(1));
        let s3 = make_snapshot(3, Some(2));

        let mut branch_map = HashMap::new();
        branch_map.insert(s3.id.clone(), vec!["main".to_string()]);

        let mut tag_map = HashMap::new();
        tag_map.insert(s1.id.clone(), vec!["v1.0".to_string()]);

        let graph =
            AncestryGraph::from_linear(vec![s3.clone(), s2.clone(), s1.clone()], &branch_map, &tag_map);

        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.num_columns, 1);
        assert_eq!(graph.nodes[0].branches, vec!["main"]);
        assert!(graph.nodes[1].branches.is_empty());
        assert_eq!(graph.nodes[2].tags, vec!["v1.0"]);
        // All in column 0
        assert!(graph.nodes.iter().all(|n| n.column == 0));
    }

    #[test]
    fn test_from_tree_empty() {
        let graph = AncestryGraph::from_tree(vec![], &HashMap::new());
        assert_eq!(graph.nodes.len(), 0);
        assert_eq!(graph.num_columns, 0);
    }

    #[test]
    fn test_from_tree_deduplicates() {
        // main: s3 -> s2 -> s1
        // feat:  s4 -> s2 -> s1  (s2 is the fork point)
        let s1 = make_snapshot(1, None);
        let s2 = make_snapshot(2, Some(1));
        let s3 = make_snapshot(3, Some(2));
        let s4 = make_snapshot(4, Some(2));

        let branch_ancestries = vec![
            ("main".to_string(), vec![s3.clone(), s2.clone(), s1.clone()]),
            ("feat".to_string(), vec![s4.clone(), s2.clone(), s1.clone()]),
        ];

        let graph = AncestryGraph::from_tree(branch_ancestries, &HashMap::new());

        // s1, s2, s3 from main + s4 from feat = 4 unique nodes
        assert_eq!(graph.nodes.len(), 4);
        assert_eq!(graph.num_columns, 2);

        // s3 should be in column 0 (main), s4 in column 1 (feat)
        let s3_node = graph.nodes.iter().find(|n| n.info.id == s3.id);
        let s4_node = graph.nodes.iter().find(|n| n.info.id == s4.id);
        assert!(s3_node.is_some());
        assert!(s4_node.is_some());
        assert_eq!(s3_node.map(|n| n.column), Some(0));
        assert_eq!(s4_node.map(|n| n.column), Some(1));
    }
}
