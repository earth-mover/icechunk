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

/// Look up labels for a snapshot from a reverse map, returning an empty vec if absent.
fn lookup_labels(map: &HashMap<SnapshotId, Vec<String>>, id: &SnapshotId) -> Vec<String> {
    map.get(id).cloned().unwrap_or_default()
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
                let branches = lookup_labels(branch_map, &info.id);
                let tags = lookup_labels(tag_map, &info.id);
                AncestryNode { info, branches, tags, column: 0 }
            })
            .collect();

        Self { nodes, num_columns: 1 }
    }

    /// Build a tree graph from multiple branches.
    ///
    /// `branch_ancestries` is a list of `(branch_name, snapshots)` where each snapshot
    /// list is in ancestry order (newest first). The first entry is the "trunk" (column 0)
    /// — its commits and shared ancestors stay in that column.
    ///
    /// `tag_map` provides tag label decorations keyed by snapshot ID.
    pub fn from_tree(
        branch_ancestries: Vec<(String, Vec<SnapshotInfo>)>,
        tag_map: &HashMap<SnapshotId, Vec<String>>,
    ) -> Self {
        if branch_ancestries.is_empty() {
            return Self { nodes: Vec::new(), num_columns: 0 };
        }

        // Register all branch tip labels up front so they're available regardless
        // of which branch "claims" the snapshot during deduplication.
        let mut branch_labels: HashMap<SnapshotId, Vec<String>> = HashMap::new();
        for (name, snapshots) in &branch_ancestries {
            if let Some(tip) = snapshots.first() {
                branch_labels.entry(tip.id.clone()).or_default().push(name.clone());
            }
        }

        let mut seen: HashMap<SnapshotId, usize> = HashMap::new();
        let mut node_map: HashMap<SnapshotId, AncestryNode> = HashMap::new();
        let num_columns = branch_ancestries.len();

        for (col, (_name, snapshots)) in branch_ancestries.iter().enumerate() {
            for info in snapshots {
                if seen.contains_key(&info.id) {
                    break; // Hit a shared ancestor — stop walking this branch
                }
                seen.insert(info.id.clone(), col);
                node_map.insert(info.id.clone(), AncestryNode {
                    branches: lookup_labels(&branch_labels, &info.id),
                    tags: lookup_labels(tag_map, &info.id),
                    column: col,
                    info: info.clone(),
                });
            }
        }

        let mut nodes: Vec<AncestryNode> = node_map.into_values().collect();
        nodes.sort_by(|a, b| b.info.flushed_at.cmp(&a.info.flushed_at));

        Self { nodes, num_columns }
    }
}

// -- ANSI rendering ----------------------------------------------------------

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";

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

fn truncate_message(msg: &str, max_len: usize) -> String {
    let first_line = msg.lines().next().unwrap_or("");
    if first_line.len() <= max_len {
        first_line.to_string()
    } else {
        format!("{}...", &first_line[..max_len - 3])
    }
}

/// What glyph to draw in a given column position.
enum Glyph {
    /// The active node: `*`
    Node,
    /// A continuing branch line: `|`
    Pipe,
    /// A merge connector: `/`
    Merge,
    /// Empty space
    Blank,
}

/// Build a line of column glyphs with ANSI colors, 2 chars per column.
fn render_columns(num_columns: usize, glyph_for: impl Fn(usize) -> (Glyph, usize)) -> String {
    let mut out = String::with_capacity(num_columns * 8);
    for c in 0..num_columns {
        let (glyph, color_col) = glyph_for(c);
        let color = branch_color(color_col);
        match glyph {
            Glyph::Node => out.push_str(&format!("{color}*{RESET} ")),
            Glyph::Pipe => out.push_str(&format!("{color}|{RESET} ")),
            Glyph::Merge => out.push_str(&format!("{color}/{RESET} ")),
            Glyph::Blank => out.push_str("  "),
        }
    }
    out
}

impl AncestryGraph {
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

    fn fmt_linear(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let color = branch_color(0);
        for (i, node) in self.nodes.iter().enumerate() {
            let short_id = &node.info.id.to_string()[..8];
            let labels = Self::format_labels(node);
            let msg = truncate_message(&node.info.message, 60);
            writeln!(f, "{color}*{RESET} {DIM}{short_id}{RESET}{labels} {msg}")?;
            if i < self.nodes.len() - 1 {
                writeln!(f, "{color}|{RESET}")?;
            }
        }
        Ok(())
    }

    fn fmt_tree(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut active: Vec<bool> = vec![false; self.num_columns];

        // Pre-compute: for each side-branch, find its fork point (parent of its oldest node).
        let mut fork_targets: HashMap<usize, SnapshotId> = HashMap::new();
        for col in 1..self.num_columns {
            if let Some(last) = self.nodes.iter().rev().find(|n| n.column == col) {
                if let Some(pid) = &last.info.parent_id {
                    fork_targets.insert(col, pid.clone());
                }
            }
        }

        for (i, node) in self.nodes.iter().enumerate() {
            let col = node.column;
            active[col] = true;

            // Node line: * for this column, | for other active columns
            let prefix = render_columns(self.num_columns, |c| {
                if c == col { (Glyph::Node, c) }
                else if active[c] { (Glyph::Pipe, c) }
                else { (Glyph::Blank, c) }
            });
            let short_id = &node.info.id.to_string()[..8];
            let labels = Self::format_labels(node);
            let msg = truncate_message(&node.info.message, 60);
            writeln!(f, "{prefix}{DIM}{short_id}{RESET}{labels} {msg}")?;

            if i >= self.nodes.len() - 1 {
                continue;
            }

            let has_more = self.nodes[i + 1..].iter().any(|n| n.column == col);

            // Columns merging into this node (their fork target is this node's id)
            let mut merging: Vec<usize> = fork_targets
                .iter()
                .filter(|(c, id)| **id == node.info.id && active[**c])
                .map(|(c, _)| *c)
                .collect();
            merging.sort();

            if !merging.is_empty() {
                // Draw a / merge line for each merging column
                for &mc in &merging {
                    let line = render_columns(self.num_columns, |c| {
                        if c == mc { (Glyph::Merge, mc) }
                        else if c == col { (Glyph::Pipe, c) }
                        else if active[c] { (Glyph::Pipe, c) }
                        else { (Glyph::Blank, c) }
                    });
                    writeln!(f, "{line}")?;
                    active[mc] = false;
                }
            } else if !has_more && col != 0 {
                // Side branch's last node — draw / toward trunk
                let line = render_columns(self.num_columns, |c| {
                    if c == col { (Glyph::Merge, col) }
                    else if active[c] { (Glyph::Pipe, c) }
                    else { (Glyph::Blank, c) }
                });
                writeln!(f, "{line}")?;
                active[col] = false;
            } else {
                // Normal connector line
                if !has_more {
                    active[col] = false;
                }
                let line = render_columns(self.num_columns, |c| {
                    if active[c] { (Glyph::Pipe, c) } else { (Glyph::Blank, c) }
                });
                writeln!(f, "{line}")?;
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
    use chrono::{Duration, Utc};
    use std::collections::BTreeMap;

    /// Create a snapshot with a distinct timestamp based on id_byte
    /// (higher id_byte = newer, so sorting by timestamp desc gives
    /// highest id_byte first).
    fn make_snapshot(id_byte: u8, parent_id_byte: Option<u8>) -> SnapshotInfo {
        let mut id_bytes = [0u8; 12];
        id_bytes[0] = id_byte;
        let parent_id = parent_id_byte.map(|b| {
            let mut p = [0u8; 12];
            p[0] = b;
            SnapshotId::new(p)
        });
        let base = Utc::now() - Duration::hours(100);
        SnapshotInfo {
            id: SnapshotId::new(id_bytes),
            parent_id,
            flushed_at: base + Duration::minutes(id_byte as i64),
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

    #[test]
    fn test_minimal_fork_display() {
        // Simplest possible fork:
        //   main: s1 -> s2 (tip)
        //   feat: s1 -> s3 (tip)
        //
        // Expected output (stripped of ANSI):
        //     * (feat) Commit 3
        //    /
        //   * (main) Commit 2
        //   |
        //   * Commit 1
        //
        // Key: no trunk | above the trunk's first * node.
        let s1 = make_snapshot(1, None);
        let s2 = make_snapshot(2, Some(1));
        let s3 = make_snapshot(3, Some(1));

        let branch_ancestries = vec![
            ("main".to_string(), vec![s2.clone(), s1.clone()]),
            ("feat".to_string(), vec![s3.clone(), s1.clone()]),
        ];

        let graph = AncestryGraph::from_tree(branch_ancestries, &HashMap::new());
        let output = graph.to_string();
        let plain = strip_ansi(&output);
        let lines: Vec<&str> = plain.lines().collect();

        eprintln!("=== minimal fork display ===");
        for line in &lines {
            eprintln!("  {:?}", line);
        }

        // First line: feat tip — should NOT have a | for trunk before it
        assert!(lines[0].contains("feat"), "first line should have feat label");
        assert!(lines[0].contains("Commit 3"), "first line should be s3");
        assert!(
            !lines[0].starts_with('|'),
            "trunk | should not appear above trunk's first node: {:?}",
            lines[0]
        );

        // There should be a / fork connector
        let has_fork = lines.iter().any(|l| l.contains('/'));
        assert!(has_fork, "should have a / fork connector: {plain}");

        // main tip should appear
        assert!(
            lines.iter().any(|l| l.contains("main") && l.contains("Commit 2")),
            "should have main Commit 2: {plain}"
        );
    }

    /// Strip ANSI escape codes from a string.
    fn strip_ansi(s: &str) -> String {
        let mut result = String::new();
        let mut in_escape = false;
        for c in s.chars() {
            if c == '\x1b' {
                in_escape = true;
            } else if in_escape {
                if c.is_ascii_alphabetic() {
                    in_escape = false;
                }
            } else {
                result.push(c);
            }
        }
        result
    }

    #[test]
    fn test_from_tree_three_branches_trunk_priority() {
        // Reproduces the demo scenario:
        //   main:       s1 -> s2 -> s3 (tip)
        //   experiment: s1 -> s2 -> s4 -> s5 (tip, forks from s2)
        //   hotfix:     s1 -> s2 -> s3 -> s6 (tip, forks from s3)
        //
        // "main" should be trunk (column 0) — its commits + shared ancestors
        // stay in column 0 even though other branches share them.
        let s1 = make_snapshot(1, None);       // repo init
        let s2 = make_snapshot(2, Some(1));    // shared ancestor
        let s3 = make_snapshot(3, Some(2));    // main tip
        let s4 = make_snapshot(4, Some(2));    // experiment commit
        let s5 = make_snapshot(5, Some(4));    // experiment tip
        let s6 = make_snapshot(6, Some(3));    // hotfix tip

        // main is listed FIRST (trunk), then others
        let branch_ancestries = vec![
            (
                "main".to_string(),
                vec![s3.clone(), s2.clone(), s1.clone()],
            ),
            (
                "experiment".to_string(),
                vec![s5.clone(), s4.clone(), s2.clone(), s1.clone()],
            ),
            (
                "hotfix".to_string(),
                vec![s6.clone(), s3.clone(), s2.clone(), s1.clone()],
            ),
        ];

        let mut tag_map = HashMap::new();
        tag_map.insert(s2.id.clone(), vec!["v1.0".to_string()]);

        let graph = AncestryGraph::from_tree(branch_ancestries, &tag_map);

        // 6 unique snapshots
        assert_eq!(graph.nodes.len(), 6);

        // Shared ancestors (s1, s2, s3) must be in column 0 (trunk)
        let find = |id: &SnapshotId| graph.nodes.iter().find(|n| &n.info.id == id);

        let s1_node = find(&s1.id).expect("s1 missing");
        let s2_node = find(&s2.id).expect("s2 missing");
        let s3_node = find(&s3.id).expect("s3 missing");
        let s4_node = find(&s4.id).expect("s4 missing");
        let s5_node = find(&s5.id).expect("s5 missing");
        let s6_node = find(&s6.id).expect("s6 missing");

        assert_eq!(s1_node.column, 0, "s1 (shared root) should be trunk");
        assert_eq!(s2_node.column, 0, "s2 (shared ancestor) should be trunk");
        assert_eq!(s3_node.column, 0, "s3 (main tip) should be trunk");
        assert_eq!(s5_node.column, 1, "s5 (experiment tip) should be side branch");
        assert_eq!(s4_node.column, 1, "s4 (experiment commit) should be side branch");
        assert_eq!(s6_node.column, 2, "s6 (hotfix tip) should be side branch");

        // Branch labels should be on the right nodes
        assert!(s3_node.branches.contains(&"main".to_string()));
        assert!(s5_node.branches.contains(&"experiment".to_string()));
        assert!(s6_node.branches.contains(&"hotfix".to_string()));

        // Tag should be on s2
        assert!(s2_node.tags.contains(&"v1.0".to_string()));

        // Display output should mention all branches and be connected
        let output = graph.to_string();
        assert!(output.contains("main"), "output should mention main");
        assert!(output.contains("experiment"), "output should mention experiment");
        assert!(output.contains("hotfix"), "output should mention hotfix");
        assert!(output.contains("v1.0"), "output should mention v1.0 tag");

        // Trunk nodes should appear with * in column 0
        // Side branch nodes should appear with * in their column
        // There should be fork connector lines (/ characters)
        assert!(
            output.contains('/') || output.contains('\\'),
            "tree output should contain fork connectors: {output}"
        );
    }
}
