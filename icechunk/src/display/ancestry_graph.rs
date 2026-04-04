use std::collections::HashMap;

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

/// Maximum number of nodes to display before truncating.
const MAX_DISPLAY_NODES: usize = 100;

/// A materialized view of repository commit history, suitable for rendering.
///
/// Built via [`Repository::ancestry_graph()`](crate::Repository) — either for a single
/// ref (linear history) or all branches (tree view).
///
/// Note: only commits reachable from branches are included. Anonymous/detached
/// snapshots (created via `session.commit("msg").anonymous()`) are not attached
/// to any branch and will not appear in the graph.
#[derive(Debug, Clone)]
pub struct AncestryGraph {
    /// Nodes in display order (newest first).
    pub nodes: Vec<AncestryNode>,
    /// Number of columns needed for rendering (1 for single-branch linear view).
    pub num_columns: usize,
    /// All branch names in the repo (sorted), used for consistent color assignment.
    pub all_branches: Vec<String>,
    /// Total number of snapshots before truncation (0 if not truncated).
    pub total_snapshots: usize,
}

/// Look up labels for a snapshot from a reverse map, returning an empty vec if absent.
fn lookup_labels(map: &HashMap<SnapshotId, Vec<String>>, id: &SnapshotId) -> Vec<String> {
    map.get(id).cloned().unwrap_or_default()
}

/// Sort branch names with "main" first, then alphabetically.
/// This determines both the trunk (column 0) and the color assignment order.
fn sort_branches_main_first(branches: &mut [String]) {
    branches.sort_by(|a, b| {
        let a_is_main = a == "main";
        let b_is_main = b == "main";
        b_is_main.cmp(&a_is_main).then(a.cmp(b))
    });
}

impl AncestryGraph {
    /// Build an ancestry graph from branch ancestry data.
    ///
    /// # Arguments
    ///
    /// - `branch_ancestries`: each entry is `(branch_name, snapshots)` where snapshots
    ///   are in ancestry order (newest first). Pass a single entry for linear
    ///   (single-branch) view. The entries are sorted internally so that "main" becomes
    ///   the trunk (column 0) — callers don't need to pre-sort.
    /// - `tag_map`: `snapshot_id` → tag names. Used to decorate nodes with tag labels
    ///   (e.g. "v1.0") so they appear in the rendered output.
    /// - `all_branches`: the full set of branch names in the repo (not just the ones
    ///   being displayed). Used to assign each branch a stable color index so that
    ///   e.g. "main" gets the same color whether viewing one branch or all branches.
    pub fn new(
        mut branch_ancestries: Vec<(String, Vec<SnapshotInfo>)>,
        tag_map: &HashMap<SnapshotId, Vec<String>>,
        all_branches: Vec<String>,
    ) -> Self {
        // Sort so "main" is the trunk (column 0), then alphabetically.
        branch_ancestries.sort_by(|a, b| {
            let a_is_main = a.0 == "main";
            let b_is_main = b.0 == "main";
            b_is_main.cmp(&a_is_main).then(a.0.cmp(&b.0))
        });

        // Sort the full branch list too for consistent color indexing.
        let mut all_branches = all_branches;
        sort_branches_main_first(&mut all_branches);

        if branch_ancestries.is_empty() {
            return Self {
                nodes: Vec::new(),
                num_columns: 0,
                all_branches,
                total_snapshots: 0,
            };
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
                node_map.insert(
                    info.id.clone(),
                    AncestryNode {
                        branches: lookup_labels(&branch_labels, &info.id),
                        tags: lookup_labels(tag_map, &info.id),
                        column: col,
                        info: info.clone(),
                    },
                );
            }
        }

        let mut nodes: Vec<AncestryNode> = node_map.into_values().collect();
        nodes.sort_by(|a, b| b.info.flushed_at.cmp(&a.info.flushed_at));

        let total_snapshots = nodes.len();
        if total_snapshots > MAX_DISPLAY_NODES {
            nodes.truncate(MAX_DISPLAY_NODES);
        }

        Self { nodes, num_columns, all_branches, total_snapshots }
    }

    /// Get a stable color index for a branch name based on its position in the
    /// sorted list of all branches in the repo.
    fn color_index_for_branch(&self, name: &str) -> usize {
        self.all_branches.iter().position(|b| b == name).unwrap_or(0)
    }

    /// Render the graph as a plain string with no ANSI color codes.
    /// Useful for CI logs, piping to files, or consumption by LLM agents.
    pub fn to_plain_string(&self) -> String {
        strip_ansi(&self.to_string())
    }
}

/// Strip ANSI escape codes from a string.
fn strip_ansi(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
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

// -- Shared layout pass ------------------------------------------------------
//
// The layout pass converts the graph into positioned elements that can be
// consumed by any renderer (ANSI text, SVG, etc.). This separation means the
// graph traversal logic (active columns, fork detection) is written once.

// TODO: Merge commit support and Sugiyama layout algorithm
//
// Currently icechunk snapshots are single-parent only, so the commit graph is a
// tree (branches diverge but never reconverge). The layout is trivial: each branch
// gets its own column, and fork points are where a side branch's oldest commit's
// parent lives in the trunk column.
//
// If icechunk adds merge commits (multi-parent snapshots), the graph becomes a DAG
// and the layout problem gets harder:
// - Edges can cross: two branches merging into different targets may produce
//   crossing lines that need to be minimized for readability.
// - Column assignment is no longer one-branch-per-column: a merge commit has
//   parents in multiple columns, requiring connector lines between them.
//
// The standard solution is the Sugiyama algorithm (layered graph drawing):
//   1. Layer assignment — assign each node to a row based on depth
//   2. Crossing reduction — reorder nodes within each row to minimize edge crossings
//   3. Coordinate assignment — compute x positions to keep edges short and straight
//
// At that point, consider using a layout crate (e.g. `ascii-dag` for text output)
// rather than hand-rolling the algorithm.

/// A positioned element in the graph layout, independent of rendering format.
#[derive(Debug, Clone)]
pub enum LayoutElement {
    /// A commit node at (row, column) — index into `AncestryGraph::nodes`.
    Node { row: usize, col: usize, node_idx: usize },
    /// A vertical line segment in a column, between two rows.
    Line { from_row: usize, to_row: usize, col: usize },
    /// A diagonal fork/merge line from one (row, col) to another.
    Fork { from_row: usize, from_col: usize, to_row: usize, to_col: usize },
}

impl AncestryGraph {
    /// Get a stable color index for a column by looking up which branch owns it.
    pub fn column_colors(&self) -> Vec<usize> {
        (0..self.num_columns)
            .map(|col| {
                self.nodes
                    .iter()
                    .find(|n| n.column == col)
                    .and_then(|n| n.branches.first())
                    .map(|name| self.color_index_for_branch(name))
                    .unwrap_or(col)
            })
            .collect()
    }

    /// Compute the graph layout as a list of positioned elements.
    ///
    /// Each node gets a row (its index in `self.nodes`) and its column. Lines and
    /// fork connectors are emitted between rows. Both the ANSI and SVG renderers
    /// consume this same layout.
    pub fn layout(&self) -> Vec<LayoutElement> {
        let mut elements = Vec::new();
        let mut active: Vec<bool> = vec![false; self.num_columns];

        // Pre-compute fork targets: side-branch column → fork point snapshot id.
        let mut fork_targets: HashMap<usize, SnapshotId> = HashMap::new();
        for col in 1..self.num_columns {
            if let Some(pid) = self
                .nodes
                .iter()
                .rev()
                .find(|n| n.column == col)
                .and_then(|last| last.info.parent_id.as_ref())
            {
                fork_targets.insert(col, pid.clone());
            }
        }

        for (row, node) in self.nodes.iter().enumerate() {
            let col = node.column;
            active[col] = true;

            elements.push(LayoutElement::Node { row, col, node_idx: row });

            if row >= self.nodes.len() - 1 {
                continue;
            }

            let next_row = row + 1;
            let has_more = self.nodes[next_row..].iter().any(|n| n.column == col);

            // Check for columns merging into this node.
            let mut merging: Vec<usize> = fork_targets
                .iter()
                .filter(|(c, id)| **id == node.info.id && active[**c])
                .map(|(c, _)| *c)
                .collect();
            merging.sort();

            if !merging.is_empty() {
                for &mc in &merging {
                    elements.push(LayoutElement::Fork {
                        from_row: row,
                        from_col: mc,
                        to_row: next_row,
                        to_col: col,
                    });
                    // Emit pipe lines for other active columns on this connector row.
                    for (c, is_active) in active.iter().enumerate() {
                        if (c == col || *is_active) && c != mc {
                            elements.push(LayoutElement::Line {
                                from_row: row,
                                to_row: next_row,
                                col: c,
                            });
                        }
                    }
                    active[mc] = false;
                }
            } else if !has_more && col != 0 {
                // Side branch's last node — fork toward trunk.
                elements.push(LayoutElement::Fork {
                    from_row: row,
                    from_col: col,
                    to_row: next_row,
                    to_col: 0,
                });
                for (c, is_active) in active.iter().enumerate() {
                    if *is_active && c != col {
                        elements.push(LayoutElement::Line {
                            from_row: row,
                            to_row: next_row,
                            col: c,
                        });
                    }
                }
                active[col] = false;
            } else {
                // Normal connector lines for all active columns.
                if !has_more {
                    active[col] = false;
                }
                for (c, is_active) in active.iter().enumerate() {
                    if *is_active {
                        elements.push(LayoutElement::Line {
                            from_row: row,
                            to_row: next_row,
                            col: c,
                        });
                    }
                }
            }
        }

        elements
    }
}

// -- Color palette -----------------------------------------------------------
//
// Colors are an interleaved subset of the Earthmover primary and secondary
// brand palette. The first entry (lime green) is reserved for the main branch.
// Tags use a dedicated icechunk blue, separate from the branch palette.

struct BranchColor {
    ansi: &'static str,
    hex: &'static str,
}

/// Branch color palette. Index 0 = main (lime green), then rotating for others.
/// Excludes midnight (#201F2C) and dark grey (#787878) from the Earthmover
/// palette — they're invisible on dark backgrounds.
const BRANCH_PALETTE: &[BranchColor] = &[
    BranchColor { ansi: "\x1b[38;2;183;228;0m", hex: "#B7E400" }, // lime green (main)
    BranchColor { ansi: "\x1b[38;2;166;83;255m", hex: "#A653FF" }, // violet
    BranchColor { ansi: "\x1b[38;2;255;101;84m", hex: "#FF6554" }, // red
    BranchColor { ansi: "\x1b[38;2;255;158;13m", hex: "#FF9E0D" }, // orange
    BranchColor { ansi: "\x1b[38;2;248;129;209m", hex: "#F881D1" }, // pink
];

/// Dedicated tag color (icechunk blue), separate from branch palette.
pub const TAG_COLOR_ANSI: &str = "\x1b[38;2;94;196;247m";
pub const TAG_COLOR_HEX: &str = "#5EC4F7";

pub fn palette_ansi(idx: usize) -> &'static str {
    BRANCH_PALETTE[idx % BRANCH_PALETTE.len()].ansi
}

pub fn palette_hex(idx: usize) -> &'static str {
    BRANCH_PALETTE[idx % BRANCH_PALETTE.len()].hex
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use std::collections::BTreeMap;

    /// Create a snapshot with a distinct timestamp based on `id_byte`
    /// (higher `id_byte` = newer, so sorting by timestamp desc gives
    /// highest `id_byte` first).
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
    fn test_empty() {
        let graph = AncestryGraph::new(vec![], &HashMap::new(), vec![]);
        assert_eq!(graph.nodes.len(), 0);
        assert_eq!(graph.num_columns, 0);
    }

    #[test]
    fn test_single_branch_with_labels() {
        let s1 = make_snapshot(1, None);
        let s2 = make_snapshot(2, Some(1));
        let s3 = make_snapshot(3, Some(2));

        let mut tag_map = HashMap::new();
        tag_map.insert(s1.id.clone(), vec!["v1.0".to_string()]);

        let graph = AncestryGraph::new(
            vec![("main".to_string(), vec![s3.clone(), s2.clone(), s1.clone()])],
            &tag_map,
            vec!["main".to_string()],
        );

        assert_eq!(graph.nodes.len(), 3);
        assert_eq!(graph.num_columns, 1);
        assert_eq!(graph.nodes[0].branches, vec!["main"]);
        assert!(graph.nodes[1].branches.is_empty());
        assert_eq!(graph.nodes[2].tags, vec!["v1.0"]);
        // All in column 0
        assert!(graph.nodes.iter().all(|n| n.column == 0));
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

        let all = vec!["feat".to_string(), "main".to_string()];
        let graph = AncestryGraph::new(branch_ancestries, &HashMap::new(), all);

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

        let all = vec!["feat".to_string(), "main".to_string()];
        let graph = AncestryGraph::new(branch_ancestries, &HashMap::new(), all);
        let output = graph.to_string();
        let plain = strip_ansi(&output);
        let lines: Vec<&str> = plain.lines().collect();

        eprintln!("=== minimal fork display ===");
        for line in &lines {
            eprintln!("  {line:?}");
        }

        // First line: feat tip — should NOT have a | for trunk before it
        assert!(lines[0].contains("feat"), "first line should have feat label");
        assert!(lines[0].contains("Commit 3"), "first line should be s3");
        assert!(
            !lines[0].starts_with('│'),
            "trunk | should not appear above trunk's first node: {:?}",
            lines[0]
        );

        // There should be a / fork connector
        let has_fork = lines.iter().any(|l| l.contains('╱'));
        assert!(has_fork, "should have a / fork connector: {plain}");

        // main tip should appear
        assert!(
            lines.iter().any(|l| l.contains("main") && l.contains("Commit 2")),
            "should have main Commit 2: {plain}"
        );
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
        let s1 = make_snapshot(1, None); // repo init
        let s2 = make_snapshot(2, Some(1)); // shared ancestor
        let s3 = make_snapshot(3, Some(2)); // main tip
        let s4 = make_snapshot(4, Some(2)); // experiment commit
        let s5 = make_snapshot(5, Some(4)); // experiment tip
        let s6 = make_snapshot(6, Some(3)); // hotfix tip

        // main is listed FIRST (trunk), then others
        let branch_ancestries = vec![
            ("main".to_string(), vec![s3.clone(), s2.clone(), s1.clone()]),
            (
                "experiment".to_string(),
                vec![s5.clone(), s4.clone(), s2.clone(), s1.clone()],
            ),
            ("hotfix".to_string(), vec![s6.clone(), s3.clone(), s2.clone(), s1.clone()]),
        ];

        let mut tag_map = HashMap::new();
        tag_map.insert(s2.id.clone(), vec!["v1.0".to_string()]);

        let all =
            vec!["experiment".to_string(), "hotfix".to_string(), "main".to_string()];
        let graph = AncestryGraph::new(branch_ancestries, &tag_map, all);

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
            output.contains('╱'),
            "tree output should contain fork connectors: {output}"
        );
    }

    #[test]
    fn test_svg_output_structure() {
        let s1 = make_snapshot(1, None);
        let s2 = make_snapshot(2, Some(1));
        let s3 = make_snapshot(3, Some(1));

        let branch_ancestries = vec![
            ("main".to_string(), vec![s2.clone(), s1.clone()]),
            ("feat".to_string(), vec![s3.clone(), s1.clone()]),
        ];

        let all = vec!["feat".to_string(), "main".to_string()];
        let graph = AncestryGraph::new(branch_ancestries, &HashMap::new(), all);
        let svg = graph.to_svg();

        // Should be valid raw SVG
        assert!(svg.contains("<svg"), "should contain SVG element");
        assert!(svg.contains("</svg>"), "should close SVG element");

        // Should have circles for nodes
        assert!(svg.contains("<circle"), "should have circle elements for nodes");

        // Should have lines for connections
        assert!(
            svg.contains("<line") || svg.contains("<path"),
            "should have line or path elements for connections"
        );

        // Should contain branch labels and commit messages
        assert!(svg.contains("main"), "should mention main branch");
        assert!(svg.contains("feat"), "should mention feat branch");
        assert!(svg.contains("Commit 1"), "should contain commit message");
        assert!(svg.contains("Commit 2"), "should contain commit message");

        // Colors should come from the Earthmover hex palette
        assert!(
            svg.contains("#B7E400") || svg.contains("#A653FF"),
            "should use Earthmover brand hex colors"
        );
    }
}
