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

impl fmt::Display for AncestryGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Placeholder — will be replaced with colored ASCII rendering in Step 3
        for node in &self.nodes {
            let short_id = &node.info.id.to_string()[..8];
            let mut labels = Vec::new();
            for b in &node.branches {
                labels.push(b.clone());
            }
            for t in &node.tags {
                labels.push(t.clone());
            }
            let label_str = if labels.is_empty() {
                String::new()
            } else {
                format!(" ({})", labels.join(", "))
            };
            writeln!(f, "* {}{} {}", short_id, label_str, node.info.message)?;
        }
        Ok(())
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
