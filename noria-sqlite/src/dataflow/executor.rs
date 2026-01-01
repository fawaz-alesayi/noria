//! Local single-threaded dataflow executor.
//!
//! The executor manages a graph of operators and propagates records through them.

use std::collections::HashMap;
use noria::DataType;
use super::{Record, Records};
use super::ops::{Operator, OperatorType, ProcessingResult};
use super::state::{State, MemoryState, LookupResult};

/// Index into the node array.
pub type NodeIndex = usize;

/// A node in the dataflow graph.
struct Node {
    /// The operator for this node.
    operator: Option<OperatorType>,
    /// Parent node indices.
    parents: Vec<NodeIndex>,
    /// Child node indices.
    children: Vec<NodeIndex>,
    /// Materialized state for this node (if any).
    state: Option<Box<dyn State>>,
    /// Human-readable name.
    name: String,
    /// Column names.
    columns: Vec<String>,
}

/// Handle to a materialized view.
#[derive(Clone)]
pub struct ViewHandle {
    node: NodeIndex,
    key_columns: Vec<usize>,
}

impl ViewHandle {
    /// Get the node index.
    pub fn node(&self) -> NodeIndex {
        self.node
    }

    /// Get the key columns.
    pub fn key_columns(&self) -> &[usize] {
        &self.key_columns
    }
}

/// The local dataflow executor.
pub struct LocalExecutor {
    /// All nodes in the graph.
    nodes: Vec<Node>,
    /// Map from table name to base table node.
    base_tables: HashMap<String, NodeIndex>,
}

impl LocalExecutor {
    /// Create a new executor.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            base_tables: HashMap::new(),
        }
    }

    /// Add a base table.
    pub fn add_base_table(&mut self, name: &str, columns: Vec<String>) -> NodeIndex {
        let idx = self.nodes.len();
        self.nodes.push(Node {
            operator: None, // Base tables have no operator
            parents: vec![],
            children: vec![],
            state: None,
            name: name.to_string(),
            columns,
        });
        self.base_tables.insert(name.to_string(), idx);
        idx
    }

    /// Add an operator node.
    pub fn add_operator(
        &mut self,
        name: &str,
        operator: OperatorType,
        parents: Vec<NodeIndex>,
        columns: Vec<String>,
    ) -> NodeIndex {
        let idx = self.nodes.len();

        // Register this node as a child of its parents
        for &parent in &parents {
            self.nodes[parent].children.push(idx);
        }

        self.nodes.push(Node {
            operator: Some(operator),
            parents,
            children: vec![],
            state: None,
            name: name.to_string(),
            columns,
        });

        idx
    }

    /// Materialize a node's output with the given key columns.
    pub fn materialize(&mut self, node: NodeIndex, key_columns: Vec<usize>) -> ViewHandle {
        let state = Box::new(MemoryState::new(key_columns.clone()));
        self.nodes[node].state = Some(state);

        ViewHandle {
            node,
            key_columns,
        }
    }

    /// Look up rows from a materialized view.
    pub fn lookup(&self, view: &ViewHandle, key: &[DataType]) -> Option<Vec<Vec<DataType>>> {
        let node = &self.nodes[view.node];
        match &node.state {
            Some(state) => match state.lookup(key) {
                LookupResult::Some(rows) => {
                    Some(rows.into_iter().map(|r| r.to_vec()).collect())
                }
                LookupResult::Empty => Some(vec![]),
                LookupResult::Missing => None,
            },
            None => None,
        }
    }

    /// Apply a write to a base table and propagate through the graph.
    pub fn apply_write(&mut self, table: &str, records: Records) {
        let base_node = match self.base_tables.get(table) {
            Some(&idx) => idx,
            None => return, // Unknown table
        };

        // Propagate from base table through graph
        self.propagate(base_node, records);
    }

    /// Propagate records from a node to its children.
    fn propagate(&mut self, from: NodeIndex, records: Records) {
        if records.is_empty() {
            return;
        }

        // Update state of the source node if it's materialized
        // (important for base tables that are directly materialized)
        if let Some(ref mut state) = self.nodes[from].state {
            let mut records_for_state = records.clone();
            state.process_records(&mut records_for_state);
        }

        // Get children to propagate to
        let children: Vec<NodeIndex> = self.nodes[from].children.clone();

        for child in children {
            // Find which parent index we are for the child
            let parent_idx = self.nodes[child]
                .parents
                .iter()
                .position(|&p| p == from)
                .unwrap_or(0);

            // For joins, we need to look up the other parent's state
            // Get the other parent index first (if any)
            let other_parent_idx = if self.nodes[child].parents.len() > 1 {
                Some(self.nodes[child].parents[1 - parent_idx])
            } else {
                None
            };

            // Process through the operator
            // We need to be careful about borrows here
            let output = if self.nodes[child].operator.is_some() {
                // For joins, we need state from the other parent
                // Since we can't hold mutable and immutable borrows simultaneously,
                // we'll pass None for now and handle joins specially later
                // TODO: Implement proper join state lookup
                let node = &mut self.nodes[child];
                let op = node.operator.as_mut().unwrap();

                // For non-join operators, pass None as state
                // Joins will need special handling
                let _ = other_parent_idx; // silence warning for now
                op.process(parent_idx, records.clone(), None)
            } else {
                ProcessingResult { results: records.clone(), lookups_needed: vec![] }
            };

            // Continue propagation to children
            // (state update happens at the start of propagate)
            self.propagate(child, output.results);
        }
    }

    /// Get statistics about the executor.
    pub fn stats(&self) -> ExecutorStats {
        let mut total_rows = 0;
        let mut materialized_nodes = 0;

        for node in &self.nodes {
            if let Some(ref state) = node.state {
                total_rows += state.len();
                materialized_nodes += 1;
            }
        }

        ExecutorStats {
            node_count: self.nodes.len(),
            materialized_nodes,
            total_rows,
        }
    }

    /// Get node information for debugging.
    pub fn describe(&self) -> String {
        let mut result = String::new();
        for (i, node) in self.nodes.iter().enumerate() {
            let op_desc = node.operator.as_ref()
                .map(|o| o.description())
                .unwrap_or_else(|| "BASE".to_string());

            result.push_str(&format!(
                "[{}] {} ({}) parents={:?} children={:?} materialized={}\n",
                i,
                node.name,
                op_desc,
                node.parents,
                node.children,
                node.state.is_some()
            ));
        }
        result
    }
}

impl Default for LocalExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the executor.
#[derive(Debug, Clone)]
pub struct ExecutorStats {
    pub node_count: usize,
    pub materialized_nodes: usize,
    pub total_rows: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::ops::{FilterOp, FilterCondition, ProjectOp, AggregateOp, AggregateFunc};

    #[test]
    fn test_simple_base_table() {
        let mut executor = LocalExecutor::new();

        let users = executor.add_base_table("users", vec!["id".into(), "name".into()]);
        let view = executor.materialize(users, vec![0]);

        // Insert data
        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
            vec![DataType::Int(2), DataType::from("Bob")],
        ].into();

        executor.apply_write("users", records);

        // Lookup
        let result = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], DataType::from("Alice"));
    }

    #[test]
    fn test_filter_chain() {
        let mut executor = LocalExecutor::new();

        // Base table
        let users = executor.add_base_table("users", vec!["id".into(), "name".into(), "active".into()]);

        // Filter for active users
        let filter = executor.add_operator(
            "active_users",
            OperatorType::Filter(FilterOp::new(
                FilterCondition::Eq(2, DataType::Int(1)), // active = 1
                3,
                vec![0],
            )),
            vec![users],
            vec!["id".into(), "name".into(), "active".into()],
        );

        let view = executor.materialize(filter, vec![0]);

        // Insert data
        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice"), DataType::Int(1)],
            vec![DataType::Int(2), DataType::from("Bob"), DataType::Int(0)],
            vec![DataType::Int(3), DataType::from("Charlie"), DataType::Int(1)],
        ].into();

        executor.apply_write("users", records);

        // Only active users should be in the view
        let result1 = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().len(), 1);

        let result2 = executor.lookup(&view, &[DataType::Int(2)]);
        assert!(result2.is_none() || result2.unwrap().is_empty());

        let result3 = executor.lookup(&view, &[DataType::Int(3)]);
        assert!(result3.is_some());
        assert_eq!(result3.unwrap().len(), 1);
    }

    #[test]
    fn test_aggregation() {
        let mut executor = LocalExecutor::new();

        // Base table: votes(article_id)
        let votes = executor.add_base_table("votes", vec!["article_id".into()]);

        // Count votes per article
        let vote_count = executor.add_operator(
            "vote_count",
            OperatorType::Aggregate(AggregateOp::new(
                vec![0], // group by article_id
                AggregateFunc::Count,
                vec![0],
            )),
            vec![votes],
            vec!["article_id".into(), "count".into()],
        );

        let view = executor.materialize(vote_count, vec![0]);

        // Insert votes
        let records: Records = vec![
            vec![DataType::Int(1)], // vote for article 1
            vec![DataType::Int(1)], // vote for article 1
            vec![DataType::Int(2)], // vote for article 2
        ].into();

        executor.apply_write("votes", records);

        // Check counts
        let result1 = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result1.is_some());
        let rows1 = result1.unwrap();
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0][1], DataType::BigInt(2)); // 2 votes for article 1

        let result2 = executor.lookup(&view, &[DataType::Int(2)]);
        assert!(result2.is_some());
        let rows2 = result2.unwrap();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][1], DataType::BigInt(1)); // 1 vote for article 2
    }

    #[test]
    fn test_incremental_delete() {
        let mut executor = LocalExecutor::new();

        let users = executor.add_base_table("users", vec!["id".into(), "name".into()]);
        let view = executor.materialize(users, vec![0]);

        // Insert
        let insert_records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
        ].into();
        executor.apply_write("users", insert_records);

        let result = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);

        // Delete
        let delete_records = Records::from(vec![
            Record::Negative(vec![DataType::Int(1), DataType::from("Alice")]),
        ]);
        executor.apply_write("users", delete_records);

        let result = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_none() || result.unwrap().is_empty());
    }

    #[test]
    fn test_executor_stats() {
        let mut executor = LocalExecutor::new();

        let users = executor.add_base_table("users", vec!["id".into(), "name".into()]);
        let _view = executor.materialize(users, vec![0]);

        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
            vec![DataType::Int(2), DataType::from("Bob")],
        ].into();

        executor.apply_write("users", records);

        let stats = executor.stats();
        assert_eq!(stats.node_count, 1);
        assert_eq!(stats.materialized_nodes, 1);
        assert_eq!(stats.total_rows, 2);
    }
}
