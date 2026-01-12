//! Dataflow operators.
//!
//! Each operator transforms input records into output records according to
//! its specific logic (filter, project, join, aggregate, etc.).

use std::collections::HashMap;
use noria::DataType;
use super::{Record, Records};
use super::state::{State, StateKey, LookupResult};

/// Result of processing records through an operator.
#[derive(Default)]
pub struct ProcessingResult {
    /// Output records to propagate downstream.
    pub results: Records,
    /// Keys that need to be looked up from upstream (for upquery).
    pub lookups_needed: Vec<Vec<DataType>>,
}

/// Trait that all operators implement.
pub trait Operator: Send {
    /// Process incoming records and produce output records.
    fn process(
        &mut self,
        from_parent: usize,
        records: Records,
        state: Option<&dyn State>,
    ) -> ProcessingResult;

    /// Get the key columns for this operator's output.
    fn key_columns(&self) -> &[usize];

    /// Get the number of output columns.
    fn output_columns(&self) -> usize;

    /// Human-readable description.
    fn description(&self) -> String;

    /// Whether this operator needs external state passed to process().
    ///
    /// Operators like Aggregate have internal state and don't need the view's state.
    /// Operators like Join need the other parent's state for lookups.
    ///
    /// When this returns false, the executor can skip expensive snapshot() calls.
    fn needs_state(&self) -> bool {
        false // Default: no state needed
    }
}

/// Types of operators we support.
#[derive(Debug, Clone)]
pub enum OperatorType {
    /// Filter rows based on a condition.
    Filter(FilterOp),
    /// Project/select columns.
    Project(ProjectOp),
    /// Join two inputs.
    Join(JoinOp),
    /// Aggregate (COUNT, SUM, etc.).
    Aggregate(AggregateOp),
    /// Identity (passthrough).
    Identity(IdentityOp),
}

impl Operator for OperatorType {
    fn process(
        &mut self,
        from_parent: usize,
        records: Records,
        state: Option<&dyn State>,
    ) -> ProcessingResult {
        match self {
            OperatorType::Filter(op) => op.process(from_parent, records, state),
            OperatorType::Project(op) => op.process(from_parent, records, state),
            OperatorType::Join(op) => op.process(from_parent, records, state),
            OperatorType::Aggregate(op) => op.process(from_parent, records, state),
            OperatorType::Identity(op) => op.process(from_parent, records, state),
        }
    }

    fn key_columns(&self) -> &[usize] {
        match self {
            OperatorType::Filter(op) => op.key_columns(),
            OperatorType::Project(op) => op.key_columns(),
            OperatorType::Join(op) => op.key_columns(),
            OperatorType::Aggregate(op) => op.key_columns(),
            OperatorType::Identity(op) => op.key_columns(),
        }
    }

    fn output_columns(&self) -> usize {
        match self {
            OperatorType::Filter(op) => op.output_columns(),
            OperatorType::Project(op) => op.output_columns(),
            OperatorType::Join(op) => op.output_columns(),
            OperatorType::Aggregate(op) => op.output_columns(),
            OperatorType::Identity(op) => op.output_columns(),
        }
    }

    fn description(&self) -> String {
        match self {
            OperatorType::Filter(op) => op.description(),
            OperatorType::Project(op) => op.description(),
            OperatorType::Join(op) => op.description(),
            OperatorType::Aggregate(op) => op.description(),
            OperatorType::Identity(op) => op.description(),
        }
    }

    fn needs_state(&self) -> bool {
        match self {
            OperatorType::Filter(op) => op.needs_state(),
            OperatorType::Project(op) => op.needs_state(),
            OperatorType::Join(op) => op.needs_state(),
            OperatorType::Aggregate(op) => op.needs_state(),
            OperatorType::Identity(op) => op.needs_state(),
        }
    }
}

// ============================================================================
// Filter Operator
// ============================================================================

/// Condition for filtering.
#[derive(Debug, Clone)]
pub enum FilterCondition {
    /// Column equals a literal value.
    Eq(usize, DataType),
    /// Column not equal to a literal value.
    Ne(usize, DataType),
    /// Column greater than a literal value.
    Gt(usize, DataType),
    /// Column less than a literal value.
    Lt(usize, DataType),
    /// Column is NULL.
    IsNull(usize),
    /// Column is not NULL.
    IsNotNull(usize),
    /// AND of multiple conditions.
    And(Vec<FilterCondition>),
    /// OR of multiple conditions.
    Or(Vec<FilterCondition>),
    /// Always true - used for placeholder conditions (WHERE col = ?).
    /// These mark key columns but don't filter during dataflow.
    AlwaysTrue,
}

impl FilterCondition {
    /// Evaluate the condition against a row.
    /// Note: Column indices are validated at view creation time, so direct indexing is safe.
    pub fn evaluate(&self, row: &[DataType]) -> bool {
        match self {
            FilterCondition::Eq(col, val) => row[*col] == *val,
            FilterCondition::Ne(col, val) => row[*col] != *val,
            FilterCondition::Gt(col, val) => row[*col] > *val,
            FilterCondition::Lt(col, val) => row[*col] < *val,
            FilterCondition::IsNull(col) => row[*col] == DataType::None,
            FilterCondition::IsNotNull(col) => row[*col] != DataType::None,
            FilterCondition::And(conds) => conds.iter().all(|c| c.evaluate(row)),
            FilterCondition::Or(conds) => conds.iter().any(|c| c.evaluate(row)),
            FilterCondition::AlwaysTrue => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterOp {
    condition: FilterCondition,
    num_columns: usize,
    key_cols: Vec<usize>,
}

impl FilterOp {
    pub fn new(condition: FilterCondition, num_columns: usize, key_cols: Vec<usize>) -> Self {
        Self { condition, num_columns, key_cols }
    }
}

impl Operator for FilterOp {
    fn process(
        &mut self,
        _from_parent: usize,
        records: Records,
        _state: Option<&dyn State>,
    ) -> ProcessingResult {
        let mut results = Records::new();

        for record in records {
            if self.condition.evaluate(record.row()) {
                results.push(record);
            }
        }

        ProcessingResult { results, lookups_needed: vec![] }
    }

    fn key_columns(&self) -> &[usize] {
        &self.key_cols
    }

    fn output_columns(&self) -> usize {
        self.num_columns
    }

    fn description(&self) -> String {
        "σ".to_string() // sigma for selection/filter
    }
}

// ============================================================================
// Project Operator
// ============================================================================

#[derive(Debug, Clone)]
pub struct ProjectOp {
    /// Which columns to emit (in order).
    emit: Vec<usize>,
    key_cols: Vec<usize>,
}

impl ProjectOp {
    pub fn new(emit: Vec<usize>, key_cols: Vec<usize>) -> Self {
        Self { emit, key_cols }
    }
}

impl Operator for ProjectOp {
    fn process(
        &mut self,
        _from_parent: usize,
        records: Records,
        _state: Option<&dyn State>,
    ) -> ProcessingResult {
        let mut results = Records::new();

        for record in records {
            let projected: Vec<DataType> = self.emit
                .iter()
                .map(|&col| record.row()[col].clone())
                .collect();

            let new_record = if record.is_positive() {
                Record::Positive(projected)
            } else {
                Record::Negative(projected)
            };
            results.push(new_record);
        }

        ProcessingResult { results, lookups_needed: vec![] }
    }

    fn key_columns(&self) -> &[usize] {
        &self.key_cols
    }

    fn output_columns(&self) -> usize {
        self.emit.len()
    }

    fn description(&self) -> String {
        "π".to_string() // pi for projection
    }
}

// ============================================================================
// Join Operator
// ============================================================================

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct JoinOp {
    /// Join type.
    join_type: JoinType,
    /// Column in left parent to join on.
    left_key: usize,
    /// Column in right parent to join on.
    right_key: usize,
    /// Which columns to emit: (is_left, column_index).
    emit: Vec<(bool, usize)>,
    key_cols: Vec<usize>,
    /// Number of columns in left input.
    left_cols: usize,
    /// Number of columns in right input.
    right_cols: usize,
}

impl JoinOp {
    pub fn new(
        join_type: JoinType,
        left_key: usize,
        right_key: usize,
        emit: Vec<(bool, usize)>,
        key_cols: Vec<usize>,
        left_cols: usize,
        right_cols: usize,
    ) -> Self {
        Self {
            join_type,
            left_key,
            right_key,
            emit,
            key_cols,
            left_cols,
            right_cols,
        }
    }
}

impl Operator for JoinOp {
    fn process(
        &mut self,
        from_parent: usize,
        records: Records,
        state: Option<&dyn State>,
    ) -> ProcessingResult {
        let mut results = Records::new();
        let mut lookups_needed = Vec::new();

        let state = match state {
            Some(s) => s,
            None => return ProcessingResult { results, lookups_needed },
        };

        // Determine which side the records are from (0 = left, 1 = right)
        let our_key = if from_parent == 0 {
            self.left_key
        } else {
            self.right_key
        };

        for record in records {
            let key_val = record.row()[our_key].clone();
            let key = [key_val];

            // Look up matching rows from the other side
            match state.lookup(&key) {
                LookupResult::Some(other_rows) => {
                    for other_row in other_rows {
                        // Combine rows
                        let (left, right) = if from_parent == 0 {
                            (record.row(), other_row)
                        } else {
                            (other_row, record.row())
                        };

                        let combined: Vec<DataType> = self.emit
                            .iter()
                            .map(|&(is_left, col)| {
                                if is_left { left[col].clone() } else { right[col].clone() }
                            })
                            .collect();

                        let new_record = if record.is_positive() {
                            Record::Positive(combined)
                        } else {
                            Record::Negative(combined)
                        };
                        results.push(new_record);
                    }
                }
                LookupResult::Empty | LookupResult::Missing => {
                    // For left join, emit with NULLs on the right side
                    if matches!(self.join_type, JoinType::Left) && from_parent == 0 {
                        let combined: Vec<DataType> = self.emit
                            .iter()
                            .map(|&(is_left, col)| {
                                if is_left { record.row()[col].clone() } else { DataType::None }
                            })
                            .collect();

                        let new_record = if record.is_positive() {
                            Record::Positive(combined)
                        } else {
                            Record::Negative(combined)
                        };
                        results.push(new_record);
                    }

                    if matches!(state.lookup(&key), LookupResult::Missing) {
                        lookups_needed.push(key.to_vec());
                    }
                }
            }
        }

        ProcessingResult { results, lookups_needed }
    }

    fn key_columns(&self) -> &[usize] {
        &self.key_cols
    }

    fn output_columns(&self) -> usize {
        self.emit.len()
    }

    fn description(&self) -> String {
        match self.join_type {
            JoinType::Inner => "⋈".to_string(),
            JoinType::Left => "⋉".to_string(),
            JoinType::Right => "⋊".to_string(),
        }
    }

    fn needs_state(&self) -> bool {
        true // Joins need the other parent's state for lookups
    }
}

// ============================================================================
// Aggregate Operator
// ============================================================================

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count,
    Sum(usize), // column to sum
    Avg(usize),
    Min(usize),
    Max(usize),
}

#[derive(Debug, Clone, Default)]
struct AggregateGroupState {
    count: i64,
    sum: i64,
}

#[derive(Debug, Clone)]
pub struct AggregateOp {
    group_by: Vec<usize>,
    func: AggregateFunc,
    key_cols: Vec<usize>,
    internal_state: HashMap<StateKey, AggregateGroupState>,
}

impl AggregateOp {
    pub fn new(group_by: Vec<usize>, func: AggregateFunc, key_cols: Vec<usize>) -> Self {
        Self {
            group_by,
            func,
            key_cols,
            internal_state: HashMap::new(),
        }
    }
}

impl Operator for AggregateOp {
    fn process(
        &mut self,
        _from_parent: usize,
        records: Records,
        _state: Option<&dyn State>,
    ) -> ProcessingResult {
        let mut results = Records::new();
        let mut group_changes: HashMap<StateKey, (i64, i64)> = HashMap::new();

        // Phase 1: Collect deltas per group
        {
            for record in &records {
                let group_key = StateKey::from_row(record.row(), &self.group_by);

                let delta = if record.is_positive() { 1 } else { -1 };

                let value_delta = match &self.func {
                    AggregateFunc::Sum(col) | AggregateFunc::Avg(col) => {
                        match &record.row()[*col] {
                            DataType::Int(v) => *v as i64 * delta,
                            DataType::BigInt(v) => *v * delta,
                            _ => 0,
                        }
                    }
                    _ => 0,
                };

                let entry = group_changes.entry(group_key).or_insert((0, 0));
                entry.0 += delta;
                entry.1 += value_delta;
            }
        }

        // Phase 2: Update internal state and emit records
        {
            for (group_key, (count_delta, sum_delta)) in group_changes {
                let internal = self.internal_state.entry(group_key.clone()).or_default();
                let old_count = internal.count;
                let old_sum = internal.sum;
                let new_count = old_count + count_delta;
                let new_sum = old_sum + sum_delta;

                internal.count = new_count;
                internal.sum = new_sum;

                // Extract group values from StateKey for row construction
                let group_values: Vec<DataType> = match &group_key {
                    StateKey::Single(v) => vec![v.clone()],
                    StateKey::Multi(vs) => vs.clone(),
                };

                if old_count > 0 {
                    let mut old_row = group_values.clone();
                    match &self.func {
                        AggregateFunc::Count => old_row.push(DataType::BigInt(old_count)),
                        AggregateFunc::Sum(_) => old_row.push(DataType::BigInt(old_sum)),
                        AggregateFunc::Avg(_) => old_row.push(DataType::BigInt(old_sum / old_count)),
                        AggregateFunc::Min(_) | AggregateFunc::Max(_) => old_row.push(DataType::BigInt(old_sum)),
                    }
                    results.push(Record::Negative(old_row));
                }

                if new_count > 0 {
                    let mut new_row = group_values;
                    match &self.func {
                        AggregateFunc::Count => new_row.push(DataType::BigInt(new_count)),
                        AggregateFunc::Sum(_) => new_row.push(DataType::BigInt(new_sum)),
                        AggregateFunc::Avg(_) => new_row.push(DataType::BigInt(new_sum / new_count)),
                        AggregateFunc::Min(_) | AggregateFunc::Max(_) => new_row.push(DataType::BigInt(new_sum)),
                    }
                    results.push(Record::Positive(new_row));
                }

                if new_count <= 0 {
                    self.internal_state.remove(&group_key);
                }
            }
        }

        ProcessingResult { results, lookups_needed: vec![] }
    }

    fn key_columns(&self) -> &[usize] {
        &self.key_cols
    }

    fn output_columns(&self) -> usize {
        self.group_by.len() + 1 // group columns + aggregate result
    }

    fn description(&self) -> String {
        match &self.func {
            AggregateFunc::Count => "γ[COUNT]".to_string(),
            AggregateFunc::Sum(_) => "γ[SUM]".to_string(),
            AggregateFunc::Avg(_) => "γ[AVG]".to_string(),
            AggregateFunc::Min(_) => "γ[MIN]".to_string(),
            AggregateFunc::Max(_) => "γ[MAX]".to_string(),
        }
    }
}

// ============================================================================
// Identity Operator
// ============================================================================

#[derive(Debug, Clone)]
pub struct IdentityOp {
    num_columns: usize,
    key_cols: Vec<usize>,
}

impl IdentityOp {
    pub fn new(num_columns: usize, key_cols: Vec<usize>) -> Self {
        Self { num_columns, key_cols }
    }
}

impl Operator for IdentityOp {
    fn process(
        &mut self,
        _from_parent: usize,
        records: Records,
        _state: Option<&dyn State>,
    ) -> ProcessingResult {
        ProcessingResult { results: records, lookups_needed: vec![] }
    }

    fn key_columns(&self) -> &[usize] {
        &self.key_cols
    }

    fn output_columns(&self) -> usize {
        self.num_columns
    }

    fn description(&self) -> String {
        "≡".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_op() {
        let mut filter = FilterOp::new(
            FilterCondition::Eq(0, DataType::Int(1)),
            2,
            vec![0],
        );

        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
            vec![DataType::Int(2), DataType::from("Bob")],
            vec![DataType::Int(1), DataType::from("Charlie")],
        ].into();

        let result = filter.process(0, records, None);

        assert_eq!(result.results.len(), 2);
        for record in &result.results {
            assert_eq!(record.row()[0], DataType::Int(1));
        }
    }

    #[test]
    fn test_project_op() {
        let mut project = ProjectOp::new(vec![1], vec![0]); // Only emit column 1

        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice"), DataType::Int(30)],
        ].into();

        let result = project.process(0, records, None);

        assert_eq!(result.results.len(), 1);
        for record in &result.results {
            assert_eq!(record.row().len(), 1);
            assert_eq!(record.row()[0], DataType::from("Alice"));
        }
    }

    #[test]
    fn test_aggregate_count() {
        let mut agg = AggregateOp::new(
            vec![0], // group by column 0
            AggregateFunc::Count,
            vec![0],
        );

        let records: Records = vec![
            vec![DataType::from("A")],
            vec![DataType::from("A")],
            vec![DataType::from("B")],
        ].into();

        let result = agg.process(0, records, None);

        // Should have 2 positive records: A->2, B->1
        let positives: Vec<_> = result.results.into_iter().filter(|r| r.is_positive()).collect();
        assert_eq!(positives.len(), 2);
    }

    #[test]
    fn test_identity_op() {
        let mut identity = IdentityOp::new(2, vec![0]);

        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice")],
        ].into();

        let result = identity.process(0, records.clone(), None);

        assert_eq!(result.results.len(), 1);
    }
}
