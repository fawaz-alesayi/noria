//! SQL to dataflow conversion.
//!
//! This module parses SQL queries using nom-sql and converts them into
//! dataflow operator graphs.

use nom_sql::{
    parse_query, ConditionBase, ConditionExpression, ConditionTree, FieldDefinitionExpression,
    FunctionArguments, FunctionExpression, Literal, Operator, SelectStatement, SqlQuery,
};
use noria::DataType;

use super::executor::{LocalExecutor, ViewHandle};
use super::ops::{AggregateFunc, AggregateOp, FilterCondition, FilterOp, OperatorType, ProjectOp};

/// Error type for SQL conversion.
#[derive(Debug, Clone)]
pub enum SqlError {
    ParseError(String),
    UnsupportedQuery(String),
    TableNotFound(String),
    ColumnNotFound(String),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::ParseError(s) => write!(f, "Parse error: {}", s),
            SqlError::UnsupportedQuery(s) => write!(f, "Unsupported query: {}", s),
            SqlError::TableNotFound(s) => write!(f, "Table not found: {}", s),
            SqlError::ColumnNotFound(s) => write!(f, "Column not found: {}", s),
        }
    }
}

impl std::error::Error for SqlError {}

/// Result type for SQL conversion.
pub type SqlResult<T> = Result<T, SqlError>;

/// Schema information for a table.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<String>,
}

/// Converts SQL queries to dataflow graphs.
pub struct SqlConverter {
    /// Known table schemas.
    schemas: Vec<TableSchema>,
}

impl SqlConverter {
    /// Create a new SQL converter.
    pub fn new() -> Self {
        Self {
            schemas: Vec::new(),
        }
    }

    /// Register a table schema.
    pub fn register_table(&mut self, name: &str, columns: Vec<String>) {
        self.schemas.push(TableSchema {
            name: name.to_string(),
            columns,
        });
    }

    /// Get schema for a table.
    fn get_schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.iter().find(|s| s.name == table)
    }

    /// Get column index in a table.
    fn get_column_index(&self, table: &str, column: &str) -> Option<usize> {
        self.get_schema(table)
            .and_then(|s| s.columns.iter().position(|c| c == column))
    }

    /// Parse and convert a SELECT query to a dataflow graph.
    pub fn convert_select(
        &self,
        sql: &str,
        executor: &mut LocalExecutor,
    ) -> SqlResult<ViewHandle> {
        // Parse SQL
        let query = parse_query(sql).map_err(|e| SqlError::ParseError(format!("{:?}", e)))?;

        match query {
            SqlQuery::Select(select) => self.build_select(select, executor),
            _ => Err(SqlError::UnsupportedQuery(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    /// Build dataflow for a SELECT statement.
    fn build_select(
        &self,
        select: SelectStatement,
        executor: &mut LocalExecutor,
    ) -> SqlResult<ViewHandle> {
        // Get the table(s)
        let tables = &select.tables;
        if tables.is_empty() {
            return Err(SqlError::UnsupportedQuery("No tables in query".to_string()));
        }

        // For now, only support single-table queries
        if tables.len() > 1 {
            return Err(SqlError::UnsupportedQuery(
                "JOINs not yet supported in SQL conversion".to_string(),
            ));
        }

        let table_name = &tables[0].name;
        let schema = self
            .get_schema(table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

        // Find or create base table node
        let base_node = executor.add_base_table(table_name, schema.columns.clone());

        // Track the current node and columns as we build the pipeline
        let mut current_node = base_node;
        let mut current_columns = schema.columns.clone();

        // Apply WHERE clause (Filter)
        if let Some(ref where_clause) = select.where_clause {
            let filter_condition =
                self.convert_condition(where_clause, table_name, &current_columns)?;

            let filter_op = FilterOp::new(filter_condition, current_columns.len(), vec![0]);

            current_node = executor.add_operator(
                &format!("{}_filter", table_name),
                OperatorType::Filter(filter_op),
                vec![current_node],
                current_columns.clone(),
            );
        }

        // Apply GROUP BY (Aggregation)
        if !select.group_by.is_none() {
            let group_by_cols = select.group_by.as_ref().unwrap();

            // Get group by column indices
            let group_indices: Vec<usize> = group_by_cols
                .columns
                .iter()
                .filter_map(|col| {
                    current_columns.iter().position(|c| c == &col.name)
                })
                .collect();

            // Check for aggregate functions in the SELECT
            if let Some(agg_info) = self.extract_aggregate(&select.fields, &current_columns)? {
                let agg_op = AggregateOp::new(group_indices.clone(), agg_info.func, group_indices.clone());

                // Update columns to be: group_by columns + aggregate result
                let mut new_columns: Vec<String> = group_indices
                    .iter()
                    .map(|&i| current_columns[i].clone())
                    .collect();
                new_columns.push(agg_info.alias);

                current_node = executor.add_operator(
                    &format!("{}_agg", table_name),
                    OperatorType::Aggregate(agg_op),
                    vec![current_node],
                    new_columns.clone(),
                );
                current_columns = new_columns;
            }
        }

        // Apply projection if columns are explicitly selected (not *)
        let needs_projection = !select.fields.iter().any(|f| {
            matches!(
                f,
                FieldDefinitionExpression::All | FieldDefinitionExpression::AllInTable(_)
            )
        });

        if needs_projection && select.group_by.is_none() {
            let (emit_indices, new_columns) =
                self.get_projection(&select.fields, &current_columns)?;

            if emit_indices != (0..current_columns.len()).collect::<Vec<_>>() {
                let project_op = ProjectOp::new(emit_indices, vec![0]);

                current_node = executor.add_operator(
                    &format!("{}_project", table_name),
                    OperatorType::Project(project_op),
                    vec![current_node],
                    new_columns.clone(),
                );
                current_columns = new_columns;
            }
        }

        // Materialize the final node
        // For now, use first column as key (TODO: use WHERE clause key)
        let key_columns = self.extract_key_columns(&select, &current_columns);
        let view = executor.materialize(current_node, key_columns);

        Ok(view)
    }

    /// Convert a condition expression to a FilterCondition.
    fn convert_condition(
        &self,
        expr: &ConditionExpression,
        table: &str,
        columns: &[String],
    ) -> SqlResult<FilterCondition> {
        match expr {
            ConditionExpression::ComparisonOp(tree) => {
                self.convert_comparison(tree, table, columns)
            }
            ConditionExpression::LogicalOp(tree) => {
                let left = self.convert_condition(&*tree.left, table, columns)?;
                let right = self.convert_condition(&*tree.right, table, columns)?;

                match tree.operator {
                    Operator::And => Ok(FilterCondition::And(vec![left, right])),
                    Operator::Or => Ok(FilterCondition::Or(vec![left, right])),
                    _ => Err(SqlError::UnsupportedQuery(format!(
                        "Unsupported logical operator: {:?}",
                        tree.operator
                    ))),
                }
            }
            ConditionExpression::Bracketed(inner) => {
                self.convert_condition(inner, table, columns)
            }
            ConditionExpression::Base(base) => {
                match base {
                    ConditionBase::Field(_) => {
                        // A bare field reference (e.g., in a boolean context)
                        Err(SqlError::UnsupportedQuery(
                            "Bare field conditions not supported".to_string(),
                        ))
                    }
                    ConditionBase::Literal(_) => {
                        // A literal value as a condition
                        Err(SqlError::UnsupportedQuery(
                            "Literal conditions not supported".to_string(),
                        ))
                    }
                    _ => Err(SqlError::UnsupportedQuery(
                        "Unsupported condition base".to_string(),
                    )),
                }
            }
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Unsupported condition expression: {:?}",
                expr
            ))),
        }
    }

    /// Convert a comparison tree to a FilterCondition.
    fn convert_comparison(
        &self,
        tree: &ConditionTree,
        _table: &str,
        columns: &[String],
    ) -> SqlResult<FilterCondition> {
        // Get the column from the left side (dereference Box)
        let col_idx = match &*tree.left {
            ConditionExpression::Base(ConditionBase::Field(col)) => {
                columns
                    .iter()
                    .position(|c| c == &col.name)
                    .ok_or_else(|| SqlError::ColumnNotFound(col.name.clone()))?
            }
            _ => {
                return Err(SqlError::UnsupportedQuery(
                    "Left side of comparison must be a column".to_string(),
                ))
            }
        };

        // Get the value from the right side (dereference Box)
        let value = match &*tree.right {
            ConditionExpression::Base(ConditionBase::Literal(lit)) => {
                self.convert_literal(lit)?
            }
            ConditionExpression::Base(ConditionBase::LiteralList(lits)) => {
                // For IN clauses, use the first literal for now
                if lits.is_empty() {
                    return Err(SqlError::UnsupportedQuery("Empty IN clause".to_string()));
                }
                self.convert_literal(&lits[0])?
            }
            ConditionExpression::Base(ConditionBase::NestedSelect(_)) => {
                return Err(SqlError::UnsupportedQuery(
                    "Subqueries not supported".to_string(),
                ))
            }
            _ => {
                return Err(SqlError::UnsupportedQuery(
                    "Right side of comparison must be a literal".to_string(),
                ))
            }
        };

        // Convert the operator
        match tree.operator {
            Operator::Equal => Ok(FilterCondition::Eq(col_idx, value)),
            Operator::NotEqual => Ok(FilterCondition::Ne(col_idx, value)),
            Operator::Greater => Ok(FilterCondition::Gt(col_idx, value)),
            Operator::Less => Ok(FilterCondition::Lt(col_idx, value)),
            Operator::GreaterOrEqual => {
                // >= is NOT (< )
                Ok(FilterCondition::Or(vec![
                    FilterCondition::Gt(col_idx, value.clone()),
                    FilterCondition::Eq(col_idx, value),
                ]))
            }
            Operator::LessOrEqual => {
                // <= is NOT (>)
                Ok(FilterCondition::Or(vec![
                    FilterCondition::Lt(col_idx, value.clone()),
                    FilterCondition::Eq(col_idx, value),
                ]))
            }
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Unsupported comparison operator: {:?}",
                tree.operator
            ))),
        }
    }

    /// Convert a SQL literal to a DataType.
    fn convert_literal(&self, lit: &Literal) -> SqlResult<DataType> {
        match lit {
            Literal::Integer(i) => Ok(DataType::BigInt(*i)),
            Literal::String(s) => Ok(DataType::from(s.as_str())),
            Literal::Null => Ok(DataType::None),
            Literal::CurrentTimestamp | Literal::CurrentDate | Literal::CurrentTime => {
                // Use current time
                Ok(DataType::from(chrono::Utc::now().to_string().as_str()))
            }
            Literal::Placeholder => {
                // Placeholder - return a marker
                Ok(DataType::None) // Will be replaced at query time
            }
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Unsupported literal: {:?}",
                lit
            ))),
        }
    }

    /// Extract aggregate function info from SELECT fields.
    fn extract_aggregate(
        &self,
        fields: &[FieldDefinitionExpression],
        columns: &[String],
    ) -> SqlResult<Option<AggInfo>> {
        for field in fields {
            if let FieldDefinitionExpression::Col(col) = field {
                if let Some(ref func) = col.function {
                    let agg_func = match func.as_ref() {
                        FunctionExpression::CountStar => AggregateFunc::Count,
                        FunctionExpression::Count(_, _) => AggregateFunc::Count,
                        FunctionExpression::Sum(func_args, _) => {
                            let col_idx = self.get_col_index_from_func_args(func_args, columns)?;
                            AggregateFunc::Sum(col_idx)
                        }
                        FunctionExpression::Avg(func_args, _) => {
                            let col_idx = self.get_col_index_from_func_args(func_args, columns)?;
                            AggregateFunc::Avg(col_idx)
                        }
                        FunctionExpression::Min(func_args) => {
                            let col_idx = self.get_col_index_from_func_args(func_args, columns)?;
                            AggregateFunc::Min(col_idx)
                        }
                        FunctionExpression::Max(func_args) => {
                            let col_idx = self.get_col_index_from_func_args(func_args, columns)?;
                            AggregateFunc::Max(col_idx)
                        }
                        FunctionExpression::GroupConcat(_, _) => continue,
                    };

                    let alias = col.alias.clone().unwrap_or_else(|| "agg".to_string());
                    return Ok(Some(AggInfo {
                        func: agg_func,
                        alias,
                    }));
                }
            }
        }
        Ok(None)
    }

    /// Get column index from FunctionArguments.
    fn get_col_index_from_func_args(
        &self,
        func_args: &FunctionArguments,
        columns: &[String],
    ) -> SqlResult<usize> {
        match func_args {
            FunctionArguments::Column(col) => columns
                .iter()
                .position(|c| c == &col.name)
                .ok_or_else(|| SqlError::ColumnNotFound(col.name.clone())),
            FunctionArguments::Conditional(_) => Err(SqlError::UnsupportedQuery(
                "CASE expressions in aggregates not yet supported".to_string(),
            )),
        }
    }

    /// Get projection column indices and names.
    fn get_projection(
        &self,
        fields: &[FieldDefinitionExpression],
        columns: &[String],
    ) -> SqlResult<(Vec<usize>, Vec<String>)> {
        let mut indices = Vec::new();
        let mut names = Vec::new();

        for field in fields {
            match field {
                FieldDefinitionExpression::All | FieldDefinitionExpression::AllInTable(_) => {
                    // SELECT * - include all columns
                    for (i, col) in columns.iter().enumerate() {
                        indices.push(i);
                        names.push(col.clone());
                    }
                }
                FieldDefinitionExpression::Col(col) => {
                    let col_name = &col.name;
                    let idx = columns
                        .iter()
                        .position(|c| c == col_name)
                        .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                    indices.push(idx);
                    names.push(col.alias.clone().unwrap_or_else(|| col_name.clone()));
                }
                FieldDefinitionExpression::Value(_) => {
                    // Literal value in SELECT - not yet supported
                    return Err(SqlError::UnsupportedQuery(
                        "Literal values in SELECT not supported".to_string(),
                    ));
                }
            }
        }

        Ok((indices, names))
    }

    /// Extract key columns from WHERE clause for materialization.
    fn extract_key_columns(&self, select: &SelectStatement, columns: &[String]) -> Vec<usize> {
        // Try to extract equality conditions from WHERE clause
        if let Some(ref where_clause) = select.where_clause {
            if let Some(col_idx) = self.extract_equality_column(where_clause, columns) {
                return vec![col_idx];
            }
        }

        // Default to first column
        if !columns.is_empty() {
            vec![0]
        } else {
            vec![]
        }
    }

    /// Extract column index from equality condition.
    fn extract_equality_column(
        &self,
        expr: &ConditionExpression,
        columns: &[String],
    ) -> Option<usize> {
        match expr {
            ConditionExpression::ComparisonOp(tree) if tree.operator == Operator::Equal => {
                if let ConditionExpression::Base(ConditionBase::Field(col)) = &*tree.left {
                    return columns.iter().position(|c| c == &col.name);
                }
            }
            ConditionExpression::LogicalOp(tree) if tree.operator == Operator::And => {
                // For AND, try the left side first
                if let Some(idx) = self.extract_equality_column(&*tree.left, columns) {
                    return Some(idx);
                }
                return self.extract_equality_column(&*tree.right, columns);
            }
            ConditionExpression::Bracketed(inner) => {
                return self.extract_equality_column(inner, columns);
            }
            _ => {}
        }
        None
    }
}

impl Default for SqlConverter {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate function info.
struct AggInfo {
    func: AggregateFunc,
    alias: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut converter = SqlConverter::new();
        converter.register_table("users", vec!["id".into(), "name".into(), "age".into()]);

        let mut executor = LocalExecutor::new();
        let view = converter
            .convert_select("SELECT * FROM users WHERE id = 1", &mut executor)
            .unwrap();

        // The view should be materialized
        assert_eq!(view.key_columns(), &[0]); // id column
    }

    #[test]
    fn test_select_with_projection() {
        let mut converter = SqlConverter::new();
        converter.register_table("users", vec!["id".into(), "name".into(), "age".into()]);

        let mut executor = LocalExecutor::new();
        let view = converter
            .convert_select("SELECT name, age FROM users WHERE id = 1", &mut executor)
            .unwrap();

        // Check that stats show the nodes
        let stats = executor.stats();
        assert!(stats.node_count >= 2); // base + project (+ maybe filter)
    }

    #[test]
    fn test_select_with_filter() {
        let mut converter = SqlConverter::new();
        converter.register_table("users", vec!["id".into(), "name".into(), "active".into()]);

        let mut executor = LocalExecutor::new();

        // First, register the table as base
        let base = executor.add_base_table("users", vec!["id".into(), "name".into(), "active".into()]);

        // Now convert a query
        let mut converter2 = SqlConverter::new();
        converter2.register_table("users", vec!["id".into(), "name".into(), "active".into()]);

        let mut executor2 = LocalExecutor::new();
        let view = converter2
            .convert_select("SELECT * FROM users WHERE active = 1", &mut executor2)
            .unwrap();

        // Insert data and check filter
        use super::super::Records;

        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice"), DataType::BigInt(1)],
            vec![DataType::Int(2), DataType::from("Bob"), DataType::BigInt(0)],
        ]
        .into();

        executor2.apply_write("users", records);

        // Only active users should be in the view
        let result = executor2.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());

        let result2 = executor2.lookup(&view, &[DataType::Int(2)]);
        // Bob is not active, so should be missing or empty
        assert!(result2.is_none() || result2.unwrap().is_empty());
    }

    #[test]
    fn test_aggregate_count() {
        let mut converter = SqlConverter::new();
        converter.register_table("votes", vec!["article_id".into(), "user_id".into()]);

        let mut executor = LocalExecutor::new();
        let view = converter
            .convert_select(
                "SELECT article_id, COUNT(*) FROM votes GROUP BY article_id",
                &mut executor,
            )
            .unwrap();

        // Check structure
        let stats = executor.stats();
        assert!(stats.node_count >= 2); // base + aggregate

        // Insert votes and check count
        use super::super::Records;

        let records: Records = vec![
            vec![DataType::Int(1), DataType::Int(100)],
            vec![DataType::Int(1), DataType::Int(101)],
            vec![DataType::Int(2), DataType::Int(102)],
        ]
        .into();

        executor.apply_write("votes", records);

        // Article 1 should have 2 votes
        let result = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());
        let rows = result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], DataType::BigInt(2));
    }

    #[test]
    fn test_unknown_table() {
        let converter = SqlConverter::new();
        let mut executor = LocalExecutor::new();

        let result = converter.convert_select("SELECT * FROM nonexistent", &mut executor);
        assert!(matches!(result, Err(SqlError::TableNotFound(_))));
    }

    #[test]
    fn test_parse_error() {
        let converter = SqlConverter::new();
        let mut executor = LocalExecutor::new();

        let result = converter.convert_select("NOT VALID SQL", &mut executor);
        assert!(matches!(result, Err(SqlError::ParseError(_))));
    }
}
