//! SQL to dataflow conversion.
//!
//! This module parses SQL queries using sqlite3-parser and converts them into
//! dataflow operator graphs. It supports:
//! - SELECT with WHERE, GROUP BY, and projections
//! - JOINs (INNER, LEFT, RIGHT, CROSS)
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)

use fallible_iterator::FallibleIterator;
use noria::DataType;
use sqlite3_parser::ast::{
    Cmd, Expr, FromClause, JoinOperator, JoinType, Literal, OneSelect, Operator, ResultColumn,
    Select, SelectTable, Stmt,
};
use sqlite3_parser::lexer::sql::Parser;

use super::executor::{LocalExecutor, NodeIndex, ViewHandle};
use super::ops::{
    AggregateFunc, AggregateOp, FilterCondition, FilterOp, JoinOp, JoinType as DataflowJoinType,
    OperatorType, ProjectOp,
};

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
        self.schemas
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(table))
    }

    /// Parse and convert a SELECT query to a dataflow graph.
    pub fn convert_select(
        &self,
        sql: &str,
        executor: &mut LocalExecutor,
    ) -> SqlResult<ViewHandle> {
        // Parse SQL using sqlite3-parser
        let mut parser = Parser::new(sql.as_bytes());
        let cmd = parser
            .next()
            .map_err(|e| SqlError::ParseError(format!("{:?}", e)))?
            .ok_or_else(|| SqlError::ParseError("Empty SQL statement".to_string()))?;

        match cmd {
            Cmd::Stmt(Stmt::Select(select)) => self.build_select(*select, executor),
            _ => Err(SqlError::UnsupportedQuery(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    /// Build dataflow for a SELECT statement.
    fn build_select(
        &self,
        select: Select,
        executor: &mut LocalExecutor,
    ) -> SqlResult<ViewHandle> {
        // Get the main body of the SELECT
        let body = &select.body;

        // We only support simple selects for now
        let one_select = match body.select {
            OneSelect::Select {
                ref columns,
                ref from,
                ref where_clause,
                ref group_by,
                ..
            } => (columns, from, where_clause, group_by),
            _ => {
                return Err(SqlError::UnsupportedQuery(
                    "Only simple SELECT statements are supported".to_string(),
                ))
            }
        };

        let (columns, from, where_clause, group_by) = one_select;

        // Get the FROM clause
        let from = from
            .as_ref()
            .ok_or_else(|| SqlError::UnsupportedQuery("SELECT requires FROM clause".to_string()))?;

        // Build from the FROM clause (handles tables and JOINs)
        let (current_node, current_columns) = self.build_from_clause(from, executor)?;
        let mut current_node = current_node;
        let mut current_columns = current_columns;

        // Get primary table name for naming operators
        let primary_table = self.get_primary_table_name(from);

        // Apply WHERE clause (Filter)
        if let Some(ref where_expr) = where_clause {
            let filter_condition = self.convert_expr_to_filter(where_expr, &current_columns)?;

            let filter_op = FilterOp::new(filter_condition, current_columns.len(), vec![0]);

            current_node = executor.add_operator(
                &format!("{}_filter", primary_table),
                OperatorType::Filter(filter_op),
                vec![current_node],
                current_columns.clone(),
            );
        }

        // Apply GROUP BY (Aggregation)
        if let Some(ref group_by_exprs) = group_by {
            // Get group by column indices
            let group_indices: Vec<usize> = group_by_exprs
                .iter()
                .filter_map(|expr| self.get_column_index_from_expr(expr, &current_columns))
                .collect();

            // Check for aggregate functions in the SELECT
            if let Some(agg_info) = self.extract_aggregate(columns, &current_columns)? {
                let agg_op =
                    AggregateOp::new(group_indices.clone(), agg_info.func, group_indices.clone());

                // Update columns to be: group_by columns + aggregate result
                let mut new_columns: Vec<String> = group_indices
                    .iter()
                    .map(|i| current_columns[*i].clone())
                    .collect();
                new_columns.push(agg_info.alias);

                current_node = executor.add_operator(
                    &format!("{}_agg", primary_table),
                    OperatorType::Aggregate(agg_op),
                    vec![current_node],
                    new_columns.clone(),
                );
                current_columns = new_columns;
            }
        }

        // Extract key columns from WHERE clause BEFORE projection
        // so we can ensure they're included in the output
        let pre_projection_key_columns = self.extract_key_columns(
            where_clause.as_ref().map(|b| b.as_ref()),
            &current_columns,
        );

        // Apply projection if columns are explicitly selected (not *)
        let needs_projection = !columns.iter().any(|c| matches!(c, ResultColumn::Star));

        let key_columns = if needs_projection && group_by.is_none() {
            if let Some((mut emit_indices, mut new_columns)) =
                self.get_projection(columns, &current_columns)?
            {
                // Ensure key columns are included in the projection
                // Track where each original key column ends up in the new projection
                let mut final_key_columns = Vec::new();
                for &key_idx in &pre_projection_key_columns {
                    // Check if this key column is already in the projection
                    if let Some(pos) = emit_indices.iter().position(|&i| i == key_idx) {
                        final_key_columns.push(pos);
                    } else {
                        // Key column not in projection - add it
                        let new_pos = emit_indices.len();
                        emit_indices.push(key_idx);
                        new_columns.push(current_columns[key_idx].clone());
                        final_key_columns.push(new_pos);
                    }
                }

                if emit_indices != (0..current_columns.len()).collect::<Vec<_>>() {
                    let project_op = ProjectOp::new(emit_indices, final_key_columns.clone());

                    current_node = executor.add_operator(
                        &format!("{}_project", primary_table),
                        OperatorType::Project(project_op),
                        vec![current_node],
                        new_columns.clone(),
                    );
                    current_columns = new_columns;
                }

                final_key_columns
            } else {
                pre_projection_key_columns
            }
        } else {
            pre_projection_key_columns
        };

        // Materialize the final node
        let view = executor.materialize(current_node, key_columns);

        Ok(view)
    }

    /// Build dataflow nodes from a FROM clause, handling JOINs.
    fn build_from_clause(
        &self,
        from: &FromClause,
        executor: &mut LocalExecutor,
    ) -> SqlResult<(NodeIndex, Vec<String>)> {
        // Start with the first table
        let select_table = from
            .select
            .as_ref()
            .ok_or_else(|| SqlError::UnsupportedQuery("FROM clause has no table".to_string()))?;
        let (mut current_node, mut current_columns) =
            self.build_select_table(select_table.as_ref(), executor)?;

        // Process any JOINs
        if let Some(ref joins) = from.joins {
            for join in joins {
                let (right_node, right_columns) =
                    self.build_select_table(&join.table, executor)?;

                // Get join key columns from ON clause
                let (left_key, right_key) = self.extract_join_keys(
                    join.constraint.as_ref(),
                    &current_columns,
                    &right_columns,
                )?;

                // Determine join type
                let join_type = match &join.operator {
                    JoinOperator::TypedJoin(Some(jt)) => {
                        if jt.contains(JoinType::LEFT) {
                            DataflowJoinType::Left
                        } else if jt.contains(JoinType::RIGHT) {
                            DataflowJoinType::Right
                        } else {
                            DataflowJoinType::Inner
                        }
                    }
                    JoinOperator::TypedJoin(None) => DataflowJoinType::Inner,
                    JoinOperator::Comma => DataflowJoinType::Inner,
                };

                // Create joined columns (left columns + right columns, excluding join key from right)
                let mut joined_columns = current_columns.clone();
                for (i, col) in right_columns.iter().enumerate() {
                    if i != right_key {
                        joined_columns.push(col.clone());
                    }
                }

                // Create emit mapping: (is_left, col_idx)
                // All left columns, then right columns except join key
                let mut emit: Vec<(bool, usize)> = (0..current_columns.len())
                    .map(|i| (true, i))
                    .collect();
                for i in 0..right_columns.len() {
                    if i != right_key {
                        emit.push((false, i));
                    }
                }

                // Create join operator
                let join_op = JoinOp::new(
                    join_type,
                    left_key,
                    right_key,
                    emit,
                    vec![0], // Key column for output
                    current_columns.len(),
                    right_columns.len(),
                );

                current_node = executor.add_operator(
                    "join",
                    OperatorType::Join(join_op),
                    vec![current_node, right_node],
                    joined_columns.clone(),
                );
                current_columns = joined_columns;
            }
        }

        Ok((current_node, current_columns))
    }

    /// Build a base table node from a SelectTable.
    fn build_select_table(
        &self,
        table: &SelectTable,
        executor: &mut LocalExecutor,
    ) -> SqlResult<(NodeIndex, Vec<String>)> {
        match table {
            SelectTable::Table(name, alias, _) => {
                let table_name = &name.name.0;
                let table_name_str = table_name.to_string();
                let schema = self
                    .get_schema(&table_name_str)
                    .ok_or_else(|| SqlError::TableNotFound(table_name_str.clone()))?;

                // Use get_or_add to reuse existing base table if it exists
                // This ensures multiple views on the same table share the same base node
                let node = executor.get_or_add_base_table(&table_name_str, schema.columns.clone());

                // Use alias if present for column prefixing
                let prefix = alias
                    .as_ref()
                    .map(|a| match a {
                        sqlite3_parser::ast::As::As(n) | sqlite3_parser::ast::As::Elided(n) => {
                            n.0.to_string()
                        }
                    })
                    .unwrap_or_else(|| table_name_str.clone());
                let _columns: Vec<String> = schema
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", prefix, c))
                    .collect();

                Ok((node, schema.columns.clone()))
            }
            SelectTable::Select(subquery, _alias) => {
                // Subquery - recursively build
                // For now, just return an error as subqueries are complex
                Err(SqlError::UnsupportedQuery(
                    "Subqueries not yet supported".to_string(),
                ))
            }
            _ => Err(SqlError::UnsupportedQuery(
                "Unsupported table type in FROM clause".to_string(),
            )),
        }
    }

    /// Get the primary table name from a FROM clause.
    fn get_primary_table_name(&self, from: &FromClause) -> String {
        match &from.select {
            Some(table) => match table.as_ref() {
                SelectTable::Table(name, _, _) => name.name.0.to_string(),
                _ => "query".to_string(),
            },
            None => "query".to_string(),
        }
    }

    /// Extract join key columns from a JOIN constraint.
    fn extract_join_keys(
        &self,
        constraint: Option<&sqlite3_parser::ast::JoinConstraint>,
        left_columns: &[String],
        right_columns: &[String],
    ) -> SqlResult<(usize, usize)> {
        use sqlite3_parser::ast::JoinConstraint;

        match constraint {
            Some(JoinConstraint::On(expr)) => {
                // Look for column = column pattern
                if let Expr::Binary(left, Operator::Equals, right) = expr {
                    let left_col = self.get_column_index_from_expr(left, left_columns);
                    let right_col = self.get_column_index_from_expr(right, right_columns);

                    if let (Some(l), Some(r)) = (left_col, right_col) {
                        return Ok((l, r));
                    }

                    // Try swapped
                    let left_col = self.get_column_index_from_expr(left, right_columns);
                    let right_col = self.get_column_index_from_expr(right, left_columns);

                    if let (Some(r), Some(l)) = (left_col, right_col) {
                        return Ok((l, r));
                    }
                }
                Err(SqlError::UnsupportedQuery(
                    "JOIN ON clause must be column = column".to_string(),
                ))
            }
            Some(JoinConstraint::Using(cols)) => {
                // USING clause - find common column
                if let Some(col_name) = cols.first() {
                    let name = &col_name.0;
                    let left_idx = left_columns
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(name));
                    let right_idx = right_columns
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(name));

                    if let (Some(l), Some(r)) = (left_idx, right_idx) {
                        return Ok((l, r));
                    }
                }
                Err(SqlError::UnsupportedQuery(
                    "USING column not found in tables".to_string(),
                ))
            }
            None => {
                // No constraint - use first columns (for CROSS JOIN)
                Ok((0, 0))
            }
        }
    }

    /// Convert an expression to a FilterCondition.
    fn convert_expr_to_filter(
        &self,
        expr: &Expr,
        columns: &[String],
    ) -> SqlResult<FilterCondition> {
        match expr {
            Expr::Binary(left, op, right) => {
                // Check if this is a comparison or logical operator
                match op {
                    Operator::And => {
                        let left_cond = self.convert_expr_to_filter(left, columns)?;
                        let right_cond = self.convert_expr_to_filter(right, columns)?;
                        Ok(FilterCondition::And(vec![left_cond, right_cond]))
                    }
                    Operator::Or => {
                        let left_cond = self.convert_expr_to_filter(left, columns)?;
                        let right_cond = self.convert_expr_to_filter(right, columns)?;
                        Ok(FilterCondition::Or(vec![left_cond, right_cond]))
                    }
                    Operator::Equals
                    | Operator::NotEquals
                    | Operator::Greater
                    | Operator::GreaterEquals
                    | Operator::Less
                    | Operator::LessEquals => {
                        self.convert_comparison(left, op, right, columns)
                    }
                    _ => Err(SqlError::UnsupportedQuery(format!(
                        "Unsupported operator in WHERE: {:?}",
                        op
                    ))),
                }
            }
            Expr::Parenthesized(inner) => {
                // Handle parenthesized expressions
                if inner.len() == 1 {
                    self.convert_expr_to_filter(&inner[0], columns)
                } else {
                    Err(SqlError::UnsupportedQuery(
                        "Multiple expressions in parentheses not supported".to_string(),
                    ))
                }
            }
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Unsupported expression in WHERE: {:?}",
                expr
            ))),
        }
    }

    /// Convert a comparison expression to a FilterCondition.
    fn convert_comparison(
        &self,
        left: &Expr,
        op: &Operator,
        right: &Expr,
        columns: &[String],
    ) -> SqlResult<FilterCondition> {
        // Get column index from left side
        let col_idx = self
            .get_column_index_from_expr(left, columns)
            .ok_or_else(|| {
                SqlError::UnsupportedQuery("Left side of comparison must be a column".to_string())
            })?;

        // Check if right side is a placeholder (?) - these mark key columns
        // and should pass through all rows (filtering happens at lookup time)
        if matches!(right, Expr::Variable(_)) {
            return Ok(FilterCondition::AlwaysTrue);
        }

        // Get value from right side
        let value = self.convert_expr_to_datatype(right)?;

        // Convert operator
        match op {
            Operator::Equals => Ok(FilterCondition::Eq(col_idx, value)),
            Operator::NotEquals => Ok(FilterCondition::Ne(col_idx, value)),
            Operator::Greater => Ok(FilterCondition::Gt(col_idx, value)),
            Operator::Less => Ok(FilterCondition::Lt(col_idx, value)),
            Operator::GreaterEquals => Ok(FilterCondition::Or(vec![
                FilterCondition::Gt(col_idx, value.clone()),
                FilterCondition::Eq(col_idx, value),
            ])),
            Operator::LessEquals => Ok(FilterCondition::Or(vec![
                FilterCondition::Lt(col_idx, value.clone()),
                FilterCondition::Eq(col_idx, value),
            ])),
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Unsupported comparison operator: {:?}",
                op
            ))),
        }
    }

    /// Get column index from an expression.
    fn get_column_index_from_expr(&self, expr: &Expr, columns: &[String]) -> Option<usize> {
        match expr {
            Expr::Id(id) => {
                let name = &id.0;
                columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(name))
            }
            Expr::Qualified(table, col) => {
                let full_name = format!("{}.{}", table.0, col.0);
                columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(&full_name))
                    .or_else(|| {
                        columns
                            .iter()
                            .position(|c| c.eq_ignore_ascii_case(&col.0))
                    })
            }
            _ => None,
        }
    }

    /// Convert an expression to a DataType value.
    fn convert_expr_to_datatype(&self, expr: &Expr) -> SqlResult<DataType> {
        match expr {
            Expr::Literal(lit) => self.convert_literal(lit),
            Expr::Unary(sqlite3_parser::ast::UnaryOperator::Negative, inner) => {
                // Handle negative numbers
                if let Expr::Literal(Literal::Numeric(n)) = inner.as_ref() {
                    let num: i64 = n.parse().unwrap_or(0);
                    Ok(DataType::BigInt(-num))
                } else {
                    Err(SqlError::UnsupportedQuery(
                        "Unary minus only supported on numbers".to_string(),
                    ))
                }
            }
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Expected literal value, got: {:?}",
                expr
            ))),
        }
    }

    /// Convert a SQL literal to a DataType.
    fn convert_literal(&self, lit: &Literal) -> SqlResult<DataType> {
        match lit {
            Literal::Numeric(n) => {
                // Try to parse as integer first, then float
                if let Ok(i) = n.parse::<i64>() {
                    Ok(DataType::BigInt(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    let int_part = f.trunc() as i64;
                    let frac_part = ((f.fract().abs()) * 1_000_000_000.0) as i32;
                    Ok(DataType::Real(int_part, frac_part))
                } else {
                    Err(SqlError::UnsupportedQuery(format!(
                        "Cannot parse numeric literal: {}",
                        n
                    )))
                }
            }
            Literal::String(s) => Ok(DataType::from(&**s)),
            Literal::Null => Ok(DataType::None),
            Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp => {
                Ok(DataType::from(chrono::Utc::now().to_string().as_str()))
            }
            Literal::Blob(_) => Err(SqlError::UnsupportedQuery(
                "BLOB literals not supported".to_string(),
            )),
            Literal::Keyword(kw) => {
                // Keywords like TRUE, FALSE
                let kw_upper = kw.to_uppercase();
                match kw_upper.as_str() {
                    "TRUE" => Ok(DataType::Int(1)),
                    "FALSE" => Ok(DataType::Int(0)),
                    _ => Err(SqlError::UnsupportedQuery(format!(
                        "Unsupported keyword literal: {}",
                        kw
                    ))),
                }
            }
        }
    }

    /// Extract aggregate function info from SELECT columns.
    fn extract_aggregate(
        &self,
        columns: &[ResultColumn],
        current_columns: &[String],
    ) -> SqlResult<Option<AggInfo>> {
        for col in columns {
            if let ResultColumn::Expr(expr, alias) = col {
                if let Some(agg) = self.extract_aggregate_from_expr(expr, current_columns)? {
                    let alias_name = alias
                        .as_ref()
                        .map(|a| match a {
                            sqlite3_parser::ast::As::As(n)
                            | sqlite3_parser::ast::As::Elided(n) => n.0.to_string(),
                        })
                        .unwrap_or_else(|| "agg".to_string());
                    return Ok(Some(AggInfo {
                        func: agg,
                        alias: alias_name,
                    }));
                }
            }
        }
        Ok(None)
    }

    /// Extract aggregate function from an expression.
    fn extract_aggregate_from_expr(
        &self,
        expr: &Expr,
        columns: &[String],
    ) -> SqlResult<Option<AggregateFunc>> {
        match expr {
            Expr::FunctionCall {
                name, args, ..
            } => {
                let func_name = name.0.to_uppercase();
                match func_name.as_str() {
                    "COUNT" => Ok(Some(AggregateFunc::Count)),
                    "SUM" => {
                        if let Some(args) = args {
                            if let Some(first_arg) = args.first() {
                                if let Some(col_idx) =
                                    self.get_column_index_from_expr(first_arg, columns)
                                {
                                    return Ok(Some(AggregateFunc::Sum(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Sum(0)))
                    }
                    "AVG" => {
                        if let Some(args) = args {
                            if let Some(first_arg) = args.first() {
                                if let Some(col_idx) =
                                    self.get_column_index_from_expr(first_arg, columns)
                                {
                                    return Ok(Some(AggregateFunc::Avg(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Avg(0)))
                    }
                    "MIN" => {
                        if let Some(args) = args {
                            if let Some(first_arg) = args.first() {
                                if let Some(col_idx) =
                                    self.get_column_index_from_expr(first_arg, columns)
                                {
                                    return Ok(Some(AggregateFunc::Min(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Min(0)))
                    }
                    "MAX" => {
                        if let Some(args) = args {
                            if let Some(first_arg) = args.first() {
                                if let Some(col_idx) =
                                    self.get_column_index_from_expr(first_arg, columns)
                                {
                                    return Ok(Some(AggregateFunc::Max(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Max(0)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::FunctionCallStar { name, .. } => {
                let func_name = name.0.to_uppercase();
                if func_name == "COUNT" {
                    Ok(Some(AggregateFunc::Count))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    /// Get projection column indices and names.
    fn get_projection(
        &self,
        columns: &[ResultColumn],
        current_columns: &[String],
    ) -> SqlResult<Option<(Vec<usize>, Vec<String>)>> {
        let mut indices = Vec::new();
        let mut names = Vec::new();

        for col in columns {
            match col {
                ResultColumn::Star => {
                    // SELECT * - include all columns
                    return Ok(None);
                }
                ResultColumn::TableStar(_) => {
                    // SELECT table.* - include all columns (simplified)
                    return Ok(None);
                }
                ResultColumn::Expr(expr, alias) => {
                    if let Some(idx) = self.get_column_index_from_expr(expr, current_columns) {
                        indices.push(idx);
                        let name = alias
                            .as_ref()
                            .map(|a| match a {
                                sqlite3_parser::ast::As::As(n)
                                | sqlite3_parser::ast::As::Elided(n) => n.0.to_string(),
                            })
                            .unwrap_or_else(|| current_columns[idx].clone());
                        names.push(name);
                    } else {
                        // Could be an expression - skip projection for now
                        return Ok(None);
                    }
                }
            }
        }

        if indices.is_empty() {
            Ok(None)
        } else {
            Ok(Some((indices, names)))
        }
    }

    /// Extract key columns from WHERE clause for materialization.
    fn extract_key_columns(&self, where_clause: Option<&Expr>, columns: &[String]) -> Vec<usize> {
        if let Some(expr) = where_clause {
            if let Some(col_idx) = self.extract_equality_column(expr, columns) {
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
    fn extract_equality_column(&self, expr: &Expr, columns: &[String]) -> Option<usize> {
        match expr {
            Expr::Binary(left, Operator::Equals, _right) => {
                self.get_column_index_from_expr(left, columns)
            }
            Expr::Binary(left, Operator::And, right) => {
                // For AND, try left side first
                self.extract_equality_column(left, columns)
                    .or_else(|| self.extract_equality_column(right, columns))
            }
            Expr::Parenthesized(inner) if inner.len() == 1 => {
                self.extract_equality_column(&inner[0], columns)
            }
            _ => None,
        }
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
        assert!(stats.node_count >= 2); // base + filter (+ maybe project)
    }

    #[test]
    fn test_select_with_filter() {
        let mut converter = SqlConverter::new();
        converter.register_table("users", vec!["id".into(), "name".into(), "active".into()]);

        let mut executor = LocalExecutor::new();
        let view = converter
            .convert_select("SELECT * FROM users WHERE active = 1", &mut executor)
            .unwrap();

        // Insert data and check filter
        use super::super::Records;

        let records: Records = vec![
            vec![DataType::Int(1), DataType::from("Alice"), DataType::BigInt(1)],
            vec![DataType::Int(2), DataType::from("Bob"), DataType::BigInt(0)],
        ]
        .into();

        executor.apply_write("users", records);

        // Only active users should be in the view
        let result = executor.lookup(&view, &[DataType::Int(1)]);
        assert!(result.is_some());

        let result2 = executor.lookup(&view, &[DataType::Int(2)]);
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
    fn test_join() {
        let mut converter = SqlConverter::new();
        converter.register_table("users", vec!["id".into(), "name".into()]);
        converter.register_table("posts", vec!["id".into(), "user_id".into(), "title".into()]);

        let mut executor = LocalExecutor::new();
        let result = converter.convert_select(
            "SELECT * FROM users JOIN posts ON users.id = posts.user_id",
            &mut executor,
        );

        // JOIN should be supported now
        assert!(result.is_ok());

        let stats = executor.stats();
        // base_users + base_posts + join
        assert!(stats.node_count >= 3);
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
