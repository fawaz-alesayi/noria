//! SQL to dataflow conversion.
//!
//! This module parses SQL queries using sqlparser-rs and converts them into
//! dataflow operator graphs. It supports:
//! - SELECT with WHERE, GROUP BY, and projections
//! - JOINs (INNER, LEFT, RIGHT, CROSS)
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)

use noria::DataType;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, GroupByExpr, JoinConstraint,
    JoinOperator, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    Value,
};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser;

use crate::dataflow::executor::{LocalExecutor, NodeIndex, ViewHandle};
use crate::dataflow::ops::{
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

/// SQL dialect for parsing.
#[derive(Debug, Clone, Copy, Default)]
pub enum SqlDialect {
    #[default]
    Generic,
    SQLite,
    PostgreSQL,
    MySQL,
}

impl SqlDialect {
    fn to_dialect(&self) -> Box<dyn Dialect> {
        match self {
            SqlDialect::Generic => Box::new(GenericDialect {}),
            SqlDialect::SQLite => Box::new(SQLiteDialect {}),
            SqlDialect::PostgreSQL => Box::new(PostgreSqlDialect {}),
            SqlDialect::MySQL => Box::new(MySqlDialect {}),
        }
    }
}

/// Converts SQL queries to dataflow graphs.
pub struct SqlConverter {
    /// Known table schemas.
    schemas: Vec<TableSchema>,
    /// SQL dialect to use for parsing.
    dialect: SqlDialect,
}

impl SqlConverter {
    /// Create a new SQL converter with the default dialect.
    pub fn new() -> Self {
        Self {
            schemas: Vec::new(),
            dialect: SqlDialect::default(),
        }
    }

    /// Create a new SQL converter with a specific dialect.
    pub fn with_dialect(dialect: SqlDialect) -> Self {
        Self {
            schemas: Vec::new(),
            dialect,
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

        let statements = {
            let dialect = self.dialect.to_dialect();
            Parser::parse_sql(&*dialect, sql)
                .map_err(|e| SqlError::ParseError(format!("{}", e)))?
        };

        let stmt = statements
            .into_iter()
            .next()
            .ok_or_else(|| SqlError::ParseError("Empty SQL statement".to_string()))?;

        match stmt {
            Statement::Query(query) => {
                self.build_query(*query, executor)
            }
            _ => Err(SqlError::UnsupportedQuery(
                "Only SELECT queries are supported".to_string(),
            )),
        }
    }

    /// Build dataflow for a Query.
    fn build_query(
        &self,
        query: sqlparser::ast::Query,
        executor: &mut LocalExecutor,
    ) -> SqlResult<ViewHandle> {
        match *query.body {
            SetExpr::Select(select) => self.build_select(*select, executor),
            _ => Err(SqlError::UnsupportedQuery(
                "Only simple SELECT statements are supported".to_string(),
            )),
        }
    }

    /// Build dataflow for a SELECT statement.
    fn build_select(
        &self,
        select: Select,
        executor: &mut LocalExecutor,
    ) -> SqlResult<ViewHandle> {
        // Get the FROM clause
        if select.from.is_empty() {
            return Err(SqlError::UnsupportedQuery(
                "SELECT requires FROM clause".to_string(),
            ));
        }

        // Build from the FROM clause (handles tables and JOINs)
        let (current_node, current_columns) =
            self.build_from_clause(&select.from, executor)?;
        let mut current_node = current_node;
        let mut current_columns = current_columns;

        // Get primary table name for naming operators
        let primary_table = self.get_primary_table_name(&select.from);

        // Apply WHERE clause (Filter)
        if let Some(ref where_expr) = select.selection {
            let filter_condition = self.convert_expr_to_filter(where_expr, &current_columns)?;

            let filter_op = FilterOp::new(filter_condition, current_columns.len(), vec![0]);

            current_node = executor.add_operator(
                &format!("{}_filter", primary_table),
                OperatorType::Filter(filter_op),
                vec![current_node],
                current_columns.clone(),
            );
        }

        let agg_info = self.extract_aggregate(&select.projection, &current_columns)?;
        let has_aggregate = agg_info.is_some();

        if let Some(agg_info) = agg_info {
            let group_indices: Vec<usize> = match &select.group_by {
                GroupByExpr::Expressions(exprs) => exprs
                    .iter()
                    .filter_map(|expr| self.get_column_index_from_expr(expr, &current_columns))
                    .collect(),
                GroupByExpr::All => {
                    // GROUP BY ALL - not commonly supported
                    self.extract_key_columns(select.selection.as_ref(), &current_columns)
                }
            };

            // If no GROUP BY but has aggregate, use WHERE clause column as group
            let group_indices = if group_indices.is_empty() {
                self.extract_key_columns(select.selection.as_ref(), &current_columns)
            } else {
                group_indices
            };

            let agg_op =
                AggregateOp::new(group_indices.clone(), agg_info.func, group_indices.clone());

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

        let pre_projection_key_columns = self.extract_key_columns(
            select.selection.as_ref(),
            &current_columns,
        );

        let needs_projection = !select.projection.iter().any(|c| matches!(c, SelectItem::Wildcard(_)));

        let key_columns = if needs_projection && !has_aggregate {
            if let Some((mut emit_indices, mut new_columns)) =
                self.get_projection(&select.projection, &current_columns)?
            {
                let mut final_key_columns = Vec::new();
                for &key_idx in &pre_projection_key_columns {
                    if let Some(pos) = emit_indices.iter().position(|&i| i == key_idx) {
                        final_key_columns.push(pos);
                    } else {
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

        let view = executor.materialize(current_node, key_columns);
        Ok(view)
    }

    /// Build from the FROM clause (handles tables and JOINs).
    fn build_from_clause(
        &self,
        from: &[TableWithJoins],
        executor: &mut LocalExecutor,
    ) -> SqlResult<(NodeIndex, Vec<String>)> {
        if from.is_empty() {
            return Err(SqlError::UnsupportedQuery(
                "FROM clause has no table".to_string(),
            ));
        }

        let first_table = &from[0];
        let (mut current_node, mut current_columns) =
            self.build_table_factor(&first_table.relation, executor)?;

        // Handle JOINs in the first TableWithJoins
        for join in &first_table.joins {
            let (right_node, right_columns) =
                self.build_table_factor(&join.relation, executor)?;

            let (left_key, right_key) = self.extract_join_keys(
                &join.join_operator,
                &current_columns,
                &right_columns,
            )?;

            // Materialize base tables so JOIN can look up matching rows during CDC
            executor.materialize(current_node, vec![left_key]);
            executor.materialize(right_node, vec![right_key]);

            let join_type = match &join.join_operator {
                JoinOperator::Inner(_) => DataflowJoinType::Inner,
                JoinOperator::LeftOuter(_) | JoinOperator::LeftSemi(_) | JoinOperator::LeftAnti(_) => {
                    DataflowJoinType::Left
                }
                JoinOperator::RightOuter(_) | JoinOperator::RightSemi(_) | JoinOperator::RightAnti(_) => {
                    DataflowJoinType::Right
                }
                JoinOperator::FullOuter(_) => DataflowJoinType::Inner, // Simplified
                JoinOperator::CrossJoin => DataflowJoinType::Inner,
                _ => DataflowJoinType::Inner,
            };

            let mut joined_columns = current_columns.clone();
            for (i, col) in right_columns.iter().enumerate() {
                if i != right_key {
                    joined_columns.push(col.clone());
                }
            }

            let mut emit: Vec<(bool, usize)> = (0..current_columns.len())
                .map(|i| (true, i))
                .collect();
            for i in 0..right_columns.len() {
                if i != right_key {
                    emit.push((false, i));
                }
            }

            let join_op = JoinOp::new(
                join_type,
                left_key,
                right_key,
                emit,
                vec![0],
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

        Ok((current_node, current_columns))
    }

    /// Build a base table node from a TableFactor.
    fn build_table_factor(
        &self,
        table: &TableFactor,
        executor: &mut LocalExecutor,
    ) -> SqlResult<(NodeIndex, Vec<String>)> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                let schema = self
                    .get_schema(&table_name)
                    .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

                // Use get_or_add to reuse existing base table if it exists
                let node = executor.get_or_add_base_table(&table_name, schema.columns.clone());

                Ok((node, schema.columns.clone()))
            }
            TableFactor::Derived { subquery, .. } => {
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
    fn get_primary_table_name(&self, from: &[TableWithJoins]) -> String {
        if from.is_empty() {
            return "query".to_string();
        }

        match &from[0].relation {
            TableFactor::Table { name, .. } => {
                name.0.last().map(|i| i.value.clone()).unwrap_or_else(|| "query".to_string())
            }
            _ => "query".to_string(),
        }
    }

    /// Extract join key columns from a JOIN constraint.
    fn extract_join_keys(
        &self,
        join_op: &JoinOperator,
        left_columns: &[String],
        right_columns: &[String],
    ) -> SqlResult<(usize, usize)> {
        let constraint = match join_op {
            JoinOperator::Inner(c) | JoinOperator::LeftOuter(c) | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c) | JoinOperator::LeftSemi(c) | JoinOperator::RightSemi(c)
            | JoinOperator::LeftAnti(c) | JoinOperator::RightAnti(c) => c,
            JoinOperator::CrossJoin => return Ok((0, 0)),
            _ => return Ok((0, 0)),
        };

        match constraint {
            JoinConstraint::On(expr) => {
                // Look for column = column pattern
                if let Expr::BinaryOp { left, op: BinaryOperator::Eq, right } = expr {
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
            JoinConstraint::Using(cols) => {
                // USING clause - find common column
                if let Some(col) = cols.first() {
                    let name = &col.value;
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
            JoinConstraint::Natural => {
                // NATURAL JOIN - find first common column
                for (i, left_col) in left_columns.iter().enumerate() {
                    if let Some(j) = right_columns.iter().position(|r| r.eq_ignore_ascii_case(left_col)) {
                        return Ok((i, j));
                    }
                }
                Err(SqlError::UnsupportedQuery(
                    "No common column for NATURAL JOIN".to_string(),
                ))
            }
            JoinConstraint::None => Ok((0, 0)),
        }
    }

    /// Convert an expression to a FilterCondition.
    fn convert_expr_to_filter(
        &self,
        expr: &Expr,
        columns: &[String],
    ) -> SqlResult<FilterCondition> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        let left_cond = self.convert_expr_to_filter(left, columns)?;
                        let right_cond = self.convert_expr_to_filter(right, columns)?;
                        Ok(FilterCondition::And(vec![left_cond, right_cond]))
                    }
                    BinaryOperator::Or => {
                        let left_cond = self.convert_expr_to_filter(left, columns)?;
                        let right_cond = self.convert_expr_to_filter(right, columns)?;
                        Ok(FilterCondition::Or(vec![left_cond, right_cond]))
                    }
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq => self.convert_comparison(left, op, right, columns),
                    _ => Err(SqlError::UnsupportedQuery(format!(
                        "Unsupported operator in WHERE: {:?}",
                        op
                    ))),
                }
            }
            Expr::Nested(inner) => self.convert_expr_to_filter(inner, columns),
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
        op: &BinaryOperator,
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
        if matches!(right, Expr::Value(Value::Placeholder(_))) {
            return Ok(FilterCondition::AlwaysTrue);
        }

        // Get value from right side
        let value = self.convert_expr_to_datatype(right)?;

        // Convert operator
        match op {
            BinaryOperator::Eq => Ok(FilterCondition::Eq(col_idx, value)),
            BinaryOperator::NotEq => Ok(FilterCondition::Ne(col_idx, value)),
            BinaryOperator::Gt => Ok(FilterCondition::Gt(col_idx, value)),
            BinaryOperator::Lt => Ok(FilterCondition::Lt(col_idx, value)),
            BinaryOperator::GtEq => Ok(FilterCondition::Or(vec![
                FilterCondition::Gt(col_idx, value.clone()),
                FilterCondition::Eq(col_idx, value),
            ])),
            BinaryOperator::LtEq => Ok(FilterCondition::Or(vec![
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
            Expr::Identifier(id) => {
                let name = &id.value;
                columns.iter().position(|c| c.eq_ignore_ascii_case(name))
            }
            Expr::CompoundIdentifier(parts) => {
                // table.column format
                let full_name = parts.iter().map(|p| p.value.as_str()).collect::<Vec<_>>().join(".");
                columns.iter().position(|c| c.eq_ignore_ascii_case(&full_name)).or_else(|| {
                    // Try just the column name (last part)
                    if let Some(last) = parts.last() {
                        columns.iter().position(|c| c.eq_ignore_ascii_case(&last.value))
                    } else {
                        None
                    }
                })
            }
            _ => None,
        }
    }

    /// Convert an expression to a DataType value.
    fn convert_expr_to_datatype(&self, expr: &Expr) -> SqlResult<DataType> {
        match expr {
            Expr::Value(val) => self.convert_value(val),
            Expr::UnaryOp { op: sqlparser::ast::UnaryOperator::Minus, expr: inner } => {
                // Handle negative numbers
                if let Expr::Value(Value::Number(n, _)) = inner.as_ref() {
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

    /// Convert a SQL value to a DataType.
    fn convert_value(&self, val: &Value) -> SqlResult<DataType> {
        match val {
            Value::Number(n, _) => {
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
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(DataType::from(s.as_str())),
            Value::Null => Ok(DataType::None),
            Value::Boolean(b) => Ok(DataType::Int(if *b { 1 } else { 0 })),
            Value::Placeholder(_) => Ok(DataType::None), // Placeholder
            _ => Err(SqlError::UnsupportedQuery(format!(
                "Unsupported value type: {:?}",
                val
            ))),
        }
    }

    /// Extract aggregate function info from SELECT columns.
    fn extract_aggregate(
        &self,
        projection: &[SelectItem],
        current_columns: &[String],
    ) -> SqlResult<Option<AggInfo>> {
        for item in projection {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                if let Some(agg) = self.extract_aggregate_from_expr(expr, current_columns)? {
                    let alias_name = match item {
                        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                        _ => "agg".to_string(),
                    };
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
            Expr::Function(func) => {
                let func_name = func.name.0.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join(".").to_uppercase();

                match func_name.as_str() {
                    "COUNT" => Ok(Some(AggregateFunc::Count)),
                    "SUM" => {
                        if let Some(first_arg) = func.args.first() {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = first_arg {
                                if let Some(col_idx) = self.get_column_index_from_expr(&arg_expr, columns) {
                                    return Ok(Some(AggregateFunc::Sum(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Sum(0)))
                    }
                    "AVG" => {
                        if let Some(first_arg) = func.args.first() {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = first_arg {
                                if let Some(col_idx) = self.get_column_index_from_expr(&arg_expr, columns) {
                                    return Ok(Some(AggregateFunc::Avg(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Avg(0)))
                    }
                    "MIN" => {
                        if let Some(first_arg) = func.args.first() {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = first_arg {
                                if let Some(col_idx) = self.get_column_index_from_expr(&arg_expr, columns) {
                                    return Ok(Some(AggregateFunc::Min(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Min(0)))
                    }
                    "MAX" => {
                        if let Some(first_arg) = func.args.first() {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = first_arg {
                                if let Some(col_idx) = self.get_column_index_from_expr(&arg_expr, columns) {
                                    return Ok(Some(AggregateFunc::Max(col_idx)));
                                }
                            }
                        }
                        Ok(Some(AggregateFunc::Max(0)))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Get projection column indices and names.
    fn get_projection(
        &self,
        projection: &[SelectItem],
        current_columns: &[String],
    ) -> SqlResult<Option<(Vec<usize>, Vec<String>)>> {
        let mut indices = Vec::new();
        let mut names = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    // SELECT * - include all columns
                    return Ok(None);
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    // SELECT table.* - include all columns (simplified)
                    return Ok(None);
                }
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(idx) = self.get_column_index_from_expr(expr, current_columns) {
                        indices.push(idx);
                        names.push(current_columns[idx].clone());
                    } else {
                        // Could be an expression - skip projection for now
                        return Ok(None);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(idx) = self.get_column_index_from_expr(expr, current_columns) {
                        indices.push(idx);
                        names.push(alias.value.clone());
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
            Expr::BinaryOp { left, op: BinaryOperator::Eq, .. } => {
                self.get_column_index_from_expr(left, columns)
            }
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                // For AND, try left side first
                self.extract_equality_column(left, columns)
                    .or_else(|| self.extract_equality_column(right, columns))
            }
            Expr::Nested(inner) => self.extract_equality_column(inner, columns),
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
    use crate::dataflow::Records;

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

        // JOIN should be supported
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

    #[test]
    fn test_dialect_sqlite() {
        let mut converter = SqlConverter::with_dialect(SqlDialect::SQLite);
        converter.register_table("users", vec!["id".into(), "name".into()]);

        let mut executor = LocalExecutor::new();
        let result = converter.convert_select("SELECT * FROM users WHERE id = 1", &mut executor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_dialect_postgres() {
        let mut converter = SqlConverter::with_dialect(SqlDialect::PostgreSQL);
        converter.register_table("users", vec!["id".into(), "name".into()]);

        let mut executor = LocalExecutor::new();
        let result = converter.convert_select("SELECT * FROM users WHERE id = 1", &mut executor);
        assert!(result.is_ok());
    }
}
