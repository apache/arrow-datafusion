// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Query Planner (produces logical plan from SQL AST)

use std::collections::HashSet;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;
use std::{convert::TryInto, vec};

use crate::catalog::TableReference;
use crate::datasource::TableProvider;
use crate::logical_plan::window_frames::{WindowFrame, WindowFrameUnits};
use crate::logical_plan::Expr::Alias;
use crate::logical_plan::{
    and, builder::expand_wildcard, col, lit, normalize_col, union_with_alias, Column,
    CreateCatalogSchema, CreateExternalTable as PlanCreateExternalTable,
    CreateMemoryTable, DFSchema, DFSchemaRef, DropTable, Expr, LogicalPlan,
    LogicalPlanBuilder, Operator, PlanType, ToDFSchema, ToStringifiedPlan,
};
use crate::optimizer::utils::exprlist_to_columns;
use crate::prelude::JoinType;
use crate::scalar::ScalarValue;
use crate::sql::utils::{make_decimal_type, normalize_ident};
use crate::{
    error::{DataFusionError, Result},
    physical_plan::udaf::AggregateUDF,
};
use crate::{
    physical_plan::udf::ScalarUDF,
    physical_plan::{aggregates, functions, window_functions},
    sql::parser::{CreateExternalTable, FileType, Statement as DFStatement},
};
use arrow::datatypes::*;
use hashbrown::HashMap;
use sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, DateTimeField, Expr as SQLExpr, FunctionArg,
    FunctionArgExpr, Ident, Join, JoinConstraint, JoinOperator, ObjectName, Query,
    Select, SelectItem, SetExpr, SetOperator, ShowStatementFilter, TableFactor,
    TableWithJoins, TrimWhereField, UnaryOperator, Value, Values as SQLValues,
};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{ObjectType, OrderByExpr, Statement};
use sqlparser::parser::ParserError::ParserError;

use super::{
    parser::DFParser,
    utils::{
        can_columns_satisfy_exprs, expr_as_column_expr, extract_aliases,
        find_aggregate_exprs, find_column_exprs, find_window_exprs, rebase_expr,
        resolve_aliases_to_exprs, resolve_positions_to_exprs,
    },
};
use crate::logical_plan::builder::project_with_alias;
use crate::logical_plan::plan::{Analyze, Explain};

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
    /// Getter for system/user-defined variable type
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType>;
}

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    schema_provider: &'a S,
}

fn plan_key(key: SQLExpr) -> Result<ScalarValue> {
    let scalar = match key {
        SQLExpr::Value(Value::Number(s, _)) => {
            ScalarValue::Int64(Some(s.parse().unwrap()))
        }
        SQLExpr::Value(Value::SingleQuotedString(s)) => ScalarValue::Utf8(Some(s)),
        _ => {
            return Err(DataFusionError::SQL(ParserError(format!(
                "Unsuported index key expression: {}",
                key
            ))))
        }
    };

    Ok(scalar)
}

fn plan_indexed(expr: Expr, mut keys: Vec<SQLExpr>) -> Result<Expr> {
    let key = keys.pop().ok_or_else(|| {
        DataFusionError::SQL(ParserError(
            "Internal error: Missing index key expression".to_string(),
        ))
    })?;

    let expr = if !keys.is_empty() {
        plan_indexed(expr, keys)?
    } else {
        expr
    };

    Ok(Expr::GetIndexedField {
        expr: Box::new(expr),
        key: plan_key(key)?,
    })
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        SqlToRel { schema_provider }
    }

    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(&self, statement: DFStatement) -> Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => self.external_table_to_plan(s),
            DFStatement::Statement(s) => self.sql_statement_to_plan(*s),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, sql: Statement) -> Result<LogicalPlan> {
        match sql {
            Statement::Explain {
                verbose,
                statement,
                analyze,
                describe_alias: _,
            } => self.explain_statement_to_plan(verbose, analyze, *statement),
            Statement::Query(query) => self.query_to_plan(*query),
            Statement::ShowVariable { variable } => self.show_variable_to_plan(&variable),
            Statement::CreateTable {
                query: Some(query),
                name,
                columns,
                constraints,
                table_properties,
                with_options,
                ..
            } if columns.is_empty()
                && constraints.is_empty()
                && table_properties.is_empty()
                && with_options.is_empty() =>
            {
                let plan = self.query_to_plan(*query)?;

                Ok(LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                    name: name.to_string(),
                    input: Arc::new(plan),
                }))
            }
            Statement::CreateTable { .. } => Err(DataFusionError::NotImplemented(
                "Only `CREATE TABLE table_name AS SELECT ...` statement is supported"
                    .to_string(),
            )),
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => Ok(LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name: schema_name.to_string(),
                if_not_exists,
                schema: Arc::new(DFSchema::empty()),
            })),
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                cascade: _,
                purge: _,
            } =>
            // We don't support cascade and purge for now.
            {
                Ok(LogicalPlan::DropTable(DropTable {
                    name: names.get(0).unwrap().to_string(),
                    if_exists,
                    schema: DFSchemaRef::new(DFSchema::empty()),
                }))
            }

            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => self.show_columns_to_plan(extended, full, &table_name, filter.as_ref()),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                sql
            ))),
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: Query) -> Result<LogicalPlan> {
        self.query_to_plan_with_alias(query, None, &mut HashMap::new())
    }

    /// Generate a logic plan from an SQL query with optional alias
    pub fn query_to_plan_with_alias(
        &self,
        query: Query,
        alias: Option<String>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let set_expr = query.body;
        if let Some(with) = query.with {
            // Process CTEs from top to bottom
            // do not allow self-references
            for cte in with.cte_tables {
                // A `WITH` block can't use the same name for many times
                let cte_name: &str = cte.alias.name.value.as_ref();
                if ctes.contains_key(cte_name) {
                    return Err(DataFusionError::SQL(ParserError(format!(
                        "WITH query name {:?} specified more than once",
                        cte_name
                    ))));
                }
                // create logical plan & pass backreferencing CTEs
                let logical_plan = self.query_to_plan_with_alias(
                    cte.query,
                    Some(cte.alias.name.value.clone()),
                    &mut ctes.clone(),
                )?;
                ctes.insert(cte.alias.name.value, logical_plan);
            }
        }
        let plan = self.set_expr_to_plan(set_expr, alias, ctes)?;

        let plan = self.order_by(plan, query.order_by)?;

        self.limit(plan, query.limit)
    }

    fn set_expr_to_plan(
        &self,
        set_expr: SetExpr,
        alias: Option<String>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_plan(*s, ctes, alias),
            SetExpr::Values(v) => self.sql_values_to_plan(v),
            SetExpr::SetOperation {
                op,
                left,
                right,
                all,
            } => {
                let left_plan = self.set_expr_to_plan(*left, None, ctes)?;
                let right_plan = self.set_expr_to_plan(*right, None, ctes)?;
                match (op, all) {
                    (SetOperator::Union, true) => {
                        union_with_alias(left_plan, right_plan, alias)
                    }
                    (SetOperator::Union, false) => {
                        let union_plan = union_with_alias(left_plan, right_plan, alias)?;
                        LogicalPlanBuilder::from(union_plan).distinct()?.build()
                    }
                    (SetOperator::Intersect, true) => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, true)
                    }
                    (SetOperator::Intersect, false) => {
                        LogicalPlanBuilder::intersect(left_plan, right_plan, false)
                    }
                    (SetOperator::Except, true) => {
                        LogicalPlanBuilder::except(left_plan, right_plan, true)
                    }
                    (SetOperator::Except, false) => {
                        LogicalPlanBuilder::except(left_plan, right_plan, false)
                    }
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(
        &self,
        statement: CreateExternalTable,
    ) -> Result<LogicalPlan> {
        let CreateExternalTable {
            name,
            columns,
            file_type,
            has_header,
            location,
        } = statement;

        // semantic checks
        match file_type {
            FileType::CSV => {}
            FileType::Parquet => {
                if !columns.is_empty() {
                    return Err(DataFusionError::Plan(
                        "Column definitions can not be specified for PARQUET files."
                            .into(),
                    ));
                }
            }
            FileType::NdJson => {}
            FileType::Avro => {}
            FileType::Arrow => {}
        };

        let schema = self.build_schema(columns)?;

        Ok(LogicalPlan::CreateExternalTable(PlanCreateExternalTable {
            schema: schema.to_dfschema_ref()?,
            name,
            location,
            file_type,
            has_header,
        }))
    }

    /// Generate a plan for EXPLAIN ... that will print out a plan
    ///
    pub fn explain_statement_to_plan(
        &self,
        verbose: bool,
        analyze: bool,
        statement: Statement,
    ) -> Result<LogicalPlan> {
        let plan = self.sql_statement_to_plan(statement)?;
        let plan = Arc::new(plan);
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(LogicalPlan::Analyze(Analyze {
                verbose,
                input: plan,
                schema,
            }))
        } else {
            let stringified_plans =
                vec![plan.to_stringified(PlanType::InitialLogicalPlan)];
            Ok(LogicalPlan::Explain(Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            }))
        }
    }

    fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.make_data_type(&column.data_type)?;
            let allow_null = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::Null);
            fields.push(Field::new(&column.name.value, data_type, allow_null));
        }

        Ok(Schema::new(fields))
    }

    /// Maps the SQL type to the corresponding Arrow `DataType`
    fn make_data_type(&self, sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::BigInt(_) => Ok(DataType::Int64),
            SQLDataType::Int(_) => Ok(DataType::Int32),
            SQLDataType::SmallInt(_) => Ok(DataType::Int16),
            SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => {
                Ok(DataType::Utf8)
            }
            SQLDataType::Decimal(precision, scale) => {
                make_decimal_type(*precision, *scale)
            }
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real => Ok(DataType::Float32),
            SQLDataType::Double => Ok(DataType::Float64),
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
            SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => Err(DataFusionError::NotImplemented(format!(
                "The SQL data type {:?} is not implemented",
                sql_type
            ))),
        }
    }

    fn plan_from_tables(
        &self,
        from: Vec<TableWithJoins>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<Vec<LogicalPlan>> {
        match from.len() {
            0 => Ok(vec![LogicalPlanBuilder::empty(true).build()?]),
            _ => from
                .into_iter()
                .map(|t| self.plan_table_with_joins(t, ctes))
                .collect::<Result<Vec<_>>>(),
        }
    }

    fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let left = self.create_relation(t.relation, ctes)?;
        match t.joins.len() {
            0 => Ok(left),
            _ => {
                let mut joins = t.joins.into_iter();
                let mut left =
                    self.parse_relation_join(left, joins.next().unwrap(), ctes)?;
                for join in joins {
                    left = self.parse_relation_join(left, join, ctes)?;
                }
                Ok(left)
            }
        }
    }

    fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let right = self.create_relation(join.relation, ctes)?;
        match join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner)
            }
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full)
            }
            JoinOperator::CrossJoin => self.parse_cross_join(left, &right),
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported JOIN operator {:?}",
                other
            ))),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let mut keys: Vec<(Column, Column)> = vec![];
                let join_schema = left.schema().join(right.schema())?;

                // parse ON expression
                let expr = self.sql_to_rex(sql_expr, &join_schema)?;

                // expression that didn't match equi-join pattern
                let mut filter = vec![];

                // extract join keys
                extract_join_keys(expr, &mut keys, &mut filter);

                let mut cols = HashSet::new();
                exprlist_to_columns(&filter, &mut cols)?;

                let (left_keys, right_keys): (Vec<Column>, Vec<Column>) =
                    keys.into_iter().unzip();

                // return the logical plan representing the join
                if filter.is_empty() {
                    let join = LogicalPlanBuilder::from(left).join(
                        &right,
                        join_type,
                        (left_keys, right_keys),
                    )?;
                    join.build()
                } else if join_type == JoinType::Inner {
                    let join = LogicalPlanBuilder::from(left).join(
                        &right,
                        join_type,
                        (left_keys, right_keys),
                    )?;
                    let join_filter_init = filter.remove(0);
                    join.filter(
                        filter
                            .into_iter()
                            .fold(join_filter_init, |acc, e| acc.and(e)),
                    )?
                    .build()
                }
                // Left join with all non-equijoin expressions from the right
                // l left join r
                // on l1=r1 and r2 > [..]
                else if join_type == JoinType::Left
                    && cols.iter().all(
                        |Column {
                             relation: qualifier,
                             name,
                         }| {
                            right
                                .schema()
                                .field_with_name(qualifier.as_deref(), name)
                                .is_ok()
                        },
                    )
                {
                    let join_filter_init = filter.remove(0);
                    LogicalPlanBuilder::from(left)
                        .join(
                            &LogicalPlanBuilder::from(right)
                                .filter(
                                    filter
                                        .into_iter()
                                        .fold(join_filter_init, |acc, e| acc.and(e)),
                                )?
                                .build()?,
                            join_type,
                            (left_keys, right_keys),
                        )?
                        .build()
                }
                // Right join with all non-equijoin expressions from the left
                // l right join r
                // on l1=r1 and l2 > [..]
                else if join_type == JoinType::Right
                    && cols.iter().all(
                        |Column {
                             relation: qualifier,
                             name,
                         }| {
                            left.schema()
                                .field_with_name(qualifier.as_deref(), name)
                                .is_ok()
                        },
                    )
                {
                    let join_filter_init = filter.remove(0);
                    LogicalPlanBuilder::from(left)
                        .filter(
                            filter
                                .into_iter()
                                .fold(join_filter_init, |acc, e| acc.and(e)),
                        )?
                        .join(&right, join_type, (left_keys, right_keys))?
                        .build()
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "Unsupported expressions in {:?} JOIN: {:?}",
                        join_type, filter
                    )))
                }
            }
            JoinConstraint::Using(idents) => {
                let keys: Vec<Column> = idents
                    .into_iter()
                    .map(|x| Column::from_name(x.value))
                    .collect();
                LogicalPlanBuilder::from(left)
                    .join_using(&right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                // https://issues.apache.org/jira/browse/ARROW-10727
                Err(DataFusionError::NotImplemented(
                    "NATURAL JOIN is not supported (https://issues.apache.org/jira/browse/ARROW-10727)".to_string(),
                ))
            }
            JoinConstraint::None => Err(DataFusionError::NotImplemented(
                "NONE constraint is not supported".to_string(),
            )),
        }
    }

    fn create_relation(
        &self,
        relation: TableFactor,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table {
                ref name, alias, ..
            } => {
                let table_name = name.to_string();
                let cte = ctes.get(&table_name);
                (
                    match (
                        cte,
                        self.schema_provider.get_table_provider(name.try_into()?),
                    ) {
                        (Some(cte_plan), _) => Ok(cte_plan.clone()),
                        (_, Some(provider)) => LogicalPlanBuilder::scan(
                            // take alias into account to support `JOIN table1 as table2`
                            alias
                                .as_ref()
                                .map(|a| a.name.value.as_str())
                                .unwrap_or(&table_name),
                            provider,
                            None,
                        )?
                        .build(),
                        (None, None) => Err(DataFusionError::Plan(format!(
                            "Table or CTE with name '{}' not found",
                            name
                        ))),
                    }?,
                    alias,
                )
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                // if alias is None, return Err
                if alias.is_none() {
                    return Err(DataFusionError::Plan(
                        "subquery in FROM must have an alias".to_string(),
                    ));
                }
                let logical_plan = self.query_to_plan_with_alias(
                    *subquery,
                    alias.as_ref().map(|a| a.name.value.to_string()),
                    ctes,
                )?;
                (
                    project_with_alias(
                        logical_plan.clone(),
                        logical_plan
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| col(field.name())),
                        alias.as_ref().map(|a| a.name.value.to_string()),
                    )?,
                    alias,
                )
            }
            TableFactor::NestedJoin(table_with_joins) => {
                (self.plan_table_with_joins(*table_with_joins, ctes)?, None)
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {:?} in create_relation",
                    relation
                )));
            }
        };
        if let Some(alias) = alias {
            let columns_alias = alias.clone().columns;
            if columns_alias.is_empty() {
                // sqlparser-rs encodes AS t as an empty list of column alias
                Ok(plan)
            } else if columns_alias.len() != plan.schema().fields().len() {
                return Err(DataFusionError::Plan(format!(
                    "Source table contains {} columns but only {} names given as column alias",
                    plan.schema().fields().len(),
                    columns_alias.len(),
                )));
            } else {
                Ok(LogicalPlanBuilder::from(plan.clone())
                    .project_with_alias(
                        plan.schema()
                            .fields()
                            .iter()
                            .zip(columns_alias.iter())
                            .map(|(field, ident)| col(field.name()).alias(&ident.value)),
                        Some(alias.name.value),
                    )?
                    .build()?)
            }
        } else {
            Ok(plan)
        }
    }

    /// Generate a logic plan from selection clause, the function contain optimization for cross join to inner join
    /// Related PR: <https://github.com/apache/arrow-datafusion/pull/1566>
    fn plan_selection(
        &self,
        selection: Option<SQLExpr>,
        plans: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        let plan = match selection {
            Some(predicate_expr) => {
                // build join schema
                let mut fields = vec![];
                let mut metadata = std::collections::HashMap::new();
                for plan in &plans {
                    fields.extend_from_slice(plan.schema().fields());
                    metadata.extend(plan.schema().metadata().clone());
                }
                let join_schema = DFSchema::new_with_metadata(fields, metadata)?;

                let filter_expr = self.sql_to_rex(predicate_expr, &join_schema)?;

                // look for expressions of the form `<column> = <column>`
                let mut possible_join_keys = vec![];
                extract_possible_join_keys(&filter_expr, &mut possible_join_keys)?;

                let mut all_join_keys = HashSet::new();

                let mut plans = plans.into_iter();
                let mut left = plans.next().unwrap(); // have at least one plan

                // List of the plans that have not yet been joined
                let mut remaining_plans: Vec<Option<LogicalPlan>> =
                    plans.into_iter().map(Some).collect();

                // Take from the list of remaining plans,
                loop {
                    let mut join_keys = vec![];

                    // Search all remaining plans for the next to
                    // join. Prefer the first one that has a join
                    // predicate in the predicate lists
                    let plan_with_idx =
                        remaining_plans.iter().enumerate().find(|(_idx, plan)| {
                            // skip plans that have been joined already
                            let plan = if let Some(plan) = plan {
                                plan
                            } else {
                                return false;
                            };

                            // can we find a match?
                            let left_schema = left.schema();
                            let right_schema = plan.schema();
                            for (l, r) in &possible_join_keys {
                                if left_schema.field_from_column(l).is_ok()
                                    && right_schema.field_from_column(r).is_ok()
                                {
                                    join_keys.push((l.clone(), r.clone()));
                                } else if left_schema.field_from_column(r).is_ok()
                                    && right_schema.field_from_column(l).is_ok()
                                {
                                    join_keys.push((r.clone(), l.clone()));
                                }
                            }
                            // stop if we found join keys
                            !join_keys.is_empty()
                        });

                    // If we did not find join keys, either there are
                    // no more plans, or we can't find any plans that
                    // can be joined with predicates
                    if join_keys.is_empty() {
                        assert!(plan_with_idx.is_none());

                        // pick the first non null plan to join
                        let plan_with_idx = remaining_plans
                            .iter()
                            .enumerate()
                            .find(|(_idx, plan)| plan.is_some());
                        if let Some((idx, _)) = plan_with_idx {
                            let plan = std::mem::take(&mut remaining_plans[idx]).unwrap();
                            left = LogicalPlanBuilder::from(left)
                                .cross_join(&plan)?
                                .build()?;
                        } else {
                            // no more plans to join
                            break;
                        }
                    } else {
                        // have a plan
                        let (idx, _) = plan_with_idx.expect("found plan node");
                        let plan = std::mem::take(&mut remaining_plans[idx]).unwrap();

                        let left_keys: Vec<Column> =
                            join_keys.iter().map(|(l, _)| l.clone()).collect();
                        let right_keys: Vec<Column> =
                            join_keys.iter().map(|(_, r)| r.clone()).collect();
                        let builder = LogicalPlanBuilder::from(left);
                        left = builder
                            .join(&plan, JoinType::Inner, (left_keys, right_keys))?
                            .build()?;
                    }

                    all_join_keys.extend(join_keys);
                }

                // remove join expressions from filter
                match remove_join_expressions(&filter_expr, &all_join_keys)? {
                    Some(filter_expr) => {
                        LogicalPlanBuilder::from(left).filter(filter_expr)?.build()
                    }
                    _ => Ok(left),
                }
            }
            None => {
                if plans.len() == 1 {
                    Ok(plans[0].clone())
                } else {
                    let mut left = plans[0].clone();
                    for right in plans.iter().skip(1) {
                        left =
                            LogicalPlanBuilder::from(left).cross_join(right)?.build()?;
                    }
                    Ok(left)
                }
            }
        };
        plan
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        select: Select,
        ctes: &mut HashMap<String, LogicalPlan>,
        alias: Option<String>,
    ) -> Result<LogicalPlan> {
        // process `from` clause
        let plans = self.plan_from_tables(select.from, ctes)?;
        let empty_from = matches!(plans.first(), Some(LogicalPlan::EmptyRelation(_)));

        // process `where` clause
        let plan = self.plan_selection(select.selection, plans)?;

        // process the SELECT expressions, with wildcards expanded.
        let select_exprs =
            self.prepare_select_exprs(&plan, select.projection, empty_from)?;

        // having and group by clause may reference aliases defined in select projection
        let projected_plan = self.project(plan.clone(), select_exprs.clone())?;
        let mut combined_schema = (**projected_plan.schema()).clone();
        combined_schema.merge(plan.schema());

        // this alias map is resolved and looked up in both having exprs and group by exprs
        let alias_map = extract_aliases(&select_exprs);

        // Optionally the HAVING expression.
        let having_expr_opt = select
            .having
            .map::<Result<Expr>, _>(|having_expr| {
                let having_expr =
                    self.sql_expr_to_logical_expr(having_expr, &combined_schema)?;
                // This step "dereferences" any aliases in the HAVING clause.
                //
                // This is how we support queries with HAVING expressions that
                // refer to aliased columns.
                //
                // For example:
                //
                //   SELECT c1 AS m FROM t HAVING m > 10;
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10;
                //
                // are rewritten as, respectively:
                //
                //   SELECT c1 AS m FROM t HAVING c1 > 10;
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING MAX(c2) > 10;
                //
                let having_expr = resolve_aliases_to_exprs(&having_expr, &alias_map)?;
                normalize_col(having_expr, &projected_plan)
            })
            .transpose()?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT...
        let mut aggr_expr_haystack = select_exprs.clone();
        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr_opt {
            aggr_expr_haystack.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        // All of the group by expressions
        let group_by_exprs = select
            .group_by
            .into_iter()
            .map(|e| {
                let group_by_expr = self.sql_expr_to_logical_expr(e, &combined_schema)?;
                let group_by_expr = resolve_aliases_to_exprs(&group_by_expr, &alias_map)?;
                let group_by_expr =
                    resolve_positions_to_exprs(&group_by_expr, &select_exprs)
                        .unwrap_or(group_by_expr);
                let group_by_expr = normalize_col(group_by_expr, &projected_plan)?;
                self.validate_schema_satisfies_exprs(
                    plan.schema(),
                    &[group_by_expr.clone()],
                )?;
                Ok(group_by_expr)
            })
            .collect::<Result<Vec<Expr>>>()?;

        // process group by, aggregation or having
        let (plan, select_exprs_post_aggr, having_expr_post_aggr_opt) = if !group_by_exprs
            .is_empty()
            || !aggr_exprs.is_empty()
        {
            self.aggregate(
                plan,
                &select_exprs,
                &having_expr_opt,
                group_by_exprs,
                aggr_exprs,
            )?
        } else {
            if let Some(having_expr) = &having_expr_opt {
                let available_columns = select_exprs
                    .iter()
                    .map(|expr| expr_as_column_expr(expr, &plan))
                    .collect::<Result<Vec<Expr>>>()?;

                // Ensure the HAVING expression is using only columns
                // provided by the SELECT.
                if !can_columns_satisfy_exprs(&available_columns, &[having_expr.clone()])?
                {
                    return Err(DataFusionError::Plan(
                        "Having references column(s) not provided by the select"
                            .to_owned(),
                    ));
                }
            }

            (plan, select_exprs, having_expr_opt)
        };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr_opt {
            LogicalPlanBuilder::from(plan)
                .filter(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        // process window function
        let window_func_exprs = find_window_exprs(&select_exprs_post_aggr);

        let plan = if window_func_exprs.is_empty() {
            plan
        } else {
            LogicalPlanBuilder::window_plan(plan, window_func_exprs)?
        };

        // process distinct clause
        let plan = if select.distinct {
            return LogicalPlanBuilder::from(plan)
                .aggregate(select_exprs_post_aggr, iter::empty::<Expr>())?
                .build();
        } else {
            plan
        };

        // generate the final projection plan
        project_with_alias(plan, select_exprs_post_aggr, alias)
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    ///
    /// Wildcards are expanded into the concrete list of columns.
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
    ) -> Result<Vec<Expr>> {
        let input_schema = plan.schema();
        projection
            .into_iter()
            .map(|expr| self.sql_select_to_rex(expr, input_schema))
            .collect::<Result<Vec<Expr>>>()?
            .into_iter()
            .map(|expr| {
                Ok(match expr {
                    Expr::Wildcard => {
                        if empty_from {
                            return Err(DataFusionError::Plan(
                                "SELECT * with no tables specified is not valid"
                                    .to_string(),
                            ));
                        }
                        expand_wildcard(input_schema, plan)?
                    }
                    _ => vec![normalize_col(expr, plan)?],
                })
            })
            .flat_map(|res| match res {
                Ok(v) => v.into_iter().map(Ok).collect(),
                Err(e) => vec![Err(e)],
            })
            .collect::<Result<Vec<Expr>>>()
    }

    /// Wrap a plan in a projection
    fn project(&self, input: LogicalPlan, expr: Vec<Expr>) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(input.schema(), &expr)?;
        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Wrap a plan in an aggregate
    fn aggregate(
        &self,
        input: LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: &Option<Expr>,
        group_by_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        let aggr_projection_exprs = group_by_exprs
            .iter()
            .chain(aggr_exprs.iter())
            .cloned()
            .collect::<Vec<Expr>>();

        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(group_by_exprs, aggr_exprs)?
            .build()?;

        // After aggregation, these are all of the columns that will be
        // available to next phases of planning.
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // Rewrite the SELECT expression to use the columns produced by the
        // aggregation.
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &aggr_projection_exprs, &input))
            .collect::<Result<Vec<Expr>>>()?;

        if !can_columns_satisfy_exprs(&column_exprs_post_aggr, &select_exprs_post_aggr)? {
            return Err(DataFusionError::Plan(
                "Projection references non-aggregate values".to_owned(),
            ));
        }

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr_opt = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, &input)?;

            if !can_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                &[having_expr_post_aggr.clone()],
            )? {
                return Err(DataFusionError::Plan(
                    "Having references non-aggregate values".to_owned(),
                ));
            }

            Some(having_expr_post_aggr)
        } else {
            None
        };

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr_opt))
    }

    /// Wrap a plan in a limit
    fn limit(&self, input: LogicalPlan, limit: Option<SQLExpr>) -> Result<LogicalPlan> {
        match limit {
            Some(limit_expr) => {
                let n = match self.sql_to_rex(limit_expr, input.schema())? {
                    Expr::Literal(ScalarValue::Int64(Some(n))) => Ok(n as usize),
                    _ => Err(DataFusionError::Plan(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;

                LogicalPlanBuilder::from(input).limit(n)?.build()
            }
            _ => Ok(input),
        }
    }

    /// Wrap the logical in a sort
    fn order_by(
        &self,
        plan: LogicalPlan,
        order_by: Vec<OrderByExpr>,
    ) -> Result<LogicalPlan> {
        if order_by.is_empty() {
            return Ok(plan);
        }

        let order_by_rex = order_by
            .into_iter()
            .map(|e| self.order_by_to_sort_expr(e, plan.schema()))
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(plan).sort(order_by_rex)?.build()
    }

    /// convert sql OrderByExpr to Expr::Sort
    fn order_by_to_sort_expr(&self, e: OrderByExpr, schema: &DFSchema) -> Result<Expr> {
        let OrderByExpr {
            asc,
            expr,
            nulls_first,
        } = e;

        let expr = match expr {
            SQLExpr::Value(Value::Number(v, _)) => {
                let field_index = v
                    .parse::<usize>()
                    .map_err(|err| DataFusionError::Plan(err.to_string()))?;

                if field_index == 0 {
                    return Err(DataFusionError::Plan(
                        "Order by index starts at 1 for column indexes".to_string(),
                    ));
                } else if schema.fields().len() < field_index {
                    return Err(DataFusionError::Plan(format!(
                        "Order by column out of bounds, specified: {}, max: {}",
                        field_index,
                        schema.fields().len()
                    )));
                }

                let field = schema.field(field_index - 1);
                Expr::Column(field.qualified_column())
            }
            e => self.sql_expr_to_logical_expr(e, schema)?,
        };
        Ok({
            let asc = asc.unwrap_or(true);
            Expr::Sort {
                expr: Box::new(expr),
                asc,
                // when asc is true, by default nulls last to be consistent with postgres
                // postgres rule: https://www.postgresql.org/docs/current/queries-order.html
                nulls_first: nulls_first.unwrap_or(!asc),
            }
        })
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    fn validate_schema_satisfies_exprs(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    Some(r) => {
                        schema.field_with_qualified_name(r, &col.name)?;
                        Ok(())
                    }
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(DataFusionError::Plan(format!(
                                "No field with unqualified name '{}'",
                                &col.name
                            )))
                        }
                    }
                }
                .map_err(|_: DataFusionError| {
                    DataFusionError::Plan(format!(
                        "Invalid identifier '{}' for schema {}",
                        col, schema
                    ))
                }),
                _ => Err(DataFusionError::Internal("Not a column".to_string())),
            })
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(&self, sql: SelectItem, schema: &DFSchema) -> Result<Expr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema),
            SelectItem::ExprWithAlias { expr, alias } => Ok(Alias(
                Box::new(self.sql_to_rex(expr, schema)?),
                normalize_ident(alias),
            )),
            SelectItem::Wildcard => Ok(Expr::Wildcard),
            SelectItem::QualifiedWildcard(_) => Err(DataFusionError::NotImplemented(
                "Qualified wildcards are not supported".to_string(),
            )),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, sql: SQLExpr, schema: &DFSchema) -> Result<Expr> {
        let mut expr = self.sql_expr_to_logical_expr(sql, schema)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema
                        .fields()
                        .iter()
                        .find(|field| match field.qualifier() {
                            Some(field_q) => {
                                field.name() == &col.name
                                    && field_q.ends_with(&format!(".{}", q))
                            }
                            _ => false,
                        }) {
                        Some(df_field) => Expr::Column(Column {
                            relation: df_field.qualifier().cloned(),
                            name: df_field.name().clone(),
                        }),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    fn sql_fn_arg_to_logical_expr(
        &self,
        sql: FunctionArg,
        schema: &DFSchema,
    ) -> Result<Expr> {
        match sql {
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Expr(arg),
            } => self.sql_expr_to_logical_expr(arg, schema),
            FunctionArg::Named {
                name: _,
                arg: FunctionArgExpr::Wildcard,
            } => Ok(Expr::Wildcard),
            FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) => {
                self.sql_expr_to_logical_expr(arg, schema)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Expr::Wildcard),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported qualified wildcard argument: {:?}",
                sql
            ))),
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: SQLExpr,
        op: BinaryOperator,
        right: SQLExpr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let operator = match op {
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Modulo => Ok(Operator::Modulo),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::Like => Ok(Operator::Like),
            BinaryOperator::NotLike => Ok(Operator::NotLike),
            BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
            BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
            BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
            BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
            BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
            BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}",
                op
            ))),
        }?;

        Ok(Expr::BinaryExpr {
            left: Box::new(self.sql_expr_to_logical_expr(left, schema)?),
            op: operator,
            right: Box::new(self.sql_expr_to_logical_expr(right, schema)?),
        })
    }

    fn parse_sql_unary_op(
        &self,
        op: UnaryOperator,
        expr: SQLExpr,
        schema: &DFSchema,
    ) -> Result<Expr> {
        match op {
            UnaryOperator::Not => Ok(Expr::Not(Box::new(
                self.sql_expr_to_logical_expr(expr, schema)?,
            ))),
            UnaryOperator::Plus => Ok(self.sql_expr_to_logical_expr(expr, schema)?),
            UnaryOperator::Minus => {
                match expr {
                    // optimization: if it's a number literal, we apply the negative operator
                    // here directly to calculate the new literal.
                    SQLExpr::Value(Value::Number(n, _)) => match n.parse::<i64>() {
                        Ok(n) => Ok(lit(-n)),
                        Err(_) => Ok(lit(-n
                            .parse::<f64>()
                            .map_err(|_e| {
                                DataFusionError::Internal(format!(
                                    "negative operator can be only applied to integer and float operands, got: {}",
                                    n))
                            })?)),
                    },
                    // not a literal, apply negative operator on expression
                    _ => Ok(Expr::Negative(Box::new(self.sql_expr_to_logical_expr(expr, schema)?))),
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL unary operator {:?}",
                op
            ))),
        }
    }

    fn sql_values_to_plan(&self, values: SQLValues) -> Result<LogicalPlan> {
        // values should not be based on any other schema
        let schema = DFSchema::empty();
        let values = values
            .0
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        SQLExpr::Value(Value::Number(n, _)) => parse_sql_number(&n),
                        SQLExpr::Value(Value::SingleQuotedString(s)) => Ok(lit(s)),
                        SQLExpr::Value(Value::Null) => {
                            Ok(Expr::Literal(ScalarValue::Utf8(None)))
                        }
                        SQLExpr::Value(Value::Boolean(n)) => Ok(lit(n)),
                        SQLExpr::UnaryOp { op, expr } => {
                            self.parse_sql_unary_op(op, *expr, &schema)
                        }
                        SQLExpr::BinaryOp { left, op, right } => {
                            self.parse_sql_binary_op(*left, op, *right, &schema)
                        }
                        other => Err(DataFusionError::NotImplemented(format!(
                            "Unsupported value {:?} in a values list expression",
                            other
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::values(values)?.build()
    }

    fn sql_expr_to_logical_expr(&self, sql: SQLExpr, schema: &DFSchema) -> Result<Expr> {
        match sql {
            SQLExpr::Value(Value::Number(n, _)) => parse_sql_number(&n),
            SQLExpr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),
            SQLExpr::Value(Value::Boolean(n)) => Ok(lit(n)),
            SQLExpr::Value(Value::Null) => Ok(Expr::Literal(ScalarValue::Utf8(None))),
            SQLExpr::Extract { field, expr } => Ok(Expr::ScalarFunction {
                fun: functions::BuiltinScalarFunction::DatePart,
                args: vec![
                    Expr::Literal(ScalarValue::Utf8(Some(format!("{}", field)))),
                    self.sql_expr_to_logical_expr(*expr, schema)?,
                ],
            }),

            SQLExpr::Value(Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            }) => self.sql_interval_to_literal(
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            ),

            SQLExpr::Identifier(id) => {
                if id.value.starts_with('@') {
                    // TODO: figure out if ScalarVariables should be insensitive.
                    let var_names = vec![id.value];
                    let ty = self
                        .schema_provider
                        .get_variable_type(&var_names)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "variable {:?} has no type information",
                                var_names
                            ))
                        })?;
                    Ok(Expr::ScalarVariable(ty, var_names))
                } else {
                    // Don't use `col()` here because it will try to
                    // interpret names with '.' as if they were
                    // compound indenfiers, but this is not a compound
                    // identifier. (e.g. it is "foo.bar" not foo.bar)
                    Ok(Expr::Column(Column {
                        relation: None,
                        name: normalize_ident(id),
                    }))
                }
            }

            SQLExpr::MapAccess { ref column, keys } => {
                if let SQLExpr::Identifier(ref id) = column.as_ref() {
                    plan_indexed(col(&id.value), keys)
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "map access requires an identifier, found column {} instead",
                        column
                    )))
                }
            }

            SQLExpr::CompoundIdentifier(ids) => {
                let mut var_names: Vec<_> = ids.into_iter().map(normalize_ident).collect();

                if &var_names[0][0..1] == "@" {
                    let ty = self
                        .schema_provider
                        .get_variable_type(&var_names)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "variable {:?} has no type information",
                                var_names
                            ))
                        })?;
                    Ok(Expr::ScalarVariable(ty, var_names))
                } else {
                    match (var_names.pop(), var_names.pop()) {
                        (Some(name), Some(relation)) if var_names.is_empty() => {
                            // table.column identifier
                            Ok(Expr::Column(Column {
                                relation: Some(relation),
                                name,
                            }))
                        }
                        _ => Err(DataFusionError::NotImplemented(format!(
                            "Unsupported compound identifier '{:?}'",
                            var_names,
                        ))),
                    }
                }
            }

            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let expr = if let Some(e) = operand {
                    Some(Box::new(self.sql_expr_to_logical_expr(*e, schema)?))
                } else {
                    None
                };
                let when_expr = conditions
                    .into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                let then_expr = results
                    .into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                let else_expr = if let Some(e) = else_result {
                    Some(Box::new(self.sql_expr_to_logical_expr(*e, schema)?))
                } else {
                    None
                };

                Ok(Expr::Case {
                    expr,
                    when_then_expr: when_expr
                        .iter()
                        .zip(then_expr.iter())
                        .map(|(w, t)| (Box::new(w.to_owned()), Box::new(t.to_owned())))
                        .collect(),
                    else_expr,
                })
            }

            SQLExpr::Cast {
                expr,
                data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                data_type: convert_data_type(&data_type)?,
            }),

            SQLExpr::TryCast {
                expr,
                data_type,
            } => Ok(Expr::TryCast {
                expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                data_type: convert_data_type(&data_type)?,
            }),

            SQLExpr::TypedString {
                ref data_type,
                ref value,
            } => Ok(Expr::Cast {
                expr: Box::new(lit(&**value)),
                data_type: convert_data_type(data_type)?,
            }),

            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),

            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema)?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => Ok(Expr::BinaryExpr {
                left: Box::new(self.sql_expr_to_logical_expr(*left, schema)?),
                op: Operator::IsDistinctFrom,
                right: Box::new(self.sql_expr_to_logical_expr(*right, schema)?),
            }),

            SQLExpr::IsNotDistinctFrom(left, right) => Ok(Expr::BinaryExpr {
                left: Box::new(self.sql_expr_to_logical_expr(*left, schema)?),
                op: Operator::IsNotDistinctFrom,
                right: Box::new(self.sql_expr_to_logical_expr(*right, schema)?),
            }),

            SQLExpr::UnaryOp { op, expr } => {
                self.parse_sql_unary_op(op, *expr, schema)
            }

            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                negated,
                low: Box::new(self.sql_expr_to_logical_expr(*low, schema)?),
                high: Box::new(self.sql_expr_to_logical_expr(*high, schema)?),
            }),

            SQLExpr::InList {
                expr,
                list,
                negated,
            } => {
                let list_expr = list
                    .into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Expr::InList {
                    expr: Box::new(self.sql_expr_to_logical_expr(*expr, schema)?),
                    list: list_expr,
                    negated,
                })
            }

            SQLExpr::BinaryOp {
                left,
                op,
                right,
            } => self.parse_sql_binary_op(*left, op, *right, schema),

            #[cfg(feature = "unicode_expressions")]
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                let args = match (substring_from, substring_for) {
                    (Some(from_expr), Some(for_expr)) => {
                        let arg = self.sql_expr_to_logical_expr(*expr, schema)?;
                        let from_logic =
                            self.sql_expr_to_logical_expr(*from_expr, schema)?;
                        let for_logic =
                            self.sql_expr_to_logical_expr(*for_expr, schema)?;
                        vec![arg, from_logic, for_logic]
                    }
                    (Some(from_expr), None) => {
                        let arg = self.sql_expr_to_logical_expr(*expr, schema)?;
                        let from_logic =
                            self.sql_expr_to_logical_expr(*from_expr, schema)?;
                        vec![arg, from_logic]
                    }
                    (None, Some(for_expr)) => {
                        let arg = self.sql_expr_to_logical_expr(*expr, schema)?;
                        let from_logic = Expr::Literal(ScalarValue::Int64(Some(1)));
                        let for_logic =
                            self.sql_expr_to_logical_expr(*for_expr, schema)?;
                        vec![arg, from_logic, for_logic]
                    }
                    (None, None) => {
                        let orig_sql = SQLExpr::Substring {
                            expr,
                            substring_from: None,
                            substring_for: None,
                        };

                        return Err(DataFusionError::Plan(format!(
                            "Substring without for/from is not valid {:?}",
                            orig_sql
                        )));
                    }
                };


                Ok(Expr::ScalarFunction {
                    fun: functions::BuiltinScalarFunction::Substr,
                    args,
                })
            }

            #[cfg(not(feature = "unicode_expressions"))]
            SQLExpr::Substring {
                ..
            } => {
                Err(DataFusionError::Internal(
                    "statement substring requires compilation with feature flag: unicode_expressions.".to_string()
                ))
            }

            SQLExpr::Trim { expr, trim_where } => {
                let (fun, where_expr) = match trim_where {
                    Some((TrimWhereField::Leading, expr)) => {
                        (functions::BuiltinScalarFunction::Ltrim, Some(expr))
                    }
                    Some((TrimWhereField::Trailing, expr)) => {
                        (functions::BuiltinScalarFunction::Rtrim, Some(expr))
                    }
                    Some((TrimWhereField::Both, expr)) => {
                        (functions::BuiltinScalarFunction::Btrim, Some(expr))
                    }
                    None => (functions::BuiltinScalarFunction::Trim, None),
                };
                let arg = self.sql_expr_to_logical_expr(*expr, schema)?;
                let args = match where_expr {
                    Some(to_trim) => {
                        let to_trim = self.sql_expr_to_logical_expr(*to_trim, schema)?;
                        vec![arg, to_trim]
                    }
                    None => vec![arg],
                };
                Ok(Expr::ScalarFunction { fun, args })
            }

            SQLExpr::Function(mut function) => {
                let name = if function.name.0.len() > 1 {
                    // DF doesn't handle compound identifiers
                    // (e.g. "foo.bar") for function names yet
                    function.name.to_string()
                } else {
                    normalize_ident(function.name.0[0].clone())
                };

                // first, scalar built-in
                if let Ok(fun) = functions::BuiltinScalarFunction::from_str(&name) {
                    let args = self.function_args_to_expr(function.args, schema)?;

                    return Ok(Expr::ScalarFunction { fun, args });
                };

                // then, window function
                if let Some(window) = function.over.take() {
                    let partition_by = window
                        .partition_by
                        .into_iter()
                        .map(|e| self.sql_expr_to_logical_expr(e, schema))
                        .collect::<Result<Vec<_>>>()?;
                    let order_by = window
                        .order_by
                        .into_iter()
                        .map(|e| self.order_by_to_sort_expr(e, schema))
                        .collect::<Result<Vec<_>>>()?;
                    let window_frame = window
                        .window_frame
                        .as_ref()
                        .map(|window_frame| {
                            let window_frame: WindowFrame = window_frame.clone().try_into()?;
                            if WindowFrameUnits::Range == window_frame.units
                                && order_by.len() != 1
                            {
                                Err(DataFusionError::Plan(format!(
                                    "With window frame of type RANGE, the order by expression must be of length 1, got {}", order_by.len())))
                            } else {
                                Ok(window_frame)
                            }
                        })
                        .transpose()?;
                    let fun = window_functions::WindowFunction::from_str(&name)?;
                    match fun {
                        window_functions::WindowFunction::AggregateFunction(
                            aggregate_fun,
                        ) => {
                            return Ok(Expr::WindowFunction {
                                fun: window_functions::WindowFunction::AggregateFunction(
                                    aggregate_fun.clone(),
                                ),
                                args: self.aggregate_fn_to_expr(
                                    aggregate_fun,
                                    function,
                                    schema,
                                )?,
                                partition_by,
                                order_by,
                                window_frame,
                            });
                        }
                        window_functions::WindowFunction::BuiltInWindowFunction(
                            window_fun,
                        ) => {
                            return Ok(Expr::WindowFunction {
                                fun: window_functions::WindowFunction::BuiltInWindowFunction(
                                    window_fun,
                                ),
                                args: self.function_args_to_expr(function.args, schema)?,
                                partition_by,
                                order_by,
                                window_frame,
                            });
                        }
                    }
                }

                // next, aggregate built-ins
                if let Ok(fun) = aggregates::AggregateFunction::from_str(&name) {
                    let distinct = function.distinct;
                    let args = self.aggregate_fn_to_expr(fun.clone(), function, schema)?;
                    return Ok(Expr::AggregateFunction {
                        fun,
                        distinct,
                        args,
                    });
                };

                // finally, user-defined functions (UDF) and UDAF
                match self.schema_provider.get_function_meta(&name) {
                    Some(fm) => {
                        let args = self.function_args_to_expr(function.args, schema)?;

                        Ok(Expr::ScalarUDF { fun: fm, args })
                    }
                    None => match self.schema_provider.get_aggregate_meta(&name) {
                        Some(fm) => {
                            let args = self.function_args_to_expr(function.args, schema)?;
                            Ok(Expr::AggregateUDF { fun: fm, args })
                        }
                        _ => Err(DataFusionError::Plan(format!(
                            "Invalid function '{}'",
                            name
                        ))),
                    },
                }
            }

            SQLExpr::Nested(e) => self.sql_expr_to_logical_expr(*e, schema),

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node {:?} in sqltorel",
                sql
            ))),
        }
    }

    fn function_args_to_expr(
        &self,
        args: Vec<FunctionArg>,
        schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        args.into_iter()
            .map(|a| self.sql_fn_arg_to_logical_expr(a, schema))
            .collect::<Result<Vec<Expr>>>()
    }

    fn aggregate_fn_to_expr(
        &self,
        fun: aggregates::AggregateFunction,
        function: sqlparser::ast::Function,
        schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        if fun == aggregates::AggregateFunction::Count {
            function
                .args
                .into_iter()
                .map(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(SQLExpr::Value(
                        Value::Number(_, _),
                    ))) => Ok(lit(1_u8)),
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(lit(1_u8)),
                    _ => self.sql_fn_arg_to_logical_expr(a, schema),
                })
                .collect::<Result<Vec<Expr>>>()
        } else {
            self.function_args_to_expr(function.args, schema)
        }
    }

    fn sql_interval_to_literal(
        &self,
        value: String,
        leading_field: Option<DateTimeField>,
        leading_precision: Option<u64>,
        last_field: Option<DateTimeField>,
        fractional_seconds_precision: Option<u64>,
    ) -> Result<Expr> {
        if leading_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with leading_precision {:?}",
                leading_precision
            )));
        }

        if last_field.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with last_field {:?}",
                last_field
            )));
        }

        if fractional_seconds_precision.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                fractional_seconds_precision
            )));
        }

        const SECONDS_PER_HOUR: f32 = 3_600_f32;
        const MILLIS_PER_SECOND: f32 = 1_000_f32;

        // We are storing parts as integers, it's why we need to align parts fractional
        // INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
        // INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
        let align_interval_parts = |month_part: f32,
                                    mut day_part: f32,
                                    mut milles_part: f32|
         -> (i32, i32, f32) {
            // Convert fractional month to days, It's not supported by Arrow types, but anyway
            day_part += (month_part - (month_part as i32) as f32) * 30_f32;

            // Convert fractional days to hours
            milles_part += (day_part - ((day_part as i32) as f32))
                * 24_f32
                * SECONDS_PER_HOUR
                * MILLIS_PER_SECOND;

            (month_part as i32, day_part as i32, milles_part)
        };

        let calculate_from_part = |interval_period_str: &str,
                                   interval_type: &str|
         -> Result<(i32, i32, f32)> {
            // @todo It's better to use Decimal in order to protect rounding errors
            // Wait https://github.com/apache/arrow/pull/9232
            let interval_period = match f32::from_str(interval_period_str) {
                Ok(n) => n,
                Err(_) => {
                    return Err(DataFusionError::SQL(ParserError(format!(
                        "Unsupported Interval Expression with value {:?}",
                        value
                    ))));
                }
            };

            if interval_period > (i32::MAX as f32) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            match interval_type.to_lowercase().as_str() {
                "year" => Ok(align_interval_parts(interval_period * 12_f32, 0.0, 0.0)),
                "month" => Ok(align_interval_parts(interval_period, 0.0, 0.0)),
                "day" | "days" => Ok(align_interval_parts(0.0, interval_period, 0.0)),
                "hour" | "hours" => {
                    Ok((0, 0, interval_period * SECONDS_PER_HOUR * MILLIS_PER_SECOND))
                }
                "minutes" | "minute" => {
                    Ok((0, 0, interval_period * 60_f32 * MILLIS_PER_SECOND))
                }
                "seconds" | "second" => Ok((0, 0, interval_period * MILLIS_PER_SECOND)),
                "milliseconds" | "millisecond" => Ok((0, 0, interval_period)),
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Invalid input syntax for type interval: {:?}",
                    value
                ))),
            }
        };

        let mut result_month: i64 = 0;
        let mut result_days: i64 = 0;
        let mut result_millis: i64 = 0;

        let mut parts = value.split_whitespace();

        loop {
            let interval_period_str = parts.next();
            if interval_period_str.is_none() {
                break;
            }

            let leading_field = leading_field
                .as_ref()
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "second".to_string());

            let unit = parts
                .next()
                .map(|part| part.to_string())
                .unwrap_or(leading_field);

            let (diff_month, diff_days, diff_millis) =
                calculate_from_part(interval_period_str.unwrap(), &unit)?;

            result_month += diff_month as i64;

            if result_month > (i32::MAX as i64) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            result_days += diff_days as i64;

            if result_days > (i32::MAX as i64) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            result_millis += diff_millis as i64;

            if result_millis > (i32::MAX as i64) {
                return Err(DataFusionError::NotImplemented(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }
        }

        // Interval is tricky thing
        // 1 day is not 24 hours because timezones, 1 year != 365/364! 30 days != 1 month
        // The true way to store and calculate intervals is to store it as it defined
        // It's why we there are 3 different interval types in Arrow
        if result_month != 0 && (result_days != 0 || result_millis != 0) {
            let result: i128 = ((result_month as i128) << 96)
                | ((result_days as i128) << 64)
                // IntervalMonthDayNano uses nanos, but IntervalDayTime uses milles
                | ((result_millis * 1_000_000_i64) as i128);

            return Ok(Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(
                result,
            ))));
        }

        // Month interval
        if result_month != 0 {
            return Ok(Expr::Literal(ScalarValue::IntervalYearMonth(Some(
                result_month as i32,
            ))));
        }

        let result: i64 = (result_days << 32) | result_millis;
        Ok(Expr::Literal(ScalarValue::IntervalDayTime(Some(result))))
    }

    fn show_variable_to_plan(&self, variable: &[Ident]) -> Result<LogicalPlan> {
        // Special case SHOW TABLES
        let variable = ObjectName(variable.to_vec()).to_string();
        if variable.as_str().eq_ignore_ascii_case("tables") {
            if self.has_table("information_schema", "tables") {
                let mut rewrite =
                    DFParser::parse_sql("SELECT * FROM information_schema.tables;")?;
                assert_eq!(rewrite.len(), 1);
                self.statement_to_plan(rewrite.pop_front().unwrap())
            } else {
                Err(DataFusionError::Plan(
                    "SHOW TABLES is not supported unless information_schema is enabled"
                        .to_string(),
                ))
            }
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "SHOW {} not implemented. Supported syntax: SHOW <TABLES>",
                variable
            )))
        }
    }

    fn show_columns_to_plan(
        &self,
        extended: bool,
        full: bool,
        table_name: &ObjectName,
        filter: Option<&ShowStatementFilter>,
    ) -> Result<LogicalPlan> {
        if filter.is_some() {
            return Err(DataFusionError::Plan(
                "SHOW COLUMNS with WHERE or LIKE is not supported".to_string(),
            ));
        }

        if !self.has_table("information_schema", "columns") {
            return Err(DataFusionError::Plan(
                "SHOW COLUMNS is not supported unless information_schema is enabled"
                    .to_string(),
            ));
        }

        if self
            .schema_provider
            .get_table_provider(table_name.try_into()?)
            .is_none()
        {
            return Err(DataFusionError::Plan(format!(
                "Unknown relation for SHOW COLUMNS: {}",
                table_name
            )));
        }

        // Figure out the where clause
        let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
        let where_clause = table_name
            .0
            .iter()
            .rev()
            .zip(columns)
            .map(|(ident, column_name)| format!(r#"{} = '{}'"#, column_name, ident))
            .collect::<Vec<_>>()
            .join(" AND ");

        // treat both FULL and EXTENDED as the same
        let select_list = if full || extended {
            "*"
        } else {
            "table_catalog, table_schema, table_name, column_name, data_type, is_nullable"
        };

        let query = format!(
            "SELECT {} FROM information_schema.columns WHERE {}",
            select_list, where_clause
        );

        let mut rewrite = DFParser::parse_sql(&query)?;
        assert_eq!(rewrite.len(), 1);
        self.statement_to_plan(rewrite.pop_front().unwrap())
    }

    /// Return true if there is a table provider available for "schema.table"
    fn has_table(&self, schema: &str, table: &str) -> bool {
        let tables_reference = TableReference::Partial { schema, table };
        self.schema_provider
            .get_table_provider(tables_reference)
            .is_some()
    }
}

/// Remove join expressions from a filter expression
fn remove_join_expressions(
    expr: &Expr,
    join_columns: &HashSet<(Column, Column)>,
) -> Result<Option<Expr>> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    if join_columns.contains(&(l.clone(), r.clone()))
                        || join_columns.contains(&(r.clone(), l.clone()))
                    {
                        Ok(None)
                    } else {
                        Ok(Some(expr.clone()))
                    }
                }
                _ => Ok(Some(expr.clone())),
            },
            Operator::And => {
                let l = remove_join_expressions(left, join_columns)?;
                let r = remove_join_expressions(right, join_columns)?;
                match (l, r) {
                    (Some(ll), Some(rr)) => Ok(Some(and(ll, rr))),
                    (Some(ll), _) => Ok(Some(ll)),
                    (_, Some(rr)) => Ok(Some(rr)),
                    _ => Ok(None),
                }
            }
            _ => Ok(Some(expr.clone())),
        },
        _ => Ok(Some(expr.clone())),
    }
}

/// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
/// Filters matching this pattern are added to `accum`
/// Filters that don't match this pattern are added to `accum_filter`
/// Examples:
/// ```text
/// foo = bar => accum=[(foo, bar)] accum_filter=[]
/// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
/// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
/// ```
fn extract_join_keys(
    expr: Expr,
    accum: &mut Vec<(Column, Column)>,
    accum_filter: &mut Vec<Expr>,
) {
    match &expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    accum.push((l.clone(), r.clone()));
                }
                _other => {
                    accum_filter.push(expr);
                }
            },
            Operator::And => {
                if let Expr::BinaryExpr { left, op: _, right } = expr {
                    extract_join_keys(*left, accum, accum_filter);
                    extract_join_keys(*right, accum, accum_filter);
                }
            }
            _other
                if matches!(**left, Expr::Column(_))
                    || matches!(**right, Expr::Column(_)) =>
            {
                accum_filter.push(expr);
            }
            _other => {
                if let Expr::BinaryExpr { left, op: _, right } = expr {
                    extract_join_keys(*left, accum, accum_filter);
                    extract_join_keys(*right, accum, accum_filter);
                }
            }
        },
        _other => {
            accum_filter.push(expr);
        }
    }
}

/// Extract join keys from a WHERE clause
fn extract_possible_join_keys(
    expr: &Expr,
    accum: &mut Vec<(Column, Column)>,
) -> Result<()> {
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            Operator::Eq => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(l), Expr::Column(r)) => {
                    accum.push((l.clone(), r.clone()));
                    Ok(())
                }
                _ => Ok(()),
            },
            Operator::And => {
                extract_possible_join_keys(left, accum)?;
                extract_possible_join_keys(right, accum)
            }
            _ => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql_type: &SQLDataType) -> Result<DataType> {
    match sql_type {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) => Ok(DataType::Utf8),
        SQLDataType::Timestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        SQLDataType::Date => Ok(DataType::Date32),
        SQLDataType::Decimal(precision, scale) => make_decimal_type(*precision, *scale),
        other => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

// Parse number in sql string, convert to Expr::Literal
fn parse_sql_number(n: &str) -> Result<Expr> {
    match n.parse::<i64>() {
        Ok(n) => Ok(lit(n)),
        Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::empty::EmptyTable;
    use crate::physical_plan::functions::Volatility;
    use crate::{logical_plan::create_udf, sql::parser::DFParser};
    use datafusion_expr::ScalarFunctionImplementation;

    use super::*;

    #[test]
    fn select_no_relation() {
        quick_test(
            "SELECT 1",
            "Projection: Int64(1)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn test_real_f32() {
        quick_test(
            "SELECT CAST(1.1 AS REAL)",
            "Projection: CAST(Float64(1.1) AS Float32)\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_column_does_not_exist() {
        let sql = "SELECT doesnotexist FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert!(matches!(
            err,
            DataFusionError::Plan(msg) if msg.contains("Invalid identifier '#doesnotexist' for schema "),
        ));
    }

    #[test]
    fn select_repeated_column() {
        let sql = "SELECT age, age FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"#person.age\" at position 0 and \"#person.age\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_wildcard_with_repeated_column() {
        let sql = "SELECT *, age FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"#person.age\" at position 3 and \"#person.age\" at position 8 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_wildcard_with_repeated_column_but_is_aliased() {
        quick_test(
            "SELECT *, first_name AS fn from person",
            "Projection: #person.id, #person.first_name, #person.last_name, #person.age, #person.state, #person.salary, #person.birth_date, #person.😀, #person.first_name AS fn\
            \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_scalar_func_with_literal_no_relation() {
        quick_test(
            "SELECT sqrt(9)",
            "Projection: sqrt(Int64(9))\
             \n  EmptyRelation",
        );
    }

    #[test]
    fn select_simple_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
        let expected = "Projection: #person.id, #person.first_name, #person.last_name\
                        \n  Filter: #person.state = Utf8(\"CO\")\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_filter_column_does_not_exist() {
        let sql = "SELECT first_name FROM person WHERE doesnotexist = 'A'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert!(matches!(
            err,
            DataFusionError::Plan(msg) if msg.contains("Invalid identifier '#doesnotexist' for schema "),
        ));
    }

    #[test]
    fn select_filter_cannot_use_alias() {
        let sql = "SELECT first_name AS x FROM person WHERE x = 'A'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert!(matches!(
            err,
            DataFusionError::Plan(msg) if msg.contains("Invalid identifier '#x' for schema "),
        ));
    }

    #[test]
    fn select_neg_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
        let expected = "Projection: #person.id, #person.first_name, #person.last_name\
                        \n  Filter: NOT #person.state\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_compound_filter() {
        let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
        let expected = "Projection: #person.id, #person.first_name, #person.last_name\
            \n  Filter: #person.state = Utf8(\"CO\") AND #person.age >= Int64(21) AND #person.age <= Int64(65)\
            \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn test_timestamp_filter() {
        let sql =
            "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

        let expected = "Projection: #person.state\
            \n  Filter: #person.birth_date < CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn test_date_filter() {
        let sql =
            "SELECT state FROM person WHERE birth_date < CAST ('2020-01-01' as date)";

        let expected = "Projection: #person.state\
            \n  Filter: #person.birth_date < CAST(Utf8(\"2020-01-01\") AS Date32)\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_all_boolean_operators() {
        let sql = "SELECT age, first_name, last_name \
                   FROM person \
                   WHERE age = 21 \
                   AND age != 21 \
                   AND age > 21 \
                   AND age >= 21 \
                   AND age < 65 \
                   AND age <= 65";
        let expected = "Projection: #person.age, #person.first_name, #person.last_name\
                        \n  Filter: #person.age = Int64(21) \
                        AND #person.age != Int64(21) \
                        AND #person.age > Int64(21) \
                        AND #person.age >= Int64(21) \
                        AND #person.age < Int64(65) \
                        AND #person.age <= Int64(65)\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_between() {
        let sql = "SELECT state FROM person WHERE age BETWEEN 21 AND 65";
        let expected = "Projection: #person.state\
            \n  Filter: #person.age BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_between_negated() {
        let sql = "SELECT state FROM person WHERE age NOT BETWEEN 21 AND 65";
        let expected = "Projection: #person.state\
            \n  Filter: #person.age NOT BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_nested() {
        let sql = "SELECT fn2, last_name
                   FROM (
                     SELECT fn1 as fn2, last_name, birth_date
                     FROM (
                       SELECT first_name AS fn1, last_name, birth_date, age
                       FROM person
                     ) AS a
                   ) AS b";
        let expected = "Projection: #b.fn2, #b.last_name\
                        \n  Projection: #b.fn2, #b.last_name, #b.birth_date, alias=b\
                        \n    Projection: #a.fn1 AS fn2, #a.last_name, #a.birth_date, alias=b\
                        \n      Projection: #a.fn1, #a.last_name, #a.birth_date, #a.age, alias=a\
                        \n        Projection: #person.first_name AS fn1, #person.last_name, #person.birth_date, #person.age, alias=a\
                        \n          TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_nested_with_filters() {
        let sql = "SELECT fn1, age
                   FROM (
                     SELECT first_name AS fn1, age
                     FROM person
                     WHERE age > 20
                   ) AS a
                   WHERE fn1 = 'X' AND age < 30";

        let expected = "Projection: #a.fn1, #a.age\
                        \n  Filter: #a.fn1 = Utf8(\"X\") AND #a.age < Int64(30)\
                        \n    Projection: #a.fn1, #a.age, alias=a\
                        \n      Projection: #person.first_name AS fn1, #person.age, alias=a\
                        \n        Filter: #person.age > Int64(20)\
                        \n          TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn table_with_column_alias() {
        let sql = "SELECT a, b, c
                   FROM lineitem l (a, b, c)";
        let expected = "Projection: #l.a, #l.b, #l.c\
                        \n  Projection: #l.l_item_id AS a, #l.l_description AS b, #l.price AS c, alias=l\
                        \n    TableScan: l projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn table_with_column_alias_number_cols() {
        let sql = "SELECT a, b, c
                   FROM lineitem l (a, b)";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Source table contains 3 columns but only 2 names given as column alias\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having() {
        let sql = "SELECT id, age
                   FROM person
                   HAVING age > 100 AND age < 200";
        let expected = "Projection: #person.id, #person.age\
                        \n  Filter: #person.age > Int64(100) AND #person.age < Int64(200)\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_with_having_referencing_column_not_in_select() {
        let sql = "SELECT id, age
                   FROM person
                   HAVING first_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references column(s) not provided by the select\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having_referencing_column_nested_in_select_expression() {
        let sql = "SELECT id, age + 1
                   FROM person
                   HAVING age > 100";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references column(s) not provided by the select\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_with_having_with_aggregate_not_in_select() {
        let sql = "SELECT first_name
                   FROM person
                   HAVING MAX(age) > 100";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_with_having_that_reuses_aggregate() {
        let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(age) < 30";
        let expected = "Projection: #MAX(person.age)\
                        \n  Filter: #MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_having_with_aggregate_not_in_select() {
        let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(first_name) > 'M'";
        let expected = "Projection: #MAX(person.age)\
                        \n  Filter: #MAX(person.first_name) > Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#person.age), MAX(#person.first_name)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_having_referencing_column_not_in_select() {
        let sql = "SELECT COUNT(*)
                   FROM person
                   HAVING first_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_aliased_with_having_referencing_aggregate_by_its_alias() {
        let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING max_age < 30";
        // FIXME: add test for having in execution
        let expected = "Projection: #MAX(person.age) AS max_age\
                        \n  Filter: #MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_aliased_with_having_that_reuses_aggregate_but_not_by_its_alias() {
        let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING MAX(age) < 30";
        let expected = "Projection: #MAX(person.age) AS max_age\
                        \n  Filter: #MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING first_name = 'M'";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #person.first_name = Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_and_where() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) < Int64(100)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      Filter: #person.id > Int64(5)\
                        \n        TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_and_where_filtering_on_aggregate_column(
    ) {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5 AND age > 18
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) < Int64(100)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      Filter: #person.id > Int64(5) AND #person.age > Int64(18)\
                        \n        TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_column_by_alias() {
        let sql = "SELECT first_name AS fn, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND fn = 'M'";
        let expected = "Projection: #person.first_name AS fn, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) > Int64(2) AND #person.first_name = Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_columns_with_and_without_their_aliases(
    ) {
        let sql = "SELECT first_name AS fn, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND max_age < 5 AND first_name = 'M' AND fn = 'N'";
        let expected = "Projection: #person.first_name AS fn, #MAX(person.age) AS max_age\
                        \n  Filter: #MAX(person.age) > Int64(2) AND #MAX(person.age) < Int64(5) AND #person.first_name = Utf8(\"M\") AND #person.first_name = Utf8(\"N\")\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_that_reuses_aggregate() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) > Int64(100)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_referencing_column_not_in_group_by() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 10 AND last_name = 'M'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Having references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_that_reuses_aggregate_multiple_times() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MAX(age) < 200";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) > Int64(100) AND #MAX(person.age) < Int64(200)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_aggreagate_not_in_select() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id) < 50";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) > Int64(100) AND #MIN(person.id) < Int64(50)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age), MIN(#person.id)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_aliased_with_group_by_with_having_referencing_aggregate_by_its_alias(
    ) {
        let sql = "SELECT first_name, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING max_age > 100";
        let expected = "Projection: #person.first_name, #MAX(person.age) AS max_age\
                        \n  Filter: #MAX(person.age) > Int64(100)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_compound_aliased_with_group_by_with_having_referencing_compound_aggregate_by_its_alias(
    ) {
        let sql = "SELECT first_name, MAX(age) + 1 AS max_age_plus_one
                   FROM person
                   GROUP BY first_name
                   HAVING max_age_plus_one > 100";
        let expected =
            "Projection: #person.first_name, #MAX(person.age) + Int64(1) AS max_age_plus_one\
                        \n  Filter: #MAX(person.age) + Int64(1) > Int64(100)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age)]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_derived_column_aggreagate_not_in_select(
    ) {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id - 2) < 50";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) > Int64(100) AND #MIN(person.id - Int64(2)) < Int64(50)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age), MIN(#person.id - Int64(2))]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aggregate_with_group_by_with_having_using_count_star_not_in_select() {
        let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND COUNT(*) < 50";
        let expected = "Projection: #person.first_name, #MAX(person.age)\
                        \n  Filter: #MAX(person.age) > Int64(100) AND #COUNT(UInt8(1)) < Int64(50)\
                        \n    Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.age), COUNT(UInt8(1))]]\
                        \n      TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_binary_expr() {
        let sql = "SELECT age + salary from person";
        let expected = "Projection: #person.age + #person.salary\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_binary_expr_nested() {
        let sql = "SELECT (age + salary)/2 from person";
        let expected = "Projection: #person.age + #person.salary / Int64(2)\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_wildcard_with_groupby() {
        quick_test(
            r#"SELECT * FROM person GROUP BY id, first_name, last_name, age, state, salary, birth_date, "😀""#,
            "Projection: #person.id, #person.first_name, #person.last_name, #person.age, #person.state, #person.salary, #person.birth_date, #person.😀\
             \n  Aggregate: groupBy=[[#person.id, #person.first_name, #person.last_name, #person.age, #person.state, #person.salary, #person.birth_date, #person.😀]], aggr=[[]]\
             \n    TableScan: person projection=None",
        );
        quick_test(
            "SELECT * FROM (SELECT first_name, last_name FROM person) AS a GROUP BY first_name, last_name",
            "Projection: #a.first_name, #a.last_name\
             \n  Aggregate: groupBy=[[#a.first_name, #a.last_name]], aggr=[[]]\
             \n    Projection: #a.first_name, #a.last_name, alias=a\
             \n      Projection: #person.first_name, #person.last_name, alias=a\
             \n        TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate() {
        quick_test(
            "SELECT MIN(age) FROM person",
            "Projection: #MIN(person.age)\
            \n  Aggregate: groupBy=[[]], aggr=[[MIN(#person.age)]]\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn test_sum_aggregate() {
        quick_test(
            "SELECT SUM(age) from person",
            "Projection: #SUM(person.age)\
            \n  Aggregate: groupBy=[[]], aggr=[[SUM(#person.age)]]\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_column_does_not_exist() {
        let sql = "SELECT MIN(doesnotexist) FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert!(matches!(
            err,
            DataFusionError::Plan(msg) if msg.contains("Invalid identifier '#doesnotexist' for schema "),
        ));
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate() {
        let sql = "SELECT MIN(age), MIN(age) FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"MIN(#person.age)\" at position 0 and \"MIN(#person.age)\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_single_alias() {
        quick_test(
            "SELECT MIN(age), MIN(age) AS a FROM person",
            "Projection: #MIN(person.age), #MIN(person.age) AS a\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(#person.age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_unique_aliases() {
        quick_test(
            "SELECT MIN(age) AS a, MIN(age) AS b FROM person",
            "Projection: #MIN(person.age) AS a, #MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(#person.age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_repeated_aggregate_with_repeated_aliases() {
        let sql = "SELECT MIN(age) AS a, MIN(age) AS a FROM person";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"MIN(#person.age) AS a\" at position 0 and \"MIN(#person.age) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby() {
        quick_test(
            "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
            "Projection: #person.state, #MIN(person.age), #MAX(person.age)\
            \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age), MAX(#person.age)]]\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_with_aliases() {
        quick_test(
            "SELECT state AS a, MIN(age) AS b FROM person GROUP BY state",
            "Projection: #person.state AS a, #MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_with_aliases_repeated() {
        let sql = "SELECT state AS a, MIN(age) AS a FROM person GROUP BY state";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"#person.state AS a\" at position 0 and \"MIN(#person.age) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_column_unselected() {
        quick_test(
            "SELECT MIN(age), MAX(age) FROM person GROUP BY state",
            "Projection: #MIN(person.age), #MAX(person.age)\
             \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age), MAX(#person.age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_in_group_by_does_not_exist() {
        let sql = "SELECT SUM(age) FROM person GROUP BY doesnotexist";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert!(matches!(
            err,
            DataFusionError::Plan(msg) if msg.contains("Column #doesnotexist not found in provided schemas"),
        ));
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_in_aggregate_does_not_exist() {
        let sql = "SELECT SUM(doesnotexist) FROM person GROUP BY first_name";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert!(matches!(
            err,
            DataFusionError::Plan(msg) if msg.contains("Invalid identifier '#doesnotexist' for schema "),
        ));
    }

    #[test]
    fn select_interval_out_of_range() {
        let sql = "SELECT INTERVAL '100000000000000000 day'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r#"NotImplemented("Interval field value out of range: \"100000000000000000 day\"")"#,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_and_column_is_in_aggregate_and_groupby() {
        quick_test(
            "SELECT MAX(first_name) FROM person GROUP BY first_name",
            "Projection: #MAX(person.first_name)\
             \n  Aggregate: groupBy=[[#person.first_name]], aggr=[[MAX(#person.first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_can_use_positions() {
        quick_test(
            "SELECT state, age AS b, COUNT(1) FROM person GROUP BY 1, 2",
            "Projection: #person.state, #person.age AS b, #COUNT(UInt8(1))\
             \n  Aggregate: groupBy=[[#person.state, #person.age]], aggr=[[COUNT(UInt8(1))]]\
             \n    TableScan: person projection=None",
        );
        quick_test(
            "SELECT state, age AS b, COUNT(1) FROM person GROUP BY 2, 1",
            "Projection: #person.state, #person.age AS b, #COUNT(UInt8(1))\
             \n  Aggregate: groupBy=[[#person.age, #person.state]], aggr=[[COUNT(UInt8(1))]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_position_out_of_range() {
        let sql = "SELECT state, MIN(age) FROM person GROUP BY 0";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );

        let sql2 = "SELECT state, MIN(age) FROM person GROUP BY 5";
        let err2 = logical_plan(sql2).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err2)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_can_use_alias() {
        quick_test(
            "SELECT state AS a, MIN(age) AS b FROM person GROUP BY a",
            "Projection: #person.state AS a, #MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_aggregate_repeated() {
        let sql = "SELECT state, MIN(age), MIN(age) FROM person GROUP BY state";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r##"Plan("Projections require unique expression names but the expression \"MIN(#person.age)\" at position 1 and \"MIN(#person.age)\" at position 2 have the same name. Consider aliasing (\"AS\") one of them.")"##,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_aggregate_repeated_and_one_has_alias() {
        quick_test(
            "SELECT state, MIN(age), MIN(age) AS ma FROM person GROUP BY state",
            "Projection: #person.state, #MIN(person.age), #MIN(person.age) AS ma\
             \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age)]]\
             \n    TableScan: person projection=None",
        )
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_unselected() {
        quick_test(
            "SELECT MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: #MIN(person.first_name)\
             \n  Aggregate: groupBy=[[#person.age + Int64(1)]], aggr=[[MIN(#person.first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_selected_and_resolvable(
    ) {
        quick_test(
            "SELECT age + 1, MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: #person.age + Int64(1), #MIN(person.first_name)\
             \n  Aggregate: groupBy=[[#person.age + Int64(1)]], aggr=[[MIN(#person.first_name)]]\
             \n    TableScan: person projection=None",
        );
        quick_test(
            "SELECT MIN(first_name), age + 1 FROM person GROUP BY age + 1",
            "Projection: #MIN(person.first_name), #person.age + Int64(1)\
             \n  Aggregate: groupBy=[[#person.age + Int64(1)]], aggr=[[MIN(#person.first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_resolvable()
    {
        quick_test(
            "SELECT ((age + 1) / 2) * (age + 1), MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: #person.age + Int64(1) / Int64(2) * #person.age + Int64(1), #MIN(person.first_name)\
             \n  Aggregate: groupBy=[[#person.age + Int64(1)]], aggr=[[MIN(#person.first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_not_resolvable(
    ) {
        // The query should fail, because age + 9 is not in the group by.
        let sql =
            "SELECT ((age + 1) / 2) * (age + 9), MIN(first_name) FROM person GROUP BY age + 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r#"Plan("Projection references non-aggregate values")"#,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_with_groupby_non_column_expression_and_its_column_selected(
    ) {
        let sql = "SELECT age, MIN(first_name) FROM person GROUP BY age + 1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            r#"Plan("Projection references non-aggregate values")"#,
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_simple_aggregate_nested_in_binary_expr_with_groupby() {
        quick_test(
            "SELECT state, MIN(age) < 10 FROM person GROUP BY state",
            "Projection: #person.state, #MIN(person.age) < Int64(10)\
             \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_simple_aggregate_and_nested_groupby_column() {
        quick_test(
            "SELECT age + 1, MAX(first_name) FROM person GROUP BY age",
            "Projection: #person.age + Int64(1), #MAX(person.first_name)\
             \n  Aggregate: groupBy=[[#person.age]], aggr=[[MAX(#person.first_name)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_aggregate_compounded_with_groupby_column() {
        quick_test(
            "SELECT age + MIN(salary) FROM person GROUP BY age",
            "Projection: #person.age + #MIN(person.salary)\
             \n  Aggregate: groupBy=[[#person.age]], aggr=[[MIN(#person.salary)]]\
             \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_aggregate_with_non_column_inner_expression_with_groupby() {
        quick_test(
            "SELECT state, MIN(age + 1) FROM person GROUP BY state",
            "Projection: #person.state, #MIN(person.age + Int64(1))\
            \n  Aggregate: groupBy=[[#person.state]], aggr=[[MIN(#person.age + Int64(1))]]\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn test_wildcard() {
        quick_test(
            "SELECT * from person",
            "Projection: #person.id, #person.first_name, #person.last_name, #person.age, #person.state, #person.salary, #person.birth_date, #person.😀\
            \n  TableScan: person projection=None",
        );
    }

    #[test]
    fn select_count_one() {
        let sql = "SELECT COUNT(1) FROM person";
        let expected = "Projection: #COUNT(UInt8(1))\
                        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_count_column() {
        let sql = "SELECT COUNT(id) FROM person";
        let expected = "Projection: #COUNT(person.id)\
                        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(#person.id)]]\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_scalar_func() {
        let sql = "SELECT sqrt(age) FROM person";
        let expected = "Projection: sqrt(#person.age)\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_aliased_scalar_func() {
        let sql = "SELECT sqrt(person.age) AS square_people FROM person";
        let expected = "Projection: sqrt(#person.age) AS square_people\
                        \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_nullif_division() {
        let sql = "SELECT c3/(c4+c5) \
                   FROM aggregate_test_100 WHERE c3/nullif(c4+c5, 0) > 0.1";
        let expected = "Projection: #aggregate_test_100.c3 / #aggregate_test_100.c4 + #aggregate_test_100.c5\
            \n  Filter: #aggregate_test_100.c3 / nullif(#aggregate_test_100.c4 + #aggregate_test_100.c5, Int64(0)) > Float64(0.1)\
            \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_with_negative_operator() {
        let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > -0.1 AND -c4 > 0";
        let expected = "Projection: #aggregate_test_100.c3\
            \n  Filter: #aggregate_test_100.c3 > Float64(-0.1) AND (- #aggregate_test_100.c4) > Int64(0)\
            \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_where_with_positive_operator() {
        let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > +0.1 AND +c4 > 0";
        let expected = "Projection: #aggregate_test_100.c3\
            \n  Filter: #aggregate_test_100.c3 > Float64(0.1) AND #aggregate_test_100.c4 > Int64(0)\
            \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_index() {
        let sql = "SELECT id FROM person ORDER BY 1";
        let expected = "Sort: #person.id ASC NULLS LAST\
                        \n  Projection: #person.id\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_multiple_index() {
        let sql = "SELECT id, state, age FROM person ORDER BY 1, 3";
        let expected = "Sort: #person.id ASC NULLS LAST, #person.age ASC NULLS LAST\
                        \n  Projection: #person.id, #person.state, #person.age\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_index_of_0() {
        let sql = "SELECT id FROM person ORDER BY 0";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Order by index starts at 1 for column indexes\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_order_by_index_oob() {
        let sql = "SELECT id FROM person ORDER BY 2";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Order by column out of bounds, specified: 2, max: 1\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn select_order_by() {
        let sql = "SELECT id FROM person ORDER BY id";
        let expected = "Sort: #person.id ASC NULLS LAST\
                        \n  Projection: #person.id\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_desc() {
        let sql = "SELECT id FROM person ORDER BY id DESC";
        let expected = "Sort: #person.id DESC NULLS FIRST\
                        \n  Projection: #person.id\
                        \n    TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_order_by_nulls_last() {
        quick_test(
            "SELECT id FROM person ORDER BY id DESC NULLS LAST",
            "Sort: #person.id DESC NULLS LAST\
            \n  Projection: #person.id\
            \n    TableScan: person projection=None",
        );

        quick_test(
            "SELECT id FROM person ORDER BY id NULLS LAST",
            "Sort: #person.id ASC NULLS LAST\
            \n  Projection: #person.id\
            \n    TableScan: person projection=None",
        );
    }

    #[test]
    fn select_group_by() {
        let sql = "SELECT state FROM person GROUP BY state";
        let expected = "Projection: #person.state\
                        \n  Aggregate: groupBy=[[#person.state]], aggr=[[]]\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_columns_not_in_select() {
        let sql = "SELECT MAX(age) FROM person GROUP BY state";
        let expected = "Projection: #MAX(person.age)\
                        \n  Aggregate: groupBy=[[#person.state]], aggr=[[MAX(#person.age)]]\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_count_star() {
        let sql = "SELECT state, COUNT(*) FROM person GROUP BY state";
        let expected = "Projection: #person.state, #COUNT(UInt8(1))\
                        \n  Aggregate: groupBy=[[#person.state]], aggr=[[COUNT(UInt8(1))]]\
                        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_group_by_needs_projection() {
        let sql = "SELECT COUNT(state), state FROM person GROUP BY state";
        let expected = "\
        Projection: #COUNT(person.state), #person.state\
        \n  Aggregate: groupBy=[[#person.state]], aggr=[[COUNT(#person.state)]]\
        \n    TableScan: person projection=None";

        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_1() {
        let sql = "SELECT c1, MIN(c12) FROM aggregate_test_100 GROUP BY c1, c13";
        let expected = "Projection: #aggregate_test_100.c1, #MIN(aggregate_test_100.c12)\
                       \n  Aggregate: groupBy=[[#aggregate_test_100.c1, #aggregate_test_100.c13]], aggr=[[MIN(#aggregate_test_100.c12)]]\
                       \n    TableScan: aggregate_test_100 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_7480_2() {
        let sql = "SELECT c1, c13, MIN(c12) FROM aggregate_test_100 GROUP BY c1";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Projection references non-aggregate values\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_csv() {
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let expected = "CreateExternalTable: \"t\"";
        quick_test(sql, expected);
    }

    #[test]
    fn create_external_table_csv_no_schema() {
        let sql = "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo.csv'";
        let expected = "CreateExternalTable: \"t\"";
        quick_test(sql, expected);
    }

    #[test]
    fn create_external_table_parquet() {
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS PARQUET LOCATION 'foo.parquet'";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"Column definitions can not be specified for PARQUET files.\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn create_external_table_parquet_no_schema() {
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = "CreateExternalTable: \"t\"";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_explicit_syntax() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id";
        let expected = "Projection: #person.id, #orders.order_id\
        \n  Inner Join: #person.id = #orders.customer_id\
        \n    TableScan: person projection=None\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_unsupported_expression() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id AND order_id > 1 ";
        let expected = "Projection: #person.id, #orders.order_id\
        \n  Filter: #orders.order_id > Int64(1)\
        \n    Inner Join: #person.id = #orders.customer_id\
        \n      TableScan: person projection=None\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn left_equijoin_unsupported_expression() {
        let sql = "SELECT id, order_id \
            FROM person \
            LEFT JOIN orders \
            ON id = customer_id AND order_id > 1";
        let expected = "Projection: #person.id, #orders.order_id\
        \n  Left Join: #person.id = #orders.customer_id\
        \n    TableScan: person projection=None\
        \n    Filter: #orders.order_id > Int64(1)\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn right_equijoin_unsupported_expression() {
        let sql = "SELECT id, order_id \
            FROM person \
            RIGHT JOIN orders \
            ON id = customer_id AND id > 1";
        let expected = "Projection: #person.id, #orders.order_id\
        \n  Right Join: #person.id = #orders.customer_id\
        \n    Filter: #person.id > Int64(1)\
        \n      TableScan: person projection=None\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn join_with_table_name() {
        let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON person.id = orders.customer_id";
        let expected = "Projection: #person.id, #orders.order_id\
        \n  Inner Join: #person.id = #orders.customer_id\
        \n    TableScan: person projection=None\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn join_with_using() {
        let sql = "SELECT person.first_name, id \
            FROM person \
            JOIN person as person2 \
            USING (id)";
        let expected = "Projection: #person.first_name, #person.id\
        \n  Inner Join: Using #person.id = #person2.id\
        \n    TableScan: person projection=None\
        \n    TableScan: person2 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn project_wildcard_on_join_with_using() {
        let sql = "SELECT * \
            FROM lineitem \
            JOIN lineitem as lineitem2 \
            USING (l_item_id)";
        let expected = "Projection: #lineitem.l_item_id, #lineitem.l_description, #lineitem.price, #lineitem2.l_description, #lineitem2.price\
        \n  Inner Join: Using #lineitem.l_item_id = #lineitem2.l_item_id\
        \n    TableScan: lineitem projection=None\
        \n    TableScan: lineitem2 projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn equijoin_explicit_syntax_3_tables() {
        let sql = "SELECT id, order_id, l_description \
            FROM person \
            JOIN orders ON id = customer_id \
            JOIN lineitem ON o_item_id = l_item_id";
        let expected =
            "Projection: #person.id, #orders.order_id, #lineitem.l_description\
            \n  Inner Join: #orders.o_item_id = #lineitem.l_item_id\
            \n    Inner Join: #person.id = #orders.customer_id\
            \n      TableScan: person projection=None\
            \n      TableScan: orders projection=None\
            \n    TableScan: lineitem projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn boolean_literal_in_condition_expression() {
        let sql = "SELECT order_id \
        FROM orders \
        WHERE delivered = false OR delivered = true";
        let expected = "Projection: #orders.order_id\
            \n  Filter: #orders.delivered = Boolean(false) OR #orders.delivered = Boolean(true)\
            \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn union() {
        let sql = "SELECT order_id from orders UNION ALL SELECT order_id FROM orders";
        let expected = "Union\
            \n  Projection: #orders.order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #orders.order_id\
            \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn union_4_combined_in_one() {
        let sql = "SELECT order_id from orders
                    UNION ALL SELECT order_id FROM orders
                    UNION ALL SELECT order_id FROM orders
                    UNION ALL SELECT order_id FROM orders";
        let expected = "Union\
            \n  Projection: #orders.order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #orders.order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #orders.order_id\
            \n    TableScan: orders projection=None\
            \n  Projection: #orders.order_id\
            \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn union_schemas_should_be_same() {
        let sql = "SELECT order_id from orders UNION ALL SELECT customer_id FROM orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"UNION ALL schemas are expected to be the same\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn empty_over() {
        let sql = "SELECT order_id, MAX(order_id) OVER () from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.order_id)\
        \n  WindowAggr: windowExpr=[[MAX(#orders.order_id)]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_with_alias() {
        let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid from orders";
        let expected = "\
        Projection: #orders.order_id AS oid, #MAX(orders.order_id) AS max_oid\
        \n  WindowAggr: windowExpr=[[MAX(#orders.order_id)]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_dup_with_alias() {
        let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid, MAX(order_id) OVER () max_oid_dup from orders";
        let expected = "\
        Projection: #orders.order_id AS oid, #MAX(orders.order_id) AS max_oid, #MAX(orders.order_id) AS max_oid_dup\
        \n  WindowAggr: windowExpr=[[MAX(#orders.order_id)]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_dup_with_different_sort() {
        let sql = "SELECT order_id oid, MAX(order_id) OVER (), MAX(order_id) OVER (ORDER BY order_id) from orders";
        let expected = "\
        Projection: #orders.order_id AS oid, #MAX(orders.order_id), #MAX(orders.order_id) ORDER BY [#orders.order_id ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.order_id)]]\
        \n    WindowAggr: windowExpr=[[MAX(#orders.order_id) ORDER BY [#orders.order_id ASC NULLS LAST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_plus() {
        let sql = "SELECT order_id, MAX(qty * 1.1) OVER () from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty * Float64(1.1))\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty * Float64(1.1))]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn empty_over_multiple() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (), min(qty) over (), aVg(qty) OVER () from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty), #MIN(orders.qty), #AVG(orders.qty)\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty), MIN(#orders.qty), AVG(#orders.qty)]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                               QUERY PLAN
    /// ----------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..87.33 rows=1000 width=8)
    ///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///         Sort Key: order_id
    ///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by() {
        let sql = "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) PARTITION BY [#orders.order_id]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) PARTITION BY [#orders.order_id]]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                     QUERY PLAN
    /// ----------------------------------------------------------------------------------
    /// WindowAgg  (cost=137.16..154.66 rows=1000 width=12)
    /// ->  Sort  (cost=137.16..139.66 rows=1000 width=12)
    ///         Sort Key: order_id
    ///         ->  WindowAgg  (cost=69.83..87.33 rows=1000 width=12)
    ///             ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                     Sort Key: order_id DESC
    ///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_order_by() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST], #MIN(orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST]]]\
        \n    WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn over_order_by_with_window_frame_double_end() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS BETWEEN 3 PRECEDING and 3 FOLLOWING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING, #MIN(orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn over_order_by_with_window_frame_single_end() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW, #MIN(orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn over_order_by_with_window_frame_range_value_check() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id RANGE 3 PRECEDING) from orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "NotImplemented(\"With WindowFrameUnits=RANGE, the bound cannot be 3 PRECEDING or FOLLOWING at the moment\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn over_order_by_with_window_frame_range_order_by_check() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (RANGE UNBOUNDED PRECEDING) from orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"With window frame of type RANGE, the order by expression must be of length 1, got 0\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn over_order_by_with_window_frame_range_order_by_check_2() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (ORDER BY order_id, qty RANGE UNBOUNDED PRECEDING) from orders";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Plan(\"With window frame of type RANGE, the order by expression must be of length 1, got 2\")",
            format!("{:?}", err)
        );
    }

    #[test]
    fn over_order_by_with_window_frame_single_end_groups() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id GROUPS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW, #MIN(orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id DESC NULLS FIRST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                     QUERY PLAN
    /// -----------------------------------------------------------------------------------
    /// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
    ///   ->  Sort  (cost=142.16..144.66 rows=1000 width=16)
    ///         Sort Key: order_id
    ///         ->  WindowAgg  (cost=72.33..92.33 rows=1000 width=16)
    ///               ->  Sort  (cost=72.33..74.83 rows=1000 width=12)
    ///                     Sort Key: ((order_id + 1))
    ///                     ->  Seq Scan on orders  (cost=0.00..22.50 rows=1000 width=12)
    /// ```
    #[test]
    fn over_order_by_two_sort_keys() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY (order_id + 1)) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST], #MIN(orders.qty) ORDER BY [#orders.order_id + Int64(1) ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST]]]\
        \n    WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id + Int64(1) ASC NULLS LAST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                        QUERY PLAN
    /// ----------------------------------------------------------------------------------------
    /// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
    ///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
    ///               Sort Key: qty, order_id
    ///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                           Sort Key: order_id, qty
    ///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_order_by_sort_keys_sorting() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.qty ASC NULLS LAST, #orders.order_id ASC NULLS LAST], #SUM(orders.qty), #MIN(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST, #orders.qty ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[SUM(#orders.qty)]]\
        \n    WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.qty ASC NULLS LAST, #orders.order_id ASC NULLS LAST]]]\
        \n      WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST, #orders.qty ASC NULLS LAST]]]\
        \n        TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                     QUERY PLAN
    /// ----------------------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..117.33 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=69.83..104.83 rows=1000 width=16)
    ///         ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                     Sort Key: order_id, qty
    ///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    ///
    /// FIXME: for now we are not detecting prefix of sorting keys in order to save one sort exec phase
    #[test]
    fn over_order_by_sort_keys_sorting_prefix_compacting() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST], #SUM(orders.qty), #MIN(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST, #orders.qty ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[SUM(#orders.qty)]]\
        \n    WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST]]]\
        \n      WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST, #orders.qty ASC NULLS LAST]]]\
        \n        TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                        QUERY PLAN
    /// ----------------------------------------------------------------------------------------
    /// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
    ///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
    ///               Sort Key: order_id, qty
    ///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                           Sort Key: qty, order_id
    ///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    ///
    /// FIXME: for now we are not detecting prefix of sorting keys in order to re-arrange with global
    /// sort
    #[test]
    fn over_order_by_sort_keys_sorting_global_order_compacting() {
        let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders ORDER BY order_id";
        let expected = "\
        Sort: #orders.order_id ASC NULLS LAST\
        \n  Projection: #orders.order_id, #MAX(orders.qty) ORDER BY [#orders.qty ASC NULLS LAST, #orders.order_id ASC NULLS LAST], #SUM(orders.qty), #MIN(orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST, #orders.qty ASC NULLS LAST]\
        \n    WindowAggr: windowExpr=[[SUM(#orders.qty)]]\
        \n      WindowAggr: windowExpr=[[MAX(#orders.qty) ORDER BY [#orders.qty ASC NULLS LAST, #orders.order_id ASC NULLS LAST]]]\
        \n        WindowAggr: windowExpr=[[MIN(#orders.qty) ORDER BY [#orders.order_id ASC NULLS LAST, #orders.qty ASC NULLS LAST]]]\
        \n          TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                               QUERY PLAN
    /// ----------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///         Sort Key: order_id, qty
    ///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by_order_by() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) PARTITION BY [#orders.order_id] ORDER BY [#orders.qty ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) PARTITION BY [#orders.order_id] ORDER BY [#orders.qty ASC NULLS LAST]]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                               QUERY PLAN
    /// ----------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
    ///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///         Sort Key: order_id, qty
    ///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by_order_by_no_dup() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) PARTITION BY [#orders.order_id, #orders.qty] ORDER BY [#orders.qty ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) PARTITION BY [#orders.order_id, #orders.qty] ORDER BY [#orders.qty ASC NULLS LAST]]]\
        \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                     QUERY PLAN
    /// ----------------------------------------------------------------------------------
    /// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
    ///   ->  Sort  (cost=142.16..144.66 rows=1000 width=12)
    ///         Sort Key: qty, order_id
    ///         ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=12)
    ///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
    ///                     Sort Key: order_id, qty
    ///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
    /// ```
    #[test]
    fn over_partition_by_order_by_mix_up() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty), MIN(qty) OVER (PARTITION BY qty ORDER BY order_id) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) PARTITION BY [#orders.order_id, #orders.qty] ORDER BY [#orders.qty ASC NULLS LAST], #MIN(orders.qty) PARTITION BY [#orders.qty] ORDER BY [#orders.order_id ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[MIN(#orders.qty) PARTITION BY [#orders.qty] ORDER BY [#orders.order_id ASC NULLS LAST]]]\
        \n    WindowAggr: windowExpr=[[MAX(#orders.qty) PARTITION BY [#orders.order_id, #orders.qty] ORDER BY [#orders.qty ASC NULLS LAST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    /// psql result
    /// ```
    ///                                  QUERY PLAN
    /// -----------------------------------------------------------------------------
    /// WindowAgg  (cost=69.83..109.83 rows=1000 width=24)
    ///   ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=20)
    ///         ->  Sort  (cost=69.83..72.33 rows=1000 width=16)
    ///               Sort Key: order_id, qty, price
    ///               ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=16)
    /// ```
    /// FIXME: for now we are not detecting prefix of sorting keys in order to save one sort exec phase
    #[test]
    fn over_partition_by_order_by_mix_up_prefix() {
        let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty), MIN(qty) OVER (PARTITION BY order_id, qty ORDER BY price) from orders";
        let expected = "\
        Projection: #orders.order_id, #MAX(orders.qty) PARTITION BY [#orders.order_id] ORDER BY [#orders.qty ASC NULLS LAST], #MIN(orders.qty) PARTITION BY [#orders.order_id, #orders.qty] ORDER BY [#orders.price ASC NULLS LAST]\
        \n  WindowAggr: windowExpr=[[MAX(#orders.qty) PARTITION BY [#orders.order_id] ORDER BY [#orders.qty ASC NULLS LAST]]]\
        \n    WindowAggr: windowExpr=[[MIN(#orders.qty) PARTITION BY [#orders.order_id, #orders.qty] ORDER BY [#orders.price ASC NULLS LAST]]]\
        \n      TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_typedstring() {
        let sql = "SELECT date '2020-12-10' AS date FROM person";
        let expected = "Projection: CAST(Utf8(\"2020-12-10\") AS Date32) AS date\
            \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn select_multibyte_column() {
        let sql = r#"SELECT "😀" FROM person"#;
        let expected = "Projection: #person.😀\
            \n  TableScan: person projection=None";
        quick_test(sql, expected);
    }

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let planner = SqlToRel::new(&MockContextProvider {});
        let result = DFParser::parse_sql(sql);
        let mut ast = result.unwrap();
        planner.statement_to_plan(ast.pop_front().unwrap())
    }

    /// Create logical plan, write with formatter, compare to expected output
    fn quick_test(sql: &str, expected: &str) {
        let plan = logical_plan(sql).unwrap();
        assert_eq!(format!("{:?}", plan), expected);
    }

    struct MockContextProvider {}

    impl ContextProvider for MockContextProvider {
        fn get_table_provider(
            &self,
            name: TableReference,
        ) -> Option<Arc<dyn TableProvider>> {
            let schema = match name.table() {
                "person" => Some(Schema::new(vec![
                    Field::new("id", DataType::UInt32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("salary", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                    Field::new("😀", DataType::Int32, false),
                ])),
                "orders" => Some(Schema::new(vec![
                    Field::new("order_id", DataType::UInt32, false),
                    Field::new("customer_id", DataType::UInt32, false),
                    Field::new("o_item_id", DataType::Utf8, false),
                    Field::new("qty", DataType::Int32, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new("delivered", DataType::Boolean, false),
                ])),
                "lineitem" => Some(Schema::new(vec![
                    Field::new("l_item_id", DataType::UInt32, false),
                    Field::new("l_description", DataType::Utf8, false),
                    Field::new("price", DataType::Float64, false),
                ])),
                "aggregate_test_100" => Some(Schema::new(vec![
                    Field::new("c1", DataType::Utf8, false),
                    Field::new("c2", DataType::UInt32, false),
                    Field::new("c3", DataType::Int8, false),
                    Field::new("c4", DataType::Int16, false),
                    Field::new("c5", DataType::Int32, false),
                    Field::new("c6", DataType::Int64, false),
                    Field::new("c7", DataType::UInt8, false),
                    Field::new("c8", DataType::UInt16, false),
                    Field::new("c9", DataType::UInt32, false),
                    Field::new("c10", DataType::UInt64, false),
                    Field::new("c11", DataType::Float32, false),
                    Field::new("c12", DataType::Float64, false),
                    Field::new("c13", DataType::Utf8, false),
                ])),
                _ => None,
            };
            schema.map(|s| -> Arc<dyn TableProvider> {
                Arc::new(EmptyTable::new(Arc::new(s)))
            })
        }

        fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
            let f: ScalarFunctionImplementation =
                Arc::new(|_| Err(DataFusionError::NotImplemented("".to_string())));
            match name {
                "my_sqrt" => Some(Arc::new(create_udf(
                    "my_sqrt",
                    vec![DataType::Float64],
                    Arc::new(DataType::Float64),
                    Volatility::Immutable,
                    f,
                ))),
                _ => None,
            }
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            unimplemented!()
        }

        fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
            unimplemented!()
        }
    }

    #[test]
    fn select_partially_qualified_column() {
        let sql = r#"SELECT person.first_name FROM public.person"#;
        let expected = "Projection: #public.person.first_name\
            \n  TableScan: public.person projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn cross_join_to_inner_join() {
        let sql = "select person.id from person, orders, lineitem where person.id = lineitem.l_item_id and orders.o_item_id = lineitem.l_description;";
        let expected = "Projection: #person.id\
                                 \n  Inner Join: #lineitem.l_description = #orders.o_item_id\
                                 \n    Inner Join: #person.id = #lineitem.l_item_id\
                                 \n      TableScan: person projection=None\
                                 \n      TableScan: lineitem projection=None\
                                 \n    TableScan: orders projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn cross_join_not_to_inner_join() {
        let sql = "select person.id from person, orders, lineitem where person.id = person.age;";
        let expected = "Projection: #person.id\
                                    \n  Filter: #person.id = #person.age\
                                    \n    CrossJoin:\
                                    \n      CrossJoin:\
                                    \n        TableScan: person projection=None\
                                    \n        TableScan: orders projection=None\
                                    \n      TableScan: lineitem projection=None";
        quick_test(sql, expected);
    }

    #[test]
    fn cte_use_same_name_multiple_times() {
        let sql = "with a as (select * from person), a as (select * from orders) select * from a;";
        let expected = "SQL error: ParserError(\"WITH query name \\\"a\\\" specified more than once\")";
        let result = logical_plan(sql).err().unwrap();
        assert_eq!(expected, format!("{}", result));
    }
}
