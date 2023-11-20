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

//! Logical plan types

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::dml::CopyTo;
use super::DdlStatement;
use crate::dml::CopyOptions;
use crate::expr::{Alias, Exists, InSubquery, Placeholder, Sort as SortExpr};
use crate::expr_rewriter::{create_col_from_scalar_expr, normalize_cols};
use crate::logical_plan::display::{GraphvizVisitor, IndentVisitor};
use crate::logical_plan::extension::UserDefinedLogicalNode;
use crate::logical_plan::{DmlStatement, Statement};
use crate::utils::{
    enumerate_grouping_sets, exprlist_to_fields, find_out_reference_exprs,
    grouping_set_expr_count, grouping_set_to_exprlist, inspect_expr_pre,
};
use crate::{
    build_join_schema, expr_vec_fmt, BinaryExpr, CreateMemoryTable, CreateView, Expr,
    ExprSchemable, LogicalPlanBuilder, Operator, TableProviderFilterPushDown,
    TableSource,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::tree_node::{
    RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter, TreeNodeVisitor,
    VisitRecursion,
};
use datafusion_common::{
    aggregate_functional_dependencies, internal_err, plan_err, Column, Constraints,
    DFField, DFSchema, DFSchemaRef, DataFusionError, FunctionalDependencies,
    OwnedTableReference, Result, ScalarValue, UnnestOptions,
};
// backwards compatibility
pub use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
pub use datafusion_common::{JoinConstraint, JoinType};

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner and the DataFrame API.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection(Projection),
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the
    /// input; If the value of `<predicate>` is true, the input row is
    /// passed to the output. If the value of `<predicate>` is false
    /// (or null), the row is discarded.
    Filter(Filter),
    /// Windows input based on a set of window spec and window
    /// function (e.g. SUM or RANK).  This is used to implement SQL
    /// window functions, and the `OVER` clause.
    Window(Window),
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM). This is used to implement SQL aggregates
    /// and `GROUP BY`.
    Aggregate(Aggregate),
    /// Sorts its input according to a list of sort expressions. This
    /// is used to implement SQL `ORDER BY`
    Sort(Sort),
    /// Join two logical plans on one or more join columns.
    /// This is used to implement SQL `JOIN`
    Join(Join),
    /// Apply Cross Join to two logical plans.
    /// This is used to implement SQL `CROSS JOIN`
    CrossJoin(CrossJoin),
    /// Repartitions the input based on a partitioning scheme. This is
    /// used to add parallelism and is sometimes referred to as an
    /// "exchange" operator in other systems
    Repartition(Repartition),
    /// Union multiple inputs with the same schema into a single
    /// output stream. This is used to implement SQL `UNION [ALL]` and
    /// `INTERSECT [ALL]`.
    Union(Union),
    /// Produces rows from a [`TableSource`], used to implement SQL
    /// `FROM` tables or views.
    TableScan(TableScan),
    /// Produces no rows: An empty relation with an empty schema that
    /// produces 0 or 1 row. This is used to implement SQL `SELECT`
    /// that has no values in the `FROM` clause.
    EmptyRelation(EmptyRelation),
    /// A named temporary relation with a schema.
    NamedRelation(NamedRelation),
    /// Produces the output of running another query.  This is used to
    /// implement SQL subqueries
    Subquery(Subquery),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Skip some number of rows, and then fetch some number of rows.
    Limit(Limit),
    /// A DataFusion [`Statement`] such as `SET VARIABLE` or `START TRANSACTION`
    Statement(Statement),
    /// Values expression. See
    /// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details. This is used to implement SQL such as
    /// `VALUES (1, 2), (3, 4)`
    Values(Values),
    /// Produces a relation with string representations of
    /// various parts of the plan. This is used to implement SQL `EXPLAIN`.
    Explain(Explain),
    /// Runs the input, and prints annotated physical plan as a string
    /// with execution metric. This is used to implement SQL
    /// `EXPLAIN ANALYZE`.
    Analyze(Analyze),
    /// Extension operator defined outside of DataFusion. This is used
    /// to extend DataFusion with custom relational operations that
    Extension(Extension),
    /// Remove duplicate rows from the input. This is used to
    /// implement SQL `SELECT DISTINCT ...`.
    Distinct(Distinct),
    /// Prepare a statement and find any bind parameters
    /// (e.g. `?`). This is used to implement SQL-prepared statements.
    Prepare(Prepare),
    /// Data Manipulaton Language (DML): Insert / Update / Delete
    Dml(DmlStatement),
    /// Data Definition Language (DDL): CREATE / DROP TABLES / VIEWS / SCHEMAS
    Ddl(DdlStatement),
    /// `COPY TO` for writing plan results to files
    Copy(CopyTo),
    /// Describe the schema of the table. This is used to implement the
    /// SQL `DESCRIBE` command from MySQL.
    DescribeTable(DescribeTable),
    /// Unnest a column that contains a nested list type such as an
    /// ARRAY. This is used to implement SQL `UNNEST`
    Unnest(Unnest),
    /// A variadic query (e.g. "Recursive CTEs")
    RecursiveQuery(RecursiveQuery),
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            LogicalPlan::EmptyRelation(EmptyRelation { schema, .. }) => schema,
            LogicalPlan::Values(Values { schema, .. }) => schema,
            LogicalPlan::TableScan(TableScan {
                projected_schema, ..
            }) => projected_schema,
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Distinct(Distinct::All(input)) => input.schema(),
            LogicalPlan::Distinct(Distinct::On(DistinctOn { schema, .. })) => schema,
            LogicalPlan::Window(Window { schema, .. }) => schema,
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::CrossJoin(CrossJoin { schema, .. }) => schema,
            LogicalPlan::Repartition(Repartition { input, .. }) => input.schema(),
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::Statement(statement) => statement.schema(),
            LogicalPlan::Subquery(Subquery { subquery, .. }) => subquery.schema(),
            LogicalPlan::SubqueryAlias(SubqueryAlias { schema, .. }) => schema,
            LogicalPlan::Prepare(Prepare { input, .. }) => input.schema(),
            LogicalPlan::Explain(explain) => &explain.schema,
            LogicalPlan::Analyze(analyze) => &analyze.schema,
            LogicalPlan::Extension(extension) => extension.node.schema(),
            LogicalPlan::Union(Union { schema, .. }) => schema,
            LogicalPlan::DescribeTable(DescribeTable { output_schema, .. }) => {
                output_schema
            }
            LogicalPlan::Dml(DmlStatement { table_schema, .. }) => table_schema,
            LogicalPlan::Copy(CopyTo { input, .. }) => input.schema(),
            LogicalPlan::Ddl(ddl) => ddl.schema(),
            LogicalPlan::Unnest(Unnest { schema, .. }) => schema,
            LogicalPlan::NamedRelation(NamedRelation { schema, .. }) => schema,
            LogicalPlan::RecursiveQuery(RecursiveQuery { static_term, .. }) => {
                // we take the schema of the static term as the schema of the entire recursive query
                static_term.schema()
            }
        }
    }

    /// Used for normalizing columns, as the fallback schemas to the main schema
    /// of the plan.
    pub fn fallback_normalize_schemas(&self) -> Vec<&DFSchema> {
        match self {
            LogicalPlan::Window(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_) => self
                .inputs()
                .iter()
                .map(|input| input.schema().as_ref())
                .collect(),
            _ => vec![],
        }
    }

    /// Get all meaningful schemas of a plan and its children plan.
    #[deprecated(since = "20.0.0")]
    pub fn all_schemas(&self) -> Vec<&DFSchemaRef> {
        match self {
            // return self and children schemas
            LogicalPlan::Window(_)
            | LogicalPlan::Projection(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_) => {
                let mut schemas = vec![self.schema()];
                self.inputs().iter().for_each(|input| {
                    schemas.push(input.schema());
                });
                schemas
            }
            // just return self.schema()
            LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::NamedRelation(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::TableScan(_) => {
                vec![self.schema()]
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery { static_term, .. }) => {
                // return only the schema of the static term
                static_term.all_schemas()
            }
            // return children schemas
            LogicalPlan::Limit(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Prepare(_) => {
                self.inputs().iter().map(|p| p.schema()).collect()
            }
            // return empty
            LogicalPlan::Statement(_) | LogicalPlan::DescribeTable(_) => vec![],
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
    }

    /// Returns the (fixed) output schema for `DESCRIBE` plans
    pub fn describe_schema() -> Schema {
        Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ])
    }

    /// returns all expressions (non-recursively) in the current
    /// logical plan node. This does not include expressions in any
    /// children
    pub fn expressions(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.inspect_expressions(|e| {
            exprs.push(e.clone());
            Ok(()) as Result<()>
        })
        // closure always returns OK
        .unwrap();
        exprs
    }

    /// Returns all the out reference(correlated) expressions (recursively) in the current
    /// logical plan nodes and all its descendant nodes.
    pub fn all_out_ref_exprs(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.inspect_expressions(|e| {
            find_out_reference_exprs(e).into_iter().for_each(|e| {
                if !exprs.contains(&e) {
                    exprs.push(e)
                }
            });
            Ok(()) as Result<(), DataFusionError>
        })
        // closure always returns OK
        .unwrap();
        self.inputs()
            .into_iter()
            .flat_map(|child| child.all_out_ref_exprs())
            .for_each(|e| {
                if !exprs.contains(&e) {
                    exprs.push(e)
                }
            });
        exprs
    }

    /// Calls `f` on all expressions (non-recursively) in the current
    /// logical plan node. This does not include expressions in any
    /// children.
    pub fn inspect_expressions<F, E>(self: &LogicalPlan, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Expr) -> Result<(), E>,
    {
        match self {
            LogicalPlan::Projection(Projection { expr, .. }) => {
                expr.iter().try_for_each(f)
            }
            LogicalPlan::Values(Values { values, .. }) => {
                values.iter().flatten().try_for_each(f)
            }
            LogicalPlan::Filter(Filter { predicate, .. }) => f(predicate),
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::Hash(expr, _) => expr.iter().try_for_each(f),
                Partitioning::DistributeBy(expr) => expr.iter().try_for_each(f),
                Partitioning::RoundRobinBatch(_) => Ok(()),
            },
            LogicalPlan::Window(Window { window_expr, .. }) => {
                window_expr.iter().try_for_each(f)
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                ..
            }) => group_expr.iter().chain(aggr_expr.iter()).try_for_each(f),
            // There are two part of expression for join, equijoin(on) and non-equijoin(filter).
            // 1. the first part is `on.len()` equijoin expressions, and the struct of each expr is `left-on = right-on`.
            // 2. the second part is non-equijoin(filter).
            LogicalPlan::Join(Join { on, filter, .. }) => {
                on.iter()
                    // it not ideal to create an expr here to analyze them, but could cache it on the Join itself
                    .map(|(l, r)| Expr::eq(l.clone(), r.clone()))
                    .try_for_each(|e| f(&e))?;

                if let Some(filter) = filter.as_ref() {
                    f(filter)
                } else {
                    Ok(())
                }
            }
            LogicalPlan::Sort(Sort { expr, .. }) => expr.iter().try_for_each(f),
            LogicalPlan::Extension(extension) => {
                // would be nice to avoid this copy -- maybe can
                // update extension to just observer Exprs
                extension.node.expressions().iter().try_for_each(f)
            }
            LogicalPlan::TableScan(TableScan { filters, .. }) => {
                filters.iter().try_for_each(f)
            }
            LogicalPlan::Unnest(Unnest { column, .. }) => {
                f(&Expr::Column(column.clone()))
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                on_expr,
                select_expr,
                sort_expr,
                ..
            })) => on_expr
                .iter()
                .chain(select_expr.iter())
                .chain(sort_expr.clone().unwrap_or(vec![]).iter())
                .try_for_each(f),
            // plans without expressions
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::NamedRelation(_)
            // TODO: not sure if this should go here
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Prepare(_) => Ok(()),
        }
    }

    /// returns all inputs of this `LogicalPlan` node. Does not
    /// include inputs to inputs, or subqueries.
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => vec![input],
            LogicalPlan::Filter(Filter { input, .. }) => vec![input],
            LogicalPlan::Repartition(Repartition { input, .. }) => vec![input],
            LogicalPlan::Window(Window { input, .. }) => vec![input],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input],
            LogicalPlan::Sort(Sort { input, .. }) => vec![input],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left, right],
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => vec![left, right],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input],
            LogicalPlan::Subquery(Subquery { subquery, .. }) => vec![subquery],
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => vec![input],
            LogicalPlan::Extension(extension) => extension.node.inputs(),
            LogicalPlan::Union(Union { inputs, .. }) => {
                inputs.iter().map(|arc| arc.as_ref()).collect()
            }
            LogicalPlan::Distinct(
                Distinct::All(input) | Distinct::On(DistinctOn { input, .. }),
            ) => vec![input],
            LogicalPlan::Explain(explain) => vec![&explain.plan],
            LogicalPlan::Analyze(analyze) => vec![&analyze.input],
            LogicalPlan::Dml(write) => vec![&write.input],
            LogicalPlan::Copy(copy) => vec![&copy.input],
            LogicalPlan::Ddl(ddl) => ddl.inputs(),
            LogicalPlan::Unnest(Unnest { input, .. }) => vec![input],
            LogicalPlan::Prepare(Prepare { input, .. }) => vec![input],
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                static_term,
                recursive_term,
                ..
            }) => vec![static_term, recursive_term],
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::NamedRelation(_)
            | LogicalPlan::Statement { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => vec![],
        }
    }

    /// returns all `Using` join columns in a logical plan
    pub fn using_columns(&self) -> Result<Vec<HashSet<Column>>, DataFusionError> {
        let mut using_columns: Vec<HashSet<Column>> = vec![];

        self.apply(&mut |plan| {
            if let LogicalPlan::Join(Join {
                join_constraint: JoinConstraint::Using,
                on,
                ..
            }) = plan
            {
                // The join keys in using-join must be columns.
                let columns =
                    on.iter().try_fold(HashSet::new(), |mut accumu, (l, r)| {
                        accumu.insert(l.try_into_col()?);
                        accumu.insert(r.try_into_col()?);
                        Result::<_, DataFusionError>::Ok(accumu)
                    })?;
                using_columns.push(columns);
            }
            Ok(VisitRecursion::Continue)
        })?;

        Ok(using_columns)
    }

    /// returns the first output expression of this `LogicalPlan` node.
    pub fn head_output_expr(&self) -> Result<Option<Expr>> {
        match self {
            LogicalPlan::Projection(projection) => {
                Ok(Some(projection.expr.as_slice()[0].clone()))
            }
            LogicalPlan::Aggregate(agg) => {
                if agg.group_expr.is_empty() {
                    Ok(Some(agg.aggr_expr.as_slice()[0].clone()))
                } else {
                    Ok(Some(agg.group_expr.as_slice()[0].clone()))
                }
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn { select_expr, .. })) => {
                Ok(Some(select_expr[0].clone()))
            }
            LogicalPlan::Filter(Filter { input, .. })
            | LogicalPlan::Distinct(Distinct::All(input))
            | LogicalPlan::Sort(Sort { input, .. })
            | LogicalPlan::Limit(Limit { input, .. })
            | LogicalPlan::Repartition(Repartition { input, .. })
            | LogicalPlan::Window(Window { input, .. }) => input.head_output_expr(),
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                ..
            }) => match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    if left.schema().fields().is_empty() {
                        right.head_output_expr()
                    } else {
                        left.head_output_expr()
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => left.head_output_expr(),
                JoinType::RightSemi | JoinType::RightAnti => right.head_output_expr(),
            },
            LogicalPlan::CrossJoin(cross) => {
                if cross.left.schema().fields().is_empty() {
                    cross.right.head_output_expr()
                } else {
                    cross.left.head_output_expr()
                }
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery { static_term, .. }) => {
                static_term.head_output_expr()
            }
            LogicalPlan::Union(union) => Ok(Some(Expr::Column(
                union.schema.fields()[0].qualified_column(),
            ))),
            LogicalPlan::TableScan(table) => Ok(Some(Expr::Column(
                table.projected_schema.fields()[0].qualified_column(),
            ))),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let expr_opt = subquery_alias.input.head_output_expr()?;
                expr_opt
                    .map(|expr| {
                        Ok(Expr::Column(create_col_from_scalar_expr(
                            &expr,
                            subquery_alias.alias.to_string(),
                        )?))
                    })
                    .map_or(Ok(None), |v| v.map(Some))
            }
            LogicalPlan::Subquery(_) => Ok(None),
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::NamedRelation(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_) => Ok(None),
        }
    }

    /// Returns a copy of this `LogicalPlan` with the new inputs
    pub fn with_new_inputs(&self, inputs: &[LogicalPlan]) -> Result<LogicalPlan> {
        // with_new_inputs use original expression,
        // so we don't need to recompute Schema.
        match &self {
            LogicalPlan::Projection(projection) => {
                // Schema of the projection may change
                // when its input changes. Hence we should use
                // `try_new` method instead of `try_new_with_schema`.
                Projection::try_new(projection.expr.to_vec(), Arc::new(inputs[0].clone()))
                    .map(LogicalPlan::Projection)
            }
            LogicalPlan::Window(Window {
                window_expr,
                schema,
                ..
            }) => Ok(LogicalPlan::Window(Window {
                input: Arc::new(inputs[0].clone()),
                window_expr: window_expr.to_vec(),
                schema: schema.clone(),
            })),
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                ..
            }) => Aggregate::try_new(
                // Schema of the aggregate may change
                // when its input changes. Hence we should use
                // `try_new` method instead of `try_new_with_schema`.
                Arc::new(inputs[0].clone()),
                group_expr.to_vec(),
                aggr_expr.to_vec(),
            )
            .map(LogicalPlan::Aggregate),
            _ => self.with_new_exprs(self.expressions(), inputs),
        }
    }

    /// Returns a new `LogicalPlan` based on `self` with inputs and
    /// expressions replaced.
    ///
    /// The exprs correspond to the same order of expressions returned
    /// by [`Self::expressions`]. This function is used by optimizers
    /// to rewrite plans using the following pattern:
    ///
    /// ```text
    /// let new_inputs = optimize_children(..., plan, props);
    ///
    /// // get the plans expressions to optimize
    /// let exprs = plan.expressions();
    ///
    /// // potentially rewrite plan expressions
    /// let rewritten_exprs = rewrite_exprs(exprs);
    ///
    /// // create new plan using rewritten_exprs in same position
    /// let new_plan = plan.new_with_exprs(rewritten_exprs, new_inputs);
    /// ```
    ///
    /// Note: sometimes [`Self::with_new_exprs`] will use schema of
    /// original plan, it will not change the scheam.  Such as
    /// `Projection/Aggregate/Window`
    pub fn with_new_exprs(
        &self,
        mut expr: Vec<Expr>,
        inputs: &[LogicalPlan],
    ) -> Result<LogicalPlan> {
        match self {
            // Since expr may be different than the previous expr, schema of the projection
            // may change. We need to use try_new method instead of try_new_with_schema method.
            LogicalPlan::Projection(Projection { .. }) => {
                Projection::try_new(expr, Arc::new(inputs[0].clone()))
                    .map(LogicalPlan::Projection)
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                table_schema,
                op,
                ..
            }) => Ok(LogicalPlan::Dml(DmlStatement {
                table_name: table_name.clone(),
                table_schema: table_schema.clone(),
                op: op.clone(),
                input: Arc::new(inputs[0].clone()),
            })),
            LogicalPlan::Copy(CopyTo {
                input: _,
                output_url,
                file_format,
                copy_options,
                single_file_output,
            }) => Ok(LogicalPlan::Copy(CopyTo {
                input: Arc::new(inputs[0].clone()),
                output_url: output_url.clone(),
                file_format: file_format.clone(),
                single_file_output: *single_file_output,
                copy_options: copy_options.clone(),
            })),
            LogicalPlan::Values(Values { schema, .. }) => {
                Ok(LogicalPlan::Values(Values {
                    schema: schema.clone(),
                    values: expr
                        .chunks_exact(schema.fields().len())
                        .map(|s| s.to_vec())
                        .collect::<Vec<_>>(),
                }))
            }
            LogicalPlan::Filter { .. } => {
                assert_eq!(1, expr.len());
                let predicate = expr.pop().unwrap();

                // filter predicates should not contain aliased expressions so we remove any aliases
                // before this logic was added we would have aliases within filters such as for
                // benchmark q6:
                //
                // lineitem.l_shipdate >= Date32(\"8766\")
                // AND lineitem.l_shipdate < Date32(\"9131\")
                // AND CAST(lineitem.l_discount AS Decimal128(30, 15)) AS lineitem.l_discount >=
                // Decimal128(Some(49999999999999),30,15)
                // AND CAST(lineitem.l_discount AS Decimal128(30, 15)) AS lineitem.l_discount <=
                // Decimal128(Some(69999999999999),30,15)
                // AND lineitem.l_quantity < Decimal128(Some(2400),15,2)

                struct RemoveAliases {}

                impl TreeNodeRewriter for RemoveAliases {
                    type N = Expr;

                    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
                        match expr {
                            Expr::Exists { .. }
                            | Expr::ScalarSubquery(_)
                            | Expr::InSubquery(_) => {
                                // subqueries could contain aliases so we don't recurse into those
                                Ok(RewriteRecursion::Stop)
                            }
                            Expr::Alias(_) => Ok(RewriteRecursion::Mutate),
                            _ => Ok(RewriteRecursion::Continue),
                        }
                    }

                    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
                        Ok(expr.unalias())
                    }
                }

                let mut remove_aliases = RemoveAliases {};
                let predicate = predicate.rewrite(&mut remove_aliases)?;

                Filter::try_new(predicate, Arc::new(inputs[0].clone()))
                    .map(LogicalPlan::Filter)
            }
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::RoundRobinBatch(n) => {
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::RoundRobinBatch(*n),
                        input: Arc::new(inputs[0].clone()),
                    }))
                }
                Partitioning::Hash(_, n) => Ok(LogicalPlan::Repartition(Repartition {
                    partitioning_scheme: Partitioning::Hash(expr, *n),
                    input: Arc::new(inputs[0].clone()),
                })),
                Partitioning::DistributeBy(_) => {
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::DistributeBy(expr),
                        input: Arc::new(inputs[0].clone()),
                    }))
                }
            },
            LogicalPlan::Window(Window {
                window_expr,
                schema,
                ..
            }) => {
                assert_eq!(window_expr.len(), expr.len());
                Ok(LogicalPlan::Window(Window {
                    input: Arc::new(inputs[0].clone()),
                    window_expr: expr,
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Aggregate(Aggregate { group_expr, .. }) => {
                // group exprs are the first expressions
                let agg_expr = expr.split_off(group_expr.len());

                Aggregate::try_new(Arc::new(inputs[0].clone()), expr, agg_expr)
                    .map(LogicalPlan::Aggregate)
            }
            LogicalPlan::Sort(Sort { fetch, .. }) => Ok(LogicalPlan::Sort(Sort {
                expr,
                input: Arc::new(inputs[0].clone()),
                fetch: *fetch,
            })),
            LogicalPlan::Join(Join {
                join_type,
                join_constraint,
                on,
                null_equals_null,
                ..
            }) => {
                let schema =
                    build_join_schema(inputs[0].schema(), inputs[1].schema(), join_type)?;

                let equi_expr_count = on.len();
                assert!(expr.len() >= equi_expr_count);

                // Assume that the last expr, if any,
                // is the filter_expr (non equality predicate from ON clause)
                let filter_expr = if expr.len() > equi_expr_count {
                    expr.pop()
                } else {
                    None
                };

                // The first part of expr is equi-exprs,
                // and the struct of each equi-expr is like `left-expr = right-expr`.
                assert_eq!(expr.len(), equi_expr_count);
                let new_on:Vec<(Expr,Expr)> = expr.into_iter().map(|equi_expr| {
                    // SimplifyExpression rule may add alias to the equi_expr.
                    let unalias_expr = equi_expr.clone().unalias();
                    if let Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) = unalias_expr {
                        Ok((*left, *right))
                    } else {
                        internal_err!(
                            "The front part expressions should be an binary equality expression, actual:{equi_expr}"
                        )
                    }
                }).collect::<Result<Vec<(Expr, Expr)>>>()?;

                Ok(LogicalPlan::Join(Join {
                    left: Arc::new(inputs[0].clone()),
                    right: Arc::new(inputs[1].clone()),
                    join_type: *join_type,
                    join_constraint: *join_constraint,
                    on: new_on,
                    filter: filter_expr,
                    schema: DFSchemaRef::new(schema),
                    null_equals_null: *null_equals_null,
                }))
            }
            LogicalPlan::CrossJoin(_) => {
                let left = inputs[0].clone();
                let right = inputs[1].clone();
                LogicalPlanBuilder::from(left).cross_join(right)?.build()
            }
            LogicalPlan::Subquery(Subquery {
                outer_ref_columns, ..
            }) => {
                let subquery = LogicalPlanBuilder::from(inputs[0].clone()).build()?;
                Ok(LogicalPlan::Subquery(Subquery {
                    subquery: Arc::new(subquery),
                    outer_ref_columns: outer_ref_columns.clone(),
                }))
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
                SubqueryAlias::try_new(inputs[0].clone(), alias.clone())
                    .map(LogicalPlan::SubqueryAlias)
            }
            LogicalPlan::Limit(Limit { skip, fetch, .. }) => {
                Ok(LogicalPlan::Limit(Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(inputs[0].clone()),
                }))
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                name,
                if_not_exists,
                or_replace,
                ..
            })) => Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                CreateMemoryTable {
                    input: Arc::new(inputs[0].clone()),
                    constraints: Constraints::empty(),
                    name: name.clone(),
                    if_not_exists: *if_not_exists,
                    or_replace: *or_replace,
                },
            ))),
            LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                name,
                or_replace,
                definition,
                ..
            })) => Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                input: Arc::new(inputs[0].clone()),
                name: name.clone(),
                or_replace: *or_replace,
                definition: definition.clone(),
            }))),
            LogicalPlan::Extension(e) => Ok(LogicalPlan::Extension(Extension {
                node: e.node.from_template(&expr, inputs),
            })),
            LogicalPlan::Union(Union { schema, .. }) => Ok(LogicalPlan::Union(Union {
                inputs: inputs.iter().cloned().map(Arc::new).collect(),
                schema: schema.clone(),
            })),
            LogicalPlan::Distinct(distinct) => {
                let distinct = match distinct {
                    Distinct::All(_) => Distinct::All(Arc::new(inputs[0].clone())),
                    Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        ..
                    }) => {
                        let sort_expr = expr.split_off(on_expr.len() + select_expr.len());
                        let select_expr = expr.split_off(on_expr.len());
                        Distinct::On(DistinctOn::try_new(
                            expr,
                            select_expr,
                            if !sort_expr.is_empty() {
                                Some(sort_expr)
                            } else {
                                None
                            },
                            Arc::new(inputs[0].clone()),
                        )?)
                    }
                };
                Ok(LogicalPlan::Distinct(distinct))
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name, is_distinct, ..
            }) => Ok(LogicalPlan::RecursiveQuery(RecursiveQuery {
                name: name.clone(),
                static_term: Arc::new(inputs[0].clone()),
                recursive_term: Arc::new(inputs[1].clone()),
                is_distinct: *is_distinct,
            })),
            LogicalPlan::Analyze(a) => {
                assert!(expr.is_empty());
                assert_eq!(inputs.len(), 1);
                Ok(LogicalPlan::Analyze(Analyze {
                    verbose: a.verbose,
                    schema: a.schema.clone(),
                    input: Arc::new(inputs[0].clone()),
                }))
            }
            LogicalPlan::Explain(_) => {
                // Explain should be handled specially in the optimizers;
                // If this check cannot pass it means some optimizer pass is
                // trying to optimize Explain directly
                if expr.is_empty() {
                    return plan_err!("Invalid EXPLAIN command. Expression is empty");
                }

                if inputs.is_empty() {
                    return plan_err!("Invalid EXPLAIN command. Inputs are empty");
                }

                Ok(self.clone())
            }
            LogicalPlan::Prepare(Prepare {
                name, data_types, ..
            }) => Ok(LogicalPlan::Prepare(Prepare {
                name: name.clone(),
                data_types: data_types.clone(),
                input: Arc::new(inputs[0].clone()),
            })),
            LogicalPlan::TableScan(ts) => {
                assert!(inputs.is_empty(), "{self:?}  should have no inputs");
                Ok(LogicalPlan::TableScan(TableScan {
                    filters: expr,
                    ..ts.clone()
                }))
            }
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::NamedRelation(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Statement(_) => {
                // All of these plan types have no inputs / exprs so should not be called
                assert!(expr.is_empty(), "{self:?} should have no exprs");
                assert!(inputs.is_empty(), "{self:?}  should have no inputs");
                Ok(self.clone())
            }
            LogicalPlan::DescribeTable(_) => Ok(self.clone()),
            LogicalPlan::Unnest(Unnest {
                column,
                schema,
                options,
                ..
            }) => {
                // Update schema with unnested column type.
                let input = Arc::new(inputs[0].clone());
                let nested_field = input.schema().field_from_column(column)?;
                let unnested_field = schema.field_from_column(column)?;
                let fields = input
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| {
                        if f == nested_field {
                            unnested_field.clone()
                        } else {
                            f.clone()
                        }
                    })
                    .collect::<Vec<_>>();

                let schema = Arc::new(
                    DFSchema::new_with_metadata(
                        fields,
                        input.schema().metadata().clone(),
                    )?
                    // We can use the existing functional dependencies as is:
                    .with_functional_dependencies(
                        input.schema().functional_dependencies().clone(),
                    ),
                );

                Ok(LogicalPlan::Unnest(Unnest {
                    input,
                    column: column.clone(),
                    schema,
                    options: options.clone(),
                }))
            }
        }
    }
    /// Replaces placeholder param values (like `$1`, `$2`) in [`LogicalPlan`]
    /// with the specified `param_values`.
    ///
    /// [`LogicalPlan::Prepare`] are
    /// converted to their inner logical plan for execution.
    ///
    /// # Example
    /// ```
    /// # use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_common::ScalarValue;
    /// # use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan, placeholder};
    /// # let schema = Schema::new(vec![
    /// #     Field::new("id", DataType::Int32, false),
    /// # ]);
    /// // Build SELECT * FROM t1 WHRERE id = $1
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(placeholder("$1"))).unwrap()
    ///     .build().unwrap();
    ///
    /// assert_eq!("Filter: t1.id = $1\
    ///            \n  TableScan: t1",
    ///             plan.display_indent().to_string()
    /// );
    ///
    /// // Fill in the parameter $1 with a literal 3
    /// let plan = plan.with_param_values(vec![
    ///   ScalarValue::from(3i32) // value at index 0 --> $1
    /// ]).unwrap();
    ///
    /// assert_eq!("Filter: t1.id = Int32(3)\
    ///             \n  TableScan: t1",
    ///             plan.display_indent().to_string()
    ///  );
    /// ```
    pub fn with_param_values(
        self,
        param_values: Vec<ScalarValue>,
    ) -> Result<LogicalPlan> {
        match self {
            LogicalPlan::Prepare(prepare_lp) => {
                // Verify if the number of params matches the number of values
                if prepare_lp.data_types.len() != param_values.len() {
                    return plan_err!(
                        "Expected {} parameters, got {}",
                        prepare_lp.data_types.len(),
                        param_values.len()
                    );
                }

                // Verify if the types of the params matches the types of the values
                let iter = prepare_lp.data_types.iter().zip(param_values.iter());
                for (i, (param_type, value)) in iter.enumerate() {
                    if *param_type != value.data_type() {
                        return plan_err!(
                            "Expected parameter of type {:?}, got {:?} at index {}",
                            param_type,
                            value.data_type(),
                            i
                        );
                    }
                }

                let input_plan = prepare_lp.input;
                input_plan.replace_params_with_values(&param_values)
            }
            _ => self.replace_params_with_values(&param_values),
        }
    }

    /// Returns the maximum number of rows that this plan can output, if known.
    ///
    /// If `None`, the plan can return any number of rows.
    /// If `Some(n)` then the plan can return at most `n` rows but may return fewer.
    pub fn max_rows(self: &LogicalPlan) -> Option<usize> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => input.max_rows(),
            LogicalPlan::Filter(Filter { input, .. }) => input.max_rows(),
            LogicalPlan::Window(Window { input, .. }) => input.max_rows(),
            LogicalPlan::Aggregate(Aggregate {
                input, group_expr, ..
            }) => {
                // Empty group_expr will return Some(1)
                if group_expr
                    .iter()
                    .all(|expr| matches!(expr, Expr::Literal(_)))
                {
                    Some(1)
                } else {
                    input.max_rows()
                }
            }
            LogicalPlan::Sort(Sort { input, fetch, .. }) => {
                match (fetch, input.max_rows()) {
                    (Some(fetch_limit), Some(input_max)) => {
                        Some(input_max.min(*fetch_limit))
                    }
                    (Some(fetch_limit), None) => Some(*fetch_limit),
                    (None, Some(input_max)) => Some(input_max),
                    (None, None) => None,
                }
            }
            LogicalPlan::Join(Join {
                left,
                right,
                join_type,
                ..
            }) => match join_type {
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                    match (left.max_rows(), right.max_rows()) {
                        (Some(left_max), Some(right_max)) => {
                            let min_rows = match join_type {
                                JoinType::Left => left_max,
                                JoinType::Right => right_max,
                                JoinType::Full => left_max + right_max,
                                _ => 0,
                            };
                            Some((left_max * right_max).max(min_rows))
                        }
                        _ => None,
                    }
                }
                JoinType::LeftSemi | JoinType::LeftAnti => left.max_rows(),
                JoinType::RightSemi | JoinType::RightAnti => right.max_rows(),
            },
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                match (left.max_rows(), right.max_rows()) {
                    (Some(left_max), Some(right_max)) => Some(left_max * right_max),
                    _ => None,
                }
            }
            LogicalPlan::Repartition(Repartition { input, .. }) => input.max_rows(),
            LogicalPlan::Union(Union { inputs, .. }) => inputs
                .iter()
                .map(|plan| plan.max_rows())
                .try_fold(0usize, |mut acc, input_max| {
                    if let Some(i_max) = input_max {
                        acc += i_max;
                        Some(acc)
                    } else {
                        None
                    }
                }),
            LogicalPlan::TableScan(TableScan { fetch, .. }) => *fetch,
            LogicalPlan::EmptyRelation(_) => Some(0),
            // TODO: not sure if this is correct
            LogicalPlan::NamedRelation(_) => None,
            LogicalPlan::RecursiveQuery(_) => None,
            LogicalPlan::Subquery(_) => None,
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => input.max_rows(),
            LogicalPlan::Limit(Limit { fetch, .. }) => *fetch,
            LogicalPlan::Distinct(
                Distinct::All(input) | Distinct::On(DistinctOn { input, .. }),
            ) => input.max_rows(),
            LogicalPlan::Values(v) => Some(v.values.len()),
            LogicalPlan::Unnest(_) => None,
            LogicalPlan::Ddl(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Prepare(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Extension(_) => None,
        }
    }
}

impl LogicalPlan {
    /// applies `op` to any subqueries in the plan
    pub(crate) fn apply_subqueries<F>(&self, op: &mut F) -> datafusion_common::Result<()>
    where
        F: FnMut(&Self) -> datafusion_common::Result<VisitRecursion>,
    {
        self.inspect_expressions(|expr| {
            // recursively look for subqueries
            inspect_expr_pre(expr, |expr| {
                match expr {
                    Expr::Exists(Exists { subquery, .. })
                    | Expr::InSubquery(InSubquery { subquery, .. })
                    | Expr::ScalarSubquery(subquery) => {
                        // use a synthetic plan so the collector sees a
                        // LogicalPlan::Subquery (even though it is
                        // actually a Subquery alias)
                        let synthetic_plan = LogicalPlan::Subquery(subquery.clone());
                        synthetic_plan.apply(op)?;
                    }
                    _ => {}
                }
                Ok::<(), DataFusionError>(())
            })
        })?;
        Ok(())
    }

    /// applies visitor to any subqueries in the plan
    pub(crate) fn visit_subqueries<V>(&self, v: &mut V) -> datafusion_common::Result<()>
    where
        V: TreeNodeVisitor<N = LogicalPlan>,
    {
        self.inspect_expressions(|expr| {
            // recursively look for subqueries
            inspect_expr_pre(expr, |expr| {
                match expr {
                    Expr::Exists(Exists { subquery, .. })
                    | Expr::InSubquery(InSubquery { subquery, .. })
                    | Expr::ScalarSubquery(subquery) => {
                        // use a synthetic plan so the visitor sees a
                        // LogicalPlan::Subquery (even though it is
                        // actually a Subquery alias)
                        let synthetic_plan = LogicalPlan::Subquery(subquery.clone());
                        synthetic_plan.visit(v)?;
                    }
                    _ => {}
                }
                Ok::<(), DataFusionError>(())
            })
        })?;
        Ok(())
    }

    /// Return a `LogicalPlan` with all placeholders (e.g $1 $2,
    /// ...) replaced with corresponding values provided in
    /// `params_values`
    ///
    /// See [`Self::with_param_values`] for examples and usage
    pub fn replace_params_with_values(
        &self,
        param_values: &[ScalarValue],
    ) -> Result<LogicalPlan> {
        let new_exprs = self
            .expressions()
            .into_iter()
            .map(|e| {
                let e = e.infer_placeholder_types(self.schema())?;
                Self::replace_placeholders_with_values(e, param_values)
            })
            .collect::<Result<Vec<_>>>()?;

        let new_inputs_with_values = self
            .inputs()
            .into_iter()
            .map(|inp| inp.replace_params_with_values(param_values))
            .collect::<Result<Vec<_>>>()?;

        self.with_new_exprs(new_exprs, &new_inputs_with_values)
    }

    /// Walk the logical plan, find any `PlaceHolder` tokens, and return a map of their IDs and DataTypes
    pub fn get_parameter_types(
        &self,
    ) -> Result<HashMap<String, Option<DataType>>, DataFusionError> {
        let mut param_types: HashMap<String, Option<DataType>> = HashMap::new();

        self.apply(&mut |plan| {
            plan.inspect_expressions(|expr| {
                expr.apply(&mut |expr| {
                    if let Expr::Placeholder(Placeholder { id, data_type }) = expr {
                        let prev = param_types.get(id);
                        match (prev, data_type) {
                            (Some(Some(prev)), Some(dt)) => {
                                if prev != dt {
                                    plan_err!("Conflicting types for {id}")?;
                                }
                            }
                            (_, Some(dt)) => {
                                param_types.insert(id.clone(), Some(dt.clone()));
                            }
                            _ => {}
                        }
                    }
                    Ok(VisitRecursion::Continue)
                })?;
                Ok::<(), DataFusionError>(())
            })?;
            Ok(VisitRecursion::Continue)
        })?;

        Ok(param_types)
    }

    /// Return an Expr with all placeholders replaced with their
    /// corresponding values provided in the params_values
    fn replace_placeholders_with_values(
        expr: Expr,
        param_values: &[ScalarValue],
    ) -> Result<Expr> {
        expr.transform(&|expr| {
            match &expr {
                Expr::Placeholder(Placeholder { id, data_type }) => {
                    if id.is_empty() || id == "$0" {
                        return plan_err!("Empty placeholder id");
                    }
                    // convert id (in format $1, $2, ..) to idx (0, 1, ..)
                    let idx = id[1..].parse::<usize>().map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to parse placeholder id: {e}"
                        ))
                    })? - 1;
                    // value at the idx-th position in param_values should be the value for the placeholder
                    let value = param_values.get(idx).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "No value found for placeholder with id {id}"
                        ))
                    })?;
                    // check if the data type of the value matches the data type of the placeholder
                    if Some(value.data_type()) != *data_type {
                        return internal_err!(
                            "Placeholder value type mismatch: expected {:?}, got {:?}",
                            data_type,
                            value.data_type()
                        );
                    }
                    // Replace the placeholder with the value
                    Ok(Transformed::Yes(Expr::Literal(value.clone())))
                }
                Expr::ScalarSubquery(qry) => {
                    let subquery =
                        Arc::new(qry.subquery.replace_params_with_values(param_values)?);
                    Ok(Transformed::Yes(Expr::ScalarSubquery(Subquery {
                        subquery,
                        outer_ref_columns: qry.outer_ref_columns.clone(),
                    })))
                }
                _ => Ok(Transformed::No(expr)),
            }
        })
    }
}

// Various implementations for printing out LogicalPlans
impl LogicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node.
    ///
    /// # Example
    ///
    /// ```text
    /// Projection: employee.id
    ///    Filter: employee.state Eq Utf8(\"CO\")\
    ///       CsvScan: employee projection=Some([0, 3])
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent
    /// let display_string = format!("{}", plan.display_indent());
    ///
    /// assert_eq!("Filter: t1.id = Int32(5)\n  TableScan: t1",
    ///             display_string);
    /// ```
    pub fn display_indent(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor::new(f, with_schema);
                match self.0.visit(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces a single line
    /// per node that includes the output schema. For example:
    ///
    /// ```text
    /// Projection: employee.id [id:Int32]\
    ///    Filter: employee.state = Utf8(\"CO\") [id:Int32, state:Utf8]\
    ///      TableScan: employee projection=[0, 3] [id:Int32, state:Utf8]";
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent_schema
    /// let display_string = format!("{}", plan.display_indent_schema());
    ///
    /// assert_eq!("Filter: t1.id = Int32(5) [id:Int32]\
    ///             \n  TableScan: t1 [id:Int32]",
    ///             display_string);
    /// ```
    pub fn display_indent_schema(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let with_schema = true;
                let mut visitor = IndentVisitor::new(f, with_schema);
                match self.0.visit(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces lines meant for
    /// graphical display using the `DOT` language. This format can be
    /// visualized using software from
    /// [`graphviz`](https://graphviz.org/)
    ///
    /// This currently produces two graphs -- one with the basic
    /// structure, and one with additional details such as schema.
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_graphviz
    /// let graphviz_string = format!("{}", plan.display_graphviz());
    /// ```
    ///
    /// If graphviz string is saved to a file such as `/tmp/example.dot`, the following
    /// commands can be used to render it as a pdf:
    ///
    /// ```bash
    ///   dot -Tpdf < /tmp/example.dot  > /tmp/example.pdf
    /// ```
    ///
    pub fn display_graphviz(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = GraphvizVisitor::new(f);

                visitor.start_graph()?;

                visitor.pre_visit_plan("LogicalPlan")?;
                self.0.visit(&mut visitor).map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0.visit(&mut visitor).map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                visitor.end_graph()?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children. For example:
    ///
    /// ```text
    /// Projection: id
    /// ```
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display
    /// let display_string = format!("{}", plan.display());
    ///
    /// assert_eq!("TableScan: t1", display_string);
    /// ```
    pub fn display(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                match self.0 {
                    LogicalPlan::EmptyRelation(_) => write!(f, "EmptyRelation"),
                    LogicalPlan::NamedRelation(NamedRelation { name, .. }) => {
                        write!(f, "NamedRelation: {}", name)
                    }
                    LogicalPlan::RecursiveQuery(RecursiveQuery {
                        is_distinct, ..
                    }) => {
                        write!(f, "RecursiveQuery: is_distinct={}", is_distinct)
                    }
                    LogicalPlan::Values(Values { ref values, .. }) => {
                        let str_values: Vec<_> = values
                            .iter()
                            // limit to only 5 values to avoid horrible display
                            .take(5)
                            .map(|row| {
                                let item = row
                                    .iter()
                                    .map(|expr| expr.to_string())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                format!("({item})")
                            })
                            .collect();

                        let elipse = if values.len() > 5 { "..." } else { "" };
                        write!(f, "Values: {}{}", str_values.join(", "), elipse)
                    }

                    LogicalPlan::TableScan(TableScan {
                        ref source,
                        ref table_name,
                        ref projection,
                        ref filters,
                        ref fetch,
                        ..
                    }) => {
                        let projected_fields = match projection {
                            Some(indices) => {
                                let schema = source.schema();
                                let names: Vec<&str> = indices
                                    .iter()
                                    .map(|i| schema.field(*i).name().as_str())
                                    .collect();
                                format!(" projection=[{}]", names.join(", "))
                            }
                            _ => "".to_string(),
                        };

                        write!(f, "TableScan: {table_name}{projected_fields}")?;

                        if !filters.is_empty() {
                            let mut full_filter = vec![];
                            let mut partial_filter = vec![];
                            let mut unsupported_filters = vec![];
                            let filters: Vec<&Expr> = filters.iter().collect();

                            if let Ok(results) =
                                source.supports_filters_pushdown(&filters)
                            {
                                filters.iter().zip(results.iter()).for_each(
                                    |(x, res)| match res {
                                        TableProviderFilterPushDown::Exact => {
                                            full_filter.push(x)
                                        }
                                        TableProviderFilterPushDown::Inexact => {
                                            partial_filter.push(x)
                                        }
                                        TableProviderFilterPushDown::Unsupported => {
                                            unsupported_filters.push(x)
                                        }
                                    },
                                );
                            }

                            if !full_filter.is_empty() {
                                write!(
                                    f,
                                    ", full_filters=[{}]",
                                    expr_vec_fmt!(full_filter)
                                )?;
                            };
                            if !partial_filter.is_empty() {
                                write!(
                                    f,
                                    ", partial_filters=[{}]",
                                    expr_vec_fmt!(partial_filter)
                                )?;
                            }
                            if !unsupported_filters.is_empty() {
                                write!(
                                    f,
                                    ", unsupported_filters=[{}]",
                                    expr_vec_fmt!(unsupported_filters)
                                )?;
                            }
                        }

                        if let Some(n) = fetch {
                            write!(f, ", fetch={n}")?;
                        }

                        Ok(())
                    }
                    LogicalPlan::Projection(Projection { ref expr, .. }) => {
                        write!(f, "Projection: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item}")?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Dml(DmlStatement { table_name, op, .. }) => {
                        write!(f, "Dml: op=[{op}] table=[{table_name}]")
                    }
                    LogicalPlan::Copy(CopyTo {
                        input: _,
                        output_url,
                        file_format,
                        single_file_output,
                        copy_options,
                    }) => {
                        let op_str = match copy_options {
                            CopyOptions::SQLOptions(statement) => statement
                                .clone()
                                .into_inner()
                                .iter()
                                .map(|(k, v)| format!("{k} {v}"))
                                .collect::<Vec<String>>()
                                .join(", "),
                            CopyOptions::WriterOptions(_) => "".into(),
                        };

                        write!(f, "CopyTo: format={file_format} output_url={output_url} single_file_output={single_file_output} options: ({op_str})")
                    }
                    LogicalPlan::Ddl(ddl) => {
                        write!(f, "{}", ddl.display())
                    }
                    LogicalPlan::Filter(Filter {
                        predicate: ref expr,
                        ..
                    }) => write!(f, "Filter: {expr}"),
                    LogicalPlan::Window(Window {
                        ref window_expr, ..
                    }) => {
                        write!(
                            f,
                            "WindowAggr: windowExpr=[[{}]]",
                            expr_vec_fmt!(window_expr)
                        )
                    }
                    LogicalPlan::Aggregate(Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    }) => write!(
                        f,
                        "Aggregate: groupBy=[[{}]], aggr=[[{}]]",
                        expr_vec_fmt!(group_expr),
                        expr_vec_fmt!(aggr_expr)
                    ),
                    LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
                        write!(f, "Sort: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item}")?;
                        }
                        if let Some(a) = fetch {
                            write!(f, ", fetch={a}")?;
                        }

                        Ok(())
                    }
                    LogicalPlan::Join(Join {
                        on: ref keys,
                        filter,
                        join_constraint,
                        join_type,
                        ..
                    }) => {
                        let join_expr: Vec<String> =
                            keys.iter().map(|(l, r)| format!("{l} = {r}")).collect();
                        let filter_expr = filter
                            .as_ref()
                            .map(|expr| format!(" Filter: {expr}"))
                            .unwrap_or_else(|| "".to_string());
                        match join_constraint {
                            JoinConstraint::On => {
                                write!(
                                    f,
                                    "{} Join: {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr
                                )
                            }
                            JoinConstraint::Using => {
                                write!(
                                    f,
                                    "{} Join: Using {}{}",
                                    join_type,
                                    join_expr.join(", "),
                                    filter_expr,
                                )
                            }
                        }
                    }
                    LogicalPlan::CrossJoin(_) => {
                        write!(f, "CrossJoin:")
                    }
                    LogicalPlan::Repartition(Repartition {
                        partitioning_scheme,
                        ..
                    }) => match partitioning_scheme {
                        Partitioning::RoundRobinBatch(n) => {
                            write!(f, "Repartition: RoundRobinBatch partition_count={n}")
                        }
                        Partitioning::Hash(expr, n) => {
                            let hash_expr: Vec<String> =
                                expr.iter().map(|e| format!("{e}")).collect();
                            write!(
                                f,
                                "Repartition: Hash({}) partition_count={}",
                                hash_expr.join(", "),
                                n
                            )
                        }
                        Partitioning::DistributeBy(expr) => {
                            let dist_by_expr: Vec<String> =
                                expr.iter().map(|e| format!("{e}")).collect();
                            write!(
                                f,
                                "Repartition: DistributeBy({})",
                                dist_by_expr.join(", "),
                            )
                        }
                    },
                    LogicalPlan::Limit(Limit {
                        ref skip,
                        ref fetch,
                        ..
                    }) => {
                        write!(
                            f,
                            "Limit: skip={}, fetch={}",
                            skip,
                            fetch.map_or_else(|| "None".to_string(), |x| x.to_string())
                        )
                    }
                    LogicalPlan::Subquery(Subquery { .. }) => {
                        write!(f, "Subquery:")
                    }
                    LogicalPlan::SubqueryAlias(SubqueryAlias { ref alias, .. }) => {
                        write!(f, "SubqueryAlias: {alias}")
                    }
                    LogicalPlan::Statement(statement) => {
                        write!(f, "{}", statement.display())
                    }
                    LogicalPlan::Distinct(distinct) => match distinct {
                        Distinct::All(_) => write!(f, "Distinct:"),
                        Distinct::On(DistinctOn {
                            on_expr,
                            select_expr,
                            sort_expr,
                            ..
                        }) => write!(
                            f,
                            "DistinctOn: on_expr=[[{}]], select_expr=[[{}]], sort_expr=[[{}]]",
                            expr_vec_fmt!(on_expr),
                            expr_vec_fmt!(select_expr),
                            if let Some(sort_expr) = sort_expr { expr_vec_fmt!(sort_expr) } else { "".to_string() },
                        ),
                    },
                    LogicalPlan::Explain { .. } => write!(f, "Explain"),
                    LogicalPlan::Analyze { .. } => write!(f, "Analyze"),
                    LogicalPlan::Union(_) => write!(f, "Union"),
                    LogicalPlan::Extension(e) => e.node.fmt_for_explain(f),
                    LogicalPlan::Prepare(Prepare {
                        name, data_types, ..
                    }) => {
                        write!(f, "Prepare: {name:?} {data_types:?} ")
                    }
                    LogicalPlan::DescribeTable(DescribeTable { .. }) => {
                        write!(f, "DescribeTable")
                    }
                    LogicalPlan::Unnest(Unnest { column, .. }) => {
                        write!(f, "Unnest: {column}")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

impl Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

impl ToStringifiedPlan for LogicalPlan {
    fn to_stringified(&self, plan_type: PlanType) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.display_indent().to_string())
    }
}

/// Produces no rows: An empty relation with an empty schema
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EmptyRelation {
    /// Whether to produce a placeholder row
    pub produce_one_row: bool,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

/// A named temporary relation with a known schema.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NamedRelation {
    /// The relation name
    pub name: String,
    /// The schema description
    pub schema: DFSchemaRef,
}

/// A variadic query operation
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RecursiveQuery {
    /// Name of the query
    pub name: String,
    /// The static term
    pub static_term: Arc<LogicalPlan>,
    /// The recursive term
    pub recursive_term: Arc<LogicalPlan>,
    /// Distinction
    pub is_distinct: bool,
}

/// Values expression. See
/// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
/// documentation for more details.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Values {
    /// The table schema
    pub schema: DFSchemaRef,
    /// Values
    pub values: Vec<Vec<Expr>>,
}

/// Evaluates an arbitrary list of expressions (essentially a
/// SELECT with an expression list) on its input.
#[derive(Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct Projection {
    /// The list of expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

impl Projection {
    /// Create a new Projection
    pub fn try_new(expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(&expr, &input)?,
            input.schema().metadata().clone(),
        )?);
        Self::try_new_with_schema(expr, input, schema)
    }

    /// Create a new Projection using the specified output schema
    pub fn try_new_with_schema(
        expr: Vec<Expr>,
        input: Arc<LogicalPlan>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        if expr.len() != schema.fields().len() {
            return plan_err!("Projection has mismatch between number of expressions ({}) and number of fields in schema ({})", expr.len(), schema.fields().len());
        }
        // Update functional dependencies of `input` according to projection
        // expressions:
        let id_key_groups = calc_func_dependencies_for_project(&expr, &input)?;
        let schema = schema.as_ref().clone();
        let schema = Arc::new(schema.with_functional_dependencies(id_key_groups));
        Ok(Self {
            expr,
            input,
            schema,
        })
    }

    /// Create a new Projection using the specified output schema
    pub fn new_from_schema(input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Self {
        let expr: Vec<Expr> = schema
            .fields()
            .iter()
            .map(|field| field.qualified_column())
            .map(Expr::Column)
            .collect();
        Self {
            expr,
            input,
            schema,
        }
    }
}

/// Aliased subquery
#[derive(Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct SubqueryAlias {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The alias for the input relation
    pub alias: OwnedTableReference,
    /// The schema with qualified field names
    pub schema: DFSchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(
        plan: LogicalPlan,
        alias: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let alias = alias.into();
        let schema: Schema = plan.schema().as_ref().clone().into();
        // Since schema is the same, other than qualifier, we can use existing
        // functional dependencies:
        let func_dependencies = plan.schema().functional_dependencies().clone();
        let schema = DFSchemaRef::new(
            DFSchema::try_from_qualified_schema(&alias, &schema)?
                .with_functional_dependencies(func_dependencies),
        );
        Ok(SubqueryAlias {
            input: Arc::new(plan),
            alias,
            schema,
        })
    }
}

/// Filters rows from its input that do not match an
/// expression (essentially a WHERE clause with a predicate
/// expression).
///
/// Semantically, `<predicate>` is evaluated for each row of the input;
/// If the value of `<predicate>` is true, the input row is passed to
/// the output. If the value of `<predicate>` is false, the row is
/// discarded.
///
/// Filter should not be created directly but instead use `try_new()`
/// and that these fields are only pub to support pattern matching
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

impl Filter {
    /// Create a new filter operator.
    pub fn try_new(predicate: Expr, input: Arc<LogicalPlan>) -> Result<Self> {
        // Filter predicates must return a boolean value so we try and validate that here.
        // Note that it is not always possible to resolve the predicate expression during plan
        // construction (such as with correlated subqueries) so we make a best effort here and
        // ignore errors resolving the expression against the schema.
        if let Ok(predicate_type) = predicate.get_type(input.schema()) {
            if predicate_type != DataType::Boolean {
                return plan_err!(
                    "Cannot create filter with non-boolean predicate '{predicate}' returning {predicate_type}"
                );
            }
        }

        // filter predicates should not be aliased
        if let Expr::Alias(Alias { expr, name, .. }) = predicate {
            return plan_err!(
                "Attempted to create Filter predicate with \
                expression `{expr}` aliased as '{name}'. Filter predicates should not be \
                aliased."
            );
        }

        Ok(Self { predicate, input })
    }
}

/// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Window {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The window function expression
    pub window_expr: Vec<Expr>,
    /// The schema description of the window output
    pub schema: DFSchemaRef,
}

impl Window {
    /// Create a new window operator.
    pub fn try_new(window_expr: Vec<Expr>, input: Arc<LogicalPlan>) -> Result<Self> {
        let mut window_fields: Vec<DFField> = input.schema().fields().clone();
        window_fields
            .extend_from_slice(&exprlist_to_fields(window_expr.iter(), input.as_ref())?);
        let metadata = input.schema().metadata().clone();

        // Update functional dependencies for window:
        let mut window_func_dependencies =
            input.schema().functional_dependencies().clone();
        window_func_dependencies.extend_target_indices(window_fields.len());

        Ok(Window {
            input,
            window_expr,
            schema: Arc::new(
                DFSchema::new_with_metadata(window_fields, metadata)?
                    .with_functional_dependencies(window_func_dependencies),
            ),
        })
    }
}

/// Produces rows from a table provider by reference or from the context
#[derive(Clone)]
pub struct TableScan {
    /// The name of the table
    pub table_name: OwnedTableReference,
    /// The source of the table
    pub source: Arc<dyn TableSource>,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// The schema description of the output
    pub projected_schema: DFSchemaRef,
    /// Optional expressions to be used as filters by the table provider
    pub filters: Vec<Expr>,
    /// Optional number of rows to read
    pub fetch: Option<usize>,
}

impl PartialEq for TableScan {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.projection == other.projection
            && self.projected_schema == other.projected_schema
            && self.filters == other.filters
            && self.fetch == other.fetch
    }
}

impl Eq for TableScan {}

impl Hash for TableScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.projection.hash(state);
        self.projected_schema.hash(state);
        self.filters.hash(state);
        self.fetch.hash(state);
    }
}

impl TableScan {
    /// Initialize TableScan with appropriate schema from the given
    /// arguments.
    pub fn try_new(
        table_name: impl Into<OwnedTableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let table_name = table_name.into();

        if table_name.table().is_empty() {
            return plan_err!("table_name cannot be empty");
        }
        let schema = table_source.schema();
        let func_dependencies = FunctionalDependencies::new_from_constraints(
            table_source.constraints(),
            schema.fields.len(),
        );
        let projected_schema = projection
            .as_ref()
            .map(|p| {
                let projected_func_dependencies =
                    func_dependencies.project_functional_dependencies(p, p.len());
                DFSchema::new_with_metadata(
                    p.iter()
                        .map(|i| {
                            DFField::from_qualified(
                                table_name.clone(),
                                schema.field(*i).clone(),
                            )
                        })
                        .collect(),
                    schema.metadata().clone(),
                )
                .map(|df_schema| {
                    df_schema.with_functional_dependencies(projected_func_dependencies)
                })
            })
            .unwrap_or_else(|| {
                DFSchema::try_from_qualified_schema(table_name.clone(), &schema).map(
                    |df_schema| df_schema.with_functional_dependencies(func_dependencies),
                )
            })?;
        let projected_schema = Arc::new(projected_schema);
        Ok(Self {
            table_name,
            source: table_source,
            projection,
            projected_schema,
            filters,
            fetch,
        })
    }
}

/// Apply Cross Join to two logical plans
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CrossJoin {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// The output schema, containing fields from the left and right inputs
    pub schema: DFSchemaRef,
}

/// Repartition the plan based on a partitioning scheme.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Repartition {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The partitioning scheme
    pub partitioning_scheme: Partitioning,
}

/// Union multiple inputs
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Union {
    /// Inputs to merge
    pub inputs: Vec<Arc<LogicalPlan>>,
    /// Union schema. Should be the same for all inputs.
    pub schema: DFSchemaRef,
}

/// Prepare a statement but do not execute it. Prepare statements can have 0 or more
/// `Expr::Placeholder` expressions that are filled in during execution
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Prepare {
    /// The name of the statement
    pub name: String,
    /// Data types of the parameters ([`Expr::Placeholder`])
    pub data_types: Vec<DataType>,
    /// The logical plan of the statements
    pub input: Arc<LogicalPlan>,
}

/// Describe the schema of table
///
/// # Example output:
///
/// ```sql
/// ❯ describe traces;
/// +--------------------+-----------------------------+-------------+
/// | column_name        | data_type                   | is_nullable |
/// +--------------------+-----------------------------+-------------+
/// | attributes         | Utf8                        | YES         |
/// | duration_nano      | Int64                       | YES         |
/// | end_time_unix_nano | Int64                       | YES         |
/// | service.name       | Dictionary(Int32, Utf8)     | YES         |
/// | span.kind          | Utf8                        | YES         |
/// | span.name          | Utf8                        | YES         |
/// | span_id            | Dictionary(Int32, Utf8)     | YES         |
/// | time               | Timestamp(Nanosecond, None) | NO          |
/// | trace_id           | Dictionary(Int32, Utf8)     | YES         |
/// | otel.status_code   | Utf8                        | YES         |
/// | parent_span_id     | Utf8                        | YES         |
/// +--------------------+-----------------------------+-------------+
/// ```
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DescribeTable {
    /// Table schema
    pub schema: Arc<Schema>,
    /// schema of describe table output
    pub output_schema: DFSchemaRef,
}

/// Produces a relation with string representations of
/// various parts of the plan
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Explain {
    /// Should extra (detailed, intermediate plans) be included?
    pub verbose: bool,
    /// The logical plan that is being EXPLAIN'd
    pub plan: Arc<LogicalPlan>,
    /// Represent the various stages plans have gone through
    pub stringified_plans: Vec<StringifiedPlan>,
    /// The output schema of the explain (2 columns of text)
    pub schema: DFSchemaRef,
    /// Used by physical planner to check if should proceed with planning
    pub logical_optimization_succeeded: bool,
}

/// Runs the actual plan, and then prints the physical plan with
/// with execution metrics.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Analyze {
    /// Should extra detail be included?
    pub verbose: bool,
    /// The logical plan that is being EXPLAIN ANALYZE'd
    pub input: Arc<LogicalPlan>,
    /// The output schema of the explain (2 columns of text)
    pub schema: DFSchemaRef,
}

/// Extension operator defined outside of DataFusion
// TODO(clippy): This clippy `allow` should be removed if
// the manual `PartialEq` is removed in favor of a derive.
// (see `PartialEq` the impl for details.)
#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Clone, Eq, Hash)]
pub struct Extension {
    /// The runtime extension operator
    pub node: Arc<dyn UserDefinedLogicalNode>,
}

// `PartialEq` cannot be derived for types containing `Arc<dyn Trait>`.
// This manual implementation should be removed if
// https://github.com/rust-lang/rust/issues/39128 is fixed.
impl PartialEq for Extension {
    fn eq(&self, other: &Self) -> bool {
        self.node.eq(&other.node)
    }
}

/// Produces the first `n` tuples from its input and discards the rest.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Limit {
    /// Number of rows to skip before fetch
    pub skip: usize,
    /// Maximum number of rows to fetch,
    /// None means fetching all rows
    pub fetch: Option<usize>,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
}

/// Removes duplicate rows from the input
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Distinct {
    /// Plain `DISTINCT` referencing all selection expressions
    All(Arc<LogicalPlan>),
    /// The `Postgres` addition, allowing separate control over DISTINCT'd and selected columns
    On(DistinctOn),
}

/// Removes duplicate rows from the input
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DistinctOn {
    /// The `DISTINCT ON` clause expression list
    pub on_expr: Vec<Expr>,
    /// The selected projection expression list
    pub select_expr: Vec<Expr>,
    /// The `ORDER BY` clause, whose initial expressions must match those of the `ON` clause when
    /// present. Note that those matching expressions actually wrap the `ON` expressions with
    /// additional info pertaining to the sorting procedure (i.e. ASC/DESC, and NULLS FIRST/LAST).
    pub sort_expr: Option<Vec<Expr>>,
    /// The logical plan that is being DISTINCT'd
    pub input: Arc<LogicalPlan>,
    /// The schema description of the DISTINCT ON output
    pub schema: DFSchemaRef,
}

impl DistinctOn {
    /// Create a new `DistinctOn` struct.
    pub fn try_new(
        on_expr: Vec<Expr>,
        select_expr: Vec<Expr>,
        sort_expr: Option<Vec<Expr>>,
        input: Arc<LogicalPlan>,
    ) -> Result<Self> {
        if on_expr.is_empty() {
            return plan_err!("No `ON` expressions provided");
        }

        let on_expr = normalize_cols(on_expr, input.as_ref())?;

        let schema = DFSchema::new_with_metadata(
            exprlist_to_fields(&select_expr, &input)?,
            input.schema().metadata().clone(),
        )?;

        let mut distinct_on = DistinctOn {
            on_expr,
            select_expr,
            sort_expr: None,
            input,
            schema: Arc::new(schema),
        };

        if let Some(sort_expr) = sort_expr {
            distinct_on = distinct_on.with_sort_expr(sort_expr)?;
        }

        Ok(distinct_on)
    }

    /// Try to update `self` with a new sort expressions.
    ///
    /// Validates that the sort expressions are a super-set of the `ON` expressions.
    pub fn with_sort_expr(mut self, sort_expr: Vec<Expr>) -> Result<Self> {
        let sort_expr = normalize_cols(sort_expr, self.input.as_ref())?;

        // Check that the left-most sort expressions are the same as the `ON` expressions.
        let mut matched = true;
        for (on, sort) in self.on_expr.iter().zip(sort_expr.iter()) {
            match sort {
                Expr::Sort(SortExpr { expr, .. }) => {
                    if on != &**expr {
                        matched = false;
                        break;
                    }
                }
                _ => return plan_err!("Not a sort expression: {sort}"),
            }
        }

        if self.on_expr.len() > sort_expr.len() || !matched {
            return plan_err!(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
            );
        }

        self.sort_expr = Some(sort_expr);
        Ok(self)
    }
}

/// Aggregates its input based on a set of grouping and aggregate
/// expressions (e.g. SUM).
#[derive(Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: DFSchemaRef,
}

impl Aggregate {
    /// Create a new aggregate operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Self> {
        let group_expr = enumerate_grouping_sets(group_expr)?;

        let is_grouping_set = matches!(group_expr.as_slice(), [Expr::GroupingSet(_)]);

        let grouping_expr: Vec<Expr> = grouping_set_to_exprlist(group_expr.as_slice())?;

        let mut fields = exprlist_to_fields(grouping_expr.iter(), &input)?;

        // Even columns that cannot be null will become nullable when used in a grouping set.
        if is_grouping_set {
            fields = fields
                .into_iter()
                .map(|field| field.with_nullable(true))
                .collect::<Vec<_>>();
        }

        fields.extend(exprlist_to_fields(aggr_expr.iter(), &input)?);

        let schema =
            DFSchema::new_with_metadata(fields, input.schema().metadata().clone())?;

        Self::try_new_with_schema(input, group_expr, aggr_expr, Arc::new(schema))
    }

    /// Create a new aggregate operator using the provided schema to avoid the overhead of
    /// building the schema again when the schema is already known.
    ///
    /// This method should only be called when you are absolutely sure that the schema being
    /// provided is correct for the aggregate. If in doubt, call [try_new](Self::try_new) instead.
    pub fn try_new_with_schema(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        if group_expr.is_empty() && aggr_expr.is_empty() {
            return plan_err!(
                "Aggregate requires at least one grouping or aggregate expression"
            );
        }
        let group_expr_count = grouping_set_expr_count(&group_expr)?;
        if schema.fields().len() != group_expr_count + aggr_expr.len() {
            return plan_err!(
                "Aggregate schema has wrong number of fields. Expected {} got {}",
                group_expr_count + aggr_expr.len(),
                schema.fields().len()
            );
        }

        let aggregate_func_dependencies =
            calc_func_dependencies_for_aggregate(&group_expr, &input, &schema)?;
        let new_schema = schema.as_ref().clone();
        let schema = Arc::new(
            new_schema.with_functional_dependencies(aggregate_func_dependencies),
        );
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }
}

/// Checks whether any expression in `group_expr` contains `Expr::GroupingSet`.
fn contains_grouping_set(group_expr: &[Expr]) -> bool {
    group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
}

/// Calculates functional dependencies for aggregate expressions.
fn calc_func_dependencies_for_aggregate(
    // Expressions in the GROUP BY clause:
    group_expr: &[Expr],
    // Input plan of the aggregate:
    input: &LogicalPlan,
    // Aggregate schema
    aggr_schema: &DFSchema,
) -> Result<FunctionalDependencies> {
    // We can do a case analysis on how to propagate functional dependencies based on
    // whether the GROUP BY in question contains a grouping set expression:
    // - If so, the functional dependencies will be empty because we cannot guarantee
    //   that GROUP BY expression results will be unique.
    // - Otherwise, it may be possible to propagate functional dependencies.
    if !contains_grouping_set(group_expr) {
        let group_by_expr_names = group_expr
            .iter()
            .map(|item| item.display_name())
            .collect::<Result<Vec<_>>>()?;
        let aggregate_func_dependencies = aggregate_functional_dependencies(
            input.schema(),
            &group_by_expr_names,
            aggr_schema,
        );
        Ok(aggregate_func_dependencies)
    } else {
        Ok(FunctionalDependencies::empty())
    }
}

/// This function projects functional dependencies of the `input` plan according
/// to projection expressions `exprs`.
fn calc_func_dependencies_for_project(
    exprs: &[Expr],
    input: &LogicalPlan,
) -> Result<FunctionalDependencies> {
    let input_fields = input.schema().fields();
    // Calculate expression indices (if present) in the input schema.
    let proj_indices = exprs
        .iter()
        .filter_map(|expr| {
            let expr_name = match expr {
                Expr::Alias(alias) => {
                    format!("{}", alias.expr)
                }
                _ => format!("{}", expr),
            };
            input_fields
                .iter()
                .position(|item| item.qualified_name() == expr_name)
        })
        .collect::<Vec<_>>();
    Ok(input
        .schema()
        .functional_dependencies()
        .project_functional_dependencies(&proj_indices, exprs.len()))
}

/// Sorts its input according to a list of sort expressions.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Sort {
    /// The sort expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Optional fetch limit
    pub fetch: Option<usize>,
}

/// Join two logical plans on one or more join columns
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// Equijoin clause expressed as pairs of (left, right) join expressions
    pub on: Vec<(Expr, Expr)>,
    /// Filters applied during join (non-equi conditions)
    pub filter: Option<Expr>,
    /// Join type
    pub join_type: JoinType,
    /// Join constraint
    pub join_constraint: JoinConstraint,
    /// The output schema, containing fields from the left and right inputs
    pub schema: DFSchemaRef,
    /// If null_equals_null is true, null == null else null != null
    pub null_equals_null: bool,
}

impl Join {
    /// Create Join with input which wrapped with projection, this method is used to help create physical join.
    pub fn try_new_with_project_input(
        original: &LogicalPlan,
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        column_on: (Vec<Column>, Vec<Column>),
    ) -> Result<Self> {
        let original_join = match original {
            LogicalPlan::Join(join) => join,
            _ => return plan_err!("Could not create join with project input"),
        };

        let on: Vec<(Expr, Expr)> = column_on
            .0
            .into_iter()
            .zip(column_on.1)
            .map(|(l, r)| (Expr::Column(l), Expr::Column(r)))
            .collect();
        let join_schema =
            build_join_schema(left.schema(), right.schema(), &original_join.join_type)?;

        Ok(Join {
            left,
            right,
            on,
            filter: original_join.filter.clone(),
            join_type: original_join.join_type,
            join_constraint: original_join.join_constraint,
            schema: Arc::new(join_schema),
            null_equals_null: original_join.null_equals_null,
        })
    }
}

/// Subquery
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Subquery {
    /// The subquery
    pub subquery: Arc<LogicalPlan>,
    /// The outer references used in the subquery
    pub outer_ref_columns: Vec<Expr>,
}

impl Subquery {
    pub fn try_from_expr(plan: &Expr) -> Result<&Subquery> {
        match plan {
            Expr::ScalarSubquery(it) => Ok(it),
            Expr::Cast(cast) => Subquery::try_from_expr(cast.expr.as_ref()),
            _ => plan_err!("Could not coerce into ScalarSubquery!"),
        }
    }

    pub fn with_plan(&self, plan: Arc<LogicalPlan>) -> Subquery {
        Subquery {
            subquery: plan,
            outer_ref_columns: self.outer_ref_columns.clone(),
        }
    }
}

impl Debug for Subquery {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "<subquery>")
    }
}

/// Logical partitioning schemes supported by the repartition operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number
    /// of partitions.
    Hash(Vec<Expr>, usize),
    /// The DISTRIBUTE BY clause is used to repartition the data based on the input expressions
    DistributeBy(Vec<Expr>),
}

/// Unnest a column that contains a nested list type. See
/// [`UnnestOptions`] for more details.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unnest {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The column to unnest
    pub column: Column,
    /// The output schema, containing the unnested field column.
    pub schema: DFSchemaRef,
    /// Options
    pub options: UnnestOptions,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::table_scan;
    use crate::{col, count, exists, in_subquery, lit, placeholder, GroupingSet};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::tree_node::TreeNodeVisitor;
    use datafusion_common::{not_impl_err, DFSchema, TableReference};
    use std::collections::HashMap;

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    fn display_plan() -> Result<LogicalPlan> {
        let plan1 = table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3]))?
            .build()?;

        table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
            .filter(in_subquery(col("state"), Arc::new(plan1)))?
            .project(vec![col("id")])?
            .build()
    }

    #[test]
    fn test_display_indent() -> Result<()> {
        let plan = display_plan()?;

        let expected = "Projection: employee_csv.id\
        \n  Filter: employee_csv.state IN (<subquery>)\
        \n    Subquery:\
        \n      TableScan: employee_csv projection=[state]\
        \n    TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{}", plan.display_indent()));
        Ok(())
    }

    #[test]
    fn test_display_indent_schema() -> Result<()> {
        let plan = display_plan()?;

        let expected = "Projection: employee_csv.id [id:Int32]\
        \n  Filter: employee_csv.state IN (<subquery>) [id:Int32, state:Utf8]\
        \n    Subquery: [state:Utf8]\
        \n      TableScan: employee_csv projection=[state] [state:Utf8]\
        \n    TableScan: employee_csv projection=[id, state] [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
        Ok(())
    }

    #[test]
    fn test_display_subquery_alias() -> Result<()> {
        let plan1 = table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3]))?
            .build()?;
        let plan1 = Arc::new(plan1);

        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .project(vec![col("id"), exists(plan1).alias("exists")])?
                .build();

        let expected = "Projection: employee_csv.id, EXISTS (<subquery>) AS exists\
        \n  Subquery:\
        \n    TableScan: employee_csv projection=[state]\
        \n  TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{}", plan?.display_indent()));
        Ok(())
    }

    #[test]
    fn test_display_graphviz() -> Result<()> {
        let plan = display_plan()?;

        let expected_graphviz = r#"
// Begin DataFusion GraphViz Plan,
// display it online here: https://dreampuf.github.io/GraphvizOnline

digraph {
  subgraph cluster_1
  {
    graph[label="LogicalPlan"]
    2[shape=box label="Projection: employee_csv.id"]
    3[shape=box label="Filter: employee_csv.state IN (<subquery>)"]
    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
    4[shape=box label="Subquery:"]
    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]
    5[shape=box label="TableScan: employee_csv projection=[state]"]
    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]
    6[shape=box label="TableScan: employee_csv projection=[id, state]"]
    3 -> 6 [arrowhead=none, arrowtail=normal, dir=back]
  }
  subgraph cluster_7
  {
    graph[label="Detailed LogicalPlan"]
    8[shape=box label="Projection: employee_csv.id\nSchema: [id:Int32]"]
    9[shape=box label="Filter: employee_csv.state IN (<subquery>)\nSchema: [id:Int32, state:Utf8]"]
    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]
    10[shape=box label="Subquery:\nSchema: [state:Utf8]"]
    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]
    11[shape=box label="TableScan: employee_csv projection=[state]\nSchema: [state:Utf8]"]
    10 -> 11 [arrowhead=none, arrowtail=normal, dir=back]
    12[shape=box label="TableScan: employee_csv projection=[id, state]\nSchema: [id:Int32, state:Utf8]"]
    9 -> 12 [arrowhead=none, arrowtail=normal, dir=back]
  }
}
// End DataFusion GraphViz Plan
"#;

        // just test for a few key lines in the output rather than the
        // whole thing to make test mainteance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert_eq!(expected_graphviz, graphviz);
        Ok(())
    }

    /// Tests for the Visitor trait and walking logical plan nodes
    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }

    impl TreeNodeVisitor for OkVisitor {
        type N = LogicalPlan;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => {
                    return not_impl_err!("unknown plan type");
                }
            };

            self.strings.push(s.into());
            Ok(VisitRecursion::Continue)
        }

        fn post_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => {
                    return not_impl_err!("unknown plan type");
                }
            };

            self.strings.push(s.into());
            Ok(VisitRecursion::Continue)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.visit(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
                "post_visit Filter",
                "post_visit Projection",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Counter than counts to zero and returns true when it gets there
    struct OptionalCounter {
        val: Option<usize>,
    }

    impl OptionalCounter {
        fn new(val: usize) -> Self {
            Self { val: Some(val) }
        }
        // Decrements the counter by 1, if any, returning true if it hits zero
        fn dec(&mut self) -> bool {
            if Some(0) == self.val {
                true
            } else {
                self.val = self.val.take().map(|i| i - 1);
                false
            }
        }
    }

    #[derive(Debug, Default)]
    /// Visitor that returns false after some number of visits
    struct StoppingVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_false_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_false_from_post_in: OptionalCounter,
    }

    impl TreeNodeVisitor for StoppingVisitor {
        type N = LogicalPlan;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            if self.return_false_from_pre_in.dec() {
                return Ok(VisitRecursion::Stop);
            }
            self.inner.pre_visit(plan)?;

            Ok(VisitRecursion::Continue)
        }

        fn post_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            if self.return_false_from_post_in.dec() {
                return Ok(VisitRecursion::Stop);
            }

            self.inner.post_visit(plan)
        }
    }

    /// test early stopping in pre-visit
    #[test]
    fn early_stopping_pre_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter"]
        );
    }

    #[test]
    fn early_stopping_post_visit() {
        let mut visitor = StoppingVisitor {
            return_false_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Visitor that returns an error after some number of visits
    struct ErrorVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_error_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_error_from_post_in: OptionalCounter,
    }

    impl TreeNodeVisitor for ErrorVisitor {
        type N = LogicalPlan;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            if self.return_error_from_pre_in.dec() {
                return not_impl_err!("Error in pre_visit");
            }

            self.inner.pre_visit(plan)
        }

        fn post_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            if self.return_error_from_post_in.dec() {
                return not_impl_err!("Error in post_visit");
            }

            self.inner.post_visit(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit(&mut visitor).unwrap_err();
        assert_eq!(
            "This feature is not implemented: Error in pre_visit",
            res.strip_backtrace()
        );
        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter"]
        );
    }

    #[test]
    fn error_post_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_post_in: OptionalCounter::new(1),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit(&mut visitor).unwrap_err();
        assert_eq!(
            "This feature is not implemented: Error in post_visit",
            res.strip_backtrace()
        );
        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[test]
    fn projection_expr_schema_mismatch() -> Result<()> {
        let empty_schema = Arc::new(DFSchema::new_with_metadata(vec![], HashMap::new())?);
        let p = Projection::try_new_with_schema(
            vec![col("a")],
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: empty_schema.clone(),
            })),
            empty_schema,
        );
        assert_eq!(p.err().unwrap().strip_backtrace(), "Error during planning: Projection has mismatch between number of expressions (1) and number of fields in schema (0)");
        Ok(())
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("state", DataType::Utf8, false),
        ]);

        table_scan(TableReference::none(), &schema, Some(vec![0, 1]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }

    /// Extension plan that panic when trying to access its input plan
    #[derive(Debug)]
    struct NoChildExtension {
        empty_schema: DFSchemaRef,
    }

    impl NoChildExtension {
        fn empty() -> Self {
            Self {
                empty_schema: Arc::new(DFSchema::empty()),
            }
        }
    }

    impl UserDefinedLogicalNode for NoChildExtension {
        fn as_any(&self) -> &dyn std::any::Any {
            unimplemented!()
        }

        fn name(&self) -> &str {
            unimplemented!()
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            panic!("Should not be called")
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.empty_schema
        }

        fn expressions(&self) -> Vec<Expr> {
            unimplemented!()
        }

        fn fmt_for_explain(&self, _: &mut fmt::Formatter) -> fmt::Result {
            unimplemented!()
        }

        fn from_template(
            &self,
            _: &[Expr],
            _: &[LogicalPlan],
        ) -> Arc<dyn UserDefinedLogicalNode> {
            unimplemented!()
        }

        fn dyn_hash(&self, _: &mut dyn Hasher) {
            unimplemented!()
        }

        fn dyn_eq(&self, _: &dyn UserDefinedLogicalNode) -> bool {
            unimplemented!()
        }
    }

    #[test]
    #[allow(deprecated)]
    fn test_extension_all_schemas() {
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoChildExtension::empty()),
        });

        let schemas = plan.all_schemas();
        assert_eq!(1, schemas.len());
        assert_eq!(0, schemas[0].fields().len());
    }

    #[test]
    fn test_replace_invalid_placeholder() {
        // test empty placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("")))
            .unwrap()
            .build()
            .unwrap();

        plan.replace_params_with_values(&[42i32.into()])
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");

        // test $0 placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("$0")))
            .unwrap()
            .build()
            .unwrap();

        plan.replace_params_with_values(&[42i32.into()])
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");
    }

    #[test]
    fn test_nullable_schema_after_grouping_set() {
        let schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Int32, false),
        ]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .aggregate(
                vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
                    vec![col("foo")],
                    vec![col("bar")],
                ]))],
                vec![count(lit(true))],
            )
            .unwrap()
            .build()
            .unwrap();

        let output_schema = plan.schema();

        assert!(output_schema
            .field_with_name(None, "foo")
            .unwrap()
            .is_nullable(),);
        assert!(output_schema
            .field_with_name(None, "bar")
            .unwrap()
            .is_nullable());
    }
}
