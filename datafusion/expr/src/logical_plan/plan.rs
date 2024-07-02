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
use crate::builder::{change_redundant_column, unnest_with_options};
use crate::expr::{Alias, Placeholder, Sort as SortExpr, WindowFunction};
use crate::expr_rewriter::{create_col_from_scalar_expr, normalize_cols};
use crate::logical_plan::display::{GraphvizVisitor, IndentVisitor};
use crate::logical_plan::extension::UserDefinedLogicalNode;
use crate::logical_plan::{DmlStatement, Statement};
use crate::utils::{
    enumerate_grouping_sets, exprlist_to_fields, find_out_reference_exprs,
    grouping_set_expr_count, grouping_set_to_exprlist, split_conjunction,
};
use crate::{
    build_join_schema, expr_vec_fmt, BinaryExpr, BuiltInWindowFunction,
    CreateMemoryTable, CreateView, Expr, ExprSchemable, LogicalPlanBuilder, Operator,
    TableProviderFilterPushDown, TableSource, WindowFunctionDefinition,
};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    aggregate_functional_dependencies, internal_err, plan_err, Column, Constraints,
    DFSchema, DFSchemaRef, DataFusionError, Dependency, FunctionalDependence,
    FunctionalDependencies, ParamValues, Result, TableReference, UnnestOptions,
};

// backwards compatibility
use crate::display::PgJsonVisitor;
use crate::logical_plan::tree_node::unwrap_arc;
pub use datafusion_common::display::{PlanType, StringifiedPlan, ToStringifiedPlan};
pub use datafusion_common::{JoinConstraint, JoinType};

/// A `LogicalPlan` is a node in a tree of relational operators (such as
/// Projection or Filter).
///
/// Represents transforming an input relation (table) to an output relation
/// (table) with a potentially different schema. Plans form a dataflow tree
/// where data flows from leaves up to the root to produce the query result.
///
/// `LogicalPlan`s can be created by the SQL query planner, the DataFrame API,
/// or programmatically (for example custom query languages).
///
/// # See also:
/// * [`Expr`]: For the expressions that are evaluated by the plan
/// * [`LogicalPlanBuilder`]: For building `LogicalPlan`s
/// * [`tree_node`]: To inspect and rewrite `LogicalPlan`s
///
/// [`tree_node`]: crate::logical_plan::tree_node
///
/// # Examples
///
/// ## Creating a LogicalPlan from SQL:
///
/// See [`SessionContext::sql`](https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.sql)
///
/// ## Creating a LogicalPlan from the DataFrame API:
///
/// See [`DataFrame::logical_plan`](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html#method.logical_plan)
///
/// ## Creating a LogicalPlan programmatically:
///
/// See [`LogicalPlanBuilder`]
///
/// # Visiting and Rewriting `LogicalPlan`s
///
/// Using the [`tree_node`] API, you can recursively walk all nodes in a
/// `LogicalPlan`. For example, to find all column references in a plan:
///
/// ```
/// # use std::collections::HashSet;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_expr::{Expr, col, lit, LogicalPlan, LogicalPlanBuilder, table_scan};
/// # use datafusion_common::tree_node::{TreeNodeRecursion, TreeNode};
/// # use datafusion_common::{Column, Result};
/// # fn employee_schema() -> Schema {
/// #    Schema::new(vec![
/// #           Field::new("name", DataType::Utf8, false),
/// #           Field::new("salary", DataType::Int32, false),
/// #       ])
/// #   }
/// // Projection(name, salary)
/// //   Filter(salary > 1000)
/// //     TableScan(employee)
/// # fn main() -> Result<()> {
/// let plan = table_scan(Some("employee"), &employee_schema(), None)?
///  .filter(col("salary").gt(lit(1000)))?
///  .project(vec![col("name")])?
///  .build()?;
///
/// // use apply to walk the plan and collect all expressions
/// let mut expressions = HashSet::new();
/// plan.apply(|node| {
///   // collect all expressions in the plan
///   node.apply_expressions(|expr| {
///    expressions.insert(expr.clone());
///    Ok(TreeNodeRecursion::Continue) // control walk of expressions
///   })?;
///   Ok(TreeNodeRecursion::Continue) // control walk of plan nodes
/// }).unwrap();
///
/// // we found the expression in projection and filter
/// assert_eq!(expressions.len(), 2);
/// println!("Found expressions: {:?}", expressions);
/// // found predicate in the Filter: employee.salary > 1000
/// let salary = Expr::Column(Column::new(Some("employee"), "salary"));
/// assert!(expressions.contains(&salary.gt(lit(1000))));
/// // found projection in the Projection: employee.name
/// let name = Expr::Column(Column::new(Some("employee"), "name"));
/// assert!(expressions.contains(&name));
/// # Ok(())
/// # }
/// ```
///
/// You can also rewrite plans using the [`tree_node`] API. For example, to
/// replace the filter predicate in a plan:
///
/// ```
/// # use std::collections::HashSet;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_expr::{Expr, col, lit, LogicalPlan, LogicalPlanBuilder, table_scan};
/// # use datafusion_common::tree_node::{TreeNodeRecursion, TreeNode};
/// # use datafusion_common::{Column, Result};
/// # fn employee_schema() -> Schema {
/// #    Schema::new(vec![
/// #           Field::new("name", DataType::Utf8, false),
/// #           Field::new("salary", DataType::Int32, false),
/// #       ])
/// #   }
/// // Projection(name, salary)
/// //   Filter(salary > 1000)
/// //     TableScan(employee)
/// # fn main() -> Result<()> {
/// use datafusion_common::tree_node::Transformed;
/// let plan = table_scan(Some("employee"), &employee_schema(), None)?
///  .filter(col("salary").gt(lit(1000)))?
///  .project(vec![col("name")])?
///  .build()?;
///
/// // use transform to rewrite the plan
/// let transformed_result = plan.transform(|node| {
///   // when we see the filter node
///   if let LogicalPlan::Filter(mut filter) = node {
///     // replace predicate with salary < 2000
///     filter.predicate = Expr::Column(Column::new(Some("employee"), "salary")).lt(lit(2000));
///     let new_plan = LogicalPlan::Filter(filter);
///     return Ok(Transformed::yes(new_plan)); // communicate the node was changed
///   }
///   // return the node unchanged
///   Ok(Transformed::no(node))
/// }).unwrap();
///
/// // Transformed result contains rewritten plan and information about
/// // whether the plan was changed
/// assert!(transformed_result.transformed);
/// let rewritten_plan = transformed_result.data;
///
/// // we found the filter
/// assert_eq!(rewritten_plan.display_indent().to_string(),
/// "Projection: employee.name\
/// \n  Filter: employee.salary < Int32(2000)\
/// \n    TableScan: employee");
/// # Ok(())
/// # }
/// ```
///
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

impl Default for LogicalPlan {
    fn default() -> Self {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }
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
            LogicalPlan::Dml(DmlStatement { output_schema, .. }) => output_schema,
            LogicalPlan::Copy(CopyTo { input, .. }) => input.schema(),
            LogicalPlan::Ddl(ddl) => ddl.schema(),
            LogicalPlan::Unnest(Unnest { schema, .. }) => schema,
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

    /// Returns all expressions (non-recursively) evaluated by the current
    /// logical plan node. This does not include expressions in any children.
    ///
    /// Note this method `clone`s all the expressions. When possible, the
    /// [`tree_node`] API should be used instead of this API.
    ///
    /// The returned expressions do not necessarily represent or even
    /// contributed to the output schema of this node. For example,
    /// `LogicalPlan::Filter` returns the filter expression even though the
    /// output of a Filter has the same columns as the input.
    ///
    /// The expressions do contain all the columns that are used by this plan,
    /// so if there are columns not referenced by these expressions then
    /// DataFusion's optimizer attempts to optimize them away.
    ///
    /// [`tree_node`]: crate::logical_plan::tree_node
    pub fn expressions(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.apply_expressions(|e| {
            exprs.push(e.clone());
            Ok(TreeNodeRecursion::Continue)
        })
        // closure always returns OK
        .unwrap();
        exprs
    }

    /// Returns all the out reference(correlated) expressions (recursively) in the current
    /// logical plan nodes and all its descendant nodes.
    pub fn all_out_ref_exprs(self: &LogicalPlan) -> Vec<Expr> {
        let mut exprs = vec![];
        self.apply_expressions(|e| {
            find_out_reference_exprs(e).into_iter().for_each(|e| {
                if !exprs.contains(&e) {
                    exprs.push(e)
                }
            });
            Ok(TreeNodeRecursion::Continue)
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

    #[deprecated(since = "37.0.0", note = "Use `apply_expressions` instead")]
    pub fn inspect_expressions<F, E>(self: &LogicalPlan, mut f: F) -> Result<(), E>
    where
        F: FnMut(&Expr) -> Result<(), E>,
    {
        let mut err = Ok(());
        self.apply_expressions(|e| {
            if let Err(e) = f(e) {
                // save the error for later (it may not be a DataFusionError
                err = Err(e);
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        // The closure always returns OK, so this will always too
        .expect("no way to return error during recursion");

        err
    }

    /// Returns all inputs / children of this `LogicalPlan` node.
    ///
    /// Note does not include inputs to inputs, or subqueries.
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
            | LogicalPlan::Statement { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => vec![],
        }
    }

    /// returns all `Using` join columns in a logical plan
    pub fn using_columns(&self) -> Result<Vec<HashSet<Column>>, DataFusionError> {
        let mut using_columns: Vec<HashSet<Column>> = vec![];

        self.apply_with_subqueries(|plan| {
            if let LogicalPlan::Join(Join {
                join_constraint: JoinConstraint::Using,
                on,
                ..
            }) = plan
            {
                // The join keys in using-join must be columns.
                let columns =
                    on.iter().try_fold(HashSet::new(), |mut accumu, (l, r)| {
                        let Some(l) = l.try_as_col().cloned() else {
                            return internal_err!(
                                "Invalid join key. Expected column, found {l:?}"
                            );
                        };
                        let Some(r) = r.try_as_col().cloned() else {
                            return internal_err!(
                                "Invalid join key. Expected column, found {r:?}"
                            );
                        };
                        accumu.insert(l);
                        accumu.insert(r);
                        Result::<_, DataFusionError>::Ok(accumu)
                    })?;
                using_columns.push(columns);
            }
            Ok(TreeNodeRecursion::Continue)
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
            LogicalPlan::Union(union) => Ok(Some(Expr::Column(Column::from(
                union.schema.qualified_field(0),
            )))),
            LogicalPlan::TableScan(table) => Ok(Some(Expr::Column(Column::from(
                table.projected_schema.qualified_field(0),
            )))),
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
    #[deprecated(since = "35.0.0", note = "please use `with_new_exprs` instead")]
    pub fn with_new_inputs(&self, inputs: &[LogicalPlan]) -> Result<LogicalPlan> {
        self.with_new_exprs(self.expressions(), inputs.to_vec())
    }

    /// Recomputes schema and type information for this LogicalPlan if needed.
    ///
    /// Some `LogicalPlan`s may need to recompute their schema if the number or
    /// type of expressions have been changed (for example due to type
    /// coercion). For example [`LogicalPlan::Projection`]s schema depends on
    /// its expressions.
    ///
    /// Some `LogicalPlan`s schema is unaffected by any changes to their
    /// expressions. For example [`LogicalPlan::Filter`] schema is always the
    /// same as its input schema.
    ///
    /// This is useful after modifying a plans `Expr`s (or input plans) via
    /// methods such as [Self::map_children] and [Self::map_expressions]. Unlike
    /// [Self::with_new_exprs], this method does not require a new set of
    /// expressions or inputs plans.
    ///
    /// # Return value
    /// Returns an error if there is some issue recomputing the schema.
    ///
    /// # Notes
    ///
    /// * Does not recursively recompute schema for input (child) plans.
    pub fn recompute_schema(self) -> Result<Self> {
        match self {
            // Since expr may be different than the previous expr, schema of the projection
            // may change. We need to use try_new method instead of try_new_with_schema method.
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema: _,
            }) => Projection::try_new(expr, input).map(LogicalPlan::Projection),
            LogicalPlan::Dml(_) => Ok(self),
            LogicalPlan::Copy(_) => Ok(self),
            LogicalPlan::Values(Values { schema, values }) => {
                // todo it isn't clear why the schema is not recomputed here
                Ok(LogicalPlan::Values(Values { schema, values }))
            }
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // todo: should this logic be moved to Filter::try_new?

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

                let predicate = predicate
                    .transform_down(|expr| {
                        match expr {
                            Expr::Exists { .. }
                            | Expr::ScalarSubquery(_)
                            | Expr::InSubquery(_) => {
                                // subqueries could contain aliases so we don't recurse into those
                                Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump))
                            }
                            Expr::Alias(_) => Ok(Transformed::new(
                                expr.unalias(),
                                true,
                                TreeNodeRecursion::Jump,
                            )),
                            _ => Ok(Transformed::no(expr)),
                        }
                    })
                    .data()?;

                Filter::try_new(predicate, input).map(LogicalPlan::Filter)
            }
            LogicalPlan::Repartition(_) => Ok(self),
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema: _,
            }) => Window::try_new(window_expr, input).map(LogicalPlan::Window),
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema: _,
            }) => Aggregate::try_new(input, group_expr, aggr_expr)
                .map(LogicalPlan::Aggregate),
            LogicalPlan::Sort(_) => Ok(self),
            LogicalPlan::Join(Join {
                left,
                right,
                filter,
                join_type,
                join_constraint,
                on,
                schema: _,
                null_equals_null,
            }) => {
                let schema =
                    build_join_schema(left.schema(), right.schema(), &join_type)?;

                let new_on: Vec<_> = on
                    .into_iter()
                    .map(|equi_expr| {
                        // SimplifyExpression rule may add alias to the equi_expr.
                        (equi_expr.0.unalias(), equi_expr.1.unalias())
                    })
                    .collect();

                Ok(LogicalPlan::Join(Join {
                    left,
                    right,
                    join_type,
                    join_constraint,
                    on: new_on,
                    filter,
                    schema: DFSchemaRef::new(schema),
                    null_equals_null,
                }))
            }
            LogicalPlan::CrossJoin(CrossJoin {
                left,
                right,
                schema: _,
            }) => {
                let join_schema =
                    build_join_schema(left.schema(), right.schema(), &JoinType::Inner)?;

                Ok(LogicalPlan::CrossJoin(CrossJoin {
                    left,
                    right,
                    schema: join_schema.into(),
                }))
            }
            LogicalPlan::Subquery(_) => Ok(self),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema: _,
            }) => SubqueryAlias::try_new(input, alias).map(LogicalPlan::SubqueryAlias),
            LogicalPlan::Limit(_) => Ok(self),
            LogicalPlan::Ddl(_) => Ok(self),
            LogicalPlan::Extension(Extension { node }) => {
                // todo make an API that does not require cloning
                // This requires a copy of the extension nodes expressions and inputs
                let expr = node.expressions();
                let inputs: Vec<_> = node.inputs().into_iter().cloned().collect();
                Ok(LogicalPlan::Extension(Extension {
                    node: node.with_exprs_and_inputs(expr, inputs)?,
                }))
            }
            LogicalPlan::Union(Union { inputs, schema }) => {
                let input_schema = inputs[0].schema();
                // If inputs are not pruned do not change schema
                // TODO this seems wrong (shouldn't we always use the schema of the input?)
                let schema = if schema.fields().len() == input_schema.fields().len() {
                    Arc::clone(&schema)
                } else {
                    Arc::clone(input_schema)
                };
                Ok(LogicalPlan::Union(Union { inputs, schema }))
            }
            LogicalPlan::Distinct(distinct) => {
                let distinct = match distinct {
                    Distinct::All(input) => Distinct::All(input),
                    Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                        schema: _,
                    }) => Distinct::On(DistinctOn::try_new(
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                    )?),
                };
                Ok(LogicalPlan::Distinct(distinct))
            }
            LogicalPlan::RecursiveQuery(_) => Ok(self),
            LogicalPlan::Analyze(_) => Ok(self),
            LogicalPlan::Explain(_) => Ok(self),
            LogicalPlan::Prepare(_) => Ok(self),
            LogicalPlan::TableScan(_) => Ok(self),
            LogicalPlan::EmptyRelation(_) => Ok(self),
            LogicalPlan::Statement(_) => Ok(self),
            LogicalPlan::DescribeTable(_) => Ok(self),
            LogicalPlan::Unnest(Unnest {
                input,
                exec_columns,
                options,
                ..
            }) => {
                // Update schema with unnested column type.
                unnest_with_options(unwrap_arc(input), exec_columns, options)
            }
        }
    }

    /// Returns a new `LogicalPlan` based on `self` with inputs and
    /// expressions replaced.
    ///
    /// Note this method creates an entirely new node, which requires a large
    /// amount of clone'ing. When possible, the [`tree_node`] API should be used
    /// instead of this API.
    ///
    /// The exprs correspond to the same order of expressions returned
    /// by [`Self::expressions`]. This function is used by optimizers
    /// to rewrite plans using the following pattern:
    ///
    /// [`tree_node`]: crate::logical_plan::tree_node
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
    pub fn with_new_exprs(
        &self,
        mut expr: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<LogicalPlan> {
        match self {
            // Since expr may be different than the previous expr, schema of the projection
            // may change. We need to use try_new method instead of try_new_with_schema method.
            LogicalPlan::Projection(Projection { .. }) => {
                Projection::try_new(expr, Arc::new(inputs.swap_remove(0)))
                    .map(LogicalPlan::Projection)
            }
            LogicalPlan::Dml(DmlStatement {
                table_name,
                table_schema,
                op,
                ..
            }) => Ok(LogicalPlan::Dml(DmlStatement::new(
                table_name.clone(),
                Arc::clone(table_schema),
                op.clone(),
                Arc::new(inputs.swap_remove(0)),
            ))),
            LogicalPlan::Copy(CopyTo {
                input: _,
                output_url,
                file_type,
                options,
                partition_by,
            }) => Ok(LogicalPlan::Copy(CopyTo {
                input: Arc::new(inputs.swap_remove(0)),
                output_url: output_url.clone(),
                file_type: Arc::clone(file_type),
                options: options.clone(),
                partition_by: partition_by.clone(),
            })),
            LogicalPlan::Values(Values { schema, .. }) => {
                Ok(LogicalPlan::Values(Values {
                    schema: Arc::clone(schema),
                    values: expr
                        .chunks_exact(schema.fields().len())
                        .map(|s| s.to_vec())
                        .collect(),
                }))
            }
            LogicalPlan::Filter { .. } => {
                assert_eq!(1, expr.len());
                let predicate = expr.pop().unwrap().unalias_nested().data;

                Filter::try_new(predicate, Arc::new(inputs.swap_remove(0)))
                    .map(LogicalPlan::Filter)
            }
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::RoundRobinBatch(n) => {
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::RoundRobinBatch(*n),
                        input: Arc::new(inputs.swap_remove(0)),
                    }))
                }
                Partitioning::Hash(_, n) => Ok(LogicalPlan::Repartition(Repartition {
                    partitioning_scheme: Partitioning::Hash(expr, *n),
                    input: Arc::new(inputs.swap_remove(0)),
                })),
                Partitioning::DistributeBy(_) => {
                    Ok(LogicalPlan::Repartition(Repartition {
                        partitioning_scheme: Partitioning::DistributeBy(expr),
                        input: Arc::new(inputs.swap_remove(0)),
                    }))
                }
            },
            LogicalPlan::Window(Window { window_expr, .. }) => {
                assert_eq!(window_expr.len(), expr.len());
                Window::try_new(expr, Arc::new(inputs.swap_remove(0)))
                    .map(LogicalPlan::Window)
            }
            LogicalPlan::Aggregate(Aggregate { group_expr, .. }) => {
                // group exprs are the first expressions
                let agg_expr = expr.split_off(group_expr.len());

                Aggregate::try_new(Arc::new(inputs.swap_remove(0)), expr, agg_expr)
                    .map(LogicalPlan::Aggregate)
            }
            LogicalPlan::Sort(Sort { fetch, .. }) => Ok(LogicalPlan::Sort(Sort {
                expr,
                input: Arc::new(inputs.swap_remove(0)),
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
                let new_on = expr.into_iter().map(|equi_expr| {
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
                    left: Arc::new(inputs.swap_remove(0)),
                    right: Arc::new(inputs.swap_remove(0)),
                    join_type: *join_type,
                    join_constraint: *join_constraint,
                    on: new_on,
                    filter: filter_expr,
                    schema: DFSchemaRef::new(schema),
                    null_equals_null: *null_equals_null,
                }))
            }
            LogicalPlan::CrossJoin(_) => {
                let left = inputs.swap_remove(0);
                let right = inputs.swap_remove(0);
                LogicalPlanBuilder::from(left).cross_join(right)?.build()
            }
            LogicalPlan::Subquery(Subquery {
                outer_ref_columns, ..
            }) => {
                let subquery = LogicalPlanBuilder::from(inputs.swap_remove(0)).build()?;
                Ok(LogicalPlan::Subquery(Subquery {
                    subquery: Arc::new(subquery),
                    outer_ref_columns: outer_ref_columns.clone(),
                }))
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
                SubqueryAlias::try_new(Arc::new(inputs.swap_remove(0)), alias.clone())
                    .map(LogicalPlan::SubqueryAlias)
            }
            LogicalPlan::Limit(Limit { skip, fetch, .. }) => {
                Ok(LogicalPlan::Limit(Limit {
                    skip: *skip,
                    fetch: *fetch,
                    input: Arc::new(inputs.swap_remove(0)),
                }))
            }
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(CreateMemoryTable {
                name,
                if_not_exists,
                or_replace,
                column_defaults,
                ..
            })) => Ok(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
                CreateMemoryTable {
                    input: Arc::new(inputs.swap_remove(0)),
                    constraints: Constraints::empty(),
                    name: name.clone(),
                    if_not_exists: *if_not_exists,
                    or_replace: *or_replace,
                    column_defaults: column_defaults.clone(),
                },
            ))),
            LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                name,
                or_replace,
                definition,
                ..
            })) => Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                input: Arc::new(inputs.swap_remove(0)),
                name: name.clone(),
                or_replace: *or_replace,
                definition: definition.clone(),
            }))),
            LogicalPlan::Extension(e) => Ok(LogicalPlan::Extension(Extension {
                node: e.node.with_exprs_and_inputs(expr, inputs)?,
            })),
            LogicalPlan::Union(Union { schema, .. }) => {
                let input_schema = inputs[0].schema();
                // If inputs are not pruned do not change schema.
                let schema = if schema.fields().len() == input_schema.fields().len() {
                    Arc::clone(schema)
                } else {
                    Arc::clone(input_schema)
                };
                Ok(LogicalPlan::Union(Union {
                    inputs: inputs.into_iter().map(Arc::new).collect(),
                    schema,
                }))
            }
            LogicalPlan::Distinct(distinct) => {
                let distinct = match distinct {
                    Distinct::All(_) => Distinct::All(Arc::new(inputs.swap_remove(0))),
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
                            Arc::new(inputs.swap_remove(0)),
                        )?)
                    }
                };
                Ok(LogicalPlan::Distinct(distinct))
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name, is_distinct, ..
            }) => Ok(LogicalPlan::RecursiveQuery(RecursiveQuery {
                name: name.clone(),
                static_term: Arc::new(inputs.swap_remove(0)),
                recursive_term: Arc::new(inputs.swap_remove(0)),
                is_distinct: *is_distinct,
            })),
            LogicalPlan::Analyze(a) => {
                assert!(expr.is_empty());
                assert_eq!(inputs.len(), 1);
                Ok(LogicalPlan::Analyze(Analyze {
                    verbose: a.verbose,
                    schema: Arc::clone(&a.schema),
                    input: Arc::new(inputs.swap_remove(0)),
                }))
            }
            LogicalPlan::Explain(e) => {
                assert!(
                    expr.is_empty(),
                    "Invalid EXPLAIN command. Expression should empty"
                );
                assert_eq!(inputs.len(), 1, "Invalid EXPLAIN command. Inputs are empty");
                Ok(LogicalPlan::Explain(Explain {
                    verbose: e.verbose,
                    plan: Arc::new(inputs.swap_remove(0)),
                    stringified_plans: e.stringified_plans.clone(),
                    schema: Arc::clone(&e.schema),
                    logical_optimization_succeeded: e.logical_optimization_succeeded,
                }))
            }
            LogicalPlan::Prepare(Prepare {
                name, data_types, ..
            }) => Ok(LogicalPlan::Prepare(Prepare {
                name: name.clone(),
                data_types: data_types.clone(),
                input: Arc::new(inputs.swap_remove(0)),
            })),
            LogicalPlan::TableScan(ts) => {
                assert!(inputs.is_empty(), "{self:?}  should have no inputs");
                Ok(LogicalPlan::TableScan(TableScan {
                    filters: expr,
                    ..ts.clone()
                }))
            }
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Statement(_) => {
                // All of these plan types have no inputs / exprs so should not be called
                assert!(expr.is_empty(), "{self:?} should have no exprs");
                assert!(inputs.is_empty(), "{self:?}  should have no inputs");
                Ok(self.clone())
            }
            LogicalPlan::DescribeTable(_) => Ok(self.clone()),
            LogicalPlan::Unnest(Unnest {
                exec_columns: columns,
                options,
                ..
            }) => {
                // Update schema with unnested column type.
                let input = inputs.swap_remove(0);
                let new_plan =
                    unnest_with_options(input, columns.clone(), options.clone())?;
                Ok(new_plan)
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
    /// assert_eq!(
    ///   "Filter: t1.id = $1\
    ///   \n  TableScan: t1",
    ///   plan.display_indent().to_string()
    /// );
    ///
    /// // Fill in the parameter $1 with a literal 3
    /// let plan = plan.with_param_values(vec![
    ///   ScalarValue::from(3i32) // value at index 0 --> $1
    /// ]).unwrap();
    ///
    /// assert_eq!(
    ///    "Filter: t1.id = Int32(3)\
    ///    \n  TableScan: t1",
    ///    plan.display_indent().to_string()
    ///  );
    ///
    /// // Note you can also used named parameters
    /// // Build SELECT * FROM t1 WHRERE id = $my_param
    /// let plan = table_scan(Some("t1"), &schema, None).unwrap()
    ///     .filter(col("id").eq(placeholder("$my_param"))).unwrap()
    ///     .build().unwrap()
    ///     // Fill in the parameter $my_param with a literal 3
    ///     .with_param_values(vec![
    ///       ("my_param", ScalarValue::from(3i32)),
    ///     ]).unwrap();
    ///
    /// assert_eq!(
    ///    "Filter: t1.id = Int32(3)\
    ///    \n  TableScan: t1",
    ///    plan.display_indent().to_string()
    ///  );
    ///
    /// ```
    pub fn with_param_values(
        self,
        param_values: impl Into<ParamValues>,
    ) -> Result<LogicalPlan> {
        let param_values = param_values.into();
        let plan_with_values = self.replace_params_with_values(&param_values)?;

        // unwrap Prepare
        Ok(if let LogicalPlan::Prepare(prepare_lp) = plan_with_values {
            param_values.verify(&prepare_lp.data_types)?;
            // try and take ownership of the input if is not shared, clone otherwise
            match Arc::try_unwrap(prepare_lp.input) {
                Ok(input) => input,
                Err(arc_input) => arc_input.as_ref().clone(),
            }
        } else {
            plan_with_values
        })
    }

    /// Returns the maximum number of rows that this plan can output, if known.
    ///
    /// If `None`, the plan can return any number of rows.
    /// If `Some(n)` then the plan can return at most `n` rows but may return fewer.
    pub fn max_rows(self: &LogicalPlan) -> Option<usize> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => input.max_rows(),
            LogicalPlan::Filter(filter) => {
                if filter.is_scalar() {
                    Some(1)
                } else {
                    filter.input.max_rows()
                }
            }
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

    /// If this node's expressions contains any references to an outer subquery
    pub fn contains_outer_reference(&self) -> bool {
        let mut contains = false;
        self.apply_expressions(|expr| {
            Ok(if expr.contains_outer() {
                contains = true;
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .unwrap();
        contains
    }

    /// Get the output expressions and their corresponding columns.
    ///
    /// The parent node may reference the output columns of the plan by expressions, such as
    /// projection over aggregate or window functions. This method helps to convert the
    /// referenced expressions into columns.
    ///
    /// See also: [`crate::utils::columnize_expr`]
    pub(crate) fn columnized_output_exprs(&self) -> Result<Vec<(&Expr, Column)>> {
        match self {
            LogicalPlan::Aggregate(aggregate) => Ok(aggregate
                .output_expressions()?
                .into_iter()
                .zip(self.schema().columns())
                .collect()),
            LogicalPlan::Window(Window {
                window_expr,
                input,
                schema,
            }) => {
                // The input could be another Window, so the result should also include the input's. For Example:
                // `EXPLAIN SELECT RANK() OVER (PARTITION BY a ORDER BY b), SUM(b) OVER (PARTITION BY a) FROM t`
                // Its plan is:
                // Projection: RANK() PARTITION BY [t.a] ORDER BY [t.b ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(t.b) PARTITION BY [t.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                //   WindowAggr: windowExpr=[[SUM(CAST(t.b AS Int64)) PARTITION BY [t.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
                //     WindowAggr: windowExpr=[[RANK() PARTITION BY [t.a] ORDER BY [t.b ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]/
                //       TableScan: t projection=[a, b]
                let mut output_exprs = input.columnized_output_exprs()?;
                let input_len = input.schema().fields().len();
                output_exprs.extend(
                    window_expr
                        .iter()
                        .zip(schema.columns().into_iter().skip(input_len)),
                );
                Ok(output_exprs)
            }
            _ => Ok(vec![]),
        }
    }
}

impl LogicalPlan {
    /// Return a `LogicalPlan` with all placeholders (e.g $1 $2,
    /// ...) replaced with corresponding values provided in
    /// `params_values`
    ///
    /// See [`Self::with_param_values`] for examples and usage with an owned
    /// `ParamValues`
    pub fn replace_params_with_values(
        self,
        param_values: &ParamValues,
    ) -> Result<LogicalPlan> {
        self.transform_up_with_subqueries(|plan| {
            let schema = Arc::clone(plan.schema());
            plan.map_expressions(|e| {
                e.infer_placeholder_types(&schema)?.transform_up(|e| {
                    if let Expr::Placeholder(Placeholder { id, .. }) = e {
                        let value = param_values.get_placeholders_with_values(&id)?;
                        Ok(Transformed::yes(Expr::Literal(value)))
                    } else {
                        Ok(Transformed::no(e))
                    }
                })
            })
        })
        .map(|res| res.data)
    }

    /// Walk the logical plan, find any `Placeholder` tokens, and return a map of their IDs and DataTypes
    pub fn get_parameter_types(
        &self,
    ) -> Result<HashMap<String, Option<DataType>>, DataFusionError> {
        let mut param_types: HashMap<String, Option<DataType>> = HashMap::new();

        self.apply_with_subqueries(|plan| {
            plan.apply_expressions(|expr| {
                expr.apply(|expr| {
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
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })
        .map(|_| param_types)
    }

    // ------------
    // Various implementations for printing out LogicalPlans
    // ------------

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
                match self.0.visit_with_subqueries(&mut visitor) {
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
                match self.0.visit_with_subqueries(&mut visitor) {
                    Ok(_) => Ok(()),
                    Err(_) => Err(fmt::Error),
                }
            }
        }
        Wrapper(self)
    }

    /// Return a displayable structure that produces plan in postgresql JSON format.
    ///
    /// Users can use this format to visualize the plan in existing plan visualization tools, for example [dalibo](https://explain.dalibo.com/)
    pub fn display_pg_json(&self) -> impl Display + '_ {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                let mut visitor = PgJsonVisitor::new(f);
                visitor.with_schema(true);
                match self.0.visit_with_subqueries(&mut visitor) {
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
                self.0
                    .visit_with_subqueries(&mut visitor)
                    .map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0
                    .visit_with_subqueries(&mut visitor)
                    .map_err(|_| fmt::Error)?;
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
                        file_type,
                        options,
                        ..
                    }) => {
                        let op_str = options
                            .iter()
                            .map(|(k, v)| format!("{k} {v}"))
                            .collect::<Vec<String>>()
                            .join(", ");

                        write!(f, "CopyTo: format={} output_url={output_url} options: ({op_str})", file_type.get_ext())
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
                    LogicalPlan::Unnest(Unnest {
                        input: plan,
                        list_type_columns: list_col_indices,
                        struct_type_columns: struct_col_indices, .. }) => {
                        let input_columns = plan.schema().columns();
                        let list_type_columns = list_col_indices
                            .iter()
                            .map(|i| &input_columns[*i])
                            .collect::<Vec<&Column>>();
                        let struct_type_columns = struct_col_indices
                            .iter()
                            .map(|i| &input_columns[*i])
                            .collect::<Vec<&Column>>();
                        // get items from input_columns indexed by list_col_indices
                        write!(f, "Unnest: lists[{}] structs[{}]", 
                        expr_vec_fmt!(list_type_columns),
                        expr_vec_fmt!(struct_type_columns))
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

/// A variadic query operation, Recursive CTE.
///
/// # Recursive Query Evaluation
///
/// From the [Postgres Docs]:
///
/// 1. Evaluate the non-recursive term. For `UNION` (but not `UNION ALL`),
/// discard duplicate rows. Include all remaining rows in the result of the
/// recursive query, and also place them in a temporary working table.
//
/// 2. So long as the working table is not empty, repeat these steps:
///
/// * Evaluate the recursive term, substituting the current contents of the
/// working table for the recursive self-reference. For `UNION` (but not `UNION
/// ALL`), discard duplicate rows and rows that duplicate any previous result
/// row. Include all remaining rows in the result of the recursive query, and
/// also place them in a temporary intermediate table.
///
/// * Replace the contents of the working table with the contents of the
/// intermediate table, then empty the intermediate table.
///
/// [Postgres Docs]: https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-RECURSIVE
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RecursiveQuery {
    /// Name of the query
    pub name: String,
    /// The static term (initial contents of the working table)
    pub static_term: Arc<LogicalPlan>,
    /// The recursive term (evaluated on the contents of the working table until
    /// it returns an empty set)
    pub recursive_term: Arc<LogicalPlan>,
    /// Should the output of the recursive term be deduplicated (`UNION`) or
    /// not (`UNION ALL`).
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
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
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
        let projection_schema = projection_schema(&input, &expr)?;
        Self::try_new_with_schema(expr, input, projection_schema)
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
        Ok(Self {
            expr,
            input,
            schema,
        })
    }

    /// Create a new Projection using the specified output schema
    pub fn new_from_schema(input: Arc<LogicalPlan>, schema: DFSchemaRef) -> Self {
        let expr: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
        Self {
            expr,
            input,
            schema,
        }
    }
}

/// Computes the schema of the result produced by applying a projection to the input logical plan.
///
/// # Arguments
///
/// * `input`: A reference to the input `LogicalPlan` for which the projection schema
/// will be computed.
/// * `exprs`: A slice of `Expr` expressions representing the projection operation to apply.
///
/// # Returns
///
/// A `Result` containing an `Arc<DFSchema>` representing the schema of the result
/// produced by the projection operation. If the schema computation is successful,
/// the `Result` will contain the schema; otherwise, it will contain an error.
pub fn projection_schema(input: &LogicalPlan, exprs: &[Expr]) -> Result<Arc<DFSchema>> {
    let mut schema = DFSchema::new_with_metadata(
        exprlist_to_fields(exprs, input)?,
        input.schema().metadata().clone(),
    )?;
    schema = schema.with_functional_dependencies(calc_func_dependencies_for_project(
        exprs, input,
    )?)?;
    Ok(Arc::new(schema))
}

/// Aliased subquery
#[derive(Clone, PartialEq, Eq, Hash)]
// mark non_exhaustive to encourage use of try_new/new()
#[non_exhaustive]
pub struct SubqueryAlias {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The alias for the input relation
    pub alias: TableReference,
    /// The schema with qualified field names
    pub schema: DFSchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(
        plan: Arc<LogicalPlan>,
        alias: impl Into<TableReference>,
    ) -> Result<Self> {
        let alias = alias.into();
        let fields = change_redundant_column(plan.schema().fields());
        let meta_data = plan.schema().as_ref().metadata().clone();
        let schema: Schema =
            DFSchema::from_unqualified_fields(fields.into(), meta_data)?.into();
        // Since schema is the same, other than qualifier, we can use existing
        // functional dependencies:
        let func_dependencies = plan.schema().functional_dependencies().clone();
        let schema = DFSchemaRef::new(
            DFSchema::try_from_qualified_schema(alias.clone(), &schema)?
                .with_functional_dependencies(func_dependencies)?,
        );
        Ok(SubqueryAlias {
            input: plan,
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

    /// Is this filter guaranteed to return 0 or 1 row in a given instantiation?
    ///
    /// This function will return `true` if its predicate contains a conjunction of
    /// `col(a) = <expr>`, where its schema has a unique filter that is covered
    /// by this conjunction.
    ///
    /// For example, for the table:
    /// ```sql
    /// CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER);
    /// ```
    /// `Filter(a = 2).is_scalar() == true`
    /// , whereas
    /// `Filter(b = 2).is_scalar() == false`
    /// and
    /// `Filter(a = 2 OR b = 2).is_scalar() == false`
    fn is_scalar(&self) -> bool {
        let schema = self.input.schema();

        let functional_dependencies = self.input.schema().functional_dependencies();
        let unique_keys = functional_dependencies.iter().filter(|dep| {
            let nullable = dep.nullable
                && dep
                    .source_indices
                    .iter()
                    .any(|&source| schema.field(source).is_nullable());
            !nullable
                && dep.mode == Dependency::Single
                && dep.target_indices.len() == schema.fields().len()
        });

        let exprs = split_conjunction(&self.predicate);
        let eq_pred_cols: HashSet<_> = exprs
            .iter()
            .filter_map(|expr| {
                let Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }) = expr
                else {
                    return None;
                };
                // This is a no-op filter expression
                if left == right {
                    return None;
                }

                match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(_), Expr::Column(_)) => None,
                    (Expr::Column(c), _) | (_, Expr::Column(c)) => {
                        Some(schema.index_of_column(c).unwrap())
                    }
                    _ => None,
                }
            })
            .collect();

        // If we have a functional dependence that is a subset of our predicate,
        // this filter is scalar
        for key in unique_keys {
            if key.source_indices.iter().all(|c| eq_pred_cols.contains(c)) {
                return true;
            }
        }
        false
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
        let fields: Vec<(Option<TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .map(|(q, f)| (q.cloned(), Arc::clone(f)))
            .collect();
        let input_len = fields.len();
        let mut window_fields = fields;
        let expr_fields = exprlist_to_fields(window_expr.as_slice(), &input)?;
        window_fields.extend_from_slice(expr_fields.as_slice());
        let metadata = input.schema().metadata().clone();

        // Update functional dependencies for window:
        let mut window_func_dependencies =
            input.schema().functional_dependencies().clone();
        window_func_dependencies.extend_target_indices(window_fields.len());

        // Since we know that ROW_NUMBER outputs will be unique (i.e. it consists
        // of consecutive numbers per partition), we can represent this fact with
        // functional dependencies.
        let mut new_dependencies = window_expr
            .iter()
            .enumerate()
            .filter_map(|(idx, expr)| {
                if let Expr::WindowFunction(WindowFunction {
                    // Function is ROW_NUMBER
                    fun:
                        WindowFunctionDefinition::BuiltInWindowFunction(
                            BuiltInWindowFunction::RowNumber,
                        ),
                    partition_by,
                    ..
                }) = expr
                {
                    // When there is no PARTITION BY, row number will be unique
                    // across the entire table.
                    if partition_by.is_empty() {
                        return Some(idx + input_len);
                    }
                }
                None
            })
            .map(|idx| {
                FunctionalDependence::new(vec![idx], vec![], false)
                    .with_mode(Dependency::Single)
            })
            .collect::<Vec<_>>();

        if !new_dependencies.is_empty() {
            for dependence in new_dependencies.iter_mut() {
                dependence.target_indices = (0..window_fields.len()).collect();
            }
            // Add the dependency introduced because of ROW_NUMBER window function to the functional dependency
            let new_deps = FunctionalDependencies::new(new_dependencies);
            window_func_dependencies.extend(new_deps);
        }

        Self::try_new_with_schema(
            window_expr,
            input,
            Arc::new(
                DFSchema::new_with_metadata(window_fields, metadata)?
                    .with_functional_dependencies(window_func_dependencies)?,
            ),
        )
    }

    pub fn try_new_with_schema(
        window_expr: Vec<Expr>,
        input: Arc<LogicalPlan>,
        schema: DFSchemaRef,
    ) -> Result<Self> {
        if window_expr.len() != schema.fields().len() - input.schema().fields().len() {
            return plan_err!(
                "Window has mismatch between number of expressions ({}) and number of fields in schema ({})",
                window_expr.len(),
                schema.fields().len() - input.schema().fields().len()
            );
        }

        Ok(Window {
            input,
            window_expr,
            schema,
        })
    }
}

/// Produces rows from a table provider by reference or from the context
#[derive(Clone)]
pub struct TableScan {
    /// The name of the table
    pub table_name: TableReference,
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
        table_name: impl Into<TableReference>,
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

                let df_schema = DFSchema::new_with_metadata(
                    p.iter()
                        .map(|i| {
                            (Some(table_name.clone()), Arc::new(schema.field(*i).clone()))
                        })
                        .collect(),
                    schema.metadata.clone(),
                )?;
                df_schema.with_functional_dependencies(projected_func_dependencies)
            })
            .unwrap_or_else(|| {
                let df_schema =
                    DFSchema::try_from_qualified_schema(table_name.clone(), &schema)?;
                df_schema.with_functional_dependencies(func_dependencies)
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
/// > describe traces;
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

impl Distinct {
    /// return a reference to the nodes input
    pub fn input(&self) -> &Arc<LogicalPlan> {
        match self {
            Distinct::All(input) => input,
            Distinct::On(DistinctOn { input, .. }) => input,
        }
    }
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
        let qualified_fields = exprlist_to_fields(select_expr.as_slice(), &input)?
            .into_iter()
            .collect();

        let dfschema = DFSchema::new_with_metadata(
            qualified_fields,
            input.schema().metadata().clone(),
        )?;

        let mut distinct_on = DistinctOn {
            on_expr,
            select_expr,
            sort_expr: None,
            input,
            schema: Arc::new(dfschema),
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

        let grouping_expr: Vec<&Expr> = grouping_set_to_exprlist(group_expr.as_slice())?;

        let mut qualified_fields = exprlist_to_fields(grouping_expr, &input)?;

        // Even columns that cannot be null will become nullable when used in a grouping set.
        if is_grouping_set {
            qualified_fields = qualified_fields
                .into_iter()
                .map(|(q, f)| (q, f.as_ref().clone().with_nullable(true).into()))
                .collect::<Vec<_>>();
        }

        qualified_fields.extend(exprlist_to_fields(aggr_expr.as_slice(), &input)?);

        let schema = DFSchema::new_with_metadata(qualified_fields, HashMap::new())?;

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
            new_schema.with_functional_dependencies(aggregate_func_dependencies)?,
        );
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }

    /// Get the output expressions.
    fn output_expressions(&self) -> Result<Vec<&Expr>> {
        let mut exprs = grouping_set_to_exprlist(self.group_expr.as_slice())?;
        exprs.extend(self.aggr_expr.iter());
        debug_assert!(exprs.len() == self.schema.fields().len());
        Ok(exprs)
    }

    /// Get the length of the group by expression in the output schema
    /// This is not simply group by expression length. Expression may be
    /// GroupingSet, etc. In these case we need to get inner expression lengths.
    pub fn group_expr_len(&self) -> Result<usize> {
        grouping_set_expr_count(&self.group_expr)
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
    let input_fields = input.schema().field_names();
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
            input_fields.iter().position(|item| *item == expr_name)
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
    /// Columns to run unnest on, can be a list of (List/Struct) columns
    pub exec_columns: Vec<Column>,
    /// refer to the indices(in the input schema) of columns
    /// that have type list to run unnest on
    pub list_type_columns: Vec<usize>,
    /// refer to the indices (in the input schema) of columns
    /// that have type struct to run unnest on
    pub struct_type_columns: Vec<usize>,
    /// Having items aligned with the output columns
    /// representing which column in the input schema each output column depends on
    pub dependency_indices: Vec<usize>,
    /// The output schema, containing the unnested field column.
    pub schema: DFSchemaRef,
    /// Options
    pub options: UnnestOptions,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::builder::LogicalTableSource;
    use crate::logical_plan::table_scan;
    use crate::{col, exists, in_subquery, lit, placeholder, GroupingSet};

    use datafusion_common::tree_node::TreeNodeVisitor;
    use datafusion_common::{not_impl_err, Constraint, ScalarValue};

    use crate::test::function_stub::count;

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

    #[test]
    fn test_display_pg_json() -> Result<()> {
        let plan = display_plan()?;

        let expected_pg_json = r#"[
  {
    "Plan": {
      "Expressions": [
        "employee_csv.id"
      ],
      "Node Type": "Projection",
      "Output": [
        "id"
      ],
      "Plans": [
        {
          "Condition": "employee_csv.state IN (<subquery>)",
          "Node Type": "Filter",
          "Output": [
            "id",
            "state"
          ],
          "Plans": [
            {
              "Node Type": "Subquery",
              "Output": [
                "state"
              ],
              "Plans": [
                {
                  "Node Type": "TableScan",
                  "Output": [
                    "state"
                  ],
                  "Plans": [],
                  "Relation Name": "employee_csv"
                }
              ]
            },
            {
              "Node Type": "TableScan",
              "Output": [
                "id",
                "state"
              ],
              "Plans": [],
              "Relation Name": "employee_csv"
            }
          ]
        }
      ]
    }
  }
]"#;

        let pg_json = format!("{}", plan.display_pg_json());

        assert_eq!(expected_pg_json, pg_json);
        Ok(())
    }

    /// Tests for the Visitor trait and walking logical plan nodes
    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }

    impl<'n> TreeNodeVisitor<'n> for OkVisitor {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => {
                    return not_impl_err!("unknown plan type");
                }
            };

            self.strings.push(s.into());
            Ok(TreeNodeRecursion::Continue)
        }

        fn f_up(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => {
                    return not_impl_err!("unknown plan type");
                }
            };

            self.strings.push(s.into());
            Ok(TreeNodeRecursion::Continue)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor);
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

    impl<'n> TreeNodeVisitor<'n> for StoppingVisitor {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_false_from_pre_in.dec() {
                return Ok(TreeNodeRecursion::Stop);
            }
            self.inner.f_down(plan)?;

            Ok(TreeNodeRecursion::Continue)
        }

        fn f_up(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_false_from_post_in.dec() {
                return Ok(TreeNodeRecursion::Stop);
            }

            self.inner.f_up(plan)
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
        let res = plan.visit_with_subqueries(&mut visitor);
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
        let res = plan.visit_with_subqueries(&mut visitor);
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

    impl<'n> TreeNodeVisitor<'n> for ErrorVisitor {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_error_from_pre_in.dec() {
                return not_impl_err!("Error in pre_visit");
            }

            self.inner.f_down(plan)
        }

        fn f_up(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
            if self.return_error_from_post_in.dec() {
                return not_impl_err!("Error in post_visit");
            }

            self.inner.f_up(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor {
            return_error_from_pre_in: OptionalCounter::new(2),
            ..Default::default()
        };
        let plan = test_plan();
        let res = plan.visit_with_subqueries(&mut visitor).unwrap_err();
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
        let res = plan.visit_with_subqueries(&mut visitor).unwrap_err();
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
        let empty_schema = Arc::new(DFSchema::empty());
        let p = Projection::try_new_with_schema(
            vec![col("a")],
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::clone(&empty_schema),
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

        let param_values = vec![ScalarValue::Int32(Some(42))];
        plan.replace_params_with_values(&param_values.clone().into())
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");

        // test $0 placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("$0")))
            .unwrap()
            .build()
            .unwrap();

        plan.replace_params_with_values(&param_values.clone().into())
            .expect_err("unexpectedly succeeded to replace an invalid placeholder");

        // test $00 placeholder
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .filter(col("id").eq(placeholder("$00")))
            .unwrap()
            .build()
            .unwrap();

        plan.replace_params_with_values(&param_values.into())
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

    #[test]
    fn test_filter_is_scalar() {
        // test empty placeholder
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let source = Arc::new(LogicalTableSource::new(schema));
        let schema = Arc::new(
            DFSchema::try_from_qualified_schema(
                TableReference::bare("tab"),
                &source.schema(),
            )
            .unwrap(),
        );
        let scan = Arc::new(LogicalPlan::TableScan(TableScan {
            table_name: TableReference::bare("tab"),
            source: Arc::clone(&source) as Arc<dyn TableSource>,
            projection: None,
            projected_schema: Arc::clone(&schema),
            filters: vec![],
            fetch: None,
        }));
        let col = schema.field_names()[0].clone();

        let filter = Filter::try_new(
            Expr::Column(col.into()).eq(Expr::Literal(ScalarValue::Int32(Some(1)))),
            scan,
        )
        .unwrap();
        assert!(!filter.is_scalar());
        let unique_schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_functional_dependencies(
                    FunctionalDependencies::new_from_constraints(
                        Some(&Constraints::new_unverified(vec![Constraint::Unique(
                            vec![0],
                        )])),
                        1,
                    ),
                )
                .unwrap(),
        );
        let scan = Arc::new(LogicalPlan::TableScan(TableScan {
            table_name: TableReference::bare("tab"),
            source,
            projection: None,
            projected_schema: Arc::clone(&unique_schema),
            filters: vec![],
            fetch: None,
        }));
        let col = schema.field_names()[0].clone();

        let filter = Filter::try_new(
            Expr::Column(col.into()).eq(Expr::Literal(ScalarValue::Int32(Some(1)))),
            scan,
        )
        .unwrap();
        assert!(filter.is_scalar());
    }

    #[test]
    fn test_transform_explain() {
        let schema = Schema::new(vec![
            Field::new("foo", DataType::Int32, false),
            Field::new("bar", DataType::Int32, false),
        ]);

        let plan = table_scan(TableReference::none(), &schema, None)
            .unwrap()
            .explain(false, false)
            .unwrap()
            .build()
            .unwrap();

        let external_filter =
            col("foo").eq(Expr::Literal(ScalarValue::Boolean(Some(true))));

        // after transformation, because plan is not the same anymore,
        // the parent plan is built again with call to LogicalPlan::with_new_inputs -> with_new_exprs
        let plan = plan
            .transform(|plan| match plan {
                LogicalPlan::TableScan(table) => {
                    let filter = Filter::try_new(
                        external_filter.clone(),
                        Arc::new(LogicalPlan::TableScan(table)),
                    )
                    .unwrap();
                    Ok(Transformed::yes(LogicalPlan::Filter(filter)))
                }
                x => Ok(Transformed::no(x)),
            })
            .data()
            .unwrap();

        let expected = "Explain\
                        \n  Filter: foo = Boolean(true)\
                        \n    TableScan: ?table?";
        let actual = format!("{}", plan.display_indent());
        assert_eq!(expected.to_string(), actual)
    }
}
