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

///! Logical plan types
use crate::logical_plan::builder::validate_unique_names;
use crate::logical_plan::display::{GraphvizVisitor, IndentVisitor};
use crate::logical_plan::extension::UserDefinedLogicalNode;
use crate::logical_plan::statement::{DmlStatement, Statement};

use crate::logical_plan::plan;
use crate::utils::{
    distinct_group_exprs, exprlist_to_fields, find_out_reference_exprs, from_plan,
    grouping_set_expr_count, inspect_expr_pre,
};
use crate::{
    build_join_schema, Expr, ExprSchemable, TableProviderFilterPushDown, TableSource,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeVisitor, VisitRecursion,
};
use datafusion_common::{
    plan_err, Column, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference,
    Result, ScalarValue, TableReference,
};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter(Filter),
    /// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
    Window(Window),
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate(Aggregate),
    /// Sorts its input according to a list of sort expressions.
    Sort(Sort),
    /// Join two logical plans on one or more join columns
    Join(Join),
    /// Apply Cross Join to two logical plans
    CrossJoin(CrossJoin),
    /// Repartition the plan based on a partitioning scheme.
    Repartition(Repartition),
    /// Union multiple inputs
    Union(Union),
    /// Produces rows from a table provider by reference or from the context
    TableScan(TableScan),
    /// Produces no rows: An empty relation with an empty schema
    EmptyRelation(EmptyRelation),
    /// Subquery
    Subquery(Subquery),
    /// Aliased relation provides, or changes, the name of a relation.
    SubqueryAlias(SubqueryAlias),
    /// Skip some number of rows, and then fetch some number of rows.
    Limit(Limit),
    /// [`Statement`]
    Statement(Statement),
    /// Creates an external table.
    CreateExternalTable(CreateExternalTable),
    /// Creates an in memory table.
    CreateMemoryTable(CreateMemoryTable),
    /// Creates a new view.
    CreateView(CreateView),
    /// Creates a new catalog schema.
    CreateCatalogSchema(CreateCatalogSchema),
    /// Creates a new catalog (aka "Database").
    CreateCatalog(CreateCatalog),
    /// Drops a table.
    DropTable(DropTable),
    /// Drops a view.
    DropView(DropView),
    /// Values expression. See
    /// [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details.
    Values(Values),
    /// Produces a relation with string representations of
    /// various parts of the plan
    Explain(Explain),
    /// Runs the actual plan, and then prints the physical plan with
    /// with execution metrics.
    Analyze(Analyze),
    /// Extension operator defined outside of DataFusion
    Extension(Extension),
    /// Remove duplicate rows from the input
    Distinct(Distinct),
    /// Prepare a statement
    Prepare(Prepare),
    /// Insert / Update / Delete
    Dml(DmlStatement),
    /// Describe the schema of table
    DescribeTable(DescribeTable),
    /// Unnest a column that contains a nested list type.
    Unnest(Unnest),
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
            LogicalPlan::Distinct(Distinct { input }) => input.schema(),
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
            LogicalPlan::CreateExternalTable(CreateExternalTable { schema, .. }) => {
                schema
            }
            LogicalPlan::Prepare(Prepare { input, .. }) => input.schema(),
            LogicalPlan::Explain(explain) => &explain.schema,
            LogicalPlan::Analyze(analyze) => &analyze.schema,
            LogicalPlan::Extension(extension) => extension.node.schema(),
            LogicalPlan::Union(Union { schema, .. }) => schema,
            LogicalPlan::CreateMemoryTable(CreateMemoryTable { input, .. })
            | LogicalPlan::CreateView(CreateView { input, .. }) => input.schema(),
            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema { schema, .. }) => {
                schema
            }
            LogicalPlan::CreateCatalog(CreateCatalog { schema, .. }) => schema,
            LogicalPlan::DropTable(DropTable { schema, .. }) => schema,
            LogicalPlan::DropView(DropView { schema, .. }) => schema,
            LogicalPlan::DescribeTable(DescribeTable { dummy_schema, .. }) => {
                dummy_schema
            }
            LogicalPlan::Dml(DmlStatement { table_schema, .. }) => table_schema,
            LogicalPlan::Unnest(Unnest { schema, .. }) => schema,
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
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::TableScan(_) => {
                vec![self.schema()]
            }
            // return children schemas
            LogicalPlan::Limit(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Prepare(_) => {
                self.inputs().iter().map(|p| p.schema()).collect()
            }
            // return empty
            LogicalPlan::Statement(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::DropView(_)
            | LogicalPlan::DescribeTable(_) => vec![],
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
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
            // plans without expressions
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::DropView(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Prepare(_) => Ok(()),
        }
    }

    /// returns all inputs of this `LogicalPlan` node. Does not
    /// include inputs to inputs, or subqueries.
    pub fn inputs(self: &LogicalPlan) -> Vec<&LogicalPlan> {
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
            LogicalPlan::Distinct(Distinct { input }) => vec![input],
            LogicalPlan::Explain(explain) => vec![&explain.plan],
            LogicalPlan::Analyze(analyze) => vec![&analyze.input],
            LogicalPlan::Dml(write) => vec![&write.input],
            LogicalPlan::CreateMemoryTable(CreateMemoryTable { input, .. })
            | LogicalPlan::CreateView(CreateView { input, .. })
            | LogicalPlan::Prepare(Prepare { input, .. }) => {
                vec![input]
            }
            LogicalPlan::Unnest(Unnest { input, .. }) => vec![input],
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::Statement { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::DropView(_)
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

    pub fn with_new_inputs(&self, inputs: &[LogicalPlan]) -> Result<LogicalPlan> {
        from_plan(self, &self.expressions(), inputs)
    }

    /// Convert a prepared [`LogicalPlan`] into its inner logical plan
    /// with all params replaced with their corresponding values
    pub fn with_param_values(
        self,
        param_values: Vec<ScalarValue>,
    ) -> Result<LogicalPlan> {
        match self {
            LogicalPlan::Prepare(prepare_lp) => {
                // Verify if the number of params matches the number of values
                if prepare_lp.data_types.len() != param_values.len() {
                    return Err(DataFusionError::Internal(format!(
                        "Expected {} parameters, got {}",
                        prepare_lp.data_types.len(),
                        param_values.len()
                    )));
                }

                // Verify if the types of the params matches the types of the values
                let iter = prepare_lp.data_types.iter().zip(param_values.iter());
                for (i, (param_type, value)) in iter.enumerate() {
                    if *param_type != value.get_datatype() {
                        return Err(DataFusionError::Internal(format!(
                            "Expected parameter of type {:?}, got {:?} at index {}",
                            param_type,
                            value.get_datatype(),
                            i
                        )));
                    }
                }

                let input_plan = prepare_lp.input;
                input_plan.replace_params_with_values(&param_values)
            }
            _ => Ok(self),
        }
    }
}

impl LogicalPlan {
    /// applies collect to any subqueries in the plan
    pub(crate) fn apply_subqueries<F>(&self, op: &mut F) -> datafusion_common::Result<()>
    where
        F: FnMut(&Self) -> datafusion_common::Result<VisitRecursion>,
    {
        self.inspect_expressions(|expr| {
            // recursively look for subqueries
            inspect_expr_pre(expr, |expr| {
                match expr {
                    Expr::Exists { subquery, .. }
                    | Expr::InSubquery { subquery, .. }
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
                    Expr::Exists { subquery, .. }
                    | Expr::InSubquery { subquery, .. }
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

    /// Return a logical plan with all placeholders/params (e.g $1 $2,
    /// ...) replaced with corresponding values provided in the
    /// params_values
    pub fn replace_params_with_values(
        &self,
        param_values: &[ScalarValue],
    ) -> Result<LogicalPlan> {
        let new_exprs = self
            .expressions()
            .into_iter()
            .map(|e| Self::replace_placeholders_with_values(e, param_values))
            .collect::<Result<Vec<_>>>()?;

        let new_inputs_with_values = self
            .inputs()
            .into_iter()
            .map(|inp| inp.replace_params_with_values(param_values))
            .collect::<Result<Vec<_>>>()?;

        from_plan(self, &new_exprs, &new_inputs_with_values)
    }

    /// Walk the logical plan, find any `PlaceHolder` tokens, and return a map of their IDs and DataTypes
    pub fn get_parameter_types(
        &self,
    ) -> Result<HashMap<String, Option<DataType>>, DataFusionError> {
        let mut param_types: HashMap<String, Option<DataType>> = HashMap::new();

        self.apply(&mut |plan| {
            plan.inspect_expressions(|expr| {
                expr.apply(&mut |expr| {
                    if let Expr::Placeholder { id, data_type } = expr {
                        let prev = param_types.get(id);
                        match (prev, data_type) {
                            (Some(Some(prev)), Some(dt)) => {
                                if prev != dt {
                                    Err(DataFusionError::Plan(format!(
                                        "Conflicting types for {id}"
                                    )))?;
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
                Expr::Placeholder { id, data_type } => {
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
                    if Some(value.get_datatype()) != *data_type {
                        return Err(DataFusionError::Internal(format!(
                            "Placeholder value type mismatch: expected {:?}, got {:?}",
                            data_type,
                            value.get_datatype()
                        )));
                    }
                    // Replace the placeholder with the value
                    Ok(Transformed::Yes(Expr::Literal(value.clone())))
                }
                Expr::ScalarSubquery(qry) => {
                    let subquery =
                        Arc::new(qry.subquery.replace_params_with_values(param_values)?);
                    Ok(Transformed::Yes(Expr::ScalarSubquery(plan::Subquery {
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
    /// per node. For example:
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
                writeln!(
                    f,
                    "// Begin DataFusion GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;

                let mut visitor = GraphvizVisitor::new(f);

                visitor.pre_visit_plan("LogicalPlan")?;
                self.0.visit(&mut visitor).map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0.visit(&mut visitor).map_err(|_| fmt::Error)?;
                visitor.post_visit_plan()?;

                writeln!(f, "}}")?;
                writeln!(f, "// End DataFusion GraphViz Plan")?;
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
                                write!(f, ", full_filters={full_filter:?}")?;
                            };
                            if !partial_filter.is_empty() {
                                write!(f, ", partial_filters={partial_filter:?}")?;
                            }
                            if !unsupported_filters.is_empty() {
                                write!(
                                    f,
                                    ", unsupported_filters={unsupported_filters:?}"
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
                            write!(f, "{expr_item:?}")?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Dml(DmlStatement { table_name, op, .. }) => {
                        write!(f, "Dml: op=[{op}] table=[{table_name}]")
                    }
                    LogicalPlan::Filter(Filter {
                        predicate: ref expr,
                        ..
                    }) => write!(f, "Filter: {expr:?}"),
                    LogicalPlan::Window(Window {
                        ref window_expr, ..
                    }) => {
                        write!(f, "WindowAggr: windowExpr=[{window_expr:?}]")
                    }
                    LogicalPlan::Aggregate(Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    }) => write!(
                        f,
                        "Aggregate: groupBy=[{group_expr:?}], aggr=[{aggr_expr:?}]"
                    ),
                    LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
                        write!(f, "Sort: ")?;
                        for (i, expr_item) in expr.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{expr_item:?}")?;
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
                                expr.iter().map(|e| format!("{e:?}")).collect();
                            write!(
                                f,
                                "Repartition: Hash({}) partition_count={}",
                                hash_expr.join(", "),
                                n
                            )
                        }
                        Partitioning::DistributeBy(expr) => {
                            let dist_by_expr: Vec<String> =
                                expr.iter().map(|e| format!("{e:?}")).collect();
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
                    LogicalPlan::CreateExternalTable(CreateExternalTable {
                        ref name,
                        ..
                    }) => {
                        write!(f, "CreateExternalTable: {name:?}")
                    }
                    LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                        name,
                        primary_key,
                        ..
                    }) => {
                        let pk: Vec<String> =
                            primary_key.iter().map(|c| c.name.to_string()).collect();
                        let mut pk = pk.join(", ");
                        if !pk.is_empty() {
                            pk = format!(" primary_key=[{pk}]");
                        }
                        write!(f, "CreateMemoryTable: {name:?}{pk}")
                    }
                    LogicalPlan::CreateView(CreateView { name, .. }) => {
                        write!(f, "CreateView: {name:?}")
                    }
                    LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                        schema_name,
                        ..
                    }) => {
                        write!(f, "CreateCatalogSchema: {schema_name:?}")
                    }
                    LogicalPlan::CreateCatalog(CreateCatalog {
                        catalog_name, ..
                    }) => {
                        write!(f, "CreateCatalog: {catalog_name:?}")
                    }
                    LogicalPlan::DropTable(DropTable {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropTable: {name:?} if not exist:={if_exists}")
                    }
                    LogicalPlan::DropView(DropView {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropView: {name:?} if not exist:={if_exists}")
                    }
                    LogicalPlan::Distinct(Distinct { .. }) => {
                        write!(f, "Distinct:")
                    }
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

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
    /// Left Semi Join
    LeftSemi,
    /// Right Semi Join
    RightSemi,
    /// Left Anti Join
    LeftAnti,
    /// Right Anti Join
    RightAnti,
}

impl JoinType {
    pub fn is_outer(self) -> bool {
        self == JoinType::Left || self == JoinType::Right || self == JoinType::Full
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let join_type = match self {
            JoinType::Inner => "Inner",
            JoinType::Left => "Left",
            JoinType::Right => "Right",
            JoinType::Full => "Full",
            JoinType::LeftSemi => "LeftSemi",
            JoinType::RightSemi => "RightSemi",
            JoinType::LeftAnti => "LeftAnti",
            JoinType::RightAnti => "RightAnti",
        };
        write!(f, "{join_type}")
    }
}

/// Join constraint
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinConstraint {
    /// Join ON
    On,
    /// Join USING
    Using,
}

/// Creates a catalog (aka "Database").
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalog {
    /// The catalog name
    pub catalog_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Creates a schema.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalogSchema {
    /// The table schema
    pub schema_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Drops a table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DropTable {
    /// The table name
    pub name: OwnedTableReference,
    /// If the table exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

/// Drops a view.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DropView {
    /// The view name
    pub name: OwnedTableReference,
    /// If the view exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

/// Produces no rows: An empty relation with an empty schema
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EmptyRelation {
    /// Whether to produce a placeholder row
    pub produce_one_row: bool,
    /// The schema description of the output
    pub schema: DFSchemaRef,
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
            return Err(DataFusionError::Plan(format!("Projection has mismatch between number of expressions ({}) and number of fields in schema ({})", expr.len(), schema.fields().len())));
        }
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

    pub fn try_from_plan(plan: &LogicalPlan) -> Result<&Projection> {
        match plan {
            LogicalPlan::Projection(it) => Ok(it),
            _ => plan_err!("Could not coerce into Projection!"),
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
    pub alias: String,
    /// The schema with qualified field names
    pub schema: DFSchemaRef,
}

impl SubqueryAlias {
    pub fn try_new(plan: LogicalPlan, alias: impl Into<String>) -> Result<Self> {
        let alias = alias.into();
        let table_ref = TableReference::bare(&alias);
        let schema: Schema = plan.schema().as_ref().clone().into();
        let schema = DFSchemaRef::new(DFSchema::try_from_qualified_schema(
            table_ref.to_owned_reference(),
            &schema,
        )?);
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
                return Err(DataFusionError::Plan(format!(
                    "Cannot create filter with non-boolean predicate '{predicate}' returning {predicate_type}"
                )));
            }
        }

        // filter predicates should not be aliased
        if let Expr::Alias(expr, alias) = predicate {
            return Err(DataFusionError::Plan(format!(
                "Attempted to create Filter predicate with \
                expression `{expr}` aliased as '{alias}'. Filter predicates should not be \
                aliased."
            )));
        }

        Ok(Self { predicate, input })
    }

    pub fn try_from_plan(plan: &LogicalPlan) -> Result<&Filter> {
        match plan {
            LogicalPlan::Filter(it) => Ok(it),
            _ => plan_err!("Could not coerce into Filter!"),
        }
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

/// Creates an in memory table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateMemoryTable {
    /// The table name
    pub name: OwnedTableReference,
    /// The ordered list of columns in the primary key, or an empty vector if none
    pub primary_key: Vec<Column>,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
}

/// Creates a view.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateView {
    /// The table name
    pub name: OwnedTableReference,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
}

/// Creates an external table.
#[derive(Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: OwnedTableReference,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: String,
    /// Whether the CSV file contains a header
    pub has_header: bool,
    /// Delimiter for CSV
    pub delimiter: char,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// SQL used to create the table, if available
    pub definition: Option<String>,
    /// Order expressions supplied by user
    pub order_exprs: Vec<Expr>,
    /// File compression type (GZIP, BZIP2, XZ, ZSTD)
    pub file_compression_type: CompressionTypeVariant,
    /// Table(provider) specific options
    pub options: HashMap<String, String>,
}

// Hashing refers to a subset of fields considered in PartialEq.
#[allow(clippy::derived_hash_with_manual_eq)]
impl Hash for CreateExternalTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
        self.location.hash(state);
        self.file_type.hash(state);
        self.has_header.hash(state);
        self.delimiter.hash(state);
        self.table_partition_cols.hash(state);
        self.if_not_exists.hash(state);
        self.definition.hash(state);
        self.file_compression_type.hash(state);
        self.order_exprs.hash(state);
        self.options.len().hash(state); // HashMap is not hashable
    }
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
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DescribeTable {
    /// Table schema
    pub schema: Arc<Schema>,
    /// Dummy schema
    pub dummy_schema: DFSchemaRef,
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
#[allow(clippy::derived_hash_with_manual_eq)] // see impl PartialEq for explanation
#[derive(Clone, Eq, Hash)]
pub struct Extension {
    /// The runtime extension operator
    pub node: Arc<dyn UserDefinedLogicalNode>,
}

impl PartialEq for Extension {
    #[allow(clippy::op_ref)] // clippy false positive
    fn eq(&self, other: &Self) -> bool {
        // must be manually derived due to a bug in #[derive(PartialEq)]
        // https://github.com/rust-lang/rust/issues/39128
        &self.node == &other.node
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
pub struct Distinct {
    /// The logical plan that is being DISTINCT'd
    pub input: Arc<LogicalPlan>,
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
    /// Create a new aggregate operator, the group_expr might contain multiple [Expr::GroupingSet] expressions
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Self> {
        if group_expr.is_empty() && aggr_expr.is_empty() {
            return Err(DataFusionError::Plan(
                "Aggregate requires at least one grouping or aggregate expression"
                    .to_string(),
            ));
        }
        let distinct_grouping_expr: Vec<Expr> =
            distinct_group_exprs(group_expr.as_slice(), true);

        let all_expr = distinct_grouping_expr.iter().chain(aggr_expr.iter());
        validate_unique_names("Aggregations", all_expr.clone())?;
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(all_expr, &input)?,
            input.schema().metadata().clone(),
        )?);

        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }

    /// Create a new aggregate operator using the provided schema to avoid the overhead of
    /// building the schema again when the schema is already known,
    /// The group_expr can not contain multiple [Expr::GroupingSet] expressions.
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
            return Err(DataFusionError::Plan(
                "Aggregate requires at least one grouping or aggregate expression"
                    .to_string(),
            ));
        }
        let group_expr_count = grouping_set_expr_count(&group_expr)?;
        if schema.fields().len() != group_expr_count + aggr_expr.len() {
            return Err(DataFusionError::Plan(format!(
                "Aggregate schema has wrong number of fields. Expected {} got {}",
                group_expr_count + aggr_expr.len(),
                schema.fields().len()
            )));
        }
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
        })
    }

    pub fn try_from_plan(plan: &LogicalPlan) -> Result<&Aggregate> {
        match plan {
            LogicalPlan::Aggregate(it) => Ok(it),
            _ => plan_err!("Could not coerce into Aggregate!"),
        }
    }
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
            .zip(column_on.1.into_iter())
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

/// Represents which type of plan, when storing multiple
/// for use in EXPLAIN plans
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlanType {
    /// The initial LogicalPlan provided to DataFusion
    InitialLogicalPlan,
    /// The LogicalPlan which results from applying an optimizer pass
    OptimizedLogicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The final, fully optimized LogicalPlan that was converted to a physical plan
    FinalLogicalPlan,
    /// The initial physical plan, prepared for execution
    InitialPhysicalPlan,
    /// The ExecutionPlan which results from applying an optimizer pass
    OptimizedPhysicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The final, fully optimized physical which would be executed
    FinalPhysicalPlan,
}

impl Display for PlanType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PlanType::InitialLogicalPlan => write!(f, "initial_logical_plan"),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                write!(f, "logical_plan after {optimizer_name}")
            }
            PlanType::FinalLogicalPlan => write!(f, "logical_plan"),
            PlanType::InitialPhysicalPlan => write!(f, "initial_physical_plan"),
            PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                write!(f, "physical_plan after {optimizer_name}")
            }
            PlanType::FinalPhysicalPlan => write!(f, "physical_plan"),
        }
    }
}

/// Represents some sort of execution plan, in String form
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::rc_buffer)]
pub struct StringifiedPlan {
    /// An identifier of what type of plan this string represents
    pub plan_type: PlanType,
    /// The string representation of the plan
    pub plan: Arc<String>,
}

impl StringifiedPlan {
    /// Create a new Stringified plan of `plan_type` with string
    /// representation `plan`
    pub fn new(plan_type: PlanType, plan: impl Into<String>) -> Self {
        StringifiedPlan {
            plan_type,
            plan: Arc::new(plan.into()),
        }
    }

    /// returns true if this plan should be displayed. Generally
    /// `verbose_mode = true` will display all available plans
    pub fn should_display(&self, verbose_mode: bool) -> bool {
        match self.plan_type {
            PlanType::FinalLogicalPlan | PlanType::FinalPhysicalPlan => true,
            _ => verbose_mode,
        }
    }
}

/// Trait for something that can be formatted as a stringified plan
pub trait ToStringifiedPlan {
    /// Create a stringified plan with the specified type
    fn to_stringified(&self, plan_type: PlanType) -> StringifiedPlan;
}

/// Unnest a column that contains a nested list type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unnest {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The column to unnest
    pub column: Column,
    /// The output schema, containing the unnested field column.
    pub schema: DFSchemaRef,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::table_scan;
    use crate::{col, exists, in_subquery, lit};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::tree_node::TreeNodeVisitor;
    use datafusion_common::DFSchema;
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

        // just test for a few key lines in the output rather than the
        // whole thing to make test mainteance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert!(
            graphviz.contains(
                r#"// Begin DataFusion GraphViz Plan (see https://graphviz.org)"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(
            graphviz.contains(
                r#"[shape=box label="TableScan: employee_csv projection=[id, state]"]"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(graphviz.contains(r#"[shape=box label="TableScan: employee_csv projection=[id, state]\nSchema: [id:Int32, state:Utf8]"]"#),
                "\n{}", plan.display_graphviz());
        assert!(
            graphviz.contains(r#"// End DataFusion GraphViz Plan"#),
            "\n{}",
            plan.display_graphviz()
        );
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
                    return Err(DataFusionError::NotImplemented(
                        "unknown plan type".to_string(),
                    ))
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
                    return Err(DataFusionError::NotImplemented(
                        "unknown plan type".to_string(),
                    ))
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
                return Err(DataFusionError::NotImplemented(
                    "Error in pre_visit".to_string(),
                ));
            }

            self.inner.pre_visit(plan)
        }

        fn post_visit(&mut self, plan: &LogicalPlan) -> Result<VisitRecursion> {
            if self.return_error_from_post_in.dec() {
                return Err(DataFusionError::NotImplemented(
                    "Error in post_visit".to_string(),
                ));
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
        let res = plan.visit(&mut visitor);

        if let Err(DataFusionError::NotImplemented(e)) = res {
            assert_eq!("Error in pre_visit", e);
        } else {
            panic!("Expected an error");
        }

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
        let res = plan.visit(&mut visitor);
        if let Err(DataFusionError::NotImplemented(e)) = res {
            assert_eq!("Error in post_visit", e);
        } else {
            panic!("Expected an error");
        }

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
        assert_eq!("Error during planning: Projection has mismatch between number of expressions (1) and number of fields in schema (0)", format!("{}", p.err().unwrap()));
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
}
