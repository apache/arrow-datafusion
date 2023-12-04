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

//! This module provides a builder for creating LogicalPlans

use std::any::Any;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::iter::zip;
use std::sync::Arc;

use crate::dml::{CopyOptions, CopyTo};
use crate::expr::Alias;
use crate::expr_rewriter::{
    coerce_plan_expr_for_schema, normalize_col,
    normalize_col_with_schemas_and_ambiguity_check, normalize_cols,
    rewrite_sort_cols_by_aggs,
};
use crate::logical_plan::{
    Aggregate, Analyze, CrossJoin, Distinct, DistinctOn, EmptyRelation, Explain, Filter,
    Join, JoinConstraint, JoinType, Limit, LogicalPlan, Partitioning, PlanType, Prepare,
    Projection, Repartition, Sort, SubqueryAlias, TableScan, Union, Unnest, Values,
    Window,
};
use crate::type_coercion::binary::comparison_coercion;
use crate::utils::{
    can_hash, columnize_expr, compare_sort_expr, expand_qualified_wildcard,
    expand_wildcard, find_valid_equijoin_key_pair, group_window_expr_by_sort_keys,
};
use crate::{
    and, binary_expr, DmlStatement, Expr, ExprSchemable, Operator,
    TableProviderFilterPushDown, TableSource, WriteOp,
};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::display::ToStringifiedPlan;
use datafusion_common::{
    plan_datafusion_err, plan_err, Column, DFField, DFSchema, DFSchemaRef,
    DataFusionError, FileType, OwnedTableReference, Result, ScalarValue, TableReference,
    ToDFSchema, UnnestOptions,
};

/// Default table name for unnamed table
pub const UNNAMED_TABLE: &str = "?table?";

/// Builder for logical plans
///
/// ```
/// # use datafusion_expr::{lit, col, LogicalPlanBuilder, logical_plan::table_scan};
/// # use datafusion_common::Result;
/// # use arrow::datatypes::{Schema, DataType, Field};
/// #
/// # fn main() -> Result<()> {
/// #
/// # fn employee_schema() -> Schema {
/// #    Schema::new(vec![
/// #           Field::new("id", DataType::Int32, false),
/// #           Field::new("first_name", DataType::Utf8, false),
/// #           Field::new("last_name", DataType::Utf8, false),
/// #           Field::new("state", DataType::Utf8, false),
/// #           Field::new("salary", DataType::Int32, false),
/// #       ])
/// #   }
/// #
/// // Create a plan similar to
/// // SELECT last_name
/// // FROM employees
/// // WHERE salary < 1000
/// let plan = table_scan(
///              Some("employee"),
///              &employee_schema(),
///              None,
///            )?
///            // Keep only rows where salary < 1000
///            .filter(col("salary").lt_eq(lit(1000)))?
///            // only show "last_name" in the final results
///            .project(vec![col("last_name")])?
///            .build()?;
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    /// Return the output schema of the plan build so far
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan.schema()
    }

    /// Create an empty relation.
    ///
    /// `produce_one_row` set to true means this empty node needs to produce a placeholder row.
    pub fn empty(produce_one_row: bool) -> Self {
        Self::from(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    /// Create a values list based relation, and the schema is inferred from data, consuming
    /// `value`. See the [Postgres VALUES](https://www.postgresql.org/docs/current/queries-values.html)
    /// documentation for more details.
    ///
    /// By default, it assigns the names column1, column2, etc. to the columns of a VALUES table.
    /// The column names are not specified by the SQL standard and different database systems do it differently,
    /// so it's usually better to override the default names with a table alias list.
    ///
    /// If the values include params/binders such as $1, $2, $3, etc, then the `param_data_types` should be provided.
    pub fn values(mut values: Vec<Vec<Expr>>) -> Result<Self> {
        if values.is_empty() {
            return plan_err!("Values list cannot be empty");
        }
        let n_cols = values[0].len();
        if n_cols == 0 {
            return plan_err!("Values list cannot be zero length");
        }
        let empty_schema = DFSchema::empty();
        let mut field_types: Vec<Option<DataType>> = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            field_types.push(None);
        }
        // hold all the null holes so that we can correct their data types later
        let mut nulls: Vec<(usize, usize)> = Vec::new();
        for (i, row) in values.iter().enumerate() {
            if row.len() != n_cols {
                return plan_err!(
                    "Inconsistent data length across values list: got {} values in row {} but expected {}",
                    row.len(),
                    i,
                    n_cols
                );
            }
            field_types = row
                .iter()
                .enumerate()
                .map(|(j, expr)| {
                    if let Expr::Literal(ScalarValue::Null) = expr {
                        nulls.push((i, j));
                        Ok(field_types[j].clone())
                    } else {
                        let data_type = expr.get_type(&empty_schema)?;
                        if let Some(prev_data_type) = &field_types[j] {
                            if prev_data_type != &data_type {
                                return plan_err!("Inconsistent data type across values list at row {i} column {j}. Was {prev_data_type} but found {data_type}")
                            }
                        }
                        Ok(Some(data_type))
                    }
                })
                .collect::<Result<Vec<Option<DataType>>>>()?;
        }
        let fields = field_types
            .iter()
            .enumerate()
            .map(|(j, data_type)| {
                // naming is following convention https://www.postgresql.org/docs/current/queries-values.html
                let name = &format!("column{}", j + 1);
                DFField::new_unqualified(
                    name,
                    data_type.clone().unwrap_or(DataType::Utf8),
                    true,
                )
            })
            .collect::<Vec<_>>();
        for (i, j) in nulls {
            values[i][j] = Expr::Literal(ScalarValue::try_from(fields[j].data_type())?);
        }
        let schema =
            DFSchemaRef::new(DFSchema::new_with_metadata(fields, HashMap::new())?);
        Ok(Self::from(LogicalPlan::Values(Values { schema, values })))
    }

    /// Convert a table provider into a builder with a TableScan
    ///
    /// Note that if you pass a string as `table_name`, it is treated
    /// as a SQL identifier, as described on [`TableReference`] and
    /// thus is normalized
    ///
    /// # Example:
    /// ```
    /// # use datafusion_expr::{lit, col, LogicalPlanBuilder,
    /// #  logical_plan::builder::LogicalTableSource, logical_plan::table_scan
    /// # };
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{Schema, DataType, Field};
    /// # use datafusion_common::TableReference;
    /// #
    /// # let employee_schema = Arc::new(Schema::new(vec![
    /// #           Field::new("id", DataType::Int32, false),
    /// # ])) as _;
    /// # let table_source = Arc::new(LogicalTableSource::new(employee_schema));
    /// // Scan table_source with the name "mytable" (after normalization)
    /// # let table = table_source.clone();
    /// let scan = LogicalPlanBuilder::scan("MyTable", table, None);
    ///
    /// // Scan table_source with the name "MyTable" by enclosing in quotes
    /// # let table = table_source.clone();
    /// let scan = LogicalPlanBuilder::scan(r#""MyTable""#, table, None);
    ///
    /// // Scan table_source with the name "MyTable" by forming the table reference
    /// # let table = table_source.clone();
    /// let table_reference = TableReference::bare("MyTable");
    /// let scan = LogicalPlanBuilder::scan(table_reference, table, None);
    /// ```
    pub fn scan(
        table_name: impl Into<OwnedTableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Self::scan_with_filters(table_name, table_source, projection, vec![])
    }

    /// Create a [CopyTo] for copying the contents of this builder to the specified file(s)
    pub fn copy_to(
        input: LogicalPlan,
        output_url: String,
        file_format: FileType,
        single_file_output: bool,
        copy_options: CopyOptions,
    ) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Copy(CopyTo {
            input: Arc::new(input),
            output_url,
            file_format,
            single_file_output,
            copy_options,
        })))
    }

    /// Create a [DmlStatement] for inserting the contents of this builder into the named table
    pub fn insert_into(
        input: LogicalPlan,
        table_name: impl Into<OwnedTableReference>,
        table_schema: &Schema,
        overwrite: bool,
    ) -> Result<Self> {
        let table_schema = table_schema.clone().to_dfschema_ref()?;

        let op = if overwrite {
            WriteOp::InsertOverwrite
        } else {
            WriteOp::InsertInto
        };

        Ok(Self::from(LogicalPlan::Dml(DmlStatement {
            table_name: table_name.into(),
            table_schema,
            op,
            input: Arc::new(input),
        })))
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan_with_filters(
        table_name: impl Into<OwnedTableReference>,
        table_source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<Self> {
        TableScan::try_new(table_name, table_source, projection, filters, None)
            .map(LogicalPlan::TableScan)
            .map(Self::from)
    }

    /// Wrap a plan in a window
    pub fn window_plan(
        input: LogicalPlan,
        window_exprs: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let mut plan = input;
        let mut groups = group_window_expr_by_sort_keys(&window_exprs)?;
        // To align with the behavior of PostgreSQL, we want the sort_keys sorted as same rule as PostgreSQL that first
        // we compare the sort key themselves and if one window's sort keys are a prefix of another
        // put the window with more sort keys first. so more deeply sorted plans gets nested further down as children.
        // The sort_by() implementation here is a stable sort.
        // Note that by this rule if there's an empty over, it'll be at the top level
        groups.sort_by(|(key_a, _), (key_b, _)| {
            for ((first, _), (second, _)) in key_a.iter().zip(key_b.iter()) {
                let key_ordering = compare_sort_expr(first, second, plan.schema());
                match key_ordering {
                    Ordering::Less => {
                        return Ordering::Less;
                    }
                    Ordering::Greater => {
                        return Ordering::Greater;
                    }
                    Ordering::Equal => {}
                }
            }
            key_b.len().cmp(&key_a.len())
        });
        for (_, exprs) in groups {
            let window_exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            // Partition and sorting is done at physical level, see the EnforceDistribution
            // and EnforceSorting rules.
            plan = LogicalPlanBuilder::from(plan)
                .window(window_exprs)?
                .build()?;
        }
        Ok(plan)
    }
    /// Apply a projection without alias.
    pub fn project(
        self,
        expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        project(self.plan, expr).map(Self::from)
    }

    /// Select the given column indices
    pub fn select(self, indices: impl IntoIterator<Item = usize>) -> Result<Self> {
        let fields = self.plan.schema().fields();
        let exprs: Vec<_> = indices
            .into_iter()
            .map(|x| Expr::Column(fields[x].qualified_column()))
            .collect();
        self.project(exprs)
    }

    /// Apply a filter
    pub fn filter(self, expr: impl Into<Expr>) -> Result<Self> {
        let expr = normalize_col(expr.into(), &self.plan)?;
        Filter::try_new(expr, Arc::new(self.plan), None)
            .map(LogicalPlan::Filter)
            .map(Self::from)
    }

    /// Make a builder for a prepare logical plan from the builder's plan
    pub fn prepare(self, name: String, data_types: Vec<DataType>) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Prepare(Prepare {
            name,
            data_types,
            input: Arc::new(self.plan),
        })))
    }

    /// Limit the number of rows returned
    ///
    /// `skip` - Number of rows to skip before fetch any row.
    ///
    /// `fetch` - Maximum number of rows to fetch, after skipping `skip` rows,
    ///          if specified.
    pub fn limit(self, skip: usize, fetch: Option<usize>) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Limit(Limit {
            skip,
            fetch,
            input: Arc::new(self.plan),
        })))
    }

    /// Apply an alias
    pub fn alias(self, alias: impl Into<OwnedTableReference>) -> Result<Self> {
        subquery_alias(self.plan, alias).map(Self::from)
    }

    /// Add missing sort columns to all downstream projection
    ///
    /// Thus, if you have a LogialPlan that selects A and B and have
    /// not requested a sort by C, this code will add C recursively to
    /// all input projections.
    ///
    /// Adding a new column is not correct if there is a `Distinct`
    /// node, which produces only distinct values of its
    /// inputs. Adding a new column to its input will result in
    /// potententially different results than with the original column.
    ///
    /// For example, if the input is like:
    ///
    /// Distinct(A, B)
    ///
    /// If the input looks like
    ///
    /// a | b | c
    /// --+---+---
    /// 1 | 2 | 3
    /// 1 | 2 | 4
    ///
    /// Distinct (A, B) --> (1,2)
    ///
    /// But Distinct (A, B, C) --> (1, 2, 3), (1, 2, 4)
    ///  (which will appear as a (1, 2), (1, 2) if a and b are projected
    ///
    /// See <https://github.com/apache/arrow-datafusion/issues/5065> for more details
    fn add_missing_columns(
        curr_plan: LogicalPlan,
        missing_cols: &[Column],
        is_distinct: bool,
    ) -> Result<LogicalPlan> {
        match curr_plan {
            LogicalPlan::Projection(Projection {
                input,
                mut expr,
                schema: _,
            }) if missing_cols.iter().all(|c| input.schema().has_column(c)) => {
                let mut missing_exprs = missing_cols
                    .iter()
                    .map(|c| normalize_col(Expr::Column(c.clone()), &input))
                    .collect::<Result<Vec<_>>>()?;

                // Do not let duplicate columns to be added, some of the
                // missing_cols may be already present but without the new
                // projected alias.
                missing_exprs.retain(|e| !expr.contains(e));
                if is_distinct {
                    Self::ambiguous_distinct_check(&missing_exprs, missing_cols, &expr)?;
                }
                expr.extend(missing_exprs);
                project((*input).clone(), expr)
            }
            _ => {
                let is_distinct =
                    is_distinct || matches!(curr_plan, LogicalPlan::Distinct(_));
                let new_inputs = curr_plan
                    .inputs()
                    .into_iter()
                    .map(|input_plan| {
                        Self::add_missing_columns(
                            (*input_plan).clone(),
                            missing_cols,
                            is_distinct,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                curr_plan.with_new_inputs(&new_inputs)
            }
        }
    }

    fn ambiguous_distinct_check(
        missing_exprs: &[Expr],
        missing_cols: &[Column],
        projection_exprs: &[Expr],
    ) -> Result<()> {
        if missing_exprs.is_empty() {
            return Ok(());
        }

        // if the missing columns are all only aliases for things in
        // the existing select list, it is ok
        //
        // This handles the special case for
        // SELECT col as <alias> ORDER BY <alias>
        //
        // As described in https://github.com/apache/arrow-datafusion/issues/5293
        let all_aliases = missing_exprs.iter().all(|e| {
            projection_exprs.iter().any(|proj_expr| {
                if let Expr::Alias(Alias { expr, .. }) = proj_expr {
                    e == expr.as_ref()
                } else {
                    false
                }
            })
        });
        if all_aliases {
            return Ok(());
        }

        let missing_col_names = missing_cols
            .iter()
            .map(|col| col.flat_name())
            .collect::<String>();

        plan_err!("For SELECT DISTINCT, ORDER BY expressions {missing_col_names} must appear in select list")
    }

    /// Apply a sort
    pub fn sort(
        self,
        exprs: impl IntoIterator<Item = impl Into<Expr>> + Clone,
    ) -> Result<Self> {
        let exprs = rewrite_sort_cols_by_aggs(exprs, &self.plan)?;

        let schema = self.plan.schema();

        // Collect sort columns that are missing in the input plan's schema
        let mut missing_cols: Vec<Column> = vec![];
        exprs
            .clone()
            .into_iter()
            .try_for_each::<_, Result<()>>(|expr| {
                let columns = expr.to_columns()?;

                columns.into_iter().for_each(|c| {
                    if schema.field_from_column(&c).is_err() {
                        missing_cols.push(c);
                    }
                });

                Ok(())
            })?;

        if missing_cols.is_empty() {
            return Ok(Self::from(LogicalPlan::Sort(Sort {
                expr: normalize_cols(exprs, &self.plan)?,
                input: Arc::new(self.plan),
                fetch: None,
            })));
        }

        // remove pushed down sort columns
        let new_expr = schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();

        let is_distinct = false;
        let plan = Self::add_missing_columns(self.plan, &missing_cols, is_distinct)?;
        let sort_plan = LogicalPlan::Sort(Sort {
            expr: normalize_cols(exprs, &plan)?,
            input: Arc::new(plan),
            fetch: None,
        });

        Projection::try_new(new_expr, Arc::new(sort_plan))
            .map(LogicalPlan::Projection)
            .map(Self::from)
    }

    /// Apply a union, preserving duplicate rows
    pub fn union(self, plan: LogicalPlan) -> Result<Self> {
        union(self.plan, plan).map(Self::from)
    }

    /// Apply a union, removing duplicate rows
    pub fn union_distinct(self, plan: LogicalPlan) -> Result<Self> {
        let left_plan: LogicalPlan = self.plan;
        let right_plan: LogicalPlan = plan;

        Ok(Self::from(LogicalPlan::Distinct(Distinct::All(Arc::new(
            union(left_plan, right_plan)?,
        )))))
    }

    /// Apply deduplication: Only distinct (different) values are returned)
    pub fn distinct(self) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Distinct(Distinct::All(Arc::new(
            self.plan,
        )))))
    }

    /// Project first values of the specified expression list according to the provided
    /// sorting expressions grouped by the `DISTINCT ON` clause expressions.
    pub fn distinct_on(
        self,
        on_expr: Vec<Expr>,
        select_expr: Vec<Expr>,
        sort_expr: Option<Vec<Expr>>,
    ) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Distinct(Distinct::On(
            DistinctOn::try_new(on_expr, select_expr, sort_expr, Arc::new(self.plan))?,
        ))))
    }

    /// Apply a join to `right` using explicitly specified columns and an
    /// optional filter expression.
    ///
    /// See [`join_on`](Self::join_on) for a more concise way to specify the
    /// join condition. Since DataFusion will automatically identify and
    /// optimize equality predicates there is no performance difference between
    /// this function and `join_on`
    ///
    /// `left_cols` and `right_cols` are used to form "equijoin" predicates (see
    /// example below), which are then combined with the optional `filter`
    /// expression.
    ///
    /// Note that in case of outer join, the `filter` is applied to only matched rows.
    pub fn join(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
    ) -> Result<Self> {
        self.join_detailed(right, join_type, join_keys, filter, false)
    }

    /// Apply a join with using the specified expressions.
    ///
    /// Note that DataFusion automatically optimizes joins, including
    /// identifying and optimizing equality predicates.
    ///
    /// # Example
    ///
    /// ```
    /// # use datafusion_expr::{Expr, col, LogicalPlanBuilder,
    /// #  logical_plan::builder::LogicalTableSource, logical_plan::JoinType,};
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{Schema, DataType, Field};
    /// # use datafusion_common::Result;
    /// # fn main() -> Result<()> {
    /// let example_schema = Arc::new(Schema::new(vec![
    ///     Field::new("a", DataType::Int32, false),
    ///     Field::new("b", DataType::Int32, false),
    ///     Field::new("c", DataType::Int32, false),
    /// ]));
    /// let table_source = Arc::new(LogicalTableSource::new(example_schema));
    /// let left_table = table_source.clone();
    /// let right_table = table_source.clone();
    ///
    /// let right_plan = LogicalPlanBuilder::scan("right", right_table, None)?.build()?;
    ///
    /// // Form the expression `(left.a != right.a)` AND `(left.b != right.b)`
    /// let exprs = vec![
    ///     col("left.a").eq(col("right.a")),
    ///     col("left.b").not_eq(col("right.b"))
    ///  ];
    ///
    /// // Perform the equivalent of `left INNER JOIN right ON (a != a2 AND b != b2)`
    /// // finding all pairs of rows from `left` and `right` where
    /// // where `a = a2` and `b != b2`.
    /// let plan = LogicalPlanBuilder::scan("left", left_table, None)?
    ///     .join_on(right_plan, JoinType::Inner, exprs)?
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn join_on(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        on_exprs: impl IntoIterator<Item = Expr>,
    ) -> Result<Self> {
        let filter = on_exprs.into_iter().reduce(Expr::and);

        self.join_detailed(
            right,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter,
            false,
        )
    }

    pub(crate) fn normalize(
        plan: &LogicalPlan,
        column: impl Into<Column> + Clone,
    ) -> Result<Column> {
        let schema = plan.schema();
        let fallback_schemas = plan.fallback_normalize_schemas();
        let using_columns = plan.using_columns()?;
        column.into().normalize_with_schemas_and_ambiguity_check(
            &[&[schema], &fallback_schemas],
            &using_columns,
        )
    }

    /// Apply a join with on constraint and specified null equality.
    ///
    /// The behavior is the same as [`join`](Self::join) except that it allows
    /// specifying the null equality behavior.
    ///
    /// If `null_equals_null=true`, rows where both join keys are `null` will be
    /// emitted. Otherwise rows where either or both join keys are `null` will be
    /// omitted.
    pub fn join_detailed(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        filter: Option<Expr>,
        null_equals_null: bool,
    ) -> Result<Self> {
        if join_keys.0.len() != join_keys.1.len() {
            return plan_err!("left_keys and right_keys were not the same length");
        }

        let filter = if let Some(expr) = filter {
            let filter = normalize_col_with_schemas_and_ambiguity_check(
                expr,
                &[&[self.schema(), right.schema()]],
                &[],
            )?;
            Some(filter)
        } else {
            None
        };

        let (left_keys, right_keys): (Vec<Result<Column>>, Vec<Result<Column>>) =
            join_keys
                .0
                .into_iter()
                .zip(join_keys.1)
                .map(|(l, r)| {
                    let l = l.into();
                    let r = r.into();

                    match (&l.relation, &r.relation) {
                        (Some(lr), Some(rr)) => {
                            let l_is_left =
                                self.plan.schema().field_with_qualified_name(lr, &l.name);
                            let l_is_right =
                                right.schema().field_with_qualified_name(lr, &l.name);
                            let r_is_left =
                                self.plan.schema().field_with_qualified_name(rr, &r.name);
                            let r_is_right =
                                right.schema().field_with_qualified_name(rr, &r.name);

                            match (l_is_left, l_is_right, r_is_left, r_is_right) {
                                (_, Ok(_), Ok(_), _) => (Ok(r), Ok(l)),
                                (Ok(_), _, _, Ok(_)) => (Ok(l), Ok(r)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(&right, r),
                                ),
                            }
                        }
                        (Some(lr), None) => {
                            let l_is_left =
                                self.plan.schema().field_with_qualified_name(lr, &l.name);
                            let l_is_right =
                                right.schema().field_with_qualified_name(lr, &l.name);

                            match (l_is_left, l_is_right) {
                                (Ok(_), _) => (Ok(l), Self::normalize(&right, r)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, r), Ok(l)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(&right, r),
                                ),
                            }
                        }
                        (None, Some(rr)) => {
                            let r_is_left =
                                self.plan.schema().field_with_qualified_name(rr, &r.name);
                            let r_is_right =
                                right.schema().field_with_qualified_name(rr, &r.name);

                            match (r_is_left, r_is_right) {
                                (Ok(_), _) => (Ok(r), Self::normalize(&right, l)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, l), Ok(r)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(&right, r),
                                ),
                            }
                        }
                        (None, None) => {
                            let mut swap = false;
                            let left_key = Self::normalize(&self.plan, l.clone())
                                .or_else(|_| {
                                    swap = true;
                                    Self::normalize(&right, l)
                                });
                            if swap {
                                (Self::normalize(&self.plan, r), left_key)
                            } else {
                                (left_key, Self::normalize(&right, r))
                            }
                        }
                    }
                })
                .unzip();

        let left_keys = left_keys.into_iter().collect::<Result<Vec<Column>>>()?;
        let right_keys = right_keys.into_iter().collect::<Result<Vec<Column>>>()?;

        let on = left_keys
            .into_iter()
            .zip(right_keys)
            .map(|(l, r)| (Expr::Column(l), Expr::Column(r)))
            .collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;

        Ok(Self::from(LogicalPlan::Join(Join {
            left: Arc::new(self.plan),
            right: Arc::new(right),
            on,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: DFSchemaRef::new(join_schema),
            null_equals_null,
        })))
    }

    /// Apply a join with using constraint, which duplicates all join columns in output schema.
    pub fn join_using(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        using_keys: Vec<impl Into<Column> + Clone>,
    ) -> Result<Self> {
        let left_keys: Vec<Column> = using_keys
            .clone()
            .into_iter()
            .map(|c| Self::normalize(&self.plan, c))
            .collect::<Result<_>>()?;
        let right_keys: Vec<Column> = using_keys
            .into_iter()
            .map(|c| Self::normalize(&right, c))
            .collect::<Result<_>>()?;

        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys).collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;
        let mut join_on: Vec<(Expr, Expr)> = vec![];
        let mut filters: Option<Expr> = None;
        for (l, r) in &on {
            if self.plan.schema().has_column(l)
                && right.schema().has_column(r)
                && can_hash(self.plan.schema().field_from_column(l)?.data_type())
            {
                join_on.push((Expr::Column(l.clone()), Expr::Column(r.clone())));
            } else if self.plan.schema().has_column(l)
                && right.schema().has_column(r)
                && can_hash(self.plan.schema().field_from_column(r)?.data_type())
            {
                join_on.push((Expr::Column(r.clone()), Expr::Column(l.clone())));
            } else {
                let expr = binary_expr(
                    Expr::Column(l.clone()),
                    Operator::Eq,
                    Expr::Column(r.clone()),
                );
                match filters {
                    None => filters = Some(expr),
                    Some(filter_expr) => filters = Some(and(expr, filter_expr)),
                }
            }
        }

        if join_on.is_empty() {
            let join = Self::from(self.plan).cross_join(right)?;
            join.filter(filters.ok_or_else(|| {
                DataFusionError::Internal("filters should not be None here".to_string())
            })?)
        } else {
            Ok(Self::from(LogicalPlan::Join(Join {
                left: Arc::new(self.plan),
                right: Arc::new(right),
                on: join_on,
                filter: filters,
                join_type,
                join_constraint: JoinConstraint::Using,
                schema: DFSchemaRef::new(join_schema),
                null_equals_null: false,
            })))
        }
    }

    /// Apply a cross join
    pub fn cross_join(self, right: LogicalPlan) -> Result<Self> {
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &JoinType::Inner)?;
        Ok(Self::from(LogicalPlan::CrossJoin(CrossJoin {
            left: Arc::new(self.plan),
            right: Arc::new(right),
            schema: DFSchemaRef::new(join_schema),
        })))
    }

    /// Repartition
    pub fn repartition(self, partitioning_scheme: Partitioning) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Repartition(Repartition {
            input: Arc::new(self.plan),
            partitioning_scheme,
        })))
    }

    /// Apply a window functions to extend the schema
    pub fn window(
        self,
        window_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let window_expr = normalize_cols(window_expr, &self.plan)?;
        validate_unique_names("Windows", &window_expr)?;
        Ok(Self::from(LogicalPlan::Window(Window::try_new(
            window_expr,
            Arc::new(self.plan),
        )?)))
    }

    /// Apply an aggregate: grouping on the `group_expr` expressions
    /// and calculating `aggr_expr` aggregates for each distinct
    /// value of the `group_expr`;
    pub fn aggregate(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;
        Aggregate::try_new(Arc::new(self.plan), group_expr, aggr_expr)
            .map(LogicalPlan::Aggregate)
            .map(Self::from)
    }

    /// Create an expression to represent the explanation of the plan
    ///
    /// if `analyze` is true, runs the actual plan and produces
    /// information about metrics during run.
    ///
    /// if `verbose` is true, prints out additional details.
    pub fn explain(self, verbose: bool, analyze: bool) -> Result<Self> {
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(Self::from(LogicalPlan::Analyze(Analyze {
                verbose,
                input: Arc::new(self.plan),
                schema,
            })))
        } else {
            let stringified_plans =
                vec![self.plan.to_stringified(PlanType::InitialLogicalPlan)];

            Ok(Self::from(LogicalPlan::Explain(Explain {
                verbose,
                plan: Arc::new(self.plan),
                stringified_plans,
                schema,
                logical_optimization_succeeded: false,
            })))
        }
    }

    /// Process intersect set operator
    pub fn intersect(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::intersect_or_except(
            left_plan,
            right_plan,
            JoinType::LeftSemi,
            is_all,
        )
    }

    /// Process except set operator
    pub fn except(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::intersect_or_except(
            left_plan,
            right_plan,
            JoinType::LeftAnti,
            is_all,
        )
    }

    /// Process intersect or except
    fn intersect_or_except(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        join_type: JoinType,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        let left_len = left_plan.schema().fields().len();
        let right_len = right_plan.schema().fields().len();

        if left_len != right_len {
            return plan_err!(
                "INTERSECT/EXCEPT query must have the same number of columns. Left is {left_len} and right is {right_len}."
            );
        }

        let join_keys = left_plan
            .schema()
            .fields()
            .iter()
            .zip(right_plan.schema().fields().iter())
            .map(|(left_field, right_field)| {
                (
                    (Column::from_name(left_field.name())),
                    (Column::from_name(right_field.name())),
                )
            })
            .unzip();
        if is_all {
            LogicalPlanBuilder::from(left_plan)
                .join_detailed(right_plan, join_type, join_keys, None, true)?
                .build()
        } else {
            LogicalPlanBuilder::from(left_plan)
                .distinct()?
                .join_detailed(right_plan, join_type, join_keys, None, true)?
                .build()
        }
    }

    /// Build the plan
    pub fn build(self) -> Result<LogicalPlan> {
        Ok(self.plan)
    }

    /// Apply a join with the expression on constraint.
    ///
    /// equi_exprs are "equijoin" predicates expressions on the existing and right inputs, respectively.
    ///
    /// filter: any other filter expression to apply during the join. equi_exprs predicates are likely
    /// to be evaluated more quickly than the filter expressions
    pub fn join_with_expr_keys(
        self,
        right: LogicalPlan,
        join_type: JoinType,
        equi_exprs: (Vec<impl Into<Expr>>, Vec<impl Into<Expr>>),
        filter: Option<Expr>,
    ) -> Result<Self> {
        if equi_exprs.0.len() != equi_exprs.1.len() {
            return plan_err!("left_keys and right_keys were not the same length");
        }

        let join_key_pairs = equi_exprs
            .0
            .into_iter()
            .zip(equi_exprs.1.into_iter())
            .map(|(l, r)| {
                let left_key = l.into();
                let right_key = r.into();

                let left_using_columns = left_key.to_columns()?;
                let normalized_left_key = normalize_col_with_schemas_and_ambiguity_check(
                    left_key,
                    &[&[self.plan.schema(), right.schema()]],
                    &[left_using_columns],
                )?;

                let right_using_columns = right_key.to_columns()?;
                let normalized_right_key = normalize_col_with_schemas_and_ambiguity_check(
                    right_key,
                    &[&[self.plan.schema(), right.schema()]],
                    &[right_using_columns],
                )?;

                // find valid equijoin
                find_valid_equijoin_key_pair(
                        &normalized_left_key,
                        &normalized_right_key,
                        self.plan.schema().clone(),
                        right.schema().clone(),
                    )?.ok_or_else(||
                        plan_datafusion_err!(
                            "can't create join plan, join key should belong to one input, error key: ({normalized_left_key},{normalized_right_key})"
                        ))
            })
            .collect::<Result<Vec<_>>>()?;

        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;

        Ok(Self::from(LogicalPlan::Join(Join {
            left: Arc::new(self.plan),
            right: Arc::new(right),
            on: join_key_pairs,
            filter,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: DFSchemaRef::new(join_schema),
            null_equals_null: false,
        })))
    }

    /// Unnest the given column.
    pub fn unnest_column(self, column: impl Into<Column>) -> Result<Self> {
        Ok(Self::from(unnest(self.plan, column.into())?))
    }

    /// Unnest the given column given [`UnnestOptions`]
    pub fn unnest_column_with_options(
        self,
        column: impl Into<Column>,
        options: UnnestOptions,
    ) -> Result<Self> {
        Ok(Self::from(unnest_with_options(
            self.plan,
            column.into(),
            options,
        )?))
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &DFSchema,
    right: &DFSchema,
    join_type: &JoinType,
) -> Result<DFSchema> {
    fn nullify_fields(fields: &[DFField]) -> Vec<DFField> {
        fields
            .iter()
            .map(|f| f.clone().with_nullable(true))
            .collect()
    }

    let right_fields = right.fields();
    let left_fields = left.fields();

    let fields: Vec<DFField> = match join_type {
        JoinType::Inner => {
            // left then right
            left_fields
                .iter()
                .chain(right_fields.iter())
                .cloned()
                .collect()
        }
        JoinType::Left => {
            // left then right, right set to nullable in case of not matched scenario
            left_fields
                .iter()
                .chain(&nullify_fields(right_fields))
                .cloned()
                .collect()
        }
        JoinType::Right => {
            // left then right, left set to nullable in case of not matched scenario
            nullify_fields(left_fields)
                .iter()
                .chain(right_fields.iter())
                .cloned()
                .collect()
        }
        JoinType::Full => {
            // left then right, all set to nullable in case of not matched scenario
            nullify_fields(left_fields)
                .iter()
                .chain(&nullify_fields(right_fields))
                .cloned()
                .collect()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            // Only use the left side for the schema
            left_fields.clone()
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            // Only use the right side for the schema
            right_fields.clone()
        }
    };
    let func_dependencies = left.functional_dependencies().join(
        right.functional_dependencies(),
        join_type,
        left_fields.len(),
    );
    let mut metadata = left.metadata().clone();
    metadata.extend(right.metadata().clone());
    DFSchema::new_with_metadata(fields, metadata)
        .map(|schema| schema.with_functional_dependencies(func_dependencies))
}

/// Errors if one or more expressions have equal names.
pub(crate) fn validate_unique_names<'a>(
    node_name: &str,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.into_iter().enumerate().try_for_each(|(position, expr)| {
        let name = expr.display_name()?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                plan_err!("{node_name} require unique expression names \
                             but the expression \"{existing_expr}\" at position {existing_position} and \"{expr}\" \
                             at position {position} have the same name. Consider aliasing (\"AS\") one of them."
                            )
            }
        }
    })
}

pub fn project_with_column_index(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            Expr::Alias(Alias { ref name, .. }) if name != schema.field(i).name() => {
                e.unalias().alias(schema.field(i).name())
            }
            Expr::Column(Column {
                relation: _,
                ref name,
            }) if name != schema.field(i).name() => e.alias(schema.field(i).name()),
            Expr::Alias { .. } | Expr::Column { .. } => e,
            _ => e.alias(schema.field(i).name()),
        })
        .collect::<Vec<_>>();

    Projection::try_new_with_schema(alias_expr, input, schema)
        .map(LogicalPlan::Projection)
}

/// Union two logical plans.
pub fn union(left_plan: LogicalPlan, right_plan: LogicalPlan) -> Result<LogicalPlan> {
    let left_col_num = left_plan.schema().fields().len();

    // check union plan length same.
    let right_col_num = right_plan.schema().fields().len();
    if right_col_num != left_col_num {
        return plan_err!(
            "Union queries must have the same number of columns, (left is {left_col_num}, right is {right_col_num})");
    }

    // create union schema
    let union_schema = zip(
        left_plan.schema().fields().iter(),
        right_plan.schema().fields().iter(),
    )
    .map(|(left_field, right_field)| {
        let nullable = left_field.is_nullable() || right_field.is_nullable();
        let data_type =
            comparison_coercion(left_field.data_type(), right_field.data_type())
                .ok_or_else(|| {
                    plan_datafusion_err!(
                "UNION Column {} (type: {}) is not compatible with column {} (type: {})",
                right_field.name(),
                right_field.data_type(),
                left_field.name(),
                left_field.data_type()
            )
                })?;

        Ok(DFField::new(
            left_field.qualifier().cloned(),
            left_field.name(),
            data_type,
            nullable,
        ))
    })
    .collect::<Result<Vec<_>>>()?
    .to_dfschema()?;

    let inputs = vec![left_plan, right_plan]
        .into_iter()
        .map(|p| {
            let plan = coerce_plan_expr_for_schema(&p, &union_schema)?;
            match plan {
                LogicalPlan::Projection(Projection { expr, input, .. }) => {
                    Ok(Arc::new(project_with_column_index(
                        expr,
                        input,
                        Arc::new(union_schema.clone()),
                    )?))
                }
                other_plan => Ok(Arc::new(other_plan)),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    if inputs.is_empty() {
        return plan_err!("Empty UNION");
    }

    Ok(LogicalPlan::Union(Union {
        inputs,
        schema: Arc::new(union_schema),
    }))
}

/// Create Projection
/// # Errors
/// This function errors under any of the following conditions:
/// * Two or more expressions have the same name
/// * An invalid expression is used (e.g. a `sort` expression)
pub fn project(
    plan: LogicalPlan,
    expr: impl IntoIterator<Item = impl Into<Expr>>,
) -> Result<LogicalPlan> {
    // TODO: move it into analyzer
    let input_schema = plan.schema();
    let mut projected_expr = vec![];
    for e in expr {
        let e = e.into();
        match e {
            Expr::Wildcard { qualifier: None } => {
                projected_expr.extend(expand_wildcard(input_schema, &plan, None)?)
            }
            Expr::Wildcard {
                qualifier: Some(qualifier),
            } => projected_expr.extend(expand_qualified_wildcard(
                &qualifier,
                input_schema,
                None,
            )?),
            _ => projected_expr
                .push(columnize_expr(normalize_col(e, &plan)?, input_schema)),
        }
    }
    validate_unique_names("Projections", projected_expr.iter())?;

    Projection::try_new(projected_expr, Arc::new(plan)).map(LogicalPlan::Projection)
}

/// Create a SubqueryAlias to wrap a LogicalPlan.
pub fn subquery_alias(
    plan: LogicalPlan,
    alias: impl Into<OwnedTableReference>,
) -> Result<LogicalPlan> {
    SubqueryAlias::try_new(plan, alias).map(LogicalPlan::SubqueryAlias)
}

/// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema.
/// This is mostly used for testing and documentation.
pub fn table_scan<'a>(
    name: Option<impl Into<TableReference<'a>>>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    table_scan_with_filters(name, table_schema, projection, vec![])
}

/// Create a LogicalPlanBuilder representing a scan of a table with the provided name and schema,
/// and inlined filters.
/// This is mostly used for testing and documentation.
pub fn table_scan_with_filters<'a>(
    name: Option<impl Into<TableReference<'a>>>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
) -> Result<LogicalPlanBuilder> {
    let table_source = table_source(table_schema);
    let name = name
        .map(|n| n.into())
        .unwrap_or_else(|| OwnedTableReference::bare(UNNAMED_TABLE))
        .to_owned_reference();
    LogicalPlanBuilder::scan_with_filters(name, table_source, projection, filters)
}

fn table_source(table_schema: &Schema) -> Arc<dyn TableSource> {
    let table_schema = Arc::new(table_schema.clone());
    Arc::new(LogicalTableSource { table_schema })
}

/// Wrap projection for a plan, if the join keys contains normal expression.
pub fn wrap_projection_for_join_if_necessary(
    join_keys: &[Expr],
    input: LogicalPlan,
) -> Result<(LogicalPlan, Vec<Column>, bool)> {
    let input_schema = input.schema();
    let alias_join_keys: Vec<Expr> = join_keys
        .iter()
        .map(|key| {
            // The display_name() of cast expression will ignore the cast info, and show the inner expression name.
            // If we do not add alais, it will throw same field name error in the schema when adding projection.
            // For example:
            //    input scan : [a, b, c],
            //    join keys: [cast(a as int)]
            //
            //  then a and cast(a as int) will use the same field name - `a` in projection schema.
            //  https://github.com/apache/arrow-datafusion/issues/4478
            if matches!(key, Expr::Cast(_)) || matches!(key, Expr::TryCast(_)) {
                let alias = format!("{key}");
                key.clone().alias(alias)
            } else {
                key.clone()
            }
        })
        .collect::<Vec<_>>();

    let need_project = join_keys.iter().any(|key| !matches!(key, Expr::Column(_)));
    let plan = if need_project {
        let mut projection = expand_wildcard(input_schema, &input, None)?;
        let join_key_items = alias_join_keys
            .iter()
            .flat_map(|expr| expr.try_into_col().is_err().then_some(expr))
            .cloned()
            .collect::<HashSet<Expr>>();
        projection.extend(join_key_items);

        LogicalPlanBuilder::from(input)
            .project(projection)?
            .build()?
    } else {
        input
    };

    let join_on = alias_join_keys
        .into_iter()
        .map(|key| {
            key.try_into_col()
                .or_else(|_| Ok(Column::from_name(key.display_name()?)))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((plan, join_on, need_project))
}

/// Basic TableSource implementation intended for use in tests and documentation. It is expected
/// that users will provide their own TableSource implementations or use DataFusion's
/// DefaultTableSource.
pub struct LogicalTableSource {
    table_schema: SchemaRef,
}

impl LogicalTableSource {
    /// Create a new LogicalTableSource
    pub fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl TableSource for LogicalTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<crate::TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

/// Create a [`LogicalPlan::Unnest`] plan
pub fn unnest(input: LogicalPlan, column: Column) -> Result<LogicalPlan> {
    unnest_with_options(input, column, UnnestOptions::new())
}

/// Create a [`LogicalPlan::Unnest`] plan with options
pub fn unnest_with_options(
    input: LogicalPlan,
    column: Column,
    options: UnnestOptions,
) -> Result<LogicalPlan> {
    let unnest_field = input.schema().field_from_column(&column)?;

    // Extract the type of the nested field in the list.
    let unnested_field = match unnest_field.data_type() {
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field) => DFField::new(
            unnest_field.qualifier().cloned(),
            unnest_field.name(),
            field.data_type().clone(),
            unnest_field.is_nullable(),
        ),
        _ => {
            // If the unnest field is not a list type return the input plan.
            return Ok(input);
        }
    };

    // Update the schema with the unnest column type changed to contain the nested type.
    let input_schema = input.schema();
    let fields = input_schema
        .fields()
        .iter()
        .map(|f| {
            if f == unnest_field {
                unnested_field.clone()
            } else {
                f.clone()
            }
        })
        .collect::<Vec<_>>();

    let metadata = input_schema.metadata().clone();
    let df_schema = DFSchema::new_with_metadata(fields, metadata)?;
    // We can use the existing functional dependencies:
    let deps = input_schema.functional_dependencies().clone();
    let schema = Arc::new(df_schema.with_functional_dependencies(deps));

    Ok(LogicalPlan::Unnest(Unnest {
        input: Arc::new(input),
        column: unnested_field.qualified_column(),
        schema,
        options,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::StringifiedPlan;
    use crate::{col, expr, expr_fn::exists, in_subquery, lit, scalar_subquery, sum};

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{OwnedTableReference, SchemaError, TableReference};

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .filter(col("state").eq(lit("CO")))?
                .project(vec![col("id")])?
                .build()?;

        let expected = "Projection: employee_csv.id\
        \n  Filter: employee_csv.state = Utf8(\"CO\")\
        \n    TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn plan_builder_schema() {
        let schema = employee_schema();
        let projection = None;
        let plan =
            LogicalPlanBuilder::scan("employee_csv", table_source(&schema), projection)
                .unwrap();
        let expected = DFSchema::try_from_qualified_schema(
            TableReference::bare("employee_csv"),
            &schema,
        )
        .unwrap();
        assert_eq!(&expected, plan.schema().as_ref());

        // Note scan of "EMPLOYEE_CSV" is treated as a SQL identifer
        // (and thus normalized to "employee"csv") as well
        let projection = None;
        let plan =
            LogicalPlanBuilder::scan("EMPLOYEE_CSV", table_source(&schema), projection)
                .unwrap();
        assert_eq!(&expected, plan.schema().as_ref());
    }

    #[test]
    fn plan_builder_empty_name() {
        let schema = employee_schema();
        let projection = None;
        let err =
            LogicalPlanBuilder::scan("", table_source(&schema), projection).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: table_name cannot be empty"
        );
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?
                .aggregate(
                    vec![col("state")],
                    vec![sum(col("salary")).alias("total_salary")],
                )?
                .project(vec![col("state"), col("total_salary")])?
                .limit(2, Some(10))?
                .build()?;

        let expected = "Limit: skip=2, fetch=10\
                \n  Projection: employee_csv.state, total_salary\
                \n    Aggregate: groupBy=[[employee_csv.state]], aggr=[[SUM(employee_csv.salary) AS total_salary]]\
                \n      TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?
                .sort(vec![
                    Expr::Sort(expr::Sort::new(Box::new(col("state")), true, true)),
                    Expr::Sort(expr::Sort::new(Box::new(col("salary")), false, false)),
                ])?
                .build()?;

        let expected = "Sort: employee_csv.state ASC NULLS FIRST, employee_csv.salary DESC NULLS LAST\
        \n  TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn plan_using_join_wildcard_projection() -> Result<()> {
        let t2 = table_scan(Some("t2"), &employee_schema(), None)?.build()?;

        let plan = table_scan(Some("t1"), &employee_schema(), None)?
            .join_using(t2, JoinType::Inner, vec!["id"])?
            .project(vec![Expr::Wildcard { qualifier: None }])?
            .build()?;

        // id column should only show up once in projection
        let expected = "Projection: t1.id, t1.first_name, t1.last_name, t1.state, t1.salary, t2.first_name, t2.last_name, t2.state, t2.salary\
        \n  Inner Join: Using t1.id = t2.id\
        \n    TableScan: t1\
        \n    TableScan: t2";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn plan_builder_union() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?;

        let plan = plan
            .clone()
            .union(plan.clone().build()?)?
            .union(plan.clone().build()?)?
            .union(plan.build()?)?
            .build()?;

        let expected = "Union\
        \n  Union\
        \n    Union\
        \n      TableScan: employee_csv projection=[state, salary]\
        \n      TableScan: employee_csv projection=[state, salary]\
        \n    TableScan: employee_csv projection=[state, salary]\
        \n  TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn plan_builder_union_distinct() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![3, 4]))?;

        let plan = plan
            .clone()
            .union_distinct(plan.clone().build()?)?
            .union_distinct(plan.clone().build()?)?
            .union_distinct(plan.build()?)?
            .build()?;

        let expected = "\
        Distinct:\
        \n  Union\
        \n    Distinct:\
        \n      Union\
        \n        Distinct:\
        \n          Union\
        \n            TableScan: employee_csv projection=[state, salary]\
        \n            TableScan: employee_csv projection=[state, salary]\
        \n        TableScan: employee_csv projection=[state, salary]\
        \n    TableScan: employee_csv projection=[state, salary]";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn plan_builder_union_different_num_columns_error() -> Result<()> {
        let plan1 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3]))?;
        let plan2 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3, 4]))?;

        let expected = "Error during planning: Union queries must have the same number of columns, (left is 1, right is 2)";
        let err_msg1 = plan1.clone().union(plan2.clone().build()?).unwrap_err();
        let err_msg2 = plan1.union_distinct(plan2.build()?).unwrap_err();

        assert_eq!(err_msg1.strip_backtrace(), expected);
        assert_eq!(err_msg2.strip_backtrace(), expected);

        Ok(())
    }

    #[test]
    fn plan_builder_simple_distinct() -> Result<()> {
        let plan =
            table_scan(Some("employee_csv"), &employee_schema(), Some(vec![0, 3]))?
                .filter(col("state").eq(lit("CO")))?
                .project(vec![col("id")])?
                .distinct()?
                .build()?;

        let expected = "\
        Distinct:\
        \n  Projection: employee_csv.id\
        \n    Filter: employee_csv.state = Utf8(\"CO\")\
        \n      TableScan: employee_csv projection=[id, state]";

        assert_eq!(expected, format!("{plan:?}"));

        Ok(())
    }

    #[test]
    fn exists_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        let subquery = LogicalPlanBuilder::from(foo)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("bar.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(bar)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .build()?;

        let expected = "Filter: EXISTS (<subquery>)\
        \n  Subquery:\
        \n    Filter: foo.a = bar.a\
        \n      Projection: foo.a\
        \n        TableScan: foo\
        \n  Projection: bar.a\
        \n    TableScan: bar";
        assert_eq!(expected, format!("{outer_query:?}"));

        Ok(())
    }

    #[test]
    fn filter_in_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        let subquery = LogicalPlanBuilder::from(foo)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("bar.a")))?
            .build()?;

        // SELECT a FROM bar WHERE a IN (SELECT a FROM foo WHERE a = bar.a)
        let outer_query = LogicalPlanBuilder::from(bar)
            .project(vec![col("a")])?
            .filter(in_subquery(col("a"), Arc::new(subquery)))?
            .build()?;

        let expected = "Filter: bar.a IN (<subquery>)\
        \n  Subquery:\
        \n    Filter: foo.a = bar.a\
        \n      Projection: foo.a\
        \n        TableScan: foo\
        \n  Projection: bar.a\
        \n    TableScan: bar";
        assert_eq!(expected, format!("{outer_query:?}"));

        Ok(())
    }

    #[test]
    fn select_scalar_subquery() -> Result<()> {
        let foo = test_table_scan_with_name("foo")?;
        let bar = test_table_scan_with_name("bar")?;

        let subquery = LogicalPlanBuilder::from(foo)
            .project(vec![col("b")])?
            .filter(col("a").eq(col("bar.a")))?
            .build()?;

        // SELECT (SELECT a FROM foo WHERE a = bar.a) FROM bar
        let outer_query = LogicalPlanBuilder::from(bar)
            .project(vec![scalar_subquery(Arc::new(subquery))])?
            .build()?;

        let expected = "Projection: (<subquery>)\
        \n  Subquery:\
        \n    Filter: foo.a = bar.a\
        \n      Projection: foo.b\
        \n        TableScan: foo\
        \n  TableScan: bar";
        assert_eq!(expected, format!("{outer_query:?}"));

        Ok(())
    }

    #[test]
    fn projection_non_unique_names() -> Result<()> {
        let plan = table_scan(
            Some("employee_csv"),
            &employee_schema(),
            // project id and first_name by column index
            Some(vec![0, 1]),
        )?
        // two columns with the same name => error
        .project(vec![col("id"), col("first_name").alias("id")]);

        match plan {
            Err(DataFusionError::SchemaError(SchemaError::AmbiguousReference {
                field:
                    Column {
                        relation: Some(OwnedTableReference::Bare { table }),
                        name,
                    },
            })) => {
                assert_eq!("employee_csv", table);
                assert_eq!("id", &name);
                Ok(())
            }
            _ => plan_err!("Plan should have returned an DataFusionError::SchemaError"),
        }
    }

    #[test]
    fn aggregate_non_unique_names() -> Result<()> {
        let plan = table_scan(
            Some("employee_csv"),
            &employee_schema(),
            // project state and salary by column index
            Some(vec![3, 4]),
        )?
        // two columns with the same name => error
        .aggregate(vec![col("state")], vec![sum(col("salary")).alias("state")]);

        match plan {
            Err(DataFusionError::SchemaError(SchemaError::AmbiguousReference {
                field:
                    Column {
                        relation: Some(OwnedTableReference::Bare { table }),
                        name,
                    },
            })) => {
                assert_eq!("employee_csv", table);
                assert_eq!("state", &name);
                Ok(())
            }
            _ => plan_err!("Plan should have returned an DataFusionError::SchemaError"),
        }
    }

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    #[test]
    fn stringified_plan() {
        let stringified_plan =
            StringifiedPlan::new(PlanType::InitialLogicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false)); // not in non verbose mode

        let stringified_plan =
            StringifiedPlan::new(PlanType::FinalLogicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(stringified_plan.should_display(false)); // display in non verbose mode too

        let stringified_plan =
            StringifiedPlan::new(PlanType::InitialPhysicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false)); // not in non verbose mode

        let stringified_plan =
            StringifiedPlan::new(PlanType::FinalPhysicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(stringified_plan.should_display(false)); // display in non verbose mode

        let stringified_plan = StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: "random opt pass".into(),
            },
            "...the plan...",
        );
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false));
    }

    fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(name), &schema, None)?.build()
    }

    #[test]
    fn plan_builder_intersect_different_num_columns_error() -> Result<()> {
        let plan1 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3]))?;
        let plan2 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3, 4]))?;

        let expected = "Error during planning: INTERSECT/EXCEPT query must have the same number of columns. \
         Left is 1 and right is 2.";
        let err_msg1 =
            LogicalPlanBuilder::intersect(plan1.build()?, plan2.build()?, true)
                .unwrap_err();

        assert_eq!(err_msg1.strip_backtrace(), expected);

        Ok(())
    }

    #[test]
    fn plan_builder_unnest() -> Result<()> {
        // Unnesting a simple column should return the child plan.
        let plan = nested_table_scan("test_table")?
            .unnest_column("scalar")?
            .build()?;

        let expected = "TableScan: test_table";
        assert_eq!(expected, format!("{plan:?}"));

        // Unnesting the strings list.
        let plan = nested_table_scan("test_table")?
            .unnest_column("strings")?
            .build()?;

        let expected = "\
        Unnest: test_table.strings\
        \n  TableScan: test_table";
        assert_eq!(expected, format!("{plan:?}"));

        // Check unnested field is a scalar
        let field = plan
            .schema()
            .field_with_name(Some(&TableReference::bare("test_table")), "strings")
            .unwrap();
        assert_eq!(&DataType::Utf8, field.data_type());

        // Unnesting multiple fields.
        let plan = nested_table_scan("test_table")?
            .unnest_column("strings")?
            .unnest_column("structs")?
            .build()?;

        let expected = "\
        Unnest: test_table.structs\
        \n  Unnest: test_table.strings\
        \n    TableScan: test_table";
        assert_eq!(expected, format!("{plan:?}"));

        // Check unnested struct list field should be a struct.
        let field = plan
            .schema()
            .field_with_name(Some(&TableReference::bare("test_table")), "structs")
            .unwrap();
        assert!(matches!(field.data_type(), DataType::Struct(_)));

        // Unnesting missing column should fail.
        let plan = nested_table_scan("test_table")?.unnest_column("missing");
        assert!(plan.is_err());

        Ok(())
    }

    fn nested_table_scan(table_name: &str) -> Result<LogicalPlanBuilder> {
        // Create a schema with a scalar field, a list of strings, and a list of structs.
        let struct_field = Field::new_struct(
            "item",
            vec![
                Field::new("a", DataType::UInt32, false),
                Field::new("b", DataType::UInt32, false),
            ],
            false,
        );
        let string_field = Field::new("item", DataType::Utf8, false);
        let schema = Schema::new(vec![
            Field::new("scalar", DataType::UInt32, false),
            Field::new_list("strings", string_field, false),
            Field::new_list("structs", struct_field, false),
        ]);

        table_scan(Some(table_name), &schema, None)
    }

    #[test]
    fn test_union_after_join() -> Result<()> {
        let values = vec![vec![lit(1)]];

        let left = LogicalPlanBuilder::values(values.clone())?
            .alias("left")?
            .build()?;
        let right = LogicalPlanBuilder::values(values)?
            .alias("right")?
            .build()?;

        let join = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

        let _ = LogicalPlanBuilder::from(join.clone())
            .union(join)?
            .build()?;

        Ok(())
    }
}
