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

use crate::datasource::{
    empty::EmptyTable,
    listing::{ListingTable, ListingTableConfig},
    MemTable, TableProvider,
};
use crate::error::{DataFusionError, Result};
use crate::logical_expr::ExprSchemable;
use crate::logical_plan::plan::{
    Aggregate, Analyze, EmptyRelation, Explain, Filter, Join, Projection, Sort,
    SubqueryAlias, TableScan, TableUDFs, ToStringifiedPlan, Union, Window,
};
use crate::optimizer::utils;
use crate::prelude::*;
use crate::scalar::ScalarValue;
use arrow::{
    datatypes::{DataType, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_data_access::object_store::ObjectStore;
use std::convert::TryFrom;
use std::iter;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::{exprlist_to_fields, Expr, JoinConstraint, JoinType, LogicalPlan, PlanType};
use crate::logical_plan::{
    columnize_expr, normalize_col, normalize_cols, provider_as_source,
    rewrite_sort_cols_by_aggs, Column, CrossJoin, DFField, DFSchema, DFSchemaRef, Limit,
    Partitioning, Repartition, Values,
};
use crate::sql::utils::group_window_expr_by_sort_keys;
use datafusion_common::ToDFSchema;

/// Default table name for unnamed table
pub const UNNAMED_TABLE: &str = "?table?";

/// Builder for logical plans
///
/// ```
/// # use datafusion::prelude::*;
/// # use datafusion::logical_plan::LogicalPlanBuilder;
/// # use datafusion::error::Result;
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
/// let plan = LogicalPlanBuilder::scan_empty(
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
    pub fn values(mut values: Vec<Vec<Expr>>) -> Result<Self> {
        if values.is_empty() {
            return Err(DataFusionError::Plan("Values list cannot be empty".into()));
        }
        let n_cols = values[0].len();
        if n_cols == 0 {
            return Err(DataFusionError::Plan(
                "Values list cannot be zero length".into(),
            ));
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
                return Err(DataFusionError::Plan(format!(
                    "Inconsistent data length across values list: got {} values in row {} but expected {}",
                    row.len(),
                    i,
                    n_cols
                )));
            }
            field_types = row
                .iter()
                .enumerate()
                .map(|(j, expr)| {
                    if let Expr::Literal(ScalarValue::Utf8(None)) = expr {
                        nulls.push((i, j));
                        Ok(field_types[j].clone())
                    } else {
                        let data_type = expr.get_type(&empty_schema)?;
                        if let Some(prev_data_type) = &field_types[j] {
                            if prev_data_type != &data_type {
                                let err = format!("Inconsistent data type across values list at row {} column {}", i, j);
                                return Err(DataFusionError::Plan(err));
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
                DFField::new(
                    None,
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

    /// Scan a memory data source
    pub fn scan_memory(
        partitions: Vec<Vec<RecordBatch>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let provider = Arc::new(MemTable::try_new(schema, partitions)?);
        Self::scan(UNNAMED_TABLE, provider, projection)
    }

    /// Scan a CSV data source
    pub async fn scan_csv(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: CsvReadOptions<'_>,
        projection: Option<Vec<usize>>,
        target_partitions: usize,
    ) -> Result<Self> {
        let path = path.into();
        Self::scan_csv_with_name(
            object_store,
            path.clone(),
            options,
            projection,
            path,
            target_partitions,
        )
        .await
    }

    /// Scan a CSV data source and register it with a given table name
    pub async fn scan_csv_with_name(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: CsvReadOptions<'_>,
        projection: Option<Vec<usize>>,
        table_name: impl Into<String>,
        target_partitions: usize,
    ) -> Result<Self> {
        let listing_options = options.to_listing_options(target_partitions);

        let path: String = path.into();

        let resolved_schema = match options.schema {
            Some(s) => Arc::new(s.to_owned()),
            None => {
                listing_options
                    .infer_schema(Arc::clone(&object_store), &path)
                    .await?
            }
        };
        let config = ListingTableConfig::new(object_store, path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;

        Self::scan(table_name, Arc::new(provider), projection)
    }

    /// Scan a Parquet data source
    pub async fn scan_parquet(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: ParquetReadOptions<'_>,
        projection: Option<Vec<usize>>,
        target_partitions: usize,
    ) -> Result<Self> {
        let path = path.into();
        Self::scan_parquet_with_name(
            object_store,
            path.clone(),
            options,
            projection,
            target_partitions,
            path,
        )
        .await
    }

    /// Scan a Parquet data source and register it with a given table name
    pub async fn scan_parquet_with_name(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: ParquetReadOptions<'_>,
        projection: Option<Vec<usize>>,
        target_partitions: usize,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let listing_options = options.to_listing_options(target_partitions);
        let path: String = path.into();

        // with parquet we resolve the schema in all cases
        let resolved_schema = listing_options
            .infer_schema(Arc::clone(&object_store), &path)
            .await?;

        let config = ListingTableConfig::new(object_store, path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let provider = ListingTable::try_new(config)?;
        Self::scan(table_name, Arc::new(provider), projection)
    }

    /// Scan an Avro data source
    pub async fn scan_avro(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: AvroReadOptions<'_>,
        projection: Option<Vec<usize>>,
        target_partitions: usize,
    ) -> Result<Self> {
        let path = path.into();
        Self::scan_avro_with_name(
            object_store,
            path.clone(),
            options,
            projection,
            path,
            target_partitions,
        )
        .await
    }

    /// Scan an Avro data source and register it with a given table name
    pub async fn scan_avro_with_name(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: AvroReadOptions<'_>,
        projection: Option<Vec<usize>>,
        table_name: impl Into<String>,
        target_partitions: usize,
    ) -> Result<Self> {
        let listing_options = options.to_listing_options(target_partitions);

        let path: String = path.into();

        let resolved_schema = match options.schema {
            Some(s) => s,
            None => {
                listing_options
                    .infer_schema(Arc::clone(&object_store), &path)
                    .await?
            }
        };
        let config = ListingTableConfig::new(object_store, path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;

        Self::scan(table_name, Arc::new(provider), projection)
    }

    /// Scan an Json data source
    pub async fn scan_json(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: NdJsonReadOptions<'_>,
        projection: Option<Vec<usize>>,
        target_partitions: usize,
    ) -> Result<Self> {
        let path = path.into();
        Self::scan_json_with_name(
            object_store,
            path.clone(),
            options,
            projection,
            path,
            target_partitions,
        )
        .await
    }

    /// Scan an Json data source and register it with a given table name
    pub async fn scan_json_with_name(
        object_store: Arc<dyn ObjectStore>,
        path: impl Into<String>,
        options: NdJsonReadOptions<'_>,
        projection: Option<Vec<usize>>,
        table_name: impl Into<String>,
        target_partitions: usize,
    ) -> Result<Self> {
        let listing_options = options.to_listing_options(target_partitions);

        let path: String = path.into();

        let resolved_schema = match options.schema {
            Some(s) => s,
            None => {
                listing_options
                    .infer_schema(Arc::clone(&object_store), &path)
                    .await?
            }
        };
        let config = ListingTableConfig::new(object_store, path)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);
        let provider = ListingTable::try_new(config)?;

        Self::scan(table_name, Arc::new(provider), projection)
    }

    /// Scan an empty data source, mainly used in tests
    pub fn scan_empty(
        name: Option<&str>,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let table_schema = Arc::new(table_schema.clone());
        let provider = Arc::new(EmptyTable::new(table_schema));
        Self::scan(name.unwrap_or(UNNAMED_TABLE), provider, projection)
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan(
        table_name: impl Into<String>,
        provider: Arc<dyn TableProvider>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Self::scan_with_filters(table_name, provider, projection, vec![])
    }

    /// Convert a table provider into a builder with a TableScan
    pub fn scan_with_filters(
        table_name: impl Into<String>,
        provider: Arc<dyn TableProvider>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<Self> {
        let table_name = table_name.into();

        if table_name.is_empty() {
            return Err(DataFusionError::Plan(
                "table_name cannot be empty".to_string(),
            ));
        }

        let schema = provider.schema();

        let projected_schema = projection
            .as_ref()
            .map(|p| {
                DFSchema::new_with_metadata(
                    p.iter()
                        .map(|i| {
                            DFField::from_qualified(&table_name, schema.field(*i).clone())
                        })
                        .collect(),
                    schema.metadata().clone(),
                )
            })
            .unwrap_or_else(|| {
                DFSchema::try_from_qualified_schema(&table_name, &schema)
            })?;

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name,
            source: provider_as_source(provider),
            projected_schema: Arc::new(projected_schema),
            projection,
            filters,
            limit: None,
        });
        Ok(Self::from(table_scan))
    }
    /// Wrap a plan in a window
    pub(crate) fn window_plan(
        input: LogicalPlan,
        window_exprs: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let mut plan = input;
        let mut groups = group_window_expr_by_sort_keys(&window_exprs)?;
        // sort by sort_key len descending, so that more deeply sorted plans gets nested further
        // down as children; to further mimic the behavior of PostgreSQL, we want stable sort
        // and a reverse so that tieing sort keys are reversed in order; note that by this rule
        // if there's an empty over, it'll be at the top level
        groups.sort_by(|(key_a, _), (key_b, _)| key_a.len().cmp(&key_b.len()));
        groups.reverse();
        for (_, exprs) in groups {
            let window_exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            // the partition and sort itself is done at physical level, see physical_planner's
            // fn create_initial_plan
            plan = LogicalPlanBuilder::from(plan)
                .window(window_exprs)?
                .build()?;
        }
        Ok(plan)
    }
    /// Apply a projection without alias.
    pub fn project(
        &self,
        expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        self.project_with_alias(expr, None)
    }

    /// Apply a projection with alias
    pub fn project_with_alias(
        &self,
        expr: impl IntoIterator<Item = impl Into<Expr>>,
        alias: Option<String>,
    ) -> Result<Self> {
        Ok(Self::from(project_with_alias(
            self.plan.clone(),
            expr,
            alias,
        )?))
    }

    /// Apply a filter
    pub fn filter(&self, expr: impl Into<Expr>) -> Result<Self> {
        let expr = normalize_col(expr.into(), &self.plan)?;
        Ok(Self::from(LogicalPlan::Filter(Filter {
            predicate: expr,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Limit(Limit {
            n,
            input: Arc::new(self.plan.clone()),
        })))
    }

    /// Apply an alias
    pub fn alias(&self, alias: &str) -> Result<Self> {
        let schema: Schema = self.schema().as_ref().clone().into();
        let schema =
            DFSchemaRef::new(DFSchema::try_from_qualified_schema(alias, &schema)?);
        Ok(Self::from(LogicalPlan::SubqueryAlias(SubqueryAlias {
            input: Arc::new(self.plan.clone()),
            alias: alias.to_string(),
            schema,
        })))
    }

    /// Add missing sort columns to all downstream projection
    fn add_missing_columns(
        &self,
        curr_plan: LogicalPlan,
        missing_cols: &[Column],
    ) -> Result<LogicalPlan> {
        match curr_plan {
            LogicalPlan::Projection(Projection {
                input,
                mut expr,
                schema: _,
                alias,
            }) if missing_cols
                .iter()
                .all(|c| input.schema().field_from_column(c).is_ok()) =>
            {
                let input_schema = input.schema();

                let missing_exprs = missing_cols
                    .iter()
                    .map(|c| normalize_col(Expr::Column(c.clone()), &input))
                    .collect::<Result<Vec<_>>>()?;

                expr.extend(missing_exprs);

                let new_schema = DFSchema::new_with_metadata(
                    exprlist_to_fields(&expr, input_schema)?,
                    input_schema.metadata().clone(),
                )?;

                Ok(LogicalPlan::Projection(Projection {
                    expr,
                    input,
                    schema: DFSchemaRef::new(new_schema),
                    alias,
                }))
            }
            _ => {
                let new_inputs = curr_plan
                    .inputs()
                    .into_iter()
                    .map(|input_plan| {
                        self.add_missing_columns((*input_plan).clone(), missing_cols)
                    })
                    .collect::<Result<Vec<_>>>()?;

                let expr = curr_plan.expressions();
                utils::from_plan(&curr_plan, &expr, &new_inputs)
            }
        }
    }

    /// Apply a sort
    pub fn sort(
        &self,
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
                let mut columns: HashSet<Column> = HashSet::new();
                utils::expr_to_columns(&expr, &mut columns)?;

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
                input: Arc::new(self.plan.clone()),
            })));
        }

        let plan = self.add_missing_columns(self.plan.clone(), &missing_cols)?;
        let sort_plan = LogicalPlan::Sort(Sort {
            expr: normalize_cols(exprs, &plan)?,
            input: Arc::new(plan.clone()),
        });
        // remove pushed down sort columns
        let new_expr = schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect();
        let new_schema = DFSchema::new_with_metadata(
            exprlist_to_fields(&new_expr, schema)?,
            schema.metadata().clone(),
        )?;

        Ok(Self::from(LogicalPlan::Projection(Projection {
            expr: new_expr,
            input: Arc::new(sort_plan),
            schema: DFSchemaRef::new(new_schema),
            alias: None,
        })))
    }

    /// Apply a union
    pub fn union(&self, plan: LogicalPlan) -> Result<Self> {
        Ok(Self::from(union_with_alias(self.plan.clone(), plan, None)?))
    }

    /// Apply deduplication: Only distinct (different) values are returned)
    pub fn distinct(&self) -> Result<Self> {
        let projection_expr = expand_wildcard(self.plan.schema(), &self.plan)?;
        let plan = LogicalPlanBuilder::from(self.plan.clone())
            .aggregate(projection_expr, iter::empty::<Expr>())?
            .build()?;
        Self::from(plan).project(vec![Expr::Wildcard])
    }

    /// Apply a join with on constraint
    pub fn join(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
    ) -> Result<Self> {
        self.join_detailed(right, join_type, join_keys, false)
    }

    fn normalize(
        plan: &LogicalPlan,
        column: impl Into<Column> + Clone,
    ) -> Result<Column> {
        let schemas = plan.all_schemas();
        let using_columns = plan.using_columns()?;
        column
            .into()
            .normalize_with_schemas(&schemas, &using_columns)
    }

    /// Apply a join with on constraint and specified null equality
    /// If null_equals_null is true then null == null, else null != null
    pub fn join_detailed(
        &self,
        right: &LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<impl Into<Column>>, Vec<impl Into<Column>>),
        null_equals_null: bool,
    ) -> Result<Self> {
        if join_keys.0.len() != join_keys.1.len() {
            return Err(DataFusionError::Plan(
                "left_keys and right_keys were not the same length".to_string(),
            ));
        }

        let (left_keys, right_keys): (Vec<Result<Column>>, Vec<Result<Column>>) =
            join_keys
                .0
                .into_iter()
                .zip(join_keys.1.into_iter())
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
                                    Self::normalize(right, r),
                                ),
                            }
                        }
                        (Some(lr), None) => {
                            let l_is_left =
                                self.plan.schema().field_with_qualified_name(lr, &l.name);
                            let l_is_right =
                                right.schema().field_with_qualified_name(lr, &l.name);

                            match (l_is_left, l_is_right) {
                                (Ok(_), _) => (Ok(l), Self::normalize(right, r)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, r), Ok(l)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(right, r),
                                ),
                            }
                        }
                        (None, Some(rr)) => {
                            let r_is_left =
                                self.plan.schema().field_with_qualified_name(rr, &r.name);
                            let r_is_right =
                                right.schema().field_with_qualified_name(rr, &r.name);

                            match (r_is_left, r_is_right) {
                                (Ok(_), _) => (Ok(r), Self::normalize(right, l)),
                                (_, Ok(_)) => (Self::normalize(&self.plan, l), Ok(r)),
                                _ => (
                                    Self::normalize(&self.plan, l),
                                    Self::normalize(right, r),
                                ),
                            }
                        }
                        (None, None) => {
                            let mut swap = false;
                            let left_key = Self::normalize(&self.plan, l.clone())
                                .or_else(|_| {
                                    swap = true;
                                    Self::normalize(right, l)
                                });
                            if swap {
                                (Self::normalize(&self.plan, r), left_key)
                            } else {
                                (left_key, Self::normalize(right, r))
                            }
                        }
                    }
                })
                .unzip();

        let left_keys = left_keys.into_iter().collect::<Result<Vec<Column>>>()?;
        let right_keys = right_keys.into_iter().collect::<Result<Vec<Column>>>()?;

        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;

        Ok(Self::from(LogicalPlan::Join(Join {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: DFSchemaRef::new(join_schema),
            null_equals_null,
        })))
    }

    /// Apply a join with using constraint, which duplicates all join columns in output schema.
    pub fn join_using(
        &self,
        right: &LogicalPlan,
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
            .map(|c| Self::normalize(right, c))
            .collect::<Result<_>>()?;

        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();
        let join_schema =
            build_join_schema(self.plan.schema(), right.schema(), &join_type)?;

        Ok(Self::from(LogicalPlan::Join(Join {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            on,
            join_type,
            join_constraint: JoinConstraint::Using,
            schema: DFSchemaRef::new(join_schema),
            null_equals_null: false,
        })))
    }

    /// Apply a cross join
    pub fn cross_join(&self, right: &LogicalPlan) -> Result<Self> {
        let schema = self.plan.schema().join(right.schema())?;
        Ok(Self::from(LogicalPlan::CrossJoin(CrossJoin {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            schema: DFSchemaRef::new(schema),
        })))
    }

    /// Repartition
    pub fn repartition(&self, partitioning_scheme: Partitioning) -> Result<Self> {
        Ok(Self::from(LogicalPlan::Repartition(Repartition {
            input: Arc::new(self.plan.clone()),
            partitioning_scheme,
        })))
    }

    /// Apply a window functions to extend the schema
    pub fn window(
        &self,
        window_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let window_expr = normalize_cols(window_expr, &self.plan)?;
        let all_expr = window_expr.iter();
        validate_unique_names("Windows", all_expr.clone(), self.plan.schema())?;
        let mut window_fields: Vec<DFField> =
            exprlist_to_fields(all_expr, self.plan.schema())?;
        window_fields.extend_from_slice(self.plan.schema().fields());
        Ok(Self::from(LogicalPlan::Window(Window {
            input: Arc::new(self.plan.clone()),
            window_expr,
            schema: Arc::new(DFSchema::new_with_metadata(
                window_fields,
                self.plan.schema().metadata().clone(),
            )?),
        })))
    }

    /// Apply an aggregate: grouping on the `group_expr` expressions
    /// and calculating `aggr_expr` aggregates for each distinct
    /// value of the `group_expr`;
    pub fn aggregate(
        &self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;
        let all_expr = group_expr.iter().chain(aggr_expr.iter());
        validate_unique_names("Aggregations", all_expr.clone(), self.plan.schema())?;
        let aggr_schema = DFSchema::new_with_metadata(
            exprlist_to_fields(all_expr, self.plan.schema())?,
            self.plan.schema().metadata().clone(),
        )?;
        Ok(Self::from(LogicalPlan::Aggregate(Aggregate {
            input: Arc::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: DFSchemaRef::new(aggr_schema),
        })))
    }

    /// Create an expression to represent the explanation of the plan
    ///
    /// if `analyze` is true, runs the actual plan and produces
    /// information about metrics during run.
    ///
    /// if `verbose` is true, prints out additional details.
    pub fn explain(&self, verbose: bool, analyze: bool) -> Result<Self> {
        let schema = LogicalPlan::explain_schema();
        let schema = schema.to_dfschema_ref()?;

        if analyze {
            Ok(Self::from(LogicalPlan::Analyze(Analyze {
                verbose,
                input: Arc::new(self.plan.clone()),
                schema,
            })))
        } else {
            let stringified_plans =
                vec![self.plan.to_stringified(PlanType::InitialLogicalPlan)];

            Ok(Self::from(LogicalPlan::Explain(Explain {
                verbose,
                plan: Arc::new(self.plan.clone()),
                stringified_plans,
                schema,
            })))
        }
    }

    /// Process intersect set operator
    pub(crate) fn intersect(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::intersect_or_except(
            left_plan,
            right_plan,
            JoinType::Semi,
            is_all,
        )
    }

    /// Process except set operator
    pub(crate) fn except(
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
        is_all: bool,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::intersect_or_except(
            left_plan,
            right_plan,
            JoinType::Anti,
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
                .join_detailed(&right_plan, join_type, join_keys, true)?
                .build()
        } else {
            LogicalPlanBuilder::from(left_plan)
                .distinct()?
                .join_detailed(&right_plan, join_type, join_keys, true)?
                .build()
        }
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &DFSchema,
    right: &DFSchema,
    join_type: &JoinType,
) -> Result<DFSchema> {
    let fields: Vec<DFField> = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            let right_fields = right.fields().iter();
            let left_fields = left.fields().iter();
            // left then right
            left_fields.chain(right_fields).cloned().collect()
        }
        JoinType::Semi | JoinType::Anti => {
            // Only use the left side for the schema
            left.fields().clone()
        }
    };

    let mut metadata = left.metadata().clone();
    metadata.extend(right.metadata().clone());
    DFSchema::new_with_metadata(fields, metadata)
}

/// Errors if one or more expressions have equal names.
fn validate_unique_names<'a>(
    node_name: &str,
    expressions: impl IntoIterator<Item = &'a Expr>,
    input_schema: &DFSchema,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.into_iter().enumerate().try_for_each(|(position, expr)| {
        let name = expr.name(input_schema)?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                Err(DataFusionError::Plan(
                    format!("{} require unique expression names \
                             but the expression \"{:?}\" at position {} and \"{:?}\" \
                             at position {} have the same name. Consider aliasing (\"AS\") one of them.",
                             node_name, existing_expr, existing_position, expr, position,
                            )
                ))
            }
        }
    })
}

pub fn project_with_column_index_alias(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
    alias: Option<String>,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            ignore_alias @ Expr::Alias { .. } => ignore_alias,
            ignore_col @ Expr::Column { .. } => ignore_col,
            x => x.alias(format!("column{}", i).as_str()),
        })
        .collect::<Vec<_>>();
    Ok(LogicalPlan::Projection(Projection {
        expr: alias_expr,
        input,
        schema,
        alias,
    }))
}

/// Union two logical plans with an optional alias.
pub fn union_with_alias(
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
    alias: Option<String>,
) -> Result<LogicalPlan> {
    let inputs = vec![left_plan, right_plan]
        .into_iter()
        .flat_map(|p| match p {
            LogicalPlan::Union(Union { inputs, .. }) => inputs,
            x => vec![x],
        })
        .map(|p| match p {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                alias,
            }) => project_with_column_index_alias(expr, input, schema, alias).unwrap(),
            x => x,
        })
        .collect::<Vec<_>>();
    if inputs.is_empty() {
        return Err(DataFusionError::Plan("Empty UNION".to_string()));
    }

    let union_schema = (**inputs[0].schema()).clone();
    let union_schema = Arc::new(match alias {
        Some(ref alias) => union_schema.replace_qualifier(alias.as_str()),
        None => union_schema.strip_qualifiers(),
    });

    inputs
        .iter()
        .skip(1)
        .try_for_each(|input_plan| -> Result<()> {
            union_schema.check_arrow_schema_type_compatible(
                &((**input_plan.schema()).clone().into()),
            )
        })?;

    Ok(LogicalPlan::Union(Union {
        inputs,
        schema: union_schema,
        alias,
    }))
}

/// Project with optional alias
/// # Errors
/// This function errors under any of the following conditions:
/// * Two or more expressions have the same name
/// * An invalid expression is used (e.g. a `sort` expression)
pub fn project_with_alias(
    plan: LogicalPlan,
    expr: impl IntoIterator<Item = impl Into<Expr>>,
    alias: Option<String>,
) -> Result<LogicalPlan> {
    let input_schema = plan.schema();
    let mut projected_expr = vec![];
    for e in expr {
        let e = e.into();
        match e {
            Expr::Wildcard => {
                projected_expr.extend(expand_wildcard(input_schema, &plan)?)
            }
            Expr::QualifiedWildcard { ref qualifier } => projected_expr
                .extend(expand_qualified_wildcard(qualifier, input_schema, &plan)?),
            _ => projected_expr
                .push(columnize_expr(normalize_col(e, &plan)?, input_schema)),
        }
    }
    validate_unique_names("Projections", projected_expr.iter(), input_schema)?;
    let input_schema = DFSchema::new_with_metadata(
        exprlist_to_fields(&projected_expr, input_schema)?,
        plan.schema().metadata().clone(),
    )?;
    let schema = match alias {
        Some(ref alias) => input_schema.replace_qualifier(alias.as_str()),
        None => input_schema,
    };
    Ok(LogicalPlan::Projection(Projection {
        expr: projected_expr,
        input: Arc::new(plan.clone()),
        schema: DFSchemaRef::new(schema),
        alias,
    }))
}

/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub(crate) fn expand_wildcard(
    schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let using_columns = plan.using_columns()?;
    let columns_to_skip = using_columns
        .into_iter()
        // For each USING JOIN condition, only expand to one column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            cols.into_iter().skip(1)
        })
        .collect::<HashSet<_>>();

    if columns_to_skip.is_empty() {
        Ok(schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect::<Vec<Expr>>())
    } else {
        Ok(schema
            .fields()
            .iter()
            .filter_map(|f| {
                let col = f.qualified_column();
                if !columns_to_skip.contains(&col) {
                    Some(Expr::Column(col))
                } else {
                    None
                }
            })
            .collect::<Vec<Expr>>())
    }
}

pub(crate) fn expand_qualified_wildcard(
    qualifier: &str,
    schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<Vec<Expr>> {
    let qualified_fields: Vec<DFField> = schema
        .fields_with_qualified(qualifier)
        .into_iter()
        .cloned()
        .collect();
    if qualified_fields.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid qualifier {}",
            qualifier
        )));
    }
    let qualifier_schema =
        DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?;
    expand_wildcard(&qualifier_schema, plan)
}

/// Build merged table udf schema from input and TableUDF experssions
pub fn build_table_udf_schema(
    input: &LogicalPlan,
    udtf_expr: &[Expr],
) -> Result<DFSchemaRef> {
    let input_schema = input.schema();
    let mut schema = (**input_schema).clone();
    schema.merge(&DFSchema::new_with_metadata(
        exprlist_to_fields(udtf_expr, input_schema)?,
        HashMap::new(),
    )?);
    Ok(Arc::new(schema))
}

pub(crate) fn table_udfs(plan: LogicalPlan, udtf_expr: Vec<Expr>) -> Result<LogicalPlan> {
    let schema = build_table_udf_schema(&plan, &udtf_expr)?;
    Ok(LogicalPlan::TableUDFs(TableUDFs {
        expr: udtf_expr,
        input: Arc::new(plan),
        schema,
    }))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::expr_fn::exists;

    use crate::logical_plan::StringifiedPlan;
    use crate::test::test_table_scan_with_name;

    use super::super::{col, lit, sum};
    use super::*;

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        .filter(col("state").eq(lit("CO")))?
        .project(vec![col("id")])?
        .build()?;

        let expected = "Projection: #employee_csv.id\
        \n  Filter: #employee_csv.state = Utf8(\"CO\")\
        \n    TableScan: employee_csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_schema() {
        let schema = employee_schema();
        let plan =
            LogicalPlanBuilder::scan_empty(Some("employee_csv"), &schema, None).unwrap();

        let expected =
            DFSchema::try_from_qualified_schema("employee_csv", &schema).unwrap();

        assert_eq!(&expected, plan.schema().as_ref())
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            Some(vec![3, 4]),
        )?
        .aggregate(
            vec![col("state")],
            vec![sum(col("salary")).alias("total_salary")],
        )?
        .project(vec![col("state"), col("total_salary")])?
        .build()?;

        let expected = "Projection: #employee_csv.state, #total_salary\
        \n  Aggregate: groupBy=[[#employee_csv.state]], aggr=[[SUM(#employee_csv.salary) AS total_salary]]\
        \n    TableScan: employee_csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            Some(vec![3, 4]),
        )?
        .sort(vec![
            Expr::Sort {
                expr: Box::new(col("state")),
                asc: true,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("salary")),
                asc: false,
                nulls_first: false,
            },
        ])?
        .build()?;

        let expected = "Sort: #employee_csv.state ASC NULLS FIRST, #employee_csv.salary DESC NULLS LAST\
        \n  TableScan: employee_csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_using_join_wildcard_projection() -> Result<()> {
        let t2 = LogicalPlanBuilder::scan_empty(Some("t2"), &employee_schema(), None)?
            .build()?;

        let plan = LogicalPlanBuilder::scan_empty(Some("t1"), &employee_schema(), None)?
            .join_using(&t2, JoinType::Inner, vec!["id"])?
            .project(vec![Expr::Wildcard])?
            .build()?;

        // id column should only show up once in projection
        let expected = "Projection: #t1.id, #t1.first_name, #t1.last_name, #t1.state, #t1.salary, #t2.first_name, #t2.last_name, #t2.state, #t2.salary\
        \n  Inner Join: Using #t1.id = #t2.id\
        \n    TableScan: t1 projection=None\
        \n    TableScan: t2 projection=None";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_union_combined_single_union() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            Some(vec![3, 4]),
        )?;

        let plan = plan
            .union(plan.build()?)?
            .union(plan.build()?)?
            .union(plan.build()?)?
            .build()?;

        // output has only one union
        let expected = "Union\
        \n  TableScan: employee_csv projection=Some([3, 4])\
        \n  TableScan: employee_csv projection=Some([3, 4])\
        \n  TableScan: employee_csv projection=Some([3, 4])\
        \n  TableScan: employee_csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

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

        let expected = "Filter: EXISTS (\
            Subquery: Filter: #foo.a = #bar.a\
            \n  Projection: #foo.a\
            \n    TableScan: foo projection=None)\
        \n  Projection: #bar.a\n    TableScan: bar projection=None";
        assert_eq!(expected, format!("{:?}", outer_query));

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

        let expected = "Filter: #bar.a IN (Subquery: Filter: #foo.a = #bar.a\
        \n  Projection: #foo.a\
        \n    TableScan: foo projection=None)\
        \n  Projection: #bar.a\
        \n    TableScan: bar projection=None";
        assert_eq!(expected, format!("{:?}", outer_query));

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

        let expected = "Projection: (Subquery: Filter: #foo.a = #bar.a\
                \n  Projection: #foo.b\
                \n    TableScan: foo projection=None)\
            \n  TableScan: bar projection=None";
        assert_eq!(expected, format!("{:?}", outer_query));

        Ok(())
    }

    #[test]
    fn projection_non_unique_names() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            // project id and first_name by column index
            Some(vec![0, 1]),
        )?
        // two columns with the same name => error
        .project(vec![col("id"), col("first_name").alias("id")]);

        match plan {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!(
                    e,
                    "Schema contains qualified field name 'employee_csv.id' \
                    and unqualified field name 'id' which would be ambiguous"
                );
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::Plan".to_string(),
            )),
        }
    }

    #[test]
    fn aggregate_non_unique_names() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_empty(
            Some("employee_csv"),
            &employee_schema(),
            // project state and salary by column index
            Some(vec![3, 4]),
        )?
        // two columns with the same name => error
        .aggregate(vec![col("state")], vec![sum(col("salary")).alias("state")]);

        match plan {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!(
                    e,
                    "Schema contains qualified field name 'employee_csv.state' and \
                    unqualified field name 'state' which would be ambiguous"
                );
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::Plan".to_string(),
            )),
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
}
