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

//! Collection of testing utility functions that are leveraged by the query optimizer rules

use std::sync::Arc;

use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
use crate::error::Result;
use crate::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::filter::FilterExec;
use crate::physical_plan::joins::utils::{ColumnIndex, JoinFilter, JoinOn};
use crate::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::physical_plan::memory::MemoryExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use crate::physical_plan::streaming::StreamingTableExec;
use crate::physical_plan::union::UnionExec;
use crate::physical_plan::windows::create_window_expr;
use crate::physical_plan::{displayable, ExecutionPlan, Partitioning};
use crate::prelude::{CsvReadOptions, SessionContext};

use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion_common::{JoinType, ScalarValue, Statistics};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{AggregateFunction, Operator, WindowFrame, WindowFunction};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::intervals::test_utils::gen_conjunctive_numerical_expr;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};

use async_trait::async_trait;

async fn register_current_csv(
    ctx: &SessionContext,
    table_name: &str,
    infinite: bool,
) -> Result<()> {
    let testdata = crate::test_util::arrow_test_data();
    let schema = crate::test_util::aggr_test_schema();
    ctx.register_csv(
        table_name,
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new()
            .schema(&schema)
            .mark_infinite(infinite),
    )
    .await?;
    Ok(())
}

#[derive(Eq, PartialEq, Debug)]
pub enum SourceType {
    Unbounded,
    Bounded,
}

#[async_trait]
pub trait SqlTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()>;
    fn expect_fail(&self) -> bool;
}

/// [UnaryTestCase] is designed for single input [ExecutionPlan]s.
pub struct UnaryTestCase {
    pub(crate) source_type: SourceType,
    pub(crate) expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for UnaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let table_is_infinite = self.source_type == SourceType::Unbounded;
        register_current_csv(ctx, "test", table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}
/// [BinaryTestCase] is designed for binary input [ExecutionPlan]s.
pub struct BinaryTestCase {
    pub(crate) source_types: (SourceType, SourceType),
    pub(crate) expect_fail: bool,
}

#[async_trait]
impl SqlTestCase for BinaryTestCase {
    async fn register_table(&self, ctx: &SessionContext) -> Result<()> {
        let left_table_is_infinite = self.source_types.0 == SourceType::Unbounded;
        let right_table_is_infinite = self.source_types.1 == SourceType::Unbounded;
        register_current_csv(ctx, "left", left_table_is_infinite).await?;
        register_current_csv(ctx, "right", right_table_is_infinite).await?;
        Ok(())
    }

    fn expect_fail(&self) -> bool {
        self.expect_fail
    }
}

pub struct QueryCase {
    pub(crate) sql: String,
    pub(crate) cases: Vec<Arc<dyn SqlTestCase>>,
    pub(crate) error_operator: String,
}

impl QueryCase {
    /// Run the test cases
    pub(crate) async fn run(&self) -> Result<()> {
        for case in &self.cases {
            let ctx = SessionContext::new();
            case.register_table(&ctx).await?;
            let error = if case.expect_fail() {
                Some(&self.error_operator)
            } else {
                None
            };
            self.run_case(ctx, error).await?;
        }
        Ok(())
    }
    async fn run_case(&self, ctx: SessionContext, error: Option<&String>) -> Result<()> {
        let dataframe = ctx.sql(self.sql.as_str()).await?;
        let plan = dataframe.create_physical_plan().await;
        if error.is_some() {
            let plan_error = plan.unwrap_err();
            let initial = error.unwrap().to_string();
            assert!(
                plan_error.to_string().contains(initial.as_str()),
                "plan_error: {:?} doesn't contain message: {:?}",
                plan_error,
                initial.as_str()
            );
        } else {
            assert!(plan.is_ok())
        }
        Ok(())
    }
}

pub fn sort_merge_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_on: &JoinOn,
    join_type: &JoinType,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        SortMergeJoinExec::try_new(
            left,
            right,
            join_on.clone(),
            *join_type,
            vec![SortOptions::default(); join_on.len()],
            false,
        )
        .unwrap(),
    )
}

/// make PhysicalSortExpr with default options
pub fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
    sort_expr_options(name, schema, SortOptions::default())
}

/// PhysicalSortExpr with specified options
pub fn sort_expr_options(
    name: &str,
    schema: &Schema,
    options: SortOptions,
) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: col(name, schema).unwrap(),
        options,
    }
}

pub fn coalesce_partitions_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalescePartitionsExec::new(input))
}

pub(crate) fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(MemoryExec::try_new(&[vec![]], schema.clone(), None).unwrap())
}

pub fn hash_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: &JoinType,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        filter,
        join_type,
        PartitionMode::Partitioned,
        true,
    )?))
}

pub fn bounded_window_exec(
    col_name: &str,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs: Vec<_> = sort_exprs.into_iter().collect();
    let schema = input.schema();

    Arc::new(
        crate::physical_plan::windows::BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunction::AggregateFunction(AggregateFunction::Count),
                "count".to_owned(),
                &[col(col_name, &schema).unwrap()],
                &[],
                &sort_exprs,
                Arc::new(WindowFrame::new(true)),
                schema.as_ref(),
            )
            .unwrap()],
            input.clone(),
            input.schema(),
            vec![],
            crate::physical_plan::windows::PartitionSearchMode::Sorted,
        )
        .unwrap(),
    )
}

pub fn filter_exec(
    predicate: Arc<dyn PhysicalExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

// Util function to get string representation of a physical plan
pub fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let formatted = displayable(plan.as_ref()).indent(true).to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    actual.iter().map(|elem| elem.to_string()).collect()
}

pub fn sort_preserving_merge_exec(
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
}

/// Create a non sorted parquet exec
pub fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
    Arc::new(ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema: schema.clone(),
            file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
            infinite_source: false,
        },
        None,
        None,
    ))
}

// Created a sorted parquet exec
pub fn parquet_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema: schema.clone(),
            file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![sort_exprs],
            infinite_source: false,
        },
        None,
        None,
    ))
}

pub fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(UnionExec::new(input))
}

pub fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    global_limit_exec(local_limit_exec(input))
}

pub fn local_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(LocalLimitExec::new(input, 100))
}

pub fn global_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(input, 0, Some(100)))
}

pub fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap())
}

pub fn aggregate_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![],
            vec![],
            vec![],
            input,
            schema,
        )
        .unwrap(),
    )
}

pub fn coalesce_batches_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalesceBatchesExec::new(input, 128))
}

pub fn sort_exec(
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new(SortExec::new(sort_exprs, input))
}

pub fn prunable_filter(left_index: ColumnIndex, right_index: ColumnIndex) -> JoinFilter {
    // Filter columns, ensure first batches will have matching rows.
    let intermediate_schema = Schema::new(vec![
        Field::new("0", DataType::Int32, true),
        Field::new("1", DataType::Int32, true),
    ]);
    let column_indices = vec![left_index, right_index];
    let filter_expr = gen_conjunctive_numerical_expr(
        col("0", &intermediate_schema).unwrap(),
        col("1", &intermediate_schema).unwrap(),
        (
            Operator::Plus,
            Operator::Minus,
            Operator::Plus,
            Operator::Plus,
        ),
        ScalarValue::Int32(Some(0)),
        ScalarValue::Int32(Some(3)),
        ScalarValue::Int32(Some(0)),
        ScalarValue::Int32(Some(3)),
        (Operator::Gt, Operator::Lt),
    );
    JoinFilter::new(filter_expr, column_indices, intermediate_schema)
}

pub fn memory_exec_with_sort(
    schema: &SchemaRef,
    sort: Option<Vec<PhysicalSortExpr>>,
) -> Arc<dyn ExecutionPlan> {
    let mem = MemoryExec::try_new(&[], schema.clone(), None).unwrap();
    Arc::new(if let Some(sort) = sort {
        mem.with_sort_information(vec![sort])
    } else {
        mem
    })
}

pub fn streaming_table_exec(
    schema: &SchemaRef,
    sort: Option<Vec<PhysicalSortExpr>>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        StreamingTableExec::try_new(schema.clone(), vec![], None, sort, true).unwrap(),
    )
}

#[macro_export]
macro_rules! assert_optimized_orthogonal {
    ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr) => {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let physical_plan = $PLAN;
        let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();

        let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
            .iter().map(|s| *s).collect();

        assert_eq!(
            expected_plan_lines, actual,
            "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES
            .iter().map(|s| *s).collect();
        //
        // Run JoinSelection - EnforceSorting
        let optimized_physical_plan = JoinSelection::new().optimize(physical_plan.clone(), state.config_options())?;
        let optimized_physical_plan =
            EnforceSorting::new().optimize(optimized_physical_plan, state.config_options())?;

        assert_eq!(physical_plan.schema(), optimized_physical_plan.schema());

        // Get string representation of the plan
        let actual = get_plan_string(&optimized_physical_plan);
        assert_eq!(
            expected_optimized_lines, actual,
            "\n**JoinSelection - EnforceSorting Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        // TODO: Apply EnforceSorting first after the https://github.com/synnada-ai/arrow-datafusion/pull/165
        //  is merged.
        // // Run EnforceSorting - JoinSelection
        // let optimized_physical_plan_2 =
        //     EnforceSorting::new().optimize(physical_plan.clone(), state.config_options())?;
        // let optimized_physical_plan_2 = JoinSelection::new().optimize(optimized_physical_plan_2.clone(), state.config_options())?;
        //
        // assert_eq!(physical_plan.schema(), optimized_physical_plan_2.schema());
        //
        // // Get string representation of the plan
        // let actual = get_plan_string(&optimized_physical_plan_2);
        // assert_eq!(
        //     expected_optimized_lines, actual,
        //     "\n**EnforceSorting - JoinSelection Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        // );

    };
}
