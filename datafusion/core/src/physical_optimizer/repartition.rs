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

//! Tests to check whether parallelism can be increased as desired

#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use std::sync::Arc;

    use crate::datasource::file_format::file_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::{CsvExec, FileScanConfig, ParquetExec};
    use crate::error::Result;
    use crate::physical_optimizer::dist_enforcement::EnforceDistribution;
    use crate::physical_optimizer::sort_enforcement::EnforceSorting;
    use crate::physical_optimizer::PhysicalOptimizerRule;
    use crate::physical_plan::aggregates::{
        AggregateExec, AggregateMode, PhysicalGroupBy,
    };
    use crate::physical_plan::expressions::{col, PhysicalSortExpr};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::ExecutionPlan;
    use crate::physical_plan::{displayable, DisplayAs, DisplayFormatType, Statistics};
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_expr::PhysicalSortRequirement;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c1", DataType::Boolean, true)]))
    }

    /// Generate FileScanConfig for file scan executors like 'ParquetExec'
    fn scan_config(sorted: bool, single_file: bool) -> FileScanConfig {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];

        let file_groups = if single_file {
            vec![vec![PartitionedFile::new("x".to_string(), 100)]]
        } else {
            vec![
                vec![PartitionedFile::new("x".to_string(), 100)],
                vec![PartitionedFile::new("y".to_string(), 200)],
            ]
        };

        FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema: schema(),
            file_groups,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: if sorted { vec![sort_exprs] } else { vec![] },
            infinite_source: false,
        }
    }

    /// Create a non sorted parquet exec
    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(false, true), None, None))
    }

    /// Create a non sorted CSV exec
    fn csv_exec() -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            scan_config(false, true),
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    /// Create a non sorted parquet exec over two files / partitions
    fn parquet_exec_two_partitions() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(false, false), None, None))
    }

    /// Create a non sorted csv exec over two files / partitions
    fn csv_exec_two_partitions() -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            scan_config(false, false),
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    // Created a sorted parquet exec
    fn parquet_exec_sorted() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(true, true), None, None))
    }

    // Created a sorted csv exec
    fn csv_exec_sorted() -> Arc<CsvExec> {
        Arc::new(CsvExec::new(
            scan_config(true, true),
            false,
            b',',
            b'"',
            None,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    // Created a sorted parquet exec with multiple files
    fn parquet_exec_multiple_sorted() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(scan_config(true, false), None, None))
    }

    fn sort_preserving_merge_exec(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let expr = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: arrow::compute::SortOptions::default(),
        }];

        Arc::new(SortPreservingMergeExec::new(expr, input))
    }

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(col("c1", &schema()).unwrap(), input).unwrap())
    }

    fn sort_exec(
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];
        let new_sort = SortExec::new(sort_exprs, input)
            .with_preserve_partitioning(preserve_partitioning);
        Arc::new(new_sort)
    }

    fn projection_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let exprs = vec![(col("c1", &schema()).unwrap(), "c1".to_string())];
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::Final,
                PhysicalGroupBy::default(),
                vec![],
                vec![],
                vec![],
                Arc::new(
                    AggregateExec::try_new(
                        AggregateMode::Partial,
                        PhysicalGroupBy::default(),
                        vec![],
                        vec![],
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            0,
            Some(100),
        ))
    }

    fn limit_exec_with_skip(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            5,
            Some(100),
        ))
    }

    fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionExec::new(input))
    }

    fn sort_required_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortRequiredExec::new(input))
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr, $FIRST_ENFORCE_DIST: expr) => {
            assert_optimized!($EXPECTED_LINES, $PLAN, $FIRST_ENFORCE_DIST, 10, false, 1024);
        };

        ($EXPECTED_LINES: expr, $PLAN: expr, $FIRST_ENFORCE_DIST: expr, $TARGET_PARTITIONS: expr, $REPARTITION_FILE_SCANS: expr, $REPARTITION_FILE_MIN_SIZE: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            let mut config = ConfigOptions::new();
            config.execution.target_partitions = $TARGET_PARTITIONS;
            config.optimizer.repartition_file_scans = $REPARTITION_FILE_SCANS;
            config.optimizer.repartition_file_min_size = $REPARTITION_FILE_MIN_SIZE;

            let optimized = if $FIRST_ENFORCE_DIST{
                // run optimizer
                let optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
                    // EnforceDistribution is an essential rule to be applied.
                    // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                    Arc::new(EnforceDistribution::new()),
                    // re-run same rule. Rule should be idempotent
                    Arc::new(EnforceDistribution::new()),

                    // EnforceSorting is an essential rule to be applied.
                    // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                    Arc::new(EnforceSorting::new()),
                ];
                let optimized = optimizers.into_iter().fold($PLAN, |plan, optimizer| {
                    optimizer.optimize(plan, &config).unwrap()
                });
                optimized
            } else {
                // run optimizer
                let optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
                    // EnforceSorting is an essential rule to be applied.
                    // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                    Arc::new(EnforceSorting::new()),

                    // EnforceDistribution is an essential rule to be applied.
                    // Otherwise, the correctness of the generated optimized plan cannot be guaranteed
                    Arc::new(EnforceDistribution::new()),
                    // re-run same rule. Rule should be idempotent
                    Arc::new(EnforceDistribution::new()),
                ];
                let optimized = optimizers.into_iter().fold($PLAN, |plan, optimizer| {
                    optimizer.optimize(plan, &config).unwrap()
                });
                optimized
            };

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent(true).to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let plan = aggregate(parquet_exec());

        let expected = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let plan = aggregate(filter_exec(parquet_exec()));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit() -> Result<()> {
        let plan = limit_exec(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit_with_skip() -> Result<()> {
        let plan = limit_exec_with_skip(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: skip=5, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit() -> Result<()> {
        let plan = limit_exec(sort_exec(parquet_exec(), false));

        let expected = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit_with_filter() -> Result<()> {
        let plan = sort_required_exec(filter_exec(sort_exec(parquet_exec(), false)));

        let expected = &[
            "SortRequiredExec",
            "FilterExec: c1@0",
            // We can use repartition here, ordering requirement by SortRequiredExec
            // is still satisfied.
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "SortExec: expr=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit() -> Result<()> {
        let plan = aggregate(limit_exec(filter_exec(limit_exec(parquet_exec()))));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit_with_skip() -> Result<()> {
        let plan = aggregate(limit_exec_with_skip(filter_exec(limit_exec(
            parquet_exec(),
        ))));

        let expected = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=5, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    // repartition works differently for limit when there is a sort below it

    #[test]
    fn repartition_ignores_union() -> Result<()> {
        let plan = union_exec(vec![parquet_exec(); 5]);

        let expected = &[
            "UnionExec",
            // Expect no repartition of ParquetExec
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_through_sort_preserving_merge() -> Result<()> {
        // sort preserving merge with non-sorted input
        let plan = sort_preserving_merge_exec(parquet_exec());

        // need repartiton and resort as the data was not sorted correctly
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "SortExec: expr=[c1@0 ASC]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        // need repartiton and resort as the data was not sorted correctly
        let expected_first_sort_enforcement = &[
            "SortExec: expr=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge() -> Result<()> {
        // sort preserving merge already sorted input,
        let plan = sort_preserving_merge_exec(parquet_exec_multiple_sorted());

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge_with_union() -> Result<()> {
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let input = union_exec(vec![parquet_exec_sorted(); 2]);
        let plan = sort_preserving_merge_exec(input);

        // should not repartition / sort (as the data was already sorted)
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort() -> Result<()> {
        //  SortRequired
        //    Parquet(sorted)

        let plan = sort_required_exec(parquet_exec_sorted());

        // should not repartition as doing so destroys the necessary sort order
        let expected = &[
            "SortRequiredExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_does_not_destroy_sort_more_complex() -> Result<()> {
        // model a more complicated scenario where one child of a union can be repartitioned for performance
        // but the other can not be
        //
        // Union
        //  SortRequired
        //    Parquet(sorted)
        //  Filter
        //    Parquet(unsorted)

        let input1 = sort_required_exec(parquet_exec_sorted());
        let input2 = filter_exec(parquet_exec());
        let plan = union_exec(vec![input1, input2]);

        // should not repartition below the SortRequired as that
        // destroys the sort order but should still repartition for
        // FilterExec
        let expected = &[
            "UnionExec",
            // union input 1: no repartitioning
            "SortRequiredExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
            // union input 2: should repartition
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_with_projection() -> Result<()> {
        // non sorted input
        let plan = sort_preserving_merge_exec(projection_exec(parquet_exec()));

        // needs to repartition / sort as the data was not sorted correctly
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "SortExec: expr=[c1@0 ASC]",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortExec: expr=[c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_ignores_transitively_with_projection() -> Result<()> {
        // sorted input
        let plan =
            sort_preserving_merge_exec(projection_exec(parquet_exec_multiple_sorted()));

        // data should not be repartitioned / resorted
        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection() -> Result<()> {
        let plan =
            sort_preserving_merge_exec(sort_exec(projection_exec(parquet_exec()), true));

        let expected = &[
            "SortExec: expr=[c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);
        assert_optimized!(expected, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_filter() -> Result<()> {
        let plan = sort_exec(filter_exec(parquet_exec()), false);

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: expr=[c1@0 ASC]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortExec: expr=[c1@0 ASC]",
            "CoalescePartitionsExec",
            "FilterExec: c1@0",
            // Expect repartition on the input of the filter (as it can benefit from additional parallelism)
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
        let plan = sort_exec(projection_exec(filter_exec(parquet_exec())), false);

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: expr=[c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "FilterExec: c1@0",
            // repartition is lowest down
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];

        assert_optimized!(expected, plan.clone(), true);

        let expected_first_sort_enforcement = &[
            "SortExec: expr=[c1@0 ASC]",
            "CoalescePartitionsExec",
            "ProjectionExec: expr=[c1@0 as c1]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        assert_optimized!(expected_first_sort_enforcement, plan, false);
        Ok(())
    }

    #[test]
    fn parallelization_single_partition() -> Result<()> {
        let plan_parquet = aggregate(parquet_exec());
        let plan_csv = aggregate(csv_exec());

        let expected_parquet = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "ParquetExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[c1]",
        ];
        let expected_csv = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "CsvExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    /// CsvExec on compressed csv file will not be partitioned
    /// (Not able to decompress chunked csv file)
    fn parallelization_compressed_csv() -> Result<()> {
        let compression_types = [
            FileCompressionType::GZIP,
            FileCompressionType::BZIP2,
            FileCompressionType::XZ,
            FileCompressionType::ZSTD,
            FileCompressionType::UNCOMPRESSED,
        ];

        let expected_not_partitioned = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        let expected_partitioned = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "CsvExec: file_groups={2 groups: [[x:0..50], [x:50..100]]}, projection=[c1], has_header=false",
        ];

        for compression_type in compression_types {
            let expected = if compression_type.is_compressed() {
                &expected_not_partitioned[..]
            } else {
                &expected_partitioned[..]
            };

            let plan = aggregate(Arc::new(CsvExec::new(
                scan_config(false, true),
                false,
                b',',
                b'"',
                None,
                compression_type,
            )));

            assert_optimized!(expected, plan, true, 2, true, 10);
        }
        Ok(())
    }

    #[test]
    fn parallelization_two_partitions() -> Result<()> {
        let plan_parquet = aggregate(parquet_exec_two_partitions());
        let plan_csv = aggregate(csv_exec_two_partitions());

        let expected_parquet = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Plan already has two partitions
            "ParquetExec: file_groups={2 groups: [[x], [y]]}, projection=[c1]",
        ];
        let expected_csv = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Plan already has two partitions
            "CsvExec: file_groups={2 groups: [[x], [y]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_two_partitions_into_four() -> Result<()> {
        let plan_parquet = aggregate(parquet_exec_two_partitions());
        let plan_csv = aggregate(csv_exec_two_partitions());

        let expected_parquet = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Multiple source files splitted across partitions
            "ParquetExec: file_groups={4 groups: [[x:0..75], [x:75..100, y:0..50], [y:50..125], [y:125..200]]}, projection=[c1]",
        ];
        let expected_csv = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            // Multiple source files splitted across partitions
            "CsvExec: file_groups={4 groups: [[x:0..75], [x:75..100, y:0..50], [y:50..125], [y:125..200]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 4, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 4, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_sorted_limit() -> Result<()> {
        let plan_parquet = limit_exec(sort_exec(parquet_exec(), false));
        let plan_csv = limit_exec(sort_exec(csv_exec(), false));

        let expected_parquet = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c1@0 ASC]",
            // Doesn't parallelize for SortExec without preserve_partitioning
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "LocalLimitExec: fetch=100",
            // data is sorted so can't repartition here
            "SortExec: expr=[c1@0 ASC]",
            // Doesn't parallelize for SortExec without preserve_partitioning
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_limit_with_filter() -> Result<()> {
        let plan_parquet = limit_exec(filter_exec(sort_exec(parquet_exec(), false)));
        let plan_csv = limit_exec(filter_exec(sort_exec(csv_exec(), false)));

        let expected_parquet = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // even though data is sorted, we can use repartition here. Since
            // ordering is not used in subsequent stages anyway.
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "SortExec: expr=[c1@0 ASC]",
            // SortExec doesn't benefit from input partitioning
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // even though data is sorted, we can use repartition here. Since
            // ordering is not used in subsequent stages anyway.
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "SortExec: expr=[c1@0 ASC]",
            // SortExec doesn't benefit from input partitioning
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_limit() -> Result<()> {
        let plan_parquet = aggregate(limit_exec(filter_exec(limit_exec(parquet_exec()))));
        let plan_csv = aggregate(limit_exec(filter_exec(limit_exec(csv_exec()))));

        let expected_parquet = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            // Limit doesn't benefit from input partitionins - no parallelism
            "LocalLimitExec: fetch=100",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "CoalescePartitionsExec",
            "AggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            "CoalescePartitionsExec",
            "LocalLimitExec: fetch=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1",
            "GlobalLimitExec: skip=0, fetch=100",
            // Limit doesn't benefit from input partitionins - no parallelism
            "LocalLimitExec: fetch=100",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_union_inputs() -> Result<()> {
        let plan_parquet = union_exec(vec![parquet_exec(); 5]);
        let plan_csv = union_exec(vec![csv_exec(); 5]);

        let expected_parquet = &[
            "UnionExec",
            // Union doesn't benefit from input partitioning - no parallelism
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1]",
        ];
        let expected_csv = &[
            "UnionExec",
            // Union doesn't benefit from input partitioning - no parallelism
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_prior_to_sort_preserving_merge() -> Result<()> {
        // sort preserving merge already sorted input,
        let plan_parquet = sort_preserving_merge_exec(parquet_exec_sorted());
        let plan_csv = sort_preserving_merge_exec(csv_exec_sorted());

        // parallelization potentially could break sort order
        let expected_parquet = &[
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_sort_preserving_merge_with_union() -> Result<()> {
        // 2 sorted parquet files unioned (partitions are concatenated, sort is preserved)
        let input_parquet = union_exec(vec![parquet_exec_sorted(); 2]);
        let input_csv = union_exec(vec![csv_exec_sorted(); 2]);
        let plan_parquet = sort_preserving_merge_exec(input_parquet);
        let plan_csv = sort_preserving_merge_exec(input_csv);

        // should not repartition / sort (as the data was already sorted)
        let expected_parquet = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            "UnionExec",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_does_not_destroy_sort() -> Result<()> {
        //  SortRequired
        //    Parquet(sorted)
        let plan_parquet = sort_required_exec(parquet_exec_sorted());
        let plan_csv = sort_required_exec(csv_exec_sorted());

        // no parallelization to preserve sort order
        let expected_parquet = &[
            "SortRequiredExec",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "SortRequiredExec",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    #[test]
    fn parallelization_ignores_transitively_with_projection() -> Result<()> {
        // sorted input
        let plan_parquet =
            sort_preserving_merge_exec(projection_exec(parquet_exec_sorted()));
        let plan_csv = sort_preserving_merge_exec(projection_exec(csv_exec_sorted()));

        // data should not be repartitioned / resorted
        let expected_parquet = &[
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC]",
        ];
        let expected_csv = &[
            "ProjectionExec: expr=[c1@0 as c1]",
            "CsvExec: file_groups={1 group: [[x]]}, projection=[c1], output_ordering=[c1@0 ASC], has_header=false",
        ];

        assert_optimized!(expected_parquet, plan_parquet, true, 2, true, 10);
        assert_optimized!(expected_csv, plan_csv, true, 2, true, 10);
        Ok(())
    }

    /// Models operators like BoundedWindowExec that require an input
    /// ordering but is easy to construct
    #[derive(Debug)]
    struct SortRequiredExec {
        input: Arc<dyn ExecutionPlan>,
    }

    impl SortRequiredExec {
        fn new(input: Arc<dyn ExecutionPlan>) -> Self {
            Self { input }
        }
    }

    impl DisplayAs for SortRequiredExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "SortRequiredExec")
        }
    }

    impl ExecutionPlan for SortRequiredExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.input.schema()
        }

        fn output_partitioning(&self) -> crate::physical_plan::Partitioning {
            self.input.output_partitioning()
        }

        fn benefits_from_input_partitioning(&self) -> Vec<bool> {
            vec![false]
        }

        fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
            self.input.output_ordering()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.input.clone()]
        }

        // model that it requires the output ordering of its input
        fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
            vec![self
                .output_ordering()
                .map(PhysicalSortRequirement::from_sort_exprs)]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq!(children.len(), 1);
            let child = children.pop().unwrap();
            Ok(Arc::new(Self::new(child)))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<crate::execution::context::TaskContext>,
        ) -> Result<crate::physical_plan::SendableRecordBatchStream> {
            unreachable!();
        }

        fn statistics(&self) -> Statistics {
            self.input.statistics()
        }
    }
}
