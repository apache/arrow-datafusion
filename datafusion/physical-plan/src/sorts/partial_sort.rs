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

//! Partial Sort deals with input data that partially
//! satisfies the required sort order. Such an input data can be
//! partitioned into segments where each segment already has the
//! required information for lexicographic sorting so sorting
//! can be done without loading the entire dataset.
//!
//! Consider a sort plan having an input with ordering `a ASC, b ASC`
//!
//! ```text
//! +---+---+---+
//! | a | b | d |
//! +---+---+---+
//! | 0 | 0 | 3 |
//! | 0 | 0 | 2 |
//! | 0 | 1 | 1 |
//! | 0 | 2 | 0 |
//! +---+---+---+
//!```
//!
//! and required ordering for the plan is `a ASC, b ASC, d ASC`.
//! The first 3 rows(segment) can be sorted as the segment already
//! has the required information for the sort, but the last row
//! requires further information as the input can continue with a
//! batch with a starting row where a and b does not change as below
//!
//! ```text
//! +---+---+---+
//! | a | b | d |
//! +---+---+---+
//! | 0 | 2 | 4 |
//! +---+---+---+
//!```
//!
//! The plan concats incoming data with such last rows of previous input
//! and continues partial sorting of the segments.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::expressions::PhysicalSortExpr;
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::LexOrdering;

use crate::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use log::trace;

/// Partial Sort execution plan.
#[derive(Debug, Clone)]
pub struct PartialSortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Length of continuous matching columns of input that satisfy
    /// the required ordering for the sort
    common_prefix_length: usize,
    /// Containing all metrics set created during sort
    metrics_set: ExecutionPlanMetricsSet,
    /// Preserve partitions of input plan. If false, the input partitions
    /// will be sorted and merged into a single output partition.
    preserve_partitioning: bool,
    /// Fetch highest/lowest n results
    fetch: Option<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl PartialSortExec {
    /// Create a new partial sort execution plan
    pub fn new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        common_prefix_length: usize,
    ) -> Self {
        assert!(common_prefix_length > 0);
        let preserve_partitioning = false;
        let cache = Self::compute_properties(&input, expr.clone(), preserve_partitioning);
        Self {
            input,
            expr,
            common_prefix_length,
            metrics_set: ExecutionPlanMetricsSet::new(),
            preserve_partitioning,
            fetch: None,
            cache,
        }
    }

    /// Whether this `PartialSortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Specify the partitioning behavior of this partial sort exec
    ///
    /// If `preserve_partitioning` is true, sorts each partition
    /// individually, producing one sorted stream for each input partition.
    ///
    /// If `preserve_partitioning` is false, sorts and merges all
    /// input partitions producing a single, sorted partition.
    pub fn with_preserve_partitioning(mut self, preserve_partitioning: bool) -> Self {
        self.preserve_partitioning = preserve_partitioning;
        self.cache = self
            .cache
            .with_partitioning(Self::output_partitioning_helper(
                &self.input,
                self.preserve_partitioning,
            ));
        self
    }

    /// Modify how many rows to include in the result
    ///
    /// If None, then all rows will be returned, in sorted order.
    /// If Some, then only the top `fetch` rows will be returned.
    /// This can reduce the memory pressure required by the sort
    /// operation since rows that are not going to be included
    /// can be dropped.
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] {
        &self.expr
    }

    /// If `Some(fetch)`, limits output to only the first "fetch" items
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn output_partitioning_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Partitioning {
        // Get output partitioning:
        if preserve_partitioning {
            input.output_partitioning().clone()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
        preserve_partitioning: bool,
    ) -> PlanProperties {
        // Calculate equivalence properties; i.e. reset the ordering equivalence
        // class with the new ordering:
        let eq_properties = input
            .equivalence_properties()
            .clone()
            .with_reorder(sort_exprs);

        // Get output partitioning:
        let output_partitioning =
            Self::output_partitioning_helper(input, preserve_partitioning);

        // Determine execution mode:
        let mode = input.execution_mode();

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl DisplayAs for PartialSortExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let expr = PhysicalSortExpr::format_list(&self.expr);
                let common_prefix_length = self.common_prefix_length;
                match self.fetch {
                    Some(fetch) => {
                        write!(f, "PartialSortExec: TopK(fetch={fetch}), expr=[{expr}], common_prefix_length=[{common_prefix_length}]", )
                    }
                    None => write!(f, "PartialSortExec: expr=[{expr}], common_prefix_length=[{common_prefix_length}]"),
                }
            }
        }
    }
}

impl ExecutionPlan for PartialSortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.preserve_partitioning {
            vec![Distribution::UnspecifiedDistribution]
        } else {
            vec![Distribution::SinglePartition]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_partial_sort = PartialSortExec::new(
            self.expr.clone(),
            children[0].clone(),
            self.common_prefix_length,
        )
        .with_fetch(self.fetch)
        .with_preserve_partitioning(self.preserve_partitioning);

        Ok(Arc::new(new_partial_sort))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start PartialSortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        let mut input = self.input.execute(partition, context.clone())?;

        trace!(
            "End PartialSortExec's input.execute for partition: {}",
            partition
        );

        // Make sure common prefix length is larger than 0
        // Otherwise, we should use SortExec.
        assert!(self.common_prefix_length > 0);
        let execution_options = &context.session_config().options().execution;

        let mut sorter = crate::sorts::sort::ExternalSorter::new(
            partition,
            input.schema(),
            self.expr.clone(),
            context.session_config().batch_size(),
            self.fetch,
            execution_options.sort_spill_reservation_bytes,
            execution_options.sort_in_place_threshold_bytes,
            &self.metrics_set,
            context.runtime_env(),
        );
        let prefix = self.common_prefix_length;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(async move {
                while let Some(batch) = input.next().await {
                    let batch = batch?;
                    sorter.insert_batch_with_prefix(batch, prefix).await?;
                }
                sorter.sort()
            })
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use futures::FutureExt;

    use datafusion_common::assert_batches_eq;

    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::collect;
    use crate::expressions::col;
    use crate::memory::MemoryExec;
    use crate::test;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};

    use super::*;

    #[tokio::test]
    async fn test_partial_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 0, 1, 1, 1]),
            ("b", &vec![1, 1, 2, 2, 3, 3]),
            ("c", &vec![1, 0, 5, 4, 3, 2]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let partial_sort_exec = Arc::new(PartialSortExec::new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_asc,
                },
            ],
            source.clone(),
            2,
        )) as Arc<dyn ExecutionPlan>;

        let result = collect(partial_sort_exec, task_ctx.clone()).await?;

        let expected_after_sort = [
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 0 | 1 | 0 |",
            "| 0 | 1 | 1 |",
            "| 0 | 2 | 5 |",
            "| 1 | 2 | 4 |",
            "| 1 | 3 | 2 |",
            "| 1 | 3 | 3 |",
            "+---+---+---+",
        ];
        assert_eq!(1, result.len());
        assert_batches_eq!(expected_after_sort, &result);
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort_spill() -> Result<()> {
        // trigger spill w/ 100 batches
        let session_config = SessionConfig::new();
        let sort_spill_reservation_bytes = session_config
            .options()
            .execution
            .sort_spill_reservation_bytes;
        let rt_config = RuntimeConfig::new()
            .with_memory_limit(sort_spill_reservation_bytes + 12288, 1.0);
        let runtime = Arc::new(RuntimeEnv::new(rt_config)?);
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime),
        );

        let partitions = 100;
        let input = test::scan_partitioned(partitions);
        let schema = input.schema();

        let sort_exec = Arc::new(PartialSortExec::new(
            vec![PhysicalSortExpr {
                expr: col("i", &schema)?,
                options: SortOptions::default(),
            }],
            Arc::new(CoalescePartitionsExec::new(input)),
            1,
        ));

        let result = collect(sort_exec.clone(), task_ctx.clone()).await?;

        assert_eq!(result.len(), 2);

        // Now, validate metrics
        let metrics = sort_exec.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 10000);
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert!(metrics.spill_count().unwrap() > 0);
        assert!(metrics.spilled_bytes().unwrap() > 0);

        let columns = result[0].columns();

        let i = as_primitive_array::<Int32Type>(&columns[0]);
        assert_eq!(i.value(0), 0);
        assert_eq!(i.value(i.len() - 1), 81);

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_partial_sort_with_fetch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source = test::build_table_scan_i32(
            ("a", &vec![0, 0, 1, 1, 1]),
            ("b", &vec![1, 2, 2, 3, 3]),
            ("c", &vec![4, 3, 2, 1, 0]),
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        for common_prefix_length in [1, 2] {
            let partial_sort_exec = Arc::new(
                PartialSortExec::new(
                    vec![
                        PhysicalSortExpr {
                            expr: col("a", &schema)?,
                            options: option_asc,
                        },
                        PhysicalSortExpr {
                            expr: col("b", &schema)?,
                            options: option_asc,
                        },
                        PhysicalSortExpr {
                            expr: col("c", &schema)?,
                            options: option_asc,
                        },
                    ],
                    source.clone(),
                    common_prefix_length,
                )
                .with_fetch(Some(4)),
            ) as Arc<dyn ExecutionPlan>;

            let result = collect(partial_sort_exec, task_ctx.clone()).await?;

            let expected_after_sort = [
                "+---+---+---+",
                "| a | b | c |",
                "+---+---+---+",
                "| 0 | 1 | 4 |",
                "| 0 | 2 | 3 |",
                "| 1 | 2 | 2 |",
                "| 1 | 3 | 0 |",
                "+---+---+---+",
            ];
            assert_eq!(1, result.len());
            assert_batches_eq!(expected_after_sort, &result);
            assert_eq!(
                task_ctx.runtime_env().memory_pool.reserved(),
                0,
                "The sort should have returned all memory used back to the memory manager"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_sort2() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let source_tables = [
            test::build_table_scan_i32(
                ("a", &vec![0, 0, 0, 0, 1, 1, 1, 1]),
                ("b", &vec![1, 1, 3, 3, 4, 4, 2, 2]),
                ("c", &vec![7, 6, 5, 4, 3, 2, 1, 0]),
            ),
            test::build_table_scan_i32(
                ("a", &vec![0, 0, 0, 0, 1, 1, 1, 1]),
                ("b", &vec![1, 1, 3, 3, 2, 2, 4, 4]),
                ("c", &vec![7, 6, 5, 4, 1, 0, 3, 2]),
            ),
        ];
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        for (common_prefix_length, source) in
            [(1, &source_tables[0]), (2, &source_tables[1])]
        {
            let partial_sort_exec = Arc::new(PartialSortExec::new(
                vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema)?,
                        options: option_asc,
                    },
                    PhysicalSortExpr {
                        expr: col("c", &schema)?,
                        options: option_asc,
                    },
                ],
                source.clone(),
                common_prefix_length,
            ));

            let result = collect(partial_sort_exec, task_ctx.clone()).await?;
            assert_eq!(1, result.len());
            assert_eq!(
                task_ctx.runtime_env().memory_pool.reserved(),
                0,
                "The sort should have returned all memory used back to the memory manager"
            );
            let expected = [
                "+---+---+---+",
                "| a | b | c |",
                "+---+---+---+",
                "| 0 | 1 | 6 |",
                "| 0 | 1 | 7 |",
                "| 0 | 3 | 4 |",
                "| 0 | 3 | 5 |",
                "| 1 | 2 | 0 |",
                "| 1 | 2 | 1 |",
                "| 1 | 4 | 2 |",
                "| 1 | 4 | 3 |",
                "+---+---+---+",
            ];
            assert_batches_eq!(expected, &result);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let field_metadata: HashMap<String, String> =
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            vec![("baz".to_string(), "barf".to_string())]
                .into_iter()
                .collect();

        let mut field = Field::new("field_name", DataType::UInt64, true);
        field.set_metadata(field_metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let schema = Arc::new(schema);

        let data: ArrayRef =
            Arc::new(vec![1, 1, 2].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(schema.clone(), vec![data])?;
        let input = Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None)?);

        let partial_sort_exec = Arc::new(PartialSortExec::new(
            vec![PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }],
            input,
            1,
        ));

        let result: Vec<RecordBatch> = collect(partial_sort_exec, task_ctx).await?;
        let expected_batch = vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                vec![1, 1, 2].into_iter().map(Some).collect::<UInt64Array>(),
            )],
        )?];

        // Data is correct
        assert_eq!(&expected_batch, &result);

        // explicitly ensure the metadata is present
        assert_eq!(result[0].schema().fields()[0].metadata(), &field_metadata);
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Float64, true),
        ]));
        let option_asc = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                    Some(3.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(20.0_f64),
                    Some(20.0_f64),
                    Some(40.0_f64),
                    Some(40.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    Some(100.0_f64),
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let partial_sort_exec = Arc::new(PartialSortExec::new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: option_asc,
                },
                PhysicalSortExpr {
                    expr: col("c", &schema)?,
                    options: option_desc,
                },
            ],
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?),
            2,
        ));

        let expected = [
            "+-----+------+-------+",
            "| a   | b    | c     |",
            "+-----+------+-------+",
            "| 1.0 | 20.0 | 20.0  |",
            "| 1.0 | 20.0 | 10.0  |",
            "| 1.0 | 40.0 | 10.0  |",
            "| 2.0 | 40.0 | 100.0 |",
            "| 2.0 | NaN  | NaN   |",
            "| 3.0 |      |       |",
            "| 3.0 |      | 100.0 |",
            "| 3.0 | NaN  | NaN   |",
            "+-----+------+-------+",
        ];

        assert_eq!(
            DataType::Float32,
            *partial_sort_exec.schema().field(0).data_type()
        );
        assert_eq!(
            DataType::Float64,
            *partial_sort_exec.schema().field(1).data_type()
        );
        assert_eq!(
            DataType::Float64,
            *partial_sort_exec.schema().field(2).data_type()
        );

        let result: Vec<RecordBatch> =
            collect(partial_sort_exec.clone(), task_ctx).await?;
        assert_batches_eq!(expected, &result);
        assert_eq!(result.len(), 1);
        let metrics = partial_sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());
        assert_eq!(DataType::Float64, *columns[2].data_type());

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float32, true),
        ]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(PartialSortExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
            1,
        ));

        let fut = collect(sort_exec, task_ctx.clone());
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }
}
