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

#[cfg(test)]
mod sp_repartition_fuzz_tests {
    use std::sync::Arc;

    use arrow::compute::{concat_batches, lexsort, SortColumn};
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};

    use datafusion::physical_plan::{
        collect,
        memory::MemoryExec,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        repartition::RepartitionExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
        sorts::streaming_merge::streaming_merge,
        stream::RecordBatchStreamAdapter,
        ExecutionPlan, Partitioning,
    };
    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use datafusion_execution::{
        config::SessionConfig, memory_pool::MemoryConsumer, SendableRecordBatchStream,
    };
    use datafusion_physical_expr::{
        equivalence::{EquivalenceClass, EquivalenceProperties},
        expressions::{col, Column},
        ConstExpr, PhysicalExpr, PhysicalSortExpr,
    };
    use test_utils::add_empty_batches;

    use itertools::izip;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    // Generate a schema which consists of 6 columns (a, b, c, d, e, f)
    fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f]));

        Ok(schema)
    }

    /// Construct a schema with random ordering
    /// among column a, b, c, d
    /// where
    /// Column [a=f] (e.g they are aliases).
    /// Column e is constant.
    fn create_random_schema(seed: u64) -> Result<(SchemaRef, EquivalenceProperties)> {
        let test_schema = create_test_schema()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let col_exprs = [col_a, col_b, col_c, col_d, col_e, col_f];

        let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
        // Define a and f are aliases
        eq_properties.add_equal_conditions(col_a, col_f)?;
        // Column e has constant value.
        eq_properties = eq_properties.add_constants([ConstExpr::new(col_e.clone())]);

        // Randomly order columns for sorting
        let mut rng = StdRng::seed_from_u64(seed);
        let mut remaining_exprs = col_exprs[0..4].to_vec(); // only a, b, c, d are sorted

        let options_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        while !remaining_exprs.is_empty() {
            let n_sort_expr = rng.gen_range(0..remaining_exprs.len() + 1);
            remaining_exprs.shuffle(&mut rng);

            let ordering = remaining_exprs
                .drain(0..n_sort_expr)
                .map(|expr| PhysicalSortExpr {
                    expr: expr.clone(),
                    options: options_asc,
                })
                .collect();

            eq_properties.add_new_orderings([ordering]);
        }

        Ok((test_schema, eq_properties))
    }

    // If we already generated a random result for one of the
    // expressions in the equivalence classes. For other expressions in the same
    // equivalence class use same result. This util gets already calculated result, when available.
    fn get_representative_arr(
        eq_group: &EquivalenceClass,
        existing_vec: &[Option<ArrayRef>],
        schema: SchemaRef,
    ) -> Option<ArrayRef> {
        for expr in eq_group.iter() {
            let col = expr.as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            if let Some(res) = &existing_vec[idx] {
                return Some(res.clone());
            }
        }
        None
    }

    // Generate a table that satisfies the given equivalence properties; i.e.
    // equivalences, ordering equivalences, and constants.
    fn generate_table_for_eq_properties(
        eq_properties: &EquivalenceProperties,
        n_elem: usize,
        n_distinct: usize,
    ) -> Result<RecordBatch> {
        let mut rng = StdRng::seed_from_u64(23);

        let schema = eq_properties.schema();
        let mut schema_vec = vec![None; schema.fields.len()];

        // Utility closure to generate random array
        let mut generate_random_array = |num_elems: usize, max_val: usize| -> ArrayRef {
            let values: Vec<u64> = (0..num_elems)
                .map(|_| rng.gen_range(0..max_val) as u64)
                .collect();
            Arc::new(UInt64Array::from_iter_values(values))
        };

        // Fill constant columns
        for constant in eq_properties.constants() {
            let col = constant.expr().as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            let arr =
                Arc::new(UInt64Array::from_iter_values(vec![0; n_elem])) as ArrayRef;
            schema_vec[idx] = Some(arr);
        }

        // Fill columns based on ordering equivalences
        for ordering in eq_properties.oeq_class().iter() {
            let (sort_columns, indices): (Vec<_>, Vec<_>) = ordering
                .iter()
                .map(|PhysicalSortExpr { expr, options }| {
                    let col = expr.as_any().downcast_ref::<Column>().unwrap();
                    let (idx, _field) = schema.column_with_name(col.name()).unwrap();
                    let arr = generate_random_array(n_elem, n_distinct);
                    (
                        SortColumn {
                            values: arr,
                            options: Some(*options),
                        },
                        idx,
                    )
                })
                .unzip();

            let sort_arrs = arrow::compute::lexsort(&sort_columns, None)?;
            for (idx, arr) in izip!(indices, sort_arrs) {
                schema_vec[idx] = Some(arr);
            }
        }

        // Fill columns based on equivalence groups
        for eq_group in eq_properties.eq_group().iter() {
            let representative_array =
                get_representative_arr(eq_group, &schema_vec, schema.clone())
                    .unwrap_or_else(|| generate_random_array(n_elem, n_distinct));

            for expr in eq_group.iter() {
                let col = expr.as_any().downcast_ref::<Column>().unwrap();
                let (idx, _field) = schema.column_with_name(col.name()).unwrap();
                schema_vec[idx] = Some(representative_array.clone());
            }
        }

        let res: Vec<_> = schema_vec
            .into_iter()
            .zip(schema.fields.iter())
            .map(|(elem, field)| {
                (
                    field.name(),
                    // Generate random values for columns that do not occur in any of the groups (equivalence, ordering equivalence, constants)
                    elem.unwrap_or_else(|| generate_random_array(n_elem, n_distinct)),
                )
            })
            .collect();

        Ok(RecordBatch::try_from_iter(res)?)
    }

    // This test checks for whether during sort preserving merge we can preserve all of the valid orderings
    // successfully. If at the input we have orderings [a ASC, b ASC], [c ASC, d ASC]
    // After sort preserving merge orderings [a ASC, b ASC], [c ASC, d ASC] should still be valid.
    #[tokio::test]
    async fn stream_merge_multi_order_preserve() -> Result<()> {
        const N_PARTITION: usize = 8;
        const N_ELEM: usize = 25;
        const N_DISTINCT: usize = 5;
        const N_DIFF_SCHEMA: usize = 20;

        use datafusion::physical_plan::common::collect;
        for seed in 0..N_DIFF_SCHEMA {
            // Create a schema with random equivalence properties
            let (_test_schema, eq_properties) = create_random_schema(seed as u64)?;
            let table_data_with_properties =
                generate_table_for_eq_properties(&eq_properties, N_ELEM, N_DISTINCT)?;
            let schema = table_data_with_properties.schema();
            let streams: Vec<SendableRecordBatchStream> = (0..N_PARTITION)
                .map(|_idx| {
                    let batch = table_data_with_properties.clone();
                    Box::pin(RecordBatchStreamAdapter::new(
                        schema.clone(),
                        futures::stream::once(async { Ok(batch) }),
                    )) as SendableRecordBatchStream
                })
                .collect::<Vec<_>>();

            // Returns concatenated version of the all available orderings
            let exprs = eq_properties
                .oeq_class()
                .output_ordering()
                .unwrap_or_default();

            let context = SessionContext::new().task_ctx();
            let mem_reservation =
                MemoryConsumer::new("test".to_string()).register(context.memory_pool());

            // Internally SortPreservingMergeExec uses this function for merging.
            let res = streaming_merge(
                streams,
                schema,
                &exprs,
                BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
                1,
                None,
                mem_reservation,
            )?;
            let res = collect(res).await?;
            // Contains the merged result.
            let res = concat_batches(&res[0].schema(), &res)?;

            for ordering in eq_properties.oeq_class().iter() {
                let err_msg = format!("error in eq properties: {:?}", eq_properties);
                let sort_solumns = ordering
                    .iter()
                    .map(|sort_expr| sort_expr.evaluate_to_sort_column(&res))
                    .collect::<Result<Vec<_>>>()?;
                let orig_columns = sort_solumns
                    .iter()
                    .map(|sort_column| sort_column.values.clone())
                    .collect::<Vec<_>>();
                let sorted_columns = lexsort(&sort_solumns, None)?;

                // Make sure after merging ordering is still valid.
                assert_eq!(orig_columns.len(), sorted_columns.len(), "{}", err_msg);
                assert!(
                    izip!(orig_columns.into_iter(), sorted_columns.into_iter())
                        .all(|(lhs, rhs)| { lhs == rhs }),
                    "{}",
                    err_msg
                )
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn sort_preserving_repartition_test() {
        let seed_start = 0;
        let seed_end = 100;
        let n_row = 1000;
        // Since ordering in the test (ORDER BY a,b,c)
        // covers all the table (table consists of a,b,c columns).
        // Result doesn't depend on the stable/unstable sort
        // behaviour. We can choose, n_distinct as we like. However,
        // we chose it a large number to decrease probability of having same rows in the table.
        let n_distinct = 1_000_000;
        for (is_first_roundrobin, is_first_sort_preserving) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            for is_second_roundrobin in [false, true] {
                let mut handles = Vec::new();

                for seed in seed_start..seed_end {
                    #[allow(clippy::disallowed_methods)] // spawn allowed only in tests
                    let job = tokio::spawn(run_sort_preserving_repartition_test(
                        make_staggered_batches::<true>(n_row, n_distinct, seed as u64),
                        is_first_roundrobin,
                        is_first_sort_preserving,
                        is_second_roundrobin,
                    ));
                    handles.push(job);
                }

                for job in handles {
                    job.await.unwrap();
                }
            }
        }
    }

    /// Check whether physical plan below
    ///     "SortPreservingMergeExec: [a@0 ASC,b@1 ASC,c@2 ASC]",
    ///     "  SortPreservingRepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=2", (Partitioning can be roundrobin also)
    ///     "    SortPreservingRepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=1", (Partitioning can be roundrobin also)
    ///     "      MemoryExec: partitions=1, partition_sizes=[75]",
    /// and / or
    ///     "SortPreservingMergeExec: [a@0 ASC,b@1 ASC,c@2 ASC]",
    ///     "  SortPreservingRepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=2", (Partitioning can be roundrobin also)
    ///     "    RepartitionExec: partitioning=Hash([Column { name: \"c\", index: 2 }], 2), input_partitions=1", (Partitioning can be roundrobin also)
    ///     "      MemoryExec: partitions=1, partition_sizes=[75]",
    /// preserves ordering. Input fed to the plan above should be same with the output of the plan.
    async fn run_sort_preserving_repartition_test(
        input1: Vec<RecordBatch>,
        // If `true`, first repartition executor after `MemoryExec` will be in `RoundRobin` mode
        // else it will be in `Hash` mode
        is_first_roundrobin: bool,
        // If `true`, first repartition executor after `MemoryExec` will be `SortPreservingRepartitionExec`
        // If `false`, first repartition executor after `MemoryExec` will be `RepartitionExec` (Since its input
        // partition number is 1, `RepartitionExec` also preserves ordering.).
        is_first_sort_preserving: bool,
        // If `true`, second repartition executor after `MemoryExec` will be in `RoundRobin` mode
        // else it will be in `Hash` mode
        is_second_roundrobin: bool,
    ) {
        let schema = input1[0].schema();
        let session_config = SessionConfig::new().with_batch_size(50);
        let ctx = SessionContext::new_with_config(session_config);
        let mut sort_keys = vec![];
        for ordering_col in ["a", "b", "c"] {
            sort_keys.push(PhysicalSortExpr {
                expr: col(ordering_col, &schema).unwrap(),
                options: SortOptions::default(),
            })
        }

        let concat_input_record = concat_batches(&schema, &input1).unwrap();

        let running_source = Arc::new(
            MemoryExec::try_new(&[input1.clone()], schema.clone(), None)
                .unwrap()
                .with_sort_information(vec![sort_keys.clone()]),
        );
        let hash_exprs = vec![col("c", &schema).unwrap()];

        let intermediate = match (is_first_roundrobin, is_first_sort_preserving) {
            (true, true) => sort_preserving_repartition_exec_round_robin(running_source),
            (true, false) => repartition_exec_round_robin(running_source),
            (false, true) => {
                sort_preserving_repartition_exec_hash(running_source, hash_exprs.clone())
            }
            (false, false) => repartition_exec_hash(running_source, hash_exprs.clone()),
        };

        let intermediate = if is_second_roundrobin {
            sort_preserving_repartition_exec_round_robin(intermediate)
        } else {
            sort_preserving_repartition_exec_hash(intermediate, hash_exprs.clone())
        };

        let final_plan = sort_preserving_merge_exec(sort_keys.clone(), intermediate);
        let task_ctx = ctx.task_ctx();

        let collected_running = collect(final_plan, task_ctx.clone()).await.unwrap();
        let concat_res = concat_batches(&schema, &collected_running).unwrap();
        assert_eq!(concat_res, concat_input_record);
    }

    fn sort_preserving_repartition_exec_round_robin(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2))
                .unwrap()
                .with_preserve_order(),
        )
    }

    fn repartition_exec_round_robin(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(2)).unwrap(),
        )
    }

    fn sort_preserving_repartition_exec_hash(
        input: Arc<dyn ExecutionPlan>,
        hash_expr: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::Hash(hash_expr, 2))
                .unwrap()
                .with_preserve_order(),
        )
    }

    fn repartition_exec_hash(
        input: Arc<dyn ExecutionPlan>,
        hash_expr: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::Hash(hash_expr, 2)).unwrap(),
        )
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    /// Return randomly sized record batches with:
    /// three sorted int64 columns 'a', 'b', 'c' ranged from 0..'n_distinct' as columns
    pub(crate) fn make_staggered_batches<const STREAM: bool>(
        len: usize,
        n_distinct: usize,
        random_seed: u64,
    ) -> Vec<RecordBatch> {
        // use a random number generator to pick a random sized output
        let mut rng = StdRng::seed_from_u64(random_seed);
        let mut input123: Vec<(i64, i64, i64)> = vec![(0, 0, 0); len];
        input123.iter_mut().for_each(|v| {
            *v = (
                rng.gen_range(0..n_distinct) as i64,
                rng.gen_range(0..n_distinct) as i64,
                rng.gen_range(0..n_distinct) as i64,
            )
        });
        input123.sort();
        let input1 =
            Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.0));
        let input2 =
            Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.1));
        let input3 =
            Int64Array::from_iter_values(input123.clone().into_iter().map(|k| k.2));

        // split into several record batches
        let mut remainder = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(input1) as ArrayRef),
            ("b", Arc::new(input2) as ArrayRef),
            ("c", Arc::new(input3) as ArrayRef),
        ])
        .unwrap();

        let mut batches = vec![];
        if STREAM {
            while remainder.num_rows() > 0 {
                let batch_size = rng.gen_range(0..50);
                if remainder.num_rows() < batch_size {
                    break;
                }
                batches.push(remainder.slice(0, batch_size));
                remainder =
                    remainder.slice(batch_size, remainder.num_rows() - batch_size);
            }
        } else {
            while remainder.num_rows() > 0 {
                let batch_size = rng.gen_range(0..remainder.num_rows() + 1);
                batches.push(remainder.slice(0, batch_size));
                remainder =
                    remainder.slice(batch_size, remainder.num_rows() - batch_size);
            }
        }
        add_empty_batches(batches, &mut rng)
    }
}
