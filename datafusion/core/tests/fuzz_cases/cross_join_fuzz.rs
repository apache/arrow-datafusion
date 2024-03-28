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

//! Fuzz Test for CrossJoinExec

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::{ArrayRef, Int32Array, PrimitiveArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion_common::utils::get_arrayref_at_indices;
use datafusion_common::{JoinType, Result};
use datafusion_execution::config::SessionConfig;
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::{collect, joins::CrossJoinExec, memory::MemoryExec};

use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

#[tokio::test]
async fn test_cross_fuzz() {
    let seeds = [2, 15, 29, 54, 87, 123, 543, 1342, 23452, 123432];
    for seed in seeds {
        run_test(seed).await;
    }
}

async fn run_test(seed: u64) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let cfg = SessionConfig::new();
    let ctx = SessionContext::new_with_config(cfg);

    let left_batch_count = rng.gen_range(0..=20);
    let mut left_row_counts = vec![];
    for _ in 0..left_batch_count {
        left_row_counts.push(rng.gen_range(0..=10));
    }
    let right_batch_count = rng.gen_range(0..=20);
    let mut right_row_counts = vec![];
    for _ in 0..right_batch_count {
        right_row_counts.push(rng.gen_range(0..=10));
    }

    let (left_data, right_data) =
        generate_data(left_row_counts.clone(), right_row_counts.clone(), &mut rng);

    let left_schema =
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let right_schema =
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

    let left_memory_exec =
        Arc::new(MemoryExec::try_new(&[left_data.clone()], left_schema, None).unwrap())
            as _;
    let right_memory_exec =
        Arc::new(MemoryExec::try_new(&[right_data.clone()], right_schema, None).unwrap())
            as _;

    let cross_join = Arc::new(CrossJoinExec::new(left_memory_exec, right_memory_exec));

    let collected = collect(cross_join, ctx.task_ctx()).await.unwrap();

    assert_results(collected, left_data, right_data).unwrap();
}

fn generate_data(
    left_row_counts: Vec<i32>,
    right_row_counts: Vec<i32>,
    rng: &mut StdRng,
) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let mut left_data = vec![];
    for count in left_row_counts {
        let mut values_a = vec![];
        for _ in 0..count {
            let value = rng.gen_range(0..1000000);
            values_a.push(value);
        }
        let column_a = Arc::new(Int32Array::from_iter_values(values_a)) as ArrayRef;

        left_data.push(RecordBatch::try_from_iter(vec![("a", column_a)]).unwrap());
    }

    let mut right_data = vec![];
    for batch_len in right_row_counts {
        let mut values_x = vec![];
        for _ in 0..batch_len {
            let value = rng.gen_range(0..1000000);
            values_x.push(value);
        }
        let column_x = Arc::new(Int32Array::from_iter_values(values_x)) as ArrayRef;

        right_data.push(RecordBatch::try_from_iter(vec![("x", column_x)]).unwrap());
    }
    (left_data, right_data)
}

fn assert_results(
    collected: Vec<RecordBatch>,
    left_data: Vec<RecordBatch>,
    right_data: Vec<RecordBatch>,
) -> Result<()> {
    if left_data.is_empty()
        || left_data.iter().all(|rb| rb.num_rows() == 0)
        || right_data.is_empty()
        || right_data.iter().all(|rb| rb.num_rows() == 0)
    {
        assert!(collected.is_empty());
        return Ok(());
    }

    // Left side batch sizes must be equal to collected batch sizes.
    let right_row_count = right_data
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    for _ in 0..right_row_count {
        let mut non_zero_idx = 0;
        for collected_batch in collected.iter() {
            if left_data[non_zero_idx % left_data.len()].num_rows() != 0 {
                assert_eq!(
                    collected_batch.num_rows(),
                    left_data[non_zero_idx % left_data.len()].num_rows()
                );
                non_zero_idx += 1;
            }
        }
    }

    let left_data = concat_batches(&left_data[0].schema(), &left_data)?;
    let right_data = concat_batches(&right_data[0].schema(), &right_data)?;

    let n_left = left_data.num_rows();
    let n_right = right_data.num_rows();

    let (result_schema, _) =
        build_join_schema(&left_data.schema(), &right_data.schema(), &JoinType::Inner);

    let left_cols = left_data.columns();
    let mut all_results = vec![];
    for idx in 0..right_data.num_rows() {
        let indices = PrimitiveArray::from_iter_values(vec![idx as u32; n_left]);
        let right_cols = get_arrayref_at_indices(right_data.columns(), &indices)?;

        let mut join_cols = vec![];
        join_cols.extend(left_cols.to_vec());
        join_cols.extend(right_cols);

        let batch = RecordBatch::try_new(Arc::new(result_schema.clone()), join_cols)?;
        all_results.push(batch);
    }

    // Check resulting row count holds.
    let join_result = concat_batches(&Arc::new(result_schema.clone()), &all_results)?;
    assert_eq!(join_result.num_rows(), n_left * n_right);

    // Check all values are as expected.
    let collected = concat_batches(&Arc::new(result_schema), &collected)?;
    assert!(join_result.eq(&collected));

    Ok(())
}
