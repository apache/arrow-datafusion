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

//! Fuzz Test for various corner cases sorting RecordBatches exceeds available memory and should spill

use arrow::{
    array::{ArrayRef, Int32Array},
    compute::SortOptions,
    record_batch::RecordBatch,
};
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::expressions::{col, PhysicalSortExpr};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use fuzz_utils::{add_empty_batches, batches_to_vec, partitions_to_sorted_vec};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_sort_1k_mem() {
    run_sort(1024, vec![(5, false), (2000, true), (1000000, true)]).await
}

#[tokio::test]
#[ignore]
async fn test_sort_100k_mem() {
    run_sort(102400, vec![(5, false), (2000, false), (1000000, true)]).await
}

#[tokio::test]
async fn test_sort_unlimited_mem() {
    run_sort(
        usize::MAX,
        vec![(5, false), (2000, false), (1000000, false)],
    )
    .await
}

/// Sort the input using SortExec and ensure the results are correct according to `Vec::sort`
async fn run_sort(pool_size: usize, size_spill: Vec<(usize, bool)>) {
    for (size, spill) in size_spill {
        let input = vec![make_staggered_batches(size)];
        let first_batch = input
            .iter()
            .flat_map(|p| p.iter())
            .next()
            .expect("at least one batch");
        let schema = first_batch.schema();

        let sort = vec![PhysicalSortExpr {
            expr: col("x", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];

        let exec = MemoryExec::try_new(&input, schema, None).unwrap();
        let sort = Arc::new(SortExec::try_new(sort, Arc::new(exec)).unwrap());

        let runtime_config = RuntimeConfig::new().with_memory_manager(
            MemoryManagerConfig::try_new_limit(pool_size, 1.0).unwrap(),
        );
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let session_ctx = SessionContext::with_config_rt(SessionConfig::new(), runtime);

        let task_ctx = session_ctx.task_ctx();
        let collected = collect(sort.clone(), task_ctx).await.unwrap();

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);

        if spill {
            assert_ne!(sort.metrics().unwrap().spill_count().unwrap(), 0);
        } else {
            assert_eq!(sort.metrics().unwrap().spill_count().unwrap(), 0);
        }

        assert_eq!(
            session_ctx
                .runtime_env()
                .memory_manager
                .get_requester_total(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );
        assert_eq!(expected, actual, "failure in @ pool_size {}", pool_size);
    }
}

/// Return randomly sized record batches in a field named 'x' of type `Int32`
/// with randomized i32 content
fn make_staggered_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();
    let mut input: Vec<i32> = vec![0; len];
    rng.fill(&mut input[..]);
    let input = Int32Array::from_iter_values(input.into_iter());

    // split into several record batches
    let mut remainder =
        RecordBatch::try_from_iter(vec![("x", Arc::new(input) as ArrayRef)]).unwrap();

    let mut batches = vec![];

    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(42);
    while remainder.num_rows() > 0 {
        let batch_size = rng.gen_range(0..remainder.num_rows() + 1);

        batches.push(remainder.slice(0, batch_size));
        remainder = remainder.slice(batch_size, remainder.num_rows() - batch_size);
    }

    add_empty_batches(batches, &mut rng)
}
