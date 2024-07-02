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

//! Common functions used for testing
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_int32_array;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

mod data_gen;
mod string_gen;
pub mod tpcds;
pub mod tpch;

pub use data_gen::AccessLogGenerator;
pub use string_gen::StringBatchGenerator;

pub use env_logger;

/// Extracts the i32 values from the set of batches and returns them as a single Vec
pub fn batches_to_vec(batches: &[RecordBatch]) -> Vec<Option<i32>> {
    batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.num_columns(), 1);
            as_int32_array(batch.column(0)).unwrap().iter()
        })
        .collect()
}

/// extract i32 values from batches and sort them
pub fn partitions_to_sorted_vec(partitions: &[Vec<RecordBatch>]) -> Vec<Option<i32>> {
    let mut values: Vec<_> = partitions
        .iter()
        .flat_map(|batches| batches_to_vec(batches).into_iter())
        .collect();

    values.sort_unstable();
    values
}

/// Adds a random number of empty record batches into the stream
pub fn add_empty_batches(
    batches: Vec<RecordBatch>,
    rng: &mut StdRng,
) -> Vec<RecordBatch> {
    let schema = batches[0].schema();

    batches
        .into_iter()
        .flat_map(|batch| {
            // insert 0, or 1 empty batches before and after the current batch
            let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
            std::iter::repeat(empty_batch.clone())
                .take(rng.gen_range(0..2))
                .chain(std::iter::once(batch))
                .chain(std::iter::repeat(empty_batch).take(rng.gen_range(0..2)))
        })
        .collect()
}

/// "stagger" batches: split the batches into random sized batches
///
/// For example, if the input batch has 1000 rows, [`stagger_batch`] might return
/// multiple batches
/// ```text
/// [
///   RecordBatch(123 rows),
///   RecordBatch(234 rows),
///   RecordBatch(634 rows),
/// ]
/// ```
pub fn stagger_batch(batch: RecordBatch) -> Vec<RecordBatch> {
    let seed = 42;
    stagger_batch_with_seed(batch, seed)
}

/// "stagger" batches: split the batches into random sized batches using the
/// specified value for a rng seed. See [`stagger_batch`] for more detail.
pub fn stagger_batch_with_seed(batch: RecordBatch, seed: u64) -> Vec<RecordBatch> {
    let mut batches = vec![];

    // use a random number generator to pick a random sized output
    let mut rng = StdRng::seed_from_u64(seed);

    let mut remainder = batch;
    while remainder.num_rows() > 0 {
        let batch_size = rng.gen_range(0..remainder.num_rows() + 1);

        batches.push(remainder.slice(0, batch_size));
        remainder = remainder.slice(batch_size, remainder.num_rows() - batch_size);
    }

    add_empty_batches(batches, &mut rng)
}

/// Table definition of a name/schema
pub struct TableDef {
    pub name: String,
    pub schema: Schema,
}

impl TableDef {
    fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema,
        }
    }
}
