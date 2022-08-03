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

//! Defines the execution plan for the hash aggregate operation

use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use ahash::RandomState;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};

use crate::error::Result;
use crate::physical_plan::aggregates::{
    evaluate_group_by, evaluate_many, AccumulatorItem, AggregateMode, PhysicalGroupBy,
};
use crate::physical_plan::hash_utils::create_hashes;
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{aggregates, AggregateExpr, PhysicalExpr};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use crate::scalar::ScalarValue;

use arrow::{array::ArrayRef, compute, compute::cast};
use arrow::{
    array::{Array, UInt32Builder},
    error::{ArrowError, Result as ArrowResult},
};
use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use hashbrown::raw::RawTable;

/*
The architecture is the following:

1. An accumulator has state that is updated on each batch.
2. At the end of the aggregation (e.g. end of batches in a partition), the accumulator converts its state to a RecordBatch of a single row
3. The RecordBatches of all accumulators are merged (`concatenate` in `rust/arrow`) together to a single RecordBatch.
4. The state's RecordBatch is `merge`d to a new state
5. The state is mapped to the final value

Why:

* Accumulators' state can be statically typed, but it is more efficient to transmit data from the accumulators via `Array`
* The `merge` operation must have access to the state of the aggregators because it uses it to correctly merge
* It uses Arrow's native dynamically typed object, `Array`.
* Arrow shines in batch operations and both `merge` and `concatenate` of uniform types are very performant.

Example: average

* the state is `n: u32` and `sum: f64`
* For every batch, we update them accordingly.
* At the end of the accumulation (of a partition), we convert `n` and `sum` to a RecordBatch of 1 row and two columns: `[n, sum]`
* The RecordBatch is (sent back / transmitted over network)
* Once all N record batches arrive, `merge` is performed, which builds a RecordBatch with N rows and 2 columns.
* Finally, `get_value` returns an array with one entry computed from the state
*/
pub(crate) struct GroupedHashAggregateStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,
    accumulators: Accumulators,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    aggr_expr: Vec<Arc<dyn AggregateExpr>>,
    group_by: PhysicalGroupBy,

    baseline_metrics: BaselineMetrics,
    random_state: RandomState,
    finished: bool,
}

impl GroupedHashAggregateStream {
    /// Create a new GroupedHashAggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> Result<Self> {
        let timer = baseline_metrics.elapsed_compute().timer();

        // The expressions to evaluate the batch, one vec of expressions per aggregation.
        // Assume create_schema() always put group columns in front of aggr columns, we set
        // col_idx_base to group expression count.
        let aggregate_expressions =
            aggregates::aggregate_expressions(&aggr_expr, &mode, group_by.expr.len())?;

        timer.done();

        Ok(Self {
            schema,
            mode,
            input,
            aggr_expr,
            group_by,
            baseline_metrics,
            aggregate_expressions,
            accumulators: Default::default(),
            random_state: Default::default(),
            finished: false,
        })
    }
}

impl Stream for GroupedHashAggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.finished {
            return Poll::Ready(None);
        }

        let elapsed_compute = this.baseline_metrics.elapsed_compute();

        loop {
            let result = match ready!(this.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();
                    let result = group_aggregate_batch(
                        &this.mode,
                        &this.random_state,
                        &this.group_by,
                        &this.aggr_expr,
                        batch,
                        &mut this.accumulators,
                        &this.aggregate_expressions,
                    );

                    timer.done();

                    match result {
                        Ok(_) => continue,
                        Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                    }
                }
                Some(Err(e)) => Err(e),
                None => {
                    this.finished = true;
                    let timer = this.baseline_metrics.elapsed_compute().timer();
                    let result = create_batch_from_map(
                        &this.mode,
                        &this.accumulators,
                        this.group_by.expr.len(),
                        &this.schema,
                    )
                    .record_output(&this.baseline_metrics);

                    timer.done();
                    result
                }
            };

            this.finished = true;
            return Poll::Ready(Some(result));
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// TODO: Make this a member function of [`GroupedHashAggregateStream`]
fn group_aggregate_batch(
    mode: &AggregateMode,
    random_state: &RandomState,
    group_by: &PhysicalGroupBy,
    aggr_expr: &[Arc<dyn AggregateExpr>],
    batch: RecordBatch,
    accumulators: &mut Accumulators,
    aggregate_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<()> {
    // evaluate the grouping expressions
    let group_by_values = evaluate_group_by(group_by, &batch)?;

    // evaluate the aggregation expressions.
    // We could evaluate them after the `take`, but since we need to evaluate all
    // of them anyways, it is more performant to do it while they are together.
    let aggr_input_values = evaluate_many(aggregate_expressions, &batch)?;

    for grouping_set_values in group_by_values {
        // 1.1 construct the key from the group values
        // 1.2 construct the mapping key if it does not exist
        // 1.3 add the row' index to `indices`

        // track which entries in `accumulators` have rows in this batch to aggregate
        let mut groups_with_rows = vec![];

        // 1.1 Calculate the group keys for the group values
        let mut batch_hashes = vec![0; batch.num_rows()];
        create_hashes(&grouping_set_values, random_state, &mut batch_hashes)?;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let Accumulators { map, group_states } = accumulators;

            let entry = map.get_mut(hash, |(_hash, group_idx)| {
                // verify that a group that we are inserting with hash is
                // actually the same key value as the group in
                // existing_idx  (aka group_values @ row)
                let group_state = &group_states[*group_idx];
                grouping_set_values
                    .iter()
                    .zip(group_state.group_by_values.iter())
                    .all(|(array, scalar)| scalar.eq_array(array, row))
            });

            match entry {
                // Existing entry for this group value
                Some((_hash, group_idx)) => {
                    let group_state = &mut group_states[*group_idx];
                    // 1.3
                    if group_state.indices.is_empty() {
                        groups_with_rows.push(*group_idx);
                    };
                    group_state.indices.push(row as u32); // remember this row
                }
                //  1.2 Need to create new entry
                None => {
                    let accumulator_set = aggregates::create_accumulators(aggr_expr)?;

                    // Copy group values out of arrays into `ScalarValue`s
                    let group_by_values = grouping_set_values
                        .iter()
                        .map(|col| ScalarValue::try_from_array(col, row))
                        .collect::<Result<Vec<_>>>()?;

                    // Add new entry to group_states and save newly created index
                    let group_state = GroupState {
                        group_by_values: group_by_values.into_boxed_slice(),
                        accumulator_set,
                        indices: vec![row as u32], // 1.3
                    };
                    let group_idx = group_states.len();
                    group_states.push(group_state);
                    groups_with_rows.push(group_idx);

                    // for hasher function, use precomputed hash value
                    map.insert(hash, (hash, group_idx), |(hash, _group_idx)| *hash);
                }
            };
        }

        // Collect all indices + offsets based on keys in this vec
        let mut batch_indices: UInt32Builder = UInt32Builder::new(0);
        let mut offsets = vec![0];
        let mut offset_so_far = 0;
        for group_idx in groups_with_rows.iter() {
            let indices = &accumulators.group_states[*group_idx].indices;
            batch_indices.append_slice(indices);
            offset_so_far += indices.len();
            offsets.push(offset_so_far);
        }
        let batch_indices = batch_indices.finish();

        // `Take` all values based on indices into Arrays
        let values: Vec<Vec<Arc<dyn Array>>> = aggr_input_values
            .iter()
            .map(|array| {
                array
                    .iter()
                    .map(|array| {
                        compute::take(
                            array.as_ref(),
                            &batch_indices,
                            None, // None: no index check
                        )
                        .unwrap()
                    })
                    .collect()
                // 2.3
            })
            .collect();

        // 2.1 for each key in this batch
        // 2.2 for each aggregation
        // 2.3 `slice` from each of its arrays the keys' values
        // 2.4 update / merge the accumulator with the values
        // 2.5 clear indices
        groups_with_rows
            .iter()
            .zip(offsets.windows(2))
            .try_for_each(|(group_idx, offsets)| {
                let group_state = &mut accumulators.group_states[*group_idx];
                // 2.2
                group_state
                    .accumulator_set
                    .iter_mut()
                    .zip(values.iter())
                    .map(|(accumulator, aggr_array)| {
                        (
                            accumulator,
                            aggr_array
                                .iter()
                                .map(|array| {
                                    // 2.3
                                    array.slice(offsets[0], offsets[1] - offsets[0])
                                })
                                .collect::<Vec<ArrayRef>>(),
                        )
                    })
                    .try_for_each(|(accumulator, values)| match mode {
                        AggregateMode::Partial => accumulator.update_batch(&values),
                        AggregateMode::FinalPartitioned | AggregateMode::Final => {
                            // note: the aggregation here is over states, not values, thus the merge
                            accumulator.merge_batch(&values)
                        }
                    })
                    // 2.5
                    .and({
                        group_state.indices.clear();
                        Ok(())
                    })
            })?;
    }

    Ok(())
}

/// The state that is built for each output group.
#[derive(Debug)]
struct GroupState {
    /// The actual group by values, one for each group column
    group_by_values: Box<[ScalarValue]>,

    // Accumulator state, one for each aggregate
    accumulator_set: Vec<AccumulatorItem>,

    /// scratch space used to collect indices for input rows in a
    /// bach that have values to aggregate. Reset on each batch
    indices: Vec<u32>,
}

/// The state of all the groups
#[derive(Default)]
struct Accumulators {
    /// Logically maps group values to an index in `group_states`
    ///
    /// Uses the raw API of hashbrown to avoid actually storing the
    /// keys in the table
    ///
    /// keys: u64 hashes of the GroupValue
    /// values: (hash, index into `group_states`)
    map: RawTable<(u64, usize)>,

    /// State for each group
    group_states: Vec<GroupState>,
}

impl std::fmt::Debug for Accumulators {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // hashes are not store inline, so could only get values
        let map_string = "RawTable";
        f.debug_struct("Accumulators")
            .field("map", &map_string)
            .field("group_states", &self.group_states)
            .finish()
    }
}

/// Create a RecordBatch with all group keys and accumulator' states or values.
fn create_batch_from_map(
    mode: &AggregateMode,
    accumulators: &Accumulators,
    num_group_expr: usize,
    output_schema: &Schema,
) -> ArrowResult<RecordBatch> {
    if accumulators.group_states.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(output_schema.to_owned())));
    }
    let accs = &accumulators.group_states[0].accumulator_set;
    let mut acc_data_types: Vec<usize> = vec![];

    // Calculate number/shape of state arrays
    match mode {
        AggregateMode::Partial => {
            for acc in accs.iter() {
                let state = acc.state()?;
                acc_data_types.push(state.len());
            }
        }
        AggregateMode::Final | AggregateMode::FinalPartitioned => {
            acc_data_types = vec![1; accs.len()];
        }
    }

    let mut columns = (0..num_group_expr)
        .map(|i| {
            ScalarValue::iter_to_array(
                accumulators
                    .group_states
                    .iter()
                    .map(|group_state| group_state.group_by_values[i].clone()),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // add state / evaluated arrays
    for (x, &state_len) in acc_data_types.iter().enumerate() {
        for y in 0..state_len {
            match mode {
                AggregateMode::Partial => {
                    let res = ScalarValue::iter_to_array(
                        accumulators.group_states.iter().map(|group_state| {
                            group_state.accumulator_set[x]
                                .state()
                                .and_then(|x| x[y].as_scalar().map(|v| v.clone()))
                                .expect("unexpected accumulator state in hash aggregate")
                        }),
                    )?;

                    columns.push(res);
                }
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    let res = ScalarValue::iter_to_array(
                        accumulators.group_states.iter().map(|group_state| {
                            group_state.accumulator_set[x].evaluate().unwrap()
                        }),
                    )?;
                    columns.push(res);
                }
            }
        }
    }

    // cast output if needed (e.g. for types like Dictionary where
    // the intermediate GroupByScalar type was not the same as the
    // output
    let columns = columns
        .iter()
        .zip(output_schema.fields().iter())
        .map(|(col, desired_field)| cast(col, desired_field.data_type()))
        .collect::<ArrowResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::new(output_schema.to_owned()), columns)
}
