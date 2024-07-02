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

//! Define a plan for unnesting values in columns that contain a list type.

use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties};
use crate::{
    DisplayFormatType, Distribution, ExecutionPlan, RecordBatchStream,
    SendableRecordBatchStream,
};

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray,
    PrimitiveArray,
};
use arrow::compute::kernels::length::length;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null, kernels, sum};
use arrow::datatypes::{DataType, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::{Int64Array, Scalar, StructArray};
use arrow_ord::cmp::lt;
use datafusion_common::{
    exec_datafusion_err, exec_err, internal_err, Result, UnnestOptions,
};
use datafusion_execution::TaskContext;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use hashbrown::HashSet;
use log::trace;

/// Unnest the given columns (either with type struct or list)
/// For list unnesting, each rows is vertically transformed into multiple rows
/// For struct unnesting, each columns is horizontally transformed into multiple columns,
/// Thus the original RecordBatch with dimension (n x m) may have new dimension (n' x m')
///
/// See [`UnnestOptions`] for more details and an example.
#[derive(Debug)]
pub struct UnnestExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The schema once the unnest is applied
    schema: SchemaRef,
    /// indices of the list-typed columns in the input schema
    list_column_indices: Vec<usize>,
    /// indices of the struct-typed columns in the input schema
    struct_column_indices: Vec<usize>,
    /// Options
    options: UnnestOptions,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl UnnestExec {
    /// Create a new [UnnestExec].
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        list_column_indices: Vec<usize>,
        struct_column_indices: Vec<usize>,
        schema: SchemaRef,
        options: UnnestOptions,
    ) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));

        UnnestExec {
            input,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            metrics: Default::default(),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            input.execution_mode(),
        )
    }
}

impl DisplayAs for UnnestExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnnestExec")
            }
        }
    }
}

impl ExecutionPlan for UnnestExec {
    fn name(&self) -> &'static str {
        "UnnestExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UnnestExec::new(
            Arc::clone(&children[0]),
            self.list_column_indices.clone(),
            self.struct_column_indices.clone(),
            Arc::clone(&self.schema),
            self.options.clone(),
        )))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let metrics = UnnestMetrics::new(partition, &self.metrics);

        Ok(Box::pin(UnnestStream {
            input,
            schema: Arc::clone(&self.schema),
            list_type_columns: self.list_column_indices.clone(),
            struct_column_indices: self.struct_column_indices.iter().copied().collect(),
            options: self.options.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[derive(Clone, Debug)]
struct UnnestMetrics {
    /// total time for column unnesting
    elapsed_compute: metrics::Time,
    /// Number of batches consumed
    input_batches: metrics::Count,
    /// Number of rows consumed
    input_rows: metrics::Count,
    /// Number of batches produced
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl UnnestMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_compute = MetricBuilder::new(metrics).elapsed_compute(partition);

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            input_batches,
            input_rows,
            output_batches,
            output_rows,
            elapsed_compute,
        }
    }
}

/// A stream that issues [RecordBatch]es with unnested column data.
struct UnnestStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// Unnested schema
    schema: Arc<Schema>,
    /// The unnest columns
    list_type_columns: Vec<usize>,
    struct_column_indices: HashSet<usize>,
    /// Options
    options: UnnestOptions,
    /// Metrics
    metrics: UnnestMetrics,
}

impl RecordBatchStream for UnnestStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[async_trait]
impl Stream for UnnestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl UnnestStream {
    /// Separate implementation function that unpins the [`UnnestStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<RecordBatch>>> {
        self.input
            .poll_next_unpin(cx)
            .map(|maybe_batch| match maybe_batch {
                Some(Ok(batch)) => {
                    let timer = self.metrics.elapsed_compute.timer();
                    let result = build_batch(
                        &batch,
                        &self.schema,
                        &self.list_type_columns,
                        &self.struct_column_indices,
                        &self.options,
                    );
                    self.metrics.input_batches.add(1);
                    self.metrics.input_rows.add(batch.num_rows());
                    if let Ok(ref batch) = result {
                        timer.done();
                        self.metrics.output_batches.add(1);
                        self.metrics.output_rows.add(batch.num_rows());
                    }

                    Some(result)
                }
                other => {
                    trace!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {}",
                        self.metrics.input_batches,
                        self.metrics.input_rows,
                        self.metrics.output_batches,
                        self.metrics.output_rows,
                        self.metrics.elapsed_compute,
                    );
                    other
                }
            })
    }
}

/// Given a set of struct column indices to flatten
/// try converting the column in input into multiple subfield columns
/// For example
/// struct_col: [a: struct(item: int, name: string), b: int]
/// with a batch
/// {a: {item: 1, name: "a"}, b: 2},
/// {a: {item: 3, name: "b"}, b: 4]
/// will be converted into
/// {a.item: 1, a.name: "a", b: 2},
/// {a.item: 3, a.name: "b", b: 4}
fn flatten_struct_cols(
    input_batch: &[Arc<dyn Array>],
    schema: &SchemaRef,
    struct_column_indices: &HashSet<usize>,
) -> Result<RecordBatch> {
    // horizontal expansion because of struct unnest
    let columns_expanded = input_batch
        .iter()
        .enumerate()
        .map(|(idx, column_data)| match struct_column_indices.get(&idx) {
            Some(_) => match column_data.data_type() {
                DataType::Struct(_) => {
                    let struct_arr =
                        column_data.as_any().downcast_ref::<StructArray>().unwrap();
                    Ok(struct_arr.columns().to_vec())
                }
                data_type => internal_err!(
                    "expecting column {} from input plan to be a struct, got {:?}",
                    idx,
                    data_type
                ),
            },
            None => Ok(vec![Arc::clone(column_data)]),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns_expanded)?)
}

/// For each row in a `RecordBatch`, some list/struct columns need to be unnested.
/// - For list columns: We will expand the values in each list into multiple rows,
/// taking the longest length among these lists, and shorter lists are padded with NULLs.
/// - For struct columns: We will expand the struct columns into multiple subfield columns.
/// For columns that don't need to be unnested, repeat their values until reaching the longest length.
fn build_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    list_type_columns: &[usize],
    struct_column_indices: &HashSet<usize>,
    options: &UnnestOptions,
) -> Result<RecordBatch> {
    let transformed = match list_type_columns.len() {
        0 => flatten_struct_cols(batch.columns(), schema, struct_column_indices),
        _ => {
            let list_arrays: Vec<ArrayRef> = list_type_columns
                .iter()
                .map(|index| {
                    ColumnarValue::Array(Arc::clone(batch.column(*index)))
                        .into_array(batch.num_rows())
                })
                .collect::<Result<_>>()?;

            let longest_length = find_longest_length(&list_arrays, options)?;
            let unnested_length = longest_length.as_primitive::<Int64Type>();
            let total_length = if unnested_length.is_empty() {
                0
            } else {
                sum(unnested_length).ok_or_else(|| {
                    exec_datafusion_err!("Failed to calculate the total unnested length")
                })? as usize
            };
            if total_length == 0 {
                return Ok(RecordBatch::new_empty(Arc::clone(schema)));
            }

            // Unnest all the list arrays
            let unnested_arrays =
                unnest_list_arrays(&list_arrays, unnested_length, total_length)?;
            let unnested_array_map: HashMap<_, _> = unnested_arrays
                .into_iter()
                .zip(list_type_columns.iter())
                .map(|(array, column)| (*column, array))
                .collect();

            // Create the take indices array for other columns
            let take_indicies = create_take_indicies(unnested_length, total_length);

            // vertical expansion because of list unnest
            let ret = flatten_list_cols_from_indices(
                batch,
                &unnested_array_map,
                &take_indicies,
            )?;
            flatten_struct_cols(&ret, schema, struct_column_indices)
        }
    };
    transformed
}

/// Find the longest list length among the given list arrays for each row.
///
/// For example if we have the following two list arrays:
///
/// ```ignore
/// l1: [1, 2, 3], null, [], [3]
/// l2: [4,5], [], null, [6, 7]
/// ```
///
/// If `preserve_nulls` is false, the longest length array will be:
///
/// ```ignore
/// longest_length: [3, 0, 0, 2]
/// ```
///
/// whereas if `preserve_nulls` is true, the longest length array will be:
///
///
/// ```ignore
/// longest_length: [3, 1, 1, 2]
/// ```
///
fn find_longest_length(
    list_arrays: &[ArrayRef],
    options: &UnnestOptions,
) -> Result<ArrayRef> {
    // The length of a NULL list
    let null_length = if options.preserve_nulls {
        Scalar::new(Int64Array::from_value(1, 1))
    } else {
        Scalar::new(Int64Array::from_value(0, 1))
    };
    let list_lengths: Vec<ArrayRef> = list_arrays
        .iter()
        .map(|list_array| {
            let mut length_array = length(list_array)?;
            // Make sure length arrays have the same type. Int64 is the most general one.
            length_array = cast(&length_array, &DataType::Int64)?;
            length_array =
                zip(&is_not_null(&length_array)?, &length_array, &null_length)?;
            Ok(length_array)
        })
        .collect::<Result<_>>()?;

    let longest_length = list_lengths.iter().skip(1).try_fold(
        Arc::clone(&list_lengths[0]),
        |longest, current| {
            let is_lt = lt(&longest, &current)?;
            zip(&is_lt, &current, &longest)
        },
    )?;
    Ok(longest_length)
}

/// Trait defining common methods used for unnesting, implemented by list array types.
trait ListArrayType: Array {
    /// Returns a reference to the values of this list.
    fn values(&self) -> &ArrayRef;

    /// Returns the start and end offset of the values for the given row.
    fn value_offsets(&self, row: usize) -> (i64, i64);
}

impl ListArrayType for ListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row].into(), offsets[row + 1].into())
    }
}

impl ListArrayType for LargeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row], offsets[row + 1])
    }
}

impl ListArrayType for FixedSizeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let start = self.value_offset(row) as i64;
        (start, start + self.value_length() as i64)
    }
}

/// Unnest multiple list arrays according to the length array.
fn unnest_list_arrays(
    list_arrays: &[ArrayRef],
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<Vec<ArrayRef>> {
    let typed_arrays = list_arrays
        .iter()
        .map(|list_array| match list_array.data_type() {
            DataType::List(_) => Ok(list_array.as_list::<i32>() as &dyn ListArrayType),
            DataType::LargeList(_) => {
                Ok(list_array.as_list::<i64>() as &dyn ListArrayType)
            }
            DataType::FixedSizeList(_, _) => {
                Ok(list_array.as_fixed_size_list() as &dyn ListArrayType)
            }
            other => exec_err!("Invalid unnest datatype {other }"),
        })
        .collect::<Result<Vec<_>>>()?;

    typed_arrays
        .iter()
        .map(|list_array| unnest_list_array(*list_array, length_array, capacity))
        .collect::<Result<_>>()
}

/// Unnest a list array according the target length array.
///
/// Consider a list array like this:
///
/// ```ignore
/// [1], [2, 3, 4], null, [5], [],
/// ```
///
/// and the length array is:
///
/// ```ignore
/// [2, 3, 2, 1, 2]
/// ```
///
/// If the length of a certain list is less than the target length, pad with NULLs.
/// So the unnested array will look like this:
///
/// ```ignore
/// [1, null, 2, 3, 4, null, null, 5, null, null]
/// ```
///
fn unnest_list_array(
    list_array: &dyn ListArrayType,
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<ArrayRef> {
    let values = list_array.values();
    let mut take_indicies_builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for row in 0..list_array.len() {
        let mut value_length = 0;
        if !list_array.is_null(row) {
            let (start, end) = list_array.value_offsets(row);
            value_length = end - start;
            for i in start..end {
                take_indicies_builder.append_value(i)
            }
        }
        let target_length = length_array.value(row);
        debug_assert!(
            value_length <= target_length,
            "value length is beyond the longest length"
        );
        // Pad with NULL values
        for _ in value_length..target_length {
            take_indicies_builder.append_null();
        }
    }
    Ok(kernels::take::take(
        &values,
        &take_indicies_builder.finish(),
        None,
    )?)
}

/// Creates take indicies that will be used to expand all columns except for the list type
/// [`columns`](UnnestExec::list_column_indices) that is being unnested.
/// Every column value needs to be repeated multiple times according to the length array.
///
/// If the length array looks like this:
///
/// ```ignore
/// [2, 3, 1]
/// ```
/// Then `create_take_indicies` will return an array like this
///
/// ```ignore
/// [0, 0, 1, 1, 1, 2]
/// ```
///
fn create_take_indicies(
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> PrimitiveArray<Int64Type> {
    // `find_longest_length()` guarantees this.
    debug_assert!(
        length_array.null_count() == 0,
        "length array should not contain nulls"
    );
    let mut builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for (index, repeat) in length_array.iter().enumerate() {
        // The length array should not contain nulls, so unwrap is safe
        let repeat = repeat.unwrap();
        (0..repeat).for_each(|_| builder.append_value(index as i64));
    }
    builder.finish()
}

/// Create the final batch given the unnested column arrays and a `indices` array
/// that is used by the take kernel to copy values.
///
/// For example if we have the following `RecordBatch`:
///
/// ```ignore
/// c1: [1], null, [2, 3, 4], null, [5, 6]
/// c2: 'a', 'b',  'c', null, 'd'
/// ```
///
/// then the `unnested_list_arrays` contains the unnest column that will replace `c1` in
/// the final batch if `preserve_nulls` is true:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// ```
///
/// And the `indices` array contains the indices that are used by `take` kernel to
/// repeat the values in `c2`:
///
/// ```ignore
/// 0, 1, 2, 2, 2, 3, 4, 4
/// ```
///
/// so that the final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// c2: 'a', 'b', 'c', 'c', 'c', null, 'd', 'd'
/// ```
///
fn flatten_list_cols_from_indices(
    batch: &RecordBatch,
    unnested_list_arrays: &HashMap<usize, ArrayRef>,
    indices: &PrimitiveArray<Int64Type>,
) -> Result<Vec<Arc<dyn Array>>> {
    let arrays = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(col_idx, arr)| match unnested_list_arrays.get(&col_idx) {
            Some(unnested_array) => Ok(Arc::clone(unnested_array)),
            None => Ok(kernels::take::take(arr, indices, None)?),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(arrays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use arrow_array::{GenericListArray, OffsetSizeTrait, StringArray};
    use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer};

    // Create a GenericListArray with the following list values:
    //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
    fn make_generic_array<OffsetSize>() -> GenericListArray<OffsetSize>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let mut values = vec![];
        let mut offsets: Vec<OffsetSize> = vec![OffsetSize::zero()];
        let mut valid = BooleanBufferBuilder::new(6);

        // [A, B, C]
        values.extend_from_slice(&[Some("A"), Some("B"), Some("C")]);
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        // []
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        // NULL with non-zero value length
        // Issue https://github.com/apache/datafusion/issues/9932
        values.push(Some("?"));
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(false);

        // [D]
        values.push(Some("D"));
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        // Another NULL with zero value length
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(false);

        // [NULL, F]
        values.extend_from_slice(&[None, Some("F")]);
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append(true);

        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        GenericListArray::<OffsetSize>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(StringArray::from(values)),
            Some(NullBuffer::new(valid.finish())),
        )
    }

    // Create a FixedSizeListArray with the following list values:
    //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
    fn make_fixed_list() -> FixedSizeListArray {
        let values = Arc::new(StringArray::from_iter([
            Some("A"),
            Some("B"),
            None,
            None,
            Some("C"),
            Some("D"),
            None,
            None,
            None,
            Some("F"),
            None,
            None,
        ]));
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let valid = NullBuffer::from(vec![true, false, true, false, true, true]);
        FixedSizeListArray::new(field, 2, values, Some(valid))
    }

    fn verify_unnest_list_array(
        list_array: &dyn ListArrayType,
        lengths: Vec<i64>,
        expected: Vec<Option<&str>>,
    ) -> datafusion_common::Result<()> {
        let length_array = Int64Array::from(lengths);
        let unnested_array = unnest_list_array(list_array, &length_array, 3 * 6)?;
        let strs = unnested_array.as_string::<i32>().iter().collect::<Vec<_>>();
        assert_eq!(strs, expected);
        Ok(())
    }

    #[test]
    fn test_unnest_list_array() -> datafusion_common::Result<()> {
        // [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = make_generic_array::<i32>();
        verify_unnest_list_array(
            &list_array,
            vec![3, 2, 1, 2, 0, 3],
            vec![
                Some("A"),
                Some("B"),
                Some("C"),
                None,
                None,
                None,
                Some("D"),
                None,
                None,
                Some("F"),
                None,
            ],
        )?;

        // [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list_array = make_fixed_list();
        verify_unnest_list_array(
            &list_array,
            vec![3, 1, 2, 0, 2, 3],
            vec![
                Some("A"),
                Some("B"),
                None,
                None,
                Some("C"),
                Some("D"),
                None,
                Some("F"),
                None,
                None,
                None,
            ],
        )?;

        Ok(())
    }

    fn verify_longest_length(
        list_arrays: &[ArrayRef],
        preserve_nulls: bool,
        expected: Vec<i64>,
    ) -> datafusion_common::Result<()> {
        let options = UnnestOptions { preserve_nulls };
        let longest_length = find_longest_length(list_arrays, &options)?;
        let expected_array = Int64Array::from(expected);
        assert_eq!(
            longest_length
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &expected_array
        );
        Ok(())
    }

    #[test]
    fn test_longest_list_length() -> datafusion_common::Result<()> {
        // Test with single ListArray
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        verify_longest_length(&[Arc::clone(&list_array)], false, vec![3, 0, 0, 1, 0, 2])?;
        verify_longest_length(&[Arc::clone(&list_array)], true, vec![3, 0, 1, 1, 1, 2])?;

        // Test with single LargeListArray
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = Arc::new(make_generic_array::<i64>()) as ArrayRef;
        verify_longest_length(&[Arc::clone(&list_array)], false, vec![3, 0, 0, 1, 0, 2])?;
        verify_longest_length(&[Arc::clone(&list_array)], true, vec![3, 0, 1, 1, 1, 2])?;

        // Test with single FixedSizeListArray
        //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list_array = Arc::new(make_fixed_list()) as ArrayRef;
        verify_longest_length(&[Arc::clone(&list_array)], false, vec![2, 0, 2, 0, 2, 2])?;
        verify_longest_length(&[Arc::clone(&list_array)], true, vec![2, 1, 2, 1, 2, 2])?;

        // Test with multiple list arrays
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list1 = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        let list2 = Arc::new(make_fixed_list()) as ArrayRef;
        let list_arrays = vec![Arc::clone(&list1), Arc::clone(&list2)];
        verify_longest_length(&list_arrays, false, vec![3, 0, 2, 1, 2, 2])?;
        verify_longest_length(&list_arrays, true, vec![3, 1, 2, 1, 2, 2])?;

        Ok(())
    }

    #[test]
    fn test_create_take_indicies() -> datafusion_common::Result<()> {
        let length_array = Int64Array::from(vec![2, 3, 1]);
        let take_indicies = create_take_indicies(&length_array, 6);
        let expected = Int64Array::from(vec![0, 0, 1, 1, 1, 2]);
        assert_eq!(take_indicies, expected);
        Ok(())
    }
}
