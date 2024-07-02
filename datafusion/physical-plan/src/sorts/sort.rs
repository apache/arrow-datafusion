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

//! Sort that deals with an arbitrary size of the input.
//! It will do in-memory sorting if it has enough memory budget
//! but spills to disk if needed.

use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::common::spawn_buffered;
use crate::expressions::PhysicalSortExpr;
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use crate::sorts::streaming_merge::streaming_merge;
use crate::stream::RecordBatchStreamAdapter;
use crate::topk::TopK;
use crate::{
    read_spill_as_stream, spill_record_batches, DisplayAs, DisplayFormatType,
    Distribution, EmptyRecordBatchStream, ExecutionMode, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties, SendableRecordBatchStream,
    Statistics,
};

use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use arrow_array::{Array, RecordBatchOptions, UInt32Array};
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::LexOrdering;

use futures::{StreamExt, TryStreamExt};
use log::{debug, trace};

struct ExternalSorterMetrics {
    /// metrics
    baseline: BaselineMetrics,

    /// count of spills during the execution of the operator
    spill_count: Count,

    /// total spilled bytes during the execution of the operator
    spilled_bytes: Count,

    /// total spilled rows during the execution of the operator
    spilled_rows: Count,
}

impl ExternalSorterMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            spilled_rows: MetricBuilder::new(metrics).spilled_rows(partition),
        }
    }
}

/// Sorts an arbitrary sized, unsorted, stream of [`RecordBatch`]es to
/// a total order. Depending on the input size and memory manager
/// configuration, writes intermediate results to disk ("spills")
/// using Arrow IPC format.
///
/// # Algorithm
///
/// 1. get a non-empty new batch from input
///
/// 2. check with the memory manager there is sufficient space to
///   buffer the batch in memory 2.1 if memory sufficient, buffer
///   batch in memory, go to 1.
///
/// 2.2 if no more memory is available, sort all buffered batches and
///     spill to file.  buffer the next batch in memory, go to 1.
///
/// 3. when input is exhausted, merge all in memory batches and spills
/// to get a total order.
///
/// # When data fits in available memory
///
/// If there is sufficient memory, data is sorted in memory to produce the output
///
/// ```text
///    ┌─────┐
///    │  2  │
///    │  3  │
///    │  1  │─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///    │  4  │
///    │  2  │                  │
///    └─────┘                  ▼
///    ┌─────┐
///    │  1  │              In memory
///    │  4  │─ ─ ─ ─ ─ ─▶ sort/merge  ─ ─ ─ ─ ─▶  total sorted output
///    │  1  │
///    └─────┘                  ▲
///      ...                    │
///
///    ┌─────┐                  │
///    │  4  │
///    │  3  │─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///    └─────┘
///
/// in_mem_batches
///
/// ```
///
/// # When data does not fit in available memory
///
///  When memory is exhausted, data is first sorted and written to one
///  or more spill files on disk:
///
/// ```text
///    ┌─────┐                               .─────────────────.
///    │  2  │                              (                   )
///    │  3  │                              │`─────────────────'│
///    │  1  │─ ─ ─ ─ ─ ─ ─                 │  ┌────┐           │
///    │  4  │             │                │  │ 1  │░          │
///    │  2  │                              │  │... │░          │
///    └─────┘             ▼                │  │ 4  │░  ┌ ─ ─   │
///    ┌─────┐                              │  └────┘░    1  │░ │
///    │  1  │         In memory            │   ░░░░░░  │    ░░ │
///    │  4  │─ ─ ▶   sort/merge    ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─▶ ... │░ │
///    │  1  │     and write to file        │           │    ░░ │
///    └─────┘                              │             4  │░ │
///      ...               ▲                │           └░─░─░░ │
///                        │                │            ░░░░░░ │
///    ┌─────┐                              │.─────────────────.│
///    │  4  │             │                (                   )
///    │  3  │─ ─ ─ ─ ─ ─ ─                  `─────────────────'
///    └─────┘
///
/// in_mem_batches                                  spills
///                                         (file on disk in Arrow
///                                               IPC format)
/// ```
///
/// Once the input is completely read, the spill files are read and
/// merged with any in memory batches to produce a single total sorted
/// output:
///
/// ```text
///   .─────────────────.
///  (                   )
///  │`─────────────────'│
///  │  ┌────┐           │
///  │  │ 1  │░          │
///  │  │... │─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─
///  │  │ 4  │░ ┌────┐   │           │
///  │  └────┘░ │ 1  │░  │           ▼
///  │   ░░░░░░ │    │░  │
///  │          │... │─ ─│─ ─ ─ ▶ merge  ─ ─ ─▶  total sorted output
///  │          │    │░  │
///  │          │ 4  │░  │           ▲
///  │          └────┘░  │           │
///  │           ░░░░░░  │
///  │.─────────────────.│           │
///  (                   )
///   `─────────────────'            │
///         spills
///                                  │
///
///                                  │
///
///     ┌─────┐                      │
///     │  1  │
///     │  4  │─ ─ ─ ─               │
///     └─────┘       │
///       ...                   In memory
///                   └ ─ ─ ─▶  sort/merge
///     ┌─────┐
///     │  4  │                      ▲
///     │  3  │─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///     └─────┘
///
///  in_mem_batches
/// ```
struct ExternalSorter {
    /// schema of the output (and the input)
    schema: SchemaRef,
    /// Potentially unsorted in memory buffer
    in_mem_batches: Vec<RecordBatch>,
    /// if `Self::in_mem_batches` are sorted
    in_mem_batches_sorted: bool,
    /// If data has previously been spilled, the locations of the
    /// spill files (in Arrow IPC format)
    spills: Vec<RefCountedTempFile>,
    /// Sort expressions
    expr: Arc<[PhysicalSortExpr]>,
    /// Runtime metrics
    metrics: ExternalSorterMetrics,
    /// If Some, the maximum number of output rows that will be
    /// produced.
    fetch: Option<usize>,
    /// Reservation for in_mem_batches
    reservation: MemoryReservation,
    /// Reservation for the merging of in-memory batches. If the sort
    /// might spill, `sort_spill_reservation_bytes` will be
    /// pre-reserved to ensure there is some space for this sort/merge.
    merge_reservation: MemoryReservation,
    /// A handle to the runtime to get spill files
    runtime: Arc<RuntimeEnv>,
    /// The target number of rows for output batches
    batch_size: usize,
    /// How much memory to reserve for performing in-memory sort/merges
    /// prior to spilling.
    sort_spill_reservation_bytes: usize,
    /// If the in size of buffered memory batches is below this size,
    /// the data will be concatenated and sorted in place rather than
    /// sort/merged.
    sort_in_place_threshold_bytes: usize,
}

impl ExternalSorter {
    // TODO: make a builder or some other nicer API to avoid the
    // clippy warning
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        schema: SchemaRef,
        expr: Vec<PhysicalSortExpr>,
        batch_size: usize,
        fetch: Option<usize>,
        sort_spill_reservation_bytes: usize,
        sort_in_place_threshold_bytes: usize,
        metrics: &ExecutionPlanMetricsSet,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let metrics = ExternalSorterMetrics::new(metrics, partition_id);
        let reservation = MemoryConsumer::new(format!("ExternalSorter[{partition_id}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        let merge_reservation =
            MemoryConsumer::new(format!("ExternalSorterMerge[{partition_id}]"))
                .register(&runtime.memory_pool);

        Self {
            schema,
            in_mem_batches: vec![],
            in_mem_batches_sorted: true,
            spills: vec![],
            expr: expr.into(),
            metrics,
            fetch,
            reservation,
            merge_reservation,
            runtime,
            batch_size,
            sort_spill_reservation_bytes,
            sort_in_place_threshold_bytes,
        }
    }

    /// Appends an unsorted [`RecordBatch`] to `in_mem_batches`
    ///
    /// Updates memory usage metrics, and possibly triggers spilling to disk
    async fn insert_batch(&mut self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }
        self.reserve_memory_for_merge()?;

        let size = input.get_array_memory_size();
        if self.reservation.try_grow(size).is_err() {
            let before = self.reservation.size();
            self.in_mem_sort().await?;
            // Sorting may have freed memory, especially if fetch is `Some`
            //
            // As such we check again, and if the memory usage has dropped by
            // a factor of 2, and we can allocate the necessary capacity,
            // we don't spill
            //
            // The factor of 2 aims to avoid a degenerate case where the
            // memory required for `fetch` is just under the memory available,
            // causing repeated re-sorting of data
            if self.reservation.size() > before / 2
                || self.reservation.try_grow(size).is_err()
            {
                self.spill().await?;
                self.reservation.try_grow(size)?
            }
        }

        self.in_mem_batches.push(input);
        self.in_mem_batches_sorted = false;
        Ok(())
    }

    fn spilled_before(&self) -> bool {
        !self.spills.is_empty()
    }

    /// Returns the final sorted output of all batches inserted via
    /// [`Self::insert_batch`] as a stream of [`RecordBatch`]es.
    ///
    /// This process could either be:
    ///
    /// 1. An in-memory sort/merge (if the input fit in memory)
    ///
    /// 2. A combined streaming merge incorporating both in-memory
    /// batches and data from spill files on disk.
    fn sort(&mut self) -> Result<SendableRecordBatchStream> {
        if self.spilled_before() {
            let mut streams = vec![];
            if !self.in_mem_batches.is_empty() {
                let in_mem_stream =
                    self.in_mem_sort_stream(self.metrics.baseline.intermediate())?;
                streams.push(in_mem_stream);
            }

            for spill in self.spills.drain(..) {
                if !spill.path().exists() {
                    return Err(DataFusionError::Internal(format!(
                        "Spill file {:?} does not exist",
                        spill.path()
                    )));
                }
                let stream = read_spill_as_stream(spill, Arc::clone(&self.schema), 2)?;
                streams.push(stream);
            }

            streaming_merge(
                streams,
                Arc::clone(&self.schema),
                &self.expr,
                self.metrics.baseline.clone(),
                self.batch_size,
                self.fetch,
                self.reservation.new_empty(),
            )
        } else if !self.in_mem_batches.is_empty() {
            self.in_mem_sort_stream(self.metrics.baseline.clone())
        } else {
            Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.schema,
            ))))
        }
    }

    /// How much memory is buffered in this `ExternalSorter`?
    fn used(&self) -> usize {
        self.reservation.size()
    }

    /// How many bytes have been spilled to disk?
    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes.value()
    }

    /// How many rows have been spilled to disk?
    fn spilled_rows(&self) -> usize {
        self.metrics.spilled_rows.value()
    }

    /// How many spill files have been created?
    fn spill_count(&self) -> usize {
        self.metrics.spill_count.value()
    }

    /// Writes any `in_memory_batches` to a spill file and clears
    /// the batches. The contents of the spill file are sorted.
    ///
    /// Returns the amount of memory freed.
    async fn spill(&mut self) -> Result<usize> {
        // we could always get a chance to free some memory as long as we are holding some
        if self.in_mem_batches.is_empty() {
            return Ok(0);
        }

        debug!("Spilling sort data of ExternalSorter to disk whilst inserting");

        self.in_mem_sort().await?;

        let spill_file = self.runtime.disk_manager.create_tmp_file("Sorting")?;
        let batches = std::mem::take(&mut self.in_mem_batches);
        let spilled_rows = spill_record_batches(
            batches,
            spill_file.path().into(),
            Arc::clone(&self.schema),
        )?;
        let used = self.reservation.free();
        self.metrics.spill_count.add(1);
        self.metrics.spilled_bytes.add(used);
        self.metrics.spilled_rows.add(spilled_rows);
        self.spills.push(spill_file);
        Ok(used)
    }

    /// Sorts the in_mem_batches in place
    async fn in_mem_sort(&mut self) -> Result<()> {
        if self.in_mem_batches_sorted {
            return Ok(());
        }

        // Release the memory reserved for merge back to the pool so
        // there is some left when `in_memo_sort_stream` requests an
        // allocation.
        self.merge_reservation.free();

        self.in_mem_batches = self
            .in_mem_sort_stream(self.metrics.baseline.intermediate())?
            .try_collect()
            .await?;

        let size: usize = self
            .in_mem_batches
            .iter()
            .map(|x| x.get_array_memory_size())
            .sum();

        // Reserve headroom for next sort/merge
        self.reserve_memory_for_merge()?;

        self.reservation.try_resize(size)?;
        self.in_mem_batches_sorted = true;
        Ok(())
    }

    /// Consumes in_mem_batches returning a sorted stream of
    /// batches. This proceeds in one of two ways:
    ///
    /// # Small Datasets
    ///
    /// For "smaller" datasets, the data is first concatenated into a
    /// single batch and then sorted. This is often faster than
    /// sorting and then merging.
    ///
    /// ```text
    ///        ┌─────┐
    ///        │  2  │
    ///        │  3  │
    ///        │  1  │─ ─ ─ ─ ┐            ┌─────┐
    ///        │  4  │                     │  2  │
    ///        │  2  │        │            │  3  │
    ///        └─────┘                     │  1  │             sorted output
    ///        ┌─────┐        ▼            │  4  │                stream
    ///        │  1  │                     │  2  │
    ///        │  4  │─ ─▶ concat ─ ─ ─ ─ ▶│  1  │─ ─ ▶  sort  ─ ─ ─ ─ ─▶
    ///        │  1  │                     │  4  │
    ///        └─────┘        ▲            │  1  │
    ///          ...          │            │ ... │
    ///                                    │  4  │
    ///        ┌─────┐        │            │  3  │
    ///        │  4  │                     └─────┘
    ///        │  3  │─ ─ ─ ─ ┘
    ///        └─────┘
    ///     in_mem_batches
    /// ```
    ///
    /// # Larger datasets
    ///
    /// For larger datasets, the batches are first sorted individually
    /// and then merged together.
    ///
    /// ```text
    ///      ┌─────┐                ┌─────┐
    ///      │  2  │                │  1  │
    ///      │  3  │                │  2  │
    ///      │  1  │─ ─▶  sort  ─ ─▶│  2  │─ ─ ─ ─ ─ ┐
    ///      │  4  │                │  3  │
    ///      │  2  │                │  4  │          │
    ///      └─────┘                └─────┘               sorted output
    ///      ┌─────┐                ┌─────┐          ▼       stream
    ///      │  1  │                │  1  │
    ///      │  4  │─ ▶  sort  ─ ─ ▶│  1  ├ ─ ─ ▶ merge  ─ ─ ─ ─▶
    ///      │  1  │                │  4  │
    ///      └─────┘                └─────┘          ▲
    ///        ...       ...         ...             │
    ///
    ///      ┌─────┐                ┌─────┐          │
    ///      │  4  │                │  3  │
    ///      │  3  │─ ▶  sort  ─ ─ ▶│  4  │─ ─ ─ ─ ─ ┘
    ///      └─────┘                └─────┘
    ///
    ///   in_mem_batches
    /// ```
    fn in_mem_sort_stream(
        &mut self,
        metrics: BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        assert_ne!(self.in_mem_batches.len(), 0);
        if self.in_mem_batches.len() == 1 {
            let batch = self.in_mem_batches.remove(0);
            let reservation = self.reservation.take();
            return self.sort_batch_stream(batch, metrics, reservation);
        }

        // If less than sort_in_place_threshold_bytes, concatenate and sort in place
        if self.reservation.size() < self.sort_in_place_threshold_bytes {
            // Concatenate memory batches together and sort
            let batch = concat_batches(&self.schema, &self.in_mem_batches)?;
            self.in_mem_batches.clear();
            self.reservation.try_resize(batch.get_array_memory_size())?;
            let reservation = self.reservation.take();
            return self.sort_batch_stream(batch, metrics, reservation);
        }

        let streams = std::mem::take(&mut self.in_mem_batches)
            .into_iter()
            .map(|batch| {
                let metrics = self.metrics.baseline.intermediate();
                let reservation = self.reservation.split(batch.get_array_memory_size());
                let input = self.sort_batch_stream(batch, metrics, reservation)?;
                Ok(spawn_buffered(input, 1))
            })
            .collect::<Result<_>>()?;

        streaming_merge(
            streams,
            Arc::clone(&self.schema),
            &self.expr,
            metrics,
            self.batch_size,
            self.fetch,
            self.merge_reservation.new_empty(),
        )
    }

    /// Sorts a single `RecordBatch` into a single stream.
    ///
    /// `reservation` accounts for the memory used by this batch and
    /// is released when the sort is complete
    fn sort_batch_stream(
        &self,
        batch: RecordBatch,
        metrics: BaselineMetrics,
        reservation: MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(batch.get_array_memory_size(), reservation.size());
        let schema = batch.schema();

        let fetch = self.fetch;
        let expressions = Arc::clone(&self.expr);
        let stream = futures::stream::once(futures::future::lazy(move |_| {
            let sorted = sort_batch(&batch, &expressions, fetch)?;
            metrics.record_output(sorted.num_rows());
            drop(batch);
            drop(reservation);
            Ok(sorted)
        }));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    /// If this sort may spill, pre-allocates
    /// `sort_spill_reservation_bytes` of memory to gurarantee memory
    /// left for the in memory sort/merge.
    fn reserve_memory_for_merge(&mut self) -> Result<()> {
        // Reserve headroom for next merge sort
        if self.runtime.disk_manager.tmp_files_enabled() {
            let size = self.sort_spill_reservation_bytes;
            if self.merge_reservation.size() != size {
                self.merge_reservation.try_resize(size)?;
            }
        }

        Ok(())
    }
}

impl Debug for ExternalSorter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ExternalSorter")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_rows", &self.spilled_rows())
            .field("spill_count", &self.spill_count())
            .finish()
    }
}

pub fn sort_batch(
    batch: &RecordBatch,
    expressions: &[PhysicalSortExpr],
    fetch: Option<usize>,
) -> Result<RecordBatch> {
    let sort_columns = expressions
        .iter()
        .map(|expr| expr.evaluate_to_sort_column(batch))
        .collect::<Result<Vec<_>>>()?;

    let indices = if is_multi_column_with_lists(&sort_columns) {
        // lex_sort_to_indices doesn't support List with more than one colum
        // https://github.com/apache/arrow-rs/issues/5454
        lexsort_to_indices_multi_columns(sort_columns, fetch)?
    } else {
        lexsort_to_indices(&sort_columns, fetch)?
    };

    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<_, _>>()?;

    let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

#[inline]
fn is_multi_column_with_lists(sort_columns: &[SortColumn]) -> bool {
    sort_columns.iter().any(|c| {
        matches!(
            c.values.data_type(),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        )
    })
}

pub(crate) fn lexsort_to_indices_multi_columns(
    sort_columns: Vec<SortColumn>,
    limit: Option<usize>,
) -> Result<UInt32Array> {
    let (fields, columns) = sort_columns.into_iter().fold(
        (vec![], vec![]),
        |(mut fields, mut columns), sort_column| {
            fields.push(SortField::new_with_options(
                sort_column.values.data_type().clone(),
                sort_column.options.unwrap_or_default(),
            ));
            columns.push(sort_column.values);
            (fields, columns)
        },
    );

    // TODO reuse converter and rows, refer to TopK.
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(&columns)?;
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

    let mut len = rows.num_rows();
    if let Some(limit) = limit {
        len = limit.min(len);
    }
    let indices =
        UInt32Array::from_iter_values(sort.iter().take(len).map(|(i, _)| *i as u32));

    Ok(indices)
}

/// Sort execution plan.
///
/// Support sorting datasets that are larger than the memory allotted
/// by the memory manager, by spilling to disk.
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
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

impl SortExec {
    /// Create a new sort execution plan that produces a single,
    /// sorted output partition.
    pub fn new(expr: Vec<PhysicalSortExpr>, input: Arc<dyn ExecutionPlan>) -> Self {
        let preserve_partitioning = false;
        let cache = Self::compute_properties(&input, expr.clone(), preserve_partitioning);
        Self {
            expr,
            input,
            metrics_set: ExecutionPlanMetricsSet::new(),
            preserve_partitioning,
            fetch: None,
            cache,
        }
    }

    /// Whether this `SortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Specify the partitioning behavior of this sort exec
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
        let mode = match input.execution_mode() {
            ExecutionMode::Unbounded | ExecutionMode::PipelineBreaking => {
                ExecutionMode::PipelineBreaking
            }
            ExecutionMode::Bounded => ExecutionMode::Bounded,
        };

        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl DisplayAs for SortExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let expr = PhysicalSortExpr::format_list(&self.expr);
                let preserve_partioning = self.preserve_partitioning;
                match self.fetch {
                    Some(fetch) => {
                        write!(f, "SortExec: TopK(fetch={fetch}), expr=[{expr}], preserve_partitioning=[{preserve_partioning}]",)
                    }
                    None => write!(f, "SortExec: expr=[{expr}], preserve_partitioning=[{preserve_partioning}]"),
                }
            }
        }
    }
}

impl ExecutionPlan for SortExec {
    fn name(&self) -> &'static str {
        "SortExec"
    }

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
            // global sort
            // TODO support RangePartition and OrderedDistribution
            vec![Distribution::SinglePartition]
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_sort = SortExec::new(self.expr.clone(), Arc::clone(&children[0]))
            .with_fetch(self.fetch)
            .with_preserve_partitioning(self.preserve_partitioning);

        Ok(Arc::new(new_sort))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start SortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        let mut input = self.input.execute(partition, Arc::clone(&context))?;

        let execution_options = &context.session_config().options().execution;

        trace!("End SortExec's input.execute for partition: {}", partition);

        if let Some(fetch) = self.fetch.as_ref() {
            let mut topk = TopK::try_new(
                partition,
                input.schema(),
                self.expr.clone(),
                *fetch,
                context.session_config().batch_size(),
                context.runtime_env(),
                &self.metrics_set,
                partition,
            )?;

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::once(async move {
                    while let Some(batch) = input.next().await {
                        let batch = batch?;
                        topk.insert_batch(batch)?;
                    }
                    topk.emit()
                })
                .try_flatten(),
            )))
        } else {
            let mut sorter = ExternalSorter::new(
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

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::once(async move {
                    while let Some(batch) = input.next().await {
                        let batch = batch?;
                        sorter.insert_batch(batch).await?;
                    }
                    sorter.sort()
                })
                .try_flatten(),
            )))
        }
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

    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::collect;
    use crate::expressions::col;
    use crate::memory::MemoryExec;
    use crate::test;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};

    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use datafusion_common::cast::as_primitive_array;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeConfig;

    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::Literal;
    use futures::FutureExt;

    #[tokio::test]
    async fn test_in_mem_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let partitions = 4;
        let csv = test::scan_partitioned(partitions);
        let schema = csv.schema();

        let sort_exec = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("i", &schema)?,
                options: SortOptions::default(),
            }],
            Arc::new(CoalescePartitionsExec::new(csv)),
        ));

        let result = collect(sort_exec, Arc::clone(&task_ctx)).await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 400);

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_spill() -> Result<()> {
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

        let sort_exec = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("i", &schema)?,
                options: SortOptions::default(),
            }],
            Arc::new(CoalescePartitionsExec::new(input)),
        ));

        let result = collect(
            Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>,
            Arc::clone(&task_ctx),
        )
        .await?;

        assert_eq!(result.len(), 2);

        // Now, validate metrics
        let metrics = sort_exec.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 10000);
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.spill_count().unwrap(), 4);
        assert_eq!(metrics.spilled_bytes().unwrap(), 38784);
        assert_eq!(metrics.spilled_rows().unwrap(), 9600);

        let columns = result[0].columns();

        let i = as_primitive_array::<Int32Type>(&columns[0])?;
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
    async fn test_sort_fetch_memory_calculation() -> Result<()> {
        // This test mirrors down the size from the example above.
        let avg_batch_size = 400;
        let partitions = 4;

        // A tuple of (fetch, expect_spillage)
        let test_options = vec![
            // Since we don't have a limit (and the memory is less than the total size of
            // all the batches we are processing, we expect it to spill.
            (None, true),
            // When we have a limit however, the buffered size of batches should fit in memory
            // since it is much lower than the total size of the input batch.
            (Some(1), false),
        ];

        for (fetch, expect_spillage) in test_options {
            let session_config = SessionConfig::new();
            let sort_spill_reservation_bytes = session_config
                .options()
                .execution
                .sort_spill_reservation_bytes;

            let rt_config = RuntimeConfig::new().with_memory_limit(
                sort_spill_reservation_bytes + avg_batch_size * (partitions - 1),
                1.0,
            );
            let runtime = Arc::new(RuntimeEnv::new(rt_config)?);
            let task_ctx = Arc::new(
                TaskContext::default()
                    .with_runtime(runtime)
                    .with_session_config(session_config),
            );

            let csv = test::scan_partitioned(partitions);
            let schema = csv.schema();

            let sort_exec = Arc::new(
                SortExec::new(
                    vec![PhysicalSortExpr {
                        expr: col("i", &schema)?,
                        options: SortOptions::default(),
                    }],
                    Arc::new(CoalescePartitionsExec::new(csv)),
                )
                .with_fetch(fetch),
            );

            let result = collect(
                Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>,
                Arc::clone(&task_ctx),
            )
            .await?;
            assert_eq!(result.len(), 1);

            let metrics = sort_exec.metrics().unwrap();
            let did_it_spill = metrics.spill_count().unwrap_or(0) > 0;
            assert_eq!(did_it_spill, expect_spillage, "with fetch: {fetch:?}");
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
            Arc::new(vec![3, 2, 1].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![data]).unwrap();
        let input = Arc::new(
            MemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None).unwrap(),
        );

        let sort_exec = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }],
            input,
        ));

        let result: Vec<RecordBatch> = collect(sort_exec, task_ctx).await?;

        let expected_data: ArrayRef =
            Arc::new(vec![1, 2, 3].into_iter().map(Some).collect::<UInt64Array>());
        let expected_batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![expected_data]).unwrap();

        // Data is correct
        assert_eq!(&vec![expected_batch], &result);

        // explicitlty ensure the metadata is present
        assert_eq!(result[0].schema().fields()[0].metadata(), &field_metadata);
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_mixed_types() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(3)]),
                    Some(vec![Some(1)]),
                    Some(vec![Some(6), None]),
                    Some(vec![Some(5)]),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(SortExec::new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                },
            ],
            Arc::new(MemoryExec::try_new(
                &[vec![batch]],
                Arc::clone(&schema),
                None,
            )?),
        ));

        assert_eq!(DataType::Int32, *sort_exec.schema().field(0).data_type());
        assert_eq!(
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            *sort_exec.schema().field(1).data_type()
        );

        let result: Vec<RecordBatch> =
            collect(Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>, task_ctx).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert_eq!(result.len(), 1);

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(1), Some(2), Some(2)])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    Some(vec![Some(6), None]),
                    Some(vec![Some(5)]),
                    Some(vec![Some(3)]),
                ])),
            ],
        )?;

        assert_eq!(expected, result[0]);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(f32::NAN),
                    None,
                    None,
                    Some(f32::NAN),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(200.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(SortExec::new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                },
            ],
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?),
        ));

        assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
        assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

        let result: Vec<RecordBatch> =
            collect(Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>, task_ctx).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());

        let a = as_primitive_array::<Float32Type>(&columns[0])?;
        let b = as_primitive_array::<Float64Type>(&columns[1])?;

        // convert result to strings to allow comparing to expected result containing NaN
        let result: Vec<(Option<String>, Option<String>)> = (0..result[0].num_rows())
            .map(|i| {
                let aval = if a.is_valid(i) {
                    Some(a.value(i).to_string())
                } else {
                    None
                };
                let bval = if b.is_valid(i) {
                    Some(b.value(i).to_string())
                } else {
                    None
                };
                (aval, bval)
            })
            .collect();

        let expected: Vec<(Option<String>, Option<String>)> = vec![
            (None, Some("10".to_owned())),
            (None, Some("20".to_owned())),
            (Some("NaN".to_owned()), Some("100".to_owned())),
            (Some("NaN".to_owned()), Some("200".to_owned())),
            (Some("3".to_owned()), Some("NaN".to_owned())),
            (Some("2".to_owned()), None),
            (Some("1".to_owned()), Some("NaN".to_owned())),
            (Some("1".to_owned()), None),
        ];

        assert_eq!(expected, result);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
        ));

        let fut = collect(sort_exec, Arc::clone(&task_ctx));
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

    #[test]
    fn test_empty_sort_batch() {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let batch =
            RecordBatch::try_new_with_options(Arc::clone(&schema), vec![], &options)
                .unwrap();

        let expressions = vec![PhysicalSortExpr {
            expr: Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            options: SortOptions::default(),
        }];

        let result = sort_batch(&batch, &expressions, None).unwrap();
        assert_eq!(result.num_rows(), 1);
    }
}
