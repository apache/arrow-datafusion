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

//! TopK: Combination of Sort / LIMIT

use arrow::{
    compute::interleave,
    row::{RowConverter, Rows, SortField},
};
use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    runtime_env::RuntimeEnv,
};
use datafusion_physical_expr::PhysicalSortExpr;
use hashbrown::HashMap;

use crate::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};

use super::metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder};

/// Global TopK
///
/// # Background
///
/// "Top K" is a common query optimization used for queries such as
/// "find the top 3 customers by revenue". The (simplified) SQL for
/// such a query might be:
///
/// ```sql
/// SELECT customer_id, revenue FROM 'sales.csv' ORDER BY revenue DESC limit 3;
/// ```
///
/// The simple plan would be:
///
/// ```sql
/// > explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
/// +--------------+----------------------------------------+
/// | plan_type    | plan                                   |
/// +--------------+----------------------------------------+
/// | logical_plan | Limit: 3                               |
/// |              |   Sort: revenue DESC NULLS FIRST       |
/// |              |     Projection: customer_id, revenue   |
/// |              |       TableScan: sales                 |
/// +--------------+----------------------------------------+
/// ```
///
/// While this plan produces the correct answer, it will fully sorts the
/// input before discarding everything other than the top 3 elements.
///
/// The same answer can be produced by simply keeping track of the top
/// K=3 elements, reducing the total amount of required buffer memory.
///
/// # Structure
///
/// This operator tracks the top K items using a `TopKHeap`.
pub struct TopK {
    /// schema of the output (and the input)
    schema: SchemaRef,
    /// Runtime metrics
    metrics: TopKMetrics,
    /// Reservation
    reservation: MemoryReservation,
    /// The target number of rows for output batches
    batch_size: usize,
    /// sort expressions
    expr: Arc<[PhysicalSortExpr]>,
    /// row converter, for sort keys
    row_converter: RowConverter,
    /// scratch space for converting rows
    scratch_rows: Rows,
    /// stores the top k values and their sort key values, in order
    heap: TopKHeap,
}

impl TopK {
    /// Create a new [`TopK`] that stores the top `k` values, as
    /// defined by the sort expressions in `expr`.
    // TOOD: make a builder or some other nicer API to avoid the
    // clippy warning
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        expr: Vec<PhysicalSortExpr>,
        k: usize,
        batch_size: usize,
        runtime: Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        let reservation = MemoryConsumer::new(format!("TopK[{partition_id}]"))
            .register(&runtime.memory_pool);

        let expr: Arc<[PhysicalSortExpr]> = expr.into();

        let sort_fields: Vec<_> = expr
            .iter()
            .map(|e| {
                Ok(SortField::new_with_options(
                    e.expr.data_type(&schema)?,
                    e.options,
                ))
            })
            .collect::<Result<_>>()?;

        // TODO there is potential to add special cases for single column sort fields
        // to improve performance
        let row_converter = RowConverter::new(sort_fields)?;
        let scratch_rows = row_converter.empty_rows(
            batch_size,
            20 * batch_size, // guestimate 20 bytes per row
        );

        Ok(Self {
            schema: Arc::clone(&schema),
            metrics: TopKMetrics::new(metrics, partition),
            reservation,
            batch_size,
            expr,
            row_converter,
            scratch_rows,
            heap: TopKHeap::new(k, batch_size, schema),
        })
    }

    /// Insert `batch`, remembering if any of its values are among
    /// the top k seen so far.
    pub fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Updates on drop
        let _timer = self.metrics.baseline.elapsed_compute().timer();

        let sort_keys: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|expr| {
                let value = expr.expr.evaluate(&batch)?;
                value.into_array(batch.num_rows())
            })
            .collect::<Result<Vec<_>>>()?;

        // reuse existing `Rows` to avoid reallocations
        let rows = &mut self.scratch_rows;
        rows.clear();
        self.row_converter.append(rows, &sort_keys)?;

        // TODO make this algorithmically better?:
        // Idea: filter out rows >= self.heap.max() early (before passing to `RowConverter`)
        //       this avoids some work and also might be better vectorizable.
        let mut batch_entry = self.heap.register_batch(batch);
        for (index, row) in rows.iter().enumerate() {
            match self.heap.max() {
                // heap has k items, and the new row is greater than the
                // current max in the heap ==> it is not a new topk
                Some(max_row) if row.as_ref() >= max_row.row() => {}
                // don't yet have k items or new item is lower than the currently k low values
                None | Some(_) => {
                    self.heap.add(&mut batch_entry, row, index);
                    self.metrics.row_replacements.add(1);
                }
            }
        }
        self.heap.insert_batch_entry(batch_entry);

        // conserve memory
        self.heap.maybe_compact()?;

        // update memory reservation
        self.reservation.try_resize(self.size())?;
        Ok(())
    }

    /// Returns the top k results broken into `batch_size` [`RecordBatch`]es, consuming the heap
    pub fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation: _,
            batch_size,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            mut heap,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer(); // time updated on drop

        let mut batch = heap.emit()?;
        metrics.baseline.output_rows().add(batch.num_rows());

        // break into record batches as needed
        let mut batches = vec![];
        loop {
            if batch.num_rows() <= batch_size {
                batches.push(Ok(batch));
                break;
            } else {
                batches.push(Ok(batch.slice(0, batch_size)));
                let remaining_length = batch.num_rows() - batch_size;
                batch = batch.slice(batch_size, remaining_length);
            }
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        )))
    }

    /// return the size of memory used by this operator, in bytes
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.row_converter.size()
            + self.scratch_rows.size()
            + self.heap.size()
    }
}

struct TopKMetrics {
    /// metrics
    pub baseline: BaselineMetrics,

    /// count of how many rows were replaced in the heap
    pub row_replacements: Count,
}

impl TopKMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            row_replacements: MetricBuilder::new(metrics)
                .counter("row_replacements", partition),
        }
    }
}

/// This structure keeps at most the *smallest* k items, using the
/// [arrow::row] format for sort keys. While it is called "topK" for
/// values like `1, 2, 3, 4, 5` the "top 3" really means the
/// *smallest* 3 , `1, 2, 3`, not the *largest* 3 `3, 4, 5`.
///
/// Using the `Row` format handles things such as ascending vs
/// descending and nulls first vs nulls last.
struct TopKHeap {
    /// The maximum number of elemenents to store in this heap.
    k: usize,
    /// The target number of rows for output batches
    batch_size: usize,
    /// Storage for up at most `k` items using a BinaryHeap. Reverserd
    /// so that the smallest k so far is on the top
    inner: BinaryHeap<TopKRow>,
    /// Storage the original row values (TopKRow only has the sort key)
    store: RecordBatchStore,
    /// The size of all owned data held by this heap
    owned_bytes: usize,
}

impl TopKHeap {
    fn new(k: usize, batch_size: usize, schema: SchemaRef) -> Self {
        assert!(k > 0);
        Self {
            k,
            batch_size,
            inner: BinaryHeap::new(),
            store: RecordBatchStore::new(schema),
            owned_bytes: 0,
        }
    }

    /// Register a [`RecordBatch`] with the heap, returning the
    /// appropriate entry
    pub fn register_batch(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        self.store.register(batch)
    }

    /// Insert a [`RecordBatchEntry`] created by a previous call to
    /// [`Self::register_batch`] into storage.
    pub fn insert_batch_entry(&mut self, entry: RecordBatchEntry) {
        self.store.insert(entry)
    }

    /// Returns the largest value stored by the heap if there are k
    /// items, otherwise returns None. Remember this structure is
    /// keeping the "smallest" k values
    fn max(&self) -> Option<&TopKRow> {
        if self.inner.len() < self.k {
            None
        } else {
            self.inner.peek()
        }
    }

    /// Adds `row` to this heap. If inserting this new item would
    /// increase the size past `k`, removes the previously smallest
    /// item.
    fn add(
        &mut self,
        batch_entry: &mut RecordBatchEntry,
        row: impl AsRef<[u8]>,
        index: usize,
    ) {
        let batch_id = batch_entry.id;
        batch_entry.uses += 1;

        assert!(self.inner.len() <= self.k);
        let row = row.as_ref();

        // Reuse storage for evicted item if possible
        let new_top_k = if self.inner.len() == self.k {
            let prev_min = self.inner.pop().unwrap();

            // Update batch use
            if prev_min.batch_id == batch_entry.id {
                batch_entry.uses -= 1;
            } else {
                self.store.unuse(prev_min.batch_id);
            }

            // update memory accounting
            self.owned_bytes -= prev_min.owned_size();
            prev_min.with_new_row(row, batch_id, index)
        } else {
            TopKRow::new(row, batch_id, index)
        };

        self.owned_bytes += new_top_k.owned_size();

        // put the new row into the heap
        self.inner.push(new_top_k)
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], resetting the inner heap
    pub fn emit(&mut self) -> Result<RecordBatch> {
        Ok(self.emit_with_state()?.0)
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], and a sorted vec of the
    /// current heap's contents
    pub fn emit_with_state(&mut self) -> Result<(RecordBatch, Vec<TopKRow>)> {
        let schema = Arc::clone(self.store.schema());

        // generate sorted rows
        let topk_rows = std::mem::take(&mut self.inner).into_sorted_vec();

        if self.store.is_empty() {
            return Ok((RecordBatch::new_empty(schema), topk_rows));
        }

        // Indices for each row within its respective RecordBatch
        let indices: Vec<_> = topk_rows
            .iter()
            .enumerate()
            .map(|(i, k)| (i, k.index))
            .collect();

        let num_columns = schema.fields().len();

        // build the output columns one at time, using the
        // `interleave` kernel to pick rows from different arrays
        let output_columns: Vec<_> = (0..num_columns)
            .map(|col| {
                let input_arrays: Vec<_> = topk_rows
                    .iter()
                    .map(|k| {
                        let entry =
                            self.store.get(k.batch_id).expect("invalid stored batch id");
                        entry.batch.column(col) as &dyn Array
                    })
                    .collect();

                // at this point `indices` contains indexes within the
                // rows and `input_arrays` contains a reference to the
                // relevant Array for that index. `interleave` pulls
                // them together into a single new array
                Ok(interleave(&input_arrays, &indices)?)
            })
            .collect::<Result<_>>()?;

        let new_batch = RecordBatch::try_new(schema, output_columns)?;
        Ok((new_batch, topk_rows))
    }

    /// Compact this heap, rewriting all stored batches into a single
    /// input batch
    pub fn maybe_compact(&mut self) -> Result<()> {
        // we compact if the number of "unused" rows in the store is
        // past some pre-defined threshold. Target holding up to
        // around 20 batches, but handle cases of large k where some
        // batches might be partially full
        let max_unused_rows = (20 * self.batch_size) + self.k;
        let unused_rows = self.store.unused_rows();

        // don't compact if the store has one extra batch or
        // unused rows is under the threshold
        if self.store.len() <= 2 || unused_rows < max_unused_rows {
            return Ok(());
        }
        // at first, compact the entire thing always into a new batch
        // (maybe we can get fancier in the future about ignoring
        // batches that have a high usage ratio already

        // Note: new batch is in the same order as inner
        let num_rows = self.inner.len();
        let (new_batch, mut topk_rows) = self.emit_with_state()?;

        // clear all old entires in store (this invalidates all
        // store_ids in `inner`)
        self.store.clear();

        let mut batch_entry = self.register_batch(new_batch);
        batch_entry.uses = num_rows;

        // rewrite all existing entries to use the new batch, and
        // remove old entries. The sortedness and their relative
        // position do not change
        for (i, topk_row) in topk_rows.iter_mut().enumerate() {
            topk_row.batch_id = batch_entry.id;
            topk_row.index = i;
        }
        self.insert_batch_entry(batch_entry);
        // restore the heap
        self.inner = BinaryHeap::from(topk_rows);

        Ok(())
    }

    /// return the size of memory used by this heap, in bytes
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + (self.inner.capacity() * std::mem::size_of::<TopKRow>())
            + self.store.size()
            + self.owned_bytes
    }
}

/// Represents one of the top K rows held in this heap. Orders
/// according to memcmp of row (e.g. the arrow Row format, but could
/// also be primtive values)
///
/// Reuses allocations to minimize runtime overhead of creating new Vecs
#[derive(Debug, PartialEq)]
struct TopKRow {
    /// the value of the sort key for this row. This contains the
    /// bytes that could be stored in `OwnedRow` but uses `Vec<u8>` to
    /// reuse allocations.
    row: Vec<u8>,
    /// the RecordBatch this row came from: an id into a [`RecordBatchStore`]
    batch_id: u32,
    /// the index in this record batch the row came from
    index: usize,
}

impl TopKRow {
    /// Create a new TopKRow with new allocation
    fn new(row: impl AsRef<[u8]>, batch_id: u32, index: usize) -> Self {
        Self {
            row: row.as_ref().to_vec(),
            batch_id,
            index,
        }
    }

    /// Create a new  TopKRow reusing the existing allocation
    fn with_new_row(
        self,
        new_row: impl AsRef<[u8]>,
        batch_id: u32,
        index: usize,
    ) -> Self {
        let Self {
            mut row,
            batch_id: _,
            index: _,
        } = self;
        row.clear();
        row.extend_from_slice(new_row.as_ref());

        Self {
            row,
            batch_id,
            index,
        }
    }

    /// Returns the number of bytes owned by this row in the heap (not
    /// including itself)
    fn owned_size(&self) -> usize {
        self.row.capacity()
    }

    /// Returns a slice to the owned row value
    fn row(&self) -> &[u8] {
        self.row.as_slice()
    }
}

impl Eq for TopKRow {}

impl PartialOrd for TopKRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopKRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

#[derive(Debug)]
struct RecordBatchEntry {
    id: u32,
    batch: RecordBatch,
    // for this batch, how many times has it been used
    uses: usize,
}

/// This structure tracks [`RecordBatch`] by an id so that:
///
/// 1. The baches can be tracked via an id that can be copied cheaply
/// 2. The total memory held by all batches is tracked
#[derive(Debug)]
struct RecordBatchStore {
    /// id generator
    next_id: u32,
    /// storage
    batches: HashMap<u32, RecordBatchEntry>,
    /// total size of all record batches tracked by this store
    batches_size: usize,
    /// schema of the batches
    schema: SchemaRef,
}

impl RecordBatchStore {
    fn new(schema: SchemaRef) -> Self {
        Self {
            next_id: 0,
            batches: HashMap::new(),
            batches_size: 0,
            schema,
        }
    }

    /// Register this batch with the store and assign an ID. No
    /// attempt is made to compare this batch to other batches
    pub fn register(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        let id = self.next_id;
        self.next_id += 1;
        RecordBatchEntry { id, batch, uses: 0 }
    }

    /// Insert a record batch entry into this store, tracking its
    /// memory use, if it has any uses
    pub fn insert(&mut self, entry: RecordBatchEntry) {
        // uses of 0 means that none of the rows in the batch were stored in the topk
        if entry.uses > 0 {
            self.batches_size += entry.batch.get_array_memory_size();
            self.batches.insert(entry.id, entry);
        }
    }

    /// Clear all values in this store, invalidating all previous batch ids
    fn clear(&mut self) {
        self.batches.clear();
        self.batches_size = 0;
    }

    fn get(&self, id: u32) -> Option<&RecordBatchEntry> {
        self.batches.get(&id)
    }

    /// returns the total number of batches stored in this store
    fn len(&self) -> usize {
        self.batches.len()
    }

    /// Returns the total number of rows in batches minus the number
    /// which are in use
    fn unused_rows(&self) -> usize {
        self.batches
            .values()
            .map(|batch_entry| batch_entry.batch.num_rows() - batch_entry.uses)
            .sum()
    }

    /// returns true if the store has nothing stored
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// return the schema of batches stored
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// remove a use from the specified batch id. If the use count
    /// reaches zero the batch entry is removed from the store
    ///
    /// panics if there were no remaining uses of id
    pub fn unuse(&mut self, id: u32) {
        let remove = if let Some(batch_entry) = self.batches.get_mut(&id) {
            batch_entry.uses = batch_entry.uses.checked_sub(1).expect("underflow");
            batch_entry.uses == 0
        } else {
            panic!("No entry for id {id}");
        };

        if remove {
            let old_entry = self.batches.remove(&id).unwrap();
            self.batches_size = self
                .batches_size
                .checked_sub(old_entry.batch.get_array_memory_size())
                .unwrap();
        }
    }

    /// returns the size of memory used by this store, including all
    /// referenced `RecordBatch`es, in bytes
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.batches.capacity()
                * (std::mem::size_of::<u32>() + std::mem::size_of::<RecordBatchEntry>())
            + self.batches_size
    }
}
