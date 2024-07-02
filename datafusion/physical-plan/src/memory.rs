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

//! Execution plan for reading in-memory batches of data

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::expressions::PhysicalSortExpr;
use super::{
    common, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, project_schema, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};

use futures::Stream;

/// Execution plan for reading in-memory batches of data
pub struct MemoryExec {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    // Sort information: one or more equivalent orderings
    sort_information: Vec<LexOrdering>,
    cache: PlanProperties,
    /// if partition sizes should be displayed
    show_sizes: bool,
}

impl fmt::Debug for MemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "partitions: [...]")?;
        write!(f, "schema: {:?}", self.projected_schema)?;
        write!(f, "projection: {:?}", self.projection)?;
        if let Some(sort_info) = &self.sort_information.first() {
            write!(f, ", output_ordering: {:?}", sort_info)?;
        }
        Ok(())
    }
}

impl DisplayAs for MemoryExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_sizes: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();

                let output_ordering = self
                    .sort_information
                    .first()
                    .map(|output_ordering| {
                        format!(
                            ", output_ordering={}",
                            PhysicalSortExpr::format_list(output_ordering)
                        )
                    })
                    .unwrap_or_default();

                if self.show_sizes {
                    write!(
                        f,
                        "MemoryExec: partitions={}, partition_sizes={partition_sizes:?}{output_ordering}",
                        partition_sizes.len(),
                    )
                } else {
                    write!(f, "MemoryExec: partitions={}", partition_sizes.len(),)
                }
            }
        }
    }
}

impl ExecutionPlan for MemoryExec {
    fn name(&self) -> &'static str {
        "MemoryExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // MemoryExec has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition].clone(),
            Arc::clone(&self.projected_schema),
            self.projection.clone(),
        )?))
    }

    /// We recompute the statistics dynamically from the arrow metadata as it is pretty cheap to do so
    fn statistics(&self) -> Result<Statistics> {
        Ok(common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        ))
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        let cache =
            Self::compute_properties(Arc::clone(&projected_schema), &[], partitions);
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: vec![],
            cache,
            show_sizes: true,
        })
    }

    /// set `show_sizes` to determine whether to display partition sizes
    pub fn with_show_sizes(mut self, show_sizes: bool) -> Self {
        self.show_sizes = show_sizes;
        self
    }

    pub fn partitions(&self) -> &[Vec<RecordBatch>] {
        &self.partitions
    }

    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }

    /// A memory table can be ordered by multiple expressions simultaneously.
    /// [`EquivalenceProperties`] keeps track of expressions that describe the
    /// global ordering of the schema. These columns are not necessarily same; e.g.
    /// ```text
    /// ┌-------┐
    /// | a | b |
    /// |---|---|
    /// | 1 | 9 |
    /// | 2 | 8 |
    /// | 3 | 7 |
    /// | 5 | 5 |
    /// └---┴---┘
    /// ```
    /// where both `a ASC` and `b DESC` can describe the table ordering. With
    /// [`EquivalenceProperties`], we can keep track of these equivalences
    /// and treat `a ASC` and `b DESC` as the same ordering requirement.
    pub fn with_sort_information(mut self, sort_information: Vec<LexOrdering>) -> Self {
        self.sort_information = sort_information;

        // We need to update equivalence properties when updating sort information.
        let eq_properties = EquivalenceProperties::new_with_orderings(
            self.schema(),
            &self.sort_information,
        );
        self.cache = self.cache.with_eq_properties(eq_properties);
        self
    }

    pub fn original_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        partitions: &[Vec<RecordBatch>],
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);
        PlanProperties::new(
            eq_properties,                                       // Equivalence Properties
            Partitioning::UnknownPartitioning(partitions.len()), // Output Partitioning
            ExecutionMode::Bounded,                              // Execution Mode
        )
    }
}

/// Iterator over batches
pub struct MemoryStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Optional memory reservation bound to the data, freed on drop
    reservation: Option<MemoryReservation>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            reservation: None,
            schema,
            projection,
            index: 0,
        })
    }

    /// Set the memory reservation for the data
    pub(super) fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];

            // return just the columns requested
            let batch = match self.projection.as_ref() {
                Some(columns) => batch.project(columns)?,
                None => batch.clone(),
            };

            Some(Ok(batch))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::memory::MemoryExec;
    use crate::ExecutionPlan;

    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::PhysicalSortExpr;

    #[test]
    fn test_memory_order_eq() -> datafusion_common::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let sort1 = vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions::default(),
            },
        ];
        let sort2 = vec![PhysicalSortExpr {
            expr: col("c", &schema)?,
            options: SortOptions::default(),
        }];
        let mut expected_output_order = vec![];
        expected_output_order.extend(sort1.clone());
        expected_output_order.extend(sort2.clone());

        let sort_information = vec![sort1.clone(), sort2.clone()];
        let mem_exec = MemoryExec::try_new(&[vec![]], schema, None)?
            .with_sort_information(sort_information);

        assert_eq!(
            mem_exec.properties().output_ordering().unwrap(),
            expected_output_order
        );
        let eq_properties = mem_exec.properties().equivalence_properties();
        assert!(eq_properties.oeq_class().contains(&sort1));
        assert!(eq_properties.oeq_class().contains(&sort2));
        Ok(())
    }
}
