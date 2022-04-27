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

//! Traits for physical query plan, supporting parallel execution for partitioned relations.

pub use self::metrics::Metric;
use self::metrics::MetricsSet;
use self::{
    coalesce_partitions::CoalescePartitionsExec, display::DisplayableExecutionPlan,
};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::{error::Result, scalar::ScalarValue};

use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;

use async_trait::async_trait;
pub use datafusion_expr::Accumulator;
pub use datafusion_expr::ColumnarValue;
pub use display::DisplayFormatType;
use futures::stream::Stream;
use std::fmt;
use std::fmt::Debug;

use datafusion_common::DataFusionError;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, pin::Pin};

/// Trait for types that stream [arrow::record_batch::RecordBatch]
pub trait RecordBatchStream: Stream<Item = ArrowResult<RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

/// Trait for a stream of record batches.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Physical planner interface
pub use self::planner::PhysicalPlanner;

/// Statistics for a physical plan node
/// Fields are optional and can be inexact because the sources
/// sometimes provide approximate estimates for performance reasons
/// and the transformations output are not always predictable.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Option<usize>,
    /// total bytes of the table rows
    pub total_byte_size: Option<usize>,
    /// Statistics on a column level
    pub column_statistics: Option<Vec<ColumnStatistics>>,
    /// If true, any field that is `Some(..)` is the actual value in the data provided by the operator (it is not
    /// an estimate). Any or all other fields might still be None, in which case no information is known.
    /// if false, any field that is `Some(..)` may contain an inexact estimate and may not be the actual value.
    pub is_exact: bool,
}
/// This table statistics are estimates about column
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Option<usize>,
    /// Maximum value of column
    pub max_value: Option<ScalarValue>,
    /// Minimum value of column
    pub min_value: Option<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Option<usize>,
}

/// `ExecutionPlan` represent nodes in the DataFusion Physical Plan.
///
/// Each `ExecutionPlan` is Partition-aware and is responsible for
/// creating the actual `async` [`SendableRecordBatchStream`]s
/// of [`RecordBatch`] that incrementally compute the operator's
/// output from its input partition.
///
/// [`ExecutionPlan`] can be displayed in an simplified form using the
/// return value from [`displayable`] in addition to the (normally
/// quite verbose) `Debug` output.
#[async_trait]
pub trait ExecutionPlan: Debug + Send + Sync {
    /// Returns the execution plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef;

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning;

    /// If the output of this operator is sorted, returns `Some(keys)`
    /// with the description of how it was sorted.
    ///
    /// For example, Sort, (obviously) produces sorted output as does
    /// SortPreservingMergeStream. Less obviously `Projection`
    /// produces sorted output if its input was sorted as it does not
    /// reorder the input rows,
    ///
    /// It is safe to return `None` here if your operator does not
    /// have any particular output order here
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>;

    /// Specifies the data distribution requirements of all the
    /// children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    /// Returns `true` if this operator relies on its inputs being
    /// produced in a certain order (for example that they are sorted
    /// a particular way) for correctness.
    ///
    /// If `true` is returned, DataFusion will not apply certain
    /// optimizations which might reorder the inputs (such as
    /// repartitioning to increase concurrency).
    ///
    /// The default implementation returns `true`
    ///
    /// WARNING: if you override this default and return `false`, your
    /// operator can not rely on datafusion preserving the input order
    /// as it will likely not.
    fn relies_on_input_order(&self) -> bool {
        true
    }

    /// Returns `false` if this operator's implementation may reorder
    /// rows within or between partitions.
    ///
    /// For example, Projection, Filter, and Limit maintain the order
    /// of inputs -- they may transform values (Projection) or not
    /// produce the same number of rows that went in (Filter and
    /// Limit), but the rows that are produced go in the same way.
    ///
    /// DataFusion uses this metadata to apply certain optimizations
    /// such as automatically repartitioning correctly.
    ///
    /// The default implementation returns `false`
    ///
    /// WARNING: if you override this default, you *MUST* ensure that
    /// the operator's maintains the ordering invariant or else
    /// DataFusion may produce incorrect results.
    fn maintains_input_order(&self) -> bool {
        false
    }

    /// Returns `true` if this operator would benefit from
    /// partitioning its input (and thus from more parallelism). For
    /// operators that do very little work the overhead of extra
    /// parallelism may outweigh any benefits
    ///
    /// The default implementation returns `true` unless this operator
    /// has signalled it requiers a single child input partition.
    fn benefits_from_input_partitioning(&self) -> bool {
        // By default try to maximize parallelism with more CPUs if
        // possible
        !matches!(
            self.required_child_distribution(),
            Distribution::SinglePartition
        )
    }

    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;

    /// Returns a new plan where all children were replaced by new plans.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// creates an iterator
    async fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`].
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Format this `ExecutionPlan` to `f` in the specified type.
    ///
    /// Should not include a newline
    ///
    /// Note this function prints a placeholder by default to preserve
    /// backwards compatibility.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(PlaceHolder)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics;
}

/// Returns a copy of this plan if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
/// Allow the vtable address comparisons for ExecutionPlan Trait Objects，it is harmless even
/// in the case of 'false-native'.
#[allow(clippy::vtable_address_comparisons)]
pub fn with_new_children_if_necessary(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if children.len() != plan.children().len() {
        Err(DataFusionError::Internal(
            "Wrong number of children".to_string(),
        ))
    } else if children.is_empty()
        || children
            .iter()
            .zip(plan.children().iter())
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        plan.with_new_children(children)
    } else {
        Ok(plan)
    }
}

/// Return a [wrapper](DisplayableExecutionPlan) around an
/// [`ExecutionPlan`] which can be displayed in various easier to
/// understand ways.
///
/// ```
/// use datafusion::prelude::*;
/// use datafusion::physical_plan::displayable;
///
/// #[tokio::main]
/// async fn main() {
///   // Hard code target_partitions as it appears in the RepartitionExec output
///   let config = SessionConfig::new()
///       .with_target_partitions(3);
///   let mut ctx = SessionContext::with_config(config);
///
///   // register the a table
///   ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).await.unwrap();
///
///   // create a plan to run a SQL query
///   let plan = ctx
///      .create_logical_plan("SELECT a FROM example WHERE a < 5")
///      .unwrap();
///   let plan = ctx.optimize(&plan).unwrap();
///   let physical_plan = ctx.create_physical_plan(&plan).await.unwrap();
///
///   // Format using display string
///   let displayable_plan = displayable(physical_plan.as_ref());
///   let plan_string = format!("{}", displayable_plan.indent());
///
///   assert_eq!("ProjectionExec: expr=[a@0 as a]\
///              \n  CoalesceBatchesExec: target_batch_size=4096\
///              \n    FilterExec: a@0 < 5\
///              \n      RepartitionExec: partitioning=RoundRobinBatch(3)\
///              \n        CsvExec: files=[tests/example.csv], has_header=true, limit=None, projection=[a]",
///               plan_string.trim());
///
///   let one_line = format!("{}", displayable_plan.one_line());
///   assert_eq!("ProjectionExec: expr=[a@0 as a]", one_line.trim());
/// }
/// ```
///
pub fn displayable(plan: &dyn ExecutionPlan) -> DisplayableExecutionPlan<'_> {
    DisplayableExecutionPlan::new(plan)
}

/// Visit all children of this plan, according to the order defined on `ExecutionPlanVisitor`.
// Note that this would be really nice if it were a method on
// ExecutionPlan, but it can not be because it takes a generic
// parameter and `ExecutionPlan` is a trait
pub fn accept<V: ExecutionPlanVisitor>(
    plan: &dyn ExecutionPlan,
    visitor: &mut V,
) -> std::result::Result<(), V::Error> {
    visitor.pre_visit(plan)?;
    for child in plan.children() {
        visit_execution_plan(child.as_ref(), visitor)?;
    }
    visitor.post_visit(plan)?;
    Ok(())
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `ExecutionPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
////
/// To use, define a struct that implements this trait and then invoke
/// ['accept'].
///
/// For example, for an execution plan that looks like:
///
/// ```text
/// ProjectionExec: #id
///    FilterExec: state = CO
///       CsvExec:
/// ```
///
/// The sequence of visit operations would be:
/// ```text
/// visitor.pre_visit(ProjectionExec)
/// visitor.pre_visit(FilterExec)
/// visitor.pre_visit(CsvExec)
/// visitor.post_visit(CsvExec)
/// visitor.post_visit(FilterExec)
/// visitor.post_visit(ProjectionExec)
/// ```
pub trait ExecutionPlanVisitor {
    /// The type of error returned by this visitor
    type Error;

    /// Invoked on an `ExecutionPlan` plan before any of its child
    /// inputs have been visited. If Ok(true) is returned, the
    /// recursion continues. If Err(..) or Ok(false) are returned, the
    /// recursion stops immediately and the error, if any, is returned
    /// to `accept`
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error>;

    /// Invoked on an `ExecutionPlan` plan *after* all of its child
    /// inputs have been visited. The return value is handled the same
    /// as the return value of `pre_visit`. The provided default
    /// implementation returns `Ok(true)`.
    fn post_visit(
        &mut self,
        _plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Recursively calls `pre_visit` and `post_visit` for this node and
/// all of its children, as described on [`ExecutionPlanVisitor`]
pub fn visit_execution_plan<V: ExecutionPlanVisitor>(
    plan: &dyn ExecutionPlan,
    visitor: &mut V,
) -> std::result::Result<(), V::Error> {
    visitor.pre_visit(plan)?;
    for child in plan.children() {
        visit_execution_plan(child.as_ref(), visitor)?;
    }
    visitor.post_visit(plan)?;
    Ok(())
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let stream = execute_stream(plan, context).await?;
    common::collect(stream).await
}

/// Execute the [ExecutionPlan] and return a single stream of results
pub async fn execute_stream(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    match plan.output_partitioning().partition_count() {
        0 => Ok(Box::pin(EmptyRecordBatchStream::new(plan.schema()))),
        1 => plan.execute(0, context).await,
        _ => {
            // merge into a single partition
            let plan = CoalescePartitionsExec::new(plan.clone());
            // CoalescePartitionsExec must produce a single partition
            assert_eq!(1, plan.output_partitioning().partition_count());
            plan.execute(0, context).await
        }
    }
}

/// Execute the [ExecutionPlan] and collect the results in memory
pub async fn collect_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<Vec<RecordBatch>>> {
    let streams = execute_stream_partitioned(plan, context).await?;
    let mut batches = Vec::with_capacity(streams.len());
    for stream in streams {
        batches.push(common::collect(stream).await?);
    }
    Ok(batches)
}

/// Execute the [ExecutionPlan] and return a vec with one stream per output partition
pub async fn execute_stream_partitioned(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<SendableRecordBatchStream>> {
    let num_partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::with_capacity(num_partitions);
    for i in 0..num_partitions {
        streams.push(plan.execute(i, context.clone()).await?);
    }
    Ok(streams)
}

/// Partitioning schemes supported by operators.
#[derive(Debug, Clone)]
pub enum Partitioning {
    /// Allocate batches using a round-robin algorithm and the specified number of partitions
    RoundRobinBatch(usize),
    /// Allocate rows based on a hash of one of more expressions and the specified number of
    /// partitions
    Hash(Vec<Arc<dyn PhysicalExpr>>, usize),
    /// Unknown partitioning scheme with a known number of partitions
    UnknownPartitioning(usize),
}

impl Partitioning {
    /// Returns the number of partitions in this partitioning scheme
    pub fn partition_count(&self) -> usize {
        use Partitioning::*;
        match self {
            RoundRobinBatch(n) | Hash(_, n) | UnknownPartitioning(n) => *n,
        }
    }
}

/// Distribution schemes
#[derive(Debug, Clone)]
pub enum Distribution {
    /// Unspecified distribution
    UnspecifiedDistribution,
    /// A single partition is required
    SinglePartition,
    /// Requires children to be distributed in such a way that the same
    /// values of the keys end up in the same partition
    HashPartitioned(Vec<Arc<dyn PhysicalExpr>>),
}

pub use datafusion_physical_expr::window::WindowExpr;
pub use datafusion_physical_expr::{AggregateExpr, PhysicalExpr};

/// Applies an optional projection to a [`SchemaRef`], returning the
/// projected schema
///
/// Example:
/// ```
/// use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
/// use datafusion::physical_plan::project_schema;
///
/// // Schema with columns 'a', 'b', and 'c'
/// let schema = SchemaRef::new(Schema::new(vec![
///   Field::new("a", DataType::Int32, true),
///   Field::new("b", DataType::Int64, true),
///   Field::new("c", DataType::Utf8, true),
/// ]));
///
/// // Pick columns 'c' and 'b'
/// let projection = Some(vec![2,1]);
/// let projected_schema = project_schema(
///    &schema,
///    projection.as_ref()
///  ).unwrap();
///
/// let expected_schema = SchemaRef::new(Schema::new(vec![
///   Field::new("c", DataType::Utf8, true),
///   Field::new("b", DataType::Int64, true),
/// ]));
///
/// assert_eq!(projected_schema, expected_schema);
/// ```
pub fn project_schema(
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
) -> Result<SchemaRef> {
    let schema = match projection {
        Some(columns) => Arc::new(schema.project(columns)?),
        None => Arc::clone(schema),
    };
    Ok(schema)
}

pub mod aggregates;
pub mod analyze;
pub mod coalesce_batches;
pub mod coalesce_partitions;
pub mod common;
pub mod cross_join;
pub mod display;
pub mod empty;
pub mod explain;
use crate::execution::context::TaskContext;
pub use datafusion_physical_expr::expressions;

pub mod file_format;
pub mod filter;
pub mod functions;
pub mod hash_aggregate;
pub mod hash_join;
pub mod hash_utils;
pub mod join_utils;
pub mod limit;
pub mod memory;
pub mod metrics;
pub mod planner;
pub mod projection;
pub mod repartition;
pub mod sort_merge_join;
pub mod sorts;
pub mod stream;
pub mod table_fun;
pub mod type_coercion;
pub mod udaf;
pub mod udf;
pub mod udtf;
pub mod union;
pub mod values;
pub mod windows;
