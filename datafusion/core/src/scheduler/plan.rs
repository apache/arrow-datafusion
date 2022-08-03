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

use arrow::datatypes::SchemaRef;
use std::sync::Arc;

use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::{ExecutionPlan, Partitioning};

use crate::scheduler::pipeline::{
    execution::ExecutionPipeline, repartition::RepartitionPipeline, Pipeline,
};

/// Identifies the [`Pipeline`] within the [`PipelinePlan`] to route output to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLink {
    /// The index of the [`Pipeline`] in [`PipelinePlan`] to route output to
    pub pipeline: usize,

    /// The child of the [`Pipeline`] to route output to
    pub child: usize,
}

/// Combines a [`Pipeline`] with an [`OutputLink`] identifying where to send its output
#[derive(Debug)]
pub struct RoutablePipeline {
    /// The pipeline that produces data
    pub pipeline: Box<dyn Pipeline>,

    /// Where to send output the output of `pipeline`
    ///
    /// If `None`, the output should be sent to the query output
    pub output: Option<OutputLink>,
}

/// [`PipelinePlan`] is the scheduler's representation of the [`ExecutionPlan`] passed to
/// [`super::Scheduler::schedule`]. It combines the list of [Pipeline`] with the information
/// necessary to route output from one stage to the next
#[derive(Debug)]
pub struct PipelinePlan {
    /// Schema of this plans output
    pub schema: SchemaRef,

    /// Number of output partitions
    pub output_partitions: usize,

    /// Pipelines that comprise this plan
    pub pipelines: Vec<RoutablePipeline>,
}

/// When converting [`ExecutionPlan`] to [`Pipeline`] we may wish to group
/// together multiple operators, [`OperatorGroup`] stores this state
struct OperatorGroup {
    /// Where to route the output of the eventual [`Pipeline`]
    output: Option<OutputLink>,

    /// The [`ExecutionPlan`] from which to start recursing
    root: Arc<dyn ExecutionPlan>,

    /// The number of times to recurse into the [`ExecutionPlan`]'s children
    depth: usize,
}

/// A utility struct to assist converting from [`ExecutionPlan`] to [`PipelinePlan`]
///
/// The [`ExecutionPlan`] is visited in a depth-first fashion, gradually building
/// up the [`RoutablePipeline`] for the [`PipelinePlan`]. As nodes are visited depth-first,
/// a node is visited only after its parent has been.
pub struct PipelinePlanner {
    task_context: Arc<TaskContext>,

    /// The schema of this plan
    schema: SchemaRef,

    /// The number of output partitions of this plan
    output_partitions: usize,

    /// The current list of completed pipelines
    completed: Vec<RoutablePipeline>,

    /// A list of [`ExecutionPlan`] still to visit, along with
    /// where they should route their output
    to_visit: Vec<(Arc<dyn ExecutionPlan>, Option<OutputLink>)>,

    /// Stores one or more operators to combine
    /// together into a single [`ExecutionPipeline`]
    execution_operators: Option<OperatorGroup>,
}

impl PipelinePlanner {
    pub fn new(plan: Arc<dyn ExecutionPlan>, task_context: Arc<TaskContext>) -> Self {
        let schema = plan.schema();
        let output_partitions = plan.output_partitioning().partition_count();
        Self {
            completed: vec![],
            to_visit: vec![(plan, None)],
            task_context,
            execution_operators: None,
            schema,
            output_partitions,
        }
    }

    /// Flush the current group of operators stored in `execution_operators`
    /// into a single [`ExecutionPipeline]
    fn flush_exec(&mut self) -> Result<usize> {
        let group = self.execution_operators.take().unwrap();
        let node_idx = self.completed.len();
        self.completed.push(RoutablePipeline {
            pipeline: Box::new(ExecutionPipeline::new(
                group.root,
                self.task_context.clone(),
                group.depth,
            )?),
            output: group.output,
        });
        Ok(node_idx)
    }

    /// Visit a non-special cased [`ExecutionPlan`]
    fn visit_exec(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        let children = plan.children();

        // Add the operator to the current group of operators to be combined
        // into a single [`ExecutionPipeline`].
        //
        // TODO: More sophisticated policy, just because we can combine them doesn't mean we should
        match self.execution_operators.as_mut() {
            Some(buffer) => {
                assert_eq!(parent, buffer.output, "QueryBuilder out of sync");
                buffer.depth += 1;
            }
            None => {
                self.execution_operators = Some(OperatorGroup {
                    output: parent,
                    root: plan,
                    depth: 0,
                })
            }
        }

        match children.len() {
            1 => {
                // Enqueue the children with the parent of the `OperatorGroup`
                self.to_visit
                    .push((children.into_iter().next().unwrap(), parent))
            }
            _ => {
                // We can only recursively group through nodes with a single child, therefore
                // if this node has multiple children, we now need to flush the buffer and
                // enqueue its children with this new pipeline as its parent
                let node = self.flush_exec()?;
                self.enqueue_children(children, node);
            }
        }

        Ok(())
    }

    /// Add the given list of children to the stack of [`ExecutionPlan`] to visit
    fn enqueue_children(
        &mut self,
        children: Vec<Arc<dyn ExecutionPlan>>,
        parent_node_idx: usize,
    ) {
        for (child_idx, child) in children.into_iter().enumerate() {
            self.to_visit.push((
                child,
                Some(OutputLink {
                    pipeline: parent_node_idx,
                    child: child_idx,
                }),
            ))
        }
    }

    /// Push a new [`RoutablePipeline`] and enqueue its children to be visited
    fn push_pipeline(
        &mut self,
        node: RoutablePipeline,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) {
        let node_idx = self.completed.len();
        self.completed.push(node);
        self.enqueue_children(children, node_idx)
    }

    /// Push a new [`RepartitionPipeline`] first flushing any buffered [`OperatorGroup`]
    fn push_repartition(
        &mut self,
        input: Partitioning,
        output: Partitioning,
        parent: Option<OutputLink>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<()> {
        let parent = match &self.execution_operators {
            Some(buffer) => {
                assert_eq!(buffer.output, parent, "QueryBuilder out of sync");
                Some(OutputLink {
                    pipeline: self.flush_exec()?,
                    child: 0, // Must be the only child
                })
            }
            None => parent,
        };

        let node = Box::new(RepartitionPipeline::try_new(input, output)?);
        self.push_pipeline(
            RoutablePipeline {
                pipeline: node,
                output: parent,
            },
            children,
        );
        Ok(())
    }

    /// Visit an [`ExecutionPlan`] operator and add it to the [`PipelinePlan`] being built
    fn visit_operator(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<OutputLink>,
    ) -> Result<()> {
        if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
            self.push_repartition(
                repartition.input().output_partitioning(),
                repartition.output_partitioning(),
                parent,
                repartition.children(),
            )
        } else if let Some(coalesce) =
            plan.as_any().downcast_ref::<CoalescePartitionsExec>()
        {
            self.push_repartition(
                coalesce.input().output_partitioning(),
                Partitioning::RoundRobinBatch(1),
                parent,
                coalesce.children(),
            )
        } else {
            self.visit_exec(plan, parent)
        }
    }

    /// Build a [`PipelinePlan`] from the [`ExecutionPlan`] provided to [`PipelinePlanner::new`]
    ///
    /// This will group all operators possible into a single [`ExecutionPipeline`], only
    /// creating new pipelines when:
    ///
    /// - encountering an operator with multiple children
    /// - encountering a repartitioning operator
    ///
    /// This latter case is because currently the repartitioning operators in DataFusion are
    /// coupled with the non-scheduler-based parallelism story
    ///
    /// The above logic is liable to change, is considered an implementation detail of the
    /// scheduler, and should not be relied upon by operators
    ///
    pub fn build(mut self) -> Result<PipelinePlan> {
        // We do a depth-first scan of the operator tree, extracting a list of [`QueryNode`]
        while let Some((plan, parent)) = self.to_visit.pop() {
            self.visit_operator(plan, parent)?;
        }

        if self.execution_operators.is_some() {
            self.flush_exec()?;
        }

        Ok(PipelinePlan {
            schema: self.schema,
            output_partitions: self.output_partitions,
            pipelines: self.completed,
        })
    }
}
