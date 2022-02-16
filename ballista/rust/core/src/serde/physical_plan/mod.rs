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

use crate::error::BallistaError;
use crate::execution_plans::{
    OutputLocation, ShuffleReaderExec, ShuffleStreamReaderExec, ShuffleWriterExec,
    UnresolvedShuffleExec,
};
use crate::serde::physical_plan::from_proto::parse_protobuf_hash_partitioning;
use crate::serde::protobuf::physical_expr_node::ExprType;
use crate::serde::protobuf::physical_plan_node::PhysicalPlanType;
use crate::serde::protobuf::repartition_exec_node::PartitionMethod;
use crate::serde::protobuf::ShuffleReaderPartition;
use crate::serde::protobuf::{PhysicalExtensionNode, PhysicalPlanNode};
use crate::serde::scheduler::{ExecutorMeta, PartitionLocation};
use crate::serde::{
    byte_to_string, proto_error, protobuf, str_to_byte, AsExecutionPlan,
    PhysicalExtensionCodec,
};
use crate::{convert_box_required, convert_required, into_physical_plan, into_required};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::object_store::local::LocalFileSystem;
use datafusion::datasource::PartitionedFile;
use datafusion::logical_plan::window_frames::WindowFrame;
use datafusion::physical_plan::aggregates::create_aggregate_expr;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::cross_join::CrossJoinExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::file_format::{
    AvroExec, CsvExec, FileScanConfig, ParquetExec,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
use datafusion::physical_plan::hash_join::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{create_window_expr, WindowAggExec};
use datafusion::physical_plan::{
    AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, WindowExpr,
};
use datafusion::prelude::ExecutionContext;
use prost::bytes::BufMut;
use prost::Message;
use std::convert::TryInto;
use std::sync::Arc;

pub mod from_proto;
pub mod to_proto;

impl AsExecutionPlan for PhysicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self, BallistaError>
    where
        Self: Sized,
    {
        PhysicalPlanNode::decode(buf).map_err(|e| {
            BallistaError::Internal(format!("failed to decode physical plan: {:?}", e))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), BallistaError>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            BallistaError::Internal(format!("failed to encode physical plan: {:?}", e))
        })
    }

    fn try_into_physical_plan(
        &self,
        ctx: &ExecutionContext,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>, BallistaError> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(projection.input, ctx, extension_codec)?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| Ok((expr.try_into()?, name.to_string())))
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, BallistaError>>(
                    )?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(filter.input, ctx, extension_codec)?;
                let predicate = filter
                    .expr
                    .as_ref()
                    .ok_or_else(|| {
                        BallistaError::General(
                            "filter (FilterExecNode) in PhysicalPlanNode is missing."
                                .to_owned(),
                        )
                    })?
                    .try_into()?;
                Ok(Arc::new(FilterExec::try_new(predicate, input)?))
            }
            PhysicalPlanType::CsvScan(scan) => Ok(Arc::new(CsvExec::new(
                decode_scan_config(scan.base_conf.as_ref().unwrap(), ctx)?,
                scan.has_header,
                str_to_byte(&scan.delimiter)?,
            ))),
            PhysicalPlanType::ParquetScan(scan) => {
                Ok(Arc::new(ParquetExec::new(
                    decode_scan_config(scan.base_conf.as_ref().unwrap(), ctx)?,
                    // TODO predicate should be de-serialized
                    None,
                )))
            }
            PhysicalPlanType::AvroScan(scan) => Ok(Arc::new(AvroExec::new(
                decode_scan_config(scan.base_conf.as_ref().unwrap(), ctx)?,
            ))),
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(coalesce_batches.input, ctx, extension_codec)?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(merge.input, ctx, extension_codec)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(repart.input, ctx, extension_codec)?;
                match repart.partition_method {
                    Some(PartitionMethod::Hash(ref hash_part)) => {
                        let expr = hash_part
                            .hash_expr
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::Hash(
                                expr,
                                hash_part.partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::RoundRobin(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::RoundRobinBatch(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    Some(PartitionMethod::Unknown(partition_count)) => {
                        Ok(Arc::new(RepartitionExec::try_new(
                            input,
                            Partitioning::UnknownPartitioning(
                                partition_count.try_into().unwrap(),
                            ),
                        )?))
                    }
                    _ => Err(BallistaError::General(
                        "Invalid partitioning scheme".to_owned(),
                    )),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(limit.input, ctx, extension_codec)?;
                Ok(Arc::new(GlobalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(limit.input, ctx, extension_codec)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.limit as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(window_agg.input, ctx, extension_codec)?;
                let input_schema = window_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        BallistaError::General(
                            "input_schema in WindowAggrNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let physical_schema: SchemaRef =
                    SchemaRef::new((&input_schema).try_into()?);

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = window_agg
                    .window_expr
                    .iter()
                    .zip(window_agg.window_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty window physical expression")
                        })?;

                        match expr_type {
                            ExprType::WindowExpr(window_node) => Ok(create_window_expr(
                                &convert_required!(window_node.window_function)?,
                                name.to_owned(),
                                &[convert_box_required!(window_node.expr)?],
                                &[],
                                &[],
                                Some(WindowFrame::default()),
                                &physical_schema,
                            )?),
                            _ => Err(BallistaError::General(
                                "Invalid expression for WindowAggrExec".to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(WindowAggExec::try_new(
                    physical_window_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
            }
            PhysicalPlanType::HashAggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(hash_agg.input, ctx, extension_codec)?;
                let mode = protobuf::AggregateMode::from_i32(hash_agg.mode).ok_or_else(|| {
                    proto_error(format!(
                        "Received a HashAggregateNode message with unknown AggregateMode {}",
                        hash_agg.mode
                    ))
                })?;
                let agg_mode: AggregateMode = match mode {
                    protobuf::AggregateMode::Partial => AggregateMode::Partial,
                    protobuf::AggregateMode::Final => AggregateMode::Final,
                    protobuf::AggregateMode::FinalPartitioned => {
                        AggregateMode::FinalPartitioned
                    }
                };
                let group = hash_agg
                    .group_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        expr.try_into().map(|expr| (expr, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let input_schema = hash_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        BallistaError::General(
                            "input_schema in HashAggregateNode is missing.".to_owned(),
                        )
                    })?
                    .clone();
                let physical_schema: SchemaRef =
                    SchemaRef::new((&input_schema).try_into()?);

                let physical_aggr_expr: Vec<Arc<dyn AggregateExpr>> = hash_agg
                    .aggr_expr
                    .iter()
                    .zip(hash_agg.aggr_expr_name.iter())
                    .map(|(expr, name)| {
                        let expr_type = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error("Unexpected empty aggregate physical expression")
                        })?;

                        match expr_type {
                            ExprType::AggregateExpr(agg_node) => {
                                let aggr_function =
                                    protobuf::AggregateFunction::from_i32(
                                        agg_node.aggr_function,
                                    )
                                        .ok_or_else(
                                            || {
                                                proto_error(format!(
                                                    "Received an unknown aggregate function: {}",
                                                    agg_node.aggr_function
                                                ))
                                            },
                                        )?;

                                Ok(create_aggregate_expr(
                                    &aggr_function.into(),
                                    false,
                                    &[convert_box_required!(agg_node.expr)?],
                                    &physical_schema,
                                    name.to_string(),
                                )?)
                            }
                            _ => Err(BallistaError::General(
                                "Invalid aggregate  expression for HashAggregateExec"
                                    .to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(HashAggregateExec::try_new(
                    agg_mode,
                    group,
                    physical_aggr_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(hashjoin.left, ctx, extension_codec)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(hashjoin.right, ctx, extension_codec)?;
                let on: Vec<(Column, Column)> = hashjoin
                    .on
                    .iter()
                    .map(|col| {
                        let left = into_required!(col.left)?;
                        let right = into_required!(col.right)?;
                        Ok((left, right))
                    })
                    .collect::<Result<_, BallistaError>>()?;
                let join_type = protobuf::JoinType::from_i32(hashjoin.join_type)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown JoinType {}",
                            hashjoin.join_type
                        ))
                    })?;

                let partition_mode =
                    protobuf::PartitionMode::from_i32(hashjoin.partition_mode)
                        .ok_or_else(|| {
                            proto_error(format!(
                        "Received a HashJoinNode message with unknown PartitionMode {}",
                        hashjoin.partition_mode
                    ))
                        })?;
                let partition_mode = match partition_mode {
                    protobuf::PartitionMode::CollectLeft => PartitionMode::CollectLeft,
                    protobuf::PartitionMode::Partitioned => PartitionMode::Partitioned,
                };
                Ok(Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    on,
                    &join_type.into(),
                    partition_mode,
                    &hashjoin.null_equals_null,
                )?))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(crossjoin.left, ctx, extension_codec)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(crossjoin.right, ctx, extension_codec)?;
                Ok(Arc::new(CrossJoinExec::try_new(left, right)?))
            }
            PhysicalPlanType::ShuffleWriter(shuffle_writer) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(shuffle_writer.input, ctx, extension_codec)?;

                let output_partitioning = parse_protobuf_hash_partitioning(
                    shuffle_writer.output_partitioning.as_ref(),
                )?;
                if !shuffle_writer.push_shuffle {
                    Ok(Arc::new(ShuffleWriterExec::try_new_pull_shuffle(
                        shuffle_writer.job_id.clone(),
                        shuffle_writer.stage_id as usize,
                        input,
                        "".to_string(), // this is intentional but hacky - the executor will fill this in
                        output_partitioning,
                    )?))
                } else {
                    let _execs: Vec<ExecutorMeta> = shuffle_writer
                        .execs
                        .to_owned()
                        .into_iter()
                        .map(|e| e.into())
                        .collect();
                    Ok(Arc::new(ShuffleWriterExec::try_new_push_shuffle(
                        shuffle_writer.job_id.clone(),
                        shuffle_writer.stage_id as usize,
                        input,
                        _execs,
                        output_partitioning,
                    )?))
                }
            }
            PhysicalPlanType::ShuffleStreamReader(shuffle_stream_reader) => {
                let schema = Arc::new(convert_required!(shuffle_stream_reader.schema)?);
                let shuffle_reader = ShuffleStreamReaderExec::new(
                    shuffle_stream_reader.stage_id as usize,
                    schema,
                    shuffle_stream_reader.partition_count as usize,
                );
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::ShuffleReader(shuffle_reader) => {
                let schema = Arc::new(convert_required!(shuffle_reader.schema)?);
                let partition_location: Vec<Vec<PartitionLocation>> = shuffle_reader
                    .partition
                    .iter()
                    .map(|p| {
                        p.location
                            .iter()
                            .map(|l| l.clone().try_into())
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, BallistaError>>()?;
                let shuffle_reader =
                    ShuffleReaderExec::try_new(partition_location, schema)?;
                Ok(Arc::new(shuffle_reader))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(sort.input, ctx, extension_codec)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {:?}",
                                self
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {:?}",
                                        self
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: expr.try_into()?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(BallistaError::General(format!(
                                "physical_plan::from_proto() {:?}",
                                self
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(SortExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Unresolved(unresolved_shuffle) => {
                let schema = Arc::new(convert_required!(unresolved_shuffle.schema)?);
                Ok(Arc::new(UnresolvedShuffleExec {
                    stage_id: unresolved_shuffle.stage_id as usize,
                    schema,
                    input_partition_count: unresolved_shuffle.input_partition_count
                        as usize,
                    output_partition_count: unresolved_shuffle.output_partition_count
                        as usize,
                }))
            }
            PhysicalPlanType::Extension(extension) => {
                let inputs: Vec<Arc<dyn ExecutionPlan>> = extension
                    .inputs
                    .iter()
                    .map(|i| i.try_into_physical_plan(ctx, extension_codec))
                    .collect::<Result<_, BallistaError>>()?;

                let extension_node =
                    extension_codec.try_decode(extension.node.as_slice(), &inputs)?;

                Ok(extension_node)
            }
        }
    }

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self, BallistaError>
    where
        Self: Sized,
    {
        let plan_clone = plan.clone();
        let plan = plan.as_any();

        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into())
                .collect::<Result<Vec<_>, BallistaError>>()?;
            let expr_name = exec.expr().iter().map(|expr| expr.1.clone()).collect();
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                    protobuf::ProjectionExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        expr_name,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Filter(Box::new(
                    protobuf::FilterExecNode {
                        input: Some(Box::new(input)),
                        expr: Some(exec.predicate().clone().try_into()?),
                    },
                ))),
            })
        } else if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                limit.input().to_owned(),
                extension_codec,
            )?;

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::GlobalLimit(Box::new(
                    protobuf::GlobalLimitExecNode {
                        input: Some(Box::new(input)),
                        limit: limit.limit() as u32,
                    },
                ))),
            })
        } else if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                limit.input().to_owned(),
                extension_codec,
            )?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::LocalLimit(Box::new(
                    protobuf::LocalLimitExecNode {
                        input: Some(Box::new(input)),
                        limit: limit.limit() as u32,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<HashJoinExec>() {
            let left = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.left().to_owned(),
                extension_codec,
            )?;
            let right = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.right().to_owned(),
                extension_codec,
            )?;
            let on: Vec<protobuf::JoinOn> = exec
                .on()
                .iter()
                .map(|tuple| protobuf::JoinOn {
                    left: Some(protobuf::PhysicalColumn {
                        name: tuple.0.name().to_string(),
                        index: tuple.0.index() as u32,
                    }),
                    right: Some(protobuf::PhysicalColumn {
                        name: tuple.1.name().to_string(),
                        index: tuple.1.index() as u32,
                    }),
                })
                .collect();
            let join_type: protobuf::JoinType = exec.join_type().to_owned().into();

            let partition_mode = match exec.partition_mode() {
                PartitionMode::CollectLeft => protobuf::PartitionMode::CollectLeft,
                PartitionMode::Partitioned => protobuf::PartitionMode::Partitioned,
            };

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashJoin(Box::new(
                    protobuf::HashJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        join_type: join_type.into(),
                        partition_mode: partition_mode.into(),
                        null_equals_null: *exec.null_equals_null(),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<CrossJoinExec>() {
            let left = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.left().to_owned(),
                extension_codec,
            )?;
            let right = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.right().to_owned(),
                extension_codec,
            )?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CrossJoin(Box::new(
                    protobuf::CrossJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<HashAggregateExec>() {
            let groups = exec
                .group_expr()
                .iter()
                .map(|expr| expr.0.to_owned().try_into())
                .collect::<Result<Vec<_>, BallistaError>>()?;
            let group_names = exec
                .group_expr()
                .iter()
                .map(|expr| expr.1.to_owned())
                .collect();
            let agg = exec
                .aggr_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>, BallistaError>>()?;
            let agg_names = exec
                .aggr_expr()
                .iter()
                .map(|expr| match expr.field() {
                    Ok(field) => Ok(field.name().clone()),
                    Err(e) => Err(BallistaError::DataFusionError(e)),
                })
                .collect::<Result<_, BallistaError>>()?;

            let agg_mode = match exec.mode() {
                AggregateMode::Partial => protobuf::AggregateMode::Partial,
                AggregateMode::Final => protobuf::AggregateMode::Final,
                AggregateMode::FinalPartitioned => {
                    protobuf::AggregateMode::FinalPartitioned
                }
            };
            let input_schema = exec.input_schema();
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashAggregate(Box::new(
                    protobuf::HashAggregateExecNode {
                        group_expr: groups,
                        group_expr_name: group_names,
                        aggr_expr: agg,
                        aggr_expr_name: agg_names,
                        mode: agg_mode as i32,
                        input: Some(Box::new(input)),
                        input_schema: Some(input_schema.as_ref().into()),
                    },
                ))),
            })
        } else if let Some(empty) = plan.downcast_ref::<EmptyExec>() {
            let schema = empty.schema().as_ref().into();
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Empty(
                    protobuf::EmptyExecNode {
                        produce_one_row: empty.produce_one_row(),
                        schema: Some(schema),
                    },
                )),
            })
        } else if let Some(coalesce_batches) = plan.downcast_ref::<CoalesceBatchesExec>()
        {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                coalesce_batches.input().to_owned(),
                extension_codec,
            )?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CoalesceBatches(Box::new(
                    protobuf::CoalesceBatchesExecNode {
                        input: Some(Box::new(input)),
                        target_batch_size: coalesce_batches.target_batch_size() as u32,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<CsvExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CsvScan(
                    protobuf::CsvScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        has_header: exec.has_header(),
                        delimiter: byte_to_string(exec.delimiter())?,
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ParquetExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(
                    protobuf::ParquetScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        // TODO serialize predicates
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<AvroExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::AvroScan(
                    protobuf::AvroScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ShuffleStreamReaderExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleStreamReader(
                    protobuf::ShuffleStreamReaderExecNode {
                        stage_id: exec.stage_id as u32,
                        partition_count: exec.partition_count as u64,
                        schema: Some(exec.schema().as_ref().into()),
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ShuffleReaderExec>() {
            let mut partition = vec![];
            for location in &exec.partition {
                partition.push(protobuf::ShuffleReaderPartition {
                    location: location
                        .iter()
                        .map(|l| l.clone().try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ShuffleReader(
                    protobuf::ShuffleReaderExecNode {
                        partition,
                        schema: Some(exec.schema().as_ref().into()),
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<CoalescePartitionsExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Merge(Box::new(
                    protobuf::CoalescePartitionsExecNode {
                        input: Some(Box::new(input)),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<RepartitionExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;

            let pb_partition_method = match exec.partitioning() {
                Partitioning::Hash(exprs, partition_count) => {
                    PartitionMethod::Hash(protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, BallistaError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                Partitioning::RoundRobinBatch(partition_count) => {
                    PartitionMethod::RoundRobin(*partition_count as u64)
                }
                Partitioning::UnknownPartitioning(partition_count) => {
                    PartitionMethod::Unknown(*partition_count as u64)
                }
            };

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Repartition(Box::new(
                    protobuf::RepartitionExecNode {
                        input: Some(Box::new(input)),
                        partition_method: Some(pb_partition_method),
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<SortExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| {
                    let sort_expr = Box::new(protobuf::PhysicalSortExprNode {
                        expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                        asc: !expr.options.descending,
                        nulls_first: expr.options.nulls_first,
                    });
                    Ok(protobuf::PhysicalExprNode {
                        expr_type: Some(protobuf::physical_expr_node::ExprType::Sort(
                            sort_expr,
                        )),
                    })
                })
                .collect::<Result<Vec<_>, BallistaError>>()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Sort(Box::new(
                    protobuf::SortExecNode {
                        input: Some(Box::new(input)),
                        expr,
                    },
                ))),
            })
        } else if let Some(exec) = plan.downcast_ref::<ShuffleWriterExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.children()[0].to_owned(),
                extension_codec,
            )?;
            // note that we use shuffle_output_partitioning() rather than output_partitioning()
            // to get the true output partitioning
            let output_partitioning = match exec.shuffle_output_partitioning() {
                Some(Partitioning::Hash(exprs, partition_count)) => {
                    Some(protobuf::PhysicalHashRepartition {
                        hash_expr: exprs
                            .iter()
                            .map(|expr| expr.clone().try_into())
                            .collect::<Result<Vec<_>, BallistaError>>()?,
                        partition_count: *partition_count as u64,
                    })
                }
                None => None,
                other => {
                    return Err(BallistaError::General(format!(
                        "physical_plan::to_proto() invalid partitioning for ShuffleWriterExec: {:?}",
                        other
                    )))
                }
            };
            match &exec.output_loc {
                OutputLocation::LocalDir(_) => Ok(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::ShuffleWriter(Box::new(
                        protobuf::ShuffleWriterExecNode {
                            job_id: exec.job_id().to_string(),
                            stage_id: exec.stage_id() as u32,
                            input: Some(Box::new(input)),
                            output_partitioning,
                            push_shuffle: false,
                            execs: Vec::new(),
                        },
                    ))),
                }),
                OutputLocation::Executors(executors) => {
                    let _execs: Vec<protobuf::ExecutorMetadata> =
                        executors.to_owned().into_iter().map(|e| e.into()).collect();
                    Ok(protobuf::PhysicalPlanNode {
                        physical_plan_type: Some(PhysicalPlanType::ShuffleWriter(
                            Box::new(protobuf::ShuffleWriterExecNode {
                                job_id: exec.job_id().to_string(),
                                stage_id: exec.stage_id() as u32,
                                input: Some(Box::new(input)),
                                output_partitioning,
                                push_shuffle: true,
                                execs: _execs,
                            }),
                        )),
                    })
                }
            }
        } else if let Some(exec) = plan.downcast_ref::<UnresolvedShuffleExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Unresolved(
                    protobuf::UnresolvedShuffleExecNode {
                        stage_id: exec.stage_id as u32,
                        schema: Some(exec.schema().as_ref().into()),
                        input_partition_count: exec.input_partition_count as u32,
                        output_partition_count: exec.output_partition_count as u32,
                    },
                )),
            })
        } else {
            let mut buf: Vec<u8> = vec![];
            extension_codec.try_encode(plan_clone.clone(), &mut buf)?;

            let inputs: Vec<PhysicalPlanNode> = plan_clone
                .children()
                .into_iter()
                .map(|i| PhysicalPlanNode::try_from_physical_plan(i, extension_codec))
                .collect::<Result<_, BallistaError>>()?;

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Extension(
                    PhysicalExtensionNode { node: buf, inputs },
                )),
            })
        }
    }
}

fn decode_scan_config(
    proto: &protobuf::FileScanExecConf,
    ctx: &ExecutionContext,
) -> Result<FileScanConfig, BallistaError> {
    let schema = Arc::new(convert_required!(proto.schema)?);
    let projection = proto
        .projection
        .iter()
        .map(|i| *i as usize)
        .collect::<Vec<_>>();
    let projection = if projection.is_empty() {
        None
    } else {
        Some(projection)
    };
    let statistics = convert_required!(proto.statistics)?;

    let file_groups: Vec<Vec<PartitionedFile>> = proto
        .file_groups
        .iter()
        .map(|f| f.try_into())
        .collect::<Result<Vec<_>, _>>()?;

    let object_store = if let Some(file) = file_groups.get(0).and_then(|h| h.get(0)) {
        ctx.object_store(file.file_meta.path())?.0
    } else {
        Arc::new(LocalFileSystem {})
    };

    Ok(FileScanConfig {
        object_store,
        file_schema: schema,
        file_groups,
        statistics,
        projection,
        limit: proto.limit.as_ref().map(|sl| sl.limit as usize),
        table_partition_cols: vec![],
    })
}

#[macro_export]
macro_rules! into_physical_plan {
    ($PB:expr, $CTX:expr, $CODEC:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into_physical_plan(&$CTX, $CODEC)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[cfg(test)]
mod roundtrip_tests {
    use std::{convert::TryInto, sync::Arc};

    use crate::serde::{AsExecutionPlan, BallistaCodec};
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::ExecutionContext;
    use datafusion::{
        arrow::{
            compute::kernels::sort::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        logical_plan::{JoinType, Operator},
        physical_plan::{
            empty::EmptyExec,
            expressions::{binary, col, lit, InListExpr, NotExpr},
            expressions::{Avg, Column, PhysicalSortExpr},
            filter::FilterExec,
            hash_aggregate::{AggregateMode, HashAggregateExec},
            hash_join::{HashJoinExec, PartitionMode},
            limit::{GlobalLimitExec, LocalLimitExec},
            AggregateExpr, ColumnarValue, Distribution, ExecutionPlan, Partitioning,
            PhysicalExpr,
        },
        scalar::ScalarValue,
    };

    use super::super::super::error::Result;
    use super::super::protobuf;
    use crate::execution_plans::{OutputLocation, ShuffleWriterExec};
    use crate::serde::protobuf::{LogicalPlanNode, PhysicalPlanNode};
    use crate::serde::scheduler::ExecutorMeta;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let ctx = ExecutionContext::new();
        let codec: BallistaCodec<LogicalPlanNode, PhysicalPlanNode> =
            BallistaCodec::default();
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec_plan.clone(),
                codec.physical_extension_codec(),
            )
            .expect("to proto");
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, codec.physical_extension_codec())
            .expect("from proto");
        assert_eq!(
            format!("{:?}", exec_plan),
            format!("{:?}", result_exec_plan)
        );
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_local_limit() -> Result<()> {
        roundtrip_test(Arc::new(LocalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_global_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_hash_join() -> Result<()> {
        let field_a = Field::new("col", DataType::Int64, false);
        let schema_left = Schema::new(vec![field_a.clone()]);
        let schema_right = Schema::new(vec![field_a]);
        let on = vec![(
            Column::new("col", schema_left.index_of("col")?),
            Column::new("col", schema_right.index_of("col")?),
        )];

        let schema_left = Arc::new(schema_left);
        let schema_right = Arc::new(schema_right);
        for join_type in &[
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Anti,
            JoinType::Semi,
        ] {
            for partition_mode in
                &[PartitionMode::Partitioned, PartitionMode::CollectLeft]
            {
                roundtrip_test(Arc::new(HashJoinExec::try_new(
                    Arc::new(EmptyExec::new(false, schema_left.clone())),
                    Arc::new(EmptyExec::new(false, schema_right.clone())),
                    on.clone(),
                    join_type,
                    *partition_mode,
                    &false,
                )?))?;
            }
        }
        Ok(())
    }

    #[test]
    fn rountrip_hash_aggregate() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "unused".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        roundtrip_test(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            groups.clone(),
            aggregates.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            schema,
        )?))
    }

    #[test]
    fn roundtrip_filter_with_not_and_in_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let field_c = Field::new("c", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
        let not = Arc::new(NotExpr::new(col("a", &schema)?));
        let in_list = Arc::new(InListExpr::new(
            col("b", &schema)?,
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            false,
        ));
        let and = binary(not, Operator::And, in_list, &schema)?;
        roundtrip_test(Arc::new(FilterExec::try_new(
            and,
            Arc::new(EmptyExec::new(false, schema.clone())),
        )?))
    }

    #[test]
    fn roundtrip_sort() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
        ];
        roundtrip_test(Arc::new(SortExec::try_new(
            sort_exprs,
            Arc::new(EmptyExec::new(false, schema)),
        )?))
    }

    #[test]
    fn roundtrip_shuffle_writer() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        roundtrip_test(Arc::new(ShuffleWriterExec::try_new_pull_shuffle(
            "job123".to_string(),
            123,
            Arc::new(EmptyExec::new(false, schema)),
            "".to_string(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4)),
        )?))
    }

    #[test]
    fn roundtrip_push_shuffle_writer() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let meta = ExecutorMeta {
            id: "123".to_owned(),
            host: "localhost".to_owned(),
            port: 123,
            grpc_port: 124,
        };
        let output = OutputLocation::Executors(vec![meta]);
        roundtrip_test(Arc::new(ShuffleWriterExec::try_new(
            "job456".to_string(),
            456,
            Arc::new(EmptyExec::new(false, schema)),
            output,
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4)),
        )?))
    }
}
