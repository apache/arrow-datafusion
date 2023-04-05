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

use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::WindowFrame;
use datafusion::physical_plan::aggregates::{create_aggregate_expr, AggregateMode};
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::file_format::{AvroExec, CsvExec, ParquetExec};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::CrossJoinExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::windows::{create_window_expr, WindowAggExec};
use datafusion::physical_plan::{
    AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, WindowExpr,
};
use datafusion_common::{DataFusionError, Result};
use prost::bytes::BufMut;
use prost::Message;

use crate::common::proto_error;
use crate::common::{csv_delimiter_to_string, str_to_byte};
use crate::physical_plan::from_proto::{
    parse_physical_expr, parse_protobuf_file_scan_config,
};
use crate::protobuf::physical_expr_node::ExprType;
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::protobuf::repartition_exec_node::PartitionMethod;
use crate::protobuf::{self, PhysicalPlanNode};
use crate::{convert_required, into_physical_plan, into_required};

pub mod from_proto;
pub mod to_proto;

impl AsExecutionPlan for PhysicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        protobuf::PhysicalPlanNode::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to decode physical plan: {e:?}"))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to encode physical plan: {e:?}"))
        })
    }

    #[allow(clippy::only_used_in_recursion)]
    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.physical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "physical_plan::from_proto() Unsupported physical plan '{self:?}'"
            ))
        })?;
        match plan {
            PhysicalPlanType::Explain(explain) => Ok(Arc::new(ExplainExec::new(
                Arc::new(explain.schema.as_ref().unwrap().try_into()?),
                explain
                    .stringified_plans
                    .iter()
                    .map(|plan| plan.into())
                    .collect(),
                explain.verbose,
            ))),
            PhysicalPlanType::Projection(projection) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    projection.input,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let exprs = projection
                    .expr
                    .iter()
                    .zip(projection.expr_name.iter())
                    .map(|(expr, name)| {
                        Ok((
                            parse_physical_expr(expr, registry, input.schema().as_ref())?,
                            name.to_string(),
                        ))
                    })
                    .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
            }
            PhysicalPlanType::Filter(filter) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    filter.input,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let predicate = filter
                    .expr
                    .as_ref()
                    .map(|expr| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                    })
                    .transpose()?
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "filter (FilterExecNode) in PhysicalPlanNode is missing."
                                .to_owned(),
                        )
                    })?;
                Ok(Arc::new(FilterExec::try_new(predicate, input)?))
            }
            PhysicalPlanType::CsvScan(scan) => Ok(Arc::new(CsvExec::new(
                parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?,
                scan.has_header,
                str_to_byte(&scan.delimiter)?,
                FileCompressionType::UNCOMPRESSED,
            ))),
            PhysicalPlanType::ParquetScan(scan) => {
                let base_config = parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?;
                let predicate = scan
                    .predicate
                    .as_ref()
                    .map(|expr| {
                        parse_physical_expr(
                            expr,
                            registry,
                            base_config.file_schema.as_ref(),
                        )
                    })
                    .transpose()?;
                Ok(Arc::new(ParquetExec::new(base_config, predicate, None)))
            }
            PhysicalPlanType::AvroScan(scan) => {
                Ok(Arc::new(AvroExec::new(parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?)))
            }
            PhysicalPlanType::CoalesceBatches(coalesce_batches) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    coalesce_batches.input,
                    registry,
                    runtime,
                    extension_codec
                )?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(merge.input, registry, runtime, extension_codec)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    repart.input,
                    registry,
                    runtime,
                    extension_codec
                )?;
                match repart.partition_method {
                    Some(PartitionMethod::Hash(ref hash_part)) => {
                        let expr = hash_part
                            .hash_expr
                            .iter()
                            .map(|e| {
                                parse_physical_expr(e, registry, input.schema().as_ref())
                            })
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
                    _ => Err(DataFusionError::Internal(
                        "Invalid partitioning scheme".to_owned(),
                    )),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(limit.input, registry, runtime, extension_codec)?;
                let fetch = if limit.fetch >= 0 {
                    Some(limit.fetch as usize)
                } else {
                    None
                };
                Ok(Arc::new(GlobalLimitExec::new(
                    input,
                    limit.skip as usize,
                    fetch,
                )))
            }
            PhysicalPlanType::LocalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(limit.input, registry, runtime, extension_codec)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.fetch as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    window_agg.input,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let input_schema = window_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
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
                            ExprType::WindowExpr(window_node) => {
                                let window_node_expr = window_node
                                    .expr
                                    .as_ref()
                                    .map(|e| {
                                        parse_physical_expr(
                                            e.as_ref(),
                                            registry,
                                            &physical_schema,
                                        )
                                    })
                                    .transpose()?
                                    .ok_or_else(|| {
                                        proto_error(
                                            "missing window_node expr expression"
                                                .to_string(),
                                        )
                                    })?;

                                Ok(create_window_expr(
                                    &convert_required!(window_node.window_function)?,
                                    name.to_owned(),
                                    &[window_node_expr],
                                    &[],
                                    &[],
                                    Arc::new(WindowFrame::new(false)),
                                    &physical_schema,
                                )?)
                            }
                            _ => Err(DataFusionError::Internal(
                                "Invalid expression for WindowAggrExec".to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                //todo fill partition keys and sort keys
                Ok(Arc::new(WindowAggExec::try_new(
                    physical_window_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                    vec![],
                )?))
            }
            PhysicalPlanType::Aggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    hash_agg.input,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let mode = protobuf::AggregateMode::from_i32(hash_agg.mode).ok_or_else(
                    || {
                        proto_error(format!(
                            "Received a AggregateNode message with unknown AggregateMode {}",
                            hash_agg.mode
                        ))
                    },
                )?;
                let agg_mode: AggregateMode = match mode {
                    protobuf::AggregateMode::Partial => AggregateMode::Partial,
                    protobuf::AggregateMode::Final => AggregateMode::Final,
                    protobuf::AggregateMode::FinalPartitioned => {
                        AggregateMode::FinalPartitioned
                    }
                };

                let num_expr = hash_agg.group_expr.len();

                let group_expr = hash_agg
                    .group_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                            .map(|expr| (expr, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let null_expr = hash_agg
                    .null_expr
                    .iter()
                    .zip(hash_agg.group_expr_name.iter())
                    .map(|(expr, name)| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                            .map(|expr| (expr, name.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let groups: Vec<Vec<bool>> = if !hash_agg.groups.is_empty() {
                    hash_agg
                        .groups
                        .chunks(num_expr)
                        .map(|g| g.to_vec())
                        .collect::<Vec<Vec<bool>>>()
                } else {
                    vec![]
                };

                let input_schema = hash_agg
                    .input_schema
                    .as_ref()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "input_schema in AggregateNode is missing.".to_owned(),
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

                                let input_phy_expr: Vec<Arc<dyn PhysicalExpr>> = agg_node.expr.iter()
                                    .map(|e| parse_physical_expr(e, registry, &physical_schema).unwrap()).collect();

                                Ok(create_aggregate_expr(
                                    &aggr_function.into(),
                                    agg_node.distinct,
                                    input_phy_expr.as_slice(),
                                    &physical_schema,
                                    name.to_string(),
                                )?)
                            }
                            _ => Err(DataFusionError::Internal(
                                "Invalid aggregate expression for AggregateExec"
                                    .to_string(),
                            )),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(AggregateExec::try_new(
                    agg_mode,
                    PhysicalGroupBy::new(
                        group_expr.clone(),
                        vec![],
                        group_expr,
                        null_expr,
                        groups,
                    ),
                    physical_aggr_expr,
                    input,
                    Arc::new((&input_schema).try_into()?),
                )?))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    hashjoin.left,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let right: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    hashjoin.right,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let on: Vec<(Column, Column)> = hashjoin
                    .on
                    .iter()
                    .map(|col| {
                        let left = into_required!(col.left)?;
                        let right = into_required!(col.right)?;
                        Ok((left, right))
                    })
                    .collect::<Result<_>>()?;
                let join_type = protobuf::JoinType::from_i32(hashjoin.join_type)
                    .ok_or_else(|| {
                        proto_error(format!(
                            "Received a HashJoinNode message with unknown JoinType {}",
                            hashjoin.join_type
                        ))
                    })?;
                let filter = hashjoin
                    .filter
                    .as_ref()
                    .map(|f| {
                        let schema = f
                            .schema
                            .as_ref()
                            .ok_or_else(|| proto_error("Missing JoinFilter schema"))?
                            .try_into()?;

                        let expression = parse_physical_expr(
                            f.expression.as_ref().ok_or_else(|| {
                                proto_error("Unexpected empty filter expression")
                            })?,
                            registry, &schema
                        )?;
                        let column_indices = f.column_indices
                            .iter()
                            .map(|i| {
                                let side = protobuf::JoinSide::from_i32(i.side)
                                    .ok_or_else(|| proto_error(format!(
                                        "Received a HashJoinNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex{
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Ok(JoinFilter::new(expression, column_indices, schema))
                    })
                    .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

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
                    protobuf::PartitionMode::Auto => PartitionMode::Auto,
                };
                Ok(Arc::new(HashJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    &join_type.into(),
                    partition_mode,
                    hashjoin.null_equals_null,
                )?))
            }
            PhysicalPlanType::Union(union) => {
                let mut inputs: Vec<Arc<dyn ExecutionPlan>> = vec![];
                for input in &union.inputs {
                    inputs.push(input.try_into_physical_plan(
                        registry,
                        runtime,
                        extension_codec,
                    )?);
                }
                Ok(Arc::new(UnionExec::new(inputs)))
            }
            PhysicalPlanType::CrossJoin(crossjoin) => {
                let left: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    crossjoin.left,
                    registry,
                    runtime,
                    extension_codec
                )?;
                let right: Arc<dyn ExecutionPlan> = into_physical_plan!(
                    crossjoin.right,
                    registry,
                    runtime,
                    extension_codec
                )?;
                Ok(Arc::new(CrossJoinExec::new(left, right)))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(sort.input, registry, runtime, extension_codec)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}"
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {self:?}"
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: parse_physical_expr(expr,registry, input.schema().as_ref())?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "physical_plan::from_proto() {self:?}"
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fetch = if sort.fetch < 0 {
                    None
                } else {
                    Some(sort.fetch as usize)
                };
                Ok(Arc::new(SortExec::new_with_partitioning(
                    exprs,
                    input,
                    sort.preserve_partitioning,
                    fetch,
                )))
            }
            PhysicalPlanType::SortPreservingMerge(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan!(sort.input, registry, runtime, extension_codec)?;
                let exprs = sort
                    .expr
                    .iter()
                    .map(|expr| {
                        let expr = expr.expr_type.as_ref().ok_or_else(|| {
                            proto_error(format!(
                                "physical_plan::from_proto() Unexpected expr {self:?}"
                            ))
                        })?;
                        if let protobuf::physical_expr_node::ExprType::Sort(sort_expr) = expr {
                            let expr = sort_expr
                                .expr
                                .as_ref()
                                .ok_or_else(|| {
                                    proto_error(format!(
                                        "physical_plan::from_proto() Unexpected sort expr {self:?}"
                                    ))
                                })?
                                .as_ref();
                            Ok(PhysicalSortExpr {
                                expr: parse_physical_expr(expr,registry, input.schema().as_ref())?,
                                options: SortOptions {
                                    descending: !sort_expr.asc,
                                    nulls_first: sort_expr.nulls_first,
                                },
                            })
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "physical_plan::from_proto() {self:?}"
                            )))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(SortPreservingMergeExec::new(exprs, input)))
            }
            PhysicalPlanType::Extension(extension) => {
                let inputs: Vec<Arc<dyn ExecutionPlan>> = extension
                    .inputs
                    .iter()
                    .map(|i| i.try_into_physical_plan(registry, runtime, extension_codec))
                    .collect::<Result<_>>()?;

                let extension_node = extension_codec.try_decode(
                    extension.node.as_slice(),
                    &inputs,
                    registry,
                )?;

                Ok(extension_node)
            }
        }
    }

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let plan_clone = plan.clone();
        let plan = plan.as_any();

        if let Some(exec) = plan.downcast_ref::<ExplainExec>() {
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Explain(
                    protobuf::ExplainExecNode {
                        schema: Some(exec.schema().as_ref().try_into()?),
                        stringified_plans: exec
                            .stringified_plans()
                            .iter()
                            .map(|plan| plan.into())
                            .collect(),
                        verbose: exec.verbose(),
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            let expr = exec
                .expr()
                .iter()
                .map(|expr| expr.0.clone().try_into())
                .collect::<Result<Vec<_>>>()?;
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
                        skip: limit.skip() as u32,
                        fetch: match limit.fetch() {
                            Some(n) => n as i64,
                            _ => -1, // no limit
                        },
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
                        fetch: limit.fetch() as u32,
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
            let filter = exec
                .filter()
                .as_ref()
                .map(|f| {
                    let expression = f.expression().to_owned().try_into()?;
                    let column_indices = f
                        .column_indices()
                        .iter()
                        .map(|i| {
                            let side: protobuf::JoinSide = i.side.to_owned().into();
                            protobuf::ColumnIndex {
                                index: i.index as u32,
                                side: side.into(),
                            }
                        })
                        .collect();
                    let schema = f.schema().try_into()?;
                    Ok(protobuf::JoinFilter {
                        expression: Some(expression),
                        column_indices,
                        schema: Some(schema),
                    })
                })
                .map_or(Ok(None), |v: Result<protobuf::JoinFilter>| v.map(Some))?;

            let partition_mode = match exec.partition_mode() {
                PartitionMode::CollectLeft => protobuf::PartitionMode::CollectLeft,
                PartitionMode::Partitioned => protobuf::PartitionMode::Partitioned,
                PartitionMode::Auto => protobuf::PartitionMode::Auto,
            };

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::HashJoin(Box::new(
                    protobuf::HashJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        join_type: join_type.into(),
                        partition_mode: partition_mode.into(),
                        null_equals_null: exec.null_equals_null(),
                        filter,
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
        } else if let Some(exec) = plan.downcast_ref::<AggregateExec>() {
            let groups: Vec<bool> = exec
                .group_expr()
                .groups()
                .iter()
                .flatten()
                .copied()
                .collect();

            let group_names = exec
                .group_expr()
                .expr()
                .iter()
                .map(|expr| expr.1.to_owned())
                .collect();

            let agg = exec
                .aggr_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;
            let agg_names = exec
                .aggr_expr()
                .iter()
                .map(|expr| match expr.field() {
                    Ok(field) => Ok(field.name().clone()),
                    Err(e) => Err(e),
                })
                .collect::<Result<_>>()?;

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

            let null_expr = exec
                .group_expr()
                .null_expr()
                .iter()
                .map(|expr| expr.0.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

            let group_expr = exec
                .group_expr()
                .expr()
                .iter()
                .map(|expr| expr.0.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(
                    protobuf::AggregateExecNode {
                        group_expr,
                        group_expr_name: group_names,
                        aggr_expr: agg,
                        aggr_expr_name: agg_names,
                        mode: agg_mode as i32,
                        input: Some(Box::new(input)),
                        input_schema: Some(input_schema.as_ref().try_into()?),
                        null_expr,
                        groups,
                    },
                ))),
            })
        } else if let Some(empty) = plan.downcast_ref::<EmptyExec>() {
            let schema = empty.schema().as_ref().try_into()?;
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
                        delimiter: csv_delimiter_to_string(exec.delimiter())?,
                    },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<ParquetExec>() {
            let predicate = exec
                .predicate()
                .map(|pred| pred.clone().try_into())
                .transpose()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(
                    protobuf::ParquetScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        predicate,
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
                            .collect::<Result<Vec<_>>>()?,
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
                .collect::<Result<Vec<_>>>()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Sort(Box::new(
                    protobuf::SortExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        fetch: match exec.fetch() {
                            Some(n) => n as i64,
                            _ => -1,
                        },
                        preserve_partitioning: exec.preserve_partitioning(),
                    },
                ))),
            })
        } else if let Some(union) = plan.downcast_ref::<UnionExec>() {
            let mut inputs: Vec<PhysicalPlanNode> = vec![];
            for input in union.inputs() {
                inputs.push(protobuf::PhysicalPlanNode::try_from_physical_plan(
                    input.to_owned(),
                    extension_codec,
                )?);
            }
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Union(
                    protobuf::UnionExecNode { inputs },
                )),
            })
        } else if let Some(exec) = plan.downcast_ref::<SortPreservingMergeExec>() {
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
                .collect::<Result<Vec<_>>>()?;
            Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::SortPreservingMerge(
                    Box::new(protobuf::SortPreservingMergeExecNode {
                        input: Some(Box::new(input)),
                        expr,
                    }),
                )),
            })
        } else {
            let mut buf: Vec<u8> = vec![];
            match extension_codec.try_encode(plan_clone.clone(), &mut buf) {
                Ok(_) => {
                    let inputs: Vec<protobuf::PhysicalPlanNode> = plan_clone
                        .children()
                        .into_iter()
                        .map(|i| {
                            protobuf::PhysicalPlanNode::try_from_physical_plan(
                                i,
                                extension_codec,
                            )
                        })
                        .collect::<Result<_>>()?;

                    Ok(protobuf::PhysicalPlanNode {
                        physical_plan_type: Some(PhysicalPlanType::Extension(
                            protobuf::PhysicalExtensionNode { node: buf, inputs },
                        )),
                    })
                }
                Err(e) => Err(DataFusionError::Internal(format!(
                    "Unsupported plan and extension codec failed with [{e}]. Plan: {plan_clone:?}"
                ))),
            }
        }
    }
}

pub trait AsExecutionPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_physical_plan(
        &self,
        registry: &dyn FunctionRegistry,
        runtime: &RuntimeEnv,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_from_physical_plan(
        plan: Arc<dyn ExecutionPlan>,
        extension_codec: &dyn PhysicalExtensionCodec,
    ) -> Result<Self>
    where
        Self: Sized;
}

pub trait PhysicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()>;
}

#[derive(Debug)]
pub struct DefaultPhysicalExtensionCodec {}

impl PhysicalExtensionCodec for DefaultPhysicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "PhysicalExtensionCodec is not provided".to_string(),
        ))
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "PhysicalExtensionCodec is not provided".to_string(),
        ))
    }
}

#[macro_export]
macro_rules! into_physical_plan {
    ($PB:expr, $REG:expr, $RUNTIME:expr, $CODEC:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field
                .as_ref()
                .try_into_physical_plan($REG, $RUNTIME, $CODEC)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

#[cfg(test)]
mod roundtrip_tests {
    use std::ops::Deref;
    use std::sync::Arc;

    use super::super::protobuf;
    use crate::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::datatypes::IntervalUnit;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use datafusion::execution::context::ExecutionProps;
    use datafusion::logical_expr::create_udf;
    use datafusion::logical_expr::{BuiltinScalarFunction, Volatility};
    use datafusion::physical_expr::expressions::DateTimeIntervalExpr;
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::expressions::{like, BinaryExpr, GetIndexedFieldExpr};
    use datafusion::physical_plan::functions;
    use datafusion::physical_plan::functions::make_scalar_function;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::{
        arrow::{
            compute::kernels::sort::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        datasource::listing::PartitionedFile,
        logical_expr::{JoinType, Operator},
        physical_plan::{
            aggregates::{AggregateExec, AggregateMode},
            empty::EmptyExec,
            expressions::{binary, col, lit, InListExpr, NotExpr},
            expressions::{Avg, Column, DistinctCount, PhysicalSortExpr},
            file_format::{FileScanConfig, ParquetExec},
            filter::FilterExec,
            joins::{HashJoinExec, PartitionMode},
            limit::{GlobalLimitExec, LocalLimitExec},
            sorts::sort::SortExec,
            AggregateExpr, ExecutionPlan, PhysicalExpr, Statistics,
        },
        prelude::SessionContext,
        scalar::ScalarValue,
    };
    use datafusion_common::Result;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let ctx = SessionContext::new();
        let codec = DefaultPhysicalExtensionCodec {};
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
                .expect("to proto");
        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.deref(), &codec)
            .expect("from proto");
        assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
        Ok(())
    }

    fn roundtrip_test_with_context(
        exec_plan: Arc<dyn ExecutionPlan>,
        ctx: SessionContext,
    ) -> Result<()> {
        let codec = DefaultPhysicalExtensionCodec {};
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
                .expect("to proto");
        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.deref(), &codec)
            .expect("from proto");
        assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_date_time_interval() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("some_date", DataType::Date32, false),
            Field::new(
                "some_interval",
                DataType::Interval(IntervalUnit::DayTime),
                false,
            ),
        ]);
        let input = Arc::new(EmptyExec::new(false, Arc::new(schema.clone())));
        let date_expr = col("some_date", &schema)?;
        let literal_expr = col("some_interval", &schema)?;
        let date_time_interval_expr = Arc::new(DateTimeIntervalExpr::try_new(
            date_expr,
            Operator::Plus,
            literal_expr,
            &schema,
        )?);
        let plan = Arc::new(ProjectionExec::try_new(
            vec![(date_time_interval_expr, "result".to_string())],
            input,
        )?);
        roundtrip_test(plan)
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
            0,
            Some(25),
        )))
    }

    #[test]
    fn roundtrip_global_skip_no_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            10,
            None, // no limit
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
            JoinType::LeftAnti,
            JoinType::RightAnti,
            JoinType::LeftSemi,
            JoinType::RightSemi,
        ] {
            for partition_mode in
                &[PartitionMode::Partitioned, PartitionMode::CollectLeft]
            {
                roundtrip_test(Arc::new(HashJoinExec::try_new(
                    Arc::new(EmptyExec::new(false, schema_left.clone())),
                    Arc::new(EmptyExec::new(false, schema_right.clone())),
                    on.clone(),
                    None,
                    join_type,
                    *partition_mode,
                    false,
                )?))?;
            }
        }
        Ok(())
    }

    #[test]
    fn rountrip_aggregate() -> Result<()> {
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

        roundtrip_test(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups.clone()),
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
            schema.as_ref(),
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
            None,
        )?))
    }

    #[test]
    fn roundtrip_sort_preserve_partitioning() -> Result<()> {
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

        roundtrip_test(Arc::new(SortExec::new_with_partitioning(
            sort_exprs.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            false,
            None,
        )))?;

        roundtrip_test(Arc::new(SortExec::new_with_partitioning(
            sort_exprs,
            Arc::new(EmptyExec::new(false, schema)),
            true,
            None,
        )))
    }

    #[test]
    fn roundtrip_parquet_exec_with_pruning_predicate() -> Result<()> {
        let scan_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: Arc::new(Schema::new(vec![Field::new(
                "col",
                DataType::Utf8,
                false,
            )])),
            file_groups: vec![vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )]],
            statistics: Statistics {
                num_rows: Some(100),
                total_byte_size: Some(1024),
                column_statistics: None,
                is_exact: false,
            },
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };

        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("col", 1)),
            Operator::Eq,
            lit("1"),
        ));
        roundtrip_test(Arc::new(ParquetExec::new(
            scan_config,
            Some(predicate),
            None,
        )))
    }

    #[test]
    fn roundtrip_builtin_scalar_function() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let input = Arc::new(EmptyExec::new(false, schema.clone()));

        let execution_props = ExecutionProps::new();

        let fun_expr = functions::create_physical_fun(
            &BuiltinScalarFunction::Abs,
            &execution_props,
        )?;

        let expr = ScalarFunctionExpr::new(
            "abs",
            fun_expr,
            vec![col("a", &schema)?],
            &DataType::Int64,
        );

        let project =
            ProjectionExec::try_new(vec![(Arc::new(expr), "a".to_string())], input)?;

        roundtrip_test(Arc::new(project))
    }

    #[test]
    fn roundtrip_scalar_udf() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let input = Arc::new(EmptyExec::new(false, schema.clone()));

        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        let scalar_fn = make_scalar_function(fn_impl);

        let udf = create_udf(
            "dummy",
            vec![DataType::Int64],
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            scalar_fn.clone(),
        );

        let expr = ScalarFunctionExpr::new(
            "dummy",
            scalar_fn,
            vec![col("a", &schema)?],
            &DataType::Int64,
        );

        let project =
            ProjectionExec::try_new(vec![(Arc::new(expr), "a".to_string())], input)?;

        let ctx = SessionContext::new();

        ctx.register_udf(udf);

        roundtrip_test_with_context(Arc::new(project), ctx)
    }

    #[test]
    fn roundtrip_distinct_count() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(DistinctCount::new(
            DataType::Int64,
            col("b", &schema)?,
            "COUNT(DISTINCT b)".to_string(),
        ))];

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "unused".to_string())];

        roundtrip_test(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::new_single(groups),
            aggregates.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            schema,
        )?))
    }

    #[test]
    fn roundtrip_like() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let input = Arc::new(EmptyExec::new(false, Arc::new(schema.clone())));
        let like_expr = like(
            false,
            false,
            col("a", &schema)?,
            col("b", &schema)?,
            &schema,
        )?;
        let plan = Arc::new(ProjectionExec::try_new(
            vec![(like_expr, "result".to_string())],
            input,
        )?);
        roundtrip_test(plan)
    }

    #[test]
    fn roundtrip_get_indexed_field() -> Result<()> {
        let fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "a",
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
        ];

        let schema = Schema::new(fields);
        let input = Arc::new(EmptyExec::new(false, Arc::new(schema.clone())));

        let col_a = col("a", &schema)?;
        let key = ScalarValue::Int64(Some(1));
        let get_indexed_field_expr = Arc::new(GetIndexedFieldExpr::new(col_a, key));

        let plan = Arc::new(ProjectionExec::try_new(
            vec![(get_indexed_field_expr, "result".to_string())],
            input,
        )?);

        roundtrip_test(plan)
    }
}
