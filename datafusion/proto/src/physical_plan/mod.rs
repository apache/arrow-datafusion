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
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonSink;
#[cfg(feature = "parquet")]
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::datasource::physical_plan::{AvroExec, CsvExec};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::aggregates::{create_aggregate_expr, AggregateMode};
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::insert::FileSinkExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    CrossJoinExec, NestedLoopJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::windows::{
    BoundedWindowAggExec, PartitionSearchMode, WindowAggExec,
};
use datafusion::physical_plan::{
    udaf, AggregateExpr, ExecutionPlan, Partitioning, PhysicalExpr, WindowExpr,
};
use datafusion_common::{internal_err, not_impl_err, DataFusionError, Result};
use prost::bytes::BufMut;
use prost::Message;

use crate::common::str_to_byte;
use crate::common::{byte_to_string, proto_error};
use crate::physical_plan::from_proto::{
    parse_physical_expr, parse_physical_sort_expr, parse_protobuf_file_scan_config,
};
use crate::protobuf::physical_aggregate_expr_node::AggregateFunction;
use crate::protobuf::physical_expr_node::ExprType;
use crate::protobuf::physical_plan_node::PhysicalPlanType;
use crate::protobuf::repartition_exec_node::PartitionMethod;
use crate::protobuf::{
    self, window_agg_exec_node, PhysicalPlanNode, PhysicalSortExprNodeCollection,
};
use crate::{convert_required, into_required};

use self::from_proto::parse_physical_window_expr;

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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &projection.input,
                    registry,
                    runtime,
                    extension_codec,
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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &filter.input,
                    registry,
                    runtime,
                    extension_codec,
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
                Ok(Arc::new(FilterExec::try_new(predicate, None, input)?))
            }
            PhysicalPlanType::CsvScan(scan) => Ok(Arc::new(CsvExec::new(
                parse_protobuf_file_scan_config(
                    scan.base_conf.as_ref().unwrap(),
                    registry,
                )?,
                scan.has_header,
                str_to_byte(&scan.delimiter, "delimiter")?,
                str_to_byte(&scan.quote, "quote")?,
                if let Some(protobuf::csv_scan_exec_node::OptionalEscape::Escape(
                    escape,
                )) = &scan.optional_escape
                {
                    Some(str_to_byte(escape, "escape")?)
                } else {
                    None
                },
                FileCompressionType::UNCOMPRESSED,
            ))),
            #[cfg(feature = "parquet")]
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
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &coalesce_batches.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                Ok(Arc::new(CoalesceBatchesExec::new(
                    input,
                    coalesce_batches.target_batch_size as usize,
                )))
            }
            PhysicalPlanType::Merge(merge) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&merge.input, registry, runtime, extension_codec)?;
                Ok(Arc::new(CoalescePartitionsExec::new(input)))
            }
            PhysicalPlanType::Repartition(repart) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &repart.input,
                    registry,
                    runtime,
                    extension_codec,
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
                    _ => internal_err!("Invalid partitioning scheme"),
                }
            }
            PhysicalPlanType::GlobalLimit(limit) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&limit.input, registry, runtime, extension_codec)?;
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
                    into_physical_plan(&limit.input, registry, runtime, extension_codec)?;
                Ok(Arc::new(LocalLimitExec::new(input, limit.fetch as usize)))
            }
            PhysicalPlanType::Window(window_agg) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &window_agg.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let input_schema = input.schema();

                let physical_window_expr: Vec<Arc<dyn WindowExpr>> = window_agg
                    .window_expr
                    .iter()
                    .map(|window_expr| {
                        parse_physical_window_expr(
                            window_expr,
                            registry,
                            input_schema.as_ref(),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let partition_keys = window_agg
                    .partition_keys
                    .iter()
                    .map(|expr| {
                        parse_physical_expr(expr, registry, input.schema().as_ref())
                    })
                    .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;

                if let Some(partition_search_mode) =
                    window_agg.partition_search_mode.as_ref()
                {
                    let partition_search_mode = match partition_search_mode {
                        window_agg_exec_node::PartitionSearchMode::Linear(_) => {
                            PartitionSearchMode::Linear
                        }
                        window_agg_exec_node::PartitionSearchMode::PartiallySorted(
                            protobuf::PartiallySortedPartitionSearchMode { columns },
                        ) => PartitionSearchMode::PartiallySorted(
                            columns.iter().map(|c| *c as usize).collect(),
                        ),
                        window_agg_exec_node::PartitionSearchMode::Sorted(_) => {
                            PartitionSearchMode::Sorted
                        }
                    };

                    Ok(Arc::new(BoundedWindowAggExec::try_new(
                        physical_window_expr,
                        input,
                        partition_keys,
                        partition_search_mode,
                    )?))
                } else {
                    Ok(Arc::new(WindowAggExec::try_new(
                        physical_window_expr,
                        input,
                        partition_keys,
                    )?))
                }
            }
            PhysicalPlanType::Aggregate(hash_agg) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &hash_agg.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let mode = protobuf::AggregateMode::try_from(hash_agg.mode).map_err(
                    |_| {
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
                    protobuf::AggregateMode::Single => AggregateMode::Single,
                    protobuf::AggregateMode::SinglePartitioned => {
                        AggregateMode::SinglePartitioned
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

                let input_schema = hash_agg.input_schema.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(
                        "input_schema in AggregateNode is missing.".to_owned(),
                    )
                })?;
                let physical_schema: SchemaRef = SchemaRef::new(input_schema.try_into()?);

                let physical_filter_expr = hash_agg
                    .filter_expr
                    .iter()
                    .map(|expr| {
                        expr.expr
                            .as_ref()
                            .map(|e| parse_physical_expr(e, registry, &physical_schema))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let physical_order_by_expr = hash_agg
                    .order_by_expr
                    .iter()
                    .map(|expr| {
                        expr.sort_expr
                            .iter()
                            .map(|e| {
                                parse_physical_sort_expr(e, registry, &physical_schema)
                            })
                            .collect::<Result<Vec<_>>>()
                            .map(|exprs| (!exprs.is_empty()).then_some(exprs))
                    })
                    .collect::<Result<Vec<_>>>()?;

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
                                let input_phy_expr: Vec<Arc<dyn PhysicalExpr>> = agg_node.expr.iter()
                                    .map(|e| parse_physical_expr(e, registry, &physical_schema).unwrap()).collect();
                                let ordering_req: Vec<PhysicalSortExpr> = agg_node.ordering_req.iter()
                                    .map(|e| parse_physical_sort_expr(e, registry, &physical_schema).unwrap()).collect();
                                agg_node.aggregate_function.as_ref().map(|func| {
                                    match func {
                                        AggregateFunction::AggrFunction(i) => {
                                            let aggr_function = protobuf::AggregateFunction::try_from(*i)
                                                .map_err(
                                                    |_| {
                                                        proto_error(format!(
                                                            "Received an unknown aggregate function: {i}"
                                                        ))
                                                    },
                                                )?;

                                            create_aggregate_expr(
                                                &aggr_function.into(),
                                                agg_node.distinct,
                                                input_phy_expr.as_slice(),
                                                &ordering_req,
                                                &physical_schema,
                                                name.to_string(),
                                            )
                                        }
                                        AggregateFunction::UserDefinedAggrFunction(udaf_name) => {
                                            let agg_udf = registry.udaf(udaf_name)?;
                                            udaf::create_aggregate_expr(agg_udf.as_ref(), &input_phy_expr, &physical_schema, name)
                                        }
                                    }
                                }).transpose()?.ok_or_else(|| {
                                    proto_error("Invalid AggregateExpr, missing aggregate_function")
                                })
                            }
                            _ => internal_err!(
                                "Invalid aggregate expression for AggregateExec"
                            ),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Arc::new(AggregateExec::try_new(
                    agg_mode,
                    PhysicalGroupBy::new(group_expr, null_expr, groups),
                    physical_aggr_expr,
                    physical_filter_expr,
                    physical_order_by_expr,
                    input,
                    Arc::new(input_schema.try_into()?),
                )?))
            }
            PhysicalPlanType::HashJoin(hashjoin) => {
                let left: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &hashjoin.left,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let right: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &hashjoin.right,
                    registry,
                    runtime,
                    extension_codec,
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
                let join_type = protobuf::JoinType::try_from(hashjoin.join_type)
                    .map_err(|_| {
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
                                let side = protobuf::JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
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

                let partition_mode = protobuf::PartitionMode::try_from(
                    hashjoin.partition_mode,
                )
                .map_err(|_| {
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
            PhysicalPlanType::SymmetricHashJoin(sym_join) => {
                let left = into_physical_plan(
                    &sym_join.left,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let right = into_physical_plan(
                    &sym_join.right,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let on = sym_join
                    .on
                    .iter()
                    .map(|col| {
                        let left = into_required!(col.left)?;
                        let right = into_required!(col.right)?;
                        Ok((left, right))
                    })
                    .collect::<Result<_>>()?;
                let join_type = protobuf::JoinType::try_from(sym_join.join_type)
                    .map_err(|_| {
                        proto_error(format!(
                            "Received a SymmetricHashJoin message with unknown JoinType {}",
                            sym_join.join_type
                        ))
                    })?;
                let filter = sym_join
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
                                let side = protobuf::JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
                                        "Received a HashJoinNode message with JoinSide in Filter {}",
                                        i.side))
                                    )?;

                                Ok(ColumnIndex{
                                    index: i.index as usize,
                                    side: side.into(),
                                })
                            })
                            .collect::<Result<_>>()?;

                        Ok(JoinFilter::new(expression, column_indices, schema))
                    })
                    .map_or(Ok(None), |v: Result<JoinFilter>| v.map(Some))?;

                let partition_mode =
                    protobuf::StreamPartitionMode::try_from(sym_join.partition_mode).map_err(|_| {
                        proto_error(format!(
                            "Received a SymmetricHashJoin message with unknown PartitionMode {}",
                            sym_join.partition_mode
                        ))
                    })?;
                let partition_mode = match partition_mode {
                    protobuf::StreamPartitionMode::SinglePartition => {
                        StreamJoinPartitionMode::SinglePartition
                    }
                    protobuf::StreamPartitionMode::PartitionedExec => {
                        StreamJoinPartitionMode::Partitioned
                    }
                };
                SymmetricHashJoinExec::try_new(
                    left,
                    right,
                    on,
                    filter,
                    &join_type.into(),
                    sym_join.null_equals_null,
                    partition_mode,
                )
                .map(|e| Arc::new(e) as _)
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
                let left: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &crossjoin.left,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                let right: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &crossjoin.right,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                Ok(Arc::new(CrossJoinExec::new(left, right)))
            }
            PhysicalPlanType::Empty(empty) => {
                let schema = Arc::new(convert_required!(empty.schema)?);
                Ok(Arc::new(EmptyExec::new(empty.produce_one_row, schema)))
            }
            PhysicalPlanType::Sort(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&sort.input, registry, runtime, extension_codec)?;
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
                            internal_err!(
                                "physical_plan::from_proto() {self:?}"
                            )
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fetch = if sort.fetch < 0 {
                    None
                } else {
                    Some(sort.fetch as usize)
                };
                let new_sort = SortExec::new(exprs, input)
                    .with_fetch(fetch)
                    .with_preserve_partitioning(sort.preserve_partitioning);

                Ok(Arc::new(new_sort))
            }
            PhysicalPlanType::SortPreservingMerge(sort) => {
                let input: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&sort.input, registry, runtime, extension_codec)?;
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
                            internal_err!(
                                "physical_plan::from_proto() {self:?}"
                            )
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let fetch = if sort.fetch < 0 {
                    None
                } else {
                    Some(sort.fetch as usize)
                };
                Ok(Arc::new(
                    SortPreservingMergeExec::new(exprs, input).with_fetch(fetch),
                ))
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
            PhysicalPlanType::NestedLoopJoin(join) => {
                let left: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.left, registry, runtime, extension_codec)?;
                let right: Arc<dyn ExecutionPlan> =
                    into_physical_plan(&join.right, registry, runtime, extension_codec)?;
                let join_type =
                    protobuf::JoinType::try_from(join.join_type).map_err(|_| {
                        proto_error(format!(
                            "Received a NestedLoopJoinExecNode message with unknown JoinType {}",
                            join.join_type
                        ))
                    })?;
                let filter = join
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
                                let side = protobuf::JoinSide::try_from(i.side)
                                    .map_err(|_| proto_error(format!(
                                        "Received a NestedLoopJoinExecNode message with JoinSide in Filter {}",
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

                Ok(Arc::new(NestedLoopJoinExec::try_new(
                    left,
                    right,
                    filter,
                    &join_type.into(),
                )?))
            }
            PhysicalPlanType::Analyze(analyze) => {
                let input: Arc<dyn ExecutionPlan> = into_physical_plan(
                    &analyze.input,
                    registry,
                    runtime,
                    extension_codec,
                )?;
                Ok(Arc::new(AnalyzeExec::new(
                    analyze.verbose,
                    analyze.show_statistics,
                    input,
                    Arc::new(convert_required!(analyze.schema)?),
                )))
            }
            PhysicalPlanType::JsonSink(sink) => {
                let input =
                    into_physical_plan(&sink.input, registry, runtime, extension_codec)?;

                let data_sink: JsonSink = sink
                    .sink
                    .as_ref()
                    .ok_or_else(|| proto_error("Missing required field in protobuf"))?
                    .try_into()?;
                let sink_schema = convert_required!(sink.sink_schema)?;
                let sort_order = sink
                    .sort_order
                    .as_ref()
                    .map(|collection| {
                        collection
                            .physical_sort_expr_nodes
                            .iter()
                            .map(|proto| {
                                parse_physical_sort_expr(proto, registry, &sink_schema)
                                    .map(Into::into)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(Arc::new(FileSinkExec::new(
                    input,
                    Arc::new(data_sink),
                    Arc::new(sink_schema),
                    sort_order,
                )))
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
            return Ok(protobuf::PhysicalPlanNode {
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
            });
        }

        if let Some(exec) = plan.downcast_ref::<ProjectionExec>() {
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
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Projection(Box::new(
                    protobuf::ProjectionExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        expr_name,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<AnalyzeExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Analyze(Box::new(
                    protobuf::AnalyzeExecNode {
                        verbose: exec.verbose(),
                        show_statistics: exec.show_statistics(),
                        input: Some(Box::new(input)),
                        schema: Some(exec.schema().as_ref().try_into()?),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<FilterExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Filter(Box::new(
                    protobuf::FilterExecNode {
                        input: Some(Box::new(input)),
                        expr: Some(exec.predicate().clone().try_into()?),
                    },
                ))),
            });
        }

        if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                limit.input().to_owned(),
                extension_codec,
            )?;

            return Ok(protobuf::PhysicalPlanNode {
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
            });
        }

        if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                limit.input().to_owned(),
                extension_codec,
            )?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::LocalLimit(Box::new(
                    protobuf::LocalLimitExecNode {
                        input: Some(Box::new(input)),
                        fetch: limit.fetch() as u32,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<HashJoinExec>() {
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

            return Ok(protobuf::PhysicalPlanNode {
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
            });
        }

        if let Some(exec) = plan.downcast_ref::<SymmetricHashJoinExec>() {
            let left = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.left().to_owned(),
                extension_codec,
            )?;
            let right = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.right().to_owned(),
                extension_codec,
            )?;
            let on = exec
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
                StreamJoinPartitionMode::SinglePartition => {
                    protobuf::StreamPartitionMode::SinglePartition
                }
                StreamJoinPartitionMode::Partitioned => {
                    protobuf::StreamPartitionMode::PartitionedExec
                }
            };

            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::SymmetricHashJoin(Box::new(
                    protobuf::SymmetricHashJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        on,
                        join_type: join_type.into(),
                        partition_mode: partition_mode.into(),
                        null_equals_null: exec.null_equals_null(),
                        filter,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<CrossJoinExec>() {
            let left = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.left().to_owned(),
                extension_codec,
            )?;
            let right = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.right().to_owned(),
                extension_codec,
            )?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CrossJoin(Box::new(
                    protobuf::CrossJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                    },
                ))),
            });
        }
        if let Some(exec) = plan.downcast_ref::<AggregateExec>() {
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

            let filter = exec
                .filter_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

            let order_by = exec
                .order_by_expr()
                .iter()
                .map(|expr| expr.to_owned().try_into())
                .collect::<Result<Vec<_>>>()?;

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
                AggregateMode::Single => protobuf::AggregateMode::Single,
                AggregateMode::SinglePartitioned => {
                    protobuf::AggregateMode::SinglePartitioned
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

            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Aggregate(Box::new(
                    protobuf::AggregateExecNode {
                        group_expr,
                        group_expr_name: group_names,
                        aggr_expr: agg,
                        filter_expr: filter,
                        order_by_expr: order_by,
                        aggr_expr_name: agg_names,
                        mode: agg_mode as i32,
                        input: Some(Box::new(input)),
                        input_schema: Some(input_schema.as_ref().try_into()?),
                        null_expr,
                        groups,
                    },
                ))),
            });
        }

        if let Some(empty) = plan.downcast_ref::<EmptyExec>() {
            let schema = empty.schema().as_ref().try_into()?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Empty(
                    protobuf::EmptyExecNode {
                        produce_one_row: empty.produce_one_row(),
                        schema: Some(schema),
                    },
                )),
            });
        }

        if let Some(coalesce_batches) = plan.downcast_ref::<CoalesceBatchesExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                coalesce_batches.input().to_owned(),
                extension_codec,
            )?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CoalesceBatches(Box::new(
                    protobuf::CoalesceBatchesExecNode {
                        input: Some(Box::new(input)),
                        target_batch_size: coalesce_batches.target_batch_size() as u32,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<CsvExec>() {
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::CsvScan(
                    protobuf::CsvScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        has_header: exec.has_header(),
                        delimiter: byte_to_string(exec.delimiter(), "delimiter")?,
                        quote: byte_to_string(exec.quote(), "quote")?,
                        optional_escape: if let Some(escape) = exec.escape() {
                            Some(protobuf::csv_scan_exec_node::OptionalEscape::Escape(
                                byte_to_string(escape, "escape")?,
                            ))
                        } else {
                            None
                        },
                    },
                )),
            });
        }

        #[cfg(feature = "parquet")]
        if let Some(exec) = plan.downcast_ref::<ParquetExec>() {
            let predicate = exec
                .predicate()
                .map(|pred| pred.clone().try_into())
                .transpose()?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::ParquetScan(
                    protobuf::ParquetScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                        predicate,
                    },
                )),
            });
        }

        if let Some(exec) = plan.downcast_ref::<AvroExec>() {
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::AvroScan(
                    protobuf::AvroScanExecNode {
                        base_conf: Some(exec.base_config().try_into()?),
                    },
                )),
            });
        }

        if let Some(exec) = plan.downcast_ref::<CoalescePartitionsExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Merge(Box::new(
                    protobuf::CoalescePartitionsExecNode {
                        input: Some(Box::new(input)),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<RepartitionExec>() {
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

            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Repartition(Box::new(
                    protobuf::RepartitionExecNode {
                        input: Some(Box::new(input)),
                        partition_method: Some(pb_partition_method),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<SortExec>() {
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
            return Ok(protobuf::PhysicalPlanNode {
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
            });
        }

        if let Some(union) = plan.downcast_ref::<UnionExec>() {
            let mut inputs: Vec<PhysicalPlanNode> = vec![];
            for input in union.inputs() {
                inputs.push(protobuf::PhysicalPlanNode::try_from_physical_plan(
                    input.to_owned(),
                    extension_codec,
                )?);
            }
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Union(
                    protobuf::UnionExecNode { inputs },
                )),
            });
        }

        if let Some(exec) = plan.downcast_ref::<SortPreservingMergeExec>() {
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
            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::SortPreservingMerge(
                    Box::new(protobuf::SortPreservingMergeExecNode {
                        input: Some(Box::new(input)),
                        expr,
                        fetch: exec.fetch().map(|f| f as i64).unwrap_or(-1),
                    }),
                )),
            });
        }

        if let Some(exec) = plan.downcast_ref::<NestedLoopJoinExec>() {
            let left = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.left().to_owned(),
                extension_codec,
            )?;
            let right = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.right().to_owned(),
                extension_codec,
            )?;

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

            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::NestedLoopJoin(Box::new(
                    protobuf::NestedLoopJoinExecNode {
                        left: Some(Box::new(left)),
                        right: Some(Box::new(right)),
                        join_type: join_type.into(),
                        filter,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<WindowAggExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;

            let window_expr =
                exec.window_expr()
                    .iter()
                    .map(|e| e.clone().try_into())
                    .collect::<Result<Vec<protobuf::PhysicalWindowExprNode>>>()?;

            let partition_keys = exec
                .partition_keys
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<protobuf::PhysicalExprNode>>>()?;

            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Window(Box::new(
                    protobuf::WindowAggExecNode {
                        input: Some(Box::new(input)),
                        window_expr,
                        partition_keys,
                        partition_search_mode: None,
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<BoundedWindowAggExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;

            let window_expr =
                exec.window_expr()
                    .iter()
                    .map(|e| e.clone().try_into())
                    .collect::<Result<Vec<protobuf::PhysicalWindowExprNode>>>()?;

            let partition_keys = exec
                .partition_keys
                .iter()
                .map(|e| e.clone().try_into())
                .collect::<Result<Vec<protobuf::PhysicalExprNode>>>()?;

            let partition_search_mode = match &exec.partition_search_mode {
                PartitionSearchMode::Linear => {
                    window_agg_exec_node::PartitionSearchMode::Linear(
                        protobuf::EmptyMessage {},
                    )
                }
                PartitionSearchMode::PartiallySorted(columns) => {
                    window_agg_exec_node::PartitionSearchMode::PartiallySorted(
                        protobuf::PartiallySortedPartitionSearchMode {
                            columns: columns.iter().map(|c| *c as u64).collect(),
                        },
                    )
                }
                PartitionSearchMode::Sorted => {
                    window_agg_exec_node::PartitionSearchMode::Sorted(
                        protobuf::EmptyMessage {},
                    )
                }
            };

            return Ok(protobuf::PhysicalPlanNode {
                physical_plan_type: Some(PhysicalPlanType::Window(Box::new(
                    protobuf::WindowAggExecNode {
                        input: Some(Box::new(input)),
                        window_expr,
                        partition_keys,
                        partition_search_mode: Some(partition_search_mode),
                    },
                ))),
            });
        }

        if let Some(exec) = plan.downcast_ref::<FileSinkExec>() {
            let input = protobuf::PhysicalPlanNode::try_from_physical_plan(
                exec.input().to_owned(),
                extension_codec,
            )?;
            let sort_order = match exec.sort_order() {
                Some(requirements) => {
                    let expr = requirements
                        .iter()
                        .map(|requirement| {
                            let expr: PhysicalSortExpr = requirement.to_owned().into();
                            let sort_expr = protobuf::PhysicalSortExprNode {
                                expr: Some(Box::new(expr.expr.to_owned().try_into()?)),
                                asc: !expr.options.descending,
                                nulls_first: expr.options.nulls_first,
                            };
                            Ok(sort_expr)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Some(PhysicalSortExprNodeCollection {
                        physical_sort_expr_nodes: expr,
                    })
                }
                None => None,
            };

            if let Some(sink) = exec.sink().as_any().downcast_ref::<JsonSink>() {
                return Ok(protobuf::PhysicalPlanNode {
                    physical_plan_type: Some(PhysicalPlanType::JsonSink(Box::new(
                        protobuf::JsonSinkExecNode {
                            input: Some(Box::new(input)),
                            sink: Some(sink.try_into()?),
                            sink_schema: Some(exec.schema().as_ref().try_into()?),
                            sort_order,
                        },
                    ))),
                });
            }

            // If unknown DataSink then let extension handle it
        }

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
            Err(e) => internal_err!(
                "Unsupported plan and extension codec failed with [{e}]. Plan: {plan_clone:?}"
            ),
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
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        not_impl_err!("PhysicalExtensionCodec is not provided")
    }
}

fn into_physical_plan(
    node: &Option<Box<PhysicalPlanNode>>,
    registry: &dyn FunctionRegistry,
    runtime: &RuntimeEnv,
    extension_codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if let Some(field) = node {
        field.try_into_physical_plan(registry, runtime, extension_codec)
    } else {
        Err(proto_error("Missing required field in protobuf"))
    }
}
