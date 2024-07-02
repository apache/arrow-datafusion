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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::sync::Arc;

use arrow::compute::SortOptions;
use chrono::{TimeZone, Utc};
use object_store::path::Path;
use object_store::ObjectMeta;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{FileRange, ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::WindowFunctionDefinition;
use datafusion::physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::{
    in_list, BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, LikeExpr,
    Literal, NegativeExpr, NotExpr, TryCastExpr,
};
use datafusion::physical_plan::windows::{create_window_expr, schema_add_window_field};
use datafusion::physical_plan::{Partitioning, PhysicalExpr, WindowExpr};
use datafusion_common::{not_impl_err, DataFusionError, Result};
use datafusion_proto_common::common::proto_error;

use crate::convert_required;
use crate::logical_plan::{self};
use crate::protobuf;
use crate::protobuf::physical_expr_node::ExprType;

use super::PhysicalExtensionCodec;

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

/// Parses a physical sort expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical sort expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_sort_expr(
    proto: &protobuf::PhysicalSortExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<PhysicalSortExpr> {
    if let Some(expr) = &proto.expr {
        let expr = parse_physical_expr(expr.as_ref(), registry, input_schema, codec)?;
        let options = SortOptions {
            descending: !proto.asc,
            nulls_first: proto.nulls_first,
        };
        Ok(PhysicalSortExpr { expr, options })
    } else {
        Err(proto_error("Unexpected empty physical expression"))
    }
}

/// Parses a physical sort expressions from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with vector of physical sort expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_sort_exprs(
    proto: &[protobuf::PhysicalSortExprNode],
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<PhysicalSortExpr>> {
    proto
        .iter()
        .map(|sort_expr| {
            parse_physical_sort_expr(sort_expr, registry, input_schema, codec)
        })
        .collect::<Result<Vec<_>>>()
}

/// Parses a physical window expr from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical window expression node.
/// * `name` - Name of the window expression.
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_window_expr(
    proto: &protobuf::PhysicalWindowExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn WindowExpr>> {
    let window_node_expr =
        parse_physical_exprs(&proto.args, registry, input_schema, codec)?;
    let partition_by =
        parse_physical_exprs(&proto.partition_by, registry, input_schema, codec)?;

    let order_by =
        parse_physical_sort_exprs(&proto.order_by, registry, input_schema, codec)?;

    let window_frame = proto
        .window_frame
        .as_ref()
        .map(|wf| wf.clone().try_into())
        .transpose()
        .map_err(|e| DataFusionError::Internal(format!("{e}")))?
        .ok_or_else(|| {
            DataFusionError::Internal(
                "Missing required field 'window_frame' in protobuf".to_string(),
            )
        })?;

    let fun = if let Some(window_func) = proto.window_function.as_ref() {
        match window_func {
            protobuf::physical_window_expr_node::WindowFunction::AggrFunction(n) => {
                let f = protobuf::AggregateFunction::try_from(*n).map_err(|_| {
                    proto_error(format!(
                        "Received an unknown window aggregate function: {n}"
                    ))
                })?;

                WindowFunctionDefinition::AggregateFunction(f.into())
            }
            protobuf::physical_window_expr_node::WindowFunction::BuiltInFunction(n) => {
                let f = protobuf::BuiltInWindowFunction::try_from(*n).map_err(|_| {
                    proto_error(format!(
                        "Received an unknown window builtin function: {n}"
                    ))
                })?;

                WindowFunctionDefinition::BuiltInWindowFunction(f.into())
            }
            protobuf::physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(udaf_name) => {
                let agg_udf = registry.udaf(udaf_name)?;
                WindowFunctionDefinition::AggregateUDF(agg_udf)
            }
        }
    } else {
        return Err(proto_error("Missing required field in protobuf"));
    };

    let name = proto.name.clone();
    // TODO: Remove extended_schema if functions are all UDAF
    let extended_schema =
        schema_add_window_field(&window_node_expr, input_schema, &fun, &name)?;
    // approx_percentile_cont and approx_percentile_cont_weight are not supported for UDAF from protobuf yet.
    let logical_exprs = &[];
    create_window_expr(
        &fun,
        name,
        &window_node_expr,
        logical_exprs,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        &extended_schema,
        false,
    )
}

pub fn parse_physical_exprs<'a, I>(
    protos: I,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<Arc<dyn PhysicalExpr>>>
where
    I: IntoIterator<Item = &'a protobuf::PhysicalExprNode>,
{
    protos
        .into_iter()
        .map(|p| parse_physical_expr(p, registry, input_schema, codec))
        .collect::<Result<Vec<_>>>()
}

/// Parses a physical expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_expr(
    proto: &protobuf::PhysicalExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        ExprType::Column(c) => {
            let pcol: Column = c.into();
            Arc::new(pcol)
        }
        ExprType::Literal(scalar) => Arc::new(Literal::new(scalar.try_into()?)),
        ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
            parse_required_physical_expr(
                binary_expr.l.as_deref(),
                registry,
                "left",
                input_schema,
                codec,
            )?,
            logical_plan::from_proto::from_proto_binary_op(&binary_expr.op)?,
            parse_required_physical_expr(
                binary_expr.r.as_deref(),
                registry,
                "right",
                input_schema,
                codec,
            )?,
        )),
        ExprType::AggregateExpr(_) => {
            return not_impl_err!(
                "Cannot convert aggregate expr node to physical expression"
            );
        }
        ExprType::WindowExpr(_) => {
            return not_impl_err!(
                "Cannot convert window expr node to physical expression"
            );
        }
        ExprType::Sort(_) => {
            return not_impl_err!("Cannot convert sort expr node to physical expression");
        }
        ExprType::IsNullExpr(e) => {
            Arc::new(IsNullExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?))
        }
        ExprType::IsNotNullExpr(e) => {
            Arc::new(IsNotNullExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?))
        }
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            registry,
            "expr",
            input_schema,
            codec,
        )?)),
        ExprType::Negative(e) => {
            Arc::new(NegativeExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?))
        }
        ExprType::InList(e) => in_list(
            parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?,
            parse_physical_exprs(&e.list, registry, input_schema, codec)?,
            &e.negated,
            input_schema,
        )?,
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema, codec))
                .transpose()?,
            e.when_then_expr
                .iter()
                .map(|e| {
                    Ok((
                        parse_required_physical_expr(
                            e.when_expr.as_ref(),
                            registry,
                            "when_expr",
                            input_schema,
                            codec,
                        )?,
                        parse_required_physical_expr(
                            e.then_expr.as_ref(),
                            registry,
                            "then_expr",
                            input_schema,
                            codec,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            e.else_expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema, codec))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?,
            convert_required!(e.arrow_type)?,
            None,
        )),
        ExprType::TryCast(e) => Arc::new(TryCastExpr::new(
            parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?,
            convert_required!(e.arrow_type)?,
        )),
        ExprType::ScalarUdf(e) => {
            let udf = match &e.fun_definition {
                Some(buf) => codec.try_decode_udf(&e.name, buf)?,
                None => registry.udf(e.name.as_str())?,
            };
            let scalar_fun_def = Arc::clone(&udf);

            let args = parse_physical_exprs(&e.args, registry, input_schema, codec)?;

            Arc::new(ScalarFunctionExpr::new(
                e.name.as_str(),
                scalar_fun_def,
                args,
                convert_required!(e.return_type)?,
            ))
        }
        ExprType::LikeExpr(like_expr) => Arc::new(LikeExpr::new(
            like_expr.negated,
            like_expr.case_insensitive,
            parse_required_physical_expr(
                like_expr.expr.as_deref(),
                registry,
                "expr",
                input_schema,
                codec,
            )?,
            parse_required_physical_expr(
                like_expr.pattern.as_deref(),
                registry,
                "pattern",
                input_schema,
                codec,
            )?,
        )),
    };

    Ok(pexpr)
}

fn parse_required_physical_expr(
    expr: Option<&protobuf::PhysicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: &str,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.map(|e| parse_physical_expr(e, registry, input_schema, codec))
        .transpose()?
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Missing required field {field:?}"))
        })
}

pub fn parse_protobuf_hash_partitioning(
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Option<Partitioning>> {
    match partitioning {
        Some(hash_part) => {
            let expr = parse_physical_exprs(
                &hash_part.hash_expr,
                registry,
                input_schema,
                codec,
            )?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}

pub fn parse_protobuf_partitioning(
    partitioning: Option<&protobuf::Partitioning>,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Option<Partitioning>> {
    match partitioning {
        Some(protobuf::Partitioning { partition_method }) => match partition_method {
            Some(protobuf::partitioning::PartitionMethod::RoundRobin(
                partition_count,
            )) => Ok(Some(Partitioning::RoundRobinBatch(
                *partition_count as usize,
            ))),
            Some(protobuf::partitioning::PartitionMethod::Hash(hash_repartition)) => {
                parse_protobuf_hash_partitioning(
                    Some(hash_repartition),
                    registry,
                    input_schema,
                    codec,
                )
            }
            Some(protobuf::partitioning::PartitionMethod::Unknown(partition_count)) => {
                Ok(Some(Partitioning::UnknownPartitioning(
                    *partition_count as usize,
                )))
            }
            None => Ok(None),
        },
        None => Ok(None),
    }
}

pub fn parse_protobuf_file_scan_config(
    proto: &protobuf::FileScanExecConf,
    registry: &dyn FunctionRegistry,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<FileScanConfig> {
    let schema: Arc<Schema> = Arc::new(convert_required!(proto.schema)?);
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

    let object_store_url = match proto.object_store_url.is_empty() {
        false => ObjectStoreUrl::parse(&proto.object_store_url)?,
        true => ObjectStoreUrl::local_filesystem(),
    };

    // Reacquire the partition column types from the schema before removing them below.
    let table_partition_cols = proto
        .table_partition_cols
        .iter()
        .map(|col| Ok(schema.field_with_name(col)?.clone()))
        .collect::<Result<Vec<_>>>()?;

    // Remove partition columns from the schema after recreating table_partition_cols
    // because the partition columns are not in the file. They are present to allow
    // the partition column types to be reconstructed after serde.
    let file_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| !table_partition_cols.contains(field))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let mut output_ordering = vec![];
    for node_collection in &proto.output_ordering {
        let sort_expr = parse_physical_sort_exprs(
            &node_collection.physical_sort_expr_nodes,
            registry,
            &schema,
            codec,
        )?;
        output_ordering.push(sort_expr);
    }

    Ok(FileScanConfig {
        object_store_url,
        file_schema,
        file_groups,
        statistics,
        projection,
        limit: proto.limit.as_ref().map(|sl| sl.limit as usize),
        table_partition_cols,
        output_ordering,
    })
}

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(val.path.as_str()),
                last_modified: Utc.timestamp_nanos(val.last_modified_ns as i64),
                size: val.size as usize,
                e_tag: None,
                version: None,
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
            statistics: val.statistics.as_ref().map(|v| v.try_into()).transpose()?,
            extensions: None,
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for Vec<PartitionedFile> {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        val.files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, _>>()
    }
}

impl TryFrom<&protobuf::JsonSink> for JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::JsonSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required!(value.config)?,
            convert_required!(value.writer_options)?,
        ))
    }
}

#[cfg(feature = "parquet")]
impl TryFrom<&protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required!(value.config)?,
            convert_required!(value.parquet_options)?,
        ))
    }
}

impl TryFrom<&protobuf::CsvSink> for CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::CsvSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required!(value.config)?,
            convert_required!(value.writer_options)?,
        ))
    }
}

impl TryFrom<&protobuf::FileSinkConfig> for FileSinkConfig {
    type Error = DataFusionError;

    fn try_from(conf: &protobuf::FileSinkConfig) -> Result<Self, Self::Error> {
        let file_groups = conf
            .file_groups
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let table_paths = conf
            .table_paths
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>>>()?;
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|protobuf::PartitionColumn { name, arrow_type }| {
                let data_type = convert_required!(arrow_type)?;
                Ok((name.clone(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            object_store_url: ObjectStoreUrl::parse(&conf.object_store_url)?,
            file_groups,
            table_paths,
            output_schema: Arc::new(convert_required!(conf.output_schema)?),
            table_partition_cols,
            overwrite: conf.overwrite,
            keep_partition_by_columns: conf.keep_partition_by_columns,
        })
    }
}
