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

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::csv::CsvSink;
use datafusion::datasource::file_format::json::JsonSink;
#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::{FileRange, ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion_common::file_options::arrow_writer::ArrowWriterOptions;
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::file_options::json_writer::JsonWriterOptions;
#[cfg(feature = "parquet")]
use datafusion_common::file_options::parquet_writer::ParquetWriterOptions;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{
    not_impl_err, DataFusionError, FileTypeWriterOptions, JoinSide, Result, ScalarValue,
};
use datafusion_execution::FunctionRegistry;
use datafusion_expr::WindowFunctionDefinition;
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion_physical_plan::expressions::{
    in_list, BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, LikeExpr,
    Literal, NegativeExpr, NotExpr, TryCastExpr,
};
use datafusion_physical_plan::expressions::{GetFieldAccessExpr, GetIndexedFieldExpr};
use datafusion_physical_plan::windows::create_window_expr;
use datafusion_physical_plan::{
    functions, ColumnStatistics, Partitioning, PhysicalExpr, Statistics, WindowExpr,
};

use crate::common::proto_error;
use crate::convert_required;
use crate::logical_plan;
use crate::protobuf;
use crate::protobuf::physical_expr_node::ExprType;

use crate::logical_plan::csv_writer_options_from_proto;
#[cfg(feature = "parquet")]
use crate::logical_plan::writer_properties_from_proto;
use chrono::{TimeZone, Utc};
use object_store::path::Path;
use object_store::ObjectMeta;

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
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_sort_expr(
    proto: &protobuf::PhysicalSortExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<PhysicalSortExpr> {
    if let Some(expr) = &proto.expr {
        let expr = parse_physical_expr(expr.as_ref(), registry, input_schema)?;
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
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_sort_exprs(
    proto: &[protobuf::PhysicalSortExprNode],
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Vec<PhysicalSortExpr>> {
    proto
        .iter()
        .map(|sort_expr| {
            if let Some(expr) = &sort_expr.expr {
                let expr = parse_physical_expr(expr.as_ref(), registry, input_schema)?;
                let options = SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                };
                Ok(PhysicalSortExpr { expr, options })
            } else {
                Err(proto_error("Unexpected empty physical expression"))
            }
        })
        .collect::<Result<Vec<_>>>()
}

/// Parses a physical window expr from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical window exprression node.
/// * `name` - Name of the window expression.
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_window_expr(
    proto: &protobuf::PhysicalWindowExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    let window_node_expr = proto
        .args
        .iter()
        .map(|e| parse_physical_expr(e, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let partition_by = proto
        .partition_by
        .iter()
        .map(|p| parse_physical_expr(p, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

    let order_by = proto
        .order_by
        .iter()
        .map(|o| parse_physical_sort_expr(o, registry, input_schema))
        .collect::<Result<Vec<_>>>()?;

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

    create_window_expr(
        &convert_required!(proto.window_function)?,
        proto.name.clone(),
        &window_node_expr,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        input_schema,
    )
}

/// Parses a physical expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function' names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn parse_physical_expr(
    proto: &protobuf::PhysicalExprNode,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
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
            )?,
            logical_plan::from_proto::from_proto_binary_op(&binary_expr.op)?,
            parse_required_physical_expr(
                binary_expr.r.as_deref(),
                registry,
                "right",
                input_schema,
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
            )?))
        }
        ExprType::IsNotNullExpr(e) => {
            Arc::new(IsNotNullExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
            )?))
        }
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            registry,
            "expr",
            input_schema,
        )?)),
        ExprType::Negative(e) => {
            Arc::new(NegativeExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
            )?))
        }
        ExprType::InList(e) => in_list(
            parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
            )?,
            e.list
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?,
            &e.negated,
            input_schema,
        )?,
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
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
                        )?,
                        parse_required_physical_expr(
                            e.then_expr.as_ref(),
                            registry,
                            "then_expr",
                            input_schema,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            e.else_expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), registry, input_schema))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            parse_required_physical_expr(
                e.expr.as_deref(),
                registry,
                "expr",
                input_schema,
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
            )?,
            convert_required!(e.arrow_type)?,
        )),
        ExprType::ScalarFunction(e) => {
            let scalar_function =
                protobuf::ScalarFunction::try_from(e.fun).map_err(|_| {
                    proto_error(
                        format!("Received an unknown scalar function: {}", e.fun,),
                    )
                })?;

            let args = e
                .args
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            // TODO Do not create new the ExecutionProps
            let execution_props = ExecutionProps::new();

            functions::create_physical_expr(
                &(&scalar_function).into(),
                &args,
                input_schema,
                &execution_props,
            )?
        }
        ExprType::ScalarUdf(e) => {
            let udf = registry.udf(e.name.as_str())?;
            let signature = udf.signature();
            let scalar_fun = udf.fun().clone();

            let args = e
                .args
                .iter()
                .map(|x| parse_physical_expr(x, registry, input_schema))
                .collect::<Result<Vec<_>, _>>()?;

            Arc::new(ScalarFunctionExpr::new(
                e.name.as_str(),
                scalar_fun,
                args,
                convert_required!(e.return_type)?,
                None,
                signature.type_signature.supports_zero_argument(),
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
            )?,
            parse_required_physical_expr(
                like_expr.pattern.as_deref(),
                registry,
                "pattern",
                input_schema,
            )?,
        )),
        ExprType::GetIndexedFieldExpr(get_indexed_field_expr) => {
            let field = match &get_indexed_field_expr.field {
                Some(protobuf::physical_get_indexed_field_expr_node::Field::NamedStructFieldExpr(named_struct_field_expr)) => GetFieldAccessExpr::NamedStructField{
                    name: convert_required!(named_struct_field_expr.name)?,
                },
                Some(protobuf::physical_get_indexed_field_expr_node::Field::ListIndexExpr(list_index_expr)) => GetFieldAccessExpr::ListIndex{
                    key: parse_required_physical_expr(
                        list_index_expr.key.as_deref(),
                        registry,
                        "key",
                        input_schema,
                    )?},
                Some(protobuf::physical_get_indexed_field_expr_node::Field::ListRangeExpr(list_range_expr)) => GetFieldAccessExpr::ListRange{
                    start: parse_required_physical_expr(
                        list_range_expr.start.as_deref(),
                        registry,
                        "start",
                        input_schema,
                    )?,
                    stop: parse_required_physical_expr(
                        list_range_expr.stop.as_deref(),
                        registry,
                        "stop",
                        input_schema
                    )?,
                    stride: parse_required_physical_expr(
                        list_range_expr.stride.as_deref(),
                        registry,
                        "stride",
                        input_schema
                    )?,
                },
                None =>
                    return Err(proto_error(
                    "Field must not be None",
                )),
            };

            Arc::new(GetIndexedFieldExpr::new(
                parse_required_physical_expr(
                    get_indexed_field_expr.arg.as_deref(),
                    registry,
                    "arg",
                    input_schema,
                )?,
                field,
            ))
        }
    };

    Ok(pexpr)
}

fn parse_required_physical_expr(
    expr: Option<&protobuf::PhysicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: &str,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.map(|e| parse_physical_expr(e, registry, input_schema))
        .transpose()?
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Missing required field {field:?}"))
        })
}

impl TryFrom<&protobuf::physical_window_expr_node::WindowFunction>
    for WindowFunctionDefinition
{
    type Error = DataFusionError;

    fn try_from(
        expr: &protobuf::physical_window_expr_node::WindowFunction,
    ) -> Result<Self, Self::Error> {
        match expr {
            protobuf::physical_window_expr_node::WindowFunction::AggrFunction(n) => {
                let f = protobuf::AggregateFunction::try_from(*n).map_err(|_| {
                    proto_error(format!(
                        "Received an unknown window aggregate function: {n}"
                    ))
                })?;

                Ok(WindowFunctionDefinition::AggregateFunction(f.into()))
            }
            protobuf::physical_window_expr_node::WindowFunction::BuiltInFunction(n) => {
                let f = protobuf::BuiltInWindowFunction::try_from(*n).map_err(|_| {
                    proto_error(format!(
                        "Received an unknown window builtin function: {n}"
                    ))
                })?;

                Ok(WindowFunctionDefinition::BuiltInWindowFunction(f.into()))
            }
        }
    }
}

pub fn parse_protobuf_hash_partitioning(
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
    registry: &dyn FunctionRegistry,
    input_schema: &Schema,
) -> Result<Option<Partitioning>> {
    match partitioning {
        Some(hash_part) => {
            let expr = hash_part
                .hash_expr
                .iter()
                .map(|e| parse_physical_expr(e, registry, input_schema))
                .collect::<Result<Vec<Arc<dyn PhysicalExpr>>, _>>()?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}

pub fn parse_protobuf_file_scan_config(
    proto: &protobuf::FileScanExecConf,
    registry: &dyn FunctionRegistry,
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

    // extract types of partition columns
    let table_partition_cols = proto
        .table_partition_cols
        .iter()
        .map(|col| Ok(schema.field_with_name(col)?.clone()))
        .collect::<Result<Vec<_>>>()?;

    let mut output_ordering = vec![];
    for node_collection in &proto.output_ordering {
        let sort_expr = node_collection
            .physical_sort_expr_nodes
            .iter()
            .map(|node| {
                let expr = node
                    .expr
                    .as_ref()
                    .map(|e| parse_physical_expr(e.as_ref(), registry, &schema))
                    .unwrap()?;
                Ok(PhysicalSortExpr {
                    expr,
                    options: SortOptions {
                        descending: !node.asc,
                        nulls_first: node.nulls_first,
                    },
                })
            })
            .collect::<Result<Vec<PhysicalSortExpr>>>()?;
        output_ordering.push(sort_expr);
    }

    Ok(FileScanConfig {
        object_store_url,
        file_schema: schema,
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

impl From<&protobuf::ColumnStats> for ColumnStatistics {
    fn from(cs: &protobuf::ColumnStats) -> ColumnStatistics {
        ColumnStatistics {
            null_count: if let Some(nc) = &cs.null_count {
                nc.clone().into()
            } else {
                Precision::Absent
            },
            max_value: if let Some(max) = &cs.max_value {
                max.clone().into()
            } else {
                Precision::Absent
            },
            min_value: if let Some(min) = &cs.min_value {
                min.clone().into()
            } else {
                Precision::Absent
            },
            distinct_count: if let Some(dc) = &cs.distinct_count {
                dc.clone().into()
            } else {
                Precision::Absent
            },
        }
    }
}

impl From<protobuf::Precision> for Precision<usize> {
    fn from(s: protobuf::Precision) -> Self {
        let Ok(precision_type) = s.precision_info.try_into() else {
            return Precision::Absent;
        };
        match precision_type {
            protobuf::PrecisionInfo::Exact => {
                if let Some(val) = s.val {
                    if let Ok(ScalarValue::UInt64(Some(val))) =
                        ScalarValue::try_from(&val)
                    {
                        Precision::Exact(val as usize)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Inexact => {
                if let Some(val) = s.val {
                    if let Ok(ScalarValue::UInt64(Some(val))) =
                        ScalarValue::try_from(&val)
                    {
                        Precision::Inexact(val as usize)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Absent => Precision::Absent,
        }
    }
}

impl From<protobuf::Precision> for Precision<ScalarValue> {
    fn from(s: protobuf::Precision) -> Self {
        let Ok(precision_type) = s.precision_info.try_into() else {
            return Precision::Absent;
        };
        match precision_type {
            protobuf::PrecisionInfo::Exact => {
                if let Some(val) = s.val {
                    if let Ok(val) = ScalarValue::try_from(&val) {
                        Precision::Exact(val)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Inexact => {
                if let Some(val) = s.val {
                    if let Ok(val) = ScalarValue::try_from(&val) {
                        Precision::Inexact(val)
                    } else {
                        Precision::Absent
                    }
                } else {
                    Precision::Absent
                }
            }
            protobuf::PrecisionInfo::Absent => Precision::Absent,
        }
    }
}

impl From<protobuf::JoinSide> for JoinSide {
    fn from(t: protobuf::JoinSide) -> Self {
        match t {
            protobuf::JoinSide::LeftSide => JoinSide::Left,
            protobuf::JoinSide::RightSide => JoinSide::Right,
        }
    }
}

impl TryFrom<&protobuf::Statistics> for Statistics {
    type Error = DataFusionError;

    fn try_from(s: &protobuf::Statistics) -> Result<Self, Self::Error> {
        // Keep it sync with Statistics::to_proto
        Ok(Statistics {
            num_rows: if let Some(nr) = &s.num_rows {
                nr.clone().into()
            } else {
                Precision::Absent
            },
            total_byte_size: if let Some(tbs) = &s.total_byte_size {
                tbs.clone().into()
            } else {
                Precision::Absent
            },
            // No column statistic (None) is encoded with empty array
            column_statistics: s.column_stats.iter().map(|s| s.into()).collect(),
        })
    }
}

impl TryFrom<&protobuf::JsonSink> for JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::JsonSink) -> Result<Self, Self::Error> {
        Ok(Self::new(convert_required!(value.config)?))
    }
}

#[cfg(feature = "parquet")]
impl TryFrom<&protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self::new(convert_required!(value.config)?))
    }
}

impl TryFrom<&protobuf::CsvSink> for CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::CsvSink) -> Result<Self, Self::Error> {
        Ok(Self::new(convert_required!(value.config)?))
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
            file_type_writer_options: convert_required!(conf.file_type_writer_options)?,
        })
    }
}

impl From<protobuf::CompressionTypeVariant> for CompressionTypeVariant {
    fn from(value: protobuf::CompressionTypeVariant) -> Self {
        match value {
            protobuf::CompressionTypeVariant::Gzip => Self::GZIP,
            protobuf::CompressionTypeVariant::Bzip2 => Self::BZIP2,
            protobuf::CompressionTypeVariant::Xz => Self::XZ,
            protobuf::CompressionTypeVariant::Zstd => Self::ZSTD,
            protobuf::CompressionTypeVariant::Uncompressed => Self::UNCOMPRESSED,
        }
    }
}

impl From<CompressionTypeVariant> for protobuf::CompressionTypeVariant {
    fn from(value: CompressionTypeVariant) -> Self {
        match value {
            CompressionTypeVariant::GZIP => Self::Gzip,
            CompressionTypeVariant::BZIP2 => Self::Bzip2,
            CompressionTypeVariant::XZ => Self::Xz,
            CompressionTypeVariant::ZSTD => Self::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => Self::Uncompressed,
        }
    }
}

impl TryFrom<&protobuf::FileTypeWriterOptions> for FileTypeWriterOptions {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::FileTypeWriterOptions) -> Result<Self, Self::Error> {
        let file_type = value
            .file_type
            .as_ref()
            .ok_or_else(|| proto_error("Missing required file_type field in protobuf"))?;

        match file_type {
            protobuf::file_type_writer_options::FileType::ArrowOptions(_) => {
                Ok(Self::Arrow(ArrowWriterOptions::new()))
            }

            protobuf::file_type_writer_options::FileType::JsonOptions(opts) => {
                let compression: CompressionTypeVariant = opts.compression().into();
                Ok(Self::JSON(JsonWriterOptions::new(compression)))
            }
            protobuf::file_type_writer_options::FileType::CsvOptions(opts) => {
                let write_options = csv_writer_options_from_proto(opts)?;
                let compression: CompressionTypeVariant = opts.compression().into();
                Ok(Self::CSV(CsvWriterOptions::new(write_options, compression)))
            }
            #[cfg(feature = "parquet")]
            protobuf::file_type_writer_options::FileType::ParquetOptions(opt) => {
                let props = opt.writer_properties.clone().unwrap_or_default();
                let writer_properties = writer_properties_from_proto(&props)?;
                Ok(Self::Parquet(ParquetWriterOptions::new(writer_properties)))
            }
        }
    }
}
