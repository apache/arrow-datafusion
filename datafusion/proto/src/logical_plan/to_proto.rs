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

//! Code to convert Arrow schemas and DataFusion logical plans to protocol buffer format, allowing
//! DataFusion logical plans to be serialized and transmitted between
//! processes.

use std::sync::Arc;

use crate::protobuf::{
    self,
    arrow_type::ArrowTypeEnum,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithStats, InitialLogicalPlan,
        InitialPhysicalPlan, InitialPhysicalPlanWithStats, OptimizedLogicalPlan,
        OptimizedPhysicalPlan,
    },
    AnalyzedLogicalPlanType, CubeNode, EmptyMessage, GroupingSetNode, LogicalExprList,
    OptimizedLogicalPlanType, OptimizedPhysicalPlanType, PlaceholderNode, RollupNode,
    UnionField, UnionValue,
};

use arrow::{
    array::ArrayRef,
    datatypes::{
        DataType, Field, IntervalMonthDayNanoType, IntervalUnit, Schema, SchemaRef,
        TimeUnit, UnionMode,
    },
    ipc::writer::{DictionaryTracker, IpcDataGenerator},
    record_batch::RecordBatch,
};
use datafusion_common::{
    Column, Constraint, Constraints, DFField, DFSchema, DFSchemaRef, OwnedTableReference,
    ScalarValue,
};
use datafusion_expr::expr::{
    self, AggregateFunctionDefinition, Alias, Between, BinaryExpr, Cast, GetFieldAccess,
    GetIndexedField, GroupingSet, InList, Like, Placeholder, ScalarFunction,
    ScalarFunctionDefinition, Sort, Unnest,
};
use datafusion_expr::{
    logical_plan::PlanType, logical_plan::StringifiedPlan, AggregateFunction,
    BuiltInWindowFunction, BuiltinScalarFunction, Expr, JoinConstraint, JoinType,
    TryCast, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};

use super::LogicalExtensionCodec;

#[derive(Debug)]
pub enum Error {
    General(String),

    InvalidScalarValue(ScalarValue),

    InvalidScalarType(DataType),

    InvalidTimeUnit(TimeUnit),

    UnsupportedScalarFunction(BuiltinScalarFunction),

    NotImplemented(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),
            Self::InvalidScalarValue(value) => {
                write!(f, "{value:?} is invalid as a DataFusion scalar value")
            }
            Self::InvalidScalarType(data_type) => {
                write!(f, "{data_type:?} is invalid as a DataFusion scalar type")
            }
            Self::InvalidTimeUnit(time_unit) => {
                write!(
                    f,
                    "Only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units, found: {time_unit:?}"
                )
            }
            Self::UnsupportedScalarFunction(function) => {
                write!(f, "Unsupported scalar function {function:?}")
            }
            Self::NotImplemented(s) => {
                write!(f, "Not implemented: {s}")
            }
        }
    }
}

impl TryFrom<&Field> for protobuf::Field {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        let arrow_type = field.data_type().try_into()?;
        Ok(Self {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(arrow_type)),
            nullable: field.is_nullable(),
            children: Vec::new(),
            metadata: field.metadata().clone(),
            dict_id: field.dict_id().unwrap_or(0),
            dict_ordered: field.dict_is_ordered().unwrap_or(false),
        })
    }
}

impl TryFrom<&DataType> for protobuf::ArrowType {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        let arrow_type_enum: ArrowTypeEnum = val.try_into()?;
        Ok(Self {
            arrow_type_enum: Some(arrow_type_enum),
        })
    }
}

impl TryFrom<&DataType> for protobuf::arrow_type::ArrowTypeEnum {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        let res = match val {
            DataType::Null => Self::None(EmptyMessage {}),
            DataType::Boolean => Self::Bool(EmptyMessage {}),
            DataType::Int8 => Self::Int8(EmptyMessage {}),
            DataType::Int16 => Self::Int16(EmptyMessage {}),
            DataType::Int32 => Self::Int32(EmptyMessage {}),
            DataType::Int64 => Self::Int64(EmptyMessage {}),
            DataType::UInt8 => Self::Uint8(EmptyMessage {}),
            DataType::UInt16 => Self::Uint16(EmptyMessage {}),
            DataType::UInt32 => Self::Uint32(EmptyMessage {}),
            DataType::UInt64 => Self::Uint64(EmptyMessage {}),
            DataType::Float16 => Self::Float16(EmptyMessage {}),
            DataType::Float32 => Self::Float32(EmptyMessage {}),
            DataType::Float64 => Self::Float64(EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => {
                Self::Timestamp(protobuf::Timestamp {
                    time_unit: protobuf::TimeUnit::from(time_unit) as i32,
                    timezone: timezone.as_deref().unwrap_or("").to_string(),
                })
            }
            DataType::Date32 => Self::Date32(EmptyMessage {}),
            DataType::Date64 => Self::Date64(EmptyMessage {}),
            DataType::Time32(time_unit) => {
                Self::Time32(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Time64(time_unit) => {
                Self::Time64(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Duration(time_unit) => {
                Self::Duration(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Interval(interval_unit) => {
                Self::Interval(protobuf::IntervalUnit::from(interval_unit) as i32)
            }
            DataType::Binary => Self::Binary(EmptyMessage {}),
            DataType::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            DataType::LargeBinary => Self::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => Self::Utf8(EmptyMessage {}),
            DataType::LargeUtf8 => Self::LargeUtf8(EmptyMessage {}),
            DataType::List(item_type) => Self::List(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::FixedSizeList(item_type, size) => {
                Self::FixedSizeList(Box::new(protobuf::FixedSizeList {
                    field_type: Some(Box::new(item_type.as_ref().try_into()?)),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => Self::LargeList(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::Struct(struct_fields) => Self::Struct(protobuf::Struct {
                sub_field_types: convert_arc_fields_to_proto_fields(struct_fields)?,
            }),
            DataType::Union(fields, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => protobuf::UnionMode::Sparse,
                    UnionMode::Dense => protobuf::UnionMode::Dense,
                };
                Self::Union(protobuf::Union {
                    union_types: convert_arc_fields_to_proto_fields(fields.iter().map(|(_, item)|item))?,
                    union_mode: union_mode.into(),
                    type_ids: fields.iter().map(|(x, _)| x as i32).collect(),
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                Self::Dictionary(Box::new(protobuf::Dictionary {
                    key: Some(Box::new(key_type.as_ref().try_into()?)),
                    value: Some(Box::new(value_type.as_ref().try_into()?)),
                }))
            }
            DataType::Decimal128(precision, scale) => Self::Decimal(protobuf::Decimal {
                precision: *precision as u32,
                scale: *scale as i32,
            }),
            DataType::Decimal256(_, _) => {
                return Err(Error::General("Proto serialization error: The Decimal256 data type is not yet supported".to_owned()))
            }
            DataType::Map(field, sorted) => {
                Self::Map(Box::new(
                    protobuf::Map {
                        field_type: Some(Box::new(field.as_ref().try_into()?)),
                        keys_sorted: *sorted,
                    }
                ))
            }
            DataType::RunEndEncoded(_, _) => {
                return Err(Error::General(
                    "Proto serialization error: The RunEndEncoded data type is not yet supported".to_owned()
                ))
            }
            DataType::Utf8View | DataType::BinaryView | DataType::ListView(_) | DataType::LargeListView(_) => {
                return Err(Error::General(format!("Proto serialization error: {val} not yet supported")))
            }
        };

        Ok(res)
    }
}

impl From<Column> for protobuf::Column {
    fn from(c: Column) -> Self {
        Self {
            relation: c.relation.map(|relation| protobuf::ColumnRelation {
                relation: relation.to_string(),
            }),
            name: c.name,
        }
    }
}

impl From<&Column> for protobuf::Column {
    fn from(c: &Column) -> Self {
        c.clone().into()
    }
}

impl TryFrom<&Schema> for protobuf::Schema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: convert_arc_fields_to_proto_fields(schema.fields())?,
            metadata: schema.metadata.clone(),
        })
    }
}

impl TryFrom<SchemaRef> for protobuf::Schema {
    type Error = Error;

    fn try_from(schema: SchemaRef) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: convert_arc_fields_to_proto_fields(schema.fields())?,
            metadata: schema.metadata.clone(),
        })
    }
}

impl TryFrom<&DFField> for protobuf::DfField {
    type Error = Error;

    fn try_from(f: &DFField) -> Result<Self, Self::Error> {
        Ok(Self {
            field: Some(f.field().as_ref().try_into()?),
            qualifier: f.qualifier().map(|r| protobuf::ColumnRelation {
                relation: r.to_string(),
            }),
        })
    }
}

impl TryFrom<&DFSchema> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchema) -> Result<Self, Self::Error> {
        let columns = s
            .fields()
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self {
            columns,
            metadata: s.metadata().clone(),
        })
    }
}

impl TryFrom<&DFSchemaRef> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchemaRef) -> Result<Self, Self::Error> {
        s.as_ref().try_into()
    }
}

impl From<&StringifiedPlan> for protobuf::StringifiedPlan {
    fn from(stringified_plan: &StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan.clone().plan_type {
                PlanType::InitialLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialLogicalPlan(EmptyMessage {})),
                }),
                PlanType::AnalyzedLogicalPlan { analyzer_name } => {
                    Some(protobuf::PlanType {
                        plan_type_enum: Some(AnalyzedLogicalPlan(
                            AnalyzedLogicalPlanType { analyzer_name },
                        )),
                    })
                }
                PlanType::FinalAnalyzedLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalAnalyzedLogicalPlan(EmptyMessage {})),
                }),
                PlanType::OptimizedLogicalPlan { optimizer_name } => {
                    Some(protobuf::PlanType {
                        plan_type_enum: Some(OptimizedLogicalPlan(
                            OptimizedLogicalPlanType { optimizer_name },
                        )),
                    })
                }
                PlanType::FinalLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalLogicalPlan(EmptyMessage {})),
                }),
                PlanType::InitialPhysicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialPhysicalPlan(EmptyMessage {})),
                }),
                PlanType::OptimizedPhysicalPlan { optimizer_name } => {
                    Some(protobuf::PlanType {
                        plan_type_enum: Some(OptimizedPhysicalPlan(
                            OptimizedPhysicalPlanType { optimizer_name },
                        )),
                    })
                }
                PlanType::FinalPhysicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalPhysicalPlan(EmptyMessage {})),
                }),
                PlanType::InitialPhysicalPlanWithStats => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialPhysicalPlanWithStats(EmptyMessage {})),
                }),
                PlanType::FinalPhysicalPlanWithStats => Some(protobuf::PlanType {
                    plan_type_enum: Some(FinalPhysicalPlanWithStats(EmptyMessage {})),
                }),
            },
            plan: stringified_plan.plan.to_string(),
        }
    }
}

impl From<&AggregateFunction> for protobuf::AggregateFunction {
    fn from(value: &AggregateFunction) -> Self {
        match value {
            AggregateFunction::Min => Self::Min,
            AggregateFunction::Max => Self::Max,
            AggregateFunction::Sum => Self::Sum,
            AggregateFunction::Avg => Self::Avg,
            AggregateFunction::BitAnd => Self::BitAnd,
            AggregateFunction::BitOr => Self::BitOr,
            AggregateFunction::BitXor => Self::BitXor,
            AggregateFunction::BoolAnd => Self::BoolAnd,
            AggregateFunction::BoolOr => Self::BoolOr,
            AggregateFunction::Count => Self::Count,
            AggregateFunction::ApproxDistinct => Self::ApproxDistinct,
            AggregateFunction::ArrayAgg => Self::ArrayAgg,
            AggregateFunction::Variance => Self::Variance,
            AggregateFunction::VariancePop => Self::VariancePop,
            AggregateFunction::Covariance => Self::Covariance,
            AggregateFunction::CovariancePop => Self::CovariancePop,
            AggregateFunction::Stddev => Self::Stddev,
            AggregateFunction::StddevPop => Self::StddevPop,
            AggregateFunction::Correlation => Self::Correlation,
            AggregateFunction::RegrSlope => Self::RegrSlope,
            AggregateFunction::RegrIntercept => Self::RegrIntercept,
            AggregateFunction::RegrCount => Self::RegrCount,
            AggregateFunction::RegrR2 => Self::RegrR2,
            AggregateFunction::RegrAvgx => Self::RegrAvgx,
            AggregateFunction::RegrAvgy => Self::RegrAvgy,
            AggregateFunction::RegrSXX => Self::RegrSxx,
            AggregateFunction::RegrSYY => Self::RegrSyy,
            AggregateFunction::RegrSXY => Self::RegrSxy,
            AggregateFunction::ApproxPercentileCont => Self::ApproxPercentileCont,
            AggregateFunction::ApproxPercentileContWithWeight => {
                Self::ApproxPercentileContWithWeight
            }
            AggregateFunction::ApproxMedian => Self::ApproxMedian,
            AggregateFunction::Grouping => Self::Grouping,
            AggregateFunction::Median => Self::Median,
            AggregateFunction::FirstValue => Self::FirstValueAgg,
            AggregateFunction::LastValue => Self::LastValueAgg,
            AggregateFunction::NthValue => Self::NthValueAgg,
            AggregateFunction::StringAgg => Self::StringAgg,
        }
    }
}

impl From<&BuiltInWindowFunction> for protobuf::BuiltInWindowFunction {
    fn from(value: &BuiltInWindowFunction) -> Self {
        match value {
            BuiltInWindowFunction::FirstValue => Self::FirstValue,
            BuiltInWindowFunction::LastValue => Self::LastValue,
            BuiltInWindowFunction::NthValue => Self::NthValue,
            BuiltInWindowFunction::Ntile => Self::Ntile,
            BuiltInWindowFunction::CumeDist => Self::CumeDist,
            BuiltInWindowFunction::PercentRank => Self::PercentRank,
            BuiltInWindowFunction::RowNumber => Self::RowNumber,
            BuiltInWindowFunction::Rank => Self::Rank,
            BuiltInWindowFunction::Lag => Self::Lag,
            BuiltInWindowFunction::Lead => Self::Lead,
            BuiltInWindowFunction::DenseRank => Self::DenseRank,
        }
    }
}

impl From<WindowFrameUnits> for protobuf::WindowFrameUnits {
    fn from(units: WindowFrameUnits) -> Self {
        match units {
            WindowFrameUnits::Rows => Self::Rows,
            WindowFrameUnits::Range => Self::Range,
            WindowFrameUnits::Groups => Self::Groups,
        }
    }
}

impl TryFrom<&WindowFrameBound> for protobuf::WindowFrameBound {
    type Error = Error;

    fn try_from(bound: &WindowFrameBound) -> Result<Self, Self::Error> {
        Ok(match bound {
            WindowFrameBound::CurrentRow => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow
                    .into(),
                bound_value: None,
            },
            WindowFrameBound::Preceding(v) => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
                bound_value: Some(v.try_into()?),
            },
            WindowFrameBound::Following(v) => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
                bound_value: Some(v.try_into()?),
            },
        })
    }
}

impl TryFrom<&WindowFrame> for protobuf::WindowFrame {
    type Error = Error;

    fn try_from(window: &WindowFrame) -> Result<Self, Self::Error> {
        Ok(Self {
            window_frame_units: protobuf::WindowFrameUnits::from(window.units).into(),
            start_bound: Some((&window.start_bound).try_into()?),
            end_bound: Some(protobuf::window_frame::EndBound::Bound(
                (&window.end_bound).try_into()?,
            )),
        })
    }
}

pub fn serialize_exprs<'a, I>(
    exprs: I,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<protobuf::LogicalExprNode>, Error>
where
    I: IntoIterator<Item = &'a Expr>,
{
    exprs
        .into_iter()
        .map(|expr| serialize_expr(expr, codec))
        .collect::<Result<Vec<_>, Error>>()
}

pub fn serialize_expr(
    expr: &Expr,
    codec: &dyn LogicalExtensionCodec,
) -> Result<protobuf::LogicalExprNode, Error> {
    use protobuf::logical_expr_node::ExprType;

    let expr_node = match expr {
        Expr::Column(c) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Column(c.into())),
        },
        Expr::Alias(Alias {
            expr,
            relation,
            name,
        }) => {
            let alias = Box::new(protobuf::AliasNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                relation: relation
                    .to_owned()
                    .map(|r| vec![r.into()])
                    .unwrap_or(vec![]),
                alias: name.to_owned(),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Alias(alias)),
            }
        }
        Expr::Literal(value) => {
            let pb_value: protobuf::ScalarValue = value.try_into()?;
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Literal(pb_value)),
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Try to linerize a nested binary expression tree of the same operator
            // into a flat vector of expressions.
            let mut exprs = vec![right.as_ref()];
            let mut current_expr = left.as_ref();
            while let Expr::BinaryExpr(BinaryExpr {
                left,
                op: current_op,
                right,
            }) = current_expr
            {
                if current_op == op {
                    exprs.push(right.as_ref());
                    current_expr = left.as_ref();
                } else {
                    break;
                }
            }
            exprs.push(current_expr);

            let binary_expr = protobuf::BinaryExprNode {
                // We need to reverse exprs since operands are expected to be
                // linearized from left innermost to right outermost (but while
                // traversing the chain we do the exact opposite).
                operands: serialize_exprs(exprs.into_iter().rev(), codec)?,
                op: format!("{op:?}"),
            };
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::BinaryExpr(binary_expr)),
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if *case_insensitive {
                let pb = Box::new(protobuf::ILikeNode {
                    negated: *negated,
                    expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                    pattern: Some(Box::new(serialize_expr(pattern.as_ref(), codec)?)),
                    escape_char: escape_char.map(|ch| ch.to_string()).unwrap_or_default(),
                });

                protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Ilike(pb)),
                }
            } else {
                let pb = Box::new(protobuf::LikeNode {
                    negated: *negated,
                    expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                    pattern: Some(Box::new(serialize_expr(pattern.as_ref(), codec)?)),
                    escape_char: escape_char.map(|ch| ch.to_string()).unwrap_or_default(),
                });

                protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Like(pb)),
                }
            }
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive: _,
        }) => {
            let pb = Box::new(protobuf::SimilarToNode {
                negated: *negated,
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                pattern: Some(Box::new(serialize_expr(pattern.as_ref(), codec)?)),
                escape_char: escape_char.map(|ch| ch.to_string()).unwrap_or_default(),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::SimilarTo(pb)),
            }
        }
        Expr::WindowFunction(expr::WindowFunction {
            ref fun,
            ref args,
            ref partition_by,
            ref order_by,
            ref window_frame,
            // TODO: support null treatment in proto
            null_treatment: _,
        }) => {
            let window_function = match fun {
                WindowFunctionDefinition::AggregateFunction(fun) => {
                    protobuf::window_expr_node::WindowFunction::AggrFunction(
                        protobuf::AggregateFunction::from(fun).into(),
                    )
                }
                WindowFunctionDefinition::BuiltInWindowFunction(fun) => {
                    protobuf::window_expr_node::WindowFunction::BuiltInFunction(
                        protobuf::BuiltInWindowFunction::from(fun).into(),
                    )
                }
                WindowFunctionDefinition::AggregateUDF(aggr_udf) => {
                    protobuf::window_expr_node::WindowFunction::Udaf(
                        aggr_udf.name().to_string(),
                    )
                }
                WindowFunctionDefinition::WindowUDF(window_udf) => {
                    protobuf::window_expr_node::WindowFunction::Udwf(
                        window_udf.name().to_string(),
                    )
                }
            };
            let arg_expr: Option<Box<protobuf::LogicalExprNode>> = if !args.is_empty() {
                let arg = &args[0];
                Some(Box::new(serialize_expr(arg, codec)?))
            } else {
                None
            };
            let partition_by = serialize_exprs(partition_by, codec)?;
            let order_by = serialize_exprs(order_by, codec)?;

            let window_frame: Option<protobuf::WindowFrame> =
                Some(window_frame.try_into()?);
            let window_expr = Box::new(protobuf::WindowExprNode {
                expr: arg_expr,
                window_function: Some(window_function),
                partition_by,
                order_by,
                window_frame,
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::WindowExpr(window_expr)),
            }
        }
        Expr::AggregateFunction(expr::AggregateFunction {
            ref func_def,
            ref args,
            ref distinct,
            ref filter,
            ref order_by,
            null_treatment: _,
        }) => match func_def {
            AggregateFunctionDefinition::BuiltIn(fun) => {
                let aggr_function = match fun {
                    AggregateFunction::ApproxDistinct => {
                        protobuf::AggregateFunction::ApproxDistinct
                    }
                    AggregateFunction::ApproxPercentileCont => {
                        protobuf::AggregateFunction::ApproxPercentileCont
                    }
                    AggregateFunction::ApproxPercentileContWithWeight => {
                        protobuf::AggregateFunction::ApproxPercentileContWithWeight
                    }
                    AggregateFunction::ArrayAgg => protobuf::AggregateFunction::ArrayAgg,
                    AggregateFunction::Min => protobuf::AggregateFunction::Min,
                    AggregateFunction::Max => protobuf::AggregateFunction::Max,
                    AggregateFunction::Sum => protobuf::AggregateFunction::Sum,
                    AggregateFunction::BitAnd => protobuf::AggregateFunction::BitAnd,
                    AggregateFunction::BitOr => protobuf::AggregateFunction::BitOr,
                    AggregateFunction::BitXor => protobuf::AggregateFunction::BitXor,
                    AggregateFunction::BoolAnd => protobuf::AggregateFunction::BoolAnd,
                    AggregateFunction::BoolOr => protobuf::AggregateFunction::BoolOr,
                    AggregateFunction::Avg => protobuf::AggregateFunction::Avg,
                    AggregateFunction::Count => protobuf::AggregateFunction::Count,
                    AggregateFunction::Variance => protobuf::AggregateFunction::Variance,
                    AggregateFunction::VariancePop => {
                        protobuf::AggregateFunction::VariancePop
                    }
                    AggregateFunction::Covariance => {
                        protobuf::AggregateFunction::Covariance
                    }
                    AggregateFunction::CovariancePop => {
                        protobuf::AggregateFunction::CovariancePop
                    }
                    AggregateFunction::Stddev => protobuf::AggregateFunction::Stddev,
                    AggregateFunction::StddevPop => {
                        protobuf::AggregateFunction::StddevPop
                    }
                    AggregateFunction::Correlation => {
                        protobuf::AggregateFunction::Correlation
                    }
                    AggregateFunction::RegrSlope => {
                        protobuf::AggregateFunction::RegrSlope
                    }
                    AggregateFunction::RegrIntercept => {
                        protobuf::AggregateFunction::RegrIntercept
                    }
                    AggregateFunction::RegrR2 => protobuf::AggregateFunction::RegrR2,
                    AggregateFunction::RegrAvgx => protobuf::AggregateFunction::RegrAvgx,
                    AggregateFunction::RegrAvgy => protobuf::AggregateFunction::RegrAvgy,
                    AggregateFunction::RegrCount => {
                        protobuf::AggregateFunction::RegrCount
                    }
                    AggregateFunction::RegrSXX => protobuf::AggregateFunction::RegrSxx,
                    AggregateFunction::RegrSYY => protobuf::AggregateFunction::RegrSyy,
                    AggregateFunction::RegrSXY => protobuf::AggregateFunction::RegrSxy,
                    AggregateFunction::ApproxMedian => {
                        protobuf::AggregateFunction::ApproxMedian
                    }
                    AggregateFunction::Grouping => protobuf::AggregateFunction::Grouping,
                    AggregateFunction::Median => protobuf::AggregateFunction::Median,
                    AggregateFunction::FirstValue => {
                        protobuf::AggregateFunction::FirstValueAgg
                    }
                    AggregateFunction::LastValue => {
                        protobuf::AggregateFunction::LastValueAgg
                    }
                    AggregateFunction::NthValue => {
                        protobuf::AggregateFunction::NthValueAgg
                    }
                    AggregateFunction::StringAgg => {
                        protobuf::AggregateFunction::StringAgg
                    }
                };

                let aggregate_expr = protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: serialize_exprs(args, codec)?,
                    distinct: *distinct,
                    filter: match filter {
                        Some(e) => Some(Box::new(serialize_expr(e, codec)?)),
                        None => None,
                    },
                    order_by: match order_by {
                        Some(e) => serialize_exprs(e, codec)?,
                        None => vec![],
                    },
                };
                protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::AggregateExpr(Box::new(aggregate_expr))),
                }
            }
            AggregateFunctionDefinition::UDF(fun) => protobuf::LogicalExprNode {
                expr_type: Some(ExprType::AggregateUdfExpr(Box::new(
                    protobuf::AggregateUdfExprNode {
                        fun_name: fun.name().to_string(),
                        args: serialize_exprs(args, codec)?,
                        filter: match filter {
                            Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                            None => None,
                        },
                        order_by: match order_by {
                            Some(e) => serialize_exprs(e, codec)?,
                            None => vec![],
                        },
                    },
                ))),
            },
            AggregateFunctionDefinition::Name(_) => {
                return Err(Error::NotImplemented(
                    "Proto serialization error: Trying to serialize a unresolved function"
                        .to_string(),
                ));
            }
        },

        Expr::ScalarVariable(_, _) => {
            return Err(Error::General(
                "Proto serialization error: Scalar Variable not supported".to_string(),
            ))
        }
        Expr::ScalarFunction(ScalarFunction { func_def, args }) => {
            let args = serialize_exprs(args, codec)?;
            match func_def {
                ScalarFunctionDefinition::BuiltIn(fun) => {
                    let fun: protobuf::ScalarFunction = fun.try_into()?;
                    protobuf::LogicalExprNode {
                        expr_type: Some(ExprType::ScalarFunction(
                            protobuf::ScalarFunctionNode {
                                fun: fun.into(),
                                args,
                            },
                        )),
                    }
                }
                ScalarFunctionDefinition::UDF(fun) => {
                    let mut buf = Vec::new();
                    let _ = codec.try_encode_udf(fun.as_ref(), &mut buf);

                    let fun_definition = if buf.is_empty() { None } else { Some(buf) };

                    protobuf::LogicalExprNode {
                        expr_type: Some(ExprType::ScalarUdfExpr(
                            protobuf::ScalarUdfExprNode {
                                fun_name: fun.name().to_string(),
                                fun_definition,
                                args,
                            },
                        )),
                    }
                }
                ScalarFunctionDefinition::Name(_) => {
                    return Err(Error::NotImplemented(
                    "Proto serialization error: Trying to serialize a unresolved function"
                        .to_string(),
                ));
                }
            }
        }
        Expr::Not(expr) => {
            let expr = Box::new(protobuf::Not {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::NotExpr(expr)),
            }
        }
        Expr::IsNull(expr) => {
            let expr = Box::new(protobuf::IsNull {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNullExpr(expr)),
            }
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(protobuf::IsNotNull {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotNullExpr(expr)),
            }
        }
        Expr::IsTrue(expr) => {
            let expr = Box::new(protobuf::IsTrue {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsTrue(expr)),
            }
        }
        Expr::IsFalse(expr) => {
            let expr = Box::new(protobuf::IsFalse {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsFalse(expr)),
            }
        }
        Expr::IsUnknown(expr) => {
            let expr = Box::new(protobuf::IsUnknown {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsUnknown(expr)),
            }
        }
        Expr::IsNotTrue(expr) => {
            let expr = Box::new(protobuf::IsNotTrue {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotTrue(expr)),
            }
        }
        Expr::IsNotFalse(expr) => {
            let expr = Box::new(protobuf::IsNotFalse {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotFalse(expr)),
            }
        }
        Expr::IsNotUnknown(expr) => {
            let expr = Box::new(protobuf::IsNotUnknown {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotUnknown(expr)),
            }
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = Box::new(protobuf::BetweenNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                negated: *negated,
                low: Some(Box::new(serialize_expr(low.as_ref(), codec)?)),
                high: Some(Box::new(serialize_expr(high.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Between(expr)),
            }
        }
        Expr::Case(case) => {
            let when_then_expr = case
                .when_then_expr
                .iter()
                .map(|(w, t)| {
                    Ok(protobuf::WhenThen {
                        when_expr: Some(serialize_expr(w.as_ref(), codec)?),
                        then_expr: Some(serialize_expr(t.as_ref(), codec)?),
                    })
                })
                .collect::<Result<Vec<protobuf::WhenThen>, Error>>()?;
            let expr = Box::new(protobuf::CaseNode {
                expr: match &case.expr {
                    Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                    None => None,
                },
                when_then_expr,
                else_expr: match &case.else_expr {
                    Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                    None => None,
                },
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Case(expr)),
            }
        }
        Expr::Cast(Cast { expr, data_type }) => {
            let expr = Box::new(protobuf::CastNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                arrow_type: Some(data_type.try_into()?),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Cast(expr)),
            }
        }
        Expr::TryCast(TryCast { expr, data_type }) => {
            let expr = Box::new(protobuf::TryCastNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                arrow_type: Some(data_type.try_into()?),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::TryCast(expr)),
            }
        }
        Expr::Sort(Sort {
            expr,
            asc,
            nulls_first,
        }) => {
            let expr = Box::new(protobuf::SortExprNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                asc: *asc,
                nulls_first: *nulls_first,
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Sort(expr)),
            }
        }
        Expr::Negative(expr) => {
            let expr = Box::new(protobuf::NegativeNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Negative(expr)),
            }
        }
        Expr::Unnest(Unnest { exprs }) => {
            let expr = protobuf::Unnest {
                exprs: serialize_exprs(exprs, codec)?,
            };
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Unnest(expr)),
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = Box::new(protobuf::InListNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                list: serialize_exprs(list, codec)?,
                negated: *negated,
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::InList(expr)),
            }
        }
        Expr::Wildcard { qualifier } => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Wildcard(protobuf::Wildcard {
                qualifier: qualifier.clone().unwrap_or("".to_string()),
            })),
        },
        Expr::ScalarSubquery(_)
        | Expr::InSubquery(_)
        | Expr::Exists { .. }
        | Expr::OuterReferenceColumn { .. } => {
            // we would need to add logical plan operators to datafusion.proto to support this
            // see discussion in https://github.com/apache/arrow-datafusion/issues/2565
            return Err(Error::General("Proto serialization error: Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists { .. } | Exp:OuterReferenceColumn not supported".to_string()));
        }
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let field = match field {
                GetFieldAccess::NamedStructField { name } => {
                    protobuf::get_indexed_field::Field::NamedStructField(
                        protobuf::NamedStructField {
                            name: Some(name.try_into()?),
                        },
                    )
                }
                GetFieldAccess::ListIndex { key } => {
                    protobuf::get_indexed_field::Field::ListIndex(Box::new(
                        protobuf::ListIndex {
                            key: Some(Box::new(serialize_expr(key.as_ref(), codec)?)),
                        },
                    ))
                }
                GetFieldAccess::ListRange {
                    start,
                    stop,
                    stride,
                } => protobuf::get_indexed_field::Field::ListRange(Box::new(
                    protobuf::ListRange {
                        start: Some(Box::new(serialize_expr(start.as_ref(), codec)?)),
                        stop: Some(Box::new(serialize_expr(stop.as_ref(), codec)?)),
                        stride: Some(Box::new(serialize_expr(stride.as_ref(), codec)?)),
                    },
                )),
            };

            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::GetIndexedField(Box::new(
                    protobuf::GetIndexedField {
                        expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                        field: Some(field),
                    },
                ))),
            }
        }

        Expr::GroupingSet(GroupingSet::Cube(exprs)) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Cube(CubeNode {
                expr: serialize_exprs(exprs, codec)?,
            })),
        },
        Expr::GroupingSet(GroupingSet::Rollup(exprs)) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Rollup(RollupNode {
                expr: serialize_exprs(exprs, codec)?,
            })),
        },
        Expr::GroupingSet(GroupingSet::GroupingSets(exprs)) => {
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::GroupingSet(GroupingSetNode {
                    expr: exprs
                        .iter()
                        .map(|expr_list| {
                            Ok(LogicalExprList {
                                expr: serialize_exprs(expr_list, codec)?,
                            })
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                })),
            }
        }
        Expr::Placeholder(Placeholder { id, data_type }) => {
            let data_type = match data_type {
                Some(data_type) => Some(data_type.try_into()?),
                None => None,
            };
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Placeholder(PlaceholderNode {
                    id: id.clone(),
                    data_type,
                })),
            }
        }
    };

    Ok(expr_node)
}

impl TryFrom<&ScalarValue> for protobuf::ScalarValue {
    type Error = Error;

    fn try_from(val: &ScalarValue) -> Result<Self, Self::Error> {
        use protobuf::scalar_value::Value;

        let data_type = val.data_type();
        match val {
            ScalarValue::Boolean(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::BoolValue(*s))
            }
            ScalarValue::Float32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Float32Value(*s))
            }
            ScalarValue::Float64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Float64Value(*s))
            }
            ScalarValue::Int8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Int8Value(*s as i32)
                })
            }
            ScalarValue::Int16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Int16Value(*s as i32)
                })
            }
            ScalarValue::Int32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Int32Value(*s))
            }
            ScalarValue::Int64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Int64Value(*s))
            }
            ScalarValue::UInt8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Uint8Value(*s as u32)
                })
            }
            ScalarValue::UInt16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Uint16Value(*s as u32)
                })
            }
            ScalarValue::UInt32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Uint32Value(*s))
            }
            ScalarValue::UInt64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Uint64Value(*s))
            }
            ScalarValue::Utf8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Utf8Value(s.to_owned())
                })
            }
            ScalarValue::LargeUtf8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::LargeUtf8Value(s.to_owned())
                })
            }
            ScalarValue::List(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::LargeList(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::FixedSizeList(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Struct(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Date32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Date32Value(*s))
            }
            ScalarValue::TimestampMicrosecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMicrosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::TimestampNanosecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeNanosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::Decimal128(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal128Value(protobuf::Decimal128 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(protobuf::scalar_value::Value::NullValue(
                        (&data_type).try_into()?,
                    )),
                }),
            },
            ScalarValue::Decimal256(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal256Value(protobuf::Decimal256 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(protobuf::scalar_value::Value::NullValue(
                        (&data_type).try_into()?,
                    )),
                }),
            },
            ScalarValue::Date64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Date64Value(*s))
            }
            ScalarValue::TimestampSecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeSecondValue(*s),
                        ),
                    })
                })
            }
            ScalarValue::TimestampMillisecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMillisecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::IntervalYearMonth(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::IntervalYearmonthValue(*s)
                })
            }
            ScalarValue::IntervalDayTime(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::IntervalDaytimeValue(*s)
                })
            }
            ScalarValue::Null => Ok(protobuf::ScalarValue {
                value: Some(Value::NullValue((&data_type).try_into()?)),
            }),

            ScalarValue::Binary(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::BinaryValue(s.to_owned())
                })
            }
            ScalarValue::LargeBinary(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::LargeBinaryValue(s.to_owned())
                })
            }
            ScalarValue::FixedSizeBinary(length, val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::FixedSizeBinaryValue(protobuf::ScalarFixedSizeBinary {
                        values: s.to_owned(),
                        length: *length,
                    })
                })
            }

            ScalarValue::Time32Second(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time32Value(protobuf::ScalarTime32Value {
                        value: Some(
                            protobuf::scalar_time32_value::Value::Time32SecondValue(*v),
                        ),
                    })
                })
            }

            ScalarValue::Time32Millisecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time32Value(protobuf::ScalarTime32Value {
                        value: Some(
                            protobuf::scalar_time32_value::Value::Time32MillisecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::Time64Microsecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time64Value(protobuf::ScalarTime64Value {
                        value: Some(
                            protobuf::scalar_time64_value::Value::Time64MicrosecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::Time64Nanosecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time64Value(protobuf::ScalarTime64Value {
                        value: Some(
                            protobuf::scalar_time64_value::Value::Time64NanosecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::IntervalMonthDayNano(v) => {
                let value = if let Some(v) = v {
                    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
                    Value::IntervalMonthDayNano(protobuf::IntervalMonthDayNanoValue {
                        months,
                        days,
                        nanos,
                    })
                } else {
                    Value::NullValue((&data_type).try_into()?)
                };

                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::DurationSecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationSecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationMillisecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationMillisecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationMicrosecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationMicrosecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationNanosecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationNanosecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::Union(val, df_fields, mode) => {
                let mut fields = Vec::<UnionField>::with_capacity(df_fields.len());
                for (id, field) in df_fields.iter() {
                    let field_id = id as i32;
                    let field = Some(field.as_ref().try_into()?);
                    let field = UnionField { field_id, field };
                    fields.push(field);
                }
                let mode = match mode {
                    UnionMode::Sparse => 0,
                    UnionMode::Dense => 1,
                };
                let value = match val {
                    None => None,
                    Some((_id, v)) => Some(Box::new(v.as_ref().try_into()?)),
                };
                let val = UnionValue {
                    value_id: val.as_ref().map(|(id, _v)| *id as i32).unwrap_or(0),
                    value,
                    fields,
                    mode,
                };
                let val = Value::UnionValue(Box::new(val));
                let val = protobuf::ScalarValue { value: Some(val) };
                Ok(val)
            }

            ScalarValue::Dictionary(index_type, val) => {
                let value: protobuf::ScalarValue = val.as_ref().try_into()?;
                Ok(protobuf::ScalarValue {
                    value: Some(Value::DictionaryValue(Box::new(
                        protobuf::ScalarDictionaryValue {
                            index_type: Some(index_type.as_ref().try_into()?),
                            value: Some(Box::new(value)),
                        },
                    ))),
                })
            }
        }
    }
}

impl TryFrom<&BuiltinScalarFunction> for protobuf::ScalarFunction {
    type Error = Error;

    fn try_from(scalar: &BuiltinScalarFunction) -> Result<Self, Self::Error> {
        let scalar_function = match scalar {
            BuiltinScalarFunction::Sqrt => Self::Sqrt,
            BuiltinScalarFunction::Cbrt => Self::Cbrt,
            BuiltinScalarFunction::Sin => Self::Sin,
            BuiltinScalarFunction::Cos => Self::Cos,
            BuiltinScalarFunction::Cot => Self::Cot,
            BuiltinScalarFunction::Sinh => Self::Sinh,
            BuiltinScalarFunction::Cosh => Self::Cosh,
            BuiltinScalarFunction::Atan => Self::Atan,
            BuiltinScalarFunction::Asinh => Self::Asinh,
            BuiltinScalarFunction::Acosh => Self::Acosh,
            BuiltinScalarFunction::Atanh => Self::Atanh,
            BuiltinScalarFunction::Exp => Self::Exp,
            BuiltinScalarFunction::Factorial => Self::Factorial,
            BuiltinScalarFunction::Gcd => Self::Gcd,
            BuiltinScalarFunction::Lcm => Self::Lcm,
            BuiltinScalarFunction::Log => Self::Log,
            BuiltinScalarFunction::Ln => Self::Ln,
            BuiltinScalarFunction::Log10 => Self::Log10,
            BuiltinScalarFunction::Degrees => Self::Degrees,
            BuiltinScalarFunction::Radians => Self::Radians,
            BuiltinScalarFunction::Floor => Self::Floor,
            BuiltinScalarFunction::Ceil => Self::Ceil,
            BuiltinScalarFunction::Round => Self::Round,
            BuiltinScalarFunction::Trunc => Self::Trunc,
            BuiltinScalarFunction::Concat => Self::Concat,
            BuiltinScalarFunction::Log2 => Self::Log2,
            BuiltinScalarFunction::Signum => Self::Signum,
            BuiltinScalarFunction::ConcatWithSeparator => Self::ConcatWithSeparator,
            BuiltinScalarFunction::EndsWith => Self::EndsWith,
            BuiltinScalarFunction::InitCap => Self::InitCap,
            BuiltinScalarFunction::Random => Self::Random,
            BuiltinScalarFunction::Strpos => Self::Strpos,
            BuiltinScalarFunction::Substr => Self::Substr,
            BuiltinScalarFunction::Translate => Self::Translate,
            BuiltinScalarFunction::Coalesce => Self::Coalesce,
            BuiltinScalarFunction::Pi => Self::Pi,
            BuiltinScalarFunction::Power => Self::Power,
            BuiltinScalarFunction::Atan2 => Self::Atan2,
            BuiltinScalarFunction::Nanvl => Self::Nanvl,
            BuiltinScalarFunction::Iszero => Self::Iszero,
            BuiltinScalarFunction::SubstrIndex => Self::SubstrIndex,
            BuiltinScalarFunction::FindInSet => Self::FindInSet,
        };

        Ok(scalar_function)
    }
}

impl From<&TimeUnit> for protobuf::TimeUnit {
    fn from(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::Millisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
}

impl From<&IntervalUnit> for protobuf::IntervalUnit {
    fn from(interval_unit: &IntervalUnit) -> Self {
        match interval_unit {
            IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => protobuf::IntervalUnit::MonthDayNano,
        }
    }
}

impl From<OwnedTableReference> for protobuf::OwnedTableReference {
    fn from(t: OwnedTableReference) -> Self {
        use protobuf::owned_table_reference::TableReferenceEnum;
        let table_reference_enum = match t {
            OwnedTableReference::Bare { table } => {
                TableReferenceEnum::Bare(protobuf::BareTableReference {
                    table: table.to_string(),
                })
            }
            OwnedTableReference::Partial { schema, table } => {
                TableReferenceEnum::Partial(protobuf::PartialTableReference {
                    schema: schema.to_string(),
                    table: table.to_string(),
                })
            }
            OwnedTableReference::Full {
                catalog,
                schema,
                table,
            } => TableReferenceEnum::Full(protobuf::FullTableReference {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            }),
        };

        protobuf::OwnedTableReference {
            table_reference_enum: Some(table_reference_enum),
        }
    }
}

impl From<JoinType> for protobuf::JoinType {
    fn from(t: JoinType) -> Self {
        match t {
            JoinType::Inner => protobuf::JoinType::Inner,
            JoinType::Left => protobuf::JoinType::Left,
            JoinType::Right => protobuf::JoinType::Right,
            JoinType::Full => protobuf::JoinType::Full,
            JoinType::LeftSemi => protobuf::JoinType::Leftsemi,
            JoinType::RightSemi => protobuf::JoinType::Rightsemi,
            JoinType::LeftAnti => protobuf::JoinType::Leftanti,
            JoinType::RightAnti => protobuf::JoinType::Rightanti,
        }
    }
}

impl From<JoinConstraint> for protobuf::JoinConstraint {
    fn from(t: JoinConstraint) -> Self {
        match t {
            JoinConstraint::On => protobuf::JoinConstraint::On,
            JoinConstraint::Using => protobuf::JoinConstraint::Using,
        }
    }
}

impl From<Constraints> for protobuf::Constraints {
    fn from(value: Constraints) -> Self {
        let constraints = value.into_iter().map(|item| item.into()).collect();
        protobuf::Constraints { constraints }
    }
}

impl From<Constraint> for protobuf::Constraint {
    fn from(value: Constraint) -> Self {
        let res = match value {
            Constraint::PrimaryKey(indices) => {
                let indices = indices.into_iter().map(|item| item as u64).collect();
                protobuf::constraint::ConstraintMode::PrimaryKey(
                    protobuf::PrimaryKeyConstraint { indices },
                )
            }
            Constraint::Unique(indices) => {
                let indices = indices.into_iter().map(|item| item as u64).collect();
                protobuf::constraint::ConstraintMode::PrimaryKey(
                    protobuf::PrimaryKeyConstraint { indices },
                )
            }
        };
        protobuf::Constraint {
            constraint_mode: Some(res),
        }
    }
}

/// Creates a scalar protobuf value from an optional value (T), and
/// encoding None as the appropriate datatype
fn create_proto_scalar<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: Option<&I>,
    null_arrow_type: &DataType,
    constructor: T,
) -> Result<protobuf::ScalarValue, Error> {
    let value = v
        .map(constructor)
        .unwrap_or(protobuf::scalar_value::Value::NullValue(
            null_arrow_type.try_into()?,
        ));

    Ok(protobuf::ScalarValue { value: Some(value) })
}

// ScalarValue::List / FixedSizeList / LargeList / Struct are serialized using
// Arrow IPC messages as a single column RecordBatch
fn encode_scalar_nested_value(
    arr: ArrayRef,
    val: &ScalarValue,
) -> Result<protobuf::ScalarValue, Error> {
    let batch = RecordBatch::try_from_iter(vec![("field_name", arr)]).map_err(|e| {
        Error::General(format!(
            "Error creating temporary batch while encoding ScalarValue::List: {e}"
        ))
    })?;

    let gen = IpcDataGenerator {};
    let mut dict_tracker = DictionaryTracker::new(false);
    let (_, encoded_message) = gen
        .encoded_batch(&batch, &mut dict_tracker, &Default::default())
        .map_err(|e| {
            Error::General(format!("Error encoding ScalarValue::List as IPC: {e}"))
        })?;

    let schema: protobuf::Schema = batch.schema().try_into()?;

    let scalar_list_value = protobuf::ScalarNestedValue {
        ipc_message: encoded_message.ipc_message,
        arrow_data: encoded_message.arrow_data,
        schema: Some(schema),
    };

    match val {
        ScalarValue::List(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::ListValue(scalar_list_value)),
        }),
        ScalarValue::LargeList(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::LargeListValue(
                scalar_list_value,
            )),
        }),
        ScalarValue::FixedSizeList(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::FixedSizeListValue(
                scalar_list_value,
            )),
        }),
        ScalarValue::Struct(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::StructValue(
                scalar_list_value,
            )),
        }),
        _ => unreachable!(),
    }
}

/// Converts a vector of `Arc<arrow::Field>`s to `protobuf::Field`s
fn convert_arc_fields_to_proto_fields<'a, I>(
    fields: I,
) -> Result<Vec<protobuf::Field>, Error>
where
    I: IntoIterator<Item = &'a Arc<Field>>,
{
    fields
        .into_iter()
        .map(|field| field.as_ref().try_into())
        .collect::<Result<Vec<_>, Error>>()
}
