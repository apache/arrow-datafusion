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

use crate::protobuf::{
    self,
    plan_type::PlanTypeEnum::{
        FinalLogicalPlan, FinalPhysicalPlan, InitialLogicalPlan, InitialPhysicalPlan,
        OptimizedLogicalPlan, OptimizedPhysicalPlan,
    },
    CubeNode, EmptyMessage, GroupingSetNode, LogicalExprList, OptimizedLogicalPlanType,
    OptimizedPhysicalPlanType, RollupNode,
};
use arrow::datatypes::{
    DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode,
};
use datafusion_common::{Column, DFField, DFSchemaRef, ScalarValue};
use datafusion_expr::expr::GroupingSet;
use datafusion_expr::{
    logical_plan::PlanType, logical_plan::StringifiedPlan, AggregateFunction,
    BuiltInWindowFunction, BuiltinScalarFunction, Expr, WindowFrame, WindowFrameBound,
    WindowFrameUnits, WindowFunction,
};

#[derive(Debug)]
pub enum Error {
    General(String),

    InconsistentListTyping(DataType, DataType),

    InconsistentListDesignated {
        value: ScalarValue,
        designated: DataType,
    },

    InvalidScalarValue(ScalarValue),

    InvalidScalarType(DataType),

    InvalidTimeUnit(TimeUnit),

    UnsupportedScalarFunction(BuiltinScalarFunction),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {}", desc),
            Self::InconsistentListTyping(type1, type2) => {
                write!(
                    f,
                    "Lists with inconsistent typing; {:?} and {:?} found within list",
                    type1, type2,
                )
            }
            Self::InconsistentListDesignated { value, designated } => {
                write!(
                    f,
                    "Value {:?} was inconsistent with designated type {:?}",
                    value, designated
                )
            }
            Self::InvalidScalarValue(value) => {
                write!(f, "{:?} is invalid as a DataFusion scalar value", value)
            }
            Self::InvalidScalarType(data_type) => {
                write!(f, "{:?} is invalid as a DataFusion scalar type", data_type)
            }
            Self::InvalidTimeUnit(time_unit) => {
                write!(
                    f,
                    "Only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units, found: {:?}",
                    time_unit
                )
            }
            Self::UnsupportedScalarFunction(function) => {
                write!(f, "Unsupported scalar function {:?}", function)
            }
        }
    }
}

impl Error {
    fn inconsistent_list_typing(type1: &DataType, type2: &DataType) -> Self {
        Self::InconsistentListTyping(type1.to_owned(), type2.to_owned())
    }

    fn inconsistent_list_designated(value: &ScalarValue, designated: &DataType) -> Self {
        Self::InconsistentListDesignated {
            value: value.to_owned(),
            designated: designated.to_owned(),
        }
    }

    fn invalid_scalar_value(value: &ScalarValue) -> Self {
        Self::InvalidScalarValue(value.to_owned())
    }

    fn invalid_scalar_type(data_type: &DataType) -> Self {
        Self::InvalidScalarType(data_type.to_owned())
    }

    fn invalid_time_unit(time_unit: &TimeUnit) -> Self {
        Self::InvalidTimeUnit(time_unit.to_owned())
    }
}

impl From<&Field> for protobuf::Field {
    fn from(field: &Field) -> Self {
        Self {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(field.data_type().into())),
            nullable: field.is_nullable(),
            children: Vec::new(),
        }
    }
}

impl From<&DataType> for protobuf::ArrowType {
    fn from(val: &DataType) -> Self {
        Self {
            arrow_type_enum: Some(val.into()),
        }
    }
}

impl From<&DataType> for protobuf::arrow_type::ArrowTypeEnum {
    fn from(val: &DataType) -> Self {
        match val {
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
                    timezone: timezone.to_owned().unwrap_or_default(),
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
                field_type: Some(Box::new(item_type.as_ref().into())),
            })),
            DataType::FixedSizeList(item_type, size) => {
                Self::FixedSizeList(Box::new(protobuf::FixedSizeList {
                    field_type: Some(Box::new(item_type.as_ref().into())),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => Self::LargeList(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().into())),
            })),
            DataType::Struct(struct_fields) => Self::Struct(protobuf::Struct {
                sub_field_types: struct_fields
                    .iter()
                    .map(|field| field.into())
                    .collect::<Vec<_>>(),
            }),
            DataType::Union(union_types, type_ids, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => protobuf::UnionMode::Sparse,
                    UnionMode::Dense => protobuf::UnionMode::Dense,
                };
                Self::Union(protobuf::Union {
                    union_types: union_types.iter().map(Into::into).collect(),
                    union_mode: union_mode.into(),
                    type_ids: type_ids.iter().map(|x| *x as i32).collect(),
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                Self::Dictionary(Box::new(protobuf::Dictionary {
                    key: Some(Box::new(key_type.as_ref().into())),
                    value: Some(Box::new(value_type.as_ref().into())),
                }))
            }
            DataType::Decimal(whole, fractional) => Self::Decimal(protobuf::Decimal {
                whole: *whole as u64,
                fractional: *fractional as u64,
            }),
            DataType::Decimal256(_, _) => {
                unimplemented!("The Decimal256 data type is not yet supported")
            }
            DataType::Map(_, _) => {
                unimplemented!("The Map data type is not yet supported")
            }
        }
    }
}

impl From<Column> for protobuf::Column {
    fn from(c: Column) -> Self {
        Self {
            relation: c
                .relation
                .map(|relation| protobuf::ColumnRelation { relation }),
            name: c.name,
        }
    }
}

impl From<&Column> for protobuf::Column {
    fn from(c: &Column) -> Self {
        c.clone().into()
    }
}

impl From<&Schema> for protobuf::Schema {
    fn from(schema: &Schema) -> Self {
        Self {
            columns: schema
                .fields()
                .iter()
                .map(protobuf::Field::from)
                .collect::<Vec<_>>(),
        }
    }
}

impl From<SchemaRef> for protobuf::Schema {
    fn from(schema: SchemaRef) -> Self {
        Self {
            columns: schema
                .fields()
                .iter()
                .map(protobuf::Field::from)
                .collect::<Vec<_>>(),
        }
    }
}

impl From<&DFField> for protobuf::DfField {
    fn from(f: &DFField) -> protobuf::DfField {
        protobuf::DfField {
            field: Some(f.field().into()),
            qualifier: f.qualifier().map(|r| protobuf::ColumnRelation {
                relation: r.to_string(),
            }),
        }
    }
}

impl From<&DFSchemaRef> for protobuf::DfSchema {
    fn from(s: &DFSchemaRef) -> protobuf::DfSchema {
        let columns = s.fields().iter().map(|f| f.into()).collect::<Vec<_>>();
        protobuf::DfSchema {
            columns,
            metadata: s.metadata().clone(),
        }
    }
}

impl From<&StringifiedPlan> for protobuf::StringifiedPlan {
    fn from(stringified_plan: &StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan.clone().plan_type {
                PlanType::InitialLogicalPlan => Some(protobuf::PlanType {
                    plan_type_enum: Some(InitialLogicalPlan(EmptyMessage {})),
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
            AggregateFunction::ApproxPercentileCont => Self::ApproxPercentileCont,
            AggregateFunction::ApproxPercentileContWithWeight => {
                Self::ApproxPercentileContWithWeight
            }
            AggregateFunction::ApproxMedian => Self::ApproxMedian,
            AggregateFunction::Grouping => Self::Grouping,
            AggregateFunction::Median => Self::Median,
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

impl From<WindowFrameBound> for protobuf::WindowFrameBound {
    fn from(bound: WindowFrameBound) -> Self {
        match bound {
            WindowFrameBound::CurrentRow => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow
                    .into(),
                bound_value: None,
            },
            WindowFrameBound::Preceding(v) => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
                bound_value: v.map(protobuf::window_frame_bound::BoundValue::Value),
            },
            WindowFrameBound::Following(v) => Self {
                window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
                bound_value: v.map(protobuf::window_frame_bound::BoundValue::Value),
            },
        }
    }
}

impl From<WindowFrame> for protobuf::WindowFrame {
    fn from(window: WindowFrame) -> Self {
        Self {
            window_frame_units: protobuf::WindowFrameUnits::from(window.units).into(),
            start_bound: Some(window.start_bound.into()),
            end_bound: Some(protobuf::window_frame::EndBound::Bound(
                window.end_bound.into(),
            )),
        }
    }
}

impl TryFrom<&Expr> for protobuf::LogicalExprNode {
    type Error = Error;

    fn try_from(expr: &Expr) -> Result<Self, Self::Error> {
        use protobuf::logical_expr_node::ExprType;

        let expr_node = match expr {
            Expr::Column(c) => Self {
                expr_type: Some(ExprType::Column(c.into())),
            },
            Expr::Alias(expr, alias) => {
                let alias = Box::new(protobuf::AliasNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    alias: alias.to_owned(),
                });
                Self {
                    expr_type: Some(ExprType::Alias(alias)),
                }
            }
            Expr::Literal(value) => {
                let pb_value: protobuf::ScalarValue = value.try_into()?;
                Self {
                    expr_type: Some(ExprType::Literal(pb_value)),
                }
            }
            Expr::BinaryExpr { left, op, right } => {
                let binary_expr = Box::new(protobuf::BinaryExprNode {
                    l: Some(Box::new(left.as_ref().try_into()?)),
                    r: Some(Box::new(right.as_ref().try_into()?)),
                    op: format!("{:?}", op),
                });
                Self {
                    expr_type: Some(ExprType::BinaryExpr(binary_expr)),
                }
            }
            Expr::WindowFunction {
                ref fun,
                ref args,
                ref partition_by,
                ref order_by,
                ref window_frame,
            } => {
                let window_function = match fun {
                    WindowFunction::AggregateFunction(fun) => {
                        protobuf::window_expr_node::WindowFunction::AggrFunction(
                            protobuf::AggregateFunction::from(fun).into(),
                        )
                    }
                    WindowFunction::BuiltInWindowFunction(fun) => {
                        protobuf::window_expr_node::WindowFunction::BuiltInFunction(
                            protobuf::BuiltInWindowFunction::from(fun).into(),
                        )
                    }
                };
                let arg_expr: Option<Box<Self>> = if !args.is_empty() {
                    let arg = &args[0];
                    Some(Box::new(arg.try_into()?))
                } else {
                    None
                };
                let partition_by = partition_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = order_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let window_frame = window_frame.map(|window_frame| {
                    protobuf::window_expr_node::WindowFrame::Frame(window_frame.into())
                });
                let window_expr = Box::new(protobuf::WindowExprNode {
                    expr: arg_expr,
                    window_function: Some(window_function),
                    partition_by,
                    order_by,
                    window_frame,
                });
                Self {
                    expr_type: Some(ExprType::WindowExpr(window_expr)),
                }
            }
            Expr::AggregateFunction {
                ref fun, ref args, ..
            } => {
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
                    AggregateFunction::ApproxMedian => {
                        protobuf::AggregateFunction::ApproxMedian
                    }
                    AggregateFunction::Grouping => protobuf::AggregateFunction::Grouping,
                    AggregateFunction::Median => protobuf::AggregateFunction::Median,
                };

                let aggregate_expr = protobuf::AggregateExprNode {
                    aggr_function: aggr_function.into(),
                    expr: args
                        .iter()
                        .map(|v| v.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                };
                Self {
                    expr_type: Some(ExprType::AggregateExpr(aggregate_expr)),
                }
            }
            Expr::ScalarVariable(_, _) => unimplemented!(),
            Expr::ScalarFunction { ref fun, ref args } => {
                let fun: protobuf::ScalarFunction = fun.try_into()?;
                let args: Vec<Self> = args
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<Self>, Error>>()?;
                Self {
                    expr_type: Some(ExprType::ScalarFunction(
                        protobuf::ScalarFunctionNode {
                            fun: fun.into(),
                            args,
                        },
                    )),
                }
            }
            Expr::ScalarUDF { fun, args } => Self {
                expr_type: Some(ExprType::ScalarUdfExpr(protobuf::ScalarUdfExprNode {
                    fun_name: fun.name.clone(),
                    args: args
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, Error>>()?,
                })),
            },
            Expr::AggregateUDF { fun, args } => Self {
                expr_type: Some(ExprType::AggregateUdfExpr(
                    protobuf::AggregateUdfExprNode {
                        fun_name: fun.name.clone(),
                        args: args.iter().map(|expr| expr.try_into()).collect::<Result<
                            Vec<_>,
                            Error,
                        >>(
                        )?,
                    },
                )),
            },
            Expr::Not(expr) => {
                let expr = Box::new(protobuf::Not {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Self {
                    expr_type: Some(ExprType::NotExpr(expr)),
                }
            }
            Expr::IsNull(expr) => {
                let expr = Box::new(protobuf::IsNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Self {
                    expr_type: Some(ExprType::IsNullExpr(expr)),
                }
            }
            Expr::IsNotNull(expr) => {
                let expr = Box::new(protobuf::IsNotNull {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Self {
                    expr_type: Some(ExprType::IsNotNullExpr(expr)),
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = Box::new(protobuf::BetweenNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    negated: *negated,
                    low: Some(Box::new(low.as_ref().try_into()?)),
                    high: Some(Box::new(high.as_ref().try_into()?)),
                });
                Self {
                    expr_type: Some(ExprType::Between(expr)),
                }
            }
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => {
                let when_then_expr = when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        Ok(protobuf::WhenThen {
                            when_expr: Some(w.as_ref().try_into()?),
                            then_expr: Some(t.as_ref().try_into()?),
                        })
                    })
                    .collect::<Result<Vec<protobuf::WhenThen>, Error>>()?;
                let expr = Box::new(protobuf::CaseNode {
                    expr: match expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                    when_then_expr,
                    else_expr: match else_expr {
                        Some(e) => Some(Box::new(e.as_ref().try_into()?)),
                        None => None,
                    },
                });
                Self {
                    expr_type: Some(ExprType::Case(expr)),
                }
            }
            Expr::Cast { expr, data_type } => {
                let expr = Box::new(protobuf::CastNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    arrow_type: Some(data_type.into()),
                });
                Self {
                    expr_type: Some(ExprType::Cast(expr)),
                }
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let expr = Box::new(protobuf::SortExprNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    asc: *asc,
                    nulls_first: *nulls_first,
                });
                Self {
                    expr_type: Some(ExprType::Sort(expr)),
                }
            }
            Expr::Negative(expr) => {
                let expr = Box::new(protobuf::NegativeNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                });
                Self {
                    expr_type: Some(ExprType::Negative(expr)),
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Box::new(protobuf::InListNode {
                    expr: Some(Box::new(expr.as_ref().try_into()?)),
                    list: list
                        .iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, Error>>()?,
                    negated: *negated,
                });
                Self {
                    expr_type: Some(ExprType::InList(expr)),
                }
            }
            Expr::Wildcard => Self {
                expr_type: Some(ExprType::Wildcard(true)),
            },
            Expr::ScalarSubquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                // we would need to add logical plan operators to datafusion.proto to support this
                // see discussion in https://github.com/apache/arrow-datafusion/issues/2565
                unimplemented!("subquery expressions are not supported yet")
            }
            Expr::GetIndexedField { key, expr } => Self {
                expr_type: Some(ExprType::GetIndexedField(Box::new(
                    protobuf::GetIndexedField {
                        key: Some(key.try_into()?),
                        expr: Some(Box::new(expr.as_ref().try_into()?)),
                    },
                ))),
            },

            Expr::GroupingSet(GroupingSet::Cube(exprs)) => Self {
                expr_type: Some(ExprType::Cube(CubeNode {
                    expr: exprs.iter().map(|expr| expr.try_into()).collect::<Result<
                        Vec<_>,
                        Self::Error,
                    >>(
                    )?,
                })),
            },
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => Self {
                expr_type: Some(ExprType::Rollup(RollupNode {
                    expr: exprs.iter().map(|expr| expr.try_into()).collect::<Result<
                        Vec<_>,
                        Self::Error,
                    >>(
                    )?,
                })),
            },
            Expr::GroupingSet(GroupingSet::GroupingSets(exprs)) => Self {
                expr_type: Some(ExprType::GroupingSet(GroupingSetNode {
                    expr: exprs
                        .iter()
                        .map(|expr_list| {
                            Ok(LogicalExprList {
                                expr: expr_list
                                    .iter()
                                    .map(|expr| expr.try_into())
                                    .collect::<Result<Vec<_>, Self::Error>>()?,
                            })
                        })
                        .collect::<Result<Vec<_>, Self::Error>>()?,
                })),
            },

            Expr::QualifiedWildcard { .. } | Expr::TryCast { .. } => unimplemented!(),
        };

        Ok(expr_node)
    }
}

impl TryFrom<&ScalarValue> for protobuf::ScalarValue {
    type Error = Error;

    fn try_from(val: &ScalarValue) -> Result<Self, Self::Error> {
        use datafusion_common::scalar;
        use protobuf::{scalar_value::Value, PrimitiveScalarType};

        let scalar_val = match val {
            scalar::ScalarValue::Boolean(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Bool, |s| {
                    Value::BoolValue(*s)
                })
            }
            scalar::ScalarValue::Float32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Float32, |s| {
                    Value::Float32Value(*s)
                })
            }
            scalar::ScalarValue::Float64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Float64, |s| {
                    Value::Float64Value(*s)
                })
            }
            scalar::ScalarValue::Int8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int8, |s| {
                    Value::Int8Value(*s as i32)
                })
            }
            scalar::ScalarValue::Int16(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int16, |s| {
                    Value::Int16Value(*s as i32)
                })
            }
            scalar::ScalarValue::Int32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int32, |s| {
                    Value::Int32Value(*s)
                })
            }
            scalar::ScalarValue::Int64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Int64, |s| {
                    Value::Int64Value(*s)
                })
            }
            scalar::ScalarValue::UInt8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint8, |s| {
                    Value::Uint8Value(*s as u32)
                })
            }
            scalar::ScalarValue::UInt16(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint16, |s| {
                    Value::Uint16Value(*s as u32)
                })
            }
            scalar::ScalarValue::UInt32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint32, |s| {
                    Value::Uint32Value(*s)
                })
            }
            scalar::ScalarValue::UInt64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Uint64, |s| {
                    Value::Uint64Value(*s)
                })
            }
            scalar::ScalarValue::Utf8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Utf8, |s| {
                    Value::Utf8Value(s.to_owned())
                })
            }
            scalar::ScalarValue::LargeUtf8(val) => {
                create_proto_scalar(val, PrimitiveScalarType::LargeUtf8, |s| {
                    Value::LargeUtf8Value(s.to_owned())
                })
            }
            scalar::ScalarValue::List(value, boxed_field) => {
                println!("Current field of list: {:?}", boxed_field);
                match value {
                    Some(values) => {
                        if values.is_empty() {
                            protobuf::ScalarValue {
                                value: Some(protobuf::scalar_value::Value::ListValue(
                                    protobuf::ScalarListValue {
                                        field: Some(boxed_field.as_ref().into()),
                                        values: Vec::new(),
                                    },
                                )),
                            }
                        } else {
                            let scalar_type = match boxed_field.data_type() {
                                DataType::List(field) => field.as_ref().data_type(),
                                unsupported => {
                                    todo!("Proper error handling {}", unsupported)
                                }
                            };
                            println!("Current scalar type for list: {:?}", scalar_type);

                            let type_checked_values: Vec<protobuf::ScalarValue> = values
                                .iter()
                                .map(|scalar| match (scalar, scalar_type) {
                                    (
                                        scalar::ScalarValue::List(_, list_type),
                                        DataType::List(field),
                                    ) => {
                                        if let DataType::List(list_field) =
                                            list_type.data_type()
                                        {
                                            let scalar_datatype = field.data_type();
                                            let list_datatype = list_field.data_type();
                                            if std::mem::discriminant(list_datatype)
                                                != std::mem::discriminant(scalar_datatype)
                                            {
                                                return Err(
                                                    Error::inconsistent_list_typing(
                                                        list_datatype,
                                                        scalar_datatype,
                                                    ),
                                                );
                                            }
                                            scalar.try_into()
                                        } else {
                                            Err(Error::inconsistent_list_designated(
                                                scalar,
                                                boxed_field.data_type(),
                                            ))
                                        }
                                    }
                                    (
                                        scalar::ScalarValue::Boolean(_),
                                        DataType::Boolean,
                                    ) => scalar.try_into(),
                                    (
                                        scalar::ScalarValue::Float32(_),
                                        DataType::Float32,
                                    ) => scalar.try_into(),
                                    (
                                        scalar::ScalarValue::Float64(_),
                                        DataType::Float64,
                                    ) => scalar.try_into(),
                                    (scalar::ScalarValue::Int8(_), DataType::Int8) => {
                                        scalar.try_into()
                                    }
                                    (scalar::ScalarValue::Int16(_), DataType::Int16) => {
                                        scalar.try_into()
                                    }
                                    (scalar::ScalarValue::Int32(_), DataType::Int32) => {
                                        scalar.try_into()
                                    }
                                    (scalar::ScalarValue::Int64(_), DataType::Int64) => {
                                        scalar.try_into()
                                    }
                                    (scalar::ScalarValue::UInt8(_), DataType::UInt8) => {
                                        scalar.try_into()
                                    }
                                    (
                                        scalar::ScalarValue::UInt16(_),
                                        DataType::UInt16,
                                    ) => scalar.try_into(),
                                    (
                                        scalar::ScalarValue::UInt32(_),
                                        DataType::UInt32,
                                    ) => scalar.try_into(),
                                    (
                                        scalar::ScalarValue::UInt64(_),
                                        DataType::UInt64,
                                    ) => scalar.try_into(),
                                    (scalar::ScalarValue::Utf8(_), DataType::Utf8) => {
                                        scalar.try_into()
                                    }
                                    (
                                        scalar::ScalarValue::LargeUtf8(_),
                                        DataType::LargeUtf8,
                                    ) => scalar.try_into(),
                                    _ => Err(Error::inconsistent_list_designated(
                                        scalar,
                                        boxed_field.data_type(),
                                    )),
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            protobuf::ScalarValue {
                                value: Some(protobuf::scalar_value::Value::ListValue(
                                    protobuf::ScalarListValue {
                                        field: Some(boxed_field.as_ref().into()),
                                        values: type_checked_values,
                                    },
                                )),
                            }
                        }
                    }
                    None => protobuf::ScalarValue {
                        value: Some(protobuf::scalar_value::Value::NullListValue(
                            boxed_field.as_ref().try_into()?,
                        )),
                    },
                }
            }
            datafusion::scalar::ScalarValue::Date32(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Date32, |s| {
                    Value::Date32Value(*s)
                })
            }
            datafusion::scalar::ScalarValue::TimestampMicrosecond(val, tz) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeMicrosecond, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_ref().unwrap_or(&"".to_string()).clone(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMicrosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            datafusion::scalar::ScalarValue::TimestampNanosecond(val, tz) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeNanosecond, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_ref().unwrap_or(&"".to_string()).clone(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeNanosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            datafusion::scalar::ScalarValue::Decimal128(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    protobuf::ScalarValue {
                        value: Some(Value::Decimal128Value(protobuf::Decimal128 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    }
                }
                None => protobuf::ScalarValue {
                    value: Some(protobuf::scalar_value::Value::NullValue(
                        PrimitiveScalarType::Decimal128 as i32,
                    )),
                },
            },
            datafusion::scalar::ScalarValue::Date64(val) => {
                create_proto_scalar(val, PrimitiveScalarType::Date64, |s| {
                    Value::Date64Value(*s)
                })
            }
            datafusion::scalar::ScalarValue::TimestampSecond(val, tz) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeSecond, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_ref().unwrap_or(&"".to_string()).clone(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeSecondValue(*s),
                        ),
                    })
                })
            }
            datafusion::scalar::ScalarValue::TimestampMillisecond(val, tz) => {
                create_proto_scalar(val, PrimitiveScalarType::TimeMillisecond, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_ref().unwrap_or(&"".to_string()).clone(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMillisecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            datafusion::scalar::ScalarValue::IntervalYearMonth(val) => {
                create_proto_scalar(val, PrimitiveScalarType::IntervalYearmonth, |s| {
                    Value::IntervalYearmonthValue(*s)
                })
            }
            datafusion::scalar::ScalarValue::IntervalDayTime(val) => {
                create_proto_scalar(val, PrimitiveScalarType::IntervalDaytime, |s| {
                    Value::IntervalDaytimeValue(*s)
                })
            }
            _ => {
                return Err(Error::invalid_scalar_value(val));
            }
        };

        Ok(scalar_val)
    }
}

impl TryFrom<&BuiltinScalarFunction> for protobuf::ScalarFunction {
    type Error = Error;

    fn try_from(scalar: &BuiltinScalarFunction) -> Result<Self, Self::Error> {
        let scalar_function = match scalar {
            BuiltinScalarFunction::Sqrt => Self::Sqrt,
            BuiltinScalarFunction::Sin => Self::Sin,
            BuiltinScalarFunction::Cos => Self::Cos,
            BuiltinScalarFunction::Tan => Self::Tan,
            BuiltinScalarFunction::Asin => Self::Asin,
            BuiltinScalarFunction::Acos => Self::Acos,
            BuiltinScalarFunction::Atan => Self::Atan,
            BuiltinScalarFunction::Exp => Self::Exp,
            BuiltinScalarFunction::Log => Self::Log,
            BuiltinScalarFunction::Ln => Self::Ln,
            BuiltinScalarFunction::Log10 => Self::Log10,
            BuiltinScalarFunction::Floor => Self::Floor,
            BuiltinScalarFunction::Ceil => Self::Ceil,
            BuiltinScalarFunction::Round => Self::Round,
            BuiltinScalarFunction::Trunc => Self::Trunc,
            BuiltinScalarFunction::Abs => Self::Abs,
            BuiltinScalarFunction::OctetLength => Self::OctetLength,
            BuiltinScalarFunction::Concat => Self::Concat,
            BuiltinScalarFunction::Lower => Self::Lower,
            BuiltinScalarFunction::Upper => Self::Upper,
            BuiltinScalarFunction::Trim => Self::Trim,
            BuiltinScalarFunction::Ltrim => Self::Ltrim,
            BuiltinScalarFunction::Rtrim => Self::Rtrim,
            BuiltinScalarFunction::ToTimestamp => Self::ToTimestamp,
            BuiltinScalarFunction::Array => Self::Array,
            BuiltinScalarFunction::NullIf => Self::NullIf,
            BuiltinScalarFunction::DatePart => Self::DatePart,
            BuiltinScalarFunction::DateTrunc => Self::DateTrunc,
            BuiltinScalarFunction::MD5 => Self::Md5,
            BuiltinScalarFunction::SHA224 => Self::Sha224,
            BuiltinScalarFunction::SHA256 => Self::Sha256,
            BuiltinScalarFunction::SHA384 => Self::Sha384,
            BuiltinScalarFunction::SHA512 => Self::Sha512,
            BuiltinScalarFunction::Digest => Self::Digest,
            BuiltinScalarFunction::ToTimestampMillis => Self::ToTimestampMillis,
            BuiltinScalarFunction::Log2 => Self::Log2,
            BuiltinScalarFunction::Signum => Self::Signum,
            BuiltinScalarFunction::Ascii => Self::Ascii,
            BuiltinScalarFunction::BitLength => Self::BitLength,
            BuiltinScalarFunction::Btrim => Self::Btrim,
            BuiltinScalarFunction::CharacterLength => Self::CharacterLength,
            BuiltinScalarFunction::Chr => Self::Chr,
            BuiltinScalarFunction::ConcatWithSeparator => Self::ConcatWithSeparator,
            BuiltinScalarFunction::InitCap => Self::InitCap,
            BuiltinScalarFunction::Left => Self::Left,
            BuiltinScalarFunction::Lpad => Self::Lpad,
            BuiltinScalarFunction::Random => Self::Random,
            BuiltinScalarFunction::RegexpReplace => Self::RegexpReplace,
            BuiltinScalarFunction::Repeat => Self::Repeat,
            BuiltinScalarFunction::Replace => Self::Replace,
            BuiltinScalarFunction::Reverse => Self::Reverse,
            BuiltinScalarFunction::Right => Self::Right,
            BuiltinScalarFunction::Rpad => Self::Rpad,
            BuiltinScalarFunction::SplitPart => Self::SplitPart,
            BuiltinScalarFunction::StartsWith => Self::StartsWith,
            BuiltinScalarFunction::Strpos => Self::Strpos,
            BuiltinScalarFunction::Substr => Self::Substr,
            BuiltinScalarFunction::ToHex => Self::ToHex,
            BuiltinScalarFunction::ToTimestampMicros => Self::ToTimestampMicros,
            BuiltinScalarFunction::ToTimestampSeconds => Self::ToTimestampSeconds,
            BuiltinScalarFunction::Now => Self::Now,
            BuiltinScalarFunction::Translate => Self::Translate,
            BuiltinScalarFunction::RegexpMatch => Self::RegexpMatch,
            BuiltinScalarFunction::Coalesce => Self::Coalesce,
            BuiltinScalarFunction::Power => Self::Power,
            BuiltinScalarFunction::Struct => Self::StructFun,
            BuiltinScalarFunction::FromUnixtime => Self::FromUnixtime,
            BuiltinScalarFunction::Atan2 => Self::Atan2,
        };

        Ok(scalar_function)
    }
}

impl TryFrom<&Field> for protobuf::ScalarType {
    type Error = Error;

    fn try_from(value: &Field) -> Result<Self, Self::Error> {
        let datatype = protobuf::scalar_type::Datatype::try_from(value.data_type())?;
        Ok(Self {
            datatype: Some(datatype),
        })
    }
}

impl TryFrom<&DataType> for protobuf::scalar_type::Datatype {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        use protobuf::PrimitiveScalarType;

        let scalar_value = match val {
            DataType::Boolean => Self::Scalar(PrimitiveScalarType::Bool as i32),
            DataType::Int8 => Self::Scalar(PrimitiveScalarType::Int8 as i32),
            DataType::Int16 => Self::Scalar(PrimitiveScalarType::Int16 as i32),
            DataType::Int32 => Self::Scalar(PrimitiveScalarType::Int32 as i32),
            DataType::Int64 => Self::Scalar(PrimitiveScalarType::Int64 as i32),
            DataType::UInt8 => Self::Scalar(PrimitiveScalarType::Uint8 as i32),
            DataType::UInt16 => Self::Scalar(PrimitiveScalarType::Uint16 as i32),
            DataType::UInt32 => Self::Scalar(PrimitiveScalarType::Uint32 as i32),
            DataType::UInt64 => Self::Scalar(PrimitiveScalarType::Uint64 as i32),
            DataType::Float32 => Self::Scalar(PrimitiveScalarType::Float32 as i32),
            DataType::Float64 => Self::Scalar(PrimitiveScalarType::Float64 as i32),
            DataType::Date32 => Self::Scalar(PrimitiveScalarType::Date32 as i32),
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => {
                    Self::Scalar(PrimitiveScalarType::TimeMicrosecond as i32)
                }
                TimeUnit::Nanosecond => {
                    Self::Scalar(PrimitiveScalarType::TimeNanosecond as i32)
                }
                _ => {
                    return Err(Error::invalid_time_unit(time_unit));
                }
            },
            DataType::Utf8 => Self::Scalar(PrimitiveScalarType::Utf8 as i32),
            DataType::LargeUtf8 => Self::Scalar(PrimitiveScalarType::LargeUtf8 as i32),
            DataType::List(field_type) => {
                let mut field_names: Vec<String> = Vec::new();
                let mut curr_field = field_type.as_ref();
                field_names.push(curr_field.name().to_owned());
                // For each nested field check nested datatype, since datafusion scalars only
                // support recursive lists with a leaf scalar type
                // any other compound types are errors.

                while let DataType::List(nested_field_type) = curr_field.data_type() {
                    curr_field = nested_field_type.as_ref();
                    field_names.push(curr_field.name().to_owned());
                    if !is_valid_scalar_type_no_list_check(curr_field.data_type()) {
                        return Err(Error::invalid_scalar_type(curr_field.data_type()));
                    }
                }
                let deepest_datatype = curr_field.data_type();
                if !is_valid_scalar_type_no_list_check(deepest_datatype) {
                    return Err(Error::invalid_scalar_type(deepest_datatype));
                }
                let pb_deepest_type: PrimitiveScalarType = match deepest_datatype {
                    DataType::Boolean => PrimitiveScalarType::Bool,
                    DataType::Int8 => PrimitiveScalarType::Int8,
                    DataType::Int16 => PrimitiveScalarType::Int16,
                    DataType::Int32 => PrimitiveScalarType::Int32,
                    DataType::Int64 => PrimitiveScalarType::Int64,
                    DataType::UInt8 => PrimitiveScalarType::Uint8,
                    DataType::UInt16 => PrimitiveScalarType::Uint16,
                    DataType::UInt32 => PrimitiveScalarType::Uint32,
                    DataType::UInt64 => PrimitiveScalarType::Uint64,
                    DataType::Float32 => PrimitiveScalarType::Float32,
                    DataType::Float64 => PrimitiveScalarType::Float64,
                    DataType::Date32 => PrimitiveScalarType::Date32,
                    DataType::Time64(time_unit) => match time_unit {
                        TimeUnit::Microsecond => PrimitiveScalarType::TimeMicrosecond,
                        TimeUnit::Nanosecond => PrimitiveScalarType::TimeNanosecond,
                        _ => {
                            return Err(Error::invalid_time_unit(time_unit));
                        }
                    },

                    DataType::Utf8 => PrimitiveScalarType::Utf8,
                    DataType::LargeUtf8 => PrimitiveScalarType::LargeUtf8,
                    _ => {
                        return Err(Error::invalid_scalar_type(val));
                    }
                };
                Self::List(protobuf::ScalarListType {
                    field_names,
                    deepest_type: pb_deepest_type as i32,
                })
            }
            DataType::Null
            | DataType::Float16
            | DataType::Timestamp(_, _)
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_, _, _)
            | DataType::Dictionary(_, _)
            | DataType::Map(_, _)
            | DataType::Decimal(_, _)
            | DataType::Decimal256(_, _) => {
                return Err(Error::invalid_scalar_type(val));
            }
        };

        Ok(scalar_value)
    }
}

impl From<&TimeUnit> for protobuf::TimeUnit {
    fn from(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::TimeMillisecond,
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

fn create_proto_scalar<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: &Option<I>,
    null_arrow_type: protobuf::PrimitiveScalarType,
    constructor: T,
) -> protobuf::ScalarValue {
    protobuf::ScalarValue {
        value: Some(v.as_ref().map(constructor).unwrap_or(
            protobuf::scalar_value::Value::NullValue(null_arrow_type as i32),
        )),
    }
}

// Does not check if list subtypes are valid
fn is_valid_scalar_type_no_list_check(datatype: &DataType) -> bool {
    match datatype {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::LargeUtf8
        | DataType::Utf8
        | DataType::Date32 => true,
        DataType::Time64(time_unit) => {
            matches!(time_unit, TimeUnit::Microsecond | TimeUnit::Nanosecond)
        }

        DataType::List(_) => true,
        _ => false,
    }
}
