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

use std::sync::Arc;

use arrow::{
    array::AsArray,
    buffer::Buffer,
    datatypes::{
        i256, DataType, Field, IntervalMonthDayNanoType, IntervalUnit, Schema, TimeUnit,
        UnionFields, UnionMode,
    },
    ipc::{reader::read_record_batch, root_as_message},
};

use datafusion::execution::registry::FunctionRegistry;
use datafusion_common::{
    arrow_datafusion_err, internal_err, plan_datafusion_err, Column, Constraint,
    Constraints, DFField, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference,
    Result, ScalarValue,
};
use datafusion_expr::expr::Unnest;
use datafusion_expr::expr::{Alias, Placeholder};
use datafusion_expr::window_frame::{check_window_frame, regularize_window_order_by};
use datafusion_expr::{
    acosh, asinh, atan, atan2, atanh, cbrt, ceil, coalesce, concat_expr, concat_ws_expr,
    cos, cosh, cot, degrees, ends_with, exp,
    expr::{self, InList, Sort, WindowFunction},
    factorial, find_in_set, floor, gcd, initcap, iszero, lcm, ln, log, log10, log2,
    logical_plan::{PlanType, StringifiedPlan},
    nanvl, pi, power, radians, random, round, signum, sin, sinh, sqrt, strpos, substr,
    substr_index, substring, translate, trunc, AggregateFunction, Between, BinaryExpr,
    BuiltInWindowFunction, BuiltinScalarFunction, Case, Cast, Expr, GetFieldAccess,
    GetIndexedField, GroupingSet,
    GroupingSet::GroupingSets,
    JoinConstraint, JoinType, Like, Operator, TryCast, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};

use crate::protobuf::{
    self,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithStats, InitialLogicalPlan,
        InitialPhysicalPlan, InitialPhysicalPlanWithStats, OptimizedLogicalPlan,
        OptimizedPhysicalPlan,
    },
    AnalyzedLogicalPlanType, CubeNode, GroupingSetNode, OptimizedLogicalPlanType,
    OptimizedPhysicalPlanType, PlaceholderNode, RollupNode,
};

use super::LogicalExtensionCodec;

#[derive(Debug)]
pub enum Error {
    General(String),

    DataFusionError(DataFusionError),

    MissingRequiredField(String),

    AtLeastOneValue(String),

    UnknownEnumVariant { name: String, value: i32 },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),

            Self::DataFusionError(desc) => {
                write!(f, "DataFusion error: {desc:?}")
            }

            Self::MissingRequiredField(name) => {
                write!(f, "Missing required field {name}")
            }
            Self::AtLeastOneValue(name) => {
                write!(f, "Must have at least one {name}, found 0")
            }
            Self::UnknownEnumVariant { name, value } => {
                write!(f, "Unknown i32 value for {name} enum: {value}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<DataFusionError> for Error {
    fn from(e: DataFusionError) -> Self {
        Error::DataFusionError(e)
    }
}

impl Error {
    fn required(field: impl Into<String>) -> Error {
        Error::MissingRequiredField(field.into())
    }

    fn unknown(name: impl Into<String>, value: i32) -> Error {
        Error::UnknownEnumVariant {
            name: name.into(),
            value,
        }
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = Error>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    ///
    /// Returns None if the option is None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn optional(self) -> Result<Option<T>, Error>;

    /// Converts an optional protobuf field to a different type, returning an error if None
    ///
    /// Returns `Error::MissingRequiredField` if None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn required(self, field: impl Into<String>) -> Result<T, Error>;
}

impl<T, U> FromOptionalField<U> for Option<T>
where
    T: TryInto<U, Error = Error>,
{
    fn optional(self) -> Result<Option<U>, Error> {
        self.map(|t| t.try_into()).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, Error> {
        match self {
            None => Err(Error::required(field)),
            Some(t) => t.try_into(),
        }
    }
}

impl From<protobuf::Column> for Column {
    fn from(c: protobuf::Column) -> Self {
        let protobuf::Column { relation, name } = c;

        Self::new(relation.map(|r| r.relation), name)
    }
}

impl From<&protobuf::Column> for Column {
    fn from(c: &protobuf::Column) -> Self {
        c.clone().into()
    }
}

impl TryFrom<&protobuf::DfSchema> for DFSchema {
    type Error = Error;

    fn try_from(df_schema: &protobuf::DfSchema) -> Result<Self, Self::Error> {
        let fields = df_schema
            .columns
            .iter()
            .map(|c| c.try_into())
            .collect::<Result<Vec<DFField>, _>>()?;
        Ok(DFSchema::new_with_metadata(
            fields,
            df_schema.metadata.clone(),
        )?)
    }
}

impl TryFrom<protobuf::DfSchema> for DFSchemaRef {
    type Error = Error;

    fn try_from(df_schema: protobuf::DfSchema) -> Result<Self, Self::Error> {
        let dfschema: DFSchema = (&df_schema).try_into()?;
        Ok(Arc::new(dfschema))
    }
}

impl TryFrom<&protobuf::DfField> for DFField {
    type Error = Error;

    fn try_from(df_field: &protobuf::DfField) -> Result<Self, Self::Error> {
        let field: Field = df_field.field.as_ref().required("field")?;

        Ok(match &df_field.qualifier {
            Some(q) => DFField::from_qualified(q.relation.clone(), field),
            None => DFField::from(field),
        })
    }
}

impl From<protobuf::WindowFrameUnits> for WindowFrameUnits {
    fn from(units: protobuf::WindowFrameUnits) -> Self {
        match units {
            protobuf::WindowFrameUnits::Rows => Self::Rows,
            protobuf::WindowFrameUnits::Range => Self::Range,
            protobuf::WindowFrameUnits::Groups => Self::Groups,
        }
    }
}

impl TryFrom<protobuf::OwnedTableReference> for OwnedTableReference {
    type Error = Error;

    fn try_from(value: protobuf::OwnedTableReference) -> Result<Self, Self::Error> {
        use protobuf::owned_table_reference::TableReferenceEnum;
        let table_reference_enum = value
            .table_reference_enum
            .ok_or_else(|| Error::required("table_reference_enum"))?;

        match table_reference_enum {
            TableReferenceEnum::Bare(protobuf::BareTableReference { table }) => {
                Ok(OwnedTableReference::bare(table))
            }
            TableReferenceEnum::Partial(protobuf::PartialTableReference {
                schema,
                table,
            }) => Ok(OwnedTableReference::partial(schema, table)),
            TableReferenceEnum::Full(protobuf::FullTableReference {
                catalog,
                schema,
                table,
            }) => Ok(OwnedTableReference::full(catalog, schema, table)),
        }
    }
}

impl TryFrom<&protobuf::ArrowType> for DataType {
    type Error = Error;

    fn try_from(arrow_type: &protobuf::ArrowType) -> Result<Self, Self::Error> {
        arrow_type
            .arrow_type_enum
            .as_ref()
            .required("arrow_type_enum")
    }
}

impl TryFrom<&protobuf::arrow_type::ArrowTypeEnum> for DataType {
    type Error = Error;
    fn try_from(
        arrow_type_enum: &protobuf::arrow_type::ArrowTypeEnum,
    ) -> Result<Self, Self::Error> {
        use protobuf::arrow_type;
        Ok(match arrow_type_enum {
            arrow_type::ArrowTypeEnum::None(_) => DataType::Null,
            arrow_type::ArrowTypeEnum::Bool(_) => DataType::Boolean,
            arrow_type::ArrowTypeEnum::Uint8(_) => DataType::UInt8,
            arrow_type::ArrowTypeEnum::Int8(_) => DataType::Int8,
            arrow_type::ArrowTypeEnum::Uint16(_) => DataType::UInt16,
            arrow_type::ArrowTypeEnum::Int16(_) => DataType::Int16,
            arrow_type::ArrowTypeEnum::Uint32(_) => DataType::UInt32,
            arrow_type::ArrowTypeEnum::Int32(_) => DataType::Int32,
            arrow_type::ArrowTypeEnum::Uint64(_) => DataType::UInt64,
            arrow_type::ArrowTypeEnum::Int64(_) => DataType::Int64,
            arrow_type::ArrowTypeEnum::Float16(_) => DataType::Float16,
            arrow_type::ArrowTypeEnum::Float32(_) => DataType::Float32,
            arrow_type::ArrowTypeEnum::Float64(_) => DataType::Float64,
            arrow_type::ArrowTypeEnum::Utf8(_) => DataType::Utf8,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::FixedSizeBinary(size) => {
                DataType::FixedSizeBinary(*size)
            }
            arrow_type::ArrowTypeEnum::LargeBinary(_) => DataType::LargeBinary,
            arrow_type::ArrowTypeEnum::Date32(_) => DataType::Date32,
            arrow_type::ArrowTypeEnum::Date64(_) => DataType::Date64,
            arrow_type::ArrowTypeEnum::Duration(time_unit) => {
                DataType::Duration(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                parse_i32_to_time_unit(time_unit)?,
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.as_str().into()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(parse_i32_to_time_unit(time_unit)?)
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => {
                DataType::Interval(parse_i32_to_interval_unit(interval_unit)?)
            }
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal {
                precision,
                scale,
            }) => DataType::Decimal128(*precision as u8, *scale as i8),
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::List(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::LargeList(Arc::new(list_type))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                let list_size = list.list_size;
                DataType::FixedSizeList(Arc::new(list_type), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                parse_proto_fields_to_fields(&strct.sub_field_types)?.into(),
            ),
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode = protobuf::UnionMode::try_from(union.union_mode)
                    .map_err(|_| Error::unknown("UnionMode", union.union_mode))?;
                let union_mode = match union_mode {
                    protobuf::UnionMode::Dense => UnionMode::Dense,
                    protobuf::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_fields = parse_proto_fields_to_fields(&union.union_types)?;

                // Default to index based type ids if not provided
                let type_ids: Vec<_> = match union.type_ids.is_empty() {
                    true => (0..union_fields.len() as i8).collect(),
                    false => union.type_ids.iter().map(|i| *i as i8).collect(),
                };

                DataType::Union(UnionFields::new(type_ids, union_fields), union_mode)
            }
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let key_datatype = dict.as_ref().key.as_deref().required("key")?;
                let value_datatype = dict.as_ref().value.as_deref().required("value")?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
            }
            arrow_type::ArrowTypeEnum::Map(map) => {
                let field: Field =
                    map.as_ref().field_type.as_deref().required("field_type")?;
                let keys_sorted = map.keys_sorted;
                DataType::Map(Arc::new(field), keys_sorted)
            }
        })
    }
}

impl TryFrom<&protobuf::Field> for Field {
    type Error = Error;
    fn try_from(field: &protobuf::Field) -> Result<Self, Self::Error> {
        let datatype = field.arrow_type.as_deref().required("arrow_type")?;
        let field = if field.dict_id != 0 {
            Self::new_dict(
                field.name.as_str(),
                datatype,
                field.nullable,
                field.dict_id,
                field.dict_ordered,
            )
            .with_metadata(field.metadata.clone())
        } else {
            Self::new(field.name.as_str(), datatype, field.nullable)
                .with_metadata(field.metadata.clone())
        };
        Ok(field)
    }
}

impl From<&protobuf::StringifiedPlan> for StringifiedPlan {
    fn from(stringified_plan: &protobuf::StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan
                .plan_type
                .as_ref()
                .and_then(|pt| pt.plan_type_enum.as_ref())
                .unwrap_or_else(|| {
                    panic!(
                        "Cannot create protobuf::StringifiedPlan from {stringified_plan:?}"
                    )
                }) {
                InitialLogicalPlan(_) => PlanType::InitialLogicalPlan,
                AnalyzedLogicalPlan(AnalyzedLogicalPlanType { analyzer_name }) => {
                    PlanType::AnalyzedLogicalPlan {
                        analyzer_name:analyzer_name.clone()
                    }
                }
                FinalAnalyzedLogicalPlan(_) => PlanType::FinalAnalyzedLogicalPlan,
                OptimizedLogicalPlan(OptimizedLogicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedLogicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalLogicalPlan(_) => PlanType::FinalLogicalPlan,
                InitialPhysicalPlan(_) => PlanType::InitialPhysicalPlan,
                InitialPhysicalPlanWithStats(_) => PlanType::InitialPhysicalPlanWithStats,
                OptimizedPhysicalPlan(OptimizedPhysicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedPhysicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalPhysicalPlan(_) => PlanType::FinalPhysicalPlan,
                FinalPhysicalPlanWithStats(_) => PlanType::FinalPhysicalPlanWithStats,
            },
            plan: Arc::new(stringified_plan.plan.clone()),
        }
    }
}

impl From<&protobuf::ScalarFunction> for BuiltinScalarFunction {
    fn from(f: &protobuf::ScalarFunction) -> Self {
        use protobuf::ScalarFunction;
        match f {
            ScalarFunction::Unknown => todo!(),
            ScalarFunction::Sqrt => Self::Sqrt,
            ScalarFunction::Cbrt => Self::Cbrt,
            ScalarFunction::Sin => Self::Sin,
            ScalarFunction::Cos => Self::Cos,
            ScalarFunction::Cot => Self::Cot,
            ScalarFunction::Atan => Self::Atan,
            ScalarFunction::Sinh => Self::Sinh,
            ScalarFunction::Cosh => Self::Cosh,
            ScalarFunction::Asinh => Self::Asinh,
            ScalarFunction::Acosh => Self::Acosh,
            ScalarFunction::Atanh => Self::Atanh,
            ScalarFunction::Exp => Self::Exp,
            ScalarFunction::Log => Self::Log,
            ScalarFunction::Ln => Self::Ln,
            ScalarFunction::Log10 => Self::Log10,
            ScalarFunction::Degrees => Self::Degrees,
            ScalarFunction::Radians => Self::Radians,
            ScalarFunction::Factorial => Self::Factorial,
            ScalarFunction::Gcd => Self::Gcd,
            ScalarFunction::Lcm => Self::Lcm,
            ScalarFunction::Floor => Self::Floor,
            ScalarFunction::Ceil => Self::Ceil,
            ScalarFunction::Round => Self::Round,
            ScalarFunction::Trunc => Self::Trunc,
            ScalarFunction::Concat => Self::Concat,
            ScalarFunction::Log2 => Self::Log2,
            ScalarFunction::Signum => Self::Signum,
            ScalarFunction::ConcatWithSeparator => Self::ConcatWithSeparator,
            ScalarFunction::EndsWith => Self::EndsWith,
            ScalarFunction::InitCap => Self::InitCap,
            ScalarFunction::Random => Self::Random,
            ScalarFunction::Strpos => Self::Strpos,
            ScalarFunction::Substr => Self::Substr,
            ScalarFunction::Translate => Self::Translate,
            ScalarFunction::Coalesce => Self::Coalesce,
            ScalarFunction::Pi => Self::Pi,
            ScalarFunction::Power => Self::Power,
            ScalarFunction::Atan2 => Self::Atan2,
            ScalarFunction::Nanvl => Self::Nanvl,
            ScalarFunction::Iszero => Self::Iszero,
            ScalarFunction::SubstrIndex => Self::SubstrIndex,
            ScalarFunction::FindInSet => Self::FindInSet,
        }
    }
}

impl From<protobuf::AggregateFunction> for AggregateFunction {
    fn from(agg_fun: protobuf::AggregateFunction) -> Self {
        match agg_fun {
            protobuf::AggregateFunction::Min => Self::Min,
            protobuf::AggregateFunction::Max => Self::Max,
            protobuf::AggregateFunction::Sum => Self::Sum,
            protobuf::AggregateFunction::Avg => Self::Avg,
            protobuf::AggregateFunction::BitAnd => Self::BitAnd,
            protobuf::AggregateFunction::BitOr => Self::BitOr,
            protobuf::AggregateFunction::BitXor => Self::BitXor,
            protobuf::AggregateFunction::BoolAnd => Self::BoolAnd,
            protobuf::AggregateFunction::BoolOr => Self::BoolOr,
            protobuf::AggregateFunction::Count => Self::Count,
            protobuf::AggregateFunction::ApproxDistinct => Self::ApproxDistinct,
            protobuf::AggregateFunction::ArrayAgg => Self::ArrayAgg,
            protobuf::AggregateFunction::Variance => Self::Variance,
            protobuf::AggregateFunction::VariancePop => Self::VariancePop,
            protobuf::AggregateFunction::Covariance => Self::Covariance,
            protobuf::AggregateFunction::CovariancePop => Self::CovariancePop,
            protobuf::AggregateFunction::Stddev => Self::Stddev,
            protobuf::AggregateFunction::StddevPop => Self::StddevPop,
            protobuf::AggregateFunction::Correlation => Self::Correlation,
            protobuf::AggregateFunction::RegrSlope => Self::RegrSlope,
            protobuf::AggregateFunction::RegrIntercept => Self::RegrIntercept,
            protobuf::AggregateFunction::RegrCount => Self::RegrCount,
            protobuf::AggregateFunction::RegrR2 => Self::RegrR2,
            protobuf::AggregateFunction::RegrAvgx => Self::RegrAvgx,
            protobuf::AggregateFunction::RegrAvgy => Self::RegrAvgy,
            protobuf::AggregateFunction::RegrSxx => Self::RegrSXX,
            protobuf::AggregateFunction::RegrSyy => Self::RegrSYY,
            protobuf::AggregateFunction::RegrSxy => Self::RegrSXY,
            protobuf::AggregateFunction::ApproxPercentileCont => {
                Self::ApproxPercentileCont
            }
            protobuf::AggregateFunction::ApproxPercentileContWithWeight => {
                Self::ApproxPercentileContWithWeight
            }
            protobuf::AggregateFunction::ApproxMedian => Self::ApproxMedian,
            protobuf::AggregateFunction::Grouping => Self::Grouping,
            protobuf::AggregateFunction::Median => Self::Median,
            protobuf::AggregateFunction::FirstValueAgg => Self::FirstValue,
            protobuf::AggregateFunction::LastValueAgg => Self::LastValue,
            protobuf::AggregateFunction::NthValueAgg => Self::NthValue,
            protobuf::AggregateFunction::StringAgg => Self::StringAgg,
        }
    }
}

impl From<protobuf::BuiltInWindowFunction> for BuiltInWindowFunction {
    fn from(built_in_function: protobuf::BuiltInWindowFunction) -> Self {
        match built_in_function {
            protobuf::BuiltInWindowFunction::RowNumber => Self::RowNumber,
            protobuf::BuiltInWindowFunction::Rank => Self::Rank,
            protobuf::BuiltInWindowFunction::PercentRank => Self::PercentRank,
            protobuf::BuiltInWindowFunction::DenseRank => Self::DenseRank,
            protobuf::BuiltInWindowFunction::Lag => Self::Lag,
            protobuf::BuiltInWindowFunction::Lead => Self::Lead,
            protobuf::BuiltInWindowFunction::FirstValue => Self::FirstValue,
            protobuf::BuiltInWindowFunction::CumeDist => Self::CumeDist,
            protobuf::BuiltInWindowFunction::Ntile => Self::Ntile,
            protobuf::BuiltInWindowFunction::NthValue => Self::NthValue,
            protobuf::BuiltInWindowFunction::LastValue => Self::LastValue,
        }
    }
}

impl TryFrom<&protobuf::Schema> for Schema {
    type Error = Error;

    fn try_from(schema: &protobuf::Schema) -> Result<Self, Self::Error> {
        let fields = schema
            .columns
            .iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new_with_metadata(fields, schema.metadata.clone()))
    }
}

impl TryFrom<&protobuf::ScalarValue> for ScalarValue {
    type Error = Error;

    fn try_from(scalar: &protobuf::ScalarValue) -> Result<Self, Self::Error> {
        use protobuf::scalar_value::Value;

        let value = scalar
            .value
            .as_ref()
            .ok_or_else(|| Error::required("value"))?;

        Ok(match value {
            Value::BoolValue(v) => Self::Boolean(Some(*v)),
            Value::Utf8Value(v) => Self::Utf8(Some(v.to_owned())),
            Value::LargeUtf8Value(v) => Self::LargeUtf8(Some(v.to_owned())),
            Value::Int8Value(v) => Self::Int8(Some(*v as i8)),
            Value::Int16Value(v) => Self::Int16(Some(*v as i16)),
            Value::Int32Value(v) => Self::Int32(Some(*v)),
            Value::Int64Value(v) => Self::Int64(Some(*v)),
            Value::Uint8Value(v) => Self::UInt8(Some(*v as u8)),
            Value::Uint16Value(v) => Self::UInt16(Some(*v as u16)),
            Value::Uint32Value(v) => Self::UInt32(Some(*v)),
            Value::Uint64Value(v) => Self::UInt64(Some(*v)),
            Value::Float32Value(v) => Self::Float32(Some(*v)),
            Value::Float64Value(v) => Self::Float64(Some(*v)),
            Value::Date32Value(v) => Self::Date32(Some(*v)),
            // ScalarValue::List is serialized using arrow IPC format
            Value::ListValue(v)
            | Value::FixedSizeListValue(v)
            | Value::LargeListValue(v)
            | Value::StructValue(v) => {
                let protobuf::ScalarNestedValue {
                    ipc_message,
                    arrow_data,
                    schema,
                } = &v;

                let schema: Schema = if let Some(schema_ref) = schema {
                    schema_ref.try_into()?
                } else {
                    return Err(Error::General(
                        "Invalid schema while deserializing ScalarValue::List"
                            .to_string(),
                    ));
                };

                let message = root_as_message(ipc_message.as_slice()).map_err(|e| {
                    Error::General(format!(
                        "Error IPC message while deserializing ScalarValue::List: {e}"
                    ))
                })?;
                let buffer = Buffer::from(arrow_data);

                let ipc_batch = message.header_as_record_batch().ok_or_else(|| {
                    Error::General(
                        "Unexpected message type deserializing ScalarValue::List"
                            .to_string(),
                    )
                })?;

                let record_batch = read_record_batch(
                    &buffer,
                    ipc_batch,
                    Arc::new(schema),
                    &Default::default(),
                    None,
                    &message.version(),
                )
                .map_err(|e| arrow_datafusion_err!(e))
                .map_err(|e| e.context("Decoding ScalarValue::List Value"))?;
                let arr = record_batch.column(0);
                match value {
                    Value::ListValue(_) => {
                        Self::List(arr.as_list::<i32>().to_owned().into())
                    }
                    Value::LargeListValue(_) => {
                        Self::LargeList(arr.as_list::<i64>().to_owned().into())
                    }
                    Value::FixedSizeListValue(_) => {
                        Self::FixedSizeList(arr.as_fixed_size_list().to_owned().into())
                    }
                    Value::StructValue(_) => {
                        Self::Struct(arr.as_struct().to_owned().into())
                    }
                    _ => unreachable!(),
                }
            }
            Value::NullValue(v) => {
                let null_type: DataType = v.try_into()?;
                null_type.try_into().map_err(Error::DataFusionError)?
            }
            Value::Decimal128Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal128(
                    Some(i128::from_be_bytes(array)),
                    val.p as u8,
                    val.s as i8,
                )
            }
            Value::Decimal256Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal256(
                    Some(i256::from_be_bytes(array)),
                    val.p as u8,
                    val.s as i8,
                )
            }
            Value::Date64Value(v) => Self::Date64(Some(*v)),
            Value::Time32Value(v) => {
                let time_value =
                    v.value.as_ref().ok_or_else(|| Error::required("value"))?;
                match time_value {
                    protobuf::scalar_time32_value::Value::Time32SecondValue(t) => {
                        Self::Time32Second(Some(*t))
                    }
                    protobuf::scalar_time32_value::Value::Time32MillisecondValue(t) => {
                        Self::Time32Millisecond(Some(*t))
                    }
                }
            }
            Value::Time64Value(v) => {
                let time_value =
                    v.value.as_ref().ok_or_else(|| Error::required("value"))?;
                match time_value {
                    protobuf::scalar_time64_value::Value::Time64MicrosecondValue(t) => {
                        Self::Time64Microsecond(Some(*t))
                    }
                    protobuf::scalar_time64_value::Value::Time64NanosecondValue(t) => {
                        Self::Time64Nanosecond(Some(*t))
                    }
                }
            }
            Value::IntervalYearmonthValue(v) => Self::IntervalYearMonth(Some(*v)),
            Value::IntervalDaytimeValue(v) => Self::IntervalDayTime(Some(*v)),
            Value::DurationSecondValue(v) => Self::DurationSecond(Some(*v)),
            Value::DurationMillisecondValue(v) => Self::DurationMillisecond(Some(*v)),
            Value::DurationMicrosecondValue(v) => Self::DurationMicrosecond(Some(*v)),
            Value::DurationNanosecondValue(v) => Self::DurationNanosecond(Some(*v)),
            Value::TimestampValue(v) => {
                let timezone = if v.timezone.is_empty() {
                    None
                } else {
                    Some(v.timezone.as_str().into())
                };

                let ts_value =
                    v.value.as_ref().ok_or_else(|| Error::required("value"))?;

                match ts_value {
                    protobuf::scalar_timestamp_value::Value::TimeMicrosecondValue(t) => {
                        Self::TimestampMicrosecond(Some(*t), timezone)
                    }
                    protobuf::scalar_timestamp_value::Value::TimeNanosecondValue(t) => {
                        Self::TimestampNanosecond(Some(*t), timezone)
                    }
                    protobuf::scalar_timestamp_value::Value::TimeSecondValue(t) => {
                        Self::TimestampSecond(Some(*t), timezone)
                    }
                    protobuf::scalar_timestamp_value::Value::TimeMillisecondValue(t) => {
                        Self::TimestampMillisecond(Some(*t), timezone)
                    }
                }
            }
            Value::DictionaryValue(v) => {
                let index_type: DataType = v
                    .index_type
                    .as_ref()
                    .ok_or_else(|| Error::required("index_type"))?
                    .try_into()?;

                let value: Self = v
                    .value
                    .as_ref()
                    .ok_or_else(|| Error::required("value"))?
                    .as_ref()
                    .try_into()?;

                Self::Dictionary(Box::new(index_type), Box::new(value))
            }
            Value::BinaryValue(v) => Self::Binary(Some(v.clone())),
            Value::LargeBinaryValue(v) => Self::LargeBinary(Some(v.clone())),
            Value::IntervalMonthDayNano(v) => Self::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(v.months, v.days, v.nanos),
            )),
            Value::UnionValue(val) => {
                let mode = match val.mode {
                    0 => UnionMode::Sparse,
                    1 => UnionMode::Dense,
                    id => Err(Error::unknown("UnionMode", id))?,
                };
                let ids = val
                    .fields
                    .iter()
                    .map(|f| f.field_id as i8)
                    .collect::<Vec<_>>();
                let fields = val
                    .fields
                    .iter()
                    .map(|f| f.field.clone())
                    .collect::<Option<Vec<_>>>();
                let fields = fields.ok_or_else(|| Error::required("UnionField"))?;
                let fields = parse_proto_fields_to_fields(&fields)?;
                let fields = UnionFields::new(ids, fields);
                let v_id = val.value_id as i8;
                let val = match &val.value {
                    None => None,
                    Some(val) => {
                        let val: ScalarValue = val
                            .as_ref()
                            .try_into()
                            .map_err(|_| Error::General("Invalid Scalar".to_string()))?;
                        Some((v_id, Box::new(val)))
                    }
                };
                Self::Union(val, fields, mode)
            }
            Value::FixedSizeBinaryValue(v) => {
                Self::FixedSizeBinary(v.length, Some(v.clone().values))
            }
        })
    }
}

impl TryFrom<protobuf::WindowFrame> for WindowFrame {
    type Error = Error;

    fn try_from(window: protobuf::WindowFrame) -> Result<Self, Self::Error> {
        let units = protobuf::WindowFrameUnits::try_from(window.window_frame_units)
            .map_err(|_| Error::unknown("WindowFrameUnits", window.window_frame_units))?
            .into();
        let start_bound = window.start_bound.required("start_bound")?;
        let end_bound = window
            .end_bound
            .map(|end_bound| match end_bound {
                protobuf::window_frame::EndBound::Bound(end_bound) => {
                    end_bound.try_into()
                }
            })
            .transpose()?
            .unwrap_or(WindowFrameBound::CurrentRow);
        Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
    }
}

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = Error;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type =
            protobuf::WindowFrameBoundType::try_from(bound.window_frame_bound_type)
                .map_err(|_| {
                    Error::unknown("WindowFrameBoundType", bound.window_frame_bound_type)
                })?;
        match bound_type {
            protobuf::WindowFrameBoundType::CurrentRow => Ok(Self::CurrentRow),
            protobuf::WindowFrameBoundType::Preceding => match bound.bound_value {
                Some(x) => Ok(Self::Preceding(ScalarValue::try_from(&x)?)),
                None => Ok(Self::Preceding(ScalarValue::UInt64(None))),
            },
            protobuf::WindowFrameBoundType::Following => match bound.bound_value {
                Some(x) => Ok(Self::Following(ScalarValue::try_from(&x)?)),
                None => Ok(Self::Following(ScalarValue::UInt64(None))),
            },
        }
    }
}

impl From<protobuf::TimeUnit> for TimeUnit {
    fn from(time_unit: protobuf::TimeUnit) -> Self {
        match time_unit {
            protobuf::TimeUnit::Second => TimeUnit::Second,
            protobuf::TimeUnit::Millisecond => TimeUnit::Millisecond,
            protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
            protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl From<protobuf::IntervalUnit> for IntervalUnit {
    fn from(interval_unit: protobuf::IntervalUnit) -> Self {
        match interval_unit {
            protobuf::IntervalUnit::YearMonth => IntervalUnit::YearMonth,
            protobuf::IntervalUnit::DayTime => IntervalUnit::DayTime,
            protobuf::IntervalUnit::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}

impl From<protobuf::JoinType> for JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        match t {
            protobuf::JoinType::Inner => JoinType::Inner,
            protobuf::JoinType::Left => JoinType::Left,
            protobuf::JoinType::Right => JoinType::Right,
            protobuf::JoinType::Full => JoinType::Full,
            protobuf::JoinType::Leftsemi => JoinType::LeftSemi,
            protobuf::JoinType::Rightsemi => JoinType::RightSemi,
            protobuf::JoinType::Leftanti => JoinType::LeftAnti,
            protobuf::JoinType::Rightanti => JoinType::RightAnti,
        }
    }
}

impl From<protobuf::JoinConstraint> for JoinConstraint {
    fn from(t: protobuf::JoinConstraint) -> Self {
        match t {
            protobuf::JoinConstraint::On => JoinConstraint::On,
            protobuf::JoinConstraint::Using => JoinConstraint::Using,
        }
    }
}

impl From<protobuf::Constraints> for Constraints {
    fn from(constraints: protobuf::Constraints) -> Self {
        Constraints::new_unverified(
            constraints
                .constraints
                .into_iter()
                .map(|item| item.into())
                .collect(),
        )
    }
}

impl From<protobuf::Constraint> for Constraint {
    fn from(value: protobuf::Constraint) -> Self {
        match value.constraint_mode.unwrap() {
            protobuf::constraint::ConstraintMode::PrimaryKey(elem) => {
                Constraint::PrimaryKey(
                    elem.indices.into_iter().map(|item| item as usize).collect(),
                )
            }
            protobuf::constraint::ConstraintMode::Unique(elem) => Constraint::Unique(
                elem.indices.into_iter().map(|item| item as usize).collect(),
            ),
        }
    }
}

pub fn parse_i32_to_time_unit(value: &i32) -> Result<TimeUnit, Error> {
    protobuf::TimeUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| Error::unknown("TimeUnit", *value))
}

pub fn parse_i32_to_interval_unit(value: &i32) -> Result<IntervalUnit, Error> {
    protobuf::IntervalUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| Error::unknown("IntervalUnit", *value))
}

pub fn parse_i32_to_aggregate_function(value: &i32) -> Result<AggregateFunction, Error> {
    protobuf::AggregateFunction::try_from(*value)
        .map(|a| a.into())
        .map_err(|_| Error::unknown("AggregateFunction", *value))
}

pub fn parse_expr(
    proto: &protobuf::LogicalExprNode,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Expr, Error> {
    use protobuf::{logical_expr_node::ExprType, window_expr_node, ScalarFunction};

    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| Error::required("expr_type"))?;

    match expr_type {
        ExprType::BinaryExpr(binary_expr) => {
            let op = from_proto_binary_op(&binary_expr.op)?;
            let operands = parse_exprs(&binary_expr.operands, registry, codec)?;

            if operands.len() < 2 {
                return Err(proto_error(
                    "A binary expression must always have at least 2 operands",
                ));
            }

            // Reduce the linearized operands (ordered by left innermost to right
            // outermost) into a single expression tree.
            Ok(operands
                .into_iter()
                .reduce(|left, right| {
                    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
                })
                .expect("Binary expression could not be reduced to a single expression."))
        }
        ExprType::GetIndexedField(get_indexed_field) => {
            let expr = parse_required_expr(
                get_indexed_field.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?;
            let field = match &get_indexed_field.field {
                Some(protobuf::get_indexed_field::Field::NamedStructField(
                    named_struct_field,
                )) => GetFieldAccess::NamedStructField {
                    name: named_struct_field
                        .name
                        .as_ref()
                        .ok_or_else(|| Error::required("value"))?
                        .try_into()?,
                },
                Some(protobuf::get_indexed_field::Field::ListIndex(list_index)) => {
                    GetFieldAccess::ListIndex {
                        key: Box::new(parse_required_expr(
                            list_index.key.as_deref(),
                            registry,
                            "key",
                            codec,
                        )?),
                    }
                }
                Some(protobuf::get_indexed_field::Field::ListRange(list_range)) => {
                    GetFieldAccess::ListRange {
                        start: Box::new(parse_required_expr(
                            list_range.start.as_deref(),
                            registry,
                            "start",
                            codec,
                        )?),
                        stop: Box::new(parse_required_expr(
                            list_range.stop.as_deref(),
                            registry,
                            "stop",
                            codec,
                        )?),
                        stride: Box::new(parse_required_expr(
                            list_range.stride.as_deref(),
                            registry,
                            "stride",
                            codec,
                        )?),
                    }
                }
                None => return Err(proto_error("Field must not be None")),
            };

            Ok(Expr::GetIndexedField(GetIndexedField::new(
                Box::new(expr),
                field,
            )))
        }
        ExprType::Column(column) => Ok(Expr::Column(column.into())),
        ExprType::Literal(literal) => {
            let scalar_value: ScalarValue = literal.try_into()?;
            Ok(Expr::Literal(scalar_value))
        }
        ExprType::WindowExpr(expr) => {
            let window_function = expr
                .window_function
                .as_ref()
                .ok_or_else(|| Error::required("window_function"))?;
            let partition_by = parse_exprs(&expr.partition_by, registry, codec)?;
            let mut order_by = parse_exprs(&expr.order_by, registry, codec)?;
            let window_frame = expr
                .window_frame
                .as_ref()
                .map::<Result<WindowFrame, _>, _>(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    check_window_frame(&window_frame, order_by.len())
                        .map(|_| window_frame)
                })
                .transpose()?
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "missing window frame during deserialization".to_string(),
                    )
                })?;
            // TODO: support proto for null treatment
            let null_treatment = None;
            regularize_window_order_by(&window_frame, &mut order_by)?;

            match window_function {
                window_expr_node::WindowFunction::AggrFunction(i) => {
                    let aggr_function = parse_i32_to_aggregate_function(i)?;

                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::AggregateFunction(
                            aggr_function,
                        ),
                        vec![parse_required_expr(expr.expr.as_deref(), registry, "expr", codec)?],
                        partition_by,
                        order_by,
                        window_frame,
                        None
                    )))
                }
                window_expr_node::WindowFunction::BuiltInFunction(i) => {
                    let built_in_function = protobuf::BuiltInWindowFunction::try_from(*i)
                        .map_err(|_| Error::unknown("BuiltInWindowFunction", *i))?
                        .into();

                    let args =
                        parse_optional_expr(expr.expr.as_deref(), registry, codec)?
                            .map(|e| vec![e])
                            .unwrap_or_else(Vec::new);

                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::BuiltInWindowFunction(
                            built_in_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        null_treatment
                    )))
                }
                window_expr_node::WindowFunction::Udaf(udaf_name) => {
                    let udaf_function = registry.udaf(udaf_name)?;
                    let args =
                        parse_optional_expr(expr.expr.as_deref(), registry, codec)?
                            .map(|e| vec![e])
                            .unwrap_or_else(Vec::new);
                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::AggregateUDF(
                            udaf_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        None,
                    )))
                }
                window_expr_node::WindowFunction::Udwf(udwf_name) => {
                    let udwf_function = registry.udwf(udwf_name)?;
                    let args =
                        parse_optional_expr(expr.expr.as_deref(), registry, codec)?
                            .map(|e| vec![e])
                            .unwrap_or_else(Vec::new);
                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::WindowUDF(
                            udwf_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        None,
                    )))
                }
            }
        }
        ExprType::AggregateExpr(expr) => {
            let fun = parse_i32_to_aggregate_function(&expr.aggr_function)?;

            Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                fun,
                parse_exprs(&expr.expr, registry, codec)?,
                expr.distinct,
                parse_optional_expr(expr.filter.as_deref(), registry, codec)?
                    .map(Box::new),
                parse_vec_expr(&expr.order_by, registry, codec)?,
                None,
            )))
        }
        ExprType::Alias(alias) => Ok(Expr::Alias(Alias::new(
            parse_required_expr(alias.expr.as_deref(), registry, "expr", codec)?,
            alias
                .relation
                .first()
                .map(|r| OwnedTableReference::try_from(r.clone()))
                .transpose()?,
            alias.alias.clone(),
        ))),
        ExprType::IsNullExpr(is_null) => Ok(Expr::IsNull(Box::new(parse_required_expr(
            is_null.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
            parse_required_expr(is_not_null.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::NotExpr(not) => Ok(Expr::Not(Box::new(parse_required_expr(
            not.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsTrue(msg) => Ok(Expr::IsTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsFalse(msg) => Ok(Expr::IsFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsUnknown(msg) => Ok(Expr::IsUnknown(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotTrue(msg) => Ok(Expr::IsNotTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotFalse(msg) => Ok(Expr::IsNotFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotUnknown(msg) => Ok(Expr::IsNotUnknown(Box::new(
            parse_required_expr(msg.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::Between(between) => Ok(Expr::Between(Between::new(
            Box::new(parse_required_expr(
                between.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            between.negated,
            Box::new(parse_required_expr(
                between.low.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                between.high.as_deref(),
                registry,
                "expr",
                codec,
            )?),
        ))),
        ExprType::Like(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            false,
        ))),
        ExprType::Ilike(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            true,
        ))),
        ExprType::SimilarTo(like) => Ok(Expr::SimilarTo(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            false,
        ))),
        ExprType::Case(case) => {
            let when_then_expr = case
                .when_then_expr
                .iter()
                .map(|e| {
                    let when_expr = parse_required_expr(
                        e.when_expr.as_ref(),
                        registry,
                        "when_expr",
                        codec,
                    )?;
                    let then_expr = parse_required_expr(
                        e.then_expr.as_ref(),
                        registry,
                        "then_expr",
                        codec,
                    )?;
                    Ok((Box::new(when_expr), Box::new(then_expr)))
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, Error>>()?;
            Ok(Expr::Case(Case::new(
                parse_optional_expr(case.expr.as_deref(), registry, codec)?.map(Box::new),
                when_then_expr,
                parse_optional_expr(case.else_expr.as_deref(), registry, codec)?
                    .map(Box::new),
            )))
        }
        ExprType::Cast(cast) => {
            let expr = Box::new(parse_required_expr(
                cast.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::Cast(Cast::new(expr, data_type)))
        }
        ExprType::TryCast(cast) => {
            let expr = Box::new(parse_required_expr(
                cast.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::TryCast(TryCast::new(expr, data_type)))
        }
        ExprType::Sort(sort) => Ok(Expr::Sort(Sort::new(
            Box::new(parse_required_expr(
                sort.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            sort.asc,
            sort.nulls_first,
        ))),
        ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(
            parse_required_expr(negative.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::Unnest(unnest) => {
            let exprs = parse_exprs(&unnest.exprs, registry, codec)?;
            Ok(Expr::Unnest(Unnest { exprs }))
        }
        ExprType::InList(in_list) => Ok(Expr::InList(InList::new(
            Box::new(parse_required_expr(
                in_list.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            parse_exprs(&in_list.list, registry, codec)?,
            in_list.negated,
        ))),
        ExprType::Wildcard(protobuf::Wildcard { qualifier }) => Ok(Expr::Wildcard {
            qualifier: if qualifier.is_empty() {
                None
            } else {
                Some(qualifier.clone())
            },
        }),
        ExprType::ScalarFunction(expr) => {
            let scalar_function = protobuf::ScalarFunction::try_from(expr.fun)
                .map_err(|_| Error::unknown("ScalarFunction", expr.fun))?;
            let args = &expr.args;

            match scalar_function {
                ScalarFunction::Unknown => Err(proto_error("Unknown scalar function")),
                ScalarFunction::Asinh => {
                    Ok(asinh(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Acosh => {
                    Ok(acosh(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Sqrt => Ok(sqrt(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Cbrt => Ok(cbrt(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Sin => Ok(sin(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Cos => Ok(cos(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Atan => Ok(atan(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Sinh => Ok(sinh(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Cosh => Ok(cosh(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Atanh => {
                    Ok(atanh(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Exp => Ok(exp(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Degrees => {
                    Ok(degrees(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Radians => {
                    Ok(radians(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Log2 => Ok(log2(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Ln => Ok(ln(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Log10 => {
                    Ok(log10(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Floor => {
                    Ok(floor(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Factorial => {
                    Ok(factorial(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Ceil => Ok(ceil(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Round => Ok(round(parse_exprs(args, registry, codec)?)),
                ScalarFunction::Trunc => Ok(trunc(parse_exprs(args, registry, codec)?)),
                ScalarFunction::Signum => {
                    Ok(signum(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::InitCap => {
                    Ok(initcap(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::Gcd => Ok(gcd(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Lcm => Ok(lcm(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Random => Ok(random()),
                ScalarFunction::Concat => {
                    Ok(concat_expr(parse_exprs(args, registry, codec)?))
                }
                ScalarFunction::ConcatWithSeparator => {
                    Ok(concat_ws_expr(parse_exprs(args, registry, codec)?))
                }
                ScalarFunction::EndsWith => Ok(ends_with(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Strpos => Ok(strpos(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Substr => {
                    if args.len() > 2 {
                        assert_eq!(args.len(), 3);
                        Ok(substring(
                            parse_expr(&args[0], registry, codec)?,
                            parse_expr(&args[1], registry, codec)?,
                            parse_expr(&args[2], registry, codec)?,
                        ))
                    } else {
                        Ok(substr(
                            parse_expr(&args[0], registry, codec)?,
                            parse_expr(&args[1], registry, codec)?,
                        ))
                    }
                }
                ScalarFunction::Translate => Ok(translate(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                    parse_expr(&args[2], registry, codec)?,
                )),
                ScalarFunction::Coalesce => {
                    Ok(coalesce(parse_exprs(args, registry, codec)?))
                }
                ScalarFunction::Pi => Ok(pi()),
                ScalarFunction::Power => Ok(power(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Log => Ok(log(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Atan2 => Ok(atan2(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Cot => Ok(cot(parse_expr(&args[0], registry, codec)?)),
                ScalarFunction::Nanvl => Ok(nanvl(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
                ScalarFunction::Iszero => {
                    Ok(iszero(parse_expr(&args[0], registry, codec)?))
                }
                ScalarFunction::SubstrIndex => Ok(substr_index(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                    parse_expr(&args[2], registry, codec)?,
                )),
                ScalarFunction::FindInSet => Ok(find_in_set(
                    parse_expr(&args[0], registry, codec)?,
                    parse_expr(&args[1], registry, codec)?,
                )),
            }
        }
        ExprType::ScalarUdfExpr(protobuf::ScalarUdfExprNode {
            fun_name,
            args,
            fun_definition,
        }) => {
            let scalar_fn = match fun_definition {
                Some(buf) => codec.try_decode_udf(fun_name, buf)?,
                None => registry.udf(fun_name.as_str())?,
            };
            Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
                scalar_fn,
                parse_exprs(args, registry, codec)?,
            )))
        }
        ExprType::AggregateUdfExpr(pb) => {
            let agg_fn = registry.udaf(pb.fun_name.as_str())?;

            Ok(Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                agg_fn,
                parse_exprs(&pb.args, registry, codec)?,
                false,
                parse_optional_expr(pb.filter.as_deref(), registry, codec)?.map(Box::new),
                parse_vec_expr(&pb.order_by, registry, codec)?,
            )))
        }

        ExprType::GroupingSet(GroupingSetNode { expr }) => {
            Ok(Expr::GroupingSet(GroupingSets(
                expr.iter()
                    .map(|expr_list| parse_exprs(&expr_list.expr, registry, codec))
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
        }
        ExprType::Cube(CubeNode { expr }) => Ok(Expr::GroupingSet(GroupingSet::Cube(
            parse_exprs(expr, registry, codec)?,
        ))),
        ExprType::Rollup(RollupNode { expr }) => Ok(Expr::GroupingSet(
            GroupingSet::Rollup(parse_exprs(expr, registry, codec)?),
        )),
        ExprType::Placeholder(PlaceholderNode { id, data_type }) => match data_type {
            None => Ok(Expr::Placeholder(Placeholder::new(id.clone(), None))),
            Some(data_type) => Ok(Expr::Placeholder(Placeholder::new(
                id.clone(),
                Some(data_type.try_into()?),
            ))),
        },
    }
}

/// Parse a vector of `protobuf::LogicalExprNode`s.
pub fn parse_exprs<'a, I>(
    protos: I,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<Expr>, Error>
where
    I: IntoIterator<Item = &'a protobuf::LogicalExprNode>,
{
    let res = protos
        .into_iter()
        .map(|elem| {
            parse_expr(elem, registry, codec).map_err(|e| plan_datafusion_err!("{}", e))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(res)
}

/// Parse an optional escape_char for Like, ILike, SimilarTo
fn parse_escape_char(s: &str) -> Result<Option<char>> {
    match s.len() {
        0 => Ok(None),
        1 => Ok(s.chars().next()),
        _ => internal_err!("Invalid length for escape char"),
    }
}

// panic here because no better way to convert from Vec to Array
fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into().unwrap_or_else(|v: Vec<T>| {
        panic!("Expected a Vec of length {} but it was {}", N, v.len())
    })
}

pub fn from_proto_binary_op(op: &str) -> Result<Operator, Error> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "IsDistinctFrom" => Ok(Operator::IsDistinctFrom),
        "IsNotDistinctFrom" => Ok(Operator::IsNotDistinctFrom),
        "BitwiseAnd" => Ok(Operator::BitwiseAnd),
        "BitwiseOr" => Ok(Operator::BitwiseOr),
        "BitwiseXor" => Ok(Operator::BitwiseXor),
        "BitwiseShiftLeft" => Ok(Operator::BitwiseShiftLeft),
        "BitwiseShiftRight" => Ok(Operator::BitwiseShiftRight),
        "RegexIMatch" => Ok(Operator::RegexIMatch),
        "RegexMatch" => Ok(Operator::RegexMatch),
        "RegexNotIMatch" => Ok(Operator::RegexNotIMatch),
        "RegexNotMatch" => Ok(Operator::RegexNotMatch),
        "StringConcat" => Ok(Operator::StringConcat),
        "AtArrow" => Ok(Operator::AtArrow),
        "ArrowAt" => Ok(Operator::ArrowAt),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{other:?}'"
        ))),
    }
}

fn parse_vec_expr(
    p: &[protobuf::LogicalExprNode],
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Option<Vec<Expr>>, Error> {
    let res = parse_exprs(p, registry, codec)?;
    // Convert empty vector to None.
    Ok((!res.is_empty()).then_some(res))
}

fn parse_optional_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Option<Expr>, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry, codec).map(Some),
        None => Ok(None),
    }
}

fn parse_required_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: impl Into<String>,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Expr, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry, codec),
        None => Err(Error::required(field)),
    }
}

fn proto_error<S: Into<String>>(message: S) -> Error {
    Error::General(message.into())
}

/// Converts a vector of `protobuf::Field`s to `Arc<arrow::Field>`s.
fn parse_proto_fields_to_fields<'a, I>(
    fields: I,
) -> std::result::Result<Vec<Field>, Error>
where
    I: IntoIterator<Item = &'a protobuf::Field>,
{
    fields
        .into_iter()
        .map(Field::try_from)
        .collect::<Result<_, _>>()
}
