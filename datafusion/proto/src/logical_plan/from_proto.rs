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

use crate::protobuf::{
    self,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, InitialLogicalPlan, InitialPhysicalPlan, OptimizedLogicalPlan,
        OptimizedPhysicalPlan,
    },
    AnalyzedLogicalPlanType, CubeNode, GroupingSetNode, OptimizedLogicalPlanType,
    OptimizedPhysicalPlanType, PlaceholderNode, RollupNode,
};
use arrow::datatypes::{
    DataType, Field, IntervalMonthDayNanoType, IntervalUnit, Schema, TimeUnit,
    UnionFields, UnionMode,
};
use datafusion::execution::registry::FunctionRegistry;
use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result,
    ScalarValue,
};
use datafusion_expr::expr::Placeholder;
use datafusion_expr::{
    abs, acos, acosh, array, ascii, asin, asinh, atan, atan2, atanh, bit_length, btrim,
    cbrt, ceil, character_length, chr, coalesce, concat_expr, concat_ws_expr, cos, cosh,
    date_bin, date_part, date_trunc, degrees, digest, exp,
    expr::{self, InList, Sort, WindowFunction},
    factorial, floor, from_unixtime, gcd, greatest, lcm, least, left, ln, log, log10,
    log2,
    logical_plan::{PlanType, StringifiedPlan},
    lower, lpad, ltrim, md5, now, nullif, octet_length, pi, power, radians, random,
    regexp_match, regexp_replace, repeat, replace, reverse, right, round, rpad, rtrim,
    sha224, sha256, sha384, sha512, signum, sin, sinh, split_part, sqrt, starts_with,
    strpos, substr, substring, tan, tanh, to_hex, to_timestamp_micros,
    to_timestamp_millis, to_timestamp_seconds, translate, trim, trunc, upper, uuid,
    window_frame::regularize,
    AggregateFunction, Between, BinaryExpr, BuiltInWindowFunction, BuiltinScalarFunction,
    Case, Cast, Expr, GetIndexedField, GroupingSet,
    GroupingSet::GroupingSets,
    JoinConstraint, JoinType, Like, Operator, TryCast, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use std::sync::Arc;

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
                strct
                    .sub_field_types
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode = protobuf::UnionMode::from_i32(union.union_mode)
                    .ok_or_else(|| Error::unknown("UnionMode", union.union_mode))?;
                let union_mode = match union_mode {
                    protobuf::UnionMode::Dense => UnionMode::Dense,
                    protobuf::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_fields = union
                    .union_types
                    .iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<Field>, _>>()?;

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

        Ok(Self::new(field.name.as_str(), datatype, field.nullable))
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
                OptimizedPhysicalPlan(OptimizedPhysicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedPhysicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalPhysicalPlan(_) => PlanType::FinalPhysicalPlan,
            },
            plan: Arc::new(stringified_plan.plan.clone()),
        }
    }
}

impl From<&protobuf::ScalarFunction> for BuiltinScalarFunction {
    fn from(f: &protobuf::ScalarFunction) -> Self {
        use protobuf::ScalarFunction;
        match f {
            ScalarFunction::Sqrt => Self::Sqrt,
            ScalarFunction::Cbrt => Self::Cbrt,
            ScalarFunction::Sin => Self::Sin,
            ScalarFunction::Cos => Self::Cos,
            ScalarFunction::Tan => Self::Tan,
            ScalarFunction::Asin => Self::Asin,
            ScalarFunction::Acos => Self::Acos,
            ScalarFunction::Atan => Self::Atan,
            ScalarFunction::Sinh => Self::Sinh,
            ScalarFunction::Cosh => Self::Cosh,
            ScalarFunction::Tanh => Self::Tanh,
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
            ScalarFunction::Abs => Self::Abs,
            ScalarFunction::OctetLength => Self::OctetLength,
            ScalarFunction::Concat => Self::Concat,
            ScalarFunction::Lower => Self::Lower,
            ScalarFunction::Upper => Self::Upper,
            ScalarFunction::Trim => Self::Trim,
            ScalarFunction::Ltrim => Self::Ltrim,
            ScalarFunction::Rtrim => Self::Rtrim,
            ScalarFunction::ToTimestamp => Self::ToTimestamp,
            ScalarFunction::Array => Self::MakeArray,
            ScalarFunction::NullIf => Self::NullIf,
            ScalarFunction::DatePart => Self::DatePart,
            ScalarFunction::DateTrunc => Self::DateTrunc,
            ScalarFunction::DateBin => Self::DateBin,
            ScalarFunction::Md5 => Self::MD5,
            ScalarFunction::Sha224 => Self::SHA224,
            ScalarFunction::Sha256 => Self::SHA256,
            ScalarFunction::Sha384 => Self::SHA384,
            ScalarFunction::Sha512 => Self::SHA512,
            ScalarFunction::Digest => Self::Digest,
            ScalarFunction::ToTimestampMillis => Self::ToTimestampMillis,
            ScalarFunction::Log2 => Self::Log2,
            ScalarFunction::Signum => Self::Signum,
            ScalarFunction::Ascii => Self::Ascii,
            ScalarFunction::BitLength => Self::BitLength,
            ScalarFunction::Btrim => Self::Btrim,
            ScalarFunction::CharacterLength => Self::CharacterLength,
            ScalarFunction::Chr => Self::Chr,
            ScalarFunction::ConcatWithSeparator => Self::ConcatWithSeparator,
            ScalarFunction::InitCap => Self::InitCap,
            ScalarFunction::Left => Self::Left,
            ScalarFunction::Lpad => Self::Lpad,
            ScalarFunction::Random => Self::Random,
            ScalarFunction::RegexpReplace => Self::RegexpReplace,
            ScalarFunction::Repeat => Self::Repeat,
            ScalarFunction::Replace => Self::Replace,
            ScalarFunction::Reverse => Self::Reverse,
            ScalarFunction::Right => Self::Right,
            ScalarFunction::Rpad => Self::Rpad,
            ScalarFunction::SplitPart => Self::SplitPart,
            ScalarFunction::StartsWith => Self::StartsWith,
            ScalarFunction::Strpos => Self::Strpos,
            ScalarFunction::Substr => Self::Substr,
            ScalarFunction::ToHex => Self::ToHex,
            ScalarFunction::ToTimestampMicros => Self::ToTimestampMicros,
            ScalarFunction::ToTimestampSeconds => Self::ToTimestampSeconds,
            ScalarFunction::Now => Self::Now,
            ScalarFunction::CurrentDate => Self::CurrentDate,
            ScalarFunction::CurrentTime => Self::CurrentTime,
            ScalarFunction::Uuid => Self::Uuid,
            ScalarFunction::Translate => Self::Translate,
            ScalarFunction::RegexpMatch => Self::RegexpMatch,
            ScalarFunction::Coalesce => Self::Coalesce,
            ScalarFunction::Pi => Self::Pi,
            ScalarFunction::Power => Self::Power,
            ScalarFunction::StructFun => Self::Struct,
            ScalarFunction::FromUnixtime => Self::FromUnixtime,
            ScalarFunction::Atan2 => Self::Atan2,
            ScalarFunction::ArrowTypeof => Self::ArrowTypeof,
            ScalarFunction::Greatest => Self::Greatest,
            ScalarFunction::Least => Self::Least,
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
            .map(|c| {
                let pb_arrow_type_res = c
                    .arrow_type
                    .as_ref()
                    .ok_or_else(|| proto_error("Protobuf deserialization error: Field message was missing required field 'arrow_type'"));
                let pb_arrow_type: &protobuf::ArrowType = match pb_arrow_type_res {
                    Ok(res) => res,
                    Err(e) => return Err(e),
                };
                Ok(Field::new(&c.name, pb_arrow_type.try_into()?, c.nullable))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(fields))
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
            Value::ListValue(scalar_list) => {
                let protobuf::ScalarListValue {
                    is_null,
                    values,
                    field,
                } = &scalar_list;

                let field: Field = field.as_ref().required("field")?;
                let field = Arc::new(field);

                let values: Result<Vec<ScalarValue>, Error> =
                    values.iter().map(|val| val.try_into()).collect();
                let values = values?;

                validate_list_values(field.as_ref(), &values)?;

                let values = if *is_null { None } else { Some(values) };

                Self::List(values, field)
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
            Value::StructValue(v) => {
                // all structs must have at least 1 field, so we treat
                // an empty values list as NULL
                let values = if v.field_values.is_empty() {
                    None
                } else {
                    Some(
                        v.field_values
                            .iter()
                            .map(|v| v.try_into())
                            .collect::<Result<Vec<ScalarValue>, _>>()?,
                    )
                };

                let fields = v
                    .fields
                    .iter()
                    .map(Field::try_from)
                    .collect::<Result<_, _>>()?;

                Self::Struct(values, fields)
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
        let units = protobuf::WindowFrameUnits::from_i32(window.window_frame_units)
            .ok_or_else(|| Error::unknown("WindowFrameUnits", window.window_frame_units))?
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
        Ok(Self {
            units,
            start_bound,
            end_bound,
        })
    }
}

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = Error;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type =
            protobuf::WindowFrameBoundType::from_i32(bound.window_frame_bound_type)
                .ok_or_else(|| {
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

pub fn parse_i32_to_time_unit(value: &i32) -> Result<TimeUnit, Error> {
    protobuf::TimeUnit::from_i32(*value)
        .map(|t| t.into())
        .ok_or_else(|| Error::unknown("TimeUnit", *value))
}

pub fn parse_i32_to_interval_unit(value: &i32) -> Result<IntervalUnit, Error> {
    protobuf::IntervalUnit::from_i32(*value)
        .map(|t| t.into())
        .ok_or_else(|| Error::unknown("IntervalUnit", *value))
}

pub fn parse_i32_to_aggregate_function(value: &i32) -> Result<AggregateFunction, Error> {
    protobuf::AggregateFunction::from_i32(*value)
        .map(|a| a.into())
        .ok_or_else(|| Error::unknown("AggregateFunction", *value))
}

/// Ensures that all `values` are of type DataType::List and have the
/// same type as field
fn validate_list_values(field: &Field, values: &[ScalarValue]) -> Result<(), Error> {
    for value in values {
        let field_type = field.data_type();
        let value_type = value.get_datatype();

        if field_type != &value_type {
            return Err(proto_error(format!(
                "Expected field type {field_type:?}, got scalar of type: {value_type:?}"
            )));
        }
    }
    Ok(())
}

pub fn parse_expr(
    proto: &protobuf::LogicalExprNode,
    registry: &dyn FunctionRegistry,
) -> Result<Expr, Error> {
    use protobuf::{logical_expr_node::ExprType, window_expr_node, ScalarFunction};

    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| Error::required("expr_type"))?;

    match expr_type {
        ExprType::BinaryExpr(binary_expr) => {
            let op = from_proto_binary_op(&binary_expr.op)?;
            let operands = binary_expr
                .operands
                .iter()
                .map(|expr| parse_expr(expr, registry))
                .collect::<Result<Vec<_>, _>>()?;

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
        ExprType::GetIndexedField(field) => {
            let key = field
                .key
                .as_ref()
                .ok_or_else(|| Error::required("value"))?
                .try_into()?;

            let expr = parse_required_expr(field.expr.as_deref(), registry, "expr")?;

            Ok(Expr::GetIndexedField(GetIndexedField::new(
                Box::new(expr),
                key,
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
            let partition_by = expr
                .partition_by
                .iter()
                .map(|e| parse_expr(e, registry))
                .collect::<Result<Vec<_>, _>>()?;
            let order_by = expr
                .order_by
                .iter()
                .map(|e| parse_expr(e, registry))
                .collect::<Result<Vec<_>, _>>()?;
            let window_frame = expr
                .window_frame
                .as_ref()
                .map::<Result<WindowFrame, _>, _>(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    regularize(window_frame, order_by.len())
                })
                .transpose()?
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "missing window frame during deserialization".to_string(),
                    )
                })?;

            match window_function {
                window_expr_node::WindowFunction::AggrFunction(i) => {
                    let aggr_function = parse_i32_to_aggregate_function(i)?;

                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::window_function::WindowFunction::AggregateFunction(
                            aggr_function,
                        ),
                        vec![parse_required_expr(expr.expr.as_deref(), registry, "expr")?],
                        partition_by,
                        order_by,
                        window_frame,
                    )))
                }
                window_expr_node::WindowFunction::BuiltInFunction(i) => {
                    let built_in_function = protobuf::BuiltInWindowFunction::from_i32(*i)
                        .ok_or_else(|| Error::unknown("BuiltInWindowFunction", *i))?
                        .into();

                    let args = parse_optional_expr(expr.expr.as_deref(), registry)?
                        .map(|e| vec![e])
                        .unwrap_or_else(Vec::new);

                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::window_function::WindowFunction::BuiltInWindowFunction(
                            built_in_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                    )))
                }
            }
        }
        ExprType::AggregateExpr(expr) => {
            let fun = parse_i32_to_aggregate_function(&expr.aggr_function)?;

            Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                fun,
                expr.expr
                    .iter()
                    .map(|e| parse_expr(e, registry))
                    .collect::<Result<Vec<_>, _>>()?,
                expr.distinct,
                parse_optional_expr(expr.filter.as_deref(), registry)?.map(Box::new),
                parse_vec_expr(&expr.order_by, registry)?,
            )))
        }
        ExprType::Alias(alias) => Ok(Expr::Alias(
            Box::new(parse_required_expr(
                alias.expr.as_deref(),
                registry,
                "expr",
            )?),
            alias.alias.clone(),
        )),
        ExprType::IsNullExpr(is_null) => Ok(Expr::IsNull(Box::new(parse_required_expr(
            is_null.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
            parse_required_expr(is_not_null.expr.as_deref(), registry, "expr")?,
        ))),
        ExprType::NotExpr(not) => Ok(Expr::Not(Box::new(parse_required_expr(
            not.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsTrue(msg) => Ok(Expr::IsTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsFalse(msg) => Ok(Expr::IsFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsUnknown(msg) => Ok(Expr::IsUnknown(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsNotTrue(msg) => Ok(Expr::IsNotTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsNotFalse(msg) => Ok(Expr::IsNotFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
        )?))),
        ExprType::IsNotUnknown(msg) => Ok(Expr::IsNotUnknown(Box::new(
            parse_required_expr(msg.expr.as_deref(), registry, "expr")?,
        ))),
        ExprType::Between(between) => Ok(Expr::Between(Between::new(
            Box::new(parse_required_expr(
                between.expr.as_deref(),
                registry,
                "expr",
            )?),
            between.negated,
            Box::new(parse_required_expr(
                between.low.as_deref(),
                registry,
                "expr",
            )?),
            Box::new(parse_required_expr(
                between.high.as_deref(),
                registry,
                "expr",
            )?),
        ))),
        ExprType::Like(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(like.expr.as_deref(), registry, "expr")?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
            )?),
            parse_escape_char(&like.escape_char)?,
        ))),
        ExprType::Ilike(like) => Ok(Expr::ILike(Like::new(
            like.negated,
            Box::new(parse_required_expr(like.expr.as_deref(), registry, "expr")?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
            )?),
            parse_escape_char(&like.escape_char)?,
        ))),
        ExprType::SimilarTo(like) => Ok(Expr::SimilarTo(Like::new(
            like.negated,
            Box::new(parse_required_expr(like.expr.as_deref(), registry, "expr")?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
            )?),
            parse_escape_char(&like.escape_char)?,
        ))),
        ExprType::Case(case) => {
            let when_then_expr = case
                .when_then_expr
                .iter()
                .map(|e| {
                    let when_expr =
                        parse_required_expr(e.when_expr.as_ref(), registry, "when_expr")?;
                    let then_expr =
                        parse_required_expr(e.then_expr.as_ref(), registry, "then_expr")?;
                    Ok((Box::new(when_expr), Box::new(then_expr)))
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, Error>>()?;
            Ok(Expr::Case(Case::new(
                parse_optional_expr(case.expr.as_deref(), registry)?.map(Box::new),
                when_then_expr,
                parse_optional_expr(case.else_expr.as_deref(), registry)?.map(Box::new),
            )))
        }
        ExprType::Cast(cast) => {
            let expr =
                Box::new(parse_required_expr(cast.expr.as_deref(), registry, "expr")?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::Cast(Cast::new(expr, data_type)))
        }
        ExprType::TryCast(cast) => {
            let expr =
                Box::new(parse_required_expr(cast.expr.as_deref(), registry, "expr")?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::TryCast(TryCast::new(expr, data_type)))
        }
        ExprType::Sort(sort) => Ok(Expr::Sort(Sort::new(
            Box::new(parse_required_expr(sort.expr.as_deref(), registry, "expr")?),
            sort.asc,
            sort.nulls_first,
        ))),
        ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(
            parse_required_expr(negative.expr.as_deref(), registry, "expr")?,
        ))),
        ExprType::InList(in_list) => Ok(Expr::InList(InList::new(
            Box::new(parse_required_expr(
                in_list.expr.as_deref(),
                registry,
                "expr",
            )?),
            in_list
                .list
                .iter()
                .map(|expr| parse_expr(expr, registry))
                .collect::<Result<Vec<_>, _>>()?,
            in_list.negated,
        ))),
        ExprType::Wildcard(_) => Ok(Expr::Wildcard),
        ExprType::ScalarFunction(expr) => {
            let scalar_function = protobuf::ScalarFunction::from_i32(expr.fun)
                .ok_or_else(|| Error::unknown("ScalarFunction", expr.fun))?;
            let args = &expr.args;

            match scalar_function {
                ScalarFunction::Asin => Ok(asin(parse_expr(&args[0], registry)?)),
                ScalarFunction::Acos => Ok(acos(parse_expr(&args[0], registry)?)),
                ScalarFunction::Asinh => Ok(asinh(parse_expr(&args[0], registry)?)),
                ScalarFunction::Acosh => Ok(acosh(parse_expr(&args[0], registry)?)),
                ScalarFunction::Array => Ok(array(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Sqrt => Ok(sqrt(parse_expr(&args[0], registry)?)),
                ScalarFunction::Cbrt => Ok(cbrt(parse_expr(&args[0], registry)?)),
                ScalarFunction::Sin => Ok(sin(parse_expr(&args[0], registry)?)),
                ScalarFunction::Cos => Ok(cos(parse_expr(&args[0], registry)?)),
                ScalarFunction::Tan => Ok(tan(parse_expr(&args[0], registry)?)),
                ScalarFunction::Atan => Ok(atan(parse_expr(&args[0], registry)?)),
                ScalarFunction::Sinh => Ok(sinh(parse_expr(&args[0], registry)?)),
                ScalarFunction::Cosh => Ok(cosh(parse_expr(&args[0], registry)?)),
                ScalarFunction::Tanh => Ok(tanh(parse_expr(&args[0], registry)?)),
                ScalarFunction::Atanh => Ok(atanh(parse_expr(&args[0], registry)?)),
                ScalarFunction::Exp => Ok(exp(parse_expr(&args[0], registry)?)),
                ScalarFunction::Degrees => Ok(degrees(parse_expr(&args[0], registry)?)),
                ScalarFunction::Radians => Ok(radians(parse_expr(&args[0], registry)?)),
                ScalarFunction::Log2 => Ok(log2(parse_expr(&args[0], registry)?)),
                ScalarFunction::Ln => Ok(ln(parse_expr(&args[0], registry)?)),
                ScalarFunction::Log10 => Ok(log10(parse_expr(&args[0], registry)?)),
                ScalarFunction::Floor => Ok(floor(parse_expr(&args[0], registry)?)),
                ScalarFunction::Factorial => {
                    Ok(factorial(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::Ceil => Ok(ceil(parse_expr(&args[0], registry)?)),
                ScalarFunction::Round => Ok(round(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Trunc => Ok(trunc(parse_expr(&args[0], registry)?)),
                ScalarFunction::Abs => Ok(abs(parse_expr(&args[0], registry)?)),
                ScalarFunction::Signum => Ok(signum(parse_expr(&args[0], registry)?)),
                ScalarFunction::OctetLength => {
                    Ok(octet_length(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::Lower => Ok(lower(parse_expr(&args[0], registry)?)),
                ScalarFunction::Upper => Ok(upper(parse_expr(&args[0], registry)?)),
                ScalarFunction::Trim => Ok(trim(parse_expr(&args[0], registry)?)),
                ScalarFunction::Ltrim => Ok(ltrim(parse_expr(&args[0], registry)?)),
                ScalarFunction::Rtrim => Ok(rtrim(parse_expr(&args[0], registry)?)),
                ScalarFunction::DatePart => Ok(date_part(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::DateTrunc => Ok(date_trunc(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::DateBin => Ok(date_bin(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                    parse_expr(&args[2], registry)?,
                )),
                ScalarFunction::Sha224 => Ok(sha224(parse_expr(&args[0], registry)?)),
                ScalarFunction::Sha256 => Ok(sha256(parse_expr(&args[0], registry)?)),
                ScalarFunction::Sha384 => Ok(sha384(parse_expr(&args[0], registry)?)),
                ScalarFunction::Sha512 => Ok(sha512(parse_expr(&args[0], registry)?)),
                ScalarFunction::Md5 => Ok(md5(parse_expr(&args[0], registry)?)),
                ScalarFunction::NullIf => Ok(nullif(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Digest => Ok(digest(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Ascii => Ok(ascii(parse_expr(&args[0], registry)?)),
                ScalarFunction::BitLength => {
                    Ok(bit_length(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::CharacterLength => {
                    Ok(character_length(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::Chr => Ok(chr(parse_expr(&args[0], registry)?)),
                ScalarFunction::InitCap => Ok(ascii(parse_expr(&args[0], registry)?)),
                ScalarFunction::Gcd => Ok(gcd(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Lcm => Ok(lcm(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Left => Ok(left(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Random => Ok(random()),
                ScalarFunction::Uuid => Ok(uuid()),
                ScalarFunction::Repeat => Ok(repeat(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Replace => Ok(replace(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                    parse_expr(&args[2], registry)?,
                )),
                ScalarFunction::Reverse => Ok(reverse(parse_expr(&args[0], registry)?)),
                ScalarFunction::Right => Ok(right(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Concat => Ok(concat_expr(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::ConcatWithSeparator => Ok(concat_ws_expr(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Greatest => Ok(greatest(
                    &args
                        .to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Least => Ok(least(
                    &args
                        .to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Lpad => Ok(lpad(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Rpad => Ok(rpad(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::RegexpReplace => Ok(regexp_replace(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::RegexpMatch => Ok(regexp_match(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Btrim => Ok(btrim(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::SplitPart => Ok(split_part(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                    parse_expr(&args[2], registry)?,
                )),
                ScalarFunction::StartsWith => Ok(starts_with(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Strpos => Ok(strpos(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Substr => {
                    if args.len() > 2 {
                        assert_eq!(args.len(), 3);
                        Ok(substring(
                            parse_expr(&args[0], registry)?,
                            parse_expr(&args[1], registry)?,
                            parse_expr(&args[2], registry)?,
                        ))
                    } else {
                        Ok(substr(
                            parse_expr(&args[0], registry)?,
                            parse_expr(&args[1], registry)?,
                        ))
                    }
                }
                ScalarFunction::ToHex => Ok(to_hex(parse_expr(&args[0], registry)?)),
                ScalarFunction::ToTimestampMillis => {
                    Ok(to_timestamp_millis(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::ToTimestampMicros => {
                    Ok(to_timestamp_micros(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::ToTimestampSeconds => {
                    Ok(to_timestamp_seconds(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::Now => Ok(now()),
                ScalarFunction::Translate => Ok(translate(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                    parse_expr(&args[2], registry)?,
                )),
                ScalarFunction::Coalesce => Ok(coalesce(
                    args.to_owned()
                        .iter()
                        .map(|expr| parse_expr(expr, registry))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ScalarFunction::Pi => Ok(pi()),
                ScalarFunction::Power => Ok(power(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::Log => Ok(log(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                ScalarFunction::FromUnixtime => {
                    Ok(from_unixtime(parse_expr(&args[0], registry)?))
                }
                ScalarFunction::Atan2 => Ok(atan2(
                    parse_expr(&args[0], registry)?,
                    parse_expr(&args[1], registry)?,
                )),
                _ => Err(proto_error(
                    "Protobuf deserialization error: Unsupported scalar function",
                )),
            }
        }
        ExprType::ScalarUdfExpr(protobuf::ScalarUdfExprNode { fun_name, args }) => {
            let scalar_fn = registry.udf(fun_name.as_str())?;
            Ok(Expr::ScalarUDF(expr::ScalarUDF::new(
                scalar_fn,
                args.iter()
                    .map(|expr| parse_expr(expr, registry))
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
        }
        ExprType::AggregateUdfExpr(pb) => {
            let agg_fn = registry.udaf(pb.fun_name.as_str())?;

            Ok(Expr::AggregateUDF(expr::AggregateUDF::new(
                agg_fn,
                pb.args
                    .iter()
                    .map(|expr| parse_expr(expr, registry))
                    .collect::<Result<Vec<_>, Error>>()?,
                parse_optional_expr(pb.filter.as_deref(), registry)?.map(Box::new),
                parse_vec_expr(&pb.order_by, registry)?,
            )))
        }

        ExprType::GroupingSet(GroupingSetNode { expr }) => {
            Ok(Expr::GroupingSet(GroupingSets(
                expr.iter()
                    .map(|expr_list| {
                        expr_list
                            .expr
                            .iter()
                            .map(|expr| parse_expr(expr, registry))
                            .collect::<Result<Vec<_>, Error>>()
                    })
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
        }
        ExprType::Cube(CubeNode { expr }) => Ok(Expr::GroupingSet(GroupingSet::Cube(
            expr.iter()
                .map(|expr| parse_expr(expr, registry))
                .collect::<Result<Vec<_>, Error>>()?,
        ))),
        ExprType::Rollup(RollupNode { expr }) => {
            Ok(Expr::GroupingSet(GroupingSet::Rollup(
                expr.iter()
                    .map(|expr| parse_expr(expr, registry))
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
        }
        ExprType::Placeholder(PlaceholderNode { id, data_type }) => match data_type {
            None => Ok(Expr::Placeholder(Placeholder::new(id.clone(), None))),
            Some(data_type) => Ok(Expr::Placeholder(Placeholder::new(
                id.clone(),
                Some(data_type.try_into()?),
            ))),
        },
    }
}

/// Parse an optional escape_char for Like, ILike, SimilarTo
fn parse_escape_char(s: &str) -> Result<Option<char>> {
    match s.len() {
        0 => Ok(None),
        1 => Ok(s.chars().next()),
        _ => Err(DataFusionError::Internal(
            "Invalid length for escape char".to_string(),
        )),
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
        other => Err(proto_error(format!(
            "Unsupported binary operator '{other:?}'"
        ))),
    }
}

fn parse_vec_expr(
    p: &[protobuf::LogicalExprNode],
    registry: &dyn FunctionRegistry,
) -> Result<Option<Vec<Expr>>, Error> {
    let res = p
        .iter()
        .map(|elem| {
            parse_expr(elem, registry).map_err(|e| DataFusionError::Plan(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    // Convert empty vector to None.
    Ok((!res.is_empty()).then_some(res))
}

fn parse_optional_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
) -> Result<Option<Expr>, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry).map(Some),
        None => Ok(None),
    }
}

fn parse_required_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: impl Into<String>,
) -> Result<Expr, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry),
        None => Err(Error::required(field)),
    }
}

fn proto_error<S: Into<String>>(message: S) -> Error {
    Error::General(message.into())
}
