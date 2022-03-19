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

use crate::protobuf;
use datafusion::{
    arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode},
    error::DataFusionError,
    logical_plan::{
        abs, acos, ascii, asin, atan, ceil, character_length, chr, concat_expr,
        concat_ws_expr, cos, digest, exp, floor, left, ln, log10, log2, now_expr, nullif,
        random, regexp_replace, repeat, replace, reverse, right, round, signum, sin,
        split_part, sqrt, starts_with, strpos, substr, tan, to_hex, to_timestamp_micros,
        to_timestamp_millis, to_timestamp_seconds, translate, trunc,
        window_frames::{WindowFrame, WindowFrameBound, WindowFrameUnits},
        Column, DFField, DFSchema, DFSchemaRef, Expr, Operator,
    },
    physical_plan::{
        aggregates::AggregateFunction, functions::BuiltinScalarFunction,
        window_functions::BuiltInWindowFunction,
    },
    prelude::{
        array, btrim, date_part, date_trunc, lower, lpad, ltrim, md5, octet_length,
        regexp_match, rpad, rtrim, sha224, sha256, sha384, sha512, trim, upper,
    },
    scalar::ScalarValue,
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
            Self::General(desc) => write!(f, "General error: {}", desc),

            Self::DataFusionError(desc) => {
                write!(f, "DataFusion error: {:?}", desc)
            }

            Self::MissingRequiredField(name) => {
                write!(f, "Missing required field {}", name)
            }
            Self::AtLeastOneValue(name) => {
                write!(f, "Must have at least one {}, found 0", name)
            }
            Self::UnknownEnumVariant { name, value } => {
                write!(f, "Unknown i32 value for {} enum: {}", name, value)
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

    fn at_least_one(field: impl Into<String>) -> Error {
        Error::AtLeastOneValue(field.into())
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
    /// Returns None if the option is None, otherwise calls [`FromField::field`]
    /// on the contained data, returning any error encountered
    fn optional(self) -> Result<Option<T>, Error>;

    /// Converts an optional protobuf field to a different type, returning an error if None
    ///
    /// Returns `Error::MissingRequiredField` if None, otherwise calls [`FromField::field`]
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

        Self {
            relation: relation.map(|r| r.relation),
            name,
        }
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
        let field = df_field.field.as_ref().required("field")?;

        Ok(match &df_field.qualifier {
            Some(q) => DFField::from_qualified(&q.relation, field),
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

impl From<protobuf::PrimitiveScalarType> for DataType {
    fn from(scalar: protobuf::PrimitiveScalarType) -> Self {
        match scalar {
            protobuf::PrimitiveScalarType::Bool => DataType::Boolean,
            protobuf::PrimitiveScalarType::Uint8 => DataType::UInt8,
            protobuf::PrimitiveScalarType::Int8 => DataType::Int8,
            protobuf::PrimitiveScalarType::Uint16 => DataType::UInt16,
            protobuf::PrimitiveScalarType::Int16 => DataType::Int16,
            protobuf::PrimitiveScalarType::Uint32 => DataType::UInt32,
            protobuf::PrimitiveScalarType::Int32 => DataType::Int32,
            protobuf::PrimitiveScalarType::Uint64 => DataType::UInt64,
            protobuf::PrimitiveScalarType::Int64 => DataType::Int64,
            protobuf::PrimitiveScalarType::Float32 => DataType::Float32,
            protobuf::PrimitiveScalarType::Float64 => DataType::Float64,
            protobuf::PrimitiveScalarType::Utf8 => DataType::Utf8,
            protobuf::PrimitiveScalarType::LargeUtf8 => DataType::LargeUtf8,
            protobuf::PrimitiveScalarType::Date32 => DataType::Date32,
            protobuf::PrimitiveScalarType::TimeMicrosecond => {
                DataType::Time64(TimeUnit::Microsecond)
            }
            protobuf::PrimitiveScalarType::TimeNanosecond => {
                DataType::Time64(TimeUnit::Nanosecond)
            }
            protobuf::PrimitiveScalarType::Null => DataType::Null,
            protobuf::PrimitiveScalarType::Decimal128 => DataType::Decimal(0, 0),
            protobuf::PrimitiveScalarType::Date64 => DataType::Date64,
            protobuf::PrimitiveScalarType::TimeSecond => {
                DataType::Timestamp(TimeUnit::Second, None)
            }
            protobuf::PrimitiveScalarType::TimeMillisecond => {
                DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            protobuf::PrimitiveScalarType::IntervalYearmonth => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            protobuf::PrimitiveScalarType::IntervalDaytime => {
                DataType::Interval(IntervalUnit::DayTime)
            }
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
                DataType::Duration(protobuf::TimeUnit::try_from(time_unit)?.into())
            }
            arrow_type::ArrowTypeEnum::Timestamp(protobuf::Timestamp {
                time_unit,
                timezone,
            }) => DataType::Timestamp(
                protobuf::TimeUnit::try_from(time_unit)?.into(),
                match timezone.len() {
                    0 => None,
                    _ => Some(timezone.to_owned()),
                },
            ),
            arrow_type::ArrowTypeEnum::Time32(time_unit) => {
                DataType::Time32(protobuf::TimeUnit::try_from(time_unit)?.into())
            }
            arrow_type::ArrowTypeEnum::Time64(time_unit) => {
                DataType::Time64(protobuf::TimeUnit::try_from(time_unit)?.into())
            }
            arrow_type::ArrowTypeEnum::Interval(interval_unit) => DataType::Interval(
                protobuf::IntervalUnit::try_from(interval_unit)?.into(),
            ),
            arrow_type::ArrowTypeEnum::Decimal(protobuf::Decimal {
                whole,
                fractional,
            }) => DataType::Decimal(*whole as usize, *fractional as usize),
            arrow_type::ArrowTypeEnum::List(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::List(Box::new(list_type))
            }
            arrow_type::ArrowTypeEnum::LargeList(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                DataType::LargeList(Box::new(list_type))
            }
            arrow_type::ArrowTypeEnum::FixedSizeList(list) => {
                let list_type =
                    list.as_ref().field_type.as_deref().required("field_type")?;
                let list_size = list.list_size;
                DataType::FixedSizeList(Box::new(list_type), list_size)
            }
            arrow_type::ArrowTypeEnum::Struct(strct) => DataType::Struct(
                strct
                    .sub_field_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            arrow_type::ArrowTypeEnum::Union(union) => {
                let union_mode = protobuf::UnionMode::from_i32(union.union_mode)
                    .ok_or_else(|| Error::unknown("UnionMode", union.union_mode))?;
                let union_mode = match union_mode {
                    protobuf::UnionMode::Dense => UnionMode::Dense,
                    protobuf::UnionMode::Sparse => UnionMode::Sparse,
                };
                let union_types = union
                    .union_types
                    .iter()
                    .map(|field| field.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                DataType::Union(union_types, union_mode)
            }
            arrow_type::ArrowTypeEnum::Dictionary(dict) => {
                let key_datatype = dict.as_ref().key.as_deref().required("key")?;
                let value_datatype = dict.as_ref().value.as_deref().required("value")?;
                DataType::Dictionary(Box::new(key_datatype), Box::new(value_datatype))
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

impl From<&protobuf::ScalarFunction> for BuiltinScalarFunction {
    fn from(f: &protobuf::ScalarFunction) -> Self {
        use protobuf::ScalarFunction;
        match f {
            ScalarFunction::Sqrt => Self::Sqrt,
            ScalarFunction::Sin => Self::Sin,
            ScalarFunction::Cos => Self::Cos,
            ScalarFunction::Tan => Self::Tan,
            ScalarFunction::Asin => Self::Asin,
            ScalarFunction::Acos => Self::Acos,
            ScalarFunction::Atan => Self::Atan,
            ScalarFunction::Exp => Self::Exp,
            ScalarFunction::Log => Self::Log,
            ScalarFunction::Ln => Self::Ln,
            ScalarFunction::Log10 => Self::Log10,
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
            ScalarFunction::Array => Self::Array,
            ScalarFunction::NullIf => Self::NullIf,
            ScalarFunction::DatePart => Self::DatePart,
            ScalarFunction::DateTrunc => Self::DateTrunc,
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
            ScalarFunction::Translate => Self::Translate,
            ScalarFunction::RegexpMatch => Self::RegexpMatch,
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
            protobuf::AggregateFunction::ApproxMedian => Self::ApproxMedian,
            protobuf::AggregateFunction::BitmapDistinct => Self::BitMapCountDistinct,
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

impl TryFrom<&i32> for protobuf::PrimitiveScalarType {
    type Error = Error;

    fn try_from(value: &i32) -> Result<Self, Self::Error> {
        protobuf::PrimitiveScalarType::from_i32(*value)
            .ok_or_else(|| Error::unknown("PrimitiveScalarType", *value))
    }
}

impl TryFrom<&i32> for protobuf::AggregateFunction {
    type Error = Error;

    fn try_from(value: &i32) -> Result<Self, Self::Error> {
        protobuf::AggregateFunction::from_i32(*value)
            .ok_or_else(|| Error::unknown("AggregateFunction", *value))
    }
}

impl TryFrom<&protobuf::scalar_type::Datatype> for DataType {
    type Error = Error;

    fn try_from(
        scalar_type: &protobuf::scalar_type::Datatype,
    ) -> Result<Self, Self::Error> {
        use protobuf::scalar_type::Datatype;

        Ok(match scalar_type {
            Datatype::Scalar(scalar_type) => {
                protobuf::PrimitiveScalarType::try_from(scalar_type)?.into()
            }
            Datatype::List(protobuf::ScalarListType {
                deepest_type,
                field_names,
            }) => {
                if field_names.is_empty() {
                    return Err(Error::at_least_one("field_names"));
                }
                let field_type =
                    protobuf::PrimitiveScalarType::try_from(deepest_type)?.into();
                //Because length is checked above it is safe to unwrap .last()
                let mut scalar_type = DataType::List(Box::new(Field::new(
                    field_names.last().unwrap().as_str(),
                    field_type,
                    true,
                )));
                //Iterate over field names in reverse order except for the last item in the vector
                for name in field_names.iter().rev().skip(1) {
                    let new_datatype = DataType::List(Box::new(Field::new(
                        name.as_str(),
                        scalar_type,
                        true,
                    )));
                    scalar_type = new_datatype;
                }
                scalar_type
            }
        })
    }
}

impl TryFrom<&protobuf::scalar_value::Value> for ScalarValue {
    type Error = Error;

    fn try_from(scalar: &protobuf::scalar_value::Value) -> Result<Self, Self::Error> {
        use protobuf::{scalar_value::Value, PrimitiveScalarType};

        let scalar = match scalar {
            Value::BoolValue(v) => ScalarValue::Boolean(Some(*v)),
            Value::Utf8Value(v) => ScalarValue::Utf8(Some(v.to_owned())),
            Value::LargeUtf8Value(v) => ScalarValue::LargeUtf8(Some(v.to_owned())),
            Value::Int8Value(v) => ScalarValue::Int8(Some(*v as i8)),
            Value::Int16Value(v) => ScalarValue::Int16(Some(*v as i16)),
            Value::Int32Value(v) => ScalarValue::Int32(Some(*v)),
            Value::Int64Value(v) => ScalarValue::Int64(Some(*v)),
            Value::Uint8Value(v) => ScalarValue::UInt8(Some(*v as u8)),
            Value::Uint16Value(v) => ScalarValue::UInt16(Some(*v as u16)),
            Value::Uint32Value(v) => ScalarValue::UInt32(Some(*v)),
            Value::Uint64Value(v) => ScalarValue::UInt64(Some(*v)),
            Value::Float32Value(v) => ScalarValue::Float32(Some(*v)),
            Value::Float64Value(v) => ScalarValue::Float64(Some(*v)),
            Value::Date32Value(v) => ScalarValue::Date32(Some(*v)),
            Value::TimeMicrosecondValue(v) => {
                ScalarValue::TimestampMicrosecond(Some(*v), None)
            }
            Value::TimeNanosecondValue(v) => {
                ScalarValue::TimestampNanosecond(Some(*v), None)
            }
            Value::ListValue(v) => v.try_into()?,
            Value::NullListValue(v) => ScalarValue::List(None, Box::new(v.try_into()?)),
            Value::NullValue(null_enum) => {
                let primitive = PrimitiveScalarType::try_from(null_enum)?;
                (&primitive).try_into()?
            }
            Value::Decimal128Value(val) => {
                let array = vec_to_array(val.value.clone());
                ScalarValue::Decimal128(
                    Some(i128::from_be_bytes(array)),
                    val.p as usize,
                    val.s as usize,
                )
            }
            Value::Date64Value(v) => ScalarValue::Date64(Some(*v)),
            Value::TimeSecondValue(v) => ScalarValue::TimestampSecond(Some(*v), None),
            Value::TimeMillisecondValue(v) => {
                ScalarValue::TimestampMillisecond(Some(*v), None)
            }
            Value::IntervalYearmonthValue(v) => ScalarValue::IntervalYearMonth(Some(*v)),
            Value::IntervalDaytimeValue(v) => ScalarValue::IntervalDayTime(Some(*v)),
        };
        Ok(scalar)
    }
}

impl TryFrom<&protobuf::ScalarListValue> for ScalarValue {
    type Error = Error;

    fn try_from(
        scalar_list_value: &protobuf::ScalarListValue,
    ) -> Result<Self, Self::Error> {
        use protobuf::{scalar_type::Datatype, PrimitiveScalarType};

        let protobuf::ScalarListValue { datatype, values } = scalar_list_value;
        let pb_scalar_type = datatype
            .as_ref()
            .ok_or_else(|| Error::required("datatype"))?;

        let scalar_type = pb_scalar_type
            .datatype
            .as_ref()
            .ok_or_else(|| Error::required("datatype"))?;
        let scalar_values = match scalar_type {
            Datatype::Scalar(scalar_type_i32) => {
                let leaf_scalar_type =
                    protobuf::PrimitiveScalarType::try_from(scalar_type_i32)?;
                let typechecked_values: Vec<datafusion::scalar::ScalarValue> = values
                    .iter()
                    .map(|protobuf::ScalarValue { value: opt_value }| {
                        let value =
                            opt_value.as_ref().ok_or_else(|| Error::required("value"))?;
                        typechecked_scalar_value_conversion(value, leaf_scalar_type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                ScalarValue::List(
                    Some(Box::new(typechecked_values)),
                    Box::new(leaf_scalar_type.into()),
                )
            }
            Datatype::List(list_type) => {
                let protobuf::ScalarListType {
                    deepest_type,
                    field_names,
                } = &list_type;
                let leaf_type = PrimitiveScalarType::try_from(deepest_type)?;
                let depth = field_names.len();

                let typechecked_values: Vec<ScalarValue> = if depth == 0 {
                    return Err(Error::at_least_one("field_names"));
                } else if depth == 1 {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            let value = opt_value
                                .as_ref()
                                .ok_or_else(|| Error::required("value"))?;
                            typechecked_scalar_value_conversion(value, leaf_type)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    values
                        .iter()
                        .map(|protobuf::ScalarValue { value: opt_value }| {
                            opt_value.as_ref().required("value")
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
                ScalarValue::List(
                    match typechecked_values.len() {
                        0 => None,
                        _ => Some(Box::new(typechecked_values)),
                    },
                    Box::new((list_type).try_into()?),
                )
            }
        };
        Ok(scalar_values)
    }
}

impl TryFrom<&protobuf::ScalarType> for DataType {
    type Error = Error;

    fn try_from(scalar: &protobuf::ScalarType) -> Result<Self, Self::Error> {
        scalar.datatype.as_ref().required("datatype")
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

impl TryFrom<&protobuf::PrimitiveScalarType> for ScalarValue {
    type Error = Error;

    fn try_from(scalar: &protobuf::PrimitiveScalarType) -> Result<Self, Self::Error> {
        use protobuf::PrimitiveScalarType;

        Ok(match scalar {
            PrimitiveScalarType::Null => {
                return Err(proto_error("Untyped null is an invalid scalar value"))
            }
            PrimitiveScalarType::Bool => Self::Boolean(None),
            PrimitiveScalarType::Uint8 => Self::UInt8(None),
            PrimitiveScalarType::Int8 => Self::Int8(None),
            PrimitiveScalarType::Uint16 => Self::UInt16(None),
            PrimitiveScalarType::Int16 => Self::Int16(None),
            PrimitiveScalarType::Uint32 => Self::UInt32(None),
            PrimitiveScalarType::Int32 => Self::Int32(None),
            PrimitiveScalarType::Uint64 => Self::UInt64(None),
            PrimitiveScalarType::Int64 => Self::Int64(None),
            PrimitiveScalarType::Float32 => Self::Float32(None),
            PrimitiveScalarType::Float64 => Self::Float64(None),
            PrimitiveScalarType::Utf8 => Self::Utf8(None),
            PrimitiveScalarType::LargeUtf8 => Self::LargeUtf8(None),
            PrimitiveScalarType::Date32 => Self::Date32(None),
            PrimitiveScalarType::TimeMicrosecond => {
                Self::TimestampMicrosecond(None, None)
            }
            PrimitiveScalarType::TimeNanosecond => Self::TimestampNanosecond(None, None),
            PrimitiveScalarType::Decimal128 => Self::Decimal128(None, 0, 0),
            PrimitiveScalarType::Date64 => Self::Date64(None),
            PrimitiveScalarType::TimeSecond => Self::TimestampSecond(None, None),
            PrimitiveScalarType::TimeMillisecond => {
                Self::TimestampMillisecond(None, None)
            }
            PrimitiveScalarType::IntervalYearmonth => Self::IntervalYearMonth(None),
            PrimitiveScalarType::IntervalDaytime => Self::IntervalDayTime(None),
        })
    }
}

impl TryFrom<&protobuf::ScalarListType> for DataType {
    type Error = Error;
    fn try_from(scalar: &protobuf::ScalarListType) -> Result<Self, Self::Error> {
        use protobuf::PrimitiveScalarType;

        let protobuf::ScalarListType {
            deepest_type,
            field_names,
        } = scalar;

        let depth = field_names.len();
        if depth == 0 {
            return Err(Error::at_least_one("field_names"));
        }

        let mut curr_type = Self::List(Box::new(Field::new(
            //Since checked vector is not empty above this is safe to unwrap
            field_names.last().unwrap(),
            PrimitiveScalarType::try_from(deepest_type)?.into(),
            true,
        )));
        //Iterates over field names in reverse order except for the last item in the vector
        for name in field_names.iter().rev().skip(1) {
            let temp_curr_type = Self::List(Box::new(Field::new(name, curr_type, true)));
            curr_type = temp_curr_type;
        }
        Ok(curr_type)
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
            Value::TimeMicrosecondValue(v) => Self::TimestampMicrosecond(Some(*v), None),
            Value::TimeNanosecondValue(v) => Self::TimestampNanosecond(Some(*v), None),
            Value::ListValue(scalar_list) => {
                let protobuf::ScalarListValue {
                    values,
                    datatype: opt_scalar_type,
                } = &scalar_list;

                let scalar_type = opt_scalar_type.as_ref().required("datatype")?;
                let scalar_type = Box::new(scalar_type);

                let typechecked_values: Vec<ScalarValue> = values
                    .iter()
                    .map(|val| val.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                Self::List(Some(Box::new(typechecked_values)), scalar_type)
            }
            Value::NullListValue(v) => {
                let datatype = v.datatype.as_ref().required("datatype")?;
                Self::List(None, Box::new(datatype))
            }
            Value::NullValue(v) => {
                let null_type_enum = protobuf::PrimitiveScalarType::try_from(v)?;
                (&null_type_enum).try_into()?
            }
            Value::Decimal128Value(val) => {
                let array = vec_to_array(val.value.clone());
                Self::Decimal128(
                    Some(i128::from_be_bytes(array)),
                    val.p as usize,
                    val.s as usize,
                )
            }
            Value::Date64Value(v) => Self::Date64(Some(*v)),
            Value::TimeSecondValue(v) => Self::TimestampSecond(Some(*v), None),
            Value::TimeMillisecondValue(v) => Self::TimestampMillisecond(Some(*v), None),
            Value::IntervalYearmonthValue(v) => Self::IntervalYearMonth(Some(*v)),
            Value::IntervalDaytimeValue(v) => Self::IntervalDayTime(Some(*v)),
        })
    }
}

impl TryFrom<&protobuf::LogicalExprNode> for Expr {
    type Error = Error;

    fn try_from(expr: &protobuf::LogicalExprNode) -> Result<Self, Self::Error> {
        use datafusion::physical_plan::window_functions;
        use protobuf::{logical_expr_node::ExprType, window_expr_node, ScalarFunction};

        let expr_type = expr
            .expr_type
            .as_ref()
            .ok_or_else(|| Error::required("expr_type"))?;

        match expr_type {
            ExprType::BinaryExpr(binary_expr) => Ok(Self::BinaryExpr {
                left: Box::new(binary_expr.l.as_deref().required("l")?),
                op: from_proto_binary_op(&binary_expr.op)?,
                right: Box::new(binary_expr.r.as_deref().required("r")?),
            }),
            ExprType::Column(column) => Ok(Self::Column(column.into())),
            ExprType::Literal(literal) => {
                let scalar_value: ScalarValue = literal.try_into()?;
                Ok(Self::Literal(scalar_value))
            }
            ExprType::WindowExpr(expr) => {
                let window_function = expr
                    .window_function
                    .as_ref()
                    .ok_or_else(|| Error::required("window_function"))?;
                let partition_by = expr
                    .partition_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let order_by = expr
                    .order_by
                    .iter()
                    .map(|e| e.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                let window_frame = expr
                    .window_frame
                    .as_ref()
                    .map::<Result<WindowFrame, _>, _>(|e| match e {
                        window_expr_node::WindowFrame::Frame(frame) => {
                            let window_frame: WindowFrame = frame.clone().try_into()?;
                            if WindowFrameUnits::Range == window_frame.units
                                && order_by.len() != 1
                            {
                                Err(proto_error("With window frame of type RANGE, the order by expression must be of length 1"))
                            } else {
                                Ok(window_frame)
                            }
                        }
                    })
                    .transpose()?;

                match window_function {
                    window_expr_node::WindowFunction::AggrFunction(i) => {
                        let aggr_function =
                            protobuf::AggregateFunction::try_from(i)?.into();

                        Ok(Self::WindowFunction {
                            fun: window_functions::WindowFunction::AggregateFunction(
                                aggr_function,
                            ),
                            args: vec![expr.expr.as_deref().required("expr")?],
                            partition_by,
                            order_by,
                            window_frame,
                        })
                    }
                    window_expr_node::WindowFunction::BuiltInFunction(i) => {
                        let built_in_function =
                            protobuf::BuiltInWindowFunction::from_i32(*i)
                                .ok_or_else(|| {
                                    Error::unknown("BuiltInWindowFunction", *i)
                                })?
                                .into();

                        Ok(Self::WindowFunction {
                            fun: window_functions::WindowFunction::BuiltInWindowFunction(
                                built_in_function,
                            ),
                            args: vec![expr.expr.as_deref().required("expr")?],
                            partition_by,
                            order_by,
                            window_frame,
                        })
                    }
                }
            }
            ExprType::AggregateExpr(expr) => {
                let fun =
                    protobuf::AggregateFunction::try_from(&expr.aggr_function)?.into();

                Ok(Self::AggregateFunction {
                    fun,
                    args: expr
                        .expr
                        .iter()
                        .map(|e| e.try_into())
                        .collect::<Result<Vec<_>, _>>()?,
                    distinct: false, //TODO
                })
            }
            ExprType::Alias(alias) => Ok(Self::Alias(
                Box::new(alias.expr.as_deref().required("expr")?),
                alias.alias.clone(),
            )),
            ExprType::IsNullExpr(is_null) => Ok(Self::IsNull(Box::new(
                is_null.expr.as_deref().required("expr")?,
            ))),
            ExprType::IsNotNullExpr(is_not_null) => Ok(Self::IsNotNull(Box::new(
                is_not_null.expr.as_deref().required("expr")?,
            ))),
            ExprType::NotExpr(not) => {
                Ok(Self::Not(Box::new(not.expr.as_deref().required("expr")?)))
            }
            ExprType::Between(between) => Ok(Self::Between {
                expr: Box::new(between.expr.as_deref().required("expr")?),
                negated: between.negated,
                low: Box::new(between.low.as_deref().required("low")?),
                high: Box::new(between.high.as_deref().required("high")?),
            }),
            ExprType::Case(case) => {
                let when_then_expr = case
                    .when_then_expr
                    .iter()
                    .map(|e| {
                        let when_expr = e.when_expr.as_ref().required("when_expr")?;
                        let then_expr = e.then_expr.as_ref().required("then_expr")?;
                        Ok((Box::new(when_expr), Box::new(then_expr)))
                    })
                    .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, Error>>()?;
                Ok(Self::Case {
                    expr: parse_optional_expr(&case.expr)?.map(Box::new),
                    when_then_expr,
                    else_expr: parse_optional_expr(&case.else_expr)?.map(Box::new),
                })
            }
            ExprType::Cast(cast) => {
                let expr = Box::new(cast.expr.as_deref().required("expr")?);
                let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
                Ok(Self::Cast { expr, data_type })
            }
            ExprType::TryCast(cast) => {
                let expr = Box::new(cast.expr.as_deref().required("expr")?);
                let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
                Ok(Self::TryCast { expr, data_type })
            }
            ExprType::Sort(sort) => Ok(Self::Sort {
                expr: Box::new(sort.expr.as_deref().required("expr")?),
                asc: sort.asc,
                nulls_first: sort.nulls_first,
            }),
            ExprType::Negative(negative) => Ok(Self::Negative(Box::new(
                negative.expr.as_deref().required("expr")?,
            ))),
            ExprType::InList(in_list) => Ok(Self::InList {
                expr: Box::new(in_list.expr.as_deref().required("expr")?),
                list: in_list
                    .list
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                negated: in_list.negated,
            }),
            ExprType::Wildcard(_) => Ok(Self::Wildcard),
            ExprType::ScalarFunction(expr) => {
                let scalar_function = protobuf::ScalarFunction::from_i32(expr.fun)
                    .ok_or_else(|| Error::unknown("ScalarFunction", expr.fun))?;
                let args = &expr.args;

                match scalar_function {
                    ScalarFunction::Asin => Ok(asin((&args[0]).try_into()?)),
                    ScalarFunction::Acos => Ok(acos((&args[0]).try_into()?)),
                    ScalarFunction::Array => Ok(array(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Sqrt => Ok(sqrt((&args[0]).try_into()?)),
                    ScalarFunction::Sin => Ok(sin((&args[0]).try_into()?)),
                    ScalarFunction::Cos => Ok(cos((&args[0]).try_into()?)),
                    ScalarFunction::Tan => Ok(tan((&args[0]).try_into()?)),
                    ScalarFunction::Atan => Ok(atan((&args[0]).try_into()?)),
                    ScalarFunction::Exp => Ok(exp((&args[0]).try_into()?)),
                    ScalarFunction::Log2 => Ok(log2((&args[0]).try_into()?)),
                    ScalarFunction::Ln => Ok(ln((&args[0]).try_into()?)),
                    ScalarFunction::Log10 => Ok(log10((&args[0]).try_into()?)),
                    ScalarFunction::Floor => Ok(floor((&args[0]).try_into()?)),
                    ScalarFunction::Ceil => Ok(ceil((&args[0]).try_into()?)),
                    ScalarFunction::Round => Ok(round((&args[0]).try_into()?)),
                    ScalarFunction::Trunc => Ok(trunc((&args[0]).try_into()?)),
                    ScalarFunction::Abs => Ok(abs((&args[0]).try_into()?)),
                    ScalarFunction::Signum => Ok(signum((&args[0]).try_into()?)),
                    ScalarFunction::OctetLength => {
                        Ok(octet_length((&args[0]).try_into()?))
                    }
                    ScalarFunction::Lower => Ok(lower((&args[0]).try_into()?)),
                    ScalarFunction::Upper => Ok(upper((&args[0]).try_into()?)),
                    ScalarFunction::Trim => Ok(trim((&args[0]).try_into()?)),
                    ScalarFunction::Ltrim => Ok(ltrim((&args[0]).try_into()?)),
                    ScalarFunction::Rtrim => Ok(rtrim((&args[0]).try_into()?)),
                    ScalarFunction::DatePart => {
                        Ok(date_part((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::DateTrunc => {
                        Ok(date_trunc((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Sha224 => Ok(sha224((&args[0]).try_into()?)),
                    ScalarFunction::Sha256 => Ok(sha256((&args[0]).try_into()?)),
                    ScalarFunction::Sha384 => Ok(sha384((&args[0]).try_into()?)),
                    ScalarFunction::Sha512 => Ok(sha512((&args[0]).try_into()?)),
                    ScalarFunction::Md5 => Ok(md5((&args[0]).try_into()?)),
                    ScalarFunction::NullIf => Ok(nullif((&args[0]).try_into()?)),
                    ScalarFunction::Digest => {
                        Ok(digest((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Ascii => Ok(ascii((&args[0]).try_into()?)),
                    ScalarFunction::BitLength => Ok((&args[0]).try_into()?),
                    ScalarFunction::CharacterLength => {
                        Ok(character_length((&args[0]).try_into()?))
                    }
                    ScalarFunction::Chr => Ok(chr((&args[0]).try_into()?)),
                    ScalarFunction::InitCap => Ok(ascii((&args[0]).try_into()?)),
                    ScalarFunction::Left => {
                        Ok(left((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Random => Ok(random()),
                    ScalarFunction::Repeat => {
                        Ok(repeat((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Replace => Ok(replace(
                        (&args[0]).try_into()?,
                        (&args[1]).try_into()?,
                        (&args[2]).try_into()?,
                    )),
                    ScalarFunction::Reverse => Ok(reverse((&args[0]).try_into()?)),
                    ScalarFunction::Right => {
                        Ok(right((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Concat => Ok(concat_expr(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::ConcatWithSeparator => Ok(concat_ws_expr(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Lpad => Ok(lpad(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Rpad => Ok(rpad(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::RegexpReplace => Ok(regexp_replace(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::RegexpMatch => Ok(regexp_match(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Btrim => Ok(btrim(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::SplitPart => Ok(split_part(
                        (&args[0]).try_into()?,
                        (&args[1]).try_into()?,
                        (&args[2]).try_into()?,
                    )),
                    ScalarFunction::StartsWith => {
                        Ok(starts_with((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Strpos => {
                        Ok(strpos((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::Substr => {
                        Ok(substr((&args[0]).try_into()?, (&args[1]).try_into()?))
                    }
                    ScalarFunction::ToHex => Ok(to_hex((&args[0]).try_into()?)),
                    ScalarFunction::ToTimestampMillis => {
                        Ok(to_timestamp_millis((&args[0]).try_into()?))
                    }
                    ScalarFunction::ToTimestampMicros => {
                        Ok(to_timestamp_micros((&args[0]).try_into()?))
                    }
                    ScalarFunction::ToTimestampSeconds => {
                        Ok(to_timestamp_seconds((&args[0]).try_into()?))
                    }
                    ScalarFunction::Now => Ok(now_expr(
                        args.to_owned()
                            .iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
                    ScalarFunction::Translate => Ok(translate(
                        (&args[0]).try_into()?,
                        (&args[1]).try_into()?,
                        (&args[2]).try_into()?,
                    )),
                    _ => Err(proto_error(
                        "Protobuf deserialization error: Unsupported scalar function",
                    )),
                }
            }
        }
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
            protobuf::WindowFrameBoundType::Preceding => {
                // FIXME implement bound value parsing
                // https://github.com/apache/arrow-datafusion/issues/361
                Ok(Self::Preceding(Some(1)))
            }
            protobuf::WindowFrameBoundType::Following => {
                // FIXME implement bound value parsing
                // https://github.com/apache/arrow-datafusion/issues/361
                Ok(Self::Following(Some(1)))
            }
        }
    }
}

impl TryFrom<&Box<protobuf::List>> for DataType {
    type Error = Error;
    fn try_from(list: &Box<protobuf::List>) -> Result<Self, Self::Error> {
        Ok(Self::List(Box::new(
            list.as_ref().field_type.as_deref().required("field_type")?,
        )))
    }
}

impl TryFrom<&i32> for protobuf::TimeUnit {
    type Error = Error;

    fn try_from(value: &i32) -> Result<Self, Self::Error> {
        protobuf::TimeUnit::from_i32(*value)
            .ok_or_else(|| Error::unknown("TimeUnit", *value))
    }
}

impl From<protobuf::TimeUnit> for TimeUnit {
    fn from(time_unit: protobuf::TimeUnit) -> Self {
        match time_unit {
            protobuf::TimeUnit::Second => TimeUnit::Second,
            protobuf::TimeUnit::TimeMillisecond => TimeUnit::Millisecond,
            protobuf::TimeUnit::Microsecond => TimeUnit::Microsecond,
            protobuf::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl TryFrom<&i32> for protobuf::IntervalUnit {
    type Error = Error;

    fn try_from(value: &i32) -> Result<Self, Self::Error> {
        protobuf::IntervalUnit::from_i32(*value)
            .ok_or_else(|| Error::unknown("IntervalUnit", *value))
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

fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into().unwrap_or_else(|v: Vec<T>| {
        panic!("Expected a Vec of length {} but it was {}", N, v.len())
    })
}

//Does not typecheck lists
fn typechecked_scalar_value_conversion(
    tested_type: &protobuf::scalar_value::Value,
    required_type: protobuf::PrimitiveScalarType,
) -> Result<ScalarValue, Error> {
    use protobuf::{scalar_value::Value, PrimitiveScalarType};

    Ok(match (tested_type, &required_type) {
        (Value::BoolValue(v), PrimitiveScalarType::Bool) => {
            ScalarValue::Boolean(Some(*v))
        }
        (Value::Int8Value(v), PrimitiveScalarType::Int8) => {
            ScalarValue::Int8(Some(*v as i8))
        }
        (Value::Int16Value(v), PrimitiveScalarType::Int16) => {
            ScalarValue::Int16(Some(*v as i16))
        }
        (Value::Int32Value(v), PrimitiveScalarType::Int32) => {
            ScalarValue::Int32(Some(*v))
        }
        (Value::Int64Value(v), PrimitiveScalarType::Int64) => {
            ScalarValue::Int64(Some(*v))
        }
        (Value::Uint8Value(v), PrimitiveScalarType::Uint8) => {
            ScalarValue::UInt8(Some(*v as u8))
        }
        (Value::Uint16Value(v), PrimitiveScalarType::Uint16) => {
            ScalarValue::UInt16(Some(*v as u16))
        }
        (Value::Uint32Value(v), PrimitiveScalarType::Uint32) => {
            ScalarValue::UInt32(Some(*v))
        }
        (Value::Uint64Value(v), PrimitiveScalarType::Uint64) => {
            ScalarValue::UInt64(Some(*v))
        }
        (Value::Float32Value(v), PrimitiveScalarType::Float32) => {
            ScalarValue::Float32(Some(*v))
        }
        (Value::Float64Value(v), PrimitiveScalarType::Float64) => {
            ScalarValue::Float64(Some(*v))
        }
        (Value::Date32Value(v), PrimitiveScalarType::Date32) => {
            ScalarValue::Date32(Some(*v))
        }
        (Value::TimeMicrosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampMicrosecond(Some(*v), None)
        }
        (Value::TimeNanosecondValue(v), PrimitiveScalarType::TimeMicrosecond) => {
            ScalarValue::TimestampNanosecond(Some(*v), None)
        }
        (Value::Utf8Value(v), PrimitiveScalarType::Utf8) => {
            ScalarValue::Utf8(Some(v.to_owned()))
        }
        (Value::LargeUtf8Value(v), PrimitiveScalarType::LargeUtf8) => {
            ScalarValue::LargeUtf8(Some(v.to_owned()))
        }

        (Value::NullValue(i32_enum), required_scalar_type) => {
            if *i32_enum == *required_scalar_type as i32 {
                let pb_scalar_type = PrimitiveScalarType::try_from(i32_enum)?;
                let scalar_value: ScalarValue = match pb_scalar_type {
                    PrimitiveScalarType::Bool => ScalarValue::Boolean(None),
                    PrimitiveScalarType::Uint8 => ScalarValue::UInt8(None),
                    PrimitiveScalarType::Int8 => ScalarValue::Int8(None),
                    PrimitiveScalarType::Uint16 => ScalarValue::UInt16(None),
                    PrimitiveScalarType::Int16 => ScalarValue::Int16(None),
                    PrimitiveScalarType::Uint32 => ScalarValue::UInt32(None),
                    PrimitiveScalarType::Int32 => ScalarValue::Int32(None),
                    PrimitiveScalarType::Uint64 => ScalarValue::UInt64(None),
                    PrimitiveScalarType::Int64 => ScalarValue::Int64(None),
                    PrimitiveScalarType::Float32 => ScalarValue::Float32(None),
                    PrimitiveScalarType::Float64 => ScalarValue::Float64(None),
                    PrimitiveScalarType::Utf8 => ScalarValue::Utf8(None),
                    PrimitiveScalarType::LargeUtf8 => ScalarValue::LargeUtf8(None),
                    PrimitiveScalarType::Date32 => ScalarValue::Date32(None),
                    PrimitiveScalarType::TimeMicrosecond => {
                        ScalarValue::TimestampMicrosecond(None, None)
                    }
                    PrimitiveScalarType::TimeNanosecond => {
                        ScalarValue::TimestampNanosecond(None, None)
                    }
                    PrimitiveScalarType::Null => {
                        return Err(proto_error(
                            "Untyped scalar null is not a valid scalar value",
                        ))
                    }
                    PrimitiveScalarType::Decimal128 => {
                        ScalarValue::Decimal128(None, 0, 0)
                    }
                    PrimitiveScalarType::Date64 => ScalarValue::Date64(None),
                    PrimitiveScalarType::TimeSecond => {
                        ScalarValue::TimestampSecond(None, None)
                    }
                    PrimitiveScalarType::TimeMillisecond => {
                        ScalarValue::TimestampMillisecond(None, None)
                    }
                    PrimitiveScalarType::IntervalYearmonth => {
                        ScalarValue::IntervalYearMonth(None)
                    }
                    PrimitiveScalarType::IntervalDaytime => {
                        ScalarValue::IntervalDayTime(None)
                    }
                };
                scalar_value
            } else {
                return Err(proto_error("Could not convert to the proper type"));
            }
        }
        (Value::Decimal128Value(val), PrimitiveScalarType::Decimal128) => {
            let array = vec_to_array(val.value.clone());
            ScalarValue::Decimal128(
                Some(i128::from_be_bytes(array)),
                val.p as usize,
                val.s as usize,
            )
        }
        (Value::Date64Value(v), PrimitiveScalarType::Date64) => {
            ScalarValue::Date64(Some(*v))
        }
        (Value::TimeSecondValue(v), PrimitiveScalarType::TimeSecond) => {
            ScalarValue::TimestampSecond(Some(*v), None)
        }
        (Value::TimeMillisecondValue(v), PrimitiveScalarType::TimeMillisecond) => {
            ScalarValue::TimestampMillisecond(Some(*v), None)
        }
        (Value::IntervalYearmonthValue(v), PrimitiveScalarType::IntervalYearmonth) => {
            ScalarValue::IntervalYearMonth(Some(*v))
        }
        (Value::IntervalDaytimeValue(v), PrimitiveScalarType::IntervalDaytime) => {
            ScalarValue::IntervalDayTime(Some(*v))
        }
        _ => return Err(proto_error("Could not convert to the proper type")),
    })
}

fn from_proto_binary_op(op: &str) -> Result<Operator, Error> {
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
        "Like" => Ok(Operator::Like),
        "NotLike" => Ok(Operator::NotLike),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{:?}'",
            other
        ))),
    }
}

fn parse_optional_expr(
    p: &Option<Box<protobuf::LogicalExprNode>>,
) -> Result<Option<Expr>, Error> {
    match p {
        Some(expr) => expr.as_ref().try_into().map(Some),
        None => Ok(None),
    }
}

fn proto_error<S: Into<String>>(message: S) -> Error {
    Error::General(message.into())
}
