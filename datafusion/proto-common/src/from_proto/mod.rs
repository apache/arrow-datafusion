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

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use crate::common::proto_error;
use crate::protobuf_common as protobuf;
use arrow::array::{ArrayRef, AsArray};
use arrow::buffer::Buffer;
use arrow::csv::WriterBuilder;
use arrow::datatypes::{
    i256, DataType, Field, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
    Schema, TimeUnit, UnionFields, UnionMode,
};
use arrow::ipc::{reader::read_record_batch, root_as_message};

use datafusion_common::{
    arrow_datafusion_err,
    config::{
        ColumnOptions, CsvOptions, JsonOptions, ParquetOptions, TableParquetOptions,
    },
    file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
    parsers::CompressionTypeVariant,
    plan_datafusion_err,
    stats::Precision,
    Column, ColumnStatistics, Constraint, Constraints, DFSchema, DFSchemaRef,
    DataFusionError, JoinSide, ScalarValue, Statistics, TableReference,
};
use datafusion_common::logical_type::field::LogicalField;
use datafusion_common::logical_type::TypeRelation;

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
    pub fn required(field: impl Into<String>) -> Error {
        Error::MissingRequiredField(field.into())
    }

    pub fn unknown(name: impl Into<String>, value: i32) -> Error {
        Error::UnknownEnumVariant {
            name: name.into(),
            value,
        }
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        plan_datafusion_err!("{}", e)
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = Error>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    ///
    /// Returns None if the option is None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn optional(self) -> datafusion_common::Result<Option<T>, Error>;

    /// Converts an optional protobuf field to a different type, returning an error if None
    ///
    /// Returns `Error::MissingRequiredField` if None, otherwise calls [`TryInto::try_into`]
    /// on the contained data, returning any error encountered
    fn required(self, field: impl Into<String>) -> datafusion_common::Result<T, Error>;
}

impl<T, U> FromOptionalField<U> for Option<T>
where
    T: TryInto<U, Error = Error>,
{
    fn optional(self) -> datafusion_common::Result<Option<U>, Error> {
        self.map(|t| t.try_into()).transpose()
    }

    fn required(self, field: impl Into<String>) -> datafusion_common::Result<U, Error> {
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

    fn try_from(
        df_schema: &protobuf::DfSchema,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let df_fields = df_schema.columns.clone();
        let qualifiers_and_fields: Vec<(Option<TableReference>, Arc<LogicalField>)> = df_fields
            .iter()
            .map(|df_field| {
                let field: LogicalField = df_field.field.as_ref().required("field")?;
                Ok((
                    df_field
                        .qualifier
                        .as_ref()
                        .map(|q| q.relation.clone().into()),
                    Arc::new(field),
                ))
            })
            .collect::<datafusion_common::Result<Vec<_>, Error>>()?;

        Ok(DFSchema::new_with_metadata(
            qualifiers_and_fields,
            df_schema.metadata.clone(),
        )?)
    }
}

impl TryFrom<protobuf::DfSchema> for DFSchemaRef {
    type Error = Error;

    fn try_from(
        df_schema: protobuf::DfSchema,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let dfschema: DFSchema = (&df_schema).try_into()?;
        Ok(Arc::new(dfschema))
    }
}

impl TryFrom<&protobuf::ArrowType> for TypeRelation {
    type Error = Error;

    fn try_from(
        arrow_type: &protobuf::ArrowType,
    ) -> datafusion_common::Result<Self, Self::Error> {
        DataType::try_from(arrow_type).map(|t| t.into())
    }
}

impl TryFrom<&protobuf::ArrowType> for DataType {
    type Error = Error;

    fn try_from(
        arrow_type: &protobuf::ArrowType,
    ) -> datafusion_common::Result<Self, Self::Error> {
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
    ) -> datafusion_common::Result<Self, Self::Error> {
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
            arrow_type::ArrowTypeEnum::Utf8View(_) => DataType::Utf8View,
            arrow_type::ArrowTypeEnum::LargeUtf8(_) => DataType::LargeUtf8,
            arrow_type::ArrowTypeEnum::Binary(_) => DataType::Binary,
            arrow_type::ArrowTypeEnum::BinaryView(_) => DataType::BinaryView,
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


impl TryFrom<&protobuf::Field> for LogicalField {
    type Error = Error;
    fn try_from(field: &protobuf::Field) -> Result<Self, Self::Error> {
        Field::try_from(field).map(|t| t.into())
    }
}

impl TryFrom<&protobuf::Schema> for Schema {
    type Error = Error;

    fn try_from(
        schema: &protobuf::Schema,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let fields = schema
            .columns
            .iter()
            .map(Field::try_from)
            .collect::<datafusion_common::Result<Vec<_>, _>>()?;
        Ok(Self::new_with_metadata(fields, schema.metadata.clone()))
    }
}

impl TryFrom<&protobuf::ScalarValue> for ScalarValue {
    type Error = Error;

    fn try_from(
        scalar: &protobuf::ScalarValue,
    ) -> datafusion_common::Result<Self, Self::Error> {
        use protobuf::scalar_value::Value;

        let value = scalar
            .value
            .as_ref()
            .ok_or_else(|| Error::required("value"))?;

        Ok(match value {
            Value::BoolValue(v) => Self::Boolean(Some(*v)),
            Value::Utf8Value(v) => Self::Utf8(Some(v.to_owned())),
            Value::Utf8ViewValue(v) => Self::Utf8View(Some(v.to_owned())),
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
                    dictionaries,
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

                let dict_by_id: HashMap<i64,ArrayRef> = dictionaries.iter().map(|protobuf::scalar_nested_value::Dictionary { ipc_message, arrow_data }| {
                    let message = root_as_message(ipc_message.as_slice()).map_err(|e| {
                        Error::General(format!(
                            "Error IPC message while deserializing ScalarValue::List dictionary message: {e}"
                        ))
                    })?;
                    let buffer = Buffer::from(arrow_data);

                    let dict_batch = message.header_as_dictionary_batch().ok_or_else(|| {
                        Error::General(
                            "Unexpected message type deserializing ScalarValue::List dictionary message"
                                .to_string(),
                        )
                    })?;

                    let id = dict_batch.id();

                    let fields_using_this_dictionary = schema.fields_with_dict_id(id);
                    let first_field = fields_using_this_dictionary.first().ok_or_else(|| {
                        Error::General("dictionary id not found in schema while deserializing ScalarValue::List".to_string())
                    })?;

                    let values: ArrayRef = match first_field.data_type() {
                        DataType::Dictionary(_, ref value_type) => {
                            // Make a fake schema for the dictionary batch.
                            let value = value_type.as_ref().clone();
                            let schema = Schema::new(vec![Field::new("", value, true)]);
                            // Read a single column
                            let record_batch = read_record_batch(
                                &buffer,
                                dict_batch.data().unwrap(),
                                Arc::new(schema),
                                &Default::default(),
                                None,
                                &message.version(),
                            )?;
                            Ok(record_batch.column(0).clone())
                        }
                        _ => Err(Error::General("dictionary id not found in schema while deserializing ScalarValue::List".to_string())),
                    }?;

                    Ok((id,values))
                }).collect::<datafusion_common::Result<HashMap<_, _>>>()?;

                let record_batch = read_record_batch(
                    &buffer,
                    ipc_batch,
                    Arc::new(schema),
                    &dict_by_id,
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
            Value::BinaryViewValue(v) => Self::BinaryView(Some(v.clone())),
            Value::LargeBinaryValue(v) => Self::LargeBinary(Some(v.clone())),
            Value::IntervalDaytimeValue(v) => Self::IntervalDayTime(Some(
                IntervalDayTimeType::make_value(v.days, v.milliseconds),
            )),
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

    fn try_from(
        s: &protobuf::Statistics,
    ) -> datafusion_common::Result<Self, Self::Error> {
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

impl TryFrom<&protobuf::CsvWriterOptions> for CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(
        opts: &protobuf::CsvWriterOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let write_options = csv_writer_options_from_proto(opts)?;
        let compression: CompressionTypeVariant = opts.compression().into();
        Ok(CsvWriterOptions::new(write_options, compression))
    }
}

impl TryFrom<&protobuf::JsonWriterOptions> for JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(
        opts: &protobuf::JsonWriterOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let compression: CompressionTypeVariant = opts.compression().into();
        Ok(JsonWriterOptions::new(compression))
    }
}

impl TryFrom<&protobuf::CsvOptions> for CsvOptions {
    type Error = DataFusionError;

    fn try_from(
        proto_opts: &protobuf::CsvOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        Ok(CsvOptions {
            has_header: proto_opts.has_header.first().map(|h| *h != 0),
            delimiter: proto_opts.delimiter[0],
            quote: proto_opts.quote[0],
            escape: proto_opts.escape.first().copied(),
            double_quote: proto_opts.has_header.first().map(|h| *h != 0),
            compression: proto_opts.compression().into(),
            schema_infer_max_rec: proto_opts.schema_infer_max_rec as usize,
            date_format: (!proto_opts.date_format.is_empty())
                .then(|| proto_opts.date_format.clone()),
            datetime_format: (!proto_opts.datetime_format.is_empty())
                .then(|| proto_opts.datetime_format.clone()),
            timestamp_format: (!proto_opts.timestamp_format.is_empty())
                .then(|| proto_opts.timestamp_format.clone()),
            timestamp_tz_format: (!proto_opts.timestamp_tz_format.is_empty())
                .then(|| proto_opts.timestamp_tz_format.clone()),
            time_format: (!proto_opts.time_format.is_empty())
                .then(|| proto_opts.time_format.clone()),
            null_value: (!proto_opts.null_value.is_empty())
                .then(|| proto_opts.null_value.clone()),
            comment: proto_opts.comment.first().copied(),
        })
    }
}

impl TryFrom<&protobuf::ParquetOptions> for ParquetOptions {
    type Error = DataFusionError;

    fn try_from(
        value: &protobuf::ParquetOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        Ok(ParquetOptions {
            enable_page_index: value.enable_page_index,
            pruning: value.pruning,
            skip_metadata: value.skip_metadata,
            metadata_size_hint: value
                .metadata_size_hint_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_options::MetadataSizeHintOpt::MetadataSizeHint(v) => Some(v as usize),
                })
                .unwrap_or(None),
            pushdown_filters: value.pushdown_filters,
            reorder_filters: value.reorder_filters,
            data_pagesize_limit: value.data_pagesize_limit as usize,
            write_batch_size: value.write_batch_size as usize,
            writer_version: value.writer_version.clone(),
            compression: value.compression_opt.clone().map(|opt| match opt {
                protobuf::parquet_options::CompressionOpt::Compression(v) => Some(v),
            }).unwrap_or(None),
            dictionary_enabled: value.dictionary_enabled_opt.as_ref().map(|protobuf::parquet_options::DictionaryEnabledOpt::DictionaryEnabled(v)| *v),
            // Continuing from where we left off in the TryFrom implementation
            dictionary_page_size_limit: value.dictionary_page_size_limit as usize,
            statistics_enabled: value
                .statistics_enabled_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_options::StatisticsEnabledOpt::StatisticsEnabled(v) => Some(v),
                })
                .unwrap_or(None),
            max_statistics_size: value
                .max_statistics_size_opt.as_ref()
                .map(|opt| match opt {
                    protobuf::parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v) => Some(*v as usize),
                })
                .unwrap_or(None),
            max_row_group_size: value.max_row_group_size as usize,
            created_by: value.created_by.clone(),
            column_index_truncate_length: value
                .column_index_truncate_length_opt.as_ref()
                .map(|opt| match opt {
                    protobuf::parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(v) => Some(*v as usize),
                })
                .unwrap_or(None),
            data_page_row_count_limit: value.data_page_row_count_limit as usize,
            encoding: value
                .encoding_opt.clone()
                .map(|opt| match opt {
                    protobuf::parquet_options::EncodingOpt::Encoding(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_on_read: value.bloom_filter_on_read,
            bloom_filter_on_write: value.bloom_filter_on_write,
            bloom_filter_fpp: value.clone()
                .bloom_filter_fpp_opt
                .map(|opt| match opt {
                    protobuf::parquet_options::BloomFilterFppOpt::BloomFilterFpp(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_ndv: value.clone()
                .bloom_filter_ndv_opt
                .map(|opt| match opt {
                    protobuf::parquet_options::BloomFilterNdvOpt::BloomFilterNdv(v) => Some(v),
                })
                .unwrap_or(None),
            allow_single_file_parallelism: value.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: value.maximum_parallel_row_group_writers as usize,
            maximum_buffered_record_batches_per_stream: value.maximum_buffered_record_batches_per_stream as usize,

        })
    }
}

impl TryFrom<&protobuf::ColumnOptions> for ColumnOptions {
    type Error = DataFusionError;
    fn try_from(
        value: &protobuf::ColumnOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        Ok(ColumnOptions {
            compression: value.compression_opt.clone().map(|opt| match opt {
                protobuf::column_options::CompressionOpt::Compression(v) => Some(v),
            }).unwrap_or(None),
            dictionary_enabled: value.dictionary_enabled_opt.as_ref().map(|protobuf::column_options::DictionaryEnabledOpt::DictionaryEnabled(v)| *v),
            statistics_enabled: value
                .statistics_enabled_opt.clone()
                .map(|opt| match opt {
                    protobuf::column_options::StatisticsEnabledOpt::StatisticsEnabled(v) => Some(v),
                })
                .unwrap_or(None),
            max_statistics_size: value
                .max_statistics_size_opt.clone()
                .map(|opt| match opt {
                    protobuf::column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v) => Some(v as usize),
                })
                .unwrap_or(None),
            encoding: value
                .encoding_opt.clone()
                .map(|opt| match opt {
                    protobuf::column_options::EncodingOpt::Encoding(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_enabled: value.bloom_filter_enabled_opt.clone().map(|opt| match opt {
                protobuf::column_options::BloomFilterEnabledOpt::BloomFilterEnabled(v) => Some(v),
            })
                .unwrap_or(None),
            bloom_filter_fpp: value
                .bloom_filter_fpp_opt.clone()
                .map(|opt| match opt {
                    protobuf::column_options::BloomFilterFppOpt::BloomFilterFpp(v) => Some(v),
                })
                .unwrap_or(None),
            bloom_filter_ndv: value
                .bloom_filter_ndv_opt.clone()
                .map(|opt| match opt {
                    protobuf::column_options::BloomFilterNdvOpt::BloomFilterNdv(v) => Some(v),
                })
                .unwrap_or(None),
        })
    }
}

impl TryFrom<&protobuf::TableParquetOptions> for TableParquetOptions {
    type Error = DataFusionError;
    fn try_from(
        value: &protobuf::TableParquetOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let mut column_specific_options: HashMap<String, ColumnOptions> = HashMap::new();
        for protobuf::ColumnSpecificOptions {
            column_name,
            options: maybe_options,
        } in &value.column_specific_options
        {
            if let Some(options) = maybe_options {
                column_specific_options.insert(column_name.clone(), options.try_into()?);
            }
        }
        Ok(TableParquetOptions {
            global: value
                .global
                .as_ref()
                .map(|v| v.try_into())
                .unwrap()
                .unwrap(),
            column_specific_options,
            key_value_metadata: Default::default(),
        })
    }
}

impl TryFrom<&protobuf::JsonOptions> for JsonOptions {
    type Error = DataFusionError;

    fn try_from(
        proto_opts: &protobuf::JsonOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = proto_opts.compression();
        Ok(JsonOptions {
            compression: compression.into(),
            schema_infer_max_rec: proto_opts.schema_infer_max_rec as usize,
        })
    }
}

pub fn parse_i32_to_time_unit(value: &i32) -> datafusion_common::Result<TimeUnit, Error> {
    protobuf::TimeUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| Error::unknown("TimeUnit", *value))
}

pub fn parse_i32_to_interval_unit(
    value: &i32,
) -> datafusion_common::Result<IntervalUnit, Error> {
    protobuf::IntervalUnit::try_from(*value)
        .map(|t| t.into())
        .map_err(|_| Error::unknown("IntervalUnit", *value))
}

// panic here because no better way to convert from Vec to Array
fn vec_to_array<T, const N: usize>(v: Vec<T>) -> [T; N] {
    v.try_into().unwrap_or_else(|v: Vec<T>| {
        panic!("Expected a Vec of length {} but it was {}", N, v.len())
    })
}

/// Converts a vector of `protobuf::Field`s to `Arc<arrow::Field>`s.
pub fn parse_proto_fields_to_fields<'a, I>(
    fields: I,
) -> std::result::Result<Vec<Field>, Error>
where
    I: IntoIterator<Item = &'a protobuf::Field>,
{
    fields
        .into_iter()
        .map(Field::try_from)
        .collect::<datafusion_common::Result<_, _>>()
}

pub(crate) fn csv_writer_options_from_proto(
    writer_options: &protobuf::CsvWriterOptions,
) -> datafusion_common::Result<WriterBuilder> {
    let mut builder = WriterBuilder::new();
    if !writer_options.delimiter.is_empty() {
        if let Some(delimiter) = writer_options.delimiter.chars().next() {
            if delimiter.is_ascii() {
                builder = builder.with_delimiter(delimiter as u8);
            } else {
                return Err(proto_error("CSV Delimiter is not ASCII"));
            }
        } else {
            return Err(proto_error("Error parsing CSV Delimiter"));
        }
    }
    if !writer_options.quote.is_empty() {
        if let Some(quote) = writer_options.quote.chars().next() {
            if quote.is_ascii() {
                builder = builder.with_quote(quote as u8);
            } else {
                return Err(proto_error("CSV Quote is not ASCII"));
            }
        } else {
            return Err(proto_error("Error parsing CSV Quote"));
        }
    }
    if !writer_options.escape.is_empty() {
        if let Some(escape) = writer_options.escape.chars().next() {
            if escape.is_ascii() {
                builder = builder.with_escape(escape as u8);
            } else {
                return Err(proto_error("CSV Escape is not ASCII"));
            }
        } else {
            return Err(proto_error("Error parsing CSV Escape"));
        }
    }
    Ok(builder
        .with_header(writer_options.has_header)
        .with_date_format(writer_options.date_format.clone())
        .with_datetime_format(writer_options.datetime_format.clone())
        .with_timestamp_format(writer_options.timestamp_format.clone())
        .with_time_format(writer_options.time_format.clone())
        .with_null(writer_options.null_value.clone())
        .with_double_quote(writer_options.double_quote))
}
