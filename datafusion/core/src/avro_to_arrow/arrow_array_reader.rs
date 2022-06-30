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

//! Avro to Arrow array readers

use crate::arrow::array::{
    make_array, Array, ArrayBuilder, ArrayData, ArrayDataBuilder, ArrayRef,
    BooleanBuilder, LargeStringArray, ListBuilder, NullArray, OffsetSizeTrait,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
    StringDictionaryBuilder,
};
use crate::arrow::buffer::{Buffer, MutableBuffer};
use crate::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNumericType, ArrowPrimitiveType, DataType, Date32Type,
    Date64Type, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Schema, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use crate::arrow::error::ArrowError;
use crate::arrow::record_batch::RecordBatch;
use crate::arrow::util::bit_util;
use crate::error::{DataFusionError, Result};
use apache_avro::{
    schema::{Schema as AvroSchema, SchemaKind},
    types::Value,
    AvroResult, Error as AvroError, Reader as AvroReader,
};
use arrow::array::{BinaryArray, GenericListArray};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError::SchemaError;
use arrow::error::Result as ArrowResult;
use num_traits::NumCast;
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;

type RecordSlice<'a> = &'a [&'a Vec<(String, Value)>];

pub struct AvroArrowArrayReader<'a, R: Read> {
    reader: AvroReader<'a, R>,
    schema: SchemaRef,
    projection: Option<Vec<String>>,
    schema_lookup: BTreeMap<String, usize>,
}

impl<'a, R: Read> AvroArrowArrayReader<'a, R> {
    pub fn try_new(
        reader: R,
        schema: SchemaRef,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        let reader = AvroReader::new(reader)?;
        let writer_schema = reader.writer_schema().clone();
        let schema_lookup = Self::schema_lookup(writer_schema)?;
        Ok(Self {
            reader,
            schema,
            projection,
            schema_lookup,
        })
    }

    pub fn schema_lookup(schema: AvroSchema) -> Result<BTreeMap<String, usize>> {
        match schema {
            AvroSchema::Record {
                lookup: ref schema_lookup,
                ..
            } => Ok(schema_lookup.clone()),
            _ => Err(DataFusionError::ArrowError(SchemaError(
                "expected avro schema to be a record".to_string(),
            ))),
        }
    }

    /// Read the next batch of records
    #[allow(clippy::should_implement_trait)]
    pub fn next_batch(&mut self, batch_size: usize) -> ArrowResult<Option<RecordBatch>> {
        let rows = self
            .reader
            .by_ref()
            .take(batch_size)
            .map(|value| match value {
                Ok(Value::Record(v)) => Ok(v),
                Err(e) => Err(ArrowError::ParseError(format!(
                    "Failed to parse avro value: {:?}",
                    e
                ))),
                other => {
                    return Err(ArrowError::ParseError(format!(
                        "Row needs to be of type object, got: {:?}",
                        other
                    )))
                }
            })
            .collect::<ArrowResult<Vec<Vec<(String, Value)>>>>()?;
        if rows.is_empty() {
            // reached end of file
            return Ok(None);
        }
        let rows = rows.iter().collect::<Vec<&Vec<(String, Value)>>>();
        let projection = self.projection.clone().unwrap_or_else(Vec::new);
        let arrays =
            self.build_struct_array(rows.as_slice(), self.schema.fields(), &projection);
        let projected_fields: Vec<Field> = if projection.is_empty() {
            self.schema.fields().to_vec()
        } else {
            projection
                .iter()
                .map(|name| self.schema.column_with_name(name))
                .flatten()
                .map(|(_, field)| field.clone())
                .collect()
        };
        let projected_schema = Arc::new(Schema::new(projected_fields));
        arrays.and_then(|arr| RecordBatch::try_new(projected_schema, arr).map(Some))
    }

    fn build_boolean_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
    ) -> ArrowResult<ArrayRef> {
        let mut builder = BooleanBuilder::new(rows.len());
        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                if let Some(boolean) = resolve_boolean(&value) {
                    builder.append_value(boolean)?
                } else {
                    builder.append_null()?;
                }
            } else {
                builder.append_null()?;
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    #[allow(clippy::unnecessary_wraps)]
    fn build_primitive_array<T: ArrowPrimitiveType + Resolver>(
        &self,
        rows: RecordSlice,
        col_name: &str,
    ) -> ArrowResult<ArrayRef>
    where
        T: ArrowNumericType,
        T::Native: num_traits::cast::NumCast,
    {
        Ok(Arc::new(
            rows.iter()
                .map(|row| {
                    self.field_lookup(col_name, row)
                        .and_then(|value| resolve_item::<T>(&value))
                })
                .collect::<PrimitiveArray<T>>(),
        ))
    }

    #[inline(always)]
    #[allow(clippy::unnecessary_wraps)]
    fn build_string_dictionary_builder<T>(
        &self,
        row_len: usize,
    ) -> ArrowResult<StringDictionaryBuilder<T>>
    where
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let key_builder = PrimitiveBuilder::<T>::new(row_len);
        let values_builder = StringBuilder::new(row_len * 5);
        Ok(StringDictionaryBuilder::new(key_builder, values_builder))
    }

    fn build_wrapped_list_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
        key_type: &DataType,
    ) -> ArrowResult<ArrayRef> {
        match *key_type {
            DataType::Int8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int8Type>(&dtype, col_name, rows)
            }
            DataType::Int16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int16Type>(&dtype, col_name, rows)
            }
            DataType::Int32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int32Type>(&dtype, col_name, rows)
            }
            DataType::Int64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::Int64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<Int64Type>(&dtype, col_name, rows)
            }
            DataType::UInt8 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt8),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt8Type>(&dtype, col_name, rows)
            }
            DataType::UInt16 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt16),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt16Type>(&dtype, col_name, rows)
            }
            DataType::UInt32 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt32Type>(&dtype, col_name, rows)
            }
            DataType::UInt64 => {
                let dtype = DataType::Dictionary(
                    Box::new(DataType::UInt64),
                    Box::new(DataType::Utf8),
                );
                self.list_array_string_array_builder::<UInt64Type>(&dtype, col_name, rows)
            }
            ref e => Err(SchemaError(format!(
                "Data type is currently not supported for dictionaries in list : {:?}",
                e
            ))),
        }
    }

    #[inline(always)]
    fn list_array_string_array_builder<D>(
        &self,
        data_type: &DataType,
        col_name: &str,
        rows: RecordSlice,
    ) -> ArrowResult<ArrayRef>
    where
        D: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Utf8 => {
                let values_builder = StringBuilder::new(rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            DataType::Dictionary(_, _) => {
                let values_builder =
                    self.build_string_dictionary_builder::<D>(rows.len() * 5)?;
                Box::new(ListBuilder::new(values_builder))
            }
            e => {
                return Err(SchemaError(format!(
                    "Nested list data builder type is not supported: {:?}",
                    e
                )))
            }
        };

        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                // value can be an array or a scalar
                let vals: Vec<Option<String>> = if let Value::String(v) = value {
                    vec![Some(v.to_string())]
                } else if let Value::Array(n) = value {
                    n.iter()
                        .map(|v| resolve_string(&v))
                        .collect::<ArrowResult<Vec<String>>>()?
                        .into_iter()
                        .map(Some)
                        .collect::<Vec<Option<String>>>()
                } else if let Value::Null = value {
                    vec![None]
                } else if !matches!(value, Value::Record(_)) {
                    vec![Some(resolve_string(&value)?)]
                } else {
                    return Err(SchemaError(
                        "Only scalars are currently supported in Avro arrays".to_string(),
                    ));
                };

                // TODO: ARROW-10335: APIs of dictionary arrays and others are different. Unify
                // them.
                match data_type {
                    DataType::Utf8 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .ok_or_else(||ArrowError::SchemaError(
                                "Cast failed for ListBuilder<StringBuilder> during nested data parsing".to_string(),
                            ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append_value(&v)?
                            } else {
                                builder.values().append_null()?
                            };
                        }

                        // Append to the list
                        builder.append(true)?;
                    }
                    DataType::Dictionary(_, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<ListBuilder<StringDictionaryBuilder<D>>>().ok_or_else(||ArrowError::SchemaError(
                            "Cast failed for ListBuilder<StringDictionaryBuilder> during nested data parsing".to_string(),
                        ))?;
                        for val in vals {
                            if let Some(v) = val {
                                let _ = builder.values().append(&v)?;
                            } else {
                                builder.values().append_null()?
                            };
                        }

                        // Append to the list
                        builder.append(true)?;
                    }
                    e => {
                        return Err(SchemaError(format!(
                            "Nested list data builder type is not supported: {:?}",
                            e
                        )))
                    }
                }
            }
        }

        Ok(builder.finish() as ArrayRef)
    }

    #[inline(always)]
    fn build_dictionary_array<T>(
        &self,
        rows: RecordSlice,
        col_name: &str,
    ) -> ArrowResult<ArrayRef>
    where
        T::Native: num_traits::cast::NumCast,
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: StringDictionaryBuilder<T> =
            self.build_string_dictionary_builder(rows.len())?;
        for row in rows {
            if let Some(value) = self.field_lookup(col_name, row) {
                if let Ok(str_v) = resolve_string(&value) {
                    builder.append(str_v).map(drop)?
                } else {
                    builder.append_null()?
                }
            } else {
                builder.append_null()?
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }

    #[inline(always)]
    fn build_string_dictionary_array(
        &self,
        rows: RecordSlice,
        col_name: &str,
        key_type: &DataType,
        value_type: &DataType,
    ) -> ArrowResult<ArrayRef> {
        if let DataType::Utf8 = *value_type {
            match *key_type {
                DataType::Int8 => self.build_dictionary_array::<Int8Type>(rows, col_name),
                DataType::Int16 => {
                    self.build_dictionary_array::<Int16Type>(rows, col_name)
                }
                DataType::Int32 => {
                    self.build_dictionary_array::<Int32Type>(rows, col_name)
                }
                DataType::Int64 => {
                    self.build_dictionary_array::<Int64Type>(rows, col_name)
                }
                DataType::UInt8 => {
                    self.build_dictionary_array::<UInt8Type>(rows, col_name)
                }
                DataType::UInt16 => {
                    self.build_dictionary_array::<UInt16Type>(rows, col_name)
                }
                DataType::UInt32 => {
                    self.build_dictionary_array::<UInt32Type>(rows, col_name)
                }
                DataType::UInt64 => {
                    self.build_dictionary_array::<UInt64Type>(rows, col_name)
                }
                _ => Err(ArrowError::SchemaError(
                    "unsupported dictionary key type".to_string(),
                )),
            }
        } else {
            Err(ArrowError::SchemaError(
                "dictionary types other than UTF-8 not yet supported".to_string(),
            ))
        }
    }

    /// Build a nested GenericListArray from a list of unnested `Value`s
    fn build_nested_list_array<OffsetSize: OffsetSizeTrait>(
        &self,
        rows: &[&Value],
        list_field: &Field,
    ) -> ArrowResult<ArrayRef> {
        // build list offsets
        let mut cur_offset = OffsetSize::zero();
        let list_len = rows.len();
        let num_list_bytes = bit_util::ceil(list_len, 8);
        let mut offsets = Vec::with_capacity(list_len + 1);
        let mut list_nulls = MutableBuffer::from_len_zeroed(num_list_bytes);
        let list_nulls = list_nulls.as_slice_mut();
        offsets.push(cur_offset);
        rows.iter().enumerate().for_each(|(i, v)| {
            // TODO: unboxing Union(Array(Union(...))) should probably be done earlier
            let v = maybe_resolve_union(v);
            if let Value::Array(a) = v {
                cur_offset += OffsetSize::from_usize(a.len()).unwrap();
                bit_util::set_bit(list_nulls, i);
            } else if let Value::Null = v {
                // value is null, not incremented
            } else {
                cur_offset += OffsetSize::one();
            }
            offsets.push(cur_offset);
        });
        let valid_len = cur_offset.to_usize().unwrap();
        let array_data = match list_field.data_type() {
            DataType::Null => NullArray::new(valid_len).data().clone(),
            DataType::Boolean => {
                let num_bytes = bit_util::ceil(valid_len, 8);
                let mut bool_values = MutableBuffer::from_len_zeroed(num_bytes);
                let mut bool_nulls =
                    MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                let mut curr_index = 0;
                rows.iter().for_each(|v| {
                    if let Value::Array(vs) = v {
                        vs.iter().for_each(|value| {
                            if let Value::Boolean(child) = value {
                                // if valid boolean, append value
                                if *child {
                                    bit_util::set_bit(
                                        bool_values.as_slice_mut(),
                                        curr_index,
                                    );
                                }
                            } else {
                                // null slot
                                bit_util::unset_bit(
                                    bool_nulls.as_slice_mut(),
                                    curr_index,
                                );
                            }
                            curr_index += 1;
                        });
                    }
                });
                ArrayData::builder(list_field.data_type().clone())
                    .len(valid_len)
                    .add_buffer(bool_values.into())
                    .null_bit_buffer(Some(bool_nulls.into()))
                    .build()
                    .unwrap()
            }
            DataType::Int8 => self.read_primitive_list_values::<Int8Type>(rows),
            DataType::Int16 => self.read_primitive_list_values::<Int16Type>(rows),
            DataType::Int32 => self.read_primitive_list_values::<Int32Type>(rows),
            DataType::Int64 => self.read_primitive_list_values::<Int64Type>(rows),
            DataType::UInt8 => self.read_primitive_list_values::<UInt8Type>(rows),
            DataType::UInt16 => self.read_primitive_list_values::<UInt16Type>(rows),
            DataType::UInt32 => self.read_primitive_list_values::<UInt32Type>(rows),
            DataType::UInt64 => self.read_primitive_list_values::<UInt64Type>(rows),
            DataType::Float16 => {
                return Err(ArrowError::SchemaError("Float16 not supported".to_string()))
            }
            DataType::Float32 => self.read_primitive_list_values::<Float32Type>(rows),
            DataType::Float64 => self.read_primitive_list_values::<Float64Type>(rows),
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => {
                return Err(ArrowError::SchemaError(
                    "Temporal types are not yet supported, see ARROW-4803".to_string(),
                ))
            }
            DataType::Utf8 => flatten_string_values(rows)
                .into_iter()
                .collect::<StringArray>()
                .data()
                .clone(),
            DataType::LargeUtf8 => flatten_string_values(rows)
                .into_iter()
                .collect::<LargeStringArray>()
                .data()
                .clone(),
            DataType::List(field) => {
                let child =
                    self.build_nested_list_array::<i32>(&flatten_values(rows), field)?;
                child.data().clone()
            }
            DataType::LargeList(field) => {
                let child =
                    self.build_nested_list_array::<i64>(&flatten_values(rows), field)?;
                child.data().clone()
            }
            DataType::Struct(fields) => {
                // extract list values, with non-lists converted to Value::Null
                let array_item_count = rows
                    .iter()
                    .map(|row| match row {
                        Value::Array(values) => values.len(),
                        _ => 1,
                    })
                    .sum();
                let num_bytes = bit_util::ceil(array_item_count, 8);
                let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                let mut struct_index = 0;
                let rows: Vec<Vec<(String, Value)>> = rows
                    .iter()
                    .map(|row| {
                        if let Value::Array(values) = row {
                            values.iter().for_each(|_| {
                                bit_util::set_bit(
                                    null_buffer.as_slice_mut(),
                                    struct_index,
                                );
                                struct_index += 1;
                            });
                            values
                                .iter()
                                .map(|v| ("".to_string(), v.clone()))
                                .collect::<Vec<(String, Value)>>()
                        } else {
                            struct_index += 1;
                            vec![("null".to_string(), Value::Null)]
                        }
                    })
                    .collect();
                let rows = rows.iter().collect::<Vec<&Vec<(String, Value)>>>();
                let arrays =
                    self.build_struct_array(rows.as_slice(), fields.as_slice(), &[])?;
                let data_type = DataType::Struct(fields.clone());
                ArrayDataBuilder::new(data_type)
                    .len(rows.len())
                    .null_bit_buffer(Some(null_buffer.into()))
                    .child_data(arrays.into_iter().map(|a| a.data().clone()).collect())
                    .build()
                    .unwrap()
            }
            datatype => {
                return Err(ArrowError::SchemaError(format!(
                    "Nested list of {:?} not supported",
                    datatype
                )));
            }
        };
        // build list
        let list_data = ArrayData::builder(DataType::List(Box::new(list_field.clone())))
            .len(list_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(array_data)
            .null_bit_buffer(Some(list_nulls.into()))
            .build()
            .unwrap();
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
    }

    /// Builds the child values of a `StructArray`, falling short of constructing the StructArray.
    /// The function does not construct the StructArray as some callers would want the child arrays.
    ///
    /// *Note*: The function is recursive, and will read nested structs.
    ///
    /// If `projection` is not empty, then all values are returned. The first level of projection
    /// occurs at the `RecordBatch` level. No further projection currently occurs, but would be
    /// useful if plucking values from a struct, e.g. getting `a.b.c.e` from `a.b.c.{d, e}`.
    fn build_struct_array(
        &self,
        rows: RecordSlice,
        struct_fields: &[Field],
        projection: &[String],
    ) -> ArrowResult<Vec<ArrayRef>> {
        let arrays: ArrowResult<Vec<ArrayRef>> = struct_fields
            .iter()
            .filter(|field| projection.is_empty() || projection.contains(field.name()))
            .map(|field| {
                match field.data_type() {
                    DataType::Null => {
                        Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef)
                    }
                    DataType::Boolean => self.build_boolean_array(rows, field.name()),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, field.name())
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, field.name())
                    }
                    DataType::Int64 => {
                        self.build_primitive_array::<Int64Type>(rows, field.name())
                    }
                    DataType::Int32 => {
                        self.build_primitive_array::<Int32Type>(rows, field.name())
                    }
                    DataType::Int16 => {
                        self.build_primitive_array::<Int16Type>(rows, field.name())
                    }
                    DataType::Int8 => {
                        self.build_primitive_array::<Int8Type>(rows, field.name())
                    }
                    DataType::UInt64 => {
                        self.build_primitive_array::<UInt64Type>(rows, field.name())
                    }
                    DataType::UInt32 => {
                        self.build_primitive_array::<UInt32Type>(rows, field.name())
                    }
                    DataType::UInt16 => {
                        self.build_primitive_array::<UInt16Type>(rows, field.name())
                    }
                    DataType::UInt8 => {
                        self.build_primitive_array::<UInt8Type>(rows, field.name())
                    }
                    // TODO: this is incomplete
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<TimestampSecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<TimestampMicrosecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<TimestampMillisecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<TimestampNanosecondType>(
                                rows,
                                field.name(),
                            ),
                    },
                    DataType::Date64 => {
                        self.build_primitive_array::<Date64Type>(rows, field.name())
                    }
                    DataType::Date32 => {
                        self.build_primitive_array::<Date32Type>(rows, field.name())
                    }
                    DataType::Time64(unit) => match unit {
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<Time64MicrosecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<Time64NanosecondType>(
                                rows,
                                field.name(),
                            ),
                        t => Err(ArrowError::SchemaError(format!(
                            "TimeUnit {:?} not supported with Time64",
                            t
                        ))),
                    },
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => self
                            .build_primitive_array::<Time32SecondType>(
                                rows,
                                field.name(),
                            ),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<Time32MillisecondType>(
                                rows,
                                field.name(),
                            ),
                        t => Err(ArrowError::SchemaError(format!(
                            "TimeUnit {:?} not supported with Time32",
                            t
                        ))),
                    },
                    DataType::Utf8 | DataType::LargeUtf8 => Ok(Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = self.field_lookup(field.name(), row);
                                maybe_value
                                    .map(|value| resolve_string(&value))
                                    .transpose()
                            })
                            .collect::<ArrowResult<StringArray>>()?,
                    )
                        as ArrayRef),
                    DataType::Binary | DataType::LargeBinary => Ok(Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = self.field_lookup(field.name(), row);
                                maybe_value.and_then(resolve_bytes)
                            })
                            .collect::<BinaryArray>(),
                    )
                        as ArrayRef),
                    DataType::List(ref list_field) => {
                        match list_field.data_type() {
                            DataType::Dictionary(ref key_ty, _) => {
                                self.build_wrapped_list_array(rows, field.name(), key_ty)
                            }
                            _ => {
                                // extract rows by name
                                let extracted_rows = rows
                                    .iter()
                                    .map(|row| {
                                        self.field_lookup(field.name(), row)
                                            .unwrap_or(&Value::Null)
                                    })
                                    .collect::<Vec<&Value>>();
                                self.build_nested_list_array::<i32>(
                                    extracted_rows.as_slice(),
                                    list_field,
                                )
                            }
                        }
                    }
                    DataType::Dictionary(ref key_ty, ref val_ty) => self
                        .build_string_dictionary_array(
                            rows,
                            field.name(),
                            key_ty,
                            val_ty,
                        ),
                    DataType::Struct(fields) => {
                        let len = rows.len();
                        let num_bytes = bit_util::ceil(len, 8);
                        let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                        let struct_rows = rows
                            .iter()
                            .enumerate()
                            .map(|(i, row)| (i, self.field_lookup(field.name(), row)))
                            .map(|(i, v)| {
                                if let Some(Value::Record(value)) = v {
                                    bit_util::set_bit(null_buffer.as_slice_mut(), i);
                                    value
                                } else {
                                    panic!("expected struct got {:?}", v);
                                }
                            })
                            .collect::<Vec<&Vec<(String, Value)>>>();
                        let arrays =
                            self.build_struct_array(struct_rows.as_slice(), fields, &[])?;
                        // construct a struct array's data in order to set null buffer
                        let data_type = DataType::Struct(fields.clone());
                        let data = ArrayDataBuilder::new(data_type)
                            .len(len)
                            .null_bit_buffer(Some(null_buffer.into()))
                            .child_data(
                                arrays.into_iter().map(|a| a.data().clone()).collect(),
                            )
                            .build()
                            .unwrap();
                        Ok(make_array(data))
                    }
                    _ => Err(ArrowError::SchemaError(format!(
                        "type {:?} not supported",
                        field.data_type()
                    ))),
                }
            })
            .collect();
        arrays
    }

    /// Read the primitive list's values into ArrayData
    fn read_primitive_list_values<T>(&self, rows: &[&Value]) -> ArrayData
    where
        T: ArrowPrimitiveType + ArrowNumericType,
        T::Native: num_traits::cast::NumCast,
    {
        let values = rows
            .iter()
            .flat_map(|row| {
                let row = maybe_resolve_union(row);
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(resolve_item::<T>)
                        .collect::<Vec<Option<T::Native>>>()
                } else if let Some(f) = resolve_item::<T>(row) {
                    vec![Some(f)]
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Option<T::Native>>>();
        let array = values.iter().collect::<PrimitiveArray<T>>();
        array.data().clone()
    }

    fn field_lookup<'b>(
        &self,
        name: &str,
        row: &'b [(String, Value)],
    ) -> Option<&'b Value> {
        self.schema_lookup
            .get(name)
            .and_then(|i| row.get(*i))
            .map(|o| &o.1)
    }
}

/// Flattens a list of Avro values, by flattening lists, and treating all other values as
/// single-value lists.
/// This is used to read into nested lists (list of list, list of struct) and non-dictionary lists.
#[inline]
fn flatten_values<'a>(values: &[&'a Value]) -> Vec<&'a Value> {
    values
        .iter()
        .flat_map(|row| {
            let v = maybe_resolve_union(row);
            if let Value::Array(values) = v {
                values.iter().collect()
            } else {
                // we interpret a scalar as a single-value list to minimise data loss
                vec![v]
            }
        })
        .collect()
}

/// Flattens a list into string values, dropping Value::Null in the process.
/// This is useful for interpreting any Avro array as string, dropping nulls.
/// See `value_as_string`.
#[inline]
fn flatten_string_values(values: &[&Value]) -> Vec<Option<String>> {
    values
        .iter()
        .flat_map(|row| {
            let row = maybe_resolve_union(row);
            if let Value::Array(values) = row {
                values
                    .iter()
                    .map(|s| resolve_string(s).ok())
                    .collect::<Vec<Option<_>>>()
            } else if let Value::Null = row {
                vec![]
            } else {
                vec![resolve_string(row).ok()]
            }
        })
        .collect::<Vec<Option<_>>>()
}

/// Reads an Avro value as a string, regardless of its type.
/// This is useful if the expected datatype is a string, in which case we preserve
/// all the values regardless of they type.
fn resolve_string(v: &Value) -> ArrowResult<String> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Bytes(bytes) => {
            String::from_utf8(bytes.to_vec()).map_err(AvroError::ConvertToUtf8)
        }
        other => Err(AvroError::GetString(other.into())),
    }
    .map_err(|e| SchemaError(format!("expected resolvable string : {:?}", e)))
}

fn resolve_u8(v: &Value) -> AvroResult<u8> {
    let int = match v {
        Value::Int(n) => Ok(Value::Int(*n)),
        Value::Long(n) => Ok(Value::Int(*n as i32)),
        other => Err(AvroError::GetU8(other.into())),
    }?;
    if let Value::Int(n) = int {
        if n >= 0 && n <= std::convert::From::from(u8::MAX) {
            return Ok(n as u8);
        }
    }

    Err(AvroError::GetU8(int.into()))
}

fn resolve_bytes(v: &Value) -> Option<Vec<u8>> {
    let v = if let Value::Union(_, b) = v { b } else { v };
    match v {
        Value::Bytes(_) => Ok(v.clone()),
        Value::String(s) => Ok(Value::Bytes(s.clone().into_bytes())),
        Value::Array(items) => Ok(Value::Bytes(
            items
                .iter()
                .map(resolve_u8)
                .collect::<std::result::Result<Vec<_>, _>>()
                .ok()?,
        )),
        other => Err(AvroError::GetBytes(other.into())),
    }
    .ok()
    .and_then(|v| match v {
        Value::Bytes(s) => Some(s),
        _ => None,
    })
}

fn resolve_boolean(value: &Value) -> Option<bool> {
    let v = if let Value::Union(_, b) = value {
        b
    } else {
        value
    };
    match v {
        Value::Boolean(boolean) => Some(*boolean),
        _ => None,
    }
}

trait Resolver: ArrowPrimitiveType {
    fn resolve(value: &Value) -> Option<Self::Native>;
}

fn resolve_item<T: Resolver>(value: &Value) -> Option<T::Native> {
    T::resolve(value)
}

fn maybe_resolve_union(value: &Value) -> &Value {
    if SchemaKind::from(value) == SchemaKind::Union {
        // Pull out the Union, and attempt to resolve against it.
        match value {
            Value::Union(_, b) => b,
            _ => unreachable!(),
        }
    } else {
        value
    }
}

impl<N> Resolver for N
where
    N: ArrowNumericType,
    N::Native: num_traits::cast::NumCast,
{
    fn resolve(value: &Value) -> Option<Self::Native> {
        let value = maybe_resolve_union(value);
        match value {
            Value::Int(i) | Value::TimeMillis(i) | Value::Date(i) => NumCast::from(*i),
            Value::Long(l)
            | Value::TimeMicros(l)
            | Value::TimestampMillis(l)
            | Value::TimestampMicros(l) => NumCast::from(*l),
            Value::Float(f) => NumCast::from(*f),
            Value::Double(f) => NumCast::from(*f),
            Value::Duration(_d) => unimplemented!(), // shenanigans type
            Value::Null => None,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::arrow::array::Array;
    use crate::arrow::datatypes::{Field, TimeUnit};
    use crate::avro_to_arrow::{Reader, ReaderBuilder};
    use arrow::array::{Int32Array, Int64Array, ListArray, TimestampMicrosecondArray};
    use arrow::datatypes::DataType;
    use std::fs::File;

    fn build_reader(name: &str, batch_size: usize) -> Reader<File> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/{}", testdata, name);
        let builder = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(batch_size);
        builder.build(File::open(filename).unwrap()).unwrap()
    }

    // TODO: Fixed, Enum, Dictionary

    #[test]
    fn test_time_avro_milliseconds() {
        let mut reader = build_reader("alltypes_plain.avro", 10);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let timestamp_col = schema.column_with_name("timestamp_col").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            timestamp_col.1.data_type()
        );
        let timestamp_array = batch
            .column(timestamp_col.0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for i in 0..timestamp_array.len() {
            assert!(timestamp_array.is_valid(i));
        }
        assert_eq!(1235865600000000, timestamp_array.value(0));
        assert_eq!(1235865660000000, timestamp_array.value(1));
        assert_eq!(1238544000000000, timestamp_array.value(2));
        assert_eq!(1238544060000000, timestamp_array.value(3));
        assert_eq!(1233446400000000, timestamp_array.value(4));
        assert_eq!(1233446460000000, timestamp_array.value(5));
        assert_eq!(1230768000000000, timestamp_array.value(6));
        assert_eq!(1230768060000000, timestamp_array.value(7));
    }

    #[test]
    fn test_avro_read_list() {
        let mut reader = build_reader("list_columns.avro", 3);
        let schema = reader.schema();
        let (col_id_index, _) = schema.column_with_name("int64_list").unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        let a_array = batch
            .column(col_id_index)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            *a_array.data_type(),
            DataType::List(Box::new(Field::new("bigint", DataType::Int64, true)))
        );
        let array = a_array.value(0);
        assert_eq!(*array.data_type(), DataType::Int64);

        assert_eq!(
            6,
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .sum::<i64>()
        );
    }
    #[test]
    fn test_avro_read_nested_list() {
        let mut reader = build_reader("nested_lists.snappy.avro", 3);
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_avro_iterator() {
        let reader = build_reader("alltypes_plain.avro", 5);
        let schema = reader.schema();
        let (col_id_index, _) = schema.column_with_name("id").unwrap();

        let mut sum_num_rows = 0;
        let mut num_batches = 0;
        let mut sum_id = 0;
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(11, batch.num_columns());
            sum_num_rows += batch.num_rows();
            num_batches += 1;
            let batch_schema = batch.schema();
            assert_eq!(schema, batch_schema);
            let a_array = batch
                .column(col_id_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            sum_id += (0..a_array.len()).map(|i| a_array.value(i)).sum::<i32>();
        }
        assert_eq!(8, sum_num_rows);
        assert_eq!(2, num_batches);
        assert_eq!(28, sum_id);
    }
}
