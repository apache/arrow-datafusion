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

//! [`min_statistics`] and [`max_statistics`] convert statistics in parquet format to arrow [`ArrayRef`].

// TODO: potentially move this to arrow-rs: https://github.com/apache/arrow-rs/issues/4328

use arrow::{array::ArrayRef, datatypes::DataType};
use arrow_array::{new_empty_array, new_null_array, Array, StructArray, UInt64Array};
use arrow_schema::{Field, FieldRef, Fields, Schema};
use datafusion_common::{
    internal_datafusion_err, internal_err, plan_err, DataFusionError, Result, ScalarValue,
};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;

// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i128(b: &[u8]) -> i128 {
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(sign_extend_be(b))
}

// Copy from arrow-rs
// https://github.com/apache/arrow-rs/blob/733b7e7fd1e8c43a404c3ce40ecf741d493c21b4/parquet/src/arrow/buffer/bit_util.rs#L55
// Convert the byte slice to fixed length byte array with the length of 16
fn sign_extend_be(b: &[u8]) -> [u8; 16] {
    assert!(b.len() <= 16, "Array too large, expected less than 16");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; 16] } else { [0u8; 16] };
    for (d, s) in result.iter_mut().skip(16 - b.len()).zip(b) {
        *d = *s;
    }
    result
}

/// Extract a single min/max statistics from a [`ParquetStatistics`] object
///
/// * `$column_statistics` is the `ParquetStatistics` object
/// * `$func is the function` (`min`/`max`) to call to get the value
/// * `$bytes_func` is the function (`min_bytes`/`max_bytes`) to call to get the value as bytes
/// * `$target_arrow_type` is the [`DataType`] of the target statistics
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident, $target_arrow_type:expr) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => {
                match $target_arrow_type {
                    // int32 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(*s.$func() as i128),
                            *precision,
                            *scale,
                        ))
                    }
                    Some(DataType::Int8) => {
                        Some(ScalarValue::Int8(Some((*s.$func()).try_into().unwrap())))
                    }
                    Some(DataType::Int16) => {
                        Some(ScalarValue::Int16(Some((*s.$func()).try_into().unwrap())))
                    }
                    Some(DataType::UInt8) => {
                        Some(ScalarValue::UInt8(Some((*s.$func()).try_into().unwrap())))
                    }
                    Some(DataType::UInt16) => {
                        Some(ScalarValue::UInt16(Some((*s.$func()).try_into().unwrap())))
                    }
                    Some(DataType::UInt32) => {
                        Some(ScalarValue::UInt32(Some((*s.$func()) as u32)))
                    }
                    Some(DataType::Date32) => {
                        Some(ScalarValue::Date32(Some(*s.$func())))
                    }
                    Some(DataType::Date64) => {
                        Some(ScalarValue::Date64(Some(i64::from(*s.$func()) * 24 * 60 * 60 * 1000)))
                    }
                    _ => Some(ScalarValue::Int32(Some(*s.$func()))),
                }
            }
            ParquetStatistics::Int64(s) => {
                match $target_arrow_type {
                    // int64 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(*s.$func() as i128),
                            *precision,
                            *scale,
                        ))
                    }
                    Some(DataType::UInt64) => {
                        Some(ScalarValue::UInt64(Some((*s.$func()) as u64)))
                    }
                    _ => Some(ScalarValue::Int64(Some(*s.$func()))),
                }
            }
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                match $target_arrow_type {
                    // decimal data type
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(from_bytes_to_i128(s.$bytes_func())),
                            *precision,
                            *scale,
                        ))
                    }
                    Some(DataType::Binary) => {
                        Some(ScalarValue::Binary(Some(s.$bytes_func().to_vec())))
                    }
                    _ => {
                        let s = std::str::from_utf8(s.$bytes_func())
                            .map(|s| s.to_string())
                            .ok();
                        if s.is_none() {
                            log::debug!(
                                "Utf8 statistics is a non-UTF8 value, ignoring it."
                            );
                        }
                        Some(ScalarValue::Utf8(s))
                    }
                }
            }
            // type not fully supported yet
            ParquetStatistics::FixedLenByteArray(s) => {
                match $target_arrow_type {
                    // just support specific logical data types, there are others each
                    // with their own ordering
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(from_bytes_to_i128(s.$bytes_func())),
                            *precision,
                            *scale,
                        ))
                    }
                    Some(DataType::FixedSizeBinary(size)) => {
                        let value = s.$bytes_func().to_vec();
                        let value = if value.len().try_into() == Ok(*size) {
                            Some(value)
                        } else {
                            log::debug!(
                                "FixedSizeBinary({}) statistics is a binary of size {}, ignoring it.",
                                size,
                                value.len(),
                            );
                            None
                        };
                        Some(ScalarValue::FixedSizeBinary(
                            *size,
                            value,
                        ))
                    }
                    _ => None,
                }
            }
        }
    }};
}

/// Lookups up the parquet column by name
///
/// Returns the parquet column index and the corresponding arrow field
pub(crate) fn parquet_column<'a>(
    parquet_schema: &SchemaDescriptor,
    arrow_schema: &'a Schema,
    name: &str,
) -> Option<(usize, &'a FieldRef)> {
    let (root_idx, field) = arrow_schema.fields.find(name)?;
    match field.data_type() {
        DataType::Struct(_) => {
            let parquet_idx = (0..parquet_schema.columns().len())
                .find(|x| parquet_schema.get_column_root_idx(*x) == root_idx)?;
            Some((parquet_idx, field))
        }
        _ => {
            if field.data_type().is_nested() {
                // Nested fields are not supported and require non-trivial logic
                // to correctly walk the parquet schema accounting for the
                // logical type rules - <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
                //
                // For example a ListArray could correspond to anything from 1 to 3 levels
                // in the parquet schema
                return None;
            }

            // This could be made more efficient (#TBD)
            let parquet_idx = (0..parquet_schema.columns().len())
                .find(|x| parquet_schema.get_column_root_idx(*x) == root_idx)?;
            Some((parquet_idx, field))
        }
    }
}

/// Extracts the min statistics from an iterator of [`ParquetStatistics`] to an [`ArrayRef`]
pub(crate) fn min_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    let scalars = iterator
        .map(|x| x.and_then(|s| get_statistic!(s, min, min_bytes, Some(data_type))));
    collect_scalars(data_type, scalars)
}

/// Extract the min statistics for struct array
pub(crate) fn struct_min_statistics(
    row_groups: &[RowGroupMetaData],
    struct_fields: &Fields,
) -> Result<ArrayRef, DataFusionError> {
    let mut child_data = Vec::new();
    let mut fields = Vec::new();

    if struct_fields.iter().any(|f| f.data_type().is_nested()) {
        return Ok(new_empty_array(&DataType::Struct(struct_fields.clone())));
    }

    for (idx, field) in struct_fields.iter().enumerate() {
        // Handle non-nested fields
        let max_value = row_groups
            .iter()
            .map(|x| x.column(idx).statistics())
            .map(|x| {
                x.and_then(|s| get_statistic!(s, min, min_bytes, Some(field.data_type())))
            });
        let array = collect_scalars(field.data_type(), max_value)?;
        child_data.push(Arc::new(array) as Arc<dyn Array>);
        fields.push(Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    // Create a StructArray from collected fields and data
    let struct_array =
        StructArray::from(fields.into_iter().zip(child_data).collect::<Vec<_>>());
    println!("the struct array is {:?}", struct_array);
    Ok(Arc::new(struct_array) as ArrayRef)
}

/// Extract the max statistics for struct array
pub(crate) fn struct_max_statistics(
    row_groups: &[RowGroupMetaData],
    struct_fields: &Fields,
) -> Result<ArrayRef, DataFusionError> {
    let mut child_data = Vec::new();
    let mut fields = Vec::new();

    if struct_fields.iter().any(|f| f.data_type().is_nested()) {
        return Ok(new_empty_array(&DataType::Struct(struct_fields.clone())));
    }

    for (idx, field) in struct_fields.iter().enumerate() {
        // Handle non-nested fields
        let max_value = row_groups
            .iter()
            .map(|x| x.column(idx).statistics())
            .map(|x| {
                x.and_then(|s| get_statistic!(s, max, max_bytes, Some(field.data_type())))
            });
        let array = collect_scalars(field.data_type(), max_value)?;
        child_data.push(Arc::new(array) as Arc<dyn Array>);
        fields.push(Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    // Create a StructArray from collected fields and data
    let struct_array =
        StructArray::from(fields.into_iter().zip(child_data).collect::<Vec<_>>());
    Ok(Arc::new(struct_array) as ArrayRef)
}

/// Extract the nullcount statistics for struct array
pub(crate) fn struct_null_count_statistics(
    row_groups: &[RowGroupMetaData],
    struct_fields: &Fields,
) -> Result<ArrayRef, DataFusionError> {
    if struct_fields.iter().any(|f| f.data_type().is_nested()) {
        return Ok(Arc::new(new_empty_array(&DataType::UInt64)) as ArrayRef);
    }

    let null_count = row_groups
        .iter()
        .filter(|rg| {
            struct_fields.iter().enumerate().all(|(idx, _)| {
                rg.column(idx)
                    .statistics()
                    .map_or(false, |stats| stats.null_count() > 0)
            })
        })
        .count() as u64;

    Ok(Arc::new(UInt64Array::from(vec![null_count])) as ArrayRef)
}

/// Extracts the max statistics from an iterator of [`ParquetStatistics`] to an [`ArrayRef`]
pub(crate) fn max_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    let scalars = iterator
        .map(|x| x.and_then(|s| get_statistic!(s, max, max_bytes, Some(data_type))));
    collect_scalars(data_type, scalars)
}

/// Builds an array from an iterator of ScalarValue
fn collect_scalars<I: Iterator<Item = Option<ScalarValue>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    let mut scalars = iterator.peekable();
    match scalars.peek().is_none() {
        true => Ok(new_empty_array(data_type)),
        false => {
            let null = ScalarValue::try_from(data_type)?;
            ScalarValue::iter_to_array(scalars.map(|x| x.unwrap_or_else(|| null.clone())))
        }
    }
}

/// What type of statistics should be extracted?
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequestedStatistics {
    /// Minimum Value
    Min,
    /// Maximum Value
    Max,
    /// Null Count, returned as a [`UInt64Array`])
    NullCount,
}

/// Extracts Parquet statistics as Arrow arrays
///
/// This is used to convert Parquet statistics to Arrow arrays, with proper type
/// conversions. This information can be used for pruning parquet files or row
/// groups based on the statistics embedded in parquet files
///
/// # Schemas
///
/// The schema of the parquet file and the arrow schema are used to convert the
/// underlying statistics value (stored as a parquet value) into the
/// corresponding Arrow  value. For example, Decimals are stored as binary in
/// parquet files.
///
/// The parquet_schema and arrow _schema do not have to be identical (for
/// example, the columns may be in different orders and one or the other schemas
/// may have additional columns). The function [`parquet_column`] is used to
/// match the column in the parquet file to the column in the arrow schema.
///
/// # Multiple parquet files
///
/// This API is designed to support efficiently extracting statistics from
/// multiple parquet files (hence why the parquet schema is passed in as an
/// argument). This is useful when building an index for a directory of parquet
/// files.
///
#[derive(Debug)]
pub struct StatisticsConverter<'a> {
    /// The name of the column to extract statistics for
    column_name: &'a str,
    /// The type of statistics to extract
    statistics_type: RequestedStatistics,
    /// The arrow schema of the query
    arrow_schema: &'a Schema,
    /// The field (with data type) of the column in the arrow schema
    arrow_field: &'a Field,
}

impl<'a> StatisticsConverter<'a> {
    /// Returns a [`UInt64Array`] with counts for each row group
    ///
    /// The returned array has no nulls, and has one value for each row group.
    /// Each value is the number of rows in the row group.
    pub fn row_counts(metadata: &ParquetMetaData) -> Result<UInt64Array> {
        let row_groups = metadata.row_groups();
        let mut builder = UInt64Array::builder(row_groups.len());
        for row_group in row_groups {
            let row_count = row_group.num_rows();
            let row_count: u64 = row_count.try_into().map_err(|e| {
                internal_datafusion_err!(
                    "Parquet row count {row_count} too large to convert to u64: {e}"
                )
            })?;
            builder.append_value(row_count);
        }
        Ok(builder.finish())
    }

    /// create an new statistics converter
    pub fn try_new(
        column_name: &'a str,
        statistics_type: RequestedStatistics,
        arrow_schema: &'a Schema,
    ) -> Result<Self> {
        // ensure the requested column is in the arrow schema
        let Some((_idx, arrow_field)) = arrow_schema.column_with_name(column_name) else {
            return plan_err!(
                "Column '{}' not found in schema for statistics conversion",
                column_name
            );
        };

        Ok(Self {
            column_name,
            statistics_type,
            arrow_schema,
            arrow_field,
        })
    }

    /// extract the statistics from a parquet file, given the parquet file's metadata
    ///
    /// The returned array contains 1 value for each row group in the parquet
    /// file in order
    ///
    /// Each value is either
    /// * the requested statistics type for the column
    /// * a null value, if the statistics can not be extracted
    ///
    /// Note that a null value does NOT mean the min or max value was actually
    /// `null` it means it the requested statistic is unknown
    ///
    /// Reasons for not being able to extract the statistics include:
    /// * the column is not present in the parquet file
    /// * statistics for the column are not present in the row group
    /// * the stored statistic value can not be converted to the requested type
    pub fn extract(&self, metadata: &ParquetMetaData) -> Result<ArrayRef> {
        let data_type = self.arrow_field.data_type();
        let num_row_groups = metadata.row_groups().len();

        let parquet_schema = metadata.file_metadata().schema_descr();
        let row_groups = metadata.row_groups();
        // find the column in the parquet schema, if not, return a null array
        let Some((parquet_idx, matched_field)) =
            parquet_column(parquet_schema, self.arrow_schema, self.column_name)
        else {
            // column was in the arrow schema but not in the parquet schema, so return a null array
            return Ok(new_null_array(data_type, num_row_groups));
        };
        match matched_field.data_type() {
            // need to deal with nested struct
            DataType::Struct(fields) => match self.statistics_type {
                RequestedStatistics::Max => {
                    Ok(struct_max_statistics(row_groups, fields)?)
                }
                RequestedStatistics::Min => {
                    Ok(struct_min_statistics(row_groups, fields)?)
                }
                RequestedStatistics::NullCount => {
                    Ok(struct_null_count_statistics(row_groups, fields)?)
                }
            },
            // currently set to return null
            v if v.is_nested() => Ok(new_null_array(data_type, num_row_groups)),
            _ => {
                // sanity check that matching field matches the arrow field
                if matched_field.as_ref() != self.arrow_field {
                    return internal_err!(
                    "Matched column '{:?}' does not match original matched column '{:?}'",
                    matched_field,
                    self.arrow_field
                );
                }

                // Get an iterator over the column statistics
                let iter = row_groups
                    .iter()
                    .map(|x| x.column(parquet_idx).statistics());

                match self.statistics_type {
                    RequestedStatistics::Min => min_statistics(data_type, iter),
                    RequestedStatistics::Max => max_statistics(data_type, iter),
                    RequestedStatistics::NullCount => {
                        let null_counts = iter.map(|stats| stats.map(|s| s.null_count()));
                        Ok(Arc::new(UInt64Array::from_iter(null_counts)))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::compute::kernels::cast_utils::Parser;
    use arrow::datatypes::{Date32Type, Date64Type};
    use arrow_array::{
        new_null_array, Array, BinaryArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, RecordBatch, StringArray, StructArray, TimestampNanosecondArray,
    };
    use arrow_schema::{Field, SchemaRef};
    use bytes::Bytes;
    use datafusion_common::test_util::parquet_test_data;
    use parquet::arrow::arrow_reader::ArrowReaderBuilder;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::path::PathBuf;
    use std::sync::Arc;

    // TODO error cases (with parquet statistics that are mismatched in expected type)

    #[test]
    fn roundtrip_empty() {
        let empty_bool_array = new_empty_array(&DataType::Boolean);
        Test {
            input: empty_bool_array.clone(),
            expected_min: empty_bool_array.clone(),
            expected_max: empty_bool_array.clone(),
        }
        .run()
    }

    #[test]
    fn roundtrip_bool() {
        Test {
            input: bool_array([
                // row group 1
                Some(true),
                None,
                Some(true),
                // row group 2
                Some(true),
                Some(false),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: bool_array([Some(true), Some(false), None]),
            expected_max: bool_array([Some(true), Some(true), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_int32() {
        Test {
            input: i32_array([
                // row group 1
                Some(1),
                None,
                Some(3),
                // row group 2
                Some(0),
                Some(5),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: i32_array([Some(1), Some(0), None]),
            expected_max: i32_array([Some(3), Some(5), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_int64() {
        Test {
            input: i64_array([
                // row group 1
                Some(1),
                None,
                Some(3),
                // row group 2
                Some(0),
                Some(5),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: i64_array([Some(1), Some(0), None]),
            expected_max: i64_array(vec![Some(3), Some(5), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_f32() {
        Test {
            input: f32_array([
                // row group 1
                Some(1.0),
                None,
                Some(3.0),
                // row group 2
                Some(-1.0),
                Some(5.0),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: f32_array([Some(1.0), Some(-1.0), None]),
            expected_max: f32_array([Some(3.0), Some(5.0), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_f64() {
        Test {
            input: f64_array([
                // row group 1
                Some(1.0),
                None,
                Some(3.0),
                // row group 2
                Some(-1.0),
                Some(5.0),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: f64_array([Some(1.0), Some(-1.0), None]),
            expected_max: f64_array([Some(3.0), Some(5.0), None]),
        }
        .run()
    }

    #[test]
    #[should_panic(
        expected = "Inconsistent types in ScalarValue::iter_to_array. Expected Int64, got TimestampNanosecond(NULL, None)"
    )]
    // Due to https://github.com/apache/datafusion/issues/8295
    fn roundtrip_timestamp() {
        Test {
            input: timestamp_array([
                // row group 1
                Some(1),
                None,
                Some(3),
                // row group 2
                Some(9),
                Some(5),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: timestamp_array([Some(1), Some(5), None]),
            expected_max: timestamp_array([Some(3), Some(9), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_decimal() {
        Test {
            input: Arc::new(
                Decimal128Array::from(vec![
                    // row group 1
                    Some(100),
                    None,
                    Some(22000),
                    // row group 2
                    Some(500000),
                    Some(330000),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ])
                .with_precision_and_scale(9, 2)
                .unwrap(),
            ),
            expected_min: Arc::new(
                Decimal128Array::from(vec![Some(100), Some(330000), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            expected_max: Arc::new(
                Decimal128Array::from(vec![Some(22000), Some(500000), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
        }
        .run()
    }

    #[test]
    fn roundtrip_utf8() {
        Test {
            input: utf8_array([
                // row group 1
                Some("A"),
                None,
                Some("Q"),
                // row group 2
                Some("ZZ"),
                Some("AA"),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: utf8_array([Some("A"), Some("AA"), None]),
            expected_max: utf8_array([Some("Q"), Some("ZZ"), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_struct() {
        let mut test = Test {
            input: struct_array(vec![
                // row group 1
                (Some(true), Some(1)),
                (None, None),
                (Some(true), Some(3)),
                // row group 2
                (Some(true), Some(0)),
                (Some(false), Some(5)),
                (None, None),
                // row group 3
                (None, None),
                (None, None),
                (None, None),
            ]),
            expected_min: struct_array(vec![
                (Some(true), Some(1)),
                (Some(true), Some(0)),
                (None, None),
            ]),

            expected_max: struct_array(vec![
                (Some(true), Some(3)),
                (Some(true), Some(0)),
                (None, None),
            ]),
        };
        // Due to https://github.com/apache/datafusion/issues/8334,
        // statistics for struct arrays are not supported
        test.expected_min =
            new_null_array(test.input.data_type(), test.expected_min.len());
        test.expected_max =
            new_null_array(test.input.data_type(), test.expected_min.len());
        test.run()
    }

    #[test]
    fn roundtrip_binary() {
        Test {
            input: Arc::new(BinaryArray::from_opt_vec(vec![
                // row group 1
                Some(b"A"),
                None,
                Some(b"Q"),
                // row group 2
                Some(b"ZZ"),
                Some(b"AA"),
                None,
                // row group 3
                None,
                None,
                None,
            ])),
            expected_min: Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"A"),
                Some(b"AA"),
                None,
            ])),
            expected_max: Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"Q"),
                Some(b"ZZ"),
                None,
            ])),
        }
        .run()
    }

    #[test]
    fn roundtrip_date32() {
        Test {
            input: date32_array(vec![
                // row group 1
                Some("2021-01-01"),
                None,
                Some("2021-01-03"),
                // row group 2
                Some("2021-01-01"),
                Some("2021-01-05"),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: date32_array(vec![
                Some("2021-01-01"),
                Some("2021-01-01"),
                None,
            ]),
            expected_max: date32_array(vec![
                Some("2021-01-03"),
                Some("2021-01-05"),
                None,
            ]),
        }
        .run()
    }

    #[test]
    fn roundtrip_date64() {
        Test {
            input: date64_array(vec![
                // row group 1
                Some("2021-01-01"),
                None,
                Some("2021-01-03"),
                // row group 2
                Some("2021-01-01"),
                Some("2021-01-05"),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: date64_array(vec![
                Some("2021-01-01"),
                Some("2021-01-01"),
                None,
            ]),
            expected_max: date64_array(vec![
                Some("2021-01-03"),
                Some("2021-01-05"),
                None,
            ]),
        }
        .run()
    }

    #[test]
    fn struct_and_non_struct() {
        // Ensures that statistics for an array that appears *after* a struct
        // array are not wrong
        let struct_col = struct_array(vec![
            // row group 1
            (Some(true), Some(1)),
            (None, None),
            (Some(true), Some(3)),
        ]);
        let int_col = i32_array([Some(100), Some(200), Some(300)]);
        let expected_min = i32_array([Some(100)]);
        let expected_max = i32_array(vec![Some(300)]);

        // use a name that shadows a name in the struct column
        match struct_col.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.get(1).unwrap().name(), "int_col")
            }
            _ => panic!("unexpected data type for struct column"),
        };

        let input_batch = RecordBatch::try_from_iter([
            ("struct_col", struct_col),
            ("int_col", int_col),
        ])
        .unwrap();

        let schema = input_batch.schema();

        let metadata = parquet_metadata(schema.clone(), input_batch);
        let parquet_schema = metadata.file_metadata().schema_descr();

        // read the int_col statistics
        let (idx, _) = parquet_column(parquet_schema, &schema, "int_col").unwrap();
        assert_eq!(idx, 2);

        let row_groups = metadata.row_groups();
        let iter = row_groups.iter().map(|x| x.column(idx).statistics());

        let min = min_statistics(&DataType::Int32, iter.clone()).unwrap();
        assert_eq!(
            &min,
            &expected_min,
            "Min. Statistics\n\n{}\n\n",
            DisplayStats(row_groups)
        );

        let max = max_statistics(&DataType::Int32, iter).unwrap();
        assert_eq!(
            &max,
            &expected_max,
            "Max. Statistics\n\n{}\n\n",
            DisplayStats(row_groups)
        );
    }

    #[test]
    fn nan_in_stats() {
        // /parquet-testing/data/nan_in_stats.parquet
        // row_groups: 1
        // "x": Double({min: Some(1.0), max: Some(NaN), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})

        TestFile::new("nan_in_stats.parquet")
            .with_column(ExpectedColumn {
                name: "x",
                expected_min: Arc::new(Float64Array::from(vec![Some(1.0)])),
                expected_max: Arc::new(Float64Array::from(vec![Some(f64::NAN)])),
            })
            .run();
    }

    #[test]
    fn alltypes_plain() {
        // /parquet-testing/data/datapage_v1-snappy-compressed-checksum.parquet
        // row_groups: 1
        // (has no statistics)
        TestFile::new("alltypes_plain.parquet")
            // No column statistics should be read as NULL, but with the right type
            .with_column(ExpectedColumn {
                name: "id",
                expected_min: i32_array([None]),
                expected_max: i32_array([None]),
            })
            .with_column(ExpectedColumn {
                name: "bool_col",
                expected_min: bool_array([None]),
                expected_max: bool_array([None]),
            })
            .run();
    }

    #[test]
    fn alltypes_tiny_pages() {
        // /parquet-testing/data/alltypes_tiny_pages.parquet
        // row_groups: 1
        // "id": Int32({min: Some(0), max: Some(7299), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "bool_col": Boolean({min: Some(false), max: Some(true), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "tinyint_col": Int32({min: Some(0), max: Some(9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "smallint_col": Int32({min: Some(0), max: Some(9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "int_col": Int32({min: Some(0), max: Some(9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "bigint_col": Int64({min: Some(0), max: Some(90), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "float_col": Float({min: Some(0.0), max: Some(9.9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "double_col": Double({min: Some(0.0), max: Some(90.89999999999999), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "date_string_col": ByteArray({min: Some(ByteArray { data: "01/01/09" }), max: Some(ByteArray { data: "12/31/10" }), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "string_col": ByteArray({min: Some(ByteArray { data: "0" }), max: Some(ByteArray { data: "9" }), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "timestamp_col": Int96({min: None, max: None, distinct_count: None, null_count: 0, min_max_deprecated: true, min_max_backwards_compatible: true})
        // "year": Int32({min: Some(2009), max: Some(2010), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "month": Int32({min: Some(1), max: Some(12), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        TestFile::new("alltypes_tiny_pages.parquet")
            .with_column(ExpectedColumn {
                name: "id",
                expected_min: i32_array([Some(0)]),
                expected_max: i32_array([Some(7299)]),
            })
            .with_column(ExpectedColumn {
                name: "bool_col",
                expected_min: bool_array([Some(false)]),
                expected_max: bool_array([Some(true)]),
            })
            .with_column(ExpectedColumn {
                name: "tinyint_col",
                expected_min: i8_array([Some(0)]),
                expected_max: i8_array([Some(9)]),
            })
            .with_column(ExpectedColumn {
                name: "smallint_col",
                expected_min: i16_array([Some(0)]),
                expected_max: i16_array([Some(9)]),
            })
            .with_column(ExpectedColumn {
                name: "int_col",
                expected_min: i32_array([Some(0)]),
                expected_max: i32_array([Some(9)]),
            })
            .with_column(ExpectedColumn {
                name: "bigint_col",
                expected_min: i64_array([Some(0)]),
                expected_max: i64_array([Some(90)]),
            })
            .with_column(ExpectedColumn {
                name: "float_col",
                expected_min: f32_array([Some(0.0)]),
                expected_max: f32_array([Some(9.9)]),
            })
            .with_column(ExpectedColumn {
                name: "double_col",
                expected_min: f64_array([Some(0.0)]),
                expected_max: f64_array([Some(90.89999999999999)]),
            })
            .with_column(ExpectedColumn {
                name: "date_string_col",
                expected_min: utf8_array([Some("01/01/09")]),
                expected_max: utf8_array([Some("12/31/10")]),
            })
            .with_column(ExpectedColumn {
                name: "string_col",
                expected_min: utf8_array([Some("0")]),
                expected_max: utf8_array([Some("9")]),
            })
            // File has no min/max for timestamp_col
            .with_column(ExpectedColumn {
                name: "timestamp_col",
                expected_min: timestamp_array([None]),
                expected_max: timestamp_array([None]),
            })
            .with_column(ExpectedColumn {
                name: "year",
                expected_min: i32_array([Some(2009)]),
                expected_max: i32_array([Some(2010)]),
            })
            .with_column(ExpectedColumn {
                name: "month",
                expected_min: i32_array([Some(1)]),
                expected_max: i32_array([Some(12)]),
            })
            .run();
    }

    #[test]
    fn fixed_length_decimal_legacy() {
        // /parquet-testing/data/fixed_length_decimal_legacy.parquet
        // row_groups: 1
        // "value": FixedLenByteArray({min: Some(FixedLenByteArray(ByteArray { data: Some(ByteBufferPtr { data: b"\0\0\0\0\0\xc8" }) })), max: Some(FixedLenByteArray(ByteArray { data: "\0\0\0\0\t`" })), distinct_count: None, null_count: 0, min_max_deprecated: true, min_max_backwards_compatible: true})

        TestFile::new("fixed_length_decimal_legacy.parquet")
            .with_column(ExpectedColumn {
                name: "value",
                expected_min: Arc::new(
                    Decimal128Array::from(vec![Some(200)])
                        .with_precision_and_scale(13, 2)
                        .unwrap(),
                ),
                expected_max: Arc::new(
                    Decimal128Array::from(vec![Some(2400)])
                        .with_precision_and_scale(13, 2)
                        .unwrap(),
                ),
            })
            .run();
    }

    const ROWS_PER_ROW_GROUP: usize = 3;

    /// Writes the input batch into a parquet file, with every every three rows as
    /// their own row group, and compares the min/maxes to the expected values
    struct Test {
        input: ArrayRef,
        expected_min: ArrayRef,
        expected_max: ArrayRef,
    }

    impl Test {
        fn run(self) {
            let Self {
                input,
                expected_min,
                expected_max,
            } = self;

            let input_batch = RecordBatch::try_from_iter([("c1", input)]).unwrap();

            let schema = input_batch.schema();

            let metadata = parquet_metadata(schema.clone(), input_batch);
            let parquet_schema = metadata.file_metadata().schema_descr();

            let row_groups = metadata.row_groups();

            for field in schema.fields() {
                if field.data_type().is_nested() {
                    let lookup = parquet_column(parquet_schema, &schema, field.name());
                    match field.data_type() {
                        DataType::Struct(_) => {}
                        _ => {
                            assert_eq!(lookup, None);
                        }
                    }
                    continue;
                }

                let (idx, f) =
                    parquet_column(parquet_schema, &schema, field.name()).unwrap();
                assert_eq!(f, field);

                let iter = row_groups.iter().map(|x| x.column(idx).statistics());
                let min = min_statistics(f.data_type(), iter.clone()).unwrap();
                assert_eq!(
                    &min,
                    &expected_min,
                    "Min. Statistics\n\n{}\n\n",
                    DisplayStats(row_groups)
                );

                let max = max_statistics(f.data_type(), iter).unwrap();
                assert_eq!(
                    &max,
                    &expected_max,
                    "Max. Statistics\n\n{}\n\n",
                    DisplayStats(row_groups)
                );
            }
        }
    }

    /// Write the specified batches out as parquet and return the metadata
    fn parquet_metadata(schema: SchemaRef, batch: RecordBatch) -> Arc<ParquetMetaData> {
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(ROWS_PER_ROW_GROUP)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
        reader.metadata().clone()
    }

    /// Formats the statistics nicely for display
    struct DisplayStats<'a>(&'a [RowGroupMetaData]);
    impl<'a> std::fmt::Display for DisplayStats<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let row_groups = self.0;
            writeln!(f, "  row_groups: {}", row_groups.len())?;
            for rg in row_groups {
                for col in rg.columns() {
                    if let Some(statistics) = col.statistics() {
                        writeln!(f, "   {}: {:?}", col.column_path(), statistics)?;
                    }
                }
            }
            Ok(())
        }
    }

    struct ExpectedColumn {
        name: &'static str,
        expected_min: ArrayRef,
        expected_max: ArrayRef,
    }

    /// Reads statistics out of the specified, and compares them to the expected values
    struct TestFile {
        file_name: &'static str,
        expected_columns: Vec<ExpectedColumn>,
    }

    impl TestFile {
        fn new(file_name: &'static str) -> Self {
            Self {
                file_name,
                expected_columns: Vec::new(),
            }
        }

        fn with_column(mut self, column: ExpectedColumn) -> Self {
            self.expected_columns.push(column);
            self
        }

        /// Reads the specified parquet file and validates that the exepcted min/max
        /// values for the specified columns are as expected.
        fn run(self) {
            let path = PathBuf::from(parquet_test_data()).join(self.file_name);
            let file = std::fs::File::open(path).unwrap();
            let reader = ArrowReaderBuilder::try_new(file).unwrap();
            let arrow_schema = reader.schema();
            let metadata = reader.metadata();
            let row_groups = metadata.row_groups();
            let parquet_schema = metadata.file_metadata().schema_descr();

            for expected_column in self.expected_columns {
                let ExpectedColumn {
                    name,
                    expected_min,
                    expected_max,
                } = expected_column;

                let (idx, field) =
                    parquet_column(parquet_schema, arrow_schema, name).unwrap();

                let iter = row_groups.iter().map(|x| x.column(idx).statistics());
                let actual_min = min_statistics(field.data_type(), iter.clone()).unwrap();
                assert_eq!(&expected_min, &actual_min, "column {name}");

                let actual_max = max_statistics(field.data_type(), iter).unwrap();
                assert_eq!(&expected_max, &actual_max, "column {name}");
            }
        }
    }

    fn bool_array(input: impl IntoIterator<Item = Option<bool>>) -> ArrayRef {
        let array: BooleanArray = input.into_iter().collect();
        Arc::new(array)
    }

    fn i8_array(input: impl IntoIterator<Item = Option<i8>>) -> ArrayRef {
        let array: Int8Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn i16_array(input: impl IntoIterator<Item = Option<i16>>) -> ArrayRef {
        let array: Int16Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn i32_array(input: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        let array: Int32Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn i64_array(input: impl IntoIterator<Item = Option<i64>>) -> ArrayRef {
        let array: Int64Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn f32_array(input: impl IntoIterator<Item = Option<f32>>) -> ArrayRef {
        let array: Float32Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn f64_array(input: impl IntoIterator<Item = Option<f64>>) -> ArrayRef {
        let array: Float64Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn timestamp_array(input: impl IntoIterator<Item = Option<i64>>) -> ArrayRef {
        let array: TimestampNanosecondArray = input.into_iter().collect();
        Arc::new(array)
    }

    fn utf8_array<'a>(input: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
        let array: StringArray = input
            .into_iter()
            .map(|s| s.map(|s| s.to_string()))
            .collect();
        Arc::new(array)
    }

    // returns a struct array with columns "bool_col" and "int_col" with the specified values
    fn struct_array(input: Vec<(Option<bool>, Option<i32>)>) -> ArrayRef {
        let boolean: BooleanArray = input.iter().map(|(b, _i)| b).collect();
        let int: Int32Array = input.iter().map(|(_b, i)| i).collect();

        let nullable = true;
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("bool_col", DataType::Boolean, nullable)),
                Arc::new(boolean) as ArrayRef,
            ),
            (
                Arc::new(Field::new("int_col", DataType::Int32, nullable)),
                Arc::new(int) as ArrayRef,
            ),
        ]);
        Arc::new(struct_array)
    }

    fn date32_array<'a>(input: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
        let array = Date32Array::from(
            input
                .into_iter()
                .map(|s| Date32Type::parse(s.unwrap_or_default()))
                .collect::<Vec<_>>(),
        );
        Arc::new(array)
    }

    fn date64_array<'a>(input: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
        let array = Date64Array::from(
            input
                .into_iter()
                .map(|s| Date64Type::parse(s.unwrap_or_default()))
                .collect::<Vec<_>>(),
        );
        Arc::new(array)
    }
}
