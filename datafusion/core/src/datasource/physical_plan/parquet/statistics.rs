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

//! [`RowGroupStatisticsConverter`] tp converts parquet RowGroup statistics to arrow [`ArrayRef`].

use arrow::{array::ArrayRef, datatypes::DataType};
use arrow_array::new_empty_array;
use arrow_schema::Field;
use datafusion_common::{Result, ScalarValue};
use parquet::file::{
    metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics,
};

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

/// Converts parquet RowGroup statistics (stored in
/// [`RowGroupMetaData`]) into an arrow [`ArrayRef`]
///
/// For example, given a parquet file with 3 Row Groups, when asked for
/// statistics for column "A" it will return a single array with 3 elements,
///
pub(crate) struct RowGroupStatisticsConverter<'a> {
    field: &'a Field,
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
                            precision,
                            scale,
                        ))
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
                            precision,
                            scale,
                        ))
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
                            precision,
                            scale,
                        ))
                    }
                    _ => {
                        let s = std::str::from_utf8(s.$bytes_func())
                            .map(|s| s.to_string())
                            .ok();
                        Some(ScalarValue::Utf8(s))
                    }
                }
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(s) => {
                match $target_arrow_type {
                    // just support the decimal data type
                    Some(DataType::Decimal128(precision, scale)) => {
                        Some(ScalarValue::Decimal128(
                            Some(from_bytes_to_i128(s.$bytes_func())),
                            precision,
                            scale,
                        ))
                    }
                    _ => None,
                }
            }
        }
    }};
}

#[derive(Debug, Clone, Copy)]
enum MinMax {
    Min,
    Max,
}

impl<'a> RowGroupStatisticsConverter<'a> {
    /// Create a new RowGoupStatisticsConverter suitable that can extract
    /// statistics for the specified field
    pub fn new(field: &'a Field) -> Self {
        Self { field }
    }

    /// Returns the min value for the column into an array ref.
    pub fn min<'b>(
        &self,
        row_group_meta_data: impl IntoIterator<Item = &'b RowGroupMetaData>,
    ) -> Result<ArrayRef> {
        self.min_max_impl(MinMax::Min, row_group_meta_data)
    }

    /// Returns the max value for the column into an array ref.
    pub fn max<'b>(
        &self,
        row_group_meta_data: impl IntoIterator<Item = &'b RowGroupMetaData>,
    ) -> Result<ArrayRef> {
        self.min_max_impl(MinMax::Max, row_group_meta_data)
    }

    /// Extracts all min/max values for the column into an array ref.
    fn min_max_impl<'b>(
        &self,
        mm: MinMax,
        row_group_meta_data: impl IntoIterator<Item = &'b RowGroupMetaData>,
    ) -> Result<ArrayRef> {
        let mut row_group_meta_data = row_group_meta_data.into_iter().peekable();

        // if it is empty, return empty array
        if row_group_meta_data.peek().is_none() {
            return Ok(new_empty_array(self.field.data_type()));
        }

        let maybe_index = row_group_meta_data.peek().and_then(|rg_meta| {
            rg_meta
                .columns()
                .iter()
                .enumerate()
                .find(|(_idx, c)| c.column_descr().name() == self.field.name())
                .map(|(idx, _c)| idx)
        });

        // don't have this column, return an array of all NULLs
        let Some(column_index) = maybe_index else {
            let num_row_groups = row_group_meta_data.count();
            let sv = ScalarValue::try_from(self.field.data_type())?;
            return sv.to_array_of_size(num_row_groups);
        };

        let stats_iter = row_group_meta_data.map(move |row_group_meta_data| {
            row_group_meta_data.column(column_index).statistics()
        });

        // this is the value to use when the statistics are not set
        let null_value = ScalarValue::try_from(self.field.data_type())?;
        match mm {
            MinMax::Min => {
                let values = stats_iter.map(|column_statistics| {
                    column_statistics
                        .and_then(|column_statistics| {
                            get_statistic!(
                                column_statistics,
                                min,
                                min_bytes,
                                Some(self.field.data_type().clone())
                            )
                        })
                        .unwrap_or_else(|| null_value.clone())
                });
                ScalarValue::iter_to_array(values)
            }
            MinMax::Max => {
                let values = stats_iter.map(|column_statistics| {
                    column_statistics
                        .and_then(|column_statistics| {
                            get_statistic!(
                                column_statistics,
                                max,
                                max_bytes,
                                Some(self.field.data_type().clone())
                            )
                        })
                        .unwrap_or_else(|| null_value.clone())
                });
                ScalarValue::iter_to_array(values)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::{
        BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
        Int32Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use arrow_schema::SchemaRef;
    use bytes::Bytes;
    use datafusion_common::test_util::parquet_test_data;
    use parquet::arrow::arrow_reader::ArrowReaderBuilder;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::metadata::ParquetMetaData;
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
            input: Arc::new(BooleanArray::from(vec![
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
            ])),
            expected_min: Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
            ])),
            expected_max: Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(true),
                None,
            ])),
        }
        .run()
    }

    #[test]
    fn roundtrip_int32() {
        Test {
            input: Arc::new(Int32Array::from(vec![
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
            ])),
            expected_min: Arc::new(Int32Array::from(vec![Some(1), Some(0), None])),
            expected_max: Arc::new(Int32Array::from(vec![Some(3), Some(5), None])),
        }
        .run()
    }

    #[test]
    fn roundtrip_int64() {
        Test {
            input: Arc::new(Int64Array::from(vec![
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
            ])),
            expected_min: Arc::new(Int64Array::from(vec![Some(1), Some(0), None])),
            expected_max: Arc::new(Int64Array::from(vec![Some(3), Some(5), None])),
        }
        .run()
    }

    #[test]
    fn roundtrip_f32() {
        Test {
            input: Arc::new(Float32Array::from(vec![
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
            ])),
            expected_min: Arc::new(Float32Array::from(vec![Some(1.0), Some(-1.0), None])),
            expected_max: Arc::new(Float32Array::from(vec![Some(3.0), Some(5.0), None])),
        }
        .run()
    }

    #[test]
    fn roundtrip_f64() {
        Test {
            input: Arc::new(Float64Array::from(vec![
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
            ])),
            expected_min: Arc::new(Float64Array::from(vec![Some(1.0), Some(-1.0), None])),
            expected_max: Arc::new(Float64Array::from(vec![Some(3.0), Some(5.0), None])),
        }
        .run()
    }

    #[test]
    #[should_panic(
        expected = "Inconsistent types in ScalarValue::iter_to_array. Expected Int64, got TimestampNanosecond(NULL, None)"
    )]
    fn roundtrip_timestamp() {
        Test {
            input: Arc::new(TimestampNanosecondArray::from(vec![
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
            ])),
            expected_min: Arc::new(TimestampNanosecondArray::from(vec![
                Some(1),
                Some(5),
                None,
            ])),
            expected_max: Arc::new(TimestampNanosecondArray::from(vec![
                Some(3),
                Some(9),
                None,
            ])),
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
            input: Arc::new(StringArray::from(vec![
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
            ])),
            expected_min: Arc::new(StringArray::from(vec![Some("A"), Some("AA"), None])),
            expected_max: Arc::new(StringArray::from(vec![Some("Q"), Some("ZZ"), None])),
        }
        .run()
    }

    #[test]
    #[should_panic(
        expected = "Inconsistent types in ScalarValue::iter_to_array. Expected Utf8, got Binary(NULL)"
    )]
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

            for field in schema.fields() {
                let converter = RowGroupStatisticsConverter::new(field);
                let row_groups = metadata.row_groups();
                let min = converter.min(row_groups).unwrap();
                assert_eq!(
                    &min,
                    &expected_min,
                    "Min. Statistics\n\n{}\n\n",
                    DisplayStats(row_groups)
                );

                let max = converter.max(row_groups).unwrap();
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

            for rg in metadata.row_groups() {
                println!(
                    "Columns: {}",
                    rg.columns()
                        .iter()
                        .map(|c| c.column_descr().name())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            println!("  row groups: {}", metadata.row_groups().len());

            for expected_column in self.expected_columns {
                let ExpectedColumn {
                    name,
                    expected_min,
                    expected_max,
                } = expected_column;

                let field = arrow_schema
                    .field_with_name(name)
                    .expect("can't find field in schema");

                let converter = RowGroupStatisticsConverter::new(field);
                let actual_min = converter.min(metadata.row_groups()).unwrap();
                assert_eq!(&expected_min, &actual_min, "column {name}");

                let actual_max = converter.max(metadata.row_groups()).unwrap();
                assert_eq!(&expected_max, &actual_max, "column {name}");
            }
        }
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
                expected_min: Arc::new(Int32Array::from(vec![None])),
                expected_max: Arc::new(Int32Array::from(vec![None])),
            })
            .with_column(ExpectedColumn {
                name: "bool_col",
                expected_min: Arc::new(BooleanArray::from(vec![None])),
                expected_max: Arc::new(BooleanArray::from(vec![None])),
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
                expected_min: Arc::new(Int32Array::from(vec![Some(0)])),
                expected_max: Arc::new(Int32Array::from(vec![Some(7299)])),
            })
            .with_column(ExpectedColumn {
                name: "bool_col",
                expected_min: Arc::new(BooleanArray::from(vec![Some(false)])),
                expected_max: Arc::new(BooleanArray::from(vec![Some(true)])),
            })
            .with_column(ExpectedColumn {
                name: "tinyint_col",
                expected_min: Arc::new(Int32Array::from(vec![Some(0)])),
                expected_max: Arc::new(Int32Array::from(vec![Some(9)])),
            })
            .with_column(ExpectedColumn {
                name: "smallint_col",
                expected_min: Arc::new(Int32Array::from(vec![Some(0)])),
                expected_max: Arc::new(Int32Array::from(vec![Some(9)])),
            })
            .with_column(ExpectedColumn {
                name: "int_col",
                expected_min: Arc::new(Int32Array::from(vec![Some(0)])),
                expected_max: Arc::new(Int32Array::from(vec![Some(9)])),
            })
            .with_column(ExpectedColumn {
                name: "bigint_col",
                expected_min: Arc::new(Int64Array::from(vec![Some(0)])),
                expected_max: Arc::new(Int64Array::from(vec![Some(90)])),
            })
            .with_column(ExpectedColumn {
                name: "float_col",
                expected_min: Arc::new(Float32Array::from(vec![Some(0.0)])),
                expected_max: Arc::new(Float32Array::from(vec![Some(9.9)])),
            })
            .with_column(ExpectedColumn {
                name: "double_col",
                expected_min: Arc::new(Float64Array::from(vec![Some(0.0)])),
                expected_max: Arc::new(Float64Array::from(vec![Some(90.89999999999999)])),
            })
            .with_column(ExpectedColumn {
                name: "date_string_col",
                expected_min: Arc::new(StringArray::from(vec![Some("01/01/09")])),
                expected_max: Arc::new(StringArray::from(vec![Some("12/31/10")])),
            })
            .with_column(ExpectedColumn {
                name: "string_col",
                expected_min: Arc::new(StringArray::from(vec![Some("0")])),
                expected_max: Arc::new(StringArray::from(vec![Some("9")])),
            })
            // File has no min/max for timestamp_col
            .with_column(ExpectedColumn {
                name: "timestamp_col",
                expected_min: Arc::new(TimestampNanosecondArray::from(vec![None])),
                expected_max: Arc::new(TimestampNanosecondArray::from(vec![None])),
            })
            .with_column(ExpectedColumn {
                name: "year",
                expected_min: Arc::new(Int32Array::from(vec![Some(2009)])),
                expected_max: Arc::new(Int32Array::from(vec![Some(2010)])),
            })
            .with_column(ExpectedColumn {
                name: "month",
                expected_min: Arc::new(Int32Array::from(vec![Some(1)])),
                expected_max: Arc::new(Int32Array::from(vec![Some(12)])),
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
}
