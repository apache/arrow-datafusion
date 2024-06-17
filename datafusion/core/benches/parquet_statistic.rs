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

//! Benchmarks of benchmark for extracting arrow statistics from parquet

use arrow::array::{ArrayRef, DictionaryArray, Float64Array, StringArray, UInt64Array};
use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{
    DataType::{self, *},
    Field, Schema,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::physical_plan::parquet::StatisticsConverter;
use parquet::{
    arrow::arrow_reader::ArrowReaderOptions, file::properties::WriterProperties,
};
use parquet::{
    arrow::{arrow_reader::ArrowReaderBuilder, ArrowWriter},
    file::properties::EnabledStatistics,
};
use std::sync::Arc;
use tempfile::NamedTempFile;
#[derive(Debug, Clone)]
enum TestTypes {
    UInt64,
    Int64,
    F64,
    String,
    Dictionary,
}

use std::fmt;

impl fmt::Display for TestTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestTypes::UInt64 => write!(f, "UInt64"),
            TestTypes::Int64 => write!(f, "Int64"),
            TestTypes::F64 => write!(f, "F64"),
            TestTypes::String => write!(f, "String"),
            TestTypes::Dictionary => write!(f, "Dictionary(Int32, String)"),
        }
    }
}

fn create_parquet_file(
    dtype: TestTypes,
    row_groups: usize,
    data_page_row_count_limit: &Option<usize>,
) -> NamedTempFile {
    let schema = match dtype {
        TestTypes::UInt64 => {
            Arc::new(Schema::new(vec![Field::new("col", DataType::UInt64, true)]))
        }
        TestTypes::Int64 => {
            Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, true)]))
        }
        TestTypes::F64 => Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            true,
        )])),
        TestTypes::String => {
            Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]))
        }
        TestTypes::Dictionary => Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(Int32), Box::new(Utf8)),
            true,
        )])),
    };

    let mut props = WriterProperties::builder().set_max_row_group_size(row_groups);
    if let Some(limit) = data_page_row_count_limit {
        props = props
            .set_data_page_row_count_limit(*limit)
            .set_statistics_enabled(EnabledStatistics::Page);
    };
    let props = props.build();

    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema.clone(), Some(props))
            .unwrap();

    for _ in 0..row_groups {
        let batch = match dtype {
            TestTypes::UInt64 => make_uint64_batch(),
            TestTypes::Int64 => make_int64_batch(),
            TestTypes::F64 => make_f64_batch(),
            TestTypes::String => make_string_batch(),
            TestTypes::Dictionary => make_dict_batch(),
        };
        if data_page_row_count_limit.is_some() {
            // Send batches one at a time. This allows the
            // writer to apply the page limit, that is only
            // checked on RecordBatch boundaries.
            for i in 0..batch.num_rows() {
                writer.write(&batch.slice(i, 1)).unwrap();
            }
        } else {
            writer.write(&batch).unwrap();
        }
    }
    writer.close().unwrap();
    file
}

fn make_uint64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(UInt64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", UInt64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_int64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Int64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_f64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Float64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_string_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Utf8, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_dict_batch() -> RecordBatch {
    let keys = Int32Array::from(vec![0, 1, 2, 3, 4]);
    let values = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let array: ArrayRef =
        Arc::new(DictionaryArray::try_new(keys, Arc::new(values)).unwrap());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "col",
            Dictionary(Box::new(Int32), Box::new(Utf8)),
            false,
        )])),
        vec![array],
    )
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let row_groups = 100;
    use TestTypes::*;
    let types = vec![Int64, UInt64, F64, String, Dictionary];
    let data_page_row_count_limits = vec![None, Some(1)];

    for dtype in types {
        for data_page_row_count_limit in &data_page_row_count_limits {
            let file =
                create_parquet_file(dtype.clone(), row_groups, data_page_row_count_limit);
            let file = file.reopen().unwrap();
            let options = ArrowReaderOptions::new().with_page_index(true);
            let reader = ArrowReaderBuilder::try_new_with_options(file, options).unwrap();
            let metadata = reader.metadata();
            let row_groups = metadata.row_groups();
            let row_group_indices: Vec<_> = (0..row_groups.len()).collect();

            let statistic_type = if data_page_row_count_limit.is_some() {
                "data page"
            } else {
                "row group"
            };

            let mut group = c.benchmark_group(format!(
                "Extract {} statistics for {}",
                statistic_type,
                dtype.clone()
            ));
            group.bench_function(
                BenchmarkId::new("extract_statistics", dtype.clone()),
                |b| {
                    b.iter(|| {
                        let converter = StatisticsConverter::try_new(
                            "col",
                            reader.schema(),
                            reader.parquet_schema(),
                        )
                        .unwrap();

                        if data_page_row_count_limit.is_some() {
                            let column_page_index = reader
                                .metadata()
                                .column_index()
                                .expect("File should have column page indices");

                            let column_offset_index = reader
                                .metadata()
                                .offset_index()
                                .expect("File should have column offset indices");

                            let _ = converter.data_page_mins(
                                column_page_index,
                                column_offset_index,
                                &row_group_indices,
                            );
                            let _ = converter.data_page_maxes(
                                column_page_index,
                                column_offset_index,
                                &row_group_indices,
                            );
                            let _ = converter.data_page_null_counts(
                                column_page_index,
                                column_offset_index,
                                &row_group_indices,
                            );
                            let _ = converter.data_page_row_counts(
                                column_offset_index,
                                row_groups,
                                &row_group_indices,
                            );
                        } else {
                            let _ = converter.row_group_mins(row_groups.iter()).unwrap();
                            let _ = converter.row_group_maxes(row_groups.iter()).unwrap();
                            let _ = converter
                                .row_group_null_counts(row_groups.iter())
                                .unwrap();
                            let _ = StatisticsConverter::row_group_row_counts(
                                row_groups.iter(),
                            )
                            .unwrap();
                        }
                    })
                },
            );
            group.finish();
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
