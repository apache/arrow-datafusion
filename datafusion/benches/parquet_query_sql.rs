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

//! Benchmarks of SQL queries again parquet data

use arrow::array::{
    ArrayRef, MutableArray, MutableDictionaryArray, MutableUtf8Array, PrimitiveArray,
    TryExtend, Utf8Array,
};
use arrow::datatypes::{DataType, Field, IntegerType, Schema, SchemaRef};

use arrow::io::parquet::write::RowGroupIterator;
use arrow::types::NativeType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::ExecutionContext;
use datafusion_common::field_util::SchemaExt;
use datafusion_common::record_batch::RecordBatch;
use parquet::compression::{Compression, CompressionOptions};
use parquet::encoding::Encoding;
use parquet::write::Version;
use rand::distributions::uniform::SampleUniform;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio_stream::StreamExt;

/// The number of batches to write
const NUM_BATCHES: usize = 2048;
/// The number of rows in each record batch to write
const WRITE_RECORD_BATCH_SIZE: usize = 1024;
/// The number of row groups expected
const EXPECTED_ROW_GROUPS: usize = 2;

fn schema() -> SchemaRef {
    let string_dictionary_type =
        DataType::Dictionary(IntegerType::Int32, Box::new(DataType::Utf8), false);

    Arc::new(Schema::new(vec![
        Field::new("dict_10_required", string_dictionary_type.clone(), false),
        Field::new("dict_10_optional", string_dictionary_type.clone(), true),
        Field::new("dict_100_required", string_dictionary_type.clone(), false),
        Field::new("dict_100_optional", string_dictionary_type.clone(), true),
        Field::new("dict_1000_required", string_dictionary_type.clone(), false),
        Field::new("dict_1000_optional", string_dictionary_type, true),
        Field::new("string_required", DataType::Utf8, false),
        Field::new("string_optional", DataType::Utf8, true),
        Field::new("i64_required", DataType::Int64, false),
        Field::new("i64_optional", DataType::Int64, true),
        Field::new("f64_required", DataType::Float64, false),
        Field::new("f64_optional", DataType::Float64, true),
    ]))
}

fn generate_batch() -> RecordBatch {
    let schema = schema();
    let len = WRITE_RECORD_BATCH_SIZE;
    RecordBatch::try_new(
        schema,
        vec![
            generate_string_dictionary("prefix", 10, len, 1.0),
            generate_string_dictionary("prefix", 10, len, 0.5),
            generate_string_dictionary("prefix", 100, len, 1.0),
            generate_string_dictionary("prefix", 100, len, 0.5),
            generate_string_dictionary("prefix", 1000, len, 1.0),
            generate_string_dictionary("prefix", 1000, len, 0.5),
            generate_strings(0..100, len, 1.0),
            generate_strings(0..100, len, 0.5),
            generate_primitive::<i64>(len, 1.0, -2000..2000),
            generate_primitive::<i64>(len, 0.5, -2000..2000),
            generate_primitive::<f64>(len, 1.0, -1000.0..1000.0),
            generate_primitive::<f64>(len, 0.5, -1000.0..1000.0),
        ],
    )
    .unwrap()
}

fn generate_string_dictionary(
    prefix: &str,
    cardinality: usize,
    len: usize,
    valid_percent: f64,
) -> ArrayRef {
    let mut rng = thread_rng();
    let strings: Vec<_> = (0..cardinality)
        .map(|x| format!("{}#{}", prefix, x))
        .collect();
    let mut dict = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    dict.try_extend((0..len).map(|_| {
        rng.gen_bool(valid_percent)
            .then(|| strings[rng.gen_range(0..cardinality)].as_str())
    }))
    .unwrap();
    dict.as_arc()
}

fn generate_strings(
    string_length_range: Range<usize>,
    len: usize,
    valid_percent: f64,
) -> ArrayRef {
    let mut rng = thread_rng();
    Arc::new(Utf8Array::<i32>::from_iter((0..len).map(|_| {
        rng.gen_bool(valid_percent).then(|| {
            let string_len = rng.gen_range(string_length_range.clone());
            (0..string_len)
                .map(|_| char::from(rng.sample(Alphanumeric)))
                .collect::<String>()
        })
    })))
}

fn generate_primitive<T>(len: usize, valid_percent: f64, range: Range<T>) -> ArrayRef
where
    T: NativeType + SampleUniform + PartialOrd,
{
    let mut rng = thread_rng();
    Arc::new(PrimitiveArray::<T>::from_iter((0..len).map(|_| {
        rng.gen_bool(valid_percent)
            .then(|| rng.gen_range(range.clone()))
    })))
}

fn generate_file() -> NamedTempFile {
    let now = Instant::now();
    let named_file = tempfile::Builder::new()
        .prefix("parquet_query_sql")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    println!("Generating parquet file - {}", named_file.path().display());
    let schema = schema();

    let options = arrow::io::parquet::write::WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
    };

    let file = named_file.as_file().try_clone().unwrap();
    let mut writer = arrow::io::parquet::write::FileWriter::try_new(
        file,
        schema.as_ref().clone(),
        options,
    )
    .unwrap();

    for _ in 0..NUM_BATCHES {
        let batch = generate_batch();
        let iter = vec![Ok(batch.into())];
        let row_groups = RowGroupIterator::try_new(
            iter.into_iter(),
            schema.as_ref(),
            options,
            vec![vec![Encoding::Plain]].repeat(schema.fields().len()),
        )
        .unwrap();
        for rg in row_groups {
            writer.write(rg.unwrap()).unwrap();
        }
    }
    let (_total_size, mut w) = writer.end(None).unwrap();
    let metadata = arrow::io::parquet::read::read_metadata(&mut w).unwrap();
    assert_eq!(
        metadata.num_rows as usize,
        WRITE_RECORD_BATCH_SIZE * NUM_BATCHES
    );
    assert_eq!(metadata.row_groups.len(), EXPECTED_ROW_GROUPS);

    println!(
        "Generated parquet file in {} seconds",
        now.elapsed().as_secs_f32()
    );

    named_file
}

fn criterion_benchmark(c: &mut Criterion) {
    let (file_path, temp_file) = match std::env::var("PARQUET_FILE") {
        Ok(file) => (file, None),
        Err(_) => {
            let temp_file = generate_file();
            (temp_file.path().display().to_string(), Some(temp_file))
        }
    };

    assert!(Path::new(&file_path).exists(), "path not found");
    println!("Using parquet file {}", file_path);

    let mut context = ExecutionContext::new();

    let rt = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    rt.block_on(context.register_parquet("t", file_path.as_str()))
        .unwrap();

    // We read the queries from a file so they can be changed without recompiling the benchmark
    let mut queries_file = File::open("benches/parquet_query_sql.sql").unwrap();
    let mut queries = String::new();
    queries_file.read_to_string(&mut queries).unwrap();

    for query in queries.split(';') {
        let query = query.trim();

        // Remove comment lines
        let query: Vec<_> = query.split('\n').filter(|x| !x.starts_with("--")).collect();
        let query = query.join(" ");

        // Ignore blank lines
        if query.is_empty() {
            continue;
        }

        let query = query.as_str();
        c.bench_function(query, |b| {
            b.iter(|| {
                let mut context = context.clone();
                rt.block_on(async move {
                    let query = context.sql(query).await.unwrap();
                    let mut stream = query.execute_stream().await.unwrap();
                    while criterion::black_box(stream.next().await).is_some() {}
                })
            });
        });
    }

    // Temporary file must outlive the benchmarks, it is deleted when dropped
    std::mem::drop(temp_file);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
