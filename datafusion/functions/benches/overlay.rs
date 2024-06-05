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

use arrow::array::{Array, Int64Array, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::string;
use std::sync::Arc;

/// Create four args, three of which are Scalars and one is a StringArray.
/// The `size` represents the length of the StringArray.
fn create_4args_with_3scalars(size: usize) -> Vec<ColumnarValue> {
    let array: StringArray = std::iter::repeat(Some("Txxxxas")).take(size).collect();
    assert_eq!(array.len(), size);
    let characters = ScalarValue::Utf8(Some("hom".to_string()));
    let pos = ScalarValue::Int64(Some(2));
    let len = ScalarValue::Int64(Some(4));
    vec![
        ColumnarValue::Array(Arc::new(array)),
        ColumnarValue::Scalar(characters),
        ColumnarValue::Scalar(pos),
        ColumnarValue::Scalar(len),
    ]
}

/// Create four args, all of which are Arrays.
/// The `size` represents the length of Array.
fn create_4args_without_scalar(size: usize) -> Vec<ColumnarValue> {
    let array: StringArray = std::iter::repeat(Some("Txxxxas")).take(size).collect();
    let characters: StringArray = std::iter::repeat(Some("hom")).take(size).collect();
    let pos: Int64Array = std::iter::repeat(Some(2)).take(size).collect();
    let len: Int64Array = std::iter::repeat(Some(4)).take(size).collect();
    vec![
        ColumnarValue::Array(Arc::new(array)),
        ColumnarValue::Array(Arc::new(characters)),
        ColumnarValue::Array(Arc::new(pos)),
        ColumnarValue::Array(Arc::new(len)),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let overlay = string::overlay();
    let sizes: Vec<usize> = vec![1024, 4096, 8192];

    for size in &sizes {
        let args = create_4args_with_3scalars(*size);
        let mut group = c.benchmark_group("4args_with_3scalars");
        group.bench_function(BenchmarkId::new("overlay", size), |b| {
            b.iter(|| criterion::black_box(overlay.invoke(&args).unwrap()))
        });
        group.finish();
    }

    for size in &sizes {
        let args = create_4args_without_scalar(*size);
        let mut group = c.benchmark_group("4args_without_scalar");
        group.bench_function(BenchmarkId::new("overlay", size), |b| {
            b.iter(|| criterion::black_box(overlay.invoke(&args).unwrap()))
        });
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
