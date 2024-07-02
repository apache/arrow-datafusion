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

use arrow::array::ArrayRef;
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::string::concat;
use std::sync::Arc;

fn create_args(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    let scalar = ScalarValue::Utf8(Some(", ".to_string()));
    vec![
        ColumnarValue::Array(Arc::clone(&array) as ArrayRef),
        ColumnarValue::Scalar(scalar),
        ColumnarValue::Array(array),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096, 8192] {
        let args = create_args(size, 32);
        let mut group = c.benchmark_group("concat function");
        group.bench_function(BenchmarkId::new("concat", size), |b| {
            b.iter(|| criterion::black_box(concat().invoke(&args).unwrap()))
        });
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
