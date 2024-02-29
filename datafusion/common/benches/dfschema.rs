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

use arrow_schema::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::{DFField, DFSchema};
use std::collections::HashMap;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("index_of_column_by_name 1000", |b| {
        b.iter(generate_bench_index_of_column_by_name_test_fn(1000))
    });
    c.bench_function("index_of_column_by_name 500", |b| {
        b.iter(generate_bench_index_of_column_by_name_test_fn(500))
    });
    c.bench_function("index_of_column_by_name 100", |b| {
        b.iter(generate_bench_index_of_column_by_name_test_fn(100))
    });
    c.bench_function("index_of_column_by_name 50", |b| {
        b.iter(generate_bench_index_of_column_by_name_test_fn(50))
    });
    c.bench_function("index_of_column_by_name 20", |b| {
        b.iter(generate_bench_index_of_column_by_name_test_fn(20))
    });
    c.bench_function("index_of_column_by_name 10", |b| {
        b.iter(generate_bench_index_of_column_by_name_test_fn(10))
    });
}

fn generate_bench_index_of_column_by_name_test_fn(n: usize) -> impl Fn() {
    let schema = generate_schema(n);
    move || {
        let middle_field_index = schema.fields().len() / 2;
        let middle_field_name = schema.fields()[middle_field_index].name();

        let result = schema
            .index_of_column_by_name(None, middle_field_name)
            .unwrap()
            .unwrap();

        assert_eq!(result, middle_field_index)
    }
}

fn generate_schema(n: usize) -> DFSchema {
    let fields: Vec<DFField> = (0..n)
        .map(|idx| format!("field{}", idx))
        .map(|field_name| DFField::new_unqualified(&field_name, DataType::Int8, true))
        .collect();
    DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
