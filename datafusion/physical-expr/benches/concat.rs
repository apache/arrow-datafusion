use arrow::util::bench_util::create_string_array_with_len;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::string_expressions::{concat, concat2};
use std::sync::Arc;

fn create_args(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    let scalar = ScalarValue::Utf8(Some(", ".to_string()));
    vec![
        ColumnarValue::Array(array.clone()),
        ColumnarValue::Scalar(scalar),
        ColumnarValue::Array(array),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    for size in [1024, 4096, 8192] {
        let args = create_args(1024, 32);
        let mut group = c.benchmark_group("concat function");
        group.bench_function(BenchmarkId::new("concat(old)", size), |b| {
            b.iter(|| criterion::black_box(concat(&args).unwrap()))
        });
        group.bench_function(BenchmarkId::new("concat(new)", size), |b| {
            b.iter(|| criterion::black_box(concat2(&args).unwrap()))
        });
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
