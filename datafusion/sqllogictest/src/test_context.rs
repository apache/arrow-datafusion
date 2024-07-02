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
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Float64Array, Int32Array, LargeBinaryArray, LargeStringArray,
    StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{create_udf, ColumnarValue, Expr, ScalarUDF, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::{
    catalog::{schema::MemorySchemaProvider, CatalogProvider, MemoryCatalogProvider},
    datasource::{MemTable, TableProvider, TableType},
    prelude::{CsvReadOptions, SessionContext},
};
use datafusion_common::cast::as_float64_array;
use datafusion_common::DataFusionError;

use async_trait::async_trait;
use log::info;
use tempfile::TempDir;

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
    /// Temporary directory created and cleared at the end of the test
    test_dir: Option<TempDir>,
}

impl TestContext {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            test_dir: None,
        }
    }

    /// Create a SessionContext, configured for the specific sqllogictest
    /// test(.slt file) , if possible.
    ///
    /// If `None` is returned (e.g. because some needed feature is not
    /// enabled), the file should be skipped
    pub async fn try_new_for_test_file(relative_path: &Path) -> Option<Self> {
        let config = SessionConfig::new()
            // hardcode target partitions so plans are deterministic
            .with_target_partitions(4);

        let mut test_ctx = TestContext::new(SessionContext::new_with_config(config));

        let file_name = relative_path.file_name().unwrap().to_str().unwrap();
        match file_name {
            "information_schema_table_types.slt" => {
                info!("Registering local temporary table");
                register_temp_table(test_ctx.session_ctx()).await;
            }
            "information_schema_columns.slt" => {
                info!("Registering table with many types");
                register_table_with_many_types(test_ctx.session_ctx()).await;
            }
            "map.slt" => {
                info!("Registering table with map");
                register_table_with_map(test_ctx.session_ctx()).await;
            }
            "avro.slt" => {
                #[cfg(feature = "avro")]
                {
                    info!("Registering avro tables");
                    register_avro_tables(&mut test_ctx).await;
                    test_ctx.ctx.enable_url_table().ok();
                }
                #[cfg(not(feature = "avro"))]
                {
                    info!("Skipping {file_name} because avro feature is not enabled");
                    return None;
                }
            }
            "describe.slt" | "arrow_files.slt" | "csv_files.slt" | "json.slt"
            | "parquet.slt" => {
                test_ctx.ctx.enable_url_table().ok();
            }
            "joins.slt" => {
                info!("Registering partition table tables");
                let example_udf = create_example_udf();
                test_ctx.ctx.register_udf(example_udf);
                register_partition_table(&mut test_ctx).await;
            }
            "metadata.slt" => {
                info!("Registering metadata table tables");
                register_metadata_tables(test_ctx.session_ctx()).await;
            }
            _ => {
                info!("Using default SessionContext");
            }
        };
        Some(test_ctx)
    }

    /// Enables the test directory feature. If not enabled,
    /// calling `testdir_path` will result in a panic.
    pub fn enable_testdir(&mut self) {
        if self.test_dir.is_none() {
            self.test_dir = Some(TempDir::new().expect("failed to create testdir"));
        }
    }

    /// Returns the path to the test directory. Panics if the test
    /// directory feature is not enabled via `enable_testdir`.
    pub fn testdir_path(&self) -> &Path {
        self.test_dir.as_ref().expect("testdir not enabled").path()
    }

    /// Returns a reference to the internal SessionContext
    pub fn session_ctx(&self) -> &SessionContext {
        &self.ctx
    }
}

#[cfg(feature = "avro")]
pub async fn register_avro_tables(ctx: &mut crate::TestContext) {
    use datafusion::prelude::AvroReadOptions;

    ctx.enable_testdir();

    let table_path = ctx.testdir_path().join("avro");
    std::fs::create_dir(&table_path).expect("failed to create avro table path");

    let testdata = datafusion::test_util::arrow_test_data();
    let alltypes_plain_file = format!("{testdata}/avro/alltypes_plain.avro");
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain1.avro", table_path.display()),
    )
    .unwrap();
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain2.avro", table_path.display()),
    )
    .unwrap();

    ctx.session_ctx()
        .register_avro(
            "alltypes_plain_multi_files",
            table_path.display().to_string().as_str(),
            AvroReadOptions::default(),
        )
        .await
        .unwrap();
}

/// Generate a partitioned CSV file and register it with an execution context
pub async fn register_partition_table(test_ctx: &mut TestContext) {
    test_ctx.enable_testdir();
    let partition_count = 1;
    let file_extension = "csv";
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));
    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = test_ctx.testdir_path().join(filename);
        let mut file = File::create(file_path).unwrap();

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes()).unwrap()
        }
    }

    // register csv file with the execution context
    test_ctx
        .ctx
        .register_csv(
            "test_partition_table",
            test_ctx.testdir_path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await
        .unwrap();
}

// registers a LOCAL TEMPORARY table.
pub async fn register_temp_table(ctx: &SessionContext) {
    struct TestTable(TableType);

    #[async_trait]
    impl TableProvider for TestTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn table_type(&self) -> TableType {
            self.0
        }

        fn schema(&self) -> SchemaRef {
            unimplemented!()
        }

        async fn scan(
            &self,
            _state: &SessionState,
            _: Option<&Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            unimplemented!()
        }
    }

    ctx.register_table(
        "datafusion.public.temp",
        Arc::new(TestTable(TableType::Temporary)),
    )
    .unwrap();
}

pub async fn register_table_with_many_types(ctx: &SessionContext) {
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    catalog
        .register_schema("my_schema", Arc::new(schema))
        .unwrap();
    ctx.register_catalog("my_catalog", Arc::new(catalog));

    ctx.register_table("my_catalog.my_schema.t2", table_with_many_types())
        .unwrap();
}

pub async fn register_table_with_map(ctx: &SessionContext) {
    let key = Field::new("key", DataType::Int64, false);
    let value = Field::new("value", DataType::Int64, true);
    let map_field =
        Field::new("entries", DataType::Struct(vec![key, value].into()), false);
    let fields = vec![
        Field::new("int_field", DataType::Int64, true),
        Field::new("map_field", DataType::Map(map_field.into(), false), true),
    ];
    let schema = Schema::new(fields);

    let memory_table = MemTable::try_new(schema.into(), vec![vec![]]).unwrap();

    ctx.register_table("table_with_map", Arc::new(memory_table))
        .unwrap();
}

fn table_with_many_types() -> Arc<dyn TableProvider> {
    let schema = Schema::new(vec![
        Field::new("int32_col", DataType::Int32, false),
        Field::new("float64_col", DataType::Float64, true),
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("large_utf8_col", DataType::LargeUtf8, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("large_binary_col", DataType::LargeBinary, false),
        Field::new(
            "timestamp_nanos",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec![Some("foo")])),
            Arc::new(LargeStringArray::from(vec![Some("bar")])),
            Arc::new(BinaryArray::from(vec![b"foo" as &[u8]])),
            Arc::new(LargeBinaryArray::from(vec![b"foo" as &[u8]])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(123)])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

/// Registers a table_with_metadata that contains both field level and Table level metadata
pub async fn register_metadata_tables(ctx: &SessionContext) {
    let id = Field::new("id", DataType::Int32, true).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the id field"),
    )]));
    let name = Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the name field"),
    )]));

    let schema = Schema::new(vec![id, name]).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the entire schema"),
    )]));

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as _,
            Arc::new(StringArray::from(vec![None, Some("bar"), Some("baz")])) as _,
        ],
    )
    .unwrap();

    ctx.register_batch("table_with_metadata", batch).unwrap();
}

/// Create a UDF function named "example". See the `sample_udf.rs` example
/// file for an explanation of the API.
fn create_example_udf() -> ScalarUDF {
    let adder = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(lhs) = &args[0] else {
            panic!("should be array")
        };
        let ColumnarValue::Array(rhs) = &args[1] else {
            panic!("should be array")
        };

        let lhs = as_float64_array(lhs).expect("cast failed");
        let rhs = as_float64_array(rhs).expect("cast failed");
        let array = lhs
            .iter()
            .zip(rhs.iter())
            .map(|(lhs, rhs)| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => Some(lhs + rhs),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });
    create_udf(
        "example",
        // Expects two f64 values:
        vec![DataType::Float64, DataType::Float64],
        // Returns an f64 value:
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        adder,
    )
}
