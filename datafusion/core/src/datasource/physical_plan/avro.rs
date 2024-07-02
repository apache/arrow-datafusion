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

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::sync::Arc;

use super::FileScanConfig;
use crate::error::Result;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::SchemaRef;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AvroExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl AvroExec {
    /// Create a new Avro reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();
        let cache = Self::compute_properties(
            Arc::clone(&projected_schema),
            &projected_output_ordering,
            &base_config,
        );
        Self {
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }
    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);
        let n_partitions = file_scan_config.file_groups.len();

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(n_partitions), // Output Partitioning
            ExecutionMode::Bounded,                          // Execution Mode
        )
    }
}

impl DisplayAs for AvroExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "AvroExec: ")?;
        self.base_config.fmt_as(t, f)
    }
}

impl ExecutionPlan for AvroExec {
    fn name(&self) -> &'static str {
        "AvroExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    #[cfg(not(feature = "avro"))]
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(crate::error::DataFusionError::NotImplemented(
            "Cannot execute avro plan without avro feature enabled".to_string(),
        ))
    }

    #[cfg(feature = "avro")]
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use super::file_stream::FileStream;
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let config = Arc::new(private::AvroConfig {
            schema: Arc::clone(&self.base_config.file_schema),
            batch_size: context.session_config().batch_size(),
            projection: self.base_config.projected_file_column_names(),
            object_store,
        });
        let opener = private::AvroOpener { config };

        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(feature = "avro")]
mod private {
    use super::*;
    use crate::datasource::avro_to_arrow::Reader as AvroReader;
    use crate::datasource::physical_plan::file_stream::{FileOpenFuture, FileOpener};
    use crate::datasource::physical_plan::FileMeta;

    use bytes::Buf;
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct AvroConfig {
        pub schema: SchemaRef,
        pub batch_size: usize,
        pub projection: Option<Vec<String>>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl AvroConfig {
        fn open<R: std::io::Read>(&self, reader: R) -> Result<AvroReader<'static, R>> {
            AvroReader::try_new(
                reader,
                Arc::clone(&self.schema),
                self.batch_size,
                self.projection.clone(),
            )
        }
    }

    pub struct AvroOpener {
        pub config: Arc<AvroConfig>,
    }

    impl FileOpener for AvroOpener {
        fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
            let config = Arc::clone(&self.config);
            Ok(Box::pin(async move {
                let r = config.object_store.get(file_meta.location()).await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = config.open(file)?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(bytes.reader())?;
                        Ok(futures::stream::iter(reader).boxed())
                    }
                }
            }))
        }
    }
}

#[cfg(test)]
#[cfg(feature = "avro")]
mod tests {
    use super::*;
    use crate::arrow::datatypes::{DataType, Field, SchemaBuilder};
    use crate::datasource::file_format::{avro::AvroFormat, FileFormat};
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::prelude::SessionContext;
    use crate::scalar::ScalarValue;
    use crate::test::object_store::local_unpartitioned_file;

    use futures::StreamExt;
    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStore;
    use rstest::*;
    use url::Url;

    #[tokio::test]
    async fn avro_exec_without_partition() -> Result<()> {
        test_with_stores(Arc::new(LocalFileSystem::new())).await
    }

    #[rstest]
    #[tokio::test]
    async fn test_chunked_avro(
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_with_stores(Arc::new(ChunkedStore::new(
            Arc::new(LocalFileSystem::new()),
            chunk_size,
        )))
        .await
    }

    async fn test_with_stores(store: Arc<dyn ObjectStore>) -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let url = Url::parse("file://").unwrap();
        session_ctx.register_object_store(&url, store.clone());

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let meta = local_unpartitioned_file(filename);

        let file_schema = AvroFormat {}
            .infer_schema(&state, &store, &[meta.clone()])
            .await?;

        let avro_exec = AvroExec::new(
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
                .with_file(meta.into())
                .with_projection(Some(vec![0, 1, 2])),
        );
        assert_eq!(
            avro_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        let mut results = avro_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = [
            "+----+----------+-------------+",
            "| id | bool_col | tinyint_col |",
            "+----+----------+-------------+",
            "| 4  | true     | 0           |",
            "| 5  | false    | 1           |",
            "| 6  | true     | 0           |",
            "| 7  | false    | 1           |",
            "| 2  | true     | 0           |",
            "| 3  | false    | 1           |",
            "| 0  | true     | 0           |",
            "| 1  | false    | 1           |",
            "+----+----------+-------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn avro_exec_missing_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let actual_schema = AvroFormat {}
            .infer_schema(&state, &object_store, &[meta.clone()])
            .await?;

        let mut builder = SchemaBuilder::from(actual_schema.fields());
        builder.push(Field::new("missing_col", DataType::Int32, true));

        let file_schema = Arc::new(builder.finish());
        // Include the missing column in the projection
        let projection = Some(vec![0, 1, 2, actual_schema.fields().len()]);

        let avro_exec = AvroExec::new(
            FileScanConfig::new(object_store_url, file_schema)
                .with_file(meta.into())
                .with_projection(projection),
        );
        assert_eq!(
            avro_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );

        let mut results = avro_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = [
            "+----+----------+-------------+-------------+",
            "| id | bool_col | tinyint_col | missing_col |",
            "+----+----------+-------------+-------------+",
            "| 4  | true     | 0           |             |",
            "| 5  | false    | 1           |             |",
            "| 6  | true     | 0           |             |",
            "| 7  | false    | 1           |             |",
            "| 2  | true     | 0           |             |",
            "| 3  | false    | 1           |             |",
            "| 0  | true     | 0           |             |",
            "| 1  | false    | 1           |             |",
            "+----+----------+-------------+-------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn avro_exec_with_partition() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/alltypes_plain.avro");
        let object_store = Arc::new(LocalFileSystem::new()) as _;
        let object_store_url = ObjectStoreUrl::local_filesystem();
        let meta = local_unpartitioned_file(filename);
        let file_schema = AvroFormat {}
            .infer_schema(&state, &object_store, &[meta.clone()])
            .await?;

        let mut partitioned_file = PartitionedFile::from(meta);
        partitioned_file.partition_values = vec![ScalarValue::from("2021-10-26")];

        let projection = Some(vec![0, 1, file_schema.fields().len(), 2]);
        let avro_exec = AvroExec::new(
            FileScanConfig::new(object_store_url, file_schema)
                // select specific columns of the files as well as the partitioning
                // column which is supposed to be the last column in the table schema.
                .with_projection(projection)
                .with_file(partitioned_file)
                .with_table_partition_cols(vec![Field::new(
                    "date",
                    DataType::Utf8,
                    false,
                )]),
        );
        assert_eq!(
            avro_exec
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );

        let mut results = avro_exec
            .execute(0, state.task_ctx())
            .expect("plan execution failed");

        let batch = results
            .next()
            .await
            .expect("plan iterator empty")
            .expect("plan iterator returned an error");

        let expected = [
            "+----+----------+------------+-------------+",
            "| id | bool_col | date       | tinyint_col |",
            "+----+----------+------------+-------------+",
            "| 4  | true     | 2021-10-26 | 0           |",
            "| 5  | false    | 2021-10-26 | 1           |",
            "| 6  | true     | 2021-10-26 | 0           |",
            "| 7  | false    | 2021-10-26 | 1           |",
            "| 2  | true     | 2021-10-26 | 0           |",
            "| 3  | false    | 2021-10-26 | 1           |",
            "| 0  | true     | 2021-10-26 | 0           |",
            "| 1  | false    | 2021-10-26 | 1           |",
            "+----+----------+------------+-------------+",
        ];
        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }
}
