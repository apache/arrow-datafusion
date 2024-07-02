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

//! [`JsonFormat`]: Line delimited JSON [`FileFormat`] abstractions

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::io::BufReader;
use std::sync::Arc;

use super::write::orchestration::stateless_multipart_put;
use super::{FileFormat, FileFormatFactory, FileScanConfig};
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::write::BatchSerializer;
use crate::datasource::physical_plan::FileGroupDisplay;
use crate::datasource::physical_plan::{FileSinkConfig, NdJsonExec};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::insert::{DataSink, DataSinkExec};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::json::reader::{infer_json_schema_from_iterator, ValueIter};
use arrow_array::RecordBatch;
use datafusion_common::config::{ConfigField, ConfigFileType, JsonOptions};
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::{not_impl_err, GetExt, DEFAULT_JSON_EXTENSION};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortRequirement};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::ExecutionPlan;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

#[derive(Default)]
/// Factory struct used to create [JsonFormat]
pub struct JsonFormatFactory {
    options: Option<JsonOptions>,
}

impl JsonFormatFactory {
    /// Creates an instance of [JsonFormatFactory]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [JsonFormatFactory] with customized default options
    pub fn new_with_options(options: JsonOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl FileFormatFactory for JsonFormatFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let json_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::JSON);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.json
            }
            Some(json_options) => {
                let mut json_options = json_options.clone();
                for (k, v) in format_options {
                    json_options.set(k, v)?;
                }
                json_options
            }
        };

        Ok(Arc::new(JsonFormat::default().with_options(json_options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(JsonFormat::default())
    }
}

impl GetExt for JsonFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".parquet" -> "parquet"
        DEFAULT_JSON_EXTENSION[1..].to_string()
    }
}

/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug, Default)]
pub struct JsonFormat {
    options: JsonOptions,
}

impl JsonFormat {
    /// Set JSON options
    pub fn with_options(mut self, options: JsonOptions) -> Self {
        self.options = options;
        self
    }

    /// Retrieve JSON options
    pub fn options(&self) -> &JsonOptions {
        &self.options
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: usize) -> Self {
        self.options.schema_infer_max_rec = max_rec;
        self
    }

    /// Set a `FileCompressionType` of JSON
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.options.compression = file_compression_type.into();
        self
    }
}

#[async_trait]
impl FileFormat for JsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        JsonFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        Ok(format!("{}{}", ext, file_compression_type.get_ext()))
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self.options.schema_infer_max_rec;
        let file_compression_type = FileCompressionType::from(self.options.compression);
        for object in objects {
            let mut take_while = || {
                let should_take = records_to_read > 0;
                if should_take {
                    records_to_read -= 1;
                }
                should_take
            };

            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let mut reader = BufReader::new(decoder);
                    let iter = ValueIter::new(&mut reader, None);
                    infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let mut reader = BufReader::new(decoder);
                    let iter = ValueIter::new(&mut reader, None);
                    infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                }
            };

            schemas.push(schema);
            if records_to_read == 0 {
                break;
            }
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec =
            NdJsonExec::new(conf, FileCompressionType::from(self.options.compression));
        Ok(Arc::new(exec))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return not_impl_err!("Overwrites are not implemented yet for Json");
        }

        let writer_options = JsonWriterOptions::try_from(&self.options)?;

        let sink_schema = Arc::clone(conf.output_schema());
        let sink = Arc::new(JsonSink::new(conf, writer_options));

        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            sink_schema,
            order_requirements,
        )) as _)
    }
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Define a struct for serializing Json records to a stream
pub struct JsonSerializer {}

impl JsonSerializer {
    /// Constructor for the JsonSerializer object
    pub fn new() -> Self {
        Self {}
    }
}

impl BatchSerializer for JsonSerializer {
    fn serialize(&self, batch: RecordBatch, _initial: bool) -> Result<Bytes> {
        let mut buffer = Vec::with_capacity(4096);
        let mut writer = json::LineDelimitedWriter::new(&mut buffer);
        writer.write(&batch)?;
        Ok(Bytes::from(buffer))
    }
}

/// Implements [`DataSink`] for writing to a Json file.
pub struct JsonSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Writer options for underlying Json writer
    writer_options: JsonWriterOptions,
}

impl Debug for JsonSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonSink").finish()
    }
}

impl DisplayAs for JsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "JsonSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_groups).fmt_as(t, f)?;
                write!(f, ")")
            }
        }
    }
}

impl JsonSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig, writer_options: JsonWriterOptions) -> Self {
        Self {
            config,
            writer_options,
        }
    }

    /// Retrieve the inner [`FileSinkConfig`].
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn multipartput_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let get_serializer = move || Arc::new(JsonSerializer::new()) as _;

        stateless_multipart_put(
            data,
            context,
            "json".into(),
            Box::new(get_serializer),
            &self.config,
            self.writer_options.compression.into(),
        )
        .await
    }
    /// Retrieve the writer options
    pub fn writer_options(&self) -> &JsonWriterOptions {
        &self.writer_options
    }
}

#[async_trait]
impl DataSink for JsonSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let total_count = self.multipartput_all(data, context).await?;
        Ok(total_count)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use super::*;
    use crate::execution::options::NdJsonReadOptions;
    use crate::physical_plan::collect;
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;

    use arrow::util::pretty;
    use datafusion_common::cast::as_int64_array;
    use datafusion_common::stats::Precision;
    use datafusion_common::{assert_batches_eq, internal_err};

    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use regex::Regex;
    use rstest::rstest;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(4, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 6 /* 12/2 */);

        // test metadata
        assert_eq!(exec.statistics()?.num_rows, Precision::Absent);
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let projection = None;
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let exec = get_exec(&state, projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean", "d: Utf8",], x);

        Ok(())
    }

    #[tokio::test]
    async fn read_int_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, projection, None).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(12, batches[0].num_rows());

        let array = as_int64_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            vec![1, -10, 2, 1, 7, 1, 1, 5, 1, 1, 1, 100000000000000],
            values
        );

        Ok(())
    }

    async fn get_exec(
        state: &SessionState,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = "tests/data/2.json";
        let format = JsonFormat::default();
        scan_format(state, &format, ".", filename, projection, limit).await
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let session = SessionContext::new();
        let ctx = session.state();
        let store = Arc::new(LocalFileSystem::new()) as _;
        let filename = "tests/data/schema_infer_limit.json";
        let format = JsonFormat::default().with_schema_infer_max_rec(3);

        let file_schema = format
            .infer_schema(&ctx, &store, &[local_unpartitioned_file(filename)])
            .await
            .expect("Schema inference");

        let fields = file_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect::<Vec<_>>();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean"], fields);
    }

    async fn count_num_partitions(ctx: &SessionContext, query: &str) -> Result<usize> {
        let result = ctx
            .sql(&format!("EXPLAIN {query}"))
            .await?
            .collect()
            .await?;

        let plan = format!("{}", &pretty::pretty_format_batches(&result)?);

        let re = Regex::new(r"file_groups=\{(\d+) group").unwrap();

        if let Some(captures) = re.captures(&plan) {
            if let Some(match_) = captures.get(1) {
                let count = match_.as_str().parse::<usize>().unwrap();
                return Ok(count);
            }
        }

        internal_err!("Query contains no Exec: file_groups")
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn it_can_read_ndjson_in_parallel(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);

        let ctx = SessionContext::new_with_config(config);

        let table_path = "tests/data/1.json";
        let options = NdJsonReadOptions::default();

        ctx.register_json("json_parallel", table_path, options)
            .await?;

        let query = "SELECT sum(a) FROM json_parallel;";

        let result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_num_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = [
            "+----------------------+",
            "| sum(json_parallel.a) |",
            "+----------------------+",
            "| -7                   |",
            "+----------------------+"
        ];

        assert_batches_eq!(expected, &result);
        assert_eq!(n_partitions, actual_partitions);

        Ok(())
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn it_can_read_empty_ndjson_in_parallel(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);

        let ctx = SessionContext::new_with_config(config);

        let table_path = "tests/data/empty.json";
        let options = NdJsonReadOptions::default();

        ctx.register_json("json_parallel_empty", table_path, options)
            .await?;

        let query = "SELECT * FROM json_parallel_empty WHERE random() > 0.5;";

        let result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_num_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = [
            "++",
            "++",
        ];

        assert_batches_eq!(expected, &result);
        assert_eq!(1, actual_partitions);

        Ok(())
    }
}
