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

//! TableProvider for stream sources, such as FIFO files

use std::any::Any;
use std::fmt::Formatter;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::datasource::provider::TableProviderFactory;
use crate::datasource::{create_ordering, TableProvider};
use crate::execution::context::SessionState;

use arrow_array::{RecordBatch, RecordBatchReader, RecordBatchWriter};
use arrow_schema::SchemaRef;
use datafusion_common::{config_err, plan_err, Constraints, DataFusionError, Result};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{CreateExternalTable, Expr, TableType};
use datafusion_physical_plan::insert::{DataSink, DataSinkExec};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};

use async_trait::async_trait;
use futures::StreamExt;

/// A [`TableProviderFactory`] for [`StreamTable`]
#[derive(Debug, Default)]
pub struct StreamTableFactory {}

#[async_trait]
impl TableProviderFactory for StreamTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());
        let location = cmd.location.clone();
        let encoding = cmd.file_type.parse()?;
        let header = if let Ok(opt) = cmd
            .options
            .get("format.has_header")
            .map(|has_header| bool::from_str(has_header))
            .transpose()
        {
            opt.unwrap_or(false)
        } else {
            return config_err!(
                "Valid values for format.has_header option are 'true' or 'false'"
            );
        };

        let source = FileStreamProvider::new_file(schema, location.into())
            .with_encoding(encoding)
            .with_batch_size(state.config().batch_size())
            .with_header(header);

        let config = StreamConfig::new(Arc::new(source))
            .with_order(cmd.order_exprs.clone())
            .with_constraints(cmd.constraints.clone());

        Ok(Arc::new(StreamTable(Arc::new(config))))
    }
}

/// The data encoding for [`StreamTable`]
#[derive(Debug, Clone)]
pub enum StreamEncoding {
    /// CSV records
    Csv,
    /// Newline-delimited JSON records
    Json,
}

impl FromStr for StreamEncoding {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            _ => plan_err!("Unrecognised StreamEncoding {}", s),
        }
    }
}

/// The StreamProvider trait is used as a generic interface for reading and writing from streaming
/// data sources (such as FIFO, Websocket, Kafka, etc.).  Implementations of the provider are
/// responsible for providing a `RecordBatchReader` and optionally a `RecordBatchWriter`.
pub trait StreamProvider: std::fmt::Debug + Send + Sync {
    /// Get a reference to the schema for this stream
    fn schema(&self) -> &SchemaRef;
    /// Provide `RecordBatchReader`
    fn reader(&self) -> Result<Box<dyn RecordBatchReader>>;
    /// Provide `RecordBatchWriter`
    fn writer(&self) -> Result<Box<dyn RecordBatchWriter>> {
        unimplemented!()
    }
    /// Display implementation when using as a DataSink
    fn stream_write_display(
        &self,
        t: DisplayFormatType,
        f: &mut Formatter,
    ) -> std::fmt::Result;
}

/// Stream data from the file at `location`
///
/// * Data will be read sequentially from the provided `location`
/// * New data will be appended to the end of the file
///
/// The encoding can be configured with [`Self::with_encoding`] and
/// defaults to [`StreamEncoding::Csv`]
#[derive(Debug)]
pub struct FileStreamProvider {
    location: PathBuf,
    encoding: StreamEncoding,
    /// Get a reference to the schema for this file stream
    pub schema: SchemaRef,
    header: bool,
    batch_size: usize,
}

impl FileStreamProvider {
    /// Stream data from the file at `location`
    ///
    /// * Data will be read sequentially from the provided `location`
    /// * New data will be appended to the end of the file
    ///
    /// The encoding can be configured with [`Self::with_encoding`] and
    /// defaults to [`StreamEncoding::Csv`]
    pub fn new_file(schema: SchemaRef, location: PathBuf) -> Self {
        Self {
            schema,
            location,
            batch_size: 1024,
            encoding: StreamEncoding::Csv,
            header: false,
        }
    }

    /// Set the batch size (the number of rows to load at one time)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Specify whether the file has a header (only applicable for [`StreamEncoding::Csv`])
    pub fn with_header(mut self, header: bool) -> Self {
        self.header = header;
        self
    }

    /// Specify an encoding for the stream
    pub fn with_encoding(mut self, encoding: StreamEncoding) -> Self {
        self.encoding = encoding;
        self
    }
}

impl StreamProvider for FileStreamProvider {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn reader(&self) -> Result<Box<dyn RecordBatchReader>> {
        let file = File::open(&self.location)?;
        let schema = Arc::clone(&self.schema);
        match &self.encoding {
            StreamEncoding::Csv => {
                let reader = arrow::csv::ReaderBuilder::new(schema)
                    .with_header(self.header)
                    .with_batch_size(self.batch_size)
                    .build(file)?;

                Ok(Box::new(reader))
            }
            StreamEncoding::Json => {
                let reader = arrow::json::ReaderBuilder::new(schema)
                    .with_batch_size(self.batch_size)
                    .build(BufReader::new(file))?;

                Ok(Box::new(reader))
            }
        }
    }

    fn writer(&self) -> Result<Box<dyn RecordBatchWriter>> {
        match &self.encoding {
            StreamEncoding::Csv => {
                let header = self.header && !self.location.exists();
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.location)?;
                let writer = arrow::csv::WriterBuilder::new()
                    .with_header(header)
                    .build(file);

                Ok(Box::new(writer))
            }
            StreamEncoding::Json => {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.location)?;
                Ok(Box::new(arrow::json::LineDelimitedWriter::new(file)))
            }
        }
    }

    fn stream_write_display(
        &self,
        _t: DisplayFormatType,
        f: &mut Formatter,
    ) -> std::fmt::Result {
        f.debug_struct("StreamWrite")
            .field("location", &self.location)
            .field("batch_size", &self.batch_size)
            .field("encoding", &self.encoding)
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

/// The configuration for a [`StreamTable`]
#[derive(Debug)]
pub struct StreamConfig {
    source: Arc<dyn StreamProvider>,
    order: Vec<Vec<Expr>>,
    constraints: Constraints,
}

impl StreamConfig {
    /// Create a new `StreamConfig` from a `StreamProvider`
    pub fn new(source: Arc<dyn StreamProvider>) -> Self {
        Self {
            source,
            order: vec![],
            constraints: Constraints::empty(),
        }
    }

    /// Specify a sort order for the stream
    pub fn with_order(mut self, order: Vec<Vec<Expr>>) -> Self {
        self.order = order;
        self
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    fn reader(&self) -> Result<Box<dyn RecordBatchReader>> {
        self.source.reader()
    }

    fn writer(&self) -> Result<Box<dyn RecordBatchWriter>> {
        self.source.writer()
    }
}

/// A [`TableProvider`] for an unbounded stream source
///
/// Currently only reading from / appending to a single file in-place is supported, but
/// other stream sources and sinks may be added in future.
///
/// Applications looking to read/write datasets comprising multiple files, e.g. [Hadoop]-style
/// data stored in object storage, should instead consider [`ListingTable`].
///
/// [Hadoop]: https://hadoop.apache.org/
/// [`ListingTable`]: crate::datasource::listing::ListingTable
pub struct StreamTable(Arc<StreamConfig>);

impl StreamTable {
    /// Create a new [`StreamTable`] for the given [`StreamConfig`]
    pub fn new(config: Arc<StreamConfig>) -> Self {
        Self(config)
    }
}

#[async_trait]
impl TableProvider for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.0.source.schema())
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.0.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => {
                let projected = self.0.source.schema().project(p)?;
                create_ordering(&projected, &self.0.order)?
            }
            None => create_ordering(self.0.source.schema(), &self.0.order)?,
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(self.0.source.schema()),
            vec![Arc::new(StreamRead(Arc::clone(&self.0))) as _],
            projection,
            projected_schema,
            true,
            limit,
        )?))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ordering = match self.0.order.first() {
            Some(x) => {
                let schema = self.0.source.schema();
                let orders = create_ordering(schema, std::slice::from_ref(x))?;
                let ordering = orders.into_iter().next().unwrap();
                Some(ordering.into_iter().map(Into::into).collect())
            }
            None => None,
        };

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(StreamWrite(Arc::clone(&self.0))),
            Arc::clone(self.0.source.schema()),
            ordering,
        )))
    }
}

struct StreamRead(Arc<StreamConfig>);

impl PartitionStream for StreamRead {
    fn schema(&self) -> &SchemaRef {
        self.0.source.schema()
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = Arc::clone(&self.0);
        let schema = Arc::clone(self.0.source.schema());
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 2);
        let tx = builder.tx();
        builder.spawn_blocking(move || {
            let reader = config.reader()?;
            for b in reader {
                if tx.blocking_send(b.map_err(Into::into)).is_err() {
                    break;
                }
            }
            Ok(())
        });
        builder.build()
    }
}

#[derive(Debug)]
struct StreamWrite(Arc<StreamConfig>);

impl DisplayAs for StreamWrite {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.0.source.stream_write_display(_t, f)
    }
}

#[async_trait]
impl DataSink for StreamWrite {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let config = Arc::clone(&self.0);
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<RecordBatch>(2);
        // Note: FIFO Files support poll so this could use AsyncFd
        let write_task = SpawnedTask::spawn_blocking(move || {
            let mut count = 0_u64;
            let mut writer = config.writer()?;
            while let Some(batch) = receiver.blocking_recv() {
                count += batch.num_rows() as u64;
                writer.write(&batch)?;
            }
            Ok(count)
        });

        while let Some(b) = data.next().await.transpose()? {
            if sender.send(b).await.is_err() {
                break;
            }
        }
        drop(sender);
        write_task.join_unwind().await
    }
}
