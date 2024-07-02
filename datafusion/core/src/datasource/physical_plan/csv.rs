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

//! Execution plan for reading CSV files

use std::any::Any;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use super::{calculate_range, FileGroupPartitioner, FileScanConfig, RangeCalculation};
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::listing::{FileRange, ListingTableUrl, PartitionedFile};
use crate::datasource::physical_plan::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::datasource::physical_plan::FileMeta;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::csv;
use arrow::datatypes::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};

use bytes::{Buf, Bytes};
use futures::{ready, StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    has_header: bool,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    comment: Option<u8>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Compression type of the file associated with CsvExec
    pub file_compression_type: FileCompressionType,
    cache: PlanProperties,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided base and specific configurations
    pub fn new(
        base_config: FileScanConfig,
        has_header: bool,
        delimiter: u8,
        quote: u8,
        escape: Option<u8>,
        comment: Option<u8>,
        file_compression_type: FileCompressionType,
    ) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();
        let cache = Self::compute_properties(
            projected_schema,
            &projected_output_ordering,
            &base_config,
        );
        Self {
            base_config,
            projected_statistics,
            has_header,
            delimiter,
            quote,
            escape,
            metrics: ExecutionPlanMetricsSet::new(),
            file_compression_type,
            cache,
            comment,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The quote character
    pub fn quote(&self) -> u8 {
        self.quote
    }

    /// Lines beginning with this byte are ignored.
    pub fn comment(&self) -> Option<u8> {
        self.comment
    }

    /// The escape character
    pub fn escape(&self) -> Option<u8> {
        self.escape
    }

    fn output_partitioning_helper(file_scan_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_scan_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_scan_config), // Output Partitioning
            ExecutionMode::Bounded,                             // Execution Mode
        )
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups;
        // Changing file groups may invalidate output partitioning. Update it also
        let output_partitioning = Self::output_partitioning_helper(&self.base_config);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
    }
}

impl DisplayAs for CsvExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CsvExec: ")?;
        self.base_config.fmt_as(t, f)?;
        write!(f, ", has_header={}", self.has_header)
    }
}

impl ExecutionPlan for CsvExec {
    fn name(&self) -> &'static str {
        "CsvExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on [`FileGroupPartitioner`] for more detail.
    ///
    /// Return `None` if can't get repartitioned(empty/compressed file).
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        // Parallel execution on compressed CSV file is not supported yet.
        if self.file_compression_type.is_compressed() {
            return Ok(None);
        }

        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_preserve_order_within_groups(
                self.properties().output_ordering().is_some(),
            )
            .with_repartition_file_min_size(repartition_file_min_size)
            .repartition_file_groups(&self.base_config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let mut new_plan = self.clone();
            new_plan = new_plan.with_file_groups(repartitioned_file_groups);
            return Ok(Some(Arc::new(new_plan)));
        }
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let config = Arc::new(CsvConfig {
            batch_size: context.session_config().batch_size(),
            file_schema: Arc::clone(&self.base_config.file_schema),
            file_projection: self.base_config.file_column_projection_indices(),
            has_header: self.has_header,
            delimiter: self.delimiter,
            quote: self.quote,
            escape: self.escape,
            object_store,
            comment: self.comment,
        });

        let opener = CsvOpener {
            config,
            file_compression_type: self.file_compression_type.to_owned(),
        };
        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A Config for [`CsvOpener`]
#[derive(Debug, Clone)]
pub struct CsvConfig {
    batch_size: usize,
    file_schema: SchemaRef,
    file_projection: Option<Vec<usize>>,
    has_header: bool,
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    object_store: Arc<dyn ObjectStore>,
    comment: Option<u8>,
}

impl CsvConfig {
    #[allow(clippy::too_many_arguments)]
    /// Returns a [`CsvConfig`]
    pub fn new(
        batch_size: usize,
        file_schema: SchemaRef,
        file_projection: Option<Vec<usize>>,
        has_header: bool,
        delimiter: u8,
        quote: u8,
        object_store: Arc<dyn ObjectStore>,
        comment: Option<u8>,
    ) -> Self {
        Self {
            batch_size,
            file_schema,
            file_projection,
            has_header,
            delimiter,
            quote,
            escape: None,
            object_store,
            comment,
        }
    }
}

impl CsvConfig {
    fn open<R: Read>(&self, reader: R) -> Result<csv::Reader<R>> {
        Ok(self.builder().build(reader)?)
    }

    fn builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new(Arc::clone(&self.file_schema))
            .with_delimiter(self.delimiter)
            .with_batch_size(self.batch_size)
            .with_header(self.has_header)
            .with_quote(self.quote);

        if let Some(proj) = &self.file_projection {
            builder = builder.with_projection(proj.clone());
        }
        if let Some(escape) = self.escape {
            builder = builder.with_escape(escape)
        }
        if let Some(comment) = self.comment {
            builder = builder.with_comment(comment);
        }

        builder
    }
}

/// A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]
pub struct CsvOpener {
    config: Arc<CsvConfig>,
    file_compression_type: FileCompressionType,
}

impl CsvOpener {
    /// Returns a [`CsvOpener`]
    pub fn new(
        config: Arc<CsvConfig>,
        file_compression_type: FileCompressionType,
    ) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for CsvOpener {
    /// Open a partitioned CSV file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// If `file_meta.range` is `Some(FileRange {start, end})`, this signifies that the partition
    /// corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    ///
    /// Examples:
    /// Consider the following partitions enclosed by braces `{}`:
    ///
    /// {A,1,2,3,4,5,6,7,8,9\n
    ///  A,1,2,3,4,5,6,7,8,9\n}
    ///  A,1,2,3,4,5,6,7,8,9\n
    ///  The lines read would be: [0, 1]
    ///
    ///  A,{1,2,3,4,5,6,7,8,9\n
    ///  A,1,2,3,4,5,6,7,8,9\n
    ///  A},1,2,3,4,5,6,7,8,9\n
    ///  The lines read would be: [1, 2]
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        // `self.config.has_header` controls whether to skip reading the 1st line header
        // If the .csv file is read in parallel and this `CsvOpener` is only reading some middle
        // partition, then don't skip first line
        let mut csv_has_header = self.config.has_header;
        if let Some(FileRange { start, .. }) = file_meta.range {
            if start != 0 {
                csv_has_header = false;
            }
        }

        let config = CsvConfig {
            has_header: csv_has_header,
            ..(*self.config).clone()
        };

        let file_compression_type = self.file_compression_type.to_owned();

        if file_meta.range.is_some() {
            assert!(
                !file_compression_type.is_compressed(),
                "Reading compressed .csv in parallel is not supported"
            );
        }

        let store = Arc::clone(&self.config.object_store);

        Ok(Box::pin(async move {
            // Current partition contains bytes [start_byte, end_byte) (might contain incomplete lines at boundaries)

            let calculated_range = calculate_range(&file_meta, &store).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    )
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store.get_opts(file_meta.location(), options).await?;

            match result.payload {
                GetResultPayload::File(mut file, _) => {
                    let is_whole_file_scanned = file_meta.range.is_none();
                    let decoder = if is_whole_file_scanned {
                        // Don't seek if no range as breaks FIFO files
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        file_compression_type.convert_read(
                            file.take((result.range.end - result.range.start) as u64),
                        )?
                    };

                    Ok(futures::stream::iter(config.open(decoder)?).boxed())
                }
                GetResultPayload::Stream(s) => {
                    let mut decoder = config.builder().build_decoder();
                    let s = s.map_err(DataFusionError::from);
                    let mut input =
                        file_compression_type.convert_stream(s.boxed())?.fuse();
                    let mut buffered = Bytes::new();

                    let s = futures::stream::poll_fn(move |cx| {
                        loop {
                            if buffered.is_empty() {
                                match ready!(input.poll_next_unpin(cx)) {
                                    Some(Ok(b)) => buffered = b,
                                    Some(Err(e)) => {
                                        return Poll::Ready(Some(Err(e.into())))
                                    }
                                    None => {}
                                };
                            }
                            let decoded = match decoder.decode(buffered.as_ref()) {
                                // Note: the decoder needs to be called with an empty
                                // array to delimt the final record
                                Ok(0) => break,
                                Ok(decoded) => decoded,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            buffered.advance(decoded);
                        }

                        Poll::Ready(decoder.flush().transpose())
                    });
                    Ok(s.boxed())
                }
            }
        }))
    }
}

pub async fn plan_to_csv(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.csv", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer = BufWriter::new(storeref, file.clone());
            let mut buffer = Vec::with_capacity(1024);
            //only write headers on first iteration
            let mut write_headers = true;
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = csv::WriterBuilder::new()
                    .with_header(write_headers)
                    .build(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
                //prevent writing headers more than once
                write_headers = false;
            }
            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;

    use super::*;
    use crate::dataframe::DataFrameWriteOptions;
    use crate::datasource::file_format::csv::CsvFormat;
    use crate::prelude::*;
    use crate::test::{partitioned_csv_config, partitioned_file_groups};
    use crate::{scalar::ScalarValue, test_util::aggr_test_schema};

    use arrow::datatypes::*;
    use datafusion_common::test_util::arrow_test_data;

    use object_store::chunked::ChunkedStore;
    use object_store::local::LocalFileSystem;
    use rstest::*;
    use tempfile::TempDir;
    use url::Url;

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups);
        config.projection = Some(vec![0, 2, 4]);

        let csv = CsvExec::new(
            config,
            true,
            b',',
            b'"',
            None,
            None,
            file_compression_type.to_owned(),
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = [
            "+----+-----+------------+",
            "| c1 | c3  | c5         |",
            "+----+-----+------------+",
            "| c  | 1   | 2033001162 |",
            "| d  | -40 | 706441268  |",
            "| b  | 29  | 994303988  |",
            "| a  | -85 | 1171968280 |",
            "| b  | -82 | 1824882165 |",
            "+----+-----+------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
        let session_ctx = SessionContext::new_with_config(cfg);
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups);
        config.projection = Some(vec![4, 0, 2]);

        let csv = CsvExec::new(
            config,
            true,
            b',',
            b'"',
            None,
            None,
            file_compression_type.to_owned(),
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = [
            "+------------+----+-----+",
            "| c5         | c1 | c3  |",
            "+------------+----+-----+",
            "| 2033001162 | c  | 1   |",
            "| 706441268  | d  | -40 |",
            "| 994303988  | b  | 29  |",
            "| 1171968280 | a  | -85 |",
            "| 1824882165 | b  | -82 |",
            "+------------+----+-----+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_limit(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let cfg = SessionConfig::new().set_str("datafusion.catalog.has_header", "true");
        let session_ctx = SessionContext::new_with_config(cfg);
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups);
        config.limit = Some(5);

        let csv = CsvExec::new(
            config,
            true,
            b',',
            b'"',
            None,
            None,
            file_compression_type.to_owned(),
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        let expected = ["+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c1 | c2 | c3  | c4     | c5         | c6                   | c7  | c8    | c9         | c10                  | c11         | c12                 | c13                            |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c  | 2  | 1   | 18109  | 2033001162 | -6513304855495910254 | 25  | 43062 | 1491205016 | 5863949479783605708  | 0.110830784 | 0.9294097332465232  | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |",
            "| d  | 5  | -40 | 22614  | 706441268  | -7542719935673075327 | 155 | 14337 | 3373581039 | 11720144131976083864 | 0.69632107  | 0.3114712539863804  | C2GT5KVyOPZpgKVl110TyZO0NcJ434 |",
            "| b  | 1  | 29  | -18218 | 994303988  | 5983957848665088916  | 204 | 9489  | 3275293996 | 14857091259186476033 | 0.53840446  | 0.17909035118828576 | AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz |",
            "| a  | 1  | -85 | -15154 | 1171968280 | 1919439543497968449  | 77  | 52286 | 774637006  | 12101411955859039553 | 0.12285209  | 0.6864391962767343  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB |",
            "| b  | 5  | -82 | 22080  | 1824882165 | 7373730676428214987  | 208 | 34331 | 3342719438 | 3330177516592499461  | 0.82634634  | 0.40975383525297016 | Ig1QcuKsjHXkproePdERo2w0mYzIqd |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+"];

        crate::assert_batches_eq!(expected, &[batch]);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema_with_missing_col();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups);
        config.limit = Some(5);

        let csv = CsvExec::new(
            config,
            true,
            b',',
            b'"',
            None,
            None,
            file_compression_type.to_owned(),
        );
        assert_eq!(14, csv.base_config.file_schema.fields().len());
        assert_eq!(14, csv.schema().fields().len());

        // errors due to https://github.com/apache/datafusion/issues/4918
        let mut it = csv.execute(0, task_ctx)?;
        let err = it.next().await.unwrap().unwrap_err().strip_backtrace();
        assert_eq!(
            err,
            "Arrow error: Csv error: incorrect number of fields for line 1, expected 14 got 13"
        );
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn csv_exec_with_partition(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        use crate::datasource::file_format::csv::CsvFormat;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups);

        // Add partition columns
        config.table_partition_cols = vec![Field::new("date", DataType::Utf8, false)];
        config.file_groups[0][0].partition_values = vec![ScalarValue::from("2021-10-26")];

        // We should be able to project on the partition column
        // Which is supposed to be after the file fields
        config.projection = Some(vec![0, config.file_schema.fields().len()]);

        // we don't have `/date=xx/` in the path but that is ok because
        // partitions are resolved during scan anyway
        let csv = CsvExec::new(
            config,
            true,
            b',',
            b'"',
            None,
            None,
            file_compression_type.to_owned(),
        );
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(2, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = [
            "+----+------------+",
            "| c1 | date       |",
            "+----+------------+",
            "| c  | 2021-10-26 |",
            "| d  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "| a  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "+----+------------+",
        ];
        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);

        let metrics = csv.metrics().expect("doesn't found metrics");
        let time_elapsed_processing = get_value(&metrics, "time_elapsed_processing");
        assert!(
            time_elapsed_processing > 0,
            "Expected time_elapsed_processing greater than 0",
        );
        Ok(())
    }

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{partition}.{file_extension}");
            let file_path = tmp_dir.path().join(filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.register_object_store(&url, store.clone());

        let task_ctx = ctx.task_ctx();

        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";
        let tmp_dir = TempDir::new()?;

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            Arc::new(CsvFormat::default()),
            file_compression_type.to_owned(),
            tmp_dir.path(),
        )
        .unwrap();

        let config = partitioned_csv_config(file_schema, file_groups);
        let csv = CsvExec::new(
            config,
            true,
            b',',
            b'"',
            None,
            None,
            file_compression_type.to_owned(),
        );

        let it = csv.execute(0, task_ctx).unwrap();
        let batches: Vec<_> = it.try_collect().await.unwrap();

        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        assert_eq!(total_rows, 100);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_chunked_csv(
        file_compression_type: FileCompressionType,
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) -> Result<()> {
        test_additional_stores(
            file_compression_type,
            Arc::new(ChunkedStore::new(
                Arc::new(LocalFileSystem::new()),
                chunk_size,
            )),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_no_trailing_delimiter() {
        let session_ctx = SessionContext::new();
        let store = object_store::memory::InMemory::new();

        let data = bytes::Bytes::from("a,b\n1,2\n3,4");
        let path = object_store::path::Path::from("a.csv");
        store.put(&path, data.into()).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx.register_object_store(&url, Arc::new(store));

        let df = session_ctx
            .read_csv("memory:///", CsvReadOptions::new())
            .await
            .unwrap();

        let result = df.collect().await.unwrap();

        let expected = [
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "| 3 | 4 |",
            "+---+---+",
        ];

        crate::assert_batches_eq!(expected, &result);
    }

    #[tokio::test]
    async fn write_csv_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();

        // register a local file system object store
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;

        let out_dir_url = "file://local/out";
        let e = df
            .write_csv(out_dir_url, DataFrameWriteOptions::new(), None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value d for column 0 at line 4");
        Ok(())
    }

    #[tokio::test]
    async fn write_csv_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_target_partitions(8)
                .set_str("datafusion.catalog.has_header", "false"),
        );

        let schema = populate_csv_partitions(&tmp_dir, 8, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // register a local file system object store
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();

        ctx.register_object_store(&local_url, local);

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out/";
        let out_dir_url = "file://local/out/";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        df.write_csv(out_dir_url, DataFrameWriteOptions::new(), None)
            .await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().set_str("datafusion.catalog.has_header", "false"),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // get name of first part
        let paths = fs::read_dir(&out_dir).unwrap();
        let mut part_0_name: String = "".to_owned();
        for path in paths {
            let path = path.unwrap();
            let name = path
                .path()
                .file_name()
                .expect("Should be a file name")
                .to_str()
                .expect("Should be a str")
                .to_owned();
            if name.ends_with("_0.csv") {
                part_0_name = name;
                break;
            }
        }

        if part_0_name.is_empty() {
            panic!("Did not find part_0 in csv output files!")
        }
        // register each partition as well as the top level dir
        let csv_read_option = CsvReadOptions::new().schema(&schema).has_header(false);
        ctx.register_csv(
            "part0",
            &format!("{out_dir}/{part_0_name}"),
            csv_read_option.clone(),
        )
        .await?;
        ctx.register_csv("allparts", &out_dir, csv_read_option)
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 80);

        Ok(())
    }

    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }

    /// Get the schema for the aggregate_test_* csv files with an additional filed not present in the files.
    fn aggr_test_schema_with_missing_col() -> SchemaRef {
        let fields =
            Fields::from_iter(aggr_test_schema().fields().iter().cloned().chain(
                std::iter::once(Arc::new(Field::new(
                    "missing_col",
                    DataType::Int64,
                    true,
                ))),
            ));

        let schema = Schema::new(fields);

        Arc::new(schema)
    }
}
