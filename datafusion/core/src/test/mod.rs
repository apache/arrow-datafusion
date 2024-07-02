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

//! Common unit test utility methods

use std::any::Any;
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

use crate::datasource::file_format::csv::CsvFormat;
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::FileFormat;
use crate::datasource::listing::PartitionedFile;
use crate::datasource::object_store::ObjectStoreUrl;
use crate::datasource::physical_plan::{CsvExec, FileScanConfig};
use crate::datasource::{MemTable, TableProvider};
use crate::error::Result;
use crate::logical_expr::LogicalPlan;
use crate::physical_plan::ExecutionPlan;
use crate::test::object_store::local_unpartitioned_file;
use crate::test_util::{aggr_test_schema, arrow_test_data};

use arrow::array::{self, Array, ArrayRef, Decimal128Builder, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, PlanProperties,
};

#[cfg(feature = "compression")]
use bzip2::write::BzEncoder;
#[cfg(feature = "compression")]
use bzip2::Compression as BzCompression;
#[cfg(feature = "compression")]
use flate2::write::GzEncoder;
#[cfg(feature = "compression")]
use flate2::Compression as GzCompression;
#[cfg(feature = "compression")]
use xz2::write::XzEncoder;
#[cfg(feature = "compression")]
use zstd::Encoder as ZstdEncoder;

pub fn create_table_dual() -> Arc<dyn TableProvider> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        dual_schema.clone(),
        vec![
            Arc::new(array::Int32Array::from(vec![1])),
            Arc::new(array::StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(dual_schema, vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

/// Returns a [`CsvExec`] that scans "aggregate_test_100.csv" with `partitions` partitions
pub fn scan_partitioned_csv(partitions: usize, work_dir: &Path) -> Result<Arc<CsvExec>> {
    let schema = aggr_test_schema();
    let filename = "aggregate_test_100.csv";
    let path = format!("{}/csv", arrow_test_data());
    let file_groups = partitioned_file_groups(
        path.as_str(),
        filename,
        partitions,
        Arc::new(CsvFormat::default()),
        FileCompressionType::UNCOMPRESSED,
        work_dir,
    )?;
    let config = partitioned_csv_config(schema, file_groups);
    Ok(Arc::new(CsvExec::new(
        config,
        true,
        b',',
        b'"',
        None,
        None,
        FileCompressionType::UNCOMPRESSED,
    )))
}

/// Returns file groups [`Vec<Vec<PartitionedFile>>`] for scanning `partitions` of `filename`
pub fn partitioned_file_groups(
    path: &str,
    filename: &str,
    partitions: usize,
    file_format: Arc<dyn FileFormat>,
    file_compression_type: FileCompressionType,
    work_dir: &Path,
) -> Result<Vec<Vec<PartitionedFile>>> {
    let path = format!("{path}/{filename}");

    let mut writers = vec![];
    let mut files = vec![];
    for i in 0..partitions {
        let filename = format!(
            "partition-{}{}",
            i,
            file_format
                .get_ext_with_compression(&file_compression_type)
                .unwrap()
        );
        let filename = work_dir.join(filename);

        let file = File::create(&filename).unwrap();

        let encoder: Box<dyn Write + Send> = match file_compression_type.to_owned() {
            FileCompressionType::UNCOMPRESSED => Box::new(file),
            #[cfg(feature = "compression")]
            FileCompressionType::GZIP => {
                Box::new(GzEncoder::new(file, GzCompression::default()))
            }
            #[cfg(feature = "compression")]
            FileCompressionType::XZ => Box::new(XzEncoder::new(file, 9)),
            #[cfg(feature = "compression")]
            FileCompressionType::ZSTD => {
                let encoder = ZstdEncoder::new(file, 0)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .auto_finish();
                Box::new(encoder)
            }
            #[cfg(feature = "compression")]
            FileCompressionType::BZIP2 => {
                Box::new(BzEncoder::new(file, BzCompression::default()))
            }
            #[cfg(not(feature = "compression"))]
            FileCompressionType::GZIP
            | FileCompressionType::BZIP2
            | FileCompressionType::XZ
            | FileCompressionType::ZSTD => {
                panic!("Compression is not supported in this build")
            }
        };

        let writer = BufWriter::new(encoder);
        writers.push(writer);
        files.push(filename);
    }

    let f = File::open(path)?;
    let f = BufReader::new(f);
    for (i, line) in f.lines().enumerate() {
        let line = line.unwrap();

        if i == 0 && file_format.get_ext() == CsvFormat::default().get_ext() {
            // write header to all partitions
            for w in writers.iter_mut() {
                w.write_all(line.as_bytes()).unwrap();
                w.write_all(b"\n").unwrap();
            }
        } else {
            // write data line to single partition
            let partition = i % partitions;
            writers[partition].write_all(line.as_bytes()).unwrap();
            writers[partition].write_all(b"\n").unwrap();
        }
    }

    // Must drop the stream before creating ObjectMeta below as drop
    // triggers finish for ZstdEncoder which writes additional data
    for mut w in writers.into_iter() {
        w.flush().unwrap();
    }

    Ok(files
        .into_iter()
        .map(|f| vec![local_unpartitioned_file(f).into()])
        .collect::<Vec<_>>())
}

/// Returns a [`FileScanConfig`] for given `file_groups`
pub fn partitioned_csv_config(
    schema: SchemaRef,
    file_groups: Vec<Vec<PartitionedFile>>,
) -> FileScanConfig {
    FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema)
        .with_file_groups(file_groups)
}

pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(actual, expected);
}

/// Returns the column names on the schema
pub fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Return a new table provider that has a single Int32 column with
/// values between `seq_start` and `seq_end`
pub fn table_with_sequence(
    seq_start: i32,
    seq_end: i32,
) -> Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from((seq_start..=seq_end).collect::<Vec<_>>()));
    let partitions = vec![vec![RecordBatch::try_new(
        schema.clone(),
        vec![arr as ArrayRef],
    )?]];
    Ok(Arc::new(MemTable::try_new(schema, partitions)?))
}

/// Return a RecordBatch with a single Int32 array with values (0..sz)
pub fn make_partition(sz: i32) -> RecordBatch {
    let seq_start = 0;
    let seq_end = sz;
    let values = (seq_start..seq_end).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from(values));
    let arr = arr as ArrayRef;

    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Return a new table which provide this decimal column
pub fn table_with_decimal() -> Arc<dyn TableProvider> {
    let batch_decimal = make_decimal();
    let schema = batch_decimal.schema();
    let partitions = vec![vec![batch_decimal]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

fn make_decimal() -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(20);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128);
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128);
    }
    let array = decimal_builder
        .finish()
        .with_precision_and_scale(10, 3)
        .unwrap();
    let schema = Schema::new(vec![Field::new("c1", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

/// Created a sorted Csv exec
pub fn csv_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(CsvExec::new(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema.clone())
            .with_file(PartitionedFile::new("x".to_string(), 100))
            .with_output_ordering(vec![sort_exprs]),
        false,
        0,
        0,
        None,
        None,
        FileCompressionType::UNCOMPRESSED,
    ))
}

// construct a stream partition for test purposes
pub(crate) struct TestStreamPartition {
    pub schema: SchemaRef,
}

impl PartitionStream for TestStreamPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        unreachable!()
    }
}

/// Create an unbounded stream exec
pub fn stream_exec_ordered(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(
        StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(TestStreamPartition {
                schema: schema.clone(),
            }) as _],
            None,
            vec![sort_exprs],
            true,
            None,
        )
        .unwrap(),
    )
}

/// Create a csv exec for tests
pub fn csv_exec_ordered(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(CsvExec::new(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema.clone())
            .with_file(PartitionedFile::new("file_path".to_string(), 100))
            .with_output_ordering(vec![sort_exprs]),
        true,
        0,
        b'"',
        None,
        None,
        FileCompressionType::UNCOMPRESSED,
    ))
}

/// A mock execution plan that simply returns the provided statistics
#[derive(Debug, Clone)]
pub struct StatisticsExec {
    stats: Statistics,
    schema: Arc<Schema>,
    cache: PlanProperties,
}

impl StatisticsExec {
    pub fn new(stats: Statistics, schema: Schema) -> Self {
        assert_eq!(
            stats.column_statistics.len(), schema.fields().len(),
            "if defined, the column statistics vector length should be the number of fields"
        );
        let cache = Self::compute_properties(Arc::new(schema.clone()));
        Self {
            stats,
            schema: Arc::new(schema),
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            // Output Partitioning
            Partitioning::UnknownPartitioning(2),
            // Execution Mode
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for StatisticsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StatisticsExec: col_count={}, row_count={:?}",
                    self.schema.fields().len(),
                    self.stats.num_rows,
                )
            }
        }
    }
}

impl ExecutionPlan for StatisticsExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("This plan only serves for testing statistics")
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.stats.clone())
    }
}

pub mod object_store;
pub mod variable;
