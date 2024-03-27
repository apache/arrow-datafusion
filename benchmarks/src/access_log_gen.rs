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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::{DataFusionError, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use parquet::file::properties::WriterProperties;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use test_utils::AccessLogGenerator;

/// Creates synthetic data that mimic's a web server access log
/// dataset
///
/// Example
///
/// dfbench access-log-gen   --path ./data/access_logs --scale-factor 1.0
///
///
/// Generates synthetic dataset in `./data/access_logs`. The size
/// of the dataset can be controlled through the `size_factor`
/// (with the default value of `1.0` generating a ~1GB parquet file).
///
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Path to folder where access log files will be generated
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Total size of each file in dataset. Defaults to 1.0, which generates
    /// 10M rows per file
    //#[structopt(long = "scale-factor", default_value = "1.0")]
    #[structopt(long = "scale-factor", default_value = "0.01")]
    scale_factor: f32,

    /// How many files are created. Defaults to 25
    #[structopt(default_value = "25", short = "n", long = "num-files")]
    num_files: NonZeroUsize,

    /// Should the files be sorted or not? Defaults to false. If true, the files will be sorted by XX, YY, and timestamp.
    #[structopt(short = "s", long = "sorted")]
    sort: bool,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let Self {
            path,
            scale_factor,
            num_files,
            sort,
        } = self;

        std::fs::create_dir_all(&path)?;

        // Create output files in parallel
        let results: Vec<_> = futures::stream::iter(0..num_files.get())
            .map(|i| write_to_file(path.clone(), i, scale_factor, sort))
            // write to files in parallel, using all cores
            .buffer_unordered(num_cpus::get())
            .try_collect()
            .await?;

        // Unnest the Vec<Vec<RecordBatch>> into a Vec<RecordBatch>
        let results: Vec<_> = results.into_iter().flat_map(|b| b.into_iter()).collect();

        println!("done!");
        println!("{}", pretty_format_batches(&results)?);

        Ok(())
    }
}

/// writes a single parquet file, returning a record batch with the count.
async fn write_to_file(
    path: PathBuf,
    file_index: usize,
    scale_factor: f32,
    sort: bool,
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    let path = path.join(format!("{file_index}.parquet"));
    let output_filename = path.to_str().expect("non utf8 path name");
    let options = DataFrameWriteOptions::new().with_single_file_output(true);
    // TODO maybe make parquet writer properties configurable?
    let writer_properties = WriterProperties::builder()
        //.set_data_page_size_limit(1024 * 1024)
        //.set_write_batch_size(1024 * 1024)
        //.set_max_row_group_size(1024 * 1024)
        .build();
    let seed = (file_index * 17) as u64;
    let provider = access_log_provider(scale_factor, seed)?;
    println!(
        "writing scale_factor: sf={scale_factor}, seed={seed} to {output_filename}..."
    );
    ctx.read_table(provider)?
        .write_parquet(output_filename, options, Some(writer_properties))
        .await
}

fn access_log_provider(scale_factor: f32, seed: u64) -> Result<Arc<dyn TableProvider>> {
    let stream = AccessLogStream::new(scale_factor, seed);

    let table = StreamingTable::try_new(stream.schema().clone(), vec![Arc::new(stream)])?;

    Ok(Arc::new(table))
}

/// Stream of access log records generated on demand
struct AccessLogStream {
    schema: SchemaRef,
    scale_factor: f32,
    seed: u64,
}

impl AccessLogStream {
    fn new(scale_factor: f32, seed: u64) -> Self {
        let schema = AccessLogGenerator::new().schema();
        Self {
            schema,
            scale_factor,
            seed,
        }
    }
}

impl PartitionStream for AccessLogStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let row_limit = (10_000_000_f32 * self.scale_factor) as usize;

        //
        let service_names: Vec<_> = ["frontend", "backend", "database", "cache"]
            .iter()
            .flat_map(|name| {
                (0..5)
                    .map(|i| format!("service-{name}{i}"))
                    .collect::<Vec<_>>()
            })
            .collect();

        let generator = AccessLogGenerator::new()
            .with_row_limit(row_limit)
            .with_pods_per_host(1..100)
            .with_containers_per_pod(10..50)
            .with_service_names(service_names)
            .with_seed(self.seed);

        let stream = futures::stream::iter(generator).map(|batch| Ok(batch));

        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}
