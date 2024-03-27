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

use datafusion_common::instant::Instant;
use std::fs;
use std::path::{Path, PathBuf};

use datafusion::common::not_impl_err;

use datafusion::error::Result;
use datafusion::prelude::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use structopt::StructOpt;

use super::get_tbl_tpch_table_schema;
use super::TPCH_TABLES;

/// Convert tpch .slt files to .parquet or .csv files
#[derive(Debug, StructOpt)]
pub struct ConvertOpt {
    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Output file format: `csv` or `parquet`
    #[structopt(short = "f", long = "format")]
    file_format: String,

    /// Compression to use when writing Parquet files
    #[structopt(short = "c", long = "compression", default_value = "zstd")]
    compression: String,

    /// Number of partitions to produce
    #[structopt(short = "n", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,
}

impl ConvertOpt {
    pub async fn run(self) -> Result<()> {
        let compression = self.compression()?;

        let input_path = self.input_path.to_str().unwrap();
        let output_path = self.output_path.to_str().unwrap();

        let output_root_path = Path::new(output_path);
        for table in TPCH_TABLES {
            let start = Instant::now();
            let schema = get_tbl_tpch_table_schema(table);

            let input_path = format!("{input_path}/{table}.tbl");
            let options = CsvReadOptions::new()
                .schema(&schema)
                .has_header(false)
                .delimiter(b'|')
                .file_extension(".tbl");

            let config = SessionConfig::new().with_batch_size(self.batch_size);
            let ctx = SessionContext::new_with_config(config);

            // build plan to read the TBL file
            let mut csv = ctx.read_csv(&input_path, options).await?;

            // Select all apart from the padding column
            let selection = csv
                .schema()
                .iter()
                .take(schema.fields.len() - 1)
                .map(|(qualifier, field)| {
                    Expr::Column(Column::from((qualifier, field.as_ref())))
                })
                .collect();

            csv = csv.select(selection)?;
            // optionally, repartition the file
            let partitions = self.partitions;
            if partitions > 1 {
                csv = csv.repartition(Partitioning::RoundRobinBatch(partitions))?
            }

            // create the physical plan
            let csv = csv.create_physical_plan().await?;

            let output_path = output_root_path.join(table);
            let output_path = output_path.to_str().unwrap().to_owned();
            fs::create_dir_all(&output_path)?;
            println!(
                "Converting '{}' to {} files in directory '{}'",
                &input_path, self.file_format, &output_path
            );
            match self.file_format.as_str() {
                "csv" => ctx.write_csv(csv, output_path).await?,
                "parquet" => {
                    let props = WriterProperties::builder()
                        .set_compression(compression)
                        .build();
                    ctx.write_parquet(csv, output_path, Some(props)).await?
                }
                other => {
                    return not_impl_err!("Invalid output format: {other}");
                }
            }
            println!("Conversion completed in {} ms", start.elapsed().as_millis());
        }

        Ok(())
    }

    /// return the compression method to use when writing parquet
    fn compression(&self) -> Result<Compression> {
        Ok(match self.compression.as_str() {
            "none" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "brotli" => Compression::BROTLI(Default::default()),
            "gzip" => Compression::GZIP(Default::default()),
            "lz4" => Compression::LZ4,
            "lz0" => Compression::LZO,
            "zstd" => Compression::ZSTD(Default::default()),
            other => {
                return not_impl_err!("Invalid compression format: {other}");
            }
        })
    }
}
