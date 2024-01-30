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

//! Benchmark data generation

use datafusion::common::Result;
use datafusion::test_util::parquet::TestParquetFile;
use datafusion::parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use structopt::StructOpt;
use test_utils::AccessLogGenerator;

// Options and builder for making an access log test file
// Note don't use docstring or else it ends up in help
#[derive(Debug, StructOpt, Clone)]
pub struct AccessLogOpt {
    /// Path to folder where access log file will be generated
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Data page size of the generated parquet file
    #[structopt(long = "page-size")]
    page_size: Option<usize>,

    /// Data page size of the generated parquet file
    #[structopt(long = "row-group-size")]
    row_group_size: Option<usize>,

    /// Total size of generated dataset. The default scale factor of 1.0 will generate a roughly 1GB parquet file
    #[structopt(long = "scale-factor", default_value = "1.0")]
    scale_factor: f32,
}

impl AccessLogOpt {
    /// Create the access log and return the file.
    ///
    /// See [`TestParquetFile`] for more details
    pub fn build(self) -> Result<TestParquetFile> {
        let path = self.path.join("logs.parquet");

        let mut props_builder = WriterProperties::builder();

        if let Some(s) = self.page_size {
            props_builder = props_builder
                .set_data_page_size_limit(s)
                .set_write_batch_size(s);
        }

        if let Some(s) = self.row_group_size {
            props_builder = props_builder.set_max_row_group_size(s);
        }
        let props = props_builder.build();

        let generator = AccessLogGenerator::new();

        let num_batches = 100_f32 * self.scale_factor;

        TestParquetFile::try_new(path, props, generator.take(num_batches as usize))
    }
}
