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
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::pin::Pin;
use std::str::FromStr;

use crate::print_format::PrintFormat;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::physical_plan::RecordBatchStream;

use futures::StreamExt;

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum MaxRows {
    /// show all rows in the output
    Unlimited,
    /// Only show n rows
    Limited(usize),
}

impl FromStr for MaxRows {
    type Err = String;

    fn from_str(maxrows: &str) -> Result<Self, Self::Err> {
        if maxrows.to_lowercase() == "inf"
            || maxrows.to_lowercase() == "infinite"
            || maxrows.to_lowercase() == "none"
        {
            Ok(Self::Unlimited)
        } else {
            match maxrows.parse::<usize>() {
                Ok(nrows)  => Ok(Self::Limited(nrows)),
                _ => Err(format!("Invalid maxrows {}. Valid inputs are natural numbers or \'none\', \'inf\', or \'infinite\' for no limit.", maxrows)),
            }
        }
    }
}

impl Display for MaxRows {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlimited => write!(f, "unlimited"),
            Self::Limited(max_rows) => write!(f, "at most {max_rows}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrintOptions {
    pub format: PrintFormat,
    pub quiet: bool,
    pub maxrows: MaxRows,
    pub color: bool,
}

fn get_timing_info_str(
    row_count: usize,
    maxrows: MaxRows,
    query_start_time: Instant,
) -> String {
    let row_word = if row_count == 1 { "row" } else { "rows" };
    let nrows_shown_msg = match maxrows {
        MaxRows::Limited(nrows) if nrows < row_count => format!(" ({} shown)", nrows),
        _ => String::new(),
    };

    format!(
        "{} {} in set{}. Query took {:.3} seconds.\n",
        row_count,
        row_word,
        nrows_shown_msg,
        query_start_time.elapsed().as_secs_f64()
    )
}

impl PrintOptions {
    pub fn print_batches_with_empty_result(
        &self,
        batches: &[RecordBatch],
        schema: SchemaRef,
        query_start_time: Instant,
    ) -> Result<()> {
        if batches.len() == 0 {
            let option = RecordBatchOptions::new().with_match_field_names(false);

            return self.print_batches(
                &[RecordBatch::try_new_with_options(schema, vec![], &option)?],
                query_start_time,
            );
        };
        return self.print_batches(batches, query_start_time);
    }

    /// Print the batches to stdout using the specified format
    pub fn print_batches(
        &self,
        batches: &[RecordBatch],
        query_start_time: Instant,
    ) -> Result<()> {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        self.format
            .print_batches(&mut writer, batches, self.maxrows, true)?;

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        let timing_info = get_timing_info_str(
            row_count,
            if self.format == PrintFormat::Table {
                self.maxrows
            } else {
                MaxRows::Unlimited
            },
            query_start_time,
        );

        if !self.quiet {
            writeln!(writer, "{timing_info}")?;
        }

        Ok(())
    }

    /// Print the stream to stdout using the specified format
    pub async fn print_stream(
        &self,
        mut stream: Pin<Box<dyn RecordBatchStream>>,
        query_start_time: Instant,
    ) -> Result<()> {
        if self.format == PrintFormat::Table {
            return Err(DataFusionError::External(
                "PrintFormat::Table is not implemented".to_string().into(),
            ));
        };

        let stdout = std::io::stdout();
        let mut writer = stdout.lock();

        let mut row_count = 0_usize;
        let mut with_header = true;

        while let Some(maybe_batch) = stream.next().await {
            let batch = maybe_batch?;
            row_count += batch.num_rows();
            self.format.print_batches(
                &mut writer,
                &[batch],
                MaxRows::Unlimited,
                with_header,
            )?;
            with_header = false;
        }

        let timing_info =
            get_timing_info_str(row_count, MaxRows::Unlimited, query_start_time);

        if !self.quiet {
            writeln!(writer, "{timing_info}")?;
        }

        Ok(())
    }
}
