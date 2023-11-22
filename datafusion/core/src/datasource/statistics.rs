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

use super::listing::PartitionedFile;
use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::physical_plan::Statistics;

use datafusion_common::stats::{Precision, StatisticsAggregator};

use futures::{Stream, StreamExt};

/// Get all files as well as the file level summary statistics (no statistic for
/// partition columns). If the optional `limit` is provided, includes only
/// sufficient files needed to read up to `limit` number of rows.
pub async fn get_statistics_with_limit(
    all_files: impl Stream<Item = Result<(PartitionedFile, Statistics)>>,
    file_schema: SchemaRef,
    limit: Option<usize>,
) -> Result<(Vec<PartitionedFile>, Statistics)> {
    let limit = limit.unwrap_or(usize::MAX);

    let mut result_files = vec![];
    let mut stats_agg = StatisticsAggregator::new(&file_schema);

    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(all_files.fuse());

    if let Some(first_file) = all_files.next().await {
        let (file, file_stats) = first_file?;
        result_files.push(file);

        stats_agg.update(&file_stats, &file_schema)?;

        // If the number of rows exceeds the limit, we can stop processing
        // files. This only applies when we know the number of rows. It also
        // currently ignores tables that have no statistics regarding the
        // number of rows.
        let conservative_num_rows = match stats_agg.num_rows() {
            Precision::Exact(nr) => nr,
            _ => usize::MIN,
        };

        if conservative_num_rows <= limit {
            while let Some(current) = all_files.next().await {
                let (file, file_stats) = current?;
                result_files.push(file);
                stats_agg.update(&file_stats, &file_schema)?;

                // If the number of rows exceeds the limit, we can stop processing
                // files.
                if let Precision::Exact(num_rows) = stats_agg.num_rows() {
                    if num_rows > limit {
                        break;
                    }
                }
            }
        }
    };

    let mut statistics = stats_agg.build();

    if all_files.next().await.is_some() {
        // If we still have files in the stream, it means that the limit kicked
        // in, and the statistic could have been different had we processed the
        // files in a different order.
        statistics = statistics.into_inexact()
    }

    Ok((result_files, statistics))
}
