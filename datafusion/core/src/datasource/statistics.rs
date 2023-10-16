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
use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::error::Result;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::{Accumulator, ColumnStatistics, Statistics};

use datafusion_common::stats::Precision;
use datafusion_common::ScalarValue;

use futures::{Stream, StreamExt};
use itertools::izip;

/// Get all files as well as the file level summary statistics (no statistic for partition columns).
/// If the optional `limit` is provided, includes only sufficient files.
/// Needed to read up to `limit` number of rows.
pub async fn get_statistics_with_limit(
    all_files: impl Stream<Item = Result<(PartitionedFile, Statistics)>>,
    file_schema: SchemaRef,
    limit: Option<usize>,
) -> Result<(Vec<PartitionedFile>, Statistics)> {
    let mut result_files = vec![];
    // These statistics can be calculated as long as at least one file has them.
    // If none of the files provide them, then they will become an absent precision.
    // The missing values will be counted as
    // - zero for summations,
    // - neutral element for extreme points.
    let mut null_counts: Vec<Precision<usize>> =
        vec![Precision::Absent; file_schema.fields().len()];
    let mut max_values: Vec<Precision<ScalarValue>> =
        vec![Precision::Absent; file_schema.fields().len()];
    let mut min_values: Vec<Precision<ScalarValue>> =
        vec![Precision::Absent; file_schema.fields().len()];
    let mut num_rows: Precision<usize> = Precision::Absent;
    let mut total_byte_size: Precision<usize> = Precision::Absent;

    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(all_files.fuse());

    if let Some(first_file) = all_files.next().await {
        let (file, file_stats) = first_file?;
        result_files.push(file);

        // First file, we set them directly from the file statistics.
        set_from_file_statistics(
            &mut num_rows,
            &mut total_byte_size,
            &mut null_counts,
            &mut max_values,
            &mut min_values,
            file_stats,
        );

        // If the number of rows exceeds the limit, we can stop processing
        // files. This only applies when we know the number of rows. It also
        // currently ignores tables that have no statistics regarding the
        // number of rows.
        if num_rows.get_value().unwrap_or(&usize::MIN) <= &limit.unwrap_or(usize::MAX) {
            while let Some(current) = all_files.next().await {
                let (file, file_stats) = current?;
                result_files.push(file);

                // Number of rows, total byte size and null counts are added for each file.
                // In case of an absent information or inexact value coming from the file,
                // it changes the statistic precision to inexact.
                num_rows = add_row_stats(file_stats.num_rows, num_rows);

                total_byte_size =
                    add_row_stats(file_stats.total_byte_size, total_byte_size);

                for (cs, target) in file_stats
                    .column_statistics
                    .iter()
                    .map(|cs| cs.null_count.clone())
                    .zip(null_counts.iter_mut())
                {
                    *target = add_row_stats(cs, target.clone());
                }

                for (i, cs) in file_stats.column_statistics.iter().enumerate() {
                    set_max_if_greater(&mut max_values, cs.max_value.clone(), i);
                }

                for (i, cs) in file_stats.column_statistics.iter().enumerate() {
                    set_min_if_lesser(&mut min_values, cs.min_value.clone(), i);
                }

                // If the number of rows exceeds the limit, we can stop processing
                // files. This only applies when we know the number of rows. It also
                // currently ignores tables that have no statistics regarding the
                // number of rows.
                if num_rows.get_value().unwrap_or(&usize::MIN)
                    > &limit.unwrap_or(usize::MAX)
                {
                    break;
                }
            }
        }
    };

    let mut statistics = Statistics {
        num_rows,
        total_byte_size,
        column_statistics: get_col_stats_vec(null_counts, max_values, min_values),
    };
    if all_files.next().await.is_some() {
        // If we still have files in the stream, it means that the limit kicked
        // in, and the statistic could have been different had we processed the
        // files in a different order.
        statistics = statistics.into_inexact()
    }

    Ok((result_files, statistics))
}

pub(crate) fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MaxAccumulator::try_new(field.data_type()).ok())
        .collect();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MinAccumulator::try_new(field.data_type()).ok())
        .collect();
    (max_values, min_values)
}

fn set_from_file_statistics(
    num_rows: &mut Precision<usize>,
    total_byte_size: &mut Precision<usize>,
    null_counts: &mut Vec<Precision<usize>>,
    max_values: &mut Vec<Precision<ScalarValue>>,
    min_values: &mut Vec<Precision<ScalarValue>>,
    file_stats: Statistics,
) {
    *num_rows = file_stats.num_rows;
    *total_byte_size = file_stats.total_byte_size;
    *null_counts = file_stats
        .column_statistics
        .iter()
        .map(|cs| cs.null_count.clone())
        .collect::<Vec<_>>();
    *max_values = file_stats
        .column_statistics
        .iter()
        .map(|cs| cs.max_value.clone())
        .collect::<Vec<_>>();
    *min_values = file_stats
        .column_statistics
        .iter()
        .map(|cs| cs.min_value.clone())
        .collect::<Vec<_>>();
}

fn add_row_stats(
    file_num_rows: Precision<usize>,
    num_rows: Precision<usize>,
) -> Precision<usize> {
    match (file_num_rows, &num_rows) {
        (Precision::Absent, _) => num_rows.to_inexact(),
        (lhs, Precision::Absent) => lhs.to_inexact(),
        (lhs, rhs) => lhs.add(rhs),
    }
}

pub(crate) fn get_col_stats_vec(
    null_counts: Vec<Precision<usize>>,
    max_values: Vec<Precision<ScalarValue>>,
    min_values: Vec<Precision<ScalarValue>>,
) -> Vec<ColumnStatistics> {
    izip!(null_counts, max_values, min_values)
        .map(|(null_count, max_value, min_value)| ColumnStatistics {
            null_count,
            max_value,
            min_value,
            distinct_count: Precision::Absent,
        })
        .collect()
}

pub(crate) fn get_col_stats(
    schema: &Schema,
    null_counts: Vec<Precision<usize>>,
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = match &max_values[i] {
                Some(max_value) => max_value.evaluate().ok(),
                None => None,
            };
            let min_value = match &min_values[i] {
                Some(min_value) => min_value.evaluate().ok(),
                None => None,
            };
            ColumnStatistics {
                null_count: null_counts[i].clone(),
                max_value: max_value.map(Precision::Exact).unwrap_or(Precision::Absent),
                min_value: min_value.map(Precision::Exact).unwrap_or(Precision::Absent),
                distinct_count: Precision::Absent,
            }
        })
        .collect()
}

/// If the given value is numerically greater than the original maximum value,
/// set the new maximum value with appropriate exactness information.
fn set_max_if_greater(
    max_values: &mut [Precision<ScalarValue>],
    max_nominee: Precision<ScalarValue>,
    index: usize,
) {
    match (&max_values[index], &max_nominee) {
        (Precision::Exact(val1), Precision::Exact(val2)) => {
            if val1 < val2 {
                max_values[index] = max_nominee;
            }
        }
        (Precision::Exact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Exact(val2)) => {
            if val1 < val2 {
                max_values[index] = max_nominee.to_inexact()
            }
        }
        (Precision::Inexact(_), Precision::Absent)
        | (Precision::Exact(_), Precision::Absent) => {
            max_values[index] = max_values[index].clone().to_inexact()
        }
        (Precision::Absent, Precision::Exact(_))
        | (Precision::Absent, Precision::Inexact(_)) => {
            max_values[index] = max_nominee.to_inexact()
        }
        (Precision::Absent, Precision::Absent) => max_values[index] = Precision::Absent,
    }
}

/// If the given value is numerically lesser than the original minimum value,
/// set the new minimum value with appropriate exactness information.
fn set_min_if_lesser(
    min_values: &mut [Precision<ScalarValue>],
    min_nominee: Precision<ScalarValue>,
    index: usize,
) {
    match (&min_values[index], &min_nominee) {
        (Precision::Exact(val1), Precision::Exact(val2)) => {
            if val1 > val2 {
                min_values[index] = min_nominee;
            }
        }
        (Precision::Exact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Inexact(val2))
        | (Precision::Inexact(val1), Precision::Exact(val2)) => {
            if val1 > val2 {
                min_values[index] = min_nominee.to_inexact()
            }
        }
        (Precision::Inexact(_), Precision::Absent)
        | (Precision::Exact(_), Precision::Absent) => {
            min_values[index] = min_values[index].clone().to_inexact()
        }
        (Precision::Absent, Precision::Exact(_))
        | (Precision::Absent, Precision::Inexact(_)) => {
            min_values[index] = min_nominee.to_inexact()
        }
        (Precision::Absent, Precision::Absent) => min_values[index] = Precision::Absent,
    }
}
