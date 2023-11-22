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

//! [`StatisticsAggregator`] for aggregating multiple statistics together

use std::collections::HashMap;
use std::fmt::Debug;

use crate::error::_internal_err;
use crate::stats::Precision;
use crate::ColumnStatistics;
use crate::{DataFusionError, Result};
use crate::{ScalarValue, Statistics};
use arrow::datatypes::Schema;

/// Aggregates one or more [`Statistics`] together, representing the result of
/// combining multiple inputs (such as individual parquet files) together.
///
/// `StatisticsAggregator` also handles "Schema Evolution", by merging
/// statistics from different schemas, where each may have a different subset of
/// columns and/or different types.
///
/// # Example Merging Different Columns
///
/// Some statistics may not be present for certain columns or fields, for
/// example if some file does not have statistics for Column B, the overall
/// statistics take this into account.
//
//
/// ```text
/// ┌─────────────────────────┐         ┌────────────┐          ┌─────────────────────────┐
/// │ ┌────────┐   ┌────────┐ │         │ ┌────────┐ │          │ ┌────────┐   ┌────────┐ │
/// │ │ max: 5 │   │ max: 9 │ │         │ │ max: 7 │ │          │ │ max: 7 │   │max: 9* │ │
/// │ │        │   │        │ │   ┃     │ │        │ │   ━━━━━  │ │        │   │        │ │
/// │ │ min: 3 │   │ min: 5 │ │ ━━╋━━   │ │ min: 1 │ │          │ │ min: 1 │   │min: 5* │ │
/// │ │        │   │        │ │   ┃     │ │        │ │   ━━━━━  │ │        │   │        │ │
/// │ └────────┘   └────────┘ │         │ └────────┘ │          │ └────────┘   └────────┘ │
/// │                         │         │            │          │                         │
/// │  Column A     Column B  │         │  Column A  │          │  Column A     Column B  │
/// └─────────────────────────┘         └────────────┘          └─────────────────────────┘
///
///        Statistics                     Statistics                    Statistics
/// ```
///
/// # Example Merging Different Types
///
/// In some cases the types of statistics may not match exactly. For example, if
/// the file schema specifies Column A is a `u64` but the statistics for one of
/// the files has a `u32` for Column A, the `u32` values will be cast to u64
/// prior to aggregation
///
/// ```text
/// ┌─────────────┐        ┌────────────┐       ┌──────────────┐
/// │┌───────────┐│        │┌──────────┐│       │ ┌────────┐   │
/// ││ max: 5u64 ││        ││max: 7u32 ││       │ │ max: 7 │   │
/// ││           ││    ┃   ││          ││  ━━━━━│ │        │   │
/// ││ min: 3u64 ││  ━━╋━━ ││min: 1u32 ││       │ │ min: 1 │   │
/// ││           ││    ┃   ││          ││  ━━━━━│ │        │   │
/// │└───────────┘│        │└──────────┘│       │ └────────┘   │
/// │             │        │            │       │              │
/// │  Column A   │        │  Column A  │       │  Column A    │
/// └─────────────┘        └────────────┘       └──────────────┘
///
///   Statistics            Statistics             Statistics
/// ```
///
/// Things to test:
/// Aggregating statistics from different types (e.g. when the table column is a float but the column is an int)
#[derive(Debug)]
pub struct StatisticsAggregator<'a> {
    num_rows: PrecisionAggregator<usize>,
    total_byte_size: PrecisionAggregator<usize>,
    column_statistics: Vec<ColumnStatisticsAggregator>,
    // Maps column name to index in column_statistics for all columns we are
    // aggregating
    col_idx_map: HashMap<&'a str, usize>,
}

impl<'a> StatisticsAggregator<'a> {
    /// Creates new aggregator the the given schema.
    ///
    /// This will start with:
    ///
    /// - 0 rows
    /// - 0 bytes
    /// - for each column:
    ///   - 0 null values
    ///   - unknown min value
    ///   - unknown max value
    /// - exact representation
    pub fn new(schema: &'a Schema) -> Self {
        let col_idx_map = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| (f.name().as_str(), idx))
            .collect::<HashMap<_, _>>();

        let column_statistics: Vec<_> = (0..col_idx_map.len())
            .map(|_| ColumnStatisticsAggregator::new())
            .collect();

        Self {
            num_rows: PrecisionAggregator::new(),
            total_byte_size: PrecisionAggregator::new(),
            column_statistics,
            col_idx_map,
        }
    }

    /// Update the StatisticsAggregator with statistics from a new file with the
    /// given schema
    ///
    /// Updates are meant to be "additive", i.e. they only add data/rows. There
    /// is NO way to remove/substract data from the accumulator.
    ///
    /// If statistics are not provided for a column, that column is assumed be
    /// entirely null (as is the result when merging schemas)
    ///
    /// Returns an error if the number of columns in the statistics and the
    /// schema are different.
    pub fn update(
        &mut self,
        update_stats: &Statistics,
        update_schema: &Schema,
    ) -> Result<()> {
        // decompose structs so we don't forget new fields added to Statistics
        let Statistics {
            num_rows: update_num_rows,
            total_byte_size: update_total_byte_size,
            column_statistics: update_column_statistics,
        } = update_stats;

        self.num_rows.update_sum(update_num_rows);
        self.total_byte_size.update_sum(update_total_byte_size);

        if update_column_statistics.len() != update_schema.fields().len() {
            return _internal_err!(
                "update_stats ({}) and schema ({}) have different column count",
                update_column_statistics.len(),
                update_schema.fields().len()
            );
        }

        let mut seen_cols = vec![false; self.col_idx_map.len()];

        for (update_field, update_col) in update_schema
            .fields()
            .iter()
            .zip(update_column_statistics.iter())
        {
            // Skip if this is some column that we are not aggregating statistics for
            let Some(idx) = self.col_idx_map.get(update_field.name().as_str()) else {
                continue;
            };
            let base_col = &mut self.column_statistics[*idx];
            seen_cols[*idx] = true;

            // decompose structs so we don't forget new fields
            let ColumnStatisticsAggregator {
                null_count: base_null_count,
                min_value: base_min_value,
                max_value: base_max_value,
            } = base_col;
            let ColumnStatistics {
                null_count: update_null_count,
                min_value: update_min_value,
                max_value: update_max_value,
                distinct_count: _update_distinct_count,
            } = update_col;

            base_null_count.update_sum(update_null_count);
            base_min_value.update_min(update_min_value);
            base_max_value.update_max(update_max_value);
        }

        // for any columns that did not appear in the update, they are all null
        // they were all-NULL and hence we can update  the null counters
        for (used, base_col) in seen_cols.into_iter().zip(&mut self.column_statistics) {
            if !used {
                base_col.null_count.update_sum(update_num_rows);
            }
        }
        Ok(())
    }

    /// Returns the currently known number of rows, if any
    pub fn num_rows(&self) -> Precision<usize> {
        self.num_rows.clone().into_precision()
    }

    /// Build aggregated statistics.
    pub fn build(self) -> Statistics {
        let Self {
            num_rows,
            total_byte_size,
            column_statistics,
            col_idx_map: _,
        } = self;

        Statistics {
            num_rows: num_rows.into_precision(),
            total_byte_size: total_byte_size.into_precision(),
            column_statistics: column_statistics
                .into_iter()
                .map(|col_agg| col_agg.into_column_statistics())
                .collect(),
        }
    }
}

/// Tracks the difference between "never seen" and "initialized to zero"
///
/// 1. "uninitialized" (`None`)
/// 1. "initialized" (`Some(Precision::Exact(...))`)
/// 2. "initialized but invalid" (`Some(Precision::Absent)`).
#[derive(Debug, Clone)]
struct PrecisionAggregator<T: Debug + Clone + PartialEq + Eq + PartialOrd> {
    inner: Option<Precision<T>>,
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> PrecisionAggregator<T> {
    fn new() -> Self {
        Default::default()
    }

    fn into_precision(self) -> Precision<T> {
        self.inner.unwrap_or(Precision::Absent)
    }
}

impl PrecisionAggregator<usize> {
    /// Adds the `other` precision, distinguishing between "never seen a value"
    /// (None) vs "we didn't know the value" (Some(Absent))
    fn update_sum(&mut self, other: &Precision<usize>) {
        self.inner = Some(
            self.inner
                .take()
                .map(|value| value.add(other))
                .unwrap_or_else(|| other.clone()),
        );
    }
}

impl PrecisionAggregator<ScalarValue> {
    /// Returns the min of self and `other`, distinguishing between "never seen
    /// a value" (None) vs "we didn't know the value" (Some(Absent))
    fn update_min(&mut self, other: &Precision<ScalarValue>) {
        self.inner = Some(
            self.inner
                .take()
                .map(|inner| inner.min(other))
                .unwrap_or_else(|| other.clone()),
        );
    }

    /// Returns the max of self and `other`, distinguishing between "never seen
    /// a value" (None) vs "we didn't know the value" (Some(Absent))
    fn update_max(&mut self, other: &Precision<ScalarValue>) {
        self.inner = Some(
            self.inner
                .take()
                .map(|inner| inner.max(other))
                .unwrap_or_else(|| other.clone()),
        );
    }
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd> Default for PrecisionAggregator<T> {
    fn default() -> Self {
        Self { inner: None }
    }
}

/// Aggregates multiple [`ColumnStatistics`]
///
/// Note this does does NOT contain a distinct count because we cannot aggregate
/// these.
#[derive(Debug, Default)]
struct ColumnStatisticsAggregator {
    null_count: PrecisionAggregator<usize>,
    min_value: PrecisionAggregator<ScalarValue>,
    max_value: PrecisionAggregator<ScalarValue>,
}
impl ColumnStatisticsAggregator {
    fn new() -> Self {
        Self::default()
    }

    fn into_column_statistics(self) -> ColumnStatistics {
        let Self {
            null_count,
            min_value,
            max_value,
        } = self;

        ColumnStatistics {
            null_count: null_count.into_precision(),
            min_value: min_value.into_precision(),
            max_value: max_value.into_precision(),
            distinct_count: Precision::Absent,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_no_cols_no_updates() {
        let schema = Schema::new(Vec::<Field>::new());
        let agg = StatisticsAggregator::new(&schema);

        let actual = agg.build();
        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&schema),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_no_updates() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let agg = StatisticsAggregator::new(&schema);

        let actual = agg.build();
        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                // col1
                ColumnStatistics {
                    null_count: Precision::Absent,
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
                // col2
                ColumnStatistics {
                    null_count: Precision::Absent,
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_valid_update_partial() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let mut agg = StatisticsAggregator::new(&schema);

        let update_schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let update_stats = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                // col1
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    distinct_count: Precision::Exact(42),
                },
                // col2
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    distinct_count: Precision::Exact(42),
                },
            ],
        };
        agg.update(&update_stats, &update_schema).unwrap();

        let update_schema = Schema::new(vec![Field::new("col2", DataType::Utf8, false)]);
        let update_stats = Statistics {
            num_rows: Precision::Exact(10_000),
            total_byte_size: Precision::Exact(100_000),
            column_statistics: vec![
                // col2
                ColumnStatistics {
                    null_count: Precision::Exact(1_000_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("c".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    distinct_count: Precision::Exact(42),
                },
            ],
        };
        agg.update(&update_stats, &update_schema).unwrap();

        let actual = agg.build();
        let expected = Statistics {
            num_rows: Precision::Exact(10_001),
            total_byte_size: Precision::Exact(100_010),
            column_statistics: vec![
                // col1
                ColumnStatistics {
                    null_count: Precision::Exact(10100),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    distinct_count: Precision::Absent,
                },
                // col2
                ColumnStatistics {
                    null_count: Precision::Exact(1_001_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    distinct_count: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_valid_update_col_reorder() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let mut agg = StatisticsAggregator::new(&schema);

        let update_schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let update_stats = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    distinct_count: Precision::Exact(42),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    distinct_count: Precision::Exact(42),
                },
            ],
        };
        agg.update(&update_stats, &update_schema).unwrap();

        let update_schema = Schema::new(vec![
            Field::new("col2", DataType::Utf8, false),
            Field::new("col1", DataType::UInt64, true),
        ]);
        let update_stats = Statistics {
            num_rows: Precision::Exact(10_000),
            total_byte_size: Precision::Exact(100_000),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1_000_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("c".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    distinct_count: Precision::Exact(42),
                },
                ColumnStatistics {
                    null_count: Precision::Exact(10_000_000),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(40))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(99))),
                    distinct_count: Precision::Exact(42),
                },
            ],
        };
        agg.update(&update_stats, &update_schema).unwrap();

        let actual = agg.build();
        let expected = Statistics {
            num_rows: Precision::Exact(10_001),
            total_byte_size: Precision::Exact(100_010),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(10_000_100),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(40))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    distinct_count: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_001_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    distinct_count: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_ignores_unknown_cols() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let mut agg = StatisticsAggregator::new(&schema);

        let update_schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col3", DataType::Utf8, false),
        ]);
        let update_stats = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                // col1
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    distinct_count: Precision::Exact(42),
                },
                // col3
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    distinct_count: Precision::Exact(42),
                },
            ],
        };
        agg.update(&update_stats, &update_schema).unwrap();

        let actual = agg.build();
        let expected = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                // col1
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    distinct_count: Precision::Absent,
                },
                // col2
                ColumnStatistics {
                    null_count: Precision::Exact(1),
                    min_value: Precision::Absent,
                    max_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_num_rows_exact() {
        let (schema, update_stats) = num_rows_schema_and_stats();
        let mut agg = StatisticsAggregator::new(&schema);
        assert_eq!(agg.num_rows(), Precision::Absent);
        agg.update(&update_stats, &schema).unwrap();
        assert_eq!(agg.num_rows(), Precision::Exact(1));
        agg.update(&update_stats, &schema).unwrap();
        assert_eq!(agg.num_rows(), Precision::Exact(2));
    }

    #[test]
    fn test_num_rows_inexact() {
        let (schema, mut update_stats) = num_rows_schema_and_stats();
        let mut agg = StatisticsAggregator::new(&schema);
        agg.update(&update_stats, &schema).unwrap();
        // add inexact stats
        update_stats.num_rows = Precision::Inexact(1);
        agg.update(&update_stats, &schema).unwrap();
        assert_eq!(agg.num_rows(), Precision::Inexact(2));
    }

    #[test]
    fn test_num_rows_absent() {
        let (schema, mut update_stats) = num_rows_schema_and_stats();
        let mut agg = StatisticsAggregator::new(&schema);
        agg.update(&update_stats, &schema).unwrap();
        // add absent stats
        update_stats.num_rows = Precision::Absent;
        agg.update(&update_stats, &schema).unwrap();
        assert_eq!(agg.num_rows(), Precision::Inexact(1));
    }

    /// Returns a schema and stats with `Precision::Exact(1)` rows for testing num_rows
    fn num_rows_schema_and_stats() -> (Schema, Statistics) {
        let schema = Schema::new(vec![Field::new("col1", DataType::UInt64, true)]);

        let stats = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown()],
        };
        (schema, stats)
    }

    #[test]
    fn test_invalidation_base() {
        let test = AggTest::new();
        test.run()
    }

    #[test]
    fn test_invalidation_num_rows() {
        let mut test = AggTest::new();
        // absent stats makes inexact precision
        test.stats1.num_rows = Precision::Absent;
        test.expected.num_rows = Precision::Inexact(2); // value of first stats
        test.run()
    }

    #[test]
    fn test_invalidation_num_rows_on_update() {
        let mut test = AggTest::new();
        // absent stats makes inexact precision
        test.stats2.num_rows = Precision::Absent;
        test.expected.num_rows = Precision::Inexact(1); // value in second stats
        test.run()
    }

    #[test]
    fn test_inexact_num_rows_on_update() {
        let mut test = AggTest::new();
        // absent stats makes inexact precision
        test.stats2.num_rows = Precision::Inexact(78);
        test.expected.num_rows = Precision::Inexact(79); // value in second stats + 78
        test.run()
    }

    #[test]
    fn test_invalidation_total_byte_size_on_update() {
        let mut test = AggTest::new();
        // absent stats on update makes inexact precision
        test.stats2.total_byte_size = Precision::Absent;
        test.expected.total_byte_size = Precision::Inexact(10); // value in first stats
        test.run()
    }

    #[test]
    fn test_invalidation_column_null_count_on_update() {
        let mut test = AggTest::new();
        // absent stats on update makes inexact precision
        test.stats2.column_statistics[0].null_count = Precision::Absent;
        test.expected.column_statistics[0].null_count = Precision::Inexact(100); // value in first stats
        test.run()
    }

    #[test]
    fn test_invalidation_column_min_on_update() {
        let mut test = AggTest::new();
        // absent stats on update makes inexact precision
        test.stats2.column_statistics[0].min_value = Precision::Absent;
        test.expected.column_statistics[0].min_value = inexact_u64(7); // value in first stats
        test.run()
    }

    #[test]
    fn test_inexact_column_min_on_update() {
        let mut test = AggTest::new();
        // absent stats on update makes inexact precision
        test.stats2.column_statistics[0].min_value = inexact_u64(3);
        test.expected.column_statistics[0].min_value = inexact_u64(3); // inexact value
        test.run()
    }

    #[test]
    fn test_invalidation_column_max_on_update() {
        let mut test = AggTest::new();
        // absent stats on update makes inexact precision
        test.stats2.column_statistics[0].max_value = Precision::Absent;
        test.expected.column_statistics[0].max_value = inexact_u64(11); // value in first stats
        test.run()
    }

    #[test]
    fn test_inexact_column_max_on_update() {
        let mut test = AggTest::new();
        // absent stats on update makes inexact precision
        test.stats2.column_statistics[0].max_value = inexact_u64(3000);
        test.expected.column_statistics[0].max_value = inexact_u64(3000); // inexact value
        test.run()
    }

    fn exact_u64(value: u64) -> Precision<ScalarValue> {
        Precision::Exact(ScalarValue::UInt64(Some(value)))
    }

    fn inexact_u64(value: u64) -> Precision<ScalarValue> {
        Precision::Inexact(ScalarValue::UInt64(Some(value)))
    }

    /// Aggregates stats1 and stats2 and compares the result with the expected statistics.
    /// #[derive(Debug, Clone)]
    struct AggTest {
        schema: Schema,
        stats1: Statistics,
        stats2: Statistics,
        expected: Statistics,
    }

    impl AggTest {
        fn run(self) {
            let Self {
                schema,
                stats1,
                stats2,
                expected,
            } = self;
            let mut stats_agg = StatisticsAggregator::new(&schema);
            stats_agg.update(&stats1, &schema).unwrap();
            stats_agg.update(&stats2, &schema).unwrap();
            let result = stats_agg.build();
            assert_eq!(result, expected);
        }

        /// Creates an `AggTest` that aggregates two single column statistics together
        fn new() -> Self {
            Self {
                schema: Schema::new(vec![Field::new("col1", DataType::UInt64, true)]),
                stats1: Statistics {
                    num_rows: Precision::Exact(1),
                    total_byte_size: Precision::Exact(10),
                    column_statistics: vec![
                        // col1
                        ColumnStatistics {
                            null_count: Precision::Exact(100),
                            min_value: exact_u64(7),
                            max_value: exact_u64(11),
                            distinct_count: Precision::Exact(3),
                        },
                    ],
                },
                stats2: Statistics {
                    num_rows: Precision::Exact(2),
                    total_byte_size: Precision::Exact(20),
                    column_statistics: vec![
                        // col1
                        ColumnStatistics {
                            null_count: Precision::Exact(200),
                            min_value: exact_u64(15),
                            max_value: exact_u64(19),
                            distinct_count: Precision::Exact(5),
                        },
                    ],
                },

                expected: Statistics {
                    num_rows: Precision::Exact(3),
                    total_byte_size: Precision::Exact(30),
                    column_statistics: vec![
                        // col1
                        ColumnStatistics {
                            null_count: Precision::Exact(300),
                            min_value: exact_u64(7),
                            max_value: exact_u64(19),
                            distinct_count: Precision::Absent,
                        },
                    ],
                },
            }
        }
    }

    #[test]
    #[should_panic(expected = "stats (0) and schema (1) have different column count")]
    fn test_asserts_schema_stats_match() {
        let schema = Schema::new(vec![Field::new("col1", DataType::UInt64, true)]);
        let mut agg = StatisticsAggregator::new(&schema);

        let update_schema = Schema::new(vec![Field::new("col1", DataType::UInt64, true)]);
        let update_stats = Statistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![],
        };
        agg.update(&update_stats, &update_schema).unwrap();
    }
}
