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

//! Contains code to filter entire pages

use arrow::array::{
    BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::DataType;
use arrow::{array::ArrayRef, datatypes::SchemaRef, error::ArrowError};
use datafusion_common::{DataFusionError, Result};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{split_conjunction, PhysicalExpr};
use log::{debug, trace};
use parquet::schema::types::ColumnDescriptor;
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, RowGroupMetaData},
        page_index::index::Index,
    },
    format::PageLocation,
};
use std::sync::Arc;

use crate::datasource::physical_plan::parquet::parquet_to_arrow_decimal_type;
use crate::datasource::physical_plan::parquet::statistics::from_bytes_to_i128;
use crate::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

use super::metrics::ParquetFileMetrics;

/// A [`PagePruningPredicate`] provides the ability to construct a [`RowSelection`]
/// based on parquet page level statistics, if any
///
/// For example, given a row group with two column (chunks) for `A`
/// and `B` with the following with page level statistics:
///
/// ```text
/// ┏━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━
///    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ┃
/// ┃     ┌──────────────┐  │     ┌──────────────┐  │  ┃
/// ┃  │  │              │     │  │              │     ┃
/// ┃     │              │  │     │     Page     │  │
///    │  │              │     │  │      3       │     ┃
/// ┃     │              │  │     │   min: "A"   │  │  ┃
/// ┃  │  │              │     │  │   max: "C"   │     ┃
/// ┃     │     Page     │  │     │ first_row: 0 │  │
///    │  │      1       │     │  │              │     ┃
/// ┃     │   min: 10    │  │     └──────────────┘  │  ┃
/// ┃  │  │   max: 20    │     │  ┌──────────────┐     ┃
/// ┃     │ first_row: 0 │  │     │              │  │
///    │  │              │     │  │     Page     │     ┃
/// ┃     │              │  │     │      4       │  │  ┃
/// ┃  │  │              │     │  │   min: "D"   │     ┃
/// ┃     │              │  │     │   max: "G"   │  │
///    │  │              │     │  │first_row: 100│     ┃
/// ┃     └──────────────┘  │     │              │  │  ┃
/// ┃  │  ┌──────────────┐     │  │              │     ┃
/// ┃     │              │  │     └──────────────┘  │
///    │  │     Page     │     │  ┌──────────────┐     ┃
/// ┃     │      2       │  │     │              │  │  ┃
/// ┃  │  │   min: 30    │     │  │     Page     │     ┃
/// ┃     │   max: 40    │  │     │      5       │  │
///    │  │first_row: 200│     │  │   min: "H"   │     ┃
/// ┃     │              │  │     │   max: "Z"   │  │  ┃
/// ┃  │  │              │     │  │first_row: 250│     ┃
/// ┃     └──────────────┘  │     │              │  │
///    │                       │  └──────────────┘     ┃
/// ┃   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ┃
/// ┃       ColumnChunk            ColumnChunk         ┃
/// ┃            A                      B
///  ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━━ ━━┛
///
///   Total rows: 300
///
/// ```
///
/// Given the predicate `A > 35 AND B = 'F'`:
///
/// Using `A > 35`: can rule out all of values in Page 1 (rows 0 -> 199)
///
/// Using `B = 'F'`: can rule out all vaues in Page 3 and Page 5 (rows 0 -> 99, and 250 -> 299)
///
/// So we can entirely skip rows 0->199 and 250->299 as we know they
/// can not contain rows that match the predicate.
#[derive(Debug)]
pub struct PagePruningPredicate {
    predicates: Vec<PruningPredicate>,
}

impl PagePruningPredicate {
    /// Create a new [`PagePruningPredicate`]
    pub fn try_new(expr: &Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Result<Self> {
        let predicates = split_conjunction(expr)
            .into_iter()
            .filter_map(|predicate| {
                match PruningPredicate::try_new(predicate.clone(), schema.clone()) {
                    Ok(p)
                        if (!p.allways_true())
                            && (p.required_columns().n_columns() < 2) =>
                    {
                        Some(Ok(p))
                    }
                    _ => None,
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { predicates })
    }

    /// Returns a [`RowSelection`] for the given file
    pub fn prune(
        &self,
        row_groups: &[usize],
        file_metadata: &ParquetMetaData,
        file_metrics: &ParquetFileMetrics,
    ) -> Result<Option<RowSelection>> {
        // scoped timer updates on drop
        let _timer_guard = file_metrics.page_index_eval_time.timer();
        if self.predicates.is_empty() {
            return Ok(None);
        }

        let page_index_predicates = &self.predicates;
        let groups = file_metadata.row_groups();

        if groups.is_empty() {
            return Ok(None);
        }

        let file_offset_indexes = file_metadata.offset_index();
        let file_page_indexes = file_metadata.column_index();
        let (file_offset_indexes, file_page_indexes) = match (
            file_offset_indexes,
            file_page_indexes,
        ) {
            (Some(o), Some(i)) => (o, i),
            _ => {
                trace!(
                    "skip page pruning due to lack of indexes. Have offset: {}, column index: {}",
                    file_offset_indexes.is_some(), file_page_indexes.is_some()
                );
                return Ok(None);
            }
        };

        let mut row_selections = Vec::with_capacity(page_index_predicates.len());
        for predicate in page_index_predicates {
            // find column index by looking in the row group metadata.
            let col_idx = find_column_index(predicate, &groups[0]);

            let mut selectors = Vec::with_capacity(row_groups.len());
            for r in row_groups.iter() {
                let row_group_metadata = &groups[*r];

                let rg_offset_indexes = file_offset_indexes.get(*r);
                let rg_page_indexes = file_page_indexes.get(*r);
                if let (Some(rg_page_indexes), Some(rg_offset_indexes), Some(col_idx)) =
                    (rg_page_indexes, rg_offset_indexes, col_idx)
                {
                    selectors.extend(
                        prune_pages_in_one_row_group(
                            row_group_metadata,
                            predicate,
                            rg_offset_indexes.get(col_idx),
                            rg_page_indexes.get(col_idx),
                            groups[*r].column(col_idx).column_descr(),
                            file_metrics,
                        )
                        .map_err(|e| {
                            ArrowError::ParquetError(format!(
                                "Fail in prune_pages_in_one_row_group: {e}"
                            ))
                        }),
                    );
                } else {
                    trace!(
                        "Did not have enough metadata to prune with page indexes, \
                         falling back to all rows",
                    );
                    // fallback select all rows
                    let all_selected =
                        vec![RowSelector::select(groups[*r].num_rows() as usize)];
                    selectors.push(all_selected);
                }
            }
            debug!(
                "Use filter and page index create RowSelection {:?} from predicate: {:?}",
                &selectors,
                predicate.predicate_expr(),
            );
            row_selections.push(selectors.into_iter().flatten().collect::<Vec<_>>());
        }

        let final_selection = combine_multi_col_selection(row_selections);
        let total_skip =
            final_selection.iter().fold(
                0,
                |acc, x| {
                    if x.skip {
                        acc + x.row_count
                    } else {
                        acc
                    }
                },
            );
        file_metrics.page_index_rows_filtered.add(total_skip);
        Ok(Some(final_selection))
    }

    /// Returns the number of filters in the [`PagePruningPredicate`]
    pub fn filter_number(&self) -> usize {
        self.predicates.len()
    }
}

/// Returns the column index in the row group metadata for the single
/// column of a single column pruning predicate.
///
/// For example, give the predicate `y > 5`
///
/// And columns in the RowGroupMetadata like `['x', 'y', 'z']` will
/// return 1.
///
/// Returns `None` if the column is not found, or if there are no
/// required columns, which is the case for predicate like `abs(i) =
/// 1` which are rewritten to `lit(true)`
///
/// Panics:
///
/// If the predicate contains more than one column reference (assumes
/// that `extract_page_index_push_down_predicates` only return
/// predicate with one col)
///
fn find_column_index(
    predicate: &PruningPredicate,
    row_group_metadata: &RowGroupMetaData,
) -> Option<usize> {
    let mut found_required_column: Option<&Column> = None;

    for required_column_details in predicate.required_columns().iter() {
        let column = &required_column_details.0;
        if let Some(found_required_column) = found_required_column.as_ref() {
            // make sure it is the same name we have seen previously
            assert_eq!(
                column.name(),
                found_required_column.name(),
                "Unexpected multi column predicate"
            );
        } else {
            found_required_column = Some(column);
        }
    }

    let column = if let Some(found_required_column) = found_required_column.as_ref() {
        found_required_column
    } else {
        trace!("No column references in pruning predicate");
        return None;
    };

    let col_idx = row_group_metadata
        .columns()
        .iter()
        .enumerate()
        .find(|(_idx, c)| c.column_descr().name() == column.name())
        .map(|(idx, _c)| idx);

    if col_idx.is_none() {
        trace!("Can not find column {} in row group meta", column.name());
    }

    col_idx
}

/// Intersects the [`RowSelector`]s
///
/// For exampe, given:
/// * `RowSelector1: [ Skip(0~199), Read(200~299)]`
/// * `RowSelector2: [ Skip(0~99), Read(100~249), Skip(250~299)]`
///
/// The final selection is the intersection of these  `RowSelector`s:
/// * `final_selection:[ Skip(0~199), Read(200~249), Skip(250~299)]`
fn combine_multi_col_selection(row_selections: Vec<Vec<RowSelector>>) -> RowSelection {
    row_selections
        .into_iter()
        .map(RowSelection::from)
        .reduce(|s1, s2| s1.intersection(&s2))
        .unwrap()
}

fn prune_pages_in_one_row_group(
    group: &RowGroupMetaData,
    predicate: &PruningPredicate,
    col_offset_indexes: Option<&Vec<PageLocation>>,
    col_page_indexes: Option<&Index>,
    col_desc: &ColumnDescriptor,
    metrics: &ParquetFileMetrics,
) -> Result<Vec<RowSelector>> {
    let num_rows = group.num_rows() as usize;
    if let (Some(col_offset_indexes), Some(col_page_indexes)) =
        (col_offset_indexes, col_page_indexes)
    {
        let target_type = parquet_to_arrow_decimal_type(col_desc);
        let pruning_stats = PagesPruningStatistics {
            col_page_indexes,
            col_offset_indexes,
            target_type: &target_type,
        };

        match predicate.prune(&pruning_stats) {
            Ok(values) => {
                let mut vec = Vec::with_capacity(values.len());
                let row_vec = create_row_count_in_each_page(col_offset_indexes, num_rows);
                assert_eq!(row_vec.len(), values.len());
                let mut sum_row = *row_vec.first().unwrap();
                let mut selected = *values.first().unwrap();
                trace!("Pruned to to {:?} using {:?}", values, pruning_stats);
                for (i, &f) in values.iter().enumerate().skip(1) {
                    if f == selected {
                        sum_row += *row_vec.get(i).unwrap();
                    } else {
                        let selector = if selected {
                            RowSelector::select(sum_row)
                        } else {
                            RowSelector::skip(sum_row)
                        };
                        vec.push(selector);
                        sum_row = *row_vec.get(i).unwrap();
                        selected = f;
                    }
                }

                let selector = if selected {
                    RowSelector::select(sum_row)
                } else {
                    RowSelector::skip(sum_row)
                };
                vec.push(selector);
                return Ok(vec);
            }
            // stats filter array could not be built
            // return a result which will not filter out any pages
            Err(e) => {
                debug!("Error evaluating page index predicate values {e}");
                metrics.predicate_evaluation_errors.add(1);
                return Ok(vec![RowSelector::select(group.num_rows() as usize)]);
            }
        }
    }
    Err(DataFusionError::ParquetError(ParquetError::General(
        "Got some error in prune_pages_in_one_row_group, plz try open the debuglog mode"
            .to_string(),
    )))
}

fn create_row_count_in_each_page(
    location: &Vec<PageLocation>,
    num_rows: usize,
) -> Vec<usize> {
    let mut vec = Vec::with_capacity(location.len());
    location.windows(2).for_each(|x| {
        let start = x[0].first_row_index as usize;
        let end = x[1].first_row_index as usize;
        vec.push(end - start);
    });
    vec.push(num_rows - location.last().unwrap().first_row_index as usize);
    vec
}

/// Wraps one col page_index in one rowGroup statistics in a way
/// that implements [`PruningStatistics`]
#[derive(Debug)]
struct PagesPruningStatistics<'a> {
    col_page_indexes: &'a Index,
    col_offset_indexes: &'a Vec<PageLocation>,
    // target_type means the logical type in schema: like 'DECIMAL' is the logical type, but the
    // real physical type in parquet file may be `INT32, INT64, FIXED_LEN_BYTE_ARRAY`
    target_type: &'a Option<DataType>,
}

// Extract the min or max value calling `func` from page idex
macro_rules! get_min_max_values_for_page_index {
    ($self:expr, $func:ident) => {{
        match $self.col_page_indexes {
            Index::NONE => None,
            Index::INT32(index) => {
                match $self.target_type {
                    // int32 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        let vec = &index.indexes;
                        let vec: Vec<Option<i128>> = vec
                            .iter()
                            .map(|x| x.$func().and_then(|x| Some(*x as i128)))
                            .collect();
                        Decimal128Array::from(vec)
                            .with_precision_and_scale(*precision, *scale)
                            .ok()
                            .map(|arr| Arc::new(arr) as ArrayRef)
                    }
                    _ => {
                        let vec = &index.indexes;
                        Some(Arc::new(Int32Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                }
            }
            Index::INT64(index) => {
                match $self.target_type {
                    // int64 to decimal with the precision and scale
                    Some(DataType::Decimal128(precision, scale)) => {
                        let vec = &index.indexes;
                        let vec: Vec<Option<i128>> = vec
                            .iter()
                            .map(|x| x.$func().and_then(|x| Some(*x as i128)))
                            .collect();
                        Decimal128Array::from(vec)
                            .with_precision_and_scale(*precision, *scale)
                            .ok()
                            .map(|arr| Arc::new(arr) as ArrayRef)
                    }
                    _ => {
                        let vec = &index.indexes;
                        Some(Arc::new(Int64Array::from_iter(
                            vec.iter().map(|x| x.$func().cloned()),
                        )))
                    }
                }
            }
            Index::FLOAT(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Float32Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::DOUBLE(index) => {
                let vec = &index.indexes;
                Some(Arc::new(Float64Array::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::BOOLEAN(index) => {
                let vec = &index.indexes;
                Some(Arc::new(BooleanArray::from_iter(
                    vec.iter().map(|x| x.$func().cloned()),
                )))
            }
            Index::BYTE_ARRAY(index) => match $self.target_type {
                Some(DataType::Decimal128(precision, scale)) => {
                    let vec = &index.indexes;
                    Decimal128Array::from(
                        vec.iter()
                            .map(|x| {
                                x.$func()
                                    .and_then(|x| Some(from_bytes_to_i128(x.as_ref())))
                            })
                            .collect::<Vec<Option<i128>>>(),
                    )
                    .with_precision_and_scale(*precision, *scale)
                    .ok()
                    .map(|arr| Arc::new(arr) as ArrayRef)
                }
                _ => {
                    let vec = &index.indexes;
                    let array: StringArray = vec
                        .iter()
                        .map(|x| x.$func())
                        .map(|x| x.and_then(|x| std::str::from_utf8(x.as_ref()).ok()))
                        .collect();
                    Some(Arc::new(array))
                }
            },
            Index::INT96(_) => {
                //Todo support these type
                None
            }
            Index::FIXED_LEN_BYTE_ARRAY(index) => match $self.target_type {
                Some(DataType::Decimal128(precision, scale)) => {
                    let vec = &index.indexes;
                    Decimal128Array::from(
                        vec.iter()
                            .map(|x| {
                                x.$func()
                                    .and_then(|x| Some(from_bytes_to_i128(x.as_ref())))
                            })
                            .collect::<Vec<Option<i128>>>(),
                    )
                    .with_precision_and_scale(*precision, *scale)
                    .ok()
                    .map(|arr| Arc::new(arr) as ArrayRef)
                }
                _ => None,
            },
        }
    }};
}

impl<'a> PruningStatistics for PagesPruningStatistics<'a> {
    fn min_values(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, min)
    }

    fn max_values(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        get_min_max_values_for_page_index!(self, max)
    }

    fn num_containers(&self) -> usize {
        self.col_offset_indexes.len()
    }

    fn null_counts(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        match self.col_page_indexes {
            Index::NONE => None,
            Index::BOOLEAN(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT32(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT64(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::FLOAT(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::DOUBLE(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::INT96(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::BYTE_ARRAY(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
            Index::FIXED_LEN_BYTE_ARRAY(index) => Some(Arc::new(Int64Array::from_iter(
                index.indexes.iter().map(|x| x.null_count),
            ))),
        }
    }
}
