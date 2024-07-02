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
use arrow::{array::ArrayRef, datatypes::SchemaRef};
use arrow_schema::Schema;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{split_conjunction, PhysicalExpr};
use log::{debug, trace};
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    file::{
        metadata::{ParquetMetaData, RowGroupMetaData},
        page_index::index::Index,
    },
    format::PageLocation,
};
use std::collections::HashSet;
use std::sync::Arc;

use super::parquet_to_arrow_decimal_type;
use super::statistics::{
    from_bytes_to_i128, parquet_column,
};
use super::access_plan::ParquetAccessPlan;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};

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
/// Using `B = 'F'`: can rule out all values in Page 3 and Page 5 (rows 0 -> 99, and 250 -> 299)
///
/// So we can entirely skip rows 0->199 and 250->299 as we know they
/// can not contain rows that match the predicate.
#[derive(Debug)]
pub struct PagePruningPredicate {
    predicates: Vec<PruningPredicate>,
}

impl PagePruningPredicate {
    /// Create a new [`PagePruningPredicate`]
    // TODO: this is infallaible -- it can not return an error
    pub fn try_new(expr: &Arc<dyn PhysicalExpr>, schema: SchemaRef) -> Result<Self> {
        let predicates = split_conjunction(expr)
            .into_iter()
            .filter_map(|predicate| {
                match PruningPredicate::try_new(predicate.clone(), schema.clone()) {
                    Ok(p)
                        if (!p.always_true())
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

    /// Returns an updated [`ParquetAccessPlan`] by applying predicates to the
    /// parquet page index, if any
    pub fn prune_plan_with_page_index(
        &self,
        mut access_plan: ParquetAccessPlan,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        file_metadata: &ParquetMetaData,
        file_metrics: &ParquetFileMetrics,
    ) -> ParquetAccessPlan {
        // scoped timer updates on drop
        let _timer_guard = file_metrics.page_index_eval_time.timer();
        if self.predicates.is_empty() {
            return access_plan;
        }

        let page_index_predicates = &self.predicates;
        let groups = file_metadata.row_groups();

        if groups.is_empty() {
            return access_plan;
        }

        let (Some(file_offset_indexes), Some(file_page_indexes)) =
            (file_metadata.offset_index(), file_metadata.column_index())
        else {
            trace!(
                    "skip page pruning due to lack of indexes. Have offset: {}, column index: {}",
                    file_metadata.offset_index().is_some(), file_metadata.column_index().is_some()
                );
            return access_plan;
        };

        // track the total number of rows that should be skipped
        let mut total_skip = 0;

        let row_group_indexes = access_plan.row_group_indexes();
        for r in row_group_indexes {
            // The selection for this particular row group
            let mut overall_selection = None;
            for predicate in page_index_predicates {
                // find column index in the parquet schema
                let col_idx = find_column_index(predicate, arrow_schema, parquet_schema);
                let row_group_metadata = &groups[r];

                let (Some(rg_page_indexes), Some(rg_offset_indexes), Some(col_idx)) = (
                    file_page_indexes.get(r),
                    file_offset_indexes.get(r),
                    col_idx,
                ) else {
                    trace!(
                        "Did not have enough metadata to prune with page indexes, \
                     falling back to all rows",
                    );
                    continue;
                };

                let selection = prune_pages_in_one_row_group(
                    row_group_metadata,
                    predicate,
                    rg_offset_indexes.get(col_idx),
                    rg_page_indexes.get(col_idx),
                    groups[r].column(col_idx).column_descr(),
                    file_metrics,
                );

                let Some(selection) = selection else {
                    trace!("No pages pruned in prune_pages_in_one_row_group");
                    continue;
                };

                debug!("Use filter and page index to create RowSelection {:?} from predicate: {:?}",
                    &selection,
                    predicate.predicate_expr(),
                );

                overall_selection = update_selection(overall_selection, selection);

                // if the overall selection has ruled out all rows, no need to
                // continue with the other predicates
                let selects_any = overall_selection
                    .as_ref()
                    .map(|selection| selection.selects_any())
                    .unwrap_or(true);

                if !selects_any {
                    break;
                }
            }

            if let Some(overall_selection) = overall_selection {
                if overall_selection.selects_any() {
                    let rows_skipped = rows_skipped(&overall_selection);
                    trace!("Overall selection from predicate skipped {rows_skipped}: {overall_selection:?}");
                    total_skip += rows_skipped;
                    access_plan.scan_selection(r, overall_selection)
                } else {
                    // Selection skips all rows, so skip the entire row group
                    let rows_skipped = groups[r].num_rows() as usize;
                    access_plan.skip(r);
                    total_skip += rows_skipped;
                    trace!(
                        "Overall selection from predicate is empty, \
                        skipping all {rows_skipped} rows in row group {r}"
                    );
                }
            }
        }

        file_metrics.page_index_rows_filtered.add(total_skip);
        access_plan
    }

    /// Returns the number of filters in the [`PagePruningPredicate`]
    pub fn filter_number(&self) -> usize {
        self.predicates.len()
    }
}

/// returns the number of rows skipped in the selection
/// TODO should this be upstreamed to RowSelection?
fn rows_skipped(selection: &RowSelection) -> usize {
    selection
        .iter()
        .fold(0, |acc, x| if x.skip { acc + x.row_count } else { acc })
}

fn update_selection(
    current_selection: Option<RowSelection>,
    row_selection: RowSelection,
) -> Option<RowSelection> {
    match current_selection {
        None => Some(row_selection),
        Some(current_selection) => Some(current_selection.intersection(&row_selection)),
    }
}

/// Returns the column index in the row parquet schema for the single
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
/// that `extract_page_index_push_down_predicates` only returns
/// predicate with one col)
fn find_column_index(
    predicate: &PruningPredicate,
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
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

    let Some(column) = found_required_column.as_ref() else {
        trace!("No column references in pruning predicate");
        return None;
    };

    parquet_column(parquet_schema, arrow_schema, column.name()).map(|x| x.0)
}

/// Returns a `RowSelection` for the pages in this RowGroup if any
/// rows can be pruned based on the page index
fn prune_pages_in_one_row_group(
    group: &RowGroupMetaData,
    predicate: &PruningPredicate,
    col_offset_indexes: Option<&Vec<PageLocation>>,
    col_page_indexes: Option<&Index>,
    col_desc: &ColumnDescriptor,
    metrics: &ParquetFileMetrics,
) -> Option<RowSelection> {
    let num_rows = group.num_rows() as usize;
    let (Some(col_offset_indexes), Some(col_page_indexes)) =
        (col_offset_indexes, col_page_indexes)
    else {
        return None;
    };

    let target_type = parquet_to_arrow_decimal_type(col_desc);
    let pruning_stats = PagesPruningStatistics {
        col_page_indexes,
        col_offset_indexes,
        target_type: &target_type,
        num_rows_in_row_group: group.num_rows(),
    };

    let values = match predicate.prune(&pruning_stats) {
        Ok(values) => values,
        Err(e) => {
            // stats filter array could not be built
            // return a result which will not filter out any pages
            debug!("Error evaluating page index predicate values {e}");
            metrics.predicate_evaluation_errors.add(1);
            return None;
        }
    };

    let mut vec = Vec::with_capacity(values.len());
    let row_vec = create_row_count_in_each_page(col_offset_indexes, num_rows);
    assert_eq!(row_vec.len(), values.len());
    let mut sum_row = *row_vec.first().unwrap();
    let mut selected = *values.first().unwrap();
    trace!("Pruned to {:?} using {:?}", values, pruning_stats);
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
    Some(RowSelection::from(vec))
}

fn create_row_count_in_each_page(
    location: &[PageLocation],
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
    num_rows_in_row_group: i64,
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

    fn row_counts(&self, _column: &datafusion_common::Column) -> Option<ArrayRef> {
        // see https://github.com/apache/arrow-rs/blob/91f0b1771308609ca27db0fb1d2d49571b3980d8/parquet/src/file/metadata.rs#L979-L982

        let row_count_per_page = self.col_offset_indexes.windows(2).map(|location| {
            Some(location[1].first_row_index - location[0].first_row_index)
        });

        // append the last page row count
        let row_count_per_page = row_count_per_page.chain(std::iter::once(Some(
            self.num_rows_in_row_group
                - self.col_offset_indexes.last().unwrap().first_row_index,
        )));

        Some(Arc::new(Int64Array::from_iter(row_count_per_page)))
    }

    fn contained(
        &self,
        _column: &datafusion_common::Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}
