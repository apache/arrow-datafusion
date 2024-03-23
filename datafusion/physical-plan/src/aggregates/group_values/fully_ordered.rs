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

use crate::aggregates::group_values::GroupValues;
use crate::common::transpose;
use arrow::compute::{cast, SortColumn};
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::utils::{evaluate_partition_ranges, get_row_at_idx};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::LexOrdering;

/// A [`GroupValues`] making use of order
pub struct GroupValuesFullyOrdered {
    /// The output schema
    schema: SchemaRef,

    /// The actual group by values, stored as `Vec<ScalarValue>` for each distinct group.
    /// `group_values[i]` holds the group value for group_index `i`.
    group_values: Option<Vec<Vec<ScalarValue>>>,
    /// The ordering of the Group by expressions.
    sort_exprs: LexOrdering,
}

impl GroupValuesFullyOrdered {
    pub fn try_new(schema: SchemaRef, sort_exprs: LexOrdering) -> Result<Self> {
        Ok(Self {
            schema,
            group_values: None,
            sort_exprs,
        })
    }
}

impl GroupValues for GroupValuesFullyOrdered {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // We shouldn't receive empty group by.
        assert!(!cols.is_empty());
        // Number of group by columns, and ordering expression length should be same.
        // Otherwise mode wouldn't be FullyOrdered.
        assert_eq!(cols.len(), self.sort_exprs.len());

        // tracks to which group each of the input rows belongs
        groups.clear();

        let n_rows = cols[0].len();
        if n_rows == 0 {
            return Ok(());
        }

        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => vec![],
        };

        let sort_columns = cols
            .iter()
            .zip(&self.sort_exprs)
            .map(|(col, sort_expr)| SortColumn {
                values: col.clone(),
                options: Some(sort_expr.options),
            })
            .collect::<Vec<_>>();
        let ranges = evaluate_partition_ranges(n_rows, &sort_columns)?;
        assert!(!ranges.is_empty());
        let first_section = &ranges[0];

        // Determine whether first group calculated from the new data, is continuation of
        // the last group from the previous data received.
        let row = get_row_at_idx(cols, first_section.start)?;
        let same_group = if let Some(last_group) = group_values.last() {
            row.eq(last_group)
        } else {
            false
        };
        if !same_group {
            group_values.push(row);
        }
        groups.extend(vec![
            group_values.len() - 1;
            first_section.end - first_section.start
        ]);
        for range in ranges[1..].iter() {
            let row = get_row_at_idx(cols, range.start)?;
            group_values.push(row);
            groups.extend(vec![group_values.len() - 1; range.end - range.start]);
        }
        assert_eq!(groups.len(), n_rows);

        self.group_values = Some(group_values);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size: usize = self
            .group_values
            .as_ref()
            .map(|items| items.iter().map(ScalarValue::size_of_vec).sum())
            .unwrap_or(0);
        group_values_size
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|items| items.len())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => vec![],
        };

        let mut output: Vec<ArrayRef> = match emit_to {
            EmitTo::All => {
                let res = transpose(group_values);
                self.group_values = None;
                res.into_iter()
                    .map(ScalarValue::iter_to_array)
                    .collect::<Result<Vec<_>>>()?
            }
            EmitTo::First(n) => {
                let first_n_section = group_values.drain(0..n).collect();
                self.group_values = Some(group_values);
                let res = transpose(first_n_section);
                res.into_iter()
                    .map(ScalarValue::iter_to_array)
                    .collect::<Result<Vec<_>>>()?
            }
        };

        // TODO: Materialize dictionaries in group keys (#7647)
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            if let DataType::Dictionary(_, v) = expected {
                let actual = array.data_type();
                if v.as_ref() != actual {
                    return Err(DataFusionError::Internal(format!(
                        "Converted group rows expected dictionary of {v} got {actual}"
                    )));
                }
                *array = cast(array.as_ref(), expected)?;
            }
        }

        Ok(output)
    }

    fn clear_shrink(&mut self, _batch: &RecordBatch) {
        if let Some(group_values) = &mut self.group_values {
            group_values.clear();
        }
    }
}
