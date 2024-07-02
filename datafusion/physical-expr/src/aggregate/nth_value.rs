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

//! Defines NTH_VALUE aggregate expression which may specify ordering requirement
//! that can evaluated at runtime during query execution

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::aggregate::array_agg_ordered::merge_ordered_arrays;
use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::{format_state_name, Literal};
use crate::{
    reverse_order_bys, AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};

use arrow_array::cast::AsArray;
use arrow_array::{new_empty_array, ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::utils::{array_into_list_array_nullable, get_row_at_idx};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::Accumulator;

/// Expression for a `NTH_VALUE(... ORDER BY ..., ...)` aggregation. In a multi
/// partition setting, partial aggregations are computed for every partition,
/// and then their results are merged.
#[derive(Debug)]
pub struct NthValueAgg {
    /// Column name
    name: String,
    /// The `DataType` for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
    /// The `N` value.
    n: i64,
    /// If the input expression can have `NULL`s
    nullable: bool,
    /// Ordering data types
    order_by_data_types: Vec<DataType>,
    /// Ordering requirement
    ordering_req: LexOrdering,
}

impl NthValueAgg {
    /// Create a new `NthValueAgg` aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        n: i64,
        name: impl Into<String>,
        input_data_type: DataType,
        nullable: bool,
        order_by_data_types: Vec<DataType>,
        ordering_req: LexOrdering,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            expr,
            n,
            nullable,
            order_by_data_types,
            ordering_req,
        }
    }
}

impl AggregateExpr for NthValueAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NthValueAccumulator::try_new(
            self.n,
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(&self.name, "nth_value"),
            Field::new("item", self.input_data_type.clone(), true),
            self.nullable, // This should be the same as field()
        )];
        if !self.ordering_req.is_empty() {
            let orderings =
                ordering_fields(&self.ordering_req, &self.order_by_data_types);
            fields.push(Field::new_list(
                format_state_name(&self.name, "nth_value_orderings"),
                Field::new("item", DataType::Struct(Fields::from(orderings)), true),
                self.nullable,
            ));
        }
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let n = Arc::new(Literal::new(ScalarValue::Int64(Some(self.n)))) as _;
        vec![Arc::clone(&self.expr), n]
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        (!self.ordering_req.is_empty()).then_some(&self.ordering_req)
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::HardRequirement
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(Self {
            name: self.name.to_string(),
            input_data_type: self.input_data_type.clone(),
            expr: Arc::clone(&self.expr),
            // index should be from the opposite side
            n: -self.n,
            nullable: self.nullable,
            order_by_data_types: self.order_by_data_types.clone(),
            // reverse requirement
            ordering_req: reverse_order_bys(&self.ordering_req),
        }) as _)
    }
}

impl PartialEq<dyn Any> for NthValueAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.order_by_data_types == x.order_by_data_types
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct NthValueAccumulator {
    n: i64,
    /// Stores entries in the `NTH_VALUE` result.
    values: VecDeque<ScalarValue>,
    /// Stores values of ordering requirement expressions corresponding to each
    /// entry in `values`. This information is used when merging results from
    /// different partitions. For detailed information how merging is done, see
    /// [`merge_ordered_arrays`].
    ordering_values: VecDeque<Vec<ScalarValue>>,
    /// Stores datatypes of expressions inside values and ordering requirement
    /// expressions.
    datatypes: Vec<DataType>,
    /// Stores the ordering requirement of the `Accumulator`.
    ordering_req: LexOrdering,
}

impl NthValueAccumulator {
    /// Create a new order-sensitive NTH_VALUE accumulator based on the given
    /// item data type.
    pub fn try_new(
        n: i64,
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
    ) -> Result<Self> {
        if n == 0 {
            // n cannot be 0
            return internal_err!("Nth value indices are 1 based. 0 is invalid index");
        }
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            n,
            values: VecDeque::new(),
            ordering_values: VecDeque::new(),
            datatypes,
            ordering_req,
        })
    }
}

impl Accumulator for NthValueAccumulator {
    /// Updates its state with the `values`. Assumes data in the `values` satisfies the required
    /// ordering for the accumulator (across consecutive batches, not just batch-wise).
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        if from_start {
            // direction is from start
            let n_remaining = n_required.saturating_sub(self.values.len());
            self.append_new_data(values, Some(n_remaining))?;
        } else {
            // direction is from end
            self.append_new_data(values, None)?;
            let start_offset = self.values.len().saturating_sub(n_required);
            if start_offset > 0 {
                self.values.drain(0..start_offset);
                self.ordering_values.drain(0..start_offset);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // First entry in the state is the aggregation result.
        let array_agg_values = &states[0];
        let n_required = self.n.unsigned_abs() as usize;
        if self.ordering_req.is_empty() {
            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;
            for v in array_agg_res.into_iter() {
                self.values.extend(v);
                if self.values.len() > n_required {
                    // There is enough data collected can stop merging
                    break;
                }
            }
        } else if let Some(agg_orderings) = states[1].as_list_opt::<i32>() {
            // 2nd entry stores values received for ordering requirement columns, for each aggregation value inside NTH_VALUE list.
            // For each `StructArray` inside NTH_VALUE list, we will receive an `Array` that stores
            // values received from its ordering requirement expression. (This information is necessary for during merging).

            // Stores NTH_VALUE results coming from each partition
            let mut partition_values: Vec<VecDeque<ScalarValue>> = vec![];
            // Stores ordering requirement expression results coming from each partition
            let mut partition_ordering_values: Vec<VecDeque<Vec<ScalarValue>>> = vec![];

            // Existing values should be merged also.
            partition_values.push(self.values.clone());

            partition_ordering_values.push(self.ordering_values.clone());

            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;

            for v in array_agg_res.into_iter() {
                partition_values.push(v.into());
            }

            let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;

            let ordering_values = orderings.into_iter().map(|partition_ordering_rows| {
                // Extract value from struct to ordering_rows for each group/partition
                partition_ordering_rows.into_iter().map(|ordering_row| {
                    if let ScalarValue::Struct(s) = ordering_row {
                        let mut ordering_columns_per_row = vec![];

                        for column in s.columns() {
                            let sv = ScalarValue::try_from_array(column, 0)?;
                            ordering_columns_per_row.push(sv);
                        }

                        Ok(ordering_columns_per_row)
                    } else {
                        exec_err!(
                            "Expects to receive ScalarValue::Struct(Some(..), _) but got: {:?}",
                            ordering_row.data_type()
                        )
                    }
                }).collect::<Result<Vec<_>>>()
            }).collect::<Result<Vec<_>>>()?;
            for ordering_values in ordering_values.into_iter() {
                partition_ordering_values.push(ordering_values.into());
            }

            let sort_options = self
                .ordering_req
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>();
            let (new_values, new_orderings) = merge_ordered_arrays(
                &mut partition_values,
                &mut partition_ordering_values,
                &sort_options,
            )?;
            self.values = new_values.into();
            self.ordering_values = new_orderings.into();
        } else {
            return exec_err!("Expects to receive a list array");
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate_values()];
        if !self.ordering_req.is_empty() {
            result.push(self.evaluate_orderings()?);
        }
        Ok(result)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        let nth_value_idx = if from_start {
            // index is from start
            let forward_idx = n_required - 1;
            (forward_idx < self.values.len()).then_some(forward_idx)
        } else {
            // index is from end
            self.values.len().checked_sub(n_required)
        };
        if let Some(idx) = nth_value_idx {
            Ok(self.values[idx].clone())
        } else {
            ScalarValue::try_from(self.datatypes[0].clone())
        }
    }

    fn size(&self) -> usize {
        let mut total = std::mem::size_of_val(self)
            + ScalarValue::size_of_vec_deque(&self.values)
            - std::mem::size_of_val(&self.values);

        // Add size of the `self.ordering_values`
        total +=
            std::mem::size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total += ScalarValue::size_of_vec(row) - std::mem::size_of_val(row);
        }

        // Add size of the `self.datatypes`
        total += std::mem::size_of::<DataType>() * self.datatypes.capacity();
        for dtype in &self.datatypes {
            total += dtype.size() - std::mem::size_of_val(dtype);
        }

        // Add size of the `self.ordering_req`
        total += std::mem::size_of::<PhysicalSortExpr>() * self.ordering_req.capacity();
        // TODO: Calculate size of each `PhysicalSortExpr` more accurately.
        total
    }
}

impl NthValueAccumulator {
    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields.clone());

        let mut column_wise_ordering_values = vec![];
        let num_columns = fields.len();
        for i in 0..num_columns {
            let column_values = self
                .ordering_values
                .iter()
                .map(|x| x[i].clone())
                .collect::<Vec<_>>();
            let array = if column_values.is_empty() {
                new_empty_array(fields[i].data_type())
            } else {
                ScalarValue::iter_to_array(column_values.into_iter())?
            };
            column_wise_ordering_values.push(array);
        }

        let ordering_array = StructArray::try_new(
            struct_field.clone(),
            column_wise_ordering_values,
            None,
        )?;

        Ok(ScalarValue::List(Arc::new(array_into_list_array_nullable(
            Arc::new(ordering_array),
        ))))
    }

    fn evaluate_values(&self) -> ScalarValue {
        let mut values_cloned = self.values.clone();
        let values_slice = values_cloned.make_contiguous();
        ScalarValue::List(ScalarValue::new_list_nullable(
            values_slice,
            &self.datatypes[0],
        ))
    }

    /// Updates state, with the `values`. Fetch contains missing number of entries for state to be complete
    /// None represents all of the new `values` need to be added to the state.
    fn append_new_data(
        &mut self,
        values: &[ArrayRef],
        fetch: Option<usize>,
    ) -> Result<()> {
        let n_row = values[0].len();
        let n_to_add = if let Some(fetch) = fetch {
            std::cmp::min(fetch, n_row)
        } else {
            n_row
        };
        for index in 0..n_to_add {
            let row = get_row_at_idx(values, index)?;
            self.values.push_back(row[0].clone());
            // At index 1, we have n index argument.
            // Ordering values cover starting from 2nd index to end
            self.ordering_values.push_back(row[2..].to_vec());
        }
        Ok(())
    }
}
