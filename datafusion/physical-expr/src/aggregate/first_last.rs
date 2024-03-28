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

//! Defines the FIRST_VALUE/LAST_VALUE aggregations.

use std::any::Any;
use std::sync::Arc;

use crate::aggregate::utils::{down_cast_any_ref, get_sort_options, ordering_fields};
use crate::expressions::format_state_name;
use crate::{
    reverse_order_bys, AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::compute::{self, lexsort_to_indices, SortColumn};
use arrow::datatypes::{DataType, Field};
use datafusion_common::utils::{compare_rows, get_arrayref_at_indices, get_row_at_idx};
use datafusion_common::{
    arrow_datafusion_err, internal_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::Accumulator;

/// FIRST_VALUE aggregate expression
#[derive(Debug, Clone)]
pub struct FirstValue {
    name: String,
    input_data_type: DataType,
    order_by_data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
    requirement_satisfied: bool,
    ignore_nulls: bool,
}

impl FirstValue {
    /// Creates a new FIRST_VALUE aggregation function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        ordering_req: LexOrdering,
        order_by_data_types: Vec<DataType>,
    ) -> Self {
        let requirement_satisfied = ordering_req.is_empty();
        Self {
            name: name.into(),
            input_data_type,
            order_by_data_types,
            expr,
            ordering_req,
            requirement_satisfied,
            ignore_nulls: false,
        }
    }

    pub fn with_ignore_nulls(mut self, ignore_nulls: bool) -> Self {
        self.ignore_nulls = ignore_nulls;
        self
    }

    /// Returns the name of the aggregate expression.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the input data type of the aggregate expression.
    pub fn input_data_type(&self) -> &DataType {
        &self.input_data_type
    }

    /// Returns the data types of the order-by columns.
    pub fn order_by_data_types(&self) -> &Vec<DataType> {
        &self.order_by_data_types
    }

    /// Returns the expression associated with the aggregate function.
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Returns the lexical ordering requirements of the aggregate expression.
    pub fn ordering_req(&self) -> &LexOrdering {
        &self.ordering_req
    }

    pub fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }

    pub fn convert_to_last(self) -> LastValue {
        let name = if self.name.starts_with("FIRST") {
            format!("LAST{}", &self.name[5..])
        } else {
            format!("LAST_VALUE({})", self.expr)
        };
        let FirstValue {
            expr,
            input_data_type,
            ordering_req,
            order_by_data_types,
            ..
        } = self;
        LastValue::new(
            expr,
            name,
            input_data_type,
            reverse_order_bys(&ordering_req),
            order_by_data_types,
        )
    }
}

impl AggregateExpr for FirstValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        FirstValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
            self.ignore_nulls,
        )
        .map(|acc| {
            Box::new(acc.with_requirement_satisfied(self.requirement_satisfied)) as _
        })
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "first_value"),
            self.input_data_type.clone(),
            true,
        )];
        fields.extend(ordering_fields(
            &self.ordering_req,
            &self.order_by_data_types,
        ));
        fields.push(Field::new(
            format_state_name(&self.name, "is_set"),
            DataType::Boolean,
            true,
        ));
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        (!self.ordering_req.is_empty()).then_some(&self.ordering_req)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone().convert_to_last()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        FirstValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
            self.ignore_nulls,
        )
        .map(|acc| {
            Box::new(acc.with_requirement_satisfied(self.requirement_satisfied)) as _
        })
    }
}

impl PartialEq<dyn Any> for FirstValue {
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
pub struct FirstValueAccumulator {
    first: ScalarValue,
    // At the beginning, `is_set` is false, which means `first` is not seen yet.
    // Once we see the first value, we set the `is_set` flag and do not update `first` anymore.
    is_set: bool,
    // Stores ordering values, of the aggregator requirement corresponding to first value
    // of the aggregator. These values are used during merging of multiple partitions.
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // Stores whether incoming data already satisfies the ordering requirement.
    requirement_satisfied: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        let requirement_satisfied = ordering_req.is_empty();
        ScalarValue::try_from(data_type).map(|first| Self {
            first,
            is_set: false,
            orderings,
            ordering_req,
            requirement_satisfied,
            ignore_nulls,
        })
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, row: &[ScalarValue]) {
        self.first = row[0].clone();
        self.orderings = row[1..].to_vec();
        self.is_set = true;
    }

    fn get_first_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in FIRST_VALUE");
        };
        if self.requirement_satisfied {
            // Get first entry according to the pre-existing ordering (0th index):
            if self.ignore_nulls {
                // If ignoring nulls, find the first non-null value.
                for i in 0..value.len() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                // If not ignoring nulls, return the first value if it exists.
                return Ok((!value.is_empty()).then_some(0));
            }
        }
        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| SortColumn {
                values: values.clone(),
                options: Some(req.options),
            })
            .collect::<Vec<_>>();

        if self.ignore_nulls {
            let indices = lexsort_to_indices(&sort_columns, None)?;
            // If ignoring nulls, find the first non-null value.
            for index in indices.iter().flatten() {
                if !value.is_null(index as usize) {
                    return Ok(Some(index as usize));
                }
            }
            Ok(None)
        } else {
            let indices = lexsort_to_indices(&sort_columns, Some(1))?;
            Ok((!indices.is_empty()).then_some(indices.value(0) as _))
        }
    }

    fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.first.clone()];
        result.extend(self.orderings.iter().cloned());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_set {
            if let Some(first_idx) = self.get_first_idx(values)? {
                let row = get_row_at_idx(values, first_idx)?;
                self.update_with_new_row(&row);
            }
        } else if !self.requirement_satisfied {
            if let Some(first_idx) = self.get_first_idx(values)? {
                let row = get_row_at_idx(values, first_idx)?;
                let orderings = &row[1..];
                if compare_rows(
                    &self.orderings,
                    orderings,
                    &get_sort_options(&self.ordering_req),
                )?
                .is_gt()
                {
                    self.update_with_new_row(&row);
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        let filtered_states = filter_states_according_to_is_set(states, flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_cols =
            convert_to_sort_cols(&filtered_states[1..is_set_idx], &self.ordering_req);

        let ordered_states = if sort_cols.is_empty() {
            // When no ordering is given, use the existing state as is:
            filtered_states
        } else {
            let indices = lexsort_to_indices(&sort_cols, None)?;
            get_arrayref_at_indices(&filtered_states, &indices)?
        };
        if !ordered_states[0].is_empty() {
            let first_row = get_row_at_idx(&ordered_states, 0)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let first_ordering = &first_row[1..is_set_idx];
            let sort_options = get_sort_options(&self.ordering_req);
            // Either there is no existing value, or there is an earlier version in new data.
            if !self.is_set
                || compare_rows(&self.orderings, first_ordering, &sort_options)?.is_gt()
            {
                // Update with first value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                self.update_with_new_row(&first_row[0..is_set_idx]);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.first)
            + self.first.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - std::mem::size_of_val(&self.orderings)
    }
}

/// LAST_VALUE aggregate expression
#[derive(Debug, Clone)]
pub struct LastValue {
    name: String,
    input_data_type: DataType,
    order_by_data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
    requirement_satisfied: bool,
    ignore_nulls: bool,
}

impl LastValue {
    /// Creates a new LAST_VALUE aggregation function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        ordering_req: LexOrdering,
        order_by_data_types: Vec<DataType>,
    ) -> Self {
        let requirement_satisfied = ordering_req.is_empty();
        Self {
            name: name.into(),
            input_data_type,
            order_by_data_types,
            expr,
            ordering_req,
            requirement_satisfied,
            ignore_nulls: false,
        }
    }

    pub fn with_ignore_nulls(mut self, ignore_nulls: bool) -> Self {
        self.ignore_nulls = ignore_nulls;
        self
    }

    /// Returns the name of the aggregate expression.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the input data type of the aggregate expression.
    pub fn input_data_type(&self) -> &DataType {
        &self.input_data_type
    }

    /// Returns the data types of the order-by columns.
    pub fn order_by_data_types(&self) -> &Vec<DataType> {
        &self.order_by_data_types
    }

    /// Returns the expression associated with the aggregate function.
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Returns the lexical ordering requirements of the aggregate expression.
    pub fn ordering_req(&self) -> &LexOrdering {
        &self.ordering_req
    }

    pub fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }

    pub fn convert_to_first(self) -> FirstValue {
        let name = if self.name.starts_with("LAST") {
            format!("FIRST{}", &self.name[4..])
        } else {
            format!("FIRST_VALUE({})", self.expr)
        };
        let LastValue {
            expr,
            input_data_type,
            ordering_req,
            order_by_data_types,
            ..
        } = self;
        FirstValue::new(
            expr,
            name,
            input_data_type,
            reverse_order_bys(&ordering_req),
            order_by_data_types,
        )
    }
}

impl AggregateExpr for LastValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        LastValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
            self.ignore_nulls,
        )
        .map(|acc| {
            Box::new(acc.with_requirement_satisfied(self.requirement_satisfied)) as _
        })
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "last_value"),
            self.input_data_type.clone(),
            true,
        )];
        fields.extend(ordering_fields(
            &self.ordering_req,
            &self.order_by_data_types,
        ));
        fields.push(Field::new(
            format_state_name(&self.name, "is_set"),
            DataType::Boolean,
            true,
        ));
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        (!self.ordering_req.is_empty()).then_some(&self.ordering_req)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone().convert_to_first()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        LastValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
            self.ignore_nulls,
        )
        .map(|acc| {
            Box::new(acc.with_requirement_satisfied(self.requirement_satisfied)) as _
        })
    }
}

impl PartialEq<dyn Any> for LastValue {
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
struct LastValueAccumulator {
    last: ScalarValue,
    // The `is_set` flag keeps track of whether the last value is finalized.
    // This information is used to discriminate genuine NULLs and NULLS that
    // occur due to empty partitions.
    is_set: bool,
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // Stores whether incoming data already satisfies the ordering requirement.
    requirement_satisfied: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl LastValueAccumulator {
    /// Creates a new `LastValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        let requirement_satisfied = ordering_req.is_empty();
        ScalarValue::try_from(data_type).map(|last| Self {
            last,
            is_set: false,
            orderings,
            ordering_req,
            requirement_satisfied,
            ignore_nulls,
        })
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, row: &[ScalarValue]) {
        self.last = row[0].clone();
        self.orderings = row[1..].to_vec();
        self.is_set = true;
    }

    fn get_last_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in LAST_VALUE");
        };
        if self.requirement_satisfied {
            // Get last entry according to the order of data:
            if self.ignore_nulls {
                // If ignoring nulls, find the last non-null value.
                for i in (0..value.len()).rev() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                return Ok((!value.is_empty()).then_some(value.len() - 1));
            }
        }
        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| {
                // Take the reverse ordering requirement. This enables us to
                // use "fetch = 1" to get the last value.
                SortColumn {
                    values: values.clone(),
                    options: Some(!req.options),
                }
            })
            .collect::<Vec<_>>();

        if self.ignore_nulls {
            let indices = lexsort_to_indices(&sort_columns, None)?;
            // If ignoring nulls, find the last non-null value.
            for index in indices.iter().flatten() {
                if !value.is_null(index as usize) {
                    return Ok(Some(index as usize));
                }
            }
            Ok(None)
        } else {
            let indices = lexsort_to_indices(&sort_columns, Some(1))?;
            Ok((!indices.is_empty()).then_some(indices.value(0) as _))
        }
    }

    fn with_requirement_satisfied(mut self, requirement_satisfied: bool) -> Self {
        self.requirement_satisfied = requirement_satisfied;
        self
    }
}

impl Accumulator for LastValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.last.clone()];
        result.extend(self.orderings.clone());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_set || self.requirement_satisfied {
            if let Some(last_idx) = self.get_last_idx(values)? {
                let row = get_row_at_idx(values, last_idx)?;
                self.update_with_new_row(&row);
            }
        } else if let Some(last_idx) = self.get_last_idx(values)? {
            let row = get_row_at_idx(values, last_idx)?;
            let orderings = &row[1..];
            // Update when there is a more recent entry
            if compare_rows(
                &self.orderings,
                orderings,
                &get_sort_options(&self.ordering_req),
            )?
            .is_lt()
            {
                self.update_with_new_row(&row);
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // LAST_VALUE(last1, last2, last3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        let filtered_states = filter_states_according_to_is_set(states, flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_cols =
            convert_to_sort_cols(&filtered_states[1..is_set_idx], &self.ordering_req);

        let ordered_states = if sort_cols.is_empty() {
            // When no ordering is given, use existing state as is:
            filtered_states
        } else {
            let indices = lexsort_to_indices(&sort_cols, None)?;
            get_arrayref_at_indices(&filtered_states, &indices)?
        };

        if !ordered_states[0].is_empty() {
            let last_idx = ordered_states[0].len() - 1;
            let last_row = get_row_at_idx(&ordered_states, last_idx)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let last_ordering = &last_row[1..is_set_idx];
            let sort_options = get_sort_options(&self.ordering_req);
            // Either there is no existing value, or there is a newer (latest)
            // version in the new data:
            if !self.is_set
                || compare_rows(&self.orderings, last_ordering, &sort_options)?.is_lt()
            {
                // Update with last value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                self.update_with_new_row(&last_row[0..is_set_idx]);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.last)
            + self.last.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - std::mem::size_of_val(&self.orderings)
    }
}

/// Filters states according to the `is_set` flag at the last column and returns
/// the resulting states.
fn filter_states_according_to_is_set(
    states: &[ArrayRef],
    flags: &BooleanArray,
) -> Result<Vec<ArrayRef>> {
    states
        .iter()
        .map(|state| compute::filter(state, flags).map_err(|e| arrow_datafusion_err!(e)))
        .collect::<Result<Vec<_>>>()
}

/// Combines array refs and their corresponding orderings to construct `SortColumn`s.
fn convert_to_sort_cols(
    arrs: &[ArrayRef],
    sort_exprs: &[PhysicalSortExpr],
) -> Vec<SortColumn> {
    arrs.iter()
        .zip(sort_exprs.iter())
        .map(|(item, sort_expr)| SortColumn {
            values: item.clone(),
            options: Some(sort_expr.options),
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::aggregate::first_last::{FirstValueAccumulator, LastValueAccumulator};

    use arrow::compute::concat;
    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::DataType;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::Accumulator;

    #[test]
    fn test_first_last_value_value() -> Result<()> {
        let mut first_accumulator =
            FirstValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;
        let mut last_accumulator =
            LastValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;
        // first value in the tuple is start of the range (inclusive),
        // second value in the tuple is end of the range (exclusive)
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())) as ArrayRef
            })
            .collect::<Vec<_>>();
        for arr in arrs {
            // Once first_value is set, accumulator should remember it.
            // It shouldn't update first_value for each new batch
            first_accumulator.update_batch(&[arr.clone()])?;
            // last_value should be updated for each new batch.
            last_accumulator.update_batch(&[arr])?;
        }
        // First Value comes from the first value of the first batch which is 0
        assert_eq!(first_accumulator.evaluate()?, ScalarValue::Int64(Some(0)));
        // Last value comes from the last value of the last batch which is 12
        assert_eq!(last_accumulator.evaluate()?, ScalarValue::Int64(Some(12)));
        Ok(())
    }

    #[test]
    fn test_first_last_state_after_merge() -> Result<()> {
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new((start..end).collect::<Int64Array>()) as ArrayRef
            })
            .collect::<Vec<_>>();

        // FirstValueAccumulator
        let mut first_accumulator =
            FirstValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;

        first_accumulator.update_batch(&[arrs[0].clone()])?;
        let state1 = first_accumulator.state()?;

        let mut first_accumulator =
            FirstValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;
        first_accumulator.update_batch(&[arrs[1].clone()])?;
        let state2 = first_accumulator.state()?;

        assert_eq!(state1.len(), state2.len());

        let mut states = vec![];

        for idx in 0..state1.len() {
            states.push(concat(&[
                &state1[idx].to_array()?,
                &state2[idx].to_array()?,
            ])?);
        }

        let mut first_accumulator =
            FirstValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;
        first_accumulator.merge_batch(&states)?;

        let merged_state = first_accumulator.state()?;
        assert_eq!(merged_state.len(), state1.len());

        // LastValueAccumulator
        let mut last_accumulator =
            LastValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;

        last_accumulator.update_batch(&[arrs[0].clone()])?;
        let state1 = last_accumulator.state()?;

        let mut last_accumulator =
            LastValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;
        last_accumulator.update_batch(&[arrs[1].clone()])?;
        let state2 = last_accumulator.state()?;

        assert_eq!(state1.len(), state2.len());

        let mut states = vec![];

        for idx in 0..state1.len() {
            states.push(concat(&[
                &state1[idx].to_array()?,
                &state2[idx].to_array()?,
            ])?);
        }

        let mut last_accumulator =
            LastValueAccumulator::try_new(&DataType::Int64, &[], vec![], false)?;
        last_accumulator.merge_batch(&states)?;

        let merged_state = last_accumulator.state()?;
        assert_eq!(merged_state.len(), state1.len());

        Ok(())
    }
}
