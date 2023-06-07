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

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::Array;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;

use std::any::Any;
use std::sync::Arc;
use datafusion_common::utils::get_row_at_idx;

/// FIRST_VALUE aggregate expression
#[derive(Debug)]
pub struct FirstValue {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl FirstValue {
    /// Creates a new FIRST_VALUE aggregation function.
    pub fn new(expr: Arc<dyn PhysicalExpr>, data_type: DataType) -> Self {
        let name = format!("FIRST_VALUE({})", expr);
        Self {
            name: name.into(),
            data_type,
            expr,
        }
    }
}

impl AggregateExpr for FirstValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstValueAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "first_value"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(LastValue::new(
            self.expr.clone(),
            vec![],
            vec![],
            self.data_type.clone(),
        )))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstValueAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for FirstValue {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct FirstValueAccumulator {
    first: ScalarValue,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        ScalarValue::try_from(data_type).map(|value| Self { first: value })
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.first.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // If we have seen first value, we shouldn't update it
        let values = &values[0];
        if !values.is_empty() {
            self.first = ScalarValue::try_from_array(values, 0)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.first)
            + self.first.size()
    }
}

/// LAST_VALUE aggregate expression
#[derive(Debug)]
pub struct LastValue {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    orderings: Vec<Field>,
    ordering_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl LastValue {
    /// Creates a new LAST_VALUE aggregation function.
    pub fn new(expr: Arc<dyn PhysicalExpr>, orderings: Vec<Field>, ordering_exprs: Vec<Arc<dyn PhysicalExpr>>, data_type: DataType) -> Self {
        let name = format!("LAST_VALUE({})", expr);
        println!("expr: {:?}", expr);
        println!("orderings: {:?}", orderings);
        Self {
            name: name.into(),
            data_type,
            expr,
            orderings,
            ordering_exprs
        }
    }
}

impl AggregateExpr for LastValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let ordering_dtypes = self.orderings.iter().map(|field| field.data_type().clone()).collect::<Vec<_>>();
        Ok(Box::new(LastValueAccumulator::try_new(&self.data_type, &ordering_dtypes)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "last_value"),
            self.data_type.clone(),
            true,
        )];
        for field in &self.orderings{
            fields.push(field.clone());
        }
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        let mut res = vec![self.expr.clone()];
        res.extend(self.ordering_exprs.clone());
        res
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(FirstValue::new(
            self.expr.clone(),
            self.data_type.clone(),
        )))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let ordering_dtypes = self.orderings.iter().map(|field| field.data_type().clone()).collect::<Vec<_>>();
        Ok(Box::new(LastValueAccumulator::try_new(&self.data_type, &ordering_dtypes)?))
    }
}

impl PartialEq<dyn Any> for LastValue {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct LastValueAccumulator {
    last: ScalarValue,
    orderings: Vec<ScalarValue>,
}

impl LastValueAccumulator {
    /// Creates a new `LastValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ordering_dtypes: &[DataType]) -> Result<Self> {
        let mut orderings = vec![];
        for dtype in ordering_dtypes{
            orderings.push(ScalarValue::try_from(dtype)?);
        }
        Ok(Self {
            last: ScalarValue::try_from(data_type)?,
            orderings,
        })
    }
}

impl Accumulator for LastValueAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut res = vec![self.last.clone()];
        res.extend(self.orderings.clone());
        println!("res len: {:?}", res.len());
        Ok(res)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for (idx, elem) in values.iter().enumerate() {
            println!("idx:{:?}, elem:{:?}", idx, elem);
        }
        if !values[0].is_empty() {
            let row = get_row_at_idx(values, values[0].len() - 1)?;
            println!("row:{:?}", row);
            // Update with last value in the array.
            self.last = row[0].clone();
            self.orderings = row[1..].to_vec();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // LAST_VALUE(last1, last2, last3, ...)
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.last) + self.last.size()
    }
}
