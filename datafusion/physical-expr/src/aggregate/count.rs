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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::Int64Array;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateState};
use datafusion_row::accessor::RowAccessor;

use crate::expressions::format_state_name;

/// COUNT aggregate expression
/// Returns the amount of non-null values of the given expression.
#[derive(Debug)]
pub struct Count {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Count {
    /// Create a new COUNT aggregate function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for Count {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "count"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        true
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(CountRowAccumulator::new(start_index)))
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - array.data().null_count()) as i64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<Int64Array>().unwrap();
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(ScalarValue::Int64(Some(
            self.count,
        )))])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
}

#[derive(Debug)]
struct CountRowAccumulator {
    state_index: usize,
}

impl CountRowAccumulator {
    pub fn new(index: usize) -> Self {
        Self { state_index: index }
    }
}

impl RowAccumulator for CountRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let array = &values[0];
        let delta = (array.len() - array.data().null_count()) as u64;
        accessor.add_u64(self.state_index, delta);
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<Int64Array>().unwrap();
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            accessor.add_i64(self.state_index, *d);
        }
        Ok(())
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&DataType::Int64, self.state_index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.state_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn count_elements() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Count,
            ScalarValue::from(5i64),
            DataType::Int64
        )
    }

    #[test]
    fn count_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Count,
            ScalarValue::from(3i64),
            DataType::Int64
        )
    }

    #[test]
    fn count_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        generic_test_op!(
            a,
            DataType::Boolean,
            Count,
            ScalarValue::from(0i64),
            DataType::Int64
        )
    }

    #[test]
    fn count_empty() -> Result<()> {
        let a: Vec<bool> = vec![];
        let a: ArrayRef = Arc::new(BooleanArray::from(a));
        generic_test_op!(
            a,
            DataType::Boolean,
            Count,
            ScalarValue::from(0i64),
            DataType::Int64
        )
    }

    #[test]
    fn count_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Count,
            ScalarValue::from(5i64),
            DataType::Int64
        )
    }

    #[test]
    fn count_large_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Count,
            ScalarValue::from(5i64),
            DataType::Int64
        )
    }
}
