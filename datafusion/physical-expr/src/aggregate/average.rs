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
use std::convert::TryFrom;
use std::sync::Arc;

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::aggregate::sum;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, AggregateState};
use datafusion_row::accessor::RowAccessor;

/// AVG aggregate expression
#[derive(Debug)]
pub struct Avg {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of avg just support FLOAT64 and Decimal data type.
        assert!(matches!(
            data_type,
            DataType::Float64 | DataType::Decimal(_, _)
        ));
        Self {
            name: name.into(),
            expr,
            data_type,
        }
    }
}

impl AggregateExpr for Avg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(
            // avg is f64 or decimal
            &self.data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "sum"),
                self.data_type.clone(),
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        matches!(
            self.data_type,
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        )
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(AvgRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
pub struct AvgAccumulator {
    // sum is used for null
    sum: ScalarValue,
    count: u64,
}

impl AvgAccumulator {
    /// Creates a new `AvgAccumulator`
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(datatype)?,
            count: 0,
        })
    }
}

impl Accumulator for AvgAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![
            AggregateState::Scalar(ScalarValue::from(self.count)),
            AggregateState::Scalar(self.sum.clone()),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];

        self.count += (values.len() - values.data().null_count()) as u64;
        self.sum = sum::sum(
            &self.sum,
            &sum::sum_batch(values, &self.sum.get_datatype())?,
        )?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        // counts are summed
        self.count += compute::sum(counts).unwrap_or(0);

        // sums are summed
        self.sum = sum::sum(
            &self.sum,
            &sum::sum_batch(&states[1], &self.sum.get_datatype())?,
        )?;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.sum {
            ScalarValue::Float64(e) => {
                Ok(ScalarValue::Float64(e.map(|f| f / self.count as f64)))
            }
            ScalarValue::Decimal128(value, precision, scale) => {
                Ok(match value {
                    None => ScalarValue::Decimal128(None, precision, scale),
                    // TODO add the checker for overflow the precision
                    Some(v) => ScalarValue::Decimal128(
                        Some(v / self.count as i128),
                        precision,
                        scale,
                    ),
                })
            }
            _ => Err(DataFusionError::Internal(
                "Sum should be f64 on average".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
struct AvgRowAccumulator {
    state_index: usize,
    sum_datatype: DataType,
}

impl AvgRowAccumulator {
    pub fn new(start_index: usize, sum_datatype: DataType) -> Self {
        Self {
            state_index: start_index,
            sum_datatype,
        }
    }
}

impl RowAccumulator for AvgRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        // count
        let delta = (values.len() - values.data().null_count()) as u64;
        accessor.add_u64(self.state_index(), delta);

        // sum
        sum::add_to_row(
            &self.sum_datatype,
            self.state_index() + 1,
            accessor,
            &sum::sum_batch(values, &self.sum_datatype)?,
        )?;
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        // count
        let delta = compute::sum(counts).unwrap_or(0);
        accessor.add_u64(self.state_index(), delta);

        // sum
        sum::add_to_row(
            &self.sum_datatype,
            self.state_index() + 1,
            accessor,
            &sum::sum_batch(&states[1], &self.sum_datatype)?,
        )?;
        Ok(())
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        assert_eq!(self.sum_datatype, DataType::Float64);
        Ok(match accessor.get_u64_opt(self.state_index()) {
            None => ScalarValue::Float64(None),
            Some(0) => ScalarValue::Float64(Some(0.0)),
            Some(n) => ScalarValue::Float64(
                accessor
                    .get_f64_opt(self.state_index() + 1)
                    .map(|f| f / n as f64),
            ),
        })
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
    fn avg_decimal() -> Result<()> {
        // test agg
        let array: ArrayRef = Arc::new(
            (1..7)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Avg,
            ScalarValue::Decimal128(Some(35000), 14, 4),
            DataType::Decimal(14, 4)
        )
    }

    #[test]
    fn avg_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Avg,
            ScalarValue::Decimal128(Some(32500), 14, 4),
            DataType::Decimal(14, 4)
        )
    }

    #[test]
    fn avg_decimal_all_nulls() -> Result<()> {
        // test agg
        let array: ArrayRef = Arc::new(
            std::iter::repeat(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Avg,
            ScalarValue::Decimal128(None, 14, 4),
            DataType::Decimal(14, 4)
        )
    }

    #[test]
    fn avg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::from(3.25f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Avg,
            ScalarValue::Float64(None),
            DataType::Float64
        )
    }

    #[test]
    fn avg_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Avg,
            ScalarValue::from(3.0f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    #[test]
    fn avg_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Avg,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }
}
