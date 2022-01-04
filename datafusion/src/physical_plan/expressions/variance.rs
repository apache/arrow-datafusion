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

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::{
    ScalarValue, MAX_PRECISION_FOR_DECIMAL128, MAX_SCALE_FOR_DECIMAL128,
};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;

use super::{format_state_name, sum};

/// STDDEV (standard deviation) aggregate expression
#[derive(Debug)]
pub struct Variance {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

/// function return type of an standard deviation
pub fn variance_return_type(arg_type: &DataType) -> Result<DataType> {
    match arg_type {
        DataType::Decimal(precision, scale) => {
            // in the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
            // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
            let new_precision = MAX_PRECISION_FOR_DECIMAL128.min(*precision + 4);
            let new_scale = MAX_SCALE_FOR_DECIMAL128.min(*scale + 4);
            Ok(DataType::Decimal(new_precision, new_scale))
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => Ok(DataType::Float64),
        other => Err(DataFusionError::Plan(format!(
            "STDDEV does not support {:?}",
            other
        ))),
    }
}

pub(crate) fn is_variance_support_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
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
            | DataType::Decimal(_, _)
    )
}

impl Variance {
    /// Create a new STDDEV aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of variance just support FLOAT64 and Decimal data type.
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

impl AggregateExpr for Variance {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
            // variance is f64 or decimal
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
}

/// An accumulator to compute variance
#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: ScalarValue,
    mean: ScalarValue,
    count: u64,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            m2: ScalarValue::from(0 as f64),
            mean: ScalarValue::from(0 as f64),
            count: 0,
        })
    }

    // TODO: There should be a generic implementation of ScalarValue arithmetic somewhere
    // There is also a similar function in averate.rs
    fn div(lhs: &ScalarValue, rhs: u64) -> Result<ScalarValue> {
        match lhs {
            ScalarValue::Float64(e) => {
                Ok(ScalarValue::Float64(e.map(|f| f / rhs as f64)))
            }
            _ => Err(DataFusionError::Internal(
                "Numerator should be f64 to calculate variance".to_string(),
            )),
        }
    }

    // TODO: There should be a generic implementation of ScalarValue arithmetic somewhere
    // This is only used to calculate multiplications of deltas which are guarenteed to be f64
    // Assumption in this function is lhs and rhs are not none values and are the same data type
    fn mul(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
        match (lhs, rhs) {
            (ScalarValue::Float64(f1),  
                ScalarValue::Float64(f2)) => {
                Ok(ScalarValue::Float64(Some(f1.unwrap() * f2.unwrap())))
            }
            _ => Err(DataFusionError::Internal(
                "Delta should be f64 to calculate variance".to_string(),
            )),
        }
    }
}

impl Accumulator for VarianceAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.count), self.mean.clone(), self.m2.clone()])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let values = &values[0];
        let is_empty = values.is_null();

        if !is_empty {
            let new_count = self.count + 1;
            let delta1 = sum::sum(values, &self.mean.arithmetic_negate())?;
            let sum = sum::sum(&self.mean, values)?;
            let new_mean = sum::sum(
                &VarianceAccumulator::div(&delta1, new_count)?,
                &self.mean)?;
            //let new_mean = VarianceAccumulator::div(&sum, 2)?;
            let delta2 = sum::sum(values, &new_mean.arithmetic_negate())?;
            let tmp = VarianceAccumulator::mul(&delta1, &delta2)?;
            let new_m2 = sum::sum(&self.m2, &tmp)?;
            self.count += 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
 
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let count = &states[0];
        let mean = &states[1];
        let m2 = &states[2];
        let mut new_count: u64 = self.count;
        // counts are summed
        if let ScalarValue::UInt64(Some(c)) = count {
            new_count += c
        } else {
            unreachable!()
        };
        let new_mean = 
            VarianceAccumulator::div(
                &sum::sum(
                    &self.mean, 
                    mean)?,
                2)?;
        let delta = sum::sum(&mean.arithmetic_negate(), &self.mean)?;
        let delta_sqrt = VarianceAccumulator::mul(&delta, &delta)?;
        let new_m2 = 
            sum::sum(
                &sum::sum(
                    &VarianceAccumulator::mul(
                        &delta_sqrt,
                        &VarianceAccumulator::div(
                            &VarianceAccumulator::mul(
                                    &ScalarValue::from(self.count), 
                                    count)?,
                                new_count)?)?,
                    &self.m2)?,
                &m2)?;

        self.count = new_count;
        self.mean = new_mean;
        self.m2 = new_m2;

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.m2 {
            ScalarValue::Float64(e) => {
                Ok(ScalarValue::Float64(e.map(|f| f / self.count as f64)))
            }
            _ => Err(DataFusionError::Internal(
                "M2 should be f64 for variance".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use crate::{error::Result, generic_test_op};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};


    #[test]
    fn variance_f64_1() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Variance,
            ScalarValue::from(0.25_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Variance,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Variance,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn test_variance_return_data_type() -> Result<()> {
        let data_type = DataType::Decimal(10, 5);
        let result_type = variance_return_type(&data_type)?;
        assert_eq!(DataType::Decimal(14, 9), result_type);

        let data_type = DataType::Decimal(36, 10);
        let result_type = variance_return_type(&data_type)?;
        assert_eq!(DataType::Decimal(38, 14), result_type);
        Ok(())
    }

    #[test]
    fn variance_decimal() -> Result<()> {
        // test agg
        let mut decimal_builder = DecimalBuilder::new(6, 10, 0);
        for i in 1..7 {
            decimal_builder.append_value(i as i128)?;
        }
        let array: ArrayRef = Arc::new(decimal_builder.finish());

        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Variance,
            ScalarValue::Decimal128(Some(35000), 14, 4),
            DataType::Decimal(14, 4)
        )
    }

    #[test]
    fn variance_decimal_with_nulls() -> Result<()> {
        let mut decimal_builder = DecimalBuilder::new(5, 10, 0);
        for i in 1..6 {
            if i == 2 {
                decimal_builder.append_null()?;
            } else {
                decimal_builder.append_value(i)?;
            }
        }
        let array: ArrayRef = Arc::new(decimal_builder.finish());
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Variance,
            ScalarValue::Decimal128(Some(32500), 14, 4),
            DataType::Decimal(14, 4)
        )
    }

    #[test]
    fn variance_decimal_all_nulls() -> Result<()> {
        // test agg
        let mut decimal_builder = DecimalBuilder::new(5, 10, 0);
        for _i in 1..6 {
            decimal_builder.append_null()?;
        }
        let array: ArrayRef = Arc::new(decimal_builder.finish());
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Variance,
            ScalarValue::Decimal128(None, 14, 4),
            DataType::Decimal(14, 4)
        )
    }

    #[test]
    fn variance_i32_with_nulls() -> Result<()> {
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
            Variance,
            ScalarValue::from(3.25f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Variance,
            ScalarValue::Float64(None),
            DataType::Float64
        )
    }

    #[test]
    fn variance_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Variance,
            ScalarValue::from(3.0f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Variance,
            ScalarValue::from(3_f64),
            DataType::Float64
        )
    }

    fn aggregate(
        batch: &RecordBatch,
        agg: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum = agg.create_accumulator()?;
        let expr = agg.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum.update_batch(&values)?;
        accum.evaluate()
    }
}
