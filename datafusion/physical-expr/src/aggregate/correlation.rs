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

use crate::aggregate::covariance::CovarianceAccumulator;
use crate::aggregate::stats::StatsType;
use crate::aggregate::stddev::StddevAccumulator;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::{array::ArrayRef, datatypes::DataType, datatypes::Field};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateState};
use std::any::Any;
use std::sync::Arc;

/// CORR aggregate expression
#[derive(Debug)]
pub struct Correlation {
    name: String,
    expr1: Arc<dyn PhysicalExpr>,
    expr2: Arc<dyn PhysicalExpr>,
}

impl Correlation {
    /// Create a new COVAR_POP aggregate function
    pub fn new(
        expr1: Arc<dyn PhysicalExpr>,
        expr2: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of correlation just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr1,
            expr2,
        }
    }
}

impl AggregateExpr for Correlation {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CorrelationAccumulator::try_new()?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "mean1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "m2_1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "mean2"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "m2_2"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "algo_const"),
                DataType::Float64,
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr1.clone(), self.expr2.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// An accumulator to compute correlation
#[derive(Debug)]
pub struct CorrelationAccumulator {
    covar: CovarianceAccumulator,
    stddev1: StddevAccumulator,
    stddev2: StddevAccumulator,
}

impl CorrelationAccumulator {
    /// Creates a new `CorrelationAccumulator`
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            covar: CovarianceAccumulator::try_new(StatsType::Population)?,
            stddev1: StddevAccumulator::try_new(StatsType::Population)?,
            stddev2: StddevAccumulator::try_new(StatsType::Population)?,
        })
    }
}

impl Accumulator for CorrelationAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![
            AggregateState::Scalar(ScalarValue::from(self.covar.get_count())),
            AggregateState::Scalar(ScalarValue::from(self.covar.get_mean1())),
            AggregateState::Scalar(ScalarValue::from(self.stddev1.get_m2())),
            AggregateState::Scalar(ScalarValue::from(self.covar.get_mean2())),
            AggregateState::Scalar(ScalarValue::from(self.stddev2.get_m2())),
            AggregateState::Scalar(ScalarValue::from(self.covar.get_algo_const())),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.covar.update_batch(values)?;
        self.stddev1.update_batch(&[values[0].clone()])?;
        self.stddev2.update_batch(&[values[1].clone()])?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states_c = [
            states[0].clone(),
            states[1].clone(),
            states[3].clone(),
            states[5].clone(),
        ];
        let states_s1 = [states[0].clone(), states[1].clone(), states[2].clone()];
        let states_s2 = [states[0].clone(), states[3].clone(), states[4].clone()];

        self.covar.merge_batch(&states_c)?;
        self.stddev1.merge_batch(&states_s1)?;
        self.stddev2.merge_batch(&states_s2)?;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let covar = self.covar.evaluate()?;
        let stddev1 = self.stddev1.evaluate()?;
        let stddev2 = self.stddev2.evaluate()?;

        if let ScalarValue::Float64(Some(c)) = covar {
            if let ScalarValue::Float64(Some(s1)) = stddev1 {
                if let ScalarValue::Float64(Some(s2)) = stddev2 {
                    if s1 == 0_f64 || s2 == 0_f64 {
                        return Ok(ScalarValue::Float64(Some(0_f64)));
                    } else {
                        return Ok(ScalarValue::Float64(Some(c / s1 / s2)));
                    }
                }
            }
        }

        Ok(ScalarValue::Float64(None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::utils::get_accum_scalar_values_as_arrays;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op2;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn correlation_f64_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4_f64, 5_f64, 7_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            Correlation,
            ScalarValue::from(0.9819805060619659),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_f64_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4_f64, -5_f64, 6_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            Correlation,
            ScalarValue::from(0.17066403719657236),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_f64_4() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4.1_f64, 5_f64, 6_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            Correlation,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_f64_6() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![
            1_f64, 2_f64, 3_f64, 1.1_f64, 2.2_f64, 3.3_f64,
        ]));
        let b = Arc::new(Float64Array::from(vec![
            4_f64, 5_f64, 6_f64, 4.4_f64, 5.5_f64, 6.6_f64,
        ]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            Correlation,
            ScalarValue::from(0.9860135594710389),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));

        generic_test_op2!(
            a,
            b,
            DataType::Int32,
            DataType::Int32,
            Correlation,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_u32() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32]));
        let b: ArrayRef = Arc::new(UInt32Array::from(vec![4_u32, 5_u32, 6_u32]));
        generic_test_op2!(
            a,
            b,
            DataType::UInt32,
            DataType::UInt32,
            Correlation,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_f32() -> Result<()> {
        let a: ArrayRef = Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32]));
        let b: ArrayRef = Arc::new(Float32Array::from(vec![4_f32, 5_f32, 6_f32]));
        generic_test_op2!(
            a,
            b,
            DataType::Float32,
            DataType::Float32,
            Correlation,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_i32_with_nulls_1() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(3)]));
        let b: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(4), None, Some(6), Some(3)]));

        generic_test_op2!(
            a,
            b,
            DataType::Int32,
            DataType::Int32,
            Correlation,
            ScalarValue::from(0.1889822365046137),
            DataType::Float64
        )
    }

    #[test]
    fn correlation_i32_with_nulls_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(4), Some(5), Some(6)]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;

        let agg = Arc::new(Correlation::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
    }

    #[test]
    fn correlation_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;

        let agg = Arc::new(Correlation::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
    }

    #[test]
    fn correlation_f64_merge_1() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from(vec![4_f64, 5_f64, 6_f64]));
        let c = Arc::new(Float64Array::from(vec![1.1_f64, 2.2_f64, 3.3_f64]));
        let d = Arc::new(Float64Array::from(vec![4.4_f64, 5.5_f64, 9.9_f64]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![c, d])?;

        let agg1 = Arc::new(Correlation::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(Correlation::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(0.8443707186481967));

        Ok(())
    }

    #[test]
    fn correlation_f64_merge_2() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from(vec![4_f64, 5_f64, 6_f64]));
        let c = Arc::new(Float64Array::from(vec![None]));
        let d = Arc::new(Float64Array::from(vec![None]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![c, d])?;

        let agg1 = Arc::new(Correlation::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(Correlation::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(1_f64));

        Ok(())
    }

    fn merge(
        batch1: &RecordBatch,
        batch2: &RecordBatch,
        agg1: Arc<dyn AggregateExpr>,
        agg2: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum1 = agg1.create_accumulator()?;
        let mut accum2 = agg2.create_accumulator()?;
        let expr1 = agg1.expressions();
        let expr2 = agg2.expressions();

        let values1 = expr1
            .iter()
            .map(|e| e.evaluate(batch1))
            .map(|r| r.map(|v| v.into_array(batch1.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        let values2 = expr2
            .iter()
            .map(|e| e.evaluate(batch2))
            .map(|r| r.map(|v| v.into_array(batch2.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum1.update_batch(&values1)?;
        accum2.update_batch(&values2)?;
        let state2 = get_accum_scalar_values_as_arrays(accum2.as_ref())?;
        accum1.merge_batch(&state2)?;
        accum1.evaluate()
    }
}
