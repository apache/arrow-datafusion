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
use std::sync::Arc;

use crate::aggregate::stats::StatsType;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::Float64Array;
use arrow::{
    array::{ArrayRef, UInt64Array},
    compute::cast,
    datatypes::DataType,
    datatypes::Field,
};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, AggregateState};

/// VAR and VAR_SAMP aggregate expression
#[derive(Debug)]
pub struct Variance {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
}

/// VAR_POP aggregate expression
#[derive(Debug)]
pub struct VariancePop {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
}

impl Variance {
    /// Create a new VARIANCE aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of variance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
        }
    }
}

impl AggregateExpr for Variance {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(StatsType::Sample)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "m2"),
                DataType::Float64,
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

impl VariancePop {
    /// Create a new VAR_POP aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of variance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr,
        }
    }
}

impl AggregateExpr for VariancePop {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(VarianceAccumulator::try_new(
            StatsType::Population,
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
                &format_state_name(&self.name, "mean"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "m2"),
                DataType::Float64,
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
/// The algrithm used is an online implementation and numerically stable. It is based on this paper:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.

#[derive(Debug)]
pub struct VarianceAccumulator {
    m2: f64,
    mean: f64,
    count: u64,
    stats_type: StatsType,
}

impl VarianceAccumulator {
    /// Creates a new `VarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            m2: 0_f64,
            mean: 0_f64,
            count: 0_u64,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean(&self) -> f64 {
        self.mean
    }

    pub fn get_m2(&self) -> f64 {
        self.m2
    }
}

impl Accumulator for VarianceAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![
            AggregateState::Scalar(ScalarValue::from(self.count)),
            AggregateState::Scalar(ScalarValue::from(self.mean)),
            AggregateState::Scalar(ScalarValue::from(self.m2)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &cast(&values[0], &DataType::Float64)?;
        let arr = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten();

        for value in arr {
            let new_count = self.count + 1;
            let delta1 = value - self.mean;
            let new_mean = delta1 / new_count as f64 + self.mean;
            let delta2 = value - new_mean;
            let new_m2 = self.m2 + delta1 * delta2;

            self.count += 1;
            self.mean = new_mean;
            self.m2 = new_m2;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let means = states[1].as_any().downcast_ref::<Float64Array>().unwrap();
        let m2s = states[2].as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_u64 {
                continue;
            }
            let new_count = self.count + c;
            let new_mean = self.mean * self.count as f64 / new_count as f64
                + means.value(i) * c as f64 / new_count as f64;
            let delta = self.mean - means.value(i);
            let new_m2 = self.m2
                + m2s.value(i)
                + delta * delta * self.count as f64 * c as f64 / new_count as f64;

            self.count = new_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let count = match self.stats_type {
            StatsType::Population => self.count,
            StatsType::Sample => {
                if self.count > 0 {
                    self.count - 1
                } else {
                    self.count
                }
            }
        };

        if count <= 1 {
            return Err(DataFusionError::Internal(
                "At least two values are needed to calculate variance".to_string(),
            ));
        }

        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.m2 / count as f64)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::utils::get_accum_scalar_values_as_arrays;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn variance_f64_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64, 2_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            VariancePop,
            ScalarValue::from(0.25_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64_2() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            VariancePop,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64_3() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Variance,
            ScalarValue::from(2.5_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_f64_4() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.1_f64, 2_f64, 3_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Variance,
            ScalarValue::from(0.9033333333333333_f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            VariancePop,
            ScalarValue::from(2_f64),
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
            VariancePop,
            ScalarValue::from(2.0f64),
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
            VariancePop,
            ScalarValue::from(2_f64),
            DataType::Float64
        )
    }

    #[test]
    fn test_variance_1_input() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64]));
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let agg = Arc::new(Variance::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
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
            VariancePop,
            ScalarValue::from(2.1875f64),
            DataType::Float64
        )
    }

    #[test]
    fn variance_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let agg = Arc::new(Variance::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));
        let actual = aggregate(&batch, agg);
        assert!(actual.is_err());

        Ok(())
    }

    #[test]
    fn variance_f64_merge_1() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from(vec![4_f64, 5_f64]));

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![b])?;

        let agg1 = Arc::new(VariancePop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(VariancePop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(2_f64));

        Ok(())
    }

    #[test]
    fn variance_f64_merge_2() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        let b = Arc::new(Float64Array::from(vec![None]));

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![b])?;

        let agg1 = Arc::new(VariancePop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(VariancePop::new(
            col("a", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(2_f64));

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
