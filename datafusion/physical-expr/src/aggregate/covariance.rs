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

use crate::aggregate::stats::StatsType;
use crate::expressions::format_state_name;

/// COVAR and COVAR_SAMP aggregate expression
#[derive(Debug)]
pub struct Covariance {
    name: String,
    expr1: Arc<dyn PhysicalExpr>,
    expr2: Arc<dyn PhysicalExpr>,
}

/// COVAR_POP aggregate expression
#[derive(Debug)]
pub struct CovariancePop {
    name: String,
    expr1: Arc<dyn PhysicalExpr>,
    expr2: Arc<dyn PhysicalExpr>,
}

impl Covariance {
    /// Create a new COVAR aggregate function
    pub fn new(
        expr1: Arc<dyn PhysicalExpr>,
        expr2: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of covariance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr1,
            expr2,
        }
    }
}

impl AggregateExpr for Covariance {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CovarianceAccumulator::try_new(StatsType::Sample)?))
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
                &format_state_name(&self.name, "mean2"),
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

impl CovariancePop {
    /// Create a new COVAR_POP aggregate function
    pub fn new(
        expr1: Arc<dyn PhysicalExpr>,
        expr2: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        // the result of covariance just support FLOAT64 data type.
        assert!(matches!(data_type, DataType::Float64));
        Self {
            name: name.into(),
            expr1,
            expr2,
        }
    }
}

impl AggregateExpr for CovariancePop {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Float64, true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CovarianceAccumulator::try_new(
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
                &format_state_name(&self.name, "mean1"),
                DataType::Float64,
                true,
            ),
            Field::new(
                &format_state_name(&self.name, "mean2"),
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

/// An accumulator to compute covariance
/// The algrithm used is an online implementation and numerically stable. It is derived from the following paper
/// for calculating variance:
/// Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products".
/// Technometrics. 4 (3): 419–420. doi:10.2307/1266577. JSTOR 1266577.
///
/// The algorithm has been analyzed here:
/// Ling, Robert F. (1974). "Comparison of Several Algorithms for Computing Sample Means and Variances".
/// Journal of the American Statistical Association. 69 (348): 859–866. doi:10.2307/2286154. JSTOR 2286154.
///
/// Though it is not covered in the original paper but is based on the same idea, as a result the algorithm is online,
/// parallelizable and numerically stable.

#[derive(Debug)]
pub struct CovarianceAccumulator {
    algo_const: f64,
    mean1: f64,
    mean2: f64,
    count: u64,
    stats_type: StatsType,
}

impl CovarianceAccumulator {
    /// Creates a new `CovarianceAccumulator`
    pub fn try_new(s_type: StatsType) -> Result<Self> {
        Ok(Self {
            algo_const: 0_f64,
            mean1: 0_f64,
            mean2: 0_f64,
            count: 0_u64,
            stats_type: s_type,
        })
    }

    pub fn get_count(&self) -> u64 {
        self.count
    }

    pub fn get_mean1(&self) -> f64 {
        self.mean1
    }

    pub fn get_mean2(&self) -> f64 {
        self.mean2
    }

    pub fn get_algo_const(&self) -> f64 {
        self.algo_const
    }
}

impl Accumulator for CovarianceAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![
            AggregateState::Scalar(ScalarValue::from(self.count)),
            AggregateState::Scalar(ScalarValue::from(self.mean1)),
            AggregateState::Scalar(ScalarValue::from(self.mean2)),
            AggregateState::Scalar(ScalarValue::from(self.algo_const)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;

        let mut arr1 = values1
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten();
        let mut arr2 = values2
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten();

        for _i in 0..values1.len() {
            let value1 = arr1.next();
            let value2 = arr2.next();

            if value1 == None || value2 == None {
                if value1 == None && value2 == None {
                    continue;
                } else {
                    return Err(DataFusionError::Internal(
                        "The two columns are not aligned".to_string(),
                    ));
                }
            }

            let new_count = self.count + 1;
            let delta1 = value1.unwrap() - self.mean1;
            let new_mean1 = delta1 / new_count as f64 + self.mean1;
            let delta2 = value2.unwrap() - self.mean2;
            let new_mean2 = delta2 / new_count as f64 + self.mean2;
            let new_c = delta1 * (value2.unwrap() - new_mean2) + self.algo_const;

            self.count += 1;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let means1 = states[1].as_any().downcast_ref::<Float64Array>().unwrap();
        let means2 = states[2].as_any().downcast_ref::<Float64Array>().unwrap();
        let cs = states[3].as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..counts.len() {
            let c = counts.value(i);
            if c == 0_u64 {
                continue;
            }
            let new_count = self.count + c;
            let new_mean1 = self.mean1 * self.count as f64 / new_count as f64
                + means1.value(i) * c as f64 / new_count as f64;
            let new_mean2 = self.mean2 * self.count as f64 / new_count as f64
                + means2.value(i) * c as f64 / new_count as f64;
            let delta1 = self.mean1 - means1.value(i);
            let delta2 = self.mean2 - means2.value(i);
            let new_c = self.algo_const
                + cs.value(i)
                + delta1 * delta2 * self.count as f64 * c as f64 / new_count as f64;

            self.count = new_count;
            self.mean1 = new_mean1;
            self.mean2 = new_mean2;
            self.algo_const = new_c;
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
                "At least two values are needed to calculate covariance".to_string(),
            ));
        }

        if self.count == 0 {
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(Some(self.algo_const / count as f64)))
        }
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
    fn covariance_f64_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4_f64, 5_f64, 6_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            CovariancePop,
            ScalarValue::from(0.6666666666666666),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_f64_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4_f64, 5_f64, 6_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            Covariance,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_f64_4() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4.1_f64, 5_f64, 6_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            Covariance,
            ScalarValue::from(0.9033333333333335_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_f64_5() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.1_f64, 2_f64, 3_f64]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![4.1_f64, 5_f64, 6_f64]));

        generic_test_op2!(
            a,
            b,
            DataType::Float64,
            DataType::Float64,
            CovariancePop,
            ScalarValue::from(0.6022222222222223_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_f64_6() -> Result<()> {
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
            CovariancePop,
            ScalarValue::from(0.7616666666666666),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));

        generic_test_op2!(
            a,
            b,
            DataType::Int32,
            DataType::Int32,
            CovariancePop,
            ScalarValue::from(0.6666666666666666_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_u32() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32]));
        let b: ArrayRef = Arc::new(UInt32Array::from(vec![4_u32, 5_u32, 6_u32]));
        generic_test_op2!(
            a,
            b,
            DataType::UInt32,
            DataType::UInt32,
            CovariancePop,
            ScalarValue::from(0.6666666666666666_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_f32() -> Result<()> {
        let a: ArrayRef = Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32]));
        let b: ArrayRef = Arc::new(Float32Array::from(vec![4_f32, 5_f32, 6_f32]));
        generic_test_op2!(
            a,
            b,
            DataType::Float32,
            DataType::Float32,
            CovariancePop,
            ScalarValue::from(0.6666666666666666_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_i32_with_nulls_1() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(4), None, Some(6)]));

        generic_test_op2!(
            a,
            b,
            DataType::Int32,
            DataType::Int32,
            CovariancePop,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn covariance_i32_with_nulls_2() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(4), Some(5), Some(6)]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;

        let agg = Arc::new(Covariance::new(
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
    fn covariance_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;

        let agg = Arc::new(Covariance::new(
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
    fn covariance_f64_merge_1() -> Result<()> {
        let a = Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64]));
        let b = Arc::new(Float64Array::from(vec![4_f64, 5_f64, 6_f64]));
        let c = Arc::new(Float64Array::from(vec![1.1_f64, 2.2_f64, 3.3_f64]));
        let d = Arc::new(Float64Array::from(vec![4.4_f64, 5.5_f64, 6.6_f64]));

        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Float64, true),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;
        let batch2 = RecordBatch::try_new(Arc::new(schema.clone()), vec![c, d])?;

        let agg1 = Arc::new(CovariancePop::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(CovariancePop::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(0.7616666666666666));

        Ok(())
    }

    #[test]
    fn covariance_f64_merge_2() -> Result<()> {
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

        let agg1 = Arc::new(CovariancePop::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let agg2 = Arc::new(CovariancePop::new(
            col("a", &schema)?,
            col("b", &schema)?,
            "bla".to_string(),
            DataType::Float64,
        ));

        let actual = merge(&batch1, &batch2, agg1, agg2)?;
        assert!(actual == ScalarValue::from(0.6666666666666666));

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
