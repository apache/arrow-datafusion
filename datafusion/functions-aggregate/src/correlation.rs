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

//! [`Correlation`]: correlation sample aggregations.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::compute::{and, filter, is_not_null};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};

use crate::covariance::CovarianceAccumulator;
use crate::stddev::StddevAccumulator;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    type_coercion::aggregates::NUMERICS,
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr_common::aggregate::stats::StatsType;

make_udaf_expr_and_func!(
    Correlation,
    corr,
    y x,
    "Correlation between two numeric values.",
    corr_udaf
);

#[derive(Debug)]
pub struct Correlation {
    signature: Signature,
}

impl Default for Correlation {
    fn default() -> Self {
        Self::new()
    }
}

impl Correlation {
    /// Create a new COVAR_POP aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Correlation {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "corr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("Correlation requires numeric input types");
        }

        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CorrelationAccumulator::try_new()?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let name = args.name;
        Ok(vec![
            Field::new(format_state_name(name, "count"), DataType::UInt64, true),
            Field::new(format_state_name(name, "mean1"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2_1"), DataType::Float64, true),
            Field::new(format_state_name(name, "mean2"), DataType::Float64, true),
            Field::new(format_state_name(name, "m2_2"), DataType::Float64, true),
            Field::new(
                format_state_name(name, "algo_const"),
                DataType::Float64,
                true,
            ),
        ])
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
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO: null input skipping logic duplicated across Correlation
        // and its children accumulators.
        // This could be simplified by splitting up input filtering and
        // calculation logic in children accumulators, and calling only
        // calculation part from Correlation
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        self.covar.update_batch(&values)?;
        self.stddev1.update_batch(&values[0..1])?;
        self.stddev2.update_batch(&values[1..2])?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
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

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.covar)
            + self.covar.size()
            - std::mem::size_of_val(&self.stddev1)
            + self.stddev1.size()
            - std::mem::size_of_val(&self.stddev2)
            + self.stddev2.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.covar.get_count()),
            ScalarValue::from(self.covar.get_mean1()),
            ScalarValue::from(self.stddev1.get_m2()),
            ScalarValue::from(self.covar.get_mean2()),
            ScalarValue::from(self.stddev2.get_m2()),
            ScalarValue::from(self.covar.get_algo_const()),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states_c = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[3]),
            Arc::clone(&states[5]),
        ];
        let states_s1 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[1]),
            Arc::clone(&states[2]),
        ];
        let states_s2 = [
            Arc::clone(&states[0]),
            Arc::clone(&states[3]),
            Arc::clone(&states[4]),
        ];

        self.covar.merge_batch(&states_c)?;
        self.stddev1.merge_batch(&states_s1)?;
        self.stddev2.merge_batch(&states_s2)?;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        self.covar.retract_batch(&values)?;
        self.stddev1.retract_batch(&values[0..1])?;
        self.stddev2.retract_batch(&values[1..2])?;
        Ok(())
    }
}
