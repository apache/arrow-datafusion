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

use crate::aggregate::approx_percentile_cont::ApproxPercentileAccumulator;
use crate::aggregate::tdigest::{Centroid, TDigest, DEFAULT_MAX_SIZE};
use crate::expressions::ApproxPercentileCont;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};

use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateState};

use std::{any::Any, sync::Arc};

/// APPROX_PERCENTILE_CONT_WITH_WEIGTH aggregate expression
#[derive(Debug)]
pub struct ApproxPercentileContWithWeight {
    approx_percentile_cont: ApproxPercentileCont,
    column_expr: Arc<dyn PhysicalExpr>,
    weight_expr: Arc<dyn PhysicalExpr>,
    percentile_expr: Arc<dyn PhysicalExpr>,
}

impl ApproxPercentileContWithWeight {
    /// Create a new [`ApproxPercentileContWithWeight`] aggregate function.
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        return_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, WeightExpr, DesiredPercentileLiteral]
        debug_assert_eq!(expr.len(), 3);

        let sub_expr = vec![expr[0].clone(), expr[2].clone()];
        let approx_percentile_cont =
            ApproxPercentileCont::new(sub_expr, name, return_type)?;

        Ok(Self {
            approx_percentile_cont,
            column_expr: expr[0].clone(),
            weight_expr: expr[1].clone(),
            percentile_expr: expr[2].clone(),
        })
    }
}

impl AggregateExpr for ApproxPercentileContWithWeight {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        self.approx_percentile_cont.field()
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self) -> Result<Vec<Field>> {
        self.approx_percentile_cont.state_fields()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![
            self.column_expr.clone(),
            self.weight_expr.clone(),
            self.percentile_expr.clone(),
        ]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let approx_percentile_cont_accumulator =
            self.approx_percentile_cont.create_plain_accumulator()?;
        let accumulator = ApproxPercentileWithWeightAccumulator::new(
            approx_percentile_cont_accumulator,
        );
        Ok(Box::new(accumulator))
    }

    fn name(&self) -> &str {
        self.approx_percentile_cont.name()
    }
}

#[derive(Debug)]
pub struct ApproxPercentileWithWeightAccumulator {
    approx_percentile_cont_accumulator: ApproxPercentileAccumulator,
}

impl ApproxPercentileWithWeightAccumulator {
    pub fn new(approx_percentile_cont_accumulator: ApproxPercentileAccumulator) -> Self {
        Self {
            approx_percentile_cont_accumulator,
        }
    }
}

impl Accumulator for ApproxPercentileWithWeightAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        self.approx_percentile_cont_accumulator.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let means = &values[0];
        let weights = &values[1];
        debug_assert_eq!(
            means.len(),
            weights.len(),
            "invalid number of values in means and weights"
        );
        let means_f64 = ApproxPercentileAccumulator::convert_to_ordered_float(means)?;
        let weights_f64 = ApproxPercentileAccumulator::convert_to_ordered_float(weights)?;
        let mut digests: Vec<TDigest> = vec![];
        for (mean, weight) in means_f64.iter().zip(weights_f64.iter()) {
            digests.push(TDigest::new_with_centroid(
                DEFAULT_MAX_SIZE,
                Centroid::new(*mean, *weight),
            ))
        }
        self.approx_percentile_cont_accumulator
            .merge_digests(&digests);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.approx_percentile_cont_accumulator.evaluate()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.approx_percentile_cont_accumulator
            .merge_batch(states)?;

        Ok(())
    }
}
