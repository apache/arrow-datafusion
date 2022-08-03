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

use crate::aggregate::tdigest::TryIntoOrderedF64;
use crate::aggregate::tdigest::{TDigest, DEFAULT_MAX_SIZE};
use crate::expressions::{format_state_name, Literal};
use crate::{AggregateExpr, PhysicalExpr};
use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field},
};
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{Accumulator, AggregateState};
use ordered_float::OrderedFloat;
use std::{any::Any, iter, sync::Arc};

/// APPROX_PERCENTILE_CONT aggregate expression
#[derive(Debug)]
pub struct ApproxPercentileCont {
    name: String,
    input_data_type: DataType,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    percentile: f64,
}

impl ApproxPercentileCont {
    /// Create a new [`ApproxPercentileCont`] aggregate function.
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredPercentileLiteral]
        debug_assert_eq!(expr.len(), 2);

        // Extract the desired percentile literal
        let lit = expr[1]
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "desired percentile argument must be float literal".to_string(),
                )
            })?
            .value();
        let percentile = match lit {
            ScalarValue::Float32(Some(q)) => *q as f64,
            ScalarValue::Float64(Some(q)) => *q as f64,
            got => return Err(DataFusionError::NotImplemented(format!(
                "Percentile value for 'APPROX_PERCENTILE_CONT' must be Float32 or Float64 literal (got data type {})",
                got
            )))
        };

        // Ensure the percentile is between 0 and 1.
        if !(0.0..=1.0).contains(&percentile) {
            return Err(DataFusionError::Plan(format!(
                "Percentile value must be between 0.0 and 1.0 inclusive, {} is invalid",
                percentile
            )));
        }

        Ok(Self {
            name: name.into(),
            input_data_type,
            // The physical expr to evaluate during accumulation
            expr,
            percentile,
        })
    }

    pub(crate) fn create_plain_accumulator(&self) -> Result<ApproxPercentileAccumulator> {
        let accumulator: ApproxPercentileAccumulator = match &self.input_data_type {
            t @ (DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64) => {
                ApproxPercentileAccumulator::new(self.percentile, t.clone())
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Support for 'APPROX_PERCENTILE_CONT' for data type {} is not implemented",
                    other
                )))
            }
        };
        Ok(accumulator)
    }
}

impl AggregateExpr for ApproxPercentileCont {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), false))
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "max_size"),
                DataType::UInt64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "sum"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "max"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "min"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "centroids"),
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr.clone()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator = self.create_plain_accumulator()?;
        Ok(Box::new(accumulator))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub struct ApproxPercentileAccumulator {
    digest: TDigest,
    percentile: f64,
    return_type: DataType,
}

impl ApproxPercentileAccumulator {
    pub fn new(percentile: f64, return_type: DataType) -> Self {
        Self {
            digest: TDigest::new(DEFAULT_MAX_SIZE),
            percentile,
            return_type,
        }
    }

    pub(crate) fn merge_digests(&mut self, digests: &[TDigest]) {
        self.digest = TDigest::merge_digests(digests);
    }

    pub(crate) fn convert_to_ordered_float(
        values: &ArrayRef,
    ) -> Result<Vec<OrderedFloat<f64>>> {
        match values.data_type() {
            DataType::Float64 => {
                let array = values.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Float32 => {
                let array = values.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int64 => {
                let array = values.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int32 => {
                let array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int16 => {
                let array = values.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int8 => {
                let array = values.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt64 => {
                let array = values.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt32 => {
                let array = values.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt16 => {
                let array = values.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt8 => {
                let array = values.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            e => Err(DataFusionError::Internal(format!(
                "APPROX_PERCENTILE_CONT is not expected to receive the type {:?}",
                e
            ))),
        }
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(self
            .digest
            .to_scalar_state()
            .iter()
            .map(|v| AggregateState::Scalar(v.clone()))
            .collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let unsorted_values =
            ApproxPercentileAccumulator::convert_to_ordered_float(values)?;
        self.digest = self.digest.merge_unsorted_f64(unsorted_values);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let q = self.digest.estimate_quantile(self.percentile);

        // These acceptable return types MUST match the validation in
        // ApproxPercentile::create_accumulator.
        Ok(match &self.return_type {
            DataType::Int8 => ScalarValue::Int8(Some(q as i8)),
            DataType::Int16 => ScalarValue::Int16(Some(q as i16)),
            DataType::Int32 => ScalarValue::Int32(Some(q as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(q as i64)),
            DataType::UInt8 => ScalarValue::UInt8(Some(q as u8)),
            DataType::UInt16 => ScalarValue::UInt16(Some(q as u16)),
            DataType::UInt32 => ScalarValue::UInt32(Some(q as u32)),
            DataType::UInt64 => ScalarValue::UInt64(Some(q as u64)),
            DataType::Float32 => ScalarValue::Float32(Some(q as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(q as f64)),
            v => unreachable!("unexpected return type {:?}", v),
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let states = (0..states[0].len())
            .map(|index| {
                states
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<Result<Vec<_>>>()
                    .map(|state| TDigest::from_scalar_state(&state))
            })
            .chain(iter::once(Ok(self.digest.clone())))
            .collect::<Result<Vec<_>>>()?;

        self.merge_digests(&states);

        Ok(())
    }
}
