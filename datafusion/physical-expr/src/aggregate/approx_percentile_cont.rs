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

use crate::aggregate::tdigest::TryIntoF64;
use crate::aggregate::tdigest::{TDigest, DEFAULT_MAX_SIZE};
use crate::aggregate::utils::{down_cast_any_ref, validate_input_percentile_expr};
use crate::expressions::{format_state_name, Literal};
use crate::{AggregateExpr, PhysicalExpr};
use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field},
};
use datafusion_common::internal_err;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_expr::Accumulator;
use std::{any::Any, iter, sync::Arc};

/// APPROX_PERCENTILE_CONT aggregate expression
#[derive(Debug)]
pub struct ApproxPercentileCont {
    name: String,
    input_data_type: DataType,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    percentile: f64,
    tdigest_max_size: Option<usize>,
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

        let percentile = validate_input_percentile_expr(&expr[1])?;

        Ok(Self {
            name: name.into(),
            input_data_type,
            // The physical expr to evaluate during accumulation
            expr,
            percentile,
            tdigest_max_size: None,
        })
    }

    /// Create a new [`ApproxPercentileCont`] aggregate function.
    pub fn new_with_max_size(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredPercentileLiteral, TDigestMaxSize]
        debug_assert_eq!(expr.len(), 3);
        let percentile = validate_input_percentile_expr(&expr[1])?;
        let max_size = validate_input_max_size_expr(&expr[2])?;
        Ok(Self {
            name: name.into(),
            input_data_type,
            // The physical expr to evaluate during accumulation
            expr,
            percentile,
            tdigest_max_size: Some(max_size),
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
                if let Some(max_size) = self.tdigest_max_size {
                    ApproxPercentileAccumulator::new_with_max_size(self.percentile, t.clone(), max_size)

                }else{
                    ApproxPercentileAccumulator::new(self.percentile, t.clone())

                }
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Support for 'APPROX_PERCENTILE_CONT' for data type {other} is not implemented"
                )))
            }
        };
        Ok(accumulator)
    }
}

impl PartialEq for ApproxPercentileCont {
    fn eq(&self, other: &ApproxPercentileCont) -> bool {
        self.name == other.name
            && self.input_data_type == other.input_data_type
            && self.percentile == other.percentile
            && self.tdigest_max_size == other.tdigest_max_size
            && self.expr.len() == other.expr.len()
            && self
                .expr
                .iter()
                .zip(other.expr.iter())
                .all(|(this, other)| this.eq(other))
    }
}

fn validate_input_max_size_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    // Extract the desired percentile literal
    let lit = expr
        .as_any()
        .downcast_ref::<Literal>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "desired percentile argument must be float literal".to_string(),
            )
        })?
        .value();
    let max_size = match lit {
        ScalarValue::UInt8(Some(q)) => *q as usize,
        ScalarValue::UInt16(Some(q)) => *q as usize,
        ScalarValue::UInt32(Some(q)) => *q as usize,
        ScalarValue::UInt64(Some(q)) => *q as usize,
        ScalarValue::Int32(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int64(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int16(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int8(Some(q)) if *q > 0 => *q as usize,
        got => return Err(DataFusionError::NotImplemented(format!(
            "Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be UInt > 0 literal (got data type {}).",
            got.get_datatype()
        )))
    };
    Ok(max_size)
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
                format_state_name(&self.name, "max_size"),
                DataType::UInt64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "sum"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "max"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "min"),
                DataType::Float64,
                false,
            ),
            Field::new_list(
                format_state_name(&self.name, "centroids"),
                Field::new("item", DataType::Float64, true),
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

impl PartialEq<dyn Any> for ApproxPercentileCont {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.eq(x))
            .unwrap_or(false)
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

    pub fn new_with_max_size(
        percentile: f64,
        return_type: DataType,
        max_size: usize,
    ) -> Self {
        Self {
            digest: TDigest::new(max_size),
            percentile,
            return_type,
        }
    }

    pub(crate) fn merge_digests(&mut self, digests: &[TDigest]) {
        self.digest = TDigest::merge_digests(digests);
    }

    pub(crate) fn convert_to_float(values: &ArrayRef) -> Result<Vec<f64>> {
        match values.data_type() {
            DataType::Float64 => {
                let array = downcast_value!(values, Float64Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Float32 => {
                let array = downcast_value!(values, Float32Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int64 => {
                let array = downcast_value!(values, Int64Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int32 => {
                let array = downcast_value!(values, Int32Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int16 => {
                let array = downcast_value!(values, Int16Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::Int8 => {
                let array = downcast_value!(values, Int8Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt64 => {
                let array = downcast_value!(values, UInt64Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt32 => {
                let array = downcast_value!(values, UInt32Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt16 => {
                let array = downcast_value!(values, UInt16Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            DataType::UInt8 => {
                let array = downcast_value!(values, UInt8Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<Result<Vec<_>>>()?)
            }
            e => internal_err!(
                "APPROX_PERCENTILE_CONT is not expected to receive the type {e:?}"
            ),
        }
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state().into_iter().collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let sorted_values = &arrow::compute::sort(values, None)?;
        let sorted_values = ApproxPercentileAccumulator::convert_to_float(sorted_values)?;
        self.digest = self.digest.merge_sorted_f64(&sorted_values);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if self.digest.count() == 0.0 {
            return Err(DataFusionError::Execution(
                "aggregate function needs at least one non-null element".to_string(),
            ));
        }
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
            DataType::Float64 => ScalarValue::Float64(Some(q)),
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

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.digest.size()
            - std::mem::size_of_val(&self.digest)
            + self.return_type.size()
            - std::mem::size_of_val(&self.return_type)
    }
}
