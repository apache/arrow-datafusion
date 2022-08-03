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

//! # Median

use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, PrimitiveArray, PrimitiveBuilder, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::compute::sort;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateState};
use std::any::Any;
use std::sync::Arc;

/// MEDIAN aggregate expression. This uses a lot of memory because all values need to be
/// stored in memory before a result can be computed. If an approximation is sufficient
/// then APPROX_MEDIAN provides a much more efficient solution.
#[derive(Debug)]
pub struct Median {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl Median {
    /// Create a new MEDIAN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
        }
    }
}

impl AggregateExpr for Median {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MedianAccumulator {
            data_type: self.data_type.clone(),
            all_values: vec![],
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "median"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct MedianAccumulator {
    data_type: DataType,
    all_values: Vec<ArrayRef>,
}

macro_rules! median {
    ($SELF:ident, $TY:ty, $SCALAR_TY:ident, $TWO:expr) => {{
        let combined = combine_arrays::<$TY>($SELF.all_values.as_slice())?;
        if combined.is_empty() {
            return Ok(ScalarValue::Null);
        }
        let sorted = sort(&combined, None)?;
        let array = sorted
            .as_any()
            .downcast_ref::<PrimitiveArray<$TY>>()
            .ok_or(DataFusionError::Internal(
                "median! macro failed to cast array to expected type".to_string(),
            ))?;
        let len = sorted.len();
        let mid = len / 2;
        if len % 2 == 0 {
            Ok(ScalarValue::$SCALAR_TY(Some(
                (array.value(mid - 1) + array.value(mid)) / $TWO,
            )))
        } else {
            Ok(ScalarValue::$SCALAR_TY(Some(array.value(mid))))
        }
    }};
}

impl Accumulator for MedianAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        let mut vec: Vec<AggregateState> = self
            .all_values
            .iter()
            .map(|v| AggregateState::Array(v.clone()))
            .collect();
        if vec.is_empty() {
            match self.data_type {
                DataType::UInt8 => vec.push(AggregateState::Array(Arc::new(
                    UInt8Array::from_value(0_u8, 0),
                ))),
                DataType::UInt16 => vec.push(AggregateState::Array(Arc::new(
                    UInt16Array::from_value(0_u16, 0),
                ))),
                DataType::UInt32 => vec.push(AggregateState::Array(Arc::new(
                    UInt32Array::from_value(0_u32, 0),
                ))),
                DataType::UInt64 => vec.push(AggregateState::Array(Arc::new(
                    UInt64Array::from_value(0_u64, 0),
                ))),
                DataType::Int8 => vec.push(AggregateState::Array(Arc::new(
                    Int8Array::from_value(0_i8, 0),
                ))),
                DataType::Int16 => vec.push(AggregateState::Array(Arc::new(
                    Int16Array::from_value(0_i16, 0),
                ))),
                DataType::Int32 => vec.push(AggregateState::Array(Arc::new(
                    Int32Array::from_value(0_i32, 0),
                ))),
                DataType::Int64 => vec.push(AggregateState::Array(Arc::new(
                    Int64Array::from_value(0_i64, 0),
                ))),
                DataType::Float32 => vec.push(AggregateState::Array(Arc::new(
                    Float32Array::from_value(0_f32, 0),
                ))),
                DataType::Float64 => vec.push(AggregateState::Array(Arc::new(
                    Float64Array::from_value(0_f64, 0),
                ))),
                _ => {
                    return Err(DataFusionError::Execution(
                        "unsupported data type for median".to_string(),
                    ))
                }
            }
        }
        Ok(vec)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let x = values[0].clone();
        self.all_values.extend_from_slice(&[x]);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for array in states {
            self.all_values.extend_from_slice(&[array.clone()]);
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.all_values[0].data_type() {
            DataType::Int8 => median!(self, arrow::datatypes::Int8Type, Int8, 2),
            DataType::Int16 => median!(self, arrow::datatypes::Int16Type, Int16, 2),
            DataType::Int32 => median!(self, arrow::datatypes::Int32Type, Int32, 2),
            DataType::Int64 => median!(self, arrow::datatypes::Int64Type, Int64, 2),
            DataType::UInt8 => median!(self, arrow::datatypes::UInt8Type, UInt8, 2),
            DataType::UInt16 => median!(self, arrow::datatypes::UInt16Type, UInt16, 2),
            DataType::UInt32 => median!(self, arrow::datatypes::UInt32Type, UInt32, 2),
            DataType::UInt64 => median!(self, arrow::datatypes::UInt64Type, UInt64, 2),
            DataType::Float32 => {
                median!(self, arrow::datatypes::Float32Type, Float32, 2_f32)
            }
            DataType::Float64 => {
                median!(self, arrow::datatypes::Float64Type, Float64, 2_f64)
            }
            _ => Err(DataFusionError::Execution(
                "unsupported data type for median".to_string(),
            )),
        }
    }
}

/// Combine all non-null values from provided arrays into a single array
fn combine_arrays<T: ArrowPrimitiveType>(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let len = arrays.iter().map(|a| a.len() - a.null_count()).sum();
    let mut builder: PrimitiveBuilder<T> = PrimitiveBuilder::new(len);
    for array in arrays {
        let array = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "combine_arrays failed to cast array to expected type".to_string(),
                )
            })?;
        for i in 0..array.len() {
            if !array.is_null(i) {
                builder.append_value(array.value(i));
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use crate::aggregate::median::combine_arrays;
    use arrow::array::{Int32Array, UInt32Array};
    use arrow::datatypes::{Int32Type, UInt32Type};
    use datafusion_common::Result;
    use std::sync::Arc;

    #[test]
    fn combine_i32_array() -> Result<()> {
        let a = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let b = combine_arrays::<Int32Type>(&[a.clone(), a])?;
        assert_eq!(
            "PrimitiveArray<Int32>\n[\n  1,\n  2,\n  3,\n  1,\n  2,\n  3,\n]",
            format!("{:?}", b)
        );
        Ok(())
    }

    #[test]
    fn combine_u32_array() -> Result<()> {
        let a = Arc::new(UInt32Array::from(vec![1, 2, 3]));
        let b = combine_arrays::<UInt32Type>(&[a.clone(), a])?;
        assert_eq!(
            "PrimitiveArray<UInt32>\n[\n  1,\n  2,\n  3,\n  1,\n  2,\n  3,\n]",
            format!("{:?}", b)
        );
        Ok(())
    }
}
