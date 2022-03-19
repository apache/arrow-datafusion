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

use std::fmt::Debug;
use std::ops::BitOrAssign;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int16Array, Int32Array, Int8Array, UInt16Array,
    UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use roaring::RoaringBitmap;

use crate::{AggregateExpr, PhysicalExpr};

use super::format_state_name;

/// BITMAP_DISTINCT aggregate expression
#[derive(Debug)]
pub struct BitMapDistinct {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl BitMapDistinct {
    /// Create a new BitmapDistinct aggregate function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            expr,
        }
    }
}

impl AggregateExpr for BitMapDistinct {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::UInt64, false))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator: Box<dyn Accumulator> = match &self.input_data_type {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32 => Box::new(BitmapDistinctCountAccumulator::try_new()),
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Support for 'bitmap_distinct' for data type {} is not implemented",
                    other
                )))
            }
        };
        Ok(accumulator)
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "bitmap_registers"),
            DataType::Binary,
            false,
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
struct BitmapDistinctCountAccumulator {
    bitmap: roaring::bitmap::RoaringBitmap,
}

impl BitmapDistinctCountAccumulator {
    fn try_new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }
}

impl Accumulator for BitmapDistinctCountAccumulator {
    //state() can be used by physical nodes to aggregate states together and send them over the network/threads, to combine values.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut bytes = vec![];
        self.bitmap.serialize_into(&mut bytes).unwrap();
        Ok(vec![ScalarValue::Binary(Some(bytes))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = &values[0];
        if value.is_empty() {
            return Ok(());
        }
        match value.data_type() {
            DataType::Int8 => {
                let array = value.as_any().downcast_ref::<Int8Array>().unwrap();
                for value in array.iter() {
                    match value {
                        Some(v) => self.bitmap.insert(v as u32),
                        None => false,
                    };
                }
            }
            DataType::Int16 => {
                let array = value.as_any().downcast_ref::<Int16Array>().unwrap();
                for value in array.iter() {
                    match value {
                        Some(v) => self.bitmap.insert(v as u32),
                        None => false,
                    };
                }
            }
            DataType::Int32 => {
                let array = value.as_any().downcast_ref::<Int32Array>().unwrap();
                for value in array.iter() {
                    match value {
                        Some(v) => self.bitmap.insert(v as u32),
                        None => false,
                    };
                }
            }
            DataType::UInt8 => {
                let array = value.as_any().downcast_ref::<UInt8Array>().unwrap();
                for value in array.iter() {
                    match value {
                        Some(v) => self.bitmap.insert(v as u32),
                        None => false,
                    };
                }
            }
            DataType::UInt16 => {
                let array = value.as_any().downcast_ref::<UInt16Array>().unwrap();
                for value in array.iter() {
                    match value {
                        Some(v) => self.bitmap.insert(v as u32),
                        None => false,
                    };
                }
            }
            DataType::UInt32 => {
                let array = value.as_any().downcast_ref::<UInt32Array>().unwrap();
                for value in array.iter() {
                    match value {
                        Some(v) => self.bitmap.insert(v as u32),
                        None => false,
                    };
                }
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "BITMAP_COUNT_DISTINCT is not expected to receive the type {:?}",
                    e
                )));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let binary_array = states[0].as_any().downcast_ref::<BinaryArray>().unwrap();

        for b in binary_array.iter() {
            let v = b.ok_or_else(|| {
                DataFusionError::Internal(
                    "Impossibly got empty binary array from states".into(),
                )
            })?;
            let bitmap = RoaringBitmap::deserialize_from(&v.to_vec()[..]).unwrap();
            self.bitmap.bitor_assign(bitmap);
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::from(self.bitmap.len()))
    }
}

pub fn is_bitmap_count_distinct_supported_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
    )
}
