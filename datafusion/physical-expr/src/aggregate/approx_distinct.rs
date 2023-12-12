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

use super::hyperloglog::HyperLogLog;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{
    ArrayRef, BinaryArray, GenericBinaryArray, GenericStringArray, OffsetSizeTrait,
    PrimitiveArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use arrow_array::{Int16Array, Int8Array, UInt16Array, UInt8Array};
use datafusion_common::{
    downcast_value, internal_err, not_impl_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::Accumulator;
use roaring::{self, RoaringBitmap};
use std::any::Any;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::BitOrAssign;
use std::sync::Arc;

/// APPROX_DISTINCT aggregate expression
#[derive(Debug)]
pub struct ApproxDistinct {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl ApproxDistinct {
    /// Create a new ApproxDistinct aggregate function.
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

impl AggregateExpr for ApproxDistinct {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::UInt64, false))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "hll_registers"),
            DataType::Binary,
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator: Box<dyn Accumulator> = match &self.input_data_type {
            // TODO support for boolean (trivial case)
            // https://github.com/apache/arrow-datafusion/issues/1109
            DataType::UInt8 | DataType::UInt16 | DataType::Int8 | DataType::Int16 => {
                Box::new(BitmaptAccumulator::new())
            }
            DataType::UInt32 => Box::new(NumericHLLAccumulator::<UInt32Type>::new()),
            DataType::UInt64 => Box::new(NumericHLLAccumulator::<UInt64Type>::new()),
            DataType::Int32 => Box::new(NumericHLLAccumulator::<Int32Type>::new()),
            DataType::Int64 => Box::new(NumericHLLAccumulator::<Int64Type>::new()),
            DataType::Utf8 => Box::new(StringHLLAccumulator::<i32>::new()),
            DataType::LargeUtf8 => Box::new(StringHLLAccumulator::<i64>::new()),
            DataType::Binary => Box::new(BinaryHLLAccumulator::<i32>::new()),
            DataType::LargeBinary => Box::new(BinaryHLLAccumulator::<i64>::new()),
            other => {
                return not_impl_err!(
                "Support for 'approx_distinct' for data type {other} is not implemented"
            )
            }
        };
        Ok(accumulator)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for ApproxDistinct {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct BitmaptAccumulator {
    bitmap: roaring::bitmap::RoaringBitmap,
}

impl BitmaptAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: roaring::bitmap::RoaringBitmap::new(),
        }
    }
}

#[derive(Debug)]
struct BinaryHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    hll: HyperLogLog<Vec<u8>>,
    phantom_data: PhantomData<T>,
}

impl<T> BinaryHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    /// new approx_distinct accumulator
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
            phantom_data: PhantomData,
        }
    }
}

#[derive(Debug)]
struct StringHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    hll: HyperLogLog<String>,
    phantom_data: PhantomData<T>,
}

impl<T> StringHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    /// new approx_distinct accumulator
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
            phantom_data: PhantomData,
        }
    }
}

#[derive(Debug)]
struct NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    hll: HyperLogLog<T::Native>,
}

impl<T> NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Hash,
{
    /// new approx_distinct accumulator
    pub fn new() -> Self {
        Self {
            hll: HyperLogLog::new(),
        }
    }
}

impl<T: Hash> From<&HyperLogLog<T>> for ScalarValue {
    fn from(v: &HyperLogLog<T>) -> ScalarValue {
        let values = v.as_ref().to_vec();
        ScalarValue::Binary(Some(values))
    }
}

impl<T: Hash> TryFrom<&[u8]> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &[u8]) -> Result<HyperLogLog<T>> {
        let arr: [u8; 16384] = v.try_into().map_err(|_| {
            DataFusionError::Internal(
                "Impossibly got invalid binary array from states".into(),
            )
        })?;
        Ok(HyperLogLog::<T>::new_with_registers(arr))
    }
}

impl<T: Hash> TryFrom<&ScalarValue> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &ScalarValue) -> Result<HyperLogLog<T>> {
        if let ScalarValue::Binary(Some(slice)) = v {
            slice.as_slice().try_into()
        } else {
            internal_err!(
                "Impossibly got invalid scalar value while converting to HyperLogLog"
            )
        }
    }
}

macro_rules! default_hllaccumulator_impl {
    () => {
        fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
            assert_eq!(1, states.len(), "expect only 1 element in the states");
            let binary_array = downcast_value!(states[0], BinaryArray);
            for v in binary_array.iter() {
                let v = v.ok_or_else(|| {
                    DataFusionError::Internal(
                        "Impossibly got empty binary array from states".into(),
                    )
                })?;
                let other = v.try_into()?;
                self.hll.merge(&other);
            }
            Ok(())
        }

        fn state(&self) -> Result<Vec<ScalarValue>> {
            let value = ScalarValue::from(&self.hll);
            Ok(vec![value])
        }

        fn evaluate(&self) -> Result<ScalarValue> {
            Ok(ScalarValue::UInt64(Some(self.hll.count() as u64)))
        }

        fn size(&self) -> usize {
            // HLL has static size
            std::mem::size_of_val(self)
        }
    };
}

impl<T> Accumulator for BinaryHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &GenericBinaryArray<T> =
            downcast_value!(values[0], GenericBinaryArray, T);
        // flatten because we would skip nulls
        self.hll
            .extend(array.into_iter().flatten().map(|v| v.to_vec()));
        Ok(())
    }

    default_hllaccumulator_impl!();
}

impl<T> Accumulator for StringHLLAccumulator<T>
where
    T: OffsetSizeTrait,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &GenericStringArray<T> =
            downcast_value!(values[0], GenericStringArray, T);
        // flatten because we would skip nulls
        self.hll
            .extend(array.into_iter().flatten().map(|i| i.to_string()));
        Ok(())
    }

    default_hllaccumulator_impl!();
}

impl<T> Accumulator for NumericHLLAccumulator<T>
where
    T: ArrowPrimitiveType + std::fmt::Debug,
    T::Native: Hash,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array: &PrimitiveArray<T> = downcast_value!(values[0], PrimitiveArray, T);
        // flatten because we would skip nulls
        self.hll.extend(array.into_iter().flatten());
        Ok(())
    }

    default_hllaccumulator_impl!();
}

impl Accumulator for BitmaptAccumulator {
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
            e => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for bitmap distinct count",
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

    fn size(&self) -> usize {
        self.bitmap.serialized_size()
    }
}
