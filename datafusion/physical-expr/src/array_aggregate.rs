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

//! Array Aggregate expressions

use std::sync::Arc;

use arrow::compute;
use arrow_array::{
    cast::AsArray,
    types::{Decimal128Type, Decimal256Type, Float64Type, Int64Type, UInt64Type},
    Array, ArrayRef, ArrowNumericType, GenericListArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::{cast::as_list_array, not_impl_err};

/// ArraySum aggregate expression
pub fn array_sum(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_list_array(&args[0])?;
    match list_array.value_type() {
        DataType::Int64 => {
            Ok(Arc::new(array_sum_internal::<Int64Type, i32>(list_array)?))
        }
        DataType::UInt64 => {
            Ok(Arc::new(array_sum_internal::<UInt64Type, i32>(list_array)?))
        }
        DataType::Float64 => Ok(Arc::new(array_sum_internal::<Float64Type, i32>(
            list_array,
        )?)),
        DataType::Decimal128(_, _) => Ok(Arc::new(array_sum_internal::<
            Decimal128Type,
            i32,
        >(list_array)?)),
        DataType::Decimal256(_, _) => Ok(Arc::new(array_sum_internal::<
            Decimal256Type,
            i32,
        >(list_array)?)),
        _ => not_impl_err!(
            "array_sum for type {:?} not implemented",
            list_array.value_type()
        ),
    }
}

fn array_sum_internal<T: ArrowNumericType, O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
) -> Result<PrimitiveArray<T>> {
    let mut data = Vec::with_capacity(array.len());
    for arr in array.iter() {
        if let Some(arr) = arr {
            let i64arr = arr.as_primitive::<T>();
            let sum = compute::sum_checked(i64arr)?;
            data.push(sum);
        } else {
            data.push(None);
        }
    }

    let mut values = Vec::with_capacity(array.len());
    let mut nulls = Vec::with_capacity(array.len());
    for v in data.into_iter() {
        if let Some(v) = v {
            values.push(v);
            nulls.push(true);
        } else {
            values.push(T::default_value());
            nulls.push(false);
        }
    }

    Ok(PrimitiveArray::<T>::new(
        values.into(),
        Some(NullBuffer::from(nulls)),
    ))
}
