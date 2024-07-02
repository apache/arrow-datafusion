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

//! array function utils

use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::DataType};

use arrow_array::{
    Array, BooleanArray, GenericListArray, ListArray, OffsetSizeTrait, Scalar,
    UInt32Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Field;
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};

use core::any::type_name;
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionImplementation};

macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast to {}",
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}
pub(crate) use downcast_arg;

pub(crate) fn check_datatypes(name: &str, args: &[&ArrayRef]) -> Result<()> {
    let data_type = args[0].data_type();
    if !args.iter().all(|arg| {
        arg.data_type().equals_datatype(data_type)
            || arg.data_type().equals_datatype(&DataType::Null)
    }) {
        let types = args.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
        return plan_err!("{name} received incompatible types: '{types:?}'.");
    }

    Ok(())
}

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    })
}

pub(crate) fn align_array_dimensions<O: OffsetSizeTrait>(
    args: Vec<ArrayRef>,
) -> Result<Vec<ArrayRef>> {
    let args_ndim = args
        .iter()
        .map(|arg| datafusion_common::utils::list_ndims(arg.data_type()))
        .collect::<Vec<_>>();
    let max_ndim = args_ndim.iter().max().unwrap_or(&0);

    // Align the dimensions of the arrays
    let aligned_args: Result<Vec<ArrayRef>> = args
        .into_iter()
        .zip(args_ndim.iter())
        .map(|(array, ndim)| {
            if ndim < max_ndim {
                let mut aligned_array = Arc::clone(&array);
                for _ in 0..(max_ndim - ndim) {
                    let data_type = aligned_array.data_type().to_owned();
                    let array_lengths = vec![1; aligned_array.len()];
                    let offsets = OffsetBuffer::<O>::from_lengths(array_lengths);

                    aligned_array = Arc::new(GenericListArray::<O>::try_new(
                        Arc::new(Field::new("item", data_type, true)),
                        offsets,
                        aligned_array,
                        None,
                    )?)
                }
                Ok(aligned_array)
            } else {
                Ok(Arc::clone(&array))
            }
        })
        .collect();

    aligned_args
}

/// Computes a BooleanArray indicating equality or inequality between elements in a list array and a specified element array.
///
/// # Arguments
///
/// * `list_array_row` - A reference to a trait object implementing the Arrow `Array` trait. It represents the list array for which the equality or inequality will be compared.
///
/// * `element_array` - A reference to a trait object implementing the Arrow `Array` trait. It represents the array with which each element in the `list_array_row` will be compared.
///
/// * `row_index` - The index of the row in the `element_array` and `list_array` to use for the comparison.
///
/// * `eq` - A boolean flag. If `true`, the function computes equality; if `false`, it computes inequality.
///
/// # Returns
///
/// Returns a `Result<BooleanArray>` representing the comparison results. The result may contain an error if there are issues with the computation.
///
/// # Example
///
/// ```text
/// compare_element_to_list(
///     [1, 2, 3], [1, 2, 3], 0, true => [true, false, false]
///     [1, 2, 3, 3, 2, 1], [1, 2, 3], 1, true => [false, true, false, false, true, false]
///
///     [[1, 2, 3], [2, 3, 4], [3, 4, 5]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 0, true => [true, false, false]
///     [[1, 2, 3], [2, 3, 4], [2, 3, 4]], [[1, 2, 3], [2, 3, 4], [3, 4, 5]], 1, false => [true, false, false]
/// )
/// ```
pub(crate) fn compare_element_to_list(
    list_array_row: &dyn Array,
    element_array: &dyn Array,
    row_index: usize,
    eq: bool,
) -> Result<BooleanArray> {
    if list_array_row.data_type() != element_array.data_type() {
        return exec_err!(
            "compare_element_to_list received incompatible types: '{:?}' and '{:?}'.",
            list_array_row.data_type(),
            element_array.data_type()
        );
    }

    let indices = UInt32Array::from(vec![row_index as u32]);
    let element_array_row = arrow::compute::take(element_array, &indices, None)?;

    // Compute all positions in list_row_array (that is itself an
    // array) that are equal to `from_array_row`
    let res = match element_array_row.data_type() {
        // arrow_ord::cmp::eq does not support ListArray, so we need to compare it by loop
        DataType::List(_) => {
            // compare each element of the from array
            let element_array_row_inner = as_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        DataType::LargeList(_) => {
            // compare each element of the from array
            let element_array_row_inner =
                as_large_list_array(&element_array_row)?.value(0);
            let list_array_row_inner = as_large_list_array(list_array_row)?;

            list_array_row_inner
                .iter()
                // compare element by element the current row of list_array
                .map(|row| {
                    row.map(|row| {
                        if eq {
                            row.eq(&element_array_row_inner)
                        } else {
                            row.ne(&element_array_row_inner)
                        }
                    })
                })
                .collect::<BooleanArray>()
        }
        _ => {
            let element_arr = Scalar::new(element_array_row);
            // use not_distinct so we can compare NULL
            if eq {
                arrow_ord::cmp::not_distinct(&list_array_row, &element_arr)?
            } else {
                arrow_ord::cmp::distinct(&list_array_row, &element_arr)?
            }
        }
    };

    Ok(res)
}

/// Returns the length of each array dimension
pub(crate) fn compute_array_dims(
    arr: Option<ArrayRef>,
) -> Result<Option<Vec<Option<u64>>>> {
    let mut value = match arr {
        Some(arr) => arr,
        None => return Ok(None),
    };
    if value.is_empty() {
        return Ok(None);
    }
    let mut res = vec![Some(value.len() as u64)];

    loop {
        match value.data_type() {
            DataType::List(..) => {
                value = downcast_arg!(value, ListArray).value(0);
                res.push(Some(value.len() as u64));
            }
            _ => return Ok(Some(res)),
        }
    }
}

/// Returns the name of the argument at index `i`, or an empty string if the index is out of bounds.
pub(super) fn get_arg_name(args: &[Expr], i: usize) -> String {
    args.get(i).map(ToString::to_string).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Int64Type;
    use datafusion_common::utils::array_into_list_array_nullable;

    /// Only test internal functions, array-related sql functions will be tested in sqllogictest `array.slt`
    #[test]
    fn test_align_array_dimensions() {
        let array1d_1 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
            ]));
        let array1d_2 =
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(6), Some(7), Some(8)]),
            ]));

        let array2d_1 = Arc::new(array_into_list_array_nullable(
            Arc::clone(&array1d_1) as ArrayRef
        )) as ArrayRef;
        let array2d_2 = Arc::new(array_into_list_array_nullable(
            Arc::clone(&array1d_2) as ArrayRef
        )) as ArrayRef;

        let res = align_array_dimensions::<i32>(vec![
            array1d_1.to_owned(),
            array2d_2.to_owned(),
        ])
        .unwrap();

        let expected = as_list_array(&array2d_1).unwrap();
        let expected_dim = datafusion_common::utils::list_ndims(array2d_1.data_type());
        assert_ne!(as_list_array(&res[0]).unwrap(), expected);
        assert_eq!(
            datafusion_common::utils::list_ndims(res[0].data_type()),
            expected_dim
        );

        let array3d_1 = Arc::new(array_into_list_array_nullable(array2d_1)) as ArrayRef;
        let array3d_2 = array_into_list_array_nullable(array2d_2.to_owned());
        let res =
            align_array_dimensions::<i32>(vec![array1d_1, Arc::new(array3d_2.clone())])
                .unwrap();

        let expected = as_list_array(&array3d_1).unwrap();
        let expected_dim = datafusion_common::utils::list_ndims(array3d_1.data_type());
        assert_ne!(as_list_array(&res[0]).unwrap(), expected);
        assert_eq!(
            datafusion_common::utils::list_ndims(res[0].data_type()),
            expected_dim
        );
    }
}
