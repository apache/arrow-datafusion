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

//! Columnar value module contains a set of types that represent a columnar value.

use arrow::array::ArrayRef;
use arrow::array::NullArray;
use arrow::compute::{kernels, CastOptions};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{internal_err, Result, ScalarValue};
use std::sync::Arc;

/// Represents the result of evaluating an expression: either a single
/// [`ScalarValue`] or an [`ArrayRef`].
///
/// While a [`ColumnarValue`] can always be converted into an array
/// for convenience, it is often much more performant to provide an
/// optimized path for scalar values.
///
/// See [`ColumnarValue::values_to_arrays`] for a function that converts
/// multiple columnar values into arrays of the same length.
#[derive(Clone, Debug)]
pub enum ColumnarValue {
    /// Array of values
    Array(ArrayRef),
    /// A single value
    Scalar(ScalarValue),
}

impl From<ArrayRef> for ColumnarValue {
    fn from(value: ArrayRef) -> Self {
        ColumnarValue::Array(value)
    }
}

impl From<ScalarValue> for ColumnarValue {
    fn from(value: ScalarValue) -> Self {
        ColumnarValue::Scalar(value)
    }
}

impl ColumnarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnarValue::Array(array_value) => array_value.data_type().clone(),
            ColumnarValue::Scalar(scalar_value) => scalar_value.data_type(),
        }
    }

    /// Convert a columnar value into an Arrow [`ArrayRef`] with the specified
    /// number of rows. [`Self::Scalar`] is converted by repeating the same
    /// scalar multiple times which is not as efficient as handling the scalar
    /// directly.
    ///
    /// See [`Self::values_to_arrays`] to convert multiple columnar values into
    /// arrays of the same length.
    ///
    /// # Errors
    ///
    /// Errors if `self` is a Scalar that fails to be converted into an array of size
    pub fn into_array(self, num_rows: usize) -> Result<ArrayRef> {
        Ok(match self {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(num_rows)?,
        })
    }

    /// null columnar values are implemented as a null array in order to pass batch
    /// num_rows
    pub fn create_null_array(num_rows: usize) -> Self {
        ColumnarValue::Array(Arc::new(NullArray::new(num_rows)))
    }

    /// Converts  [`ColumnarValue`]s to [`ArrayRef`]s with the same length.
    ///
    /// # Performance Note
    ///
    /// This function expands any [`ScalarValue`] to an array. This expansion
    /// permits using a single function in terms of arrays, but it can be
    /// inefficient compared to handling the scalar value directly.
    ///
    /// Thus, it is recommended to provide specialized implementations for
    /// scalar values if performance is a concern.
    ///
    /// # Errors
    ///
    /// If there are multiple array arguments that have different lengths
    pub fn values_to_arrays(args: &[ColumnarValue]) -> Result<Vec<ArrayRef>> {
        if args.is_empty() {
            return Ok(vec![]);
        }

        let mut array_len = None;
        for arg in args {
            array_len = match (arg, array_len) {
                (ColumnarValue::Array(a), None) => Some(a.len()),
                (ColumnarValue::Array(a), Some(array_len)) => {
                    if array_len == a.len() {
                        Some(array_len)
                    } else {
                        return internal_err!(
                            "Arguments has mixed length. Expected length: {array_len}, found length: {}", a.len()
                        );
                    }
                }
                (ColumnarValue::Scalar(_), array_len) => array_len,
            }
        }

        // If array_len is none, it means there are only scalars, so make a 1 element array
        let inferred_length = array_len.unwrap_or(1);

        let args = args
            .iter()
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        Ok(args)
    }

    /// Cast's this [ColumnarValue] to the specified `DataType`
    pub fn cast_to(
        &self,
        cast_type: &DataType,
        cast_options: Option<&CastOptions<'static>>,
    ) -> Result<ColumnarValue> {
        let cast_options = cast_options.cloned().unwrap_or(DEFAULT_CAST_OPTIONS);
        match self {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                kernels::cast::cast_with_options(array, cast_type, &cast_options)?,
            )),
            ColumnarValue::Scalar(scalar) => {
                let scalar_array =
                    if cast_type == &DataType::Timestamp(TimeUnit::Nanosecond, None) {
                        if let ScalarValue::Float64(Some(float_ts)) = scalar {
                            ScalarValue::Int64(Some(
                                (float_ts * 1_000_000_000_f64).trunc() as i64,
                            ))
                            .to_array()?
                        } else {
                            scalar.to_array()?
                        }
                    } else {
                        scalar.to_array()?
                    };
                let cast_array = kernels::cast::cast_with_options(
                    &scalar_array,
                    cast_type,
                    &cast_options,
                )?;
                let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
                Ok(ColumnarValue::Scalar(cast_scalar))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_to_arrays() {
        // (input, expected)
        let cases = vec![
            // empty
            TestCase {
                input: vec![],
                expected: vec![],
            },
            // one array of length 3
            TestCase {
                input: vec![ColumnarValue::Array(make_array(1, 3))],
                expected: vec![make_array(1, 3)],
            },
            // two arrays length 3
            TestCase {
                input: vec![
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Array(make_array(2, 3)),
                ],
                expected: vec![make_array(1, 3), make_array(2, 3)],
            },
            // array and scalar
            TestCase {
                input: vec![
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                ],
                expected: vec![
                    make_array(1, 3),
                    make_array(100, 3), // scalar is expanded
                ],
            },
            // scalar and array
            TestCase {
                input: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Array(make_array(1, 3)),
                ],
                expected: vec![
                    make_array(100, 3), // scalar is expanded
                    make_array(1, 3),
                ],
            },
            // multiple scalars and array
            TestCase {
                input: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Array(make_array(1, 3)),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(200))),
                ],
                expected: vec![
                    make_array(100, 3), // scalar is expanded
                    make_array(1, 3),
                    make_array(200, 3), // scalar is expanded
                ],
            },
        ];
        for case in cases {
            case.run();
        }
    }

    #[test]
    #[should_panic(
        expected = "Arguments has mixed length. Expected length: 3, found length: 4"
    )]
    fn values_to_arrays_mixed_length() {
        ColumnarValue::values_to_arrays(&[
            ColumnarValue::Array(make_array(1, 3)),
            ColumnarValue::Array(make_array(2, 4)),
        ])
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Arguments has mixed length. Expected length: 3, found length: 7"
    )]
    fn values_to_arrays_mixed_length_and_scalar() {
        ColumnarValue::values_to_arrays(&[
            ColumnarValue::Array(make_array(1, 3)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
            ColumnarValue::Array(make_array(2, 7)),
        ])
        .unwrap();
    }

    struct TestCase {
        input: Vec<ColumnarValue>,
        expected: Vec<ArrayRef>,
    }

    impl TestCase {
        fn run(self) {
            let Self { input, expected } = self;

            assert_eq!(
                ColumnarValue::values_to_arrays(&input).unwrap(),
                expected,
                "\ninput: {input:?}\nexpected: {expected:?}"
            );
        }
    }

    /// Makes an array of length `len` with all elements set to `val`
    fn make_array(val: i32, len: usize) -> ArrayRef {
        Arc::new(arrow::array::Int32Array::from(vec![val; len]))
    }
}
