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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use bitflags::bitflags;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};
use datafusion_physical_expr::functions::Hint;
use std::sync::Arc;

/// Creates a function to identify the optimal return type of a string function given
/// the type of its first argument.
///
/// If the input type is `LargeUtf8` or `LargeBinary` the return type is
/// `$largeUtf8Type`,
///
/// If the input type is `Utf8` or `Binary` the return type is `$utf8Type`,
macro_rules! get_optimal_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        pub(crate) fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                // LargeBinary inputs are automatically coerced to Utf8
                DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                // Binary inputs are automatically coerced to Utf8
                DataType::Utf8 | DataType::Binary => $utf8Type,
                DataType::Null => DataType::Null,
                DataType::Dictionary(_, value_type) => match **value_type {
                    DataType::LargeUtf8 | DataType::LargeBinary => $largeUtf8Type,
                    DataType::Utf8 | DataType::Binary => $utf8Type,
                    DataType::Null => DataType::Null,
                    _ => {
                        return datafusion_common::exec_err!(
                            "The {} function can only accept strings, but got {:?}.",
                            name.to_uppercase(),
                            **value_type
                        );
                    }
                },
                data_type => {
                    return datafusion_common::exec_err!(
                        "The {} function can only accept strings, but got {:?}.",
                        name.to_uppercase(),
                        data_type
                    );
                }
            })
        }
    };
}

// `utf8_to_str_type`: returns either a Utf8 or LargeUtf8 based on the input type size.
get_optimal_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);

// `utf8_to_int_type`: returns either a Int32 or Int64 based on the input type size.
get_optimal_return_type!(utf8_to_int_type, DataType::Int64, DataType::Int32);

/// Creates a scalar function implementation for the given function.
/// * `inner` - the function to be executed
/// * `hints` - hints to be used when expanding scalars to arrays
pub(super) fn make_scalar_function<F>(
    inner: F,
    hints: Vec<Hint>,
) -> ScalarFunctionImplementation
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

        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
            .map(|(arg, hint)| {
                // Decide on the length to expand this scalar to depending
                // on the given hints.
                let expansion_len = match hint {
                    Hint::AcceptsSingular => 1,
                    Hint::Pad => inferred_length,
                };
                arg.clone().into_array(expansion_len)
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

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

#[cfg(test)]
pub mod test {
    /// $FUNC ScalarUDFImpl to test
    /// $ARGS arguments (vec) to pass to function
    /// $EXPECTED a Result<ColumnarValue>
    /// $EXPECTED_TYPE is the expected value type
    /// $EXPECTED_DATA_TYPE is the expected result type
    /// $ARRAY_TYPE is the column type after function applied
    macro_rules! test_function {
        ($FUNC:expr, $ARGS:expr, $EXPECTED:expr, $EXPECTED_TYPE:ty, $EXPECTED_DATA_TYPE:expr, $ARRAY_TYPE:ident) => {
            let expected: Result<Option<$EXPECTED_TYPE>> = $EXPECTED;
            let func = $FUNC;

            let type_array = $ARGS.iter().map(|arg| arg.data_type()).collect::<Vec<_>>();
            let return_type = func.return_type(&type_array);

            match expected {
                Ok(expected) => {
                    assert_eq!(return_type.is_ok(), true);
                    assert_eq!(return_type.unwrap(), $EXPECTED_DATA_TYPE);

                    let result = func.invoke($ARGS);
                    assert_eq!(result.is_ok(), true);

                    let len = $ARGS
                        .iter()
                        .fold(Option::<usize>::None, |acc, arg| match arg {
                            ColumnarValue::Scalar(_) => acc,
                            ColumnarValue::Array(a) => Some(a.len()),
                        });
                    let inferred_length = len.unwrap_or(1);
                    let result = result.unwrap().clone().into_array(inferred_length).expect("Failed to convert to array");
                    let result = result.as_any().downcast_ref::<$ARRAY_TYPE>().expect("Failed to convert to type");

                    // value is correct
                    match expected {
                        Some(v) => assert_eq!(result.value(0), v),
                        None => assert!(result.is_null(0)),
                    };
                }
                Err(expected_error) => {
                    if return_type.is_err() {
                        match return_type {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => { datafusion_common::assert_contains!(expected_error.strip_backtrace(), error.strip_backtrace()); }
                        }
                    }
                    else {
                        // invoke is expected error - cannot use .expect_err() due to Debug not being implemented
                        match func.invoke($ARGS) {
                            Ok(_) => assert!(false, "expected error"),
                            Err(error) => {
                                assert!(expected_error.strip_backtrace().starts_with(&error.strip_backtrace()));
                            }
                        }
                    }
                }
            };
        };
    }

    #[allow(unused_imports)]
    pub(crate) use test_function;
}

bitflags! {
    /// Represents the position of the Scalar in args.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct ScalarFlags: u8 {
        // There are no Scalars in the args
        const None = 0b00000000;

        // Represents the 1th arg is Scalar
        const A = 0b00000001;
        // Represents the 2th arg is Scalar
        const B = 0b00000010;
        // Represents the 3th arg is Scalar
        const C = 0b00000100;
        // Represents the 4th arg is Scalar
        const D = 0b00001000;

        const AB = Self::A.bits() | Self::B.bits();
        const AC = Self::A.bits() | Self::C.bits();
        const AD = Self::A.bits() | Self::D.bits();
        const BC = Self::B.bits() | Self::C.bits();
        const BD = Self::B.bits() | Self::D.bits();
        const CD = Self::C.bits() | Self::D.bits();

        const ABC = Self::A.bits() | Self::B.bits() | Self::C.bits();
        const ABD = Self::A.bits() | Self::B.bits() | Self::D.bits();
        const ACD = Self::A.bits() | Self::C.bits() | Self::D.bits();
        const BCD = Self::B.bits() | Self::C.bits() | Self::D.bits();

        const ABCD = Self::A.bits() | Self::B.bits() | Self::C.bits() | Self::D.bits();
    }
}

impl ScalarFlags {
    pub fn try_create(args: &[ArrayRef]) -> Result<Self> {
        let mut flag: u8 = 0;
        args.iter().enumerate().for_each(|(i, arg)| {
            if arg.len() == 1 {
                flag |= 1 << i;
            }
        });
        Self::from_bits(flag).ok_or_else(|| {
            DataFusionError::Execution(format!("Unsupported ScalarFlags: {}", flag))
        })
    }
}
