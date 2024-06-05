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

/// macro that exports a list of function names as:
/// 1. individual functions in an `expr_fn` module
/// 2. a single function that returns a list of all functions
///
/// Equivalent to
/// ```text
/// pub mod expr_fn {
///     use super::*;
///     /// Return encode(arg)
///     pub fn encode(args: Vec<Expr>) -> Expr {
///         super::encode().call(args)
///     }
///  ...
/// /// Return a list of all functions in this package
/// pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
///     vec![
///       encode(),
///       decode()
///    ]
/// }
/// ```
///
/// Exported functions accept:
/// - `Vec<Expr>` argument (single argument followed by a comma)
/// - Variable number of `Expr` arguments (zero or more arguments, must be without commas)
macro_rules! export_functions {
    ($(($FUNC:ident, $DOC:expr, $($arg:tt)*)),*) => {
        $(
            // switch to single-function cases below
            export_functions!(single $FUNC, $DOC, $($arg)*);
        )*
    };

    // single vector argument (a single argument followed by a comma)
    (single $FUNC:ident, $DOC:expr, $arg:ident,) => {
        #[doc = $DOC]
        pub fn $FUNC($arg: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr {
            super::$FUNC().call($arg)
        }
    };

    // variadic arguments (zero or more arguments, without commas)
    (single $FUNC:ident, $DOC:expr, $($arg:ident)*) => {
        #[doc = $DOC]
        pub fn $FUNC($($arg: datafusion_expr::Expr),*) -> datafusion_expr::Expr {
            super::$FUNC().call(vec![$($arg),*])
        }
    };
}

/// Creates a singleton `ScalarUDF` of the `$UDF` function named `$GNAME` and a
/// function named `$NAME` which returns that function named $NAME.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
macro_rules! make_udf_function {
    ($UDF:ty, $GNAME:ident, $NAME:ident) => {
        /// Singleton instance of the function
        static $GNAME: std::sync::OnceLock<std::sync::Arc<datafusion_expr::ScalarUDF>> =
            std::sync::OnceLock::new();

        /// Return a [`ScalarUDF`] for [`$UDF`]
        ///
        /// [`ScalarUDF`]: datafusion_expr::ScalarUDF
        pub fn $NAME() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
            $GNAME
                .get_or_init(|| {
                    std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
                        <$UDF>::new(),
                    ))
                })
                .clone()
        }
    };
}

/// Macro creates a sub module if the feature is not enabled
///
/// The rationale for providing stub functions is to help users to configure datafusion
/// properly (so they get an error telling them why a function is not available)
/// instead of getting a cryptic "no function found" message at runtime.
macro_rules! make_stub_package {
    ($name:ident, $feature:literal) => {
        #[cfg(not(feature = $feature))]
        #[doc = concat!("Disabled. Enable via feature flag `", $feature, "`")]
        pub mod $name {
            use datafusion_expr::ScalarUDF;
            use log::debug;
            use std::sync::Arc;

            /// Returns an empty list of functions when the feature is not enabled
            pub fn functions() -> Vec<Arc<ScalarUDF>> {
                debug!("{} functions disabled", stringify!($name));
                vec![]
            }
        }
    };
}

/// Invokes a function on each element of an array and returns the result as a new array
///
/// $ARG: ArrayRef
/// $NAME: name of the function (for error messages)
/// $ARGS_TYPE: the type of array to cast the argument to
/// $RETURN_TYPE: the type of array to return
/// $FUNC: the function to apply to each element of $ARG
macro_rules! make_function_scalar_inputs_return_type {
    ($ARG: expr, $NAME:expr, $ARG_TYPE:ident, $RETURN_TYPE:ident, $FUNC: block) => {{
        let arg = downcast_arg!($ARG, $NAME, $ARG_TYPE);

        arg.iter()
            .map(|a| match a {
                Some(a) => Some($FUNC(a)),
                _ => None,
            })
            .collect::<$RETURN_TYPE>()
    }};
}

/// Downcast an argument to a specific array type, returning an internal error
/// if the cast fails
///
/// $ARG: ArrayRef
/// $NAME: name of the argument (for error messages)
/// $ARRAY_TYPE: the type of array to cast the argument to
macro_rules! downcast_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to {}",
                $NAME,
                std::any::type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

/// Macro to create a unary math UDF.
///
/// A unary math function takes an argument of type Float32 or Float64,
/// applies a unary floating function to the argument, and returns a value of the same type.
///
/// $UDF: the name of the UDF struct that implements `ScalarUDFImpl`
/// $GNAME: a singleton instance of the UDF
/// $NAME: the name of the function
/// $UNARY_FUNC: the unary function to apply to the argument
/// $OUTPUT_ORDERING: the output ordering calculation method of the function
macro_rules! make_math_unary_udf {
    ($UDF:ident, $GNAME:ident, $NAME:ident, $UNARY_FUNC:ident, $OUTPUT_ORDERING:expr) => {
        make_udf_function!($NAME::$UDF, $GNAME, $NAME);

        mod $NAME {
            use std::any::Any;
            use std::sync::Arc;

            use arrow::array::{ArrayRef, Float32Array, Float64Array};
            use arrow::datatypes::DataType;
            use datafusion_common::{exec_err, DataFusionError, Result};
            use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
            use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

            #[derive(Debug)]
            pub struct $UDF {
                signature: Signature,
            }

            impl $UDF {
                pub fn new() -> Self {
                    use DataType::*;
                    Self {
                        signature: Signature::uniform(
                            1,
                            vec![Float64, Float32],
                            Volatility::Immutable,
                        ),
                    }
                }
            }

            impl ScalarUDFImpl for $UDF {
                fn as_any(&self) -> &dyn Any {
                    self
                }
                fn name(&self) -> &str {
                    stringify!($NAME)
                }

                fn signature(&self) -> &Signature {
                    &self.signature
                }

                fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                    let arg_type = &arg_types[0];

                    match arg_type {
                        DataType::Float32 => Ok(DataType::Float32),
                        // For other types (possible values float64/null/int), use Float64
                        _ => Ok(DataType::Float64),
                    }
                }

                fn output_ordering(
                    &self,
                    input: &[ExprProperties],
                ) -> Result<SortProperties> {
                    $OUTPUT_ORDERING(input)
                }

                fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
                    let args = ColumnarValue::values_to_arrays(args)?;

                    let arr: ArrayRef = match args[0].data_type() {
                        DataType::Float64 => {
                            Arc::new(make_function_scalar_inputs_return_type!(
                                &args[0],
                                self.name(),
                                Float64Array,
                                Float64Array,
                                { f64::$UNARY_FUNC }
                            ))
                        }
                        DataType::Float32 => {
                            Arc::new(make_function_scalar_inputs_return_type!(
                                &args[0],
                                self.name(),
                                Float32Array,
                                Float32Array,
                                { f32::$UNARY_FUNC }
                            ))
                        }
                        other => {
                            return exec_err!(
                                "Unsupported data type {other:?} for function {}",
                                self.name()
                            )
                        }
                    };
                    Ok(ColumnarValue::Array(arr))
                }
            }
        }
    };
}

/// Macro to create a binary math UDF.
///
/// A binary math function takes two arguments of types Float32 or Float64,
/// applies a binary floating function to the argument, and returns a value of the same type.
///
/// $UDF: the name of the UDF struct that implements `ScalarUDFImpl`
/// $GNAME: a singleton instance of the UDF
/// $NAME: the name of the function
/// $BINARY_FUNC: the binary function to apply to the argument
/// $OUTPUT_ORDERING: the output ordering calculation method of the function
macro_rules! make_math_binary_udf {
    ($UDF:ident, $GNAME:ident, $NAME:ident, $BINARY_FUNC:ident, $OUTPUT_ORDERING:expr) => {
        make_udf_function!($NAME::$UDF, $GNAME, $NAME);

        mod $NAME {
            use std::any::Any;
            use std::sync::Arc;

            use arrow::array::{ArrayRef, Float32Array, Float64Array};
            use arrow::datatypes::DataType;
            use datafusion_common::{exec_err, DataFusionError, Result};
            use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
            use datafusion_expr::TypeSignature::*;
            use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

            #[derive(Debug)]
            pub struct $UDF {
                signature: Signature,
            }

            impl $UDF {
                pub fn new() -> Self {
                    use DataType::*;
                    Self {
                        signature: Signature::one_of(
                            vec![
                                Exact(vec![Float32, Float32]),
                                Exact(vec![Float64, Float64]),
                            ],
                            Volatility::Immutable,
                        ),
                    }
                }
            }

            impl ScalarUDFImpl for $UDF {
                fn as_any(&self) -> &dyn Any {
                    self
                }
                fn name(&self) -> &str {
                    stringify!($NAME)
                }

                fn signature(&self) -> &Signature {
                    &self.signature
                }

                fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                    let arg_type = &arg_types[0];

                    match arg_type {
                        DataType::Float32 => Ok(DataType::Float32),
                        // For other types (possible values float64/null/int), use Float64
                        _ => Ok(DataType::Float64),
                    }
                }

                fn output_ordering(
                    &self,
                    input: &[ExprProperties],
                ) -> Result<SortProperties> {
                    $OUTPUT_ORDERING(input)
                }

                fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
                    let args = ColumnarValue::values_to_arrays(args)?;

                    let arr: ArrayRef = match args[0].data_type() {
                        DataType::Float64 => Arc::new(make_function_inputs2!(
                            &args[0],
                            &args[1],
                            "y",
                            "x",
                            Float64Array,
                            { f64::$BINARY_FUNC }
                        )),

                        DataType::Float32 => Arc::new(make_function_inputs2!(
                            &args[0],
                            &args[1],
                            "y",
                            "x",
                            Float32Array,
                            { f32::$BINARY_FUNC }
                        )),
                        other => {
                            return exec_err!(
                                "Unsupported data type {other:?} for function {}",
                                self.name()
                            )
                        }
                    };
                    Ok(ColumnarValue::Array(arr))
                }
            }
        }
    };
}

macro_rules! make_function_scalar_inputs {
    ($ARG: expr, $NAME:expr, $ARRAY_TYPE:ident, $FUNC: block) => {{
        let arg = downcast_arg!($ARG, $NAME, $ARRAY_TYPE);

        arg.iter()
            .map(|a| match a {
                Some(a) => Some($FUNC(a)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE>()
    }};
}

macro_rules! make_function_inputs2 {
    ($ARG1: expr, $ARG2: expr, $NAME1:expr, $NAME2: expr, $ARRAY_TYPE:ident, $FUNC: block) => {{
        let arg1 = downcast_arg!($ARG1, $NAME1, $ARRAY_TYPE);
        let arg2 = downcast_arg!($ARG2, $NAME2, $ARRAY_TYPE);

        arg1.iter()
            .zip(arg2.iter())
            .map(|(a1, a2)| match (a1, a2) {
                (Some(a1), Some(a2)) => Some($FUNC(a1, a2.try_into().ok()?)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE>()
    }};
    ($ARG1: expr, $ARG2: expr, $NAME1:expr, $NAME2: expr, $ARRAY_TYPE1:ident, $ARRAY_TYPE2:ident, $FUNC: block) => {{
        let arg1 = downcast_arg!($ARG1, $NAME1, $ARRAY_TYPE1);
        let arg2 = downcast_arg!($ARG2, $NAME2, $ARRAY_TYPE2);

        arg1.iter()
            .zip(arg2.iter())
            .map(|(a1, a2)| match (a1, a2) {
                (Some(a1), Some(a2)) => Some($FUNC(a1, a2.try_into().ok()?)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE1>()
    }};
}

macro_rules! array_iter {
    ($ARRAY:expr) => {{
        $ARRAY.iter()
    }};
}

macro_rules! scalar_iter {
    ($ARRAY:expr) => {{
        let value = if $ARRAY.is_null(0) {
            None
        } else {
            Some($ARRAY.value(0))
        };
        std::iter::repeat(value)
    }};
}

macro_rules! make_function_3args {
    (ScalarFlags::None, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(array_iter!($ARG0), array_iter!($ARG1), array_iter!($ARG2))
    };
    (ScalarFlags::A, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(scalar_iter!($ARG0), array_iter!($ARG1), array_iter!($ARG2))
    };
    (ScalarFlags::B, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(array_iter!($ARG0), scalar_iter!($ARG1), array_iter!($ARG2))
    };
    (ScalarFlags::C, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(array_iter!($ARG0), array_iter!($ARG1), scalar_iter!($ARG2))
    };
    (ScalarFlags::AB, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(scalar_iter!($ARG0), scalar_iter!($ARG1), array_iter!($ARG2))
    };
    (ScalarFlags::AC, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(scalar_iter!($ARG0), array_iter!($ARG1), scalar_iter!($ARG2))
    };
    (ScalarFlags::BC, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(array_iter!($ARG0), scalar_iter!($ARG1), scalar_iter!($ARG2))
    };
    (ScalarFlags::ABC, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        $FUNC(array_iter!($ARG0), array_iter!($ARG1), array_iter!($ARG2))
    };
}

macro_rules! make_function_4args {
    (ScalarFlags::None, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            array_iter!($ARG1),
            array_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::A, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            array_iter!($ARG1),
            array_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::B, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            scalar_iter!($ARG1),
            array_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::C, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            array_iter!($ARG1),
            scalar_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::D, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            array_iter!($ARG1),
            array_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::AB, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            scalar_iter!($ARG1),
            array_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::AC, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            array_iter!($ARG1),
            scalar_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::AD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            array_iter!($ARG1),
            array_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::BC, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            scalar_iter!($ARG1),
            scalar_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::BD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            scalar_iter!($ARG1),
            array_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::CD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            array_iter!($ARG1),
            scalar_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::ABC, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            scalar_iter!($ARG1),
            scalar_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
    (ScalarFlags::ABD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            scalar_iter!($ARG1),
            array_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::ACD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            scalar_iter!($ARG0),
            array_iter!($ARG1),
            scalar_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::BCD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        $FUNC(
            array_iter!($ARG0),
            scalar_iter!($ARG1),
            scalar_iter!($ARG2),
            scalar_iter!($ARG3),
        )
    };
    (ScalarFlags::ABCD, $FUNC:ident, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        // all args use array_iter
        $FUNC(
            array_iter!($ARG0),
            array_iter!($ARG1),
            array_iter!($ARG2),
            array_iter!($ARG3),
        )
    };
}

macro_rules! invoke_function_impl {
    ($FLAG:expr, $FUNC:expr, $ARG0:expr, $ARG1:expr, $ARG2:expr, [$($FLAG_ITEM:tt),+]) => {
        match $FLAG {
            $(ScalarFlags::$FLAG_ITEM => {
                let func = $FUNC;
                make_function_3args!(
                    ScalarFlags::$FLAG_ITEM,
                    func,
                    $ARG0,
                    $ARG1,
                    $ARG2
                )
            }),+
            _ => unreachable!("{:?}", $FLAG),
        }
    };
    ($FLAG:expr, $FUNC:expr, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr, [$($FLAG_ITEM:tt),+]) => {
        match $FLAG {
            $(ScalarFlags::$FLAG_ITEM => {
                let func = $FUNC;
                make_function_4args!(
                    ScalarFlags::$FLAG_ITEM,
                    func,
                    $ARG0,
                    $ARG1,
                    $ARG2,
                    $ARG3
                )
            }),+
            _ => unreachable!("{:?}", $FLAG),
        }
    };
}

macro_rules! invoke_function {
    ($FLAG:expr, $FUNC:expr, $ARG0:expr, $ARG1:expr, $ARG2:expr) => {
        invoke_function_impl!(
            $FLAG,
            $FUNC,
            $ARG0,
            $ARG1,
            $ARG2,
            [None, A, B, C, AB, AC, BC, ABC]
        )
    };
    ($FLAG:expr, $FUNC:expr, $ARG0:expr, $ARG1:expr, $ARG2:expr, $ARG3:expr) => {
        invoke_function_impl!(
            $FLAG,
            $FUNC,
            $ARG0,
            $ARG1,
            $ARG2,
            $ARG3,
            [None, A, B, C, D, AB, AC, AD, BC, BD, CD, ABC, ABD, ACD, BCD, ABCD]
        )
    };
}
