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

use std::any::Any;
use std::cmp::{max, Ordering};
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::exec_err;
use datafusion_common::Result;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::utils::{make_scalar_function, utf8_to_str_type};

#[derive(Debug)]
pub(super) struct RightFunc {
    signature: Signature,
}

impl RightFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Int64]), Exact(vec![LargeUtf8, Int64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RightFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "right"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "right")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(right::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(right::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function right"),
        }
    }
}

/// Returns last n characters in the string, or when n is negative, returns all but first |n| characters.
/// right('abcde', 2) = 'de'
/// The implementation uses UTF-8 code points as characters
pub fn right<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let n_array = as_int64_array(&args[1])?;

    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => match n.cmp(&0) {
                Ordering::Less => Some(
                    string
                        .chars()
                        .skip(n.unsigned_abs() as usize)
                        .collect::<String>(),
                ),
                Ordering::Equal => Some("".to_string()),
                Ordering::Greater => Some(
                    string
                        .chars()
                        .skip(max(string.chars().count() as i64 - n, 0) as usize)
                        .collect::<String>(),
                ),
            },
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::right::RightFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("de")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(200))),
            ],
            Ok(Some("abcde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-2))),
            ],
            Ok(Some("cde")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-200))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("joséésoj")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RightFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("joséésoj")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-3))),
            ],
            Ok(Some("éésoj")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            Right,
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcde")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            internal_err!(
                "function right requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
