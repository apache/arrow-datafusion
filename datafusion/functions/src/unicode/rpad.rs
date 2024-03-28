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
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use unicode_segmentation::UnicodeSegmentation;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub(super) struct RPadFunc {
    signature: Signature,
}

impl RPadFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Int64]),
                    Exact(vec![LargeUtf8, Int64]),
                    Exact(vec![Utf8, Int64, Utf8]),
                    Exact(vec![LargeUtf8, Int64, Utf8]),
                    Exact(vec![Utf8, Int64, LargeUtf8]),
                    Exact(vec![LargeUtf8, Int64, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RPadFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "rpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "rpad")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(rpad::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(rpad::<i64>, vec![])(args),
            other => exec_err!("Unsupported data type {other:?} for function rpad"),
        }
    }
}

/// Extends the string to length 'length' by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.
/// rpad('hi', 5, 'xy') = 'hixyx'
pub fn rpad<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let length_array = as_int64_array(&args[1])?;

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .map(|(string, length)| match (string, length) {
                    (Some(string), Some(length)) => {
                        if length > i32::MAX as i64 {
                            return exec_err!(
                                "rpad requested length {length} too large"
                            );
                        }

                        let length = if length < 0 { 0 } else { length as usize };
                        if length == 0 {
                            Ok(Some("".to_string()))
                        } else {
                            let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                            if length < graphemes.len() {
                                Ok(Some(graphemes[..length].concat()))
                            } else {
                                let mut s = string.to_string();
                                s.push_str(" ".repeat(length - graphemes.len()).as_str());
                                Ok(Some(s))
                            }
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<GenericStringArray<T>>>()?;
            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let length_array = as_int64_array(&args[1])?;
            let fill_array = as_generic_string_array::<T>(&args[2])?;

            let result = string_array
                .iter()
                .zip(length_array.iter())
                .zip(fill_array.iter())
                .map(|((string, length), fill)| match (string, length, fill) {
                    (Some(string), Some(length), Some(fill)) => {
                        if length > i32::MAX as i64 {
                            return exec_err!(
                                "rpad requested length {length} too large"
                            );
                        }

                        let length = if length < 0 { 0 } else { length as usize };
                        let graphemes = string.graphemes(true).collect::<Vec<&str>>();
                        let fill_chars = fill.chars().collect::<Vec<char>>();

                        if length < graphemes.len() {
                            Ok(Some(graphemes[..length].concat()))
                        } else if fill_chars.is_empty() {
                            Ok(Some(string.to_string()))
                        } else {
                            let mut s = string.to_string();
                            let mut char_vector =
                                Vec::<char>::with_capacity(length - graphemes.len());
                            for l in 0..length - graphemes.len() {
                                char_vector
                                    .push(*fill_chars.get(l % fill_chars.len()).unwrap());
                            }
                            s.push_str(char_vector.iter().collect::<String>().as_str());
                            Ok(Some(s))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => exec_err!(
            "rpad was called with {other} arguments. It requires at least 2 and at most 3."
        ),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::rpad::RPadFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("josé")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("josé ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("xy")))),
            ],
            Ok(Some("hixyx")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(21))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcdef")))),
            ],
            Ok(Some("hiabcdefabcdefabcdefa")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(" ")))),
            ],
            Ok(Some("hi   ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("")))),
            ],
            Ok(Some("hi")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("xy")))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("xy")))),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("hi")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("josé")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("xy")))),
            ],
            Ok(Some("joséxyxyxy")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(feature = "unicode_expressions")]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("josé")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(10))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("éñ")))),
            ],
            Ok(Some("josééñéñéñ")),
            &str,
            Utf8,
            StringArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            RPadFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("josé")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ],
            internal_err!(
                "function rpad requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
