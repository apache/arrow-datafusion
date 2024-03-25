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
use datafusion_common::{exec_err, Result};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::string::common::*;

#[derive(Debug)]
pub(super) struct SplitPartFunc {
    signature: Signature,
}

impl SplitPartFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![LargeUtf8, Utf8, Int64]),
                    Exact(vec![Utf8, LargeUtf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SplitPartFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "split_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "split_part")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(split_part::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(split_part::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function split_part")
            }
        }
    }
}

/// Splits string at occurrences of delimiter and returns the n'th field (counting from one).
/// split_part('abc~@~def~@~ghi', '~@~', 2) = 'def'
fn split_part<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;
    let n_array = as_int64_array(&args[2])?;
    let result = string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(n_array.iter())
        .map(|((string, delimiter), n)| match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                if n <= 0 {
                    exec_err!("field position must be greater than zero")
                } else {
                    let split_string: Vec<&str> = string.split(delimiter).collect();
                    match split_string.get(n as usize - 1) {
                        Some(s) => Ok(Some(*s)),
                        None => Ok(Some("")),
                    }
                }
            }
            _ => Ok(None),
        })
        .collect::<Result<GenericStringArray<T>>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType::Utf8;

    use datafusion_common::ScalarValue;
    use datafusion_common::{exec_err, Result};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::common::test::test_function;
    use crate::string::split_part::SplitPartFunc;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ],
            Ok(Some("def")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(20))),
            ],
            Ok(Some("")),
            &str,
            Utf8,
            StringArray
        );
        test_function!(
            SplitPartFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from(
                    "abc~@~def~@~ghi"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("~@~")))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ],
            exec_err!("field position must be greater than zero"),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
