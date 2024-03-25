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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! String expressions

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, GenericStringArray, Int32Array, Int64Array, OffsetSizeTrait,
        StringArray,
    },
    datatypes::DataType,
};

use datafusion_common::Result;
use datafusion_common::{
    cast::{as_generic_string_array, as_string_array},
    exec_err, ScalarValue,
};
use datafusion_expr::ColumnarValue;

/// Concatenates the text representations of all the arguments. NULL arguments are ignored.
/// concat('abcde', 2, NULL, 22) = 'abcde222'
pub fn concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return exec_err!(
            "concat was called with {} arguments. It requires at least 1.",
            args.len()
        );
    }

    // first, decide whether to return a scalar or a vector.
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });
    if let Some(size) = return_array.next() {
        let result = (0..size)
            .map(|index| {
                let mut owned_string: String = "".to_owned();
                for arg in args {
                    match arg {
                        ColumnarValue::Scalar(ScalarValue::Utf8(maybe_value)) => {
                            if let Some(value) = maybe_value {
                                owned_string.push_str(value);
                            }
                        }
                        ColumnarValue::Array(v) => {
                            if v.is_valid(index) {
                                let v = as_string_array(v).unwrap();
                                owned_string.push_str(v.value(index));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Some(owned_string)
            })
            .collect::<StringArray>();

        Ok(ColumnarValue::Array(Arc::new(result)))
    } else {
        // short avenue with only scalars
        let initial = Some("".to_string());
        let result = args.iter().fold(initial, |mut acc, rhs| {
            if let Some(ref mut inner) = acc {
                match rhs {
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => {
                        inner.push_str(v);
                    }
                    ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
                    _ => unreachable!(""),
                };
            };
            acc
        });
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
    }
}

/// Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored.
/// concat_ws(',', 'abcde', 2, NULL, 22) = 'abcde,2,22'
pub fn concat_ws(args: &[ArrayRef]) -> Result<ArrayRef> {
    // downcast all arguments to strings
    let args = args
        .iter()
        .map(|e| as_string_array(e))
        .collect::<Result<Vec<&StringArray>>>()?;

    // do not accept 0 or 1 arguments.
    if args.len() < 2 {
        return exec_err!(
            "concat_ws was called with {} arguments. It requires at least 2.",
            args.len()
        );
    }

    // first map is the iterator, second is for the `Option<_>`
    let result = args[0]
        .iter()
        .enumerate()
        .map(|(index, x)| {
            x.map(|sep: &str| {
                let string_vec = args[1..]
                    .iter()
                    .flat_map(|arg| {
                        if !arg.is_null(index) {
                            Some(arg.value(index))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<&str>>();
                string_vec.join(sep)
            })
        })
        .collect::<StringArray>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.
/// initcap('hi THOMAS') = 'Hi Thomas'
pub fn initcap<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    let result = string_array
        .iter()
        .map(|string| {
            string.map(|string: &str| {
                let mut char_vector = Vec::<char>::new();
                let mut previous_character_letter_or_number = false;
                for c in string.chars() {
                    if previous_character_letter_or_number {
                        char_vector.push(c.to_ascii_lowercase());
                    } else {
                        char_vector.push(c.to_ascii_uppercase());
                    }
                    previous_character_letter_or_number = c.is_ascii_uppercase()
                        || c.is_ascii_lowercase()
                        || c.is_ascii_digit();
                }
                char_vector.iter().collect::<String>()
            })
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the position of the first occurrence of substring in string.
/// The position is counted from 1. If the substring is not found, returns 0.
/// For example, instr('Helloworld', 'world') = 6.
pub fn instr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let substr_array = as_generic_string_array::<T>(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let result = string_array
                .iter()
                .zip(substr_array.iter())
                .map(|(string, substr)| match (string, substr) {
                    (Some(string), Some(substr)) => string
                        .find(substr)
                        .map_or(Some(0), |index| Some((index + 1) as i32)),
                    _ => None,
                })
                .collect::<Int32Array>();

            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = string_array
                .iter()
                .zip(substr_array.iter())
                .map(|(string, substr)| match (string, substr) {
                    (Some(string), Some(substr)) => string
                        .find(substr)
                        .map_or(Some(0), |index| Some((index + 1) as i64)),
                    _ => None,
                })
                .collect::<Int64Array>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "instr was called with {other} datatype arguments. It requires Utf8 or LargeUtf8."
            )
        }
    }
}

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::starts_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns true if string ends with suffix.
/// ends_with('alphabet', 'abet') = 't'
pub fn ends_with<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let left = as_generic_string_array::<T>(&args[0])?;
    let right = as_generic_string_array::<T>(&args[1])?;

    let result = arrow::compute::kernels::comparison::ends_with(left, right)?;

    Ok(Arc::new(result) as ArrayRef)
}
