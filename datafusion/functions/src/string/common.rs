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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::Result;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::ColumnarValue;

pub(crate) enum TrimType {
    Left,
    Right,
    Both,
}

impl Display for TrimType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimType::Left => write!(f, "ltrim"),
            TrimType::Right => write!(f, "rtrim"),
            TrimType::Both => write!(f, "btrim"),
        }
    }
}

pub(crate) fn general_trim<T: OffsetSizeTrait>(
    args: &[ArrayRef],
    trim_type: TrimType,
) -> Result<ArrayRef> {
    let func = match trim_type {
        TrimType::Left => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_start_matches::<&[char]>(input, pattern.as_ref())
        },
        TrimType::Right => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_end_matches::<&[char]>(input, pattern.as_ref())
        },
        TrimType::Both => |input, pattern: &str| {
            let pattern = pattern.chars().collect::<Vec<char>>();
            str::trim_end_matches::<&[char]>(
                str::trim_start_matches::<&[char]>(input, pattern.as_ref()),
                pattern.as_ref(),
            )
        },
    };

    let string_array = as_generic_string_array::<T>(&args[0])?;

    match args.len() {
        1 => {
            let result = string_array
                .iter()
                .map(|string| string.map(|string: &str| func(string, " ")))
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .map(|(string, characters)| match (string, characters) {
                    (Some(string), Some(characters)) => Some(func(string, characters)),
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
            "{trim_type} was called with {other} arguments. It requires at least 1 and at most 2."
        )
        }
    }
}

/// applies a unary expression to `args[0]` that is expected to be downcastable to
/// a `GenericStringArray` and returns a `GenericStringArray` (which may have a different offset)
/// # Errors
/// This function errors when:
/// * the number of arguments is not 1
/// * the first argument is not castable to a `GenericStringArray`
#[allow(dead_code)]
pub(crate) fn unary_string_function<'a, T, O, F, R>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<GenericStringArray<O>>
where
    R: AsRef<str>,
    O: OffsetSizeTrait,
    T: OffsetSizeTrait,
    F: Fn(&'a str) -> R,
{
    if args.len() != 1 {
        return exec_err!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name
        );
    }

    let string_array = as_generic_string_array::<T>(args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    Ok(string_array.iter().map(|string| string.map(&op)).collect())
}

#[allow(dead_code)]
pub(crate) fn handle<'a, F, R>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    R: AsRef<str>,
    F: Fn(&'a str) -> R,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_string_function::<
                    i32,
                    i32,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            DataType::LargeUtf8 => {
                Ok(ColumnarValue::Array(Arc::new(unary_string_function::<
                    i64,
                    i64,
                    _,
                    _,
                >(
                    &[a.as_ref()], op, name
                )?)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x).as_ref().to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

pub(crate) fn case_conversion<'a, F>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    F: Fn(&'a str) -> String,
{
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(convert_array::<i32, _>(
                array,
                |string| op(string),
            )?)),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(convert_array::<i64, _>(
                array,
                |string| op(string),
            )?)),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

fn convert_array<'a, O, F>(array: &'a ArrayRef, op: F) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    let string_array = as_generic_string_array::<O>(array)?;
    let value_data = string_array.value_data();

    // SAFETY: all items stored in value_data satisfy UTF8.
    // ref: impl ByteArrayNativeType for str {...}
    let str_values = unsafe { std::str::from_utf8_unchecked(value_data) };

    // conversion
    let converted_values = op(str_values);
    let bytes = converted_values.into_bytes();

    // build result
    let values = Buffer::from_vec(bytes);
    let offsets = string_array.offsets().clone();
    let nulls = string_array.nulls().cloned();

    // SAFETY: offsets and nulls are consistent with the input array.
    Ok(Arc::new(unsafe {
        GenericStringArray::<O>::new_unchecked(offsets, values, nulls)
    }))
}
