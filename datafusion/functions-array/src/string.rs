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

//! [`ScalarUDFImpl`] definitions for array_to_string and string_to_array functions.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, GenericListArray,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, ListBuilder,
    OffsetSizeTrait, StringArray, StringBuilder, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, TypeSignature};

use datafusion_common::{plan_err, DataFusionError, Result};

use std::any::{type_name, Any};

use crate::utils::{downcast_arg, make_scalar_function};
use arrow_schema::DataType::{FixedSizeList, LargeList, LargeUtf8, List, Null, Utf8};
use datafusion_common::cast::{
    as_generic_string_array, as_large_list_array, as_list_array, as_string_array,
};
use datafusion_common::exec_err;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::sync::Arc;

macro_rules! to_string {
    ($ARG:expr, $ARRAY:expr, $DELIMITER:expr, $NULL_STRING:expr, $WITH_NULL_STRING:expr, $ARRAY_TYPE:ident) => {{
        let arr = downcast_arg!($ARRAY, $ARRAY_TYPE);
        for x in arr {
            match x {
                Some(x) => {
                    $ARG.push_str(&x.to_string());
                    $ARG.push_str($DELIMITER);
                }
                None => {
                    if $WITH_NULL_STRING {
                        $ARG.push_str($NULL_STRING);
                        $ARG.push_str($DELIMITER);
                    }
                }
            }
        }
        Ok($ARG)
    }};
}

macro_rules! call_array_function {
    ($DATATYPE:expr, false) => {
        match $DATATYPE {
            DataType::Utf8 => array_function!(StringArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            _ => unreachable!(),
        }
    };
    ($DATATYPE:expr, $INCLUDE_LIST:expr) => {{
        match $DATATYPE {
            DataType::List(_) => array_function!(ListArray),
            DataType::Utf8 => array_function!(StringArray),
            DataType::LargeUtf8 => array_function!(LargeStringArray),
            DataType::Boolean => array_function!(BooleanArray),
            DataType::Float32 => array_function!(Float32Array),
            DataType::Float64 => array_function!(Float64Array),
            DataType::Int8 => array_function!(Int8Array),
            DataType::Int16 => array_function!(Int16Array),
            DataType::Int32 => array_function!(Int32Array),
            DataType::Int64 => array_function!(Int64Array),
            DataType::UInt8 => array_function!(UInt8Array),
            DataType::UInt16 => array_function!(UInt16Array),
            DataType::UInt32 => array_function!(UInt32Array),
            DataType::UInt64 => array_function!(UInt64Array),
            _ => unreachable!(),
        }
    }};
}

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    ArrayToString,
    array_to_string,
    array delimiter, // arg name
    "converts each element to its text representation.", // doc
    array_to_string_udf // internal function name
);
#[derive(Debug)]
pub(super) struct ArrayToString {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayToString {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![
                String::from("array_to_string"),
                String::from("list_to_string"),
                String::from("array_join"),
                String::from("list_join"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayToString {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Utf8,
            _ => {
                return plan_err!("The array_to_string function can only accept List/LargeList/FixedSizeList.");
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_to_string_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

make_udf_function!(
    StringToArray,
    string_to_array,
    string delimiter null_string, // arg name
    "splits a `string` based on a `delimiter` and returns an array of parts. Any parts matching the optional `null_string` will be replaced with `NULL`", // doc
    string_to_array_udf // internal function name
);
#[derive(Debug)]
pub(super) struct StringToArray {
    signature: Signature,
    aliases: Vec<String>,
}

impl StringToArray {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Uniform(2, vec![Utf8, LargeUtf8]),
                    TypeSignature::Uniform(3, vec![Utf8, LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("string_to_array"),
                String::from("string_to_list"),
            ],
        }
    }
}

impl ScalarUDFImpl for StringToArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            Utf8 | LargeUtf8 => {
                List(Arc::new(Field::new("item", arg_types[0].clone(), true)))
            }
            _ => {
                return plan_err!(
                    "The string_to_array function can only accept Utf8 or LargeUtf8."
                );
            }
        })
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            Utf8 => make_scalar_function(string_to_array_inner::<i32>)(args),
            LargeUtf8 => make_scalar_function(string_to_array_inner::<i64>)(args),
            other => {
                exec_err!("unsupported type for string_to_array function as {other}")
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_to_string SQL function
pub(super) fn array_to_string_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_to_string expects two or three arguments");
    }

    let arr = &args[0];

    let delimiters = as_string_array(&args[1])?;
    let delimiters: Vec<Option<&str>> = delimiters.iter().collect();

    let mut null_string = String::from("");
    let mut with_null_string = false;
    if args.len() == 3 {
        null_string = as_string_array(&args[2])?.value(0).to_string();
        with_null_string = true;
    }

    fn compute_array_to_string(
        arg: &mut String,
        arr: ArrayRef,
        delimiter: String,
        null_string: String,
        with_null_string: bool,
    ) -> Result<&mut String> {
        match arr.data_type() {
            List(..) => {
                let list_array = as_list_array(&arr)?;
                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            LargeList(..) => {
                let list_array = as_large_list_array(&arr)?;
                for i in 0..list_array.len() {
                    compute_array_to_string(
                        arg,
                        list_array.value(i),
                        delimiter.clone(),
                        null_string.clone(),
                        with_null_string,
                    )?;
                }

                Ok(arg)
            }
            Null => Ok(arg),
            data_type => {
                macro_rules! array_function {
                    ($ARRAY_TYPE:ident) => {
                        to_string!(
                            arg,
                            arr,
                            &delimiter,
                            &null_string,
                            with_null_string,
                            $ARRAY_TYPE
                        )
                    };
                }
                call_array_function!(data_type, false)
            }
        }
    }

    fn generate_string_array<O: OffsetSizeTrait>(
        list_arr: &GenericListArray<O>,
        delimiters: Vec<Option<&str>>,
        null_string: String,
        with_null_string: bool,
    ) -> Result<StringArray> {
        let mut res: Vec<Option<String>> = Vec::new();
        for (arr, &delimiter) in list_arr.iter().zip(delimiters.iter()) {
            if let (Some(arr), Some(delimiter)) = (arr, delimiter) {
                let mut arg = String::from("");
                let s = compute_array_to_string(
                    &mut arg,
                    arr,
                    delimiter.to_string(),
                    null_string.clone(),
                    with_null_string,
                )?
                .clone();

                if let Some(s) = s.strip_suffix(delimiter) {
                    res.push(Some(s.to_string()));
                } else {
                    res.push(Some(s));
                }
            } else {
                res.push(None);
            }
        }

        Ok(StringArray::from(res))
    }

    let arr_type = arr.data_type();
    let string_arr = match arr_type {
        List(_) | FixedSizeList(_, _) => {
            let list_array = as_list_array(&arr)?;
            generate_string_array::<i32>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        LargeList(_) => {
            let list_array = as_large_list_array(&arr)?;
            generate_string_array::<i64>(
                list_array,
                delimiters,
                null_string,
                with_null_string,
            )?
        }
        _ => {
            let mut arg = String::from("");
            let mut res: Vec<Option<String>> = Vec::new();
            // delimiter length is 1
            assert_eq!(delimiters.len(), 1);
            let delimiter = delimiters[0].unwrap();
            let s = compute_array_to_string(
                &mut arg,
                arr.clone(),
                delimiter.to_string(),
                null_string,
                with_null_string,
            )?
            .clone();

            if !s.is_empty() {
                let s = s.strip_suffix(delimiter).unwrap().to_string();
                res.push(Some(s));
            } else {
                res.push(Some(s));
            }
            StringArray::from(res)
        }
    };

    Ok(Arc::new(string_arr))
}

/// String_to_array SQL function
/// Splits string at occurrences of delimiter and returns an array of parts
/// string_to_array('abc~@~def~@~ghi', '~@~') = '["abc", "def", "ghi"]'
pub fn string_to_array_inner<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("string_to_array expects two or three arguments");
    }
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;

    let mut list_builder = ListBuilder::new(StringBuilder::with_capacity(
        string_array.len(),
        string_array.get_buffer_memory_size(),
    ));

    match args.len() {
        2 => {
            string_array.iter().zip(delimiter_array.iter()).for_each(
                |(string, delimiter)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            list_builder.values().append_value(string);
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                list_builder.values().append_value(s);
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                list_builder.values().append_value(c);
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                },
            );
        }

        3 => {
            let null_value_array = as_generic_string_array::<T>(&args[2])?;
            string_array
                .iter()
                .zip(delimiter_array.iter())
                .zip(null_value_array.iter())
                .for_each(|((string, delimiter), null_value)| {
                    match (string, delimiter) {
                        (Some(string), Some("")) => {
                            if Some(string) == null_value {
                                list_builder.values().append_null();
                            } else {
                                list_builder.values().append_value(string);
                            }
                            list_builder.append(true);
                        }
                        (Some(string), Some(delimiter)) => {
                            string.split(delimiter).for_each(|s| {
                                if Some(s) == null_value {
                                    list_builder.values().append_null();
                                } else {
                                    list_builder.values().append_value(s);
                                }
                            });
                            list_builder.append(true);
                        }
                        (Some(string), None) => {
                            string.chars().map(|c| c.to_string()).for_each(|c| {
                                if Some(c.as_str()) == null_value {
                                    list_builder.values().append_null();
                                } else {
                                    list_builder.values().append_value(c);
                                }
                            });
                            list_builder.append(true);
                        }
                        _ => list_builder.append(false), // null value
                    }
                });
        }
        _ => {
            return exec_err!(
                "Expect string_to_array function to take two or three parameters"
            )
        }
    }

    let list_array = list_builder.finish();
    Ok(Arc::new(list_array) as ArrayRef)
}
