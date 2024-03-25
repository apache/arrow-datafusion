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

//! [`ScalarUDFImpl`] definitions for array_union, array_intersect and array_distinct functions.

use crate::make_array::make_array_inner;
use crate::utils::make_scalar_function;
use arrow::array::{new_empty_array, Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow::buffer::OffsetBuffer;
use arrow::compute;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{RowConverter, SortField};
use arrow_schema::DataType::{FixedSizeList, LargeList, List, Null};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use itertools::Itertools;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_function!(
    ArrayUnion,
    array_union,
    array1 array2,
    "returns an array of the elements in the union of array1 and array2 without duplicates.",
    array_union_udf
);

make_udf_function!(
    ArrayIntersect,
    array_intersect,
    first_array second_array,
    "returns an array of the elements in the intersection of array1 and array2.",
    array_intersect_udf
);

make_udf_function!(
    ArrayDistinct,
    array_distinct,
    array,
    "returns distinct values from the array after removing duplicates.",
    array_distinct_udf
);

#[derive(Debug)]
pub(super) struct ArrayUnion {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayUnion {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![String::from("array_union"), String::from("list_union")],
        }
    }
}

impl ScalarUDFImpl for ArrayUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_union"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (&arg_types[0], &arg_types[1]) {
            (&Null, dt) => Ok(dt.clone()),
            (dt, Null) => Ok(dt.clone()),
            (dt, _) => Ok(dt.clone()),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_union_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayIntersect {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayIntersect {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec![
                String::from("array_intersect"),
                String::from("list_intersect"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayIntersect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_intersect"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (arg_types[0].clone(), arg_types[1].clone()) {
            (Null, Null) | (Null, _) => Ok(Null),
            (_, Null) => Ok(List(Arc::new(Field::new("item", Null, true)))),
            (dt, _) => Ok(dt),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_intersect_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

#[derive(Debug)]
pub(super) struct ArrayDistinct {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDistinct {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
            aliases: vec!["array_distinct".to_string(), "list_distinct".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayDistinct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_distinct"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            List(field) | FixedSizeList(field, _) => Ok(List(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            LargeList(field) => Ok(LargeList(Arc::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            )))),
            _ => exec_err!(
                "Not reachable, data_type should be List, LargeList or FixedSizeList"
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_distinct_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// array_distinct SQL function
/// example: from list [1, 3, 2, 3, 1, 2, 4] to [1, 2, 3, 4]
fn array_distinct_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("array_distinct needs one argument");
    }

    // handle null
    if args[0].data_type() == &Null {
        return Ok(args[0].clone());
    }

    // handle for list & largelist
    match args[0].data_type() {
        List(field) => {
            let array = as_list_array(&args[0])?;
            general_array_distinct(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(&args[0])?;
            general_array_distinct(array, field)
        }
        array_type => exec_err!("array_distinct does not support type '{array_type:?}'"),
    }
}

#[derive(Debug, PartialEq)]
enum SetOp {
    Union,
    Intersect,
}

impl Display for SetOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOp::Union => write!(f, "array_union"),
            SetOp::Intersect => write!(f, "array_intersect"),
        }
    }
}

fn generic_set_lists<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: Arc<Field>,
    set_op: SetOp,
) -> Result<ArrayRef> {
    if matches!(l.value_type(), Null) {
        let field = Arc::new(Field::new("item", r.value_type(), true));
        return general_array_distinct::<OffsetSize>(r, &field);
    } else if matches!(r.value_type(), Null) {
        let field = Arc::new(Field::new("item", l.value_type(), true));
        return general_array_distinct::<OffsetSize>(l, &field);
    }

    if l.value_type() != r.value_type() {
        return internal_err!("{set_op:?} is not implemented for '{l:?}' and '{r:?}'");
    }

    let dt = l.value_type();

    let mut offsets = vec![OffsetSize::usize_as(0)];
    let mut new_arrays = vec![];

    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    for (first_arr, second_arr) in l.iter().zip(r.iter()) {
        if let (Some(first_arr), Some(second_arr)) = (first_arr, second_arr) {
            let l_values = converter.convert_columns(&[first_arr])?;
            let r_values = converter.convert_columns(&[second_arr])?;

            let l_iter = l_values.iter().sorted().dedup();
            let values_set: HashSet<_> = l_iter.clone().collect();
            let mut rows = if set_op == SetOp::Union {
                l_iter.collect::<Vec<_>>()
            } else {
                vec![]
            };
            for r_val in r_values.iter().sorted().dedup() {
                match set_op {
                    SetOp::Union => {
                        if !values_set.contains(&r_val) {
                            rows.push(r_val);
                        }
                    }
                    SetOp::Intersect => {
                        if values_set.contains(&r_val) {
                            rows.push(r_val);
                        }
                    }
                }
            }

            let last_offset = match offsets.last().copied() {
                Some(offset) => offset,
                None => return internal_err!("offsets should not be empty"),
            };
            offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
            let arrays = converter.convert_rows(rows)?;
            let array = match arrays.first() {
                Some(array) => array.clone(),
                None => {
                    return internal_err!("{set_op}: failed to get array from rows");
                }
            };
            new_arrays.push(array);
        }
    }

    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    let arr = GenericListArray::<OffsetSize>::try_new(field, offsets, values, None)?;
    Ok(Arc::new(arr))
}

fn general_set_op(
    array1: &ArrayRef,
    array2: &ArrayRef,
    set_op: SetOp,
) -> Result<ArrayRef> {
    match (array1.data_type(), array2.data_type()) {
        (Null, List(field)) => {
            if set_op == SetOp::Intersect {
                return Ok(new_empty_array(&Null));
            }
            let array = as_list_array(&array2)?;
            general_array_distinct::<i32>(array, field)
        }

        (List(field), Null) => {
            if set_op == SetOp::Intersect {
                return make_array_inner(&[]);
            }
            let array = as_list_array(&array1)?;
            general_array_distinct::<i32>(array, field)
        }
        (Null, LargeList(field)) => {
            if set_op == SetOp::Intersect {
                return Ok(new_empty_array(&Null));
            }
            let array = as_large_list_array(&array2)?;
            general_array_distinct::<i64>(array, field)
        }
        (LargeList(field), Null) => {
            if set_op == SetOp::Intersect {
                return make_array_inner(&[]);
            }
            let array = as_large_list_array(&array1)?;
            general_array_distinct::<i64>(array, field)
        }
        (Null, Null) => Ok(new_empty_array(&Null)),

        (List(field), List(_)) => {
            let array1 = as_list_array(&array1)?;
            let array2 = as_list_array(&array2)?;
            generic_set_lists::<i32>(array1, array2, field.clone(), set_op)
        }
        (LargeList(field), LargeList(_)) => {
            let array1 = as_large_list_array(&array1)?;
            let array2 = as_large_list_array(&array2)?;
            generic_set_lists::<i64>(array1, array2, field.clone(), set_op)
        }
        (data_type1, data_type2) => {
            internal_err!(
                "{set_op} does not support types '{data_type1:?}' and '{data_type2:?}'"
            )
        }
    }
}

/// Array_union SQL function
fn array_union_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_union needs two arguments");
    }
    let array1 = &args[0];
    let array2 = &args[1];

    general_set_op(array1, array2, SetOp::Union)
}

/// array_intersect SQL function
fn array_intersect_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_intersect needs two arguments");
    }

    let array1 = &args[0];
    let array2 = &args[1];

    general_set_op(array1, array2, SetOp::Intersect)
}

fn general_array_distinct<OffsetSize: OffsetSizeTrait>(
    array: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<ArrayRef> {
    let dt = array.value_type();
    let mut offsets = Vec::with_capacity(array.len());
    offsets.push(OffsetSize::usize_as(0));
    let mut new_arrays = Vec::with_capacity(array.len());
    let converter = RowConverter::new(vec![SortField::new(dt)])?;
    // distinct for each list in ListArray
    for arr in array.iter().flatten() {
        let values = converter.convert_columns(&[arr])?;
        // sort elements in list and remove duplicates
        let rows = values.iter().sorted().dedup().collect::<Vec<_>>();
        let last_offset: OffsetSize = offsets.last().copied().unwrap();
        offsets.push(last_offset + OffsetSize::usize_as(rows.len()));
        let arrays = converter.convert_rows(rows)?;
        let array = match arrays.first() {
            Some(array) => array.clone(),
            None => {
                return internal_err!("array_distinct: failed to get array from rows")
            }
        };
        new_arrays.push(array);
    }
    let offsets = OffsetBuffer::new(offsets.into());
    let new_arrays_ref = new_arrays.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    let values = compute::concat(&new_arrays_ref)?;
    Ok(Arc::new(GenericListArray::<OffsetSize>::try_new(
        field.clone(),
        offsets,
        values,
        None,
    )?))
}
