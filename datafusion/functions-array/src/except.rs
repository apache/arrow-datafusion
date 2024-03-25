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

//! [`ScalarUDFImpl`] definitions for array_except function.

use crate::utils::{check_datatypes, make_scalar_function};
use arrow::row::{RowConverter, SortField};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

make_udf_function!(
    ArrayExcept,
    array_except,
    first_array second_array,
    "returns an array of the elements that appear in the first array but not in the second.",
    array_except_udf
);

#[derive(Debug)]
pub(super) struct ArrayExcept {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayExcept {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            aliases: vec!["array_except".to_string(), "list_except".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayExcept {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_except"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match (&arg_types[0].clone(), &arg_types[1].clone()) {
            (DataType::Null, _) | (_, DataType::Null) => Ok(arg_types[0].clone()),
            (dt, _) => Ok(dt.clone()),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_except_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Array_except SQL function
pub fn array_except_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_except needs two arguments");
    }

    let array1 = &args[0];
    let array2 = &args[1];

    match (array1.data_type(), array2.data_type()) {
        (DataType::Null, _) | (_, DataType::Null) => Ok(array1.to_owned()),
        (DataType::List(field), DataType::List(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i32>();
            let list2 = array2.as_list::<i32>();
            let result = general_except::<i32>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (DataType::LargeList(field), DataType::LargeList(_)) => {
            check_datatypes("array_except", &[array1, array2])?;
            let list1 = array1.as_list::<i64>();
            let list2 = array2.as_list::<i64>();
            let result = general_except::<i64>(list1, list2, field)?;
            Ok(Arc::new(result))
        }
        (dt1, dt2) => {
            internal_err!("array_except got unexpected types: {dt1:?} and {dt2:?}")
        }
    }
}

fn general_except<OffsetSize: OffsetSizeTrait>(
    l: &GenericListArray<OffsetSize>,
    r: &GenericListArray<OffsetSize>,
    field: &FieldRef,
) -> Result<GenericListArray<OffsetSize>> {
    let converter = RowConverter::new(vec![SortField::new(l.value_type())])?;

    let l_values = l.values().to_owned();
    let r_values = r.values().to_owned();
    let l_values = converter.convert_columns(&[l_values])?;
    let r_values = converter.convert_columns(&[r_values])?;

    let mut offsets = Vec::<OffsetSize>::with_capacity(l.len() + 1);
    offsets.push(OffsetSize::usize_as(0));

    let mut rows = Vec::with_capacity(l_values.num_rows());
    let mut dedup = HashSet::new();

    for (l_w, r_w) in l.offsets().windows(2).zip(r.offsets().windows(2)) {
        let l_slice = l_w[0].as_usize()..l_w[1].as_usize();
        let r_slice = r_w[0].as_usize()..r_w[1].as_usize();
        for i in r_slice {
            let right_row = r_values.row(i);
            dedup.insert(right_row);
        }
        for i in l_slice {
            let left_row = l_values.row(i);
            if dedup.insert(left_row) {
                rows.push(left_row);
            }
        }

        offsets.push(OffsetSize::usize_as(rows.len()));
        dedup.clear();
    }

    if let Some(values) = converter.convert_rows(rows)?.first() {
        Ok(GenericListArray::<OffsetSize>::new(
            field.to_owned(),
            OffsetBuffer::new(offsets.into()),
            values.to_owned(),
            l.nulls().cloned(),
        ))
    } else {
        internal_err!("array_except failed to convert rows")
    }
}
