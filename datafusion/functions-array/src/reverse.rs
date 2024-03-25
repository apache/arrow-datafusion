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

//! [`ScalarUDFImpl`] definitions for array_reverse function.

use crate::utils::make_scalar_function;
use arrow::array::{Capacities, MutableArrayData};
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType::{LargeList, List, Null};
use arrow_schema::{DataType, FieldRef};
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{exec_err, Result};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

make_udf_function!(
    ArrayReverse,
    array_reverse,
    array,
    "reverses the order of elements in the array.",
    array_reverse_udf
);

#[derive(Debug)]
pub(super) struct ArrayReverse {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayReverse {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            aliases: vec!["array_reverse".to_string(), "list_reverse".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayReverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(array_reverse_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// array_reverse SQL function
pub fn array_reverse_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    if arg.len() != 1 {
        return exec_err!("array_reverse needs one argument");
    }

    match &arg[0].data_type() {
        List(field) => {
            let array = as_list_array(&arg[0])?;
            general_array_reverse::<i32>(array, field)
        }
        LargeList(field) => {
            let array = as_large_list_array(&arg[0])?;
            general_array_reverse::<i64>(array, field)
        }
        Null => Ok(arg[0].clone()),
        array_type => exec_err!("array_reverse does not support type '{array_type:?}'."),
    }
}

fn general_array_reverse<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    field: &FieldRef,
) -> Result<ArrayRef>
where
    O: TryFrom<i64>,
{
    let values = array.values();
    let original_data = values.to_data();
    let capacity = Capacities::Array(original_data.len());
    let mut offsets = vec![O::usize_as(0)];
    let mut nulls = vec![];
    let mut mutable =
        MutableArrayData::with_capacities(vec![&original_data], false, capacity);

    for (row_index, offset_window) in array.offsets().windows(2).enumerate() {
        // skip the null value
        if array.is_null(row_index) {
            nulls.push(false);
            offsets.push(offsets[row_index] + O::one());
            mutable.extend(0, 0, 1);
            continue;
        } else {
            nulls.push(true);
        }

        let start = offset_window[0];
        let end = offset_window[1];

        let mut index = end - O::one();
        let mut cnt = 0;

        while index >= start {
            mutable.extend(0, index.to_usize().unwrap(), index.to_usize().unwrap() + 1);
            index = index - O::one();
            cnt += 1;
        }
        offsets.push(offsets[row_index] + O::usize_as(cnt));
    }

    let data = mutable.freeze();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field.clone(),
        OffsetBuffer::<O>::new(offsets.into()),
        arrow_array::make_array(data),
        Some(nulls.into()),
    )?))
}
