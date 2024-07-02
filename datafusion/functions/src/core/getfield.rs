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

use arrow::array::{
    make_array, Array, Capacities, MutableArrayData, Scalar, StringArray,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_map_array, as_struct_array};
use datafusion_common::{
    exec_err, plan_datafusion_err, plan_err, ExprSchema, Result, ScalarValue,
};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct GetFieldFunc {
    signature: Signature,
}

impl Default for GetFieldFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GetFieldFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

// get_field(struct_array, field_name)
impl ScalarUDFImpl for GetFieldFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "get_field"
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        if args.len() != 2 {
            return exec_err!(
                "get_field function requires 2 arguments, got {}",
                args.len()
            );
        }

        let name = match &args[1] {
            Expr::Literal(name) => name,
            _ => {
                return exec_err!(
                    "get_field function requires the argument field_name to be a string"
                );
            }
        };

        Ok(format!("{}[{}]", args[0].display_name()?, name))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        todo!()
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        if args.len() != 2 {
            return exec_err!(
                "get_field function requires 2 arguments, got {}",
                args.len()
            );
        }

        let name = match &args[1] {
            Expr::Literal(name) => name,
            _ => {
                return exec_err!(
                    "get_field function requires the argument field_name to be a string"
                );
            }
        };
        let data_type = args[0].get_type(schema)?;
        match (data_type, name) {
            (DataType::Map(fields, _), _) => {
                match fields.data_type() {
                    DataType::Struct(fields) if fields.len() == 2 => {
                        // Arrow's MapArray is essentially a ListArray of structs with two columns. They are
                        // often named "key", and "value", but we don't require any specific naming here;
                        // instead, we assume that the second columnis the "value" column both here and in
                        // execution.
                        let value_field = fields.get(1).expect("fields should have exactly two members");
                        Ok(value_field.data_type().clone())
                    },
                    _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
                }
            }
            (DataType::Struct(fields), ScalarValue::Utf8(Some(s))) => {
                if s.is_empty() {
                    plan_err!(
                        "Struct based indexed access requires a non empty string"
                    )
                } else {
                    let field = fields.iter().find(|f| f.name() == s);
                    field.ok_or(plan_datafusion_err!("Field {s} not found in struct")).map(|f| f.data_type().clone())
                }
            }
            (DataType::Struct(_), _) => plan_err!(
                "Only UTF8 strings are valid as an indexed field in a struct"
            ),
            (DataType::Null, _) => Ok(DataType::Null),
            (other, _) => plan_err!("The expression to get an indexed field is only valid for `List`, `Struct`, `Map` or `Null` types, got {other}"),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "get_field function requires 2 arguments, got {}",
                args.len()
            );
        }

        if args[0].data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }

        let arrays = ColumnarValue::values_to_arrays(args)?;
        let array = Arc::clone(&arrays[0]);

        let name = match &args[1] {
            ColumnarValue::Scalar(name) => name,
            _ => {
                return exec_err!(
                    "get_field function requires the argument field_name to be a string"
                );
            }
        };

        match (array.data_type(), name) {
            (DataType::Map(_, _), ScalarValue::Utf8(Some(k))) => {
                let map_array = as_map_array(array.as_ref())?;
                let key_scalar: Scalar<arrow::array::GenericByteArray<arrow::datatypes::GenericStringType<i32>>> = Scalar::new(StringArray::from(vec![k.clone()]));
                let keys = arrow::compute::kernels::cmp::eq(&key_scalar, map_array.keys())?;

                // note that this array has more entries than the expected output/input size
                // because maparray is flatten
                let original_data =  map_array.entries().column(1).to_data();
                let capacity = Capacities::Array(original_data.len());
                let mut mutable =
                    MutableArrayData::with_capacities(vec![&original_data], true,
                         capacity);

                for entry in 0..map_array.len(){
                    let start = map_array.value_offsets()[entry] as usize;
                    let end = map_array.value_offsets()[entry + 1] as usize;

                    let maybe_matched =
                                        keys.slice(start, end-start).
                                        iter().enumerate().
                                        find(|(_, t)| t.unwrap());
                    if maybe_matched.is_none(){
                        mutable.extend_nulls(1);
                        continue
                    }
                    let (match_offset,_) = maybe_matched.unwrap();
                    mutable.extend(0, start + match_offset, start + match_offset + 1);
                }
                let data = mutable.freeze();
                let data = make_array(data);
                Ok(ColumnarValue::Array(data))
            }
            (DataType::Struct(_), ScalarValue::Utf8(Some(k))) => {
                let as_struct_array = as_struct_array(&array)?;
                match as_struct_array.column_by_name(k) {
                    None => exec_err!("get indexed field {k} not found in struct"),
                    Some(col) => Ok(ColumnarValue::Array(Arc::clone(col))),
                }
            }
            (DataType::Struct(_), name) => exec_err!(
                "get indexed field is only possible on struct with utf8 indexes. \
                             Tried with {name:?} index"
            ),
            (DataType::Null, _) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
            (dt, name) => exec_err!(
                "get indexed field is only possible on lists with int64 indexes or struct \
                                         with utf8 indexes. Tried {dt:?} with {name:?} index"
            ),
        }
    }
}
