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

use arrow::compute::kernels::length::bit_length;
use std::any::Any;

use arrow::datatypes::DataType;

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Volatility};
use datafusion_expr::{ScalarUDFImpl, Signature};

use crate::string::common::*;

#[derive(Debug)]
pub(super) struct BitLengthFunc {
    signature: Signature,
}

impl BitLengthFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitLengthFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "bit_length")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "bit_length function requires 1 argument, got {}",
                args.len()
            );
        }

        match &args[0] {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(bit_length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| (x.len() * 8) as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| (x.len() * 8) as i64)),
                )),
                _ => unreachable!(),
            },
        }
    }
}
