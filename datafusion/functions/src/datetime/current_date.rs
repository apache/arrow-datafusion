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

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Date32;
use chrono::{Datelike, NaiveDate};

use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct CurrentDateFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for CurrentDateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentDateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(0, Volatility::Stable),
            aliases: vec![String::from("today")],
        }
    }
}

/// Create an implementation of `current_date()` that always returns the
/// specified current date.
///
/// The semantics of `current_date()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
impl ScalarUDFImpl for CurrentDateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "current_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Date32)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        internal_err!(
            "invoke should not be called on a simplified current_date() function"
        )
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn simplify(
        &self,
        _args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let now_ts = info.execution_props().query_execution_start_time;
        let days = Some(
            now_ts.num_days_from_ce()
                - NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .num_days_from_ce(),
        );
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::Date32(days),
        )))
    }
}
