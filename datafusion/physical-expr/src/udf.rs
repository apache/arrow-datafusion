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

//! UDF support
use crate::utils::extract_constant_args;
use crate::{PhysicalExpr, ScalarFunctionExpr};
use arrow::datatypes::Schema;
use datafusion_common::Result;
pub use datafusion_expr::ScalarUDF;
use std::sync::Arc;

/// Create a physical expression of the UDF.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDF.
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_exprs_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let constant_args = extract_constant_args(input_phy_exprs);

    Ok(Arc::new(ScalarFunctionExpr::new(
        &fun.name,
        fun.fun.clone(),
        input_phy_exprs.to_vec(),
        fun.return_type
            .infer(&input_exprs_types, &constant_args)?
            .as_ref(),
        None,
    )))
}
