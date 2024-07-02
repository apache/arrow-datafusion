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

use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::planner::{PlannerResult, RawDictionaryExpr, UserDefinedSQLPlanner};

use super::named_struct;

#[derive(Default)]
pub struct CoreFunctionPlanner {}

impl UserDefinedSQLPlanner for CoreFunctionPlanner {
    fn plan_dictionary_literal(
        &self,
        expr: RawDictionaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawDictionaryExpr>> {
        let mut args = vec![];
        for (k, v) in expr.keys.into_iter().zip(expr.values.into_iter()) {
            args.push(k);
            args.push(v);
        }
        Ok(PlannerResult::Planned(named_struct().call(args)))
    }
}
