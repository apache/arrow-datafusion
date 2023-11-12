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

//! System variable provider

use crate::error::Result;
use crate::scalar::ScalarValue;
use crate::variable::VarProvider;
use arrow::datatypes::DataType;
use datafusion_common::logical_type::LogicalType;

/// System variable
#[derive(Default, Debug)]
pub struct SystemVar {}

impl SystemVar {
    /// new system variable
    pub fn new() -> Self {
        Self {}
    }
}

impl VarProvider for SystemVar {
    /// get system variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        let s = format!("{}-{}", "system-var", var_names.concat());
        Ok(ScalarValue::Utf8(Some(s)))
    }

    fn get_type(&self, _: &[String]) -> Option<DataType> {
        Some(LogicalType::Utf8)
    }
}

/// user defined variable
#[derive(Default, Debug)]
pub struct UserDefinedVar {}

impl UserDefinedVar {
    /// new user defined variable
    pub fn new() -> Self {
        Self {}
    }
}

impl VarProvider for UserDefinedVar {
    /// Get user defined variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        if var_names[0] != "@integer" {
            let s = format!("{}-{}", "user-defined-var", var_names.concat());
            Ok(ScalarValue::Utf8(Some(s)))
        } else {
            Ok(ScalarValue::Int32(Some(41)))
        }
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        if var_names[0] != "@integer" {
            Some(LogicalType::Utf8)
        } else {
            Some(LogicalType::Int32)
        }
    }
}
