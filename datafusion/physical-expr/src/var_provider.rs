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

//! Variable provider

use datafusion_common::{logical_type::LogicalType, Result, ScalarValue};

/// Variable type, system/user defined
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VarType {
    /// System variable, like @@version
    System,
    /// User defined variable, like @name
    UserDefined,
}

/// A var provider for `@variable` and `@@variable` runtime values.
pub trait VarProvider: std::fmt::Debug {
    /// Get variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue>;

    /// Return the type of the given variable
    fn get_type(&self, var_names: &[String]) -> Option<LogicalType>;
}

pub fn is_system_variables(variable_names: &[String]) -> bool {
    !variable_names.is_empty() && variable_names[0].get(0..2) == Some("@@")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_is_system_variables() {
        assert!(!is_system_variables(&["N".into(), "\"\"".into()]));
        assert!(is_system_variables(&["@@D".into(), "F".into(), "J".into()]));
        assert!(!is_system_variables(&[
            "J".into(),
            "@@F".into(),
            "K".into()
        ]));
        assert!(!is_system_variables(&[]));
        assert!(is_system_variables(&[
            "@@longvariablenamethatIdontknowhwyanyonewoulduse".into()
        ]));
        assert!(!is_system_variables(&["@F".into()]));
    }
}
