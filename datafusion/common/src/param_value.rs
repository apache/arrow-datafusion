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

use crate::error::{_plan_datafusion_err, _plan_err};
use crate::{Result, ScalarValue};
use std::collections::HashMap;
use crate::logical_type::TypeRelation;

/// The parameter value corresponding to the placeholder
#[derive(Debug, Clone)]
pub enum ParamValues {
    /// For positional query parameters, like `SELECT * FROM test WHERE a > $1 AND b = $2`
    List(Vec<ScalarValue>),
    /// For named query parameters, like `SELECT * FROM test WHERE a > $foo AND b = $goo`
    Map(HashMap<String, ScalarValue>),
}

impl ParamValues {
    /// Verify parameter list length and type
    pub fn verify(&self, expect: &[TypeRelation]) -> Result<()> {
        match self {
            ParamValues::List(list) => {
                // Verify if the number of params matches the number of values
                if expect.len() != list.len() {
                    return _plan_err!(
                        "Expected {} parameters, got {}",
                        expect.len(),
                        list.len()
                    );
                }

                // Verify if the types of the params matches the types of the values
                let iter = expect.iter().zip(list.iter());
                for (i, (param_type, value)) in iter.enumerate() {
                    if *param_type != value.data_type().into() {
                        return _plan_err!(
                            "Expected parameter of type {:?}, got {:?} at index {}",
                            param_type,
                            value.data_type(),
                            i
                        );
                    }
                }
                Ok(())
            }
            ParamValues::Map(_) => {
                // If it is a named query, variables can be reused,
                // but the lengths are not necessarily equal
                Ok(())
            }
        }
    }

    pub fn get_placeholders_with_values(&self, id: &str) -> Result<ScalarValue> {
        match self {
            ParamValues::List(list) => {
                if id.is_empty() {
                    return _plan_err!("Empty placeholder id");
                }
                // convert id (in format $1, $2, ..) to idx (0, 1, ..)
                let idx = id[1..]
                    .parse::<usize>()
                    .map_err(|e| {
                        _plan_datafusion_err!("Failed to parse placeholder id: {e}")
                    })?
                    .checked_sub(1);
                // value at the idx-th position in param_values should be the value for the placeholder
                let value = idx.and_then(|idx| list.get(idx)).ok_or_else(|| {
                    _plan_datafusion_err!("No value found for placeholder with id {id}")
                })?;
                Ok(value.clone())
            }
            ParamValues::Map(map) => {
                // convert name (in format $a, $b, ..) to mapped values (a, b, ..)
                let name = &id[1..];
                // value at the name position in param_values should be the value for the placeholder
                let value = map.get(name).ok_or_else(|| {
                    _plan_datafusion_err!("No value found for placeholder with name {id}")
                })?;
                Ok(value.clone())
            }
        }
    }
}

impl From<Vec<ScalarValue>> for ParamValues {
    fn from(value: Vec<ScalarValue>) -> Self {
        Self::List(value)
    }
}

impl<K> From<Vec<(K, ScalarValue)>> for ParamValues
where
    K: Into<String>,
{
    fn from(value: Vec<(K, ScalarValue)>) -> Self {
        let value: HashMap<String, ScalarValue> =
            value.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Self::Map(value)
    }
}

impl<K> From<HashMap<K, ScalarValue>> for ParamValues
where
    K: Into<String>,
{
    fn from(value: HashMap<K, ScalarValue>) -> Self {
        let value: HashMap<String, ScalarValue> =
            value.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Self::Map(value)
    }
}
