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

use arrow::datatypes::DataType;

#[cfg(test)]
pub fn create_decimal_array(
    array: &[Option<i128>],
    precision: usize,
    scale: usize,
) -> datafusion_common::Result<arrow::array::Int128Array> {
    use arrow::array::{Int128Vec, TryPush};
    let mut decimal_builder = Int128Vec::from_data(
        DataType::Decimal(precision, scale),
        Vec::<i128>::with_capacity(array.len()),
        None,
    );

    for value in array {
        match value {
            None => {
                decimal_builder.push(None);
            }
            Some(v) => {
                decimal_builder.try_push(Some(*v))?;
            }
        }
    }
    Ok(decimal_builder.into())
}

#[cfg(test)]
pub fn create_decimal_array_from_slice(
    array: &[i128],
    precision: usize,
    scale: usize,
) -> datafusion_common::Result<arrow::array::Int128Array> {
    let decimal_array_values: Vec<Option<i128>> =
        array.into_iter().map(|v| Some(*v)).collect();
    create_decimal_array(&decimal_array_values, precision, scale)
}
