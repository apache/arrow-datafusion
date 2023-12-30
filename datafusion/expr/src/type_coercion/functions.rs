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

use crate::signature::TIMEZONE_WILDCARD;
use crate::{Signature, TypeSignature};
use arrow::{
    compute::can_cast_types,
    datatypes::{DataType, TimeUnit},
};
use datafusion_common::utils::list_ndims;
use datafusion_common::{internal_err, plan_err, DataFusionError, Result};

use super::binary::{comparison_coercion, comparison_coercion_for_iter};

/// Performs type coercion for function arguments.
///
/// Returns the data types to which each argument must be coerced to
/// match `signature`.
///
/// For more details on coercion in general, please see the
/// [`type_coercion`](crate::type_coercion) module.
pub fn data_types(
    current_types: &[DataType],
    signature: &Signature,
) -> Result<Vec<DataType>> {
    if current_types.is_empty() {
        if signature.type_signature.supports_zero_argument() {
            return Ok(vec![]);
        } else {
            return plan_err!(
                "Coercion from {:?} to the signature {:?} failed.",
                current_types,
                &signature.type_signature
            );
        }
    }

    let valid_types = get_valid_types(&signature.type_signature, current_types)?;

    if valid_types
        .iter()
        .any(|data_type| data_type == current_types)
    {
        return Ok(current_types.to_vec());
    }

    // Try and coerce the argument types to match the signature, returning the
    // coerced types from the first matching signature.
    for valid_types in valid_types {
        if let Some(types) = maybe_data_types(&valid_types, current_types) {
            return Ok(types);
        }
    }

    // none possible -> Error
    plan_err!(
        "Coercion from {:?} to the signature {:?} failed.",
        current_types,
        &signature.type_signature
    )
}

/// Returns a Vec of all possible valid argument types for the given signature.
fn get_valid_types(
    signature: &TypeSignature,
    current_types: &[DataType],
) -> Result<Vec<Vec<DataType>>> {
    let valid_types = match signature {
        TypeSignature::Variadic(valid_types) => valid_types
            .iter()
            .map(|valid_type| current_types.iter().map(|_| valid_type.clone()).collect())
            .collect(),
        TypeSignature::Uniform(number, valid_types) => valid_types
            .iter()
            .map(|valid_type| (0..*number).map(|_| valid_type.clone()).collect())
            .collect(),
        TypeSignature::VariadicEqual => {
            let new_type = comparison_coercion_for_iter(current_types);
            match new_type {
                Ok(new_type) => vec![vec![new_type; current_types.len()]],
                Err(e) => return Err(e),
            }
        }
        TypeSignature::VariadicAny => {
            vec![current_types.to_vec()]
        }

        TypeSignature::Exact(valid_types) => vec![valid_types.clone()],
        TypeSignature::ArrayAndElement => {
            if current_types.len() != 2 {
                return Ok(vec![vec![]]);
            }

            let array_type = &current_types[0];
            let elem_type = &current_types[1];

            // We follow Postgres on `array_append(Null, T)`, which is not valid.
            if array_type.eq(&DataType::Null) {
                return Ok(vec![vec![]]);
            }

            // We need to find the coerced base type, mainly for cases like:
            // `array_append(List(null), i64)` -> `List(i64)`
            let array_base_type = datafusion_common::utils::base_type(array_type);
            let elem_base_type = datafusion_common::utils::base_type(elem_type);
            let new_base_type = comparison_coercion(&array_base_type, &elem_base_type);

            if new_base_type.is_none() {
                return internal_err!(
                    "Coercion from {array_base_type:?} to {elem_base_type:?} not supported."
                );
            }
            let new_base_type = new_base_type.unwrap();

            let array_type = datafusion_common::utils::coerced_type_with_base_type_only(
                array_type,
                &new_base_type,
            );

            if let DataType::List(ref field) = array_type {
                let elem_type = field.data_type();
                return Ok(vec![vec![array_type.clone(), elem_type.to_owned()]]);
            } else {
                return Ok(vec![vec![]]);
            }
        }
        TypeSignature::ArrayConcat => {
            let base_types = current_types
                .iter()
                .map(datafusion_common::utils::base_type)
                .collect::<Vec<_>>();

            let new_base_type = comparison_coercion_for_iter(base_types.as_slice());
            match new_base_type {
                Ok(new_base_type) => {
                    let array_types = current_types
                        .iter()
                        .map(|t| {
                            if t.eq(&DataType::Null) {
                                t.to_owned()
                            } else {
                                datafusion_common::utils::coerced_type_with_base_type_only(
                                    t,
                                    &new_base_type,
                                )
                            }
                        })
                        .collect::<Vec<_>>();
                    return Ok(vec![array_types]);
                }
                Err(e) => return Err(e),
            }
        }
        TypeSignature::Any(number) => {
            if current_types.len() != *number {
                return plan_err!(
                    "The function expected {} arguments but received {}",
                    number,
                    current_types.len()
                );
            }
            vec![(0..*number).map(|i| current_types[i].clone()).collect()]
        }
        TypeSignature::OneOf(types) => types
            .iter()
            .filter_map(|t| get_valid_types(t, current_types).ok())
            .flatten()
            .collect::<Vec<_>>(),
    };

    Ok(valid_types)
}

/// Try to coerce the current argument types to match the given `valid_types`.
///
/// For example, if a function `func` accepts arguments of  `(int64, int64)`,
/// but was called with `(int32, int64)`, this function could match the
/// valid_types by coercing the first argument to `int64`, and would return
/// `Some([int64, int64])`.
fn maybe_data_types(
    valid_types: &[DataType],
    current_types: &[DataType],
) -> Option<Vec<DataType>> {
    if valid_types.len() != current_types.len() {
        return None;
    }

    let mut new_type = Vec::with_capacity(valid_types.len());
    for (i, valid_type) in valid_types.iter().enumerate() {
        let current_type = &current_types[i];

        if current_type == valid_type {
            new_type.push(current_type.clone())
        } else {
            // attempt to coerce
            if let Some(valid_type) = coerced_from(valid_type, current_type) {
                new_type.push(valid_type)
            } else {
                // not possible
                return None;
            }
        }
    }
    Some(new_type)
}

/// Return true if a value of type `type_from` can be coerced
/// (losslessly converted) into a value of `type_to`
///
/// See the module level documentation for more detail on coercion.
pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
    if type_into == type_from {
        return true;
    }
    if let Some(coerced) = coerced_from(type_into, type_from) {
        return coerced == *type_into;
    }
    false
}

fn coerced_from<'a>(
    type_into: &'a DataType,
    type_from: &'a DataType,
) -> Option<DataType> {
    use self::DataType::*;

    match type_into {
        // coerced into type_into
        Int8 if matches!(type_from, Null | Int8) => Some(type_into.clone()),
        Int16 if matches!(type_from, Null | Int8 | Int16 | UInt8) => {
            Some(type_into.clone())
        }
        Int32 if matches!(type_from, Null | Int8 | Int16 | Int32 | UInt8 | UInt16) => {
            Some(type_into.clone())
        }
        Int64
            if matches!(
                type_from,
                Null | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32
            ) =>
        {
            Some(type_into.clone())
        }
        UInt8 if matches!(type_from, Null | UInt8) => Some(type_into.clone()),
        UInt16 if matches!(type_from, Null | UInt8 | UInt16) => Some(type_into.clone()),
        UInt32 if matches!(type_from, Null | UInt8 | UInt16 | UInt32) => {
            Some(type_into.clone())
        }
        UInt64 if matches!(type_from, Null | UInt8 | UInt16 | UInt32 | UInt64) => {
            Some(type_into.clone())
        }
        Float32
            if matches!(
                type_from,
                Null | Int8
                    | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float32
            ) =>
        {
            Some(type_into.clone())
        }
        Float64
            if matches!(
                type_from,
                Null | Int8
                    | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float32
                    | Float64
                    | Decimal128(_, _)
            ) =>
        {
            Some(type_into.clone())
        }
        Timestamp(TimeUnit::Nanosecond, None)
            if matches!(
                type_from,
                Null | Timestamp(_, None) | Date32 | Utf8 | LargeUtf8
            ) =>
        {
            Some(type_into.clone())
        }
        Interval(_) if matches!(type_from, Utf8 | LargeUtf8) => Some(type_into.clone()),
        // Any type can be coerced into strings
        Utf8 | LargeUtf8 => Some(type_into.clone()),
        Null if can_cast_types(type_from, type_into) => Some(type_into.clone()),

        // Only accept list with the same number of dimensions unless the type is Null.
        // List with different dimensions should be handled in TypeSignature or other places before this.
        List(_)
            if datafusion_common::utils::base_type(type_from).eq(&Null)
                || list_ndims(type_from) == list_ndims(type_into) =>
        {
            Some(type_into.clone())
        }

        Timestamp(unit, Some(tz)) if tz.as_ref() == TIMEZONE_WILDCARD => {
            match type_from {
                Timestamp(_, Some(from_tz)) => {
                    Some(Timestamp(unit.clone(), Some(from_tz.clone())))
                }
                Null | Date32 | Utf8 | LargeUtf8 | Timestamp(_, None) => {
                    // In the absence of any other information assume the time zone is "+00" (UTC).
                    Some(Timestamp(unit.clone(), Some("+00".into())))
                }
                _ => None,
            }
        }
        Timestamp(_, Some(_))
            if matches!(
                type_from,
                Null | Timestamp(_, _) | Date32 | Utf8 | LargeUtf8
            ) =>
        {
            Some(type_into.clone())
        }

        // cannot coerce
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_maybe_data_types() {
        // this vec contains: arg1, arg2, expected result
        let cases = vec![
            // 2 entries, same values
            (
                vec![DataType::UInt8, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                Some(vec![DataType::UInt8, DataType::UInt16]),
            ),
            // 2 entries, can coerse values
            (
                vec![DataType::UInt16, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                Some(vec![DataType::UInt16, DataType::UInt16]),
            ),
            // 0 entries, all good
            (vec![], vec![], Some(vec![])),
            // 2 entries, can't coerce
            (
                vec![DataType::Boolean, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                None,
            ),
            // u32 -> u16 is possible
            (
                vec![DataType::Boolean, DataType::UInt32],
                vec![DataType::Boolean, DataType::UInt16],
                Some(vec![DataType::Boolean, DataType::UInt32]),
            ),
            // UTF8 -> Timestamp
            (
                vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+TZ".into())),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+01".into())),
                ],
                vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
                Some(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00".into())),
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+01".into())),
                ]),
            ),
        ];

        for case in cases {
            assert_eq!(maybe_data_types(&case.0, &case.1), case.2)
        }
    }

    #[test]
    fn test_get_valid_types_one_of() -> Result<()> {
        let signature =
            TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]);

        let invalid_types = get_valid_types(
            &signature,
            &[DataType::Int32, DataType::Int32, DataType::Int32],
        )?;
        assert_eq!(invalid_types.len(), 0);

        let args = vec![DataType::Int32, DataType::Int32];
        let valid_types = get_valid_types(&signature, &args)?;
        assert_eq!(valid_types.len(), 1);
        assert_eq!(valid_types[0], args);

        let args = vec![DataType::Int32];
        let valid_types = get_valid_types(&signature, &args)?;
        assert_eq!(valid_types.len(), 1);
        assert_eq!(valid_types[0], args);

        Ok(())
    }
}
