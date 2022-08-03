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

//! Functionality used both on logical and physical plans

use crate::error::{DataFusionError, Result};
use ahash::{CallHasher, RandomState};
use arrow::array::{
    Array, ArrayRef, BasicDecimalArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, DictionaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use std::sync::Arc;

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_null(random_state: &RandomState, hashes_buffer: &'_ mut [u64], mul_col: bool) {
    if mul_col {
        hashes_buffer.iter_mut().for_each(|hash| {
            // stable hash for null value
            *hash = combine_hashes(i128::get_hash(&1, random_state), *hash);
        })
    } else {
        hashes_buffer.iter_mut().for_each(|hash| {
            *hash = i128::get_hash(&1, random_state);
        })
    }
}

fn hash_decimal128<'a>(
    array: &ArrayRef,
    random_state: &RandomState,
    hashes_buffer: &'a mut [u64],
    mul_col: bool,
) {
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    if array.null_count() == 0 {
        if mul_col {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = combine_hashes(
                    i128::get_hash(&array.value(i).as_i128(), random_state),
                    *hash,
                );
            }
        } else {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = i128::get_hash(&array.value(i).as_i128(), random_state);
            }
        }
    } else if mul_col {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = combine_hashes(
                    i128::get_hash(&array.value(i).as_i128(), random_state),
                    *hash,
                );
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = i128::get_hash(&array.value(i).as_i128(), random_state);
            }
        }
    }
}

macro_rules! hash_array {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = combine_hashes(
                        $ty::get_hash(&array.value(i), $random_state),
                        *hash,
                    );
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $ty::get_hash(&array.value(i), $random_state);
                }
            }
        } else {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes(
                            $ty::get_hash(&array.value(i), $random_state),
                            *hash,
                        );
                    }
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(&array.value(i), $random_state);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_primitive {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes($ty::get_hash(value, $random_state), *hash);
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $ty::get_hash(value, $random_state)
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash =
                            combine_hashes($ty::get_hash(value, $random_state), *hash);
                    }
                }
            } else {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(value, $random_state);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_float {
    ($array_type:ident, $column: ident, $ty: ident, $hashes: ident, $random_state: ident, $multi_col: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes(
                        $ty::get_hash(
                            &$ty::from_le_bytes(value.to_le_bytes()),
                            $random_state,
                        ),
                        *hash,
                    );
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $ty::get_hash(
                        &$ty::from_le_bytes(value.to_le_bytes()),
                        $random_state,
                    )
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash = combine_hashes(
                            $ty::get_hash(
                                &$ty::from_le_bytes(value.to_le_bytes()),
                                $random_state,
                            ),
                            *hash,
                        );
                    }
                }
            } else {
                for (i, (hash, value)) in
                    $hashes.iter_mut().zip(values.iter()).enumerate()
                {
                    if !array.is_null(i) {
                        *hash = $ty::get_hash(
                            &$ty::from_le_bytes(value.to_le_bytes()),
                            $random_state,
                        );
                    }
                }
            }
        }
    };
}

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    multi_col: bool,
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();

    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let dict_values = Arc::clone(dict_array.values());
    let mut dict_hashes = vec![0; dict_values.len()];
    create_hashes(&[dict_values], random_state, &mut dict_hashes)?;

    // combine hash for each index in values
    if multi_col {
        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key
                    .to_usize()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Can not convert key value {:?} to usize in dictionary of type {:?}",
                            key, dict_array.data_type()
                        ))
                    })?;
                *hash = combine_hashes(dict_hashes[idx], *hash)
            } // no update for Null, consistent with other hashes
        }
    } else {
        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key
                    .to_usize()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Can not convert key value {:?} to usize in dictionary of type {:?}",
                            key, dict_array.data_type()
                        ))
                    })?;
                *hash = dict_hashes[idx]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

/// Test version of `create_hashes` that produces the same value for
/// all hashes (to test collisions)
///
/// See comments on `hashes_buffer` for more details
#[cfg(feature = "force_hash_collisions")]
pub fn create_hashes<'a>(
    _arrays: &[ArrayRef],
    _random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    Ok(hashes_buffer)
}

/// Test version of `create_row_hashes` that produces the same value for
/// all hashes (to test collisions)
///
/// See comments on `hashes_buffer` for more details
#[cfg(feature = "force_hash_collisions")]
pub fn create_row_hashes<'a>(
    _rows: &[Vec<u8>],
    _random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    Ok(hashes_buffer)
}

/// Creates hash values for every row, based on their raw bytes.
#[cfg(not(feature = "force_hash_collisions"))]
pub fn create_row_hashes<'a>(
    rows: &[Vec<u8>],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    for (i, hash) in hashes_buffer.iter_mut().enumerate() {
        *hash = <Vec<u8>>::get_hash(&rows[i], random_state);
    }
    Ok(hashes_buffer)
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[cfg(not(feature = "force_hash_collisions"))]
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    // combine hashes with `combine_hashes` if we have more than 1 column
    let multi_col = arrays.len() > 1;

    for col in arrays {
        match col.data_type() {
            DataType::Null => {
                hash_null(random_state, hashes_buffer, multi_col);
            }
            DataType::Decimal128(_, _) => {
                hash_decimal128(col, random_state, hashes_buffer, multi_col);
            }
            DataType::UInt8 => {
                hash_array_primitive!(
                    UInt8Array,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt16 => {
                hash_array_primitive!(
                    UInt16Array,
                    col,
                    u16,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt32 => {
                hash_array_primitive!(
                    UInt32Array,
                    col,
                    u32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::UInt64 => {
                hash_array_primitive!(
                    UInt64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int8 => {
                hash_array_primitive!(
                    Int8Array,
                    col,
                    i8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int16 => {
                hash_array_primitive!(
                    Int16Array,
                    col,
                    i16,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int32 => {
                hash_array_primitive!(
                    Int32Array,
                    col,
                    i32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Int64 => {
                hash_array_primitive!(
                    Int64Array,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Float32 => {
                hash_array_float!(
                    Float32Array,
                    col,
                    u32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Float64 => {
                hash_array_float!(
                    Float64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Second, None) => {
                hash_array_primitive!(
                    TimestampSecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                hash_array_primitive!(
                    TimestampMillisecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                hash_array_primitive!(
                    TimestampMicrosecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                hash_array_primitive!(
                    TimestampNanosecondArray,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Date32 => {
                hash_array_primitive!(
                    Date32Array,
                    col,
                    i32,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Date64 => {
                hash_array_primitive!(
                    Date64Array,
                    col,
                    i64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Boolean => {
                hash_array!(
                    BooleanArray,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Utf8 => {
                hash_array!(
                    StringArray,
                    col,
                    str,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::LargeUtf8 => {
                hash_array!(
                    LargeStringArray,
                    col,
                    str,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Dictionary(index_type, _) => match **index_type {
                DataType::Int8 => {
                    create_hashes_dictionary::<Int8Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::Int16 => {
                    create_hashes_dictionary::<Int16Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::Int32 => {
                    create_hashes_dictionary::<Int32Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::Int64 => {
                    create_hashes_dictionary::<Int64Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::UInt8 => {
                    create_hashes_dictionary::<UInt8Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::UInt16 => {
                    create_hashes_dictionary::<UInt16Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::UInt32 => {
                    create_hashes_dictionary::<UInt32Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                DataType::UInt64 => {
                    create_hashes_dictionary::<UInt64Type>(
                        col,
                        random_state,
                        hashes_buffer,
                        multi_col,
                    )?;
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported dictionary type in hasher hashing: {}",
                        col.data_type(),
                    )))
                }
            },
            _ => {
                // This is internal because we should have caught this before.
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type in hasher: {}",
                    col.data_type()
                )));
            }
        }
    }
    Ok(hashes_buffer)
}

#[cfg(test)]
mod tests {
    use crate::from_slice::FromSlice;
    use arrow::{array::DictionaryArray, datatypes::Int8Type};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn create_hashes_for_decimal_array() -> Result<()> {
        let array = vec![1, 2, 3, 4]
            .into_iter()
            .map(Some)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(20, 3)
            .unwrap();
        let array_ref = Arc::new(array);
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; array_ref.len()];
        let hashes = create_hashes(&[array_ref], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4);
        Ok(())
    }

    #[test]
    fn create_hashes_for_float_arrays() -> Result<()> {
        let f32_arr = Arc::new(Float32Array::from_slice(&[0.12, 0.5, 1f32, 444.7]));
        let f64_arr = Arc::new(Float64Array::from_slice(&[0.12, 0.5, 1f64, 444.7]));

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; f32_arr.len()];
        let hashes = create_hashes(&[f32_arr], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4,);

        let hashes = create_hashes(&[f64_arr], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4,);

        Ok(())
    }

    #[test]
    // Tests actual values of hashes, which are different if forcing collisions
    #[cfg(not(feature = "force_hash_collisions"))]
    fn create_hashes_for_dict_arrays() {
        let strings = vec![Some("foo"), None, Some("bar"), Some("foo"), None];

        let string_array = Arc::new(strings.iter().cloned().collect::<StringArray>());
        let dict_array = Arc::new(
            strings
                .iter()
                .cloned()
                .collect::<DictionaryArray<Int8Type>>(),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let mut string_hashes = vec![0; strings.len()];
        create_hashes(&[string_array], &random_state, &mut string_hashes).unwrap();

        let mut dict_hashes = vec![0; strings.len()];
        create_hashes(&[dict_array], &random_state, &mut dict_hashes).unwrap();

        // Null values result in a zero hash,
        for (val, hash) in strings.iter().zip(string_hashes.iter()) {
            match val {
                Some(_) => assert_ne!(*hash, 0),
                None => assert_eq!(*hash, 0),
            }
        }

        // same logical values should hash to the same hash value
        assert_eq!(string_hashes, dict_hashes);

        // Same values should map to same hash values
        assert_eq!(strings[1], strings[4]);
        assert_eq!(dict_hashes[1], dict_hashes[4]);
        assert_eq!(strings[0], strings[3]);
        assert_eq!(dict_hashes[0], dict_hashes[3]);

        // different strings should map to different hash values
        assert_ne!(strings[0], strings[2]);
        assert_ne!(dict_hashes[0], dict_hashes[2]);
    }

    #[test]
    // Tests actual values of hashes, which are different if forcing collisions
    #[cfg(not(feature = "force_hash_collisions"))]
    fn create_multi_column_hash_for_dict_arrays() {
        let strings1 = vec![Some("foo"), None, Some("bar")];
        let strings2 = vec![Some("blarg"), Some("blah"), None];

        let string_array = Arc::new(strings1.iter().cloned().collect::<StringArray>());
        let dict_array = Arc::new(
            strings2
                .iter()
                .cloned()
                .collect::<DictionaryArray<Int32Type>>(),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let mut one_col_hashes = vec![0; strings1.len()];
        create_hashes(&[dict_array.clone()], &random_state, &mut one_col_hashes).unwrap();

        let mut two_col_hashes = vec![0; strings1.len()];
        create_hashes(
            &[dict_array, string_array],
            &random_state,
            &mut two_col_hashes,
        )
        .unwrap();

        assert_eq!(one_col_hashes.len(), 3);
        assert_eq!(two_col_hashes.len(), 3);

        assert_ne!(one_col_hashes, two_col_hashes);
    }
}
