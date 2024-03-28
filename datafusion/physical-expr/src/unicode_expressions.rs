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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! Unicode expressions

use std::cmp::max;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, GenericStringArray, OffsetSizeTrait, PrimitiveArray},
    datatypes::{ArrowNativeType, ArrowPrimitiveType},
};
use hashbrown::HashMap;
use unicode_segmentation::UnicodeSegmentation;

use datafusion_common::{
    cast::{as_generic_string_array, as_int64_array},
    exec_err, Result,
};

/// Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)
/// strpos('high', 'ig') = 2
/// The implementation uses UTF-8 code points as characters
pub fn strpos<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    let string_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[0])?;

    let substring_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[1])?;

    let result = string_array
        .iter()
        .zip(substring_array.iter())
        .map(|(string, substring)| match (string, substring) {
            (Some(string), Some(substring)) => {
                // the find method returns the byte index of the substring
                // Next, we count the number of the chars until that byte
                T::Native::from_usize(
                    string
                        .find(substring)
                        .map(|x| string[..x].chars().count() + 1)
                        .unwrap_or(0),
                )
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Extracts the substring of string starting at the start'th character, and extending for count characters if that is specified. (Same as substring(string from start for count).)
/// substr('alphabet', 3) = 'phabet'
/// substr('alphabet', 3, 2) = 'ph'
/// The implementation uses UTF-8 code points as characters
pub fn substr<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let start_array = as_int64_array(&args[1])?;

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .map(|(string, start)| match (string, start) {
                    (Some(string), Some(start)) => {
                        if start <= 0 {
                            Some(string.to_string())
                        } else {
                            Some(string.chars().skip(start as usize - 1).collect())
                        }
                    }
                    _ => None,
                })
                .collect::<GenericStringArray<T>>();

            Ok(Arc::new(result) as ArrayRef)
        }
        3 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let start_array = as_int64_array(&args[1])?;
            let count_array = as_int64_array(&args[2])?;

            let result = string_array
                .iter()
                .zip(start_array.iter())
                .zip(count_array.iter())
                .map(|((string, start), count)| match (string, start, count) {
                    (Some(string), Some(start), Some(count)) => {
                        if count < 0 {
                            exec_err!(
                                "negative substring length not allowed: substr(<str>, {start}, {count})"
                            )
                        } else {
                            let skip = max(0, start - 1);
                            let count = max(0, count + (if start < 1 {start - 1} else {0}));
                            Ok(Some(string.chars().skip(skip as usize).take(count as usize).collect::<String>()))
                        }
                    }
                    _ => Ok(None),
                })
                .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!("substr was called with {other} arguments. It requires 2 or 3.")
        }
    }
}

/// Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.
/// translate('12345', '143', 'ax') = 'a2x5'
pub fn translate<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = as_generic_string_array::<T>(&args[2])?;

    let result = string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
        .map(|((string, from), to)| match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                // create a hashmap of [char, index] to change from O(n) to O(1) for from list
                let from_map: HashMap<&str, usize> = from
                    .graphemes(true)
                    .collect::<Vec<&str>>()
                    .iter()
                    .enumerate()
                    .map(|(index, c)| (c.to_owned(), index))
                    .collect();

                let to = to.graphemes(true).collect::<Vec<&str>>();

                Some(
                    string
                        .graphemes(true)
                        .collect::<Vec<&str>>()
                        .iter()
                        .flat_map(|c| match from_map.get(*c) {
                            Some(n) => to.get(*n).copied(),
                            None => Some(*c),
                        })
                        .collect::<Vec<&str>>()
                        .concat(),
                )
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the substring from str before count occurrences of the delimiter delim. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
/// SUBSTRING_INDEX('www.apache.org', '.', 1) = www
/// SUBSTRING_INDEX('www.apache.org', '.', 2) = www.apache
/// SUBSTRING_INDEX('www.apache.org', '.', -2) = apache.org
/// SUBSTRING_INDEX('www.apache.org', '.', -1) = org
pub fn substr_index<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "substr_index was called with {} arguments. It requires 3.",
            args.len()
        );
    }

    let string_array = as_generic_string_array::<T>(&args[0])?;
    let delimiter_array = as_generic_string_array::<T>(&args[1])?;
    let count_array = as_int64_array(&args[2])?;

    let result = string_array
        .iter()
        .zip(delimiter_array.iter())
        .zip(count_array.iter())
        .map(|((string, delimiter), n)| match (string, delimiter, n) {
            (Some(string), Some(delimiter), Some(n)) => {
                // In MySQL, these cases will return an empty string.
                if n == 0 || string.is_empty() || delimiter.is_empty() {
                    return Some(String::new());
                }

                let splitted: Box<dyn Iterator<Item = _>> = if n > 0 {
                    Box::new(string.split(delimiter))
                } else {
                    Box::new(string.rsplit(delimiter))
                };
                let occurrences = usize::try_from(n.unsigned_abs()).unwrap_or(usize::MAX);
                // The length of the substring covered by substr_index.
                let length = splitted
                    .take(occurrences) // at least 1 element, since n != 0
                    .map(|s| s.len() + delimiter.len())
                    .sum::<usize>()
                    - delimiter.len();
                if n > 0 {
                    Some(string[..length].to_owned())
                } else {
                    Some(string[string.len() - length..].to_owned())
                }
            }
            _ => None,
        })
        .collect::<GenericStringArray<T>>();

    Ok(Arc::new(result) as ArrayRef)
}

///Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings
///A string list is a string composed of substrings separated by , characters.
pub fn find_in_set<T: ArrowPrimitiveType>(args: &[ArrayRef]) -> Result<ArrayRef>
where
    T::Native: OffsetSizeTrait,
{
    if args.len() != 2 {
        return exec_err!(
            "find_in_set was called with {} arguments. It requires 2.",
            args.len()
        );
    }

    let str_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[0])?;
    let str_list_array: &GenericStringArray<T::Native> =
        as_generic_string_array::<T::Native>(&args[1])?;

    let result = str_array
        .iter()
        .zip(str_list_array.iter())
        .map(|(string, str_list)| match (string, str_list) {
            (Some(string), Some(str_list)) => {
                let mut res = 0;
                let str_set: Vec<&str> = str_list.split(',').collect();
                for (idx, str) in str_set.iter().enumerate() {
                    if str == &string {
                        res = idx + 1;
                        break;
                    }
                }
                T::Native::from_usize(res)
            }
            _ => None,
        })
        .collect::<PrimitiveArray<T>>();
    Ok(Arc::new(result) as ArrayRef)
}
