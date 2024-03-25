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

//! "string" DataFusion functions

use std::sync::Arc;

use datafusion_expr::ScalarUDF;

mod ascii;
mod bit_length;
mod btrim;
mod chr;
mod common;
mod levenshtein;
mod lower;
mod ltrim;
mod octet_length;
mod overlay;
mod repeat;
mod replace;
mod rtrim;
mod split_part;
mod starts_with;
mod to_hex;
mod upper;
mod uuid;

// create UDFs
make_udf_function!(ascii::AsciiFunc, ASCII, ascii);
make_udf_function!(bit_length::BitLengthFunc, BIT_LENGTH, bit_length);
make_udf_function!(btrim::BTrimFunc, BTRIM, btrim);
make_udf_function!(chr::ChrFunc, CHR, chr);
make_udf_function!(levenshtein::LevenshteinFunc, LEVENSHTEIN, levenshtein);
make_udf_function!(ltrim::LtrimFunc, LTRIM, ltrim);
make_udf_function!(lower::LowerFunc, LOWER, lower);
make_udf_function!(octet_length::OctetLengthFunc, OCTET_LENGTH, octet_length);
make_udf_function!(overlay::OverlayFunc, OVERLAY, overlay);
make_udf_function!(repeat::RepeatFunc, REPEAT, repeat);
make_udf_function!(replace::ReplaceFunc, REPLACE, replace);
make_udf_function!(rtrim::RtrimFunc, RTRIM, rtrim);
make_udf_function!(starts_with::StartsWithFunc, STARTS_WITH, starts_with);
make_udf_function!(split_part::SplitPartFunc, SPLIT_PART, split_part);
make_udf_function!(to_hex::ToHexFunc, TO_HEX, to_hex);
make_udf_function!(upper::UpperFunc, UPPER, upper);
make_udf_function!(uuid::UuidFunc, UUID, uuid);

pub mod expr_fn {
    use datafusion_expr::Expr;

    #[doc = "Returns the numeric code of the first character of the argument."]
    pub fn ascii(arg1: Expr) -> Expr {
        super::ascii().call(vec![arg1])
    }

    #[doc = "Returns the number of bits in the `string`"]
    pub fn bit_length(arg: Expr) -> Expr {
        super::bit_length().call(vec![arg])
    }

    #[doc = "Removes all characters, spaces by default, from both sides of a string"]
    pub fn btrim(args: Vec<Expr>) -> Expr {
        super::btrim().call(args)
    }

    #[doc = "Converts the Unicode code point to a UTF8 character"]
    pub fn chr(arg: Expr) -> Expr {
        super::chr().call(vec![arg])
    }

    #[doc = "Returns the Levenshtein distance between the two given strings"]
    pub fn levenshtein(arg1: Expr, arg2: Expr) -> Expr {
        super::levenshtein().call(vec![arg1, arg2])
    }

    #[doc = "Converts a string to lowercase."]
    pub fn lower(arg1: Expr) -> Expr {
        super::lower().call(vec![arg1])
    }

    #[doc = "Removes all characters, spaces by default, from the beginning of a string"]
    pub fn ltrim(args: Vec<Expr>) -> Expr {
        super::ltrim().call(args)
    }

    #[doc = "returns the number of bytes of a string"]
    pub fn octet_length(args: Vec<Expr>) -> Expr {
        super::octet_length().call(args)
    }

    #[doc = "replace the substring of string that starts at the start'th character and extends for count characters with new substring"]
    pub fn overlay(args: Vec<Expr>) -> Expr {
        super::overlay().call(args)
    }

    #[doc = "Repeats the `string` to `n` times"]
    pub fn repeat(string: Expr, n: Expr) -> Expr {
        super::repeat().call(vec![string, n])
    }

    #[doc = "Replaces all occurrences of `from` with `to` in the `string`"]
    pub fn replace(string: Expr, from: Expr, to: Expr) -> Expr {
        super::replace().call(vec![string, from, to])
    }

    #[doc = "Removes all characters, spaces by default, from the end of a string"]
    pub fn rtrim(args: Vec<Expr>) -> Expr {
        super::rtrim().call(args)
    }

    #[doc = "Splits a string based on a delimiter and picks out the desired field based on the index."]
    pub fn split_part(string: Expr, delimiter: Expr, index: Expr) -> Expr {
        super::split_part().call(vec![string, delimiter, index])
    }

    #[doc = "Returns true if string starts with prefix."]
    pub fn starts_with(arg1: Expr, arg2: Expr) -> Expr {
        super::starts_with().call(vec![arg1, arg2])
    }

    #[doc = "Converts an integer to a hexadecimal string."]
    pub fn to_hex(arg1: Expr) -> Expr {
        super::to_hex().call(vec![arg1])
    }

    #[doc = "Removes all characters, spaces by default, from both sides of a string"]
    pub fn trim(args: Vec<Expr>) -> Expr {
        super::btrim().call(args)
    }

    #[doc = "Converts a string to uppercase."]
    pub fn upper(arg1: Expr) -> Expr {
        super::upper().call(vec![arg1])
    }

    #[doc = "returns uuid v4 as a string value"]
    pub fn uuid() -> Expr {
        super::uuid().call(vec![])
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        ascii(),
        bit_length(),
        btrim(),
        chr(),
        levenshtein(),
        lower(),
        ltrim(),
        octet_length(),
        overlay(),
        repeat(),
        replace(),
        rtrim(),
        split_part(),
        starts_with(),
        to_hex(),
        upper(),
        uuid(),
    ]
}
