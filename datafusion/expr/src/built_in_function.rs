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

//! Built-in functions module contains all the built-in functions definitions.

use crate::Volatility;
use datafusion_common::{DataFusionError, Result};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use lazy_static::lazy_static;

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumIter, Copy)]
pub enum BuiltinScalarFunction {
    // math functions
    /// abs
    Abs,
    /// acos
    Acos,
    /// asin
    Asin,
    /// atan
    Atan,
    /// atan2
    Atan2,
    /// acosh
    Acosh,
    /// asinh
    Asinh,
    /// atanh
    Atanh,
    /// cbrt
    Cbrt,
    /// ceil
    Ceil,
    /// coalesce
    Coalesce,
    /// cos
    Cos,
    /// cos
    Cosh,
    /// degrees
    Degrees,
    /// Digest
    Digest,
    /// exp
    Exp,
    /// factorial
    Factorial,
    /// floor
    Floor,
    /// gcd, Greatest common divisor
    Gcd,
    /// lcm, Least common multiple
    Lcm,
    /// ln, Natural logarithm
    Ln,
    /// log, same as log10
    Log,
    /// log10
    Log10,
    /// log2
    Log2,
    /// pi
    Pi,
    /// power
    Power,
    /// radians
    Radians,
    /// round
    Round,
    /// signum
    Signum,
    /// sin
    Sin,
    /// sinh
    Sinh,
    /// sqrt
    Sqrt,
    /// tan
    Tan,
    /// tanh
    Tanh,
    /// trunc
    Trunc,

    // string functions
    /// construct an array from columns
    MakeArray,
    /// ascii
    Ascii,
    /// bit_length
    BitLength,
    /// btrim
    Btrim,
    /// character_length
    CharacterLength,
    /// chr
    Chr,
    /// concat
    Concat,
    /// concat_ws
    ConcatWithSeparator,
    /// date_part
    DatePart,
    /// date_trunc
    DateTrunc,
    /// date_bin
    DateBin,
    /// initcap
    InitCap,
    /// left
    Left,
    /// lpad
    Lpad,
    /// lower
    Lower,
    /// ltrim
    Ltrim,
    /// md5
    MD5,
    /// nullif
    NullIf,
    /// octet_length
    OctetLength,
    /// random
    Random,
    /// regexp_replace
    RegexpReplace,
    /// repeat
    Repeat,
    /// replace
    Replace,
    /// reverse
    Reverse,
    /// right
    Right,
    /// rpad
    Rpad,
    /// rtrim
    Rtrim,
    /// sha224
    SHA224,
    /// sha256
    SHA256,
    /// sha384
    SHA384,
    /// Sha512
    SHA512,
    /// split_part
    SplitPart,
    /// starts_with
    StartsWith,
    /// strpos
    Strpos,
    /// substr
    Substr,
    /// to_hex
    ToHex,
    /// to_timestamp
    ToTimestamp,
    /// to_timestamp_millis
    ToTimestampMillis,
    /// to_timestamp_micros
    ToTimestampMicros,
    /// to_timestamp_seconds
    ToTimestampSeconds,
    /// from_unixtime
    FromUnixtime,
    ///now
    Now,
    ///current_date
    CurrentDate,
    /// current_time
    CurrentTime,
    /// translate
    Translate,
    /// trim
    Trim,
    /// upper
    Upper,
    /// uuid
    Uuid,
    /// regexp_match
    RegexpMatch,
    /// struct
    Struct,
    /// arrow_typeof
    ArrowTypeof,
    /// greatest
    Greatest,
    /// least
    Least,
}

lazy_static! {
    /// Maps the sql function name to `BuiltinScalarFunction`
    static ref NAME_TO_FUNCTION: HashMap<&'static str, BuiltinScalarFunction> = {
        let mut map: HashMap<&'static str, BuiltinScalarFunction> = HashMap::new();
        BuiltinScalarFunction::iter().for_each(|func| {
            let a = aliases(&func);
            a.iter().for_each(|a| {map.insert(a, func);});
        });
        map
    };

    /// Maps `BuiltinScalarFunction` --> canonical sql function
    /// First alias in the array is used to display function names
    static ref FUNCTION_TO_NAME: HashMap<BuiltinScalarFunction, &'static str> = {
        let mut map: HashMap<BuiltinScalarFunction, &'static str> = HashMap::new();
        BuiltinScalarFunction::iter().for_each(|func| {
            map.insert(func, aliases(&func).first().unwrap_or(&"NO_ALIAS"));
        });
        map
    };
}

impl BuiltinScalarFunction {
    /// an allowlist of functions to take zero arguments, so that they will get special treatment
    /// while executing.
    pub fn supports_zero_argument(&self) -> bool {
        matches!(
            self,
            BuiltinScalarFunction::Pi
                | BuiltinScalarFunction::Random
                | BuiltinScalarFunction::Now
                | BuiltinScalarFunction::CurrentDate
                | BuiltinScalarFunction::CurrentTime
                | BuiltinScalarFunction::Uuid
        )
    }
    /// Returns the [Volatility] of the builtin function.
    pub fn volatility(&self) -> Volatility {
        match self {
            // Immutable scalar builtins
            BuiltinScalarFunction::Abs => Volatility::Immutable,
            BuiltinScalarFunction::Acos => Volatility::Immutable,
            BuiltinScalarFunction::Asin => Volatility::Immutable,
            BuiltinScalarFunction::Atan => Volatility::Immutable,
            BuiltinScalarFunction::Atan2 => Volatility::Immutable,
            BuiltinScalarFunction::Acosh => Volatility::Immutable,
            BuiltinScalarFunction::Asinh => Volatility::Immutable,
            BuiltinScalarFunction::Atanh => Volatility::Immutable,
            BuiltinScalarFunction::Ceil => Volatility::Immutable,
            BuiltinScalarFunction::Coalesce => Volatility::Immutable,
            BuiltinScalarFunction::Cos => Volatility::Immutable,
            BuiltinScalarFunction::Cosh => Volatility::Immutable,
            BuiltinScalarFunction::Degrees => Volatility::Immutable,
            BuiltinScalarFunction::Exp => Volatility::Immutable,
            BuiltinScalarFunction::Factorial => Volatility::Immutable,
            BuiltinScalarFunction::Floor => Volatility::Immutable,
            BuiltinScalarFunction::Gcd => Volatility::Immutable,
            BuiltinScalarFunction::Lcm => Volatility::Immutable,
            BuiltinScalarFunction::Ln => Volatility::Immutable,
            BuiltinScalarFunction::Log => Volatility::Immutable,
            BuiltinScalarFunction::Log10 => Volatility::Immutable,
            BuiltinScalarFunction::Log2 => Volatility::Immutable,
            BuiltinScalarFunction::Pi => Volatility::Immutable,
            BuiltinScalarFunction::Power => Volatility::Immutable,
            BuiltinScalarFunction::Round => Volatility::Immutable,
            BuiltinScalarFunction::Signum => Volatility::Immutable,
            BuiltinScalarFunction::Sin => Volatility::Immutable,
            BuiltinScalarFunction::Sinh => Volatility::Immutable,
            BuiltinScalarFunction::Sqrt => Volatility::Immutable,
            BuiltinScalarFunction::Cbrt => Volatility::Immutable,
            BuiltinScalarFunction::Tan => Volatility::Immutable,
            BuiltinScalarFunction::Tanh => Volatility::Immutable,
            BuiltinScalarFunction::Trunc => Volatility::Immutable,
            BuiltinScalarFunction::MakeArray => Volatility::Immutable,
            BuiltinScalarFunction::Ascii => Volatility::Immutable,
            BuiltinScalarFunction::BitLength => Volatility::Immutable,
            BuiltinScalarFunction::Btrim => Volatility::Immutable,
            BuiltinScalarFunction::CharacterLength => Volatility::Immutable,
            BuiltinScalarFunction::Chr => Volatility::Immutable,
            BuiltinScalarFunction::Concat => Volatility::Immutable,
            BuiltinScalarFunction::ConcatWithSeparator => Volatility::Immutable,
            BuiltinScalarFunction::DatePart => Volatility::Immutable,
            BuiltinScalarFunction::DateTrunc => Volatility::Immutable,
            BuiltinScalarFunction::DateBin => Volatility::Immutable,
            BuiltinScalarFunction::InitCap => Volatility::Immutable,
            BuiltinScalarFunction::Left => Volatility::Immutable,
            BuiltinScalarFunction::Lpad => Volatility::Immutable,
            BuiltinScalarFunction::Lower => Volatility::Immutable,
            BuiltinScalarFunction::Ltrim => Volatility::Immutable,
            BuiltinScalarFunction::MD5 => Volatility::Immutable,
            BuiltinScalarFunction::NullIf => Volatility::Immutable,
            BuiltinScalarFunction::OctetLength => Volatility::Immutable,
            BuiltinScalarFunction::Radians => Volatility::Immutable,
            BuiltinScalarFunction::RegexpReplace => Volatility::Immutable,
            BuiltinScalarFunction::Repeat => Volatility::Immutable,
            BuiltinScalarFunction::Replace => Volatility::Immutable,
            BuiltinScalarFunction::Reverse => Volatility::Immutable,
            BuiltinScalarFunction::Right => Volatility::Immutable,
            BuiltinScalarFunction::Rpad => Volatility::Immutable,
            BuiltinScalarFunction::Rtrim => Volatility::Immutable,
            BuiltinScalarFunction::SHA224 => Volatility::Immutable,
            BuiltinScalarFunction::SHA256 => Volatility::Immutable,
            BuiltinScalarFunction::SHA384 => Volatility::Immutable,
            BuiltinScalarFunction::SHA512 => Volatility::Immutable,
            BuiltinScalarFunction::Digest => Volatility::Immutable,
            BuiltinScalarFunction::SplitPart => Volatility::Immutable,
            BuiltinScalarFunction::StartsWith => Volatility::Immutable,
            BuiltinScalarFunction::Strpos => Volatility::Immutable,
            BuiltinScalarFunction::Substr => Volatility::Immutable,
            BuiltinScalarFunction::ToHex => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestamp => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampMillis => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampMicros => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampSeconds => Volatility::Immutable,
            BuiltinScalarFunction::Translate => Volatility::Immutable,
            BuiltinScalarFunction::Trim => Volatility::Immutable,
            BuiltinScalarFunction::Upper => Volatility::Immutable,
            BuiltinScalarFunction::RegexpMatch => Volatility::Immutable,
            BuiltinScalarFunction::Struct => Volatility::Immutable,
            BuiltinScalarFunction::FromUnixtime => Volatility::Immutable,
            BuiltinScalarFunction::ArrowTypeof => Volatility::Immutable,
            BuiltinScalarFunction::Greatest => Volatility::Immutable,
            BuiltinScalarFunction::Least => Volatility::Immutable,

            // Stable builtin functions
            BuiltinScalarFunction::Now => Volatility::Stable,
            BuiltinScalarFunction::CurrentDate => Volatility::Stable,
            BuiltinScalarFunction::CurrentTime => Volatility::Stable,

            // Volatile builtin functions
            BuiltinScalarFunction::Random => Volatility::Volatile,
            BuiltinScalarFunction::Uuid => Volatility::Volatile,
        }
    }
}

fn aliases(func: &BuiltinScalarFunction) -> &'static [&'static str] {
    match func {
        BuiltinScalarFunction::Abs => &["abs"],
        BuiltinScalarFunction::Acos => &["acos"],
        BuiltinScalarFunction::Acosh => &["acosh"],
        BuiltinScalarFunction::Asin => &["asin"],
        BuiltinScalarFunction::Asinh => &["asinh"],
        BuiltinScalarFunction::Atan => &["atan"],
        BuiltinScalarFunction::Atanh => &["atanh"],
        BuiltinScalarFunction::Atan2 => &["atan2"],
        BuiltinScalarFunction::Cbrt => &["cbrt"],
        BuiltinScalarFunction::Ceil => &["ceil"],
        BuiltinScalarFunction::Cos => &["cos"],
        BuiltinScalarFunction::Cosh => &["cosh"],
        BuiltinScalarFunction::Degrees => &["degrees"],
        BuiltinScalarFunction::Exp => &["exp"],
        BuiltinScalarFunction::Factorial => &["factorial"],
        BuiltinScalarFunction::Floor => &["floor"],
        BuiltinScalarFunction::Gcd => &["gcd"],
        BuiltinScalarFunction::Lcm => &["lcm"],
        BuiltinScalarFunction::Ln => &["ln"],
        BuiltinScalarFunction::Log => &["log"],
        BuiltinScalarFunction::Log10 => &["log10"],
        BuiltinScalarFunction::Log2 => &["log2"],
        BuiltinScalarFunction::Pi => &["pi"],
        BuiltinScalarFunction::Power => &["power", "pow"],
        BuiltinScalarFunction::Radians => &["radians"],
        BuiltinScalarFunction::Random => &["random"],
        BuiltinScalarFunction::Round => &["round"],
        BuiltinScalarFunction::Signum => &["signum"],
        BuiltinScalarFunction::Sin => &["sin"],
        BuiltinScalarFunction::Sinh => &["sinh"],
        BuiltinScalarFunction::Sqrt => &["sqrt"],
        BuiltinScalarFunction::Tan => &["tan"],
        BuiltinScalarFunction::Tanh => &["tanh"],
        BuiltinScalarFunction::Trunc => &["trunc"],

        // conditional functions
        BuiltinScalarFunction::Coalesce => &["coalesce"],
        BuiltinScalarFunction::NullIf => &["nullif"],

        // string functions
        BuiltinScalarFunction::Ascii => &["ascii"],
        BuiltinScalarFunction::BitLength => &["bit_length"],
        BuiltinScalarFunction::Btrim => &["btrim"],
        BuiltinScalarFunction::CharacterLength => {
            &["character_length", "char_length", "length"]
        }
        BuiltinScalarFunction::Concat => &["concat"],
        BuiltinScalarFunction::ConcatWithSeparator => &["concat_ws"],
        BuiltinScalarFunction::Chr => &["chr"],
        BuiltinScalarFunction::InitCap => &["initcap"],
        BuiltinScalarFunction::Left => &["left"],
        BuiltinScalarFunction::Lower => &["lower"],
        BuiltinScalarFunction::Lpad => &["lpad"],
        BuiltinScalarFunction::Ltrim => &["ltrim"],
        BuiltinScalarFunction::OctetLength => &["octet_length"],
        BuiltinScalarFunction::Repeat => &["repeat"],
        BuiltinScalarFunction::Replace => &["replace"],
        BuiltinScalarFunction::Reverse => &["reverse"],
        BuiltinScalarFunction::Right => &["right"],
        BuiltinScalarFunction::Rpad => &["rpad"],
        BuiltinScalarFunction::Rtrim => &["rtrim"],
        BuiltinScalarFunction::SplitPart => &["split_part"],
        BuiltinScalarFunction::StartsWith => &["starts_with"],
        BuiltinScalarFunction::Strpos => &["strpos"],
        BuiltinScalarFunction::Substr => &["substr"],
        BuiltinScalarFunction::ToHex => &["to_hex"],
        BuiltinScalarFunction::Translate => &["translate"],
        BuiltinScalarFunction::Trim => &["trim"],
        BuiltinScalarFunction::Upper => &["upper"],
        BuiltinScalarFunction::Uuid => &["uuid"],

        // comparison functions
        BuiltinScalarFunction::Greatest => &["greatest"],
        BuiltinScalarFunction::Least => &["least"],

        // regex functions
        BuiltinScalarFunction::RegexpMatch => &["regexp_match"],
        BuiltinScalarFunction::RegexpReplace => &["regexp_replace"],

        // time/date functions
        BuiltinScalarFunction::Now => &["now"],
        BuiltinScalarFunction::CurrentDate => &["current_date"],
        BuiltinScalarFunction::CurrentTime => &["current_time"],
        BuiltinScalarFunction::DateBin => &["date_bin"],
        BuiltinScalarFunction::DateTrunc => &["date_trunc", "datetrunc"],
        BuiltinScalarFunction::DatePart => &["date_part", "datepart"],
        BuiltinScalarFunction::ToTimestamp => &["to_timestamp"],
        BuiltinScalarFunction::ToTimestampMillis => &["to_timestamp_millis"],
        BuiltinScalarFunction::ToTimestampMicros => &["to_timestamp_micros"],
        BuiltinScalarFunction::ToTimestampSeconds => &["to_timestamp_seconds"],
        BuiltinScalarFunction::FromUnixtime => &["from_unixtime"],

        // hashing functions
        BuiltinScalarFunction::Digest => &["digest"],
        BuiltinScalarFunction::MD5 => &["md5"],
        BuiltinScalarFunction::SHA224 => &["sha224"],
        BuiltinScalarFunction::SHA256 => &["sha256"],
        BuiltinScalarFunction::SHA384 => &["sha384"],
        BuiltinScalarFunction::SHA512 => &["sha512"],

        // other functions
        BuiltinScalarFunction::Struct => &["struct"],
        BuiltinScalarFunction::ArrowTypeof => &["arrow_typeof"],

        // array functions
        BuiltinScalarFunction::MakeArray => &["make_array"],
    }
}

impl fmt::Display for BuiltinScalarFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // .unwrap is safe here because compiler makes sure the map will have matches for each BuiltinScalarFunction
        write!(f, "{}", FUNCTION_TO_NAME.get(self).unwrap())
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> Result<BuiltinScalarFunction> {
        if let Some(func) = NAME_TO_FUNCTION.get(name) {
            Ok(*func)
        } else {
            Err(DataFusionError::Plan(format!(
                "There is no built-in function named {name}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test for BuiltinScalarFunction's Display and from_str() implementations.
    // For each variant in BuiltinScalarFunction, it converts the variant to a string
    // and then back to a variant. The test asserts that the original variant and
    // the reconstructed variant are the same.
    fn test_display_and_from_str() {
        for (_, func_original) in NAME_TO_FUNCTION.iter() {
            let func_name = func_original.to_string();
            let func_from_str = BuiltinScalarFunction::from_str(&func_name).unwrap();
            assert_eq!(func_from_str, *func_original);
        }
    }
}
