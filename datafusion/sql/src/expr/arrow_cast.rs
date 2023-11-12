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

//! Implementation of the `arrow_cast` function that allows
//! casting to arbitrary arrow types (rather than SQL types)

use std::{fmt::Display, iter::Peekable, str::Chars, sync::Arc};

use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion_common::{
    plan_datafusion_err, DFSchema, DataFusionError, Result, ScalarValue,
};

use datafusion_common::plan_err;
use datafusion_expr::{Expr, ExprSchemable};

pub const ARROW_CAST_NAME: &str = "arrow_cast";

/// Create an [`Expr`] that evaluates the `arrow_cast` function
///
/// This function is not a [`BuiltinScalarFunction`] because the
/// return type of [`BuiltinScalarFunction`] depends only on the
/// *types* of the arguments. However, the type of `arrow_type` depends on
/// the *value* of its second argument.
///
/// Use the `cast` function to cast to SQL type (which is then mapped
/// to the corresponding arrow type). For example to cast to `int`
/// (which is then mapped to the arrow type `Int32`)
///
/// ```sql
/// select cast(column_x as int) ...
/// ```
///
/// Use the `arrow_cast` functiont to cast to a specfic arrow type
///
/// For example
/// ```sql
/// select arrow_cast(column_x, 'Float64')
/// ```
/// [`BuiltinScalarFunction`]: datafusion_expr::BuiltinScalarFunction
pub fn create_arrow_cast(mut args: Vec<Expr>, schema: &DFSchema) -> Result<Expr> {
    if args.len() != 2 {
        return plan_err!("arrow_cast needs 2 arguments, {} provided", args.len());
    }
    let arg1 = args.pop().unwrap();
    let arg0 = args.pop().unwrap();

    // arg1 must be a string
    let data_type_string = if let Expr::Literal(ScalarValue::Utf8(Some(v))) = arg1 {
        v
    } else {
        return plan_err!(
            "arrow_cast requires its second argument to be a constant string, got {arg1}"
        );
    };

    // do the actual lookup to the appropriate data type
    let data_type = parse_data_type(&data_type_string)?.into();

    arg0.cast_to(&data_type, schema)
}

/// Parses `str` into a `DataType`.
///
/// `parse_data_type` is the the reverse of [`DataType`]'s `Display`
/// impl, and maintains the invariant that
/// `parse_data_type(data_type.to_string()) == data_type`
///
/// Example:
/// ```
/// # use datafusion_sql::parse_data_type;
/// # use arrow_schema::DataType;
/// let display_value = "Int32";
///
/// // "Int32" is the Display value of `DataType`
/// assert_eq!(display_value, &format!("{}", DataType::Int32));
///
/// // parse_data_type coverts "Int32" back to `DataType`:
/// let data_type = parse_data_type(display_value).unwrap();
/// assert_eq!(data_type, DataType::Int32);
/// ```
///
/// Remove if added to arrow: <https://github.com/apache/arrow-rs/issues/3821>
pub fn parse_data_type(val: &str) -> Result<DataType> {
    Parser::new(val).parse()
}

fn make_error(val: &str, msg: &str) -> DataFusionError {
    plan_datafusion_err!("Unsupported type '{val}'. Must be a supported arrow type name such as 'Int32' or 'Timestamp(Nanosecond, None)'. Error {msg}" )
}

fn make_error_expected(val: &str, expected: &Token, actual: &Token) -> DataFusionError {
    make_error(val, &format!("Expected '{expected}', got '{actual}'"))
}

#[derive(Debug)]
/// Implementation of `parse_data_type`, modeled after <https://github.com/sqlparser-rs/sqlparser-rs>
struct Parser<'a> {
    val: &'a str,
    tokenizer: Tokenizer<'a>,
}

impl<'a> Parser<'a> {
    fn new(val: &'a str) -> Self {
        Self {
            val,
            tokenizer: Tokenizer::new(val),
        }
    }

    fn parse(mut self) -> Result<DataType> {
        let data_type = self.parse_next_type()?;
        // ensure that there is no trailing content
        if self.tokenizer.next().is_some() {
            Err(make_error(
                self.val,
                &format!("checking trailing content after parsing '{data_type}'"),
            ))
        } else {
            Ok(data_type)
        }
    }

    /// parses the next full DataType
    fn parse_next_type(&mut self) -> Result<DataType> {
        match self.next_token()? {
            Token::SimpleType(data_type) => Ok(data_type),
            Token::Timestamp => self.parse_timestamp(),
            Token::Time32 => self.parse_time32(),
            Token::Time64 => self.parse_time64(),
            Token::Duration => self.parse_duration(),
            Token::Interval => self.parse_interval(),
            Token::FixedSizeBinary => self.parse_fixed_size_binary(),
            Token::Decimal128 => self.parse_decimal_128(),
            Token::Decimal256 => self.parse_decimal_256(),
            Token::Dictionary => self.parse_dictionary(),
            Token::List => self.parse_list(),
            tok => Err(make_error(
                self.val,
                &format!("finding next type, got unexpected '{tok}'"),
            )),
        }
    }

    /// Parses the List type
    fn parse_list(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let data_type = self.parse_next_type()?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::List(Arc::new(Field::new(
            "item", data_type, true,
        ))))
    }

    /// Parses the next timeunit
    fn parse_time_unit(&mut self, context: &str) -> Result<TimeUnit> {
        match self.next_token()? {
            Token::TimeUnit(time_unit) => Ok(time_unit),
            tok => Err(make_error(
                self.val,
                &format!("finding TimeUnit for {context}, got {tok}"),
            )),
        }
    }

    /// Parses the next timezone
    fn parse_timezone(&mut self, context: &str) -> Result<Option<String>> {
        match self.next_token()? {
            Token::None => Ok(None),
            Token::Some => {
                self.expect_token(Token::LParen)?;
                let timezone = self.parse_double_quoted_string("Timezone")?;
                self.expect_token(Token::RParen)?;
                Ok(Some(timezone))
            }
            tok => Err(make_error(
                self.val,
                &format!("finding Timezone for {context}, got {tok}"),
            )),
        }
    }

    /// Parses the next double quoted string
    fn parse_double_quoted_string(&mut self, context: &str) -> Result<String> {
        match self.next_token()? {
            Token::DoubleQuotedString(s) => Ok(s),
            tok => Err(make_error(
                self.val,
                &format!("finding double quoted string for {context}, got '{tok}'"),
            )),
        }
    }

    /// Parses the next integer value
    fn parse_i64(&mut self, context: &str) -> Result<i64> {
        match self.next_token()? {
            Token::Integer(v) => Ok(v),
            tok => Err(make_error(
                self.val,
                &format!("finding i64 for {context}, got '{tok}'"),
            )),
        }
    }

    /// Parses the next i32 integer value
    fn parse_i32(&mut self, context: &str) -> Result<i32> {
        let length = self.parse_i64(context)?;
        length.try_into().map_err(|e| {
            make_error(
                self.val,
                &format!("converting {length} into i32 for {context}: {e}"),
            )
        })
    }

    /// Parses the next i8 integer value
    fn parse_i8(&mut self, context: &str) -> Result<i8> {
        let length = self.parse_i64(context)?;
        length.try_into().map_err(|e| {
            make_error(
                self.val,
                &format!("converting {length} into i8 for {context}: {e}"),
            )
        })
    }

    /// Parses the next u8 integer value
    fn parse_u8(&mut self, context: &str) -> Result<u8> {
        let length = self.parse_i64(context)?;
        length.try_into().map_err(|e| {
            make_error(
                self.val,
                &format!("converting {length} into u8 for {context}: {e}"),
            )
        })
    }

    /// Parses the next timestamp (called after `Timestamp` has been consumed)
    fn parse_timestamp(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Timestamp")?;
        self.expect_token(Token::Comma)?;
        let timezone = self.parse_timezone("Timestamp")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Timestamp(time_unit, timezone.map(Into::into)))
    }

    /// Parses the next Time32 (called after `Time32` has been consumed)
    fn parse_time32(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Time32")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Time32(time_unit))
    }

    /// Parses the next Time64 (called after `Time64` has been consumed)
    fn parse_time64(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Time64")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Time64(time_unit))
    }

    /// Parses the next Duration (called after `Duration` has been consumed)
    fn parse_duration(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let time_unit = self.parse_time_unit("Duration")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Duration(time_unit))
    }

    /// Parses the next Interval (called after `Interval` has been consumed)
    fn parse_interval(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let interval_unit = match self.next_token()? {
            Token::IntervalUnit(interval_unit) => interval_unit,
            tok => {
                return Err(make_error(
                    self.val,
                    &format!("finding IntervalUnit for Interval, got {tok}"),
                ))
            }
        };
        self.expect_token(Token::RParen)?;
        Ok(DataType::Interval(interval_unit))
    }

    /// Parses the next FixedSizeBinary (called after `FixedSizeBinary` has been consumed)
    fn parse_fixed_size_binary(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let length = self.parse_i32("FixedSizeBinary")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::FixedSizeBinary(length))
    }

    /// Parses the next Decimal128 (called after `Decimal128` has been consumed)
    fn parse_decimal_128(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let precision = self.parse_u8("Decimal128")?;
        self.expect_token(Token::Comma)?;
        let scale = self.parse_i8("Decimal128")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Decimal128(precision, scale))
    }

    /// Parses the next Decimal256 (called after `Decimal256` has been consumed)
    fn parse_decimal_256(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let precision = self.parse_u8("Decimal256")?;
        self.expect_token(Token::Comma)?;
        let scale = self.parse_i8("Decimal256")?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Decimal256(precision, scale))
    }

    /// Parses the next Dictionary (called after `Dictionary` has been consumed)
    fn parse_dictionary(&mut self) -> Result<DataType> {
        self.expect_token(Token::LParen)?;
        let key_type = self.parse_next_type()?;
        self.expect_token(Token::Comma)?;
        let value_type = self.parse_next_type()?;
        self.expect_token(Token::RParen)?;
        Ok(DataType::Dictionary(
            Box::new(key_type),
            Box::new(value_type),
        ))
    }

    /// return the next token, or an error if there are none left
    fn next_token(&mut self) -> Result<Token> {
        match self.tokenizer.next() {
            None => Err(make_error(self.val, "finding next token")),
            Some(token) => token,
        }
    }

    /// consume the next token, returning OK(()) if it matches tok, and Err if not
    fn expect_token(&mut self, tok: Token) -> Result<()> {
        let next_token = self.next_token()?;
        if next_token == tok {
            Ok(())
        } else {
            Err(make_error_expected(self.val, &tok, &next_token))
        }
    }
}

/// returns true if this character is a separator
fn is_separator(c: char) -> bool {
    c == '(' || c == ')' || c == ',' || c == ' '
}

#[derive(Debug)]
/// Splits a strings like Dictionary(Int32, Int64) into tokens sutable for parsing
///
/// For example the string "Timestamp(Nanosecond, None)" would be parsed into:
///
/// * Token::Timestamp
/// * Token::Lparen
/// * Token::IntervalUnit(IntervalUnit::Nanosecond)
/// * Token::Comma,
/// * Token::None,
/// * Token::Rparen,
struct Tokenizer<'a> {
    val: &'a str,
    chars: Peekable<Chars<'a>>,
    // temporary buffer for parsing words
    word: String,
}

impl<'a> Tokenizer<'a> {
    fn new(val: &'a str) -> Self {
        Self {
            val,
            chars: val.chars().peekable(),
            word: String::new(),
        }
    }

    /// returns the next char, without consuming it
    fn peek_next_char(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }

    /// returns the next char, and consuming it
    fn next_char(&mut self) -> Option<char> {
        self.chars.next()
    }

    /// parse the characters in val starting at pos, until the next
    /// `,`, `(`, or `)` or end of line
    fn parse_word(&mut self) -> Result<Token> {
        // reset temp space
        self.word.clear();
        loop {
            match self.peek_next_char() {
                None => break,
                Some(c) if is_separator(c) => break,
                Some(c) => {
                    self.next_char();
                    self.word.push(c);
                }
            }
        }

        if let Some(c) = self.word.chars().next() {
            // if it started with a number, try parsing it as an integer
            if c == '-' || c.is_numeric() {
                let val: i64 = self.word.parse().map_err(|e| {
                    make_error(
                        self.val,
                        &format!("parsing {} as integer: {e}", self.word),
                    )
                })?;
                return Ok(Token::Integer(val));
            }
            // if it started with a double quote `"`, try parsing it as a double quoted string
            else if c == '"' {
                let len = self.word.chars().count();

                // to verify it's double quoted
                if let Some(last_c) = self.word.chars().last() {
                    if last_c != '"' || len < 2 {
                        return Err(make_error(
                            self.val,
                            &format!("parsing {} as double quoted string: last char must be \"", self.word),
                        ));
                    }
                }

                if len == 2 {
                    return Err(make_error(
                        self.val,
                        &format!("parsing {} as double quoted string: empty string isn't supported", self.word),
                    ));
                }

                let val: String = self.word.parse().map_err(|e| {
                    make_error(
                        self.val,
                        &format!("parsing {} as double quoted string: {e}", self.word),
                    )
                })?;

                let s = val[1..len - 1].to_string();
                if s.contains('"') {
                    return Err(make_error(
                        self.val,
                        &format!("parsing {} as double quoted string: escaped double quote isn't supported", self.word),
                    ));
                }

                return Ok(Token::DoubleQuotedString(s));
            }
        }

        // figure out what the word was
        let token = match self.word.as_str() {
            "Null" => Token::SimpleType(DataType::Null),
            "Boolean" => Token::SimpleType(DataType::Boolean),

            "Int8" => Token::SimpleType(DataType::Int8),
            "Int16" => Token::SimpleType(DataType::Int16),
            "Int32" => Token::SimpleType(DataType::Int32),
            "Int64" => Token::SimpleType(DataType::Int64),

            "UInt8" => Token::SimpleType(DataType::UInt8),
            "UInt16" => Token::SimpleType(DataType::UInt16),
            "UInt32" => Token::SimpleType(DataType::UInt32),
            "UInt64" => Token::SimpleType(DataType::UInt64),

            "Utf8" => Token::SimpleType(DataType::Utf8),
            "LargeUtf8" => Token::SimpleType(DataType::LargeUtf8),
            "Binary" => Token::SimpleType(DataType::Binary),
            "LargeBinary" => Token::SimpleType(DataType::LargeBinary),

            "Float16" => Token::SimpleType(DataType::Float16),
            "Float32" => Token::SimpleType(DataType::Float32),
            "Float64" => Token::SimpleType(DataType::Float64),

            "Date32" => Token::SimpleType(DataType::Date32),
            "Date64" => Token::SimpleType(DataType::Date64),

            "List" => Token::List,

            "Second" => Token::TimeUnit(TimeUnit::Second),
            "Millisecond" => Token::TimeUnit(TimeUnit::Millisecond),
            "Microsecond" => Token::TimeUnit(TimeUnit::Microsecond),
            "Nanosecond" => Token::TimeUnit(TimeUnit::Nanosecond),

            "Timestamp" => Token::Timestamp,
            "Time32" => Token::Time32,
            "Time64" => Token::Time64,
            "Duration" => Token::Duration,
            "Interval" => Token::Interval,
            "Dictionary" => Token::Dictionary,

            "FixedSizeBinary" => Token::FixedSizeBinary,
            "Decimal128" => Token::Decimal128,
            "Decimal256" => Token::Decimal256,

            "YearMonth" => Token::IntervalUnit(IntervalUnit::YearMonth),
            "DayTime" => Token::IntervalUnit(IntervalUnit::DayTime),
            "MonthDayNano" => Token::IntervalUnit(IntervalUnit::MonthDayNano),

            "Some" => Token::Some,
            "None" => Token::None,

            _ => {
                return Err(make_error(
                    self.val,
                    &format!("unrecognized word: {}", self.word),
                ))
            }
        };
        Ok(token)
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.peek_next_char()? {
                ' ' => {
                    // skip whitespace
                    self.next_char();
                    continue;
                }
                '(' => {
                    self.next_char();
                    return Some(Ok(Token::LParen));
                }
                ')' => {
                    self.next_char();
                    return Some(Ok(Token::RParen));
                }
                ',' => {
                    self.next_char();
                    return Some(Ok(Token::Comma));
                }
                _ => return Some(self.parse_word()),
            }
        }
    }
}

/// Grammar is
///
#[derive(Debug, PartialEq)]
enum Token {
    // Null, or Int32
    SimpleType(DataType),
    Timestamp,
    Time32,
    Time64,
    Duration,
    Interval,
    FixedSizeBinary,
    Decimal128,
    Decimal256,
    Dictionary,
    TimeUnit(TimeUnit),
    IntervalUnit(IntervalUnit),
    LParen,
    RParen,
    Comma,
    Some,
    None,
    Integer(i64),
    DoubleQuotedString(String),
    List,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::SimpleType(t) => write!(f, "{t}"),
            Token::List => write!(f, "List"),
            Token::Timestamp => write!(f, "Timestamp"),
            Token::Time32 => write!(f, "Time32"),
            Token::Time64 => write!(f, "Time64"),
            Token::Duration => write!(f, "Duration"),
            Token::Interval => write!(f, "Interval"),
            Token::TimeUnit(u) => write!(f, "TimeUnit({u:?})"),
            Token::IntervalUnit(u) => write!(f, "IntervalUnit({u:?})"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
            Token::Some => write!(f, "Some"),
            Token::None => write!(f, "None"),
            Token::FixedSizeBinary => write!(f, "FixedSizeBinary"),
            Token::Decimal128 => write!(f, "Decimal128"),
            Token::Decimal256 => write!(f, "Decimal256"),
            Token::Dictionary => write!(f, "Dictionary"),
            Token::Integer(v) => write!(f, "Integer({v})"),
            Token::DoubleQuotedString(s) => write!(f, "DoubleQuotedString({s})"),
        }
    }
}

#[cfg(test)]
mod test {
    use arrow_schema::{IntervalUnit, TimeUnit};

    use super::*;

    #[test]
    fn test_parse_data_type() {
        // this ensures types can be parsed correctly from their string representations
        for dt in list_datatypes() {
            round_trip(dt)
        }
    }

    /// convert data_type to a string, and then parse it as a type
    /// verifying it is the same
    fn round_trip(data_type: DataType) {
        let data_type_string = data_type.to_string();
        println!("Input '{data_type_string}' ({data_type:?})");
        let parsed_type = parse_data_type(&data_type_string).unwrap();
        assert_eq!(
            data_type, parsed_type,
            "Mismatch parsing {data_type_string}"
        );
    }

    fn list_datatypes() -> Vec<DataType> {
        vec![
            // ---------
            // Non Nested types
            // ---------
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            // we can't cover all possible timezones, here we only test utc and +08:00
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+08:00".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+08:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("+08:00".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+08:00".into())),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Decimal128(7, 12),
            DataType::Decimal256(6, 13),
            // ---------
            // Nested types
            // ---------
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::FixedSizeBinary(23)),
            ),
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(
                    // nested dictionaries are probably a bad idea but they are possible
                    DataType::Dictionary(
                        Box::new(DataType::Int8),
                        Box::new(DataType::Utf8),
                    ),
                ),
            ),
            // TODO support more structured types (List, LargeList, Struct, Union, Map, RunEndEncoded, etc)
        ]
    }

    #[test]
    fn test_parse_data_type_whitespace_tolerance() {
        // (string to parse, expected DataType)
        let cases = [
            ("Int8", DataType::Int8),
            (
                "Timestamp        (Nanosecond,      None)",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "Timestamp        (Nanosecond,      None)  ",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "          Timestamp        (Nanosecond,      None               )",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "Timestamp        (Nanosecond,      None               )  ",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
        ];

        for (data_type_string, expected_data_type) in cases {
            println!("Parsing '{data_type_string}', expecting '{expected_data_type:?}'");
            let parsed_data_type = parse_data_type(data_type_string).unwrap();
            assert_eq!(parsed_data_type, expected_data_type);
        }
    }

    #[test]
    fn parse_data_type_errors() {
        // (string to parse, expected error message)
        let cases = [
            ("", "Unsupported type ''"),
            ("", "Error finding next token"),
            ("null", "Unsupported type 'null'"),
            ("Nu", "Unsupported type 'Nu'"),
            (
                r#"Timestamp(Nanosecond, Some(+00:00))"#,
                "Error unrecognized word: +00:00",
            ),
            (
                r#"Timestamp(Nanosecond, Some("+00:00))"#,
                r#"parsing "+00:00 as double quoted string: last char must be ""#,
            ),
            (
                r#"Timestamp(Nanosecond, Some(""))"#,
                r#"parsing "" as double quoted string: empty string isn't supported"#,
            ),
            (
                r#"Timestamp(Nanosecond, Some("+00:00""))"#,
                r#"parsing "+00:00"" as double quoted string: escaped double quote isn't supported"#,
            ),
            ("Timestamp(Nanosecond, ", "Error finding next token"),
            (
                "Float32 Float32",
                "trailing content after parsing 'Float32'",
            ),
            ("Int32, ", "trailing content after parsing 'Int32'"),
            ("Int32(3), ", "trailing content after parsing 'Int32'"),
            ("FixedSizeBinary(Int32), ", "Error finding i64 for FixedSizeBinary, got 'Int32'"),
            ("FixedSizeBinary(3.0), ", "Error parsing 3.0 as integer: invalid digit found in string"),
            // too large for i32
            ("FixedSizeBinary(4000000000), ", "Error converting 4000000000 into i32 for FixedSizeBinary: out of range integral type conversion attempted"),
            // can't have negative precision
            ("Decimal128(-3, 5)", "Error converting -3 into u8 for Decimal128: out of range integral type conversion attempted"),
            ("Decimal256(-3, 5)", "Error converting -3 into u8 for Decimal256: out of range integral type conversion attempted"),
            ("Decimal128(3, 500)", "Error converting 500 into i8 for Decimal128: out of range integral type conversion attempted"),
            ("Decimal256(3, 500)", "Error converting 500 into i8 for Decimal256: out of range integral type conversion attempted"),

        ];

        for (data_type_string, expected_message) in cases {
            print!("Parsing '{data_type_string}', expecting '{expected_message}'");
            match parse_data_type(data_type_string) {
                Ok(d) => panic!(
                    "Expected error while parsing '{data_type_string}', but got '{d}'"
                ),
                Err(e) => {
                    let message = e.to_string();
                    assert!(
                        message.contains(expected_message),
                        "\n\ndid not find expected in actual.\n\nexpected: {expected_message}\nactual:{message}\n"
                    );
                    // errors should also contain  a help message
                    assert!(message.contains("Must be a supported arrow type name such as 'Int32' or 'Timestamp(Nanosecond, None)'"));
                }
            }
            println!(" Ok");
        }
    }
}
