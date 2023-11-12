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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use arrow::array::new_null_array;
use arrow::compute::kernels::cast_utils::parse_interval_month_day_nano;
use arrow::datatypes::DECIMAL128_MAX_PRECISION;
use arrow_schema::DataType;
use datafusion_common::logical_type::LogicalType;
use datafusion_common::{
    not_impl_err, plan_err, DFSchema, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{BinaryExpr, Placeholder};
use datafusion_expr::{lit, Expr, Operator};
use log::debug;
use sqlparser::ast::{BinaryOperator, Expr as SQLExpr, Interval, Value};
use sqlparser::parser::ParserError::ParserError;
use std::borrow::Cow;
use std::collections::HashSet;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn parse_value(
        &self,
        value: Value,
        param_data_types: &[LogicalType],
    ) -> Result<Expr> {
        match value {
            Value::Number(n, _) => self.parse_sql_number(&n, false),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(lit(s)),
            Value::Null => Ok(Expr::Literal(ScalarValue::Null)),
            Value::Boolean(n) => Ok(lit(n)),
            Value::Placeholder(param) => {
                Self::create_placeholder_expr(param, param_data_types)
            }
            Value::HexStringLiteral(s) => {
                if let Some(v) = try_decode_hex_literal(&s) {
                    Ok(lit(v))
                } else {
                    plan_err!("Invalid HexStringLiteral '{s}'")
                }
            }
            _ => plan_err!("Unsupported Value '{value:?}'"),
        }
    }

    /// Parse number in sql string, convert to Expr::Literal
    pub(super) fn parse_sql_number(
        &self,
        unsigned_number: &str,
        negative: bool,
    ) -> Result<Expr> {
        let signed_number: Cow<str> = if negative {
            Cow::Owned(format!("-{unsigned_number}"))
        } else {
            Cow::Borrowed(unsigned_number)
        };

        // Try to parse as i64 first, then u64 if negative is false, then decimal or f64

        if let Ok(n) = signed_number.parse::<i64>() {
            return Ok(lit(n));
        }

        if !negative {
            if let Ok(n) = unsigned_number.parse::<u64>() {
                return Ok(lit(n));
            }
        }

        if self.options.parse_float_as_decimal {
            parse_decimal_128(unsigned_number, negative)
        } else {
            signed_number.parse::<f64>().map(lit).map_err(|_| {
                DataFusionError::from(ParserError(format!(
                    "Cannot parse {signed_number} as f64"
                )))
            })
        }
    }

    /// Create a placeholder expression
    /// This is the same as Postgres's prepare statement syntax in which a placeholder starts with `$` sign and then
    /// number 1, 2, ... etc. For example, `$1` is the first placeholder; $2 is the second one and so on.
    fn create_placeholder_expr(
        param: String,
        param_data_types: &[LogicalType],
    ) -> Result<Expr> {
        // Parse the placeholder as a number because it is the only support from sqlparser and postgres
        let index = param[1..].parse::<usize>();
        let idx = match index {
            Ok(0) => {
                return plan_err!(
                    "Invalid placeholder, zero is not a valid index: {param}"
                );
            }
            Ok(index) => index - 1,
            Err(_) => {
                return plan_err!("Invalid placeholder, not a number: {param}");
            }
        };
        // Check if the placeholder is in the parameter list
        let param_type = param_data_types.get(idx);
        // Data type of the parameter
        debug!(
            "type of param {} param_data_types[idx]: {:?}",
            param, param_type
        );

        Ok(Expr::Placeholder(Placeholder::new(
            param,
            param_type.cloned(),
        )))
    }

    pub(super) fn sql_array_literal(
        &self,
        elements: Vec<SQLExpr>,
        schema: &DFSchema,
    ) -> Result<Expr> {
        let mut values = Vec::with_capacity(elements.len());

        for element in elements {
            let value = self.sql_expr_to_logical_expr(
                element,
                schema,
                &mut PlannerContext::new(),
            )?;
            match value {
                Expr::Literal(scalar) => {
                    values.push(scalar);
                }
                _ => {
                    return not_impl_err!(
                        "Arrays with elements other than literal are not supported: {value}"
                    );
                }
            }
        }

        let data_types: HashSet<DataType> =
            values.iter().map(|e| e.data_type()).collect();

        if data_types.is_empty() {
            Ok(lit(ScalarValue::List(new_null_array(&DataType::Null, 0))))
        } else if data_types.len() > 1 {
            not_impl_err!("Arrays with different types are not supported: {data_types:?}")
        } else {
            let data_type = values[0].data_type();
            let arr = ScalarValue::new_list(&values, &data_type);
            Ok(lit(ScalarValue::List(arr)))
        }
    }

    /// Convert a SQL interval expression to a DataFusion logical plan
    /// expression
    pub(super) fn sql_interval_to_expr(
        &self,
        negative: bool,
        interval: Interval,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if interval.leading_precision.is_some() {
            return not_impl_err!(
                "Unsupported Interval Expression with leading_precision {:?}",
                interval.leading_precision
            );
        }

        if interval.last_field.is_some() {
            return not_impl_err!(
                "Unsupported Interval Expression with last_field {:?}",
                interval.last_field
            );
        }

        if interval.fractional_seconds_precision.is_some() {
            return not_impl_err!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                interval.fractional_seconds_precision
            );
        }

        // Only handle string exprs for now
        let value = match *interval.value {
            SQLExpr::Value(
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ) => {
                if negative {
                    format!("-{s}")
                } else {
                    s
                }
            }
            // Support expressions like `interval '1 month' + date/timestamp`.
            // Such expressions are parsed like this by sqlparser-rs
            //
            // Interval
            //   BinaryOp
            //     Value(StringLiteral)
            //     Cast
            //       Value(StringLiteral)
            //
            // This code rewrites them to the following:
            //
            // BinaryOp
            //   Interval
            //     Value(StringLiteral)
            //   Cast
            //      Value(StringLiteral)
            SQLExpr::BinaryOp { left, op, right } => {
                let df_op = match op {
                    BinaryOperator::Plus => Operator::Plus,
                    BinaryOperator::Minus => Operator::Minus,
                    _ => {
                        return not_impl_err!("Unsupported interval operator: {op:?}");
                    }
                };
                match (interval.leading_field, left.as_ref(), right.as_ref()) {
                    (_, _, SQLExpr::Value(_)) => {
                        let left_expr = self.sql_interval_to_expr(
                            negative,
                            Interval {
                                value: left,
                                leading_field: interval.leading_field,
                                leading_precision: None,
                                last_field: None,
                                fractional_seconds_precision: None,
                            },
                            schema,
                            planner_context,
                        )?;
                        let right_expr = self.sql_interval_to_expr(
                            false,
                            Interval {
                                value: right,
                                leading_field: interval.leading_field,
                                leading_precision: None,
                                last_field: None,
                                fractional_seconds_precision: None,
                            },
                            schema,
                            planner_context,
                        )?;
                        return Ok(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left_expr),
                            df_op,
                            Box::new(right_expr),
                        )));
                    }
                    // In this case, the left node is part of the interval
                    // expr and the right node is an independent expr.
                    //
                    // Leading field is not supported when the right operand
                    // is not a value.
                    (None, _, _) => {
                        let left_expr = self.sql_interval_to_expr(
                            negative,
                            Interval {
                                value: left,
                                leading_field: None,
                                leading_precision: None,
                                last_field: None,
                                fractional_seconds_precision: None,
                            },
                            schema,
                            planner_context,
                        )?;
                        let right_expr = self.sql_expr_to_logical_expr(
                            *right,
                            schema,
                            planner_context,
                        )?;
                        return Ok(Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left_expr),
                            df_op,
                            Box::new(right_expr),
                        )));
                    }
                    _ => {
                        let value = SQLExpr::BinaryOp { left, op, right };
                        return not_impl_err!(
                            "Unsupported interval argument. Expected string literal, got: {value:?}"
                        );
                    }
                }
            }
            _ => {
                return not_impl_err!(
                    "Unsupported interval argument. Expected string literal, got: {:?}",
                    interval.value
                );
            }
        };

        let value = if has_units(&value) {
            // If the interval already contains a unit
            // `INTERVAL '5 month' rather than `INTERVAL '5' month`
            // skip the other unit
            value
        } else {
            // leading_field really means the unit if specified
            // for example, "month" in  `INTERVAL '5' month`
            match interval.leading_field.as_ref() {
                Some(leading_field) => {
                    format!("{value} {leading_field}")
                }
                None => {
                    // default to seconds for the units
                    // `INTERVAL '5' is parsed as '5 seconds'
                    format!("{value} seconds")
                }
            }
        };

        let val = parse_interval_month_day_nano(&value)?;
        Ok(lit(ScalarValue::IntervalMonthDayNano(Some(val))))
    }
}

// TODO make interval parsing better in arrow-rs / expose `IntervalType`
fn has_units(val: &str) -> bool {
    val.ends_with("century")
        || val.ends_with("centuries")
        || val.ends_with("decade")
        || val.ends_with("decades")
        || val.ends_with("year")
        || val.ends_with("years")
        || val.ends_with("month")
        || val.ends_with("months")
        || val.ends_with("week")
        || val.ends_with("weeks")
        || val.ends_with("day")
        || val.ends_with("days")
        || val.ends_with("hour")
        || val.ends_with("hours")
        || val.ends_with("minute")
        || val.ends_with("minutes")
        || val.ends_with("second")
        || val.ends_with("seconds")
        || val.ends_with("millisecond")
        || val.ends_with("milliseconds")
        || val.ends_with("microsecond")
        || val.ends_with("microseconds")
        || val.ends_with("nanosecond")
        || val.ends_with("nanoseconds")
}

/// Try to decode bytes from hex literal string.
///
/// None will be returned if the input literal is hex-invalid.
fn try_decode_hex_literal(s: &str) -> Option<Vec<u8>> {
    let hex_bytes = s.as_bytes();

    let mut decoded_bytes = Vec::with_capacity((hex_bytes.len() + 1) / 2);

    let start_idx = hex_bytes.len() % 2;
    if start_idx > 0 {
        // The first byte is formed of only one char.
        decoded_bytes.push(try_decode_hex_char(hex_bytes[0])?);
    }

    for i in (start_idx..hex_bytes.len()).step_by(2) {
        let high = try_decode_hex_char(hex_bytes[i])?;
        let low = try_decode_hex_char(hex_bytes[i + 1])?;
        decoded_bytes.push(high << 4 | low);
    }

    Some(decoded_bytes)
}

/// Try to decode a byte from a hex char.
///
/// None will be returned if the input char is hex-invalid.
const fn try_decode_hex_char(c: u8) -> Option<u8> {
    match c {
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'0'..=b'9' => Some(c - b'0'),
        _ => None,
    }
}

/// Parse Decimal128 from a string
///
/// TODO: support parsing from scientific notation
fn parse_decimal_128(unsigned_number: &str, negative: bool) -> Result<Expr> {
    // remove leading zeroes
    let trimmed = unsigned_number.trim_start_matches('0');
    // parse precision and scale, remove decimal point if exists
    let (precision, scale, replaced_str) = if trimmed == "." {
        // special cases for numbers such as “0.”, “000.”, and so on.
        (1, 0, Cow::Borrowed("0"))
    } else if let Some(i) = trimmed.find('.') {
        (
            trimmed.len() - 1,
            trimmed.len() - i - 1,
            Cow::Owned(trimmed.replace('.', "")),
        )
    } else {
        // no decimal point, keep as is
        (trimmed.len(), 0, Cow::Borrowed(trimmed))
    };

    let number = replaced_str.parse::<i128>().map_err(|e| {
        DataFusionError::from(ParserError(format!(
            "Cannot parse {replaced_str} as i128 when building decimal: {e}"
        )))
    })?;

    // check precision overflow
    if precision as u8 > DECIMAL128_MAX_PRECISION {
        return Err(DataFusionError::from(ParserError(format!(
            "Cannot parse {replaced_str} as i128 when building decimal: precision overflow"
        ))));
    }

    Ok(Expr::Literal(ScalarValue::Decimal128(
        Some(if negative { -number } else { number }),
        precision as u8,
        scale as i8,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_hex_literal() {
        let cases = [
            ("", Some(vec![])),
            ("FF00", Some(vec![255, 0])),
            ("a00a", Some(vec![160, 10])),
            ("FF0", Some(vec![15, 240])),
            ("f", Some(vec![15])),
            ("FF0X", None),
            ("X0", None),
            ("XX", None),
            ("x", None),
        ];

        for (input, expect) in cases {
            let output = try_decode_hex_literal(input);
            assert_eq!(output, expect);
        }
    }
}
