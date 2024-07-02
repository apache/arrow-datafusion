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

//! Functions for creating logical expressions

use crate::expr::{
    AggregateFunction, BinaryExpr, Cast, Exists, GroupingSet, InList, InSubquery,
    Placeholder, TryCast, Unnest,
};
use crate::function::{
    AccumulatorArgs, AccumulatorFactoryFunction, PartitionEvaluatorFactory,
    StateFieldsArgs,
};
use crate::{
    aggregate_function, conditional_expressions::CaseBuilder, logical_plan::Subquery,
    AggregateUDF, Expr, LogicalPlan, Operator, ScalarFunctionImplementation, ScalarUDF,
    Signature, Volatility,
};
use crate::{AggregateUDFImpl, ColumnarValue, ScalarUDFImpl, WindowUDF, WindowUDFImpl};
use arrow::compute::kernels::cast_utils::{
    parse_interval_day_time, parse_interval_month_day_nano, parse_interval_year_month,
};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{Column, Result, ScalarValue};
use std::any::Any;
use std::fmt::Debug;
use std::ops::Not;
use std::sync::Arc;
use datafusion_common::logical_type::TypeRelation;

/// Create a column expression based on a qualified or unqualified column name. Will
/// normalize unquoted identifiers according to SQL rules (identifiers will become lowercase).
///
/// For example:
///
/// ```rust
/// # use datafusion_expr::col;
/// let c1 = col("a");
/// let c2 = col("A");
/// assert_eq!(c1, c2);
///
/// // note how quoting with double quotes preserves the case
/// let c3 = col(r#""A""#);
/// assert_ne!(c1, c3);
/// ```
pub fn col(ident: impl Into<Column>) -> Expr {
    Expr::Column(ident.into())
}

/// Create an out reference column which hold a reference that has been resolved to a field
/// outside of the current plan.
pub fn out_ref_col(dt: impl Into<TypeRelation>, ident: impl Into<Column>) -> Expr {
    Expr::OuterReferenceColumn(dt.into(), ident.into())
}

/// Create an unqualified column expression from the provided name, without normalizing
/// the column.
///
/// For example:
///
/// ```rust
/// # use datafusion_expr::{col, ident};
/// let c1 = ident("A"); // not normalized staying as column 'A'
/// let c2 = col("A"); // normalized via SQL rules becoming column 'a'
/// assert_ne!(c1, c2);
///
/// let c3 = col(r#""A""#);
/// assert_eq!(c1, c3);
///
/// let c4 = col("t1.a"); // parses as relation 't1' column 'a'
/// let c5 = ident("t1.a"); // parses as column 't1.a'
/// assert_ne!(c4, c5);
/// ```
pub fn ident(name: impl Into<String>) -> Expr {
    Expr::Column(Column::from_name(name))
}

/// Create placeholder value that will be filled in (such as `$1`)
///
/// Note the parameter type can be inferred using [`Expr::infer_placeholder_types`]
///
/// # Example
///
/// ```rust
/// # use datafusion_expr::{placeholder};
/// let p = placeholder("$0"); // $0, refers to parameter 1
/// assert_eq!(p.to_string(), "$0")
/// ```
pub fn placeholder(id: impl Into<String>) -> Expr {
    Expr::Placeholder(Placeholder {
        id: id.into(),
        data_type: None,
    })
}

/// Create an '*' [`Expr::Wildcard`] expression that matches all columns
///
/// # Example
///
/// ```rust
/// # use datafusion_expr::{wildcard};
/// let p = wildcard();
/// assert_eq!(p.to_string(), "*")
/// ```
pub fn wildcard() -> Expr {
    Expr::Wildcard { qualifier: None }
}

/// Return a new expression `left <op> right`
pub fn binary_expr(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

/// Return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::And,
        Box::new(right),
    ))
}

/// Return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Or,
        Box::new(right),
    ))
}

/// Return a new expression with a logical NOT
pub fn not(expr: Expr) -> Expr {
    expr.not()
}

/// Create an expression to represent the min() aggregate function
pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Min,
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::Max,
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

/// Create an expression to represent the array_agg() aggregate function
pub fn array_agg(expr: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new(
        aggregate_function::AggregateFunction::ArrayAgg,
        vec![expr],
        false,
        None,
        None,
        None,
    ))
}

/// Return a new expression with bitwise AND
pub fn bitwise_and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseAnd,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise OR
pub fn bitwise_or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseOr,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise XOR
pub fn bitwise_xor(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseXor,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise SHIFT RIGHT
pub fn bitwise_shift_right(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseShiftRight,
        Box::new(right),
    ))
}

/// Return a new expression with bitwise SHIFT LEFT
pub fn bitwise_shift_left(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::BitwiseShiftLeft,
        Box::new(right),
    ))
}

/// Create an in_list expression
pub fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList(InList::new(Box::new(expr), list, negated))
}

/// Create an EXISTS subquery expression
pub fn exists(subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::Exists(Exists {
        subquery: Subquery {
            subquery,
            outer_ref_columns,
        },
        negated: false,
    })
}

/// Create a NOT EXISTS subquery expression
pub fn not_exists(subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::Exists(Exists {
        subquery: Subquery {
            subquery,
            outer_ref_columns,
        },
        negated: true,
    })
}

/// Create an IN subquery expression
pub fn in_subquery(expr: Expr, subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::InSubquery(InSubquery::new(
        Box::new(expr),
        Subquery {
            subquery,
            outer_ref_columns,
        },
        false,
    ))
}

/// Create a NOT IN subquery expression
pub fn not_in_subquery(expr: Expr, subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::InSubquery(InSubquery::new(
        Box::new(expr),
        Subquery {
            subquery,
            outer_ref_columns,
        },
        true,
    ))
}

/// Create a scalar subquery expression
pub fn scalar_subquery(subquery: Arc<LogicalPlan>) -> Expr {
    let outer_ref_columns = subquery.all_out_ref_exprs();
    Expr::ScalarSubquery(Subquery {
        subquery,
        outer_ref_columns,
    })
}

/// Create a grouping set
pub fn grouping_set(exprs: Vec<Vec<Expr>>) -> Expr {
    Expr::GroupingSet(GroupingSet::GroupingSets(exprs))
}

/// Create a grouping set for all combination of `exprs`
pub fn cube(exprs: Vec<Expr>) -> Expr {
    Expr::GroupingSet(GroupingSet::Cube(exprs))
}

/// Create a grouping set for rollup
pub fn rollup(exprs: Vec<Expr>) -> Expr {
    Expr::GroupingSet(GroupingSet::Rollup(exprs))
}

/// Create a cast expression
pub fn cast(expr: Expr, data_type: impl Into<TypeRelation>) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), data_type.into()))
}

/// Create a try cast expression
pub fn try_cast(expr: Expr, data_type: impl Into<TypeRelation>) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(expr), data_type.into()))
}

/// Create is null expression
pub fn is_null(expr: Expr) -> Expr {
    Expr::IsNull(Box::new(expr))
}

/// Create is true expression
pub fn is_true(expr: Expr) -> Expr {
    Expr::IsTrue(Box::new(expr))
}

/// Create is not true expression
pub fn is_not_true(expr: Expr) -> Expr {
    Expr::IsNotTrue(Box::new(expr))
}

/// Create is false expression
pub fn is_false(expr: Expr) -> Expr {
    Expr::IsFalse(Box::new(expr))
}

/// Create is not false expression
pub fn is_not_false(expr: Expr) -> Expr {
    Expr::IsNotFalse(Box::new(expr))
}

/// Create is unknown expression
pub fn is_unknown(expr: Expr) -> Expr {
    Expr::IsUnknown(Box::new(expr))
}

/// Create is not unknown expression
pub fn is_not_unknown(expr: Expr) -> Expr {
    Expr::IsNotUnknown(Box::new(expr))
}

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
pub fn case(expr: Expr) -> CaseBuilder {
    CaseBuilder::new(Some(Box::new(expr)), vec![], vec![], None)
}

/// Create a CASE WHEN statement with boolean WHEN expressions and no base expression.
pub fn when(when: Expr, then: Expr) -> CaseBuilder {
    CaseBuilder::new(None, vec![when], vec![then], None)
}

/// Create a Unnest expression
pub fn unnest(expr: Expr) -> Expr {
    Expr::Unnest(Unnest {
        expr: Box::new(expr),
    })
}

/// Convenience method to create a new user defined scalar function (UDF) with a
/// specific signature and specific return type.
///
/// Note this function does not expose all available features of [`ScalarUDF`],
/// such as
///
/// * computing return types based on input types
/// * multiple [`Signature`]s
/// * aliases
///
/// See [`ScalarUDF`] for details and examples on how to use the full
/// functionality.
pub fn create_udf(
    name: &str,
    input_types: Vec<DataType>,
    return_type: Arc<DataType>,
    volatility: Volatility,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type = Arc::try_unwrap(return_type).unwrap_or_else(|t| t.as_ref().clone());
    ScalarUDF::from(SimpleScalarUDF::new(
        name,
        input_types,
        return_type,
        volatility,
        fun,
    ))
}

/// Implements [`ScalarUDFImpl`] for functions that have a single signature and
/// return type.
pub struct SimpleScalarUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
    fun: ScalarFunctionImplementation,
}

impl Debug for SimpleScalarUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl SimpleScalarUDF {
    /// Create a new `SimpleScalarUDF` from a name, input types, return type and
    /// implementation. Implementing [`ScalarUDFImpl`] allows more flexibility
    pub fn new(
        name: impl Into<String>,
        input_types: Vec<DataType>,
        return_type: DataType,
        volatility: Volatility,
        fun: ScalarFunctionImplementation,
    ) -> Self {
        let name = name.into();
        let signature = Signature::exact(input_types, volatility);
        Self {
            name,
            signature,
            return_type,
            fun,
        }
    }
}

impl ScalarUDFImpl for SimpleScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        (self.fun)(args)
    }
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Accumulator's implementation`.
pub fn create_udaf(
    name: &str,
    input_type: Vec<DataType>,
    return_type: Arc<DataType>,
    volatility: Volatility,
    accumulator: AccumulatorFactoryFunction,
    state_type: Arc<Vec<DataType>>,
) -> AggregateUDF {
    let return_type = Arc::try_unwrap(return_type).unwrap_or_else(|t| t.as_ref().clone());
    let state_type = Arc::try_unwrap(state_type).unwrap_or_else(|t| t.as_ref().clone());
    let state_fields = state_type
        .into_iter()
        .enumerate()
        .map(|(i, t)| Field::new(format!("{i}"), t, true))
        .collect::<Vec<_>>();
    AggregateUDF::from(SimpleAggregateUDF::new(
        name,
        input_type,
        return_type,
        volatility,
        accumulator,
        state_fields,
    ))
}

/// Implements [`AggregateUDFImpl`] for functions that have a single signature and
/// return type.
pub struct SimpleAggregateUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
    accumulator: AccumulatorFactoryFunction,
    state_fields: Vec<Field>,
}

impl Debug for SimpleAggregateUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl SimpleAggregateUDF {
    /// Create a new `AggregateUDFImpl` from a name, input types, return type, state type and
    /// implementation. Implementing [`AggregateUDFImpl`] allows more flexibility
    pub fn new(
        name: impl Into<String>,
        input_type: Vec<DataType>,
        return_type: DataType,
        volatility: Volatility,
        accumulator: AccumulatorFactoryFunction,
        state_fields: Vec<Field>,
    ) -> Self {
        let name = name.into();
        let signature = Signature::exact(input_type, volatility);
        Self {
            name,
            signature,
            return_type,
            accumulator,
            state_fields,
        }
    }

    pub fn new_with_signature(
        name: impl Into<String>,
        signature: Signature,
        return_type: DataType,
        accumulator: AccumulatorFactoryFunction,
        state_fields: Vec<Field>,
    ) -> Self {
        let name = name.into();
        Self {
            name,
            signature,
            return_type,
            accumulator,
            state_fields,
        }
    }
}

impl AggregateUDFImpl for SimpleAggregateUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn crate::Accumulator>> {
        (self.accumulator)(acc_args)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(self.state_fields.clone())
    }
}

/// Creates a new UDWF with a specific signature, state type and return type.
///
/// The signature and state type must match the [`PartitionEvaluator`]'s implementation`.
///
/// [`PartitionEvaluator`]: crate::PartitionEvaluator
pub fn create_udwf(
    name: &str,
    input_type: DataType,
    return_type: Arc<DataType>,
    volatility: Volatility,
    partition_evaluator_factory: PartitionEvaluatorFactory,
) -> WindowUDF {
    let return_type = Arc::try_unwrap(return_type).unwrap_or_else(|t| t.as_ref().clone());
    WindowUDF::from(SimpleWindowUDF::new(
        name,
        input_type,
        return_type,
        volatility,
        partition_evaluator_factory,
    ))
}

/// Implements [`WindowUDFImpl`] for functions that have a single signature and
/// return type.
pub struct SimpleWindowUDF {
    name: String,
    signature: Signature,
    return_type: DataType,
    partition_evaluator_factory: PartitionEvaluatorFactory,
}

impl Debug for SimpleWindowUDF {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("WindowUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("return_type", &"<func>")
            .field("partition_evaluator_factory", &"<FUNC>")
            .finish()
    }
}

impl SimpleWindowUDF {
    /// Create a new `SimpleWindowUDF` from a name, input types, return type and
    /// implementation. Implementing [`WindowUDFImpl`] allows more flexibility
    pub fn new(
        name: impl Into<String>,
        input_type: DataType,
        return_type: DataType,
        volatility: Volatility,
        partition_evaluator_factory: PartitionEvaluatorFactory,
    ) -> Self {
        let name = name.into();
        let signature = Signature::exact([input_type].to_vec(), volatility);
        Self {
            name,
            signature,
            return_type,
            partition_evaluator_factory,
        }
    }
}

impl WindowUDFImpl for SimpleWindowUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn partition_evaluator(&self) -> Result<Box<dyn crate::PartitionEvaluator>> {
        (self.partition_evaluator_factory)()
    }
}

pub fn interval_year_month_lit(value: &str) -> Expr {
    let interval = parse_interval_year_month(value).ok();
    Expr::Literal(ScalarValue::IntervalYearMonth(interval))
}

pub fn interval_datetime_lit(value: &str) -> Expr {
    let interval = parse_interval_day_time(value).ok();
    Expr::Literal(ScalarValue::IntervalDayTime(interval))
}

pub fn interval_month_day_nano_lit(value: &str) -> Expr {
    let interval = parse_interval_month_day_nano(value).ok();
    Expr::Literal(ScalarValue::IntervalMonthDayNano(interval))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn filter_is_null_and_is_not_null() {
        let col_null = col("col1");
        let col_not_null = ident("col2");
        assert_eq!(format!("{}", col_null.is_null()), "col1 IS NULL");
        assert_eq!(
            format!("{}", col_not_null.is_not_null()),
            "col2 IS NOT NULL"
        );
    }
}
