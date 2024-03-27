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

//! Expression simplification API

use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::Not;

use super::inlist_simplifier::ShortenInListSimplifier;
use super::utils::*;
use crate::analyzer::type_coercion::TypeCoercionRewriter;
use crate::simplify_expressions::guarantees::GuaranteeRewriter;
use crate::simplify_expressions::regex::simplify_regex_expr;
use crate::simplify_expressions::SimplifyInfo;

use arrow::{
    array::{new_null_array, AsArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{
    cast::{as_large_list_array, as_list_array},
    tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter},
};
use datafusion_common::{
    internal_err, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{InList, InSubquery};
use datafusion_expr::simplify::ExprSimplifyResult;
use datafusion_expr::{
    and, lit, or, BinaryExpr, BuiltinScalarFunction, Case, ColumnarValue, Expr, Like,
    Operator, ScalarFunctionDefinition, Volatility,
};
use datafusion_expr::{expr::ScalarFunction, interval_arithmetic::NullableInterval};
use datafusion_physical_expr::{create_physical_expr, execution_props::ExecutionProps};

/// This structure handles API for expression simplification
///
/// Provides simplification information based on DFSchema and
/// [`ExecutionProps`]. This is the default implementation used by DataFusion
///
/// For example:
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use datafusion_expr::{col, lit};
/// use datafusion_common::{DataFusionError, ToDFSchema};
/// use datafusion_expr::execution_props::ExecutionProps;
/// use datafusion_expr::simplify::SimplifyContext;
/// use datafusion_optimizer::simplify_expressions::ExprSimplifier;
///
/// // Create the schema
/// let schema = Schema::new(vec![
///     Field::new("i", DataType::Int64, false),
///   ])
///   .to_dfschema_ref().unwrap();
///
/// // Create the simplifier
/// let props = ExecutionProps::new();
/// let context = SimplifyContext::new(&props)
///    .with_schema(schema);
/// let simplifier = ExprSimplifier::new(context);
///
/// // Use the simplifier
///
/// // b < 2 or (1 > 3)
/// let expr = col("b").lt(lit(2)).or(lit(1).gt(lit(3)));
///
/// // b < 2
/// let simplified = simplifier.simplify(expr).unwrap();
/// assert_eq!(simplified, col("b").lt(lit(2)));
/// ```
pub struct ExprSimplifier<S> {
    info: S,
    /// Guarantees about the values of columns. This is provided by the user
    /// in [ExprSimplifier::with_guarantees()].
    guarantees: Vec<(Expr, NullableInterval)>,
    /// Should expressions be canonicalized before simplification? Defaults to
    /// true
    canonicalize: bool,
}

pub const THRESHOLD_INLINE_INLIST: usize = 3;

impl<S: SimplifyInfo> ExprSimplifier<S> {
    /// Create a new `ExprSimplifier` with the given `info` such as an
    /// instance of [`SimplifyContext`]. See
    /// [`simplify`](Self::simplify) for an example.
    ///
    /// [`SimplifyContext`]: datafusion_expr::simplify::SimplifyContext
    pub fn new(info: S) -> Self {
        Self {
            info,
            guarantees: vec![],
            canonicalize: true,
        }
    }

    /// Simplifies this [`Expr`]`s as much as possible, evaluating
    /// constants and applying algebraic simplifications.
    ///
    /// The types of the expression must match what operators expect,
    /// or else an error may occur trying to evaluate. See
    /// [`coerce`](Self::coerce) for a function to help.
    ///
    /// # Example:
    ///
    /// `b > 2 AND b > 2`
    ///
    /// can be written to
    ///
    /// `b > 2`
    ///
    /// ```
    /// use arrow::datatypes::DataType;
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_common::Result;
    /// use datafusion_expr::execution_props::ExecutionProps;
    /// use datafusion_expr::simplify::SimplifyContext;
    /// use datafusion_expr::simplify::SimplifyInfo;
    /// use datafusion_optimizer::simplify_expressions::ExprSimplifier;
    /// use datafusion_common::DFSchema;
    /// use std::sync::Arc;
    ///
    /// /// Simple implementation that provides `Simplifier` the information it needs
    /// /// See SimplifyContext for a structure that does this.
    /// #[derive(Default)]
    /// struct Info {
    ///   execution_props: ExecutionProps,
    /// };
    ///
    /// impl SimplifyInfo for Info {
    ///   fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(false)
    ///   }
    ///   fn nullable(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(true)
    ///   }
    ///   fn execution_props(&self) -> &ExecutionProps {
    ///     &self.execution_props
    ///   }
    ///   fn get_data_type(&self, expr: &Expr) -> Result<DataType> {
    ///     Ok(DataType::Int32)
    ///   }
    /// }
    ///
    /// // Create the simplifier
    /// let simplifier = ExprSimplifier::new(Info::default());
    ///
    /// // b < 2
    /// let b_lt_2 = col("b").gt(lit(2));
    ///
    /// // (b < 2) OR (b < 2)
    /// let expr = b_lt_2.clone().or(b_lt_2.clone());
    ///
    /// // (b < 2) OR (b < 2) --> (b < 2)
    /// let expr = simplifier.simplify(expr).unwrap();
    /// assert_eq!(expr, b_lt_2);
    /// ```
    pub fn simplify(&self, mut expr: Expr) -> Result<Expr> {
        let mut simplifier = Simplifier::new(&self.info);
        let mut const_evaluator = ConstEvaluator::try_new(self.info.execution_props())?;
        let mut shorten_in_list_simplifier = ShortenInListSimplifier::new();
        let mut guarantee_rewriter = GuaranteeRewriter::new(&self.guarantees);

        if self.canonicalize {
            expr = expr.rewrite(&mut Canonicalizer::new()).data()?
        }

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        expr.rewrite(&mut const_evaluator)
            .data()?
            .rewrite(&mut simplifier)
            .data()?
            .rewrite(&mut guarantee_rewriter)
            .data()?
            // run both passes twice to try an minimize simplifications that we missed
            .rewrite(&mut const_evaluator)
            .data()?
            .rewrite(&mut simplifier)
            .data()?
            // shorten inlist should be started after other inlist rules are applied
            .rewrite(&mut shorten_in_list_simplifier)
            .data()
    }

    /// Apply type coercion to an [`Expr`] so that it can be
    /// evaluated as a [`PhysicalExpr`](datafusion_physical_expr::PhysicalExpr).
    ///
    /// See the [type coercion module](datafusion_expr::type_coercion)
    /// documentation for more details on type coercion
    ///
    // Would be nice if this API could use the SimplifyInfo
    // rather than creating an DFSchemaRef coerces rather than doing
    // it manually.
    // https://github.com/apache/arrow-datafusion/issues/3793
    pub fn coerce(&self, expr: Expr, schema: DFSchemaRef) -> Result<Expr> {
        let mut expr_rewrite = TypeCoercionRewriter { schema };

        expr.rewrite(&mut expr_rewrite).data()
    }

    /// Input guarantees about the values of columns.
    ///
    /// The guarantees can simplify expressions. For example, if a column `x` is
    /// guaranteed to be `3`, then the expression `x > 1` can be replaced by the
    /// literal `true`.
    ///
    /// The guarantees are provided as a `Vec<(Expr, NullableInterval)>`,
    /// where the [Expr] is a column reference and the [NullableInterval]
    /// is an interval representing the known possible values of that column.
    ///
    /// ```rust
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_expr::interval_arithmetic::{Interval, NullableInterval};
    /// use datafusion_common::{Result, ScalarValue, ToDFSchema};
    /// use datafusion_expr::execution_props::ExecutionProps;
    /// use datafusion_expr::simplify::SimplifyContext;
    /// use datafusion_optimizer::simplify_expressions::ExprSimplifier;
    ///
    /// let schema = Schema::new(vec![
    ///   Field::new("x", DataType::Int64, false),
    ///   Field::new("y", DataType::UInt32, false),
    ///   Field::new("z", DataType::Int64, false),
    ///   ])
    ///   .to_dfschema_ref().unwrap();
    ///
    /// // Create the simplifier
    /// let props = ExecutionProps::new();
    /// let context = SimplifyContext::new(&props)
    ///    .with_schema(schema);
    ///
    /// // Expression: (x >= 3) AND (y + 2 < 10) AND (z > 5)
    /// let expr_x = col("x").gt_eq(lit(3_i64));
    /// let expr_y = (col("y") + lit(2_u32)).lt(lit(10_u32));
    /// let expr_z = col("z").gt(lit(5_i64));
    /// let expr = expr_x.and(expr_y).and(expr_z.clone());
    ///
    /// let guarantees = vec![
    ///    // x ∈ [3, 5]
    ///    (
    ///        col("x"),
    ///        NullableInterval::NotNull {
    ///            values: Interval::make(Some(3_i64), Some(5_i64)).unwrap()
    ///        }
    ///    ),
    ///    // y = 3
    ///    (col("y"), NullableInterval::from(ScalarValue::UInt32(Some(3)))),
    /// ];
    /// let simplifier = ExprSimplifier::new(context).with_guarantees(guarantees);
    /// let output = simplifier.simplify(expr).unwrap();
    /// // Expression becomes: true AND true AND (z > 5), which simplifies to
    /// // z > 5.
    /// assert_eq!(output, expr_z);
    /// ```
    pub fn with_guarantees(mut self, guarantees: Vec<(Expr, NullableInterval)>) -> Self {
        self.guarantees = guarantees;
        self
    }

    /// Should [`Canonicalizer`] be applied before simplification?
    ///
    /// If true (the default), the expression will be rewritten to canonical
    /// form before simplification. This is useful to ensure that the simplifier
    /// can apply all possible simplifications.
    ///
    /// Some expressions, such as those in some Joins, can not be canonicalized
    /// without changing their meaning. In these cases, canonicalization should
    /// be disabled.
    ///
    /// ```rust
    /// use arrow::datatypes::{DataType, Field, Schema};
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_expr::interval_arithmetic::{Interval, NullableInterval};
    /// use datafusion_common::{Result, ScalarValue, ToDFSchema};
    /// use datafusion_expr::execution_props::ExecutionProps;
    /// use datafusion_expr::simplify::SimplifyContext;
    /// use datafusion_optimizer::simplify_expressions::ExprSimplifier;
    ///
    /// let schema = Schema::new(vec![
    ///   Field::new("a", DataType::Int64, false),
    ///   Field::new("b", DataType::Int64, false),
    ///   Field::new("c", DataType::Int64, false),
    ///   ])
    ///   .to_dfschema_ref().unwrap();
    ///
    /// // Create the simplifier
    /// let props = ExecutionProps::new();
    /// let context = SimplifyContext::new(&props)
    ///    .with_schema(schema);
    /// let simplifier = ExprSimplifier::new(context);
    ///
    /// // Expression: a = c AND 1 = b
    /// let expr = col("a").eq(col("c")).and(lit(1).eq(col("b")));
    ///
    /// // With canonicalization, the expression is rewritten to canonical form
    /// // (though it is no simpler in this case):
    /// let canonical = simplifier.simplify(expr.clone()).unwrap();
    /// // Expression has been rewritten to: (c = a AND b = 1)
    /// assert_eq!(canonical, col("c").eq(col("a")).and(col("b").eq(lit(1))));
    ///
    /// // If canonicalization is disabled, the expression is not changed
    /// let non_canonicalized = simplifier
    ///   .with_canonicalize(false)
    ///   .simplify(expr.clone())
    ///   .unwrap();
    ///
    /// assert_eq!(non_canonicalized, expr);
    /// ```
    pub fn with_canonicalize(mut self, canonicalize: bool) -> Self {
        self.canonicalize = canonicalize;
        self
    }
}

/// Canonicalize any BinaryExprs that are not in canonical form
///
/// `<literal> <op> <col>` is rewritten to `<col> <op> <literal>`
///
/// `<col1> <op> <col2>` is rewritten so that the name of `col1` sorts higher
/// than `col2` (`a > b` would be canonicalized to `b < a`)
struct Canonicalizer {}

impl Canonicalizer {
    fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for Canonicalizer {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return Ok(Transformed::no(expr));
        };
        match (left.as_ref(), right.as_ref(), op.swap()) {
            // <col1> <op> <col2>
            (Expr::Column(left_col), Expr::Column(right_col), Some(swapped_op))
                if right_col > left_col =>
            {
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left: right,
                    op: swapped_op,
                    right: left,
                })))
            }
            // <literal> <op> <col>
            (Expr::Literal(_a), Expr::Column(_b), Some(swapped_op)) => {
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left: right,
                    op: swapped_op,
                    right: left,
                })))
            }
            _ => Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                left,
                op,
                right,
            }))),
        }
    }
}

#[allow(rustdoc::private_intra_doc_links)]
/// Partially evaluate `Expr`s so constant subtrees are evaluated at plan time.
///
/// Note it does not handle algebraic rewrites such as `(a or false)`
/// --> `a`, which is handled by [`Simplifier`]
struct ConstEvaluator<'a> {
    /// `can_evaluate` is used during the depth-first-search of the
    /// `Expr` tree to track if any siblings (or their descendants) were
    /// non evaluatable (e.g. had a column reference or volatile
    /// function)
    ///
    /// Specifically, `can_evaluate[N]` represents the state of
    /// traversal when we are N levels deep in the tree, one entry for
    /// this Expr and each of its parents.
    ///
    /// After visiting all siblings if `can_evaluate.top()` is true, that
    /// means there were no non evaluatable siblings (or their
    /// descendants) so this `Expr` can be evaluated
    can_evaluate: Vec<bool>,

    execution_props: &'a ExecutionProps,
    input_schema: DFSchema,
    input_batch: RecordBatch,
}

#[allow(dead_code)]
/// The simplify result of ConstEvaluator
enum ConstSimplifyResult {
    // Expr was simplifed and contains the new expression
    Simplified(ScalarValue),
    // Evaluation encountered an error, contains the original expression
    SimplifyRuntimeError(DataFusionError, Expr),
}

impl<'a> TreeNodeRewriter for ConstEvaluator<'a> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // Default to being able to evaluate this node
        self.can_evaluate.push(true);

        // if this expr is not ok to evaluate, mark entire parent
        // stack as not ok (as all parents have at least one child or
        // descendant that can not be evaluated

        if !Self::can_evaluate(&expr) {
            // walk back up stack, marking first parent that is not mutable
            let parent_iter = self.can_evaluate.iter_mut().rev();
            for p in parent_iter {
                if !*p {
                    // optimization: if we find an element on the
                    // stack already marked, know all elements above are also marked
                    break;
                }
                *p = false;
            }
        }

        // NB: do not short circuit recursion even if we find a non
        // evaluatable node (so we can fold other children, args to
        // functions, etc)
        Ok(Transformed::no(expr))
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match self.can_evaluate.pop() {
            // Certain expressions such as `CASE` and `COALESCE` are short circuiting
            // and may not evalute all their sub expressions. Thus if
            // if any error is countered during simplification, return the original
            // so that normal evaluation can occur
            Some(true) => {
                let result = self.evaluate_to_scalar(expr);
                match result {
                    ConstSimplifyResult::Simplified(s) => {
                        Ok(Transformed::yes(Expr::Literal(s)))
                    }
                    ConstSimplifyResult::SimplifyRuntimeError(_, expr) => {
                        Ok(Transformed::yes(expr))
                    }
                }
            }
            Some(false) => Ok(Transformed::no(expr)),
            _ => internal_err!("Failed to pop can_evaluate"),
        }
    }
}

impl<'a> ConstEvaluator<'a> {
    /// Create a new `ConstantEvaluator`. Session constants (such as
    /// the time for `now()` are taken from the passed
    /// `execution_props`.
    pub fn try_new(execution_props: &'a ExecutionProps) -> Result<Self> {
        // The dummy column name is unused and doesn't matter as only
        // expressions without column references can be evaluated
        static DUMMY_COL_NAME: &str = ".";
        let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);
        let input_schema = DFSchema::try_from(schema.clone())?;
        // Need a single "input" row to produce a single output row
        let col = new_null_array(&DataType::Null, 1);
        let input_batch = RecordBatch::try_new(std::sync::Arc::new(schema), vec![col])?;

        Ok(Self {
            can_evaluate: vec![],
            execution_props,
            input_schema,
            input_batch,
        })
    }

    /// Can a function of the specified volatility be evaluated?
    fn volatility_ok(volatility: Volatility) -> bool {
        match volatility {
            Volatility::Immutable => true,
            // Values for functions such as now() are taken from ExecutionProps
            Volatility::Stable => true,
            Volatility::Volatile => false,
        }
    }

    /// Can the expression be evaluated at plan time, (assuming all of
    /// its children can also be evaluated)?
    fn can_evaluate(expr: &Expr) -> bool {
        // check for reasons we can't evaluate this node
        //
        // NOTE all expr types are listed here so when new ones are
        // added they can be checked for their ability to be evaluated
        // at plan time
        match expr {
            // Has no runtime cost, but needed during planning
            Expr::Alias(..)
            | Expr::AggregateFunction { .. }
            | Expr::ScalarVariable(_, _)
            | Expr::Column(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::WindowFunction { .. }
            | Expr::Sort { .. }
            | Expr::GroupingSet(_)
            | Expr::Wildcard { .. }
            | Expr::Placeholder(_) => false,
            Expr::ScalarFunction(ScalarFunction { func_def, .. }) => match func_def {
                ScalarFunctionDefinition::BuiltIn(fun) => {
                    Self::volatility_ok(fun.volatility())
                }
                ScalarFunctionDefinition::UDF(fun) => {
                    Self::volatility_ok(fun.signature().volatility)
                }
                ScalarFunctionDefinition::Name(_) => false,
            },
            Expr::Literal(_)
            | Expr::Unnest(_)
            | Expr::BinaryExpr { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::SimilarTo { .. }
            | Expr::Case(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::InList { .. }
            | Expr::GetIndexedField { .. } => true,
        }
    }

    /// Internal helper to evaluates an Expr
    pub(crate) fn evaluate_to_scalar(&mut self, expr: Expr) -> ConstSimplifyResult {
        if let Expr::Literal(s) = expr {
            return ConstSimplifyResult::Simplified(s);
        }

        let phys_expr =
            match create_physical_expr(&expr, &self.input_schema, self.execution_props) {
                Ok(e) => e,
                Err(err) => return ConstSimplifyResult::SimplifyRuntimeError(err, expr),
            };
        let col_val = match phys_expr.evaluate(&self.input_batch) {
            Ok(v) => v,
            Err(err) => return ConstSimplifyResult::SimplifyRuntimeError(err, expr),
        };
        match col_val {
            ColumnarValue::Array(a) => {
                if a.len() != 1 {
                    ConstSimplifyResult::SimplifyRuntimeError(
                        DataFusionError::Execution(format!("Could not evaluate the expression, found a result of length {}", a.len())),
                        expr,
                    )
                } else if as_list_array(&a).is_ok() {
                    ConstSimplifyResult::Simplified(ScalarValue::List(
                        a.as_list::<i32>().to_owned().into(),
                    ))
                } else if as_large_list_array(&a).is_ok() {
                    ConstSimplifyResult::Simplified(ScalarValue::LargeList(
                        a.as_list::<i64>().to_owned().into(),
                    ))
                } else {
                    // Non-ListArray
                    match ScalarValue::try_from_array(&a, 0) {
                        Ok(s) => ConstSimplifyResult::Simplified(s),
                        Err(err) => ConstSimplifyResult::SimplifyRuntimeError(err, expr),
                    }
                }
            }
            ColumnarValue::Scalar(s) => ConstSimplifyResult::Simplified(s),
        }
    }
}

/// Simplifies [`Expr`]s by applying algebraic transformation rules
///
/// Example transformations that are applied:
/// * `expr = true` and `expr != false` to `expr` when `expr` is of boolean type
/// * `expr = false` and `expr != true` to `!expr` when `expr` is of boolean type
/// * `true = true` and `false = false` to `true`
/// * `false = true` and `true = false` to `false`
/// * `!!expr` to `expr`
/// * `expr = null` and `expr != null` to `null`
struct Simplifier<'a, S> {
    info: &'a S,
}

impl<'a, S> Simplifier<'a, S> {
    pub fn new(info: &'a S) -> Self {
        Self { info }
    }
}

impl<'a, S: SimplifyInfo> TreeNodeRewriter for Simplifier<'a, S> {
    type Node = Expr;

    /// rewrite the expression simplifying any constant expressions
    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        use datafusion_expr::Operator::{
            And, BitwiseAnd, BitwiseOr, BitwiseShiftLeft, BitwiseShiftRight, BitwiseXor,
            Divide, Eq, Modulo, Multiply, NotEq, Or, RegexIMatch, RegexMatch,
            RegexNotIMatch, RegexNotMatch,
        };

        let info = self.info;
        Ok(match expr {
            //
            // Rules for Eq
            //

            // true = A  --> A
            // false = A --> !A
            // null = A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Eq,
                right,
            }) if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                Transformed::yes(match as_bool_lit(*left)? {
                    Some(true) => *right,
                    Some(false) => Expr::Not(right),
                    None => lit_bool_null(),
                })
            }
            // A = true  --> A
            // A = false --> !A
            // A = null --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Eq,
                right,
            }) if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                Transformed::yes(match as_bool_lit(*right)? {
                    Some(true) => *left,
                    Some(false) => Expr::Not(left),
                    None => lit_bool_null(),
                })
            }
            // Rules for NotEq
            //

            // true != A  --> !A
            // false != A --> A
            // null != A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: NotEq,
                right,
            }) if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                Transformed::yes(match as_bool_lit(*left)? {
                    Some(true) => Expr::Not(right),
                    Some(false) => *right,
                    None => lit_bool_null(),
                })
            }
            // A != true  --> !A
            // A != false --> A
            // A != null --> null,
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: NotEq,
                right,
            }) if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                Transformed::yes(match as_bool_lit(*right)? {
                    Some(true) => Expr::Not(left),
                    Some(false) => *left,
                    None => lit_bool_null(),
                })
            }

            //
            // Rules for OR
            //

            // true OR A --> true (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right: _,
            }) if is_true(&left) => Transformed::yes(*left),
            // false OR A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_false(&left) => Transformed::yes(*right),
            // A OR true --> true (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Or,
                right,
            }) if is_true(&right) => Transformed::yes(*right),
            // A OR false --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_false(&right) => Transformed::yes(*left),
            // A OR !A ---> true (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_not_of(&right, &left) && !info.nullable(&left)? => {
                Transformed::yes(Expr::Literal(ScalarValue::Boolean(Some(true))))
            }
            // !A OR A ---> true (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if is_not_of(&left, &right) && !info.nullable(&right)? => {
                Transformed::yes(Expr::Literal(ScalarValue::Boolean(Some(true))))
            }
            // (..A..) OR A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if expr_contains(&left, &right, Or) => Transformed::yes(*left),
            // A OR (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if expr_contains(&right, &left, Or) => Transformed::yes(*right),
            // A OR (A AND B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if !info.nullable(&right)? && is_op_with(And, &right, &left) => {
                Transformed::yes(*left)
            }
            // (A AND B) OR A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Or,
                right,
            }) if !info.nullable(&left)? && is_op_with(And, &left, &right) => {
                Transformed::yes(*right)
            }

            //
            // Rules for AND
            //

            // true AND A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_true(&left) => Transformed::yes(*right),
            // false AND A --> false (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right: _,
            }) if is_false(&left) => Transformed::yes(*left),
            // A AND true --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_true(&right) => Transformed::yes(*left),
            // A AND false --> false (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: And,
                right,
            }) if is_false(&right) => Transformed::yes(*right),
            // A AND !A ---> false (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_not_of(&right, &left) && !info.nullable(&left)? => {
                Transformed::yes(Expr::Literal(ScalarValue::Boolean(Some(false))))
            }
            // !A AND A ---> false (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if is_not_of(&left, &right) && !info.nullable(&right)? => {
                Transformed::yes(Expr::Literal(ScalarValue::Boolean(Some(false))))
            }
            // (..A..) AND A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if expr_contains(&left, &right, And) => Transformed::yes(*left),
            // A AND (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if expr_contains(&right, &left, And) => Transformed::yes(*right),
            // A AND (A OR B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if !info.nullable(&right)? && is_op_with(Or, &right, &left) => {
                Transformed::yes(*left)
            }
            // (A OR B) AND A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: And,
                right,
            }) if !info.nullable(&left)? && is_op_with(Or, &left, &right) => {
                Transformed::yes(*right)
            }

            //
            // Rules for Multiply
            //

            // A * 1 --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if is_one(&right) => Transformed::yes(*left),
            // 1 * A --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if is_one(&left) => Transformed::yes(*right),
            // A * null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Multiply,
                right,
            }) if is_null(&right) => Transformed::yes(*right),
            // null * A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),

            // A * 0 --> 0 (if A is not null and not floating, since NAN * 0 -> NAN)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if !info.nullable(&left)?
                && !info.get_data_type(&left)?.is_floating()
                && is_zero(&right) =>
            {
                Transformed::yes(*right)
            }
            // 0 * A --> 0 (if A is not null and not floating, since 0 * NAN -> NAN)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Multiply,
                right,
            }) if !info.nullable(&right)?
                && !info.get_data_type(&right)?.is_floating()
                && is_zero(&left) =>
            {
                Transformed::yes(*left)
            }

            //
            // Rules for Divide
            //

            // A / 1 --> A
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right,
            }) if is_one(&right) => Transformed::yes(*left),
            // null / A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Divide,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),
            // A / null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Divide,
                right,
            }) if is_null(&right) => Transformed::yes(*right),

            //
            // Rules for Modulo
            //

            // A % null --> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: Modulo,
                right,
            }) if is_null(&right) => Transformed::yes(*right),
            // null % A --> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),
            // A % 1 --> 0 (if A is not nullable and not floating, since NAN % 1 --> NAN)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Modulo,
                right,
            }) if !info.nullable(&left)?
                && !info.get_data_type(&left)?.is_floating()
                && is_one(&right) =>
            {
                Transformed::yes(lit(0))
            }

            //
            // Rules for BitwiseAnd
            //

            // A & null -> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: BitwiseAnd,
                right,
            }) if is_null(&right) => Transformed::yes(*right),

            // null & A -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),

            // A & 0 -> 0 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => Transformed::yes(*right),

            // 0 & A -> 0 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if !info.nullable(&right)? && is_zero(&left) => Transformed::yes(*left),

            // !A & A -> 0 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if is_negative_of(&left, &right) && !info.nullable(&right)? => {
                Transformed::yes(Expr::Literal(ScalarValue::new_zero(
                    &info.get_data_type(&left)?,
                )?))
            }

            // A & !A -> 0 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if is_negative_of(&right, &left) && !info.nullable(&left)? => {
                Transformed::yes(Expr::Literal(ScalarValue::new_zero(
                    &info.get_data_type(&left)?,
                )?))
            }

            // (..A..) & A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if expr_contains(&left, &right, BitwiseAnd) => Transformed::yes(*left),

            // A & (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if expr_contains(&right, &left, BitwiseAnd) => Transformed::yes(*right),

            // A & (A | B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if !info.nullable(&right)? && is_op_with(BitwiseOr, &right, &left) => {
                Transformed::yes(*left)
            }

            // (A | B) & A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseAnd,
                right,
            }) if !info.nullable(&left)? && is_op_with(BitwiseOr, &left, &right) => {
                Transformed::yes(*right)
            }

            //
            // Rules for BitwiseOr
            //

            // A | null -> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: BitwiseOr,
                right,
            }) if is_null(&right) => Transformed::yes(*right),

            // null | A -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),

            // A | 0 -> A (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if is_zero(&right) => Transformed::yes(*left),

            // 0 | A -> A (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if is_zero(&left) => Transformed::yes(*right),

            // !A | A -> -1 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if is_negative_of(&left, &right) && !info.nullable(&right)? => {
                Transformed::yes(Expr::Literal(ScalarValue::new_negative_one(
                    &info.get_data_type(&left)?,
                )?))
            }

            // A | !A -> -1 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if is_negative_of(&right, &left) && !info.nullable(&left)? => {
                Transformed::yes(Expr::Literal(ScalarValue::new_negative_one(
                    &info.get_data_type(&left)?,
                )?))
            }

            // (..A..) | A --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if expr_contains(&left, &right, BitwiseOr) => Transformed::yes(*left),

            // A | (..A..) --> (..A..)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if expr_contains(&right, &left, BitwiseOr) => Transformed::yes(*right),

            // A | (A & B) --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if !info.nullable(&right)? && is_op_with(BitwiseAnd, &right, &left) => {
                Transformed::yes(*left)
            }

            // (A & B) | A --> A (if B not null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseOr,
                right,
            }) if !info.nullable(&left)? && is_op_with(BitwiseAnd, &left, &right) => {
                Transformed::yes(*right)
            }

            //
            // Rules for BitwiseXor
            //

            // A ^ null -> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: BitwiseXor,
                right,
            }) if is_null(&right) => Transformed::yes(*right),

            // null ^ A -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),

            // A ^ 0 -> A (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right,
            }) if !info.nullable(&left)? && is_zero(&right) => Transformed::yes(*left),

            // 0 ^ A -> A (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right,
            }) if !info.nullable(&right)? && is_zero(&left) => Transformed::yes(*right),

            // !A ^ A -> -1 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right,
            }) if is_negative_of(&left, &right) && !info.nullable(&right)? => {
                Transformed::yes(Expr::Literal(ScalarValue::new_negative_one(
                    &info.get_data_type(&left)?,
                )?))
            }

            // A ^ !A -> -1 (if A not nullable)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right,
            }) if is_negative_of(&right, &left) && !info.nullable(&left)? => {
                Transformed::yes(Expr::Literal(ScalarValue::new_negative_one(
                    &info.get_data_type(&left)?,
                )?))
            }

            // (..A..) ^ A --> (the expression without A, if number of A is odd, otherwise one A)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right,
            }) if expr_contains(&left, &right, BitwiseXor) => {
                let expr = delete_xor_in_complex_expr(&left, &right, false);
                Transformed::yes(if expr == *right {
                    Expr::Literal(ScalarValue::new_zero(&info.get_data_type(&right)?)?)
                } else {
                    expr
                })
            }

            // A ^ (..A..) --> (the expression without A, if number of A is odd, otherwise one A)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseXor,
                right,
            }) if expr_contains(&right, &left, BitwiseXor) => {
                let expr = delete_xor_in_complex_expr(&right, &left, true);
                Transformed::yes(if expr == *left {
                    Expr::Literal(ScalarValue::new_zero(&info.get_data_type(&left)?)?)
                } else {
                    expr
                })
            }

            //
            // Rules for BitwiseShiftRight
            //

            // A >> null -> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: BitwiseShiftRight,
                right,
            }) if is_null(&right) => Transformed::yes(*right),

            // null >> A -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseShiftRight,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),

            // A >> 0 -> A (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseShiftRight,
                right,
            }) if is_zero(&right) => Transformed::yes(*left),

            //
            // Rules for BitwiseShiftRight
            //

            // A << null -> null
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op: BitwiseShiftLeft,
                right,
            }) if is_null(&right) => Transformed::yes(*right),

            // null << A -> null
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseShiftLeft,
                right: _,
            }) if is_null(&left) => Transformed::yes(*left),

            // A << 0 -> A (even if A is null)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: BitwiseShiftLeft,
                right,
            }) if is_zero(&right) => Transformed::yes(*left),

            //
            // Rules for Not
            //
            Expr::Not(inner) => Transformed::yes(negate_clause(*inner)),

            //
            // Rules for Negative
            //
            Expr::Negative(inner) => Transformed::yes(distribute_negation(*inner)),

            //
            // Rules for Case
            //

            // CASE
            //   WHEN X THEN A
            //   WHEN Y THEN B
            //   ...
            //   ELSE Q
            // END
            //
            // ---> (X AND A) OR (Y AND B AND NOT X) OR ... (NOT (X OR Y) AND Q)
            //
            // Note: the rationale for this rewrite is that the expr can then be further
            // simplified using the existing rules for AND/OR
            Expr::Case(Case {
                expr: None,
                when_then_expr,
                else_expr,
            }) if !when_then_expr.is_empty()
                && when_then_expr.len() < 3 // The rewrite is O(n!) so limit to small number
                && info.is_boolean_type(&when_then_expr[0].1)? =>
            {
                // The disjunction of all the when predicates encountered so far
                let mut filter_expr = lit(false);
                // The disjunction of all the cases
                let mut out_expr = lit(false);

                for (when, then) in when_then_expr {
                    let case_expr = when
                        .as_ref()
                        .clone()
                        .and(filter_expr.clone().not())
                        .and(*then);

                    out_expr = out_expr.or(case_expr);
                    filter_expr = filter_expr.or(*when);
                }

                if let Some(else_expr) = else_expr {
                    let case_expr = filter_expr.not().and(*else_expr);
                    out_expr = out_expr.or(case_expr);
                }

                // Do a first pass at simplification
                out_expr.rewrite(self)?
            }

            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(udf),
                args,
            }) => match udf.simplify(args, info)? {
                ExprSimplifyResult::Original(args) => {
                    Transformed::no(Expr::ScalarFunction(ScalarFunction {
                        func_def: ScalarFunctionDefinition::UDF(udf),
                        args,
                    }))
                }
                ExprSimplifyResult::Simplified(expr) => Transformed::yes(expr),
            },

            // log
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::Log),
                args,
            }) => Transformed::yes(simpl_log(args, info)?),

            // power
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::Power),
                args,
            }) => Transformed::yes(simpl_power(args, info)?),

            // concat
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::Concat),
                args,
            }) => Transformed::yes(simpl_concat(args)?),

            // concat_ws
            Expr::ScalarFunction(ScalarFunction {
                func_def:
                    ScalarFunctionDefinition::BuiltIn(
                        BuiltinScalarFunction::ConcatWithSeparator,
                    ),
                args,
            }) => match &args[..] {
                [delimiter, vals @ ..] => {
                    Transformed::yes(simpl_concat_ws(delimiter, vals)?)
                }
                _ => Transformed::yes(Expr::ScalarFunction(ScalarFunction::new(
                    BuiltinScalarFunction::ConcatWithSeparator,
                    args,
                ))),
            },

            //
            // Rules for Between
            //

            // a between 3 and 5  -->  a >= 3 AND a <=5
            // a not between 3 and 5  -->  a < 3 OR a > 5
            Expr::Between(between) => Transformed::yes(if between.negated {
                let l = *between.expr.clone();
                let r = *between.expr;
                or(l.lt(*between.low), r.gt(*between.high))
            } else {
                and(
                    between.expr.clone().gt_eq(*between.low),
                    between.expr.lt_eq(*between.high),
                )
            }),

            //
            // Rules for regexes
            //
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: op @ (RegexMatch | RegexNotMatch | RegexIMatch | RegexNotIMatch),
                right,
            }) => Transformed::yes(simplify_regex_expr(left, op, right)?),

            // Rules for Like
            Expr::Like(Like {
                expr,
                pattern,
                negated,
                escape_char: _,
                case_insensitive: _,
            }) if !is_null(&expr)
                && matches!(
                    pattern.as_ref(),
                    Expr::Literal(ScalarValue::Utf8(Some(pattern_str))) if pattern_str == "%"
                ) =>
            {
                Transformed::yes(lit(!negated))
            }

            // a is not null/unknown --> true (if a is not nullable)
            Expr::IsNotNull(expr) | Expr::IsNotUnknown(expr)
                if !info.nullable(&expr)? =>
            {
                Transformed::yes(lit(true))
            }

            // a is null/unknown --> false (if a is not nullable)
            Expr::IsNull(expr) | Expr::IsUnknown(expr) if !info.nullable(&expr)? => {
                Transformed::yes(lit(false))
            }

            // expr IN () --> false
            // expr NOT IN () --> true
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) if list.is_empty() && *expr != Expr::Literal(ScalarValue::Null) => {
                Transformed::yes(lit(negated))
            }

            // null in (x, y, z) --> null
            // null not in (x, y, z) --> null
            Expr::InList(InList {
                expr,
                list: _,
                negated: _,
            }) if is_null(expr.as_ref()) => Transformed::yes(lit_bool_null()),

            // expr IN ((subquery)) -> expr IN (subquery), see ##5529
            Expr::InList(InList {
                expr,
                mut list,
                negated,
            }) if list.len() == 1
                && matches!(list.first(), Some(Expr::ScalarSubquery { .. })) =>
            {
                let Expr::ScalarSubquery(subquery) = list.remove(0) else {
                    unreachable!()
                };

                Transformed::yes(Expr::InSubquery(InSubquery::new(
                    expr, subquery, negated,
                )))
            }

            // Combine multiple OR expressions into a single IN list expression if possible
            //
            // i.e. `a = 1 OR a = 2 OR a = 3` -> `a IN (1, 2, 3)`
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) if are_inlist_and_eq(left.as_ref(), right.as_ref()) => {
                let lhs = to_inlist(*left).unwrap();
                let rhs = to_inlist(*right).unwrap();
                let mut seen: HashSet<Expr> = HashSet::new();
                let list = lhs
                    .list
                    .into_iter()
                    .chain(rhs.list)
                    .filter(|e| seen.insert(e.to_owned()))
                    .collect::<Vec<_>>();

                let merged_inlist = InList {
                    expr: lhs.expr,
                    list,
                    negated: false,
                };

                Transformed::yes(Expr::InList(merged_inlist))
            }

            // Simplify expressions that is guaranteed to be true or false to a literal boolean expression
            //
            // Rules:
            // If both expressions are `IN` or `NOT IN`, then we can apply intersection or union on both lists
            //   Intersection:
            //     1. `a in (1,2,3) AND a in (4,5) -> a in (), which is false`
            //     2. `a in (1,2,3) AND a in (2,3,4) -> a in (2,3)`
            //     3. `a not in (1,2,3) OR a not in (3,4,5,6) -> a not in (3)`
            //   Union:
            //     4. `a not int (1,2,3) AND a not in (4,5,6) -> a not in (1,2,3,4,5,6)`
            //     # This rule is handled by `or_in_list_simplifier.rs`
            //     5. `a in (1,2,3) OR a in (4,5,6) -> a in (1,2,3,4,5,6)`
            // If one of the expressions is `IN` and another one is `NOT IN`, then we apply exception on `In` expression
            //     6. `a in (1,2,3,4) AND a not in (1,2,3,4,5) -> a in (), which is false`
            //     7. `a not in (1,2,3,4) AND a in (1,2,3,4,5) -> a = 5`
            //     8. `a in (1,2,3,4) AND a not in (5,6,7,8) -> a in (1,2,3,4)`
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) if are_inlist_and_eq_and_match_neg(
                left.as_ref(),
                right.as_ref(),
                false,
                false,
            ) =>
            {
                match (*left, *right) {
                    (Expr::InList(l1), Expr::InList(l2)) => {
                        return inlist_intersection(l1, l2, false).map(Transformed::yes);
                    }
                    // Matched previously once
                    _ => unreachable!(),
                }
            }

            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) if are_inlist_and_eq_and_match_neg(
                left.as_ref(),
                right.as_ref(),
                true,
                true,
            ) =>
            {
                match (*left, *right) {
                    (Expr::InList(l1), Expr::InList(l2)) => {
                        return inlist_union(l1, l2, true).map(Transformed::yes);
                    }
                    // Matched previously once
                    _ => unreachable!(),
                }
            }

            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) if are_inlist_and_eq_and_match_neg(
                left.as_ref(),
                right.as_ref(),
                false,
                true,
            ) =>
            {
                match (*left, *right) {
                    (Expr::InList(l1), Expr::InList(l2)) => {
                        return inlist_except(l1, l2).map(Transformed::yes);
                    }
                    // Matched previously once
                    _ => unreachable!(),
                }
            }

            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) if are_inlist_and_eq_and_match_neg(
                left.as_ref(),
                right.as_ref(),
                true,
                false,
            ) =>
            {
                match (*left, *right) {
                    (Expr::InList(l1), Expr::InList(l2)) => {
                        return inlist_except(l2, l1).map(Transformed::yes);
                    }
                    // Matched previously once
                    _ => unreachable!(),
                }
            }

            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) if are_inlist_and_eq_and_match_neg(
                left.as_ref(),
                right.as_ref(),
                true,
                true,
            ) =>
            {
                match (*left, *right) {
                    (Expr::InList(l1), Expr::InList(l2)) => {
                        return inlist_intersection(l1, l2, true).map(Transformed::yes);
                    }
                    // Matched previously once
                    _ => unreachable!(),
                }
            }

            // no additional rewrites possible
            expr => Transformed::no(expr),
        })
    }
}

// TODO: We might not need this after defer pattern for Box is stabilized. https://github.com/rust-lang/rust/issues/87121
fn are_inlist_and_eq_and_match_neg(
    left: &Expr,
    right: &Expr,
    is_left_neg: bool,
    is_right_neg: bool,
) -> bool {
    match (left, right) {
        (Expr::InList(l), Expr::InList(r)) => {
            l.expr == r.expr && l.negated == is_left_neg && r.negated == is_right_neg
        }
        _ => false,
    }
}

// TODO: We might not need this after defer pattern for Box is stabilized. https://github.com/rust-lang/rust/issues/87121
fn are_inlist_and_eq(left: &Expr, right: &Expr) -> bool {
    let left = as_inlist(left);
    let right = as_inlist(right);
    if let (Some(lhs), Some(rhs)) = (left, right) {
        matches!(lhs.expr.as_ref(), Expr::Column(_))
            && matches!(rhs.expr.as_ref(), Expr::Column(_))
            && lhs.expr == rhs.expr
            && !lhs.negated
            && !rhs.negated
    } else {
        false
    }
}

/// Try to convert an expression to an in-list expression
fn as_inlist(expr: &Expr) -> Option<Cow<InList>> {
    match expr {
        Expr::InList(inlist) => Some(Cow::Borrowed(inlist)),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(_), Expr::Literal(_)) => Some(Cow::Owned(InList {
                    expr: left.clone(),
                    list: vec![*right.clone()],
                    negated: false,
                })),
                (Expr::Literal(_), Expr::Column(_)) => Some(Cow::Owned(InList {
                    expr: right.clone(),
                    list: vec![*left.clone()],
                    negated: false,
                })),
                _ => None,
            }
        }
        _ => None,
    }
}

fn to_inlist(expr: Expr) -> Option<InList> {
    match expr {
        Expr::InList(inlist) => Some(inlist),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(_), Expr::Literal(_)) => Some(InList {
                expr: left,
                list: vec![*right],
                negated: false,
            }),
            (Expr::Literal(_), Expr::Column(_)) => Some(InList {
                expr: right,
                list: vec![*left],
                negated: false,
            }),
            _ => None,
        },
        _ => None,
    }
}

/// Return the union of two inlist expressions
/// maintaining the order of the elements in the two lists
fn inlist_union(mut l1: InList, l2: InList, negated: bool) -> Result<Expr> {
    // extend the list in l1 with the elements in l2 that are not already in l1
    let l1_items: HashSet<_> = l1.list.iter().collect();

    // keep all l2 items that do not also appear in l1
    let keep_l2: Vec<_> = l2
        .list
        .into_iter()
        .filter_map(|e| if l1_items.contains(&e) { None } else { Some(e) })
        .collect();

    l1.list.extend(keep_l2);
    l1.negated = negated;
    Ok(Expr::InList(l1))
}

/// Return the intersection of two inlist expressions
/// maintaining the order of the elements in the two lists
fn inlist_intersection(mut l1: InList, l2: InList, negated: bool) -> Result<Expr> {
    let l2_items = l2.list.iter().collect::<HashSet<_>>();

    // remove all items from l1 that are not in l2
    l1.list.retain(|e| l2_items.contains(e));

    // e in () is always false
    // e not in () is always true
    if l1.list.is_empty() {
        return Ok(lit(negated));
    }
    Ok(Expr::InList(l1))
}

/// Return the all items in l1 that are not in l2
/// maintaining the order of the elements in the two lists
fn inlist_except(mut l1: InList, l2: InList) -> Result<Expr> {
    let l2_items = l2.list.iter().collect::<HashSet<_>>();

    // keep only items from l1 that are not in l2
    l1.list.retain(|e| !l2_items.contains(e));

    if l1.list.is_empty() {
        return Ok(lit(false));
    }
    Ok(Expr::InList(l1))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        ops::{BitAnd, BitOr, BitXor},
        sync::Arc,
    };

    use super::*;
    use crate::simplify_expressions::SimplifyContext;
    use crate::test::test_table_scan_with_name;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{assert_contains, ToDFSchema};
    use datafusion_expr::{interval_arithmetic::Interval, *};
    use datafusion_physical_expr::execution_props::ExecutionProps;

    // ------------------------------
    // --- ExprSimplifier tests -----
    // ------------------------------
    #[test]
    fn api_basic() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        let expr = lit(1) + lit(2);
        let expected = lit(3);
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    #[test]
    fn basic_coercion() {
        let schema = test_schema();
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(schema.clone()));

        // Note expr type is int32 (not int64)
        // (1i64 + 2i32) < i
        let expr = (lit(1i64) + lit(2i32)).lt(col("i"));
        // should fully simplify to 3 < i (though i has been coerced to i64)
        let expected = lit(3i64).lt(col("i"));

        // Would be nice if this API could use the SimplifyInfo
        // rather than creating an DFSchemaRef coerces rather than doing
        // it manually.
        // https://github.com/apache/arrow-datafusion/issues/3793
        let expr = simplifier.coerce(expr, schema).unwrap();

        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    fn test_schema() -> DFSchemaRef {
        Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("b", DataType::Boolean, true),
        ])
        .to_dfschema_ref()
        .unwrap()
    }

    #[test]
    fn simplify_and_constant_prop() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        // should be able to simplify to false
        // (i * (1 - 2)) > 0
        let expr = (col("i") * (lit(1) - lit(1))).gt(lit(0));
        let expected = lit(false);
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    #[test]
    fn simplify_and_constant_prop_with_case() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        //   CASE
        //     WHEN i>5 AND false THEN i > 5
        //     WHEN i<5 AND true THEN i < 5
        //     ELSE false
        //   END
        //
        // Can be simplified to `i < 5`
        let expr = when(col("i").gt(lit(5)).and(lit(false)), col("i").gt(lit(5)))
            .when(col("i").lt(lit(5)).and(lit(true)), col("i").lt(lit(5)))
            .otherwise(lit(false))
            .unwrap();
        let expected = col("i").lt(lit(5));
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    // ------------------------------
    // --- Simplifier tests -----
    // ------------------------------

    #[test]
    fn test_simplify_canonicalize() {
        {
            let expr = lit(1).lt(col("c2")).and(col("c2").gt(lit(1)));
            let expected = col("c2").gt(lit(1));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1").lt(col("c2")).and(col("c2").gt(col("c1")));
            let expected = col("c2").gt(col("c1"));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1")
                .eq(lit(1))
                .and(lit(1).eq(col("c1")))
                .and(col("c1").eq(lit(3)));
            let expected = col("c1").eq(lit(1)).and(col("c1").eq(lit(3)));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1")
                .eq(col("c2"))
                .and(col("c1").gt(lit(5)))
                .and(col("c2").eq(col("c1")));
            let expected = col("c2").eq(col("c1")).and(col("c1").gt(lit(5)));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1")
                .eq(lit(1))
                .and(col("c2").gt(lit(3)).or(lit(3).lt(col("c2"))));
            let expected = col("c1").eq(lit(1)).and(col("c2").gt(lit(3)));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1").lt(lit(5)).and(col("c1").gt_eq(lit(5)));
            let expected = col("c1").lt(lit(5)).and(col("c1").gt_eq(lit(5)));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1").lt(lit(5)).and(col("c1").gt_eq(lit(5)));
            let expected = col("c1").lt(lit(5)).and(col("c1").gt_eq(lit(5)));
            assert_eq!(simplify(expr), expected);
        }
        {
            let expr = col("c1").gt(col("c2")).and(col("c1").gt(col("c2")));
            let expected = col("c2").lt(col("c1"));
            assert_eq!(simplify(expr), expected);
        }
    }

    #[test]
    fn test_simplify_or_true() {
        let expr_a = col("c2").or(lit(true));
        let expr_b = lit(true).or(col("c2"));
        let expected = lit(true);

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_or_false() {
        let expr_a = lit(false).or(col("c2"));
        let expr_b = col("c2").or(lit(false));
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_or_same() {
        let expr = col("c2").or(col("c2"));
        let expected = col("c2");

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_not_self() {
        // A OR !A if A is not nullable --> true
        // !A OR A if A is not nullable --> true
        let expr_a = col("c2_non_null").or(col("c2_non_null").not());
        let expr_b = col("c2_non_null").not().or(col("c2_non_null"));
        let expected = lit(true);

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_and_false() {
        let expr_a = lit(false).and(col("c2"));
        let expr_b = col("c2").and(lit(false));
        let expected = lit(false);

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_and_same() {
        let expr = col("c2").and(col("c2"));
        let expected = col("c2");

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_true() {
        let expr_a = lit(true).and(col("c2"));
        let expr_b = col("c2").and(lit(true));
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_and_not_self() {
        // A AND !A if A is not nullable --> false
        // !A AND A if A is not nullable --> false
        let expr_a = col("c2_non_null").and(col("c2_non_null").not());
        let expr_b = col("c2_non_null").not().and(col("c2_non_null"));
        let expected = lit(false);

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_multiply_by_one() {
        let expr_a = col("c2") * lit(1);
        let expr_b = lit(1) * col("c2");
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);

        let expr = col("c2") * lit(ScalarValue::Decimal128(Some(10000000000), 38, 10));
        assert_eq!(simplify(expr), expected);

        let expr = lit(ScalarValue::Decimal128(Some(10000000000), 31, 10)) * col("c2");
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_multiply_by_null() {
        let null = Expr::Literal(ScalarValue::Null);
        // A * null --> null
        {
            let expr = col("c2") * null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null * A --> null
        {
            let expr = null.clone() * col("c2");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_multiply_by_zero() {
        // cannot optimize A * null (null * A) if A is nullable
        {
            let expr_a = col("c2") * lit(0);
            let expr_b = lit(0) * col("c2");

            assert_eq!(simplify(expr_a.clone()), expr_a);
            assert_eq!(simplify(expr_b.clone()), expr_b);
        }
        // 0 * A --> 0 if A is not nullable
        {
            let expr = lit(0) * col("c2_non_null");
            assert_eq!(simplify(expr), lit(0));
        }
        // A * 0 --> 0 if A is not nullable
        {
            let expr = col("c2_non_null") * lit(0);
            assert_eq!(simplify(expr), lit(0));
        }
        // A * Decimal128(0) --> 0 if A is not nullable
        {
            let expr = col("c2_non_null") * lit(ScalarValue::Decimal128(Some(0), 31, 10));
            assert_eq!(
                simplify(expr),
                lit(ScalarValue::Decimal128(Some(0), 31, 10))
            );
            let expr = binary_expr(
                lit(ScalarValue::Decimal128(Some(0), 31, 10)),
                Operator::Multiply,
                col("c2_non_null"),
            );
            assert_eq!(
                simplify(expr),
                lit(ScalarValue::Decimal128(Some(0), 31, 10))
            );
        }
    }

    #[test]
    fn test_simplify_divide_by_one() {
        let expr = binary_expr(col("c2"), Operator::Divide, lit(1));
        let expected = col("c2");
        assert_eq!(simplify(expr), expected);
        let expr = col("c2") / lit(ScalarValue::Decimal128(Some(10000000000), 31, 10));
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_null() {
        // A / null --> null
        let null = lit(ScalarValue::Null);
        {
            let expr = col("c") / null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null / A --> null
        {
            let expr = null.clone() / col("c");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_divide_by_same() {
        let expr = col("c2") / col("c2");
        // if c2 is null, c2 / c2 = null, so can't simplify
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_modulo_by_null() {
        let null = lit(ScalarValue::Null);
        // A % null --> null
        {
            let expr = col("c2") % null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null % A --> null
        {
            let expr = null.clone() % col("c2");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_modulo_by_one() {
        let expr = col("c2") % lit(1);
        // if c2 is null, c2 % 1 = null, so can't simplify
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_zero_by_zero() {
        // because divide by 0 maybe occur in short-circuit expression
        // so we should not simplify this, and throw error in runtime
        let expr = lit(0) / lit(0);
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_by_zero() {
        // because divide by 0 maybe occur in short-circuit expression
        // so we should not simplify this, and throw error in runtime
        let expr = col("c2_non_null") / lit(0);
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_modulo_by_one_non_null() {
        let expr = col("c2_non_null") % lit(1);
        let expected = lit(0);
        assert_eq!(simplify(expr), expected);
        let expr =
            col("c2_non_null") % lit(ScalarValue::Decimal128(Some(10000000000), 31, 10));
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_bitwise_xor_by_null() {
        let null = lit(ScalarValue::Null);
        // A ^ null --> null
        {
            let expr = col("c2") ^ null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null ^ A --> null
        {
            let expr = null.clone() ^ col("c2");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_bitwise_shift_right_by_null() {
        let null = lit(ScalarValue::Null);
        // A >> null --> null
        {
            let expr = col("c2") >> null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null >> A --> null
        {
            let expr = null.clone() >> col("c2");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_bitwise_shift_left_by_null() {
        let null = lit(ScalarValue::Null);
        // A << null --> null
        {
            let expr = col("c2") << null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null << A --> null
        {
            let expr = null.clone() << col("c2");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_bitwise_and_by_zero() {
        // A & 0 --> 0
        {
            let expr = col("c2_non_null") & lit(0);
            assert_eq!(simplify(expr), lit(0));
        }
        // 0 & A --> 0
        {
            let expr = lit(0) & col("c2_non_null");
            assert_eq!(simplify(expr), lit(0));
        }
    }

    #[test]
    fn test_simplify_bitwise_or_by_zero() {
        // A | 0 --> A
        {
            let expr = col("c2_non_null") | lit(0);
            assert_eq!(simplify(expr), col("c2_non_null"));
        }
        // 0 | A --> A
        {
            let expr = lit(0) | col("c2_non_null");
            assert_eq!(simplify(expr), col("c2_non_null"));
        }
    }

    #[test]
    fn test_simplify_bitwise_xor_by_zero() {
        // A ^ 0 --> A
        {
            let expr = col("c2_non_null") ^ lit(0);
            assert_eq!(simplify(expr), col("c2_non_null"));
        }
        // 0 ^ A --> A
        {
            let expr = lit(0) ^ col("c2_non_null");
            assert_eq!(simplify(expr), col("c2_non_null"));
        }
    }

    #[test]
    fn test_simplify_bitwise_bitwise_shift_right_by_zero() {
        // A >> 0 --> A
        {
            let expr = col("c2_non_null") >> lit(0);
            assert_eq!(simplify(expr), col("c2_non_null"));
        }
    }

    #[test]
    fn test_simplify_bitwise_bitwise_shift_left_by_zero() {
        // A << 0 --> A
        {
            let expr = col("c2_non_null") << lit(0);
            assert_eq!(simplify(expr), col("c2_non_null"));
        }
    }

    #[test]
    fn test_simplify_bitwise_and_by_null() {
        let null = lit(ScalarValue::Null);
        // A & null --> null
        {
            let expr = col("c2") & null.clone();
            assert_eq!(simplify(expr), null);
        }
        // null & A --> null
        {
            let expr = null.clone() & col("c2");
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_composed_bitwise_and() {
        // ((c2 > 5) & (c1 < 6)) & (c2 > 5) --> (c2 > 5) & (c1 < 6)

        let expr = bitwise_and(
            bitwise_and(col("c2").gt(lit(5)), col("c1").lt(lit(6))),
            col("c2").gt(lit(5)),
        );
        let expected = bitwise_and(col("c2").gt(lit(5)), col("c1").lt(lit(6)));

        assert_eq!(simplify(expr), expected);

        // (c2 > 5) & ((c2 > 5) & (c1 < 6)) --> (c2 > 5) & (c1 < 6)

        let expr = bitwise_and(
            col("c2").gt(lit(5)),
            bitwise_and(col("c2").gt(lit(5)), col("c1").lt(lit(6))),
        );
        let expected = bitwise_and(col("c2").gt(lit(5)), col("c1").lt(lit(6)));
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_composed_bitwise_or() {
        // ((c2 > 5) | (c1 < 6)) | (c2 > 5) --> (c2 > 5) | (c1 < 6)

        let expr = bitwise_or(
            bitwise_or(col("c2").gt(lit(5)), col("c1").lt(lit(6))),
            col("c2").gt(lit(5)),
        );
        let expected = bitwise_or(col("c2").gt(lit(5)), col("c1").lt(lit(6)));

        assert_eq!(simplify(expr), expected);

        // (c2 > 5) | ((c2 > 5) | (c1 < 6)) --> (c2 > 5) | (c1 < 6)

        let expr = bitwise_or(
            col("c2").gt(lit(5)),
            bitwise_or(col("c2").gt(lit(5)), col("c1").lt(lit(6))),
        );
        let expected = bitwise_or(col("c2").gt(lit(5)), col("c1").lt(lit(6)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_composed_bitwise_xor() {
        // with an even number of the column "c2"
        // c2 ^ ((c2 ^ (c2 | c1)) ^ (c1 & c2)) --> (c2 | c1) ^ (c1 & c2)

        let expr = bitwise_xor(
            col("c2"),
            bitwise_xor(
                bitwise_xor(col("c2"), bitwise_or(col("c2"), col("c1"))),
                bitwise_and(col("c1"), col("c2")),
            ),
        );

        let expected = bitwise_xor(
            bitwise_or(col("c2"), col("c1")),
            bitwise_and(col("c1"), col("c2")),
        );

        assert_eq!(simplify(expr), expected);

        // with an odd number of the column "c2"
        // c2 ^ (c2 ^ (c2 | c1)) ^ ((c1 & c2) ^ c2) --> c2 ^ ((c2 | c1) ^ (c1 & c2))

        let expr = bitwise_xor(
            col("c2"),
            bitwise_xor(
                bitwise_xor(col("c2"), bitwise_or(col("c2"), col("c1"))),
                bitwise_xor(bitwise_and(col("c1"), col("c2")), col("c2")),
            ),
        );

        let expected = bitwise_xor(
            col("c2"),
            bitwise_xor(
                bitwise_or(col("c2"), col("c1")),
                bitwise_and(col("c1"), col("c2")),
            ),
        );

        assert_eq!(simplify(expr), expected);

        // with an even number of the column "c2"
        // ((c2 ^ (c2 | c1)) ^ (c1 & c2)) ^ c2 --> (c2 | c1) ^ (c1 & c2)

        let expr = bitwise_xor(
            bitwise_xor(
                bitwise_xor(col("c2"), bitwise_or(col("c2"), col("c1"))),
                bitwise_and(col("c1"), col("c2")),
            ),
            col("c2"),
        );

        let expected = bitwise_xor(
            bitwise_or(col("c2"), col("c1")),
            bitwise_and(col("c1"), col("c2")),
        );

        assert_eq!(simplify(expr), expected);

        // with an odd number of the column "c2"
        // (c2 ^ (c2 | c1)) ^ ((c1 & c2) ^ c2) ^ c2 --> ((c2 | c1) ^ (c1 & c2)) ^ c2

        let expr = bitwise_xor(
            bitwise_xor(
                bitwise_xor(col("c2"), bitwise_or(col("c2"), col("c1"))),
                bitwise_xor(bitwise_and(col("c1"), col("c2")), col("c2")),
            ),
            col("c2"),
        );

        let expected = bitwise_xor(
            bitwise_xor(
                bitwise_or(col("c2"), col("c1")),
                bitwise_and(col("c1"), col("c2")),
            ),
            col("c2"),
        );

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_negated_bitwise_and() {
        // !c4 & c4 --> 0
        let expr = (-col("c4_non_null")) & col("c4_non_null");
        let expected = lit(0u32);

        assert_eq!(simplify(expr), expected);
        // c4 & !c4 --> 0
        let expr = col("c4_non_null") & (-col("c4_non_null"));
        let expected = lit(0u32);

        assert_eq!(simplify(expr), expected);

        // !c3 & c3 --> 0
        let expr = (-col("c3_non_null")) & col("c3_non_null");
        let expected = lit(0i64);

        assert_eq!(simplify(expr), expected);
        // c3 & !c3 --> 0
        let expr = col("c3_non_null") & (-col("c3_non_null"));
        let expected = lit(0i64);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_negated_bitwise_or() {
        // !c4 | c4 --> -1
        let expr = (-col("c4_non_null")) | col("c4_non_null");
        let expected = lit(-1i32);

        assert_eq!(simplify(expr), expected);

        // c4 | !c4 --> -1
        let expr = col("c4_non_null") | (-col("c4_non_null"));
        let expected = lit(-1i32);

        assert_eq!(simplify(expr), expected);

        // !c3 | c3 --> -1
        let expr = (-col("c3_non_null")) | col("c3_non_null");
        let expected = lit(-1i64);

        assert_eq!(simplify(expr), expected);

        // c3 | !c3 --> -1
        let expr = col("c3_non_null") | (-col("c3_non_null"));
        let expected = lit(-1i64);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_negated_bitwise_xor() {
        // !c4 ^ c4 --> -1
        let expr = (-col("c4_non_null")) ^ col("c4_non_null");
        let expected = lit(-1i32);

        assert_eq!(simplify(expr), expected);

        // c4 ^ !c4 --> -1
        let expr = col("c4_non_null") ^ (-col("c4_non_null"));
        let expected = lit(-1i32);

        assert_eq!(simplify(expr), expected);

        // !c3 ^ c3 --> -1
        let expr = (-col("c3_non_null")) ^ col("c3_non_null");
        let expected = lit(-1i64);

        assert_eq!(simplify(expr), expected);

        // c3 ^ !c3 --> -1
        let expr = col("c3_non_null") ^ (-col("c3_non_null"));
        let expected = lit(-1i64);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_bitwise_and_or() {
        // (c2 < 3) & ((c2 < 3) | c1) -> (c2 < 3)
        let expr = bitwise_and(
            col("c2_non_null").lt(lit(3)),
            bitwise_or(col("c2_non_null").lt(lit(3)), col("c1_non_null")),
        );
        let expected = col("c2_non_null").lt(lit(3));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_bitwise_or_and() {
        // (c2 < 3) | ((c2 < 3) & c1) -> (c2 < 3)
        let expr = bitwise_or(
            col("c2_non_null").lt(lit(3)),
            bitwise_and(col("c2_non_null").lt(lit(3)), col("c1_non_null")),
        );
        let expected = col("c2_non_null").lt(lit(3));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_simple_bitwise_and() {
        // (c2 > 5) & (c2 > 5) -> (c2 > 5)
        let expr = (col("c2").gt(lit(5))).bitand(col("c2").gt(lit(5)));
        let expected = col("c2").gt(lit(5));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_simple_bitwise_or() {
        // (c2 > 5) | (c2 > 5) -> (c2 > 5)
        let expr = (col("c2").gt(lit(5))).bitor(col("c2").gt(lit(5)));
        let expected = col("c2").gt(lit(5));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_simple_bitwise_xor() {
        // c4 ^ c4 -> 0
        let expr = (col("c4")).bitxor(col("c4"));
        let expected = lit(0u32);

        assert_eq!(simplify(expr), expected);

        // c3 ^ c3 -> 0
        let expr = col("c3").bitxor(col("c3"));
        let expected = lit(0i64);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_modulo_by_zero_non_null() {
        // because modulo by 0 maybe occur in short-circuit expression
        // so we should not simplify this, and throw error in runtime.
        let expr = col("c2_non_null") % lit(0);
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_simple_and() {
        // (c2 > 5) AND (c2 > 5) -> (c2 > 5)
        let expr = (col("c2").gt(lit(5))).and(col("c2").gt(lit(5)));
        let expected = col("c2").gt(lit(5));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_composed_and() {
        // ((c2 > 5) AND (c1 < 6)) AND (c2 > 5)
        let expr = and(
            and(col("c2").gt(lit(5)), col("c1").lt(lit(6))),
            col("c2").gt(lit(5)),
        );
        let expected = and(col("c2").gt(lit(5)), col("c1").lt(lit(6)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_negated_and() {
        // (c2 > 5) AND !(c2 > 5) --> (c2 > 5) AND (c2 <= 5)
        let expr = and(col("c2").gt(lit(5)), Expr::not(col("c2").gt(lit(5))));
        let expected = col("c2").gt(lit(5)).and(col("c2").lt_eq(lit(5)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_and() {
        let l = col("c2").gt(lit(5));
        let r = and(col("c1").lt(lit(6)), col("c2").gt(lit(5)));

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5))
        let expr = or(l.clone(), r.clone());

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5)
        let expr = or(l, r);

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_and_non_null() {
        let l = col("c2_non_null").gt(lit(5));
        let r = and(col("c1_non_null").lt(lit(6)), col("c2_non_null").gt(lit(5)));

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5)) --> c2 > 5
        let expr = or(l.clone(), r.clone());

        // This is only true if `c1 < 6` is not nullable / can not be null.
        let expected = col("c2_non_null").gt(lit(5));

        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5) --> c2 > 5
        let expr = or(l, r);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_or() {
        let l = col("c2").gt(lit(5));
        let r = or(col("c1").lt(lit(6)), col("c2").gt(lit(5)));

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = and(l.clone(), r.clone());

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = and(l, r);
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_or_non_null() {
        let l = col("c2_non_null").gt(lit(5));
        let r = or(col("c1_non_null").lt(lit(6)), col("c2_non_null").gt(lit(5)));

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = and(l.clone(), r.clone());

        // This is only true if `c1 < 6` is not nullable / can not be null.
        let expected = col("c2_non_null").gt(lit(5));

        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = and(l, r);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_by_de_morgan_laws() {
        // Laws with logical operations
        // !(c3 AND c4) --> !c3 OR !c4
        let expr = and(col("c3"), col("c4")).not();
        let expected = or(col("c3").not(), col("c4").not());
        assert_eq!(simplify(expr), expected);
        // !(c3 OR c4) --> !c3 AND !c4
        let expr = or(col("c3"), col("c4")).not();
        let expected = and(col("c3").not(), col("c4").not());
        assert_eq!(simplify(expr), expected);
        // !(!c3) --> c3
        let expr = col("c3").not().not();
        let expected = col("c3");
        assert_eq!(simplify(expr), expected);

        // Laws with bitwise operations
        // !(c3 & c4) --> !c3 | !c4
        let expr = -bitwise_and(col("c3"), col("c4"));
        let expected = bitwise_or(-col("c3"), -col("c4"));
        assert_eq!(simplify(expr), expected);
        // !(c3 | c4) --> !c3 & !c4
        let expr = -bitwise_or(col("c3"), col("c4"));
        let expected = bitwise_and(-col("c3"), -col("c4"));
        assert_eq!(simplify(expr), expected);
        // !(!c3) --> c3
        let expr = -(-col("c3"));
        let expected = col("c3");
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_null_and_false() {
        let expr = and(lit_bool_null(), lit(false));
        let expr_eq = lit(false);

        assert_eq!(simplify(expr), expr_eq);
    }

    #[test]
    fn test_simplify_divide_null_by_null() {
        let null = lit(ScalarValue::Int32(None));
        let expr_plus = null.clone() / null.clone();
        let expr_eq = null;

        assert_eq!(simplify(expr_plus), expr_eq);
    }

    #[test]
    fn test_simplify_simplify_arithmetic_expr() {
        let expr_plus = lit(1) + lit(1);

        assert_eq!(simplify(expr_plus), lit(2));
    }

    #[test]
    fn test_simplify_simplify_eq_expr() {
        let expr_eq = binary_expr(lit(1), Operator::Eq, lit(1));

        assert_eq!(simplify(expr_eq), lit(true));
    }

    #[test]
    fn test_simplify_log() {
        // Log(c3, 1) ===> 0
        {
            let expr = log(col("c3_non_null"), lit(1));
            let expected = lit(0i64);
            assert_eq!(simplify(expr), expected);
        }
        // Log(c3, c3) ===> 1
        {
            let expr = log(col("c3_non_null"), col("c3_non_null"));
            let expected = lit(1i64);
            assert_eq!(simplify(expr), expected);
        }
        // Log(c3, Power(c3, c4)) ===> c4
        {
            let expr = log(
                col("c3_non_null"),
                power(col("c3_non_null"), col("c4_non_null")),
            );
            let expected = col("c4_non_null");
            assert_eq!(simplify(expr), expected);
        }
        // Log(c3, c4) ===> Log(c3, c4)
        {
            let expr = log(col("c3_non_null"), col("c4_non_null"));
            let expected = log(col("c3_non_null"), col("c4_non_null"));
            assert_eq!(simplify(expr), expected);
        }
    }

    #[test]
    fn test_simplify_power() {
        // Power(c3, 0) ===> 1
        {
            let expr = power(col("c3_non_null"), lit(0));
            let expected = lit(1i64);
            assert_eq!(simplify(expr), expected);
        }
        // Power(c3, 1) ===> c3
        {
            let expr = power(col("c3_non_null"), lit(1));
            let expected = col("c3_non_null");
            assert_eq!(simplify(expr), expected);
        }
        // Power(c3, Log(c3, c4)) ===> c4
        {
            let expr = power(
                col("c3_non_null"),
                log(col("c3_non_null"), col("c4_non_null")),
            );
            let expected = col("c4_non_null");
            assert_eq!(simplify(expr), expected);
        }
        // Power(c3, c4) ===> Power(c3, c4)
        {
            let expr = power(col("c3_non_null"), col("c4_non_null"));
            let expected = power(col("c3_non_null"), col("c4_non_null"));
            assert_eq!(simplify(expr), expected);
        }
    }

    #[test]
    fn test_simplify_concat_ws() {
        let null = lit(ScalarValue::Utf8(None));
        // the delimiter is not a literal
        {
            let expr = concat_ws(col("c"), vec![lit("a"), null.clone(), lit("b")]);
            let expected = concat_ws(col("c"), vec![lit("a"), lit("b")]);
            assert_eq!(simplify(expr), expected);
        }

        // the delimiter is an empty string
        {
            let expr = concat_ws(lit(""), vec![col("a"), lit("c"), lit("b")]);
            let expected = concat(&[col("a"), lit("cb")]);
            assert_eq!(simplify(expr), expected);
        }

        // the delimiter is a not-empty string
        {
            let expr = concat_ws(
                lit("-"),
                vec![
                    null.clone(),
                    col("c0"),
                    lit("hello"),
                    null.clone(),
                    lit("rust"),
                    col("c1"),
                    lit(""),
                    lit(""),
                    null,
                ],
            );
            let expected = concat_ws(
                lit("-"),
                vec![col("c0"), lit("hello-rust"), col("c1"), lit("-")],
            );
            assert_eq!(simplify(expr), expected)
        }
    }

    #[test]
    fn test_simplify_concat_ws_with_null() {
        let null = lit(ScalarValue::Utf8(None));
        // null delimiter -> null
        {
            let expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
            assert_eq!(simplify(expr), null);
        }

        // filter out null args
        {
            let expr = concat_ws(lit("|"), vec![col("c1"), null.clone(), col("c2")]);
            let expected = concat_ws(lit("|"), vec![col("c1"), col("c2")]);
            assert_eq!(simplify(expr), expected);
        }

        // nested test
        {
            let sub_expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
            let expr = concat_ws(lit("|"), vec![sub_expr, col("c3")]);
            assert_eq!(simplify(expr), concat_ws(lit("|"), vec![col("c3")]));
        }

        // null delimiter (nested)
        {
            let sub_expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
            let expr = concat_ws(sub_expr, vec![col("c3"), col("c4")]);
            assert_eq!(simplify(expr), null);
        }
    }

    #[test]
    fn test_simplify_concat() {
        let null = lit(ScalarValue::Utf8(None));
        let expr = concat(&[
            null.clone(),
            col("c0"),
            lit("hello "),
            null.clone(),
            lit("rust"),
            col("c1"),
            lit(""),
            null,
        ]);
        let expected = concat(&[col("c0"), lit("hello rust"), col("c1")]);
        assert_eq!(simplify(expr), expected)
    }

    #[test]
    fn test_simplify_regex() {
        // malformed regex
        assert_contains!(
            try_simplify(regex_match(col("c1"), lit("foo{")))
                .unwrap_err()
                .to_string(),
            "regex parse error"
        );

        // unsupported cases
        assert_no_change(regex_match(col("c1"), lit("foo.*")));
        assert_no_change(regex_match(col("c1"), lit("(foo)")));
        assert_no_change(regex_match(col("c1"), lit("^foo")));
        assert_no_change(regex_match(col("c1"), lit("foo$")));
        assert_no_change(regex_match(col("c1"), lit("%")));
        assert_no_change(regex_match(col("c1"), lit("_")));
        assert_no_change(regex_match(col("c1"), lit("f%o")));
        assert_no_change(regex_match(col("c1"), lit("f_o")));

        // empty cases
        assert_change(regex_match(col("c1"), lit("")), lit(true));
        assert_change(regex_not_match(col("c1"), lit("")), lit(false));
        assert_change(regex_imatch(col("c1"), lit("")), lit(true));
        assert_change(regex_not_imatch(col("c1"), lit("")), lit(false));

        // single character
        assert_change(regex_match(col("c1"), lit("x")), like(col("c1"), "%x%"));

        // single word
        assert_change(regex_match(col("c1"), lit("foo")), like(col("c1"), "%foo%"));

        // regular expressions that match an exact literal
        assert_change(regex_match(col("c1"), lit("^$")), col("c1").eq(lit("")));
        assert_change(
            regex_not_match(col("c1"), lit("^$")),
            col("c1").not_eq(lit("")),
        );
        assert_change(
            regex_match(col("c1"), lit("^foo$")),
            col("c1").eq(lit("foo")),
        );
        assert_change(
            regex_not_match(col("c1"), lit("^foo$")),
            col("c1").not_eq(lit("foo")),
        );

        // regular expressions that match exact captured literals
        assert_change(
            regex_match(col("c1"), lit("^(foo|bar)$")),
            col("c1").eq(lit("foo")).or(col("c1").eq(lit("bar"))),
        );
        assert_change(
            regex_not_match(col("c1"), lit("^(foo|bar)$")),
            col("c1")
                .not_eq(lit("foo"))
                .and(col("c1").not_eq(lit("bar"))),
        );
        assert_change(
            regex_match(col("c1"), lit("^(foo)$")),
            col("c1").eq(lit("foo")),
        );
        assert_change(
            regex_match(col("c1"), lit("^(foo|bar|baz)$")),
            ((col("c1").eq(lit("foo"))).or(col("c1").eq(lit("bar"))))
                .or(col("c1").eq(lit("baz"))),
        );
        assert_change(
            regex_match(col("c1"), lit("^(foo|bar|baz|qux)$")),
            col("c1")
                .in_list(vec![lit("foo"), lit("bar"), lit("baz"), lit("qux")], false),
        );
        assert_change(
            regex_match(col("c1"), lit("^(fo_o)$")),
            col("c1").eq(lit("fo_o")),
        );
        assert_change(
            regex_match(col("c1"), lit("^(fo_o)$")),
            col("c1").eq(lit("fo_o")),
        );
        assert_change(
            regex_match(col("c1"), lit("^(fo_o|ba_r)$")),
            col("c1").eq(lit("fo_o")).or(col("c1").eq(lit("ba_r"))),
        );
        assert_change(
            regex_not_match(col("c1"), lit("^(fo_o|ba_r)$")),
            col("c1")
                .not_eq(lit("fo_o"))
                .and(col("c1").not_eq(lit("ba_r"))),
        );
        assert_change(
            regex_match(col("c1"), lit("^(fo_o|ba_r|ba_z)$")),
            ((col("c1").eq(lit("fo_o"))).or(col("c1").eq(lit("ba_r"))))
                .or(col("c1").eq(lit("ba_z"))),
        );
        assert_change(
            regex_match(col("c1"), lit("^(fo_o|ba_r|baz|qu_x)$")),
            col("c1").in_list(
                vec![lit("fo_o"), lit("ba_r"), lit("baz"), lit("qu_x")],
                false,
            ),
        );

        // regular expressions that mismatch captured literals
        assert_no_change(regex_match(col("c1"), lit("(foo|bar)")));
        assert_no_change(regex_match(col("c1"), lit("(foo|bar)*")));
        assert_no_change(regex_match(col("c1"), lit("(fo_o|b_ar)")));
        assert_no_change(regex_match(col("c1"), lit("(foo|ba_r)*")));
        assert_no_change(regex_match(col("c1"), lit("(fo_o|ba_r)*")));
        assert_no_change(regex_match(col("c1"), lit("^(foo|bar)*")));
        assert_no_change(regex_match(col("c1"), lit("^foo|bar$")));
        assert_no_change(regex_match(col("c1"), lit("^(foo)(bar)$")));
        assert_no_change(regex_match(col("c1"), lit("^")));
        assert_no_change(regex_match(col("c1"), lit("$")));
        assert_no_change(regex_match(col("c1"), lit("$^")));
        assert_no_change(regex_match(col("c1"), lit("$foo^")));

        // OR-chain
        assert_change(
            regex_match(col("c1"), lit("foo|bar|baz")),
            like(col("c1"), "%foo%")
                .or(like(col("c1"), "%bar%"))
                .or(like(col("c1"), "%baz%")),
        );
        assert_change(
            regex_match(col("c1"), lit("foo|x|baz")),
            like(col("c1"), "%foo%")
                .or(like(col("c1"), "%x%"))
                .or(like(col("c1"), "%baz%")),
        );
        assert_change(
            regex_not_match(col("c1"), lit("foo|bar|baz")),
            not_like(col("c1"), "%foo%")
                .and(not_like(col("c1"), "%bar%"))
                .and(not_like(col("c1"), "%baz%")),
        );
        // both anchored expressions (translated to equality) and unanchored
        assert_change(
            regex_match(col("c1"), lit("foo|^x$|baz")),
            like(col("c1"), "%foo%")
                .or(col("c1").eq(lit("x")))
                .or(like(col("c1"), "%baz%")),
        );
        assert_change(
            regex_not_match(col("c1"), lit("foo|^bar$|baz")),
            not_like(col("c1"), "%foo%")
                .and(col("c1").not_eq(lit("bar")))
                .and(not_like(col("c1"), "%baz%")),
        );
        // Too many patterns (MAX_REGEX_ALTERNATIONS_EXPANSION)
        assert_no_change(regex_match(col("c1"), lit("foo|bar|baz|blarg|bozo|etc")));
    }

    #[track_caller]
    fn assert_no_change(expr: Expr) {
        let optimized = simplify(expr.clone());
        assert_eq!(expr, optimized);
    }

    #[track_caller]
    fn assert_change(expr: Expr, expected: Expr) {
        let optimized = simplify(expr);
        assert_eq!(optimized, expected);
    }

    fn regex_match(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::RegexMatch,
            right: Box::new(right),
        })
    }

    fn regex_not_match(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::RegexNotMatch,
            right: Box::new(right),
        })
    }

    fn regex_imatch(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::RegexIMatch,
            right: Box::new(right),
        })
    }

    fn regex_not_imatch(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::RegexNotIMatch,
            right: Box::new(right),
        })
    }

    fn like(expr: Expr, pattern: &str) -> Expr {
        Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(lit(pattern)),
            escape_char: None,
            case_insensitive: false,
        })
    }

    fn not_like(expr: Expr, pattern: &str) -> Expr {
        Expr::Like(Like {
            negated: true,
            expr: Box::new(expr),
            pattern: Box::new(lit(pattern)),
            escape_char: None,
            case_insensitive: false,
        })
    }

    fn ilike(expr: Expr, pattern: &str) -> Expr {
        Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(lit(pattern)),
            escape_char: None,
            case_insensitive: true,
        })
    }

    fn not_ilike(expr: Expr, pattern: &str) -> Expr {
        Expr::Like(Like {
            negated: true,
            expr: Box::new(expr),
            pattern: Box::new(lit(pattern)),
            escape_char: None,
            case_insensitive: true,
        })
    }

    // ------------------------------
    // ----- Simplifier tests -------
    // ------------------------------

    fn try_simplify(expr: Expr) -> Result<Expr> {
        let schema = expr_test_schema();
        let execution_props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&execution_props).with_schema(schema),
        );
        simplifier.simplify(expr)
    }

    fn simplify(expr: Expr) -> Expr {
        try_simplify(expr).unwrap()
    }

    fn simplify_with_guarantee(
        expr: Expr,
        guarantees: Vec<(Expr, NullableInterval)>,
    ) -> Expr {
        let schema = expr_test_schema();
        let execution_props = ExecutionProps::new();
        let simplifier = ExprSimplifier::new(
            SimplifyContext::new(&execution_props).with_schema(schema),
        )
        .with_guarantees(guarantees);
        simplifier.simplify(expr).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualifed_fields(
                vec![
                    Field::new("c1", DataType::Utf8, true),
                    Field::new("c2", DataType::Boolean, true),
                    Field::new("c3", DataType::Int64, true),
                    Field::new("c4", DataType::UInt32, true),
                    Field::new("c1_non_null", DataType::Utf8, false),
                    Field::new("c2_non_null", DataType::Boolean, false),
                    Field::new("c3_non_null", DataType::Int64, false),
                    Field::new("c4_non_null", DataType::UInt32, false),
                ]
                .into(),
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn simplify_expr_null_comparison() {
        // x = null is always null
        assert_eq!(
            simplify(lit(true).eq(lit(ScalarValue::Boolean(None)))),
            lit(ScalarValue::Boolean(None)),
        );

        // null != null is always null
        assert_eq!(
            simplify(
                lit(ScalarValue::Boolean(None)).not_eq(lit(ScalarValue::Boolean(None)))
            ),
            lit(ScalarValue::Boolean(None)),
        );

        // x != null is always null
        assert_eq!(
            simplify(col("c2").not_eq(lit(ScalarValue::Boolean(None)))),
            lit(ScalarValue::Boolean(None)),
        );

        // null = x is always null
        assert_eq!(
            simplify(lit(ScalarValue::Boolean(None)).eq(col("c2"))),
            lit(ScalarValue::Boolean(None)),
        );
    }

    #[test]
    fn simplify_expr_is_not_null() {
        assert_eq!(
            simplify(Expr::IsNotNull(Box::new(col("c1")))),
            Expr::IsNotNull(Box::new(col("c1")))
        );

        // 'c1_non_null IS NOT NULL' is always true
        assert_eq!(
            simplify(Expr::IsNotNull(Box::new(col("c1_non_null")))),
            lit(true)
        );
    }

    #[test]
    fn simplify_expr_is_null() {
        assert_eq!(
            simplify(Expr::IsNull(Box::new(col("c1")))),
            Expr::IsNull(Box::new(col("c1")))
        );

        // 'c1_non_null IS NULL' is always false
        assert_eq!(
            simplify(Expr::IsNull(Box::new(col("c1_non_null")))),
            lit(false)
        );
    }

    #[test]
    fn simplify_expr_is_unknown() {
        assert_eq!(simplify(col("c2").is_unknown()), col("c2").is_unknown(),);

        // 'c2_non_null is unknown' is always false
        assert_eq!(simplify(col("c2_non_null").is_unknown()), lit(false));
    }

    #[test]
    fn simplify_expr_is_not_known() {
        assert_eq!(
            simplify(col("c2").is_not_unknown()),
            col("c2").is_not_unknown()
        );

        // 'c2_non_null is not unknown' is always true
        assert_eq!(simplify(col("c2_non_null").is_not_unknown()), lit(true));
    }

    #[test]
    fn simplify_expr_eq() {
        let schema = expr_test_schema();
        assert_eq!(col("c2").get_type(&schema).unwrap(), DataType::Boolean);

        // true = true -> true
        assert_eq!(simplify(lit(true).eq(lit(true))), lit(true));

        // true = false -> false
        assert_eq!(simplify(lit(true).eq(lit(false))), lit(false),);

        // c2 = true -> c2
        assert_eq!(simplify(col("c2").eq(lit(true))), col("c2"));

        // c2 = false => !c2
        assert_eq!(simplify(col("c2").eq(lit(false))), col("c2").not(),);
    }

    #[test]
    fn simplify_expr_eq_skip_nonboolean_type() {
        let schema = expr_test_schema();

        // When one of the operand is not of boolean type, folding the
        // other boolean constant will change return type of
        // expression to non-boolean.
        //
        // Make sure c1 column to be used in tests is not boolean type
        assert_eq!(col("c1").get_type(&schema).unwrap(), DataType::Utf8);

        // don't fold c1 = foo
        assert_eq!(simplify(col("c1").eq(lit("foo"))), col("c1").eq(lit("foo")),);
    }

    #[test]
    fn simplify_expr_not_eq() {
        let schema = expr_test_schema();

        assert_eq!(col("c2").get_type(&schema).unwrap(), DataType::Boolean);

        // c2 != true -> !c2
        assert_eq!(simplify(col("c2").not_eq(lit(true))), col("c2").not(),);

        // c2 != false -> c2
        assert_eq!(simplify(col("c2").not_eq(lit(false))), col("c2"),);

        // test constant
        assert_eq!(simplify(lit(true).not_eq(lit(true))), lit(false),);

        assert_eq!(simplify(lit(true).not_eq(lit(false))), lit(true),);
    }

    #[test]
    fn simplify_expr_not_eq_skip_nonboolean_type() {
        let schema = expr_test_schema();

        // when one of the operand is not of boolean type, folding the
        // other boolean constant will change return type of
        // expression to non-boolean.
        assert_eq!(col("c1").get_type(&schema).unwrap(), DataType::Utf8);

        assert_eq!(
            simplify(col("c1").not_eq(lit("foo"))),
            col("c1").not_eq(lit("foo")),
        );
    }

    #[test]
    fn simplify_expr_case_when_then_else() {
        // CASE WHEN c2 != false THEN "ok" == "not_ok" ELSE c2 == true
        // -->
        // CASE WHEN c2 THEN false ELSE c2
        // -->
        // false
        assert_eq!(
            simplify(Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit("not_ok"))),
                )],
                Some(Box::new(col("c2").eq(lit(true)))),
            ))),
            col("c2").not().and(col("c2")) // #1716
        );

        // CASE WHEN c2 != false THEN "ok" == "ok" ELSE c2
        // -->
        // CASE WHEN c2 THEN true ELSE c2
        // -->
        // c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit("ok"))),
                )],
                Some(Box::new(col("c2").eq(lit(true)))),
            )))),
            col("c2").or(col("c2").not().and(col("c2"))) // #1716
        );

        // CASE WHEN ISNULL(c2) THEN true ELSE c2
        // -->
        // ISNULL(c2) OR c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![(Box::new(col("c2").is_null()), Box::new(lit(true)),)],
                Some(Box::new(col("c2"))),
            )))),
            col("c2")
                .is_null()
                .or(col("c2").is_not_null().and(col("c2")))
        );

        // CASE WHEN c1 then true WHEN c2 then false ELSE true
        // --> c1 OR (NOT(c1) AND c2 AND FALSE) OR (NOT(c1 OR c2) AND TRUE)
        // --> c1 OR (NOT(c1) AND NOT(c2))
        // --> c1 OR NOT(c2)
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![
                    (Box::new(col("c1")), Box::new(lit(true)),),
                    (Box::new(col("c2")), Box::new(lit(false)),),
                ],
                Some(Box::new(lit(true))),
            )))),
            col("c1").or(col("c1").not().and(col("c2").not()))
        );

        // CASE WHEN c1 then true WHEN c2 then true ELSE false
        // --> c1 OR (NOT(c1) AND c2 AND TRUE) OR (NOT(c1 OR c2) AND FALSE)
        // --> c1 OR (NOT(c1) AND c2)
        // --> c1 OR c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case(Case::new(
                None,
                vec![
                    (Box::new(col("c1")), Box::new(lit(true)),),
                    (Box::new(col("c2")), Box::new(lit(false)),),
                ],
                Some(Box::new(lit(true))),
            )))),
            col("c1").or(col("c1").not().and(col("c2").not()))
        );
    }

    #[test]
    fn simplify_expr_bool_or() {
        // col || true is always true
        assert_eq!(simplify(col("c2").or(lit(true))), lit(true),);

        // col || false is always col
        assert_eq!(simplify(col("c2").or(lit(false))), col("c2"),);

        // true || null is always true
        assert_eq!(simplify(lit(true).or(lit_bool_null())), lit(true),);

        // null || true is always true
        assert_eq!(simplify(lit_bool_null().or(lit(true))), lit(true),);

        // false || null is always null
        assert_eq!(simplify(lit(false).or(lit_bool_null())), lit_bool_null(),);

        // null || false is always null
        assert_eq!(simplify(lit_bool_null().or(lit(false))), lit_bool_null(),);

        // ( c1 BETWEEN Int32(0) AND Int32(10) ) OR Boolean(NULL)
        // it can be either NULL or  TRUE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10)`
        // and should not be rewritten
        let expr = col("c1").between(lit(0), lit(10));
        let expr = expr.or(lit_bool_null());
        let result = simplify(expr);

        let expected_expr = or(
            and(col("c1").gt_eq(lit(0)), col("c1").lt_eq(lit(10))),
            lit_bool_null(),
        );
        assert_eq!(expected_expr, result);
    }

    #[test]
    fn simplify_inlist() {
        assert_eq!(simplify(in_list(col("c1"), vec![], false)), lit(false));
        assert_eq!(simplify(in_list(col("c1"), vec![], true)), lit(true));

        // null in (...)  --> null
        assert_eq!(
            simplify(in_list(lit_bool_null(), vec![col("c1"), lit(1)], false)),
            lit_bool_null()
        );

        // null not in (...)  --> null
        assert_eq!(
            simplify(in_list(lit_bool_null(), vec![col("c1"), lit(1)], true)),
            lit_bool_null()
        );

        assert_eq!(
            simplify(in_list(col("c1"), vec![lit(1)], false)),
            col("c1").eq(lit(1))
        );
        assert_eq!(
            simplify(in_list(col("c1"), vec![lit(1)], true)),
            col("c1").not_eq(lit(1))
        );

        // more complex expressions can be simplified if list contains
        // one element only
        assert_eq!(
            simplify(in_list(col("c1") * lit(10), vec![lit(2)], false)),
            (col("c1") * lit(10)).eq(lit(2))
        );

        assert_eq!(
            simplify(in_list(col("c1"), vec![lit(1), lit(2)], false)),
            col("c1").eq(lit(1)).or(col("c1").eq(lit(2)))
        );
        assert_eq!(
            simplify(in_list(col("c1"), vec![lit(1), lit(2)], true)),
            col("c1").not_eq(lit(1)).and(col("c1").not_eq(lit(2)))
        );

        let subquery = Arc::new(test_table_scan_with_name("test").unwrap());
        assert_eq!(
            simplify(in_list(
                col("c1"),
                vec![scalar_subquery(subquery.clone())],
                false
            )),
            in_subquery(col("c1"), subquery.clone())
        );
        assert_eq!(
            simplify(in_list(
                col("c1"),
                vec![scalar_subquery(subquery.clone())],
                true
            )),
            not_in_subquery(col("c1"), subquery)
        );

        let subquery1 =
            scalar_subquery(Arc::new(test_table_scan_with_name("test1").unwrap()));
        let subquery2 =
            scalar_subquery(Arc::new(test_table_scan_with_name("test2").unwrap()));

        // c1 NOT IN (<subquery1>, <subquery2>) -> c1 != <subquery1> AND c1 != <subquery2>
        assert_eq!(
            simplify(in_list(
                col("c1"),
                vec![subquery1.clone(), subquery2.clone()],
                true
            )),
            col("c1")
                .not_eq(subquery1.clone())
                .and(col("c1").not_eq(subquery2.clone()))
        );

        // c1 IN (<subquery1>, <subquery2>) -> c1 == <subquery1> OR c1 == <subquery2>
        assert_eq!(
            simplify(in_list(
                col("c1"),
                vec![subquery1.clone(), subquery2.clone()],
                false
            )),
            col("c1").eq(subquery1).or(col("c1").eq(subquery2))
        );

        // 1. c1 IN (1,2,3,4) AND c1 IN (5,6,7,8) -> false
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], false).and(
            in_list(col("c1"), vec![lit(5), lit(6), lit(7), lit(8)], false),
        );
        assert_eq!(simplify(expr.clone()), lit(false));

        // 2. c1 IN (1,2,3,4) AND c1 IN (4,5,6,7) -> c1 = 4
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], false).and(
            in_list(col("c1"), vec![lit(4), lit(5), lit(6), lit(7)], false),
        );
        assert_eq!(simplify(expr.clone()), col("c1").eq(lit(4)));

        // 3. c1 NOT IN (1, 2, 3, 4) OR c1 NOT IN (5, 6, 7, 8) -> true
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], true).or(
            in_list(col("c1"), vec![lit(5), lit(6), lit(7), lit(8)], true),
        );
        assert_eq!(simplify(expr.clone()), lit(true));

        // 3.5 c1 NOT IN (1, 2, 3, 4) OR c1 NOT IN (4, 5, 6, 7) -> c1 != 4 (4 overlaps)
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], true).or(
            in_list(col("c1"), vec![lit(4), lit(5), lit(6), lit(7)], true),
        );
        assert_eq!(simplify(expr.clone()), col("c1").not_eq(lit(4)));

        // 4. c1 NOT IN (1,2,3,4) AND c1 NOT IN (4,5,6,7) -> c1 NOT IN (1,2,3,4,5,6,7)
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], true).and(
            in_list(col("c1"), vec![lit(4), lit(5), lit(6), lit(7)], true),
        );
        assert_eq!(
            simplify(expr.clone()),
            in_list(
                col("c1"),
                vec![lit(1), lit(2), lit(3), lit(4), lit(5), lit(6), lit(7)],
                true
            )
        );

        // 5. c1 IN (1,2,3,4) OR c1 IN (2,3,4,5) -> c1 IN (1,2,3,4,5)
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], false).or(
            in_list(col("c1"), vec![lit(2), lit(3), lit(4), lit(5)], false),
        );
        assert_eq!(
            simplify(expr.clone()),
            in_list(
                col("c1"),
                vec![lit(1), lit(2), lit(3), lit(4), lit(5)],
                false
            )
        );

        // 6. c1 IN (1,2,3) AND c1 NOT INT (1,2,3,4,5) -> false
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3)], false).and(in_list(
            col("c1"),
            vec![lit(1), lit(2), lit(3), lit(4), lit(5)],
            true,
        ));
        assert_eq!(simplify(expr.clone()), lit(false));

        // 7. c1 NOT IN (1,2,3,4) AND c1 IN (1,2,3,4,5) -> c1 = 5
        let expr =
            in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], true).and(in_list(
                col("c1"),
                vec![lit(1), lit(2), lit(3), lit(4), lit(5)],
                false,
            ));
        assert_eq!(simplify(expr.clone()), col("c1").eq(lit(5)));

        // 8. c1 IN (1,2,3,4) AND c1 NOT IN (5,6,7,8) -> c1 IN (1,2,3,4)
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], false).and(
            in_list(col("c1"), vec![lit(5), lit(6), lit(7), lit(8)], true),
        );
        assert_eq!(
            simplify(expr.clone()),
            in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], false)
        );

        // inlist with more than two expressions
        // c1 IN (1,2,3,4,5,6) AND c1 IN (1,3,5,6) AND c1 IN (3,6) -> c1 = 3 OR c1 = 6
        let expr = in_list(
            col("c1"),
            vec![lit(1), lit(2), lit(3), lit(4), lit(5), lit(6)],
            false,
        )
        .and(in_list(
            col("c1"),
            vec![lit(1), lit(3), lit(5), lit(6)],
            false,
        ))
        .and(in_list(col("c1"), vec![lit(3), lit(6)], false));
        assert_eq!(
            simplify(expr.clone()),
            col("c1").eq(lit(3)).or(col("c1").eq(lit(6)))
        );

        // c1 NOT IN (1,2,3,4) AND c1 IN (5,6,7,8) AND c1 NOT IN (3,4,5,6) AND c1 IN (8,9,10) -> c1 = 8
        let expr = in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], true).and(
            in_list(col("c1"), vec![lit(5), lit(6), lit(7), lit(8)], false)
                .and(in_list(
                    col("c1"),
                    vec![lit(3), lit(4), lit(5), lit(6)],
                    true,
                ))
                .and(in_list(col("c1"), vec![lit(8), lit(9), lit(10)], false)),
        );
        assert_eq!(simplify(expr.clone()), col("c1").eq(lit(8)));

        // Contains non-InList expression
        // c1 NOT IN (1,2,3,4) OR c1 != 5 OR c1 NOT IN (6,7,8,9) -> c1 NOT IN (1,2,3,4) OR c1 != 5 OR c1 NOT IN (6,7,8,9)
        let expr =
            in_list(col("c1"), vec![lit(1), lit(2), lit(3), lit(4)], true).or(col("c1")
                .not_eq(lit(5))
                .or(in_list(
                    col("c1"),
                    vec![lit(6), lit(7), lit(8), lit(9)],
                    true,
                )));
        // TODO: Further simplify this expression
        // https://github.com/apache/arrow-datafusion/issues/8970
        // assert_eq!(simplify(expr.clone()), lit(true));
        assert_eq!(simplify(expr.clone()), expr);
    }

    #[test]
    fn simplify_large_or() {
        let expr = (0..5)
            .map(|i| col("c1").eq(lit(i)))
            .fold(lit(false), |acc, e| acc.or(e));
        assert_eq!(
            simplify(expr),
            in_list(col("c1"), (0..5).map(lit).collect(), false),
        );
    }

    #[test]
    fn simplify_expr_bool_and() {
        // col & true is always col
        assert_eq!(simplify(col("c2").and(lit(true))), col("c2"),);
        // col & false is always false
        assert_eq!(simplify(col("c2").and(lit(false))), lit(false),);

        // true && null is always null
        assert_eq!(simplify(lit(true).and(lit_bool_null())), lit_bool_null(),);

        // null && true is always null
        assert_eq!(simplify(lit_bool_null().and(lit(true))), lit_bool_null(),);

        // false && null is always false
        assert_eq!(simplify(lit(false).and(lit_bool_null())), lit(false),);

        // null && false is always false
        assert_eq!(simplify(lit_bool_null().and(lit(false))), lit(false),);

        // c1 BETWEEN Int32(0) AND Int32(10) AND Boolean(NULL)
        // it can be either NULL or FALSE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10)`
        // and the Boolean(NULL) should remain
        let expr = col("c1").between(lit(0), lit(10));
        let expr = expr.and(lit_bool_null());
        let result = simplify(expr);

        let expected_expr = and(
            and(col("c1").gt_eq(lit(0)), col("c1").lt_eq(lit(10))),
            lit_bool_null(),
        );
        assert_eq!(expected_expr, result);
    }

    #[test]
    fn simplify_expr_between() {
        // c2 between 3 and 4 is c2 >= 3 and c2 <= 4
        let expr = col("c2").between(lit(3), lit(4));
        assert_eq!(
            simplify(expr),
            and(col("c2").gt_eq(lit(3)), col("c2").lt_eq(lit(4)))
        );

        // c2 not between 3 and 4 is c2 < 3 or c2 > 4
        let expr = col("c2").not_between(lit(3), lit(4));
        assert_eq!(
            simplify(expr),
            or(col("c2").lt(lit(3)), col("c2").gt(lit(4)))
        );
    }

    #[test]
    fn test_like_and_ilke() {
        // test non-null values
        let expr = like(col("c1"), "%");
        assert_eq!(simplify(expr), lit(true));

        let expr = not_like(col("c1"), "%");
        assert_eq!(simplify(expr), lit(false));

        let expr = ilike(col("c1"), "%");
        assert_eq!(simplify(expr), lit(true));

        let expr = not_ilike(col("c1"), "%");
        assert_eq!(simplify(expr), lit(false));

        // test null values
        let null = lit(ScalarValue::Utf8(None));
        let expr = like(null.clone(), "%");
        assert_eq!(simplify(expr), lit_bool_null());

        let expr = not_like(null.clone(), "%");
        assert_eq!(simplify(expr), lit_bool_null());

        let expr = ilike(null.clone(), "%");
        assert_eq!(simplify(expr), lit_bool_null());

        let expr = not_ilike(null, "%");
        assert_eq!(simplify(expr), lit_bool_null());
    }

    #[test]
    fn test_simplify_with_guarantee() {
        // (c3 >= 3) AND (c4 + 2 < 10 OR (c1 NOT IN ("a", "b")))
        let expr_x = col("c3").gt(lit(3_i64));
        let expr_y = (col("c4") + lit(2_u32)).lt(lit(10_u32));
        let expr_z = col("c1").in_list(vec![lit("a"), lit("b")], true);
        let expr = expr_x.clone().and(expr_y.clone().or(expr_z));

        // All guaranteed null
        let guarantees = vec![
            (col("c3"), NullableInterval::from(ScalarValue::Int64(None))),
            (col("c4"), NullableInterval::from(ScalarValue::UInt32(None))),
            (col("c1"), NullableInterval::from(ScalarValue::Utf8(None))),
        ];

        let output = simplify_with_guarantee(expr.clone(), guarantees);
        assert_eq!(output, lit_bool_null());

        // All guaranteed false
        let guarantees = vec![
            (
                col("c3"),
                NullableInterval::NotNull {
                    values: Interval::make(Some(0_i64), Some(2_i64)).unwrap(),
                },
            ),
            (
                col("c4"),
                NullableInterval::from(ScalarValue::UInt32(Some(9))),
            ),
            (col("c1"), NullableInterval::from(ScalarValue::from("a"))),
        ];
        let output = simplify_with_guarantee(expr.clone(), guarantees);
        assert_eq!(output, lit(false));

        // Guaranteed false or null -> no change.
        let guarantees = vec![
            (
                col("c3"),
                NullableInterval::MaybeNull {
                    values: Interval::make(Some(0_i64), Some(2_i64)).unwrap(),
                },
            ),
            (
                col("c4"),
                NullableInterval::MaybeNull {
                    values: Interval::make(Some(9_u32), Some(9_u32)).unwrap(),
                },
            ),
            (
                col("c1"),
                NullableInterval::NotNull {
                    values: Interval::try_new(
                        ScalarValue::from("d"),
                        ScalarValue::from("f"),
                    )
                    .unwrap(),
                },
            ),
        ];
        let output = simplify_with_guarantee(expr.clone(), guarantees);
        assert_eq!(&output, &expr_x);

        // Sufficient true guarantees
        let guarantees = vec![
            (
                col("c3"),
                NullableInterval::from(ScalarValue::Int64(Some(9))),
            ),
            (
                col("c4"),
                NullableInterval::from(ScalarValue::UInt32(Some(3))),
            ),
        ];
        let output = simplify_with_guarantee(expr.clone(), guarantees);
        assert_eq!(output, lit(true));

        // Only partially simplify
        let guarantees = vec![(
            col("c4"),
            NullableInterval::from(ScalarValue::UInt32(Some(3))),
        )];
        let output = simplify_with_guarantee(expr.clone(), guarantees);
        assert_eq!(&output, &expr_x);
    }

    #[test]
    fn test_expression_partial_simplify_1() {
        // (1 + 2) + (4 / 0) -> 3 + (4 / 0)
        let expr = (lit(1) + lit(2)) + (lit(4) / lit(0));
        let expected = (lit(3)) + (lit(4) / lit(0));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_expression_partial_simplify_2() {
        // (1 > 2) and (4 / 0) -> false
        let expr = (lit(1).gt(lit(2))).and(lit(4) / lit(0));
        let expected = lit(false);

        assert_eq!(simplify(expr), expected);
    }
}
