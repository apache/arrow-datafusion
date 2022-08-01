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

//! Simplify expressions optimizer rule

use crate::expr_simplifier::ExprSimplifiable;
use crate::{expr_simplifier::SimplifyInfo, OptimizerConfig, OptimizerRule};
use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    expr_fn::{and, or},
    expr_rewriter::RewriteRecursion,
    expr_rewriter::{ExprRewritable, ExprRewriter},
    lit,
    logical_plan::LogicalPlan,
    utils::from_plan,
    ColumnarValue, Expr, ExprSchemable, Operator, Volatility,
};
use datafusion_physical_expr::{create_physical_expr, execution_props::ExecutionProps};

/// Provides simplification information based on schema and properties
pub(crate) struct SimplifyContext<'a, 'b> {
    schemas: Vec<&'a DFSchemaRef>,
    props: &'b ExecutionProps,
}

impl<'a, 'b> SimplifyContext<'a, 'b> {
    /// Create a new SimplifyContext
    pub fn new(schemas: Vec<&'a DFSchemaRef>, props: &'b ExecutionProps) -> Self {
        Self { schemas, props }
    }
}

impl<'a, 'b> SimplifyInfo for SimplifyContext<'a, 'b> {
    /// returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        for schema in &self.schemas {
            if let Ok(DataType::Boolean) = expr.get_type(schema) {
                return Ok(true);
            }
        }

        Ok(false)
    }
    /// Returns true if expr is nullable
    fn nullable(&self, expr: &Expr) -> Result<bool> {
        self.schemas
            .iter()
            .find_map(|schema| {
                // expr may be from another input, so ignore errors
                // by converting to None to keep trying
                expr.nullable(schema.as_ref()).ok()
            })
            .ok_or_else(|| {
                // This means we weren't able to compute `Expr::nullable` with
                // *any* input schemas, signalling a problem
                DataFusionError::Internal(format!(
                    "Could not find find columns in '{}' during simplify",
                    expr
                ))
            })
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.props
    }
}

/// Optimizer Pass that simplifies [`LogicalPlan`]s by rewriting
/// [`Expr`]`s evaluating constants and applying algebraic
/// simplifications
///
/// # Introduction
/// It uses boolean algebra laws to simplify or reduce the number of terms in expressions.
///
/// # Example:
/// `Filter: b > 2 AND b > 2`
/// is optimized to
/// `Filter: b > 2`
///
#[derive(Default)]
pub struct SimplifyExpressions {}

/// returns true if `needle` is found in a chain of search_op
/// expressions. Such as: (A AND B) AND C
fn expr_contains(expr: &Expr, needle: &Expr, search_op: Operator) -> bool {
    match expr {
        Expr::BinaryExpr { left, op, right } if *op == search_op => {
            expr_contains(left, needle, search_op)
                || expr_contains(right, needle, search_op)
        }
        _ => expr == needle,
    }
}

fn is_one(s: &Expr) -> bool {
    match s {
        Expr::Literal(ScalarValue::Int8(Some(1)))
        | Expr::Literal(ScalarValue::Int16(Some(1)))
        | Expr::Literal(ScalarValue::Int32(Some(1)))
        | Expr::Literal(ScalarValue::Int64(Some(1)))
        | Expr::Literal(ScalarValue::UInt8(Some(1)))
        | Expr::Literal(ScalarValue::UInt16(Some(1)))
        | Expr::Literal(ScalarValue::UInt32(Some(1)))
        | Expr::Literal(ScalarValue::UInt64(Some(1))) => true,
        Expr::Literal(ScalarValue::Float32(Some(v))) if *v == 1. => true,
        Expr::Literal(ScalarValue::Float64(Some(v))) if *v == 1. => true,
        _ => false,
    }
}

fn is_true(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => *v,
        _ => false,
    }
}

/// returns true if expr is a
/// `Expr::Literal(ScalarValue::Boolean(v))` , false otherwise
fn is_bool_lit(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Boolean(_)))
}

/// Return a literal NULL value of Boolean data type
fn lit_bool_null() -> Expr {
    Expr::Literal(ScalarValue::Boolean(None))
}

fn is_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v) => v.is_null(),
        _ => false,
    }
}

fn is_false(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v))) => !(*v),
        _ => false,
    }
}

/// returns true if `haystack` looks like (needle OP X) or (X OP needle)
fn is_op_with(target_op: Operator, haystack: &Expr, needle: &Expr) -> bool {
    matches!(haystack, Expr::BinaryExpr { left, op, right } if op == &target_op && (needle == left.as_ref() || needle == right.as_ref()))
}

/// returns the contained boolean value in `expr` as
/// `Expr::Literal(ScalarValue::Boolean(v))`.
///
/// panics if expr is not a literal boolean
fn as_bool_lit(expr: Expr) -> Option<bool> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(v)) => v,
        _ => panic!("Expected boolean literal, got {:?}", expr),
    }
}

/// negate a Not clause
/// input is the clause to be negated.(args of Not clause)
/// For BinaryExpr, use the negator of op instead.
///    not ( A > B) ===> (A <= B)
/// For BoolExpr, not (A and B) ===> (not A) or (not B)
///     not (A or B) ===> (not A) and (not B)
///     not (not A) ===> A
/// For NullExpr, not (A is not null) ===> A is null
///     not (A is null) ===> A is not null
/// For InList, not (A not in (..)) ===> A in (..)
///     not (A in (..)) ===> A not in (..)
/// For Between, not (A between B and C) ===> (A not between B and C)
///     not (A not between B and C) ===> (A between B and C)
/// For others, use Not clause
fn negate_clause(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            match op {
                // not (A = B) ===> (A <> B)
                Operator::Eq => Expr::BinaryExpr {
                    left,
                    op: Operator::NotEq,
                    right,
                },
                // not (A <> B) ===> (A = B)
                Operator::NotEq => Expr::BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                },
                // not (A < B) ===> (A >= B)
                Operator::Lt => Expr::BinaryExpr {
                    left,
                    op: Operator::GtEq,
                    right,
                },
                // not (A <= B) ===> (A > B)
                Operator::LtEq => Expr::BinaryExpr {
                    left,
                    op: Operator::Gt,
                    right,
                },
                // not (A > B) ===> (A <= B)
                Operator::Gt => Expr::BinaryExpr {
                    left,
                    op: Operator::LtEq,
                    right,
                },
                // not (A >= B) ===> (A < B)
                Operator::GtEq => Expr::BinaryExpr {
                    left,
                    op: Operator::Lt,
                    right,
                },
                // not (A like 'B') ===> (A not like 'B')
                Operator::Like => Expr::BinaryExpr {
                    left,
                    op: Operator::NotLike,
                    right,
                },
                // not (A not like 'B') ===> (A like 'B')
                Operator::NotLike => Expr::BinaryExpr {
                    left,
                    op: Operator::Like,
                    right,
                },
                // not (A is distinct from B) ===> (A is not distinct from B)
                Operator::IsDistinctFrom => Expr::BinaryExpr {
                    left,
                    op: Operator::IsNotDistinctFrom,
                    right,
                },
                // not (A is not distinct from B) ===> (A is distinct from B)
                Operator::IsNotDistinctFrom => Expr::BinaryExpr {
                    left,
                    op: Operator::IsDistinctFrom,
                    right,
                },
                // not (A and B) ===> (not A) or (not B)
                Operator::And => {
                    let left = negate_clause(*left);
                    let right = negate_clause(*right);

                    or(left, right)
                }
                // not (A or B) ===> (not A) and (not B)
                Operator::Or => {
                    let left = negate_clause(*left);
                    let right = negate_clause(*right);

                    and(left, right)
                }
                // use not clause
                _ => Expr::Not(Box::new(Expr::BinaryExpr { left, op, right })),
            }
        }
        // not (not A) ===> A
        Expr::Not(expr) => *expr,
        // not (A is not null) ===> A is null
        Expr::IsNotNull(expr) => expr.is_null(),
        // not (A is null) ===> A is not null
        Expr::IsNull(expr) => expr.is_not_null(),
        // not (A not in (..)) ===> A in (..)
        // not (A in (..)) ===> A not in (..)
        Expr::InList {
            expr,
            list,
            negated,
        } => expr.in_list(list, !negated),
        // not (A between B and C) ===> (A not between B and C)
        // not (A not between B and C) ===> (A between B and C)
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr,
            negated: !negated,
            low,
            high,
        },
        // use not clause
        _ => Expr::Not(Box::new(expr)),
    }
}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut execution_props = ExecutionProps::new();
        execution_props.query_execution_start_time =
            optimizer_config.query_execution_start_time;
        self.optimize_internal(plan, &execution_props)
    }
}

impl SimplifyExpressions {
    fn optimize_internal(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        // We need to pass down the all schemas within the plan tree to `optimize_expr` in order to
        // to evaluate expression types. For example, a projection plan's schema will only include
        // projected columns. With just the projected schema, it's not possible to infer types for
        // expressions that references non-projected columns within the same project plan or its
        // children plans.
        let info = SimplifyContext::new(plan.all_schemas(), execution_props);

        let new_inputs = plan
            .inputs()
            .iter()
            .map(|input| self.optimize_internal(input, execution_props))
            .collect::<Result<Vec<_>>>()?;

        let expr = plan
            .expressions()
            .into_iter()
            .map(|e| {
                // We need to keep original expression name, if any.
                // Constant folding should not change expression name.
                let name = &e.name(plan.schema());

                // Apply the actual simplification logic
                let new_e = e.simplify(&info)?;

                let new_name = &new_e.name(plan.schema());

                if let (Ok(expr_name), Ok(new_expr_name)) = (name, new_name) {
                    if expr_name != new_expr_name {
                        Ok(new_e.alias(expr_name))
                    } else {
                        Ok(new_e)
                    }
                } else {
                    Ok(new_e)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        from_plan(plan, &expr, &new_inputs)
    }
}

impl SimplifyExpressions {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

#[allow(rustdoc::private_intra_doc_links)]
/// Partially evaluate `Expr`s so constant subtrees are evaluated at plan time.
///
/// Note it does not handle algebraic rewrites such as `(a or false)`
/// --> `a`, which is handled by [`Simplifier`]
///
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::simplify_expressions::ConstEvaluator;
/// # use datafusion_physical_expr::execution_props::ExecutionProps;
/// # use datafusion_expr::expr_rewriter::ExprRewritable;
///
/// let execution_props = ExecutionProps::new();
/// let mut const_evaluator = ConstEvaluator::new(&execution_props);
///
/// // (1 + 2) + a
/// let expr = (lit(1) + lit(2)) + col("a");
///
/// // is rewritten to (3 + a);
/// let rewritten = expr.rewrite(&mut const_evaluator).unwrap();
/// assert_eq!(rewritten, lit(3) + col("a"));
/// ```
pub struct ConstEvaluator<'a> {
    /// can_evaluate is used during the depth-first-search of the
    /// Expr tree to track if any siblings (or their descendants) were
    /// non evaluatable (e.g. had a column reference or volatile
    /// function)
    ///
    /// Specifically, `can_evaluate[N]` represents the state of
    /// traversal when we are N levels deep in the tree, one entry for
    /// this Expr and each of its parents.
    ///
    /// After visiting all siblings if can_evauate.top() is true, that
    /// means there were no non evaluatable siblings (or their
    /// descendants) so this Expr can be evaluated
    can_evaluate: Vec<bool>,

    execution_props: &'a ExecutionProps,
    input_schema: DFSchema,
    input_batch: RecordBatch,
}

impl<'a> ExprRewriter for ConstEvaluator<'a> {
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        // Default to being able to evaluate this node
        self.can_evaluate.push(true);

        // if this expr is not ok to evaluate, mark entire parent
        // stack as not ok (as all parents have at least one child or
        // descendant that can not be evaluated

        if !Self::can_evaluate(expr) {
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
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if self.can_evaluate.pop().unwrap() {
            let scalar = self.evaluate_to_scalar(expr)?;
            Ok(Expr::Literal(scalar))
        } else {
            Ok(expr)
        }
    }
}

impl<'a> ConstEvaluator<'a> {
    /// Create a new `ConstantEvaluator`. Session constants (such as
    /// the time for `now()` are taken from the passed
    /// `execution_props`.
    pub fn new(execution_props: &'a ExecutionProps) -> Self {
        let input_schema = DFSchema::empty();

        // The dummy column name is unused and doesn't matter as only
        // expressions without column references can be evaluated
        static DUMMY_COL_NAME: &str = ".";
        let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);

        // Need a single "input" row to produce a single output row
        let col = new_null_array(&DataType::Null, 1);
        let input_batch =
            RecordBatch::try_new(std::sync::Arc::new(schema), vec![col]).unwrap();

        Self {
            can_evaluate: vec![],
            execution_props,
            input_schema,
            input_batch,
        }
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
            | Expr::AggregateUDF { .. }
            | Expr::ScalarVariable(_, _)
            | Expr::Column(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::WindowFunction { .. }
            | Expr::Sort { .. }
            | Expr::GroupingSet(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => false,
            Expr::ScalarFunction { fun, .. } => Self::volatility_ok(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => Self::volatility_ok(fun.signature.volatility),
            Expr::Literal(_)
            | Expr::BinaryExpr { .. }
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Between { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::InList { .. }
            | Expr::GetIndexedField { .. } => true,
        }
    }

    /// Internal helper to evaluates an Expr
    pub(crate) fn evaluate_to_scalar(&self, expr: Expr) -> Result<ScalarValue> {
        if let Expr::Literal(s) = expr {
            return Ok(s);
        }

        let phys_expr = create_physical_expr(
            &expr,
            &self.input_schema,
            &self.input_batch.schema(),
            self.execution_props,
        )?;
        let col_val = phys_expr.evaluate(&self.input_batch)?;
        match col_val {
            ColumnarValue::Array(a) => {
                if a.len() != 1 {
                    Err(DataFusionError::Execution(format!(
                        "Could not evaluate the expressison, found a result of length {}",
                        a.len()
                    )))
                } else {
                    Ok(ScalarValue::try_from_array(&a, 0)?)
                }
            }
            ColumnarValue::Scalar(s) => Ok(s),
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
pub(crate) struct Simplifier<'a, S> {
    info: &'a S,
}

impl<'a, S> Simplifier<'a, S> {
    pub fn new(info: &'a S) -> Self {
        Self { info }
    }
}

impl<'a, S: SimplifyInfo> ExprRewriter for Simplifier<'a, S> {
    /// rewrite the expression simplifying any constant expressions
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        use Expr::*;
        use Operator::{And, Divide, Eq, Multiply, NotEq, Or};

        let info = self.info;
        let new_expr = match expr {
            //
            // Rules for Eq
            //

            // true = A  --> A
            // false = A --> !A
            // null = A --> null
            BinaryExpr {
                left,
                op: Eq,
                right,
            } if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                match as_bool_lit(*left) {
                    Some(true) => *right,
                    Some(false) => Not(right),
                    None => lit_bool_null(),
                }
            }
            // A = true  --> A
            // A = false --> !A
            // A = null --> null
            BinaryExpr {
                left,
                op: Eq,
                right,
            } if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                match as_bool_lit(*right) {
                    Some(true) => *left,
                    Some(false) => Not(left),
                    None => lit_bool_null(),
                }
            }

            //
            // Rules for NotEq
            //

            // true != A  --> !A
            // false != A --> A
            // null != A --> null
            BinaryExpr {
                left,
                op: NotEq,
                right,
            } if is_bool_lit(&left) && info.is_boolean_type(&right)? => {
                match as_bool_lit(*left) {
                    Some(true) => Not(right),
                    Some(false) => *right,
                    None => lit_bool_null(),
                }
            }
            // A != true  --> !A
            // A != false --> A
            // A != null --> null,
            BinaryExpr {
                left,
                op: NotEq,
                right,
            } if is_bool_lit(&right) && info.is_boolean_type(&left)? => {
                match as_bool_lit(*right) {
                    Some(true) => Not(left),
                    Some(false) => *left,
                    None => lit_bool_null(),
                }
            }

            //
            // Rules for OR
            //

            // true OR A --> true (even if A is null)
            BinaryExpr {
                left,
                op: Or,
                right: _,
            } if is_true(&left) => *left,
            // false OR A --> A
            BinaryExpr {
                left,
                op: Or,
                right,
            } if is_false(&left) => *right,
            // A OR true --> true (even if A is null)
            BinaryExpr {
                left: _,
                op: Or,
                right,
            } if is_true(&right) => *right,
            // A OR false --> A
            BinaryExpr {
                left,
                op: Or,
                right,
            } if is_false(&right) => *left,
            // (..A..) OR A --> (..A..)
            BinaryExpr {
                left,
                op: Or,
                right,
            } if expr_contains(&left, &right, Or) => *left,
            // A OR (..A..) --> (..A..)
            BinaryExpr {
                left,
                op: Or,
                right,
            } if expr_contains(&right, &left, Or) => *right,
            // A OR (A AND B) --> A (if B not null)
            BinaryExpr {
                left,
                op: Or,
                right,
            } if !info.nullable(&right)? && is_op_with(And, &right, &left) => *left,
            // (A AND B) OR A --> A (if B not null)
            BinaryExpr {
                left,
                op: Or,
                right,
            } if !info.nullable(&left)? && is_op_with(And, &left, &right) => *right,

            //
            // Rules for AND
            //

            // true AND A --> A
            BinaryExpr {
                left,
                op: And,
                right,
            } if is_true(&left) => *right,
            // false AND A --> false (even if A is null)
            BinaryExpr {
                left,
                op: And,
                right: _,
            } if is_false(&left) => *left,
            // A AND true --> A
            BinaryExpr {
                left,
                op: And,
                right,
            } if is_true(&right) => *left,
            // A AND false --> false (even if A is null)
            BinaryExpr {
                left: _,
                op: And,
                right,
            } if is_false(&right) => *right,
            // (..A..) AND A --> (..A..)
            BinaryExpr {
                left,
                op: And,
                right,
            } if expr_contains(&left, &right, And) => *left,
            // A AND (..A..) --> (..A..)
            BinaryExpr {
                left,
                op: And,
                right,
            } if expr_contains(&right, &left, And) => *right,
            // A AND (A OR B) --> A (if B not null)
            BinaryExpr {
                left,
                op: And,
                right,
            } if !info.nullable(&right)? && is_op_with(Or, &right, &left) => *left,
            // (A OR B) AND A --> A (if B not null)
            BinaryExpr {
                left,
                op: And,
                right,
            } if !info.nullable(&left)? && is_op_with(Or, &left, &right) => *right,

            //
            // Rules for Multiply
            //
            BinaryExpr {
                left,
                op: Multiply,
                right,
            } if is_one(&right) => *left,
            BinaryExpr {
                left,
                op: Multiply,
                right,
            } if is_one(&left) => *right,

            //
            // Rules for Divide
            //

            // A / 1 --> A
            BinaryExpr {
                left,
                op: Divide,
                right,
            } if is_one(&right) => *left,
            // A / null --> null
            BinaryExpr {
                left,
                op: Divide,
                right,
            } if left == right && is_null(&left) => *left,
            // A / A --> 1 (if a is not nullable)
            BinaryExpr {
                left,
                op: Divide,
                right,
            } if !info.nullable(&left)? && left == right => lit(1),

            //
            // Rules for Not
            //
            Not(inner) => negate_clause(*inner),

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
            Case {
                expr: None,
                when_then_expr,
                else_expr,
            } if !when_then_expr.is_empty()
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

            expr => {
                // no additional rewrites possible
                expr
            }
        };
        Ok(new_expr)
    }
}

/// A macro to assert that one string is contained within another with
/// a nice error message if they are not.
///
/// Usage: `assert_contains!(actual, expected)`
///
/// Is a macro so test error
/// messages are on the same line as the failure;
///
/// Both arguments must be convertable into Strings (Into<String>)
#[macro_export]
macro_rules! assert_contains {
    ($ACTUAL: expr, $EXPECTED: expr) => {
        let actual_value: String = $ACTUAL.into();
        let expected_value: String = $EXPECTED.into();
        assert!(
            actual_value.contains(&expected_value),
            "Can not find expected in actual.\n\nExpected:\n{}\n\nActual:\n{}",
            expected_value,
            actual_value
        );
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use chrono::{DateTime, TimeZone, Utc};
    use datafusion_common::DFField;
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        and, binary_expr, call_fn, col, create_udf, lit, lit_timestamp_nano,
        logical_plan::builder::LogicalPlanBuilder, BuiltinScalarFunction, Expr,
        ExprSchemable, ScalarUDF,
    };
    use datafusion_physical_expr::functions::make_scalar_function;
    use std::collections::HashMap;
    use std::sync::Arc;

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
    fn test_simplify_multiply_by_one() {
        let expr_a = binary_expr(col("c2"), Operator::Multiply, lit(1));
        let expr_b = binary_expr(lit(1), Operator::Multiply, col("c2"));
        let expected = col("c2");

        assert_eq!(simplify(expr_a), expected);
        assert_eq!(simplify(expr_b), expected);
    }

    #[test]
    fn test_simplify_divide_by_one() {
        let expr = binary_expr(col("c2"), Operator::Divide, lit(1));
        let expected = col("c2");

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_by_same() {
        let expr = binary_expr(col("c2"), Operator::Divide, col("c2"));
        // if c2 is null, c2 / c2 = null, so can't simplify
        let expected = expr.clone();

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_divide_by_same_non_null() {
        let expr = binary_expr(col("c2_non_null"), Operator::Divide, col("c2_non_null"));
        let expected = lit(1);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_simple_and() {
        // (c > 5) AND (c > 5)
        let expr = (col("c2").gt(lit(5))).and(col("c2").gt(lit(5)));
        let expected = col("c2").gt(lit(5));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_composed_and() {
        // ((c > 5) AND (c1 < 6)) AND (c > 5)
        let expr = binary_expr(
            binary_expr(col("c2").gt(lit(5)), Operator::And, col("c1").lt(lit(6))),
            Operator::And,
            col("c2").gt(lit(5)),
        );
        let expected =
            binary_expr(col("c2").gt(lit(5)), Operator::And, col("c1").lt(lit(6)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_negated_and() {
        // (c > 5) AND !(c > 5) -- > (c > 5) AND (c <= 5)
        let expr = binary_expr(
            col("c2").gt(lit(5)),
            Operator::And,
            Expr::not(col("c2").gt(lit(5))),
        );
        let expected = col("c2").gt(lit(5)).and(col("c2").lt_eq(lit(5)));

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_and() {
        let l = col("c2").gt(lit(5));
        let r = binary_expr(col("c1").lt(lit(6)), Operator::And, col("c2").gt(lit(5)));

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5))
        let expr = binary_expr(l.clone(), Operator::Or, r.clone());

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5)
        let expr = binary_expr(l, Operator::Or, r);

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_or_and_non_null() {
        let l = col("c2_non_null").gt(lit(5));
        let r = binary_expr(
            col("c1_non_null").lt(lit(6)),
            Operator::And,
            col("c2_non_null").gt(lit(5)),
        );

        // (c2 > 5) OR ((c1 < 6) AND (c2 > 5)) --> c2 > 5
        let expr = binary_expr(l.clone(), Operator::Or, r.clone());

        // This is only true if `c1 < 6` is not nullable / can not be null.
        let expected = col("c2_non_null").gt(lit(5));

        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) AND (c2 > 5)) OR (c2 > 5) --> c2 > 5
        let expr = binary_expr(l, Operator::Or, r);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_or() {
        let l = col("c2").gt(lit(5));
        let r = binary_expr(col("c1").lt(lit(6)), Operator::Or, col("c2").gt(lit(5)));

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = binary_expr(l.clone(), Operator::And, r.clone());

        // no rewrites if c1 can be null
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = binary_expr(l, Operator::And, r);
        let expected = expr.clone();
        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_and_or_non_null() {
        let l = col("c2_non_null").gt(lit(5));
        let r = binary_expr(
            col("c1_non_null").lt(lit(6)),
            Operator::Or,
            col("c2_non_null").gt(lit(5)),
        );

        // (c2 > 5) AND ((c1 < 6) OR (c2 > 5)) --> c2 > 5
        let expr = binary_expr(l.clone(), Operator::And, r.clone());

        // This is only true if `c1 < 6` is not nullable / can not be null.
        let expected = col("c2_non_null").gt(lit(5));

        assert_eq!(simplify(expr), expected);

        // ((c1 < 6) OR (c2 > 5)) AND (c2 > 5) --> c2 > 5
        let expr = binary_expr(l, Operator::And, r);

        assert_eq!(simplify(expr), expected);
    }

    #[test]
    fn test_simplify_null_and_false() {
        let expr = binary_expr(lit_bool_null(), Operator::And, lit(false));
        let expr_eq = lit(false);

        assert_eq!(simplify(expr), expr_eq);
    }

    #[test]
    fn test_simplify_divide_null_by_null() {
        let null = Expr::Literal(ScalarValue::Int32(None));
        let expr_plus = binary_expr(null.clone(), Operator::Divide, null.clone());
        let expr_eq = null;

        assert_eq!(simplify(expr_plus), expr_eq);
    }

    #[test]
    fn test_simplify_simplify_arithmetic_expr() {
        let expr_plus = binary_expr(lit(1), Operator::Plus, lit(1));
        let expr_eq = binary_expr(lit(1), Operator::Eq, lit(1));

        assert_eq!(simplify(expr_plus), lit(2));
        assert_eq!(simplify(expr_eq), lit(true));
    }

    // ------------------------------
    // --- ConstEvaluator tests -----
    // ------------------------------

    #[test]
    fn test_const_evaluator() {
        // true --> true
        test_evaluate(lit(true), lit(true));
        // true or true --> true
        test_evaluate(lit(true).or(lit(true)), lit(true));
        // true or false --> true
        test_evaluate(lit(true).or(lit(false)), lit(true));

        // "foo" == "foo" --> true
        test_evaluate(lit("foo").eq(lit("foo")), lit(true));
        // "foo" != "foo" --> false
        test_evaluate(lit("foo").not_eq(lit("foo")), lit(false));

        // c = 1 --> c = 1
        test_evaluate(col("c").eq(lit(1)), col("c").eq(lit(1)));
        // c = 1 + 2 --> c + 3
        test_evaluate(col("c").eq(lit(1) + lit(2)), col("c").eq(lit(3)));
        // (foo != foo) OR (c = 1) --> false OR (c = 1)
        test_evaluate(
            (lit("foo").not_eq(lit("foo"))).or(col("c").eq(lit(1))),
            lit(false).or(col("c").eq(lit(1))),
        );
    }

    #[test]
    fn test_const_evaluator_scalar_functions() {
        // concat("foo", "bar") --> "foobar"
        let expr = call_fn("concat", vec![lit("foo"), lit("bar")]).unwrap();
        test_evaluate(expr, lit("foobar"));

        // ensure arguments are also constant folded
        // concat("foo", concat("bar", "baz")) --> "foobarbaz"
        let concat1 = call_fn("concat", vec![lit("bar"), lit("baz")]).unwrap();
        let expr = call_fn("concat", vec![lit("foo"), concat1]).unwrap();
        test_evaluate(expr, lit("foobarbaz"));

        // Check non string arguments
        // to_timestamp("2020-09-08T12:00:00+00:00") --> timestamp(1599566400000000000i64)
        let expr =
            call_fn("to_timestamp", vec![lit("2020-09-08T12:00:00+00:00")]).unwrap();
        test_evaluate(expr, lit_timestamp_nano(1599566400000000000i64));

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = call_fn("to_timestamp", vec![col("a")]).unwrap();
        test_evaluate(expr.clone(), expr);

        // check that non foldable arguments are folded
        // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
        let expr = call_fn("to_timestamp", vec![col("a")]).unwrap();
        test_evaluate(expr.clone(), expr);

        // volatile / stable functions should not be evaluated
        // rand() + (1 + 2) --> rand() + 3
        let fun = BuiltinScalarFunction::Random;
        assert_eq!(fun.volatility(), Volatility::Volatile);
        let rand = Expr::ScalarFunction { args: vec![], fun };
        let expr = rand.clone() + (lit(1) + lit(2));
        let expected = rand + lit(3);
        test_evaluate(expr, expected);

        // parenthesization matters: can't rewrite
        // (rand() + 1) + 2 --> (rand() + 1) + 2)
        let fun = BuiltinScalarFunction::Random;
        let rand = Expr::ScalarFunction { args: vec![], fun };
        let expr = (rand + lit(1)) + lit(2);
        test_evaluate(expr.clone(), expr);
    }

    #[test]
    fn test_const_evaluator_now() {
        let ts_nanos = 1599566400000000000i64;
        let time = chrono::Utc.timestamp_nanos(ts_nanos);
        let ts_string = "2020-09-08T12:05:00+00:00";

        // now() --> ts
        test_evaluate_with_start_time(now_expr(), lit_timestamp_nano(ts_nanos), &time);

        // CAST(now() as int64) + 100 --> ts + 100
        let expr = cast_to_int64_expr(now_expr()) + lit(100);
        test_evaluate_with_start_time(expr, lit(ts_nanos + 100), &time);

        //  now() < cast(to_timestamp(...) as int) + 50000 ---> true
        let expr = cast_to_int64_expr(now_expr())
            .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000));
        test_evaluate_with_start_time(expr, lit(true), &time);
    }

    fn now_expr() -> Expr {
        call_fn("now", vec![]).unwrap()
    }

    fn cast_to_int64_expr(expr: Expr) -> Expr {
        Expr::Cast {
            expr: expr.into(),
            data_type: DataType::Int64,
        }
    }

    fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
        call_fn("to_timestamp", vec![lit(arg.into())]).unwrap()
    }

    #[test]
    fn test_evaluator_udfs() {
        let args = vec![lit(1) + lit(2), lit(30) + lit(40)];
        let folded_args = vec![lit(3), lit(70)];

        // immutable UDF should get folded
        // udf_add(1+2, 30+40) --> 73
        let expr = Expr::ScalarUDF {
            args: args.clone(),
            fun: make_udf_add(Volatility::Immutable),
        };
        test_evaluate(expr, lit(73));

        // stable UDF should be entirely folded
        // udf_add(1+2, 30+40) --> 73
        let fun = make_udf_add(Volatility::Stable);
        let expr = Expr::ScalarUDF {
            args: args.clone(),
            fun: Arc::clone(&fun),
        };
        test_evaluate(expr, lit(73));

        // volatile UDF should have args folded
        // udf_add(1+2, 30+40) --> udf_add(3, 70)
        let fun = make_udf_add(Volatility::Volatile);
        let expr = Expr::ScalarUDF {
            args,
            fun: Arc::clone(&fun),
        };
        let expected_expr = Expr::ScalarUDF {
            args: folded_args,
            fun: Arc::clone(&fun),
        };
        test_evaluate(expr, expected_expr);
    }

    // Make a UDF that adds its two values together, with the specified volatility
    fn make_udf_add(volatility: Volatility) -> Arc<ScalarUDF> {
        let input_types = vec![DataType::Int32, DataType::Int32];
        let return_type = Arc::new(DataType::Int32);

        let fun = |args: &[ArrayRef]| {
            let arg0 = &args[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");
            let arg1 = &args[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");

            // 2. perform the computation
            let array = arg0
                .iter()
                .zip(arg1.iter())
                .map(|args| {
                    if let (Some(arg0), Some(arg1)) = args {
                        Some(arg0 + arg1)
                    } else {
                        // one or both args were Null
                        None
                    }
                })
                .collect::<Int32Array>();

            Ok(Arc::new(array) as ArrayRef)
        };

        let fun = make_scalar_function(fun);
        Arc::new(create_udf(
            "udf_add",
            input_types,
            return_type,
            volatility,
            fun,
        ))
    }

    fn test_evaluate_with_start_time(
        input_expr: Expr,
        expected_expr: Expr,
        date_time: &DateTime<Utc>,
    ) {
        let execution_props = ExecutionProps {
            query_execution_start_time: *date_time,
            var_providers: None,
        };

        let mut const_evaluator = ConstEvaluator::new(&execution_props);
        let evaluated_expr = input_expr
            .clone()
            .rewrite(&mut const_evaluator)
            .expect("successfully evaluated");

        assert_eq!(
            evaluated_expr, expected_expr,
            "Mismatch evaluating {}\n  Expected:{}\n  Got:{}",
            input_expr, expected_expr, evaluated_expr
        );
    }

    fn test_evaluate(input_expr: Expr, expected_expr: Expr) {
        test_evaluate_with_start_time(input_expr, expected_expr, &Utc::now())
    }

    // ------------------------------
    // ----- Simplifier tests -------
    // ------------------------------

    fn simplify(expr: Expr) -> Expr {
        let schema = expr_test_schema();
        let execution_props = ExecutionProps::new();
        let info = SimplifyContext::new(vec![&schema], &execution_props);
        expr.simplify(&info).unwrap()
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "c1", DataType::Utf8, true),
                    DFField::new(None, "c2", DataType::Boolean, true),
                    DFField::new(None, "c1_non_null", DataType::Utf8, false),
                    DFField::new(None, "c2_non_null", DataType::Boolean, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn simplify_expr_not_not() {
        assert_eq!(simplify(col("c2").not().not().not()), col("c2").not(),);
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
    fn simplify_expr_eq() {
        let schema = expr_test_schema();
        assert_eq!(col("c2").get_type(&schema).unwrap(), DataType::Boolean);

        // true = ture -> true
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
        // CASE WHERE c2 != false THEN "ok" == "not_ok" ELSE c2 == true
        // -->
        // CASE WHERE c2 THEN false ELSE c2
        // -->
        // false
        assert_eq!(
            simplify(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit("not_ok"))),
                )],
                else_expr: Some(Box::new(col("c2").eq(lit(true)))),
            }),
            col("c2").not().and(col("c2")) // #1716
        );

        // CASE WHERE c2 != false THEN "ok" == "ok" ELSE c2
        // -->
        // CASE WHERE c2 THEN true ELSE c2
        // -->
        // c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2").not_eq(lit(false))),
                    Box::new(lit("ok").eq(lit("ok"))),
                )],
                else_expr: Some(Box::new(col("c2").eq(lit(true)))),
            })),
            col("c2").or(col("c2").not().and(col("c2"))) // #1716
        );

        // CASE WHERE ISNULL(c2) THEN true ELSE c2
        // -->
        // ISNULL(c2) OR c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case {
                expr: None,
                when_then_expr: vec![(
                    Box::new(col("c2").is_null()),
                    Box::new(lit(true)),
                )],
                else_expr: Some(Box::new(col("c2"))),
            })),
            col("c2")
                .is_null()
                .or(col("c2").is_not_null().and(col("c2")))
        );

        // CASE WHERE c1 then true WHERE c2 then false ELSE true
        // --> c1 OR (NOT(c1) AND c2 AND FALSE) OR (NOT(c1 OR c2) AND TRUE)
        // --> c1 OR (NOT(c1) AND NOT(c2))
        // --> c1 OR NOT(c2)
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case {
                expr: None,
                when_then_expr: vec![
                    (Box::new(col("c1")), Box::new(lit(true)),),
                    (Box::new(col("c2")), Box::new(lit(false)),)
                ],
                else_expr: Some(Box::new(lit(true))),
            })),
            col("c1").or(col("c1").not().and(col("c2").not()))
        );

        // CASE WHERE c1 then true WHERE c2 then true ELSE false
        // --> c1 OR (NOT(c1) AND c2 AND TRUE) OR (NOT(c1 OR c2) AND FALSE)
        // --> c1 OR (NOT(c1) AND c2)
        // --> c1 OR c2
        //
        // Need to call simplify 2x due to
        // https://github.com/apache/arrow-datafusion/issues/1160
        assert_eq!(
            simplify(simplify(Expr::Case {
                expr: None,
                when_then_expr: vec![
                    (Box::new(col("c1")), Box::new(lit(true)),),
                    (Box::new(col("c2")), Box::new(lit(false)),)
                ],
                else_expr: Some(Box::new(lit(true))),
            })),
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
        let expr = Expr::Between {
            expr: Box::new(col("c1")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        };
        let expr = expr.or(lit_bool_null());
        let result = simplify(expr.clone());
        assert_eq!(expr, result);
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
        // it can be either NULL or FALSE depending on the value of `c1 BETWEEN Int32(0) AND Int32(10`
        // and should not be rewritten
        let expr = Expr::Between {
            expr: Box::new(col("c1")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        };
        let expr = expr.and(lit_bool_null());
        let result = simplify(expr.clone());
        assert_eq!(expr, result);
    }

    // ------------------------------
    // -- SimplifyExpressions tests -
    // (test plans are simplified correctly)
    // ------------------------------

    fn test_table_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
        ]);
        table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SimplifyExpressions::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn test_simplify_optimized_plan() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])
            .unwrap()
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))
            .unwrap()
            .build()
            .unwrap();

        assert_optimized_plan_eq(
            &plan,
            "\
	        Filter: #test.b > Int32(1) AS test.b > Int32(1) AND test.b > Int32(1)\
            \n  Projection: #test.a\
            \n    TableScan: test",
        );
    }

    #[test]
    fn test_simplify_optimized_plan_with_composed_and() {
        let table_scan = test_table_scan();
        // ((c > 5) AND (d < 6)) AND (c > 5) --> (c > 5) AND (d < 6)
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])
            .unwrap()
            .filter(and(
                and(col("a").gt(lit(5)), col("b").lt(lit(6))),
                col("a").gt(lit(5)),
            ))
            .unwrap()
            .build()
            .unwrap();

        assert_optimized_plan_eq(
            &plan,
            "\
            Filter: #test.a > Int32(5) AND #test.b < Int32(6) AS test.a > Int32(5) AND test.b < Int32(6) AND test.a > Int32(5)\
            \n  Projection: #test.a, #test.b\
	        \n    TableScan: test",
        );
    }

    #[test]
    fn test_simplity_optimized_plan_eq_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(true)))
            .unwrap()
            .filter(col("c").eq(lit(false)))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: #test.a\
        \n  Filter: NOT #test.c AS test.c = Boolean(false)\
        \n    Filter: #test.b AS test.b = Boolean(true)\
        \n      TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_not_eq_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)))
            .unwrap()
            .filter(col("c").not_eq(lit(false)))
            .unwrap()
            .limit(None, Some(1))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: #test.a\
        \n  Limit: skip=None, fetch=1\
        \n    Filter: #test.c AS test.c != Boolean(false)\
        \n      Filter: NOT #test.b AS test.b != Boolean(true)\
        \n        TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_and_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).and(col("c").eq(lit(true))))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: #test.a\
        \n  Filter: NOT #test.b AND #test.c AS test.b != Boolean(true) AND test.c = Boolean(true)\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_or_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).or(col("c").eq(lit(false))))
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: #test.a\
        \n  Filter: NOT #test.b OR NOT #test.c AS test.b != Boolean(true) OR test.c = Boolean(false)\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_not_expr() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(false)).not())
            .unwrap()
            .project(vec![col("a")])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: #test.a\
        \n  Filter: #test.b AS NOT test.b = Boolean(false)\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_support_projection() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("d"), col("b").eq(lit(false))])
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Projection: #test.a, #test.d, NOT #test.b AS test.b = Boolean(false)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_support_aggregate() {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c"), col("b")])
            .unwrap()
            .aggregate(
                vec![col("a"), col("c")],
                vec![
                    datafusion_expr::max(col("b").eq(lit(true))),
                    datafusion_expr::min(col("b")),
                ],
            )
            .unwrap()
            .build()
            .unwrap();

        let expected = "\
        Aggregate: groupBy=[[#test.a, #test.c]], aggr=[[MAX(#test.b) AS MAX(test.b = Boolean(true)), MIN(#test.b)]]\
        \n  Projection: #test.a, #test.c, #test.b\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn test_simplity_optimized_plan_support_values() {
        let expr1 = Expr::BinaryExpr {
            left: Box::new(lit(1)),
            op: Operator::Plus,
            right: Box::new(lit(2)),
        };
        let expr2 = Expr::BinaryExpr {
            left: Box::new(lit(2)),
            op: Operator::Minus,
            right: Box::new(lit(1)),
        };
        let values = vec![vec![expr1, expr2]];
        let plan = LogicalPlanBuilder::values(values).unwrap().build().unwrap();

        let expected = "\
        Values: (Int32(3) AS Int32(1) + Int32(2), Int32(1) AS Int32(2) - Int32(1))";

        assert_optimized_plan_eq(&plan, expected);
    }

    // expect optimizing will result in an error, returning the error string
    fn get_optimized_plan_err(plan: &LogicalPlan, date_time: &DateTime<Utc>) -> String {
        let mut config = OptimizerConfig::new();
        config.query_execution_start_time = *date_time;
        let rule = SimplifyExpressions::new();

        let err = rule
            .optimize(plan, &mut config)
            .expect_err("expected optimization to fail");

        err.to_string()
    }

    fn get_optimized_plan_formatted(
        plan: &LogicalPlan,
        date_time: &DateTime<Utc>,
    ) -> String {
        let mut config = OptimizerConfig::new();
        config.query_execution_start_time = *date_time;
        let rule = SimplifyExpressions::new();

        let optimized_plan = rule
            .optimize(plan, &mut config)
            .expect("failed to optimize plan");
        format!("{:?}", optimized_plan)
    }

    #[test]
    fn to_timestamp_expr_folded() {
        let table_scan = test_table_scan();
        let proj = vec![to_timestamp_expr("2020-09-08T12:00:00+00:00")];

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: TimestampNanosecond(1599566400000000000, None) AS totimestamp(Utf8(\"2020-09-08T12:00:00+00:00\"))\
            \n  TableScan: test"
            .to_string();
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn to_timestamp_expr_wrong_arg() {
        let table_scan = test_table_scan();
        let proj = vec![to_timestamp_expr("I'M NOT A TIMESTAMP")];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Error parsing 'I'M NOT A TIMESTAMP' as timestamp";
        let actual = get_optimized_plan_err(&plan, &Utc::now());
        assert_contains!(actual, expected);
    }

    #[test]
    fn cast_expr() {
        let table_scan = test_table_scan();
        let proj = vec![Expr::Cast {
            expr: Box::new(lit("0")),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected = "Projection: Int32(0) AS CAST(Utf8(\"0\") AS Int32)\
            \n  TableScan: test";
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
    }

    #[test]
    fn cast_expr_wrong_arg() {
        let table_scan = test_table_scan();
        let proj = vec![Expr::Cast {
            expr: Box::new(lit("")),
            data_type: DataType::Int32,
        }];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let expected =
            "Cannot cast string '' to value of arrow::datatypes::types::Int32Type type";
        let actual = get_optimized_plan_err(&plan, &Utc::now());
        assert_contains!(actual, expected);
    }

    #[test]
    fn multiple_now_expr() {
        let table_scan = test_table_scan();
        let time = Utc::now();
        let proj = vec![
            now_expr(),
            Expr::Alias(Box::new(now_expr()), "t2".to_string()),
        ];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        // expect the same timestamp appears in both exprs
        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = format!(
            "Projection: TimestampNanosecond({}, Some(\"UTC\")) AS now(), TimestampNanosecond({}, Some(\"UTC\")) AS t2\
            \n  TableScan: test",
            time.timestamp_nanos(),
            time.timestamp_nanos()
        );

        assert_eq!(actual, expected);
    }

    #[test]
    fn simplify_and_eval() {
        // demonstrate a case where the evaluation needs to run prior
        // to the simplifier for it to work
        let table_scan = test_table_scan();
        let time = Utc::now();
        // (true or false) != col --> !col
        let proj = vec![lit(true).or(lit(false)).not_eq(col("a"))];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)
            .unwrap()
            .build()
            .unwrap();

        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected =
            "Projection: NOT #test.a AS Boolean(true) OR Boolean(false) != test.a\
                        \n  TableScan: test";

        assert_eq!(actual, expected);
    }

    #[test]
    fn now_less_than_timestamp() {
        let table_scan = test_table_scan();

        let ts_string = "2020-09-08T12:05:00+00:00";
        let time = chrono::Utc.timestamp_nanos(1599566400000000000i64);

        //  now() < cast(to_timestamp(...) as int) + 5000000000
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                cast_to_int64_expr(now_expr())
                    .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000)),
            )
            .unwrap()
            .build()
            .unwrap();

        // Note that constant folder runs and folds the entire
        // expression down to a single constant (true)
        let expected = "Filter: Boolean(true) AS CAST(now() AS Int64) < CAST(totimestamp(Utf8(\"2020-09-08T12:05:00+00:00\")) AS Int64) + Int32(50000)\
                        \n  TableScan: test";
        let actual = get_optimized_plan_formatted(&plan, &time);

        assert_eq!(expected, actual);
    }

    #[test]
    fn select_date_plus_interval() {
        let table_scan = test_table_scan();

        let ts_string = "2020-09-08T12:05:00+00:00";
        let time = chrono::Utc.timestamp_nanos(1599566400000000000i64);

        //  now() < cast(to_timestamp(...) as int) + 5000000000
        let schema = table_scan.schema();

        let date_plus_interval_expr = to_timestamp_expr(ts_string)
            .cast_to(&DataType::Date32, schema)
            .unwrap()
            + Expr::Literal(ScalarValue::IntervalDayTime(Some(123i64 << 32)));

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .project(vec![date_plus_interval_expr])
            .unwrap()
            .build()
            .unwrap();

        println!("{:?}", plan);

        // Note that constant folder runs and folds the entire
        // expression down to a single constant (true)
        let expected = r#"Projection: Date32("18636") AS CAST(totimestamp(Utf8("2020-09-08T12:05:00+00:00")) AS Date32) + IntervalDayTime("528280977408")
  TableScan: test"#;
        let actual = get_optimized_plan_formatted(&plan, &time);

        assert_eq!(actual, expected);
    }

    #[test]
    fn simplify_not_binary() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d <= Int32(10) AS NOT test.d > Int32(10)\
            \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_bool_and() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).and(col("d").lt(lit(100))).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d <= Int32(10) OR #test.d >= Int32(100) AS NOT test.d > Int32(10) AND test.d < Int32(100)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_bool_or() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).or(col("d").lt(lit(100))).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d <= Int32(10) AND #test.d >= Int32(100) AS NOT test.d > Int32(10) OR test.d < Int32(100)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not().not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d > Int32(10) AS NOT NOT test.d > Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_null() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_null().not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d IS NOT NULL AS NOT test.d IS NULL\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_null() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_not_null().not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d IS NULL AS NOT test.d IS NOT NULL\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_in() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], false).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d NOT IN ([Int32(1), Int32(2), Int32(3)]) AS NOT test.d IN (Map { iter: Iter([Int32(1), Int32(2), Int32(3)]) })\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_in() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], true).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d IN ([Int32(1), Int32(2), Int32(3)]) AS NOT test.d NOT IN (Map { iter: Iter([Int32(1), Int32(2), Int32(3)]) })\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_between() {
        let table_scan = test_table_scan();
        let qual = Expr::Between {
            expr: Box::new(col("d")),
            negated: false,
            low: Box::new(lit(1)),
            high: Box::new(lit(10)),
        };

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d NOT BETWEEN Int32(1) AND Int32(10) AS NOT test.d BETWEEN Int32(1) AND Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_between() {
        let table_scan = test_table_scan();
        let qual = Expr::Between {
            expr: Box::new(col("d")),
            negated: true,
            low: Box::new(lit(1)),
            high: Box::new(lit(10)),
        };

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d BETWEEN Int32(1) AND Int32(10) AS NOT test.d NOT BETWEEN Int32(1) AND Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_like() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").like(col("b")).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.a NOT LIKE #test.b AS NOT test.a LIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_like() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").not_like(col("b")).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.a LIKE #test.b AS NOT test.a NOT LIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_distinct_from() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsDistinctFrom, lit(10)).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d IS NOT DISTINCT FROM Int32(10) AS NOT test.d IS DISTINCT FROM Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }

    #[test]
    fn simplify_not_not_distinct_from() {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsNotDistinctFrom, lit(10)).not())
            .unwrap()
            .build()
            .unwrap();
        let expected = "Filter: #test.d IS DISTINCT FROM Int32(10) AS NOT test.d IS NOT DISTINCT FROM Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected);
    }
}
