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

//! Expr module contains core type definition for `Expr`.

use crate::built_in_function;
use crate::expr_fn::binary_expr;
use crate::logical_plan::Subquery;
use crate::udaf;
use crate::utils::{expr_to_columns, find_out_reference_exprs};
use crate::window_frame;
use crate::window_function;
use crate::Operator;
use crate::{aggregate_function, ExprSchemable};
use datafusion_common::logical_type::LogicalType;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{internal_err, DFSchema};
use datafusion_common::{plan_err, Column, DataFusionError, Result, ScalarValue};
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter, Write};
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Arc;

/// `Expr` is a central struct of DataFusion's query API, and
/// represent logical expressions such as `A + 1`, or `CAST(c1 AS
/// int)`.
///
/// An `Expr` can compute its [DataType]
/// and nullability, and has functions for building up complex
/// expressions.
///
/// # Examples
///
/// ## Create an expression `c1` referring to column named "c1"
/// ```
/// # use datafusion_common::Column;
/// # use datafusion_expr::{lit, col, Expr};
/// let expr = col("c1");
/// assert_eq!(expr, Expr::Column(Column::from_name("c1")));
/// ```
///
/// ## Create the expression `c1 + c2` to add columns "c1" and "c2" together
/// ```
/// # use datafusion_expr::{lit, col, Operator, Expr};
/// let expr = col("c1") + col("c2");
///
/// assert!(matches!(expr, Expr::BinaryExpr { ..} ));
/// if let Expr::BinaryExpr(binary_expr) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   assert_eq!(*binary_expr.right, col("c2"));
///   assert_eq!(binary_expr.op, Operator::Plus);
/// }
/// ```
///
/// ## Create expression `c1 = 42` to compare the value in column "c1" to the literal value `42`
/// ```
/// # use datafusion_common::ScalarValue;
/// # use datafusion_expr::{lit, col, Operator, Expr};
/// let expr = col("c1").eq(lit(42_i32));
///
/// assert!(matches!(expr, Expr::BinaryExpr { .. } ));
/// if let Expr::BinaryExpr(binary_expr) = expr {
///   assert_eq!(*binary_expr.left, col("c1"));
///   let scalar = ScalarValue::Int32(Some(42));
///   assert_eq!(*binary_expr.right, Expr::Literal(scalar));
///   assert_eq!(binary_expr.op, Operator::Eq);
/// }
/// ```
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Alias),
    /// A named reference to a qualified filed in a schema.
    Column(Column),
    /// A named reference to a variable in a registry.
    ScalarVariable(LogicalType, Vec<String>),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// LIKE expression
    Like(Like),
    /// LIKE expression that uses regular expressions
    SimilarTo(Like),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// True if argument is not NULL, false otherwise. This expression itself is never NULL.
    IsNotNull(Box<Expr>),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsNull(Box<Expr>),
    /// True if argument is true, false otherwise. This expression itself is never NULL.
    IsTrue(Box<Expr>),
    /// True if argument is  false, false otherwise. This expression itself is never NULL.
    IsFalse(Box<Expr>),
    /// True if argument is NULL, false otherwise. This expression itself is never NULL.
    IsUnknown(Box<Expr>),
    /// True if argument is FALSE or NULL, false otherwise. This expression itself is never NULL.
    IsNotTrue(Box<Expr>),
    /// True if argument is TRUE OR NULL, false otherwise. This expression itself is never NULL.
    IsNotFalse(Box<Expr>),
    /// True if argument is TRUE or FALSE, false otherwise. This expression itself is never NULL.
    IsNotUnknown(Box<Expr>),
    /// arithmetic negation of an expression, the operand must be of a signed numeric data type
    Negative(Box<Expr>),
    /// Returns the field of a [`arrow::array::ListArray`] or
    /// [`arrow::array::StructArray`] by index or range
    GetIndexedField(GetIndexedField),
    /// Whether an expression is between a given range.
    Between(Between),
    /// The CASE expression is similar to a series of nested if/else and there are two forms that
    /// can be used. The first form consists of a series of boolean "when" expressions with
    /// corresponding "then" expressions, and an optional "else" expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    ///
    /// The second form uses a base expression and then a series of "when" clauses that match on a
    /// literal value.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    Case(Case),
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast(Cast),
    /// Casts the expression to a given type and will return a null value if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    TryCast(TryCast),
    /// A sort expression, that can be used to sort values.
    Sort(Sort),
    /// Represents the call of a built-in scalar function with a set of arguments.
    ScalarFunction(ScalarFunction),
    /// Represents the call of a user-defined scalar function with arguments.
    ScalarUDF(ScalarUDF),
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction(AggregateFunction),
    /// Represents the call of a window function with arguments.
    WindowFunction(WindowFunction),
    /// aggregate function
    AggregateUDF(AggregateUDF),
    /// Returns whether the list contains the expr value.
    InList(InList),
    /// EXISTS subquery
    Exists(Exists),
    /// IN subquery
    InSubquery(InSubquery),
    /// Scalar subquery
    ScalarSubquery(Subquery),
    /// Represents a reference to all available fields.
    ///
    /// This expr has to be resolved to a list of columns before translating logical
    /// plan into physical plan.
    Wildcard,
    /// Represents a reference to all available fields in a specific schema.
    ///    
    /// This expr has to be resolved to a list of columns before translating logical
    /// plan into physical plan.
    QualifiedWildcard { qualifier: String },
    /// List of grouping set expressions. Only valid in the context of an aggregate
    /// GROUP BY expression list
    GroupingSet(GroupingSet),
    /// A place holder for parameters in a prepared statement
    /// (e.g. `$foo` or `$1`)
    Placeholder(Placeholder),
    /// A place holder which hold a reference to a qualified field
    /// in the outer query, used for correlated sub queries.
    OuterReferenceColumn(LogicalType, Column),
}

/// Alias expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Alias {
    pub expr: Box<Expr>,
    pub name: String,
}

impl Alias {
    pub fn new(expr: Expr, name: impl Into<String>) -> Self {
        Self {
            expr: Box::new(expr),
            name: name.into(),
        }
    }
}

/// Binary expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct BinaryExpr {
    /// Left-hand side of the expression
    pub left: Box<Expr>,
    /// The comparison operator
    pub op: Operator,
    /// Right-hand side of the expression
    pub right: Box<Expr>,
}

impl BinaryExpr {
    /// Create a new binary expression
    pub fn new(left: Box<Expr>, op: Operator, right: Box<Expr>) -> Self {
        Self { left, op, right }
    }
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Put parentheses around child binary expressions so that we can see the difference
        // between `(a OR b) AND c` and `a OR (b AND c)`. We only insert parentheses when needed,
        // based on operator precedence. For example, `(a AND b) OR c` and `a AND b OR c` are
        // equivalent and the parentheses are not necessary.

        fn write_child(
            f: &mut Formatter<'_>,
            expr: &Expr,
            precedence: u8,
        ) -> fmt::Result {
            match expr {
                Expr::BinaryExpr(child) => {
                    let p = child.op.precedence();
                    if p == 0 || p < precedence {
                        write!(f, "({child})")?;
                    } else {
                        write!(f, "{child}")?;
                    }
                }
                _ => write!(f, "{expr}")?,
            }
            Ok(())
        }

        let precedence = self.op.precedence();
        write_child(f, self.left.as_ref(), precedence)?;
        write!(f, " {} ", self.op)?;
        write_child(f, self.right.as_ref(), precedence)
    }
}

/// CASE expression
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Case {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub expr: Option<Box<Expr>>,
    /// One or more when/then expressions
    pub when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
    /// Optional "else" expression
    pub else_expr: Option<Box<Expr>>,
}

impl Case {
    /// Create a new Case expression
    pub fn new(
        expr: Option<Box<Expr>>,
        when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
        else_expr: Option<Box<Expr>>,
    ) -> Self {
        Self {
            expr,
            when_then_expr,
            else_expr,
        }
    }
}

/// LIKE expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
    /// Whether to ignore case on comparing
    pub case_insensitive: bool,
}

impl Like {
    /// Create a new Like expression
    pub fn new(
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
        case_insensitive: bool,
    ) -> Self {
        Self {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }
    }
}

/// BETWEEN expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Between {
    /// The value to compare
    pub expr: Box<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
    /// The low end of the range
    pub low: Box<Expr>,
    /// The high end of the range
    pub high: Box<Expr>,
}

impl Between {
    /// Create a new Between expression
    pub fn new(expr: Box<Expr>, negated: bool, low: Box<Expr>, high: Box<Expr>) -> Self {
        Self {
            expr,
            negated,
            low,
            high,
        }
    }
}

/// ScalarFunction expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ScalarFunction {
    /// The function
    pub fun: built_in_function::BuiltinScalarFunction,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
}

impl ScalarFunction {
    /// Create a new ScalarFunction expression
    pub fn new(fun: built_in_function::BuiltinScalarFunction, args: Vec<Expr>) -> Self {
        Self { fun, args }
    }
}

/// ScalarUDF expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ScalarUDF {
    /// The function
    pub fun: Arc<crate::ScalarUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
}

impl ScalarUDF {
    /// Create a new ScalarUDF expression
    pub fn new(fun: Arc<crate::ScalarUDF>, args: Vec<Expr>) -> Self {
        Self { fun, args }
    }
}

/// Access a sub field of a nested type, such as `Field` or `List`
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum GetFieldAccess {
    /// Named field, for example `struct["name"]`
    NamedStructField { name: ScalarValue },
    /// Single list index, for example: `list[i]`
    ListIndex { key: Box<Expr> },
    /// List range, for example `list[i:j]`
    ListRange { start: Box<Expr>, stop: Box<Expr> },
}

/// Returns the field of a [`arrow::array::ListArray`] or
/// [`arrow::array::StructArray`] by `key`. See [`GetFieldAccess`] for
/// details.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct GetIndexedField {
    /// The expression to take the field from
    pub expr: Box<Expr>,
    /// The name of the field to take
    pub field: GetFieldAccess,
}

impl GetIndexedField {
    /// Create a new GetIndexedField expression
    pub fn new(expr: Box<Expr>, field: GetFieldAccess) -> Self {
        Self { expr, field }
    }
}

/// Cast expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: LogicalType,
}

impl Cast {
    /// Create a new Cast expression
    pub fn new(expr: Box<Expr>, data_type: LogicalType) -> Self {
        Self { expr, data_type }
    }
}

/// TryCast Expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TryCast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: LogicalType,
}

impl TryCast {
    /// Create a new TryCast expression
    pub fn new(expr: Box<Expr>, data_type: LogicalType) -> Self {
        Self { expr, data_type }
    }
}

/// SORT expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Sort {
    /// The expression to sort on
    pub expr: Box<Expr>,
    /// The direction of the sort
    pub asc: bool,
    /// Whether to put Nulls before all other data values
    pub nulls_first: bool,
}

impl Sort {
    /// Create a new Sort expression
    pub fn new(expr: Box<Expr>, asc: bool, nulls_first: bool) -> Self {
        Self {
            expr,
            asc,
            nulls_first,
        }
    }
}

/// Aggregate function
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateFunction {
    /// Name of the function
    pub fun: aggregate_function::AggregateFunction,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ordering
    pub order_by: Option<Vec<Expr>>,
}

impl AggregateFunction {
    pub fn new(
        fun: aggregate_function::AggregateFunction,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Option<Vec<Expr>>,
    ) -> Self {
        Self {
            fun,
            args,
            distinct,
            filter,
            order_by,
        }
    }
}

/// Window function
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WindowFunction {
    /// Name of the function
    pub fun: window_function::WindowFunction,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// List of partition by expressions
    pub partition_by: Vec<Expr>,
    /// List of order by expressions
    pub order_by: Vec<Expr>,
    /// Window frame
    pub window_frame: window_frame::WindowFrame,
}

impl WindowFunction {
    /// Create a new Window expression
    pub fn new(
        fun: window_function::WindowFunction,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<Expr>,
        window_frame: window_frame::WindowFrame,
    ) -> Self {
        Self {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        }
    }
}

// Exists expression.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Exists {
    /// subquery that will produce a single column of data
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl Exists {
    // Create a new Exists expression.
    pub fn new(subquery: Subquery, negated: bool) -> Self {
        Self { subquery, negated }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct AggregateUDF {
    /// The function
    pub fun: Arc<udaf::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
    /// Optional ORDER BY applied prior to aggregating
    pub order_by: Option<Vec<Expr>>,
}

impl AggregateUDF {
    /// Create a new AggregateUDF expression
    pub fn new(
        fun: Arc<udaf::AggregateUDF>,
        args: Vec<Expr>,
        filter: Option<Box<Expr>>,
        order_by: Option<Vec<Expr>>,
    ) -> Self {
        Self {
            fun,
            args,
            filter,
            order_by,
        }
    }
}

/// InList expression
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InList {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// The list of values to compare against
    pub list: Vec<Expr>,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InList {
    /// Create a new InList expression
    pub fn new(expr: Box<Expr>, list: Vec<Expr>, negated: bool) -> Self {
        Self {
            expr,
            list,
            negated,
        }
    }
}

/// IN subquery
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InSubquery {
    /// The expression to compare
    pub expr: Box<Expr>,
    /// Subquery that will produce a single column of data to compare against
    pub subquery: Subquery,
    /// Whether the expression is negated
    pub negated: bool,
}

impl InSubquery {
    /// Create a new InSubquery expression
    pub fn new(expr: Box<Expr>, subquery: Subquery, negated: bool) -> Self {
        Self {
            expr,
            subquery,
            negated,
        }
    }
}

/// Placeholder, representing bind parameter values such as `$1`.
///
/// The type of these parameters is inferred using [`Expr::infer_placeholder_types`]
/// or can be specified directly using `PREPARE` statements.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Placeholder {
    /// The identifier of the parameter, including the leading `$` (e.g, `"$1"` or `"$foo"`)
    pub id: String,
    /// The type the parameter will be filled in with
    pub data_type: Option<LogicalType>,
}

impl Placeholder {
    /// Create a new Placeholder expression
    pub fn new(id: String, data_type: Option<LogicalType>) -> Self {
        Self { id, data_type }
    }
}

/// Grouping sets
/// See <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>
/// for Postgres definition.
/// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
/// for Apache Spark definition.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum GroupingSet {
    /// Rollup grouping sets
    Rollup(Vec<Expr>),
    /// Cube grouping sets
    Cube(Vec<Expr>),
    /// User-defined grouping sets
    GroupingSets(Vec<Vec<Expr>>),
}

impl GroupingSet {
    /// Return all distinct exprs in the grouping set. For `CUBE` and `ROLLUP` this
    /// is just the underlying list of exprs. For `GROUPING SET` we need to deduplicate
    /// the exprs in the underlying sets.
    pub fn distinct_expr(&self) -> Vec<Expr> {
        match self {
            GroupingSet::Rollup(exprs) => exprs.clone(),
            GroupingSet::Cube(exprs) => exprs.clone(),
            GroupingSet::GroupingSets(groups) => {
                let mut exprs: Vec<Expr> = vec![];
                for exp in groups.iter().flatten() {
                    if !exprs.contains(exp) {
                        exprs.push(exp.clone());
                    }
                }
                exprs
            }
        }
    }
}

/// Fixed seed for the hashing so that Ords are consistent across runs
const SEED: ahash::RandomState = ahash::RandomState::with_seeds(0, 0, 0, 0);

impl PartialOrd for Expr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let mut hasher = SEED.build_hasher();
        self.hash(&mut hasher);
        let s = hasher.finish();

        let mut hasher = SEED.build_hasher();
        other.hash(&mut hasher);
        let o = hasher.finish();

        Some(s.cmp(&o))
    }
}

impl Expr {
    /// Returns the name of this expression as it should appear in a schema. This name
    /// will not include any CAST expressions.
    pub fn display_name(&self) -> Result<String> {
        create_name(self)
    }

    /// Returns the name of this expression as it should appear in a schema. This name
    /// will not include any CAST expressions.
    #[deprecated(since = "14.0.0", note = "please use `display_name` instead")]
    pub fn name(&self) -> Result<String> {
        self.display_name()
    }

    /// Returns a full and complete string representation of this expression.
    pub fn canonical_name(&self) -> String {
        format!("{self}")
    }

    /// Return String representation of the variant represented by `self`
    /// Useful for non-rust based bindings
    pub fn variant_name(&self) -> &str {
        match self {
            Expr::AggregateFunction { .. } => "AggregateFunction",
            Expr::AggregateUDF { .. } => "AggregateUDF",
            Expr::Alias(..) => "Alias",
            Expr::Between { .. } => "Between",
            Expr::BinaryExpr { .. } => "BinaryExpr",
            Expr::Case { .. } => "Case",
            Expr::Cast { .. } => "Cast",
            Expr::Column(..) => "Column",
            Expr::OuterReferenceColumn(_, _) => "Outer",
            Expr::Exists { .. } => "Exists",
            Expr::GetIndexedField { .. } => "GetIndexedField",
            Expr::GroupingSet(..) => "GroupingSet",
            Expr::InList { .. } => "InList",
            Expr::InSubquery(..) => "InSubquery",
            Expr::IsNotNull(..) => "IsNotNull",
            Expr::IsNull(..) => "IsNull",
            Expr::Like { .. } => "Like",
            Expr::SimilarTo { .. } => "RLike",
            Expr::IsTrue(..) => "IsTrue",
            Expr::IsFalse(..) => "IsFalse",
            Expr::IsUnknown(..) => "IsUnknown",
            Expr::IsNotTrue(..) => "IsNotTrue",
            Expr::IsNotFalse(..) => "IsNotFalse",
            Expr::IsNotUnknown(..) => "IsNotUnknown",
            Expr::Literal(..) => "Literal",
            Expr::Negative(..) => "Negative",
            Expr::Not(..) => "Not",
            Expr::Placeholder(_) => "Placeholder",
            Expr::QualifiedWildcard { .. } => "QualifiedWildcard",
            Expr::ScalarFunction(..) => "ScalarFunction",
            Expr::ScalarSubquery { .. } => "ScalarSubquery",
            Expr::ScalarUDF(..) => "ScalarUDF",
            Expr::ScalarVariable(..) => "ScalarVariable",
            Expr::Sort { .. } => "Sort",
            Expr::TryCast { .. } => "TryCast",
            Expr::WindowFunction { .. } => "WindowFunction",
            Expr::Wildcard => "Wildcard",
        }
    }

    /// Return `self == other`
    pub fn eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Eq, other)
    }

    /// Return `self != other`
    pub fn not_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::NotEq, other)
    }

    /// Return `self > other`
    pub fn gt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Gt, other)
    }

    /// Return `self >= other`
    pub fn gt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::GtEq, other)
    }

    /// Return `self < other`
    pub fn lt(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Lt, other)
    }

    /// Return `self <= other`
    pub fn lt_eq(self, other: Expr) -> Expr {
        binary_expr(self, Operator::LtEq, other)
    }

    /// Return `self && other`
    pub fn and(self, other: Expr) -> Expr {
        binary_expr(self, Operator::And, other)
    }

    /// Return `self || other`
    pub fn or(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Or, other)
    }

    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(self),
            Box::new(other),
            None,
            false,
        ))
    }

    /// Return `self NOT LIKE other`
    pub fn not_like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            true,
            Box::new(self),
            Box::new(other),
            None,
            false,
        ))
    }

    /// Return `self ILIKE other`
    pub fn ilike(self, other: Expr) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(self),
            Box::new(other),
            None,
            true,
        ))
    }

    /// Return `self NOT ILIKE other`
    pub fn not_ilike(self, other: Expr) -> Expr {
        Expr::Like(Like::new(true, Box::new(self), Box::new(other), None, true))
    }

    /// Return the name to use for the specific Expr, recursing into
    /// `Expr::Sort` as appropriate
    pub fn name_for_alias(&self) -> Result<String> {
        match self {
            // call Expr::display_name() on a Expr::Sort will throw an error
            Expr::Sort(Sort { expr, .. }) => expr.name_for_alias(),
            expr => expr.display_name(),
        }
    }

    /// Ensure `expr` has the name as `original_name` by adding an
    /// alias if necessary.
    pub fn alias_if_changed(self, original_name: String) -> Result<Expr> {
        let new_name = self.name_for_alias()?;

        if new_name == original_name {
            return Ok(self);
        }

        Ok(self.alias(original_name))
    }

    /// Return `self AS name` alias expression
    pub fn alias(self, name: impl Into<String>) -> Expr {
        match self {
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => Expr::Sort(Sort::new(Box::new(expr.alias(name)), asc, nulls_first)),
            _ => Expr::Alias(Alias::new(self, name.into())),
        }
    }

    /// Remove an alias from an expression if one exists.
    pub fn unalias(self) -> Expr {
        match self {
            Expr::Alias(alias) => alias.expr.as_ref().clone(),
            _ => self,
        }
    }

    /// Return `self IN <list>` if `negated` is false, otherwise
    /// return `self NOT IN <list>`.a
    pub fn in_list(self, list: Vec<Expr>, negated: bool) -> Expr {
        Expr::InList(InList::new(Box::new(self), list, negated))
    }

    /// Return `IsNull(Box(self))
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    /// Return `IsNotNull(Box(self))
    pub fn is_not_null(self) -> Expr {
        Expr::IsNotNull(Box::new(self))
    }

    /// Create a sort expression from an existing expression.
    ///
    /// ```
    /// # use datafusion_expr::col;
    /// let sort_expr = col("foo").sort(true, true); // SORT ASC NULLS_FIRST
    /// ```
    pub fn sort(self, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort(Sort::new(Box::new(self), asc, nulls_first))
    }

    /// Return `IsTrue(Box(self))`
    pub fn is_true(self) -> Expr {
        Expr::IsTrue(Box::new(self))
    }

    /// Return `IsNotTrue(Box(self))`
    pub fn is_not_true(self) -> Expr {
        Expr::IsNotTrue(Box::new(self))
    }

    /// Return `IsFalse(Box(self))`
    pub fn is_false(self) -> Expr {
        Expr::IsFalse(Box::new(self))
    }

    /// Return `IsNotFalse(Box(self))`
    pub fn is_not_false(self) -> Expr {
        Expr::IsNotFalse(Box::new(self))
    }

    /// Return `IsUnknown(Box(self))`
    pub fn is_unknown(self) -> Expr {
        Expr::IsUnknown(Box::new(self))
    }

    /// Return `IsNotUnknown(Box(self))`
    pub fn is_not_unknown(self) -> Expr {
        Expr::IsNotUnknown(Box::new(self))
    }

    /// return `self BETWEEN low AND high`
    pub fn between(self, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between::new(
            Box::new(self),
            false,
            Box::new(low),
            Box::new(high),
        ))
    }

    /// return `self NOT BETWEEN low AND high`
    pub fn not_between(self, low: Expr, high: Expr) -> Expr {
        Expr::Between(Between::new(
            Box::new(self),
            true,
            Box::new(low),
            Box::new(high),
        ))
    }

    /// Return access to the named field. Example `expr["name"]`
    ///
    /// ## Access field "my_field" from column "c1"
    ///
    /// For example if column "c1" holds documents like this
    ///
    /// ```json
    /// {
    ///   "my_field": 123.34,
    ///   "other_field": "Boston",
    /// }
    /// ```
    ///
    /// You can access column "my_field" with
    ///
    /// ```
    /// # use datafusion_expr::{col};
    /// let expr = col("c1")
    ///    .field("my_field");
    /// assert_eq!(expr.display_name().unwrap(), "c1[my_field]");
    /// ```
    pub fn field(self, name: impl Into<String>) -> Self {
        Expr::GetIndexedField(GetIndexedField {
            expr: Box::new(self),
            field: GetFieldAccess::NamedStructField {
                name: ScalarValue::Utf8(Some(name.into())),
            },
        })
    }

    /// Return access to the element field. Example `expr["name"]`
    ///
    /// ## Example Access element 2 from column "c1"
    ///
    /// For example if column "c1" holds documents like this
    ///
    /// ```json
    /// [10, 20, 30, 40]
    /// ```
    ///
    /// You can access the value "30" with
    ///
    /// ```
    /// # use datafusion_expr::{lit, col, Expr};
    /// let expr = col("c1")
    ///    .index(lit(3));
    /// assert_eq!(expr.display_name().unwrap(), "c1[Int32(3)]");
    /// ```
    pub fn index(self, key: Expr) -> Self {
        Expr::GetIndexedField(GetIndexedField {
            expr: Box::new(self),
            field: GetFieldAccess::ListIndex { key: Box::new(key) },
        })
    }

    /// Return elements between `1` based `start` and `stop`, for
    /// example `expr[1:3]`
    ///
    /// ## Example: Access element 2, 3, 4 from column "c1"
    ///
    /// For example if column "c1" holds documents like this
    ///
    /// ```json
    /// [10, 20, 30, 40]
    /// ```
    ///
    /// You can access the value `[20, 30, 40]` with
    ///
    /// ```
    /// # use datafusion_expr::{lit, col};
    /// let expr = col("c1")
    ///    .range(lit(2), lit(4));
    /// assert_eq!(expr.display_name().unwrap(), "c1[Int32(2):Int32(4)]");
    /// ```
    pub fn range(self, start: Expr, stop: Expr) -> Self {
        Expr::GetIndexedField(GetIndexedField {
            expr: Box::new(self),
            field: GetFieldAccess::ListRange {
                start: Box::new(start),
                stop: Box::new(stop),
            },
        })
    }

    pub fn try_into_col(&self) -> Result<Column> {
        match self {
            Expr::Column(it) => Ok(it.clone()),
            _ => plan_err!("Could not coerce '{self}' into Column!"),
        }
    }

    /// Return all referenced columns of this expression.
    pub fn to_columns(&self) -> Result<HashSet<Column>> {
        let mut using_columns = HashSet::new();
        expr_to_columns(self, &mut using_columns)?;

        Ok(using_columns)
    }

    /// Return true when the expression contains out reference(correlated) expressions.
    pub fn contains_outer(&self) -> bool {
        !find_out_reference_exprs(self).is_empty()
    }

    /// Recursively find all [`Expr::Placeholder`] expressions, and
    /// to infer their [`DataType`] from the context of their use.
    ///
    /// For example, gicen an expression like `<int32> = $0` will infer `$0` to
    /// have type `int32`.
    pub fn infer_placeholder_types(self, schema: &DFSchema) -> Result<Expr> {
        self.transform(&|mut expr| {
            // Default to assuming the arguments are the same type
            if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = &mut expr {
                rewrite_placeholder(left.as_mut(), right.as_ref(), schema)?;
                rewrite_placeholder(right.as_mut(), left.as_ref(), schema)?;
            };
            if let Expr::Between(Between {
                expr,
                negated: _,
                low,
                high,
            }) = &mut expr
            {
                rewrite_placeholder(low.as_mut(), expr.as_ref(), schema)?;
                rewrite_placeholder(high.as_mut(), expr.as_ref(), schema)?;
            }
            Ok(Transformed::Yes(expr))
        })
    }
}

// modifies expr if it is a placeholder with datatype of right
fn rewrite_placeholder(expr: &mut Expr, other: &Expr, schema: &DFSchema) -> Result<()> {
    if let Expr::Placeholder(Placeholder { id: _, data_type }) = expr {
        if data_type.is_none() {
            let other_dt = other.get_type(schema);
            match other_dt {
                Err(e) => {
                    Err(e.context(format!(
                        "Can not find type of {other} needed to infer type of {expr}"
                    )))?;
                }
                Ok(dt) => {
                    *data_type = Some(dt);
                }
            }
        };
    }
    Ok(())
}

#[macro_export]
macro_rules! expr_vec_fmt {
    ( $ARRAY:expr ) => {{
        $ARRAY
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<String>>()
            .join(", ")
    }};
}

/// Format expressions for display as part of a logical plan. In many cases, this will produce
/// similar output to `Expr.name()` except that column names will be prefixed with '#'.
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(Alias { expr, name, .. }) => write!(f, "{expr} AS {name}"),
            Expr::Column(c) => write!(f, "{c}"),
            Expr::OuterReferenceColumn(_, c) => write!(f, "outer_ref({c})"),
            Expr::ScalarVariable(_, var_names) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v) => write!(f, "{v:?}"),
            Expr::Case(case) => {
                write!(f, "CASE ")?;
                if let Some(e) = &case.expr {
                    write!(f, "{e} ")?;
                }
                for (w, t) in &case.when_then_expr {
                    write!(f, "WHEN {w} THEN {t} ")?;
                }
                if let Some(e) = &case.else_expr {
                    write!(f, "ELSE {e} ")?;
                }
                write!(f, "END")
            }
            Expr::Cast(Cast { expr, data_type }) => {
                write!(f, "CAST({expr} AS {data_type:?})")
            }
            Expr::TryCast(TryCast { expr, data_type }) => {
                write!(f, "TRY_CAST({expr} AS {data_type:?})")
            }
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::Negative(expr) => write!(f, "(- {expr})"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "{expr} IS TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr} IS FALSE"),
            Expr::IsUnknown(expr) => write!(f, "{expr} IS UNKNOWN"),
            Expr::IsNotTrue(expr) => write!(f, "{expr} IS NOT TRUE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr} IS NOT FALSE"),
            Expr::IsNotUnknown(expr) => write!(f, "{expr} IS NOT UNKNOWN"),
            Expr::Exists(Exists {
                subquery,
                negated: true,
            }) => write!(f, "NOT EXISTS ({subquery:?})"),
            Expr::Exists(Exists {
                subquery,
                negated: false,
            }) => write!(f, "EXISTS ({subquery:?})"),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated: true,
            }) => write!(f, "{expr} NOT IN ({subquery:?})"),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated: false,
            }) => write!(f, "{expr} IN ({subquery:?})"),
            Expr::ScalarSubquery(subquery) => write!(f, "({subquery:?})"),
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => {
                if *asc {
                    write!(f, "{expr} ASC")?;
                } else {
                    write!(f, "{expr} DESC")?;
                }
                if *nulls_first {
                    write!(f, " NULLS FIRST")
                } else {
                    write!(f, " NULLS LAST")
                }
            }
            Expr::ScalarFunction(func) => {
                fmt_function(f, &func.fun.to_string(), false, &func.args, true)
            }
            Expr::ScalarUDF(ScalarUDF { fun, args }) => {
                fmt_function(f, &fun.name, false, args, true)
            }
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }) => {
                fmt_function(f, &fun.to_string(), false, args, true)?;
                if !partition_by.is_empty() {
                    write!(f, " PARTITION BY [{}]", expr_vec_fmt!(partition_by))?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(order_by))?;
                }
                write!(
                    f,
                    " {} BETWEEN {} AND {}",
                    window_frame.units, window_frame.start_bound, window_frame.end_bound
                )?;
                Ok(())
            }
            Expr::AggregateFunction(AggregateFunction {
                fun,
                distinct,
                ref args,
                filter,
                order_by,
                ..
            }) => {
                fmt_function(f, &fun.to_string(), *distinct, args, true)?;
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe})")?;
                }
                if let Some(ob) = order_by {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(ob))?;
                }
                Ok(())
            }
            Expr::AggregateUDF(AggregateUDF {
                fun,
                ref args,
                filter,
                order_by,
                ..
            }) => {
                fmt_function(f, &fun.name, false, args, true)?;
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe})")?;
                }
                if let Some(ob) = order_by {
                    write!(f, " ORDER BY [{}]", expr_vec_fmt!(ob))?;
                }
                Ok(())
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                write!(f, "{expr}")?;
                let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " {op_name} {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " {op_name} {pattern}")
                }
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive: _,
            }) => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " SIMILAR TO {pattern} ESCAPE '{char}'")
                } else {
                    write!(f, " SIMILAR TO {pattern}")
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                if *negated {
                    write!(f, "{expr} NOT IN ([{}])", expr_vec_fmt!(list))
                } else {
                    write!(f, "{expr} IN ([{}])", expr_vec_fmt!(list))
                }
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard { qualifier } => write!(f, "{qualifier}.*"),
            Expr::GetIndexedField(GetIndexedField { field, expr }) => match field {
                GetFieldAccess::NamedStructField { name } => {
                    write!(f, "({expr})[{name}]")
                }
                GetFieldAccess::ListIndex { key } => write!(f, "({expr})[{key}]"),
                GetFieldAccess::ListRange { start, stop } => {
                    write!(f, "({expr})[{start}:{stop}]")
                }
            },
            Expr::GroupingSet(grouping_sets) => match grouping_sets {
                GroupingSet::Rollup(exprs) => {
                    // ROLLUP (c0, c1, c2)
                    write!(f, "ROLLUP ({})", expr_vec_fmt!(exprs))
                }
                GroupingSet::Cube(exprs) => {
                    // CUBE (c0, c1, c2)
                    write!(f, "CUBE ({})", expr_vec_fmt!(exprs))
                }
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    // GROUPING SETS ((c0), (c1, c2), (c3, c4))
                    write!(
                        f,
                        "GROUPING SETS ({})",
                        lists_of_exprs
                            .iter()
                            .map(|exprs| format!("({})", expr_vec_fmt!(exprs)))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
            },
            Expr::Placeholder(Placeholder { id, .. }) => write!(f, "{id}"),
        }
    }
}

fn fmt_function(
    f: &mut fmt::Formatter,
    fun: &str,
    distinct: bool,
    args: &[Expr],
    display: bool,
) -> fmt::Result {
    let args: Vec<String> = match display {
        true => args.iter().map(|arg| format!("{arg}")).collect(),
        false => args.iter().map(|arg| format!("{arg:?}")).collect(),
    };

    // let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{}({}{})", fun, distinct_str, args.join(", "))
}

fn create_function_name(fun: &str, distinct: bool, args: &[Expr]) -> Result<String> {
    let names: Vec<String> = args.iter().map(create_name).collect::<Result<_>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

/// Returns a readable name of an expression based on the input schema.
/// This function recursively transverses the expression for names such as "CAST(a > 2)".
fn create_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Alias(Alias { name, .. }) => Ok(name.clone()),
        Expr::Column(c) => Ok(c.flat_name()),
        Expr::OuterReferenceColumn(_, c) => Ok(format!("outer_ref({})", c.flat_name())),
        Expr::ScalarVariable(_, variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(binary_expr) => {
            let left = create_name(binary_expr.left.as_ref())?;
            let right = create_name(binary_expr.right.as_ref())?;
            Ok(format!("{} {} {}", left, binary_expr.op, right))
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            let s = format!(
                "{} {}{} {} {}",
                expr,
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" },
                pattern,
                if let Some(char) = escape_char {
                    format!("CHAR '{char}'")
                } else {
                    "".to_string()
                }
            );
            Ok(s)
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive: _,
        }) => {
            let s = format!(
                "{} {} {} {}",
                expr,
                if *negated {
                    "NOT SIMILAR TO"
                } else {
                    "SIMILAR TO"
                },
                pattern,
                if let Some(char) = escape_char {
                    format!("CHAR '{char}'")
                } else {
                    "".to_string()
                }
            );
            Ok(s)
        }
        Expr::Case(case) => {
            let mut name = "CASE ".to_string();
            if let Some(e) = &case.expr {
                let e = create_name(e)?;
                let _ = write!(name, "{e} ");
            }
            for (w, t) in &case.when_then_expr {
                let when = create_name(w)?;
                let then = create_name(t)?;
                let _ = write!(name, "WHEN {when} THEN {then} ");
            }
            if let Some(e) = &case.else_expr {
                let e = create_name(e)?;
                let _ = write!(name, "ELSE {e} ");
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast(Cast { expr, .. }) => {
            // CAST does not change the expression name
            create_name(expr)
        }
        Expr::TryCast(TryCast { expr, .. }) => {
            // CAST does not change the expression name
            create_name(expr)
        }
        Expr::Not(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::Negative(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("(- {expr})"))
        }
        Expr::IsNull(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsFalse(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsUnknown(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS UNKNOWN"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT TRUE"))
        }
        Expr::IsNotFalse(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::IsNotUnknown(expr) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr} IS NOT UNKNOWN"))
        }
        Expr::Exists(Exists { negated: true, .. }) => Ok("NOT EXISTS".to_string()),
        Expr::Exists(Exists { negated: false, .. }) => Ok("EXISTS".to_string()),
        Expr::InSubquery(InSubquery { negated: true, .. }) => Ok("NOT IN".to_string()),
        Expr::InSubquery(InSubquery { negated: false, .. }) => Ok("IN".to_string()),
        Expr::ScalarSubquery(subquery) => {
            Ok(subquery.subquery.schema().field(0).name().clone())
        }
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let expr = create_name(expr)?;
            match field {
                GetFieldAccess::NamedStructField { name } => {
                    Ok(format!("{expr}[{name}]"))
                }
                GetFieldAccess::ListIndex { key } => {
                    let key = create_name(key)?;
                    Ok(format!("{expr}[{key}]"))
                }
                GetFieldAccess::ListRange { start, stop } => {
                    let start = create_name(start)?;
                    let stop = create_name(stop)?;
                    Ok(format!("{expr}[{start}:{stop}]"))
                }
            }
        }
        Expr::ScalarFunction(func) => {
            create_function_name(&func.fun.to_string(), false, &func.args)
        }
        Expr::ScalarUDF(ScalarUDF { fun, args }) => {
            create_function_name(&fun.name, false, args)
        }
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            window_frame,
            partition_by,
            order_by,
        }) => {
            let mut parts: Vec<String> =
                vec![create_function_name(&fun.to_string(), false, args)?];
            if !partition_by.is_empty() {
                parts.push(format!("PARTITION BY [{}]", expr_vec_fmt!(partition_by)));
            }
            if !order_by.is_empty() {
                parts.push(format!("ORDER BY [{}]", expr_vec_fmt!(order_by)));
            }
            parts.push(format!("{window_frame}"));
            Ok(parts.join(" "))
        }
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            filter,
            order_by,
        }) => {
            let mut name = create_function_name(&fun.to_string(), *distinct, args)?;
            if let Some(fe) = filter {
                name = format!("{name} FILTER (WHERE {fe})");
            };
            if let Some(order_by) = order_by {
                name = format!("{name} ORDER BY [{}]", expr_vec_fmt!(order_by));
            };
            Ok(name)
        }
        Expr::AggregateUDF(AggregateUDF {
            fun,
            args,
            filter,
            order_by,
        }) => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e)?);
            }
            let mut info = String::new();
            if let Some(fe) = filter {
                info += &format!(" FILTER (WHERE {fe})");
            }
            if let Some(ob) = order_by {
                info += &format!(" ORDER BY ([{}])", expr_vec_fmt!(ob));
            }
            Ok(format!("{}({}){}", fun.name, names.join(","), info))
        }
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => {
                Ok(format!("ROLLUP ({})", create_names(exprs.as_slice())?))
            }
            GroupingSet::Cube(exprs) => {
                Ok(format!("CUBE ({})", create_names(exprs.as_slice())?))
            }
            GroupingSet::GroupingSets(lists_of_exprs) => {
                let mut list_of_names = vec![];
                for exprs in lists_of_exprs {
                    list_of_names.push(format!("({})", create_names(exprs.as_slice())?));
                }
                Ok(format!("GROUPING SETS ({})", list_of_names.join(", ")))
            }
        },
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = create_name(expr)?;
            let list = list.iter().map(create_name);
            if *negated {
                Ok(format!("{expr} NOT IN ({list:?})"))
            } else {
                Ok(format!("{expr} IN ({list:?})"))
            }
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = create_name(expr)?;
            let low = create_name(low)?;
            let high = create_name(high)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} AND {high}"))
            } else {
                Ok(format!("{expr} BETWEEN {low} AND {high}"))
            }
        }
        Expr::Sort { .. } => {
            internal_err!("Create name does not support sort expression")
        }
        Expr::Wildcard => Ok("*".to_string()),
        Expr::QualifiedWildcard { .. } => {
            internal_err!("Create name does not support qualified wildcard")
        }
        Expr::Placeholder(Placeholder { id, .. }) => Ok((*id).to_string()),
    }
}

/// Create a comma separated list of names from a list of expressions
fn create_names(exprs: &[Expr]) -> Result<String> {
    Ok(exprs
        .iter()
        .map(create_name)
        .collect::<Result<Vec<String>>>()?
        .join(", "))
}

#[cfg(test)]
mod test {
    use crate::expr::Cast;
    use crate::expr_fn::col;
    use crate::{case, lit, Expr};
    use datafusion_common::logical_type::LogicalType;
    use datafusion_common::Column;
    use datafusion_common::{Result, ScalarValue};

    #[test]
    fn format_case_when() -> Result<()> {
        let expr = case(col("a"))
            .when(lit(1), lit(true))
            .when(lit(0), lit(false))
            .otherwise(lit(ScalarValue::Null))?;
        let expected = "CASE a WHEN Int32(1) THEN Boolean(true) WHEN Int32(0) THEN Boolean(false) ELSE NULL END";
        assert_eq!(expected, expr.canonical_name());
        assert_eq!(expected, format!("{expr}"));
        assert_eq!(expected, expr.display_name()?);
        Ok(())
    }

    #[test]
    fn format_cast() -> Result<()> {
        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Float32(Some(1.23)))),
            data_type: LogicalType::Utf8,
        });
        let expected_canonical = "CAST(Float32(1.23) AS Utf8)";
        assert_eq!(expected_canonical, expr.canonical_name());
        assert_eq!(expected_canonical, format!("{expr}"));
        // note that CAST intentionally has a name that is different from its `Display`
        // representation. CAST does not change the name of expressions.
        assert_eq!("Float32(1.23)", expr.display_name()?);
        Ok(())
    }

    #[test]
    fn test_partial_ord() {
        // Test validates that partial ord is defined for Expr using hashes, not
        // intended to exhaustively test all possibilities
        let exp1 = col("a") + lit(1);
        let exp2 = col("a") + lit(2);
        let exp3 = !(col("a") + lit(2));

        assert!(exp1 < exp2);
        assert!(exp2 > exp1);
        assert!(exp2 > exp3);
        assert!(exp3 < exp2);
    }

    #[test]
    fn test_collect_expr() -> Result<()> {
        // single column
        {
            let expr = &Expr::Cast(Cast::new(Box::new(col("a")), LogicalType::Float64));
            let columns = expr.to_columns()?;
            assert_eq!(1, columns.len());
            assert!(columns.contains(&Column::from_name("a")));
        }

        // multiple columns
        {
            let expr = col("a") + col("b") + lit(1);
            let columns = expr.to_columns()?;
            assert_eq!(2, columns.len());
            assert!(columns.contains(&Column::from_name("a")));
            assert!(columns.contains(&Column::from_name("b")));
        }

        Ok(())
    }

    #[test]
    fn test_logical_ops() {
        assert_eq!(
            format!("{}", lit(1u32).eq(lit(2u32))),
            "UInt32(1) = UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).not_eq(lit(2u32))),
            "UInt32(1) != UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).gt(lit(2u32))),
            "UInt32(1) > UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).gt_eq(lit(2u32))),
            "UInt32(1) >= UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).lt(lit(2u32))),
            "UInt32(1) < UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).lt_eq(lit(2u32))),
            "UInt32(1) <= UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).and(lit(2u32))),
            "UInt32(1) AND UInt32(2)"
        );
        assert_eq!(
            format!("{}", lit(1u32).or(lit(2u32))),
            "UInt32(1) OR UInt32(2)"
        );
    }
}
