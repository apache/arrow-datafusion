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

use crate::aggregate_function;
use crate::built_in_function;
use crate::expr_fn::binary_expr;
use crate::logical_plan::Subquery;
use crate::utils::{expr_to_columns, find_hidden_columns, find_out_reference_exprs};
use crate::window_frame;
use crate::window_function;
use crate::AggregateUDF;
use crate::Operator;
use crate::ScalarUDF;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::{plan_err, Column};
use datafusion_common::{DataFusionError, ScalarValue};
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter, Write};
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;

/// `Expr` is a central struct of DataFusion's query API, and
/// represent logical expressions such as `A + 1`, or `CAST(c1 AS
/// int)`.
///
/// An `Expr` can compute its [DataType](arrow::datatypes::DataType)
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
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Box<Expr>, String),
    /// A named reference to a qualified filed in a schema.
    Column(Column),
    /// A named reference to a variable in a registry.
    ScalarVariable(DataType, Vec<String>),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr(BinaryExpr),
    /// LIKE expression
    Like(Like),
    /// Case-insensitive LIKE expression
    ILike(Like),
    /// LIKE expression that uses regular expressions
    SimilarTo(Like),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// Whether an expression is not Null. This expression is never null.
    IsNotNull(Box<Expr>),
    /// Whether an expression is Null. This expression is never null.
    IsNull(Box<Expr>),
    /// Whether an expression is True. Boolean operation
    IsTrue(Box<Expr>),
    /// Whether an expression is False. Boolean operation
    IsFalse(Box<Expr>),
    /// Whether an expression is Unknown. Boolean operation
    IsUnknown(Box<Expr>),
    /// Whether an expression is not True. Boolean operation
    IsNotTrue(Box<Expr>),
    /// Whether an expression is not False. Boolean operation
    IsNotFalse(Box<Expr>),
    /// Whether an expression is not Unknown. Boolean operation
    IsNotUnknown(Box<Expr>),
    /// arithmetic negation of an expression, the operand must be of a signed numeric data type
    Negative(Box<Expr>),
    /// Returns the field of a [`arrow::array::ListArray`] or [`arrow::array::StructArray`] by key
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
    ScalarFunction {
        /// The function
        fun: built_in_function::BuiltinScalarFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of a user-defined scalar function with arguments.
    ScalarUDF {
        /// The function
        fun: Arc<ScalarUDF>,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction(AggregateFunction),
    /// Represents the call of a window function with arguments.
    WindowFunction(WindowFunction),
    /// aggregate function
    AggregateUDF {
        /// The function
        fun: Arc<AggregateUDF>,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// Optional filter applied prior to aggregating
        filter: Option<Box<Expr>>,
    },
    /// Returns whether the list contains the expr value.
    InList {
        /// The expression to compare
        expr: Box<Expr>,
        /// A list of values to compare against
        list: Vec<Expr>,
        /// Whether the expression is negated
        negated: bool,
    },
    /// EXISTS subquery
    Exists {
        /// subquery that will produce a single column of data
        subquery: Subquery,
        /// Whether the expression is negated
        negated: bool,
    },
    /// IN subquery
    InSubquery {
        /// The expression to compare
        expr: Box<Expr>,
        /// subquery that will produce a single column of data to compare against
        subquery: Subquery,
        /// Whether the expression is negated
        negated: bool,
    },
    /// Scalar subquery
    ScalarSubquery(Subquery),
    /// Represents a reference to all fields in a schema.
    Wildcard,
    /// Represents a reference to all fields in a specific schema.
    QualifiedWildcard { qualifier: String },
    /// List of grouping set expressions. Only valid in the context of an aggregate
    /// GROUP BY expression list
    GroupingSet(GroupingSet),
    /// A place holder for parameters in a prepared statement
    /// (e.g. `$foo` or `$1`)
    Placeholder {
        /// The identifier of the parameter (e.g, $1 or $foo)
        id: String,
        /// The type the parameter will be filled in with
        data_type: Option<DataType>,
    },
    /// A place holder which hold a reference to a qualified field
    /// in the outer query, used for correlated sub queries.
    OuterReferenceColumn(DataType, Column),
    /// A hidden column used by the system internally
    HiddenColumn(DataType, String),
    /// A hidden expr pair used by the system internally, evaluated to a HiddenColumn
    HiddenExpr(Box<Expr>, Box<Expr>),
}

/// Binary expression
#[derive(Clone, PartialEq, Eq, Hash)]
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
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Like {
    pub negated: bool,
    pub expr: Box<Expr>,
    pub pattern: Box<Expr>,
    pub escape_char: Option<char>,
}

impl Like {
    /// Create a new Like expression
    pub fn new(
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
    ) -> Self {
        Self {
            negated,
            expr,
            pattern,
            escape_char,
        }
    }
}

/// BETWEEN expression
#[derive(Clone, PartialEq, Eq, Hash)]
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

/// Returns the field of a [`arrow::array::ListArray`] or [`arrow::array::StructArray`] by key
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct GetIndexedField {
    /// the expression to take the field from
    pub expr: Box<Expr>,
    /// The name of the field to take
    pub key: ScalarValue,
}

impl GetIndexedField {
    /// Create a new GetIndexedField expression
    pub fn new(expr: Box<Expr>, key: ScalarValue) -> Self {
        Self { expr, key }
    }
}

/// Cast expression
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Cast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl Cast {
    /// Create a new Cast expression
    pub fn new(expr: Box<Expr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

/// TryCast Expression
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TryCast {
    /// The expression being cast
    pub expr: Box<Expr>,
    /// The `DataType` the expression will yield
    pub data_type: DataType,
}

impl TryCast {
    /// Create a new TryCast expression
    pub fn new(expr: Box<Expr>, data_type: DataType) -> Self {
        Self { expr, data_type }
    }
}

/// SORT expression
#[derive(Clone, PartialEq, Eq, Hash)]
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
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AggregateFunction {
    /// Name of the function
    pub fun: aggregate_function::AggregateFunction,
    /// List of expressions to feed to the functions as arguments
    pub args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    pub distinct: bool,
    /// Optional filter
    pub filter: Option<Box<Expr>>,
}

impl AggregateFunction {
    pub fn new(
        fun: aggregate_function::AggregateFunction,
        args: Vec<Expr>,
        distinct: bool,
        filter: Option<Box<Expr>>,
    ) -> Self {
        Self {
            fun,
            args,
            distinct,
            filter,
        }
    }
}

/// Window function
#[derive(Clone, PartialEq, Eq, Hash)]
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

/// Grouping sets
/// See <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>
/// for Postgres definition.
/// See <https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-groupby.html>
/// for Apache Spark definition.
#[derive(Clone, PartialEq, Eq, Hash)]
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
    pub fn distinct_expr(&self, include_hidden: bool) -> Vec<Expr> {
        match self {
            GroupingSet::Rollup(exprs) => exprs.clone(),
            GroupingSet::Cube(exprs) => exprs.clone(),
            GroupingSet::GroupingSets(groups) => {
                let mut exprs: Vec<Expr> = vec![];
                for exp in groups.iter().flatten() {
                    if let Expr::HiddenExpr(_, second) = exp {
                        if include_hidden && !exprs.contains(second) {
                            exprs.push(*second.clone());
                        }
                    } else if !exprs.contains(exp) {
                        exprs.push(exp.clone());
                    }
                }
                exprs
            }
        }
    }

    pub fn contains_duplicate_grouping(&self) -> bool {
        match self {
            GroupingSet::Rollup(_) => false,
            GroupingSet::Cube(_) => false,
            GroupingSet::GroupingSets(groups) => {
                let exclude_hidden = groups
                    .clone()
                    .into_iter()
                    .map(|group| {
                        group
                            .into_iter()
                            .filter(|e| !matches!(e, Expr::HiddenExpr(_, _)))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                let exclude_hidden_len = exclude_hidden.len();
                let distinct_set = exclude_hidden.into_iter().collect::<HashSet<_>>();
                exclude_hidden_len != distinct_set.len()
            }
        }
    }

    pub fn contains_hidden_expr(&self) -> bool {
        match self {
            GroupingSet::Rollup(_) => false,
            GroupingSet::Cube(_) => false,
            GroupingSet::GroupingSets(groups) => groups
                .iter()
                .flatten()
                .any(|e| matches!(e, Expr::HiddenExpr(_, _))),
        }
    }

    /// Return the input exprs len in the grouping set
    pub fn input_expr_len(&self) -> usize {
        match self {
            GroupingSet::Rollup(exprs) => exprs.len(),
            GroupingSet::Cube(exprs) => exprs.len(),
            GroupingSet::GroupingSets(groups) => groups.len(),
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
            Expr::InSubquery { .. } => "InSubquery",
            Expr::IsNotNull(..) => "IsNotNull",
            Expr::IsNull(..) => "IsNull",
            Expr::Like { .. } => "Like",
            Expr::ILike { .. } => "ILike",
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
            Expr::Placeholder { .. } => "Placeholder",
            Expr::QualifiedWildcard { .. } => "QualifiedWildcard",
            Expr::ScalarFunction { .. } => "ScalarFunction",
            Expr::ScalarSubquery { .. } => "ScalarSubquery",
            Expr::ScalarUDF { .. } => "ScalarUDF",
            Expr::ScalarVariable(..) => "ScalarVariable",
            Expr::Sort { .. } => "Sort",
            Expr::TryCast { .. } => "TryCast",
            Expr::WindowFunction { .. } => "WindowFunction",
            Expr::Wildcard => "Wildcard",
            Expr::HiddenColumn(..) => "HiddenColumn",
            Expr::HiddenExpr(..) => "HiddenExpr",
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

    /// Return `self & other`
    pub fn bitwise_and(self, other: Expr) -> Expr {
        binary_expr(self, Operator::BitwiseAnd, other)
    }

    /// Return `self | other`
    pub fn bitwise_or(self, other: Expr) -> Expr {
        binary_expr(self, Operator::BitwiseOr, other)
    }

    /// Return `self ^ other`
    pub fn bitwise_xor(self, other: Expr) -> Expr {
        binary_expr(self, Operator::BitwiseXor, other)
    }

    /// Return `self >> other`
    pub fn bitwise_shift_right(self, other: Expr) -> Expr {
        binary_expr(self, Operator::BitwiseShiftRight, other)
    }

    /// Return `self << other`
    pub fn bitwise_shift_left(self, other: Expr) -> Expr {
        binary_expr(self, Operator::BitwiseShiftLeft, other)
    }

    /// Return `!self`
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Expr {
        !self
    }

    /// Calculate the modulus of two expressions.
    /// Return `self % other`
    pub fn modulus(self, other: Expr) -> Expr {
        binary_expr(self, Operator::Modulo, other)
    }

    /// Return `self LIKE other`
    pub fn like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(false, Box::new(self), Box::new(other), None))
    }

    /// Return `self NOT LIKE other`
    pub fn not_like(self, other: Expr) -> Expr {
        Expr::Like(Like::new(true, Box::new(self), Box::new(other), None))
    }

    /// Return `self ILIKE other`
    pub fn ilike(self, other: Expr) -> Expr {
        Expr::ILike(Like::new(false, Box::new(self), Box::new(other), None))
    }

    /// Return `self NOT ILIKE other`
    pub fn not_ilike(self, other: Expr) -> Expr {
        Expr::ILike(Like::new(true, Box::new(self), Box::new(other), None))
    }

    /// Return `self AS name` alias expression
    pub fn alias(self, name: impl Into<String>) -> Expr {
        Expr::Alias(Box::new(self), name.into())
    }

    /// Remove an alias from an expression if one exists.
    pub fn unalias(self) -> Expr {
        match self {
            Expr::Alias(expr, _) => expr.as_ref().clone(),
            _ => self,
        }
    }

    /// Return `self IN <list>` if `negated` is false, otherwise
    /// return `self NOT IN <list>`.a
    pub fn in_list(self, list: Vec<Expr>, negated: bool) -> Expr {
        Expr::InList {
            expr: Box::new(self),
            list,
            negated,
        }
    }

    /// Return `IsNull(Box(self))
    #[allow(clippy::wrong_self_convention)]
    pub fn is_null(self) -> Expr {
        Expr::IsNull(Box::new(self))
    }

    /// Return `IsNotNull(Box(self))
    #[allow(clippy::wrong_self_convention)]
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

    pub fn try_into_col(&self) -> Result<Column> {
        match self {
            Expr::Column(it) => Ok(it.clone()),
            _ => plan_err!(format!("Could not coerce '{self}' into Column!")),
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

    /// Return true when the expression contains hidden columns.
    pub fn contains_hidden_columns(&self) -> bool {
        !find_hidden_columns(self).is_empty()
    }
}

impl Not for Expr {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::Like(Like::new(!negated, expr, pattern, escape_char)),
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::ILike(Like::new(!negated, expr, pattern, escape_char)),
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => Expr::SimilarTo(Like::new(!negated, expr, pattern, escape_char)),
            _ => Expr::Not(Box::new(self)),
        }
    }
}

/// Format expressions for display as part of a logical plan. In many cases, this will produce
/// similar output to `Expr.name()` except that column names will be prefixed with '#'.
impl fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Format expressions for display as part of a logical plan. In many cases, this will produce
/// similar output to `Expr.name()` except that column names will be prefixed with '#'.
impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{expr:?} AS {alias}"),
            Expr::Column(c) => write!(f, "{c}"),
            Expr::OuterReferenceColumn(_, c) => write!(f, "outer_ref({c})"),
            Expr::ScalarVariable(_, var_names) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v) => write!(f, "{v:?}"),
            Expr::Case(case) => {
                write!(f, "CASE ")?;
                if let Some(e) = &case.expr {
                    write!(f, "{e:?} ")?;
                }
                for (w, t) in &case.when_then_expr {
                    write!(f, "WHEN {w:?} THEN {t:?} ")?;
                }
                if let Some(e) = &case.else_expr {
                    write!(f, "ELSE {e:?} ")?;
                }
                write!(f, "END")
            }
            Expr::Cast(Cast { expr, data_type }) => {
                write!(f, "CAST({expr:?} AS {data_type:?})")
            }
            Expr::TryCast(TryCast { expr, data_type }) => {
                write!(f, "TRY_CAST({expr:?} AS {data_type:?})")
            }
            Expr::Not(expr) => write!(f, "NOT {expr:?}"),
            Expr::Negative(expr) => write!(f, "(- {expr:?})"),
            Expr::IsNull(expr) => write!(f, "{expr:?} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr:?} IS NOT NULL"),
            Expr::IsTrue(expr) => write!(f, "{expr:?} IS TRUE"),
            Expr::IsFalse(expr) => write!(f, "{expr:?} IS FALSE"),
            Expr::IsUnknown(expr) => write!(f, "{expr:?} IS UNKNOWN"),
            Expr::IsNotTrue(expr) => write!(f, "{expr:?} IS NOT TRUE"),
            Expr::IsNotFalse(expr) => write!(f, "{expr:?} IS NOT FALSE"),
            Expr::IsNotUnknown(expr) => write!(f, "{expr:?} IS NOT UNKNOWN"),
            Expr::Exists {
                subquery,
                negated: true,
            } => write!(f, "NOT EXISTS ({subquery:?})"),
            Expr::Exists {
                subquery,
                negated: false,
            } => write!(f, "EXISTS ({subquery:?})"),
            Expr::InSubquery {
                expr,
                subquery,
                negated: true,
            } => write!(f, "{expr:?} NOT IN ({subquery:?})"),
            Expr::InSubquery {
                expr,
                subquery,
                negated: false,
            } => write!(f, "{expr:?} IN ({subquery:?})"),
            Expr::ScalarSubquery(subquery) => write!(f, "({subquery:?})"),
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::Sort(Sort {
                expr,
                asc,
                nulls_first,
            }) => {
                if *asc {
                    write!(f, "{expr:?} ASC")?;
                } else {
                    write!(f, "{expr:?} DESC")?;
                }
                if *nulls_first {
                    write!(f, " NULLS FIRST")
                } else {
                    write!(f, " NULLS LAST")
                }
            }
            Expr::ScalarFunction { fun, args, .. } => {
                fmt_function(f, &fun.to_string(), false, args, false)
            }
            Expr::ScalarUDF { fun, ref args, .. } => {
                fmt_function(f, &fun.name, false, args, false)
            }
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }) => {
                fmt_function(f, &fun.to_string(), false, args, false)?;
                if !partition_by.is_empty() {
                    write!(f, " PARTITION BY {partition_by:?}")?;
                }
                if !order_by.is_empty() {
                    write!(f, " ORDER BY {order_by:?}")?;
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
                ..
            }) => {
                fmt_function(f, &fun.to_string(), *distinct, args, true)?;
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe})")?;
                }
                Ok(())
            }
            Expr::AggregateUDF {
                fun,
                ref args,
                filter,
                ..
            } => {
                fmt_function(f, &fun.name, false, args, false)?;
                if let Some(fe) = filter {
                    write!(f, " FILTER (WHERE {fe})")?;
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
                    write!(f, "{expr:?} NOT BETWEEN {low:?} AND {high:?}")
                } else {
                    write!(f, "{expr:?} BETWEEN {low:?} AND {high:?}")
                }
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                write!(f, "{expr:?}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " LIKE {pattern:?} ESCAPE '{char}'")
                } else {
                    write!(f, " LIKE {pattern:?}")
                }
            }
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                write!(f, "{expr:?}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " ILIKE {pattern:?} ESCAPE '{char}'")
                } else {
                    write!(f, " ILIKE {pattern:?}")
                }
            }
            Expr::SimilarTo(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                write!(f, "{expr:?}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                if let Some(char) = escape_char {
                    write!(f, " SIMILAR TO {pattern:?} ESCAPE '{char}'")
                } else {
                    write!(f, " SIMILAR TO {pattern:?}")
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                if *negated {
                    write!(f, "{expr:?} NOT IN ({list:?})")
                } else {
                    write!(f, "{expr:?} IN ({list:?})")
                }
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::QualifiedWildcard { qualifier } => write!(f, "{qualifier}.*"),
            Expr::GetIndexedField(GetIndexedField { key, expr }) => {
                write!(f, "({expr:?})[{key}]")
            }
            Expr::GroupingSet(grouping_sets) => match grouping_sets {
                GroupingSet::Rollup(exprs) => {
                    // ROLLUP (c0, c1, c2)
                    write!(
                        f,
                        "ROLLUP ({})",
                        exprs
                            .iter()
                            .map(|e| format!("{e}"))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
                GroupingSet::Cube(exprs) => {
                    // CUBE (c0, c1, c2)
                    write!(
                        f,
                        "CUBE ({})",
                        exprs
                            .iter()
                            .map(|e| format!("{e}"))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
                GroupingSet::GroupingSets(lists_of_exprs) => {
                    // GROUPING SETS ((c0), (c1, c2), (c3, c4))
                    write!(
                        f,
                        "GROUPING SETS ({})",
                        lists_of_exprs
                            .iter()
                            .map(|exprs| format!(
                                "({})",
                                exprs
                                    .iter()
                                    .map(|e| format!("{e}"))
                                    .collect::<Vec<String>>()
                                    .join(", ")
                            ))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )
                }
            },
            Expr::Placeholder { id, .. } => write!(f, "{id}"),
            Expr::HiddenColumn(_, c) => write!(f, "#{}", c),
            Expr::HiddenExpr(first, _) => write!(f, "{}", first),
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
        Expr::Alias(_, name) => Ok(name.clone()),
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
        }) => {
            let s = format!(
                "{} {} {} {}",
                expr,
                if *negated { "NOT LIKE" } else { "LIKE" },
                pattern,
                if let Some(char) = escape_char {
                    format!("CHAR '{char}'")
                } else {
                    "".to_string()
                }
            );
            Ok(s)
        }
        Expr::ILike(Like {
            negated,
            expr,
            pattern,
            escape_char,
        }) => {
            let s = format!(
                "{} {} {} {}",
                expr,
                if *negated { "NOT ILIKE" } else { "ILIKE" },
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
        Expr::Exists { negated: true, .. } => Ok("NOT EXISTS".to_string()),
        Expr::Exists { negated: false, .. } => Ok("EXISTS".to_string()),
        Expr::InSubquery { negated: true, .. } => Ok("NOT IN".to_string()),
        Expr::InSubquery { negated: false, .. } => Ok("IN".to_string()),
        Expr::ScalarSubquery(subquery) => {
            Ok(subquery.subquery.schema().field(0).name().clone())
        }
        Expr::GetIndexedField(GetIndexedField { key, expr }) => {
            let expr = create_name(expr)?;
            Ok(format!("{expr}[{key}]"))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_name(&fun.to_string(), false, args)
        }
        Expr::ScalarUDF { fun, args, .. } => create_function_name(&fun.name, false, args),
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
                parts.push(format!("PARTITION BY {partition_by:?}"));
            }
            if !order_by.is_empty() {
                parts.push(format!("ORDER BY {order_by:?}"));
            }
            parts.push(format!("{window_frame}"));
            Ok(parts.join(" "))
        }
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            filter,
        }) => {
            let name = create_function_name(&fun.to_string(), *distinct, args)?;
            if let Some(fe) = filter {
                Ok(format!("{name} FILTER (WHERE {fe})"))
            } else {
                Ok(name)
            }
        }
        Expr::AggregateUDF { fun, args, filter } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e)?);
            }
            let filter = if let Some(fe) = filter {
                format!(" FILTER (WHERE {fe})")
            } else {
                "".to_string()
            };
            Ok(format!("{}({}){}", fun.name, names.join(","), filter))
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
        Expr::InList {
            expr,
            list,
            negated,
        } => {
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
        Expr::Sort { .. } => Err(DataFusionError::Internal(
            "Create name does not support sort expression".to_string(),
        )),
        Expr::Wildcard => Ok("*".to_string()),
        Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
            "Create name does not support qualified wildcard".to_string(),
        )),
        Expr::Placeholder { id, .. } => Ok((*id).to_string()),
        Expr::HiddenColumn(_, c) => Ok(format!("#{}", c)),
        Expr::HiddenExpr(first, _) => Ok(format!("#{}", first)),
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
    use arrow::datatypes::DataType;
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
        assert_eq!(expected, format!("{expr:?}"));
        assert_eq!(expected, expr.display_name()?);
        Ok(())
    }

    #[test]
    fn format_cast() -> Result<()> {
        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Float32(Some(1.23)))),
            data_type: DataType::Utf8,
        });
        let expected_canonical = "CAST(Float32(1.23) AS Utf8)";
        assert_eq!(expected_canonical, expr.canonical_name());
        assert_eq!(expected_canonical, format!("{expr}"));
        assert_eq!(expected_canonical, format!("{expr:?}"));
        // note that CAST intentionally has a name that is different from its `Display`
        // representation. CAST does not change the name of expressions.
        assert_eq!("Float32(1.23)", expr.display_name()?);
        Ok(())
    }

    #[test]
    fn test_not() {
        assert_eq!(lit(1).not(), !lit(1));
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
            let expr = &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64));
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
}
