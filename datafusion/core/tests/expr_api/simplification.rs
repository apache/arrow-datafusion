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

//! This program demonstrates the DataFusion expression simplification API.

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, Int32Array};
use arrow_buffer::IntervalDayTime;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::{error::Result, execution::context::ExecutionProps, prelude::*};
use datafusion_common::cast::as_int32_array;
use datafusion_common::ScalarValue;
use datafusion_common::{DFSchemaRef, ToDFSchema};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::logical_plan::builder::table_scan_with_filters;
use datafusion_expr::simplify::SimplifyInfo;
use datafusion_expr::{
    expr, table_scan, Cast, ColumnarValue, ExprSchemable, LogicalPlan,
    LogicalPlanBuilder, ScalarUDF, Volatility,
};
use datafusion_functions::{math, string};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyExpressions};
use datafusion_optimizer::{OptimizerContext, OptimizerRule};
use std::sync::Arc;

/// In order to simplify expressions, DataFusion must have information
/// about the expressions.
///
/// You can provide that information using DataFusion [DFSchema]
/// objects or from some other implementation
struct MyInfo {
    /// The input schema
    schema: DFSchemaRef,

    /// Execution specific details needed for constant evaluation such
    /// as the current time for `now()` and [VariableProviders]
    execution_props: ExecutionProps,
}

impl SimplifyInfo for MyInfo {
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        Ok(matches!(
            expr.get_type(self.schema.as_ref())?,
            DataType::Boolean
        ))
    }

    fn nullable(&self, expr: &Expr) -> Result<bool> {
        expr.nullable(self.schema.as_ref())
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    fn get_data_type(&self, expr: &Expr) -> Result<DataType> {
        expr.get_type(self.schema.as_ref())
    }
}

impl From<DFSchemaRef> for MyInfo {
    fn from(schema: DFSchemaRef) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }
}

/// A schema like:
///
/// a: Int32 (possibly with nulls)
/// b: Int32
/// s: Utf8
fn schema() -> DFSchemaRef {
    Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
    ])
    .to_dfschema_ref()
    .unwrap()
}

fn test_table_scan() -> LogicalPlan {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Boolean, false),
        Field::new("c", DataType::Boolean, false),
        Field::new("d", DataType::UInt32, false),
        Field::new("e", DataType::UInt32, true),
    ]);
    table_scan(Some("test"), &schema, None)
        .expect("creating scan")
        .build()
        .expect("building plan")
}

fn get_optimized_plan_formatted(plan: LogicalPlan, date_time: &DateTime<Utc>) -> String {
    let config = OptimizerContext::new().with_query_execution_start_time(*date_time);

    // Use Optimizer to do plan traversal
    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}
    let optimizer = Optimizer::with_rules(vec![Arc::new(SimplifyExpressions::new())]);
    let optimized_plan = optimizer.optimize(plan, &config, observe).unwrap();

    format!("{optimized_plan:?}")
}

// ------------------------------
// --- ConstEvaluator tests -----
// ------------------------------
fn test_evaluate_with_start_time(
    input_expr: Expr,
    expected_expr: Expr,
    date_time: &DateTime<Utc>,
) {
    let execution_props =
        ExecutionProps::new().with_query_execution_start_time(*date_time);

    let info: MyInfo = MyInfo {
        schema: schema(),
        execution_props,
    };
    let simplifier = ExprSimplifier::new(info);
    let simplified_expr = simplifier
        .simplify(input_expr.clone())
        .expect("successfully evaluated");

    assert_eq!(
        simplified_expr, expected_expr,
        "Mismatch evaluating {input_expr}\n  Expected:{expected_expr}\n  Got:{simplified_expr}"
    );
}

fn test_evaluate(input_expr: Expr, expected_expr: Expr) {
    test_evaluate_with_start_time(input_expr, expected_expr, &Utc::now())
}

// Make a UDF that adds its two values together, with the specified volatility
fn make_udf_add(volatility: Volatility) -> Arc<ScalarUDF> {
    let input_types = vec![DataType::Int32, DataType::Int32];
    let return_type = Arc::new(DataType::Int32);

    let fun = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arg0 = as_int32_array(&args[0])?;
        let arg1 = as_int32_array(&args[1])?;

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

        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });

    Arc::new(create_udf(
        "udf_add",
        input_types,
        return_type,
        volatility,
        fun,
    ))
}

fn cast_to_int64_expr(expr: Expr) -> Expr {
    Expr::Cast(Cast::new(expr.into(), DataType::Int64))
}

fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
    to_timestamp(vec![lit(arg.into())])
}

#[test]
fn basic() {
    let info: MyInfo = schema().into();

    // The `Expr` is a core concept in DataFusion, and DataFusion can
    // help simplify it.

    // For example 'a < (2 + 3)' can be rewritten into the easier to
    // optimize form `a < 5` automatically
    let expr = col("a").lt(lit(2i32) + lit(3i32));

    let simplifier = ExprSimplifier::new(info);
    let simplified = simplifier.simplify(expr).unwrap();
    assert_eq!(simplified, col("a").lt(lit(5i32)));
}

#[test]
fn fold_and_simplify() {
    let info: MyInfo = schema().into();

    // What will it do with the expression `concat('foo', 'bar') == 'foobar')`?
    let expr = concat(vec![lit("foo"), lit("bar")]).eq(lit("foobar"));

    // Since datafusion applies both simplification *and* rewriting
    // some expressions can be entirely simplified
    let simplifier = ExprSimplifier::new(info);
    let simplified = simplifier.simplify(expr).unwrap();
    assert_eq!(simplified, lit(true))
}

#[test]
/// Ensure that timestamp expressions are folded so they aren't invoked on each row
fn to_timestamp_expr_folded() -> Result<()> {
    let table_scan = test_table_scan();
    let proj = vec![to_timestamp_expr("2020-09-08T12:00:00+00:00")];

    let plan = LogicalPlanBuilder::from(table_scan)
        .project(proj)?
        .build()?;

    let expected = "Projection: TimestampNanosecond(1599566400000000000, None) AS to_timestamp(Utf8(\"2020-09-08T12:00:00+00:00\"))\
            \n  TableScan: test"
        .to_string();
    let actual = get_optimized_plan_formatted(plan, &Utc::now());
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn now_less_than_timestamp() -> Result<()> {
    let table_scan = test_table_scan();

    let ts_string = "2020-09-08T12:05:00+00:00";
    let time = Utc.timestamp_nanos(1599566400000000000i64);

    //  cast(now() as int) < cast(to_timestamp(...) as int) + 50000_i64
    let plan = LogicalPlanBuilder::from(table_scan)
        .filter(
            cast_to_int64_expr(now())
                .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000_i64)),
        )?
        .build()?;

    // Note that constant folder runs and folds the entire
    // expression down to a single constant (true)
    let expected = "Filter: Boolean(true)\
                        \n  TableScan: test";
    let actual = get_optimized_plan_formatted(plan, &time);

    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn select_date_plus_interval() -> Result<()> {
    let table_scan = test_table_scan();

    let ts_string = "2020-09-08T12:05:00+00:00";
    let time = Utc.timestamp_nanos(1599566400000000000i64);

    //  now() < cast(to_timestamp(...) as int) + 5000000000
    let schema = table_scan.schema();

    let date_plus_interval_expr = to_timestamp_expr(ts_string)
        .cast_to(&DataType::Date32, schema)?
        + Expr::Literal(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
            days: 123,
            milliseconds: 0,
        })));

    let plan = LogicalPlanBuilder::from(table_scan.clone())
        .project(vec![date_plus_interval_expr])?
        .build()?;

    // Note that constant folder runs and folds the entire
    // expression down to a single constant (true)
    let expected = r#"Projection: Date32("2021-01-09") AS to_timestamp(Utf8("2020-09-08T12:05:00+00:00")) + IntervalDayTime("IntervalDayTime { days: 123, milliseconds: 0 }")
  TableScan: test"#;
    let actual = get_optimized_plan_formatted(plan, &time);

    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn simplify_project_scalar_fn() -> Result<()> {
    // Issue https://github.com/apache/datafusion/issues/5996
    let schema = Schema::new(vec![Field::new("f", DataType::Float64, false)]);
    let plan = table_scan(Some("test"), &schema, None)?
        .project(vec![power(col("f"), lit(1.0))])?
        .build()?;

    // before simplify: power(t.f, 1.0)
    // after simplify:  t.f as "power(t.f, 1.0)"
    let expected = "Projection: test.f AS power(test.f,Float64(1))\
                      \n  TableScan: test";
    let actual = get_optimized_plan_formatted(plan, &Utc::now());
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn simplify_scan_predicate() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("f", DataType::Float64, false),
        Field::new("g", DataType::Float64, false),
    ]);
    let plan = table_scan_with_filters(
        Some("test"),
        &schema,
        None,
        vec![col("g").eq(power(col("f"), lit(1.0)))],
    )?
    .build()?;

    // before simplify: t.g = power(t.f, 1.0)
    // after simplify:  (t.g = t.f) as "t.g = power(t.f, 1.0)"
    let expected = "TableScan: test, full_filters=[g = f AS g = power(f,Float64(1))]";
    let actual = get_optimized_plan_formatted(plan, &Utc::now());
    assert_eq!(expected, actual);
    Ok(())
}

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
        col("c").eq(lit(1)),
    );
}

#[test]
fn test_const_evaluator_scalar_functions() {
    // concat("foo", "bar") --> "foobar"
    let expr = string::expr_fn::concat(vec![lit("foo"), lit("bar")]);
    test_evaluate(expr, lit("foobar"));

    // ensure arguments are also constant folded
    // concat("foo", concat("bar", "baz")) --> "foobarbaz"
    let concat1 = string::expr_fn::concat(vec![lit("bar"), lit("baz")]);
    let expr = string::expr_fn::concat(vec![lit("foo"), concat1]);
    test_evaluate(expr, lit("foobarbaz"));

    // Check non string arguments
    // to_timestamp("2020-09-08T12:00:00+00:00") --> timestamp(1599566400i64)
    let expr = to_timestamp(vec![lit("2020-09-08T12:00:00+00:00")]);
    test_evaluate(expr, lit_timestamp_nano(1599566400000000000i64));

    // check that non foldable arguments are folded
    // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
    let expr = to_timestamp(vec![col("a")]);
    test_evaluate(expr.clone(), expr);

    // volatile / stable functions should not be evaluated
    // rand() + (1 + 2) --> rand() + 3
    let fun = math::random();
    assert_eq!(fun.signature().volatility, Volatility::Volatile);
    let rand = Expr::ScalarFunction(ScalarFunction::new_udf(fun, vec![]));
    let expr = rand.clone() + (lit(1) + lit(2));
    let expected = rand + lit(3);
    test_evaluate(expr, expected);

    // parenthesization matters: can't rewrite
    // (rand() + 1) + 2 --> (rand() + 1) + 2)
    let fun = math::random();
    let rand = Expr::ScalarFunction(ScalarFunction::new_udf(fun, vec![]));
    let expr = (rand + lit(1)) + lit(2);
    test_evaluate(expr.clone(), expr);
}

#[test]
fn test_const_evaluator_now() {
    let ts_nanos = 1599566400000000000i64;
    let time = chrono::Utc.timestamp_nanos(ts_nanos);
    let ts_string = "2020-09-08T12:05:00+00:00";
    // now() --> ts
    test_evaluate_with_start_time(now(), lit_timestamp_nano(ts_nanos), &time);

    // CAST(now() as int64) + 100_i64 --> ts + 100_i64
    let expr = cast_to_int64_expr(now()) + lit(100_i64);
    test_evaluate_with_start_time(expr, lit(ts_nanos + 100), &time);

    //  CAST(now() as int64) < cast(to_timestamp(...) as int64) + 50000_i64 ---> true
    let expr = cast_to_int64_expr(now())
        .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000i64));
    test_evaluate_with_start_time(expr, lit(true), &time);
}

#[test]
fn test_evaluator_udfs() {
    let args = vec![lit(1) + lit(2), lit(30) + lit(40)];
    let folded_args = vec![lit(3), lit(70)];

    // immutable UDF should get folded
    // udf_add(1+2, 30+40) --> 73
    let expr = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
        make_udf_add(Volatility::Immutable),
        args.clone(),
    ));
    test_evaluate(expr, lit(73));

    // stable UDF should be entirely folded
    // udf_add(1+2, 30+40) --> 73
    let fun = make_udf_add(Volatility::Stable);
    let expr = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
        Arc::clone(&fun),
        args.clone(),
    ));
    test_evaluate(expr, lit(73));

    // volatile UDF should have args folded
    // udf_add(1+2, 30+40) --> udf_add(3, 70)
    let fun = make_udf_add(Volatility::Volatile);
    let expr =
        Expr::ScalarFunction(expr::ScalarFunction::new_udf(Arc::clone(&fun), args));
    let expected_expr = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
        Arc::clone(&fun),
        folded_args,
    ));
    test_evaluate(expr, expected_expr);
}

#[test]
fn multiple_now() -> Result<()> {
    let table_scan = test_table_scan();
    let time = Utc::now();
    let proj = vec![now(), now().alias("t2")];
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(proj)?
        .build()?;

    // expect the same timestamp appears in both exprs
    let actual = get_optimized_plan_formatted(plan, &time);
    let expected = format!(
        "Projection: TimestampNanosecond({}, Some(\"+00:00\")) AS now(), TimestampNanosecond({}, Some(\"+00:00\")) AS t2\
            \n  TableScan: test",
        time.timestamp_nanos_opt().unwrap(),
        time.timestamp_nanos_opt().unwrap()
    );

    assert_eq!(expected, actual);
    Ok(())
}

// ------------------------------
// --- Simplifier tests -----
// ------------------------------

fn expr_test_schema() -> DFSchemaRef {
    Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Boolean, true),
        Field::new("c3", DataType::Int64, true),
        Field::new("c4", DataType::UInt32, true),
        Field::new("c1_non_null", DataType::Utf8, false),
        Field::new("c2_non_null", DataType::Boolean, false),
        Field::new("c3_non_null", DataType::Int64, false),
        Field::new("c4_non_null", DataType::UInt32, false),
    ])
    .to_dfschema_ref()
    .unwrap()
}

fn test_simplify(input_expr: Expr, expected_expr: Expr) {
    let info: MyInfo = MyInfo {
        schema: expr_test_schema(),
        execution_props: ExecutionProps::new(),
    };
    let simplifier = ExprSimplifier::new(info);
    let simplified_expr = simplifier
        .simplify(input_expr.clone())
        .expect("successfully evaluated");

    assert_eq!(
        simplified_expr, expected_expr,
        "Mismatch evaluating {input_expr}\n  Expected:{expected_expr}\n  Got:{simplified_expr}"
    );
}
fn test_simplify_with_cycle_info(
    input_expr: Expr,
    expected_expr: Expr,
    expected_cycle_count: usize,
    expected_iteration_count: usize,
) {
    let info: MyInfo = MyInfo {
        schema: expr_test_schema(),
        execution_props: ExecutionProps::new(),
    };
    let simplifier = ExprSimplifier::new(info);
    let (simplified_expr, info) = simplifier
        .simplify_with_cycle_info(input_expr.clone())
        .expect("successfully evaluated");

    let total_iterations = info.total_iterations();
    let completed_cycles = info.completed_cycles();
    assert_eq!(
        simplified_expr, expected_expr,
        "Mismatch evaluating {input_expr}\n  Expected:{expected_expr}\n  Got:{simplified_expr}"
    );
    assert_eq!(
        completed_cycles, expected_cycle_count,
        "Mismatch simplifier cycle count\n Expected: {expected_cycle_count}\n Got:{completed_cycles}"
    );
    assert_eq!(
        total_iterations, expected_iteration_count,
        "Mismatch simplifier cycle count\n Expected: {expected_iteration_count}\n Got:{total_iterations}"
    );
}

#[test]
fn test_simplify_log() {
    // Log(c3, 1) ===> 0
    {
        let expr = log(col("c3_non_null"), lit(1));
        test_simplify(expr, lit(0i64));
    }
    // Log(c3, c3) ===> 1
    {
        let expr = log(col("c3_non_null"), col("c3_non_null"));
        let expected = lit(1i64);
        test_simplify(expr, expected);
    }
    // Log(c3, Power(c3, c4)) ===> c4
    {
        let expr = log(
            col("c3_non_null"),
            power(col("c3_non_null"), col("c4_non_null")),
        );
        let expected = col("c4_non_null");
        test_simplify(expr, expected);
    }
    // Log(c3, c4) ===> Log(c3, c4)
    {
        let expr = log(col("c3_non_null"), col("c4_non_null"));
        let expected = log(col("c3_non_null"), col("c4_non_null"));
        test_simplify(expr, expected);
    }
}

#[test]
fn test_simplify_power() {
    // Power(c3, 0) ===> 1
    {
        let expr = power(col("c3_non_null"), lit(0));
        let expected = lit(1i64);
        test_simplify(expr, expected)
    }
    // Power(c3, 1) ===> c3
    {
        let expr = power(col("c3_non_null"), lit(1));
        let expected = col("c3_non_null");
        test_simplify(expr, expected)
    }
    // Power(c3, Log(c3, c4)) ===> c4
    {
        let expr = power(
            col("c3_non_null"),
            log(col("c3_non_null"), col("c4_non_null")),
        );
        let expected = col("c4_non_null");
        test_simplify(expr, expected)
    }
    // Power(c3, c4) ===> Power(c3, c4)
    {
        let expr = power(col("c3_non_null"), col("c4_non_null"));
        let expected = power(col("c3_non_null"), col("c4_non_null"));
        test_simplify(expr, expected)
    }
}

#[test]
fn test_simplify_concat_ws() {
    let null = lit(ScalarValue::Utf8(None));
    // the delimiter is not a literal
    {
        let expr = concat_ws(col("c"), vec![lit("a"), null.clone(), lit("b")]);
        let expected = concat_ws(col("c"), vec![lit("a"), lit("b")]);
        test_simplify(expr, expected);
    }

    // the delimiter is an empty string
    {
        let expr = concat_ws(lit(""), vec![col("a"), lit("c"), lit("b")]);
        let expected = concat(vec![col("a"), lit("cb")]);
        test_simplify(expr, expected);
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
        test_simplify(expr, expected)
    }
}

#[test]
fn test_simplify_concat_ws_with_null() {
    let null = lit(ScalarValue::Utf8(None));
    // null delimiter -> null
    {
        let expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
        test_simplify(expr, null.clone());
    }

    // filter out null args
    {
        let expr = concat_ws(lit("|"), vec![col("c1"), null.clone(), col("c2")]);
        let expected = concat_ws(lit("|"), vec![col("c1"), col("c2")]);
        test_simplify(expr, expected);
    }

    // nested test
    {
        let sub_expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
        let expr = concat_ws(lit("|"), vec![sub_expr, col("c3")]);
        test_simplify(expr, concat_ws(lit("|"), vec![col("c3")]));
    }

    // null delimiter (nested)
    {
        let sub_expr = concat_ws(null.clone(), vec![col("c1"), col("c2")]);
        let expr = concat_ws(sub_expr, vec![col("c3"), col("c4")]);
        test_simplify(expr, null);
    }
}

#[test]
fn test_simplify_concat() {
    let null = lit(ScalarValue::Utf8(None));
    let expr = concat(vec![
        null.clone(),
        col("c0"),
        lit("hello "),
        null.clone(),
        lit("rust"),
        col("c1"),
        lit(""),
        null,
    ]);
    let expected = concat(vec![col("c0"), lit("hello rust"), col("c1")]);
    test_simplify(expr, expected)
}
#[test]
fn test_simplify_cycles() {
    // cast(now() as int64) < cast(to_timestamp(0) as int64) + i64::MAX
    let expr = cast(now(), DataType::Int64)
        .lt(cast(to_timestamp(vec![lit(0)]), DataType::Int64) + lit(i64::MAX));
    let expected = lit(true);
    test_simplify_with_cycle_info(expr, expected, 2, 7);
}
