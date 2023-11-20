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

#[cfg(test)]
use std::collections::HashMap;
use std::{sync::Arc, vec};

use arrow_schema::*;
use sqlparser::dialect::{Dialect, GenericDialect, HiveDialect, MySqlDialect};

use datafusion_common::plan_err;
use datafusion_common::{
    assert_contains, config::ConfigOptions, DataFusionError, Result, ScalarValue,
    TableReference,
};
use datafusion_expr::{
    logical_plan::{LogicalPlan, Prepare},
    AggregateUDF, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_sql::{
    parser::DFParser,
    planner::{ContextProvider, ParserOptions, SqlToRel},
};

use rstest::rstest;

#[test]
fn parse_decimals() {
    let test_data = [
        ("1", "Int64(1)"),
        ("001", "Int64(1)"),
        ("0.1", "Decimal128(Some(1),1,1)"),
        ("0.01", "Decimal128(Some(1),2,2)"),
        ("1.0", "Decimal128(Some(10),2,1)"),
        ("10.01", "Decimal128(Some(1001),4,2)"),
        (
            "10000000000000000000.00",
            "Decimal128(Some(1000000000000000000000),22,2)",
        ),
        ("18446744073709551615", "UInt64(18446744073709551615)"),
        (
            "18446744073709551616",
            "Decimal128(Some(18446744073709551616),20,0)",
        ),
    ];
    for (a, b) in test_data {
        let sql = format!("SELECT {a}");
        let expected = format!("Projection: {b}\n  EmptyRelation");
        quick_test_with_options(
            &sql,
            &expected,
            ParserOptions {
                parse_float_as_decimal: true,
                enable_ident_normalization: false,
            },
        );
    }
}

#[test]
fn parse_ident_normalization() {
    let test_data = [
        (
            "SELECT LENGTH('str')",
            "Ok(Projection: character_length(Utf8(\"str\"))\n  EmptyRelation)",
            false,
        ),
        (
            "SELECT CONCAT('Hello', 'World')",
            "Ok(Projection: concat(Utf8(\"Hello\"), Utf8(\"World\"))\n  EmptyRelation)",
            false,
        ),
        (
            "SELECT age FROM person",
            "Ok(Projection: person.age\n  TableScan: person)",
            true,
        ),
        (
            "SELECT AGE FROM PERSON",
            "Ok(Projection: person.age\n  TableScan: person)",
            true,
        ),
        (
            "SELECT AGE FROM PERSON",
            "Error during planning: No table named: PERSON found",
            false,
        ),
        (
            "SELECT Id FROM UPPERCASE_test",
            "Ok(Projection: UPPERCASE_test.Id\
                \n  TableScan: UPPERCASE_test)",
            false,
        ),
        (
            "SELECT \"Id\", lower FROM \"UPPERCASE_test\"",
            "Ok(Projection: UPPERCASE_test.Id, UPPERCASE_test.lower\
                \n  TableScan: UPPERCASE_test)",
            true,
        ),
    ];

    for (sql, expected, enable_ident_normalization) in test_data {
        let plan = logical_plan_with_options(
            sql,
            ParserOptions {
                parse_float_as_decimal: false,
                enable_ident_normalization,
            },
        );
        if plan.is_ok() {
            assert_eq!(expected, format!("{plan:?}"));
        } else {
            assert_eq!(expected, plan.unwrap_err().strip_backtrace());
        }
    }
}

#[test]
fn select_no_relation() {
    quick_test(
        "SELECT 1",
        "Projection: Int64(1)\
             \n  EmptyRelation",
    );
}

#[test]
fn test_real_f32() {
    quick_test(
        "SELECT CAST(1.1 AS REAL)",
        "Projection: CAST(Float64(1.1) AS Float32)\
             \n  EmptyRelation",
    );
}

#[test]
fn test_int_decimal_default() {
    quick_test(
        "SELECT CAST(10 AS DECIMAL)",
        "Projection: CAST(Int64(10) AS Decimal128(38, 10))\
             \n  EmptyRelation",
    );
}

#[test]
fn test_int_decimal_no_scale() {
    quick_test(
        "SELECT CAST(10 AS DECIMAL(5))",
        "Projection: CAST(Int64(10) AS Decimal128(5, 0))\
             \n  EmptyRelation",
    );
}

#[test]
fn test_tinyint() {
    quick_test(
        "SELECT CAST(6 AS TINYINT)",
        "Projection: CAST(Int64(6) AS Int8)\
             \n  EmptyRelation",
    );
}

#[test]
fn cast_from_subquery() {
    quick_test(
        "SELECT CAST (a AS FLOAT) FROM (SELECT 1 AS a)",
        "Projection: CAST(a AS Float32)\
            \n  Projection: Int64(1) AS a\
            \n    EmptyRelation",
    );
}

#[test]
fn try_cast_from_aggregation() {
    quick_test(
        "SELECT TRY_CAST(SUM(age) AS FLOAT) FROM person",
        "Projection: TRY_CAST(SUM(person.age) AS Float32)\
            \n  Aggregate: groupBy=[[]], aggr=[[SUM(person.age)]]\
            \n    TableScan: person",
    );
}

#[test]
fn cast_to_invalid_decimal_type_precision_0() {
    // precision == 0
    {
        let sql = "SELECT CAST(10 AS DECIMAL(0))";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Error during planning: Decimal(precision = 0, scale = 0) should satisfy `0 < precision <= 76`, and `scale <= precision`.",
            err.strip_backtrace()
        );
    }
}

#[test]
fn cast_to_invalid_decimal_type_precision_gt_38() {
    // precision > 38
    {
        let sql = "SELECT CAST(10 AS DECIMAL(39))";
        let plan = "Projection: CAST(Int64(10) AS Decimal256(39, 0))\n  EmptyRelation";
        quick_test(sql, plan);
    }
}

#[test]
fn cast_to_invalid_decimal_type_precision_gt_76() {
    // precision > 76
    {
        let sql = "SELECT CAST(10 AS DECIMAL(79))";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Error during planning: Decimal(precision = 79, scale = 0) should satisfy `0 < precision <= 76`, and `scale <= precision`.",
            err.strip_backtrace()
        );
    }
}

#[test]
fn cast_to_invalid_decimal_type_precision_lt_scale() {
    // precision < scale
    {
        let sql = "SELECT CAST(10 AS DECIMAL(5, 10))";
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Error during planning: Decimal(precision = 5, scale = 10) should satisfy `0 < precision <= 76`, and `scale <= precision`.",
            err.strip_backtrace()
        );
    }
}

#[test]
fn plan_create_table_with_pk() {
    let sql = "create table person (id int, name string, primary key(id))";
    let plan = r#"
CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0])]
  EmptyRelation
    "#
    .trim();
    quick_test(sql, plan);

    let sql = "create table person (id int primary key, name string)";
    let plan = r#"
CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0])]
  EmptyRelation
    "#
    .trim();
    quick_test(sql, plan);

    let sql =
        "create table person (id int, name string unique not null, primary key(id))";
    let plan = r#"
CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0]), Unique([1])]
  EmptyRelation
    "#
    .trim();
    quick_test(sql, plan);

    let sql = "create table person (id int, name varchar,  primary key(name,  id));";
    let plan = r#"
CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([1, 0])]
  EmptyRelation
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn plan_create_table_with_multi_pk() {
    let sql = "create table person (id int, name string primary key, primary key(id))";
    let plan = r#"
CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0]), PrimaryKey([1])]
  EmptyRelation
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn plan_create_table_with_unique() {
    let sql = "create table person (id int unique, name string)";
    let plan = "CreateMemoryTable: Bare { table: \"person\" } constraints=[Unique([0])]\n  EmptyRelation";
    quick_test(sql, plan);
}

#[test]
fn plan_create_table_no_pk() {
    let sql = "create table person (id int, name string)";
    let plan = r#"
CreateMemoryTable: Bare { table: "person" }
  EmptyRelation
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn plan_create_table_check_constraint() {
    let sql = "create table person (id int, name string, unique(id))";
    let plan = "CreateMemoryTable: Bare { table: \"person\" } constraints=[Unique([0])]\n  EmptyRelation";
    quick_test(sql, plan);
}

#[test]
fn plan_start_transaction() {
    let sql = "start transaction";
    let plan = "TransactionStart: ReadWrite Serializable";
    quick_test(sql, plan);
}

#[test]
fn plan_start_transaction_isolation() {
    let sql = "start transaction isolation level read committed";
    let plan = "TransactionStart: ReadWrite ReadCommitted";
    quick_test(sql, plan);
}

#[test]
fn plan_start_transaction_read_only() {
    let sql = "start transaction read only";
    let plan = "TransactionStart: ReadOnly Serializable";
    quick_test(sql, plan);
}

#[test]
fn plan_start_transaction_fully_qualified() {
    let sql = "start transaction isolation level read committed read only";
    let plan = "TransactionStart: ReadOnly ReadCommitted";
    quick_test(sql, plan);
}

#[test]
fn plan_start_transaction_overly_qualified() {
    let sql = r#"start transaction
isolation level read committed
read only
isolation level repeatable read
"#;
    let plan = "TransactionStart: ReadOnly RepeatableRead";
    quick_test(sql, plan);
}

#[test]
fn plan_commit_transaction() {
    let sql = "commit transaction";
    let plan = "TransactionEnd: Commit chain:=false";
    quick_test(sql, plan);
}

#[test]
fn plan_commit_transaction_chained() {
    let sql = "commit transaction and chain";
    let plan = "TransactionEnd: Commit chain:=true";
    quick_test(sql, plan);
}

#[test]
fn plan_rollback_transaction() {
    let sql = "rollback transaction";
    let plan = "TransactionEnd: Rollback chain:=false";
    quick_test(sql, plan);
}

#[test]
fn plan_rollback_transaction_chained() {
    let sql = "rollback transaction and chain";
    let plan = "TransactionEnd: Rollback chain:=true";
    quick_test(sql, plan);
}

#[test]
fn plan_copy_to() {
    let sql = "COPY test_decimal to 'output.csv'";
    let plan = r#"
CopyTo: format=csv output_url=output.csv single_file_output=true options: ()
  TableScan: test_decimal
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn plan_explain_copy_to() {
    let sql = "EXPLAIN COPY test_decimal to 'output.csv'";
    let plan = r#"
Explain
  CopyTo: format=csv output_url=output.csv single_file_output=true options: ()
    TableScan: test_decimal
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn plan_copy_to_query() {
    let sql = "COPY (select * from test_decimal limit 10) to 'output.csv'";
    let plan = r#"
CopyTo: format=csv output_url=output.csv single_file_output=true options: ()
  Limit: skip=0, fetch=10
    Projection: test_decimal.id, test_decimal.price
      TableScan: test_decimal
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn plan_insert() {
    let sql =
        "insert into person (id, first_name, last_name) values (1, 'Alan', 'Turing')";
    let plan = "Dml: op=[Insert Into] table=[person]\
                \n  Projection: CAST(column1 AS UInt32) AS id, column2 AS first_name, column3 AS last_name, \
                        CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, \
                        CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀\
                \n    Values: (Int64(1), Utf8(\"Alan\"), Utf8(\"Turing\"))";
    quick_test(sql, plan);
}

#[test]
fn plan_insert_no_target_columns() {
    let sql = "INSERT INTO test_decimal VALUES (1, 2), (3, 4)";
    let plan = r#"
Dml: op=[Insert Into] table=[test_decimal]
  Projection: CAST(column1 AS Int32) AS id, CAST(column2 AS Decimal128(10, 2)) AS price
    Values: (Int64(1), Int64(2)), (Int64(3), Int64(4))
    "#
    .trim();
    quick_test(sql, plan);
}

#[rstest]
#[case::duplicate_columns(
    "INSERT INTO test_decimal (id, price, price) VALUES (1, 2, 3), (4, 5, 6)",
    "Schema error: Schema contains duplicate unqualified field name price"
)]
#[case::non_existing_column(
    "INSERT INTO test_decimal (nonexistent, price) VALUES (1, 2), (4, 5)",
    "Schema error: No field named nonexistent. Valid fields are id, price."
)]
#[case::type_mismatch(
    "INSERT INTO test_decimal SELECT '2022-01-01', to_timestamp('2022-01-01T12:00:00')",
    "Error during planning: Cannot automatically convert Timestamp(Nanosecond, None) to Decimal128(10, 2)"
)]
#[case::target_column_count_mismatch(
    "INSERT INTO person (id, first_name, last_name) VALUES ($1, $2)",
    "Error during planning: Column count doesn't match insert query!"
)]
#[case::source_column_count_mismatch(
    "INSERT INTO person VALUES ($1, $2)",
    "Error during planning: Column count doesn't match insert query!"
)]
#[case::extra_placeholder(
    "INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3, $4)",
    "Error during planning: Placeholder $4 refers to a non existent column"
)]
#[case::placeholder_type_unresolved(
    "INSERT INTO person (id, first_name, last_name) VALUES ($2, $4, $6)",
    "Error during planning: Placeholder type could not be resolved"
)]
#[test]
fn test_insert_schema_errors(#[case] sql: &str, #[case] error: &str) {
    let err = logical_plan(sql).unwrap_err();
    assert_eq!(err.strip_backtrace(), error)
}

#[test]
fn plan_update() {
    let sql = "update person set last_name='Kay' where id=1";
    let plan = r#"
Dml: op=[Update] table=[person]
  Projection: person.id AS id, person.first_name AS first_name, Utf8("Kay") AS last_name, person.age AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
    Filter: person.id = Int64(1)
      TableScan: person
      "#
    .trim();
    quick_test(sql, plan);
}

#[rstest]
#[case::missing_assignement_target("UPDATE person SET doesnotexist = true")]
#[case::missing_assignement_expression("UPDATE person SET age = doesnotexist + 42")]
#[case::missing_selection_expression(
    "UPDATE person SET age = 42 WHERE doesnotexist = true"
)]
#[test]
fn update_column_does_not_exist(#[case] sql: &str) {
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn plan_delete() {
    let sql = "delete from person where id=1";
    let plan = r#"
Dml: op=[Delete] table=[person]
  Filter: id = Int64(1)
    TableScan: person
    "#
    .trim();
    quick_test(sql, plan);
}

#[test]
fn select_column_does_not_exist() {
    let sql = "SELECT doesnotexist FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_repeated_column() {
    let sql = "SELECT age, age FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projections require unique expression names but the expression \"person.age\" at position 0 and \"person.age\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.",
        err.strip_backtrace()
    );
}

#[test]
fn select_wildcard_with_repeated_column() {
    let sql = "SELECT *, age FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projections require unique expression names but the expression \"person.age\" at position 3 and \"person.age\" at position 8 have the same name. Consider aliasing (\"AS\") one of them.",
        err.strip_backtrace()
    );
}

#[test]
fn select_wildcard_with_repeated_column_but_is_aliased() {
    quick_test(
            "SELECT *, first_name AS fn from person",
            "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, person.first_name AS fn\
            \n  TableScan: person",
        );
}

#[test]
fn select_scalar_func_with_literal_no_relation() {
    quick_test(
        "SELECT sqrt(9)",
        "Projection: sqrt(Int64(9))\
             \n  EmptyRelation",
    );
}

#[test]
fn select_simple_filter() {
    let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
    let expected = "Projection: person.id, person.first_name, person.last_name\
                        \n  Filter: person.state = Utf8(\"CO\")\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_filter_column_does_not_exist() {
    let sql = "SELECT first_name FROM person WHERE doesnotexist = 'A'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_filter_cannot_use_alias() {
    let sql = "SELECT first_name AS x FROM person WHERE x = 'A'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "x");
}

#[test]
fn select_neg_filter() {
    let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
    let expected = "Projection: person.id, person.first_name, person.last_name\
                        \n  Filter: NOT person.state\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_compound_filter() {
    let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
    let expected = "Projection: person.id, person.first_name, person.last_name\
            \n  Filter: person.state = Utf8(\"CO\") AND person.age >= Int64(21) AND person.age <= Int64(65)\
            \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_timestamp_filter() {
    let sql = "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";

    let expected = "Projection: person.state\
            \n  Filter: person.birth_date < CAST(Int64(158412331400600000) AS Timestamp(Nanosecond, None))\
            \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn test_date_filter() {
    let sql = "SELECT state FROM person WHERE birth_date < CAST ('2020-01-01' as date)";

    let expected = "Projection: person.state\
            \n  Filter: person.birth_date < CAST(Utf8(\"2020-01-01\") AS Date32)\
            \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_all_boolean_operators() {
    let sql = "SELECT age, first_name, last_name \
                   FROM person \
                   WHERE age = 21 \
                   AND age != 21 \
                   AND age > 21 \
                   AND age >= 21 \
                   AND age < 65 \
                   AND age <= 65";
    let expected = "Projection: person.age, person.first_name, person.last_name\
                        \n  Filter: person.age = Int64(21) \
                        AND person.age != Int64(21) \
                        AND person.age > Int64(21) \
                        AND person.age >= Int64(21) \
                        AND person.age < Int64(65) \
                        AND person.age <= Int64(65)\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_between() {
    let sql = "SELECT state FROM person WHERE age BETWEEN 21 AND 65";
    let expected = "Projection: person.state\
            \n  Filter: person.age BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_between_negated() {
    let sql = "SELECT state FROM person WHERE age NOT BETWEEN 21 AND 65";
    let expected = "Projection: person.state\
            \n  Filter: person.age NOT BETWEEN Int64(21) AND Int64(65)\
            \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_nested() {
    let sql = "SELECT fn2, last_name
                   FROM (
                     SELECT fn1 as fn2, last_name, birth_date
                     FROM (
                       SELECT first_name AS fn1, last_name, birth_date, age
                       FROM person
                     ) AS a
                   ) AS b";
    let expected = "Projection: b.fn2, b.last_name\
        \n  SubqueryAlias: b\
        \n    Projection: a.fn1 AS fn2, a.last_name, a.birth_date\
        \n      SubqueryAlias: a\
        \n        Projection: person.first_name AS fn1, person.last_name, person.birth_date, person.age\
        \n          TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_nested_with_filters() {
    let sql = "SELECT fn1, age
                   FROM (
                     SELECT first_name AS fn1, age
                     FROM person
                     WHERE age > 20
                   ) AS a
                   WHERE fn1 = 'X' AND age < 30";

    let expected = "Projection: a.fn1, a.age\
        \n  Filter: a.fn1 = Utf8(\"X\") AND a.age < Int64(30)\
        \n    SubqueryAlias: a\
        \n      Projection: person.first_name AS fn1, person.age\
        \n        Filter: person.age > Int64(20)\
        \n          TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn table_with_column_alias() {
    let sql = "SELECT a, b, c
                   FROM lineitem l (a, b, c)";
    let expected = "Projection: l.a, l.b, l.c\
        \n  SubqueryAlias: l\
        \n    Projection: lineitem.l_item_id AS a, lineitem.l_description AS b, lineitem.price AS c\
        \n      TableScan: lineitem";

    quick_test(sql, expected);
}

#[test]
fn table_with_column_alias_number_cols() {
    let sql = "SELECT a, b, c
                   FROM lineitem l (a, b)";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Source table contains 3 columns but only 2 names given as column alias",
        err.strip_backtrace()
    );
}

#[test]
fn select_with_ambiguous_column() {
    let sql = "SELECT id FROM person a, person b";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Schema error: Ambiguous reference to unqualified field id",
        err.strip_backtrace()
    );
}

#[test]
fn join_with_ambiguous_column() {
    // This is legal.
    let sql = "SELECT id FROM person a join person b using(id)";
    let expected = "Projection: a.id\
                        \n  Inner Join: Using a.id = b.id\
                        \n    SubqueryAlias: a\
                        \n      TableScan: person\
                        \n    SubqueryAlias: b\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn where_selection_with_ambiguous_column() {
    let sql = "SELECT * FROM person a, person b WHERE id = id + 1";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "SchemaError(AmbiguousReference { field: Column { relation: None, name: \"id\" } })",
        format!("{err:?}")
    );
}

#[test]
fn natural_join() {
    let sql = "SELECT * FROM lineitem a NATURAL JOIN lineitem b";
    let expected = "Projection: a.l_item_id, a.l_description, a.price\
                        \n  Inner Join: Using a.l_item_id = b.l_item_id, a.l_description = b.l_description, a.price = b.price\
                        \n    SubqueryAlias: a\
                        \n      TableScan: lineitem\
                        \n    SubqueryAlias: b\
                        \n      TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn natural_left_join() {
    let sql = "SELECT l_item_id FROM lineitem a NATURAL LEFT JOIN lineitem b";
    let expected = "Projection: a.l_item_id\
                        \n  Left Join: Using a.l_item_id = b.l_item_id, a.l_description = b.l_description, a.price = b.price\
                        \n    SubqueryAlias: a\
                        \n      TableScan: lineitem\
                        \n    SubqueryAlias: b\
                        \n      TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn natural_right_join() {
    let sql = "SELECT l_item_id FROM lineitem a NATURAL RIGHT JOIN lineitem b";
    let expected = "Projection: a.l_item_id\
                        \n  Right Join: Using a.l_item_id = b.l_item_id, a.l_description = b.l_description, a.price = b.price\
                        \n    SubqueryAlias: a\
                        \n      TableScan: lineitem\
                        \n    SubqueryAlias: b\
                        \n      TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn natural_join_no_common_becomes_cross_join() {
    let sql = "SELECT * FROM person a NATURAL JOIN lineitem b";
    let expected = "Projection: a.id, a.first_name, a.last_name, a.age, a.state, a.salary, a.birth_date, a.😀, b.l_item_id, b.l_description, b.price\
                        \n  CrossJoin:\
                        \n    SubqueryAlias: a\
                        \n      TableScan: person\
                        \n    SubqueryAlias: b\
                        \n      TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn using_join_multiple_keys() {
    let sql = "SELECT * FROM person a join person b using (id, age)";
    let expected = "Projection: a.id, a.first_name, a.last_name, a.age, a.state, a.salary, a.birth_date, a.😀, \
        b.first_name, b.last_name, b.state, b.salary, b.birth_date, b.😀\
                        \n  Inner Join: Using a.id = b.id, a.age = b.age\
                        \n    SubqueryAlias: a\
                        \n      TableScan: person\
                        \n    SubqueryAlias: b\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn using_join_multiple_keys_subquery() {
    let sql =
        "SELECT age FROM (SELECT * FROM person a join person b using (id, age, state))";
    let expected = "Projection: a.age\
                        \n  Projection: a.id, a.first_name, a.last_name, a.age, a.state, a.salary, a.birth_date, a.😀, \
        b.first_name, b.last_name, b.salary, b.birth_date, b.😀\
                        \n    Inner Join: Using a.id = b.id, a.age = b.age, a.state = b.state\
                        \n      SubqueryAlias: a\
                        \n        TableScan: person\
                        \n      SubqueryAlias: b\
                        \n        TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn using_join_multiple_keys_qualified_wildcard_select() {
    let sql = "SELECT a.* FROM person a join person b using (id, age)";
    let expected =
        "Projection: a.id, a.first_name, a.last_name, a.age, a.state, a.salary, a.birth_date, a.😀\
                        \n  Inner Join: Using a.id = b.id, a.age = b.age\
                        \n    SubqueryAlias: a\
                        \n      TableScan: person\
                        \n    SubqueryAlias: b\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn using_join_multiple_keys_select_all_columns() {
    let sql = "SELECT a.*, b.* FROM person a join person b using (id, age)";
    let expected = "Projection: a.id, a.first_name, a.last_name, a.age, a.state, a.salary, a.birth_date, a.😀, \
        b.id, b.first_name, b.last_name, b.age, b.state, b.salary, b.birth_date, b.😀\
                        \n  Inner Join: Using a.id = b.id, a.age = b.age\
                        \n    SubqueryAlias: a\
                        \n      TableScan: person\
                        \n    SubqueryAlias: b\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn using_join_multiple_keys_multiple_joins() {
    let sql = "SELECT * FROM person a join person b using (id, age, state) join person c using (id, age, state)";
    let expected = "Projection: a.id, a.first_name, a.last_name, a.age, a.state, a.salary, a.birth_date, a.😀, \
        b.first_name, b.last_name, b.salary, b.birth_date, b.😀, \
        c.first_name, c.last_name, c.salary, c.birth_date, c.😀\
                        \n  Inner Join: Using a.id = c.id, a.age = c.age, a.state = c.state\
                        \n    Inner Join: Using a.id = b.id, a.age = b.age, a.state = b.state\
                        \n      SubqueryAlias: a\
                        \n        TableScan: person\
                        \n      SubqueryAlias: b\
                        \n        TableScan: person\
                        \n    SubqueryAlias: c\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_with_having() {
    let sql = "SELECT id, age
                   FROM person
                   HAVING age > 100 AND age < 200";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
            "Error during planning: HAVING clause references: person.age > Int64(100) AND person.age < Int64(200) must appear in the GROUP BY clause or be used in an aggregate function",
            err.strip_backtrace()
        );
}

#[test]
fn select_with_having_referencing_column_not_in_select() {
    let sql = "SELECT id, age
                   FROM person
                   HAVING first_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
            "Error during planning: HAVING clause references: person.first_name = Utf8(\"M\") must appear in the GROUP BY clause or be used in an aggregate function",
            err.strip_backtrace()
        );
}

#[test]
fn select_with_having_refers_to_invalid_column() {
    let sql = "SELECT id, MAX(age)
                   FROM person
                   GROUP BY id
                   HAVING first_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
            "Error during planning: HAVING clause references non-aggregate values: Expression person.first_name could not be resolved from available columns: person.id, MAX(person.age)",
            err.strip_backtrace()
        );
}

#[test]
fn select_with_having_referencing_column_nested_in_select_expression() {
    let sql = "SELECT id, age + 1
                   FROM person
                   HAVING age > 100";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
            "Error during planning: HAVING clause references: person.age > Int64(100) must appear in the GROUP BY clause or be used in an aggregate function",
            err.strip_backtrace()
        );
}

#[test]
fn select_with_having_with_aggregate_not_in_select() {
    let sql = "SELECT first_name
                   FROM person
                   HAVING MAX(age) > 100";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
            "Error during planning: Projection references non-aggregate values: Expression person.first_name could not be resolved from available columns: MAX(person.age)",
            err.strip_backtrace()
        );
}

#[test]
fn select_aggregate_with_having_that_reuses_aggregate() {
    let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(age) < 30";
    let expected = "Projection: MAX(person.age)\
                        \n  Filter: MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_having_with_aggregate_not_in_select() {
    let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(first_name) > 'M'";
    let expected = "Projection: MAX(person.age)\
                        \n  Filter: MAX(person.first_name) > Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age), MAX(person.first_name)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_having_referencing_column_not_in_select() {
    let sql = "SELECT COUNT(*)
                   FROM person
                   HAVING first_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: HAVING clause references non-aggregate values: Expression person.first_name could not be resolved from available columns: COUNT(*)",
        err.strip_backtrace()
    );
}

#[test]
fn select_aggregate_aliased_with_having_referencing_aggregate_by_its_alias() {
    let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING max_age < 30";
    // FIXME: add test for having in execution
    let expected = "Projection: MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_aliased_with_having_that_reuses_aggregate_but_not_by_its_alias() {
    let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING MAX(age) < 30";
    let expected = "Projection: MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) < Int64(30)\
                        \n    Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING first_name = 'M'";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: person.first_name = Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_and_where() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) < Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      Filter: person.id > Int64(5)\
                        \n        TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_and_where_filtering_on_aggregate_column() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5 AND age > 18
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) < Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      Filter: person.id > Int64(5) AND person.age > Int64(18)\
                        \n        TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_using_column_by_alias() {
    let sql = "SELECT first_name AS fn, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND fn = 'M'";
    let expected = "Projection: person.first_name AS fn, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(2) AND person.first_name = Utf8(\"M\")\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_using_columns_with_and_without_their_aliases(
) {
    let sql = "SELECT first_name AS fn, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND max_age < 5 AND first_name = 'M' AND fn = 'N'";
    let expected = "Projection: person.first_name AS fn, MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) > Int64(2) AND MAX(person.age) < Int64(5) AND person.first_name = Utf8(\"M\") AND person.first_name = Utf8(\"N\")\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_that_reuses_aggregate() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_referencing_column_not_in_group_by() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 10 AND last_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: HAVING clause references non-aggregate values: Expression person.last_name could not be resolved from available columns: person.first_name, MAX(person.age)",
        err.strip_backtrace()
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_that_reuses_aggregate_multiple_times() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MAX(age) < 200";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND MAX(person.age) < Int64(200)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_using_aggreagate_not_in_select() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id) < 50";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND MIN(person.id) < Int64(50)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age), MIN(person.id)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_aliased_with_group_by_with_having_referencing_aggregate_by_its_alias()
{
    let sql = "SELECT first_name, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING max_age > 100";
    let expected = "Projection: person.first_name, MAX(person.age) AS max_age\
                        \n  Filter: MAX(person.age) > Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_compound_aliased_with_group_by_with_having_referencing_compound_aggregate_by_its_alias(
) {
    let sql = "SELECT first_name, MAX(age) + 1 AS max_age_plus_one
                   FROM person
                   GROUP BY first_name
                   HAVING max_age_plus_one > 100";
    let expected = "Projection: person.first_name, MAX(person.age) + Int64(1) AS max_age_plus_one\
                        \n  Filter: MAX(person.age) + Int64(1) > Int64(100)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_using_derived_column_aggreagate_not_in_select(
) {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id - 2) < 50";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND MIN(person.id - Int64(2)) < Int64(50)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age), MIN(person.id - Int64(2))]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aggregate_with_group_by_with_having_using_count_star_not_in_select() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND COUNT(*) < 50";
    let expected = "Projection: person.first_name, MAX(person.age)\
                        \n  Filter: MAX(person.age) > Int64(100) AND COUNT(*) < Int64(50)\
                        \n    Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.age), COUNT(*)]]\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_binary_expr() {
    let sql = "SELECT age + salary from person";
    let expected = "Projection: person.age + person.salary\
                        \n  TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_binary_expr_nested() {
    let sql = "SELECT (age + salary)/2 from person";
    let expected = "Projection: (person.age + person.salary) / Int64(2)\
                        \n  TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_at_arrow_operator() {
    let sql = "SELECT left @> right from array";
    let expected = "Projection: array.left @> array.right\
                        \n  TableScan: array";
    quick_test(sql, expected);
}

#[test]
fn select_arrow_at_operator() {
    let sql = "SELECT left <@ right from array";
    let expected = "Projection: array.left <@ array.right\
                        \n  TableScan: array";
    quick_test(sql, expected);
}

#[test]
fn select_wildcard_with_groupby() {
    quick_test(
            r#"SELECT * FROM person GROUP BY id, first_name, last_name, age, state, salary, birth_date, "😀""#,
            "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
             \n  Aggregate: groupBy=[[person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀]], aggr=[[]]\
             \n    TableScan: person",
        );
    quick_test(
            "SELECT * FROM (SELECT first_name, last_name FROM person) AS a GROUP BY first_name, last_name",
            "Projection: a.first_name, a.last_name\
            \n  Aggregate: groupBy=[[a.first_name, a.last_name]], aggr=[[]]\
            \n    SubqueryAlias: a\
            \n      Projection: person.first_name, person.last_name\
            \n        TableScan: person",
        );
}

#[test]
fn select_simple_aggregate() {
    quick_test(
        "SELECT MIN(age) FROM person",
        "Projection: MIN(person.age)\
            \n  Aggregate: groupBy=[[]], aggr=[[MIN(person.age)]]\
            \n    TableScan: person",
    );
}

#[test]
fn test_sum_aggregate() {
    quick_test(
        "SELECT SUM(age) from person",
        "Projection: SUM(person.age)\
            \n  Aggregate: groupBy=[[]], aggr=[[SUM(person.age)]]\
            \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_column_does_not_exist() {
    let sql = "SELECT MIN(doesnotexist) FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_simple_aggregate_repeated_aggregate() {
    let sql = "SELECT MIN(age), MIN(age) FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projections require unique expression names but the expression \"MIN(person.age)\" at position 0 and \"MIN(person.age)\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.",
        err.strip_backtrace()
    );
}

#[test]
fn select_simple_aggregate_repeated_aggregate_with_single_alias() {
    quick_test(
        "SELECT MIN(age), MIN(age) AS a FROM person",
        "Projection: MIN(person.age), MIN(person.age) AS a\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_repeated_aggregate_with_unique_aliases() {
    quick_test(
        "SELECT MIN(age) AS a, MIN(age) AS b FROM person",
        "Projection: MIN(person.age) AS a, MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_respect_nulls() {
    let sql = "SELECT MIN(age) RESPECT NULLS FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_contains!(
        err.strip_backtrace(),
        "This feature is not implemented: Null treatment in aggregate functions is not supported: RESPECT NULLS"
    );
}
#[test]
fn select_from_typed_string_values() {
    quick_test(
            "SELECT col1, col2 FROM (VALUES (TIMESTAMP '2021-06-10 17:01:00Z', DATE '2004-04-09')) as t (col1, col2)",
            "Projection: t.col1, t.col2\
            \n  SubqueryAlias: t\
            \n    Projection: column1 AS col1, column2 AS col2\
            \n      Values: (CAST(Utf8(\"2021-06-10 17:01:00Z\") AS Timestamp(Nanosecond, None)), CAST(Utf8(\"2004-04-09\") AS Date32))",
        );
}

#[test]
fn select_simple_aggregate_repeated_aggregate_with_repeated_aliases() {
    let sql = "SELECT MIN(age) AS a, MIN(age) AS a FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projections require unique expression names but the expression \"MIN(person.age) AS a\" at position 0 and \"MIN(person.age) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.",
        err.strip_backtrace()
    );
}

#[test]
fn select_simple_aggregate_with_groupby() {
    quick_test(
        "SELECT state, MIN(age), MAX(age) FROM person GROUP BY state",
        "Projection: person.state, MIN(person.age), MAX(person.age)\
            \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age), MAX(person.age)]]\
            \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_with_aliases() {
    quick_test(
        "SELECT state AS a, MIN(age) AS b FROM person GROUP BY state",
        "Projection: person.state AS a, MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_with_aliases_repeated() {
    let sql = "SELECT state AS a, MIN(age) AS a FROM person GROUP BY state";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projections require unique expression names but the expression \"person.state AS a\" at position 0 and \"MIN(person.age) AS a\" at position 1 have the same name. Consider aliasing (\"AS\") one of them.",
        err.strip_backtrace()
    );
}

#[test]
fn select_simple_aggregate_with_groupby_column_unselected() {
    quick_test(
        "SELECT MIN(age), MAX(age) FROM person GROUP BY state",
        "Projection: MIN(person.age), MAX(person.age)\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age), MAX(person.age)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_and_column_in_group_by_does_not_exist() {
    let sql = "SELECT SUM(age) FROM person GROUP BY doesnotexist";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!("Schema error: No field named doesnotexist. Valid fields are \"SUM(person.age)\", \
        person.id, person.first_name, person.last_name, person.age, person.state, \
        person.salary, person.birth_date, person.\"😀\".", err.strip_backtrace());
}

#[test]
fn select_simple_aggregate_with_groupby_and_column_in_aggregate_does_not_exist() {
    let sql = "SELECT SUM(doesnotexist) FROM person GROUP BY first_name";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_interval_out_of_range() {
    let sql = "SELECT INTERVAL '100000000000000000 day'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Arrow error: Invalid argument error: Unable to represent 100000000000000000 days in a signed 32-bit integer",
        err.strip_backtrace(),
    );
}

#[test]
fn select_array_no_common_type() {
    let sql = "SELECT [1, true, null]";
    let err = logical_plan(sql).expect_err("query should have failed");

    // HashSet doesn't guarantee order
    assert_contains!(
        err.strip_backtrace(),
        "This feature is not implemented: Arrays with different types are not supported: "
    );
}

#[test]
fn select_array_non_literal_type() {
    let sql = "SELECT [now()]";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "This feature is not implemented: Arrays with elements other than literal are not supported: now()",
        err.strip_backtrace()
    );
}

#[test]
fn select_simple_aggregate_with_groupby_and_column_is_in_aggregate_and_groupby() {
    quick_test(
        "SELECT MAX(first_name) FROM person GROUP BY first_name",
        "Projection: MAX(person.first_name)\
             \n  Aggregate: groupBy=[[person.first_name]], aggr=[[MAX(person.first_name)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_can_use_positions() {
    quick_test(
        "SELECT state, age AS b, COUNT(1) FROM person GROUP BY 1, 2",
        "Projection: person.state, person.age AS b, COUNT(Int64(1))\
             \n  Aggregate: groupBy=[[person.state, person.age]], aggr=[[COUNT(Int64(1))]]\
             \n    TableScan: person",
    );
    quick_test(
        "SELECT state, age AS b, COUNT(1) FROM person GROUP BY 2, 1",
        "Projection: person.state, person.age AS b, COUNT(Int64(1))\
             \n  Aggregate: groupBy=[[person.age, person.state]], aggr=[[COUNT(Int64(1))]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_position_out_of_range() {
    let sql = "SELECT state, MIN(age) FROM person GROUP BY 0";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projection references non-aggregate values: Expression person.state could not be resolved from available columns: Int64(0), MIN(person.age)",
            err.strip_backtrace()
        );

    let sql2 = "SELECT state, MIN(age) FROM person GROUP BY 5";
    let err2 = logical_plan(sql2).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projection references non-aggregate values: Expression person.state could not be resolved from available columns: Int64(5), MIN(person.age)",
            err2.strip_backtrace()
        );
}

#[test]
fn select_simple_aggregate_with_groupby_can_use_alias() {
    quick_test(
        "SELECT state AS a, MIN(age) AS b FROM person GROUP BY a",
        "Projection: person.state AS a, MIN(person.age) AS b\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_aggregate_repeated() {
    let sql = "SELECT state, MIN(age), MIN(age) FROM person GROUP BY state";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projections require unique expression names but the expression \"MIN(person.age)\" at position 1 and \"MIN(person.age)\" at position 2 have the same name. Consider aliasing (\"AS\") one of them.",
        err.strip_backtrace()
    );
}

#[test]
fn select_simple_aggregate_with_groupby_aggregate_repeated_and_one_has_alias() {
    quick_test(
        "SELECT state, MIN(age), MIN(age) AS ma FROM person GROUP BY state",
        "Projection: person.state, MIN(person.age), MIN(person.age) AS ma\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
    )
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_unselected() {
    quick_test(
        "SELECT MIN(first_name) FROM person GROUP BY age + 1",
        "Projection: MIN(person.first_name)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_selected_and_resolvable() {
    quick_test(
        "SELECT age + 1, MIN(first_name) FROM person GROUP BY age + 1",
        "Projection: person.age + Int64(1), MIN(person.first_name)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
    );
    quick_test(
        "SELECT MIN(first_name), age + 1 FROM person GROUP BY age + 1",
        "Projection: MIN(person.first_name), person.age + Int64(1)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_resolvable() {
    quick_test(
            "SELECT ((age + 1) / 2) * (age + 1), MIN(first_name) FROM person GROUP BY age + 1",
            "Projection: person.age + Int64(1) / Int64(2) * person.age + Int64(1), MIN(person.first_name)\
             \n  Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[MIN(person.first_name)]]\
             \n    TableScan: person",
        );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_not_resolvable()
{
    // The query should fail, because age + 9 is not in the group by.
    let sql = "SELECT ((age + 1) / 2) * (age + 9), MIN(first_name) FROM person GROUP BY age + 1";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projection references non-aggregate values: Expression person.age could not be resolved from available columns: person.age + Int64(1), MIN(person.first_name)",
            err.strip_backtrace()
        );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_and_its_column_selected() {
    let sql = "SELECT age, MIN(first_name) FROM person GROUP BY age + 1";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projection references non-aggregate values: Expression person.age could not be resolved from available columns: person.age + Int64(1), MIN(person.first_name)",
            err.strip_backtrace()
        );
}

#[test]
fn select_simple_aggregate_nested_in_binary_expr_with_groupby() {
    quick_test(
        "SELECT state, MIN(age) < 10 FROM person GROUP BY state",
        "Projection: person.state, MIN(person.age) < Int64(10)\
             \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_simple_aggregate_and_nested_groupby_column() {
    quick_test(
        "SELECT age + 1, MAX(first_name) FROM person GROUP BY age",
        "Projection: person.age + Int64(1), MAX(person.first_name)\
             \n  Aggregate: groupBy=[[person.age]], aggr=[[MAX(person.first_name)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_aggregate_compounded_with_groupby_column() {
    quick_test(
        "SELECT age + MIN(salary) FROM person GROUP BY age",
        "Projection: person.age + MIN(person.salary)\
             \n  Aggregate: groupBy=[[person.age]], aggr=[[MIN(person.salary)]]\
             \n    TableScan: person",
    );
}

#[test]
fn select_aggregate_with_non_column_inner_expression_with_groupby() {
    quick_test(
        "SELECT state, MIN(age + 1) FROM person GROUP BY state",
        "Projection: person.state, MIN(person.age + Int64(1))\
            \n  Aggregate: groupBy=[[person.state]], aggr=[[MIN(person.age + Int64(1))]]\
            \n    TableScan: person",
    );
}

#[test]
fn test_wildcard() {
    quick_test(
            "SELECT * from person",
            "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
            \n  TableScan: person",
        );
}

#[test]
fn select_count_one() {
    let sql = "SELECT COUNT(1) FROM person";
    let expected = "Projection: COUNT(Int64(1))\
                        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(Int64(1))]]\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_count_column() {
    let sql = "SELECT COUNT(id) FROM person";
    let expected = "Projection: COUNT(person.id)\
                        \n  Aggregate: groupBy=[[]], aggr=[[COUNT(person.id)]]\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_approx_median() {
    let sql = "SELECT approx_median(age) FROM person";
    let expected = "Projection: APPROX_MEDIAN(person.age)\
                        \n  Aggregate: groupBy=[[]], aggr=[[APPROX_MEDIAN(person.age)]]\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_scalar_func() {
    let sql = "SELECT sqrt(age) FROM person";
    let expected = "Projection: sqrt(person.age)\
                        \n  TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_aliased_scalar_func() {
    let sql = "SELECT sqrt(person.age) AS square_people FROM person";
    let expected = "Projection: sqrt(person.age) AS square_people\
                        \n  TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_where_nullif_division() {
    let sql = "SELECT c3/(c4+c5) \
                   FROM aggregate_test_100 WHERE c3/nullif(c4+c5, 0) > 0.1";
    let expected = "Projection: aggregate_test_100.c3 / (aggregate_test_100.c4 + aggregate_test_100.c5)\
            \n  Filter: aggregate_test_100.c3 / nullif(aggregate_test_100.c4 + aggregate_test_100.c5, Int64(0)) > Float64(0.1)\
            \n    TableScan: aggregate_test_100";
    quick_test(sql, expected);
}

#[test]
fn select_where_with_negative_operator() {
    let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > -0.1 AND -c4 > 0";
    let expected = "Projection: aggregate_test_100.c3\
            \n  Filter: aggregate_test_100.c3 > Float64(-0.1) AND (- aggregate_test_100.c4) > Int64(0)\
            \n    TableScan: aggregate_test_100";
    quick_test(sql, expected);
}

#[test]
fn select_where_with_positive_operator() {
    let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > +0.1 AND +c4 > 0";
    let expected = "Projection: aggregate_test_100.c3\
            \n  Filter: aggregate_test_100.c3 > Float64(0.1) AND aggregate_test_100.c4 > Int64(0)\
            \n    TableScan: aggregate_test_100";
    quick_test(sql, expected);
}

#[test]
fn select_where_compound_identifiers() {
    let sql = "SELECT aggregate_test_100.c3 \
    FROM public.aggregate_test_100 \
    WHERE aggregate_test_100.c3 > 0.1";
    let expected = "Projection: public.aggregate_test_100.c3\
            \n  Filter: public.aggregate_test_100.c3 > Float64(0.1)\
            \n    TableScan: public.aggregate_test_100";
    quick_test(sql, expected);
}

#[test]
fn select_order_by_index() {
    let sql = "SELECT id FROM person ORDER BY 1";
    let expected = "Sort: person.id ASC NULLS LAST\
                        \n  Projection: person.id\
                        \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_order_by_multiple_index() {
    let sql = "SELECT id, state, age FROM person ORDER BY 1, 3";
    let expected = "Sort: person.id ASC NULLS LAST, person.age ASC NULLS LAST\
                        \n  Projection: person.id, person.state, person.age\
                        \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_order_by_index_of_0() {
    let sql = "SELECT id FROM person ORDER BY 0";
    let err = logical_plan(sql)
        .expect_err("query should have failed")
        .strip_backtrace();
    assert_eq!(
        "Error during planning: Order by index starts at 1 for column indexes",
        err
    );
}

#[test]
fn select_order_by_index_oob() {
    let sql = "SELECT id FROM person ORDER BY 2";
    let err = logical_plan(sql)
        .expect_err("query should have failed")
        .strip_backtrace();
    assert_eq!(
        "Error during planning: Order by column out of bounds, specified: 2, max: 1",
        err
    );
}

#[test]
fn select_order_by() {
    let sql = "SELECT id FROM person ORDER BY id";
    let expected = "Sort: person.id ASC NULLS LAST\
                        \n  Projection: person.id\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_order_by_desc() {
    let sql = "SELECT id FROM person ORDER BY id DESC";
    let expected = "Sort: person.id DESC NULLS FIRST\
                        \n  Projection: person.id\
                        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_order_by_nulls_last() {
    quick_test(
        "SELECT id FROM person ORDER BY id DESC NULLS LAST",
        "Sort: person.id DESC NULLS LAST\
            \n  Projection: person.id\
            \n    TableScan: person",
    );

    quick_test(
        "SELECT id FROM person ORDER BY id NULLS LAST",
        "Sort: person.id ASC NULLS LAST\
            \n  Projection: person.id\
            \n    TableScan: person",
    );
}

#[test]
fn select_group_by() {
    let sql = "SELECT state FROM person GROUP BY state";
    let expected = "Projection: person.state\
                        \n  Aggregate: groupBy=[[person.state]], aggr=[[]]\
                        \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_group_by_columns_not_in_select() {
    let sql = "SELECT MAX(age) FROM person GROUP BY state";
    let expected = "Projection: MAX(person.age)\
                        \n  Aggregate: groupBy=[[person.state]], aggr=[[MAX(person.age)]]\
                        \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_group_by_count_star() {
    let sql = "SELECT state, COUNT(*) FROM person GROUP BY state";
    let expected = "Projection: person.state, COUNT(*)\
                        \n  Aggregate: groupBy=[[person.state]], aggr=[[COUNT(*)]]\
                        \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_group_by_needs_projection() {
    let sql = "SELECT COUNT(state), state FROM person GROUP BY state";
    let expected = "\
        Projection: COUNT(person.state), person.state\
        \n  Aggregate: groupBy=[[person.state]], aggr=[[COUNT(person.state)]]\
        \n    TableScan: person";

    quick_test(sql, expected);
}

#[test]
fn select_7480_1() {
    let sql = "SELECT c1, MIN(c12) FROM aggregate_test_100 GROUP BY c1, c13";
    let expected = "Projection: aggregate_test_100.c1, MIN(aggregate_test_100.c12)\
                       \n  Aggregate: groupBy=[[aggregate_test_100.c1, aggregate_test_100.c13]], aggr=[[MIN(aggregate_test_100.c12)]]\
                       \n    TableScan: aggregate_test_100";
    quick_test(sql, expected);
}

#[test]
fn select_7480_2() {
    let sql = "SELECT c1, c13, MIN(c12) FROM aggregate_test_100 GROUP BY c1";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_eq!(
        "Error during planning: Projection references non-aggregate values: Expression aggregate_test_100.c13 could not be resolved from available columns: aggregate_test_100.c1, MIN(aggregate_test_100.c12)",
        err.strip_backtrace()
    );
}

#[test]
fn create_external_table_csv() {
    let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
    let expected = "CreateExternalTable: Bare { table: \"t\" }";
    quick_test(sql, expected);
}

#[test]
fn create_external_table_with_pk() {
    let sql = "CREATE EXTERNAL TABLE t(c1 int, primary key(c1)) STORED AS CSV LOCATION 'foo.csv'";
    let expected =
        "CreateExternalTable: Bare { table: \"t\" } constraints=[PrimaryKey([0])]";
    quick_test(sql, expected);
}

#[test]
fn create_schema_with_quoted_name() {
    let sql = "CREATE SCHEMA \"quoted_schema_name\"";
    let expected = "CreateCatalogSchema: \"quoted_schema_name\"";
    quick_test(sql, expected);
}

#[test]
fn create_schema_with_quoted_unnormalized_name() {
    let sql = "CREATE SCHEMA \"Foo\"";
    let expected = "CreateCatalogSchema: \"Foo\"";
    quick_test(sql, expected);
}

#[test]
fn create_schema_with_unquoted_normalized_name() {
    let sql = "CREATE SCHEMA Foo";
    let expected = "CreateCatalogSchema: \"foo\"";
    quick_test(sql, expected);
}

#[test]
fn create_external_table_custom() {
    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    let expected = r#"CreateExternalTable: Bare { table: "dt" }"#;
    quick_test(sql, expected);
}

#[test]
fn create_external_table_csv_no_schema() {
    let sql = "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo.csv'";
    let expected = "CreateExternalTable: Bare { table: \"t\" }";
    quick_test(sql, expected);
}

#[test]
fn create_external_table_with_compression_type() {
    // positive case
    let sqls = vec![
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE GZIP LOCATION 'foo.csv.gz'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE BZIP2 LOCATION 'foo.csv.bz2'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS JSON COMPRESSION TYPE GZIP LOCATION 'foo.json.gz'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS JSON COMPRESSION TYPE BZIP2 LOCATION 'foo.json.bz2'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS NONSTANDARD COMPRESSION TYPE GZIP LOCATION 'foo.unk'",
        ];
    for sql in sqls {
        let expected = "CreateExternalTable: Bare { table: \"t\" }";
        quick_test(sql, expected);
    }

    // negative case
    let sqls = vec![
        "CREATE EXTERNAL TABLE t STORED AS AVRO COMPRESSION TYPE GZIP LOCATION 'foo.avro'",
        "CREATE EXTERNAL TABLE t STORED AS AVRO COMPRESSION TYPE BZIP2 LOCATION 'foo.avro'",
        "CREATE EXTERNAL TABLE t STORED AS PARQUET COMPRESSION TYPE GZIP LOCATION 'foo.parquet'",
        "CREATE EXTERNAL TABLE t STORED AS PARQUET COMPRESSION TYPE BZIP2 LOCATION 'foo.parquet'",
        "CREATE EXTERNAL TABLE t STORED AS ARROW COMPRESSION TYPE GZIP LOCATION 'foo.arrow'",
        "CREATE EXTERNAL TABLE t STORED AS ARROW COMPRESSION TYPE BZIP2 LOCATION 'foo.arrow'",
    ];
    for sql in sqls {
        let err = logical_plan(sql).expect_err("query should have failed");
        assert_eq!(
            "Error during planning: File compression type cannot be set for PARQUET, AVRO, or ARROW files.",
            err.strip_backtrace()
        );
    }
}

#[test]
fn create_external_table_parquet() {
    let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS PARQUET LOCATION 'foo.parquet'";
    let expected = "CreateExternalTable: Bare { table: \"t\" }";
    quick_test(sql, expected);
}

#[test]
fn create_external_table_parquet_sort_order() {
    let sql = "create external table foo(a varchar, b varchar, c timestamp) stored as parquet location '/tmp/foo' with order (c)";
    let expected = "CreateExternalTable: Bare { table: \"foo\" }";
    quick_test(sql, expected);
}

#[test]
fn create_external_table_parquet_no_schema() {
    let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
    let expected = "CreateExternalTable: Bare { table: \"t\" }";
    quick_test(sql, expected);
}

#[test]
fn equijoin_explicit_syntax() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id";
    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id = orders.customer_id\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn equijoin_with_condition() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id AND order_id > 1 ";
    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id = orders.customer_id AND orders.order_id > Int64(1)\
            \n    TableScan: person\
            \n    TableScan: orders";

    quick_test(sql, expected);
}

#[test]
fn left_equijoin_with_conditions() {
    let sql = "SELECT id, order_id \
            FROM person \
            LEFT JOIN orders \
            ON id = customer_id AND order_id > 1 AND age < 30";
    let expected = "Projection: person.id, orders.order_id\
            \n  Left Join:  Filter: person.id = orders.customer_id AND orders.order_id > Int64(1) AND person.age < Int64(30)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn right_equijoin_with_conditions() {
    let sql = "SELECT id, order_id \
            FROM person \
            RIGHT JOIN orders \
            ON id = customer_id AND id > 1 AND order_id < 100";

    let expected = "Projection: person.id, orders.order_id\
            \n  Right Join:  Filter: person.id = orders.customer_id AND person.id > Int64(1) AND orders.order_id < Int64(100)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn full_equijoin_with_conditions() {
    let sql = "SELECT id, order_id \
            FROM person \
            FULL JOIN orders \
            ON id = customer_id AND id > 1 AND order_id < 100";
    let expected = "Projection: person.id, orders.order_id\
            \n  Full Join:  Filter: person.id = orders.customer_id AND person.id > Int64(1) AND orders.order_id < Int64(100)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn join_with_table_name() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON person.id = orders.customer_id";
    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id = orders.customer_id\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn join_with_using() {
    let sql = "SELECT person.first_name, id \
            FROM person \
            JOIN person as person2 \
            USING (id)";
    let expected = "Projection: person.first_name, person.id\
        \n  Inner Join: Using person.id = person2.id\
        \n    TableScan: person\
        \n    SubqueryAlias: person2\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn project_wildcard_on_join_with_using() {
    let sql = "SELECT * \
            FROM lineitem \
            JOIN lineitem as lineitem2 \
            USING (l_item_id)";
    let expected = "Projection: lineitem.l_item_id, lineitem.l_description, lineitem.price, lineitem2.l_description, lineitem2.price\
        \n  Inner Join: Using lineitem.l_item_id = lineitem2.l_item_id\
        \n    TableScan: lineitem\
        \n    SubqueryAlias: lineitem2\
        \n      TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn equijoin_explicit_syntax_3_tables() {
    let sql = "SELECT id, order_id, l_description \
            FROM person \
            JOIN orders ON id = customer_id \
            JOIN lineitem ON o_item_id = l_item_id";
    let expected = "Projection: person.id, orders.order_id, lineitem.l_description\
            \n  Inner Join:  Filter: orders.o_item_id = lineitem.l_item_id\
            \n    Inner Join:  Filter: person.id = orders.customer_id\
            \n      TableScan: person\
            \n      TableScan: orders\
            \n    TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn boolean_literal_in_condition_expression() {
    let sql = "SELECT order_id \
        FROM orders \
        WHERE delivered = false OR delivered = true";
    let expected = "Projection: orders.order_id\
            \n  Filter: orders.delivered = Boolean(false) OR orders.delivered = Boolean(true)\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn union() {
    let sql = "SELECT order_id from orders UNION SELECT order_id FROM orders";
    let expected = "\
        Distinct:\
        \n  Union\
        \n    Projection: orders.order_id\
        \n      TableScan: orders\
        \n    Projection: orders.order_id\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn union_all() {
    let sql = "SELECT order_id from orders UNION ALL SELECT order_id FROM orders";
    let expected = "Union\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.order_id\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn union_with_different_column_names() {
    let sql = "SELECT order_id from orders UNION ALL SELECT customer_id FROM orders";
    let expected = "Union\
            \n  Projection: orders.order_id\
            \n    TableScan: orders\
            \n  Projection: orders.customer_id AS order_id\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn union_values_with_no_alias() {
    let sql = "SELECT 1, 2 UNION ALL SELECT 3, 4";
    let expected = "Union\
            \n  Projection: Int64(1) AS Int64(1), Int64(2) AS Int64(2)\
            \n    EmptyRelation\
            \n  Projection: Int64(3) AS Int64(1), Int64(4) AS Int64(2)\
            \n    EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_incompatible_data_type() {
    let sql = "SELECT interval '1 year 1 day' UNION ALL SELECT 1";
    let err = logical_plan(sql)
        .expect_err("query should have failed")
        .strip_backtrace();
    assert_eq!(
       "Error during planning: UNION Column Int64(1) (type: Int64) is not compatible with column IntervalMonthDayNano(\"950737950189618795196236955648\") (type: Interval(MonthDayNano))",
       err
    );
}

#[test]
fn union_with_different_decimal_data_types() {
    let sql = "SELECT 1 a UNION ALL SELECT 1.1 a";
    let expected = "Union\
            \n  Projection: CAST(Int64(1) AS Float64) AS a\
            \n    EmptyRelation\
            \n  Projection: Float64(1.1) AS a\
            \n    EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_null() {
    let sql = "SELECT NULL a UNION ALL SELECT 1.1 a";
    let expected = "Union\
            \n  Projection: CAST(NULL AS Float64) AS a\
            \n    EmptyRelation\
            \n  Projection: Float64(1.1) AS a\
            \n    EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_float_and_string() {
    let sql = "SELECT 'a' a UNION ALL SELECT 1.1 a";
    let expected = "Union\
            \n  Projection: Utf8(\"a\") AS a\
            \n    EmptyRelation\
            \n  Projection: CAST(Float64(1.1) AS Utf8) AS a\
            \n    EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_multiply_cols() {
    let sql = "SELECT 'a' a, 1 b UNION ALL SELECT 1.1 a, 1.1 b";
    let expected = "Union\
            \n  Projection: Utf8(\"a\") AS a, CAST(Int64(1) AS Float64) AS b\
            \n    EmptyRelation\
            \n  Projection: CAST(Float64(1.1) AS Utf8) AS a, Float64(1.1) AS b\
            \n    EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn sorted_union_with_different_types_and_group_by() {
    let sql = "SELECT a FROM (select 1 a) x GROUP BY 1 UNION ALL (SELECT a FROM (select 1.1 a) x GROUP BY 1) ORDER BY 1";
    let expected = "Sort: x.a ASC NULLS LAST\
        \n  Union\
        \n    Projection: CAST(x.a AS Float64) AS a\
        \n      Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n        SubqueryAlias: x\
        \n          Projection: Int64(1) AS a\
        \n            EmptyRelation\
        \n    Projection: x.a\
        \n      Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n        SubqueryAlias: x\
        \n          Projection: Float64(1.1) AS a\
        \n            EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_binary_expr_and_cast() {
    let sql = "SELECT cast(0.0 + a as integer) FROM (select 1 a) x GROUP BY 1 UNION ALL (SELECT 2.1 + a FROM (select 1 a) x GROUP BY 1)";
    let expected = "Union\
        \n  Projection: CAST(Float64(0) + x.a AS Float64) AS Float64(0) + x.a\
        \n    Aggregate: groupBy=[[CAST(Float64(0) + x.a AS Int32)]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Int64(1) AS a\
        \n          EmptyRelation\
        \n  Projection: Float64(2.1) + x.a AS Float64(0) + x.a\
        \n    Aggregate: groupBy=[[Float64(2.1) + x.a]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Int64(1) AS a\
        \n          EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_aliases() {
    let sql = "SELECT a as a1 FROM (select 1 a) x GROUP BY 1 UNION ALL (SELECT a as a1 FROM (select 1.1 a) x GROUP BY 1)";
    let expected = "Union\
        \n  Projection: CAST(x.a AS Float64) AS a1\
        \n    Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Int64(1) AS a\
        \n          EmptyRelation\
        \n  Projection: x.a AS a1\
        \n    Aggregate: groupBy=[[x.a]], aggr=[[]]\
        \n      SubqueryAlias: x\
        \n        Projection: Float64(1.1) AS a\
        \n          EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn union_with_incompatible_data_types() {
    let sql = "SELECT 'a' a UNION ALL SELECT true a";
    let err = logical_plan(sql)
        .expect_err("query should have failed")
        .strip_backtrace();
    assert_eq!(
        "Error during planning: UNION Column a (type: Boolean) is not compatible with column a (type: Utf8)",
        err
    );
}

#[test]
fn empty_over() {
    let sql = "SELECT order_id, MAX(order_id) OVER () from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn empty_over_with_alias() {
    let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid from orders";
    let expected = "\
        Projection: orders.order_id AS oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn empty_over_dup_with_alias() {
    let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid, MAX(order_id) OVER () max_oid_dup from orders";
    let expected = "\
        Projection: orders.order_id AS oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid_dup\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn empty_over_dup_with_different_sort() {
    let sql = "SELECT order_id oid, MAX(order_id) OVER (), MAX(order_id) OVER (ORDER BY order_id) from orders";
    let expected = "\
        Projection: orders.order_id AS oid, MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MAX(orders.order_id) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.order_id) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn empty_over_plus() {
    let sql = "SELECT order_id, MAX(qty * 1.1) OVER () from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty * Float64(1.1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty * Float64(1.1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn empty_over_multiple() {
    let sql = "SELECT order_id, MAX(qty) OVER (), min(qty) over (), aVg(qty) OVER () from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, AVG(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, AVG(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                               QUERY PLAN
/// ----------------------------------------------------------------------
/// WindowAgg  (cost=69.83..87.33 rows=1000 width=8)
///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///         Sort Key: order_id
///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by() {
    let sql = "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// ----------------------------------------------------------------------------------
/// WindowAgg  (cost=137.16..154.66 rows=1000 width=12)
/// ->  Sort  (cost=137.16..139.66 rows=1000 width=12)
///         Sort Key: order_id
///         ->  WindowAgg  (cost=69.83..87.33 rows=1000 width=12)
///             ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                     Sort Key: order_id DESC
///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_order_by() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn over_order_by_with_window_frame_double_end() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS BETWEEN 3 PRECEDING and 3 FOLLOWING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn over_order_by_with_window_frame_single_end() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn over_order_by_with_window_frame_single_end_groups() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id GROUPS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// -----------------------------------------------------------------------------------
/// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
///   ->  Sort  (cost=142.16..144.66 rows=1000 width=16)
///         Sort Key: order_id
///         ->  WindowAgg  (cost=72.33..92.33 rows=1000 width=16)
///               ->  Sort  (cost=72.33..74.83 rows=1000 width=12)
///                     Sort Key: ((order_id + 1))
///                     ->  Seq Scan on orders  (cost=0.00..22.50 rows=1000 width=12)
/// ```
#[test]
fn over_order_by_two_sort_keys() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY (order_id + 1)) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) ORDER BY [orders.order_id + Int64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id + Int64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                        QUERY PLAN
/// ----------------------------------------------------------------------------------------
/// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
///               Sort Key: qty, order_id
///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                           Sort Key: order_id, qty
///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_order_by_sort_keys_sorting() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n        TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// ----------------------------------------------------------------------------------
/// WindowAgg  (cost=69.83..117.33 rows=1000 width=24)
///   ->  WindowAgg  (cost=69.83..104.83 rows=1000 width=16)
///         ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                     Sort Key: order_id, qty
///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_order_by_sort_keys_sorting_prefix_compacting() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n        TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                        QUERY PLAN
/// ----------------------------------------------------------------------------------------
/// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
///               Sort Key: order_id, qty
///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                           Sort Key: qty, order_id
///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
///
/// FIXME: for now we are not detecting prefix of sorting keys in order to re-arrange with global
/// sort
#[test]
fn over_order_by_sort_keys_sorting_global_order_compacting() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), SUM(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders ORDER BY order_id";
    let expected = "\
        Sort: orders.order_id ASC NULLS LAST\
        \n  Projection: orders.order_id, MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n    WindowAggr: windowExpr=[[SUM(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n      WindowAggr: windowExpr=[[MAX(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n        WindowAggr: windowExpr=[[MIN(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n          TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                               QUERY PLAN
/// ----------------------------------------------------------------------
/// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///         Sort Key: order_id, qty
///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by_order_by() {
    let sql =
        "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                               QUERY PLAN
/// ----------------------------------------------------------------------
/// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///         Sort Key: order_id, qty
///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by_order_by_no_dup() {
    let sql =
        "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// ----------------------------------------------------------------------------------
/// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
///   ->  Sort  (cost=142.16..144.66 rows=1000 width=12)
///         Sort Key: qty, order_id
///         ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=12)
///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                     Sort Key: order_id, qty
///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by_order_by_mix_up() {
    let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty), MIN(qty) OVER (PARTITION BY qty ORDER BY order_id) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) PARTITION BY [orders.qty] ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MIN(orders.qty) PARTITION BY [orders.qty] ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

/// psql result
/// ```text
///                                  QUERY PLAN
/// -----------------------------------------------------------------------------
/// WindowAgg  (cost=69.83..109.83 rows=1000 width=24)
///   ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=20)
///         ->  Sort  (cost=69.83..72.33 rows=1000 width=16)
///               Sort Key: order_id, qty, price
///               ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=16)
/// ```
/// FIXME: for now we are not detecting prefix of sorting keys in order to save one sort exec phase
#[test]
fn over_partition_by_order_by_mix_up_prefix() {
    let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty), MIN(qty) OVER (PARTITION BY order_id, qty ORDER BY price) from orders";
    let expected = "\
        Projection: orders.order_id, MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, MIN(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.price ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
        \n  WindowAggr: windowExpr=[[MAX(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    WindowAggr: windowExpr=[[MIN(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.price ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n      TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn approx_median_window() {
    let sql =
        "SELECT order_id, APPROX_MEDIAN(qty) OVER(PARTITION BY order_id) from orders";
    let expected = "\
        Projection: orders.order_id, APPROX_MEDIAN(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
        \n  WindowAggr: windowExpr=[[APPROX_MEDIAN(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn select_arrow_cast() {
    let sql = "SELECT arrow_cast(1234, 'Float64'), arrow_cast('foo', 'LargeUtf8')";
    let expected = "\
    Projection: CAST(Int64(1234) AS Float64), CAST(Utf8(\"foo\") AS LargeUtf8)\
    \n  EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn select_typed_date_string() {
    let sql = "SELECT date '2020-12-10' AS date";
    let expected = "Projection: CAST(Utf8(\"2020-12-10\") AS Date32) AS date\
            \n  EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn select_typed_time_string() {
    let sql = "SELECT TIME '08:09:10.123' AS time";
    let expected =
        "Projection: CAST(Utf8(\"08:09:10.123\") AS Time64(Nanosecond)) AS time\
            \n  EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn select_multibyte_column() {
    let sql = r#"SELECT "😀" FROM person"#;
    let expected = "Projection: person.😀\
            \n  TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn select_groupby_orderby() {
    // ensure that references are correctly resolved in the order by clause
    // see https://github.com/apache/arrow-datafusion/issues/4854
    let sql = r#"SELECT
  avg(age) AS "value",
  date_trunc('month', birth_date) AS "birth_date"
  FROM person GROUP BY birth_date ORDER BY birth_date;
"#;
    // expect that this is not an ambiguous reference
    let expected =
        "Sort: birth_date ASC NULLS LAST\
         \n  Projection: AVG(person.age) AS value, date_trunc(Utf8(\"month\"), person.birth_date) AS birth_date\
         \n    Aggregate: groupBy=[[person.birth_date]], aggr=[[AVG(person.age)]]\
         \n      TableScan: person";
    quick_test(sql, expected);

    // Use fully qualified `person.birth_date` as argument to date_trunc, plan should be the same
    let sql = r#"SELECT
  avg(age) AS "value",
  date_trunc('month', person.birth_date) AS "birth_date"
  FROM person GROUP BY birth_date ORDER BY birth_date;
"#;
    quick_test(sql, expected);

    // Use fully qualified `person.birth_date` as group by, plan should be the same
    let sql = r#"SELECT
  avg(age) AS "value",
  date_trunc('month', birth_date) AS "birth_date"
  FROM person GROUP BY person.birth_date ORDER BY birth_date;
"#;
    quick_test(sql, expected);

    // Use fully qualified `person.birth_date` in both group and date_trunc, plan should be the same
    let sql = r#"SELECT
  avg(age) AS "value",
  date_trunc('month', person.birth_date) AS "birth_date"
  FROM person GROUP BY person.birth_date ORDER BY birth_date;
"#;
    quick_test(sql, expected);
}

fn logical_plan(sql: &str) -> Result<LogicalPlan> {
    logical_plan_with_options(sql, ParserOptions::default())
}

fn logical_plan_with_options(sql: &str, options: ParserOptions) -> Result<LogicalPlan> {
    let dialect = &GenericDialect {};
    logical_plan_with_dialect_and_options(sql, dialect, options)
}

fn logical_plan_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<LogicalPlan> {
    let context = MockContextProvider::default();
    let planner = SqlToRel::new(&context);
    let result = DFParser::parse_sql_with_dialect(sql, dialect);
    let mut ast = result?;
    planner.statement_to_plan(ast.pop_front().unwrap())
}

fn logical_plan_with_dialect_and_options(
    sql: &str,
    dialect: &dyn Dialect,
    options: ParserOptions,
) -> Result<LogicalPlan> {
    let context = MockContextProvider::default();
    let planner = SqlToRel::new_with_options(&context, options);
    let result = DFParser::parse_sql_with_dialect(sql, dialect);
    let mut ast = result?;
    planner.statement_to_plan(ast.pop_front().unwrap())
}

/// Create logical plan, write with formatter, compare to expected output
fn quick_test(sql: &str, expected: &str) {
    let plan = logical_plan(sql).unwrap();
    assert_eq!(format!("{plan:?}"), expected);
}

fn quick_test_with_options(sql: &str, expected: &str, options: ParserOptions) {
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_eq!(format!("{plan:?}"), expected);
}

fn prepare_stmt_quick_test(
    sql: &str,
    expected_plan: &str,
    expected_data_types: &str,
) -> LogicalPlan {
    let plan = logical_plan(sql).unwrap();

    let assert_plan = plan.clone();
    // verify plan
    assert_eq!(format!("{assert_plan:?}"), expected_plan);

    // verify data types
    if let LogicalPlan::Prepare(Prepare { data_types, .. }) = assert_plan {
        let dt = format!("{data_types:?}");
        assert_eq!(dt, expected_data_types);
    }

    plan
}

fn prepare_stmt_replace_params_quick_test(
    plan: LogicalPlan,
    param_values: Vec<ScalarValue>,
    expected_plan: &str,
) -> LogicalPlan {
    // replace params
    let plan = plan.with_param_values(param_values).unwrap();
    assert_eq!(format!("{plan:?}"), expected_plan);

    plan
}

#[derive(Default)]
struct MockContextProvider {
    options: ConfigOptions,
    udafs: HashMap<String, Arc<AggregateUDF>>,
}

impl ContextProvider for MockContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let schema = match name.table() {
            "test" => Ok(Schema::new(vec![
                Field::new("t_date32", DataType::Date32, false),
                Field::new("t_date64", DataType::Date64, false),
            ])),
            "j1" => Ok(Schema::new(vec![
                Field::new("j1_id", DataType::Int32, false),
                Field::new("j1_string", DataType::Utf8, false),
            ])),
            "j2" => Ok(Schema::new(vec![
                Field::new("j2_id", DataType::Int32, false),
                Field::new("j2_string", DataType::Utf8, false),
            ])),
            "j3" => Ok(Schema::new(vec![
                Field::new("j3_id", DataType::Int32, false),
                Field::new("j3_string", DataType::Utf8, false),
            ])),
            "test_decimal" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("price", DataType::Decimal128(10, 2), false),
            ])),
            "person" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("salary", DataType::Float64, false),
                Field::new(
                    "birth_date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("😀", DataType::Int32, false),
            ])),
            "orders" => Ok(Schema::new(vec![
                Field::new("order_id", DataType::UInt32, false),
                Field::new("customer_id", DataType::UInt32, false),
                Field::new("o_item_id", DataType::Utf8, false),
                Field::new("qty", DataType::Int32, false),
                Field::new("price", DataType::Float64, false),
                Field::new("delivered", DataType::Boolean, false),
            ])),
            "array" => Ok(Schema::new(vec![
                Field::new(
                    "left",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    false,
                ),
                Field::new(
                    "right",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    false,
                ),
            ])),
            "lineitem" => Ok(Schema::new(vec![
                Field::new("l_item_id", DataType::UInt32, false),
                Field::new("l_description", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
            ])),
            "aggregate_test_100" => Ok(Schema::new(vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::UInt32, false),
                Field::new("c3", DataType::Int8, false),
                Field::new("c4", DataType::Int16, false),
                Field::new("c5", DataType::Int32, false),
                Field::new("c6", DataType::Int64, false),
                Field::new("c7", DataType::UInt8, false),
                Field::new("c8", DataType::UInt16, false),
                Field::new("c9", DataType::UInt32, false),
                Field::new("c10", DataType::UInt64, false),
                Field::new("c11", DataType::Float32, false),
                Field::new("c12", DataType::Float64, false),
                Field::new("c13", DataType::Utf8, false),
            ])),
            "UPPERCASE_test" => Ok(Schema::new(vec![
                Field::new("Id", DataType::UInt32, false),
                Field::new("lower", DataType::UInt32, false),
            ])),
            _ => plan_err!("No table named: {} found", name.table()),
        };

        match schema {
            Ok(t) => Ok(Arc::new(EmptyTable::new(Arc::new(t)))),
            Err(e) => Err(e),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.udafs.get(name).map(Arc::clone)
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

#[test]
fn select_partially_qualified_column() {
    let sql = r#"SELECT person.first_name FROM public.person"#;
    let expected = "Projection: public.person.first_name\
            \n  TableScan: public.person";
    quick_test(sql, expected);
}

#[test]
fn cross_join_not_to_inner_join() {
    let sql =
        "select person.id from person, orders, lineitem where person.id = person.age;";
    let expected = "Projection: person.id\
                                    \n  Filter: person.id = person.age\
                                    \n    CrossJoin:\
                                    \n      CrossJoin:\
                                    \n        TableScan: person\
                                    \n        TableScan: orders\
                                    \n      TableScan: lineitem";
    quick_test(sql, expected);
}

#[test]
fn join_with_aliases() {
    let sql = "select peeps.id, folks.first_name from person as peeps join person as folks on peeps.id = folks.id";
    let expected = "Projection: peeps.id, folks.first_name\
            \n  Inner Join:  Filter: peeps.id = folks.id\
            \n    SubqueryAlias: peeps\
            \n      TableScan: person\
            \n    SubqueryAlias: folks\
            \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn cte_use_same_name_multiple_times() {
    let sql =
        "with a as (select * from person), a as (select * from orders) select * from a;";
    let expected =
        "SQL error: ParserError(\"WITH query name \\\"a\\\" specified more than once\")";
    let result = logical_plan(sql).err().unwrap();
    assert_eq!(result.strip_backtrace(), expected);
}

#[test]
fn negative_interval_plus_interval_in_projection() {
    let sql = "select -interval '2 days' + interval '5 days';";
    let expected =
    "Projection: IntervalMonthDayNano(\"79228162477370849446124847104\") + IntervalMonthDayNano(\"92233720368547758080\")\n  EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn complex_interval_expression_in_projection() {
    let sql = "select -interval '2 days' + interval '5 days'+ (-interval '3 days' + interval '5 days');";
    let expected =
    "Projection: IntervalMonthDayNano(\"79228162477370849446124847104\") + IntervalMonthDayNano(\"92233720368547758080\") + IntervalMonthDayNano(\"79228162458924105372415295488\") + IntervalMonthDayNano(\"92233720368547758080\")\n  EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn negative_sum_intervals_in_projection() {
    let sql = "select -((interval '2 days' + interval '5 days') + -(interval '4 days' + interval '7 days'));";
    let expected =
    "Projection: (- IntervalMonthDayNano(\"36893488147419103232\") + IntervalMonthDayNano(\"92233720368547758080\") + (- IntervalMonthDayNano(\"73786976294838206464\") + IntervalMonthDayNano(\"129127208515966861312\")))\n  EmptyRelation";
    quick_test(sql, expected);
}

#[test]
fn date_plus_interval_in_projection() {
    let sql = "select t_date32 + interval '5 days' FROM test";
    let expected =
        "Projection: test.t_date32 + IntervalMonthDayNano(\"92233720368547758080\")\
                            \n  TableScan: test";
    quick_test(sql, expected);
}

#[test]
fn date_plus_interval_in_filter() {
    let sql = "select t_date64 FROM test \
                    WHERE t_date64 \
                    BETWEEN cast('1999-12-31' as date) \
                        AND cast('1999-12-31' as date) + interval '30 days'";
    let expected =
            "Projection: test.t_date64\
            \n  Filter: test.t_date64 BETWEEN CAST(Utf8(\"1999-12-31\") AS Date32) AND CAST(Utf8(\"1999-12-31\") AS Date32) + IntervalMonthDayNano(\"553402322211286548480\")\
            \n    TableScan: test";
    quick_test(sql, expected);
}

#[test]
fn exists_subquery() {
    let sql = "SELECT id FROM person p WHERE EXISTS \
            (SELECT first_name FROM person \
            WHERE last_name = p.last_name \
            AND state = p.state)";

    let expected = "Projection: p.id\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.first_name\
        \n        Filter: person.last_name = outer_ref(p.last_name) AND person.state = outer_ref(p.state)\
        \n          TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn exists_subquery_schema_outer_schema_overlap() {
    // both the outer query and the schema select from unaliased "person"
    let sql = "SELECT person.id FROM person, person p \
            WHERE person.id = p.id AND EXISTS \
            (SELECT person.first_name FROM person, person p2 \
            WHERE person.id = p2.id \
            AND person.last_name = p.last_name \
            AND person.state = p.state)";

    let expected = "Projection: person.id\
        \n  Filter: person.id = p.id AND EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.first_name\
        \n        Filter: person.id = p2.id AND person.last_name = outer_ref(p.last_name) AND person.state = outer_ref(p.state)\
        \n          CrossJoin:\
        \n            TableScan: person\
        \n            SubqueryAlias: p2\
        \n              TableScan: person\
        \n    CrossJoin:\
        \n      TableScan: person\
        \n      SubqueryAlias: p\
        \n        TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn exists_subquery_wildcard() {
    let sql = "SELECT id FROM person p WHERE EXISTS \
            (SELECT * FROM person \
            WHERE last_name = p.last_name \
            AND state = p.state)";

    let expected = "Projection: p.id\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
        \n        Filter: person.last_name = outer_ref(p.last_name) AND person.state = outer_ref(p.state)\
        \n          TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn in_subquery_uncorrelated() {
    let sql = "SELECT id FROM person p WHERE id IN \
            (SELECT id FROM person)";

    let expected = "Projection: p.id\
        \n  Filter: p.id IN (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.id\
        \n        TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn not_in_subquery_correlated() {
    let sql = "SELECT id FROM person p WHERE id NOT IN \
            (SELECT id FROM person WHERE last_name = p.last_name AND state = 'CO')";

    let expected = "Projection: p.id\
        \n  Filter: p.id NOT IN (<subquery>)\
        \n    Subquery:\
        \n      Projection: person.id\
        \n        Filter: person.last_name = outer_ref(p.last_name) AND person.state = Utf8(\"CO\")\
        \n          TableScan: person\
        \n    SubqueryAlias: p\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn scalar_subquery() {
    let sql =
        "SELECT p.id, (SELECT MAX(id) FROM person WHERE last_name = p.last_name) FROM person p";

    let expected = "Projection: p.id, (<subquery>)\
        \n  Subquery:\
        \n    Projection: MAX(person.id)\
        \n      Aggregate: groupBy=[[]], aggr=[[MAX(person.id)]]\
        \n        Filter: person.last_name = outer_ref(p.last_name)\
        \n          TableScan: person\
        \n  SubqueryAlias: p\
        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn scalar_subquery_reference_outer_field() {
    let sql = "SELECT j1_string, j2_string \
        FROM j1, j2 \
        WHERE j1_id = j2_id - 1 \
        AND j2_id < (SELECT count(*) \
            FROM j1, j3 \
            WHERE j2_id = j1_id \
            AND j1_id = j3_id)";

    let expected = "Projection: j1.j1_string, j2.j2_string\
        \n  Filter: j1.j1_id = j2.j2_id - Int64(1) AND j2.j2_id < (<subquery>)\
        \n    Subquery:\
        \n      Projection: COUNT(*)\
        \n        Aggregate: groupBy=[[]], aggr=[[COUNT(*)]]\
        \n          Filter: outer_ref(j2.j2_id) = j1.j1_id AND j1.j1_id = j3.j3_id\
        \n            CrossJoin:\
        \n              TableScan: j1\
        \n              TableScan: j3\
        \n    CrossJoin:\
        \n      TableScan: j1\
        \n      TableScan: j2";

    quick_test(sql, expected);
}

#[test]
fn subquery_references_cte() {
    let sql = "WITH \
        cte AS (SELECT * FROM person) \
        SELECT * FROM person WHERE EXISTS (SELECT * FROM cte WHERE id = person.id)";

    let expected = "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Projection: cte.id, cte.first_name, cte.last_name, cte.age, cte.state, cte.salary, cte.birth_date, cte.😀\
        \n        Filter: cte.id = outer_ref(person.id)\
        \n          SubqueryAlias: cte\
        \n            Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀\
        \n              TableScan: person\
        \n    TableScan: person";

    quick_test(sql, expected)
}

#[test]
fn cte_with_no_column_names() {
    let sql = "WITH \
        numbers AS ( \
            SELECT 1 as a, 2 as b, 3 as c \
        ) \
        SELECT * FROM numbers;";

    let expected = "Projection: numbers.a, numbers.b, numbers.c\
        \n  SubqueryAlias: numbers\
        \n    Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS c\
        \n      EmptyRelation";

    quick_test(sql, expected)
}

#[test]
fn cte_with_column_names() {
    let sql = "WITH \
        numbers(a, b, c) AS ( \
            SELECT 1, 2, 3 \
        ) \
        SELECT * FROM numbers;";

    let expected = "Projection: numbers.a, numbers.b, numbers.c\
        \n  SubqueryAlias: numbers\
        \n    Projection: Int64(1) AS a, Int64(2) AS b, Int64(3) AS c\
        \n      Projection: Int64(1), Int64(2), Int64(3)\
        \n        EmptyRelation";

    quick_test(sql, expected)
}

#[test]
fn cte_with_column_aliases_precedence() {
    // The end result should always be what CTE specification says
    let sql = "WITH \
        numbers(a, b, c) AS ( \
            SELECT 1 as x, 2 as y, 3 as z \
        ) \
        SELECT * FROM numbers;";

    let expected = "Projection: numbers.a, numbers.b, numbers.c\
        \n  SubqueryAlias: numbers\
        \n    Projection: x AS a, y AS b, z AS c\
        \n      Projection: Int64(1) AS x, Int64(2) AS y, Int64(3) AS z\
        \n        EmptyRelation";
    quick_test(sql, expected)
}

#[test]
fn cte_unbalanced_number_of_columns() {
    let sql = "WITH \
        numbers(a) AS ( \
            SELECT 1, 2, 3 \
        ) \
        SELECT * FROM numbers;";

    let expected = "Error during planning: Source table contains 3 columns but only 1 names given as column alias";
    let result = logical_plan(sql).err().unwrap();
    assert_eq!(result.strip_backtrace(), expected);
}

#[test]
fn aggregate_with_rollup() {
    let sql =
        "SELECT id, state, age, COUNT(*) FROM person GROUP BY id, ROLLUP (state, age)";
    let expected = "Projection: person.id, person.state, person.age, COUNT(*)\
    \n  Aggregate: groupBy=[[GROUPING SETS ((person.id), (person.id, person.state), (person.id, person.state, person.age))]], aggr=[[COUNT(*)]]\
    \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn aggregate_with_rollup_with_grouping() {
    let sql = "SELECT id, state, age, grouping(state), grouping(age), grouping(state) + grouping(age), COUNT(*) \
        FROM person GROUP BY id, ROLLUP (state, age)";
    let expected = "Projection: person.id, person.state, person.age, GROUPING(person.state), GROUPING(person.age), GROUPING(person.state) + GROUPING(person.age), COUNT(*)\
    \n  Aggregate: groupBy=[[GROUPING SETS ((person.id), (person.id, person.state), (person.id, person.state, person.age))]], aggr=[[GROUPING(person.state), GROUPING(person.age), COUNT(*)]]\
    \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn rank_partition_grouping() {
    let sql = "select
            sum(age) as total_sum,
            state,
            last_name,
            grouping(state) + grouping(last_name) as x,
            rank() over (
                partition by grouping(state) + grouping(last_name),
                case when grouping(last_name) = 0 then state end
                order by sum(age) desc
                ) as the_rank
            from
                person
            group by rollup(state, last_name)";
    let expected = "Projection: SUM(person.age) AS total_sum, person.state, person.last_name, GROUPING(person.state) + GROUPING(person.last_name) AS x, RANK() PARTITION BY [GROUPING(person.state) + GROUPING(person.last_name), CASE WHEN GROUPING(person.last_name) = Int64(0) THEN person.state END] ORDER BY [SUM(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS the_rank\
        \n  WindowAggr: windowExpr=[[RANK() PARTITION BY [GROUPING(person.state) + GROUPING(person.last_name), CASE WHEN GROUPING(person.last_name) = Int64(0) THEN person.state END] ORDER BY [SUM(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]\
        \n    Aggregate: groupBy=[[ROLLUP (person.state, person.last_name)]], aggr=[[SUM(person.age), GROUPING(person.state), GROUPING(person.last_name)]]\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn aggregate_with_cube() {
    let sql =
        "SELECT id, state, age, COUNT(*) FROM person GROUP BY id, CUBE (state, age)";
    let expected = "Projection: person.id, person.state, person.age, COUNT(*)\
    \n  Aggregate: groupBy=[[GROUPING SETS ((person.id), (person.id, person.state), (person.id, person.age), (person.id, person.state, person.age))]], aggr=[[COUNT(*)]]\
    \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn round_decimal() {
    let sql = "SELECT round(price/3, 2) FROM test_decimal";
    let expected = "Projection: round(test_decimal.price / Int64(3), Int64(2))\
        \n  TableScan: test_decimal";
    quick_test(sql, expected);
}

#[test]
fn aggregate_with_grouping_sets() {
    let sql = "SELECT id, state, age, COUNT(*) FROM person GROUP BY id, GROUPING SETS ((state), (state, age), (id, state))";
    let expected = "Projection: person.id, person.state, person.age, COUNT(*)\
    \n  Aggregate: groupBy=[[GROUPING SETS ((person.id, person.state), (person.id, person.state, person.age), (person.id, person.id, person.state))]], aggr=[[COUNT(*)]]\
    \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn join_on_disjunction_condition() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders ON id = customer_id OR person.age > 30";
    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id = orders.customer_id OR person.age > Int64(30)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn join_on_complex_condition() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders ON id = customer_id AND (person.age > 30 OR person.last_name = 'X')";
    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id = orders.customer_id AND (person.age > Int64(30) OR person.last_name = Utf8(\"X\"))\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn hive_aggregate_with_filter() -> Result<()> {
    let dialect = &HiveDialect {};
    let sql = "SELECT SUM(age) FILTER (WHERE age > 4) FROM person";
    let plan = logical_plan_with_dialect(sql, dialect)?;
    let expected = "Projection: SUM(person.age) FILTER (WHERE person.age > Int64(4))\
        \n  Aggregate: groupBy=[[]], aggr=[[SUM(person.age) FILTER (WHERE person.age > Int64(4))]]\
        \n    TableScan: person"
        .to_string();
    assert_eq!(plan.display_indent().to_string(), expected);
    Ok(())
}

#[test]
fn order_by_unaliased_name() {
    // https://github.com/apache/arrow-datafusion/issues/3160
    // This query was failing with:
    // SchemaError(FieldNotFound { qualifier: Some("p"), name: "state", valid_fields: ["z", "q"] })
    let sql =
        "select p.state z, sum(age) q from person p group by p.state order by p.state";
    let expected = "Projection: z, q\
        \n  Sort: p.state ASC NULLS LAST\
        \n    Projection: p.state AS z, SUM(p.age) AS q, p.state\
        \n      Aggregate: groupBy=[[p.state]], aggr=[[SUM(p.age)]]\
        \n        SubqueryAlias: p\
        \n          TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn order_by_ambiguous_name() {
    let sql = "select * from person a join person b using (id) order by age";
    let expected = "Schema error: Ambiguous reference to unqualified field age";

    let err = logical_plan(sql).unwrap_err();
    assert_eq!(err.strip_backtrace(), expected);
}

#[test]
fn group_by_ambiguous_name() {
    let sql = "select max(id) from person a join person b using (id) group by age";
    let expected = "Schema error: Ambiguous reference to unqualified field age";

    let err = logical_plan(sql).unwrap_err();
    assert_eq!(err.strip_backtrace(), expected);
}

#[test]
fn test_zero_offset_with_limit() {
    let sql = "select id from person where person.id > 100 LIMIT 5 OFFSET 0;";
    let expected = "Limit: skip=0, fetch=5\
                                    \n  Projection: person.id\
                                    \n    Filter: person.id > Int64(100)\
                                    \n      TableScan: person";
    quick_test(sql, expected);

    // Flip the order of LIMIT and OFFSET in the query. Plan should remain the same.
    let sql = "SELECT id FROM person WHERE person.id > 100 OFFSET 0 LIMIT 5;";
    quick_test(sql, expected);
}

#[test]
fn test_offset_no_limit() {
    let sql = "SELECT id FROM person WHERE person.id > 100 OFFSET 5;";
    let expected = "Limit: skip=5, fetch=None\
        \n  Projection: person.id\
        \n    Filter: person.id > Int64(100)\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_offset_after_limit() {
    let sql = "select id from person where person.id > 100 LIMIT 5 OFFSET 3;";
    let expected = "Limit: skip=3, fetch=5\
        \n  Projection: person.id\
        \n    Filter: person.id > Int64(100)\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_offset_before_limit() {
    let sql = "select id from person where person.id > 100 OFFSET 3 LIMIT 5;";
    let expected = "Limit: skip=3, fetch=5\
        \n  Projection: person.id\
        \n    Filter: person.id > Int64(100)\
        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_distribute_by() {
    let sql = "select id from person distribute by state";
    let expected = "Repartition: DistributeBy(state)\
        \n  Projection: person.id\
        \n    TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_double_quoted_literal_string() {
    // Assert double quoted literal string is parsed correctly like single quoted one in specific
    // dialect.
    let dialect = &MySqlDialect {};
    let single_quoted_res = format!(
        "{:?}",
        logical_plan_with_dialect("SELECT '1'", dialect).unwrap()
    );
    let double_quoted_res = format!(
        "{:?}",
        logical_plan_with_dialect("SELECT \"1\"", dialect).unwrap()
    );
    assert_eq!(single_quoted_res, double_quoted_res);

    // It should return error in other dialect.
    assert!(logical_plan("SELECT \"1\"").is_err());
}

#[test]
fn test_constant_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id = 10";

    let expected = "Projection: person.id, orders.order_id\
        \n  Inner Join:  Filter: person.id = Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_right_left_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON orders.customer_id * 2 = person.id + 10";

    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: orders.customer_id * Int64(2) = person.id + Int64(10)\
            \n    TableScan: person\
            \n    TableScan: orders";

    quick_test(sql, expected);
}

#[test]
fn test_single_column_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + 10 = orders.customer_id * 2";

    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id + Int64(10) = orders.customer_id * Int64(2)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_multiple_column_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + person.age + 10 = orders.customer_id * 2 - orders.price";

    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id + person.age + Int64(10) = orders.customer_id * Int64(2) - orders.price\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_left_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + person.age + 10 = orders.customer_id";

    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id + person.age + Int64(10) = orders.customer_id\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_right_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id = orders.customer_id * 2 - orders.price";

    let expected = "Projection: person.id, orders.order_id\
            \n  Inner Join:  Filter: person.id = orders.customer_id * Int64(2) - orders.price\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_noneq_with_filter_join() {
    // inner join
    let sql = "SELECT person.id, person.first_name \
        FROM person INNER JOIN orders \
        ON person.age > 10";
    let expected = "Projection: person.id, person.first_name\
        \n  Inner Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders";
    quick_test(sql, expected);
    // left join
    let sql = "SELECT person.id, person.first_name \
        FROM person LEFT JOIN orders \
        ON person.age > 10";
    let expected = "Projection: person.id, person.first_name\
        \n  Left Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders";
    quick_test(sql, expected);
    // right join
    let sql = "SELECT person.id, person.first_name \
        FROM person RIGHT JOIN orders \
        ON person.age > 10";
    let expected = "Projection: person.id, person.first_name\
        \n  Right Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders";
    quick_test(sql, expected);
    // full join
    let sql = "SELECT person.id, person.first_name \
        FROM person FULL JOIN orders \
        ON person.age > 10";
    let expected = "Projection: person.id, person.first_name\
        \n  Full Join:  Filter: person.age > Int64(10)\
        \n    TableScan: person\
        \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_one_side_constant_full_join() {
    // TODO: this sql should be parsed as join after
    // https://github.com/apache/arrow-datafusion/issues/2877 is resolved.
    let sql = "SELECT id, order_id \
            FROM person \
            FULL OUTER JOIN orders \
            ON person.id = 10";

    let expected = "Projection: person.id, orders.order_id\
            \n  Full Join:  Filter: person.id = Int64(10)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_select_all_inner_join() {
    let sql = "SELECT *
            FROM person \
            INNER JOIN orders \
            ON orders.customer_id * 2 = person.id + 10";

    let expected = "Projection: person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person.😀, orders.order_id, orders.customer_id, orders.o_item_id, orders.qty, orders.price, orders.delivered\
            \n  Inner Join:  Filter: orders.customer_id * Int64(2) = person.id + Int64(10)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_select_join_key_inner_join() {
    let sql = "SELECT  orders.customer_id * 2,  person.id + 10
            FROM person
            INNER JOIN orders
            ON orders.customer_id * 2 = person.id + 10";

    let expected = "Projection: orders.customer_id * Int64(2), person.id + Int64(10)\
            \n  Inner Join:  Filter: orders.customer_id * Int64(2) = person.id + Int64(10)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_select_order_by() {
    let sql = "SELECT '1' from person order by id";

    let expected = "Projection: Utf8(\"1\")\n  Sort: person.id ASC NULLS LAST\n    Projection: Utf8(\"1\"), person.id\n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_select_distinct_order_by() {
    let sql = "SELECT distinct '1' from person order by id";

    let expected =
        "Error during planning: For SELECT DISTINCT, ORDER BY expressions id must appear in select list";

    // It should return error.
    let result = logical_plan(sql);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.strip_backtrace(), expected);
}

#[rstest]
#[case::select_cluster_by_unsupported(
    "SELECT customer_name, SUM(order_total) as total_order_amount FROM orders CLUSTER BY customer_name",
    "This feature is not implemented: CLUSTER BY"
)]
#[case::select_lateral_view_unsupported(
    "SELECT id, number FROM person LATERAL VIEW explode(numbers) exploded_table AS number",
    "This feature is not implemented: LATERAL VIEWS"
)]
#[case::select_qualify_unsupported(
    "SELECT i, p, o FROM person QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1",
    "This feature is not implemented: QUALIFY"
)]
#[case::select_top_unsupported(
    "SELECT TOP (5) * FROM person",
    "This feature is not implemented: TOP"
)]
#[case::select_sort_by_unsupported(
    "SELECT * FROM person SORT BY id",
    "This feature is not implemented: SORT BY"
)]
#[test]
fn test_select_unsupported_syntax_errors(#[case] sql: &str, #[case] error: &str) {
    let err = logical_plan(sql).unwrap_err();
    assert_eq!(err.strip_backtrace(), error)
}

#[test]
fn select_order_by_with_cast() {
    let sql =
        "SELECT first_name AS first_name FROM (SELECT first_name AS first_name FROM person) ORDER BY CAST(first_name as INT)";
    let expected = "Sort: CAST(first_name AS first_name AS Int32) ASC NULLS LAST\
                        \n  Projection: first_name AS first_name\
                        \n    Projection: person.first_name AS first_name\
                        \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_duplicated_left_join_key_inner_join() {
    //  person.id * 2 happen twice in left side.
    let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON person.id * 2 = orders.customer_id + 10 and person.id * 2 = orders.order_id";

    let expected = "Projection: person.id, person.age\
            \n  Inner Join:  Filter: person.id * Int64(2) = orders.customer_id + Int64(10) AND person.id * Int64(2) = orders.order_id\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_duplicated_right_join_key_inner_join() {
    //  orders.customer_id + 10 happen twice in right side.
    let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON person.id * 2 = orders.customer_id + 10 and person.id =  orders.customer_id + 10";

    let expected = "Projection: person.id, person.age\
            \n  Inner Join:  Filter: person.id * Int64(2) = orders.customer_id + Int64(10) AND person.id = orders.customer_id + Int64(10)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_ambiguous_column_references_in_on_join() {
    let sql = "select p1.id, p1.age, p2.id
            from person as p1
            INNER JOIN person as p2
            ON id = 1";

    let expected = "Schema error: Ambiguous reference to unqualified field id";

    // It should return error.
    let result = logical_plan(sql);
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert_eq!(err.strip_backtrace(), expected);
}

#[test]
fn test_ambiguous_column_references_with_in_using_join() {
    let sql = "select p1.id, p1.age, p2.id
            from person as p1
            INNER JOIN person as p2
            using(id)";

    let expected = "Projection: p1.id, p1.age, p2.id\
            \n  Inner Join: Using p1.id = p2.id\
            \n    SubqueryAlias: p1\
            \n      TableScan: person\
            \n    SubqueryAlias: p2\
            \n      TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_prepare_statement_to_plan_panic_param_format() {
    // param is not number following the $ sign
    // panic due to error returned from the parser
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $foo";
    assert_eq!(
        logical_plan(sql).unwrap_err().strip_backtrace(),
        "Error during planning: Invalid placeholder, not a number: $foo"
    );
}

#[test]
fn test_prepare_statement_to_plan_panic_param_zero() {
    // param is zero following the $ sign
    // panic due to error returned from the parser
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $0";
    assert_eq!(
        logical_plan(sql).unwrap_err().strip_backtrace(),
        "Error during planning: Invalid placeholder, zero is not a valid index: $0"
    );
}

#[test]
fn test_prepare_statement_to_plan_panic_prepare_wrong_syntax() {
    // param is not number following the $ sign
    // panic due to error returned from the parser
    let sql = "PREPARE AS SELECT id, age  FROM person WHERE age = $foo";
    assert_eq!(
        logical_plan(sql).unwrap_err().strip_backtrace(),
        "SQL error: ParserError(\"Expected AS, found: SELECT\")"
    )
}

#[test]
fn test_prepare_statement_to_plan_panic_no_relation_and_constant_param() {
    let sql = "PREPARE my_plan(INT) AS SELECT id + $1";
    assert_eq!(
        logical_plan(sql).unwrap_err().strip_backtrace(),
        "Schema error: No field named id."
    )
}

#[test]
fn test_prepare_statement_should_infer_types() {
    // only provide 1 data type while using 2 params
    let sql = "PREPARE my_plan(INT) AS SELECT 1 + $1 + $2";
    let plan = logical_plan(sql).unwrap();
    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        ("$1".to_string(), Some(DataType::Int32)),
        ("$2".to_string(), Some(DataType::Int64)),
    ]);
    assert_eq!(actual_types, expected_types);
}

#[test]
fn test_non_prepare_statement_should_infer_types() {
    // Non prepared statements (like SELECT) should also have their parameter types inferred
    let sql = "SELECT 1 + $1";
    let plan = logical_plan(sql).unwrap();
    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        // constant 1 is inferred to be int64
        ("$1".to_string(), Some(DataType::Int64)),
    ]);
    assert_eq!(actual_types, expected_types);
}

#[test]
#[should_panic(
    expected = "value: SQL(ParserError(\"Expected [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found: $1\""
)]
fn test_prepare_statement_to_plan_panic_is_param() {
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age is $1";
    logical_plan(sql).unwrap();
}

#[test]
fn test_prepare_statement_to_plan_no_param() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";

    let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = Int64(10)\
        \n      TableScan: person";

    let expected_dt = "[Int32]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let expected_plan = "Projection: person.id, person.age\
        \n  Filter: person.age = Int64(10)\
        \n    TableScan: person";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);

    //////////////////////////////////////////
    // no embedded parameter and no declare it
    let sql = "PREPARE my_plan AS SELECT id, age  FROM person WHERE age = 10";

    let expected_plan = "Prepare: \"my_plan\" [] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = Int64(10)\
        \n      TableScan: person";

    let expected_dt = "[]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![];
    let expected_plan = "Projection: person.id, person.age\
        \n  Filter: person.age = Int64(10)\
        \n    TableScan: person";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_to_plan_one_param_no_value_panic() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";
    let plan = logical_plan(sql).unwrap();
    // declare 1 param but provide 0
    let param_values = vec![];
    assert_eq!(
        plan.with_param_values(param_values)
            .unwrap_err()
            .strip_backtrace(),
        "Error during planning: Expected 1 parameters, got 0"
    );
}

#[test]
fn test_prepare_statement_to_plan_one_param_one_value_different_type_panic() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = 10";
    let plan = logical_plan(sql).unwrap();
    // declare 1 param but provide 0
    let param_values = vec![ScalarValue::Float64(Some(20.0))];
    assert_eq!(
        plan.with_param_values(param_values)
            .unwrap_err()
            .strip_backtrace(),
        "Error during planning: Expected parameter of type Int32, got Float64 at index 0"
    );
}

#[test]
fn test_prepare_statement_to_plan_no_param_on_value_panic() {
    // no embedded parameter but still declare it
    let sql = "PREPARE my_plan AS SELECT id, age  FROM person WHERE age = 10";
    let plan = logical_plan(sql).unwrap();
    // declare 1 param but provide 0
    let param_values = vec![ScalarValue::Int32(Some(10))];
    assert_eq!(
        plan.with_param_values(param_values)
            .unwrap_err()
            .strip_backtrace(),
        "Error during planning: Expected 0 parameters, got 1"
    );
}

#[test]
fn test_prepare_statement_to_plan_params_as_constants() {
    let sql = "PREPARE my_plan(INT) AS SELECT $1";

    let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: $1\n    EmptyRelation";
    let expected_dt = "[Int32]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let expected_plan = "Projection: Int32(10)\n  EmptyRelation";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);

    ///////////////////////////////////////
    let sql = "PREPARE my_plan(INT) AS SELECT 1 + $1";

    let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: Int64(1) + $1\n    EmptyRelation";
    let expected_dt = "[Int32]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let expected_plan = "Projection: Int64(1) + Int32(10)\n  EmptyRelation";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);

    ///////////////////////////////////////
    let sql = "PREPARE my_plan(INT, DOUBLE) AS SELECT 1 + $1 + $2";

    let expected_plan = "Prepare: \"my_plan\" [Int32, Float64] \
        \n  Projection: Int64(1) + $1 + $2\n    EmptyRelation";
    let expected_dt = "[Int32, Float64]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Int32(Some(10)),
        ScalarValue::Float64(Some(10.0)),
    ];
    let expected_plan = "Projection: Int64(1) + Int32(10) + Float64(10)\n  EmptyRelation";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_infer_types_from_join() {
    let sql =
        "SELECT id, order_id FROM person JOIN orders ON id = customer_id and age = $1";

    let expected_plan = r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id AND person.age = $1
    TableScan: person
    TableScan: orders
    "#
    .trim();

    let expected_dt = "[Int32]";
    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([("$1".to_string(), Some(DataType::Int32))]);
    assert_eq!(actual_types, expected_types);

    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let expected_plan = r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id AND person.age = Int32(10)
    TableScan: person
    TableScan: orders
    "#
    .trim();
    let plan = plan.replace_params_with_values(&param_values).unwrap();

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_infer_types_from_predicate() {
    let sql = "SELECT id, age FROM person WHERE age = $1";

    let expected_plan = r#"
Projection: person.id, person.age
  Filter: person.age = $1
    TableScan: person
        "#
    .trim();

    let expected_dt = "[Int32]";
    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([("$1".to_string(), Some(DataType::Int32))]);
    assert_eq!(actual_types, expected_types);

    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let expected_plan = r#"
Projection: person.id, person.age
  Filter: person.age = Int32(10)
    TableScan: person
        "#
    .trim();
    let plan = plan.replace_params_with_values(&param_values).unwrap();

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_infer_types_from_between_predicate() {
    let sql = "SELECT id, age FROM person WHERE age BETWEEN $1 AND $2";

    let expected_plan = r#"
Projection: person.id, person.age
  Filter: person.age BETWEEN $1 AND $2
    TableScan: person
        "#
    .trim();

    let expected_dt = "[Int32]";
    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        ("$1".to_string(), Some(DataType::Int32)),
        ("$2".to_string(), Some(DataType::Int32)),
    ]);
    assert_eq!(actual_types, expected_types);

    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10)), ScalarValue::Int32(Some(30))];
    let expected_plan = r#"
Projection: person.id, person.age
  Filter: person.age BETWEEN Int32(10) AND Int32(30)
    TableScan: person
        "#
    .trim();
    let plan = plan.replace_params_with_values(&param_values).unwrap();

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_infer_types_subquery() {
    let sql = "SELECT id, age FROM person WHERE age = (select max(age) from person where id = $1)";

    let expected_plan = r#"
Projection: person.id, person.age
  Filter: person.age = (<subquery>)
    Subquery:
      Projection: MAX(person.age)
        Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]
          Filter: person.id = $1
            TableScan: person
    TableScan: person
        "#
    .trim();

    let expected_dt = "[Int32]";
    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([("$1".to_string(), Some(DataType::UInt32))]);
    assert_eq!(actual_types, expected_types);

    // replace params with values
    let param_values = vec![ScalarValue::UInt32(Some(10))];
    let expected_plan = r#"
Projection: person.id, person.age
  Filter: person.age = (<subquery>)
    Subquery:
      Projection: MAX(person.age)
        Aggregate: groupBy=[[]], aggr=[[MAX(person.age)]]
          Filter: person.id = UInt32(10)
            TableScan: person
    TableScan: person
        "#
    .trim();
    let plan = plan.replace_params_with_values(&param_values).unwrap();

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_update_infer() {
    let sql = "update person set age=$1 where id=$2";

    let expected_plan = r#"
Dml: op=[Update] table=[person]
  Projection: person.id AS id, person.first_name AS first_name, person.last_name AS last_name, $1 AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
    Filter: person.id = $2
      TableScan: person
        "#
        .trim();

    let expected_dt = "[Int32]";
    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        ("$1".to_string(), Some(DataType::Int32)),
        ("$2".to_string(), Some(DataType::UInt32)),
    ]);
    assert_eq!(actual_types, expected_types);

    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(42)), ScalarValue::UInt32(Some(1))];
    let expected_plan = r#"
Dml: op=[Update] table=[person]
  Projection: person.id AS id, person.first_name AS first_name, person.last_name AS last_name, Int32(42) AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
    Filter: person.id = UInt32(1)
      TableScan: person
        "#
        .trim();
    let plan = plan.replace_params_with_values(&param_values).unwrap();

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_insert_infer() {
    let sql = "insert into person (id, first_name, last_name) values ($1, $2, $3)";

    let expected_plan = "Dml: op=[Insert Into] table=[person]\
                        \n  Projection: column1 AS id, column2 AS first_name, column3 AS last_name, \
                                    CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, \
                                    CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀\
                        \n    Values: ($1, $2, $3)";

    let expected_dt = "[Int32]";
    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    let actual_types = plan.get_parameter_types().unwrap();
    let expected_types = HashMap::from([
        ("$1".to_string(), Some(DataType::UInt32)),
        ("$2".to_string(), Some(DataType::Utf8)),
        ("$3".to_string(), Some(DataType::Utf8)),
    ]);
    assert_eq!(actual_types, expected_types);

    // replace params with values
    let param_values = vec![
        ScalarValue::UInt32(Some(1)),
        ScalarValue::Utf8(Some("Alan".to_string())),
        ScalarValue::Utf8(Some("Turing".to_string())),
    ];
    let expected_plan = "Dml: op=[Insert Into] table=[person]\
                        \n  Projection: column1 AS id, column2 AS first_name, column3 AS last_name, \
                                    CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, \
                                    CAST(NULL AS Timestamp(Nanosecond, None)) AS birth_date, CAST(NULL AS Int32) AS 😀\
                        \n    Values: (UInt32(1), Utf8(\"Alan\"), Utf8(\"Turing\"))";
    let plan = plan.replace_params_with_values(&param_values).unwrap();

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_to_plan_one_param() {
    let sql = "PREPARE my_plan(INT) AS SELECT id, age  FROM person WHERE age = $1";

    let expected_plan = "Prepare: \"my_plan\" [Int32] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = $1\
        \n      TableScan: person";

    let expected_dt = "[Int32]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![ScalarValue::Int32(Some(10))];
    let expected_plan = "Projection: person.id, person.age\
        \n  Filter: person.age = Int32(10)\
        \n    TableScan: person";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_to_plan_data_type() {
    let sql = "PREPARE my_plan(DOUBLE) AS SELECT id, age  FROM person WHERE age = $1";

    // age is defined as Int32 but prepare statement declares it as DOUBLE/Float64
    // Prepare statement and its logical plan should be created successfully
    let expected_plan = "Prepare: \"my_plan\" [Float64] \
        \n  Projection: person.id, person.age\
        \n    Filter: person.age = $1\
        \n      TableScan: person";

    let expected_dt = "[Float64]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values still succeed and use Float64
    let param_values = vec![ScalarValue::Float64(Some(10.0))];
    let expected_plan = "Projection: person.id, person.age\
        \n  Filter: person.age = Float64(10)\
        \n    TableScan: person";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_to_plan_multi_params() {
    let sql = "PREPARE my_plan(INT, STRING, DOUBLE, INT, DOUBLE, STRING) AS
        SELECT id, age, $6
        FROM person
        WHERE age IN ($1, $4) AND salary > $3 and salary < $5 OR first_name < $2";

    let expected_plan = "Prepare: \"my_plan\" [Int32, Utf8, Float64, Int32, Float64, Utf8] \
        \n  Projection: person.id, person.age, $6\
        \n    Filter: person.age IN ([$1, $4]) AND person.salary > $3 AND person.salary < $5 OR person.first_name < $2\
        \n      TableScan: person";

    let expected_dt = "[Int32, Utf8, Float64, Int32, Float64, Utf8]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Int32(Some(10)),
        ScalarValue::Utf8(Some("abc".to_string())),
        ScalarValue::Float64(Some(100.0)),
        ScalarValue::Int32(Some(20)),
        ScalarValue::Float64(Some(200.0)),
        ScalarValue::Utf8(Some("xyz".to_string())),
    ];
    let expected_plan =
            "Projection: person.id, person.age, Utf8(\"xyz\")\
        \n  Filter: person.age IN ([Int32(10), Int32(20)]) AND person.salary > Float64(100) AND person.salary < Float64(200) OR person.first_name < Utf8(\"abc\")\
        \n    TableScan: person";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_to_plan_having() {
    let sql = "PREPARE my_plan(INT, DOUBLE, DOUBLE, DOUBLE) AS
        SELECT id, SUM(age)
        FROM person \
        WHERE salary > $2
        GROUP BY id
        HAVING sum(age) < $1 AND SUM(age) > 10 OR SUM(age) in ($3, $4)\
        ";

    let expected_plan = "Prepare: \"my_plan\" [Int32, Float64, Float64, Float64] \
        \n  Projection: person.id, SUM(person.age)\
        \n    Filter: SUM(person.age) < $1 AND SUM(person.age) > Int64(10) OR SUM(person.age) IN ([$3, $4])\
        \n      Aggregate: groupBy=[[person.id]], aggr=[[SUM(person.age)]]\
        \n        Filter: person.salary > $2\
        \n          TableScan: person";

    let expected_dt = "[Int32, Float64, Float64, Float64]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Int32(Some(10)),
        ScalarValue::Float64(Some(100.0)),
        ScalarValue::Float64(Some(200.0)),
        ScalarValue::Float64(Some(300.0)),
    ];
    let expected_plan =
            "Projection: person.id, SUM(person.age)\
        \n  Filter: SUM(person.age) < Int32(10) AND SUM(person.age) > Int64(10) OR SUM(person.age) IN ([Float64(200), Float64(300)])\
        \n    Aggregate: groupBy=[[person.id]], aggr=[[SUM(person.age)]]\
        \n      Filter: person.salary > Float64(100)\
        \n        TableScan: person";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_prepare_statement_to_plan_value_list() {
    let sql = "PREPARE my_plan(STRING, STRING) AS SELECT * FROM (VALUES(1, $1), (2, $2)) AS t (num, letter);";

    let expected_plan = "Prepare: \"my_plan\" [Utf8, Utf8] \
        \n  Projection: t.num, t.letter\
        \n    SubqueryAlias: t\
        \n      Projection: column1 AS num, column2 AS letter\
        \n        Values: (Int64(1), $1), (Int64(2), $2)";

    let expected_dt = "[Utf8, Utf8]";

    let plan = prepare_stmt_quick_test(sql, expected_plan, expected_dt);

    ///////////////////
    // replace params with values
    let param_values = vec![
        ScalarValue::Utf8(Some("a".to_string())),
        ScalarValue::Utf8(Some("b".to_string())),
    ];
    let expected_plan = "Projection: t.num, t.letter\
        \n  SubqueryAlias: t\
        \n    Projection: column1 AS num, column2 AS letter\
        \n      Values: (Int64(1), Utf8(\"a\")), (Int64(2), Utf8(\"b\"))";

    prepare_stmt_replace_params_quick_test(plan, param_values, expected_plan);
}

#[test]
fn test_table_alias() {
    let sql = "select * from (\
          (select id from person) t1 \
            CROSS JOIN \
          (select age from person) t2 \
        ) as f";

    let expected = "Projection: f.id, f.age\
        \n  SubqueryAlias: f\
        \n    CrossJoin:\
        \n      SubqueryAlias: t1\
        \n        Projection: person.id\
        \n          TableScan: person\
        \n      SubqueryAlias: t2\
        \n        Projection: person.age\
        \n          TableScan: person";
    quick_test(sql, expected);

    let sql = "select * from (\
          (select id from person) t1 \
            CROSS JOIN \
          (select age from person) t2 \
        ) as f (c1, c2)";

    let expected = "Projection: f.c1, f.c2\
        \n  SubqueryAlias: f\
        \n    Projection: t1.id AS c1, t2.age AS c2\
        \n      CrossJoin:\
        \n        SubqueryAlias: t1\
        \n          Projection: person.id\
        \n            TableScan: person\
        \n        SubqueryAlias: t2\
        \n          Projection: person.age\
        \n            TableScan: person";
    quick_test(sql, expected);
}

#[test]
fn test_inner_join_with_cast_key() {
    let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON cast(person.id as Int) = cast(orders.customer_id as Int)";

    let expected = "Projection: person.id, person.age\
            \n  Inner Join:  Filter: CAST(person.id AS Int32) = CAST(orders.customer_id AS Int32)\
            \n    TableScan: person\
            \n    TableScan: orders";
    quick_test(sql, expected);
}

#[test]
fn test_multi_grouping_sets() {
    let sql = "SELECT person.id, person.age
            FROM person
            GROUP BY
                person.id,
                GROUPING SETS ((person.age,person.salary),(person.age))";

    let expected = "Projection: person.id, person.age\
    \n  Aggregate: groupBy=[[GROUPING SETS ((person.id, person.age, person.salary), (person.id, person.age))]], aggr=[[]]\
    \n    TableScan: person";
    quick_test(sql, expected);

    let sql = "SELECT person.id, person.age
            FROM person
            GROUP BY
                person.id,
                GROUPING SETS ((person.age, person.salary),(person.age)),
                ROLLUP(person.state, person.birth_date)";

    let expected = "Projection: person.id, person.age\
    \n  Aggregate: groupBy=[[GROUPING SETS (\
        (person.id, person.age, person.salary), \
        (person.id, person.age, person.salary, person.state), \
        (person.id, person.age, person.salary, person.state, person.birth_date), \
        (person.id, person.age), \
        (person.id, person.age, person.state), \
        (person.id, person.age, person.state, person.birth_date))]], aggr=[[]]\
    \n    TableScan: person";
    quick_test(sql, expected);
}

fn assert_field_not_found(err: DataFusionError, name: &str) {
    match err {
        DataFusionError::SchemaError { .. } => {
            let msg = format!("{err}");
            let expected = format!("Schema error: No field named {name}.");
            if !msg.starts_with(&expected) {
                panic!("error [{msg}] did not start with [{expected}]");
            }
        }
        _ => panic!("assert_field_not_found wrong error type"),
    }
}

struct EmptyTable {
    table_schema: SchemaRef,
}

impl EmptyTable {
    fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl TableSource for EmptyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for tests
    let _ = env_logger::try_init();
}
