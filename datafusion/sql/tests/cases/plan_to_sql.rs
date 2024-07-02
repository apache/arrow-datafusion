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

use std::vec;

use arrow_schema::*;
use datafusion_common::{DFSchema, Result, TableReference};
use datafusion_expr::test::function_stub::{count_udaf, sum_udaf};
use datafusion_expr::{col, table_scan};
use datafusion_sql::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_sql::unparser::dialect::{
    DefaultDialect as UnparserDefaultDialect, Dialect as UnparserDialect,
    MySqlDialect as UnparserMySqlDialect,
};
use datafusion_sql::unparser::{expr_to_sql, plan_to_sql, Unparser};

use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;
use crate::common::MockContextProvider;

#[test]
fn roundtrip_expr() {
    let tests: Vec<(TableReference, &str, &str)> = vec![
        (TableReference::bare("person"), "age > 35", r#"(age > 35)"#),
        (
            TableReference::bare("person"),
            "id = '10'",
            r#"(id = '10')"#,
        ),
        (
            TableReference::bare("person"),
            "CAST(id AS VARCHAR)",
            r#"CAST(id AS VARCHAR)"#,
        ),
        (
            TableReference::bare("person"),
            "sum((age * 2))",
            r#"sum((age * 2))"#,
        ),
    ];

    let roundtrip = |table, sql: &str| -> Result<String> {
        let dialect = GenericDialect {};
        let sql_expr = Parser::new(&dialect).try_with_sql(sql)?.parse_expr()?;

        let context = MockContextProvider::default().with_udaf(sum_udaf());
        let schema = context.get_table_source(table)?.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let sql_to_rel = SqlToRel::new(&context);
        let expr =
            sql_to_rel.sql_to_expr(sql_expr, &df_schema, &mut PlannerContext::new())?;

        let ast = expr_to_sql(&expr)?;

        Ok(format!("{}", ast))
    };

    for (table, query, expected) in tests {
        let actual = roundtrip(table, query).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn roundtrip_statement() -> Result<()> {
    let tests: Vec<&str> = vec![
            "select 1;",
            "select 1 limit 0;",
            "select ta.j1_id from j1 ta join (select 1 as j1_id) tb on ta.j1_id = tb.j1_id;",
            "select ta.j1_id from j1 ta join (select 1 as j1_id) tb on ta.j1_id = tb.j1_id where ta.j1_id > 1;",
            "select ta.j1_id from (select 1 as j1_id) ta;",
            "select ta.j1_id from j1 ta;",
            "select ta.j1_id from j1 ta order by ta.j1_id;",
            "select * from j1 ta order by ta.j1_id, ta.j1_string desc;",
            "select * from j1 limit 10;",
            "select ta.j1_id from j1 ta where ta.j1_id > 1;",
            "select ta.j1_id, tb.j2_string from j1 ta join j2 tb on (ta.j1_id = tb.j2_id);",
            "select ta.j1_id, tb.j2_string, tc.j3_string from j1 ta join j2 tb on (ta.j1_id = tb.j2_id) join j3 tc on (ta.j1_id = tc.j3_id);",
            "select * from (select id, first_name from person)",
            "select * from (select id, first_name from (select * from person))",
            "select id, count(*) as cnt from (select id from person) group by id",
            "select (id-1)/2, count(*) / (sum(id/10)-1) as agg_expr from (select (id-1) as id from person) group by id",
            "select CAST(id/2 as VARCHAR) NOT LIKE 'foo*' from person where NOT EXISTS (select ta.j1_id, tb.j2_string from j1 ta join j2 tb on (ta.j1_id = tb.j2_id))",
            r#"select "First Name" from person_quoted_cols"#,
            "select DISTINCT id FROM person",
            "select DISTINCT on (id) id, first_name from person",
            "select DISTINCT on (id) id, first_name from person order by id",
            r#"select id, count("First Name") as cnt from (select id, "First Name" from person_quoted_cols) group by id"#,
            "select id, count(*) as cnt from (select p1.id as id from person p1 inner join person p2 on p1.id=p2.id) group by id",
            "select id, count(*), first_name from person group by first_name, id",
            "select id, sum(age), first_name from person group by first_name, id",
            "select id, count(*), first_name 
            from person 
            where id!=3 and first_name=='test'
            group by first_name, id 
            having count(*)>5 and count(*)<10
            order by count(*)",
            r#"select id, count("First Name") as count_first_name, "Last Name" 
            from person_quoted_cols
            where id!=3 and "First Name"=='test'
            group by "Last Name", id 
            having count_first_name>5 and count_first_name<10
            order by count_first_name, "Last Name""#,
            r#"select p.id, count("First Name") as count_first_name,
            "Last Name", sum(qp.id/p.id - (select sum(id) from person_quoted_cols) ) / (select count(*) from person) 
            from (select id, "First Name", "Last Name" from person_quoted_cols) qp
            inner join (select * from person) p
            on p.id = qp.id
            where p.id!=3 and "First Name"=='test' and qp.id in 
            (select id from (select id, count(*) from person group by id having count(*) > 0))
            group by "Last Name", p.id 
            having count_first_name>5 and count_first_name<10
            order by count_first_name, "Last Name""#,
            r#"SELECT j1_string as string FROM j1
            UNION ALL
            SELECT j2_string as string FROM j2"#,
            r#"SELECT j1_string as string FROM j1
            UNION ALL
            SELECT j2_string as string FROM j2
            ORDER BY string DESC
            LIMIT 10"#,
            "SELECT id, count(*) over (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 
            last_name, sum(id) over (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 
            first_name from person",
            r#"SELECT id, count(distinct id) over (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 
            sum(id) OVER (PARTITION BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) from person"#,
            "SELECT id, sum(id) OVER (PARTITION BY first_name ROWS BETWEEN 5 PRECEDING AND 2 FOLLOWING) from person",            
        ];

    // For each test sql string, we transform as follows:
    // sql -> ast::Statement (s1) -> LogicalPlan (p1) -> ast::Statement (s2) -> LogicalPlan (p2)
    // We test not that s1==s2, but rather p1==p2. This ensures that unparser preserves the logical
    // query information of the original sql string and disreguards other differences in syntax or
    // quoting.
    for query in tests {
        let dialect = GenericDialect {};
        let statement = Parser::new(&dialect)
            .try_with_sql(query)?
            .parse_statement()?;

        let context = MockContextProvider::default()
            .with_udaf(sum_udaf())
            .with_udaf(count_udaf());
        let sql_to_rel = SqlToRel::new(&context);
        let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

        let roundtrip_statement = plan_to_sql(&plan)?;

        let actual = format!("{}", &roundtrip_statement);
        println!("roundtrip sql: {actual}");
        println!("plan {}", plan.display_indent());

        let plan_roundtrip = sql_to_rel
            .sql_statement_to_plan(roundtrip_statement.clone())
            .unwrap();

        assert_eq!(plan, plan_roundtrip);
    }

    Ok(())
}

#[test]
fn roundtrip_crossjoin() -> Result<()> {
    let query = "select j1.j1_id, j2.j2_string from j1, j2";

    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(query)?
        .parse_statement()?;

    let context = MockContextProvider::default();
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

    let roundtrip_statement = plan_to_sql(&plan)?;

    let actual = format!("{}", &roundtrip_statement);
    println!("roundtrip sql: {actual}");
    println!("plan {}", plan.display_indent());

    let plan_roundtrip = sql_to_rel
        .sql_statement_to_plan(roundtrip_statement.clone())
        .unwrap();

    let expected = "Projection: j1.j1_id, j2.j2_string\
        \n  Inner Join:  Filter: Boolean(true)\
        \n    TableScan: j1\
        \n    TableScan: j2";

    assert_eq!(format!("{plan_roundtrip:?}"), expected);

    Ok(())
}

#[test]
fn roundtrip_statement_with_dialect() -> Result<()> {
    struct TestStatementWithDialect {
        sql: &'static str,
        expected: &'static str,
        parser_dialect: Box<dyn Dialect>,
        unparser_dialect: Box<dyn UnparserDialect>,
    }
    let tests: Vec<TestStatementWithDialect> = vec![
        TestStatementWithDialect {
            sql: "select ta.j1_id from j1 ta order by j1_id limit 10;",
            expected:
                "SELECT `ta`.`j1_id` FROM `j1` AS `ta` ORDER BY `ta`.`j1_id` ASC LIMIT 10",
            parser_dialect: Box::new(MySqlDialect {}),
            unparser_dialect: Box::new(UnparserMySqlDialect {}),
        },
        TestStatementWithDialect {
            sql: "select ta.j1_id from j1 ta order by j1_id limit 10;",
            expected: r#"SELECT ta.j1_id FROM j1 AS ta ORDER BY ta.j1_id ASC NULLS LAST LIMIT 10"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
        TestStatementWithDialect {
            sql: "SELECT j1_id FROM j1
                  UNION ALL
                  SELECT tb.j2_id as j1_id FROM j2 tb
                  ORDER BY j1_id
                  LIMIT 10;",
            expected: r#"SELECT j1.j1_id FROM j1 UNION ALL SELECT tb.j2_id AS j1_id FROM j2 AS tb ORDER BY j1_id ASC NULLS LAST LIMIT 10"#,
            parser_dialect: Box::new(GenericDialect {}),
            unparser_dialect: Box::new(UnparserDefaultDialect {}),
        },
    ];

    for query in tests {
        let statement = Parser::new(&*query.parser_dialect)
            .try_with_sql(query.sql)?
            .parse_statement()?;

        let context = MockContextProvider::default();
        let sql_to_rel = SqlToRel::new(&context);
        let plan = sql_to_rel
            .sql_statement_to_plan(statement)
            .unwrap_or_else(|e| panic!("Failed to parse sql: {}\n{e}", query.sql));

        let unparser = Unparser::new(&*query.unparser_dialect);
        let roundtrip_statement = unparser.plan_to_sql(&plan)?;

        let actual = format!("{}", &roundtrip_statement);
        println!("roundtrip sql: {actual}");
        println!("plan {}", plan.display_indent());

        assert_eq!(query.expected, actual);
    }

    Ok(())
}

#[test]
fn test_unnest_logical_plan() -> Result<()> {
    let query = "select unnest(struct_col), unnest(array_col), struct_col, array_col from unnest_table";

    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(query)?
        .parse_statement()?;

    let context = MockContextProvider::default();
    let sql_to_rel = SqlToRel::new(&context);
    let plan = sql_to_rel.sql_statement_to_plan(statement).unwrap();

    let expected = "Projection: unnest(unnest_table.struct_col).field1, unnest(unnest_table.struct_col).field2, unnest(unnest_table.array_col), unnest_table.struct_col, unnest_table.array_col\
        \n  Unnest: lists[unnest(unnest_table.array_col)] structs[unnest(unnest_table.struct_col)]\
        \n    Projection: unnest_table.struct_col AS unnest(unnest_table.struct_col), unnest_table.array_col AS unnest(unnest_table.array_col), unnest_table.struct_col, unnest_table.array_col\
        \n      TableScan: unnest_table";

    assert_eq!(format!("{plan:?}"), expected);

    Ok(())
}

#[test]
fn test_table_references_in_plan_to_sql() {
    fn test(table_name: &str, expected_sql: &str) {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        let plan = table_scan(Some(table_name), &schema, None)
            .unwrap()
            .project(vec![col("id"), col("value")])
            .unwrap()
            .build()
            .unwrap();
        let sql = plan_to_sql(&plan).unwrap();

        assert_eq!(format!("{}", sql), expected_sql)
    }

    test("catalog.schema.table", "SELECT catalog.\"schema\".\"table\".id, catalog.\"schema\".\"table\".\"value\" FROM catalog.\"schema\".\"table\"");
    test("schema.table", "SELECT \"schema\".\"table\".id, \"schema\".\"table\".\"value\" FROM \"schema\".\"table\"");
    test(
        "table",
        "SELECT \"table\".id, \"table\".\"value\" FROM \"table\"",
    );
}
