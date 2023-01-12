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

use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;
use log::debug;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[tokio::test]
async fn correlated_recursive_scalar_subquery() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"
select c_custkey from customer
where c_acctbal < (
    select sum(o_totalprice) from orders
    where o_custkey = c_custkey
    and o_totalprice < (
            select sum(l_extendedprice) as price from lineitem where l_orderkey = o_orderkey
    )
) order by c_custkey;"#;

    // assert plan
    let dataframe = ctx.sql(sql).await.unwrap();
    debug!("input:\n{}", dataframe.logical_plan().display_indent());

    let plan = dataframe.into_optimized_plan().unwrap();
    let actual = format!("{}", plan.display_indent());

    let expected = "Sort: customer.c_custkey ASC NULLS LAST\
    \n  Projection: customer.c_custkey\
    \n    Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey Filter: CAST(customer.c_acctbal AS Decimal128(25, 2)) < __scalar_sq_1.__value\
    \n      TableScan: customer projection=[c_custkey, c_acctbal]\
    \n      SubqueryAlias: __scalar_sq_1\
    \n        Projection: orders.o_custkey, SUM(orders.o_totalprice) AS __value\
    \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[SUM(orders.o_totalprice)]]\
    \n            Inner Join: orders.o_orderkey = __scalar_sq_2.l_orderkey Filter: CAST(orders.o_totalprice AS Decimal128(25, 2)) < __scalar_sq_2.__value\
    \n              TableScan: orders projection=[o_orderkey, o_custkey, o_totalprice]\
    \n              SubqueryAlias: __scalar_sq_2\
    \n                Projection: lineitem.l_orderkey, SUM(lineitem.l_extendedprice) AS price AS __value\
    \n                  Aggregate: groupBy=[[lineitem.l_orderkey]], aggr=[[SUM(lineitem.l_extendedprice)]]\
    \n                    TableScan: lineitem projection=[l_orderkey, l_extendedprice]";
    assert_eq!(actual, expected);

    Ok(())
}

#[tokio::test]
async fn correlated_where_in() -> Result<()> {
    let orders = r#"1,3691,O,194029.55,1996-01-02,5-LOW,Clerk#000000951,0,
65,1627,P,99763.79,1995-03-18,1-URGENT,Clerk#000000632,0,
"#;
    let lineitems = r#"1,15519,785,1,17,24386.67,0.04,0.02,N,O,1996-03-13,1996-02-12,1996-03-22,DELIVER IN PERSON,TRUCK,
1,6731,732,2,36,58958.28,0.09,0.06,N,O,1996-04-12,1996-02-28,1996-04-20,TAKE BACK RETURN,MAIL,
65,5970,481,1,26,48775.22,0.03,0.03,A,F,1995-04-20,1995-04-25,1995-05-13,NONE,TRUCK,
65,7382,897,2,22,28366.36,0,0.05,N,O,1995-07-17,1995-06-04,1995-07-19,COLLECT COD,FOB,
"#;

    let ctx = SessionContext::new();
    register_tpch_csv_data(&ctx, "orders", orders).await?;
    register_tpch_csv_data(&ctx, "lineitem", lineitems).await?;

    let sql = r#"select o_orderkey from orders
where o_orderstatus in (
    select l_linestatus from lineitem where l_orderkey = orders.o_orderkey
);"#;

    // assert plan
    let dataframe = ctx.sql(sql).await.unwrap();
    let plan = dataframe.into_optimized_plan().unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Projection: orders.o_orderkey
  LeftSemi Join: orders.o_orderstatus = __correlated_sq_1.l_linestatus, orders.o_orderkey = __correlated_sq_1.l_orderkey
    TableScan: orders projection=[o_orderkey, o_orderstatus]
    SubqueryAlias: __correlated_sq_1
      Projection: lineitem.l_linestatus AS l_linestatus, lineitem.l_orderkey AS l_orderkey
        TableScan: lineitem projection=[l_orderkey, l_linestatus]"#;
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+",
        "| o_orderkey |",
        "+------------+",
        "| 1          |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
