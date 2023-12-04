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

#[tokio::test]
async fn string_coercion() -> Result<()> {
    let vendor_id_utf8: StringArray =
        vec![Some("124"), Some("345")].into_iter().collect();

    let vendor_id_dict: DictionaryArray<Int16Type> =
        vec![Some("124"), Some("345")].into_iter().collect();

    let batch = RecordBatch::try_from_iter(vec![
        ("vendor_id_utf8", Arc::new(vendor_id_utf8) as _),
        ("vendor_id_dict", Arc::new(vendor_id_dict) as _),
    ])
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;

    let expected = [
        "+----------------+----------------+",
        "| vendor_id_utf8 | vendor_id_dict |",
        "+----------------+----------------+",
        "| 124            | 124            |",
        "+----------------+----------------+",
    ];

    // Compare utf8 column with numeric constant
    let sql = "SELECT * from t where vendor_id_utf8 = 124";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    // Compare dictionary encoded utf8 column with numeric constant
    let sql = "SELECT * from t where vendor_id_dict = 124";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    // Compare dictionary encoded utf8 column with numeric constant with explicit cast
    let sql = "SELECT * from t where cast(vendor_id_utf8 as varchar) = 124";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
// Test issue: https://github.com/apache/arrow-datafusion/issues/3635
async fn multiple_or_predicates() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "lineitem").await?;
    register_tpch_csv(&ctx, "part").await?;
    let sql = "explain select
    l_partkey
    from
    lineitem,
    part
    where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and l_quantity >= 1 and l_quantity <= 1 + 10
            and p_size between 1 and 5
        )
    or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and l_quantity >= 10 and l_quantity <= 10 + 10
            and p_size between 1 and 10
        )
    or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#34'
            and l_quantity >= 20 and l_quantity <= 20 + 10
            and p_size between 1 and 15
        )";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan().unwrap();
    // Note that we expect `part.p_partkey = lineitem.l_partkey` to have been
    // factored out and appear only once in the following plan
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: lineitem.l_partkey [l_partkey:Int64]",
        "    Inner Join: lineitem.l_partkey = part.p_partkey Filter: part.p_brand = Utf8(\"Brand#12\") AND lineitem.l_quantity >= Decimal128(Some(100),15,2) AND lineitem.l_quantity <= Decimal128(Some(1100),15,2) AND part.p_size <= Int32(5) OR part.p_brand = Utf8(\"Brand#23\") AND lineitem.l_quantity >= Decimal128(Some(1000),15,2) AND lineitem.l_quantity <= Decimal128(Some(2000),15,2) AND part.p_size <= Int32(10) OR part.p_brand = Utf8(\"Brand#34\") AND lineitem.l_quantity >= Decimal128(Some(2000),15,2) AND lineitem.l_quantity <= Decimal128(Some(3000),15,2) AND part.p_size <= Int32(15) [l_partkey:Int64, l_quantity:Decimal128(15, 2), p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
        "      Filter: lineitem.l_quantity >= Decimal128(Some(100),15,2) AND lineitem.l_quantity <= Decimal128(Some(1100),15,2) OR lineitem.l_quantity >= Decimal128(Some(1000),15,2) AND lineitem.l_quantity <= Decimal128(Some(2000),15,2) OR lineitem.l_quantity >= Decimal128(Some(2000),15,2) AND lineitem.l_quantity <= Decimal128(Some(3000),15,2) [l_partkey:Int64, l_quantity:Decimal128(15, 2)]",
        "        TableScan: lineitem projection=[l_partkey, l_quantity], partial_filters=[lineitem.l_quantity >= Decimal128(Some(100),15,2) AND lineitem.l_quantity <= Decimal128(Some(1100),15,2) OR lineitem.l_quantity >= Decimal128(Some(1000),15,2) AND lineitem.l_quantity <= Decimal128(Some(2000),15,2) OR lineitem.l_quantity >= Decimal128(Some(2000),15,2) AND lineitem.l_quantity <= Decimal128(Some(3000),15,2)] [l_partkey:Int64, l_quantity:Decimal128(15, 2)]",
        "      Filter: (part.p_brand = Utf8(\"Brand#12\") AND part.p_size <= Int32(5) OR part.p_brand = Utf8(\"Brand#23\") AND part.p_size <= Int32(10) OR part.p_brand = Utf8(\"Brand#34\") AND part.p_size <= Int32(15)) AND part.p_size >= Int32(1) [p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
        "        TableScan: part projection=[p_partkey, p_brand, p_size], partial_filters=[part.p_size >= Int32(1), part.p_brand = Utf8(\"Brand#12\") AND part.p_size <= Int32(5) OR part.p_brand = Utf8(\"Brand#23\") AND part.p_size <= Int32(10) OR part.p_brand = Utf8(\"Brand#34\") AND part.p_size <= Int32(15)] [p_partkey:Int64, p_brand:Utf8, p_size:Int32]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    Ok(())
}

// Fix for issue#78 join predicates from inside of OR expr also pulled up properly.
#[tokio::test]
async fn tpch_q19_pull_predicates_to_innerjoin_simplified() -> Result<()> {
    let ctx = SessionContext::new();

    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let partsupp = r#"63700,7311,100,993.49,ven ideas. quickly even packages print. pending multipliers must have to are fluff"#;
    register_tpch_csv_data(&ctx, "partsupp", partsupp).await?;

    let sql = r#"
select
    p_partkey,
    sum(l_extendedprice),
    avg(l_discount),
    count(distinct ps_suppkey)
from
    lineitem,
    part,
    partsupp
where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#12'
            and p_partkey = ps_partkey
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#23'
            and ps_partkey = p_partkey
        )
        group by p_partkey
        ;"#;

    let dataframe = ctx.sql(sql).await.unwrap();
    let plan = dataframe.into_optimized_plan().unwrap();
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    let expected = vec![
        "Aggregate: groupBy=[[part.p_partkey]], aggr=[[SUM(lineitem.l_extendedprice), AVG(lineitem.l_discount), COUNT(DISTINCT partsupp.ps_suppkey)]] [p_partkey:Int64, SUM(lineitem.l_extendedprice):Decimal128(25, 2);N, AVG(lineitem.l_discount):Decimal128(19, 6);N, COUNT(DISTINCT partsupp.ps_suppkey):Int64;N]",
        "  Projection: lineitem.l_extendedprice, lineitem.l_discount, part.p_partkey, partsupp.ps_suppkey [l_extendedprice:Decimal128(15, 2), l_discount:Decimal128(15, 2), p_partkey:Int64, ps_suppkey:Int64]",
        "    Inner Join: part.p_partkey = partsupp.ps_partkey [l_extendedprice:Decimal128(15, 2), l_discount:Decimal128(15, 2), p_partkey:Int64, ps_partkey:Int64, ps_suppkey:Int64]",
        "      Projection: lineitem.l_extendedprice, lineitem.l_discount, part.p_partkey [l_extendedprice:Decimal128(15, 2), l_discount:Decimal128(15, 2), p_partkey:Int64]",
        "        Inner Join: lineitem.l_partkey = part.p_partkey [l_partkey:Int64, l_extendedprice:Decimal128(15, 2), l_discount:Decimal128(15, 2), p_partkey:Int64]",
        "          TableScan: lineitem projection=[l_partkey, l_extendedprice, l_discount] [l_partkey:Int64, l_extendedprice:Decimal128(15, 2), l_discount:Decimal128(15, 2)]",
        "          Filter: part.p_brand = Utf8(\"Brand#12\") OR part.p_brand = Utf8(\"Brand#23\"), projection=[p_partkey] [p_partkey:Int64]",
        "            TableScan: part projection=[p_partkey, p_brand], partial_filters=[part.p_brand = Utf8(\"Brand#12\") OR part.p_brand = Utf8(\"Brand#23\")] [p_partkey:Int64, p_brand:Utf8]",
        "      TableScan: partsupp projection=[ps_partkey, ps_suppkey] [ps_partkey:Int64, ps_suppkey:Int64]",
    ];

    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = ["+-----------+-------------------------------+--------------------------+-------------------------------------+",
        "| p_partkey | SUM(lineitem.l_extendedprice) | AVG(lineitem.l_discount) | COUNT(DISTINCT partsupp.ps_suppkey) |",
        "+-----------+-------------------------------+--------------------------+-------------------------------------+",
        "| 63700     | 13309.60                      | 0.100000                 | 1                                   |",
        "+-----------+-------------------------------+--------------------------+-------------------------------------+"];
    assert_batches_eq!(expected, &results);

    Ok(())
}
