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

use datafusion::arrow::array::ArrayRef;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use datafusion_substrait::logical_plan::{
    consumer::from_substrait_plan, producer::to_substrait_plan,
};

use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::{not_impl_err, plan_err, DFSchema, DFSchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{
    Extension, LogicalPlan, Repartition, UserDefinedLogicalNode, Volatility,
};
use datafusion::optimizer::simplify_expressions::expr_simplifier::THRESHOLD_INLINE_INLIST;
use datafusion::prelude::*;

use substrait::proto::extensions::simple_extension_declaration::MappingType;
use substrait::proto::rel::RelType;
use substrait::proto::{plan_rel, Plan, Rel};

struct MockSerializerRegistry;

impl SerializerRegistry for MockSerializerRegistry {
    fn serialize_logical_plan(
        &self,
        node: &dyn UserDefinedLogicalNode,
    ) -> Result<Vec<u8>> {
        if node.name() == "MockUserDefinedLogicalPlan" {
            let node = node
                .as_any()
                .downcast_ref::<MockUserDefinedLogicalPlan>()
                .unwrap();
            node.serialize()
        } else {
            unreachable!()
        }
    }

    fn deserialize_logical_plan(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<std::sync::Arc<dyn datafusion::logical_expr::UserDefinedLogicalNode>>
    {
        if name == "MockUserDefinedLogicalPlan" {
            MockUserDefinedLogicalPlan::deserialize(bytes)
        } else {
            unreachable!()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct MockUserDefinedLogicalPlan {
    /// Replacement for serialize/deserialize data
    validation_bytes: Vec<u8>,
    inputs: Vec<LogicalPlan>,
    empty_schema: DFSchemaRef,
}

impl UserDefinedLogicalNode for MockUserDefinedLogicalPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "MockUserDefinedLogicalPlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.empty_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MockUserDefinedLogicalPlan [validation_bytes={:?}]",
            self.validation_bytes
        )
    }

    fn from_template(
        &self,
        _: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(Self {
            validation_bytes: self.validation_bytes.clone(),
            inputs: inputs.to_vec(),
            empty_schema: Arc::new(DFSchema::empty()),
        })
    }

    fn dyn_hash(&self, _: &mut dyn std::hash::Hasher) {
        unimplemented!()
    }

    fn dyn_eq(&self, _: &dyn UserDefinedLogicalNode) -> bool {
        unimplemented!()
    }
}

impl MockUserDefinedLogicalPlan {
    pub fn new(validation_bytes: Vec<u8>) -> Self {
        Self {
            validation_bytes,
            inputs: vec![],
            empty_schema: Arc::new(DFSchema::empty()),
        }
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        Ok(self.validation_bytes.clone())
    }

    fn deserialize(bytes: &[u8]) -> Result<Arc<dyn UserDefinedLogicalNode>>
    where
        Self: Sized,
    {
        Ok(Arc::new(MockUserDefinedLogicalPlan::new(bytes.to_vec())))
    }
}

#[tokio::test]
async fn simple_select() -> Result<()> {
    roundtrip("SELECT a, b FROM data").await
}

#[tokio::test]
async fn wildcard_select() -> Result<()> {
    roundtrip("SELECT * FROM data").await
}

#[tokio::test]
async fn select_with_filter() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a > 1").await
}

#[tokio::test]
async fn select_with_reused_functions() -> Result<()> {
    let sql = "SELECT * FROM data WHERE a > 1 AND a < 10 AND b > 0";
    roundtrip(sql).await?;
    let (mut function_names, mut function_anchors) = function_extension_info(sql).await?;
    function_names.sort();
    function_anchors.sort();

    assert_eq!(function_names, ["and", "gt", "lt"]);
    assert_eq!(function_anchors, [0, 1, 2]);

    Ok(())
}

#[tokio::test]
async fn select_with_filter_date() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE c > CAST('2020-01-01' AS DATE)").await
}

#[tokio::test]
async fn select_with_filter_bool_expr() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d AND a > 1").await
}

#[tokio::test]
async fn select_with_limit() -> Result<()> {
    roundtrip_fill_na("SELECT * FROM data LIMIT 100").await
}

#[tokio::test]
async fn select_without_limit() -> Result<()> {
    roundtrip_fill_na("SELECT * FROM data OFFSET 10").await
}

#[tokio::test]
async fn select_with_limit_offset() -> Result<()> {
    roundtrip("SELECT * FROM data LIMIT 200 OFFSET 10").await
}

#[tokio::test]
async fn simple_aggregate() -> Result<()> {
    roundtrip("SELECT a, sum(b) FROM data GROUP BY a").await
}

#[tokio::test]
async fn aggregate_distinct_with_having() -> Result<()> {
    roundtrip("SELECT a, count(distinct b) FROM data GROUP BY a, c HAVING count(b) > 100")
        .await
}

#[tokio::test]
async fn aggregate_multiple_keys() -> Result<()> {
    roundtrip("SELECT a, c, avg(b) FROM data GROUP BY a, c").await
}

#[tokio::test]
async fn aggregate_grouping_sets() -> Result<()> {
    roundtrip(
        "SELECT a, c, d, avg(b) FROM data GROUP BY GROUPING SETS ((a, c), (a), (d), ())",
    )
    .await
}

#[tokio::test]
async fn aggregate_grouping_rollup() -> Result<()> {
    assert_expected_plan(
        "SELECT a, c, e, avg(b) FROM data GROUP BY ROLLUP (a, c, e)",
        "Aggregate: groupBy=[[GROUPING SETS ((data.a, data.c, data.e), (data.a, data.c), (data.a), ())]], aggr=[[AVG(data.b)]]\
        \n  TableScan: data projection=[a, b, c, e]"
    ).await
}

#[tokio::test]
async fn decimal_literal() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE b > 2.5").await
}

#[tokio::test]
async fn null_decimal_literal() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE b = NULL").await
}

#[tokio::test]
async fn u32_literal() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE e > 4294967295").await
}

#[tokio::test]
async fn simple_distinct() -> Result<()> {
    test_alias(
        "SELECT distinct a FROM data",
        "SELECT a FROM data GROUP BY a",
    )
    .await
}

#[tokio::test]
async fn select_distinct_two_fields() -> Result<()> {
    test_alias(
        "SELECT distinct a, b FROM data",
        "SELECT a, b FROM data GROUP BY a, b",
    )
    .await
}

#[tokio::test]
async fn simple_alias() -> Result<()> {
    test_alias("SELECT d1.a, d1.b FROM data d1", "SELECT a, b FROM data").await
}

#[tokio::test]
async fn two_table_alias() -> Result<()> {
    test_alias(
        "SELECT d1.a FROM data d1 JOIN data2 d2 ON d1.a = d2.a",
        "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
    )
    .await
}

#[tokio::test]
async fn between_integers() -> Result<()> {
    test_alias(
        "SELECT * FROM data WHERE a BETWEEN 2 AND 6",
        "SELECT * FROM data WHERE a >= 2 AND a <= 6",
    )
    .await
}

#[tokio::test]
async fn not_between_integers() -> Result<()> {
    test_alias(
        "SELECT * FROM data WHERE a NOT BETWEEN 2 AND 6",
        "SELECT * FROM data WHERE a < 2 OR a > 6",
    )
    .await
}

#[tokio::test]
async fn simple_scalar_function_abs() -> Result<()> {
    roundtrip("SELECT ABS(a) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_isnan() -> Result<()> {
    roundtrip("SELECT ISNAN(a) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_pow() -> Result<()> {
    roundtrip("SELECT POW(a, 2) FROM data").await
}

#[tokio::test]
async fn simple_scalar_function_substr() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = SUBSTR('datafusion', 0, 3)").await
}

#[tokio::test]
async fn simple_scalar_function_is_null() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a IS NULL").await
}

#[tokio::test]
async fn simple_scalar_function_is_not_null() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a IS NOT NULL").await
}

#[tokio::test]
async fn case_without_base_expression() -> Result<()> {
    roundtrip("SELECT (CASE WHEN a >= 0 THEN 'positive' ELSE 'negative' END) FROM data")
        .await
}

#[tokio::test]
async fn case_with_base_expression() -> Result<()> {
    roundtrip(
        "SELECT (CASE a
                            WHEN 0 THEN 'zero'
                            WHEN 1 THEN 'one'
                            ELSE 'other'
                           END) FROM data",
    )
    .await
}

#[tokio::test]
async fn cast_decimal_to_int() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = CAST(2.5 AS int)").await
}

#[tokio::test]
async fn implicit_cast() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a = b").await
}

#[tokio::test]
async fn aggregate_case() -> Result<()> {
    assert_expected_plan(
        "SELECT SUM(CASE WHEN a > 0 THEN 1 ELSE NULL END) FROM data",
        "Aggregate: groupBy=[[]], aggr=[[SUM(CASE WHEN data.a > Int64(0) THEN Int64(1) ELSE Int64(NULL) END)]]\
         \n  TableScan: data projection=[a]",
    )
        .await
}

#[tokio::test]
async fn roundtrip_inlist_1() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE a IN (1, 2, 3)").await
}

#[tokio::test]
// Test with length <= datafusion_optimizer::simplify_expressions::expr_simplifier::THRESHOLD_INLINE_INLIST
async fn roundtrip_inlist_2() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE f IN ('a', 'b', 'c')").await
}

#[tokio::test]
// Test with length > datafusion_optimizer::simplify_expressions::expr_simplifier::THRESHOLD_INLINE_INLIST
async fn roundtrip_inlist_3() -> Result<()> {
    let inlist = (0..THRESHOLD_INLINE_INLIST + 1)
        .map(|i| format!("'{i}'"))
        .collect::<Vec<_>>()
        .join(", ");

    roundtrip(&format!("SELECT * FROM data WHERE f IN ({inlist})")).await
}

#[tokio::test]
async fn roundtrip_inlist_4() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE f NOT IN ('a', 'b', 'c', 'd')").await
}

#[tokio::test]
async fn roundtrip_inlist_5() -> Result<()> {
    // on roundtrip there is an additional projection during TableScan which includes all column of the table,
    // using assert_expected_plan here as a workaround
    assert_expected_plan(
    "SELECT a, f FROM data WHERE (f IN ('a', 'b', 'c') OR a in (SELECT data2.a FROM data2 WHERE f IN ('b', 'c', 'd')))",
    "Filter: data.f = Utf8(\"a\") OR data.f = Utf8(\"b\") OR data.f = Utf8(\"c\") OR data.a IN (<subquery>)\
    \n  Subquery:\
    \n    Projection: data2.a\
    \n      Filter: data2.f IN ([Utf8(\"b\"), Utf8(\"c\"), Utf8(\"d\")])\
    \n        TableScan: data2 projection=[a, b, c, d, e, f]\
    \n  TableScan: data projection=[a, f], partial_filters=[data.f = Utf8(\"a\") OR data.f = Utf8(\"b\") OR data.f = Utf8(\"c\") OR data.a IN (<subquery>)]\
    \n    Subquery:\
    \n      Projection: data2.a\
    \n        Filter: data2.f IN ([Utf8(\"b\"), Utf8(\"c\"), Utf8(\"d\")])\
    \n          TableScan: data2 projection=[a, b, c, d, e, f]").await
}

#[tokio::test]
async fn roundtrip_cross_join() -> Result<()> {
    roundtrip("SELECT * FROM data CROSS JOIN data2").await
}

#[tokio::test]
async fn roundtrip_inner_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_non_equi_inner_join() -> Result<()> {
    roundtrip_verify_post_join_filter(
        "SELECT data.a FROM data JOIN data2 ON data.a <> data2.a",
    )
    .await
}

#[tokio::test]
async fn roundtrip_non_equi_join() -> Result<()> {
    roundtrip_verify_post_join_filter(
        "SELECT data.a FROM data, data2 WHERE data.a = data2.a AND data.e > data2.a",
    )
    .await
}

#[tokio::test]
async fn roundtrip_exists_filter() -> Result<()> {
    assert_expected_plan(
        "SELECT b FROM data d1 WHERE EXISTS (SELECT * FROM data2 d2 WHERE d2.a = d1.a AND d2.e != d1.e)",
        "Projection: data.b\
        \n  LeftSemi Join: data.a = data2.a Filter: data2.e != CAST(data.e AS Int64)\
        \n    TableScan: data projection=[a, b, e]\
        \n    TableScan: data2 projection=[a, e]"
    ).await
}

#[tokio::test]
async fn inner_join() -> Result<()> {
    assert_expected_plan(
        "SELECT data.a FROM data JOIN data2 ON data.a = data2.a",
        "Projection: data.a\
         \n  Inner Join: data.a = data2.a\
         \n    TableScan: data projection=[a]\
         \n    TableScan: data2 projection=[a]",
    )
    .await
}

#[tokio::test]
async fn roundtrip_left_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data LEFT JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_right_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data RIGHT JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_outer_join() -> Result<()> {
    roundtrip("SELECT data.a FROM data FULL OUTER JOIN data2 ON data.a = data2.a").await
}

#[tokio::test]
async fn roundtrip_arithmetic_ops() -> Result<()> {
    roundtrip("SELECT a - a FROM data").await?;
    roundtrip("SELECT a + a FROM data").await?;
    roundtrip("SELECT a * a FROM data").await?;
    roundtrip("SELECT a / a FROM data").await?;
    roundtrip("SELECT a = a FROM data").await?;
    roundtrip("SELECT a != a FROM data").await?;
    roundtrip("SELECT a > a FROM data").await?;
    roundtrip("SELECT a >= a FROM data").await?;
    roundtrip("SELECT a < a FROM data").await?;
    roundtrip("SELECT a <= a FROM data").await?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_like() -> Result<()> {
    roundtrip("SELECT f FROM data WHERE f LIKE 'a%b'").await
}

#[tokio::test]
async fn roundtrip_ilike() -> Result<()> {
    roundtrip("SELECT f FROM data WHERE f ILIKE 'a%b'").await
}

#[tokio::test]
async fn roundtrip_not() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE NOT d").await
}

#[tokio::test]
async fn roundtrip_negative() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE -a = 1").await
}

#[tokio::test]
async fn roundtrip_is_true() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS TRUE").await
}

#[tokio::test]
async fn roundtrip_is_false() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS FALSE").await
}

#[tokio::test]
async fn roundtrip_is_not_true() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS NOT TRUE").await
}

#[tokio::test]
async fn roundtrip_is_not_false() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS NOT FALSE").await
}

#[tokio::test]
async fn roundtrip_is_unknown() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS UNKNOWN").await
}

#[tokio::test]
async fn roundtrip_is_not_unknown() -> Result<()> {
    roundtrip("SELECT * FROM data WHERE d IS NOT UNKNOWN").await
}

#[tokio::test]
async fn roundtrip_union() -> Result<()> {
    roundtrip("SELECT a, e FROM data UNION SELECT a, e FROM data").await
}

#[tokio::test]
async fn roundtrip_union2() -> Result<()> {
    roundtrip(
        "SELECT a, b FROM data UNION SELECT a, b FROM data UNION SELECT a, b FROM data",
    )
    .await
}

#[tokio::test]
async fn roundtrip_union_all() -> Result<()> {
    roundtrip("SELECT a, e FROM data UNION ALL SELECT a, e FROM data").await
}

#[tokio::test]
async fn simple_intersect() -> Result<()> {
    assert_expected_plan(
        "SELECT COUNT(*) FROM (SELECT data.a FROM data INTERSECT SELECT data2.a FROM data2);",
        "Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
         \n  Projection: \
         \n    LeftSemi Join: data.a = data2.a\
         \n      Aggregate: groupBy=[[data.a]], aggr=[[]]\
         \n        TableScan: data projection=[a]\
         \n      TableScan: data2 projection=[a]",
    )
        .await
}

#[tokio::test]
async fn simple_intersect_table_reuse() -> Result<()> {
    assert_expected_plan(
        "SELECT COUNT(*) FROM (SELECT data.a FROM data INTERSECT SELECT data.a FROM data);",
        "Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
         \n  Projection: \
         \n    LeftSemi Join: data.a = data.a\
         \n      Aggregate: groupBy=[[data.a]], aggr=[[]]\
         \n        TableScan: data projection=[a]\
         \n      TableScan: data projection=[a]",
    )
        .await
}

#[tokio::test]
async fn simple_window_function() -> Result<()> {
    roundtrip("SELECT RANK() OVER (PARTITION BY a ORDER BY b), d, SUM(b) OVER (PARTITION BY a) FROM data;").await
}

#[tokio::test]
async fn qualified_schema_table_reference() -> Result<()> {
    roundtrip("SELECT * FROM public.data;").await
}

#[tokio::test]
async fn qualified_catalog_schema_table_reference() -> Result<()> {
    roundtrip("SELECT a,b,c,d,e FROM datafusion.public.data;").await
}

#[tokio::test]
async fn roundtrip_inner_join_table_reuse_zero_index() -> Result<()> {
    assert_expected_plan(
        "SELECT d1.b, d2.c FROM data d1 JOIN data d2 ON d1.a = d2.a",
        "Projection: data.b, data.c\
         \n  Inner Join: data.a = data.a\
         \n    TableScan: data projection=[a, b]\
         \n    TableScan: data projection=[a, c]",
    )
    .await
}

#[tokio::test]
async fn roundtrip_inner_join_table_reuse_non_zero_index() -> Result<()> {
    assert_expected_plan(
        "SELECT d1.b, d2.c FROM data d1 JOIN data d2 ON d1.b = d2.b",
        "Projection: data.b, data.c\
         \n  Inner Join: data.b = data.b\
         \n    TableScan: data projection=[b]\
         \n    TableScan: data projection=[b, c]",
    )
    .await
}

/// Construct a plan that contains several literals of types that are currently supported.
/// This case ignores:
/// - Date64, for this literal is not supported
/// - FixedSizeBinary, for converting UTF-8 literal to FixedSizeBinary is not supported
/// - List, this nested type is not supported in arrow_cast
/// - Decimal128 and Decimal256, them will fallback to UTF8 cast expr rather than plain literal.
#[tokio::test]
async fn all_type_literal() -> Result<()> {
    roundtrip_all_types(
        "select * from data where
            bool_col = TRUE AND
            int8_col = arrow_cast('0', 'Int8') AND
            uint8_col = arrow_cast('0', 'UInt8') AND
            int16_col = arrow_cast('0', 'Int16') AND
            uint16_col = arrow_cast('0', 'UInt16') AND
            int32_col = arrow_cast('0', 'Int32') AND
            uint32_col = arrow_cast('0', 'UInt32') AND
            int64_col = arrow_cast('0', 'Int64') AND
            uint64_col = arrow_cast('0', 'UInt64') AND
            float32_col = arrow_cast('0', 'Float32') AND
            float64_col = arrow_cast('0', 'Float64') AND
            sec_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Second, None)') AND
            ms_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Millisecond, None)') AND
            us_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Microsecond, None)') AND
            ns_timestamp_col = arrow_cast('2020-01-01 00:00:00', 'Timestamp (Nanosecond, None)') AND
            date32_col = arrow_cast('2020-01-01', 'Date32') AND
            binary_col = arrow_cast('binary', 'Binary') AND
            large_binary_col = arrow_cast('large_binary', 'LargeBinary') AND
            utf8_col = arrow_cast('utf8', 'Utf8') AND
            large_utf8_col = arrow_cast('large_utf8', 'LargeUtf8');",
    )
        .await
}

/// Construct a plan that cast columns. Only those SQL types are supported for now.
#[tokio::test]
async fn new_test_grammar() -> Result<()> {
    roundtrip_all_types(
        "select
            bool_col::boolean,
            int8_col::tinyint,
            uint8_col::tinyint unsigned,
            int16_col::smallint,
            uint16_col::smallint unsigned,
            int32_col::integer,
            uint32_col::integer unsigned,
            int64_col::bigint,
            uint64_col::bigint unsigned,
            float32_col::float,
            float64_col::double,
            decimal_128_col::decimal(10, 2),
            date32_col::date,
            binary_col::bytea
            from data",
    )
    .await
}

#[tokio::test]
async fn extension_logical_plan() -> Result<()> {
    let ctx = create_context().await?;
    let validation_bytes = "MockUserDefinedLogicalPlan".as_bytes().to_vec();
    let ext_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(MockUserDefinedLogicalPlan {
            validation_bytes,
            inputs: vec![],
            empty_schema: Arc::new(DFSchema::empty()),
        }),
    });

    let proto = to_substrait_plan(&ext_plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;

    let plan1str = format!("{ext_plan:?}");
    let plan2str = format!("{plan2:?}");
    assert_eq!(plan1str, plan2str);

    Ok(())
}

#[tokio::test]
async fn roundtrip_aggregate_udf() -> Result<()> {
    #[derive(Debug)]
    struct Dummy {}

    impl Accumulator for Dummy {
        fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
            Ok(vec![])
        }

        fn update_batch(
            &mut self,
            _values: &[ArrayRef],
        ) -> datafusion::error::Result<()> {
            Ok(())
        }

        fn merge_batch(&mut self, _states: &[ArrayRef]) -> datafusion::error::Result<()> {
            Ok(())
        }

        fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
            Ok(ScalarValue::Float64(None))
        }

        fn size(&self) -> usize {
            std::mem::size_of_val(self)
        }
    }

    let dummy_agg = create_udaf(
        // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
        "dummy_agg",
        // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
        vec![DataType::Int64],
        // the return type; DataFusion expects this to match the type returned by `evaluate`.
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        // This is the accumulator factory; DataFusion uses it to create new accumulators.
        Arc::new(|_| Ok(Box::new(Dummy {}))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![DataType::Float64, DataType::UInt32]),
    );

    let ctx = create_context().await?;
    ctx.register_udaf(dummy_agg);

    roundtrip_with_ctx("select dummy_agg(a) from data", ctx).await
}

#[tokio::test]
async fn roundtrip_repartition_roundrobin() -> Result<()> {
    let ctx = create_context().await?;
    let scan_plan = ctx.sql("SELECT * FROM data").await?.into_optimized_plan()?;
    let plan = LogicalPlan::Repartition(Repartition {
        input: Box::new(scan_plan),
        partitioning_scheme: Partitioning::RoundRobinBatch(8),
    });

    let proto = to_substrait_plan(&plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    assert_eq!(format!("{plan:?}"), format!("{plan2:?}"));
    Ok(())
}

#[tokio::test]
async fn roundtrip_repartition_hash() -> Result<()> {
    let ctx = create_context().await?;
    let scan_plan = ctx.sql("SELECT * FROM data").await?.into_optimized_plan()?;
    let plan = LogicalPlan::Repartition(Repartition {
        input: Box::new(scan_plan),
        partitioning_scheme: Partitioning::Hash(vec![col("data.a")], 8),
    });

    let proto = to_substrait_plan(&plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    assert_eq!(format!("{plan:?}"), format!("{plan2:?}"));
    Ok(())
}

fn check_post_join_filters(rel: &Rel) -> Result<()> {
    // search for target_rel and field value in proto
    match &rel.rel_type {
        Some(RelType::Join(join)) => {
            // check if join filter is None
            if join.post_join_filter.is_some() {
                plan_err!(
                    "DataFusion generated Susbtrait plan cannot have post_join_filter in JoinRel"
                )
            } else {
                // recursively check JoinRels
                match check_post_join_filters(join.left.as_ref().unwrap().as_ref()) {
                    Err(e) => Err(e),
                    Ok(_) => {
                        check_post_join_filters(join.right.as_ref().unwrap().as_ref())
                    }
                }
            }
        }
        Some(RelType::Project(p)) => {
            check_post_join_filters(p.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Filter(filter)) => {
            check_post_join_filters(filter.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Fetch(fetch)) => {
            check_post_join_filters(fetch.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Sort(sort)) => {
            check_post_join_filters(sort.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Aggregate(agg)) => {
            check_post_join_filters(agg.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::Set(set)) => {
            for input in &set.inputs {
                match check_post_join_filters(input) {
                    Err(e) => return Err(e),
                    Ok(_) => continue,
                }
            }
            Ok(())
        }
        Some(RelType::ExtensionSingle(ext)) => {
            check_post_join_filters(ext.input.as_ref().unwrap().as_ref())
        }
        Some(RelType::ExtensionMulti(ext)) => {
            for input in &ext.inputs {
                match check_post_join_filters(input) {
                    Err(e) => return Err(e),
                    Ok(_) => continue,
                }
            }
            Ok(())
        }
        Some(RelType::ExtensionLeaf(_)) | Some(RelType::Read(_)) => Ok(()),
        _ => not_impl_err!(
            "Unsupported RelType: {:?} in post join filter check",
            rel.rel_type
        ),
    }
}

async fn verify_post_join_filter_value(proto: Box<Plan>) -> Result<()> {
    for relation in &proto.relations {
        match relation.rel_type.as_ref() {
            Some(rt) => match rt {
                plan_rel::RelType::Rel(rel) => match check_post_join_filters(rel) {
                    Err(e) => return Err(e),
                    Ok(_) => continue,
                },
                plan_rel::RelType::Root(root) => {
                    match check_post_join_filters(root.input.as_ref().unwrap()) {
                        Err(e) => return Err(e),
                        Ok(_) => continue,
                    }
                }
            },
            None => return plan_err!("Cannot parse plan relation: None"),
        }
    }

    Ok(())
}

async fn assert_expected_plan(sql: &str, expected_plan_str: &str) -> Result<()> {
    let ctx = create_context().await?;
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;
    let plan2str = format!("{plan2:?}");
    assert_eq!(expected_plan_str, &plan2str);
    Ok(())
}

async fn roundtrip_fill_na(sql: &str) -> Result<()> {
    let ctx = create_context().await?;
    let df = ctx.sql(sql).await?;
    let plan1 = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan1, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    // Format plan string and replace all None's with 0
    let plan1str = format!("{plan1:?}").replace("None", "0");
    let plan2str = format!("{plan2:?}").replace("None", "0");

    assert_eq!(plan1str, plan2str);
    Ok(())
}

async fn test_alias(sql_with_alias: &str, sql_no_alias: &str) -> Result<()> {
    // Since we ignore the SubqueryAlias in the producer, the result should be
    // the same as producing a Substrait plan from the same query without aliases
    // sql_with_alias -> substrait -> logical plan = sql_no_alias -> substrait -> logical plan
    let ctx = create_context().await?;

    let df_a = ctx.sql(sql_with_alias).await?;
    let proto_a = to_substrait_plan(&df_a.into_optimized_plan()?, &ctx)?;
    let plan_with_alias = from_substrait_plan(&ctx, &proto_a).await?;

    let df = ctx.sql(sql_no_alias).await?;
    let proto = to_substrait_plan(&df.into_optimized_plan()?, &ctx)?;
    let plan = from_substrait_plan(&ctx, &proto).await?;

    println!("{plan_with_alias:#?}");
    println!("{plan:#?}");

    let plan1str = format!("{plan_with_alias:?}");
    let plan2str = format!("{plan:?}");
    assert_eq!(plan1str, plan2str);
    Ok(())
}

async fn roundtrip_with_ctx(sql: &str, ctx: SessionContext) -> Result<()> {
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    println!("{plan:#?}");
    println!("{plan2:#?}");

    let plan1str = format!("{plan:?}");
    let plan2str = format!("{plan2:?}");
    assert_eq!(plan1str, plan2str);
    Ok(())
}

async fn roundtrip(sql: &str) -> Result<()> {
    roundtrip_with_ctx(sql, create_context().await?).await
}

async fn roundtrip_verify_post_join_filter(sql: &str) -> Result<()> {
    let ctx = create_context().await?;
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    println!("{plan:#?}");
    println!("{plan2:#?}");

    let plan1str = format!("{plan:?}");
    let plan2str = format!("{plan2:?}");
    assert_eq!(plan1str, plan2str);

    // verify that the join filters are None
    verify_post_join_filter_value(proto).await
}

async fn roundtrip_all_types(sql: &str) -> Result<()> {
    let ctx = create_all_type_context().await?;
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx)?;
    let plan2 = from_substrait_plan(&ctx, &proto).await?;
    let plan2 = ctx.state().optimize(&plan2)?;

    println!("{plan:#?}");
    println!("{plan2:#?}");

    let plan1str = format!("{plan:?}");
    let plan2str = format!("{plan2:?}");
    assert_eq!(plan1str, plan2str);
    Ok(())
}

async fn function_extension_info(sql: &str) -> Result<(Vec<String>, Vec<u32>)> {
    let ctx = create_context().await?;
    let df = ctx.sql(sql).await?;
    let plan = df.into_optimized_plan()?;
    let proto = to_substrait_plan(&plan, &ctx)?;

    let mut function_names: Vec<String> = vec![];
    let mut function_anchors: Vec<u32> = vec![];
    for e in &proto.extensions {
        let (function_anchor, function_name) = match e.mapping_type.as_ref().unwrap() {
            MappingType::ExtensionFunction(ext_f) => (ext_f.function_anchor, &ext_f.name),
            _ => unreachable!("Producer does not generate a non-function extension"),
        };
        function_names.push(function_name.to_string());
        function_anchors.push(function_anchor);
    }

    Ok((function_names, function_anchors))
}

async fn create_context() -> Result<SessionContext> {
    let state = SessionState::new_with_config_rt(
        SessionConfig::default(),
        Arc::new(RuntimeEnv::default()),
    )
    .with_serializer_registry(Arc::new(MockSerializerRegistry));
    let ctx = SessionContext::new_with_state(state);
    let mut explicit_options = CsvReadOptions::new();
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Decimal128(5, 2), true),
        Field::new("c", DataType::Date32, true),
        Field::new("d", DataType::Boolean, true),
        Field::new("e", DataType::UInt32, true),
        Field::new("f", DataType::Utf8, true),
    ]);
    explicit_options.schema = Some(&schema);
    ctx.register_csv("data", "tests/testdata/data.csv", explicit_options)
        .await?;
    ctx.register_csv("data2", "tests/testdata/data.csv", CsvReadOptions::new())
        .await?;
    Ok(ctx)
}

/// Cover all supported types
async fn create_all_type_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let mut explicit_options = CsvReadOptions::new();
    let schema = Schema::new(vec![
        Field::new("bool_col", DataType::Boolean, true),
        Field::new("int8_col", DataType::Int8, true),
        Field::new("uint8_col", DataType::UInt8, true),
        Field::new("int16_col", DataType::Int16, true),
        Field::new("uint16_col", DataType::UInt16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("uint32_col", DataType::UInt32, true),
        Field::new("int64_col", DataType::Int64, true),
        Field::new("uint64_col", DataType::UInt64, true),
        Field::new("float32_col", DataType::Float32, true),
        Field::new("float64_col", DataType::Float64, true),
        Field::new(
            "sec_timestamp_col",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "ms_timestamp_col",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "us_timestamp_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "ns_timestamp_col",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("date32_col", DataType::Date32, true),
        Field::new("date64_col", DataType::Date64, true),
        Field::new("binary_col", DataType::Binary, true),
        Field::new("large_binary_col", DataType::LargeBinary, true),
        Field::new("fixed_size_binary_col", DataType::FixedSizeBinary(42), true),
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("large_utf8_col", DataType::LargeUtf8, true),
        Field::new_list("list_col", Field::new("item", DataType::Int64, true), true),
        Field::new_list(
            "large_list_col",
            Field::new("item", DataType::Int64, true),
            true,
        ),
        Field::new("decimal_128_col", DataType::Decimal128(10, 2), true),
        Field::new("decimal_256_col", DataType::Decimal256(10, 2), true),
    ]);
    explicit_options.schema = Some(&schema);
    explicit_options.has_header = false;
    ctx.register_csv("data", "tests/testdata/empty.csv", explicit_options)
        .await?;

    Ok(ctx)
}
