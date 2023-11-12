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

use crate::decorrelate::{PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR};
use crate::optimizer::ApplyOrder;
use crate::utils::{conjunction, replace_qualified_name};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter,
};
use datafusion_common::{plan_err, Column, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr_rewriter::create_col_from_scalar_expr;
use datafusion_expr::logical_plan::{JoinType, Subquery};
use datafusion_expr::{expr, EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct ScalarSubqueryToJoin {}

impl ScalarSubqueryToJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that have a scalar subquery in them (and recurses when found)
    ///
    /// # Arguments
    /// * `predicate` - A conjunction to split and search
    ///
    /// Returns a tuple (subqueries, rewrite expression)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        alias_gen: Arc<AliasGenerator>,
    ) -> Result<(Vec<(Subquery, String)>, Expr)> {
        let mut extract = ExtractScalarSubQuery {
            sub_query_info: vec![],
            alias_gen,
        };
        let new_expr = predicate.clone().rewrite(&mut extract)?;
        Ok((extract.sub_query_info, new_expr))
    }
}

impl OptimizerRule for ScalarSubqueryToJoin {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let (subqueries, mut rewrite_expr) = self.extract_subquery_exprs(
                    &filter.predicate,
                    config.alias_generator(),
                )?;

                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(None);
                }

                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = filter.input.as_ref().clone();
                for (subquery, alias) in subqueries {
                    if let Some((optimized_subquery, expr_check_map)) =
                        build_join(&subquery, &cur_input, &alias)?
                    {
                        if !expr_check_map.is_empty() {
                            rewrite_expr =
                                rewrite_expr.clone().transform_up(&|expr| {
                                    if let Expr::Column(col) = &expr {
                                        if let Some(map_expr) =
                                            expr_check_map.get(&col.name)
                                        {
                                            Ok(Transformed::Yes(map_expr.clone()))
                                        } else {
                                            Ok(Transformed::No(expr))
                                        }
                                    } else {
                                        Ok(Transformed::No(expr))
                                    }
                                })?;
                        }
                        cur_input = optimized_subquery;
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(None);
                    }
                }
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .filter(rewrite_expr)?
                    .build()?;
                Ok(Some(new_plan))
            }
            LogicalPlan::Projection(projection) => {
                let mut all_subqueryies = vec![];
                let mut expr_to_rewrite_expr_map = HashMap::new();
                let mut subquery_to_expr_map = HashMap::new();
                for expr in projection.expr.iter() {
                    let (subqueries, rewrite_exprs) =
                        self.extract_subquery_exprs(expr, config.alias_generator())?;
                    for (subquery, _) in &subqueries {
                        subquery_to_expr_map.insert(subquery.clone(), expr.clone());
                    }
                    all_subqueryies.extend(subqueries);
                    expr_to_rewrite_expr_map.insert(expr, rewrite_exprs);
                }
                if all_subqueryies.is_empty() {
                    // regular projection, no subquery exists clause here
                    return Ok(None);
                }
                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = projection.input.as_ref().clone();
                for (subquery, alias) in all_subqueryies {
                    if let Some((optimized_subquery, expr_check_map)) =
                        build_join(&subquery, &cur_input, &alias)?
                    {
                        cur_input = optimized_subquery;
                        if !expr_check_map.is_empty() {
                            if let Some(expr) = subquery_to_expr_map.get(&subquery) {
                                if let Some(rewrite_expr) =
                                    expr_to_rewrite_expr_map.get(expr)
                                {
                                    let new_expr =
                                        rewrite_expr.clone().transform_up(&|expr| {
                                            if let Expr::Column(col) = &expr {
                                                if let Some(map_expr) =
                                                    expr_check_map.get(&col.name)
                                                {
                                                    Ok(Transformed::Yes(map_expr.clone()))
                                                } else {
                                                    Ok(Transformed::No(expr))
                                                }
                                            } else {
                                                Ok(Transformed::No(expr))
                                            }
                                        })?;
                                    expr_to_rewrite_expr_map.insert(expr, new_expr);
                                }
                            }
                        }
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(None);
                    }
                }

                let mut proj_exprs = vec![];
                for expr in projection.expr.iter() {
                    let old_expr_name = expr.display_name()?;
                    let new_expr = expr_to_rewrite_expr_map.get(expr).unwrap();
                    let new_expr_name = new_expr.display_name()?;
                    if new_expr_name != old_expr_name {
                        proj_exprs.push(new_expr.clone().alias(old_expr_name))
                    } else {
                        proj_exprs.push(new_expr.clone());
                    }
                }
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .project(proj_exprs)?
                    .build()?;
                Ok(Some(new_plan))
            }

            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

struct ExtractScalarSubQuery {
    sub_query_info: Vec<(Subquery, String)>,
    alias_gen: Arc<AliasGenerator>,
}

impl TreeNodeRewriter for ExtractScalarSubQuery {
    type N = Expr;

    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        match expr {
            Expr::ScalarSubquery(_) => Ok(RewriteRecursion::Mutate),
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::ScalarSubquery(subquery) => {
                let subqry_alias = self.alias_gen.next("__scalar_sq");
                self.sub_query_info
                    .push((subquery.clone(), subqry_alias.clone()));
                let scalar_expr = subquery
                    .subquery
                    .head_output_expr()?
                    .map_or(plan_err!("single expression required."), Ok)?;
                Ok(Expr::Column(create_col_from_scalar_expr(
                    &scalar_expr,
                    subqry_alias,
                )?))
            }
            _ => Ok(expr),
        }
    }
}

/// Takes a query like:
///
/// ```text
/// select id from customers where balance >
///     (select avg(total) from orders where orders.c_id = customers.id)
/// ```
///
/// and optimizes it into:
///
/// ```text
/// select c.id from customers c
/// left join (select c_id, avg(total) as val from orders group by c_id) o on o.c_id = c.c_id
/// where c.balance > o.val
/// ```
///
/// Or a query like:
///
/// ```text
/// select id from customers where balance >
///     (select avg(total) from orders)
/// ```
///
/// and optimizes it into:
///
/// ```text
/// select c.id from customers c
/// left join (select avg(total) as val from orders) a
/// where c.balance > a.val
/// ```
///
/// # Arguments
///
/// * `query_info` - The subquery portion of the `where` (select avg(total) from orders)
/// * `filter_input` - The non-subquery portion (from customers)
/// * `outer_others` - Any additional parts to the `where` expression (and c.x = y)
/// * `subquery_alias` - Subquery aliases
fn build_join(
    subquery: &Subquery,
    filter_input: &LogicalPlan,
    subquery_alias: &str,
) -> Result<Option<(LogicalPlan, HashMap<String, Expr>)>> {
    let subquery_plan = subquery.subquery.as_ref();
    let mut pull_up = PullUpCorrelatedExpr {
        join_filters: vec![],
        correlated_subquery_cols_map: Default::default(),
        in_predicate_opt: None,
        exists_sub_query: false,
        can_pull_up: true,
        need_handle_count_bug: true,
        collected_count_expr_map: Default::default(),
        pull_up_having_expr: None,
    };
    let new_plan = subquery_plan.clone().rewrite(&mut pull_up)?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let collected_count_expr_map =
        pull_up.collected_count_expr_map.get(&new_plan).cloned();
    let sub_query_alias = LogicalPlanBuilder::from(new_plan)
        .alias(subquery_alias.to_string())?
        .build()?;

    let mut all_correlated_cols = BTreeSet::new();
    pull_up
        .correlated_subquery_cols_map
        .values()
        .for_each(|cols| all_correlated_cols.extend(cols.clone()));

    // alias the join filter
    let join_filter_opt =
        conjunction(pull_up.join_filters).map_or(Ok(None), |filter| {
            replace_qualified_name(filter, &all_correlated_cols, subquery_alias)
                .map(Option::Some)
        })?;

    // join our sub query into the main plan
    let new_plan = if join_filter_opt.is_none() {
        match filter_input {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: _,
            }) => sub_query_alias,
            _ => {
                // if not correlated, group down to 1 row and left join on that (preserving row count)
                LogicalPlanBuilder::from(filter_input.clone())
                    .join_on(sub_query_alias, JoinType::Left, None)?
                    .build()?
            }
        }
    } else {
        // left join if correlated, grouping by the join keys so we don't change row count
        LogicalPlanBuilder::from(filter_input.clone())
            .join_on(sub_query_alias, JoinType::Left, join_filter_opt)?
            .build()?
    };
    let mut computation_project_expr = HashMap::new();
    if let Some(expr_map) = collected_count_expr_map {
        for (name, result) in expr_map {
            let computer_expr = if let Some(filter) = &pull_up.pull_up_having_expr {
                Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![
                        (
                            Box::new(Expr::IsNull(Box::new(Expr::Column(
                                Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                            )))),
                            Box::new(result),
                        ),
                        (
                            Box::new(Expr::Not(Box::new(filter.clone()))),
                            Box::new(Expr::Literal(ScalarValue::Null)),
                        ),
                    ],
                    else_expr: Some(Box::new(Expr::Column(Column::new_unqualified(
                        name.clone(),
                    )))),
                })
            } else {
                Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(Expr::IsNull(Box::new(Expr::Column(
                            Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                        )))),
                        Box::new(result),
                    )],
                    else_expr: Some(Box::new(Expr::Column(Column::new_unqualified(
                        name.clone(),
                    )))),
                })
            };
            computation_project_expr.insert(name, computer_expr);
        }
    }

    Ok(Some((new_plan, computation_project_expr)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::{logical_type::LogicalType as DataType, Result};
    use datafusion_expr::{
        col, lit, logical_plan::LogicalPlanBuilder, max, min, out_ref_col,
        scalar_subquery, sum, Between,
    };
    use std::ops::Add;

    /// Test multiple correlated subqueries
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                lit(1)
                    .lt(scalar_subquery(orders.clone()))
                    .and(lit(1).lt(scalar_subquery(orders))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: Int32(1) < __scalar_sq_1.MAX(orders.o_custkey) AND Int32(1) < __scalar_sq_2.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: __scalar_sq_2.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n            Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(
                    col("lineitem.l_orderkey")
                        .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
                )?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![sum(col("lineitem.l_extendedprice"))],
                )?
                .project(vec![sum(col("lineitem.l_extendedprice"))])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                        .and(col("orders.o_totalprice").lt(scalar_subquery(lineitem))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("orders.o_totalprice"))])?
                .project(vec![sum(col("orders.o_totalprice"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_acctbal").lt(scalar_subquery(orders)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_acctbal < __scalar_sq_1.SUM(orders.o_totalprice) [c_custkey:Int64, c_name:Utf8, SUM(orders.o_totalprice):Float64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, SUM(orders.o_totalprice):Float64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [SUM(orders.o_totalprice):Float64;N, o_custkey:Int64]\
        \n        Projection: SUM(orders.o_totalprice), orders.o_custkey [SUM(orders.o_totalprice):Float64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[SUM(orders.o_totalprice)]] [o_custkey:Int64, SUM(orders.o_totalprice):Float64;N]\
        \n            Filter: orders.o_totalprice < __scalar_sq_2.SUM(lineitem.l_extendedprice) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N]\
        \n              Left Join:  Filter: __scalar_sq_2.l_orderkey = orders.o_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N]\
        \n                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n                SubqueryAlias: __scalar_sq_2 [SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64]\
        \n                  Projection: SUM(lineitem.l_extendedprice), lineitem.l_orderkey [SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64]\
        \n                    Aggregate: groupBy=[[lineitem.l_orderkey]], aggr=[[SUM(lineitem.l_extendedprice)]] [l_orderkey:Int64, SUM(lineitem.l_extendedprice):Float64;N]\
        \n                      TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]";
        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional subquery filters
    #[test]
    fn scalar_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar subquery with no columns in schema
    #[test]
    fn scalar_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // it will optimize, but fail for the same reason the unoptimized query would
        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for scalar subquery with both columns in schema
    #[test]
    fn scalar_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar subquery not equal
    #[test]
    fn scalar_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .not_eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Correlated column is not allowed in predicate: outer_ref(customer.c_custkey) != orders.o_custkey";

        assert_analyzer_check_err(vec![], &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery less than
    #[test]
    fn scalar_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .lt(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Correlated column is not allowed in predicate: outer_ref(customer.c_custkey) < orders.o_custkey";

        assert_analyzer_check_err(vec![], &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with subquery disjunction
    #[test]
    fn scalar_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Correlated column is not allowed in predicate: outer_ref(customer.c_custkey) = orders.o_custkey OR orders.o_orderkey = Int32(1)";

        assert_analyzer_check_err(vec![], &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar without projection
    #[test]
    fn scalar_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Scalar subquery should only return one column";
        assert_analyzer_check_err(vec![], &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar expressions
    #[test]
    fn scalar_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![col("MAX(orders.o_custkey)").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) + Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey) + Int32(1), orders.o_custkey [MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar subquery multiple projected columns
    #[test]
    fn scalar_subquery_multi_col() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey"), col("orders.o_orderkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Scalar subquery should only return one column";
        assert_analyzer_check_err(vec![], &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional filters
    #[test]
    fn scalar_subquery_additional_filters_with_non_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .gt_eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey >= __scalar_sq_1.MAX(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn scalar_subquery_additional_filters_with_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar subquery filter with disjustions
    #[test]
    fn scalar_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .or(col("customer.c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
                .aggregate(Vec::<Expr>::new(), vec![min(col("c"))])?
                .project(vec![min(col("c"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(col("test.c").lt(scalar_subquery(sq)))?
            .project(vec![col("test.c")])?
            .build()?;

        let expected = "Projection: test.c [c:UInt32]\
        \n  Filter: test.c < __scalar_sq_1.MIN(sq.c) [a:UInt32, b:UInt32, c:UInt32, MIN(sq.c):UInt32;N, a:UInt32;N]\
        \n    Left Join:  Filter: test.a = __scalar_sq_1.a [a:UInt32, b:UInt32, c:UInt32, MIN(sq.c):UInt32;N, a:UInt32;N]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __scalar_sq_1 [MIN(sq.c):UInt32;N, a:UInt32]\
        \n        Projection: MIN(sq.c), sq.a [MIN(sq.c):UInt32;N, a:UInt32]\
        \n          Aggregate: groupBy=[[sq.a]], aggr=[[MIN(sq.c)]] [a:UInt32, MIN(sq.c):UInt32;N]\
        \n            TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for non-correlated scalar subquery with no filters
    #[test]
    fn scalar_subquery_non_correlated_no_filters_with_non_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").lt(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey < __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn scalar_subquery_non_correlated_no_filters_with_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn correlated_scalar_subquery_in_between_clause() -> Result<()> {
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
                .project(vec![min(col("orders.o_custkey"))])?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let between_expr = Expr::Between(Between {
            expr: Box::new(col("customer.c_custkey")),
            negated: false,
            low: Box::new(scalar_subquery(sq1)),
            high: Box::new(scalar_subquery(sq2)),
        });

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(between_expr)?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey BETWEEN __scalar_sq_1.MIN(orders.o_custkey) AND __scalar_sq_2.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [MIN(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Projection: MIN(orders.o_custkey), orders.o_custkey [MIN(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n            Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MIN(orders.o_custkey)]] [o_custkey:Int64, MIN(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn uncorrelated_scalar_subquery_in_between_clause() -> Result<()> {
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
                .project(vec![min(col("orders.o_custkey"))])?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let between_expr = Expr::Between(Between {
            expr: Box::new(col("customer.c_custkey")),
            negated: false,
            low: Box::new(scalar_subquery(sq1)),
            high: Box::new(scalar_subquery(sq2)),
        });

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(between_expr)?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey BETWEEN __scalar_sq_1.MIN(orders.o_custkey) AND __scalar_sq_2.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, MAX(orders.o_custkey):Int64;N]\
        \n      Left Join:  [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [MIN(orders.o_custkey):Int64;N]\
        \n          Projection: MIN(orders.o_custkey) [MIN(orders.o_custkey):Int64;N]\
        \n            Aggregate: groupBy=[[]], aggr=[[MIN(orders.o_custkey)]] [MIN(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_multi_rules_optimized_plan_eq_display_indent(
            vec![Arc::new(ScalarSubqueryToJoin::new())],
            &plan,
            expected,
        );
        Ok(())
    }
}
