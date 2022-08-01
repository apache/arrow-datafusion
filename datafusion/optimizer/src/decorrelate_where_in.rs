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

use crate::utils::{
    alias_cols, exprs_to_join_cols, find_join_exprs, merge_cols, only_or_err,
    split_conjunction, swap_table, verify_not_disjunction,
};
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::context;
use datafusion_expr::logical_plan::{Filter, JoinType, Projection, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use log::debug;
use std::sync::Arc;

#[derive(Default)]
pub struct DecorrelateWhereIn {}

impl DecorrelateWhereIn {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Finds expressions that have a where in subquery (and recurses when found)
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    /// * `optimizer_config` - For generating unique subquery aliases
    ///
    /// Returns a tuple (subqueries, non-subquery expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let mut filters = vec![];
        split_conjunction(predicate, &mut filters); // TODO: disjunctions

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let subquery = self.optimize(&subquery.subquery, optimizer_config)?;
                    let subquery = Arc::new(subquery);
                    let subquery = Subquery { subquery };
                    let subquery =
                        SubqueryInfo::new(subquery.clone(), (**expr).clone(), *negated);
                    subqueries.push(subquery);
                    // TODO: if subquery doesn't get optimized, optimized children are lost
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateWhereIn {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter {
                predicate,
                input: filter_input,
            }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(filter_input, optimizer_config)?;

                let (subqueries, other_exprs) =
                    self.extract_subquery_exprs(predicate, optimizer_config)?;
                let optimized_plan = LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized_input),
                });
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(optimized_plan);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = (**filter_input).clone();
                for subquery in subqueries {
                    cur_input = optimize_where_in(
                        &subquery,
                        &cur_input,
                        &other_exprs,
                        optimizer_config,
                    )?;
                }
                Ok(cur_input)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "decorrelate_where_in"
    }
}

fn optimize_where_in(
    query_info: &SubqueryInfo,
    outer_input: &LogicalPlan,
    outer_other_exprs: &[Expr],
    optimizer_config: &mut OptimizerConfig,
) -> datafusion_common::Result<LogicalPlan> {
    let proj = Projection::try_from_plan(&query_info.query.subquery)
        .map_err(|e| context!("a projection is required", e))?;
    let mut subqry_input = proj.input.clone();
    let proj = only_or_err(proj.expr.as_slice())
        .map_err(|e| context!("single expression projection required", e))?;
    let subquery_col = proj
        .try_into_col()
        .map_err(|e| context!("single column projection required", e))?;
    let outer_col = query_info
        .where_in_expr
        .try_into_col()
        .map_err(|e| context!("column comparison required", e))?;

    // If subquery is correlated, grab necessary information
    let mut subqry_cols = vec![];
    let mut outer_cols = vec![];
    let mut join_filters = None;
    let mut other_subqry_exprs = vec![];
    if let LogicalPlan::Filter(subqry_filter) = (*subqry_input).clone() {
        // split into filters
        let mut subqry_filter_exprs = vec![];
        split_conjunction(&subqry_filter.predicate, &mut subqry_filter_exprs);
        verify_not_disjunction(&subqry_filter_exprs)?;

        // Grab column names to join on
        let (col_exprs, other_exprs) =
            find_join_exprs(subqry_filter_exprs, subqry_filter.input.schema())
                .map_err(|e| context!("column correlation not found", e))?;
        if !col_exprs.is_empty() {
            // it's correlated
            subqry_input = subqry_filter.input.clone();
            (outer_cols, subqry_cols, join_filters) =
                exprs_to_join_cols(&col_exprs, subqry_filter.input.schema(), false)
                    .map_err(|e| context!("column correlation not found", e))?;
            other_subqry_exprs = other_exprs;
        }
    }

    let (subqry_cols, outer_cols) =
        merge_cols((&[subquery_col], &subqry_cols), (&[outer_col], &outer_cols));

    // build subquery side of join - the thing the subquery was querying
    let subqry_alias = format!("__sq_{}", optimizer_config.next_id());
    let mut subqry_plan = LogicalPlanBuilder::from((*subqry_input).clone());
    if let Some(expr) = combine_filters(&other_subqry_exprs) {
        // if the subquery had additional expressions, restore them
        subqry_plan = subqry_plan.filter(expr)?
    }
    let projection = alias_cols(&subqry_cols);
    let subqry_plan = subqry_plan
        .project_with_alias(projection, Some(subqry_alias.clone()))?
        .build()?;
    debug!("subquery plan:\n{}", subqry_plan.display_indent());

    // qualify the join columns for outside the subquery
    let subqry_cols = swap_table(&subqry_alias, &subqry_cols);
    let join_keys = (outer_cols, subqry_cols);

    // join our sub query into the main plan
    let join_type = match query_info.negated {
        true => JoinType::Anti,
        false => JoinType::Semi,
    };
    let mut new_plan = LogicalPlanBuilder::from(outer_input.clone()).join(
        &subqry_plan,
        join_type,
        join_keys,
        join_filters,
    )?;
    if let Some(expr) = combine_filters(outer_other_exprs) {
        new_plan = new_plan.filter(expr)? // if the main query had additional expressions, restore them
    }
    let new_plan = new_plan.build()?;

    debug!("where in optimized:\n{}", new_plan.display_indent());
    Ok(new_plan)
}

struct SubqueryInfo {
    query: Subquery,
    where_in_expr: Expr,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, expr: Expr, negated: bool) -> Self {
        Self {
            query,
            where_in_expr: expr,
            negated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Result;
    use datafusion_expr::{
        col, in_subquery, lit, logical_plan::LogicalPlanBuilder, not_in_subquery,
    };
    use std::ops::Add;

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        let _ = env_logger::try_init();
    }

    /// Test multiple correlated subqueries
    /// See subqueries.rs where_in_multiple()
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("customer.c_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), orders.clone())
                    .and(in_subquery(col("customer.c_custkey"), orders)),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent());

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Semi Join: #customer.c_custkey = #__sq_2.o_custkey [c_custkey:Int64, c_name:Utf8]
    Semi Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
      TableScan: customer [c_custkey:Int64, c_name:Utf8]
      Projection: #orders.o_custkey AS o_custkey, alias=__sq_1 [o_custkey:Int64]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
    Projection: #orders.o_custkey AS o_custkey, alias=__sq_2 [o_custkey:Int64]
      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]"#;
        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test recursive correlated subqueries
    /// See subqueries.rs where_in_recursive()
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(col("lineitem.l_orderkey").eq(col("orders.o_orderkey")))?
                .project(vec![col("lineitem.l_orderkey")])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    in_subquery(col("orders.o_orderkey"), lineitem)
                        .and(col("orders.o_custkey").eq(col("customer.c_custkey"))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), orders))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Semi Join: #customer.c_custkey = #__sq_2.o_custkey [c_custkey:Int64, c_name:Utf8]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]
    Projection: #orders.o_custkey AS o_custkey, alias=__sq_2 [o_custkey:Int64]
      Semi Join: #orders.o_orderkey = #__sq_1.l_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        Projection: #lineitem.l_orderkey AS l_orderkey, alias=__sq_1 [l_orderkey:Int64]
          TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter with additional subquery filters
    #[test]
    fn in_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Semi Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]
    Projection: #orders.o_custkey AS o_custkey, alias=__sq_1 [o_custkey:Int64]
      Filter: #orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery with no columns in schema
    #[test]
    fn in_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("customer.c_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Query will fail, but we can still transform the plan
        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Semi Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]
    Projection: #orders.o_custkey AS o_custkey, alias=__sq_1 [o_custkey:Int64]
      Filter: #customer.c_custkey = #customer.c_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for IN subquery with both columns in schema
    #[test]
    fn in_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Semi Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]
    Projection: #orders.o_custkey AS o_custkey, alias=__sq_1 [o_custkey:Int64]
      Filter: #orders.o_custkey = #orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery not equal
    #[test]
    fn in_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").not_eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Semi Join: #customer.c_custkey = #__sq_1.o_custkey Filter: #customer.c_custkey != #orders.o_custkey [c_custkey:Int64, c_name:Utf8]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]
    Projection: #orders.o_custkey AS o_custkey, alias=__sq_1 [o_custkey:Int64]
      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery less than
    #[test]
    fn in_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").lt(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // can't optimize on arbitrary expressions (yet)
        assert_optimizer_err(
            &DecorrelateWhereIn::new(),
            &plan,
            "column correlation not found",
        );
        Ok(())
    }

    /// Test for correlated IN subquery filter with subquery disjunction
    #[test]
    fn in_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimizer_err(
            &DecorrelateWhereIn::new(),
            &plan,
            "Optimizing disjunctions not supported!",
        );
        Ok(())
    }

    /// Test for correlated IN without projection
    #[test]
    fn in_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Maybe okay if the table only has a single column?
        assert_optimizer_err(
            &DecorrelateWhereIn::new(),
            &plan,
            "a projection is required",
        );
        Ok(())
    }

    /// Test for correlated IN subquery join on expression
    #[test]
    fn in_subquery_join_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey").add(lit(1)), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // TODO: support join on expression
        assert_optimizer_err(
            &DecorrelateWhereIn::new(),
            &plan,
            "column comparison required",
        );
        Ok(())
    }

    /// Test for correlated IN expressions
    #[test]
    fn in_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // TODO: support join on expressions?
        assert_optimizer_err(
            &DecorrelateWhereIn::new(),
            &plan,
            "single column projection required",
        );
        Ok(())
    }

    /// Test for correlated IN subquery multiple projected columns
    #[test]
    fn in_subquery_multi_col() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey"), col("orders.o_orderkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), sq)
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimizer_err(
            &DecorrelateWhereIn::new(),
            &plan,
            "single expression projection required",
        );
        Ok(())
    }

    /// Test for correlated IN subquery filter with additional filters
    #[test]
    fn should_support_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), sq)
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Filter: #customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
    Semi Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
      TableScan: customer [c_custkey:Int64, c_name:Utf8]
      Projection: #orders.o_custkey AS o_custkey, alias=__sq_1 [o_custkey:Int64]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter with disjustions
    #[test]
    fn in_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), sq)
                    .or(col("customer.c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // TODO: support disjunction - for now expect unaltered plan
        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Filter: #customer.c_custkey IN (<subquery>) OR #customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
    Subquery: [o_custkey:Int64]
      Projection: #orders.o_custkey [o_custkey:Int64]
        Filter: #customer.c_custkey = #orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter
    #[test]
    fn in_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(col("test.a").eq(col("sq.a")))?
                .project(vec![col("c")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(in_subquery(col("c"), sq))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = r#"Projection: #test.b [b:UInt32]
  Semi Join: #test.c = #__sq_1.c, #test.a = #__sq_1.a [a:UInt32, b:UInt32, c:UInt32]
    TableScan: test [a:UInt32, b:UInt32, c:UInt32]
    Projection: #sq.c AS c, #sq.a AS a, alias=__sq_1 [c:UInt32, a:UInt32]
      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for single IN subquery filter
    #[test]
    fn in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = r#"Projection: #test.b [b:UInt32]
  Semi Join: #test.c = #__sq_1.c [a:UInt32, b:UInt32, c:UInt32]
    TableScan: test [a:UInt32, b:UInt32, c:UInt32]
    Projection: #sq.c AS c, alias=__sq_1 [c:UInt32]
      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }

    /// Test for single NOT IN subquery filter
    #[test]
    fn not_in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = r#"Projection: #test.b [b:UInt32]
  Anti Join: #test.c = #__sq_1.c [a:UInt32, b:UInt32, c:UInt32]
    TableScan: test [a:UInt32, b:UInt32, c:UInt32]
    Projection: #sq.c AS c, alias=__sq_1 [c:UInt32]
      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]"#;

        assert_optimized_plan_eq(&DecorrelateWhereIn::new(), &plan, expected);
        Ok(())
    }
}
