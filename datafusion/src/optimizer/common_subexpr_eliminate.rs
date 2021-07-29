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

//! Eliminate common sub-expression.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::error::Result;
use crate::execution::context::ExecutionProps;
use crate::logical_plan::{
    col, DFField, DFSchema, Expr, ExprRewriter, ExpressionVisitor, LogicalPlan,
    Recursion, RewriteRecursion,
};
use crate::optimizer::optimizer::OptimizerRule;

/// A map from expression's identifier to tuple including
/// - the expression itself (cloned)
/// - counter
/// - DataType of this expression.
type ExprSet = HashMap<Identifier, (Expr, usize, DataType)>;

/// Identifier type. Current implementation use describe of a expression (type String) as
/// Identifier.
///
/// A Identifier should (ideally) be able to "hash", "accumulate", "equal" and "have no
/// collision (as low as possible)"
///
/// Since a identifier is likely to be copied many times, it is better that a identifier
/// is small or "copy". otherwise some kinds of reference count is needed. String description
/// here is not such a good choose.
type Identifier = String;
/// Perform Common Sub-expression Elimination optimization.
///
/// Currently only common sub-expressions within one logical plan will
/// be eliminated.
pub struct CommonSubexprEliminate {}

impl OptimizerRule for CommonSubexprEliminate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        optimize(plan, execution_props)
    }

    fn name(&self) -> &str {
        "common_sub_expression_eliminate"
    }
}

impl CommonSubexprEliminate {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn optimize(plan: &LogicalPlan, execution_props: &ExecutionProps) -> Result<LogicalPlan> {
    let mut expr_set = ExprSet::new();
    let mut affected_id = HashSet::new();

    match plan {
        LogicalPlan::Projection {
            expr,
            input,
            schema: _,
        } => {
            let mut arrays = vec![];
            for e in expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(e, &mut expr_set, &mut id_array, data_type)?;
                arrays.push(id_array);
            }

            return optimize(input, execution_props);
        }
        LogicalPlan::Filter { predicate, input } => {
            let data_type = predicate.get_type(input.schema())?;
            let mut id_array = vec![];
            expr_to_identifier(predicate, &mut expr_set, &mut id_array, data_type)?;

            return optimize(input, execution_props);
        }
        LogicalPlan::Window {
            input,
            window_expr,
            schema: _,
        } => {
            let mut arrays = vec![];
            for e in window_expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(e, &mut expr_set, &mut id_array, data_type)?;
                arrays.push(id_array);
            }

            return optimize(input, execution_props);
        }
        LogicalPlan::Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        } => {
            // collect id
            let mut group_arrays = vec![];
            for e in group_expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(e, &mut expr_set, &mut id_array, data_type)?;
                group_arrays.push(id_array);
            }
            let mut aggr_arrays = vec![];
            for e in aggr_expr {
                let data_type = e.get_type(input.schema())?;
                let mut id_array = vec![];
                expr_to_identifier(e, &mut expr_set, &mut id_array, data_type)?;
                aggr_arrays.push(id_array);
            }

            // rewrite
            let new_group_expr = group_expr
                .iter()
                .cloned()
                .zip(group_arrays.into_iter())
                .map(|(expr, id_array)| {
                    replace_common_expr(expr, &id_array, &mut expr_set, &mut affected_id)
                })
                .collect::<Result<Vec<_>>>()?;
            let new_aggr_expr = aggr_expr
                .iter()
                .cloned()
                .zip(aggr_arrays.into_iter())
                .map(|(expr, id_array)| {
                    replace_common_expr(expr, &id_array, &mut expr_set, &mut affected_id)
                })
                .collect::<Result<Vec<_>>>()?;

            let mut new_input = optimize(input, execution_props)?;
            if !affected_id.is_empty() {
                new_input = build_project_plan(new_input, affected_id, &expr_set)?;
            }

            return Ok(LogicalPlan::Aggregate {
                input: Arc::new(new_input),
                group_expr: new_group_expr,
                aggr_expr: new_aggr_expr,
                schema: schema.clone(),
            });
        }
        LogicalPlan::Sort { expr: _, input: _ } => {}
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoin { .. }
        | LogicalPlan::Repartition { .. }
        | LogicalPlan::Union { .. }
        | LogicalPlan::TableScan { .. }
        | LogicalPlan::EmptyRelation { .. }
        | LogicalPlan::Limit { .. }
        | LogicalPlan::CreateExternalTable { .. }
        | LogicalPlan::Explain { .. }
        | LogicalPlan::Extension { .. } => {}
    }
    Ok(plan.clone())
}

fn build_project_plan(
    input: LogicalPlan,
    affected_id: HashSet<Identifier>,
    expr_set: &ExprSet,
) -> Result<LogicalPlan> {
    let mut project_exprs = vec![];
    let mut fields = vec![];

    for id in affected_id {
        let (expr, _, data_type) = expr_set.get(&id).unwrap();
        // todo: check `nullable`
        fields.push(DFField::new(None, &id, data_type.clone(), true));
        project_exprs.push(expr.clone());
    }

    let mut schema = DFSchema::new(fields)?;
    schema.merge(input.schema());

    Ok(LogicalPlan::Projection {
        expr: project_exprs,
        input: Arc::new(input),
        schema: Arc::new(schema),
    })
}

/// Go through an expression tree and generate identifier.
///
/// An identifier contains information of the expression itself and its sub-expression.
/// This visitor implementation use a stack `visit_stack` to track traversal, which
/// lets us know when a sub-tree's visiting is finished. When `pre_visit` is called
/// (traversing to a new node), an `EnterMark` and an `ExprItem` will be pushed into stack.
/// And try to pop out a `EnterMark` on leaving a node (`post_visit()`). All `ExprItem`
/// before the first `EnterMark` is considered to be sub-tree of the leaving node.
///
/// This visitor also records identifier in `id_array`. Makes the following traverse
/// pass can get the identifier of a node without recalculate it. We assign each node
/// in the expr tree a series number, start from 1, maintained by `series_number`.
/// Series number represents the order we left (`post_visit`) a node. Has the property
/// that child node's series number always smaller than parent's. While `id_array` is
/// organized in the order we enter (`pre_visit`) a node. `node_count` helps us to
/// get the index of `id_array` for each node.
///
/// `Expr` without sub-expr (column, literal etc.) will not have identifier
/// because they should not be recognized as common sub-expr.
struct ExprIdentifierVisitor<'a> {
    // param
    expr_set: &'a mut ExprSet,
    /// series number (usize) and identifier.
    id_array: &'a mut Vec<(usize, Identifier)>,
    data_type: DataType,

    // inner states
    visit_stack: Vec<VisitRecord>,
    /// increased in pre_visit, start from 0.
    node_count: usize,
    /// increased in post_visit, start from 1.
    series_number: usize,
}

/// Record item that used when traversing a expression tree.
enum VisitRecord {
    /// `usize` is the monotone increasing series number assigned in pre_visit().
    /// Starts from 0. Is used to index the identifier array `id_array` in post_visit().
    EnterMark(usize),
    /// Accumulated identifier of sub expression.
    ExprItem(Identifier),
}

impl ExprIdentifierVisitor<'_> {
    fn desc_expr(expr: &Expr) -> String {
        let mut desc = String::new();
        match expr {
            Expr::Column(column) => {
                desc.push_str("Column-");
                desc.push_str(&column.flat_name());
            }
            Expr::ScalarVariable(var_names) => {
                // accum.insert(Column::from_name(var_names.join(".")));
                desc.push_str("ScalarVariable-");
                desc.push_str(&var_names.join("."));
            }
            Expr::Alias(_, alias) => {
                desc.push_str("Alias-");
                desc.push_str(alias);
            }
            Expr::Literal(value) => {
                desc.push_str("Literal");
                desc.push_str(&value.to_string());
            }
            Expr::BinaryExpr { op, .. } => {
                desc.push_str("BinaryExpr-");
                desc.push_str(&op.to_string());
            }
            Expr::Not(_) => {}
            Expr::IsNotNull(_) => {}
            Expr::IsNull(_) => {}
            Expr::Negative(_) => {}
            Expr::Between { .. } => {}
            Expr::Case { .. } => {}
            Expr::Cast { .. } => {}
            Expr::TryCast { .. } => {}
            Expr::Sort { .. } => {}
            Expr::ScalarFunction { .. } => {}
            Expr::ScalarUDF { .. } => {}
            Expr::WindowFunction { .. } => {}
            Expr::AggregateFunction { fun, distinct, .. } => {
                desc.push_str("AggregateFunction-");
                desc.push_str(&fun.to_string());
                desc.push_str(&distinct.to_string());
            }
            Expr::AggregateUDF { .. } => {}
            Expr::InList { .. } => {}
            Expr::Wildcard => {}
        }

        desc
    }

    /// Find the first `EnterMark` in the stack, and accumulates every `ExprItem`
    /// before it.
    fn pop_enter_mark(&mut self) -> (usize, Identifier) {
        let mut desc = String::new();

        while let Some(item) = self.visit_stack.pop() {
            match item {
                VisitRecord::EnterMark(idx) => {
                    return (idx, desc);
                }
                VisitRecord::ExprItem(s) => {
                    desc.push_str(&s);
                }
            }
        }

        unreachable!("Enter mark should paired with node number");
    }
}

impl ExpressionVisitor for ExprIdentifierVisitor<'_> {
    fn pre_visit(mut self, _expr: &Expr) -> Result<Recursion<Self>> {
        self.visit_stack
            .push(VisitRecord::EnterMark(self.node_count));
        self.node_count += 1;
        // put placeholder
        self.id_array.push((0, "".to_string()));
        Ok(Recursion::Continue(self))
    }

    fn post_visit(mut self, expr: &Expr) -> Result<Self> {
        self.series_number += 1;

        let (idx, sub_expr_desc) = self.pop_enter_mark();
        // skip exprs should not be recognize.
        if matches!(
            expr,
            Expr::Literal(..)
                | Expr::Column(..)
                | Expr::ScalarVariable(..)
                | Expr::Wildcard
        ) {
            self.id_array[idx].0 = self.series_number;
            let desc = Self::desc_expr(expr);
            self.visit_stack.push(VisitRecord::ExprItem(desc));
            return Ok(self);
        }
        let mut desc = Self::desc_expr(expr);
        desc.push_str(&sub_expr_desc);

        self.id_array[idx] = (self.series_number, desc.clone());
        self.visit_stack.push(VisitRecord::ExprItem(desc.clone()));
        let data_type = self.data_type.clone();
        self.expr_set
            .entry(desc.clone())
            .or_insert_with(|| (expr.clone(), 0, data_type))
            .1 += 1;
        Ok(self)
    }
}

/// Go through an expression tree and generate identifier for every node in this tree.
fn expr_to_identifier(
    expr: &Expr,
    expr_set: &mut ExprSet,
    id_array: &mut Vec<(usize, Identifier)>,
    data_type: DataType,
) -> Result<()> {
    expr.accept(ExprIdentifierVisitor {
        expr_set,
        id_array,
        data_type,
        visit_stack: vec![],
        node_count: 0,
        series_number: 0,
    })?;

    Ok(())
}

/// Rewrite expression by replacing detected common sub-expression with
/// the corresponding temporary column name. That column contains the
/// evaluate result of replaced expression.
struct CommonSubexprRewriter<'a> {
    expr_set: &'a mut ExprSet,
    id_array: &'a [(usize, Identifier)],
    /// Which identifier is replaced.
    affected_id: &'a mut HashSet<Identifier>,

    /// the max series number we have rewritten. Other expression nodes
    /// with smaller series number is already replaced and shouldn't
    /// do anything with them.
    max_series_number: usize,
    /// current node's information's index in `id_array`.
    curr_index: usize,
}

impl ExprRewriter for CommonSubexprRewriter<'_> {
    fn pre_visit(&mut self, _: &Expr) -> Result<RewriteRecursion> {
        if self.curr_index >= self.id_array.len()
            || self.max_series_number > self.id_array[self.curr_index].0
        {
            return Ok(RewriteRecursion::Stop);
        }

        let curr_id = &self.id_array[self.curr_index].1;
        // skip `Expr`s without identifier (empty identifier).
        if curr_id.is_empty() {
            self.curr_index += 1;
            return Ok(RewriteRecursion::Continue);
        }
        let (_, counter, _) = self.expr_set.get(curr_id).unwrap();
        if *counter > 1 {
            self.affected_id.insert(curr_id.clone());
            Ok(RewriteRecursion::Mutate)
        } else {
            self.curr_index += 1;
            Ok(RewriteRecursion::Continue)
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if self.curr_index >= self.id_array.len() {
            return Ok(expr);
        }

        let (series_number, id) = &self.id_array[self.curr_index];
        if *series_number < self.max_series_number || id.is_empty() {
            return Ok(expr);
        }
        self.max_series_number = *series_number;

        // step index, skip all sub-node (which has smaller series number).
        self.curr_index += 1;
        while self.curr_index < self.id_array.len()
            && *series_number > self.id_array[self.curr_index].0
        {
            self.curr_index += 1;
        }

        Ok(col(id))
    }
}

fn replace_common_expr(
    expr: Expr,
    id_array: &[(usize, Identifier)],
    expr_set: &mut ExprSet,
    affected_id: &mut HashSet<Identifier>,
) -> Result<Expr> {
    expr.rewrite(&mut CommonSubexprRewriter {
        expr_set,
        id_array,
        affected_id,
        max_series_number: 0,
        curr_index: 0,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::logical_plan::{binary_expr, col, lit, sum, LogicalPlanBuilder, Operator};
    use crate::test::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimizer = CommonSubexprEliminate {};
        let optimized_plan = optimizer
            .optimize(plan, &ExecutionProps::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
    }

    #[test]
    fn dev_driver_tpch_q1_simplified() -> Result<()> {
        // SQL:
        //  select
        //      sum(a * (1 - b)),
        //      sum(a * (1 - b) * (1 + c))
        //  from T

        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![],
                vec![
                    sum(binary_expr(
                        col("a"),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Minus, col("b")),
                    )),
                    sum(binary_expr(
                        binary_expr(
                            col("a"),
                            Operator::Multiply,
                            binary_expr(lit(1), Operator::Minus, col("b")),
                        ),
                        Operator::Multiply,
                        binary_expr(lit(1), Operator::Plus, col("c")),
                    )),
                ],
            )?
            .build()?;

        let expected = "Aggregate: groupBy=[[]], aggr=[[SUM(#BinaryExpr-*BinaryExpr--Column-test.bLiteral1Column-test.a), SUM(#BinaryExpr-*BinaryExpr--Column-test.bLiteral1Column-test.a Multiply Int32(1) Plus #test.c)]]\
        \n  Projection: #test.a Multiply Int32(1) Minus #test.b\
        \n    TableScan: test projection=None";

        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
