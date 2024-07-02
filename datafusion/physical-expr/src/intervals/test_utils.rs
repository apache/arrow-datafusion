// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Test utilities for the interval arithmetic library

use std::sync::Arc;

use crate::expressions::{binary, BinaryExpr, Literal};
use crate::PhysicalExpr;
use arrow_schema::Schema;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::Operator;

#[allow(clippy::too_many_arguments)]
/// This test function generates a conjunctive statement with two numeric
/// terms with the following form:
/// left_col (op_1) a  >/>= right_col (op_2) b AND left_col (op_3) c </<= right_col (op_4) d
pub fn gen_conjunctive_numerical_expr(
    left_col: Arc<dyn PhysicalExpr>,
    right_col: Arc<dyn PhysicalExpr>,
    op: (Operator, Operator, Operator, Operator),
    a: ScalarValue,
    b: ScalarValue,
    c: ScalarValue,
    d: ScalarValue,
    bounds: (Operator, Operator),
) -> Arc<dyn PhysicalExpr> {
    let (op_1, op_2, op_3, op_4) = op;
    let left_and_1 = Arc::new(BinaryExpr::new(
        Arc::clone(&left_col),
        op_1,
        Arc::new(Literal::new(a)),
    ));
    let left_and_2 = Arc::new(BinaryExpr::new(
        Arc::clone(&right_col),
        op_2,
        Arc::new(Literal::new(b)),
    ));
    let right_and_1 =
        Arc::new(BinaryExpr::new(left_col, op_3, Arc::new(Literal::new(c))));
    let right_and_2 =
        Arc::new(BinaryExpr::new(right_col, op_4, Arc::new(Literal::new(d))));
    let (greater_op, less_op) = bounds;

    let left_expr = Arc::new(BinaryExpr::new(left_and_1, greater_op, left_and_2));
    let right_expr = Arc::new(BinaryExpr::new(right_and_1, less_op, right_and_2));
    Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr))
}

#[allow(clippy::too_many_arguments)]
/// This test function generates a conjunctive statement with
/// two scalar values with the following form:
/// left_col (op_1) a  > right_col (op_2) b AND left_col (op_3) c < right_col (op_4) d
pub fn gen_conjunctive_temporal_expr(
    left_col: Arc<dyn PhysicalExpr>,
    right_col: Arc<dyn PhysicalExpr>,
    op_1: Operator,
    op_2: Operator,
    op_3: Operator,
    op_4: Operator,
    a: ScalarValue,
    b: ScalarValue,
    c: ScalarValue,
    d: ScalarValue,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let left_and_1 = binary(
        Arc::clone(&left_col),
        op_1,
        Arc::new(Literal::new(a)),
        schema,
    )?;
    let left_and_2 = binary(
        Arc::clone(&right_col),
        op_2,
        Arc::new(Literal::new(b)),
        schema,
    )?;
    let right_and_1 = binary(left_col, op_3, Arc::new(Literal::new(c)), schema)?;
    let right_and_2 = binary(right_col, op_4, Arc::new(Literal::new(d)), schema)?;
    let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::Gt, left_and_2));
    let right_expr = Arc::new(BinaryExpr::new(right_and_1, Operator::Lt, right_and_2));
    Ok(Arc::new(BinaryExpr::new(
        left_expr,
        Operator::And,
        right_expr,
    )))
}
