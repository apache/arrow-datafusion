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

use std::sync::Arc;

use crate::expressions::Column;
use crate::PhysicalExpr;

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;

/// Stores the mapping between source expressions and target expressions for a
/// projection.
#[derive(Debug, Clone)]
pub struct ProjectionMapping {
    /// Mapping between source expressions and target expressions.
    /// Vector indices correspond to the indices after projection.
    pub map: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)>,
}

impl ProjectionMapping {
    /// Constructs the mapping between a projection's input and output
    /// expressions.
    ///
    /// For example, given the input projection expressions (`a + b`, `c + d`)
    /// and an output schema with two columns `"c + d"` and `"a + b"`, the
    /// projection mapping would be:
    ///
    /// ```text
    ///  [0]: (c + d, col("c + d"))
    ///  [1]: (a + b, col("a + b"))
    /// ```
    ///
    /// where `col("c + d")` means the column named `"c + d"`.
    pub fn try_new(
        expr: &[(Arc<dyn PhysicalExpr>, String)],
        input_schema: &SchemaRef,
    ) -> Result<Self> {
        // Construct a map from the input expressions to the output expression of the projection:
        expr.iter()
            .enumerate()
            .map(|(expr_idx, (expression, name))| {
                let target_expr = Arc::new(Column::new(name, expr_idx)) as _;
                expression
                    .clone()
                    .transform_down_old(&|e| match e.as_any().downcast_ref::<Column>() {
                        Some(col) => {
                            // Sometimes, an expression and its name in the input_schema
                            // doesn't match. This can cause problems, so we make sure
                            // that the expression name matches with the name in `input_schema`.
                            // Conceptually, `source_expr` and `expression` should be the same.
                            let idx = col.index();
                            let matching_input_field = input_schema.field(idx);
                            let matching_input_column =
                                Column::new(matching_input_field.name(), idx);
                            Ok(Transformed::Yes(Arc::new(matching_input_column)))
                        }
                        None => Ok(Transformed::No(e)),
                    })
                    .map(|source_expr| (source_expr, target_expr))
            })
            .collect::<Result<Vec<_>>>()
            .map(|map| Self { map })
    }

    /// Iterate over pairs of (source, target) expressions
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = &(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> + '_ {
        self.map.iter()
    }

    /// This function returns the target expression for a given source expression.
    ///
    /// # Arguments
    ///
    /// * `expr` - Source physical expression.
    ///
    /// # Returns
    ///
    /// An `Option` containing the target for the given source expression,
    /// where a `None` value means that `expr` is not inside the mapping.
    pub fn target_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        self.map
            .iter()
            .find(|(source, _)| source.eq(expr))
            .map(|(_, target)| target.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::equivalence::tests::{
        apply_projection, convert_to_orderings, convert_to_orderings_owned,
        create_random_schema, generate_table_for_eq_properties, is_table_same_after_sort,
        output_schema,
    };
    use crate::equivalence::EquivalenceProperties;
    use crate::execution_props::ExecutionProps;
    use crate::expressions::{col, BinaryExpr, Literal};
    use crate::functions::create_physical_expr;
    use crate::PhysicalSortExpr;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::{SortOptions, TimeUnit};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{BuiltinScalarFunction, Operator};
    use itertools::Itertools;
    use std::sync::Arc;

    #[test]
    fn project_orderings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_ts = &col("ts", &schema)?;
        let interval = Arc::new(Literal::new(ScalarValue::IntervalDayTime(Some(2))))
            as Arc<dyn PhysicalExpr>;
        let date_bin_func = &create_physical_expr(
            &BuiltinScalarFunction::DateBin,
            &[interval, col_ts.clone()],
            &schema,
            &ExecutionProps::default(),
        )?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_d = Arc::new(BinaryExpr::new(
            col_b.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let b_plus_e = Arc::new(BinaryExpr::new(
            col_b.clone(),
            Operator::Plus,
            col_e.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let c_plus_d = Arc::new(BinaryExpr::new(
            col_c.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
                // projection exprs
                vec![(col_b, "b_new".to_string()), (col_a, "a_new".to_string())],
                // expected
                vec![
                    // [b_new ASC]
                    vec![("b_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // empty ordering
                ],
                // projection exprs
                vec![(col_c, "c_new".to_string()), (col_b, "b_new".to_string())],
                // expected
                vec![
                    // no ordering at the output
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [ts ASC]
                    vec![(col_ts, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_ts, "ts_new".to_string()),
                    (date_bin_func, "date_bin_res".to_string()),
                ],
                // expected
                vec![
                    // [date_bin_res ASC]
                    vec![("date_bin_res", option_asc)],
                    // [ts_new ASC]
                    vec![("ts_new", option_asc)],
                ],
            ),
            // ---------- TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC]
                    vec![(col_a, option_asc), (col_ts, option_asc)],
                    // [b ASC, ts ASC]
                    vec![(col_b, option_asc), (col_ts, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_ts, "ts_new".to_string()),
                    (date_bin_func, "date_bin_res".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, ts_new ASC]
                    vec![("a_new", option_asc), ("ts_new", option_asc)],
                    // [a_new ASC, date_bin_res ASC]
                    vec![("a_new", option_asc), ("date_bin_res", option_asc)],
                    // [b_new ASC, ts_new ASC]
                    vec![("b_new", option_asc), ("ts_new", option_asc)],
                    // [b_new ASC, date_bin_res ASC]
                    vec![("b_new", option_asc), ("date_bin_res", option_asc)],
                ],
            ),
            // ---------- TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a + b ASC]
                    vec![(&a_plus_b, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a + b ASC]
                    vec![("a+b", option_asc)],
                ],
            ),
            // ---------- TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [a + b ASC, c ASC]
                    vec![(&a_plus_b, option_asc), (&col_c, option_asc)],
                ],
                // projection exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_c, "c_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a + b ASC, c_new ASC]
                    vec![("a+b", option_asc), ("c_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 7 ----------
            (
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // b as b_new, a as a_new, d as d_new b+d
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, d_new ASC]
                    vec![("a_new", option_asc), ("d_new", option_asc)],
                    // [a_new ASC, b+d ASC]
                    vec![("a_new", option_asc), ("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 8 ----------
            (
                // orderings
                vec![
                    // [b+d ASC]
                    vec![(&b_plus_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [b+d ASC]
                    vec![("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 9 ----------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_d, option_asc),
                        (col_b, option_asc),
                    ],
                    // [c ASC]
                    vec![(col_c, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (col_c, "c_new".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("d_new", option_asc),
                        ("b_new", option_asc),
                    ],
                    // [c_new ASC],
                    vec![("c_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 10 ----------
            (
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_c, "c_new".to_string()),
                    (&c_plus_d, "c+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c_new", option_asc),
                    ],
                    // [a_new ASC, b_new ASC, c+d ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c+d", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 11 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, d ASC]
                    vec![(col_a, option_asc), (col_d, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&b_plus_d, "b+d".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, b + d ASC]
                    vec![("a_new", option_asc), ("b+d", option_asc)],
                ],
            ),
            // ------- TEST CASE 12 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // proj exprs
                vec![(col_c, "c_new".to_string()), (col_a, "a_new".to_string())],
                // expected
                vec![
                    // [a_new ASC]
                    vec![("a_new", option_asc)],
                ],
            ),
            // ------- TEST CASE 13 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC, a + b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (&a_plus_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_b, "b_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("b_new", option_asc),
                        ("c_new", option_asc),
                    ],
                    // [a_new ASC, a+b ASC, c_new ASC]
                    vec![
                        ("a_new", option_asc),
                        ("a+b", option_asc),
                        ("c_new", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 14 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [c ASC, b ASC]
                    vec![(col_c, option_asc), (col_b, option_asc)],
                    // [d ASC, e ASC]
                    vec![(col_d, option_asc), (col_e, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_d, "d_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&b_plus_e, "b+e".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("a_new", option_asc),
                        ("d_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [d_new ASC, a_new ASC, b+e ASC]
                    vec![
                        ("d_new", option_asc),
                        ("a_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [c_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("c_new", option_asc),
                        ("d_new", option_asc),
                        ("b+e", option_asc),
                    ],
                    // [d_new ASC, c_new ASC, b+e ASC]
                    vec![
                        ("d_new", option_asc),
                        ("c_new", option_asc),
                        ("b+e", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 15 ----------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_c, option_asc),
                        (&col_b, option_asc),
                    ],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (&a_plus_b, "a+b".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, d_new ASC, b+e ASC]
                    vec![
                        ("a_new", option_asc),
                        ("c_new", option_asc),
                        ("a+b", option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 16 ----------
            (
                // orderings
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [c ASC, b DESC]
                    vec![(col_c, option_asc), (col_b, option_desc)],
                    // [e ASC]
                    vec![(col_e, option_asc)],
                ],
                // proj exprs
                vec![
                    (col_c, "c_new".to_string()),
                    (col_a, "a_new".to_string()),
                    (col_b, "b_new".to_string()),
                    (&b_plus_e, "b+e".to_string()),
                ],
                // expected
                vec![
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b_new", option_asc)],
                    // [a_new ASC, b_new ASC]
                    vec![("a_new", option_asc), ("b+e", option_asc)],
                    // [c_new ASC, b_new DESC]
                    vec![("c_new", option_asc), ("b_new", option_desc)],
                ],
            ),
        ];

        for (idx, (orderings, proj_exprs, expected)) in test_cases.into_iter().enumerate()
        {
            let mut eq_properties = EquivalenceProperties::new(schema.clone());

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_new_orderings(orderings);

            let proj_exprs = proj_exprs
                .into_iter()
                .map(|(expr, name)| (expr.clone(), name))
                .collect::<Vec<_>>();
            let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &schema)?;
            let output_schema = output_schema(&projection_mapping, &schema)?;

            let expected = expected
                .into_iter()
                .map(|ordering| {
                    ordering
                        .into_iter()
                        .map(|(name, options)| {
                            (col(name, &output_schema).unwrap(), options)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let expected = convert_to_orderings_owned(&expected);

            let projected_eq = eq_properties.project(&projection_mapping, output_schema);
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test_idx: {:?}, actual: {:?}, expected: {:?}, projection_mapping: {:?}",
                idx, orderings.orderings, expected, projection_mapping
            );

            assert_eq!(orderings.len(), expected.len(), "{}", err_msg);
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }

    #[test]
    fn project_orderings2() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_ts = &col("ts", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
        )) as Arc<dyn PhysicalExpr>;
        let interval = Arc::new(Literal::new(ScalarValue::IntervalDayTime(Some(2))))
            as Arc<dyn PhysicalExpr>;
        let date_bin_ts = &create_physical_expr(
            &BuiltinScalarFunction::DateBin,
            &[interval, col_ts.clone()],
            &schema,
            &ExecutionProps::default(),
        )?;

        let round_c = &create_physical_expr(
            &BuiltinScalarFunction::Round,
            &[col_c.clone()],
            &schema,
            &ExecutionProps::default(),
        )?;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_b, "b_new".to_string()),
            (col_a, "a_new".to_string()),
            (col_c, "c_new".to_string()),
            (date_bin_ts, "date_bin_res".to_string()),
            (round_c, "round_c_res".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (expr.clone(), name))
            .collect::<Vec<_>>();
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_new = &col("a_new", &output_schema)?;
        let col_b_new = &col("b_new", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_date_bin_res = &col("date_bin_res", &output_schema)?;
        let col_round_c_res = &col("round_c_res", &output_schema)?;
        let a_new_plus_b_new = Arc::new(BinaryExpr::new(
            col_a_new.clone(),
            Operator::Plus,
            col_b_new.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [a ASC]
                    vec![(col_a, option_asc)],
                ],
                // expected
                vec![
                    // [b_new ASC]
                    vec![(col_a_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [a+b ASC]
                    vec![(&a_plus_b, option_asc)],
                ],
                // expected
                vec![
                    // [b_new ASC]
                    vec![(&a_new_plus_b_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC]
                    vec![(col_a, option_asc), (col_ts, option_asc)],
                ],
                // expected
                vec![
                    // [a_new ASC, date_bin_res ASC]
                    vec![(col_a_new, option_asc), (col_date_bin_res, option_asc)],
                ],
            ),
            // ---------- TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, ts ASC, b ASC]
                    vec![
                        (col_a, option_asc),
                        (col_ts, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // expected
                vec![
                    // [a_new ASC, date_bin_res ASC]
                    // Please note that result is not [a_new ASC, date_bin_res ASC, b_new ASC]
                    // because, datebin_res may not be 1-1 function. Hence without introducing ts
                    // dependency we cannot guarantee any ordering after date_bin_res column.
                    vec![(col_a_new, option_asc), (col_date_bin_res, option_asc)],
                ],
            ),
            // ---------- TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
                // expected
                vec![
                    // [a_new ASC, round_c_res ASC, c_new ASC]
                    vec![(col_a_new, option_asc), (col_round_c_res, option_asc)],
                    // [a_new ASC, c_new ASC]
                    vec![(col_a_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [c ASC, b ASC]
                    vec![(col_c, option_asc), (col_b, option_asc)],
                ],
                // expected
                vec![
                    // [round_c_res ASC]
                    vec![(col_round_c_res, option_asc)],
                    // [c_new ASC, b_new ASC]
                    vec![(col_c_new, option_asc), (col_b_new, option_asc)],
                ],
            ),
            // ---------- TEST CASE 7 ------------
            (
                // orderings
                vec![
                    // [a+b ASC, c ASC]
                    vec![(&a_plus_b, option_asc), (col_c, option_asc)],
                ],
                // expected
                vec![
                    // [a+b ASC, round(c) ASC, c_new ASC]
                    vec![
                        (&a_new_plus_b_new, option_asc),
                        (&col_round_c_res, option_asc),
                    ],
                    // [a+b ASC, c_new ASC]
                    vec![(&a_new_plus_b_new, option_asc), (col_c_new, option_asc)],
                ],
            ),
        ];

        for (idx, (orderings, expected)) in test_cases.iter().enumerate() {
            let mut eq_properties = EquivalenceProperties::new(schema.clone());

            let orderings = convert_to_orderings(orderings);
            eq_properties.add_new_orderings(orderings);

            let expected = convert_to_orderings(expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, output_schema.clone());
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "test idx: {:?}, actual: {:?}, expected: {:?}, projection_mapping: {:?}",
                idx, orderings.orderings, expected, projection_mapping
            );

            assert_eq!(orderings.len(), expected.len(), "{}", err_msg);
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }
        Ok(())
    }

    #[test]
    fn project_orderings3() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("f", DataType::Int32, true),
        ]));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;
        let col_f = &col("f", &schema)?;
        let a_plus_b = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_b.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let proj_exprs = vec![
            (col_c, "c_new".to_string()),
            (col_d, "d_new".to_string()),
            (&a_plus_b, "a+b".to_string()),
        ];
        let proj_exprs = proj_exprs
            .into_iter()
            .map(|(expr, name)| (expr.clone(), name))
            .collect::<Vec<_>>();
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &schema)?;
        let output_schema = output_schema(&projection_mapping, &schema)?;

        let col_a_plus_b_new = &col("a+b", &output_schema)?;
        let col_c_new = &col("c_new", &output_schema)?;
        let col_d_new = &col("d_new", &output_schema)?;

        let test_cases = vec![
            // ---------- TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                ],
                // equal conditions
                vec![],
                // expected
                vec![
                    // [d_new ASC, c_new ASC, a+b ASC]
                    vec![
                        (col_d_new, option_asc),
                        (col_c_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                    // [c_new ASC, d_new ASC, a+b ASC]
                    vec![
                        (col_c_new, option_asc),
                        (col_d_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                ],
            ),
            // ---------- TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, e ASC], Please note that a=e
                    vec![(col_c, option_asc), (col_e, option_asc)],
                ],
                // equal conditions
                vec![(col_e, col_a)],
                // expected
                vec![
                    // [d_new ASC, c_new ASC, a+b ASC]
                    vec![
                        (col_d_new, option_asc),
                        (col_c_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                    // [c_new ASC, d_new ASC, a+b ASC]
                    vec![
                        (col_c_new, option_asc),
                        (col_d_new, option_asc),
                        (col_a_plus_b_new, option_asc),
                    ],
                ],
            ),
            // ---------- TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, option_asc), (col_b, option_asc)],
                    // [c ASC, e ASC], Please note that a=f
                    vec![(col_c, option_asc), (col_e, option_asc)],
                ],
                // equal conditions
                vec![(col_a, col_f)],
                // expected
                vec![
                    // [d_new ASC]
                    vec![(col_d_new, option_asc)],
                    // [c_new ASC]
                    vec![(col_c_new, option_asc)],
                ],
            ),
        ];
        for (orderings, equal_columns, expected) in test_cases {
            let mut eq_properties = EquivalenceProperties::new(schema.clone());
            for (lhs, rhs) in equal_columns {
                eq_properties.add_equal_conditions(lhs, rhs);
            }

            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_new_orderings(orderings);

            let expected = convert_to_orderings(&expected);

            let projected_eq =
                eq_properties.project(&projection_mapping, output_schema.clone());
            let orderings = projected_eq.oeq_class();

            let err_msg = format!(
                "actual: {:?}, expected: {:?}, projection_mapping: {:?}",
                orderings.orderings, expected, projection_mapping
            );

            assert_eq!(orderings.len(), expected.len(), "{}", err_msg);
            for expected_ordering in &expected {
                assert!(orderings.contains(expected_ordering), "{}", err_msg)
            }
        }

        Ok(())
    }

    #[test]
    fn project_orderings_random() -> Result<()> {
        const N_RANDOM_SCHEMA: usize = 20;
        const N_ELEMENTS: usize = 125;
        const N_DISTINCT: usize = 5;

        for seed in 0..N_RANDOM_SCHEMA {
            // Create a random schema with random properties
            let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
            // Generate a data that satisfies properties given
            let table_data_with_properties =
                generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;
            // Floor(a)
            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
            )?;
            // a + b
            let a_plus_b = Arc::new(BinaryExpr::new(
                col("a", &test_schema)?,
                Operator::Plus,
                col("b", &test_schema)?,
            )) as Arc<dyn PhysicalExpr>;
            let proj_exprs = vec![
                (col("a", &test_schema)?, "a_new"),
                (col("b", &test_schema)?, "b_new"),
                (col("c", &test_schema)?, "c_new"),
                (col("d", &test_schema)?, "d_new"),
                (col("e", &test_schema)?, "e_new"),
                (col("f", &test_schema)?, "f_new"),
                (floor_a, "floor(a)"),
                (a_plus_b, "a+b"),
            ];

            for n_req in 0..=proj_exprs.len() {
                for proj_exprs in proj_exprs.iter().combinations(n_req) {
                    let proj_exprs = proj_exprs
                        .into_iter()
                        .map(|(expr, name)| (expr.clone(), name.to_string()))
                        .collect::<Vec<_>>();
                    let (projected_batch, projected_eq) = apply_projection(
                        proj_exprs.clone(),
                        &table_data_with_properties,
                        &eq_properties,
                    )?;

                    // Make sure each ordering after projection is valid.
                    for ordering in projected_eq.oeq_class().iter() {
                        let err_msg = format!(
                            "Error in test case ordering:{:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}, proj_exprs: {:?}",
                            ordering, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants, proj_exprs
                        );
                        // Since ordered section satisfies schema, we expect
                        // that result will be same after sort (e.g sort was unnecessary).
                        assert!(
                            is_table_same_after_sort(
                                ordering.clone(),
                                projected_batch.clone(),
                            )?,
                            "{}",
                            err_msg
                        );
                    }
                }
            }
        }

        Ok(())
    }

    #[test]
    fn ordering_satisfy_after_projection_random() -> Result<()> {
        const N_RANDOM_SCHEMA: usize = 20;
        const N_ELEMENTS: usize = 125;
        const N_DISTINCT: usize = 5;
        const SORT_OPTIONS: SortOptions = SortOptions {
            descending: false,
            nulls_first: false,
        };

        for seed in 0..N_RANDOM_SCHEMA {
            // Create a random schema with random properties
            let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
            // Generate a data that satisfies properties given
            let table_data_with_properties =
                generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;
            // Floor(a)
            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
            )?;
            // a + b
            let a_plus_b = Arc::new(BinaryExpr::new(
                col("a", &test_schema)?,
                Operator::Plus,
                col("b", &test_schema)?,
            )) as Arc<dyn PhysicalExpr>;
            let proj_exprs = vec![
                (col("a", &test_schema)?, "a_new"),
                (col("b", &test_schema)?, "b_new"),
                (col("c", &test_schema)?, "c_new"),
                (col("d", &test_schema)?, "d_new"),
                (col("e", &test_schema)?, "e_new"),
                (col("f", &test_schema)?, "f_new"),
                (floor_a, "floor(a)"),
                (a_plus_b, "a+b"),
            ];

            for n_req in 0..=proj_exprs.len() {
                for proj_exprs in proj_exprs.iter().combinations(n_req) {
                    let proj_exprs = proj_exprs
                        .into_iter()
                        .map(|(expr, name)| (expr.clone(), name.to_string()))
                        .collect::<Vec<_>>();
                    let (projected_batch, projected_eq) = apply_projection(
                        proj_exprs.clone(),
                        &table_data_with_properties,
                        &eq_properties,
                    )?;

                    let projection_mapping =
                        ProjectionMapping::try_new(&proj_exprs, &test_schema)?;

                    let projected_exprs = projection_mapping
                        .iter()
                        .map(|(_source, target)| target.clone())
                        .collect::<Vec<_>>();

                    for n_req in 0..=projected_exprs.len() {
                        for exprs in projected_exprs.iter().combinations(n_req) {
                            let requirement = exprs
                                .into_iter()
                                .map(|expr| PhysicalSortExpr {
                                    expr: expr.clone(),
                                    options: SORT_OPTIONS,
                                })
                                .collect::<Vec<_>>();
                            let expected = is_table_same_after_sort(
                                requirement.clone(),
                                projected_batch.clone(),
                            )?;
                            let err_msg = format!(
                                "Error in test case requirement:{:?}, expected: {:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}, projected_eq.oeq_class: {:?}, projected_eq.eq_group: {:?}, projected_eq.constants: {:?}, projection_mapping: {:?}",
                                requirement, expected, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants, projected_eq.oeq_class, projected_eq.eq_group, projected_eq.constants, projection_mapping
                            );
                            // Check whether ordering_satisfy API result and
                            // experimental result matches.
                            assert_eq!(
                                projected_eq.ordering_satisfy(&requirement),
                                expected,
                                "{}",
                                err_msg
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
