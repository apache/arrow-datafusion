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

use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::izip;

pub use datafusion_physical_expr_common::physical_expr::down_cast_any_ref;

/// Shared [`PhysicalExpr`].
pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;

/// This function is similar to the `contains` method of `Vec`. It finds
/// whether `expr` is among `physical_exprs`.
pub fn physical_exprs_contains(
    physical_exprs: &[Arc<dyn PhysicalExpr>],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    physical_exprs
        .iter()
        .any(|physical_expr| physical_expr.eq(expr))
}

/// Checks whether the given physical expression slices are equal.
pub fn physical_exprs_equal(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    lhs.len() == rhs.len() && izip!(lhs, rhs).all(|(lhs, rhs)| lhs.eq(rhs))
}

/// Checks whether the given physical expression slices are equal in the sense
/// of bags (multi-sets), disregarding their orderings.
pub fn physical_exprs_bag_equal(
    lhs: &[Arc<dyn PhysicalExpr>],
    rhs: &[Arc<dyn PhysicalExpr>],
) -> bool {
    // TODO: Once we can use `HashMap`s with `Arc<dyn PhysicalExpr>`, this
    //       function should use a `HashMap` to reduce computational complexity.
    if lhs.len() == rhs.len() {
        let mut rhs_vec = rhs.to_vec();
        for expr in lhs {
            if let Some(idx) = rhs_vec.iter().position(|e| expr.eq(e)) {
                rhs_vec.swap_remove(idx);
            } else {
                return false;
            }
        }
        true
    } else {
        false
    }
}

/// This utility function removes duplicates from the given `exprs` vector.
/// Note that this function does not necessarily preserve its input ordering.
pub fn deduplicate_physical_exprs(exprs: &mut Vec<Arc<dyn PhysicalExpr>>) {
    // TODO: Once we can use `HashSet`s with `Arc<dyn PhysicalExpr>`, this
    //       function should use a `HashSet` to reduce computational complexity.
    // See issue: https://github.com/apache/datafusion/issues/8027
    let mut idx = 0;
    while idx < exprs.len() {
        let mut rest_idx = idx + 1;
        while rest_idx < exprs.len() {
            if exprs[idx].eq(&exprs[rest_idx]) {
                exprs.swap_remove(rest_idx);
            } else {
                rest_idx += 1;
            }
        }
        idx += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expressions::{Column, Literal};
    use crate::physical_expr::{
        deduplicate_physical_exprs, physical_exprs_bag_equal, physical_exprs_contains,
        physical_exprs_equal, PhysicalExpr,
    };

    use datafusion_common::ScalarValue;

    #[test]
    fn test_physical_exprs_contains() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit4 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(4)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_a_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;

        // lit(true), lit(false), lit(4), lit(2), Col(a), Col(b)
        let physical_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::clone(&lit_true),
            Arc::clone(&lit_false),
            Arc::clone(&lit4),
            Arc::clone(&lit2),
            Arc::clone(&col_a_expr),
            Arc::clone(&col_b_expr),
        ];
        // below expressions are inside physical_exprs
        assert!(physical_exprs_contains(&physical_exprs, &lit_true));
        assert!(physical_exprs_contains(&physical_exprs, &lit2));
        assert!(physical_exprs_contains(&physical_exprs, &col_b_expr));

        // below expressions are not inside physical_exprs
        assert!(!physical_exprs_contains(&physical_exprs, &col_c_expr));
        assert!(!physical_exprs_contains(&physical_exprs, &lit1));
    }

    #[test]
    fn test_physical_exprs_equal() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let vec1 = vec![Arc::clone(&lit_true), Arc::clone(&lit_false)];
        let vec2 = vec![Arc::clone(&lit_true), Arc::clone(&col_b_expr)];
        let vec3 = vec![Arc::clone(&lit2), Arc::clone(&lit1)];
        let vec4 = vec![Arc::clone(&lit_true), Arc::clone(&lit_false)];

        // these vectors are same
        assert!(physical_exprs_equal(&vec1, &vec1));
        assert!(physical_exprs_equal(&vec1, &vec4));
        assert!(physical_exprs_bag_equal(&vec1, &vec1));
        assert!(physical_exprs_bag_equal(&vec1, &vec4));

        // these vectors are different
        assert!(!physical_exprs_equal(&vec1, &vec2));
        assert!(!physical_exprs_equal(&vec1, &vec3));
        assert!(!physical_exprs_bag_equal(&vec1, &vec2));
        assert!(!physical_exprs_bag_equal(&vec1, &vec3));
    }

    #[test]
    fn test_physical_exprs_set_equal() {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(!physical_exprs_bag_equal(
            list1.as_slice(),
            list2.as_slice()
        ));
        assert!(!physical_exprs_bag_equal(
            list2.as_slice(),
            list1.as_slice()
        ));
        assert!(!physical_exprs_equal(list1.as_slice(), list2.as_slice()));
        assert!(!physical_exprs_equal(list2.as_slice(), list1.as_slice()));

        let list3: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
        ];
        let list4: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert!(physical_exprs_bag_equal(list3.as_slice(), list4.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list3.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list4.as_slice()));
        assert!(!physical_exprs_equal(list3.as_slice(), list4.as_slice()));
        assert!(!physical_exprs_equal(list4.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list3.as_slice(), list3.as_slice()));
        assert!(physical_exprs_bag_equal(list4.as_slice(), list4.as_slice()));
    }

    #[test]
    fn test_deduplicate_physical_exprs() {
        let lit_true = &(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>);
        let lit_false = &(Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>);
        let lit4 = &(Arc::new(Literal::new(ScalarValue::Int32(Some(4))))
            as Arc<dyn PhysicalExpr>);
        let lit2 = &(Arc::new(Literal::new(ScalarValue::Int32(Some(2))))
            as Arc<dyn PhysicalExpr>);
        let col_a_expr = &(Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>);
        let col_b_expr = &(Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>);

        // First vector in the tuple is arguments, second one is the expected value.
        let test_cases = vec![
            // ---------- TEST CASE 1----------//
            (
                vec![
                    lit_true, lit_false, lit4, lit2, col_a_expr, col_a_expr, col_b_expr,
                    lit_true, lit2,
                ],
                vec![lit_true, lit_false, lit4, lit2, col_a_expr, col_b_expr],
            ),
            // ---------- TEST CASE 2----------//
            (
                vec![lit_true, lit_true, lit_false, lit4],
                vec![lit_true, lit4, lit_false],
            ),
        ];
        for (exprs, expected) in test_cases {
            let mut exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = expected.into_iter().cloned().collect::<Vec<_>>();
            deduplicate_physical_exprs(&mut exprs);
            assert!(physical_exprs_equal(&exprs, &expected));
        }
    }
}
