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

use arrow::array::{make_array, Array, ArrayRef, BooleanArray, MutableArrayData};
use arrow::compute::{and_kleene, is_not_null, SlicesIterator};
use arrow::datatypes::Schema;

use datafusion_common::{exec_err, Result};
use datafusion_expr::expr::Alias;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::Expr;

use crate::expressions::literal::Literal;
use crate::expressions::{self, CastExpr};
use crate::physical_expr::PhysicalExpr;
use crate::sort_expr::PhysicalSortExpr;
use crate::tree_node::ExprContext;

/// Represents a [`PhysicalExpr`] node with associated properties (order and
/// range) in a context where properties are tracked.
pub type ExprPropertiesNode = ExprContext<ExprProperties>;

impl ExprPropertiesNode {
    /// Constructs a new `ExprPropertiesNode` with unknown properties for a
    /// given physical expression. This node initializes with default properties
    /// and recursively applies this to all child expressions.
    pub fn new_unknown(expr: Arc<dyn PhysicalExpr>) -> Self {
        let children = expr
            .children()
            .into_iter()
            .cloned()
            .map(Self::new_unknown)
            .collect();
        Self {
            expr,
            data: ExprProperties::new_unknown(),
            children,
        }
    }
}

/// Scatter `truthy` array by boolean mask. When the mask evaluates `true`, next values of `truthy`
/// are taken, when the mask evaluates `false` values null values are filled.
///
/// # Arguments
/// * `mask` - Boolean values used to determine where to put the `truthy` values
/// * `truthy` - All values of this array are to scatter according to `mask` into final result.
pub fn scatter(mask: &BooleanArray, truthy: &dyn Array) -> Result<ArrayRef> {
    let truthy = truthy.to_data();

    // update the mask so that any null values become false
    // (SlicesIterator doesn't respect nulls)
    let mask = and_kleene(mask, &is_not_null(mask)?)?;

    let mut mutable = MutableArrayData::new(vec![&truthy], true, mask.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;
    // keep track of current position we have in truthy array
    let mut true_pos = 0;

    SlicesIterator::new(&mask).for_each(|(start, end)| {
        // the gap needs to be filled with nulls
        if start > filled {
            mutable.extend_nulls(start - filled);
        }
        // fill with truthy values
        let len = end - start;
        mutable.extend(0, true_pos, true_pos + len);
        true_pos += len;
        filled = end;
    });
    // the remaining part is falsy
    if filled < mask.len() {
        mutable.extend_nulls(mask.len() - filled);
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

/// Reverses the ORDER BY expression, which is useful during equivalent window
/// expression construction. For instance, 'ORDER BY a ASC, NULLS LAST' turns into
/// 'ORDER BY a DESC, NULLS FIRST'.
pub fn reverse_order_bys(order_bys: &[PhysicalSortExpr]) -> Vec<PhysicalSortExpr> {
    order_bys
        .iter()
        .map(|e| PhysicalSortExpr {
            expr: Arc::clone(&e.expr),
            options: !e.options,
        })
        .collect()
}

/// Converts `datafusion_expr::Expr` into corresponding `Arc<dyn PhysicalExpr>`.
/// If conversion is not supported yet, returns Error.
pub fn limited_convert_logical_expr_to_physical_expr(
    expr: &Expr,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match expr {
        Expr::Alias(Alias { expr, .. }) => {
            Ok(limited_convert_logical_expr_to_physical_expr(expr, schema)?)
        }
        Expr::Column(col) => expressions::column::col(&col.name, schema),
        Expr::Cast(cast_expr) => Ok(Arc::new(CastExpr::new(
            limited_convert_logical_expr_to_physical_expr(
                cast_expr.expr.as_ref(),
                schema,
            )?,
            cast_expr.data_type.clone(),
            None,
        ))),
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        _ => exec_err!(
            "Unsupported expression: {expr} for conversion to Arc<dyn PhysicalExpr>"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use datafusion_common::cast::{as_boolean_array, as_int32_array};

    use super::*;

    #[test]
    fn scatter_int() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        // the output array is expected to be the same length as the mask array
        let expected =
            Int32Array::from_iter(vec![Some(1), Some(10), None, None, Some(11)]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_int_end_with_false() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11, 100]));
        let mask = BooleanArray::from(vec![true, false, true, false, false, false]);

        // output should be same length as mask
        let expected =
            Int32Array::from_iter(vec![Some(1), None, Some(10), None, None, None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_with_null_mask() -> Result<()> {
        let truthy = Arc::new(Int32Array::from(vec![1, 10, 11]));
        let mask: BooleanArray = vec![Some(false), None, Some(true), Some(true), None]
            .into_iter()
            .collect();

        // output should treat nulls as though they are false
        let expected = Int32Array::from_iter(vec![None, None, Some(1), Some(10), None]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_int32_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }

    #[test]
    fn scatter_boolean() -> Result<()> {
        let truthy = Arc::new(BooleanArray::from(vec![false, false, false, true]));
        let mask = BooleanArray::from(vec![true, true, false, false, true]);

        // the output array is expected to be the same length as the mask array
        let expected = BooleanArray::from_iter(vec![
            Some(false),
            Some(false),
            None,
            None,
            Some(false),
        ]);
        let result = scatter(&mask, truthy.as_ref())?;
        let result = as_boolean_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }
}
