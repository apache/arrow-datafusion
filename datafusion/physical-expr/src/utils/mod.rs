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

mod guarantee;
pub use guarantee::{Guarantee, LiteralGuarantee};

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::expressions::{BinaryExpr, Column};
use crate::tree_node::ExprContext;
use crate::PhysicalExpr;
use crate::PhysicalSortExpr;

use arrow::datatypes::SchemaRef;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::Result;
use datafusion_expr::Operator;

use itertools::Itertools;
use petgraph::graph::NodeIndex;
use petgraph::stable_graph::StableGraph;

/// Assume the predicate is in the form of CNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 AND b1 <= b2 AND c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
pub fn split_conjunction(
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<&Arc<dyn PhysicalExpr>> {
    split_impl(&Operator::And, predicate, vec![])
}

/// Assume the predicate is in the form of DNF, split the predicate to a Vec of PhysicalExprs.
///
/// For example, split "a1 = a2 OR b1 <= b2 OR c1 != c2" into ["a1 = a2", "b1 <= b2", "c1 != c2"]
pub fn split_disjunction(
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<&Arc<dyn PhysicalExpr>> {
    split_impl(&Operator::Or, predicate, vec![])
}

fn split_impl<'a>(
    operator: &Operator,
    predicate: &'a Arc<dyn PhysicalExpr>,
    mut exprs: Vec<&'a Arc<dyn PhysicalExpr>>,
) -> Vec<&'a Arc<dyn PhysicalExpr>> {
    match predicate.as_any().downcast_ref::<BinaryExpr>() {
        Some(binary) if binary.op() == operator => {
            let exprs = split_impl(operator, binary.left(), exprs);
            split_impl(operator, binary.right(), exprs)
        }
        Some(_) | None => {
            exprs.push(predicate);
            exprs
        }
    }
}

/// This function maps back requirement after ProjectionExec
/// to the Executor for its input.
// Specifically, `ProjectionExec` changes index of `Column`s in the schema of its input executor.
// This function changes requirement given according to ProjectionExec schema to the requirement
// according to schema of input executor to the ProjectionExec.
// For instance, Column{"a", 0} would turn to Column{"a", 1}. Please note that this function assumes that
// name of the Column is unique. If we have a requirement such that Column{"a", 0}, Column{"a", 1}.
// This function will produce incorrect result (It will only emit single Column as a result).
pub fn map_columns_before_projection(
    parent_required: &[Arc<dyn PhysicalExpr>],
    proj_exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Vec<Arc<dyn PhysicalExpr>> {
    let column_mapping = proj_exprs
        .iter()
        .filter_map(|(expr, name)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|column| (name.clone(), column.clone()))
        })
        .collect::<HashMap<_, _>>();
    parent_required
        .iter()
        .filter_map(|r| {
            r.as_any()
                .downcast_ref::<Column>()
                .and_then(|c| column_mapping.get(c.name()))
        })
        .map(|e| Arc::new(e.clone()) as _)
        .collect()
}

/// This function returns all `Arc<dyn PhysicalExpr>`s inside the given
/// `PhysicalSortExpr` sequence.
pub fn convert_to_expr<T: Borrow<PhysicalSortExpr>>(
    sequence: impl IntoIterator<Item = T>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    sequence
        .into_iter()
        .map(|elem| Arc::clone(&elem.borrow().expr))
        .collect()
}

/// This function finds the indices of `targets` within `items` using strict
/// equality.
pub fn get_indices_of_exprs_strict<T: Borrow<Arc<dyn PhysicalExpr>>>(
    targets: impl IntoIterator<Item = T>,
    items: &[Arc<dyn PhysicalExpr>],
) -> Vec<usize> {
    targets
        .into_iter()
        .filter_map(|target| items.iter().position(|e| e.eq(target.borrow())))
        .collect()
}

pub type ExprTreeNode<T> = ExprContext<Option<T>>;

/// This struct is used to convert a [`PhysicalExpr`] tree into a DAEG (i.e. an expression
/// DAG) by collecting identical expressions in one node. Caller specifies the node type
/// in the DAEG via the `constructor` argument, which constructs nodes in the DAEG from
/// the [`ExprTreeNode`] ancillary object.
struct PhysicalExprDAEGBuilder<'a, T, F: Fn(&ExprTreeNode<NodeIndex>) -> Result<T>> {
    // The resulting DAEG (expression DAG).
    graph: StableGraph<T, usize>,
    // A vector of visited expression nodes and their corresponding node indices.
    visited_plans: Vec<(Arc<dyn PhysicalExpr>, NodeIndex)>,
    // A function to convert an input expression node to T.
    constructor: &'a F,
}

impl<'a, T, F: Fn(&ExprTreeNode<NodeIndex>) -> Result<T>>
    PhysicalExprDAEGBuilder<'a, T, F>
{
    // This method mutates an expression node by transforming it to a physical expression
    // and adding it to the graph. The method returns the mutated expression node.
    fn mutate(
        &mut self,
        mut node: ExprTreeNode<NodeIndex>,
    ) -> Result<Transformed<ExprTreeNode<NodeIndex>>> {
        // Get the expression associated with the input expression node.
        let expr = &node.expr;

        // Check if the expression has already been visited.
        let node_idx = match self.visited_plans.iter().find(|(e, _)| expr.eq(e)) {
            // If the expression has been visited, return the corresponding node index.
            Some((_, idx)) => *idx,
            // If the expression has not been visited, add a new node to the graph and
            // add edges to its child nodes. Add the visited expression to the vector
            // of visited expressions and return the newly created node index.
            None => {
                let node_idx = self.graph.add_node((self.constructor)(&node)?);
                for expr_node in node.children.iter() {
                    self.graph.add_edge(node_idx, expr_node.data.unwrap(), 0);
                }
                self.visited_plans.push((Arc::clone(expr), node_idx));
                node_idx
            }
        };
        // Set the data field of the input expression node to the corresponding node index.
        node.data = Some(node_idx);
        // Return the mutated expression node.
        Ok(Transformed::yes(node))
    }
}

// A function that builds a directed acyclic graph of physical expression trees.
pub fn build_dag<T, F>(
    expr: Arc<dyn PhysicalExpr>,
    constructor: &F,
) -> Result<(NodeIndex, StableGraph<T, usize>)>
where
    F: Fn(&ExprTreeNode<NodeIndex>) -> Result<T>,
{
    // Create a new expression tree node from the input expression.
    let init = ExprTreeNode::new_default(expr);
    // Create a new `PhysicalExprDAEGBuilder` instance.
    let mut builder = PhysicalExprDAEGBuilder {
        graph: StableGraph::<T, usize>::new(),
        visited_plans: Vec::<(Arc<dyn PhysicalExpr>, NodeIndex)>::new(),
        constructor,
    };
    // Use the builder to transform the expression tree node into a DAG.
    let root = init.transform_up(|node| builder.mutate(node)).data()?;
    // Return a tuple containing the root node index and the DAG.
    Ok((root.data.unwrap(), builder.graph))
}

/// Recursively extract referenced [`Column`]s within a [`PhysicalExpr`].
pub fn collect_columns(expr: &Arc<dyn PhysicalExpr>) -> HashSet<Column> {
    let mut columns = HashSet::<Column>::new();
    expr.apply(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            if !columns.iter().any(|c| c.eq(column)) {
                columns.insert(column.clone());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    // pre_visit always returns OK, so this will always too
    .expect("no way to return error during recursion");
    columns
}

/// Re-assign column indices referenced in predicate according to given schema.
/// This may be helpful when dealing with projections.
pub fn reassign_predicate_columns(
    pred: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    ignore_not_found: bool,
) -> Result<Arc<dyn PhysicalExpr>> {
    pred.transform_down(|expr| {
        let expr_any = expr.as_any();

        if let Some(column) = expr_any.downcast_ref::<Column>() {
            let index = match schema.index_of(column.name()) {
                Ok(idx) => idx,
                Err(_) if ignore_not_found => usize::MAX,
                Err(e) => return Err(e.into()),
            };
            return Ok(Transformed::yes(Arc::new(Column::new(
                column.name(),
                index,
            ))));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Merge left and right sort expressions, checking for duplicates.
pub fn merge_vectors(
    left: &[PhysicalSortExpr],
    right: &[PhysicalSortExpr],
) -> Vec<PhysicalSortExpr> {
    left.iter()
        .cloned()
        .chain(right.iter().cloned())
        .unique()
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::any::Any;
    use std::fmt::{Display, Formatter};

    use super::*;
    use crate::expressions::{binary, cast, col, in_list, lit, Literal};

    use arrow_array::{ArrayRef, Float32Array, Float64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::{exec_err, DataFusionError, ScalarValue};
    use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

    use petgraph::visit::Bfs;

    #[derive(Debug, Clone)]
    pub struct TestScalarUDF {
        pub(crate) signature: Signature,
    }

    impl TestScalarUDF {
        pub fn new() -> Self {
            use DataType::*;
            Self {
                signature: Signature::uniform(
                    1,
                    vec![Float64, Float32],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for TestScalarUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "test-scalar-udf"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            let arg_type = &arg_types[0];

            match arg_type {
                DataType::Float32 => Ok(DataType::Float32),
                _ => Ok(DataType::Float64),
            }
        }

        fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
            Ok(input[0].sort_properties)
        }

        fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(args)?;

            let arr: ArrayRef = match args[0].data_type() {
                DataType::Float64 => Arc::new({
                    let arg = &args[0]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "could not cast {} to {}",
                                self.name(),
                                std::any::type_name::<Float64Array>()
                            ))
                        })?;

                    arg.iter()
                        .map(|a| a.map(f64::floor))
                        .collect::<Float64Array>()
                }),
                DataType::Float32 => Arc::new({
                    let arg = &args[0]
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "could not cast {} to {}",
                                self.name(),
                                std::any::type_name::<Float32Array>()
                            ))
                        })?;

                    arg.iter()
                        .map(|a| a.map(f32::floor))
                        .collect::<Float32Array>()
                }),
                other => {
                    return exec_err!(
                        "Unsupported data type {other:?} for function {}",
                        self.name()
                    );
                }
            };
            Ok(ColumnarValue::Array(arr))
        }
    }

    #[derive(Clone)]
    struct DummyProperty {
        expr_type: String,
    }

    /// This is a dummy node in the DAEG; it stores a reference to the actual
    /// [PhysicalExpr] as well as a dummy property.
    #[derive(Clone)]
    struct PhysicalExprDummyNode {
        pub expr: Arc<dyn PhysicalExpr>,
        pub property: DummyProperty,
    }

    impl Display for PhysicalExprDummyNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.expr)
        }
    }

    fn make_dummy_node(node: &ExprTreeNode<NodeIndex>) -> Result<PhysicalExprDummyNode> {
        let expr = Arc::clone(&node.expr);
        let dummy_property = if expr.as_any().is::<BinaryExpr>() {
            "Binary"
        } else if expr.as_any().is::<Column>() {
            "Column"
        } else if expr.as_any().is::<Literal>() {
            "Literal"
        } else {
            "Other"
        }
        .to_owned();
        Ok(PhysicalExprDummyNode {
            expr,
            property: DummyProperty {
                expr_type: dummy_property,
            },
        })
    }

    #[test]
    fn test_build_dag() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("0", DataType::Int32, true),
            Field::new("1", DataType::Int32, true),
            Field::new("2", DataType::Int32, true),
        ]);
        let expr = binary(
            cast(
                binary(
                    col("0", &schema)?,
                    Operator::Plus,
                    col("1", &schema)?,
                    &schema,
                )?,
                &schema,
                DataType::Int64,
            )?,
            Operator::Gt,
            binary(
                cast(col("2", &schema)?, &schema, DataType::Int64)?,
                Operator::Plus,
                lit(ScalarValue::Int64(Some(10))),
                &schema,
            )?,
            &schema,
        )?;
        let mut vector_dummy_props = vec![];
        let (root, graph) = build_dag(expr, &make_dummy_node)?;
        let mut bfs = Bfs::new(&graph, root);
        while let Some(node_index) = bfs.next(&graph) {
            let node = &graph[node_index];
            vector_dummy_props.push(node.property.clone());
        }

        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Binary")
                .count(),
            3
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Column")
                .count(),
            3
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Literal")
                .count(),
            1
        );
        assert_eq!(
            vector_dummy_props
                .iter()
                .filter(|property| property.expr_type == "Other")
                .count(),
            2
        );
        Ok(())
    }

    #[test]
    fn test_convert_to_expr() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt64, false)]);
        let sort_expr = vec![PhysicalSortExpr {
            expr: col("a", &schema)?,
            options: Default::default(),
        }];
        assert!(convert_to_expr(&sort_expr)[0].eq(&sort_expr[0].expr));
        Ok(())
    }

    #[test]
    fn test_get_indices_of_exprs_strict() {
        let list1: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("a", 0)),
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("d", 3)),
        ];
        let list2: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(Column::new("b", 1)),
            Arc::new(Column::new("c", 2)),
            Arc::new(Column::new("a", 0)),
        ];
        assert_eq!(get_indices_of_exprs_strict(&list1, &list2), vec![2, 0, 1]);
        assert_eq!(get_indices_of_exprs_strict(&list2, &list1), vec![1, 2, 0]);
    }

    #[test]
    fn test_reassign_predicate_columns_in_list() {
        let int_field = Field::new("should_not_matter", DataType::Int64, true);
        let dict_field = Field::new(
            "id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        );
        let schema_small = Arc::new(Schema::new(vec![dict_field.clone()]));
        let schema_big = Arc::new(Schema::new(vec![int_field, dict_field]));
        let pred = in_list(
            Arc::new(Column::new_with_schema("id", &schema_big).unwrap()),
            vec![lit(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("2")),
            ))],
            &false,
            &schema_big,
        )
        .unwrap();

        let actual = reassign_predicate_columns(pred, &schema_small, false).unwrap();

        let expected = in_list(
            Arc::new(Column::new_with_schema("id", &schema_small).unwrap()),
            vec![lit(ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("2")),
            ))],
            &false,
            &schema_small,
        )
        .unwrap();

        assert_eq!(actual.as_ref(), expected.as_any());
    }

    #[test]
    fn test_collect_columns() -> Result<()> {
        let expr1 = Arc::new(Column::new("col1", 2)) as _;
        let mut expected = HashSet::new();
        expected.insert(Column::new("col1", 2));
        assert_eq!(collect_columns(&expr1), expected);

        let expr2 = Arc::new(Column::new("col2", 5)) as _;
        let mut expected = HashSet::new();
        expected.insert(Column::new("col2", 5));
        assert_eq!(collect_columns(&expr2), expected);

        let expr3 = Arc::new(BinaryExpr::new(expr1, Operator::Plus, expr2)) as _;
        let mut expected = HashSet::new();
        expected.insert(Column::new("col1", 2));
        expected.insert(Column::new("col2", 5));
        assert_eq!(collect_columns(&expr3), expected);
        Ok(())
    }
}
