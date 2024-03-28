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
//! This module provides logic for displaying LogicalPlans in various styles

use std::collections::HashMap;
use std::fmt;

use crate::{
    expr_vec_fmt, Aggregate, DescribeTable, Distinct, DistinctOn, DmlStatement, Expr,
    Filter, Join, Limit, LogicalPlan, Partitioning, Prepare, Projection, RecursiveQuery,
    Repartition, Sort, Subquery, SubqueryAlias, TableProviderFilterPushDown, TableScan,
    Unnest, Values, Window,
};

use crate::dml::CopyTo;
use arrow::datatypes::Schema;
use datafusion_common::display::GraphvizBuilder;
use datafusion_common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::DataFusionError;
use serde_json::json;

/// Formats plans with a single line per node. For example:
///
/// Projection: id
///    Filter: state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
pub struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// If true, includes summarized schema information
    with_schema: bool,
    /// The current indent
    indent: usize,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    /// Create a visitor that will write a formatted LogicalPlan to f. If `with_schema` is
    /// true, includes schema information on each line.
    pub fn new(f: &'a mut fmt::Formatter<'b>, with_schema: bool) -> Self {
        Self {
            f,
            with_schema,
            indent: 0,
        }
    }
}

impl<'a, 'b> TreeNodeVisitor for IndentVisitor<'a, 'b> {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        plan: &LogicalPlan,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        write!(self.f, "{}", plan.display())?;
        if self.with_schema {
            write!(
                self.f,
                " {}",
                display_schema(&plan.schema().as_ref().to_owned().into())
            )?;
        }

        self.indent += 1;
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(
        &mut self,
        _plan: &LogicalPlan,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        self.indent -= 1;
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Print the schema in a compact representation to `buf`
///
/// For example: `foo:Utf8` if `foo` can not be null, and
/// `foo:Utf8;N` if `foo` is nullable.
///
/// ```
/// use arrow::datatypes::{Field, Schema, DataType};
/// # use datafusion_expr::logical_plan::display_schema;
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("first_name", DataType::Utf8, true),
///  ]);
///
///  assert_eq!(
///      "[id:Int32, first_name:Utf8;N]",
///      format!("{}", display_schema(&schema))
///  );
/// ```
pub fn display_schema(schema: &Schema) -> impl fmt::Display + '_ {
    struct Wrapper<'a>(&'a Schema);

    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "[")?;
            for (idx, field) in self.0.fields().iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                let nullable_str = if field.is_nullable() { ";N" } else { "" };
                write!(
                    f,
                    "{}:{:?}{}",
                    field.name(),
                    field.data_type(),
                    nullable_str
                )?;
            }
            write!(f, "]")
        }
    }
    Wrapper(schema)
}

/// Formats plans for graphical display using the `DOT` language. This
/// format can be visualized using software from
/// [`graphviz`](https://graphviz.org/)
pub struct GraphvizVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    graphviz_builder: GraphvizBuilder,
    /// If true, includes summarized schema information
    with_schema: bool,

    /// Holds the ids (as generated from `graphviz_builder` of all
    /// parent nodes
    parent_ids: Vec<usize>,
}

impl<'a, 'b> GraphvizVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self {
            f,
            graphviz_builder: GraphvizBuilder::default(),
            with_schema: false,
            parent_ids: Vec::new(),
        }
    }

    /// Sets a flag which controls if the output schema is displayed
    pub fn set_with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }

    pub fn pre_visit_plan(&mut self, label: &str) -> fmt::Result {
        self.graphviz_builder.start_cluster(self.f, label)
    }

    pub fn post_visit_plan(&mut self) -> fmt::Result {
        self.graphviz_builder.end_cluster(self.f)
    }

    pub fn start_graph(&mut self) -> fmt::Result {
        self.graphviz_builder.start_graph(self.f)
    }

    pub fn end_graph(&mut self) -> fmt::Result {
        self.graphviz_builder.end_graph(self.f)
    }
}

impl<'a, 'b> TreeNodeVisitor for GraphvizVisitor<'a, 'b> {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        plan: &LogicalPlan,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        let id = self.graphviz_builder.next_id();

        // Create a new graph node for `plan` such as
        // id [label="foo"]
        let label = if self.with_schema {
            format!(
                r"{}\nSchema: {}",
                plan.display(),
                display_schema(&plan.schema().as_ref().to_owned().into())
            )
        } else {
            format!("{}", plan.display())
        };

        self.graphviz_builder
            .add_node(self.f, id, &label, None)
            .map_err(|_e| DataFusionError::Internal("Fail to format".to_string()))?;

        // Create an edge to our parent node, if any
        //  parent_id -> id
        if let Some(parent_id) = self.parent_ids.last() {
            self.graphviz_builder
                .add_edge(self.f, *parent_id, id)
                .map_err(|_e| DataFusionError::Internal("Fail to format".to_string()))?;
        }

        self.parent_ids.push(id);
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(
        &mut self,
        _plan: &LogicalPlan,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        // always be non-empty as pre_visit always pushes
        // So it should always be Ok(true)
        let res = self.parent_ids.pop();
        res.ok_or(DataFusionError::Internal("Fail to format".to_string()))
            .map(|_| TreeNodeRecursion::Continue)
    }
}

/// Formats plans to display as postgresql plan json format.
///
/// There are already many existing visualizer for this format, for example [dalibo](https://explain.dalibo.com/).
/// Unfortunately, there is no formal spec for this format, but it is widely used in the PostgreSQL community.
///
/// Here is an example of the format:
///
/// ```json
/// [
///     {
///         "Plan": {
///             "Node Type": "Sort",
///             "Output": [
///                 "question_1.id",
///                 "question_1.title",
///                 "question_1.text",
///                 "question_1.file",
///                 "question_1.type",
///                 "question_1.source",
///                 "question_1.exam_id"
///             ],
///             "Sort Key": [
///                 "question_1.id"
///             ],
///             "Plans": [
///                 {
///                     "Node Type": "Seq Scan",
///                     "Parent Relationship": "Left",
///                     "Relation Name": "question",
///                     "Schema": "public",
///                     "Alias": "question_1",
///                     "Output": [
///                        "question_1.id",
///                         "question_1.title",
///                        "question_1.text",
///                         "question_1.file",
///                         "question_1.type",
///                         "question_1.source",
///                         "question_1.exam_id"
///                     ],
///                     "Filter": "(question_1.exam_id = 1)"
///                 }
///             ]
///         }
///     }
/// ]
/// ```
pub struct PgJsonVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,

    /// A mapping from plan node id to the plan node json representation.
    objects: HashMap<u32, serde_json::Value>,

    next_id: u32,

    /// If true, includes summarized schema information
    with_schema: bool,

    /// Holds the ids (as generated from `graphviz_builder` of all
    /// parent nodes
    parent_ids: Vec<u32>,
}

impl<'a, 'b> PgJsonVisitor<'a, 'b> {
    pub fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self {
            f,
            objects: HashMap::new(),
            next_id: 0,
            with_schema: false,
            parent_ids: Vec::new(),
        }
    }

    /// Sets a flag which controls if the output schema is displayed
    pub fn with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }

    /// Converts a logical plan node to a json object.
    fn to_json_value(node: &LogicalPlan) -> serde_json::Value {
        match node {
            LogicalPlan::EmptyRelation(_) => {
                json!({
                    "Node Type": "EmptyRelation",
                })
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery { is_distinct, .. }) => {
                json!({
                    "Node Type": "RecursiveQuery",
                    "Is Distinct": is_distinct,
                })
            }
            LogicalPlan::Values(Values { ref values, .. }) => {
                let str_values = values
                    .iter()
                    // limit to only 5 values to avoid horrible display
                    .take(5)
                    .map(|row| {
                        let item = row
                            .iter()
                            .map(|expr| expr.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({item})")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                let elipse = if values.len() > 5 { "..." } else { "" };

                let values_str = format!("{}{}", str_values, elipse);
                json!({
                    "Node Type": "Values",
                    "Values": values_str
                })
            }
            LogicalPlan::TableScan(TableScan {
                ref source,
                ref table_name,
                ref filters,
                ref fetch,
                ..
            }) => {
                let mut object = json!({
                    "Node Type": "TableScan",
                    "Relation Name": table_name.table(),
                });

                if let Some(s) = table_name.schema() {
                    object["Schema"] = serde_json::Value::String(s.to_string());
                }

                if let Some(c) = table_name.catalog() {
                    object["Catalog"] = serde_json::Value::String(c.to_string());
                }

                if !filters.is_empty() {
                    let mut full_filter = vec![];
                    let mut partial_filter = vec![];
                    let mut unsupported_filters = vec![];
                    let filters: Vec<&Expr> = filters.iter().collect();

                    if let Ok(results) = source.supports_filters_pushdown(&filters) {
                        filters.iter().zip(results.iter()).for_each(
                            |(x, res)| match res {
                                TableProviderFilterPushDown::Exact => full_filter.push(x),
                                TableProviderFilterPushDown::Inexact => {
                                    partial_filter.push(x)
                                }
                                TableProviderFilterPushDown::Unsupported => {
                                    unsupported_filters.push(x)
                                }
                            },
                        );
                    }

                    if !full_filter.is_empty() {
                        object["Full Filters"] = serde_json::Value::String(
                            expr_vec_fmt!(full_filter).to_string(),
                        );
                    };
                    if !partial_filter.is_empty() {
                        object["Partial Filters"] = serde_json::Value::String(
                            expr_vec_fmt!(partial_filter).to_string(),
                        );
                    }
                    if !unsupported_filters.is_empty() {
                        object["Unsupported Filters"] = serde_json::Value::String(
                            expr_vec_fmt!(unsupported_filters).to_string(),
                        );
                    }
                }

                if let Some(f) = fetch {
                    object["Fetch"] = serde_json::Value::Number((*f).into());
                }

                object
            }
            LogicalPlan::Projection(Projection { ref expr, .. }) => {
                json!({
                    "Node Type": "Projection",
                    "Expressions": expr.iter().map(|e| e.to_string()).collect::<Vec<_>>()
                })
            }
            LogicalPlan::Dml(DmlStatement { table_name, op, .. }) => {
                json!({
                    "Node Type": "Projection",
                    "Operation": op.name(),
                    "Table Name": table_name.table()
                })
            }
            LogicalPlan::Copy(CopyTo {
                input: _,
                output_url,
                format_options,
                partition_by: _,
                options,
            }) => {
                let op_str = options
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ");
                json!({
                    "Node Type": "CopyTo",
                    "Output URL": output_url,
                    "Format Options": format!("{}", format_options),
                    "Options": op_str
                })
            }
            LogicalPlan::Ddl(ddl) => {
                json!({
                    "Node Type": "Ddl",
                    "Operation": format!("{}", ddl.display())
                })
            }
            LogicalPlan::Filter(Filter {
                predicate: ref expr,
                ..
            }) => {
                json!({
                    "Node Type": "Filter",
                    "Condition": format!("{}", expr)
                })
            }
            LogicalPlan::Window(Window {
                ref window_expr, ..
            }) => {
                json!({
                    "Node Type": "WindowAggr",
                    "Expressions": expr_vec_fmt!(window_expr)
                })
            }
            LogicalPlan::Aggregate(Aggregate {
                ref group_expr,
                ref aggr_expr,
                ..
            }) => {
                json!({
                    "Node Type": "Aggregate",
                    "Group By": expr_vec_fmt!(group_expr),
                    "Aggregates": expr_vec_fmt!(aggr_expr)
                })
            }
            LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
                let mut object = json!({
                    "Node Type": "Sort",
                    "Sort Key": expr_vec_fmt!(expr),
                });

                if let Some(fetch) = fetch {
                    object["Fetch"] = serde_json::Value::Number((*fetch).into());
                }

                object
            }
            LogicalPlan::Join(Join {
                on: ref keys,
                filter,
                join_constraint,
                join_type,
                ..
            }) => {
                let join_expr: Vec<String> =
                    keys.iter().map(|(l, r)| format!("{l} = {r}")).collect();
                let filter_expr = filter
                    .as_ref()
                    .map(|expr| format!(" Filter: {expr}"))
                    .unwrap_or_else(|| "".to_string());
                json!({
                    "Node Type": format!("{} Join", join_type),
                    "Join Constraint": format!("{:?}", join_constraint),
                    "Join Keys": join_expr.join(", "),
                    "Filter": format!("{}", filter_expr)
                })
            }
            LogicalPlan::CrossJoin(_) => {
                json!({
                    "Node Type": "Cross Join"
                })
            }
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::RoundRobinBatch(n) => {
                    json!({
                        "Node Type": "Repartition",
                        "Partitioning Scheme": "RoundRobinBatch",
                        "Partition Count": n
                    })
                }
                Partitioning::Hash(expr, n) => {
                    let hash_expr: Vec<String> =
                        expr.iter().map(|e| format!("{e}")).collect();

                    json!({
                        "Node Type": "Repartition",
                        "Partitioning Scheme": "Hash",
                        "Partition Count": n,
                        "Partitioning Key": hash_expr
                    })
                }
                Partitioning::DistributeBy(expr) => {
                    let dist_by_expr: Vec<String> =
                        expr.iter().map(|e| format!("{e}")).collect();
                    json!({
                        "Node Type": "Repartition",
                        "Partitioning Scheme": "DistributeBy",
                        "Partitioning Key": dist_by_expr
                    })
                }
            },
            LogicalPlan::Limit(Limit {
                ref skip,
                ref fetch,
                ..
            }) => {
                let mut object = serde_json::json!(
                    {
                        "Node Type": "Limit",
                        "Skip": skip,
                    }
                );
                if let Some(f) = fetch {
                    object["Fetch"] = serde_json::Value::Number((*f).into());
                };
                object
            }
            LogicalPlan::Subquery(Subquery { .. }) => {
                json!({
                    "Node Type": "Subquery"
                })
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { ref alias, .. }) => {
                json!({
                    "Node Type": "Subquery",
                    "Alias": alias.table(),
                })
            }
            LogicalPlan::Statement(statement) => {
                json!({
                    "Node Type": "Statement",
                    "Statement": format!("{}", statement.display())
                })
            }
            LogicalPlan::Distinct(distinct) => match distinct {
                Distinct::All(_) => {
                    json!({
                        "Node Type": "DistinctAll"
                    })
                }
                Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    ..
                }) => {
                    let mut object = json!({
                        "Node Type": "DistinctOn",
                        "On": expr_vec_fmt!(on_expr),
                        "Select": expr_vec_fmt!(select_expr),
                    });
                    if let Some(sort_expr) = sort_expr {
                        object["Sort"] = serde_json::Value::String(
                            expr_vec_fmt!(sort_expr).to_string(),
                        );
                    }

                    object
                }
            },
            LogicalPlan::Explain { .. } => {
                json!({
                    "Node Type": "Explain"
                })
            }
            LogicalPlan::Analyze { .. } => {
                json!({
                    "Node Type": "Analyze"
                })
            }
            LogicalPlan::Union(_) => {
                json!({
                    "Node Type": "Union"
                })
            }
            LogicalPlan::Extension(e) => {
                json!({
                    "Node Type": e.node.name(),
                    "Detail": format!("{:?}", e.node)
                })
            }
            LogicalPlan::Prepare(Prepare {
                name, data_types, ..
            }) => {
                json!({
                    "Node Type": "Prepare",
                    "Name": name,
                    "Data Types": format!("{:?}", data_types)
                })
            }
            LogicalPlan::DescribeTable(DescribeTable { .. }) => {
                json!({
                    "Node Type": "DescribeTable"
                })
            }
            LogicalPlan::Unnest(Unnest { column, .. }) => {
                json!({
                    "Node Type": "Unnest",
                    "Column": format!("{}", column)
                })
            }
        }
    }
}

impl<'a, 'b> TreeNodeVisitor for PgJsonVisitor<'a, 'b> {
    type Node = LogicalPlan;

    fn f_down(
        &mut self,
        node: &LogicalPlan,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        let id = self.next_id;
        self.next_id += 1;
        let mut object = Self::to_json_value(node);

        object["Plans"] = serde_json::Value::Array(vec![]);

        if self.with_schema {
            object["Output"] = serde_json::Value::Array(
                node.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .map(serde_json::Value::String)
                    .collect(),
            );
        };

        self.objects.insert(id, object);
        self.parent_ids.push(id);
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(
        &mut self,
        _node: &Self::Node,
    ) -> datafusion_common::Result<TreeNodeRecursion> {
        let id = self.parent_ids.pop().unwrap();

        let current_node = self.objects.remove(&id).expect("Missing current node!");

        if let Some(parent_id) = self.parent_ids.last() {
            let parent_node = self
                .objects
                .get_mut(parent_id)
                .expect("Missing parent node!");
            let plans = parent_node
                .get_mut("Plans")
                .and_then(|p| p.as_array_mut())
                .expect("Plans should be an array");

            plans.push(current_node);
        } else {
            // This is the root node
            let plan = serde_json::json!([{"Plan": current_node}]);
            write!(
                self.f,
                "{}",
                serde_json::to_string_pretty(&plan)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            )?;
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn test_display_empty_schema() {
        let schema = Schema::empty();
        assert_eq!("[]", format!("{}", display_schema(&schema)));
    }

    #[test]
    fn test_display_schema() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, true),
        ]);

        assert_eq!(
            "[id:Int32, first_name:Utf8;N]",
            format!("{}", display_schema(&schema))
        );
    }
}
