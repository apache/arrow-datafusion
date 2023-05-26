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

use crate::expressions::{BinaryExpr, Column};
use crate::{PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement};

use arrow::datatypes::SchemaRef;
use arrow_schema::SortOptions;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Equivalence Properties is a vec of EquivalentClass.
#[derive(Debug, Clone)]
pub struct EquivalenceProperties<T = Column> {
    classes: Vec<EquivalentClass<T>>,
    schema: SchemaRef,
}

impl<T: PartialEq + Clone> EquivalenceProperties<T> {
    pub fn new(schema: SchemaRef) -> Self {
        EquivalenceProperties {
            classes: vec![],
            schema,
        }
    }

    pub fn classes(&self) -> &[EquivalentClass<T>] {
        &self.classes
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn extend<I: IntoIterator<Item = EquivalentClass<T>>>(&mut self, iter: I) {
        for ec in iter {
            self.classes.push(ec)
        }
    }

    /// Adds new equal conditions into the EquivalenceProperties. New equal
    /// conditions usually come from equality predicates in a join/filter.
    pub fn add_equal_conditions(&mut self, new_conditions: (&T, &T)) {
        let mut idx1: Option<usize> = None;
        let mut idx2: Option<usize> = None;
        for (idx, class) in self.classes.iter_mut().enumerate() {
            let contains_first = class.contains(new_conditions.0);
            let contains_second = class.contains(new_conditions.1);
            match (contains_first, contains_second) {
                (true, false) => {
                    class.insert(new_conditions.1.clone());
                    idx1 = Some(idx);
                }
                (false, true) => {
                    class.insert(new_conditions.0.clone());
                    idx2 = Some(idx);
                }
                (true, true) => {
                    idx1 = Some(idx);
                    idx2 = Some(idx);
                    break;
                }
                (false, false) => {}
            }
        }

        match (idx1, idx2) {
            (Some(idx_1), Some(idx_2)) if idx_1 != idx_2 => {
                // need to merge the two existing EquivalentClasses
                let second_eq_class = self.classes.get(idx_2).unwrap().clone();
                let first_eq_class = self.classes.get_mut(idx_1).unwrap();
                for prop in second_eq_class.iter() {
                    if !first_eq_class.contains(prop) {
                        first_eq_class.insert(prop.clone());
                    }
                }
                self.classes.remove(idx_2);
            }
            (None, None) => {
                // adding new pairs
                self.classes.push(EquivalentClass::<T>::new(
                    new_conditions.0.clone(),
                    vec![new_conditions.1.clone()],
                ));
            }
            _ => {}
        }
    }
}

fn deduplicate_vector<T: PartialEq>(in_data: Vec<T>) -> Vec<T> {
    let mut result = vec![];
    for elem in in_data {
        if !result.contains(&elem) {
            result.push(elem);
        }
    }
    result
}

fn get_elem_position<T: PartialEq>(in_data: &[T], elem: &T) -> Option<usize> {
    in_data.iter().position(|item| item.eq(elem))
}

fn remove_from_vec<T: PartialEq>(in_data: &mut Vec<T>, elem: &T) -> bool {
    if let Some(idx) = get_elem_position(in_data, elem) {
        in_data.remove(idx);
        true
    } else {
        false
    }
}

fn get_column_indices_helper(
    indices: &mut Vec<(usize, String)>,
    expr: &Arc<dyn PhysicalExpr>,
) {
    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
        indices.push((col.index(), col.name().to_string()))
    } else if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
        get_column_indices_helper(indices, binary_expr.left());
        get_column_indices_helper(indices, binary_expr.right());
    };
}

/// Get the indices of the columns occur in the expression
fn get_column_indices_names(expr: &Arc<dyn PhysicalExpr>) -> Vec<(usize, String)> {
    let mut result = vec![];
    get_column_indices_helper(&mut result, expr);
    result
}

/// `OrderingEquivalenceProperties` keeps track of columns that describe the
/// global ordering of the schema. These columns are not necessarily same; e.g.
/// ```text
/// ┌-------┐
/// | a | b |
/// |---|---|
/// | 1 | 9 |
/// | 2 | 8 |
/// | 3 | 7 |
/// | 5 | 5 |
/// └---┴---┘
/// ```
/// where both `a ASC` and `b DESC` can describe the table ordering. With
/// `OrderingEquivalenceProperties`, we can keep track of these equivalences
/// and treat `a ASC` and `b DESC` as the same ordering requirement.
pub type OrderingEquivalenceProperties = EquivalenceProperties<Vec<PhysicalSortExpr>>;

/// EquivalentClass is a set of [`Column`]s or [`OrderedColumn`]s that are known
/// to have the same value in all tuples in a relation. `EquivalentClass<Column>`
/// is generated by equality predicates, typically equijoin conditions and equality
/// conditions in filters. `EquivalentClass<OrderedColumn>` is generated by the
/// `ROW_NUMBER` window function.
#[derive(Debug, Clone)]
pub struct EquivalentClass<T = Column> {
    /// First element in the EquivalentClass
    head: T,
    /// Other equal columns
    others: Vec<T>,
}

impl<T: PartialEq + Clone> EquivalentClass<T> {
    pub fn new(head: T, others: Vec<T>) -> EquivalentClass<T> {
        let others = deduplicate_vector(others);
        EquivalentClass { head, others }
    }

    pub fn head(&self) -> &T {
        &self.head
    }

    pub fn others(&self) -> &[T] {
        &self.others
    }

    pub fn contains(&self, col: &T) -> bool {
        self.head == *col || self.others.contains(col)
    }

    pub fn insert(&mut self, col: T) -> bool {
        if self.head != col && !self.others.contains(&col) {
            self.others.push(col);
            true
        } else {
            false
        }
    }

    pub fn remove(&mut self, col: &T) -> bool {
        let removed = remove_from_vec(&mut self.others, col);
        // If the the removed entry is head, shit other such that first entry becomes head in others.
        if !removed && *col == self.head {
            let one_col = self.others.first().cloned();
            if let Some(col) = one_col {
                let removed = remove_from_vec(&mut self.others, &col);
                self.head = col;
                removed
            } else {
                false
            }
        } else {
            removed
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ T> {
        std::iter::once(&self.head).chain(self.others.iter())
    }

    pub fn len(&self) -> usize {
        self.others.len() + 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// /// This object represents a [`Column`] with a definite ordering.
// #[derive(Debug, Hash, PartialEq, Eq, Clone)]
// pub struct OrderedColumn {
//     pub col: Column,
//     pub options: SortOptions,
// }
//
// impl OrderedColumn {
//     pub fn new(col: Column, options: SortOptions) -> Self {
//         Self { col, options }
//     }
// }
//
// impl From<OrderedColumn> for PhysicalSortExpr {
//     fn from(value: OrderedColumn) -> Self {
//         PhysicalSortExpr {
//             expr: Arc::new(value.col) as _,
//             options: value.options,
//         }
//     }
// }
//
// impl From<OrderedColumn> for PhysicalSortRequirement {
//     fn from(value: OrderedColumn) -> Self {
//         PhysicalSortRequirement {
//             expr: Arc::new(value.col) as _,
//             options: Some(value.options),
//         }
//     }
// }

/// `Vec<OrderedColumn>` stores the lexicographical ordering for a schema.
/// OrderingEquivalentClass keeps track of different alternative orderings than can
/// describe the schema.
/// For instance, for the table below
/// |a|b|c|d|
/// |1|4|3|1|
/// |2|3|3|2|
/// |3|1|2|2|
/// |3|2|1|3|
/// both `vec![a ASC, b ASC]` and `vec![c DESC, d ASC]` describe the ordering of the table.
/// For this case, we say that `vec![a ASC, b ASC]`, and `vec![c DESC, d ASC]` are ordering equivalent.
pub type OrderingEquivalentClass = EquivalentClass<Vec<PhysicalSortExpr>>;

impl OrderingEquivalentClass {
    /// This function extends ordering equivalences with alias information.
    /// For instance, assume column a and b are aliases,
    /// and column (a ASC), (c DESC) are ordering equivalent. We append (b ASC) to ordering equivalence,
    /// since b is alias of colum a. After this function (a ASC), (c DESC), (b ASC) would be ordering equivalent.
    fn update_with_aliases(&mut self, columns_map: &HashMap<Column, Vec<Column>>) {
        for (column, columns) in columns_map {
            let col_expr = Arc::new(column.clone()) as Arc<dyn PhysicalExpr>;
            let mut to_insert = vec![];
            for ordering in std::iter::once(&self.head).chain(self.others.iter()) {
                for (idx, item) in ordering.iter().enumerate() {
                    if item.expr.eq(&col_expr) {
                        for col in columns {
                            let col_expr = Arc::new(col.clone()) as Arc<dyn PhysicalExpr>;
                            let mut normalized = self.head.clone();
                            // Change the corresponding entry in the head with the alias column:
                            let entry = &mut normalized[idx];
                            (entry.expr, entry.options) = (col_expr, item.options);
                            to_insert.push(normalized);
                        }
                    }
                }
            }
            for items in to_insert {
                self.insert(items);
            }
        }
    }
}

/// This function applies the given projection to the given equivalence
/// properties to compute the resulting (projected) equivalence properties; e.g.
/// 1) Adding an alias, which can introduce additional equivalence properties,
///    as in Projection(a, a as a1, a as a2).
/// 2) Truncate the [`EquivalentClass`]es that are not in the output schema.
pub fn project_equivalence_properties(
    input_eq: EquivalenceProperties,
    alias_map: &HashMap<Column, Vec<Column>>,
    output_eq: &mut EquivalenceProperties,
) {
    let mut eq_classes = input_eq.classes().to_vec();
    for (column, columns) in alias_map {
        let mut find_match = false;
        for class in eq_classes.iter_mut() {
            if class.contains(column) {
                for col in columns {
                    class.insert(col.clone());
                }
                find_match = true;
                break;
            }
        }
        if !find_match {
            eq_classes.push(EquivalentClass::new(column.clone(), columns.clone()));
        }
    }

    // Prune columns that are no longer in the schema from equivalences.
    let schema = output_eq.schema();
    let fields = schema.fields();
    for class in eq_classes.iter_mut() {
        let columns_to_remove = class
            .iter()
            .filter(|column| {
                let idx = column.index();
                idx >= fields.len() || fields[idx].name() != column.name()
            })
            .cloned()
            .collect::<Vec<_>>();
        for column in columns_to_remove {
            class.remove(&column);
        }
    }
    eq_classes.retain(|props| props.len() > 1);

    output_eq.extend(eq_classes);
}

/// This function applies the given projection to the given ordering
/// equivalence properties to compute the resulting (projected) ordering
/// equivalence properties; e.g.
/// 1) Adding an alias, which can introduce additional ordering equivalence
///    properties, as in Projection(a, a as a1, a as a2) extends global ordering
///    of a to a1 and a2.
/// 2) Truncate the [`OrderingEquivalentClass`]es that are not in the output schema.
pub fn project_ordering_equivalence_properties(
    input_eq: OrderingEquivalenceProperties,
    columns_map: &HashMap<Column, Vec<Column>>,
    output_eq: &mut OrderingEquivalenceProperties,
) {
    let mut eq_classes = input_eq.classes().to_vec();
    for class in eq_classes.iter_mut() {
        class.update_with_aliases(columns_map);
    }

    // Prune columns that no longer is in the schema from from the OrderingEquivalenceProperties.
    let schema = output_eq.schema();
    let fields = schema.fields();
    for class in eq_classes.iter_mut() {
        let columns_to_remove = class
            .iter()
            .filter(|columns| {
                columns.iter().any(|column| {
                    let indices_names = get_column_indices_names(&column.expr);
                    indices_names.into_iter().any(|(idx, name)| {
                        idx >= fields.len() || fields[idx].name() != &name
                    })
                    // let idx = column.col.index();
                    // idx >= fields.len() || fields[idx].name() != column.col.name()
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        for column in columns_to_remove {
            class.remove(&column);
        }
    }
    eq_classes.retain(|props| props.len() > 1);

    output_eq.extend(eq_classes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;

    use datafusion_expr::Operator;
    use std::sync::Arc;

    #[test]
    fn add_equal_conditions_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let mut eq_properties = EquivalenceProperties::new(schema);
        let new_condition = (&Column::new("a", 0), &Column::new("b", 1));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);

        let new_condition = (&Column::new("b", 1), &Column::new("a", 0));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);
        assert_eq!(eq_properties.classes()[0].len(), 2);
        assert!(eq_properties.classes()[0].contains(&Column::new("a", 0)));
        assert!(eq_properties.classes()[0].contains(&Column::new("b", 1)));

        let new_condition = (&Column::new("b", 1), &Column::new("c", 2));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);
        assert_eq!(eq_properties.classes()[0].len(), 3);
        assert!(eq_properties.classes()[0].contains(&Column::new("a", 0)));
        assert!(eq_properties.classes()[0].contains(&Column::new("b", 1)));
        assert!(eq_properties.classes()[0].contains(&Column::new("c", 2)));

        let new_condition = (&Column::new("x", 3), &Column::new("y", 4));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 2);

        let new_condition = (&Column::new("x", 3), &Column::new("a", 0));
        eq_properties.add_equal_conditions(new_condition);
        assert_eq!(eq_properties.classes().len(), 1);
        assert_eq!(eq_properties.classes()[0].len(), 5);
        assert!(eq_properties.classes()[0].contains(&Column::new("a", 0)));
        assert!(eq_properties.classes()[0].contains(&Column::new("b", 1)));
        assert!(eq_properties.classes()[0].contains(&Column::new("c", 2)));
        assert!(eq_properties.classes()[0].contains(&Column::new("x", 3)));
        assert!(eq_properties.classes()[0].contains(&Column::new("y", 4)));

        Ok(())
    }

    #[test]
    fn project_equivalence_properties_test() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let mut input_properties = EquivalenceProperties::new(input_schema);
        let new_condition = (&Column::new("a", 0), &Column::new("b", 1));
        input_properties.add_equal_conditions(new_condition);
        let new_condition = (&Column::new("b", 1), &Column::new("c", 2));
        input_properties.add_equal_conditions(new_condition);

        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::Int64, true),
            Field::new("a2", DataType::Int64, true),
            Field::new("a3", DataType::Int64, true),
            Field::new("a4", DataType::Int64, true),
        ]));

        let mut alias_map = HashMap::new();
        alias_map.insert(
            Column::new("a", 0),
            vec![
                Column::new("a1", 0),
                Column::new("a2", 1),
                Column::new("a3", 2),
                Column::new("a4", 3),
            ],
        );
        let mut out_properties = EquivalenceProperties::new(out_schema);

        project_equivalence_properties(input_properties, &alias_map, &mut out_properties);
        assert_eq!(out_properties.classes().len(), 1);
        assert_eq!(out_properties.classes()[0].len(), 4);
        assert!(out_properties.classes()[0].contains(&Column::new("a1", 0)));
        assert!(out_properties.classes()[0].contains(&Column::new("a2", 1)));
        assert!(out_properties.classes()[0].contains(&Column::new("a3", 2)));
        assert!(out_properties.classes()[0].contains(&Column::new("a4", 3)));

        Ok(())
    }

    #[test]
    fn test_deduplicate_vector() -> Result<()> {
        assert_eq!(deduplicate_vector(vec![1, 1, 2, 3, 3]), vec![1, 2, 3]);
        assert_eq!(
            deduplicate_vector(vec![1, 2, 3, 4, 3, 2, 1, 0]),
            vec![1, 2, 3, 4, 0]
        );
        Ok(())
    }

    #[test]
    fn test_get_elem_position() -> Result<()> {
        assert_eq!(get_elem_position(&[1, 1, 2, 3, 3], &2), Some(2));
        assert_eq!(get_elem_position(&[1, 1, 2, 3, 3], &1), Some(0));
        assert_eq!(get_elem_position(&[1, 1, 2, 3, 3], &5), None);
        Ok(())
    }

    #[test]
    fn test_remove_from_vec() -> Result<()> {
        let mut in_data = vec![1, 1, 2, 3, 3];
        remove_from_vec(&mut in_data, &5);
        assert_eq!(in_data, vec![1, 1, 2, 3, 3]);
        remove_from_vec(&mut in_data, &2);
        assert_eq!(in_data, vec![1, 1, 3, 3]);
        remove_from_vec(&mut in_data, &2);
        assert_eq!(in_data, vec![1, 1, 3, 3]);
        remove_from_vec(&mut in_data, &3);
        assert_eq!(in_data, vec![1, 1, 3]);
        remove_from_vec(&mut in_data, &3);
        assert_eq!(in_data, vec![1, 1]);
        Ok(())
    }

    #[test]
    fn test_column_indices_names() -> Result<()> {
        let expr1 = Arc::new(Column::new("col1", 2)) as _;
        assert_eq!(
            get_column_indices_names(&expr1),
            vec![(2, "col1".to_string())]
        );
        let expr2 = Arc::new(Column::new("col2", 5)) as _;
        assert_eq!(
            get_column_indices_names(&expr2),
            vec![(5, "col2".to_string())]
        );
        let expr3 = Arc::new(BinaryExpr::new(expr1, Operator::Plus, expr2)) as _;
        assert_eq!(
            get_column_indices_names(&expr3),
            vec![(2, "col1".to_string()), (5, "col2".to_string())]
        );
        Ok(())
    }
}
