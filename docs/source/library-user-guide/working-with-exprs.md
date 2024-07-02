<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Working with `Expr`s

<!-- https://github.com/apache/datafusion/issues/7304 -->

`Expr` is short for "expression". It is a core abstraction in DataFusion for representing a computation, and follows the standard "expression tree" abstraction found in most compilers and databases.

For example, the SQL expression `a + b` would be represented as an `Expr` with a `BinaryExpr` variant. A `BinaryExpr` has a left and right `Expr` and an operator.

As another example, the SQL expression `a + b * c` would be represented as an `Expr` with a `BinaryExpr` variant. The left `Expr` would be `a` and the right `Expr` would be another `BinaryExpr` with a left `Expr` of `b` and a right `Expr` of `c`. As a classic expression tree, this would look like:

```text
            ┌────────────────────┐
            │     BinaryExpr     │
            │       op: +        │
            └────────────────────┘
                   ▲     ▲
           ┌───────┘     └────────────────┐
           │                              │
┌────────────────────┐         ┌────────────────────┐
│     Expr::Col      │         │     BinaryExpr     │
│       col: a       │         │       op: *        │
└────────────────────┘         └────────────────────┘
                                        ▲    ▲
                               ┌────────┘    └─────────┐
                               │                       │
                    ┌────────────────────┐  ┌────────────────────┐
                    │     Expr::Col      │  │     Expr::Col      │
                    │       col: b       │  │       col: c       │
                    └────────────────────┘  └────────────────────┘
```

As the writer of a library, you can use `Expr`s to represent computations that you want to perform. This guide will walk you through how to make your own scalar UDF as an `Expr` and how to rewrite `Expr`s to inline the simple UDF.

## Creating and Evaluating `Expr`s

Please see [expr_api.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs) for well commented code for creating, evaluating, simplifying, and analyzing `Expr`s.

## A Scalar UDF Example

We'll use a `ScalarUDF` expression as our example. This necessitates implementing an actual UDF, and for ease we'll use the same example from the [adding UDFs](./adding-udfs.md) guide.

So assuming you've written that function, you can use it to create an `Expr`:

```rust
let add_one_udf = create_udf(
    "add_one",
    vec![DataType::Int64],
    Arc::new(DataType::Int64),
    Volatility::Immutable,
    make_scalar_function(add_one),  // <-- the function we wrote
);

// make the expr `add_one(5)`
let expr = add_one_udf.call(vec![lit(5)]);

// make the expr `add_one(my_column)`
let expr = add_one_udf.call(vec![col("my_column")]);
```

If you'd like to learn more about `Expr`s, before we get into the details of creating and rewriting them, you can read the [expression user-guide](./../user-guide/expressions.md).

## Rewriting `Expr`s

There are several examples of rewriting and working with `Exprs`:

- [expr_api.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/expr_api.rs)
- [analyzer_rule.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/analyzer_rule.rs)
- [optimizer_rule.rs](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/optimizer_rule.rs)

Rewriting Expressions is the process of taking an `Expr` and transforming it into another `Expr`. This is useful for a number of reasons, including:

- Simplifying `Expr`s to make them easier to evaluate
- Optimizing `Expr`s to make them faster to evaluate
- Converting `Expr`s to other forms, e.g. converting a `BinaryExpr` to a `CastExpr`

In our example, we'll use rewriting to update our `add_one` UDF, to be rewritten as a `BinaryExpr` with a `Literal` of 1. We're effectively inlining the UDF.

### Rewriting with `transform`

To implement the inlining, we'll need to write a function that takes an `Expr` and returns a `Result<Expr>`. If the expression is _not_ to be rewritten `Transformed::no` is used to wrap the original `Expr`. If the expression _is_ to be rewritten, `Transformed::yes` is used to wrap the new `Expr`.

```rust
fn rewrite_add_one(expr: Expr) -> Result<Expr> {
    expr.transform(&|expr| {
        Ok(match expr {
            Expr::ScalarUDF(scalar_fun) if scalar_fun.fun.name == "add_one" => {
                let input_arg = scalar_fun.args[0].clone();
                let new_expression = input_arg + lit(1i64);

                Transformed::yes(new_expression)
            }
            _ => Transformed::no(expr),
        })
    })
}
```

### Creating an `OptimizerRule`

In DataFusion, an `OptimizerRule` is a trait that supports rewriting`Expr`s that appear in various parts of the `LogicalPlan`. It follows DataFusion's general mantra of trait implementations to drive behavior.

We'll call our rule `AddOneInliner` and implement the `OptimizerRule` trait. The `OptimizerRule` trait has two methods:

- `name` - returns the name of the rule
- `try_optimize` - takes a `LogicalPlan` and returns an `Option<LogicalPlan>`. If the rule is able to optimize the plan, it returns `Some(LogicalPlan)` with the optimized plan. If the rule is not able to optimize the plan, it returns `None`.

```rust
struct AddOneInliner {}

impl OptimizerRule for AddOneInliner {
    fn name(&self) -> &str {
        "add_one_inliner"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // Map over the expressions and rewrite them
        let new_expressions = plan
            .expressions()
            .into_iter()
            .map(|expr| rewrite_add_one(expr))
            .collect::<Result<Vec<_>>>()?;

        let inputs = plan.inputs().into_iter().cloned().collect::<Vec<_>>();

        let plan = plan.with_new_exprs(&new_expressions, &inputs);

        plan.map(Some)
    }
}
```

Note the use of `rewrite_add_one` which is mapped over `plan.expressions()` to rewrite the expressions, then `plan.with_new_exprs` is used to create a new `LogicalPlan` with the rewritten expressions.

We're almost there. Let's just test our rule works properly.

## Testing the Rule

Testing the rule is fairly simple, we can create a SessionState with our rule and then create a DataFrame and run a query. The logical plan will be optimized by our rule.

```rust
use datafusion::prelude::*;

let rules = Arc::new(AddOneInliner {});
let state = ctx.state().with_optimizer_rules(vec![rules]);

let ctx = SessionContext::with_state(state);
ctx.register_udf(add_one);

let sql = "SELECT add_one(1) AS added_one";
let plan = ctx.sql(sql).await?.logical_plan();

println!("{:?}", plan);
```

This results in the following output:

```text
Projection: Int64(1) + Int64(1) AS added_one
  EmptyRelation
```

I.e. the `add_one` UDF has been inlined into the projection.

## Getting the data type of the expression

The `arrow::datatypes::DataType` of the expression can be obtained by calling the `get_type` given something that implements `Expr::Schemable`, for example a `DFschema` object:

```rust
use arrow_schema::DataType;
use datafusion::common::{DFField, DFSchema};
use datafusion::logical_expr::{col, ExprSchemable};
use std::collections::HashMap;

let expr = col("c1") + col("c2");
let schema = DFSchema::new_with_metadata(
    vec![
        DFField::new_unqualified("c1", DataType::Int32, true),
        DFField::new_unqualified("c2", DataType::Float32, true),
    ],
    HashMap::new(),
)
.unwrap();
print!("type = {}", expr.get_type(&schema).unwrap());
```

This results in the following output:

```text
type = Float32
```

## Conclusion

In this guide, we've seen how to create `Expr`s programmatically and how to rewrite them. This is useful for simplifying and optimizing `Expr`s. We've also seen how to test our rule to ensure it works properly.
