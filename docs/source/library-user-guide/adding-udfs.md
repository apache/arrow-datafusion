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

# Adding User Defined Functions: Scalar/Window/Aggregate

User Defined Functions (UDFs) are functions that can be used in the context of DataFusion execution.

This page covers how to add UDFs to DataFusion. In particular, it covers how to add Scalar, Window, and Aggregate UDFs.

| UDF Type  | Description                                                                                                | Example                                                                                                            |
| --------- | ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Scalar    | A function that takes a row of data and returns a single value.                                            | [simple_udf.rs](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udf.rs)   |
| Window    | A function that takes a row of data and returns a single value, but also has access to the rows around it. | [simple_udwf.rs](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udwf.rs) |
| Aggregate | A function that takes a group of rows and returns a single value.                                          | [simple_udaf.rs](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/simple_udaf.rs) |

First we'll talk about adding an Scalar UDF end-to-end, then we'll talk about the differences between the different types of UDFs.

## Adding a Scalar UDF

A Scalar UDF is a function that takes a row of data and returns a single value. For example, this function takes a single i64 and returns a single i64 with 1 added to it:

<!-- include: library_udfs::add_one -->

```rust
fn add_one(args: &[ArrayRef]) -> Result<ArrayRef> {
    let i64s = as_int64_array(&args[0])?;

    let new_array = i64s
        .iter()
        .map(|array_elem| array_elem.map(|value| value + 1))
        .collect::<Int64Array>();

    Ok(Arc::new(new_array))
}
```

For brevity, we'll skipped some error handling, but e.g. you may want to check that `args.len()` is the expected number of arguments.

This "works" in isolation, i.e. if you have a slice of `ArrayRef`s, you can call `add_one` and it will return a new `ArrayRef` with 1 added to each value.

<!-- include: library_udfs::call_add_one -->

```rust
let input = vec![Some(1), None, Some(3)];
let input = Arc::new(Int64Array::from(input)) as ArrayRef;

let result = add_one(&[input])?;
let result = result
    .as_any()
    .downcast_ref::<Int64Array>()
    .expect("result is Int64Array");

assert_eq!(result, &Int64Array::from(vec![Some(2), None, Some(4)]));
```

The challenge however is that DataFusion doesn't know about this function. We need to register it with DataFusion so that it can be used in the context of a query.

### Registering a Scalar UDF

To register a Scalar UDF, you need to wrap the function implementation in a `ScalarUDF` struct and then register it with the `SessionContext`. DataFusion provides the `create_udf` and `make_scalar_function` helper functions to make this easier.

<!-- include: library_udfs::create_udf -->

```rust
let udf = create_udf(
    "add_one",
    vec![DataType::Int64],
    Arc::new(DataType::Int64),
    Volatility::Immutable,
    make_scalar_function(add_one),
);
```

A few things to note:

- The first argument is the name of the function. This is the name that will be used in SQL queries.
- The second argument is a vector of `DataType`s. This is the list of argument types that the function accepts. I.e. in this case, the function accepts a single `Int64` argument.
- The third argument is the return type of the function. I.e. in this case, the function returns an `Int64`.
- The fourth argument is the volatility of the function. In short, this is used to determine if the function's performance can be optimized in some situations. In this case, the function is `Immutable` because it always returns the same value for the same input. A random number generator would be `Volatile` because it returns a different value for the same input.
- The fifth argument is the function implementation. This is the function that we defined above.

That gives us a `ScalarUDF` that we can register with the `SessionContext`:

<!-- include: library_udfs::register_udf -->

```rust
let ctx = SessionContext::new();
ctx.register_udf(udf);
```

At this point, you can use the `add_one` function in your query:

<!-- include: library_udfs::call_udf -->

```rust
let sql = "SELECT add_one(1)";
let df = ctx.sql(&sql).await?;
```

## Adding a Window UDF

Scalar UDFs are functions that take a row of data and return a single value. Window UDFs are similar, but they also have access to the rows around them. Access to the the proximal rows is helpful, but adds some complexity to the implementation.

Body coming soon.

## Adding an Aggregate UDF

Aggregate UDFs are functions that take a group of rows and return a single value. These are akin to SQL's `SUM` or `COUNT` functions.

Body coming soon.
