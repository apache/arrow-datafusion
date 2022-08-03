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

# Invariants

This document enumerates invariants of DataFusion's logical and physical planes
(functions, and nodes). Some of these invariants are currently not enforced.
This document assumes that the reader is familiar with some of the codebase,
including rust arrow's RecordBatch and Array.

## Rational

DataFusion's computational model is built on top of a dynamically typed arrow
object, Array, that offers the interface `Array::as_any` to downcast itself to
its statically typed versions (e.g. `Int32Array`). DataFusion uses
`Array::data_type` to perform the respective downcasting on its physical
operations. DataFusion uses a dynamic type system because the queries being
executed are not always known at compile time: they are only known during the
runtime (or query time) of programs built with DataFusion. This document is
built on top of this principle.

In dynamically typed interfaces, it is up to developers to enforce type
invariances. This document declares some of these invariants, so that users
know what they can expect from a query in DataFusion, and DataFusion developers
know what they need to enforce at the coding level.

## Notation

- Field or physical field: the tuple name, `arrow::DataType` and nullability flag (a bool whether values can be null), represented in this document by `PF(name, type, nullable)`
- Logical field: Field with a relation name. Represented in this document by `LF(relation, name, type, nullable)`
- Projected plan: plan with projection as the root node.
- Logical schema: a vector of logical fields, used by logical plan.
- Physical schema: a vector of physical fields, used by both physical plan and Arrow record batch.

### Logical

#### Function

An object that knows its valid incoming logical fields and how to derive its
output logical field from its arguments' logical fields. A functions' output
field is itself a function of its input fields:

```
logical_field(lf1: LF, lf2: LF, ...) -> LF
```

Examples:

- `plus(a,b) -> LF(None, "{a} Plus {b}", d(a.type,b.type), a.nullable | b.nullable)` where d is the function mapping input types to output type (`get_supertype` in our current implementation).
- `length(a) -> LF(None, "length({a})", u32, a.nullable)`

#### Plan

A tree composed of other plans and functions (e.g. `Projection c1 + c2, c1 - c2 AS sum12; Scan c1 as u32, c2 as u64`)
that knows how to derive its schema.

Certain plans have a frozen schema (e.g. Scan), while others derive their
schema from their child nodes.

#### Column

An identifier in a logical plan consists of field name and relation name.

### Physical

#### Function

An object that knows how to derive its physical field from its arguments'
physical fields, and also how to actually perform the computation on data. A
functions' output physical field is a function of its input physical fields:

```
physical_field(PF1, PF2, ...) -> PF
```

Examples:

- `plus(a,b) -> PF("{a} Plus {b}", d(a.type,b.type), a.nullable | b.nullable)` where d is a complex function (`get_supertype` in our current implementation) whose computation is for each element in the columns, sum the two entries together and return it in the same type as the smallest type of both columns.
- `length(&str) -> PF("length({a})", u32, a.nullable)` whose computation is "count number of bytes in the string".

#### Plan

A tree (e.g. `Projection c1 + c2, c1 - c2 AS sum12; Scan c1 as u32, c2 as u64`)
that knows how to derive its metadata and compute itself.

Note how the physical plane does not know how to derive field names: field
names are solely a property of the logical plane, as they are not needed in the
physical plane.

#### Column

A type of physical node in a physical plan consists of a field name and unique index.

### Data Sources' registry

A map of source name/relation -> Schema plus associated properties necessary to read data from it (e.g. file path).

### Functions' registry

A map of function name -> logical + physical function.

### Physical Planner

A function that knows how to derive a physical plan from a logical plan:

```
plan(LogicalPlan) -> PhysicalPlan
```

### Logical Optimizer

A function that accepts a logical plan and returns an (optimized) logical plan
which computes the same results, but in a more efficient manner:

```
optimize(LogicalPlan) -> LogicalPlan
```

### Physical Optimizer

A function that accepts a physical plan and returns an (optimized) physical
plan which computes the same results, but may differ based on the actual
hardware or execution environment being run:

```
optimize(PhysicalPlan) -> PhysicalPlan
```

### Builder

A function that knows how to build a new logical plan from an existing logical
plan and some extra parameters.

```
build(logical_plan, params...) -> logical_plan
```

## Invariants

The following subsections describe invariants. Since functions' output schema
depends on its arguments' schema (e.g. min, plus), the resulting schema can
only be derived based on a known set of input schemas (TableProvider).
Likewise, schemas of functions depend on the specific registry of functions
registered (e.g. does `my_op` return u32 or u64?). Thus, in this section, the
wording "same schema" is understood to mean "same schema under a given registry
of data sources and functions".

### (relation, name) tuples in logical fields and logical columns are unique

Every logical field's (relation, name) tuple in a logical schema MUST be unique.
Every logical column's (relation, name) tuple in a logical plan MUST be unique.

This invariant guarantees that `SELECT t1.id, t2.id FROM t1 JOIN t2...`
unambiguously selects the field `t1.id` and `t2.id` in a logical schema in the
logical plane.

#### Responsibility

It is the logical builder and optimizer's responsibility to guarantee this
invariant.

#### Validation

Builder and optimizer MUST error if this invariant is violated on any logical
node that creates a new schema (e.g. scan, projection, aggregation, join, etc.).

### Physical schema is consistent with data

The contents of every Array in every RecordBatch in every partition returned by
a physical plan MUST be consistent with RecordBatch's schema, in that every
Array in the RecordBatch must be downcastable to its corresponding type
declared in the RecordBatch.

#### Responsibility

Physical functions MUST guarantee this invariant. This is particularly
important in aggregate functions, whose aggregating type may be different from
the intermediary types during calculations (e.g. sum(i32) -> i64).

#### Validation

Since the validation of this invariant is computationally expensive, execution
contexts CAN validate this invariant. It is acceptable for physical nodes to
`panic!` if their input does not satisfy this invariant.

### Physical schema is consistent in physical functions

The schema of every Array returned by a physical function MUST match the
DataType reported by the physical function itself.

This ensures that when a physical function claims that it returns a type
(e.g. Int32), users can safely downcast its resulting Array to the
corresponding type (e.g. Int32Array), as well as to write data to formats that
have a schema with nullability flag (e.g. parquet).

#### Responsibility

It is the responsibility of the developer that writes a physical function to
guarantee this invariant.

In particular:

- The derived DataType matches the code it uses to build the array for every branch of valid input type combinations.
- The nullability flag matches how the values are built.

#### Validation

Since the validation of this invariant is computationally expensive, execution
contexts CAN validate this invariant.

### The physical schema is invariant under planning

The physical schema derived by a physical plan returned by the planner MUST be
equivalent to the physical schema derived by the logical plan passed to the
planner. Specifically:

```
plan(logical_plan).schema === logical_plan.physical_schema
```

Logical plan's physical schema is defined as logical schema with relation
qualifiers stripped for all logical fields:

```
logical_plan.physical_schema = vector[ strip_relation(f) for f in logical_plan.logical_fields ]
```

This is used to ensure that the physical schema of its (logical) plan is what
it gets in record batches, so that users can rely on the optimized logical plan
to know the resulting physical schema.

Note that since a logical plan can be as simple as a single projection with a
single function, `Projection f(c1,c2)`, a corollary of this is that the
physical schema of every `logical function -> physical function` must be
invariant under planning.

#### Responsibility

Developers of physical and logical plans and planners MUST guarantee this
invariant for every triplet (logical plan, physical plan, conversion rule).

#### Validation

Planners MUST validate this invariant. In particular they MUST return an error
when, during planning, a physical function's derived schema does not match the
logical functions' derived schema.

### The output schema equals the physical plan schema

The schema of every RecordBatch in every partition outputted by a physical plan
MUST be equal to the schema of the physical plan. Specifically:

```
physical_plan.evaluate(batch).schema = physical_plan.schema
```

Together with other invariants, this ensures that the consumers of record
batches do not need to know the output schema of the physical plan; they can
safely rely on the record batch's schema to perform downscaling and naming.

#### Responsibility

Physical nodes MUST guarantee this invariant.

#### Validation

Execution Contexts CAN validate this invariant.

### Logical schema is invariant under logical optimization

The logical schema derived by a projected logical plan returned by the logical
optimizer MUST be equivalent to the logical schema derived by the logical plan
passed to the planner:

```
optimize(logical_plan).schema === logical_plan.schema
```

This is used to ensure that plans can be optimized without jeopardizing future
referencing logical columns (name and index) or assumptions about their
schemas.

#### Responsibility

Logical optimizers MUST guarantee this invariant.

#### Validation

Users of logical optimizers SHOULD validate this invariant.

### Physical schema is invariant under physical optimization

The physical schema derived by a projected physical plan returned by the
physical optimizer MUST match the physical schema derived by the physical plan
passed to the planner:

```
optimize(physical_plan).schema === physical_plan.schema
```

This is used to ensure that plans can be optimized without jeopardizing future
references of logical columns (name and index) or assumptions about their
schemas.

#### Responsibility

Optimizers MUST guarantee this invariant.

#### Validation

Users of optimizers SHOULD validate this invariant.
