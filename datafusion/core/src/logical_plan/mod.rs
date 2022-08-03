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

//! This is a legacy module that only contains re-exports of other modules

mod expr;
pub mod plan;
pub mod window_frames;

pub use crate::datasource::{provider_as_source, source_as_provider};
pub use crate::execution::FunctionRegistry;
pub use datafusion_common::{
    Column, DFField, DFSchema, DFSchemaRef, ExprSchema, ToDFSchema,
};
pub use datafusion_expr::{
    abs, acos, and, approx_distinct, approx_percentile_cont, array, ascii, asin, atan,
    atan2, avg, bit_length, btrim, call_fn, case, ceil, character_length, chr, coalesce,
    col, combine_filters, concat, concat_expr, concat_ws, concat_ws_expr, cos, count,
    count_distinct, create_udaf, create_udf, date_part, date_trunc, digest, exists, exp,
    expr_rewriter,
    expr_rewriter::{
        normalize_col, normalize_col_with_schemas, normalize_cols, replace_col,
        rewrite_sort_cols_by_aggs, unnormalize_col, unnormalize_cols, ExprRewritable,
        ExprRewriter, RewriteRecursion,
    },
    expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
    floor, from_unixtime, in_list, in_subquery, initcap, left, length, lit,
    lit_timestamp_nano, ln, log10, log2,
    logical_plan::{
        builder::{
            build_join_schema, union_with_alias, LogicalPlanBuilder, UNNAMED_TABLE,
        },
        CreateCatalog, CreateCatalogSchema, CreateExternalTable, CreateMemoryTable,
        CreateView, CrossJoin, DropTable, EmptyRelation, FileType, JoinConstraint,
        JoinType, Limit, LogicalPlan, Partitioning, PlanType, PlanVisitor, Repartition,
        StringifiedPlan, Subquery, TableScan, ToStringifiedPlan, Union,
        UserDefinedLogicalNode, Values,
    },
    lower, lpad, ltrim, max, md5, min, not_exists, not_in_subquery, now, now_expr,
    nullif, octet_length, or, power, random, regexp_match, regexp_replace, repeat,
    replace, reverse, right, round, rpad, rtrim, scalar_subquery, sha224, sha256, sha384,
    sha512, signum, sin, split_part, sqrt, starts_with, strpos, substr, sum, tan, to_hex,
    to_timestamp_micros, to_timestamp_millis, to_timestamp_nanos, to_timestamp_seconds,
    translate, trim, trunc, unalias, upper, when, Expr, ExprSchemable, Literal, Operator,
};
pub use datafusion_optimizer::expr_simplifier::{ExprSimplifiable, SimplifyInfo};
