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
#![deny(clippy::clone_on_ref_ptr)]

//! Serialize / Deserialize DataFusion Plans to bytes
//!
//! This crate provides support for serializing and deserializing the
//! following structures to and from bytes:
//!
//! 1. [`LogicalPlan`]'s (including [`Expr`]),
//! 2. [`ExecutionPlan`]s (including [`PhysicalExpr`])
//!
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan
//! [`Expr`]: datafusion_expr::Expr
//! [`ExecutionPlan`]: datafusion::physical_plan::ExecutionPlan
//! [`PhysicalExpr`]: datafusion::physical_expr::PhysicalExpr
//!
//! Internally, this crate is implemented by converting the plans to [protocol
//! buffers] using [prost].
//!
//! [protocol buffers]: https://developers.google.com/protocol-buffers
//! [prost]: https://docs.rs/prost/latest/prost/
//!
//! # Version Compatibility
//!
//! The serialized form are not guaranteed to be compatible across
//! DataFusion versions. A plan serialized with one version of DataFusion
//! may not be able to deserialized with a different version.
//!
//! # See Also
//!
//! The binary format created by this crate supports the full range of DataFusion
//! plans, but is DataFusion specific. See [datafusion-substrait] for a crate
//! which can encode many DataFusion plans using the [substrait.io] standard.
//!
//! [datafusion-substrait]: https://docs.rs/datafusion-substrait/latest/datafusion_substrait
//! [substrait.io]: https://substrait.io
//!
//! # Example: Serializing [`Expr`]s
//! ```
//! # use datafusion_common::Result;
//! # use datafusion_expr::{col, lit, Expr};
//! # use datafusion_proto::bytes::Serializeable;
//! # fn main() -> Result<()>{
//!  // Create a new `Expr` a < 32
//!  let expr = col("a").lt(lit(5i32));
//!
//!  // Convert it to bytes (for sending over the network, etc.)
//!  let bytes = expr.to_bytes()?;
//!
//!  // Decode bytes from somewhere (over network, etc.) back to Expr
//!  let decoded_expr = Expr::from_bytes(&bytes)?;
//!  assert_eq!(expr, decoded_expr);
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Serializing [`LogicalPlan`]s
//! ```
//! # use datafusion::prelude::*;
//! # use datafusion_common::Result;
//! # use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};
//! # #[tokio::main]
//! # async fn main() -> Result<()>{
//!  // Create a plan that scans table 't'
//!  let ctx = SessionContext::new();
//!  ctx.register_csv("t1", "tests/testdata/test.csv", CsvReadOptions::default()).await?;
//!  let plan = ctx.table("t1").await?.into_optimized_plan()?;
//!
//!  // Convert the plan into bytes (for sending over the network, etc.)
//!  let bytes = logical_plan_to_bytes(&plan)?;
//!
//!  // Decode bytes from somewhere (over network, etc.) back to LogicalPlan
//!  let logical_round_trip = logical_plan_from_bytes(&bytes, &ctx)?;
//!  assert_eq!(format!("{:?}", plan), format!("{:?}", logical_round_trip));
//! # Ok(())
//! # }
//! ```
//! # Example: Serializing [`ExecutionPlan`]s
//!
//! ```
//! # use datafusion::prelude::*;
//! # use datafusion_common::Result;
//! # use datafusion_proto::bytes::{physical_plan_from_bytes,physical_plan_to_bytes};
//! # #[tokio::main]
//! # async fn main() -> Result<()>{
//!  // Create a plan that scans table 't'
//!  let ctx = SessionContext::new();
//!  ctx.register_csv("t1", "tests/testdata/test.csv", CsvReadOptions::default()).await?;
//!  let physical_plan = ctx.table("t1").await?.create_physical_plan().await?;
//!
//!  // Convert the plan into bytes (for sending over the network, etc.)
//!  let bytes = physical_plan_to_bytes(physical_plan.clone())?;
//!
//!  // Decode bytes from somewhere (over network, etc.) back to ExecutionPlan
//!  let physical_round_trip = physical_plan_from_bytes(&bytes, &ctx)?;
//!  assert_eq!(format!("{:?}", physical_plan), format!("{:?}", physical_round_trip));
//! # Ok(())
//! # }
//! ```
pub mod bytes;
pub mod common;
pub mod generated;
pub mod logical_plan;
pub mod physical_plan;

pub mod protobuf {
    pub use crate::generated::datafusion::*;
    pub use datafusion_proto_common::common::proto_error;
    pub use datafusion_proto_common::protobuf_common::{
        ArrowOptions, ArrowType, AvroFormat, AvroOptions, CsvFormat, DfSchema,
        EmptyMessage, Field, JoinSide, ParquetFormat, ScalarValue, Schema,
    };
    pub use datafusion_proto_common::{FromProtoError, ToProtoError};
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);
