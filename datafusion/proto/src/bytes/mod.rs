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

//! Serialization / Deserialization to Bytes
use crate::logical_plan::{
    self, AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use crate::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use crate::protobuf;
use datafusion_common::{plan_datafusion_err, DataFusionError, Result};
use datafusion_expr::{
    create_udaf, create_udf, create_udwf, AggregateUDF, Expr, LogicalPlan, Volatility,
    WindowUDF,
};
use prost::{
    bytes::{Bytes, BytesMut},
    Message,
};
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_execution::registry::FunctionRegistry;
use datafusion_physical_plan::ExecutionPlan;

mod registry;

/// Encodes something (such as [`Expr`]) to/from a stream of
/// bytes.
///
/// ```
/// use datafusion_expr::{col, lit, Expr};
/// use datafusion_proto::bytes::Serializeable;
///
/// // Create a new `Expr` a < 32
/// let expr = col("a").lt(lit(5i32));
///
/// // Convert it to an opaque form
/// let bytes = expr.to_bytes().unwrap();
///
/// // Decode bytes from somewhere (over network, etc.)
/// let decoded_expr = Expr::from_bytes(&bytes).unwrap();
/// assert_eq!(expr, decoded_expr);
/// ```
pub trait Serializeable: Sized {
    /// Convert `self` to an opaque byte stream
    fn to_bytes(&self) -> Result<Bytes>;

    /// Convert `bytes` (the output of [`to_bytes`]) back into an
    /// object. This will error if the serialized bytes contain any
    /// user defined functions, in which case use
    /// [`from_bytes_with_registry`]
    ///
    /// [`to_bytes`]: Self::to_bytes
    /// [`from_bytes_with_registry`]: Self::from_bytes_with_registry
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::from_bytes_with_registry(bytes, &registry::NoRegistry {})
    }

    /// Convert `bytes` (the output of [`to_bytes`]) back into an
    /// object resolving user defined functions with the specified
    /// `registry`
    ///
    /// [`to_bytes`]: Self::to_bytes
    fn from_bytes_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self>;
}

impl Serializeable for Expr {
    fn to_bytes(&self) -> Result<Bytes> {
        let mut buffer = BytesMut::new();
        let protobuf: protobuf::LogicalExprNode = self
            .try_into()
            .map_err(|e| plan_datafusion_err!("Error encoding expr as protobuf: {e}"))?;

        protobuf
            .encode(&mut buffer)
            .map_err(|e| plan_datafusion_err!("Error encoding protobuf as bytes: {e}"))?;

        let bytes: Bytes = buffer.into();

        // the produced byte stream may lead to "recursion limit" errors, see
        // https://github.com/apache/arrow-datafusion/issues/3968
        // Until the underlying prost issue ( https://github.com/tokio-rs/prost/issues/736 ) is fixed, we try to
        // deserialize the data here and check for errors.
        //
        // Need to provide some placeholder registry because the stream may contain UDFs
        struct PlaceHolderRegistry;

        impl FunctionRegistry for PlaceHolderRegistry {
            fn udfs(&self) -> std::collections::HashSet<String> {
                std::collections::HashSet::default()
            }

            fn udf(&self, name: &str) -> Result<Arc<datafusion_expr::ScalarUDF>> {
                Ok(Arc::new(create_udf(
                    name,
                    vec![],
                    Arc::new(arrow::datatypes::DataType::Null),
                    Volatility::Immutable,
                    Arc::new(|_| unimplemented!()),
                )))
            }

            fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
                Ok(Arc::new(create_udaf(
                    name,
                    vec![arrow::datatypes::DataType::Null],
                    Arc::new(arrow::datatypes::DataType::Null),
                    Volatility::Immutable,
                    Arc::new(|_| unimplemented!()),
                    Arc::new(vec![]),
                )))
            }

            fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
                Ok(Arc::new(create_udwf(
                    name,
                    arrow::datatypes::DataType::Null,
                    Arc::new(arrow::datatypes::DataType::Null),
                    Volatility::Immutable,
                    Arc::new(|| unimplemented!()),
                )))
            }
        }
        Expr::from_bytes_with_registry(&bytes, &PlaceHolderRegistry)?;

        Ok(bytes)
    }

    fn from_bytes_with_registry(
        bytes: &[u8],
        registry: &dyn FunctionRegistry,
    ) -> Result<Self> {
        let protobuf = protobuf::LogicalExprNode::decode(bytes)
            .map_err(|e| plan_datafusion_err!("Error decoding expr as protobuf: {e}"))?;

        logical_plan::from_proto::parse_expr(&protobuf, registry)
            .map_err(|e| plan_datafusion_err!("Error parsing protobuf into Expr: {e}"))
    }
}

/// Serialize a LogicalPlan as bytes
pub fn logical_plan_to_bytes(plan: &LogicalPlan) -> Result<Bytes> {
    let extension_codec = DefaultLogicalExtensionCodec {};
    logical_plan_to_bytes_with_extension_codec(plan, &extension_codec)
}

/// Serialize a LogicalPlan as JSON
#[cfg(feature = "json")]
pub fn logical_plan_to_json(plan: &LogicalPlan) -> Result<String> {
    let extension_codec = DefaultLogicalExtensionCodec {};
    let protobuf =
        protobuf::LogicalPlanNode::try_from_logical_plan(plan, &extension_codec)
            .map_err(|e| plan_datafusion_err!("Error serializing plan: {e}"))?;
    serde_json::to_string(&protobuf)
        .map_err(|e| plan_datafusion_err!("Error serializing plan: {e}"))
}

/// Serialize a LogicalPlan as bytes, using the provided extension codec
pub fn logical_plan_to_bytes_with_extension_codec(
    plan: &LogicalPlan,
    extension_codec: &dyn LogicalExtensionCodec,
) -> Result<Bytes> {
    let protobuf =
        protobuf::LogicalPlanNode::try_from_logical_plan(plan, extension_codec)?;
    let mut buffer = BytesMut::new();
    protobuf
        .encode(&mut buffer)
        .map_err(|e| plan_datafusion_err!("Error encoding protobuf as bytes: {e}"))?;
    Ok(buffer.into())
}

/// Deserialize a LogicalPlan from JSON
#[cfg(feature = "json")]
pub fn logical_plan_from_json(json: &str, ctx: &SessionContext) -> Result<LogicalPlan> {
    let back: protobuf::LogicalPlanNode = serde_json::from_str(json)
        .map_err(|e| plan_datafusion_err!("Error serializing plan: {e}"))?;
    let extension_codec = DefaultLogicalExtensionCodec {};
    back.try_into_logical_plan(ctx, &extension_codec)
}

/// Deserialize a LogicalPlan from bytes
pub fn logical_plan_from_bytes(
    bytes: &[u8],
    ctx: &SessionContext,
) -> Result<LogicalPlan> {
    let extension_codec = DefaultLogicalExtensionCodec {};
    logical_plan_from_bytes_with_extension_codec(bytes, ctx, &extension_codec)
}

/// Deserialize a LogicalPlan from bytes
pub fn logical_plan_from_bytes_with_extension_codec(
    bytes: &[u8],
    ctx: &SessionContext,
    extension_codec: &dyn LogicalExtensionCodec,
) -> Result<LogicalPlan> {
    let protobuf = protobuf::LogicalPlanNode::decode(bytes)
        .map_err(|e| plan_datafusion_err!("Error decoding expr as protobuf: {e}"))?;
    protobuf.try_into_logical_plan(ctx, extension_codec)
}

/// Serialize a PhysicalPlan as bytes
pub fn physical_plan_to_bytes(plan: Arc<dyn ExecutionPlan>) -> Result<Bytes> {
    let extension_codec = DefaultPhysicalExtensionCodec {};
    physical_plan_to_bytes_with_extension_codec(plan, &extension_codec)
}

/// Serialize a PhysicalPlan as JSON
#[cfg(feature = "json")]
pub fn physical_plan_to_json(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
    let extension_codec = DefaultPhysicalExtensionCodec {};
    let protobuf =
        protobuf::PhysicalPlanNode::try_from_physical_plan(plan, &extension_codec)
            .map_err(|e| plan_datafusion_err!("Error serializing plan: {e}"))?;
    serde_json::to_string(&protobuf)
        .map_err(|e| plan_datafusion_err!("Error serializing plan: {e}"))
}

/// Serialize a PhysicalPlan as bytes, using the provided extension codec
pub fn physical_plan_to_bytes_with_extension_codec(
    plan: Arc<dyn ExecutionPlan>,
    extension_codec: &dyn PhysicalExtensionCodec,
) -> Result<Bytes> {
    let protobuf =
        protobuf::PhysicalPlanNode::try_from_physical_plan(plan, extension_codec)?;
    let mut buffer = BytesMut::new();
    protobuf
        .encode(&mut buffer)
        .map_err(|e| plan_datafusion_err!("Error encoding protobuf as bytes: {e}"))?;
    Ok(buffer.into())
}

/// Deserialize a PhysicalPlan from JSON
#[cfg(feature = "json")]
pub fn physical_plan_from_json(
    json: &str,
    ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let back: protobuf::PhysicalPlanNode = serde_json::from_str(json)
        .map_err(|e| plan_datafusion_err!("Error serializing plan: {e}"))?;
    let extension_codec = DefaultPhysicalExtensionCodec {};
    back.try_into_physical_plan(ctx, &ctx.runtime_env(), &extension_codec)
}

/// Deserialize a PhysicalPlan from bytes
pub fn physical_plan_from_bytes(
    bytes: &[u8],
    ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let extension_codec = DefaultPhysicalExtensionCodec {};
    physical_plan_from_bytes_with_extension_codec(bytes, ctx, &extension_codec)
}

/// Deserialize a PhysicalPlan from bytes
pub fn physical_plan_from_bytes_with_extension_codec(
    bytes: &[u8],
    ctx: &SessionContext,
    extension_codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let protobuf = protobuf::PhysicalPlanNode::decode(bytes)
        .map_err(|e| plan_datafusion_err!("Error decoding expr as protobuf: {e}"))?;
    protobuf.try_into_physical_plan(ctx, &ctx.runtime_env(), extension_codec)
}
