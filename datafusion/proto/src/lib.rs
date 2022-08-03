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

//! Serde code for logical plans and expressions.

use datafusion_common::DataFusionError;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/datafusion.rs"));

    #[cfg(feature = "json")]
    include!(concat!(env!("OUT_DIR"), "/datafusion.serde.rs"));
}

pub mod bytes;
pub mod from_proto;
pub mod logical_plan;
pub mod to_proto;

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme_example_test);

impl From<from_proto::Error> for DataFusionError {
    fn from(e: from_proto::Error) -> Self {
        DataFusionError::Plan(e.to_string())
    }
}

impl From<to_proto::Error> for DataFusionError {
    fn from(e: to_proto::Error) -> Self {
        DataFusionError::Plan(e.to_string())
    }
}

#[cfg(test)]
mod roundtrip_tests {
    use super::from_proto::parse_expr;
    use super::protobuf;
    use crate::bytes::{
        logical_plan_from_bytes, logical_plan_from_bytes_with_extension_codec,
        logical_plan_to_bytes, logical_plan_to_bytes_with_extension_codec,
    };
    use crate::logical_plan::LogicalExtensionCodec;
    use arrow::{
        array::ArrayRef,
        datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionMode},
    };
    use datafusion::logical_plan::create_udaf;
    use datafusion::physical_plan::functions::make_scalar_function;
    use datafusion::prelude::{create_udf, CsvReadOptions, SessionContext};
    use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
    use datafusion_expr::expr::GroupingSet;
    use datafusion_expr::logical_plan::{Extension, UserDefinedLogicalNode};
    use datafusion_expr::{
        col, lit, Accumulator, AggregateFunction, AggregateState,
        BuiltinScalarFunction::Sqrt, Expr, LogicalPlan, Volatility,
    };
    use prost::Message;
    use std::any::Any;
    use std::fmt;
    use std::fmt::Debug;
    use std::fmt::Formatter;
    use std::sync::Arc;

    #[cfg(feature = "json")]
    fn roundtrip_json_test(proto: &protobuf::LogicalExprNode) {
        let string = serde_json::to_string(proto).unwrap();
        let back: protobuf::LogicalExprNode = serde_json::from_str(&string).unwrap();
        assert_eq!(proto, &back);
    }

    #[cfg(not(feature = "json"))]
    fn roundtrip_json_test(_proto: &protobuf::LogicalExprNode) {}

    // Given a DataFusion logical Expr, convert it to protobuf and back, using debug formatting to test
    // equality.
    fn roundtrip_expr_test<T, E>(initial_struct: T, ctx: SessionContext)
    where
        for<'a> &'a T: TryInto<protobuf::LogicalExprNode, Error = E> + Debug,
        E: Debug,
    {
        let proto: protobuf::LogicalExprNode = (&initial_struct).try_into().unwrap();
        let round_trip: Expr = parse_expr(&proto, &ctx).unwrap();

        assert_eq!(
            format!("{:?}", &initial_struct),
            format!("{:?}", round_trip)
        );

        roundtrip_json_test(&proto);
    }

    fn new_box_field(name: &str, dt: DataType, nullable: bool) -> Box<Field> {
        Box::new(Field::new(name, dt, nullable))
    }

    #[tokio::test]
    async fn roundtrip_logical_plan() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_csv("t1", "testdata/test.csv", CsvReadOptions::default())
            .await?;
        let scan = ctx.table("t1")?.to_logical_plan()?;
        let topk_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(TopKPlanNode::new(3, scan, col("revenue"))),
        });
        let extension_codec = TopKExtensionCodec {};
        let bytes =
            logical_plan_to_bytes_with_extension_codec(&topk_plan, &extension_codec)?;
        let logical_round_trip =
            logical_plan_from_bytes_with_extension_codec(&bytes, &ctx, &extension_codec)?;
        assert_eq!(
            format!("{:?}", topk_plan),
            format!("{:?}", logical_round_trip)
        );
        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan_with_extension() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_csv("t1", "testdata/test.csv", CsvReadOptions::default())
            .await?;
        let plan = ctx.table("t1")?.to_logical_plan()?;
        let bytes = logical_plan_to_bytes(&plan)?;
        let logical_round_trip = logical_plan_from_bytes(&bytes, &ctx)?;
        assert_eq!(format!("{:?}", plan), format!("{:?}", logical_round_trip));
        Ok(())
    }

    pub mod proto {
        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopKPlanProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,

            #[prost(message, optional, tag = "2")]
            pub expr: ::core::option::Option<crate::protobuf::LogicalExprNode>,
        }

        #[derive(Clone, PartialEq, ::prost::Message)]
        pub struct TopKExecProto {
            #[prost(uint64, tag = "1")]
            pub k: u64,
        }
    }

    struct TopKPlanNode {
        k: usize,
        input: LogicalPlan,
        /// The sort expression (this example only supports a single sort
        /// expr)
        expr: Expr,
    }

    impl TopKPlanNode {
        pub fn new(k: usize, input: LogicalPlan, expr: Expr) -> Self {
            Self { k, input, expr }
        }
    }

    impl Debug for TopKPlanNode {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            self.fmt_for_explain(f)
        }
    }

    impl UserDefinedLogicalNode for TopKPlanNode {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            vec![&self.input]
        }

        /// Schema for TopK is the same as the input
        fn schema(&self) -> &DFSchemaRef {
            self.input.schema()
        }

        fn expressions(&self) -> Vec<Expr> {
            vec![self.expr.clone()]
        }

        /// For example: `TopK: k=10`
        fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TopK: k={}", self.k)
        }

        fn from_template(
            &self,
            exprs: &[Expr],
            inputs: &[LogicalPlan],
        ) -> Arc<dyn UserDefinedLogicalNode> {
            assert_eq!(inputs.len(), 1, "input size inconsistent");
            assert_eq!(exprs.len(), 1, "expression size inconsistent");
            Arc::new(TopKPlanNode {
                k: self.k,
                input: inputs[0].clone(),
                expr: exprs[0].clone(),
            })
        }
    }

    #[derive(Debug)]
    pub struct TopKExtensionCodec {}

    impl LogicalExtensionCodec for TopKExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[LogicalPlan],
            ctx: &SessionContext,
        ) -> Result<Extension, DataFusionError> {
            if let Some((input, _)) = inputs.split_first() {
                let proto = proto::TopKPlanProto::decode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to decode logical plan: {:?}",
                        e
                    ))
                })?;

                if let Some(expr) = proto.expr.as_ref() {
                    let node = TopKPlanNode::new(
                        proto.k as usize,
                        input.clone(),
                        parse_expr(expr, ctx)?,
                    );

                    Ok(Extension {
                        node: Arc::new(node),
                    })
                } else {
                    Err(DataFusionError::Internal(
                        "invalid plan, no expr".to_string(),
                    ))
                }
            } else {
                Err(DataFusionError::Internal(
                    "invalid plan, no input".to_string(),
                ))
            }
        }

        fn try_encode(
            &self,
            node: &Extension,
            buf: &mut Vec<u8>,
        ) -> Result<(), DataFusionError> {
            if let Some(exec) = node.node.as_any().downcast_ref::<TopKPlanNode>() {
                let proto = proto::TopKPlanProto {
                    k: exec.k as u64,
                    expr: Some((&exec.expr).try_into()?),
                };

                proto.encode(buf).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "failed to encode logical plan: {:?}",
                        e
                    ))
                })?;

                Ok(())
            } else {
                Err(DataFusionError::Internal(
                    "unsupported plan type".to_string(),
                ))
            }
        }
    }

    #[test]
    fn scalar_values_error_serialization() {
        let should_fail_on_seralize: Vec<ScalarValue> = vec![
            // Should fail due to inconsistent types
            ScalarValue::List(
                Some(vec![
                    ScalarValue::Int16(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                new_box_field(
                    "item",
                    DataType::List(new_box_field("item", DataType::Int16, true)),
                    true,
                ),
            ),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(32.0)),
                ]),
                new_box_field(
                    "item",
                    DataType::List(new_box_field("item", DataType::Int16, true)),
                    true,
                ),
            ),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::List(
                        None,
                        new_box_field(
                            "item",
                            DataType::List(new_box_field(
                                "level2",
                                DataType::Float32,
                                true,
                            )),
                            true,
                        ),
                    ),
                    ScalarValue::List(
                        Some(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ]),
                        new_box_field(
                            "item",
                            DataType::List(new_box_field(
                                "level2",
                                DataType::Float32,
                                true,
                            )),
                            true,
                        ),
                    ),
                    ScalarValue::List(
                        None,
                        new_box_field(
                            "item",
                            DataType::List(new_box_field(
                                "lists are typed inconsistently",
                                DataType::Int16,
                                true,
                            )),
                            true,
                        ),
                    ),
                ]),
                new_box_field(
                    "item",
                    DataType::List(new_box_field(
                        "level1",
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                        true,
                    )),
                    true,
                ),
            ),
        ];

        for test_case in should_fail_on_seralize.into_iter() {
            let res: std::result::Result<
                super::protobuf::ScalarValue,
                super::to_proto::Error,
            > = (&test_case).try_into();
            assert!(
                res.is_err(),
                "The value {:?} should not have been able to serialize. Serialized to :{:?}",
                test_case, res
            );
        }
    }

    #[test]
    fn round_trip_scalar_values() {
        let should_pass: Vec<ScalarValue> = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::List(None, new_box_field("item", DataType::Boolean, true)),
            ScalarValue::Date32(None),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(Some(f32::MAX)),
            ScalarValue::Float32(Some(f32::MIN)),
            ScalarValue::Float32(Some(-2000.0)),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Float64(Some(f64::MAX)),
            ScalarValue::Float64(Some(f64::MIN)),
            ScalarValue::Float64(Some(-2000.0)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int8(Some(-15)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int16(Some(-15)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(-15)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(-15)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(0)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(0)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(0)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(0)),
            ScalarValue::Utf8(Some(String::from("Test string   "))),
            ScalarValue::LargeUtf8(Some(String::from("Test Large utf8"))),
            ScalarValue::Date32(Some(0)),
            ScalarValue::Date32(Some(i32::MAX)),
            ScalarValue::TimestampNanosecond(Some(0), None),
            ScalarValue::TimestampNanosecond(Some(i64::MAX), None),
            ScalarValue::TimestampNanosecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampNanosecond(None, None),
            ScalarValue::TimestampMicrosecond(Some(0), None),
            ScalarValue::TimestampMicrosecond(Some(i64::MAX), None),
            ScalarValue::TimestampMicrosecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::TimestampMillisecond(Some(0), None),
            ScalarValue::TimestampMillisecond(Some(i64::MAX), None),
            ScalarValue::TimestampMillisecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampMillisecond(None, None),
            ScalarValue::TimestampSecond(Some(0), None),
            ScalarValue::TimestampSecond(Some(i64::MAX), None),
            ScalarValue::TimestampSecond(Some(0), Some("UTC".to_string())),
            ScalarValue::TimestampSecond(None, None),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::Float32(Some(-213.1)),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(5.5)),
                    ScalarValue::Float32(Some(2.0)),
                    ScalarValue::Float32(Some(1.0)),
                ]),
                new_box_field(
                    "item",
                    DataType::List(new_box_field("level1", DataType::Float32, true)),
                    true,
                ),
            ),
            ScalarValue::List(
                Some(vec![
                    ScalarValue::List(
                        None,
                        new_box_field(
                            "item",
                            DataType::List(new_box_field(
                                "level2",
                                DataType::Float32,
                                true,
                            )),
                            true,
                        ),
                    ),
                    ScalarValue::List(
                        Some(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ]),
                        new_box_field(
                            "item",
                            DataType::List(new_box_field(
                                "level2",
                                DataType::Float32,
                                true,
                            )),
                            true,
                        ),
                    ),
                ]),
                new_box_field(
                    "item",
                    DataType::List(new_box_field(
                        "level1",
                        DataType::List(new_box_field("level2", DataType::Float32, true)),
                        true,
                    )),
                    true,
                ),
            ),
        ];

        for test_case in should_pass.into_iter() {
            let proto: super::protobuf::ScalarValue = (&test_case).try_into().unwrap();
            let _roundtrip: ScalarValue = (&proto).try_into().unwrap();
        }
    }

    #[test]
    fn round_trip_scalar_types() {
        let should_pass: Vec<DataType> = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Utf8,
            DataType::LargeUtf8,
            // Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Boolean, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field("Level2", DataType::Date32, true)),
                true,
            )),
        ];

        let should_fail: Vec<DataType> = vec![
            DataType::Null,
            DataType::Float16,
            // Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Decimal(1345, 5431),
            // Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            // Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            // Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ],
                vec![0, 2, 3],
                UnionMode::Dense,
            ),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                    Field::new(
                        "nested_struct",
                        DataType::Struct(vec![
                            Field::new("nullable", DataType::Boolean, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("datatype", DataType::Binary, false),
                        ]),
                        true,
                    ),
                ],
                vec![1, 2, 3],
                UnionMode::Sparse,
            ),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in should_pass.into_iter() {
            let field = Field::new("item", test_case, true);
            let proto: super::protobuf::Field = (&field).try_into().unwrap();
            let roundtrip: Field = (&proto).try_into().unwrap();
            assert_eq!(format!("{:?}", field), format!("{:?}", roundtrip));
        }

        let mut success: Vec<DataType> = Vec::new();
        for test_case in should_fail.into_iter() {
            let proto: std::result::Result<
                super::protobuf::ScalarType,
                super::to_proto::Error,
            > = (&Field::new("item", test_case.clone(), true)).try_into();
            if proto.is_ok() {
                success.push(test_case)
            }
        }
        assert!(
            success.is_empty(),
            "These should have resulted in an error but completed successfully: {:?}",
            success
        );
    }

    #[test]
    fn round_trip_datatype() {
        let test_cases: Vec<DataType> = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            // Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Decimal(1345, 5431),
            // Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            // Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            // Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ],
                vec![7, 5, 3],
                UnionMode::Sparse,
            ),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                    Field::new(
                        "nested_struct",
                        DataType::Struct(vec![
                            Field::new("nullable", DataType::Boolean, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("datatype", DataType::Binary, false),
                        ]),
                        true,
                    ),
                ],
                vec![5, 8, 1],
                UnionMode::Dense,
            ),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in test_cases.into_iter() {
            let proto: super::protobuf::ArrowType = (&test_case).into();
            let roundtrip: DataType = (&proto).try_into().unwrap();
            assert_eq!(format!("{:?}", test_case), format!("{:?}", roundtrip));
        }
    }

    #[test]
    fn roundtrip_null_scalar_values() {
        let test_types = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::Date32(None),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::TimestampNanosecond(None, None),
            ScalarValue::List(
                None,
                Box::new(Field::new("item", DataType::Boolean, false)),
            ),
        ];

        for test_case in test_types.into_iter() {
            let proto_scalar: super::protobuf::ScalarValue =
                (&test_case).try_into().unwrap();
            let returned_scalar: datafusion::scalar::ScalarValue =
                (&proto_scalar).try_into().unwrap();
            assert_eq!(
                format!("{:?}", &test_case),
                format!("{:?}", returned_scalar)
            );
        }
    }

    #[test]
    fn roundtrip_not() {
        let test_expr = Expr::Not(Box::new(lit(1.0_f32)));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_is_null() {
        let test_expr = Expr::IsNull(Box::new(col("id")));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_is_not_null() {
        let test_expr = Expr::IsNotNull(Box::new(col("id")));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_between() {
        let test_expr = Expr::Between {
            expr: Box::new(lit(1.0_f32)),
            negated: true,
            low: Box::new(lit(2.0_f32)),
            high: Box::new(lit(3.0_f32)),
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_case() {
        let test_expr = Expr::Case {
            expr: Some(Box::new(lit(1.0_f32))),
            when_then_expr: vec![(Box::new(lit(2.0_f32)), Box::new(lit(3.0_f32)))],
            else_expr: Some(Box::new(lit(4.0_f32))),
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_cast() {
        let test_expr = Expr::Cast {
            expr: Box::new(lit(1.0_f32)),
            data_type: DataType::Boolean,
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_sort_expr() {
        let test_expr = Expr::Sort {
            expr: Box::new(lit(1.0_f32)),
            asc: true,
            nulls_first: true,
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_negative() {
        let test_expr = Expr::Negative(Box::new(lit(1.0_f32)));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_inlist() {
        let test_expr = Expr::InList {
            expr: Box::new(lit(1.0_f32)),
            list: vec![lit(2.0_f32)],
            negated: true,
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_wildcard() {
        let test_expr = Expr::Wildcard;

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_sqrt() {
        let test_expr = Expr::ScalarFunction {
            fun: Sqrt,
            args: vec![col("col")],
        };
        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_approx_percentile_cont() {
        let test_expr = Expr::AggregateFunction {
            fun: AggregateFunction::ApproxPercentileCont,
            args: vec![col("bananas"), lit(0.42_f32)],
            distinct: false,
        };

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_aggregate_udf() {
        #[derive(Debug)]
        struct Dummy {}

        impl Accumulator for Dummy {
            fn state(&self) -> datafusion::error::Result<Vec<AggregateState>> {
                Ok(vec![])
            }

            fn update_batch(
                &mut self,
                _values: &[ArrayRef],
            ) -> datafusion::error::Result<()> {
                Ok(())
            }

            fn merge_batch(
                &mut self,
                _states: &[ArrayRef],
            ) -> datafusion::error::Result<()> {
                Ok(())
            }

            fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
                Ok(ScalarValue::Float64(None))
            }
        }

        let dummy_agg = create_udaf(
            // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
            "dummy_agg",
            // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
            DataType::Float64,
            // the return type; DataFusion expects this to match the type returned by `evaluate`.
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            // This is the accumulator factory; DataFusion uses it to create new accumulators.
            Arc::new(|| Ok(Box::new(Dummy {}))),
            // This is the description of the state. `state()` must match the types here.
            Arc::new(vec![DataType::Float64, DataType::UInt32]),
        );

        let test_expr = Expr::AggregateUDF {
            fun: Arc::new(dummy_agg.clone()),
            args: vec![lit(1.0_f64)],
        };

        let mut ctx = SessionContext::new();
        ctx.register_udaf(dummy_agg);

        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_scalar_udf() {
        let fn_impl = |args: &[ArrayRef]| Ok(Arc::new(args[0].clone()) as ArrayRef);

        let scalar_fn = make_scalar_function(fn_impl);

        let udf = create_udf(
            "dummy",
            vec![DataType::Utf8],
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            scalar_fn,
        );

        let test_expr = Expr::ScalarUDF {
            fun: Arc::new(udf.clone()),
            args: vec![lit("")],
        };

        let mut ctx = SessionContext::new();
        ctx.register_udf(udf);

        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_grouping_sets() {
        let test_expr = Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a")],
            vec![col("b")],
            vec![col("a"), col("b")],
        ]));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_rollup() {
        let test_expr = Expr::GroupingSet(GroupingSet::Rollup(vec![col("a"), col("b")]));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }

    #[test]
    fn roundtrip_cube() {
        let test_expr = Expr::GroupingSet(GroupingSet::Cube(vec![col("a"), col("b")]));

        let ctx = SessionContext::new();
        roundtrip_expr_test(test_expr, ctx);
    }
}
