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

//! Convert DataFusion logical plan to JIT execution plan.

use datafusion_common::Result;

use crate::api::Assembler;
use crate::{
    api::GeneratedFunction,
    ast::{Expr as JITExpr, I64, PTR_SIZE},
};

fn build_calc_fn(
    assembler: &Assembler,
    jit_expr: JITExpr,
    input_names: Vec<String>,
) -> Result<GeneratedFunction> {
    let mut builder = assembler.new_func_builder("calc_fn");
    for input in &input_names {
        builder = builder.param(format!("{}_array", input), I64);
    }
    let mut builder = builder.param("result", I64).param("len", I64);

    let mut fn_body = builder.enter_block();

    fn_body.declare_as("index", fn_body.lit_i(0))?;
    fn_body.while_block(
        |cond| cond.lt(cond.id("index")?, cond.id("len")?),
        |w| {
            w.declare_as("offset", w.mul(w.id("index")?, w.lit_i(PTR_SIZE as i64))?)?;
            for input in &input_names {
                w.declare_as(
                    format!("{}_ptr", input),
                    w.add(w.id(format!("{}_array", input))?, w.id("offset")?)?,
                )?;
                w.declare_as(input, w.deref(w.id(format!("{}_ptr", input))?, I64)?)?;
            }
            w.declare_as("res_ptr", w.add(w.id("result")?, w.id("offset")?)?)?;
            w.declare_as("res", jit_expr.clone())?;
            w.store(w.id("res")?, w.id("res_ptr")?)?;

            w.assign("index", w.add(w.id("index")?, w.lit_i(1))?)?;
            Ok(())
        },
    )?;

    let gen_func = fn_body.build();
    Ok(gen_func)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{Array, PrimitiveArray},
        datatypes::{DataType, Int64Type},
    };
    use datafusion_common::{DFSchema, DataFusionError};
    use datafusion_expr::Expr as DFExpr;

    use crate::ast::BinaryExpr;

    use super::*;

    fn run_df_expr(
        assembler: &Assembler,
        df_expr: DFExpr,
        schema: Arc<DFSchema>,
        lhs: PrimitiveArray<Int64Type>,
        rhs: PrimitiveArray<Int64Type>,
    ) -> Result<PrimitiveArray<Int64Type>> {
        if lhs.null_count() != 0 || rhs.null_count() != 0 {
            return Err(DataFusionError::NotImplemented(
                "Computing on nullable array not yet supported".to_string(),
            ));
        }
        if lhs.len() != rhs.len() {
            return Err(DataFusionError::NotImplemented(
                "Computing on different length arrays not yet supported".to_string(),
            ));
        }

        let input_fields = schema.field_names();
        let jit_expr: JITExpr = (df_expr, schema).try_into()?;

        let len = lhs.len();
        let result: Vec<i64> = Vec::with_capacity(len);

        let gen_func = build_calc_fn(assembler, jit_expr, input_fields)?;

        println!("{}", format!("{}", &gen_func));

        todo!()
    }

    #[test]
    fn mvp_driver() {
        let array_a: PrimitiveArray<Int64Type> =
            PrimitiveArray::from_iter_values((0..10).map(|x| x + 1));
        let array_b: PrimitiveArray<Int64Type> =
            PrimitiveArray::from_iter_values((0..10).map(|x| x + 1));

        let df_expr = datafusion_expr::col("a") + datafusion_expr::col("b");
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    datafusion_common::DFField::new(
                        Some("table1"),
                        "a",
                        DataType::Int64,
                        false,
                    ),
                    datafusion_common::DFField::new(
                        Some("table1"),
                        "b",
                        DataType::Int64,
                        false,
                    ),
                ],
                std::collections::HashMap::new(),
            )
            .unwrap(),
        );

        let assembler = Assembler::default();
        let result = run_df_expr(&assembler, df_expr, schema, array_a, array_b);
    }

    #[test]
    fn calc_fn_builder() {
        let expr = JITExpr::Binary(BinaryExpr::Add(
            Box::new(JITExpr::Identifier("table1.a".to_string(), I64)),
            Box::new(JITExpr::Identifier("table1.b".to_string(), I64)),
        ));
        let fields = vec!["table1.a".to_string(), "table1.b".to_string()];

        let expected = r#"fn calc_fn_0(table1.a_array: i64, table1.b_array: i64, result: i64, len: i64) -> () {
    let index: i64;
    index = 0;
    while index < len {
        let offset: i64;
        offset = index * 8;
        let table1.a_ptr: i64;
        table1.a_ptr = table1.a_array + offset;
        let table1.a: i64;
        table1.a = *(table1.a_ptr);
        let table1.b_ptr: i64;
        table1.b_ptr = table1.b_array + offset;
        let table1.b: i64;
        table1.b = *(table1.b_ptr);
        let res_ptr: i64;
        res_ptr = result + offset;
        let res: i64;
        res = table1.a + table1.b;
        *(res_ptr) = res
        index = index + 1;
    }
}"#;

        let assembler = Assembler::default();
        let gen_func = build_calc_fn(&assembler, expr, fields).unwrap();
        assert_eq!(format!("{}", &gen_func), expected);
    }
}
