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

//! "unicode" DataFusion functions

use std::sync::Arc;

use datafusion_expr::ScalarUDF;

mod character_length;
mod left;
mod lpad;
mod reverse;
mod right;
mod rpad;

// create UDFs
make_udf_function!(
    character_length::CharacterLengthFunc,
    CHARACTER_LENGTH,
    character_length
);
make_udf_function!(left::LeftFunc, LEFT, left);
make_udf_function!(lpad::LPadFunc, LPAD, lpad);
make_udf_function!(right::RightFunc, RIGHT, right);
make_udf_function!(reverse::ReverseFunc, REVERSE, reverse);
make_udf_function!(rpad::RPadFunc, RPAD, rpad);

pub mod expr_fn {
    use datafusion_expr::Expr;

    #[doc = "the number of characters in the `string`"]
    pub fn char_length(string: Expr) -> Expr {
        character_length(string)
    }

    #[doc = "the number of characters in the `string`"]
    pub fn character_length(string: Expr) -> Expr {
        super::character_length().call(vec![string])
    }

    #[doc = "the number of characters in the `string`"]
    pub fn length(string: Expr) -> Expr {
        character_length(string)
    }

    #[doc = "returns the first `n` characters in the `string`"]
    pub fn left(string: Expr, n: Expr) -> Expr {
        super::left().call(vec![string, n])
    }

    #[doc = "fill up a string to the length by prepending the characters"]
    pub fn lpad(args: Vec<Expr>) -> Expr {
        super::lpad().call(args)
    }

    #[doc = "reverses the `string`"]
    pub fn reverse(string: Expr) -> Expr {
        super::reverse().call(vec![string])
    }

    #[doc = "returns the last `n` characters in the `string`"]
    pub fn right(string: Expr, n: Expr) -> Expr {
        super::right().call(vec![string, n])
    }

    #[doc = "fill up a string to the length by appending the characters"]
    pub fn rpad(args: Vec<Expr>) -> Expr {
        super::rpad().call(args)
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        character_length(),
        left(),
        lpad(),
        reverse(),
        right(),
        rpad(),
    ]
}
