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

//! "core" DataFusion functions

mod arrow_cast;
mod arrowtypeof;
mod getfield;
mod named_struct;
mod nullif;
mod nvl;
mod nvl2;
mod r#struct;

// create UDFs
make_udf_function!(arrow_cast::ArrowCastFunc, ARROW_CAST, arrow_cast);
make_udf_function!(nullif::NullIfFunc, NULLIF, nullif);
make_udf_function!(nvl::NVLFunc, NVL, nvl);
make_udf_function!(nvl2::NVL2Func, NVL2, nvl2);
make_udf_function!(arrowtypeof::ArrowTypeOfFunc, ARROWTYPEOF, arrow_typeof);
make_udf_function!(r#struct::StructFunc, STRUCT, r#struct);
make_udf_function!(named_struct::NamedStructFunc, NAMED_STRUCT, named_struct);
make_udf_function!(getfield::GetFieldFunc, GET_FIELD, get_field);

// Export the functions out of this package, both as expr_fn as well as a list of functions
export_functions!(
    (nullif, arg_1 arg_2, "returns NULL if value1 equals value2; otherwise it returns value1. This can be used to perform the inverse operation of the COALESCE expression."),
    (arrow_cast, arg_1 arg_2, "returns arg_1 cast to the `arrow_type` given the second argument. This can be used to cast to a specific `arrow_type`."),
    (nvl, arg_1 arg_2, "returns value2 if value1 is NULL; otherwise it returns value1"),
    (nvl2, arg_1 arg_2 arg_3, "Returns value2 if value1 is not NULL; otherwise, it returns value3."),
    (arrow_typeof, arg_1, "Returns the Arrow type of the input expression."),
    (r#struct, args, "Returns a struct with the given arguments"),
    (named_struct, args, "Returns a struct with the given names and arguments pairs"),
    (get_field, arg_1 arg_2, "Returns the value of the field with the given name from the struct")
);
