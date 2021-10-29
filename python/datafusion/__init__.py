# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pyarrow as pa

from ._internal import DataFrame, ExecutionContext, Expression

__all__ = ["DataFrame", "ExecutionContext", "Expression", "column", "literal"]


def column(value):
    return Expression.column(value)


def literal(value):
    if not isinstance(value, pa.Scalar):
        value = pa.scalar(value)
    return Expression.literal(value)


# def udf():
# """Create a new User Defined Function"""
#     let name = fun.getattr(py, "__qualname__")?.extract::<String>(py)?;
#     create_udf(fun, input_types, return_type, volatility, &name)


# udaf():
#  // let name = accumulator
#         //     .getattr(py, "__qualname__")?
#         //     .extract::<String>(py)?;
