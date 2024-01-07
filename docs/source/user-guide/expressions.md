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

# Expression API

DataFrame methods such as `select` and `filter` accept one or more logical expressions and there are many functions
available for creating logical expressions. These are documented below.

:::{tip}
Most functions and methods may receive and return an `Expr`, which can be chained together using a fluent-style API:

```rust
// create the expression `(a > 6) AND (b < 7)`
col("a").gt(lit(6)).and(col("b").lt(lit(7)))
```

:::

## Identifiers

| Syntax     | Description                                  |
| ---------- | -------------------------------------------- |
| col(ident) | Reference a column in a dataframe `col("a")` |

:::{note}
ident
: A type which implement `Into<Column>` trait
:::

## Literal Values

| Syntax     | Description                                        |
| ---------- | -------------------------------------------------- |
| lit(value) | Literal value such as `lit(123)` or `lit("hello")` |

:::{note}
value
: A type which implement `Literal`
:::

## Boolean Expressions

| Syntax              | Description |
| ------------------- | ----------- |
| and(x, y), x.and(y) | Logical AND |
| or(x, y), x.or(y)   | Logical OR  |
| !x, not(x), x.not() | Logical NOT |

:::{note}
`!` is a bitwise or logical complement operator in Rust, but it only works as a logical NOT in expression API.
:::

:::{note}
Since `&&` and `||` are existed as logical operators in Rust, but those are not overloadable and not works with expression API.
:::

## Bitwise Expressions

| Syntax                                      | Description |
| ------------------------------------------- | ----------- |
| x & y, bitwise_and(x, y), x.bitand(y)       | AND         |
| x \| y, bitwise_or(x, y), x.bitor(y)        | OR          |
| x ^ y, bitwise_xor(x, y), x.bitxor(y)       | XOR         |
| x << y, bitwise_shift_left(x, y), x.shl(y)  | Left shift  |
| x >> y, bitwise_shift_right(x, y), x.shr(y) | Right shift |

## Comparison Expressions

| Syntax      | Description           |
| ----------- | --------------------- |
| x.eq(y)     | Equal                 |
| x.not_eq(y) | Not Equal             |
| x.gt(y)     | Greater Than          |
| x.gt_eq(y)  | Greater Than or Equal |
| x.lt(y)     | Less Than             |
| x.lt_eq(y)  | Less Than or Equal    |

:::{note}
Comparison operators (`<`, `<=`, `==`, `>=`, `>`) could be overloaded by the `PartialOrd` and `PartialEq` trait in Rust,
but these operators always return a `bool` which makes them not work with the expression API.
:::

## Arithmetic Expressions

| Syntax           | Description    |
| ---------------- | -------------- |
| x + y, x.add(y)  | Addition       |
| x - y, x.sub(y)  | Subtraction    |
| x \* y, x.mul(y) | Multiplication |
| x / y, x.div(y)  | Division       |
| x % y, x.rem(y)  | Remainder      |
| -x, x.neg()      | Negation       |

## Math Functions

| Syntax                | Description                                       |
| --------------------- | ------------------------------------------------- |
| abs(x)                | absolute value                                    |
| acos(x)               | inverse cosine                                    |
| acosh(x)              | inverse hyperbolic cosine                         |
| asin(x)               | inverse sine                                      |
| asinh(x)              | inverse hyperbolic sine                           |
| atan(x)               | inverse tangent                                   |
| atanh(x)              | inverse hyperbolic tangent                        |
| atan2(y, x)           | inverse tangent of y / x                          |
| cbrt(x)               | cube root                                         |
| ceil(x)               | nearest integer greater than or equal to argument |
| cos(x)                | cosine                                            |
| cosh(x)               | hyperbolic cosine                                 |
| degrees(x)            | converts radians to degrees                       |
| exp(x)                | exponential                                       |
| factorial(x)          | factorial                                         |
| floor(x)              | nearest integer less than or equal to argument    |
| gcd(x, y)             | greatest common divisor                           |
| isnan(x)              | predicate determining whether NaN/-NaN or not     |
| iszero(x)             | predicate determining whether 0.0/-0.0 or not     |
| lcm(x, y)             | least common multiple                             |
| ln(x)                 | natural logarithm                                 |
| log(base, x)          | logarithm of x for a particular base              |
| log10(x)              | base 10 logarithm                                 |
| log2(x)               | base 2 logarithm                                  |
| nanvl(x, y)           | returns x if x is not NaN otherwise returns y     |
| pi()                  | approximate value of π                            |
| power(base, exponent) | base raised to the power of exponent              |
| radians(x)            | converts degrees to radians                       |
| round(x)              | round to nearest integer                          |
| signum(x)             | sign of the argument (-1, 0, +1)                  |
| sin(x)                | sine                                              |
| sinh(x)               | hyperbolic sine                                   |
| sqrt(x)               | square root                                       |
| tan(x)                | tangent                                           |
| tanh(x)               | hyperbolic tangent                                |
| trunc(x)              | truncate toward zero                              |

:::{note}
Unlike to some databases the math functions in Datafusion works the same way as Rust math functions, avoiding failing on corner cases e.g

```sql
❯ select log(-1), log(0), sqrt(-1);
+----------------+---------------+-----------------+
| log(Int64(-1)) | log(Int64(0)) | sqrt(Int64(-1)) |
+----------------+---------------+-----------------+
| NaN            | -inf          | NaN             |
+----------------+---------------+-----------------+
```

:::

## Conditional Expressions

| Syntax                                                                                                                                                                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| coalesce([value, ...])                                                                                                                                                                     | Returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| case(expr)</br>&nbsp;&nbsp;&nbsp;&nbsp;.when(expr)</br>&nbsp;&nbsp;&nbsp;&nbsp;.end(),</br>case(expr)</br>&nbsp;&nbsp;&nbsp;&nbsp;.when(expr)</br>&nbsp;&nbsp;&nbsp;&nbsp;.otherwise(expr) | CASE expression. The expression may chain multiple `when` expressions and end with an `end` or `otherwise` expression. Example:</br> <pre><code>case(col("a") % lit(3))</br>&nbsp;&nbsp;&nbsp;&nbsp;.when(lit(0), lit("A"))</br>&nbsp;&nbsp;&nbsp;&nbsp;.when(lit(1), lit("B"))</br>&nbsp;&nbsp;&nbsp;&nbsp;.when(lit(2), lit("C"))</br>&nbsp;&nbsp;&nbsp;&nbsp;.end()</code></pre>or, end with `otherwise` to match any other conditions: <pre><code>case(col("b").gt(lit(100)))</br>&nbsp;&nbsp;&nbsp;&nbsp;.when(lit(true), lit("value > 100"))</br>&nbsp;&nbsp;&nbsp;&nbsp;.otherwise(lit("value <= 100"))</code></pre> |
| nullif(value1, value2)                                                                                                                                                                     | Returns a null value if `value1` equals `value2`; otherwise it returns `value1`. This can be used to perform the inverse operation of the `coalesce` expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## String Expressions

| Syntax                                         | Description                                                                                                                                                                                                                              |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ascii(character)                               | Returns a numeric representation of the character (`character`). Example: `ascii('a') -> 97`                                                                                                                                             |
| bit_length(text)                               | Returns the length of the string (`text`) in bits. Example: `bit_length('spider') -> 48`                                                                                                                                                 |
| btrim(text, characters)                        | Removes all specified characters (`characters`) from both the beginning and the end of the string (`text`). Example: `btrim('aabchelloccb', 'abc') -> hello`                                                                             |
| char_length(text)                              | Returns number of characters in the string (`text`). The same as `character_length` and `length`. Example: `character_length('lion') -> 4`                                                                                               |
| character_length(text)                         | Returns number of characters in the string (`text`). The same as `char_length` and `length`. Example: `char_length('lion') -> 4`                                                                                                         |
| concat(value1, [value2 [, ...]])               | Concatenates the text representations (`value1, [value2 [, ...]]`) of all the arguments. NULL arguments are ignored. Example: `concat('aaa', 'bbc', NULL, 321) -> aaabbc321`                                                             |
| concat_ws(separator, value1, [value2 [, ...]]) | Concatenates the text representations (`value1, [value2 [, ...]]`) of all the arguments with the separator (`separator`). NULL arguments are ignored. `concat_ws('/', 'path', 'to', NULL, 'my', 'folder', 123) -> path/to/my/folder/123` |
| chr(integer)                                   | Returns a character by its numeric representation (`integer`). Example: `chr(90) -> 8`                                                                                                                                                   |
| initcap                                        | Converts the first letter of each word to upper case and the rest to lower case. Example: `initcap('hi TOM') -> Hi Tom`                                                                                                                  |
| left(text, number)                             | Returns a certain number (`number`) of first characters (`text`). Example: `left('like', 2) -> li`                                                                                                                                       |
| length(text)                                   | Returns number of characters in the string (`text`). The same as `character_length` and `char_length`. Example: `length('lion') -> 4`                                                                                                    |
| lower(text)                                    | Converts all characters in the string (`text`) into lower case. Example: `lower('HELLO') -> hello`                                                                                                                                       |
| lpad(text, length, [, fill])                   | Extends the string to length (`length`) by prepending the characters (`fill`) (a space by default). Example: `lpad('bb', 5, 'a') → aaabb`                                                                                                |
| ltrim(text, text)                              | Removes all specified characters (`characters`) from the beginning of the string (`text`). Example: `ltrim('aabchelloccb', 'abc') -> helloccb`                                                                                           |
| md5(text)                                      | Computes the MD5 hash of the argument (`text`).                                                                                                                                                                                          |
| octet_length(text)                             | Returns number of bytes in the string (`text`).                                                                                                                                                                                          |
| repeat(text, number)                           | Repeats the string the specified number of times. Example: `repeat('1', 4) -> 1111`                                                                                                                                                      |
| replace(string, from, to)                      | Replaces a specified string (`from`) with another specified string (`to`) in the string (`string`). Example: `replace('Hello', 'replace', 'el') -> Hola`                                                                                 |
| reverse(text)                                  | Reverses the order of the characters in the string (`text`). Example: `reverse('hello') -> olleh`                                                                                                                                        |
| right(text, number)                            | Returns a certain number (`number`) of last characters (`text`). Example: `right('like', 2) -> ke`                                                                                                                                       |
| rpad(text, length, [, fill])                   | Extends the string to length (`length`) by prepending the characters (`fill`) (a space by default). Example: `rpad('bb', 5, 'a') → bbaaa`                                                                                                |
| rtrim                                          | Removes all specified characters (`characters`) from the end of the string (`text`). Example: `rtrim('aabchelloccb', 'abc') -> aabchello`                                                                                                |
| digest(input, algorithm)                       | Computes the binary hash of `input`, using the `algorithm`.                                                                                                                                                                              |
| split_part(string, delimiter, index)           | Splits the string (`string`) based on a delimiter (`delimiter`) and picks out the desired field based on the index (`index`).                                                                                                            |
| starts_with(string, prefix)                    | Returns `true` if the string (`string`) starts with the specified prefix (`prefix`). If not, it returns `false`. Example: `starts_with('Hi Tom', 'Hi') -> true`                                                                          |
| strpos                                         | Finds the position from where the `substring` matches the `string`                                                                                                                                                                       |
| substr(string, position, [, length])           | Returns substring from the position (`position`) with length (`length`) characters in the string (`string`).                                                                                                                             |
| translate(string, from, to)                    | Replaces the characters in `from` with the counterpart in `to`. Example: `translate('abcde', 'acd', '15') -> 1b5e`                                                                                                                       |
| trim(string)                                   | Removes all characters, space by default from the string (`string`)                                                                                                                                                                      |
| upper                                          | Converts all characters in the string into upper case. Example: `upper('hello') -> HELLO`                                                                                                                                                |

## Array Expressions

| Syntax                                | Description                                                                                                                                                              |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| array_aggregate(array, name)          | Allows the execution of arbitrary existing aggregate functions on the elements of a list.`array_aggregate([1, 2, 3], 'sum') -> 6`                                        |
| array_append(array, element)          | Appends an element to the end of an array. `array_append([1, 2, 3], 4) -> [1, 2, 3, 4]`                                                                                  |
| array_concat(array[, ..., array_n])   | Concatenates arrays. `array_concat([1, 2, 3], [4, 5, 6]) -> [1, 2, 3, 4, 5, 6]`                                                                                          |
| array_has(array, element)             | Returns true if the array contains the element `array_has([1,2,3], 1) -> true`                                                                                           |
| array_has_all(array, sub-array)       | Returns true if all elements of sub-array exist in array `array_has_all([1,2,3], [1,3]) -> true`                                                                         |
| array_has_any(array, sub-array)       | Returns true if any elements exist in both arrays `array_has_any([1,2,3], [1,4]) -> true`                                                                                |
| array_dims(array)                     | Returns an array of the array's dimensions. `array_dims([[1, 2, 3], [4, 5, 6]]) -> [2, 3]`                                                                               |
| array_distinct(array)                 | Returns distinct values from the array after removing duplicates. `array_distinct([1, 3, 2, 3, 1, 2, 4]) -> [1, 2, 3, 4]`                                                |
| array_element(array, index)           | Extracts the element with the index n from the array `array_element([1, 2, 3, 4], 3) -> 3`                                                                               |
| flatten(array)                        | Converts an array of arrays to a flat array `flatten([[1], [2, 3], [4, 5, 6]]) -> [1, 2, 3, 4, 5, 6]`                                                                    |
| array_length(array, dimension)        | Returns the length of the array dimension. `array_length([1, 2, 3, 4, 5]) -> 5`                                                                                          |
| array_ndims(array)                    | Returns the number of dimensions of the array. `array_ndims([[1, 2, 3], [4, 5, 6]]) -> 2`                                                                                |
| array_pop_front(array)                | Returns the array without the first element. `array_pop_front([1, 2, 3]) -> [2, 3]`                                                                                      |
| array_pop_back(array)                 | Returns the array without the last element. `array_pop_back([1, 2, 3]) -> [1, 2]`                                                                                        |
| array_position(array, element)        | Searches for an element in the array, returns first occurrence. `array_position([1, 2, 2, 3, 4], 2) -> 2`                                                                |
| array_positions(array, element)       | Searches for an element in the array, returns all occurrences. `array_positions([1, 2, 2, 3, 4], 2) -> [2, 3]`                                                           |
| array_prepend(array, element)         | Prepends an element to the beginning of an array. `array_prepend(1, [2, 3, 4]) -> [1, 2, 3, 4]`                                                                          |
| array_repeat(element, count)          | Returns an array containing element `count` times. `array_repeat(1, 3) -> [1, 1, 1]`                                                                                     |
| array_remove(array, element)          | Removes the first element from the array equal to the given value. `array_remove([1, 2, 2, 3, 2, 1, 4], 2) -> [1, 2, 3, 2, 1, 4]`                                        |
| array_remove_n(array, element, max)   | Removes the first `max` elements from the array equal to the given value. `array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2) -> [1, 3, 2, 1, 4]`                               |
| array_remove_all(array, element)      | Removes all elements from the array equal to the given value. `array_remove_all([1, 2, 2, 3, 2, 1, 4], 2) -> [1, 3, 1, 4]`                                               |
| array_replace(array, from, to)        | Replaces the first occurrence of the specified element with another specified element. `array_replace([1, 2, 2, 3, 2, 1, 4], 2, 5) -> [1, 5, 2, 3, 2, 1, 4]`             |
| array_replace_n(array, from, to, max) | Replaces the first `max` occurrences of the specified element with another specified element. `array_replace_n([1, 2, 2, 3, 2, 1, 4], 2, 5, 2) -> [1, 5, 5, 3, 2, 1, 4]` |
| array_replace_all(array, from, to)    | Replaces all occurrences of the specified element with another specified element. `array_replace_all([1, 2, 2, 3, 2, 1, 4], 2, 5) -> [1, 5, 5, 3, 5, 1, 4]`              |
| array_slice(array, index)             | Returns a slice of the array. `array_slice([1, 2, 3, 4, 5, 6, 7, 8], 3, 6) -> [3, 4, 5, 6]`                                                                              |
| array_sum(array)                      | Returns the sum of the array. `array_sum([1, 2, 3]) -> 6`                                                                                                                |
| array_to_string(array, delimiter)     | Converts each element to its text representation. `array_to_string([1, 2, 3, 4], ',') -> 1,2,3,4`                                                                        |
| array_intersect(array1, array2)       | Returns an array of the elements in the intersection of array1 and array2. `array_intersect([1, 2, 3, 4], [5, 6, 3, 4]) -> [3, 4]`                                       |
| array_union(array1, array2)           | Returns an array of the elements in the union of array1 and array2 without duplicates. `array_union([1, 2, 3, 4], [5, 6, 3, 4]) -> [1, 2, 3, 4, 5, 6]`                   |
| array_except(array1, array2)          | Returns an array of the elements that appear in the first array but not in the second. `array_except([1, 2, 3, 4], [5, 6, 3, 4]) -> [3, 4]`                              |
| cardinality(array)                    | Returns the total number of elements in the array. `cardinality([[1, 2, 3], [4, 5, 6]]) -> 6`                                                                            |
| make_array(value1, [value2 [, ...]])  | Returns an Arrow array using the specified input expressions. `make_array(1, 2, 3) -> [1, 2, 3]`                                                                         |
| range(start [, stop, step])           | Returns an Arrow array between start and stop with step. `SELECT range(2, 10, 3) -> [2, 5, 8]`                                                                           |
| trim_array(array, n)                  | Deprecated                                                                                                                                                               |

## Regular Expressions

| Syntax         | Description                                                                   |
| -------------- | ----------------------------------------------------------------------------- |
| regexp_match   | Matches a regular expression against a string and returns matched substrings. |
| regexp_replace | Replaces strings that match a regular expression                              |

## Temporal Expressions

| Syntax               | Description                                            |
| -------------------- | ------------------------------------------------------ |
| date_part            | Extracts a subfield from the date.                     |
| date_trunc           | Truncates the date to a specified level of precision.  |
| from_unixtime        | Returns the unix time in format.                       |
| to_timestamp         | Converts a string to a `Timestamp(_, _)`               |
| to_timestamp_millis  | Converts a string to a `Timestamp(Milliseconds, None)` |
| to_timestamp_micros  | Converts a string to a `Timestamp(Microseconds, None)` |
| to_timestamp_seconds | Converts a string to a `Timestamp(Seconds, None)`      |
| now()                | Returns current time.                                  |

## Other Expressions

| Syntax                       | Description                                                                                                |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------- |
| array([value1, ...])         | Returns an array of fixed size with each argument (`[value1, ...]`) on it.                                 |
| in_list(expr, list, negated) | Returns `true` if (`expr`) belongs or not belongs (`negated`) to a list (`list`), otherwise returns false. |
| random()                     | Returns a random value from 0 (inclusive) to 1 (exclusive).                                                |
| sha224(text)                 | Computes the SHA224 hash of the argument (`text`).                                                         |
| sha256(text)                 | Computes the SHA256 hash of the argument (`text`).                                                         |
| sha384(text)                 | Computes the SHA384 hash of the argument (`text`).                                                         |
| sha512(text)                 | Computes the SHA512 hash of the argument (`text`).                                                         |
| to_hex(integer)              | Converts the integer (`integer`) to the corresponding hexadecimal string.                                  |

## Aggregate Functions

| Syntax                                                            | Description                                                                             |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| avg(expr)                                                         | Сalculates the average value for `expr`.                                                |
| approx_distinct(expr)                                             | Calculates an approximate count of the number of distinct values for `expr`.            |
| approx_median(expr)                                               | Calculates an approximation of the median for `expr`.                                   |
| approx_percentile_cont(expr, percentile)                          | Calculates an approximation of the specified `percentile` for `expr`.                   |
| approx_percentile_cont_with_weight(expr, weight_expr, percentile) | Calculates an approximation of the specified `percentile` for `expr` and `weight_expr`. |
| bit_and(expr)                                                     | Computes the bitwise AND of all non-null input values for `expr`.                       |
| bit_or(expr)                                                      | Computes the bitwise OR of all non-null input values for `expr`.                        |
| bit_xor(expr)                                                     | Computes the bitwise exclusive OR of all non-null input values for `expr`.              |
| bool_and(expr)                                                    | Returns true if all non-null input values (`expr`) are true, otherwise false.           |
| bool_or(expr)                                                     | Returns true if any non-null input value (`expr`) is true, otherwise false.             |
| count(expr)                                                       | Returns the number of rows for `expr`.                                                  |
| count_distinct                                                    | Creates an expression to represent the count(distinct) aggregate function               |
| cube(exprs)                                                       | Creates a grouping set for all combination of `exprs`                                   |
| grouping_set(exprs)                                               | Create a grouping set.                                                                  |
| max(expr)                                                         | Finds the maximum value of `expr`.                                                      |
| median(expr)                                                      | Сalculates the median of `expr`.                                                        |
| min(expr)                                                         | Finds the minimum value of `expr`.                                                      |
| rollup(exprs)                                                     | Creates a grouping set for rollup sets.                                                 |
| sum(expr)                                                         | Сalculates the sum of `expr`.                                                           |

## Subquery Expressions

| Syntax          | Description                                                                                   |
| --------------- | --------------------------------------------------------------------------------------------- |
| exists          | Creates an `EXISTS` subquery expression                                                       |
| in_subquery     | `df1.filter(in_subquery(col("foo"), df2))?` is the equivalent of the SQL `WHERE foo IN <df2>` |
| not_exists      | Creates a `NOT EXISTS` subquery expression                                                    |
| not_in_subquery | Creates a `NOT IN` subquery expression                                                        |
| scalar_subquery | Creates a scalar subquery expression                                                          |

## User-Defined Function Expressions

| Syntax      | Description                                                               |
| ----------- | ------------------------------------------------------------------------- |
| create_udf  | Creates a new UDF with a specific signature and specific return type.     |
| create_udaf | Creates a new UDAF with a specific signature, state type and return type. |
