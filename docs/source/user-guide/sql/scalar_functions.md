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

# Scalar Functions

## Math Functions

- [abs](#abs)
- [acos](#acos)
- [acosh](#acosh)
- [asin](#asin)
- [asinh](#asinh)
- [atan](#atan)
- [atanh](#atanh)
- [atan2](#atan2)
- [cbrt](#cbrt)
- [ceil](#ceil)
- [cos](#cos)
- [cosh](#cosh)
- [degrees](#degrees)
- [exp](#exp)
- [factorial](#factorial)
- [floor](#floor)
- [gcd](#gcd)
- [isnan](#isnan)
- [iszero](#iszero)
- [lcm](#lcm)
- [ln](#ln)
- [log](#log)
- [log10](#log10)
- [log2](#log2)
- [nanvl](#nanvl)
- [pi](#pi)
- [power](#power)
- [pow](#pow)
- [radians](#radians)
- [random](#random)
- [round](#round)
- [signum](#signum)
- [sin](#sin)
- [sinh](#sinh)
- [sqrt](#sqrt)
- [tan](#tan)
- [tanh](#tanh)
- [trunc](#trunc)

### `abs`

Returns the absolute value of a number.

```
abs(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `acos`

Returns the arc cosine or inverse cosine of a number.

```
acos(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `acosh`

Returns the area hyperbolic cosine or inverse hyperbolic cosine of a number.

```
acosh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `asin`

Returns the arc sine or inverse sine of a number.

```
asin(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `asinh`

Returns the area hyperbolic sine or inverse hyperbolic sine of a number.

```
asinh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `atan`

Returns the arc tangent or inverse tangent of a number.

```
atan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `atanh`

Returns the area hyperbolic tangent or inverse hyperbolic tangent of a number.

```
atanh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `atan2`

Returns the arc tangent or inverse tangent of `expression_y / expression_x`.

```
atan2(expression_y, expression_x)
```

#### Arguments

- **expression_y**: First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_x**: Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `cbrt`

Returns the cube root of a number.

```
cbrt(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `ceil`

Returns the nearest integer greater than or equal to a number.

```
ceil(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `cos`

Returns the cosine of a number.

```
cos(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `cosh`

Returns the hyperbolic cosine of a number.

```
cosh(numeric_expression)
```

### `degrees`

Converts radians to degrees.

```
degrees(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `exp`

Returns the base-e exponential of a number.

```
exp(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to use as the exponent.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `factorial`

Factorial. Returns 1 if value is less than 2.

```
factorial(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `floor`

Returns the nearest integer less than or equal to a number.

```
floor(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `gcd`

Returns the greatest common divisor of `expression_x` and `expression_y`. Returns 0 if both inputs are zero.

```
gcd(expression_x, expression_y)
```

#### Arguments

- **expression_x**: First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_y**: Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `isnan`

Returns true if a given number is +NaN or -NaN otherwise returns false.

```
isnan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `iszero`

Returns true if a given number is +0.0 or -0.0 otherwise returns false.

```
iszero(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `lcm`

Returns the least common multiple of `expression_x` and `expression_y`. Returns 0 if either input is zero.

```
lcm(expression_x, expression_y)
```

#### Arguments

- **expression_x**: First numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_y**: Second numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `ln`

Returns the natural logarithm of a number.

```
ln(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `log`

Returns the base-x logarithm of a number.
Can either provide a specified base, or if omitted then takes the base-10 of a number.

```
log(base, numeric_expression)
log(numeric_expression)
```

#### Arguments

- **base**: Base numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `log10`

Returns the base-10 logarithm of a number.

```
log10(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `log2`

Returns the base-2 logarithm of a number.

```
log2(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `nanvl`

Returns the first argument if it's not _NaN_.
Returns the second argument otherwise.

```
nanvl(expression_x, expression_y)
```

#### Arguments

- **expression_x**: Numeric expression to return if it's not _NaN_.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression_y**: Numeric expression to return if the first expression is _NaN_.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `pi`

Returns an approximate value of π.

```
pi()
```

### `power`

Returns a base expression raised to the power of an exponent.

```
power(base, exponent)
```

#### Arguments

- **base**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **exponent**: Exponent numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

#### Aliases

- pow

### `pow`

_Alias of [power](#power)._

### `radians`

Converts degrees to radians.

```
radians(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `random`

Returns a random float value in the range [0, 1).
The random seed is unique to each row.

```
random()
```

### `round`

Rounds a number to the nearest integer.

```
round(numeric_expression[, decimal_places])
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **decimal_places**: Optional. The number of decimal places to round to.
  Defaults to 0.

### `signum`

Returns the sign of a number.
Negative numbers return `-1`.
Zero and positive numbers return `1`.

```
signum(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sin`

Returns the sine of a number.

```
sin(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sinh`

Returns the hyperbolic sine of a number.

```
sinh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `sqrt`

Returns the square root of a number.

```
sqrt(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `tan`

Returns the tangent of a number.

```
tan(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `tanh`

Returns the hyperbolic tangent of a number.

```
tanh(numeric_expression)
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `trunc`

Truncates a number to a whole number or truncated to the specified decimal places.

```
trunc(numeric_expression[, decimal_places])
```

#### Arguments

- **numeric_expression**: Numeric expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

- **decimal_places**: Optional. The number of decimal places to
  truncate to. Defaults to 0 (truncate to a whole number). If
  `decimal_places` is a positive integer, truncates digits to the
  right of the decimal point. If `decimal_places` is a negative
  integer, replaces digits to the left of the decimal point with `0`.

## Conditional Functions

- [coalesce](#coalesce)
- [nullif](#nullif)
- [nvl](#nvl)
- [nvl2](#nvl2)
- [ifnull](#ifnull)

### `coalesce`

Returns the first of its arguments that is not _null_.
Returns _null_ if all arguments are _null_.
This function is often used to substitute a default value for _null_ values.

```
coalesce(expression1[, ..., expression_n])
```

#### Arguments

- **expression1, expression_n**:
  Expression to use if previous expressions are _null_.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  Pass as many expression arguments as necessary.

### `nullif`

Returns _null_ if _expression1_ equals _expression2_; otherwise it returns _expression1_.
This can be used to perform the inverse operation of [`coalesce`](#coalesce).

```
nullif(expression1, expression2)
```

#### Arguments

- **expression1**: Expression to compare and return if equal to expression2.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: Expression to compare to expression1.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `nvl`

Returns _expression2_ if _expression1_ is NULL; otherwise it returns _expression1_.

```
nvl(expression1, expression2)
```

#### Arguments

- **expression1**: return if expression1 not is NULL.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: return if expression1 is NULL.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `nvl2`

Returns _expression2_ if _expression1_ is not NULL; otherwise it returns _expression3_.

```
nvl2(expression1, expression2, expression3)
```

#### Arguments

- **expression1**: conditional expression.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression2**: return if expression1 is not NULL.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **expression3**: return if expression1 is NULL.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `ifnull`

_Alias of [nvl](#nvl)._

## String Functions

- [ascii](#ascii)
- [bit_length](#bit_length)
- [btrim](#btrim)
- [char_length](#char_length)
- [character_length](#character_length)
- [concat](#concat)
- [concat_ws](#concat_ws)
- [chr](#chr)
- [ends_with](#ends_with)
- [initcap](#initcap)
- [instr](#instr)
- [left](#left)
- [length](#length)
- [lower](#lower)
- [lpad](#lpad)
- [ltrim](#ltrim)
- [octet_length](#octet_length)
- [repeat](#repeat)
- [replace](#replace)
- [reverse](#reverse)
- [right](#right)
- [rpad](#rpad)
- [rtrim](#rtrim)
- [split_part](#split_part)
- [starts_with](#starts_with)
- [strpos](#strpos)
- [substr](#substr)
- [to_hex](#to_hex)
- [translate](#translate)
- [trim](#trim)
- [upper](#upper)
- [uuid](#uuid)
- [overlay](#overlay)
- [levenshtein](#levenshtein)
- [substr_index](#substr_index)
- [find_in_set](#find_in_set)
- [position](#position)

### `ascii`

Returns the ASCII value of the first character in a string.

```
ascii(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[chr](#chr)

### `bit_length`

Returns the bit length of a string.

```
bit_length(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[length](#length),
[octet_length](#octet_length)

### `btrim`

Trims the specified trim string from the start and end of a string.
If no trim string is provided, all whitespace is removed from the start and end
of the input string.

```
btrim(str[, trim_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **trim_str**: String expression to trim from the beginning and end of the input string.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  _Default is whitespace characters._

**Related functions**:
[ltrim](#ltrim),
[rtrim](#rtrim)

#### Aliases

- trim

### `char_length`

_Alias of [length](#length)._

### `character_length`

_Alias of [length](#length)._

### `concat`

Concatenates multiple strings together.

```
concat(str[, ..., str_n])
```

#### Arguments

- **str**: String expression to concatenate.
  Can be a constant, column, or function, and any combination of string operators.
- **str_n**: Subsequent string column or literal string to concatenate.

**Related functions**:
[concat_ws](#concat_ws)

### `concat_ws`

Concatenates multiple strings together with a specified separator.

```
concat(separator, str[, ..., str_n])
```

#### Arguments

- **separator**: Separator to insert between concatenated strings.
- **str**: String expression to concatenate.
  Can be a constant, column, or function, and any combination of string operators.
- **str_n**: Subsequent string column or literal string to concatenate.

**Related functions**:
[concat](#concat)

### `chr`

Returns the character with the specified ASCII or Unicode code value.

```
chr(expression)
```

#### Arguments

- **expression**: Expression containing the ASCII or Unicode code value to operate on.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

**Related functions**:
[ascii](#ascii)

### `ends_with`

Tests if a string ends with a substring.

```
ends_with(str, substr)
```

#### Arguments

- **str**: String expression to test.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring to test for.

### `initcap`

Capitalizes the first character in each word in the input string.
Words are delimited by non-alphanumeric characters.

```
initcap(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[lower](#lower),
[upper](#upper)

### `instr`

_Alias of [strpos](#strpos)._

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring expression to search for.
  Can be a constant, column, or function, and any combination of string operators.

### `left`

Returns a specified number of characters from the left side of a string.

```
left(str, n)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: Number of characters to return.

**Related functions**:
[right](#right)

### `length`

Returns the number of characters in a string.

```
length(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

#### Aliases

- char_length
- character_length

**Related functions**:
[bit_length](#bit_length),
[octet_length](#octet_length)

### `lower`

Converts a string to lower-case.

```
lower(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[initcap](#initcap),
[upper](#upper)

### `lpad`

Pads the left side of a string with another string to a specified string length.

```
lpad(str, n[, padding_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: String length to pad to.
- **padding_str**: String expression to pad with.
  Can be a constant, column, or function, and any combination of string operators.
  _Default is a space._

**Related functions**:
[rpad](#rpad)

### `ltrim`

Trims the specified trim string from the beginning of a string.
If no trim string is provided, all whitespace is removed from the start
of the input string.

```
ltrim(str[, trim_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **trim_str**: String expression to trim from the beginning of the input string.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  _Default is whitespace characters._

**Related functions**:
[btrim](#btrim),
[rtrim](#rtrim)

### `octet_length`

Returns the length of a string in bytes.

```
octet_length(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[bit_length](#bit_length),
[length](#length)

### `repeat`

Returns a string with an input string repeated a specified number.

```
repeat(str, n)
```

#### Arguments

- **str**: String expression to repeat.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: Number of times to repeat the input string.

### `replace`

Replaces all occurrences of a specified substring in a string with a new substring.

```
replace(str, substr, replacement)
```

#### Arguments

- **str**: String expression to repeat.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring expression to replace in the input string.
  Can be a constant, column, or function, and any combination of string operators.
- **replacement**: Replacement substring expression.
  Can be a constant, column, or function, and any combination of string operators.

### `reverse`

Reverses the character order of a string.

```
reverse(str)
```

#### Arguments

- **str**: String expression to repeat.
  Can be a constant, column, or function, and any combination of string operators.

### `right`

Returns a specified number of characters from the right side of a string.

```
right(str, n)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: Number of characters to return.

**Related functions**:
[left](#left)

### `rpad`

Pads the right side of a string with another string to a specified string length.

```
rpad(str, n[, padding_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **n**: String length to pad to.
- **padding_str**: String expression to pad with.
  Can be a constant, column, or function, and any combination of string operators.
  _Default is a space._

**Related functions**:
[lpad](#lpad)

### `rtrim`

Trims the specified trim string from the end of a string.
If no trim string is provided, all whitespace is removed from the end
of the input string.

```
rtrim(str[, trim_str])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **trim_str**: String expression to trim from the end of the input string.
  Can be a constant, column, or function, and any combination of arithmetic operators.
  _Default is whitespace characters._

**Related functions**:
[btrim](#btrim),
[ltrim](#ltrim)

### `split_part`

Splits a string based on a specified delimiter and returns the substring in the
specified position.

```
split_part(str, delimiter, pos)
```

#### Arguments

- **str**: String expression to spit.
  Can be a constant, column, or function, and any combination of string operators.
- **delimiter**: String or character to split on.
- **pos**: Position of the part to return.

### `starts_with`

Tests if a string starts with a substring.

```
starts_with(str, substr)
```

#### Arguments

- **str**: String expression to test.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring to test for.

### `strpos`

Returns the starting position of a specified substring in a string.
Positions begin at 1.
If the substring does not exist in the string, the function returns 0.

```
strpos(str, substr)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **substr**: Substring expression to search for.
  Can be a constant, column, or function, and any combination of string operators.

#### Aliases

- instr

### `substr`

Extracts a substring of a specified number of characters from a specific
starting position in a string.

```
substr(str, start_pos[, length])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **start_pos**: Character position to start the substring at.
  The first character in the string has a position of 1.
- **length**: Number of characters to extract.
  If not specified, returns the rest of the string after the start position.

### `translate`

Translates characters in a string to specified translation characters.

```
translate(str, chars, translation)
```

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **chars**: Characters to translate.
- **translation**: Translation characters. Translation characters replace only
  characters at the same position in the **chars** string.

### `to_hex`

Converts an integer to a hexadecimal string.

```
to_hex(int)
```

#### Arguments

- **int**: Integer expression to convert.
  Can be a constant, column, or function, and any combination of arithmetic operators.

### `trim`

_Alias of [btrim](#btrim)._

### `upper`

Converts a string to upper-case.

```
upper(str)
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

**Related functions**:
[initcap](#initcap),
[lower](#lower)

### `uuid`

Returns UUID v4 string value which is unique per row.

```
uuid()
```

### `overlay`

Returns the string which is replaced by another string from the specified position and specified count length.
For example, `overlay('Txxxxas' placing 'hom' from 2 for 4) → Thomas`

```
overlay(str PLACING substr FROM pos [FOR count])
```

#### Arguments

- **str**: String expression to operate on.
- **substr**: the string to replace part of str.
- **pos**: the start position to replace of str.
- **count**: the count of characters to be replaced from start position of str. If not specified, will use substr length instead.

### `levenshtein`

Returns the Levenshtein distance between the two given strings.
For example, `levenshtein('kitten', 'sitting') = 3`

```
levenshtein(str1, str2)
```

#### Arguments

- **str1**: String expression to compute Levenshtein distance with str2.
- **str2**: String expression to compute Levenshtein distance with str1.

### `substr_index`

Returns the substring from str before count occurrences of the delimiter delim.
If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
For example, `substr_index('www.apache.org', '.', 1) = www`, `substr_index('www.apache.org', '.', -1) = org`

```
substr_index(str, delim, count)
```

#### Arguments

- **str**: String expression to operate on.
- **delim**: the string to find in str to split str.
- **count**: The number of times to search for the delimiter. Can be both a positive or negative number.

### `find_in_set`

Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings.
For example, `find_in_set('b', 'a,b,c,d') = 2`

```
find_in_set(str, strlist)
```

#### Arguments

- **str**: String expression to find in strlist.
- **strlist**: A string list is a string composed of substrings separated by , characters.

## Binary String Functions

- [decode](#decode)
- [encode](#encode)

### `encode`

Encode binary data into a textual representation.

```
encode(expression, format)
```

#### Arguments

- **expression**: Expression containing string or binary data

- **format**: Supported formats are: `base64`, `hex`

**Related functions**:
[decode](#decode)

### `decode`

Decode binary data from textual representation in string.

```
decode(expression, format)
```

#### Arguments

- **expression**: Expression containing encoded string data

- **format**: Same arguments as [encode](#encode)

**Related functions**:
[encode](#encode)

## Regular Expression Functions

Apache DataFusion uses a [PCRE-like] regular expression [syntax]
(minus support for several features including look-around and backreferences).
The following regular expression functions are supported:

- [regexp_like](#regexp_like)
- [regexp_match](#regexp_match)
- [regexp_replace](#regexp_replace)

[pcre-like]: https://en.wikibooks.org/wiki/Regular_Expressions/Perl-Compatible_Regular_Expressions
[syntax]: https://docs.rs/regex/latest/regex/#syntax

### `regexp_like`

Returns true if a [regular expression] has at least one match in a string,
false otherwise.

[regular expression]: https://docs.rs/regex/latest/regex/#syntax

```
regexp_like(str, regexp[, flags])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **regexp**: Regular expression to test against the string expression.
  Can be a constant, column, or function.
- **flags**: Optional regular expression flags that control the behavior of the
  regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?

#### Example

```sql
select regexp_like('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
+--------------------------------------------------------+
| regexp_like(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
+--------------------------------------------------------+
| true                                                   |
+--------------------------------------------------------+
SELECT regexp_like('aBc', '(b|d)', 'i');
+--------------------------------------------------+
| regexp_like(Utf8("aBc"),Utf8("(b|d)"),Utf8("i")) |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/regexp.rs)

### `regexp_match`

Returns a list of [regular expression](https://docs.rs/regex/latest/regex/#syntax) matches in a string.

```
regexp_match(str, regexp[, flags])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **regexp**: Regular expression to match against.
  Can be a constant, column, or function.
- **flags**: Optional regular expression flags that control the behavior of the
  regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?

#### Example

```sql
select regexp_match('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
+---------------------------------------------------------+
| regexp_match(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
+---------------------------------------------------------+
| [Köln]                                                  |
+---------------------------------------------------------+
SELECT regexp_match('aBc', '(b|d)', 'i');
+---------------------------------------------------+
| regexp_match(Utf8("aBc"),Utf8("(b|d)"),Utf8("i")) |
+---------------------------------------------------+
| [B]                                               |
+---------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/regexp.rs)

### `regexp_replace`

Replaces substrings in a string that match a [regular expression](https://docs.rs/regex/latest/regex/#syntax).

```
regexp_replace(str, regexp, replacement[, flags])
```

#### Arguments

- **str**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **regexp**: Regular expression to match against.
  Can be a constant, column, or function.
- **replacement**: Replacement string expression.
  Can be a constant, column, or function, and any combination of string operators.
- **flags**: Optional regular expression flags that control the behavior of the
  regular expression. The following flags are supported:
  - **g**: (global) Search globally and don't return after the first match
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?

#### Example

```sql
SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g');
+------------------------------------------------------------------------+
| regexp_replace(Utf8("foobarbaz"),Utf8("b(..)"),Utf8("X\1Y"),Utf8("g")) |
+------------------------------------------------------------------------+
| fooXarYXazY                                                            |
+------------------------------------------------------------------------+
SELECT regexp_replace('aBc', '(b|d)', 'Ab\\1a', 'i');
+-------------------------------------------------------------------+
| regexp_replace(Utf8("aBc"),Utf8("(b|d)"),Utf8("Ab\1a"),Utf8("i")) |
+-------------------------------------------------------------------+
| aAbBac                                                            |
+-------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/regexp.rs)

### `position`

Returns the position of `substr` in `origstr` (counting from 1). If `substr` does
not appear in `origstr`, return 0.

```
position(substr in origstr)
```

#### Arguments

- **substr**: The pattern string.
- **origstr**: The model string.

## Time and Date Functions

- [now](#now)
- [current_date](#current_date)
- [current_time](#current_time)
- [date_bin](#date_bin)
- [date_trunc](#date_trunc)
- [datetrunc](#datetrunc)
- [date_part](#date_part)
- [datepart](#datepart)
- [extract](#extract)
- [today](#today)
- [make_date](#make_date)
- [to_char](#to_char)
- [to_timestamp](#to_timestamp)
- [to_timestamp_millis](#to_timestamp_millis)
- [to_timestamp_micros](#to_timestamp_micros)
- [to_timestamp_seconds](#to_timestamp_seconds)
- [to_timestamp_nanos](#to_timestamp_nanos)
- [from_unixtime](#from_unixtime)

### `now`

Returns the current UTC timestamp.

The `now()` return value is determined at query time and will return the same timestamp,
no matter when in the query plan the function executes.

```
now()
```

### `current_date`

Returns the current UTC date.

The `current_date()` return value is determined at query time and will return the same date,
no matter when in the query plan the function executes.

```
current_date()
```

#### Aliases

- today

### `today`

_Alias of [current_date](#current_date)._

### `current_time`

Returns the current UTC time.

The `current_time()` return value is determined at query time and will return the same time,
no matter when in the query plan the function executes.

```
current_time()
```

### `date_bin`

Calculates time intervals and returns the start of the interval nearest to the specified timestamp.
Use `date_bin` to downsample time series data by grouping rows into time-based "bins" or "windows"
and applying an aggregate or selector function to each window.

For example, if you "bin" or "window" data into 15 minute intervals, an input
timestamp of `2023-01-01T18:18:18Z` will be updated to the start time of the 15
minute bin it is in: `2023-01-01T18:15:00Z`.

```
date_bin(interval, expression, origin-timestamp)
```

#### Arguments

- **interval**: Bin interval.
- **expression**: Time expression to operate on.
  Can be a constant, column, or function.
- **origin-timestamp**: Optional. Starting point used to determine bin boundaries. If not specified
  defaults `1970-01-01T00:00:00Z` (the UNIX epoch in UTC).

The following intervals are supported:

- nanoseconds
- microseconds
- milliseconds
- seconds
- minutes
- hours
- days
- weeks
- months
- years
- century

### `date_trunc`

Truncates a timestamp value to a specified precision.

```
date_trunc(precision, expression)
```

#### Arguments

- **precision**: Time precision to truncate to.
  The following precisions are supported:

  - year / YEAR
  - quarter / QUARTER
  - month / MONTH
  - week / WEEK
  - day / DAY
  - hour / HOUR
  - minute / MINUTE
  - second / SECOND

- **expression**: Time expression to operate on.
  Can be a constant, column, or function.

#### Aliases

- datetrunc

### `datetrunc`

_Alias of [date_trunc](#date_trunc)._

### `date_part`

Returns the specified part of the date as an integer.

```
date_part(part, expression)
```

#### Arguments

- **part**: Part of the date to return.
  The following date parts are supported:

  - year
  - quarter _(emits value in inclusive range [1, 4] based on which quartile of the year the date is in)_
  - month
  - week _(week of the year)_
  - day _(day of the month)_
  - hour
  - minute
  - second
  - millisecond
  - microsecond
  - nanosecond
  - dow _(day of the week)_
  - doy _(day of the year)_
  - epoch _(seconds since Unix epoch)_

- **expression**: Time expression to operate on.
  Can be a constant, column, or function.

#### Aliases

- datepart

### `datepart`

_Alias of [date_part](#date_part)._

### `extract`

Returns a sub-field from a time value as an integer.

```
extract(field FROM source)
```

Equivalent to calling `date_part('field', source)`. For example, these are equivalent:

```sql
extract(day FROM '2024-04-13'::date)
date_part('day', '2024-04-13'::date)
```

See [date_part](#date_part).

### `make_date`

Make a date from year/month/day component parts.

```
make_date(year, month, day)
```

#### Arguments

- **year**: Year to use when making the date.
  Can be a constant, column or function, and any combination of arithmetic operators.
- **month**: Month to use when making the date.
  Can be a constant, column or function, and any combination of arithmetic operators.
- **day**: Day to use when making the date.
  Can be a constant, column or function, and any combination of arithmetic operators.

#### Example

```
❯ select make_date(2023, 1, 31);
+-------------------------------------------+
| make_date(Int64(2023),Int64(1),Int64(31)) |
+-------------------------------------------+
| 2023-01-31                                |
+-------------------------------------------+
❯ select make_date('2023', '01', '31');
+-----------------------------------------------+
| make_date(Utf8("2023"),Utf8("01"),Utf8("31")) |
+-----------------------------------------------+
| 2023-01-31                                    |
+-----------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/make_date.rs)

### `to_char`

Returns a string representation of a date, time, timestamp or duration based
on a [Chrono format]. Unlike the PostgreSQL equivalent of this function
numerical formatting is not supported.

```
to_char(expression, format)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function that results in a
  date, time, timestamp or duration.
- **format**: A [Chrono format] string to use to convert the expression.

#### Example

```
❯ ❯ select to_char('2023-03-01'::date, '%d-%m-%Y');
+----------------------------------------------+
| to_char(Utf8("2023-03-01"),Utf8("%d-%m-%Y")) |
+----------------------------------------------+
| 01-03-2023                                   |
+----------------------------------------------+
```

Additional examples can be found [here]

[here]: https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/to_char.rs

#### Aliases

- date_format

### `to_timestamp`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00Z`).
Supports strings, integer, unsigned integer, and double types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono formats] are provided.
Integers, unsigned integers, and doubles are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

Note: `to_timestamp` returns `Timestamp(Nanosecond)`. The supported range for integer input is between `-9223372037` and `9223372036`.
Supported range for string input is between `1677-09-21T00:12:44.0` and `2262-04-11T23:47:16.0`. Please use `to_timestamp_seconds`
for the input outside of supported bounds.

```
to_timestamp(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

[chrono format]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

#### Example

```
❯ select to_timestamp('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------+
| to_timestamp(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                             |
+-----------------------------------------------------------+
❯ select to_timestamp('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------+
| to_timestamp(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                          |
+--------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_millis`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as milliseconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

```
to_timestamp_millis(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
❯ select to_timestamp_millis('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_millis(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123                                          |
+------------------------------------------------------------------+
❯ select to_timestamp_millis('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_millis(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123                                                                                       |
+---------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_micros`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as microseconds since the unix epoch (`1970-01-01T00:00:00Z`)
Returns the corresponding timestamp.

```
to_timestamp_micros(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
❯ select to_timestamp_micros('2023-01-31T09:26:56.123456789-05:00');
+------------------------------------------------------------------+
| to_timestamp_micros(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+------------------------------------------------------------------+
| 2023-01-31T14:26:56.123456                                       |
+------------------------------------------------------------------+
❯ select to_timestamp_micros('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+---------------------------------------------------------------------------------------------------------------+
| to_timestamp_micros(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+---------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456                                                                                    |
+---------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_nanos`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000000000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

```
to_timestamp_nanos(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
❯ select to_timestamp_nanos('2023-01-31T09:26:56.123456789-05:00');
+-----------------------------------------------------------------+
| to_timestamp_nanos(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-----------------------------------------------------------------+
| 2023-01-31T14:26:56.123456789                                   |
+-----------------------------------------------------------------+
❯ select to_timestamp_nanos('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+--------------------------------------------------------------------------------------------------------------+
| to_timestamp_nanos(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+--------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00.123456789                                                                                |
+---------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `to_timestamp_seconds`

Converts a value to a timestamp (`YYYY-MM-DDT00:00:00.000Z`).
Supports strings, integer, and unsigned integer types as input.
Strings are parsed as RFC3339 (e.g. '2023-07-20T05:44:00') if no [Chrono format]s are provided.
Integers and unsigned integers are interpreted as seconds since the unix epoch (`1970-01-01T00:00:00Z`).
Returns the corresponding timestamp.

```
to_timestamp_seconds(expression[, ..., format_n])
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.
- **format_n**: Optional [Chrono format] strings to use to parse the expression. Formats will be tried in the order
  they appear with the first successful one being returned. If none of the formats successfully parse the expression
  an error will be returned.

#### Example

```
❯ select to_timestamp_seconds('2023-01-31T09:26:56.123456789-05:00');
+-------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("2023-01-31T09:26:56.123456789-05:00")) |
+-------------------------------------------------------------------+
| 2023-01-31T14:26:56                                               |
+-------------------------------------------------------------------+
❯ select to_timestamp_seconds('03:59:00.123456789 05-17-2023', '%c', '%+', '%H:%M:%S%.f %m-%d-%Y');
+----------------------------------------------------------------------------------------------------------------+
| to_timestamp_seconds(Utf8("03:59:00.123456789 05-17-2023"),Utf8("%c"),Utf8("%+"),Utf8("%H:%M:%S%.f %m-%d-%Y")) |
+----------------------------------------------------------------------------------------------------------------+
| 2023-05-17T03:59:00                                                                                            |
+----------------------------------------------------------------------------------------------------------------+
```

Additional examples can be found [here](https://github.com/apache/arrow-datafusion/blob/main/datafusion-examples/examples/to_timestamp.rs)

### `from_unixtime`

Converts an integer to RFC3339 timestamp format (`YYYY-MM-DDT00:00:00.000000000Z`).
Integers and unsigned integers are interpreted as nanoseconds since the unix epoch (`1970-01-01T00:00:00Z`)
return the corresponding timestamp.

```
from_unixtime(expression)
```

#### Arguments

- **expression**: Expression to operate on.
  Can be a constant, column, or function, and any combination of arithmetic operators.

## Array Functions

- [array_append](#array_append)
- [array_sort](#array_sort)
- [array_cat](#array_cat)
- [array_concat](#array_concat)
- [array_contains](#array_contains)
- [array_dims](#array_dims)
- [array_distinct](#array_distinct)
- [array_has](#array_has)
- [array_has_all](#array_has_all)
- [array_has_any](#array_has_any)
- [array_element](#array_element)
- [array_empty](#array_empty)
- [array_except](#array_except)
- [array_extract](#array_extract)
- [array_fill](#array_fill)
- [array_indexof](#array_indexof)
- [array_intersect](#array_intersect)
- [array_join](#array_join)
- [array_length](#array_length)
- [array_ndims](#array_ndims)
- [array_prepend](#array_prepend)
- [array_pop_front](#array_pop_front)
- [array_pop_back](#array_pop_back)
- [array_position](#array_position)
- [array_positions](#array_positions)
- [array_push_back](#array_push_back)
- [array_push_front](#array_push_front)
- [array_repeat](#array_repeat)
- [array_resize](#array_resize)
- [array_remove](#array_remove)
- [array_remove_n](#array_remove_n)
- [array_remove_all](#array_remove_all)
- [array_replace](#array_replace)
- [array_replace_n](#array_replace_n)
- [array_replace_all](#array_replace_all)
- [array_reverse](#array_reverse)
- [array_slice](#array_slice)
- [array_to_string](#array_to_string)
- [array_union](#array_union)
- [cardinality](#cardinality)
- [empty](#empty)
- [flatten](#flatten)
- [generate_series](#generate_series)
- [list_append](#list_append)
- [list_sort](#list_sort)
- [list_cat](#list_cat)
- [list_concat](#list_concat)
- [list_dims](#list_dims)
- [list_distinct](#list_distinct)
- [list_element](#list_element)
- [list_except](#list_except)
- [list_extract](#list_extract)
- [list_has](#list_has)
- [list_has_all](#list_has_all)
- [list_has_any](#list_has_any)
- [list_indexof](#list_indexof)
- [list_intersect](#list_intersect)
- [list_join](#list_join)
- [list_length](#list_length)
- [list_ndims](#list_ndims)
- [list_prepend](#list_prepend)
- [list_pop_back](#list_pop_back)
- [list_pop_front](#list_pop_front)
- [list_position](#list_position)
- [list_positions](#list_positions)
- [list_push_back](#list_push_back)
- [list_push_front](#list_push_front)
- [list_repeat](#list_repeat)
- [list_resize](#list_resize)
- [list_remove](#list_remove)
- [list_remove_n](#list_remove_n)
- [list_remove_all](#list_remove_all)
- [list_replace](#list_replace)
- [list_replace_n](#list_replace_n)
- [list_replace_all](#list_replace_all)
- [list_slice](#list_slice)
- [list_to_string](#list_to_string)
- [list_union](#list_union)
- [make_array](#make_array)
- [make_list](#make_list)
- [string_to_array](#string_to_array)
- [string_to_list](#string_to_list)
- [trim_array](#trim_array)
- [range](#range)

### `array_append`

Appends an element to the end of an array.

```
array_append(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to append to the array.

#### Example

```
❯ select array_append([1, 2, 3], 4);
+--------------------------------------+
| array_append(List([1,2,3]),Int64(4)) |
+--------------------------------------+
| [1, 2, 3, 4]                         |
+--------------------------------------+
```

#### Aliases

- array_push_back
- list_append
- list_push_back

### `array_sort`

Sort array.

```
array_sort(array, desc, nulls_first)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **desc**: Whether to sort in descending order(`ASC` or `DESC`).
- **nulls_first**: Whether to sort nulls first(`NULLS FIRST` or `NULLS LAST`).

#### Example

```
❯ select array_sort([3, 1, 2]);
+-----------------------------+
| array_sort(List([3,1,2]))   |
+-----------------------------+
| [1, 2, 3]                   |
+-----------------------------+
```

#### Aliases

- list_sort

### `array_resize`

Resizes the list to contain size elements. Initializes new elements with value or empty if value is not set.

```
array_resize(array, size, value)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **size**: New size of given array.
- **value**: Defines new elements' value or empty if value is not set.

#### Example

```
❯ select array_resize([1, 2, 3], 5, 0);
+-------------------------------------+
| array_resize(List([1,2,3],5,0))     |
+-------------------------------------+
| [1, 2, 3, 0, 0]                     |
+-------------------------------------+
```

#### Aliases

- list_resize

### `array_cat`

_Alias of [array_concat](#array_concat)._

### `array_concat`

Concatenates arrays.

```
array_concat(array[, ..., array_n])
```

#### Arguments

- **array**: Array expression to concatenate.
  Can be a constant, column, or function, and any combination of array operators.
- **array_n**: Subsequent array column or literal array to concatenate.

#### Example

```
❯ select array_concat([1, 2], [3, 4], [5, 6]);
+---------------------------------------------------+
| array_concat(List([1,2]),List([3,4]),List([5,6])) |
+---------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                |
+---------------------------------------------------+
```

#### Aliases

- array_cat
- list_cat
- list_concat

### `array_contains`

_Alias of [array_has](#array_has)._

### `array_has`

Returns true if the array contains the element

```
array_has(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Scalar or Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Aliases

- list_has

### `array_has_all`

Returns true if all elements of sub-array exist in array

```
array_has_all(array, sub-array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **sub-array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Aliases

- list_has_all

### `array_has_any`

Returns true if any elements exist in both arrays

```
array_has_any(array, sub-array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **sub-array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Aliases

- list_has_any

### `array_dims`

Returns an array of the array's dimensions.

```
array_dims(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_dims([[1, 2, 3], [4, 5, 6]]);
+---------------------------------+
| array_dims(List([1,2,3,4,5,6])) |
+---------------------------------+
| [2, 3]                          |
+---------------------------------+
```

#### Aliases

- list_dims

### `array_distinct`

Returns distinct values from the array after removing duplicates.

```
array_distinct(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_distinct([1, 3, 2, 3, 1, 2, 4]);
+---------------------------------+
| array_distinct(List([1,2,3,4])) |
+---------------------------------+
| [1, 2, 3, 4]                    |
+---------------------------------+
```

#### Aliases

- list_distinct

### `array_element`

Extracts the element with the index n from the array.

```
array_element(array, index)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **index**: Index to extract the element from the array.

#### Example

```
❯ select array_element([1, 2, 3, 4], 3);
+-----------------------------------------+
| array_element(List([1,2,3,4]),Int64(3)) |
+-----------------------------------------+
| 3                                       |
+-----------------------------------------+
```

#### Aliases

- array_extract
- list_element
- list_extract

### `array_extract`

_Alias of [array_element](#array_element)._

### `array_fill`

Returns an array filled with copies of the given value.

DEPRECATED: use `array_repeat` instead!

```
array_fill(element, array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to copy to the array.

### `flatten`

Converts an array of arrays to a flat array

- Applies to any depth of nested arrays
- Does not change arrays that are already flat

The flattened array contains all the elements from all source arrays.

#### Arguments

- **array**: Array expression
  Can be a constant, column, or function, and any combination of array operators.

```
flatten(array)
```

### `array_indexof`

_Alias of [array_position](#array_position)._

### `array_intersect`

Returns an array of elements in the intersection of array1 and array2.

```
array_intersect(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 3, 4]);       |
+----------------------------------------------------+
| [3, 4]                                             |
+----------------------------------------------------+
❯ select array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_intersect([1, 2, 3, 4], [5, 6, 7, 8]);       |
+----------------------------------------------------+
| []                                                 |
+----------------------------------------------------+
```

---

#### Aliases

- list_intersect

### `array_join`

_Alias of [array_to_string](#array_to_string)._

### `array_length`

Returns the length of the array dimension.

```
array_length(array, dimension)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **dimension**: Array dimension.

#### Example

```
❯ select array_length([1, 2, 3, 4, 5]);
+---------------------------------+
| array_length(List([1,2,3,4,5])) |
+---------------------------------+
| 5                               |
+---------------------------------+
```

#### Aliases

- list_length

### `array_ndims`

Returns the number of dimensions of the array.

```
array_ndims(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_ndims([[1, 2, 3], [4, 5, 6]]);
+----------------------------------+
| array_ndims(List([1,2,3,4,5,6])) |
+----------------------------------+
| 2                                |
+----------------------------------+
```

#### Aliases

- list_ndims

### `array_prepend`

Prepends an element to the beginning of an array.

```
array_prepend(element, array)
```

#### Arguments

- **element**: Element to prepend to the array.
- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_prepend(1, [2, 3, 4]);
+---------------------------------------+
| array_prepend(Int64(1),List([2,3,4])) |
+---------------------------------------+
| [1, 2, 3, 4]                          |
+---------------------------------------+
```

#### Aliases

- array_push_front
- list_prepend
- list_push_front

### `array_pop_front`

Returns the array without the first element.

```
array_pop_front(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_pop_front([1, 2, 3]);
+-------------------------------+
| array_pop_front(List([1,2,3])) |
+-------------------------------+
| [2, 3]                        |
+-------------------------------+
```

#### Aliases

- list_pop_front

### `array_pop_back`

Returns the array without the last element.

```
array_pop_back(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_pop_back([1, 2, 3]);
+-------------------------------+
| array_pop_back(List([1,2,3])) |
+-------------------------------+
| [1, 2]                        |
+-------------------------------+
```

#### Aliases

- list_pop_back

### `array_position`

Returns the position of the first occurrence of the specified element in the array.

```
array_position(array, element)
array_position(array, element, index)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to search for position in the array.
- **index**: Index at which to start searching.

#### Example

```
❯ select array_position([1, 2, 2, 3, 1, 4], 2);
+----------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2)) |
+----------------------------------------------+
| 2                                            |
+----------------------------------------------+
```

#### Aliases

- array_indexof
- list_indexof
- list_position

### `array_positions`

Searches for an element in the array, returns all occurrences.

```
array_positions(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to search for positions in the array.

#### Example

```
❯ select array_positions([1, 2, 2, 3, 1, 4], 2);
+-----------------------------------------------+
| array_positions(List([1,2,2,3,1,4]),Int64(2)) |
+-----------------------------------------------+
| [2, 3]                                        |
+-----------------------------------------------+
```

#### Aliases

- list_positions

### `array_push_back`

_Alias of [array_append](#array_append)._

### `array_push_front`

_Alias of [array_prepend](#array_prepend)._

### `array_repeat`

Returns an array containing element `count` times.

```
array_repeat(element, count)
```

#### Arguments

- **element**: Element expression.
  Can be a constant, column, or function, and any combination of array operators.
- **count**: Value of how many times to repeat the element.

#### Example

```
❯ select array_repeat(1, 3);
+---------------------------------+
| array_repeat(Int64(1),Int64(3)) |
+---------------------------------+
| [1, 1, 1]                       |
+---------------------------------+
```

```
❯ select array_repeat([1, 2], 2);
+------------------------------------+
| array_repeat(List([1,2]),Int64(2)) |
+------------------------------------+
| [[1, 2], [1, 2]]                   |
+------------------------------------+
```

#### Aliases

- list_repeat

### `array_remove`

Removes the first element from the array equal to the given value.

```
array_remove(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.

#### Example

```
❯ select array_remove([1, 2, 2, 3, 2, 1, 4], 2);
+----------------------------------------------+
| array_remove(List([1,2,2,3,2,1,4]),Int64(2)) |
+----------------------------------------------+
| [1, 2, 3, 2, 1, 4]                           |
+----------------------------------------------+
```

#### Aliases

- list_remove

### `array_remove_n`

Removes the first `max` elements from the array equal to the given value.

```
array_remove_n(array, element, max)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.
- **max**: Number of first occurrences to remove.

#### Example

```
❯ select array_remove_n([1, 2, 2, 3, 2, 1, 4], 2, 2);
+---------------------------------------------------------+
| array_remove_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(2)) |
+---------------------------------------------------------+
| [1, 3, 2, 1, 4]                                         |
+---------------------------------------------------------+
```

#### Aliases

- list_remove_n

### `array_remove_all`

Removes all elements from the array equal to the given value.

```
array_remove_all(array, element)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **element**: Element to be removed from the array.

#### Example

```
❯ select array_remove_all([1, 2, 2, 3, 2, 1, 4], 2);
+--------------------------------------------------+
| array_remove_all(List([1,2,2,3,2,1,4]),Int64(2)) |
+--------------------------------------------------+
| [1, 3, 1, 4]                                     |
+--------------------------------------------------+
```

#### Aliases

- list_remove_all

### `array_replace`

Replaces the first occurrence of the specified element with another specified element.

```
array_replace(array, from, to)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.

#### Example

```
❯ select array_replace([1, 2, 2, 3, 2, 1, 4], 2, 5);
+--------------------------------------------------------+
| array_replace(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+--------------------------------------------------------+
| [1, 5, 2, 3, 2, 1, 4]                                  |
+--------------------------------------------------------+
```

#### Aliases

- list_replace

### `array_replace_n`

Replaces the first `max` occurrences of the specified element with another specified element.

```
array_replace_n(array, from, to, max)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.
- **max**: Number of first occurrences to replace.

#### Example

```
❯ select array_replace_n([1, 2, 2, 3, 2, 1, 4], 2, 5, 2);
+-------------------------------------------------------------------+
| array_replace_n(List([1,2,2,3,2,1,4]),Int64(2),Int64(5),Int64(2)) |
+-------------------------------------------------------------------+
| [1, 5, 5, 3, 2, 1, 4]                                             |
+-------------------------------------------------------------------+
```

#### Aliases

- list_replace_n

### `array_replace_all`

Replaces all occurrences of the specified element with another specified element.

```
array_replace_all(array, from, to)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **from**: Initial element.
- **to**: Final element.

#### Example

```
❯ select array_replace_all([1, 2, 2, 3, 2, 1, 4], 2, 5);
+------------------------------------------------------------+
| array_replace_all(List([1,2,2,3,2,1,4]),Int64(2),Int64(5)) |
+------------------------------------------------------------+
| [1, 5, 5, 3, 5, 1, 4]                                      |
+------------------------------------------------------------+
```

#### Aliases

- list_replace_all

### `array_reverse`

Returns the array with the order of the elements reversed.

```
array_reverse(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_reverse([1, 2, 3, 4]);
+------------------------------------------------------------+
| array_reverse(List([1, 2, 3, 4]))                          |
+------------------------------------------------------------+
| [4, 3, 2, 1]                                               |
+------------------------------------------------------------+
```

#### Aliases

- list_reverse

### `array_slice`

Returns a slice of the array based on 1-indexed start and end positions.

```
array_slice(array, begin, end)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **begin**: Index of the first element.
  If negative, it counts backward from the end of the array.
- **end**: Index of the last element.
  If negative, it counts backward from the end of the array.
- **stride**: Stride of the array slice. The default is 1.

#### Example

```
❯ select array_slice([1, 2, 3, 4, 5, 6, 7, 8], 3, 6);
+--------------------------------------------------------+
| array_slice(List([1,2,3,4,5,6,7,8]),Int64(3),Int64(6)) |
+--------------------------------------------------------+
| [3, 4, 5, 6]                                           |
+--------------------------------------------------------+
```

#### Aliases

- list_slice

### `array_to_string`

Converts each element to its text representation.

```
array_to_string(array, delimiter)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **delimiter**: Array element separator.

#### Example

```
❯ select array_to_string([[1, 2, 3, 4], [5, 6, 7, 8]], ',');
+----------------------------------------------------+
| array_to_string(List([1,2,3,4,5,6,7,8]),Utf8(",")) |
+----------------------------------------------------+
| 1,2,3,4,5,6,7,8                                    |
+----------------------------------------------------+
```

#### Aliases

- array_join
- list_join
- list_to_string

### `array_union`

Returns an array of elements that are present in both arrays (all elements from both arrays) with out duplicates.

```
array_union(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_union([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6]                                 |
+----------------------------------------------------+
❯ select array_union([1, 2, 3, 4], [5, 6, 7, 8]);
+----------------------------------------------------+
| array_union([1, 2, 3, 4], [5, 6, 7, 8]);           |
+----------------------------------------------------+
| [1, 2, 3, 4, 5, 6, 7, 8]                           |
+----------------------------------------------------+
```

---

#### Aliases

- list_union

### `array_except`

Returns an array of the elements that appear in the first array but not in the second.

```
array_except(array1, array2)
```

#### Arguments

- **array1**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **array2**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select array_except([1, 2, 3, 4], [5, 6, 3, 4]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [5, 6, 3, 4]);           |
+----------------------------------------------------+
| [1, 2]                                 |
+----------------------------------------------------+
❯ select array_except([1, 2, 3, 4], [3, 4, 5, 6]);
+----------------------------------------------------+
| array_except([1, 2, 3, 4], [3, 4, 5, 6]);           |
+----------------------------------------------------+
| [1, 2]                                 |
+----------------------------------------------------+
```

---

#### Aliases

- list_except

### `cardinality`

Returns the total number of elements in the array.

```
cardinality(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select cardinality([[1, 2, 3, 4], [5, 6, 7, 8]]);
+--------------------------------------+
| cardinality(List([1,2,3,4,5,6,7,8])) |
+--------------------------------------+
| 8                                    |
+--------------------------------------+
```

### `empty`

Returns 1 for an empty array or 0 for a non-empty array.

```
empty(array)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.

#### Example

```
❯ select empty([1]);
+------------------+
| empty(List([1])) |
+------------------+
| 0                |
+------------------+
```

#### Aliases

- array_empty,
- list_empty

### `generate_series`

Similar to the range function, but it includes the upper bound.

```
generate_series(start, stop, step)
```

#### Arguments

- **start**: start of the range
- **end**: end of the range (included)
- **step**: increase by step (can not be 0)

#### Example

```
❯ select generate_series(1,3);
+------------------------------------+
| generate_series(Int64(1),Int64(3)) |
+------------------------------------+
| [1, 2, 3]                          |
+------------------------------------+
```

### `list_append`

_Alias of [array_append](#array_append)._

### `list_cat`

_Alias of [array_concat](#array_concat)._

### `list_concat`

_Alias of [array_concat](#array_concat)._

### `list_dims`

_Alias of [array_dims](#array_dims)._

### `list_distinct`

_Alias of [array_dims](#array_distinct)._

### `list_element`

_Alias of [array_element](#array_element)._

### `list_empty`

_Alias of [empty](#empty)._

### `list_except`

_Alias of [array_element](#array_except)._

### `list_extract`

_Alias of [array_element](#array_element)._

### `list_has`

_Alias of [array_has](#array_has)._

### `list_has_all`

_Alias of [array_has_all](#array_has_all)._

### `list_has_any`

_Alias of [array_has_any](#array_has_any)._

### `list_indexof`

_Alias of [array_position](#array_position)._

### `list_intersect`

_Alias of [array_position](#array_intersect)._

### `list_join`

_Alias of [array_to_string](#array_to_string)._

### `list_length`

_Alias of [array_length](#array_length)._

### `list_ndims`

_Alias of [array_ndims](#array_ndims)._

### `list_prepend`

_Alias of [array_prepend](#array_prepend)._

### `list_pop_back`

_Alias of [array_pop_back](#array_pop_back)._

### `list_pop_front`

_Alias of [array_pop_front](#array_pop_front)._

### `list_position`

_Alias of [array_position](#array_position)._

### `list_positions`

_Alias of [array_positions](#array_positions)._

### `list_push_back`

_Alias of [array_append](#array_append)._

### `list_push_front`

_Alias of [array_prepend](#array_prepend)._

### `list_repeat`

_Alias of [array_repeat](#array_repeat)._

### `list_resize`

_Alias of [array_resize](#array_resize)._

### `list_remove`

_Alias of [array_remove](#array_remove)._

### `list_remove_n`

_Alias of [array_remove_n](#array_remove_n)._

### `list_remove_all`

_Alias of [array_remove_all](#array_remove_all)._

### `list_replace`

_Alias of [array_replace](#array_replace)._

### `list_replace_n`

_Alias of [array_replace_n](#array_replace_n)._

### `list_replace_all`

_Alias of [array_replace_all](#array_replace_all)._

### `list_reverse`

_Alias of [array_reverse](#array_reverse)._

### `list_slice`

_Alias of [array_slice](#array_slice)._

### `list_sort`

_Alias of [array_sort](#array_sort)._

### `list_to_string`

_Alias of [array_to_string](#array_to_string)._

### `list_union`

_Alias of [array_union](#array_union)._

### `make_array`

Returns an Arrow array using the specified input expressions.

```
make_array(expression1[, ..., expression_n])
```

### `array_empty`

_Alias of [empty](#empty)._

#### Arguments

- **expression_n**: Expression to include in the output array.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

#### Example

```
❯ select make_array(1, 2, 3, 4, 5);
+----------------------------------------------------------+
| make_array(Int64(1),Int64(2),Int64(3),Int64(4),Int64(5)) |
+----------------------------------------------------------+
| [1, 2, 3, 4, 5]                                          |
+----------------------------------------------------------+
```

#### Aliases

- make_list

### `make_list`

_Alias of [make_array](#make_array)._

### `string_to_array`

Splits a string in to an array of substrings based on a delimiter. Any substrings matching the optional `null_str` argument are replaced with NULL.
`SELECT string_to_array('abc##def', '##')` or `SELECT string_to_array('abc def', ' ', 'def')`

```
starts_with(str, delimiter[, null_str])
```

#### Arguments

- **str**: String expression to split.
- **delimiter**: Delimiter string to split on.
- **null_str**: Substring values to be replaced with `NULL`

#### Aliases

- string_to_list

### `string_to_list`

_Alias of [string_to_array](#string_to_array)._

### `trim_array`

Removes the last n elements from the array.

DEPRECATED: use `array_slice` instead!

```
trim_array(array, n)
```

#### Arguments

- **array**: Array expression.
  Can be a constant, column, or function, and any combination of array operators.
- **n**: Element to trim the array.

### `range`

Returns an Arrow array between start and stop with step. `SELECT range(2, 10, 3) -> [2, 5, 8]` or `SELECT range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);`

The range start..end contains all values with start <= x < end. It is empty if start >= end.

Step can not be 0 (then the range will be nonsense.).

Note that when the required range is a number, it accepts (stop), (start, stop), and (start, stop, step) as parameters, but when the required range is a date, it must be 3 non-NULL parameters.
For example,

```
SELECT range(3);
SELECT range(1,5);
SELECT range(1,5,1);
```

are allowed in number ranges

but in date ranges, only

```
SELECT range(DATE '1992-09-01', DATE '1993-03-01', INTERVAL '1' MONTH);
```

is allowed, and

```
SELECT range(DATE '1992-09-01', DATE '1993-03-01', NULL);
SELECT range(NULL, DATE '1993-03-01', INTERVAL '1' MONTH);
SELECT range(DATE '1992-09-01', NULL, INTERVAL '1' MONTH);
```

are not allowed

#### Arguments

- **start**: start of the range
- **end**: end of the range (not included)
- **step**: increase by step (can not be 0)

#### Aliases

- generate_series

## Struct Functions

- [struct](#struct)
- [named_struct](#named_struct)

### `struct`

Returns an Arrow struct using the specified input expressions optionally named.
Fields in the returned struct use the optional name or the `cN` naming convention.
For example: `c0`, `c1`, `c2`, etc.

```
struct(expression1[, ..., expression_n])
```

For example, this query converts two columns `a` and `b` to a single column with
a struct type of fields `field_a` and `c1`:

```
select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+

select struct(a as field_a, b) from t;
+--------------------------------------------------+
| named_struct(Utf8("field_a"),t.a,Utf8("c1"),t.b) |
+--------------------------------------------------+
| {field_a: 1, c1: 2}                              |
| {field_a: 3, c1: 4}                              |
+--------------------------------------------------+
```

#### Arguments

- **expression_n**: Expression to include in the output struct.
  Can be a constant, column, or function, any combination of arithmetic or
  string operators, or a named expression of previous listed .

### `named_struct`

Returns an Arrow struct using the specified name and input expressions pairs.

```
named_struct(expression1_name, expression1_input[, ..., expression_n_name, expression_n_input])
```

For example, this query converts two columns `a` and `b` to a single column with
a struct type of fields `field_a` and `field_b`:

```
select * from t;
+---+---+
| a | b |
+---+---+
| 1 | 2 |
| 3 | 4 |
+---+---+

select named_struct('field_a', a, 'field_b', b) from t;
+-------------------------------------------------------+
| named_struct(Utf8("field_a"),t.a,Utf8("field_b"),t.b) |
+-------------------------------------------------------+
| {field_a: 1, field_b: 2}                              |
| {field_a: 3, field_b: 4}                              |
+-------------------------------------------------------+
```

#### Arguments

- **expression_n_name**: Name of the column field.
  Must be a constant string.
- **expression_n_input**: Expression to include in the output struct.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

## Hashing Functions

- [digest](#digest)
- [md5](#md5)
- [sha224](#sha224)
- [sha256](#sha256)
- [sha384](#sha384)
- [sha512](#sha512)

### `digest`

Computes the binary hash of an expression using the specified algorithm.

```
digest(expression, algorithm)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.
- **algorithm**: String expression specifying algorithm to use.
  Must be one of:

  - md5
  - sha224
  - sha256
  - sha384
  - sha512
  - blake2s
  - blake2b
  - blake3

### `md5`

Computes an MD5 128-bit checksum for a string expression.

```
md5(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha224`

Computes the SHA-224 hash of a binary string.

```
sha224(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha256`

Computes the SHA-256 hash of a binary string.

```
sha256(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha384`

Computes the SHA-384 hash of a binary string.

```
sha384(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

### `sha512`

Computes the SHA-512 hash of a binary string.

```
sha512(expression)
```

#### Arguments

- **expression**: String expression to operate on.
  Can be a constant, column, or function, and any combination of string operators.

## Other Functions

- [arrow_cast](#arrow_cast)
- [arrow_typeof](#arrow_typeof)

### `arrow_cast`

Casts a value to a specific Arrow data type:

```
arrow_cast(expression, datatype)
```

#### Arguments

- **expression**: Expression to cast.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.
- **datatype**: [Arrow data type](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) name
  to cast to, as a string. The format is the same as that returned by [`arrow_typeof`]

#### Example

```
❯ select arrow_cast(-5, 'Int8') as a,
  arrow_cast('foo', 'Dictionary(Int32, Utf8)') as b,
  arrow_cast('bar', 'LargeUtf8') as c,
  arrow_cast('2023-01-02T12:53:02', 'Timestamp(Microsecond, Some("+08:00"))') as d
  ;
+----+-----+-----+---------------------------+
| a  | b   | c   | d                         |
+----+-----+-----+---------------------------+
| -5 | foo | bar | 2023-01-02T12:53:02+08:00 |
+----+-----+-----+---------------------------+
1 row in set. Query took 0.001 seconds.
```

### `arrow_typeof`

Returns the name of the underlying [Arrow data type](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) of the expression:

```
arrow_typeof(expression)
```

#### Arguments

- **expression**: Expression to evaluate.
  Can be a constant, column, or function, and any combination of arithmetic or
  string operators.

#### Example

```
❯ select arrow_typeof('foo'), arrow_typeof(1);
+---------------------------+------------------------+
| arrow_typeof(Utf8("foo")) | arrow_typeof(Int64(1)) |
+---------------------------+------------------------+
| Utf8                      | Int64                  |
+---------------------------+------------------------+
1 row in set. Query took 0.001 seconds.
```
