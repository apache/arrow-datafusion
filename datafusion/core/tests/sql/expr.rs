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

use datafusion::datasource::empty::EmptyTable;

use super::*;

#[tokio::test]
async fn test_boolean_expressions() -> Result<()> {
    test_expression!("true", "true");
    test_expression!("false", "false");
    test_expression!("false = false", "true");
    test_expression!("true = false", "false");
    Ok(())
}

#[tokio::test]
async fn test_mathematical_expressions_with_null() -> Result<()> {
    test_expression!("sqrt(NULL)", "NULL");
    test_expression!("cbrt(NULL)", "NULL");
    test_expression!("sin(NULL)", "NULL");
    test_expression!("cos(NULL)", "NULL");
    test_expression!("tan(NULL)", "NULL");
    test_expression!("asin(NULL)", "NULL");
    test_expression!("acos(NULL)", "NULL");
    test_expression!("atan(NULL)", "NULL");
    test_expression!("sinh(NULL)", "NULL");
    test_expression!("cosh(NULL)", "NULL");
    test_expression!("tanh(NULL)", "NULL");
    test_expression!("asinh(NULL)", "NULL");
    test_expression!("acosh(NULL)", "NULL");
    test_expression!("atanh(NULL)", "NULL");
    test_expression!("floor(NULL)", "NULL");
    test_expression!("ceil(NULL)", "NULL");
    test_expression!("round(NULL)", "NULL");
    test_expression!("trunc(NULL)", "NULL");
    test_expression!("abs(NULL)", "NULL");
    test_expression!("signum(NULL)", "NULL");
    test_expression!("exp(NULL)", "NULL");
    test_expression!("ln(NULL)", "NULL");
    test_expression!("log2(NULL)", "NULL");
    test_expression!("log10(NULL)", "NULL");
    test_expression!("power(NULL, 2)", "NULL");
    test_expression!("power(NULL, NULL)", "NULL");
    test_expression!("power(2, NULL)", "NULL");
    test_expression!("atan2(NULL, NULL)", "NULL");
    test_expression!("atan2(1, NULL)", "NULL");
    test_expression!("atan2(NULL, 1)", "NULL");
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "crypto_expressions"), ignore)]
async fn test_crypto_expressions() -> Result<()> {
    test_expression!("md5('tom')", "34b7da764b21d298ef307d04d8152dc5");
    test_expression!("digest('tom','md5')", "34b7da764b21d298ef307d04d8152dc5");
    test_expression!("md5('')", "d41d8cd98f00b204e9800998ecf8427e");
    test_expression!("digest('','md5')", "d41d8cd98f00b204e9800998ecf8427e");
    test_expression!("md5(NULL)", "NULL");
    test_expression!("digest(NULL,'md5')", "NULL");
    test_expression!(
        "sha224('tom')",
        "0bf6cb62649c42a9ae3876ab6f6d92ad36cb5414e495f8873292be4d"
    );
    test_expression!(
        "digest('tom','sha224')",
        "0bf6cb62649c42a9ae3876ab6f6d92ad36cb5414e495f8873292be4d"
    );
    test_expression!(
        "sha224('')",
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
    );
    test_expression!(
        "digest('','sha224')",
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f"
    );
    test_expression!("sha224(NULL)", "NULL");
    test_expression!("digest(NULL,'sha224')", "NULL");
    test_expression!(
        "sha256('tom')",
        "e1608f75c5d7813f3d4031cb30bfb786507d98137538ff8e128a6ff74e84e643"
    );
    test_expression!(
        "digest('tom','sha256')",
        "e1608f75c5d7813f3d4031cb30bfb786507d98137538ff8e128a6ff74e84e643"
    );
    test_expression!(
        "sha256('')",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    test_expression!(
        "digest('','sha256')",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    test_expression!("sha256(NULL)", "NULL");
    test_expression!("digest(NULL,'sha256')", "NULL");
    test_expression!("sha384('tom')", "096f5b68aa77848e4fdf5c1c0b350de2dbfad60ffd7c25d9ea07c6c19b8a4d55a9187eb117c557883f58c16dfac3e343");
    test_expression!("digest('tom','sha384')", "096f5b68aa77848e4fdf5c1c0b350de2dbfad60ffd7c25d9ea07c6c19b8a4d55a9187eb117c557883f58c16dfac3e343");
    test_expression!("sha384('')", "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b");
    test_expression!("digest('','sha384')", "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da274edebfe76f65fbd51ad2f14898b95b");
    test_expression!("sha384(NULL)", "NULL");
    test_expression!("digest(NULL,'sha384')", "NULL");
    test_expression!("sha512('tom')", "6e1b9b3fe840680e37051f7ad5e959d6f39ad0f8885d855166f55c659469d3c8b78118c44a2a49c72ddb481cd6d8731034e11cc030070ba843a90b3495cb8d3e");
    test_expression!("digest('tom','sha512')", "6e1b9b3fe840680e37051f7ad5e959d6f39ad0f8885d855166f55c659469d3c8b78118c44a2a49c72ddb481cd6d8731034e11cc030070ba843a90b3495cb8d3e");
    test_expression!("sha512('')", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
    test_expression!("digest('','sha512')", "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");
    test_expression!("sha512(NULL)", "NULL");
    test_expression!("digest(NULL,'sha512')", "NULL");
    test_expression!("digest(NULL,'blake2s')", "NULL");
    test_expression!("digest(NULL,'blake2b')", "NULL");
    test_expression!("digest('','blake2b')", "786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce");
    test_expression!("digest('tom','blake2b')", "482499a18da10a18d8d35ab5eb4c635551ec5b8d3ff37c3e87a632caf6680fe31566417834b4732e26e0203d1cad4f5366cb7ab57d89694e4c1fda3e26af2c23");
    test_expression!(
        "digest('','blake2s')",
        "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9"
    );
    test_expression!(
        "digest('tom','blake2s')",
        "5fc3f2b3a07cade5023c3df566e4d697d3823ba1b72bfb3e84cf7e768b2e7529"
    );
    test_expression!(
        "digest('','blake3')",
        "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
    );
    Ok(())
}

#[tokio::test]
async fn test_array_index() -> Result<()> {
    // By default PostgreSQL uses a one-based numbering convention for arrays, that is, an array of n elements starts with array[1] and ends with array[n]
    test_expression!("([5,4,3,2,1])[1]", "5");
    test_expression!("([5,4,3,2,1])[2]", "4");
    test_expression!("([5,4,3,2,1])[5]", "1");
    test_expression!("([[1, 2], [2, 3], [3,4]])[1]", "[1, 2]");
    test_expression!("([[1, 2], [2, 3], [3,4]])[3]", "[3, 4]");
    test_expression!("([[1, 2], [2, 3], [3,4]])[1][1]", "1");
    test_expression!("([[1, 2], [2, 3], [3,4]])[2][2]", "3");
    test_expression!("([[1, 2], [2, 3], [3,4]])[3][2]", "4");
    // out of bounds
    test_expression!("([5,4,3,2,1])[0]", "NULL");
    test_expression!("([5,4,3,2,1])[6]", "NULL");
    // test_expression!("([5,4,3,2,1])[-1]", "NULL");
    test_expression!("([5,4,3,2,1])[100]", "NULL");

    Ok(())
}

#[tokio::test]
async fn test_array_literals() -> Result<()> {
    // Named, just another syntax
    test_expression!("ARRAY[1,2,3,4,5]", "[1, 2, 3, 4, 5]");
    // Unnamed variant
    test_expression!("[1,2,3,4,5]", "[1, 2, 3, 4, 5]");
    test_expression!("[true, false]", "[true, false]");
    test_expression!("['str1', 'str2']", "[str1, str2]");
    test_expression!("[[1,2], [3,4]]", "[[1, 2], [3, 4]]");

    // TODO: Not supported in parser, uncomment when it will be available
    // test_expression!(
    //     "[]",
    //     "[]"
    // );

    Ok(())
}

#[tokio::test]
async fn test_struct_literals() -> Result<()> {
    test_expression!("STRUCT(1,2,3,4,5)", "{c0: 1, c1: 2, c2: 3, c3: 4, c4: 5}");
    test_expression!("STRUCT(Null)", "{c0: }");
    test_expression!("STRUCT(2)", "{c0: 2}");
    test_expression!("STRUCT('1',Null)", "{c0: 1, c1: }");
    test_expression!("STRUCT(true, false)", "{c0: true, c1: false}");
    test_expression!("STRUCT('str1', 'str2')", "{c0: str1, c1: str2}");

    Ok(())
}

#[tokio::test]
async fn binary_bitwise_shift() -> Result<()> {
    test_expression!("2 << 10", "2048");
    test_expression!("2048 >> 10", "2");
    test_expression!("2048 << NULL", "NULL");
    test_expression!("2048 >> NULL", "NULL");

    Ok(())
}

#[tokio::test]
async fn test_comparison_func_expressions() -> Result<()> {
    test_expression!("greatest(1,2,3)", "3");
    test_expression!("least(1,2,3)", "1");

    Ok(())
}

#[tokio::test]
async fn test_comparison_func_array_scalar_expression() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t1", Arc::new(table))?;
    let sql = "SELECT greatest(a, 2), least(a, 2) from t1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_batches_eq!(
        &[
            "+-------------------------+----------------------+",
            "| greatest(t1.a,Int64(2)) | least(t1.a,Int64(2)) |",
            "+-------------------------+----------------------+",
            "| 2                       | 1                    |",
            "| 2                       | 2                    |",
            "| 3                       | 2                    |",
            "+-------------------------+----------------------+",
        ],
        &actual
    );
    Ok(())
}

#[tokio::test]
async fn test_interval_expressions() -> Result<()> {
    // day nano intervals
    test_expression!(
        "interval '1'",
        "0 years 0 mons 0 days 0 hours 0 mins 1.000000000 secs"
    );
    test_expression!(
        "interval '1 second'",
        "0 years 0 mons 0 days 0 hours 0 mins 1.000000000 secs"
    );
    test_expression!(
        "interval '500 milliseconds'",
        "0 years 0 mons 0 days 0 hours 0 mins 0.500000000 secs"
    );
    test_expression!(
        "interval '5 second'",
        "0 years 0 mons 0 days 0 hours 0 mins 5.000000000 secs"
    );
    test_expression!(
        "interval '0.5 minute'",
        "0 years 0 mons 0 days 0 hours 0 mins 30.000000000 secs"
    );
    test_expression!(
        "interval '.5 minute'",
        "0 years 0 mons 0 days 0 hours 0 mins 30.000000000 secs"
    );
    test_expression!(
        "interval '5 minute'",
        "0 years 0 mons 0 days 0 hours 5 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '5 minute 1 second'",
        "0 years 0 mons 0 days 0 hours 5 mins 1.000000000 secs"
    );
    test_expression!(
        "interval '1 hour'",
        "0 years 0 mons 0 days 1 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '5 hour'",
        "0 years 0 mons 0 days 5 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 day'",
        "0 years 0 mons 1 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 week'",
        "0 years 0 mons 7 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '2 weeks'",
        "0 years 0 mons 14 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 day 1'",
        "0 years 0 mons 1 days 0 hours 0 mins 1.000000000 secs"
    );
    test_expression!(
        "interval '0.5'",
        "0 years 0 mons 0 days 0 hours 0 mins 0.500000000 secs"
    );
    test_expression!(
        "interval '0.5 day 1'",
        "0 years 0 mons 0 days 12 hours 0 mins 1.000000000 secs"
    );
    test_expression!(
        "interval '0.49 day'",
        "0 years 0 mons 0 days 11 hours 45 mins 36.000000000 secs"
    );
    test_expression!(
        "interval '0.499 day'",
        "0 years 0 mons 0 days 11 hours 58 mins 33.600000000 secs"
    );
    test_expression!(
        "interval '0.4999 day'",
        "0 years 0 mons 0 days 11 hours 59 mins 51.360000000 secs"
    );
    test_expression!(
        "interval '0.49999 day'",
        "0 years 0 mons 0 days 11 hours 59 mins 59.136000000 secs"
    );
    test_expression!(
        "interval '0.49999999999 day'",
        "0 years 0 mons 0 days 11 hours 59 mins 59.999999136 secs"
    );
    test_expression!(
        "interval '5 day'",
        "0 years 0 mons 5 days 0 hours 0 mins 0.000000000 secs"
    );
    // Hour is ignored, this matches PostgreSQL
    test_expression!(
        "interval '5 day' hour",
        "0 years 0 mons 5 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '5 day 4 hours 3 minutes 2 seconds 100 milliseconds'",
        "0 years 0 mons 5 days 4 hours 3 mins 2.100000000 secs"
    );
    // month intervals
    test_expression!(
        "interval '0.5 month'",
        "0 years 0 mons 15 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '0.5' month",
        "0 years 0 mons 15 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 month'",
        "0 years 1 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1' MONTH",
        "0 years 1 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '5 month'",
        "0 years 5 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '13 month'",
        "0 years 13 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '0.5 year'",
        "0 years 6 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 year'",
        "0 years 12 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 decade'",
        "0 years 120 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '2 decades'",
        "0 years 240 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 century'",
        "0 years 1200 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '2 year'",
        "0 years 24 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '2' year",
        "0 years 24 mons 0 days 0 hours 0 mins 0.000000000 secs"
    );
    // complex
    test_expression!(
        "interval '1 year 1 day'",
        "0 years 12 mons 1 days 0 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 year 1 day 1 hour'",
        "0 years 12 mons 1 days 1 hours 0 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 year 1 day 1 hour 1 minute'",
        "0 years 12 mons 1 days 1 hours 1 mins 0.000000000 secs"
    );
    test_expression!(
        "interval '1 year 1 day 1 hour 1 minute 1 second'",
        "0 years 12 mons 1 days 1 hours 1 mins 1.000000000 secs"
    );

    Ok(())
}

#[cfg(feature = "unicode_expressions")]
#[tokio::test]
async fn test_substring_expr() -> Result<()> {
    test_expression!("substring('alphabet' from 2 for 1)", "l");
    test_expression!("substring('alphabet' from 8)", "t");
    test_expression!("substring('alphabet' for 1)", "a");

    Ok(())
}

#[tokio::test]
async fn test_string_expressions() -> Result<()> {
    test_expression!("ascii('')", "0");
    test_expression!("ascii('x')", "120");
    test_expression!("ascii(NULL)", "NULL");
    test_expression!("bit_length('')", "0");
    test_expression!("bit_length('chars')", "40");
    test_expression!("bit_length('josé')", "40");
    test_expression!("bit_length(NULL)", "NULL");
    test_expression!("btrim(' xyxtrimyyx ', NULL)", "NULL");
    test_expression!("btrim(' xyxtrimyyx ')", "xyxtrimyyx");
    test_expression!("btrim('\n xyxtrimyyx \n')", "\n xyxtrimyyx \n");
    test_expression!("btrim('xyxtrimyyx', 'xyz')", "trim");
    test_expression!("btrim('\nxyxtrimyyx\n', 'xyz\n')", "trim");
    test_expression!("btrim(NULL, 'xyz')", "NULL");
    test_expression!("chr(CAST(120 AS int))", "x");
    test_expression!("chr(CAST(128175 AS int))", "💯");
    test_expression!("chr(CAST(NULL AS int))", "NULL");
    test_expression!("concat('a','b','c')", "abc");
    test_expression!("concat('abcde', 2, NULL, 22)", "abcde222");
    test_expression!("concat(NULL)", "");
    test_expression!("concat_ws(',', 'abcde', 2, NULL, 22)", "abcde,2,22");
    test_expression!("concat_ws('|','a','b','c')", "a|b|c");
    test_expression!("concat_ws('|',NULL)", "");
    test_expression!("concat_ws(NULL,'a',NULL,'b','c')", "NULL");
    test_expression!("concat_ws('|','a',NULL)", "a");
    test_expression!("concat_ws('|','a',NULL,NULL)", "a");
    test_expression!("initcap('')", "");
    test_expression!("initcap('hi THOMAS')", "Hi Thomas");
    test_expression!("initcap(NULL)", "NULL");
    test_expression!("lower('')", "");
    test_expression!("lower('TOM')", "tom");
    test_expression!("lower(NULL)", "NULL");
    test_expression!("ltrim(' zzzytest ', NULL)", "NULL");
    test_expression!("ltrim(' zzzytest ')", "zzzytest ");
    test_expression!("ltrim('zzzytest', 'xyz')", "test");
    test_expression!("ltrim(NULL, 'xyz')", "NULL");
    test_expression!("octet_length('')", "0");
    test_expression!("octet_length('chars')", "5");
    test_expression!("octet_length('josé')", "5");
    test_expression!("octet_length(NULL)", "NULL");
    test_expression!("repeat('Pg', 4)", "PgPgPgPg");
    test_expression!("repeat('Pg', CAST(NULL AS INT))", "NULL");
    test_expression!("repeat(NULL, 4)", "NULL");
    test_expression!("replace('abcdefabcdef', 'cd', 'XX')", "abXXefabXXef");
    test_expression!("replace('abcdefabcdef', 'cd', NULL)", "NULL");
    test_expression!("replace('abcdefabcdef', 'notmatch', 'XX')", "abcdefabcdef");
    test_expression!("replace('abcdefabcdef', NULL, 'XX')", "NULL");
    test_expression!("replace(NULL, 'cd', 'XX')", "NULL");
    test_expression!("rtrim(' testxxzx ')", " testxxzx");
    test_expression!("rtrim(' zzzytest ', NULL)", "NULL");
    test_expression!("rtrim('testxxzx', 'xyz')", "test");
    test_expression!("rtrim(NULL, 'xyz')", "NULL");
    test_expression!("split_part('abc~@~def~@~ghi', '~@~', 2)", "def");
    test_expression!("split_part('abc~@~def~@~ghi', '~@~', 20)", "");
    test_expression!("split_part(NULL, '~@~', 20)", "NULL");
    test_expression!("split_part('abc~@~def~@~ghi', NULL, 20)", "NULL");
    test_expression!(
        "split_part('abc~@~def~@~ghi', '~@~', CAST(NULL AS INT))",
        "NULL"
    );
    test_expression!("starts_with('alphabet', 'alph')", "true");
    test_expression!("starts_with('alphabet', 'blph')", "false");
    test_expression!("starts_with(NULL, 'blph')", "NULL");
    test_expression!("starts_with('alphabet', NULL)", "NULL");
    test_expression!("to_hex(2147483647)", "7fffffff");
    test_expression!("to_hex(9223372036854775807)", "7fffffffffffffff");
    test_expression!("to_hex(CAST(NULL AS int))", "NULL");
    test_expression!("trim(' tom ')", "tom");
    test_expression!("trim(LEADING ' tom ')", "tom ");
    test_expression!("trim(TRAILING ' tom ')", " tom");
    test_expression!("trim(BOTH ' tom ')", "tom");
    test_expression!("trim(LEADING ' ' FROM ' tom ')", "tom ");
    test_expression!("trim(TRAILING ' ' FROM ' tom ')", " tom");
    test_expression!("trim(BOTH ' ' FROM ' tom ')", "tom");
    test_expression!("trim(' ' FROM ' tom ')", "tom");
    test_expression!("trim(LEADING 'x' FROM 'xxxtomxxx')", "tomxxx");
    test_expression!("trim(TRAILING 'x' FROM 'xxxtomxxx')", "xxxtom");
    test_expression!("trim(BOTH 'x' FROM 'xxxtomxx')", "tom");
    test_expression!("trim('x' FROM 'xxxtomxx')", "tom");
    test_expression!("trim(LEADING 'xy' FROM 'xyxabcxyzdefxyx')", "abcxyzdefxyx");
    test_expression!("trim(TRAILING 'xy' FROM 'xyxabcxyzdefxyx')", "xyxabcxyzdef");
    test_expression!("trim(BOTH 'xy' FROM 'xyxabcxyzdefxyx')", "abcxyzdef");
    test_expression!("trim('xy' FROM 'xyxabcxyzdefxyx')", "abcxyzdef");
    test_expression!("trim(' tom')", "tom");
    test_expression!("trim('')", "");
    test_expression!("trim('tom ')", "tom");
    test_expression!("upper('')", "");
    test_expression!("upper('tom')", "TOM");
    test_expression!("upper(NULL)", "NULL");
    Ok(())
}

#[tokio::test]
#[cfg_attr(not(feature = "regex_expressions"), ignore)]
async fn test_regex_expressions() -> Result<()> {
    test_expression!("regexp_replace('ABCabcABC', '(abc)', 'X', 'gi')", "XXX");
    test_expression!("regexp_replace('ABCabcABC', '(abc)', 'X', 'i')", "XabcABC");
    test_expression!("regexp_replace('foobarbaz', 'b..', 'X', 'g')", "fooXX");
    test_expression!("regexp_replace('foobarbaz', 'b..', 'X')", "fooXbaz");
    test_expression!(
        "regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g')",
        "fooXarYXazY"
    );
    test_expression!(
        "regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', NULL)",
        "NULL"
    );
    test_expression!("regexp_replace('foobarbaz', 'b(..)', NULL, 'g')", "NULL");
    test_expression!("regexp_replace('foobarbaz', NULL, 'X\\1Y', 'g')", "NULL");
    test_expression!("regexp_replace('Thomas', '.[mN]a.', 'M')", "ThM");
    test_expression!("regexp_replace(NULL, 'b(..)', 'X\\1Y', 'g')", "NULL");
    test_expression!("regexp_match('foobarbequebaz', '')", "[]");
    test_expression!(
        "regexp_match('foobarbequebaz', '(bar)(beque)')",
        "[bar, beque]"
    );
    test_expression!("regexp_match('foobarbequebaz', '(ba3r)(bequ34e)')", "NULL");
    test_expression!("regexp_match('aaa-0', '.*-(\\d)')", "[0]");
    test_expression!("regexp_match('bb-1', '.*-(\\d)')", "[1]");
    test_expression!("regexp_match('aa', '.*-(\\d)')", "NULL");
    test_expression!("regexp_match(NULL, '.*-(\\d)')", "NULL");
    test_expression!("regexp_match('aaa-0', NULL)", "NULL");
    Ok(())
}

#[tokio::test]
async fn test_cast_expressions() -> Result<()> {
    test_expression!("CAST([1,2,3,4] AS INT[])", "[1, 2, 3, 4]");
    test_expression!(
        "CAST([1,2,3,4] AS NUMERIC(10,4)[])",
        "[1.0000, 2.0000, 3.0000, 4.0000]"
    );
    test_expression!("CAST('0' AS INT)", "0");
    test_expression!("CAST(NULL AS INT)", "NULL");
    test_expression!("TRY_CAST('0' AS INT)", "0");
    test_expression!("TRY_CAST('x' AS INT)", "NULL");
    Ok(())
}

#[tokio::test]
async fn test_random_expression() -> Result<()> {
    let ctx = create_ctx();
    let sql = "SELECT random() r1";
    let actual = execute(&ctx, sql).await;
    let r1 = actual[0][0].parse::<f64>().unwrap();
    assert!(0.0 <= r1);
    assert!(r1 < 1.0);
    Ok(())
}

#[tokio::test]
async fn test_uuid_expression() -> Result<()> {
    let ctx = create_ctx();
    let sql = "SELECT uuid()";
    let actual = execute(&ctx, sql).await;
    let uuid = actual[0][0].parse::<uuid::Uuid>().unwrap();
    assert_eq!(uuid.get_version_num(), 4);
    Ok(())
}

#[tokio::test]
async fn test_extract_date_part() -> Result<()> {
    test_expression!("date_part('YEAR', CAST('2000-01-01' AS DATE))", "2000.0");
    test_expression!(
        "EXTRACT(year FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "2020.0"
    );
    test_expression!("date_part('QUARTER', CAST('2000-01-01' AS DATE))", "1.0");
    test_expression!(
        "EXTRACT(quarter FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "3.0"
    );
    test_expression!("date_part('MONTH', CAST('2000-01-01' AS DATE))", "1.0");
    test_expression!(
        "EXTRACT(month FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "9.0"
    );
    test_expression!("date_part('WEEK', CAST('2003-01-01' AS DATE))", "1.0");
    test_expression!(
        "EXTRACT(WEEK FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "37.0"
    );
    test_expression!("date_part('DAY', CAST('2000-01-01' AS DATE))", "1.0");
    test_expression!(
        "EXTRACT(day FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "8.0"
    );
    test_expression!("date_part('DOY', CAST('2000-01-01' AS DATE))", "1.0");
    test_expression!(
        "EXTRACT(doy FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "252.0"
    );
    test_expression!("date_part('DOW', CAST('2000-01-01' AS DATE))", "6.0");
    test_expression!(
        "EXTRACT(dow FROM to_timestamp('2020-09-08T12:00:00+00:00'))",
        "2.0"
    );
    test_expression!("date_part('HOUR', CAST('2000-01-01' AS DATE))", "0.0");
    test_expression!(
        "EXTRACT(hour FROM to_timestamp('2020-09-08T12:03:03+00:00'))",
        "12.0"
    );
    test_expression!(
        "EXTRACT(minute FROM to_timestamp('2020-09-08T12:12:00+00:00'))",
        "12.0"
    );
    test_expression!(
        "date_part('minute', to_timestamp('2020-09-08T12:12:00+00:00'))",
        "12.0"
    );
    test_expression!(
        "EXTRACT(second FROM to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "12.12345678"
    );
    test_expression!(
        "EXTRACT(millisecond FROM to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "12123.45678"
    );
    test_expression!(
        "EXTRACT(microsecond FROM to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "12123456.78"
    );
    test_expression!(
        "EXTRACT(nanosecond FROM to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "1.212345678e10"
    );
    test_expression!(
        "date_part('second', to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "12.12345678"
    );
    test_expression!(
        "date_part('millisecond', to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "12123.45678"
    );
    test_expression!(
        "date_part('microsecond', to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "12123456.78"
    );
    test_expression!(
        "date_part('nanosecond', to_timestamp('2020-09-08T12:00:12.12345678+00:00'))",
        "1.212345678e10"
    );
    Ok(())
}

#[tokio::test]
async fn test_extract_epoch() -> Result<()> {
    test_expression!(
        "extract(epoch from '1870-01-01T07:29:10.256'::timestamp)",
        "-3155646649.744"
    );
    test_expression!(
        "extract(epoch from '2000-01-01T00:00:00.000'::timestamp)",
        "946684800.0"
    );
    test_expression!(
        "extract(epoch from to_timestamp('2000-01-01T00:00:00+00:00'))",
        "946684800.0"
    );
    test_expression!("extract(epoch from NULL::timestamp)", "NULL");
    Ok(())
}

#[tokio::test]
async fn test_extract_date_part_func() -> Result<()> {
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "year"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "quarter"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "month"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "week"
        ),
        "true"
    );
    test_expression!(
        format!("(date_part('{0}', now()) = EXTRACT({0} FROM now()))", "day"),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "hour"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "minute"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "second"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "millisecond"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "microsecond"
        ),
        "true"
    );
    test_expression!(
        format!(
            "(date_part('{0}', now()) = EXTRACT({0} FROM now()))",
            "nanosecond"
        ),
        "true"
    );

    Ok(())
}

#[tokio::test]
async fn test_in_list_scalar() -> Result<()> {
    test_expression!("'a' IN ('a','b')", "true");
    test_expression!("'c' IN ('a','b')", "false");
    test_expression!("'c' NOT IN ('a','b')", "true");
    test_expression!("'a' NOT IN ('a','b')", "false");
    test_expression!("NULL IN ('a','b')", "NULL");
    test_expression!("NULL NOT IN ('a','b')", "NULL");
    test_expression!("'a' IN ('a','b',NULL)", "true");
    test_expression!("'c' IN ('a','b',NULL)", "NULL");
    test_expression!("'a' NOT IN ('a','b',NULL)", "false");
    test_expression!("'c' NOT IN ('a','b',NULL)", "NULL");
    test_expression!("0 IN (0,1,2)", "true");
    test_expression!("3 IN (0,1,2)", "false");
    test_expression!("3 NOT IN (0,1,2)", "true");
    test_expression!("0 NOT IN (0,1,2)", "false");
    test_expression!("NULL IN (0,1,2)", "NULL");
    test_expression!("NULL NOT IN (0,1,2)", "NULL");
    test_expression!("0 IN (0,1,2,NULL)", "true");
    test_expression!("3 IN (0,1,2,NULL)", "NULL");
    test_expression!("0 NOT IN (0,1,2,NULL)", "false");
    test_expression!("3 NOT IN (0,1,2,NULL)", "NULL");
    test_expression!("0.0 IN (0.0,0.1,0.2)", "true");
    test_expression!("0.3 IN (0.0,0.1,0.2)", "false");
    test_expression!("0.3 NOT IN (0.0,0.1,0.2)", "true");
    test_expression!("0.0 NOT IN (0.0,0.1,0.2)", "false");
    test_expression!("NULL IN (0.0,0.1,0.2)", "NULL");
    test_expression!("NULL NOT IN (0.0,0.1,0.2)", "NULL");
    test_expression!("0.0 IN (0.0,0.1,0.2,NULL)", "true");
    test_expression!("0.3 IN (0.0,0.1,0.2,NULL)", "NULL");
    test_expression!("0.0 NOT IN (0.0,0.1,0.2,NULL)", "false");
    test_expression!("0.3 NOT IN (0.0,0.1,0.2,NULL)", "NULL");
    test_expression!("'1' IN ('a','b',1)", "true");
    test_expression!("'2' IN ('a','b',1)", "false");
    test_expression!("'2' NOT IN ('a','b',1)", "true");
    test_expression!("'1' NOT IN ('a','b',1)", "false");
    test_expression!("NULL IN ('a','b',1)", "NULL");
    test_expression!("NULL NOT IN ('a','b',1)", "NULL");
    test_expression!("'1' IN ('a','b',NULL,1)", "true");
    test_expression!("'2' IN ('a','b',NULL,1)", "NULL");
    test_expression!("'1' NOT IN ('a','b',NULL,1)", "false");
    test_expression!("'2' NOT IN ('a','b',NULL,1)", "NULL");
    Ok(())
}

#[tokio::test]
async fn csv_query_nullif_divide_by_0() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT c8/nullif(c7, 0) FROM aggregate_test_100";
    let actual = execute(&ctx, sql).await;
    let actual = &actual[80..90]; // We just want to compare rows 80-89
    let expected = vec![
        vec!["258"],
        vec!["664"],
        vec!["NULL"],
        vec!["22"],
        vec!["164"],
        vec!["448"],
        vec!["365"],
        vec!["1640"],
        vec!["671"],
        vec!["203"],
    ];
    assert_eq!(expected, actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_avg_sqrt() -> Result<()> {
    let ctx = create_ctx();
    register_aggregate_csv(&ctx).await?;
    let sql = "SELECT avg(custom_sqrt(c12)) FROM aggregate_test_100";
    let mut actual = execute(&ctx, sql).await;
    actual.sort();
    let expected = vec![vec!["0.6706002946036462"]];
    assert_float_eq(&expected, &actual);
    Ok(())
}

#[tokio::test]
async fn nested_subquery() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("a", DataType::Int16, false),
    ]);
    let empty_table = Arc::new(EmptyTable::new(Arc::new(schema)));
    ctx.register_table("t1", empty_table.clone())?;
    ctx.register_table("t2", empty_table)?;
    let sql = "SELECT COUNT(*) as cnt \
    FROM (\
        (SELECT id FROM t1) EXCEPT \
        (SELECT id FROM t2)\
        ) foo";
    let actual = execute_to_batches(&ctx, sql).await;
    // the purpose of this test is just to make sure the query produces a valid plan
    #[rustfmt::skip]
    let expected = vec![
        "+-----+",
        "| cnt |",
        "+-----+",
        "| 0   |",
        "+-----+"
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn comparisons_with_null_lt() {
    let ctx = SessionContext::new();

    // we expect all the following queries to yield a two null values
    let cases = vec![
        // 1. Numeric comparison with NULL
        "select column1 < NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 <= NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 > NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 >= NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 = NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select column1 != NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // 1.1 Float value comparison with NULL
        "select column3 < NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // String comparison with NULL
        "select column2 < NULL from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // Boolean comparison with NULL
        "select column1 < NULL from (VALUES (true), (false)) as t",
        // ----
        // ---- same queries, reversed argument order (as they go through
        // ---- a different evaluation path)
        // ----

        // 1. Numeric comparison with NULL
        "select NULL < column1  from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL <= column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL > column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL >= column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL = column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        "select NULL != column1 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // 1.1 Float value comparison with NULL
        "select NULL < column3 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // String comparison with NULL
        "select NULL < column2 from (VALUES (1, 'foo' ,2.3), (2, 'bar', 5.4)) as t",
        // Boolean comparison with NULL
        "select NULL < column1 from (VALUES (true), (false)) as t",
    ];

    for sql in cases {
        println!("Computing: {sql}");

        let mut actual = execute_to_batches(&ctx, sql).await;
        assert_eq!(actual.len(), 1);

        let batch = actual.pop().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.columns()[0].is_null(0));
        assert!(batch.columns()[0].is_null(1));
    }
}

#[tokio::test]
async fn binary_mathematical_operator_with_null_lt() {
    let ctx = SessionContext::new();

    let cases = vec![
        // 1. Integer and NULL
        "select column1 + NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 - NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 * NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 / NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column1 % NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        // 2. Float and NULL
        "select column2 + NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 - NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 * NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 / NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select column2 % NULL from (VALUES (1, 2.3), (2, 5.4)) as t",
        // ----
        // ---- same queries, reversed argument order
        // ----
        // 3. NULL and Integer
        "select NULL + column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL - column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL * column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL / column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL % column1 from (VALUES (1, 2.3), (2, 5.4)) as t",
        // 4. NULL and Float
        "select NULL + column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL - column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL * column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL / column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
        "select NULL % column2 from (VALUES (1, 2.3), (2, 5.4)) as t",
    ];

    for sql in cases {
        println!("Computing: {sql}");

        let mut actual = execute_to_batches(&ctx, sql).await;
        assert_eq!(actual.len(), 1);

        let batch = actual.pop().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.columns()[0].is_null(0));
        assert!(batch.columns()[0].is_null(1));
    }
}

#[tokio::test]
async fn query_binary_eq() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Binary, true),
        Field::new("c2", DataType::LargeBinary, true),
        Field::new("c3", DataType::Binary, true),
        Field::new("c4", DataType::LargeBinary, true),
    ]));

    let c1 = BinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b"two"),
        None,
        Some(b""),
        Some(b"three"),
    ]);
    let c2 = LargeBinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b"two"),
        None,
        Some(b""),
        Some(b"three"),
    ]);
    let c3 = BinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b""),
        None,
        Some(b""),
        Some(b"three"),
    ]);
    let c4 = LargeBinaryArray::from_opt_vec(vec![
        Some(b"one"),
        Some(b"two"),
        None,
        Some(b""),
        Some(b""),
    ]);

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(c1), Arc::new(c2), Arc::new(c3), Arc::new(c4)],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("test", data)?;

    let sql = "
        SELECT sha256(c1)=digest('one', 'sha256'), sha256(c2)=sha256('two'), digest(c1, 'blake2b')=digest(c3, 'blake2b'), c2=c4
        FROM test
    ";
    let actual = execute(&ctx, sql).await;
    let expected = vec![
        vec!["true", "false", "true", "true"],
        vec!["false", "true", "false", "true"],
        vec!["NULL", "NULL", "NULL", "NULL"],
        vec!["false", "false", "true", "true"],
        vec!["false", "false", "true", "false"],
    ];
    assert_eq!(expected, actual);
    Ok(())
}
