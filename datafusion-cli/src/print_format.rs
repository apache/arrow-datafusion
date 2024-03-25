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

//! Print format variants

use std::str::FromStr;

use crate::print_options::MaxRows;

use arrow::csv::writer::WriterBuilder;
use arrow::json::{ArrayWriter, LineDelimitedWriter};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion::error::Result;

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ValueEnum, Clone, Copy)]
pub enum PrintFormat {
    Csv,
    Tsv,
    Table,
    Json,
    NdJson,
    Automatic,
}

impl FromStr for PrintFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

macro_rules! batches_to_json {
    ($WRITER: ident, $writer: expr, $batches: expr) => {{
        {
            if !$batches.is_empty() {
                let mut json_writer = $WRITER::new(&mut *$writer);
                for batch in $batches {
                    json_writer.write(batch)?;
                }
                json_writer.finish()?;
                json_finish!($WRITER, $writer);
            }
        }
        Ok(()) as Result<()>
    }};
}

macro_rules! json_finish {
    (ArrayWriter, $writer: expr) => {{
        writeln!($writer)?;
    }};
    (LineDelimitedWriter, $writer: expr) => {{}};
}

fn print_batches_with_sep<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    delimiter: u8,
    with_header: bool,
) -> Result<()> {
    let builder = WriterBuilder::new()
        .with_header(with_header)
        .with_delimiter(delimiter);
    let mut csv_writer = builder.build(writer);

    for batch in batches {
        csv_writer.write(batch)?;
    }

    Ok(())
}

fn keep_only_maxrows(s: &str, maxrows: usize) -> String {
    let lines: Vec<String> = s.lines().map(String::from).collect();

    assert!(lines.len() >= maxrows + 4); // 4 lines for top and bottom border

    let last_line = &lines[lines.len() - 1]; // bottom border line

    let spaces = last_line.len().saturating_sub(4);
    let dotted_line = format!("| .{:<spaces$}|", "", spaces = spaces);

    let mut result = lines[0..(maxrows + 3)].to_vec(); // Keep top border and `maxrows` lines
    result.extend(vec![dotted_line; 3]); // Append ... lines
    result.push(last_line.clone());

    result.join("\n")
}

fn format_batches_with_maxrows<W: std::io::Write>(
    writer: &mut W,
    batches: &[RecordBatch],
    maxrows: MaxRows,
) -> Result<()> {
    match maxrows {
        MaxRows::Limited(maxrows) => {
            // Filter batches to meet the maxrows condition
            let mut filtered_batches = Vec::new();
            let mut row_count: usize = 0;
            let mut over_limit = false;
            for batch in batches {
                if row_count + batch.num_rows() > maxrows {
                    // If adding this batch exceeds maxrows, slice the batch
                    let limit = maxrows - row_count;
                    let sliced_batch = batch.slice(0, limit);
                    filtered_batches.push(sliced_batch);
                    over_limit = true;
                    break;
                } else {
                    filtered_batches.push(batch.clone());
                    row_count += batch.num_rows();
                }
            }

            let formatted = pretty_format_batches_with_options(
                &filtered_batches,
                &DEFAULT_FORMAT_OPTIONS,
            )?;
            if over_limit {
                let mut formatted_str = format!("{}", formatted);
                formatted_str = keep_only_maxrows(&formatted_str, maxrows);
                writeln!(writer, "{}", formatted_str)?;
            } else {
                writeln!(writer, "{}", formatted)?;
            }
        }
        MaxRows::Unlimited => {
            let formatted =
                pretty_format_batches_with_options(batches, &DEFAULT_FORMAT_OPTIONS)?;
            writeln!(writer, "{}", formatted)?;
        }
    }

    Ok(())
}

impl PrintFormat {
    /// Print the batches to a writer using the specified format
    pub fn print_batches<W: std::io::Write>(
        &self,
        writer: &mut W,
        batches: &[RecordBatch],
        maxrows: MaxRows,
        with_header: bool,
    ) -> Result<()> {
        // filter out any empty batches
        let batches: Vec<_> = batches
            .iter()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        if batches.is_empty() {
            return Ok(());
        }

        match self {
            Self::Csv | Self::Automatic => {
                print_batches_with_sep(writer, &batches, b',', with_header)
            }
            Self::Tsv => print_batches_with_sep(writer, &batches, b'\t', with_header),
            Self::Table => {
                if maxrows == MaxRows::Limited(0) {
                    return Ok(());
                }
                format_batches_with_maxrows(writer, &batches, maxrows)
            }
            Self::Json => batches_to_json!(ArrayWriter, writer, &batches),
            Self::NdJson => batches_to_json!(LineDelimitedWriter, writer, &batches),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn print_empty() {
        for format in [
            PrintFormat::Csv,
            PrintFormat::Tsv,
            PrintFormat::Table,
            PrintFormat::Json,
            PrintFormat::NdJson,
            PrintFormat::Automatic,
        ] {
            // no output for empty batches, even with header set
            PrintBatchesTest::new()
                .with_format(format)
                .with_batches(vec![])
                .with_expected(&[""])
                .run();
        }
    }

    #[test]
    fn print_csv_no_header() {
        #[rustfmt::skip]
        let expected = &[
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Csv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::No)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_csv_with_header() {
        #[rustfmt::skip]
        let expected = &[
            "a,b,c",
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Csv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_tsv_no_header() {
        #[rustfmt::skip]
        let expected = &[
            "1\t4\t7",
            "2\t5\t8",
            "3\t6\t9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Tsv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::No)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_tsv_with_header() {
        #[rustfmt::skip]
        let expected = &[
            "a\tb\tc",
            "1\t4\t7",
            "2\t5\t8",
            "3\t6\t9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Tsv)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_table() {
        let expected = &[
            "+---+---+---+",
            "| a | b | c |",
            "+---+---+---+",
            "| 1 | 4 | 7 |",
            "| 2 | 5 | 8 |",
            "| 3 | 6 | 9 |",
            "+---+---+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Ignored)
            .with_expected(expected)
            .run();
    }
    #[test]
    fn print_json() {
        let expected =
            &[r#"[{"a":1,"b":4,"c":7},{"a":2,"b":5,"c":8},{"a":3,"b":6,"c":9}]"#];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Json)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Ignored)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_ndjson() {
        let expected = &[
            r#"{"a":1,"b":4,"c":7}"#,
            r#"{"a":2,"b":5,"c":8}"#,
            r#"{"a":3,"b":6,"c":9}"#,
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::NdJson)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Ignored)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_automatic_no_header() {
        #[rustfmt::skip]
            let expected = &[
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Automatic)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::No)
            .with_expected(expected)
            .run();
    }
    #[test]
    fn print_automatic_with_header() {
        #[rustfmt::skip]
            let expected = &[
            "a,b,c",
            "1,4,7",
            "2,5,8",
            "3,6,9",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Automatic)
            .with_batches(split_batch(three_column_batch()))
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_maxrows_unlimited() {
        #[rustfmt::skip]
            let expected = &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "+---+",
        ];

        // should print out entire output with no truncation if unlimited or
        // limit greater than number of batches or equal to the number of batches
        for max_rows in [MaxRows::Unlimited, MaxRows::Limited(5), MaxRows::Limited(3)] {
            PrintBatchesTest::new()
                .with_format(PrintFormat::Table)
                .with_batches(vec![one_column_batch()])
                .with_maxrows(max_rows)
                .with_expected(expected)
                .run();
        }
    }

    #[test]
    fn print_maxrows_limited_one_batch() {
        #[rustfmt::skip]
            let expected = &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| . |",
            "| . |",
            "| . |",
            "+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![one_column_batch()])
            .with_maxrows(MaxRows::Limited(1))
            .with_expected(expected)
            .run();
    }

    #[test]
    fn print_maxrows_limited_multi_batched() {
        #[rustfmt::skip]
            let expected = &[
            "+---+",
            "| a |",
            "+---+",
            "| 1 |",
            "| 2 |",
            "| 3 |",
            "| 1 |",
            "| 2 |",
            "| . |",
            "| . |",
            "| . |",
            "+---+",
        ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![
                one_column_batch(),
                one_column_batch(),
                one_column_batch(),
            ])
            .with_maxrows(MaxRows::Limited(5))
            .with_expected(expected)
            .run();
    }

    #[test]
    fn test_print_batches_empty_batches() {
        let batch = one_column_batch();
        let empty_batch = RecordBatch::new_empty(batch.schema());

        #[rustfmt::skip]
            let expected =&[
                "+---+",
                "| a |",
                "+---+",
                "| 1 |",
                "| 2 |",
                "| 3 |",
                "+---+",
            ];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![empty_batch.clone(), batch, empty_batch])
            .with_expected(expected)
            .run();
    }

    #[test]
    fn test_print_batches_empty_batches_no_header() {
        let empty_batch = RecordBatch::new_empty(one_column_batch().schema());

        // empty batches should not print a header
        let expected = &[""];

        PrintBatchesTest::new()
            .with_format(PrintFormat::Table)
            .with_batches(vec![empty_batch])
            .with_header(WithHeader::Yes)
            .with_expected(expected)
            .run();
    }

    #[derive(Debug)]
    struct PrintBatchesTest {
        format: PrintFormat,
        batches: Vec<RecordBatch>,
        maxrows: MaxRows,
        with_header: WithHeader,
        expected: Vec<&'static str>,
    }

    /// How to test with_header
    #[derive(Debug, Clone)]
    enum WithHeader {
        Yes,
        No,
        /// output should be the same with or without header
        Ignored,
    }

    impl PrintBatchesTest {
        fn new() -> Self {
            Self {
                format: PrintFormat::Table,
                batches: vec![],
                maxrows: MaxRows::Unlimited,
                with_header: WithHeader::Ignored,
                expected: vec![],
            }
        }

        /// set the format
        fn with_format(mut self, format: PrintFormat) -> Self {
            self.format = format;
            self
        }

        /// set the batches to convert
        fn with_batches(mut self, batches: Vec<RecordBatch>) -> Self {
            self.batches = batches;
            self
        }

        /// set maxrows
        fn with_maxrows(mut self, maxrows: MaxRows) -> Self {
            self.maxrows = maxrows;
            self
        }

        /// set with_header
        fn with_header(mut self, with_header: WithHeader) -> Self {
            self.with_header = with_header;
            self
        }

        /// set expected output
        fn with_expected(mut self, expected: &[&'static str]) -> Self {
            self.expected = expected.to_vec();
            self
        }

        /// run the test
        fn run(self) {
            let actual = self.output();
            let actual: Vec<_> = actual.trim_end().split('\n').collect();
            let expected = self.expected;
            assert_eq!(
                actual, expected,
                "\n\nactual:\n{actual:#?}\n\nexpected:\n{expected:#?}"
            );
        }

        /// formats batches using parameters and returns the resulting output
        fn output(&self) -> String {
            match self.with_header {
                WithHeader::Yes => self.output_with_header(true),
                WithHeader::No => self.output_with_header(false),
                WithHeader::Ignored => {
                    let output = self.output_with_header(true);
                    // ensure the output is the same without header
                    let output_without_header = self.output_with_header(false);
                    assert_eq!(
                        output, output_without_header,
                        "Expected output to be the same with or without header"
                    );
                    output
                }
            }
        }

        fn output_with_header(&self, with_header: bool) -> String {
            let mut buffer: Vec<u8> = vec![];
            self.format
                .print_batches(&mut buffer, &self.batches, self.maxrows, with_header)
                .unwrap();
            String::from_utf8(buffer).unwrap()
        }
    }

    /// Return a batch with three columns and three rows
    fn three_column_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap()
    }

    /// return a batch with one column and three rows
    fn one_column_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap()
    }

    /// Slice the record batch into 2 batches
    fn split_batch(batch: RecordBatch) -> Vec<RecordBatch> {
        assert!(batch.num_rows() > 1);
        let split = batch.num_rows() / 2;
        vec![
            batch.slice(0, split),
            batch.slice(split, batch.num_rows() - split),
        ]
    }
}
