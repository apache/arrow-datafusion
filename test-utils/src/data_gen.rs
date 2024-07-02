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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Decimal128Builder, Int32Builder, StringBuilder, StringDictionaryBuilder,
    TimestampNanosecondBuilder, UInt16Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone)]
struct GeneratorOptions {
    row_limit: usize,
    pods_per_host: Range<usize>,
    containers_per_pod: Range<usize>,
    entries_per_container: Range<usize>,
}

impl Default for GeneratorOptions {
    fn default() -> Self {
        Self {
            row_limit: usize::MAX,
            pods_per_host: 1..15,
            containers_per_pod: 1..3,
            entries_per_container: 1024..8192,
        }
    }
}

/// Creates access log like entries
#[derive(Default)]
struct BatchBuilder {
    service: StringDictionaryBuilder<Int32Type>,
    host: StringDictionaryBuilder<Int32Type>,
    pod: StringDictionaryBuilder<Int32Type>,
    container: StringDictionaryBuilder<Int32Type>,
    image: StringDictionaryBuilder<Int32Type>,
    time: TimestampNanosecondBuilder,
    client_addr: StringBuilder,
    request_duration: Int32Builder,
    request_user_agent: StringBuilder,
    request_method: StringBuilder,
    request_host: StringBuilder,
    request_bytes: Int32Builder,
    response_bytes: Int32Builder,
    response_status: UInt16Builder,
    prices_status: Decimal128Builder,

    options: GeneratorOptions,
    row_count: usize,
}

impl BatchBuilder {
    fn schema() -> SchemaRef {
        let utf8_dict =
            || DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        Arc::new(Schema::new(vec![
            Field::new("service", utf8_dict(), true),
            Field::new("host", utf8_dict(), false),
            Field::new("pod", utf8_dict(), false),
            Field::new("container", utf8_dict(), false),
            Field::new("image", utf8_dict(), false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("client_addr", DataType::Utf8, true),
            Field::new("request_duration_ns", DataType::Int32, false),
            Field::new("request_user_agent", DataType::Utf8, true),
            Field::new("request_method", DataType::Utf8, true),
            Field::new("request_host", DataType::Utf8, true),
            Field::new("request_bytes", DataType::Int32, true),
            Field::new("response_bytes", DataType::Int32, true),
            Field::new("response_status", DataType::UInt16, false),
            Field::new("decimal_price", DataType::Decimal128(38, 0), false),
        ]))
    }

    fn is_finished(&self) -> bool {
        self.options.row_limit <= self.row_count
    }

    fn append(&mut self, rng: &mut StdRng, host: &str, service: &str) {
        let num_pods = rng.gen_range(self.options.pods_per_host.clone());
        let pods = generate_sorted_strings(rng, num_pods, 30..40);
        for pod in pods {
            let num_containers = rng.gen_range(self.options.containers_per_pod.clone());
            for container_idx in 0..num_containers {
                let container = format!("{service}_container_{container_idx}");
                let image = format!(
                    "{container}@sha256:30375999bf03beec2187843017b10c9e88d8b1a91615df4eb6350fb39472edd9"
                );

                let num_entries =
                    rng.gen_range(self.options.entries_per_container.clone());
                for i in 0..num_entries {
                    if self.is_finished() {
                        return;
                    }

                    let time = i as i64 * 1024;
                    self.append_row(rng, host, &pod, service, &container, &image, time);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_row(
        &mut self,
        rng: &mut StdRng,
        host: &str,
        pod: &str,
        service: &str,
        container: &str,
        image: &str,
        time: i64,
    ) {
        self.row_count += 1;

        let methods = &["GET", "PUT", "POST", "HEAD", "PATCH", "DELETE"];
        let status = &[200, 204, 400, 503, 403];

        self.service.append(service).unwrap();
        self.host.append(host).unwrap();
        self.pod.append(pod).unwrap();
        self.container.append(container).unwrap();
        self.image.append(image).unwrap();
        self.time.append_value(time);

        self.client_addr.append_value(format!(
            "{}.{}.{}.{}",
            rng.gen::<u8>(),
            rng.gen::<u8>(),
            rng.gen::<u8>(),
            rng.gen::<u8>()
        ));
        self.request_duration.append_value(rng.gen());
        self.request_user_agent
            .append_value(random_string(rng, 20..100));
        self.request_method
            .append_value(methods[rng.gen_range(0..methods.len())]);
        self.request_host
            .append_value(format!("https://{service}.mydomain.com"));

        self.request_bytes
            .append_option(rng.gen_bool(0.9).then(|| rng.gen()));
        self.response_bytes
            .append_option(rng.gen_bool(0.9).then(|| rng.gen()));
        self.response_status
            .append_value(status[rng.gen_range(0..status.len())]);
        self.prices_status.append_value(self.row_count as i128);
    }

    fn finish(mut self, schema: SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.service.finish()),
                Arc::new(self.host.finish()),
                Arc::new(self.pod.finish()),
                Arc::new(self.container.finish()),
                Arc::new(self.image.finish()),
                Arc::new(self.time.finish()),
                Arc::new(self.client_addr.finish()),
                Arc::new(self.request_duration.finish()),
                Arc::new(self.request_user_agent.finish()),
                Arc::new(self.request_method.finish()),
                Arc::new(self.request_host.finish()),
                Arc::new(self.request_bytes.finish()),
                Arc::new(self.response_bytes.finish()),
                Arc::new(self.response_status.finish()),
                Arc::new(
                    self.prices_status
                        .finish()
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ),
            ],
        )
        .unwrap()
    }
}

fn random_string(rng: &mut StdRng, len_range: Range<usize>) -> String {
    let len = rng.gen_range(len_range);
    (0..len)
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect::<String>()
}

fn generate_sorted_strings(
    rng: &mut StdRng,
    count: usize,
    str_len: Range<usize>,
) -> Vec<String> {
    let mut strings: Vec<_> = (0..count)
        .map(|_| random_string(rng, str_len.clone()))
        .collect();

    strings.sort_unstable();
    strings
}

/// Iterator that generates sorted, [`RecordBatch`]es with randomly generated data with
/// an access log style schema for tracing or monitoring type
/// usecases.
///
/// This is useful for writing tests queries on such data
///
/// Here are the columns with example data:
///
/// ```text
/// service:             'backend'
/// host:                'i-1ec3ca3151468928.ec2.internal'
/// pod:                 'aqcathnxqsphdhgjtgvxsfyiwbmhlmg'
/// container:           'backend_container_0'
/// image:               'backend_container_0@sha256:30375999bf03beec2187843017b10c9e88d8b1a91615df4eb6350fb39472edd9'
/// time:                '1970-01-01 00:00:00'
/// client_addr:         '127.216.178.64'
/// request_duration_ns: -1261239112
/// request_user_agent:  'kxttrfiiietlsaygzphhwlqcgngnumuphliejmxfdznuurswhdcicrlprbnocibvsbukiohjjbjdygwbfhxqvurm'
/// request_method:      'PUT'
/// request_host:        'https://backend.mydomain.com'
/// request_bytes:       -312099516
/// response_bytes:      1448834362
/// response_status:     200
/// ```
#[derive(Debug)]
pub struct AccessLogGenerator {
    schema: SchemaRef,
    rng: StdRng,
    host_idx: usize,
    /// maximum rows per batch
    max_batch_size: usize,
    /// How many rows have been returned so far
    row_count: usize,
    /// Options
    options: GeneratorOptions,
}

impl Default for AccessLogGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl AccessLogGenerator {
    pub fn new() -> Self {
        let seed = [
            1, 0, 0, 0, 23, 0, 3, 0, 200, 1, 0, 0, 210, 30, 8, 0, 1, 0, 21, 0, 6, 0, 0,
            0, 0, 0, 5, 0, 0, 0, 0, 0,
        ];

        Self {
            schema: BatchBuilder::schema(),
            host_idx: 0,
            rng: StdRng::from_seed(seed),
            max_batch_size: usize::MAX,
            row_count: 0,
            options: Default::default(),
        }
    }

    /// Return the schema of the [`RecordBatch`]es  created
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Limit the maximum batch size
    pub fn with_max_batch_size(mut self, batch_size: usize) -> Self {
        self.max_batch_size = batch_size;
        self
    }

    /// Return up to row_limit rows;
    pub fn with_row_limit(mut self, row_limit: usize) -> Self {
        self.options.row_limit = row_limit;
        self
    }

    /// Set the number of pods per host
    pub fn with_pods_per_host(mut self, range: Range<usize>) -> Self {
        self.options.pods_per_host = range;
        self
    }

    /// Set the number of containers per pod
    pub fn with_containers_per_pod(mut self, range: Range<usize>) -> Self {
        self.options.containers_per_pod = range;
        self
    }

    /// Set the number of log entries per container
    pub fn with_entries_per_container(mut self, range: Range<usize>) -> Self {
        self.options.entries_per_container = range;
        self
    }
}

impl Iterator for AccessLogGenerator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_count == self.options.row_limit {
            return None;
        }

        let row_limit = self
            .max_batch_size
            .min(self.options.row_limit - self.row_count);

        let mut builder = BatchBuilder {
            options: GeneratorOptions {
                row_limit,
                ..self.options.clone()
            },
            ..Default::default()
        };

        let host = format!(
            "i-{:016x}.ec2.internal",
            self.host_idx * 0x7d87f8ed5c5 + 0x1ec3ca3151468928
        );
        self.host_idx += 1;

        for service in &["frontend", "backend", "database", "cache"] {
            if self.rng.gen_bool(0.5) {
                continue;
            }
            if builder.is_finished() {
                break;
            }
            builder.append(&mut self.rng, &host, service);
        }

        let batch = builder.finish(Arc::clone(&self.schema));

        self.row_count += batch.num_rows();
        Some(batch)
    }
}
