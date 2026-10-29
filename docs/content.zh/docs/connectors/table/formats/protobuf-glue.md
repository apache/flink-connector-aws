---
title: "Protobuf (Glue Schema Registry)"
weight: 3
type: docs
---
<!--
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

# Protobuf Format (AWS Glue Schema Registry)

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The Protobuf Glue Schema Registry format (`protobuf-glue`) allows you to read and write Protocol Buffers data with schemas managed by [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

Dependencies
------------

{{< sql_connector_download_table "protobuf-glue" >}}

The Protobuf-Glue format is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

#### SQL Client JAR

For SQL Client usage, download the fat JAR `flink-sql-protobuf-glue-schema-registry` from the table above and place it in the `lib/` directory of your Flink installation. The SQL JAR bundles all required dependencies including the AWS Glue Schema Registry serializer/deserializer libraries.

#### Maven Dependency

To use the format in a DataStream or Table API program, add the following dependency to your project:

{{< connector_artifact flink-protobuf-glue-schema-registry protobuf-glue >}}

How to create a table with Protobuf-Glue format
-------------------------------------------------

Here is an example to create a table using the Kinesis connector with the Protobuf-Glue format:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category` STRING,
  `behavior` STRING,
  `ts` TIMESTAMP(3)
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:us-east-1:012345678901:stream/my-stream',
  'aws.region' = 'us-east-1',
  'source.init.position' = 'LATEST',
  'format' = 'protobuf-glue',
  'protobuf-glue.aws.region' = 'us-east-1',
  'protobuf-glue.registry.name' = 'my-registry',
  'protobuf-glue.schema.name' = 'my-protobuf-schema'
);
```


Protobuf Descriptor Auto-Generation
-------------------------------------

When writing data (sink), the Protobuf-Glue format automatically generates a Protobuf descriptor (`.proto` schema definition) from the Flink table schema and registers it with Glue Schema Registry using `DataFormat.PROTOBUF`.

When reading data (source), the format strips the GSR header bytes (18 bytes: 1 header version byte + 1 compression byte + 16 UUID bytes) from incoming records and deserializes the Protobuf payload into Flink `RowData`.

Format Options
--------------

<table class="table table-bordered">
    <thead>
    <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Forwarded</th>
        <th class="text-center" style="width: 10%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 40%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify the format identifier. Use <code>'protobuf-glue'</code>.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.aws.region</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>AWS region for the Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.registry.name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.schema.name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name under which to register/look up the schema in Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.aws.endpoint</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom AWS endpoint URL for Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.cache.size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">200</td>
      <td>Integer</td>
      <td>Maximum number of items in the schema cache.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.cache.ttlMs</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">86400000</td>
      <td>Long</td>
      <td>Cache TTL in milliseconds. Defaults to 1 day (86400000 ms).</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.schema.autoRegistration</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to auto-register schemas with Glue Schema Registry when writing data.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.schema.compatibility</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">BACKWARD</td>
      <td>String</td>
      <td>Schema compatibility mode applied by the registry when a new schema version is registered (see <a href="https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-compatibility">AWS Glue Schema Registry compatibility modes</a>). Defaults to the registry client's default, <code>BACKWARD</code>. Supported values: <code>NONE</code>, <code>DISABLED</code>, <code>BACKWARD</code>, <code>BACKWARD_ALL</code>, <code>FORWARD</code>, <code>FORWARD_ALL</code>, <code>FULL</code>, <code>FULL_ALL</code>.</td>
    </tr>
    <tr>
      <td><h5>protobuf-glue.schema.compression</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">NONE</td>
      <td>String</td>
      <td>Compression type for schema data. Supported values: <code>NONE</code>, <code>ZLIB</code>.</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
-----------------

The Protobuf-Glue format generates a proto3 message from the table's row type and maps Flink SQL types to Protobuf field types as follows:

<table class="table table-bordered">
    <thead>
    <tr>
        <th class="text-left">Flink SQL Type</th>
        <th class="text-left">Protobuf Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>bool</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code> / <code>SMALLINT</code> / <code>INT</code></td>
      <td><code>int32</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>int64</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>float</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>string</code> (lossless <code>BigDecimal</code> text form)</td>
    </tr>
    <tr>
      <td><code>CHAR</code> / <code>VARCHAR</code> / <code>STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BINARY</code> / <code>VARBINARY</code> / <code>BYTES</code></td>
      <td><code>bytes</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>int32</code> (epoch days)</td>
    </tr>
    <tr>
      <td><code>TIME(p)</code>, p &le; 3</td>
      <td><code>int32</code> (milliseconds of day)</td>
    </tr>
    <tr>
      <td><code>TIMESTAMP(p)</code> / <code>TIMESTAMP_LTZ(p)</code>, p &le; 3</td>
      <td><code>int64</code> (epoch milliseconds)</td>
    </tr>
    </tbody>
</table>

{{< hint warning >}}
`ARRAY`, `MAP`, `MULTISET` and `ROW` columns are **not** supported by this format and are rejected at table creation (see Limitations). The format does not generate `repeated`, `map` or nested `message` fields.
{{< /hint >}}

Limitations
-----------

* **Scalar fields only**: the generated proto3 schema supports the scalar Flink SQL types listed in the mapping above (`BOOLEAN`, the `INT` family, `FLOAT`/`DOUBLE`, `DECIMAL`, `CHAR`/`VARCHAR`, `BINARY`/`VARBINARY`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`). Complex types (`ARRAY`, `MAP`, `MULTISET`, `ROW`, `RAW`, structured types) are rejected at table creation with an `UnsupportedOperationException` naming the type, instead of being silently coerced to `string`. Nested messages and `repeated` fields are therefore not produced by this format.
* **Millisecond time precision**: `TIME` is written as `int32` milliseconds of day and `TIMESTAMP`/`TIMESTAMP_LTZ` as `int64` epoch milliseconds. A column declared with a precision above 3 (for example `TIMESTAMP(6)`) is rejected at table creation with an `IllegalArgumentException`, because its sub-millisecond digits could not be transported; declare precision 3 or lower. `DATE` is written as `int32` epoch days.
* **DECIMAL is text**: `DECIMAL` is written as its lossless `BigDecimal` text form in a `string` field with the writer's scale. On read the value is rescaled to the reading table's `DECIMAL(p, s)` (rounding `HALF_UP`, like `CAST`). A value whose integer part does not fit the reading table's precision fails the read with a message naming the value and the declared type, rather than producing `NULL`. Declared length of `CHAR`/`VARCHAR` is enforced on the Flink side only.
* **Field names are sanitized**: SQL column names that are not valid proto identifiers are rewritten (see the format description above); the original column name is preserved as the field's `json_name`, so reads through the same table round-trip correctly. Two columns that sanitize to the same identifier (for example `` `a b` `` and `` `a-b` ``, both `a_b`) are rejected at table creation with both column names.
* **NOT NULL is enforced on write**: a `NULL` reaching the serializer in a `NOT NULL` column fails the record instead of being written as the Protobuf type default (`0`, `""`, `false`). SQL sinks enforce `NOT NULL` before the format (see `table.exec.sink.not-null-enforcer`); the check protects DataStream users of the serialization schema.
* **Reading foreign schemas**: on the source, only proto3 scalar field types in the writer schema are decoded (`bool`, the `int32`/`int64` families, `float`, `double`, `string`, `bytes`). A writer schema using other field types fails the task with an `IllegalArgumentException` naming the field type.

Usage with Kinesis and Firehose Connectors
------------------------------------------

### Kinesis Source

```sql
CREATE TABLE KinesisSource (
  `user_id` BIGINT,
  `event_type` STRING,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:us-east-1:012345678901:stream/events',
  'aws.region' = 'us-east-1',
  'source.init.position' = 'LATEST',
  'format' = 'protobuf-glue',
  'protobuf-glue.aws.region' = 'us-east-1',
  'protobuf-glue.registry.name' = 'my-registry',
  'protobuf-glue.schema.name' = 'events-protobuf'
);
```

### Firehose Sink

```sql
CREATE TABLE FirehoseSink (
  `user_id` BIGINT,
  `event_type` STRING,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'firehose',
  'delivery-stream' = 'my-delivery-stream',
  'aws.region' = 'us-east-1',
  'format' = 'protobuf-glue',
  'protobuf-glue.aws.region' = 'us-east-1',
  'protobuf-glue.registry.name' = 'my-registry',
  'protobuf-glue.schema.name' = 'events-protobuf',
  'protobuf-glue.schema.autoRegistration' = 'true'
);
```
