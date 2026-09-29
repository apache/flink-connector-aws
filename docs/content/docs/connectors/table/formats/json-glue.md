---
title: "JSON (Glue Schema Registry)"
weight: 2
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

# JSON Format (AWS Glue Schema Registry)

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The JSON Glue Schema Registry format (`json-glue`) allows you to read and write JSON data with schemas managed by [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

Dependencies
------------

{{< sql_connector_download_table "json-glue" >}}

The JSON-Glue format is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

#### SQL Client JAR

For SQL Client usage, download the fat JAR `flink-sql-json-glue-schema-registry` from the table above and place it in the `lib/` directory of your Flink installation. The SQL JAR bundles all required dependencies including the AWS Glue Schema Registry serializer/deserializer libraries.

#### Maven Dependency

To use the format in a DataStream or Table API program, add the following dependency to your project:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-json-glue-schema-registry</artifactId>
  <version>6.0.0</version>
</dependency>
```

How to create a table with JSON-Glue format
--------------------------------------------

Here is an example to create a table using the Kinesis connector with the JSON-Glue format:

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
  'format' = 'json-glue',
  'json-glue.aws.region' = 'us-east-1',
  'json-glue.registry.name' = 'my-registry',
  'json-glue.schema.name' = 'my-json-schema'
);
```


JSON Schema Auto-Derivation
----------------------------

When writing data (sink), the JSON-Glue format automatically derives a JSON Schema from the Flink table schema and registers it with Glue Schema Registry. The JSON Schema is generated from the `RowType` field names and types.

When reading data (source), the format strips the GSR header bytes (18 bytes: 1 header version byte + 1 compression byte + 16 UUID bytes) from incoming records and delegates JSON deserialization to Flink's built-in `JsonRowDataDeserializationSchema`.

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
      <td>Specify the format identifier. Use <code>'json-glue'</code>.</td>
    </tr>
    <tr>
      <td><h5>json-glue.aws.region</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>AWS region for the Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>json-glue.registry.name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>json-glue.schema.name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name under which to register/look up the schema in Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>json-glue.aws.endpoint</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom AWS endpoint URL for Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>json-glue.cache.size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">200</td>
      <td>Integer</td>
      <td>Maximum number of items in the schema cache.</td>
    </tr>
    <tr>
      <td><h5>json-glue.cache.ttlMs</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">86400000</td>
      <td>Long</td>
      <td>Cache TTL in milliseconds. Defaults to 1 day (86400000 ms).</td>
    </tr>
    <tr>
      <td><h5>json-glue.schema.autoRegistration</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to auto-register schemas with Glue Schema Registry when writing data.</td>
    </tr>
    <tr>
      <td><h5>json-glue.schema.compatibility</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">NONE</td>
      <td>String</td>
      <td>Schema compatibility mode. Supported values: <code>NONE</code>, <code>DISABLED</code>, <code>BACKWARD</code>, <code>BACKWARD_ALL</code>, <code>FORWARD</code>, <code>FORWARD_ALL</code>, <code>FULL</code>, <code>FULL_ALL</code>.</td>
    </tr>
    <tr>
      <td><h5>json-glue.schema.compression</h5></td>
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

The JSON-Glue format uses Flink's built-in JSON serialization/deserialization. The mapping between Flink SQL types and JSON types follows the standard Flink JSON format:

<table class="table table-bordered">
    <thead>
    <tr>
        <th class="text-left">Flink SQL Type</th>
        <th class="text-left">JSON Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code> / <code>SMALLINT</code> / <code>INT</code> / <code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code> / <code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BYTES</code></td>
      <td><code>string</code> (Base64 encoded)</td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string</code> (date format)</td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string</code> (time format)</td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string</code> (timestamp format)</td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>MAP</code></td>
      <td><code>object</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>object</code></td>
    </tr>
    </tbody>
</table>

Limitations
-----------

* **Type support**: the writer schema is a JSON Schema generated from the table's row type. The supported Flink SQL types are the scalar types (`BOOLEAN`, the `INT` family, `FLOAT`/`DOUBLE`, `DECIMAL`, `CHAR`/`VARCHAR`, `BINARY`/`VARBINARY`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_LTZ`) and the container types `ARRAY`, `MAP` and `ROW`. Any other type (`MULTISET`, `INTERVAL`, `RAW`, structured types) is rejected at table creation with an `UnsupportedOperationException` naming the type, instead of being silently coerced to `string`.
* **`MAP` keys must be strings**: JSON object keys are strings, so a `MAP` whose key type is not `CHAR`/`VARCHAR` is rejected with the same exception.
* **Lossy JSON Schema types**: the generated schema declares `integer` for every `INT` family type, `number` for `FLOAT`/`DOUBLE`/`DECIMAL`, and `string` (with a `format` or `contentEncoding` annotation) for temporal and binary types. Precision, scale and length are not part of the registered schema; they are enforced by the table definition on the Flink side only.
* **Validation is structural**: records are (de)serialized with Flink's built-in JSON format; the registered schema is used for registry compatibility checks and for the GSR header, not for per-record JSON Schema validation.

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
  'format' = 'json-glue',
  'json-glue.aws.region' = 'us-east-1',
  'json-glue.registry.name' = 'my-registry',
  'json-glue.schema.name' = 'events-json'
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
  'format' = 'json-glue',
  'json-glue.aws.region' = 'us-east-1',
  'json-glue.registry.name' = 'my-registry',
  'json-glue.schema.name' = 'events-json',
  'json-glue.schema.autoRegistration' = 'true'
);
```
