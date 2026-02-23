---
title: "Avro (Glue Schema Registry)"
weight: 1
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

# Avro Format (AWS Glue Schema Registry)

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The Avro Glue Schema Registry format (`avro-glue`) allows you to read and write Avro data with schemas managed by [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

Dependencies
------------

{{< sql_connector_download_table "avro-glue" >}}

The Avro-Glue format is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

#### SQL Client JAR

For SQL Client usage, download the fat JAR `flink-sql-avro-glue-schema-registry` from the table above and place it in the `lib/` directory of your Flink installation. The SQL JAR bundles all required dependencies including the AWS Glue Schema Registry serializer/deserializer libraries.

#### Maven Dependency

To use the format in a DataStream or Table API program, add the following dependency to your project:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro-glue-schema-registry</artifactId>
  <version>6.0.0</version>
</dependency>
```

How to create a table with Avro-Glue format
--------------------------------------------

Here is an example to create a table using the Kinesis connector with the Avro-Glue format:

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
  'format' = 'avro-glue',
  'avro-glue.aws.region' = 'us-east-1',
  'avro-glue.registry.name' = 'my-registry',
  'avro-glue.schema.name' = 'my-avro-schema'
);
```


Schema Namespace Override
-------------------------

Flink's `AvroSchemaConverter` auto-generates an Avro schema from the SQL table definition. The generated schema uses a default namespace (`org.apache.flink.avro.generated`) and record name (`record`) that may differ from schemas already registered in Glue Schema Registry.

If your GSR registry already contains a schema with a specific namespace or record name, you can override the auto-generated values using the `avro.namespace` and `avro.record-name` options:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category` STRING
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:us-east-1:012345678901:stream/my-stream',
  'aws.region' = 'us-east-1',
  'format' = 'avro-glue',
  'avro-glue.aws.region' = 'us-east-1',
  'avro-glue.registry.name' = 'my-registry',
  'avro-glue.schema.name' = 'my-avro-schema',
  'avro-glue.avro.namespace' = 'com.mycompany.events',
  'avro-glue.avro.record-name' = 'UserEvent'
);
```

Alternatively, you can set `schema.fetchFromRegistry` to `true` to fetch the schema directly from GSR at runtime instead of using the auto-generated one:

```sql
CREATE TABLE KinesisTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `category` STRING
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:us-east-1:012345678901:stream/my-stream',
  'aws.region' = 'us-east-1',
  'format' = 'avro-glue',
  'avro-glue.aws.region' = 'us-east-1',
  'avro-glue.registry.name' = 'my-registry',
  'avro-glue.schema.name' = 'my-avro-schema',
  'avro-glue.schema.fetchFromRegistry' = 'true'
);
```

If the schema is not found in GSR, the format falls back to the auto-generated schema (optionally patched with `avro.namespace` and `avro.record-name` if provided).

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
      <td>Specify the format identifier. Use <code>'avro-glue'</code>.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.aws.region</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>AWS region for the Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.registry.name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.schema.name</h5></td>
      <td>required</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Schema name under which to register/look up the schema in Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.aws.endpoint</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom AWS endpoint URL for Glue Schema Registry.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.schema.type</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">GENERIC_RECORD</td>
      <td>String</td>
      <td>Avro record type. Supported values: <code>GENERIC_RECORD</code>, <code>SPECIFIC_RECORD</code>.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.avro.namespace</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Override the namespace in the auto-generated Avro schema. Use this to match schemas already registered in GSR with a different namespace.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.avro.record-name</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Override the record name in the auto-generated Avro schema. Use this to match schemas already registered in GSR with a different record name.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.schema.fetchFromRegistry</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to fetch the schema from GSR instead of using the auto-generated one. Falls back to auto-generated schema if the schema is not found.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.cache.size</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">200</td>
      <td>Integer</td>
      <td>Maximum number of items in the schema cache.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.cache.ttlMs</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">86400000</td>
      <td>Long</td>
      <td>Cache TTL in milliseconds. Defaults to 1 day (86400000 ms).</td>
    </tr>
    <tr>
      <td><h5>avro-glue.schema.autoRegistration</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to auto-register schemas with Glue Schema Registry when writing data.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.schema.compatibility</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">NONE</td>
      <td>String</td>
      <td>Schema compatibility mode. Supported values: <code>NONE</code>, <code>DISABLED</code>, <code>BACKWARD</code>, <code>BACKWARD_ALL</code>, <code>FORWARD</code>, <code>FORWARD_ALL</code>, <code>FULL</code>, <code>FULL_ALL</code>.</td>
    </tr>
    <tr>
      <td><h5>avro-glue.schema.compression</h5></td>
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

The Avro-Glue format uses Flink's built-in `AvroSchemaConverter` to map between Flink SQL types and Avro types. The mapping follows the same rules as the standard Flink Avro format:

<table class="table table-bordered">
    <thead>
    <tr>
        <th class="text-left">Flink SQL Type</th>
        <th class="text-left">Avro Type</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code> / <code>SMALLINT</code> / <code>INT</code></td>
      <td><code>int</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>long</code></td>
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
      <td><code>STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BYTES</code></td>
      <td><code>bytes</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>bytes</code> (logical type: <code>decimal</code>)</td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>int</code> (logical type: <code>date</code>)</td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>int</code> (logical type: <code>time-millis</code>)</td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>long</code> (logical type: <code>timestamp-millis</code>)</td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>MAP</code> (key must be <code>STRING</code>)</td>
      <td><code>map</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>record</code></td>
    </tr>
    </tbody>
</table>

{{< hint info >}}
Nullable Flink SQL types are mapped to Avro union types `["null", "type"]`. Flink SQL types are nullable by default, so the auto-generated Avro schema will use union types for all fields unless `NOT NULL` constraints are specified.
{{< /hint >}}

Usage with Kinesis and Firehose Connectors
------------------------------------------

The Avro-Glue format can be used with any Flink SQL connector that supports custom formats. Here are examples with the Kinesis and Firehose connectors:

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
  'format' = 'avro-glue',
  'avro-glue.aws.region' = 'us-east-1',
  'avro-glue.registry.name' = 'my-registry',
  'avro-glue.schema.name' = 'events-avro'
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
  'format' = 'avro-glue',
  'avro-glue.aws.region' = 'us-east-1',
  'avro-glue.registry.name' = 'my-registry',
  'avro-glue.schema.name' = 'events-avro',
  'avro-glue.schema.autoRegistration' = 'true'
);
```
