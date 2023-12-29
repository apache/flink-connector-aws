---
title: Avro Glue Schema Registry
weight: 5
type: docs
aliases:
- /dev/table/connectors/formats/avro-glue.html
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

# AWS Glue Avro Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The AWS Glue Schema Registry (``avro-glue``) format allows you to read records that were serialized by ``com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer`` and to write records that can in turn be read by ``com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer``.
These records have their schemas stored out-of-band in a configured registry provided by the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-schemas).

When reading (deserializing) a record with this format the Avro writer schema is fetched from the configured AWS Glue Schema Registry, based on the schema version id encoded in the record, while the reader schema is inferred from table schema.

When writing (serializing) a record with this format the Avro schema is inferred from the table schema and used to retrieve a schema id to be encoded with the data. 
The lookup is performed against the configured AWS Glue Schema Registry under the [value](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-schemas) given in `avro-glue.schema-name`.
Optionally, you can enable schema auto-registration, allowing the writer to register a new schema version in the schema registry, directly. The new schema will be accepted only if it does not violate the compatibility mode that was set when the schema was created in the first place.

The AWS Glue Schema format can only be used in conjunction with the [Apache Kafka SQL connector]({{< ref "docs/connectors/table/kafka" >}}) or the [Upsert Kafka SQL Connector]({{< ref "docs/connectors/table/upsert-kafka" >}}).

Dependencies
------------

{{< sql_download_table "avro-glue" >}}

How to create tables with Avro-Glue format
--------------


Example of a table using raw UTF-8 string as Kafka key and Avro records registered in the Schema Registry as Kafka values:

```sql
CREATE TABLE user_created (

  -- one column mapped to the Kafka raw UTF-8 key
  the_kafka_key STRING,

  -- a few columns mapped to the Avro fields of the Kafka value
  id STRING,
  name STRING, 
  email STRING  

) WITH (

  'connector' = 'kafka',
  'topic' = 'user_events_example1',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- UTF-8 string as Kafka keys, using the 'the_kafka_key' table column
  'key.format' = 'raw',
  'key.fields' = 'the_kafka_key',

  'value.format' = 'avro-glue',
  'value.avro-glue.region' = 'us-east-1',
  'value.avro-glue.registry.name' = 'my-schema-registry',
  'value.avro-glue.schema.name' = 'my-schema-name',
  'avro-glue.schema.autoRegistration' = 'true',  
  'value.fields-include' = 'EXCEPT_KEY'
)
```

You can write data into the Kafka table as follows:

```
INSERT INTO user_created
SELECT
  -- replicating the user id into a column mapped to the kafka key
  id as the_kafka_key,

  -- all values
  id, name, email
FROM some_table
```

Format Options
----------------

Note that naming of the properties slightly diverges from the [AWS Glue client code](https://github.com/awslabs/aws-glue-schema-registry/blob/master/common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java#L20) properties, to match with the conventions used by other Flink formats.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>format</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Specify what format to use, here should be <code>'avro-glue'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.aws.region</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Specify what AWS region the Glue Schema Registry is, such as <code>'us-east-1'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.aws.endpoint</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The HTTP endpoint to use for AWS calls.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.registry.name</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The name (not the ARN) of the Glue schema registry in which to store the schemas.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.schema.name</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The name under which to store the schema in the registry.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.schema.autoRegistration</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">false</td>
            <td>Boolean</td>
            <td>Whether new schemas should be automatically registered rather than treated as errors. Only used when writing (serializing). Ignored when reading (deserializing).(</td>
        </tr>
        <tr>
            <td><h5>avro-glue.schema.compression</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">NONE</td>
            <td>String</td>
            <td>What kind of compression to use.  Valid values are <code>'NONE'</code> and <code>'ZLIB'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.schema.compatibility</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">BACKWARD</td>
            <td>String</td>
            <td>The schema compatibility mode under which to store the schema.  Valid values are: 
              <code>'NONE'</code>,
              <code>'DISABLED'</code>,
              <code>'BACKWARD'</code>,
              <code>'BACKWARD_ALL'</code>,
              <code>'FORWARD'</code>,
              <code>'FORWARD_ALL'</code>,
              <code>'FULL'</code>, and 
              <code>'FULL_ALL'</code>.
              Only used when schema auto-registration is enabled and when the schema is registered in the first place.     
              Ignored when reading or when a new schema version is auto-registered in an existing schema.
            </td>
        </tr>
        <tr>
            <td><h5>avro-glue.cache.size</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">200</td>
            <td>Integer</td>
            <td>The size (in number of items, not bytes) of the cache the Glue client code should manage</td>
        </tr>
        <tr>
            <td><h5>avro-glue.cache.ttlMs</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">1 day (24 * 60 * 60 * 1000)</td>
            <td>Integer</td>
            <td>The time to live (in milliseconds) for cache entries.  Defaults to 1 day.</td>
        </tr>
    </tbody>
</table>

Note that the schema type (Generic or Specific Record) cannot be specified while using Table API.

Schema Auto-registration
------------------------

By default, the schema auto-registration is disabled. When writing to a Kafka table new records are accepted only if a schema version that matches the table schema exactly is already registered in the Schema Registry at `registry.name` and `schema.name`. Otherwise, an exception is thrown.

You can enable schema auto-registration setting the property `avro-glue.schema.autoRegistration` = `true`.

When auto-registration is enabled, Flink will first check whether a schema matching the table schema is already registered in the Schema Registry. If the schema is already registered, the writer will reuse the schemaId.
If the table schema does not match any schema version already registered at the specified `registry.name` and `schema.name`, the writer will try to auto-register a new schema version.

When auto-registering a new schema version, there are two different cases:

1. No schema is registered at the specified `registry.name` and `schema.name`: a new schema, matching the table schema, will be registered. The compatibility mode is set to the value of the `schema.compatibility` property.
2. Another, different schema version is already registered at the specified `registry.name` and `schema.name`: in this case the new schema version will be accepted only it does not violate the schema evolution rules defined by the Compatibility Mode that has been set when the Schema has been created in the first place.

When auto-registering a new schema, the schema compatibility mode is set based on the `avro-glue.schema.compatibility` property.

Note that `avro-glue.schema.compatibility` is used only when a new schema is auto-registered in the first place. When a new schema version is auto-registered in an existing schema, the compatibility mode of the schema is never changed and the `avro-glue.schema.compatibility` is ignored.

Data Type Mapping
----------------

Currently, Apache Flink always uses the table schema to derive the Avro reader schema during deserialization and Avro writer schema during serialization. Explicitly defining an Avro schema is not supported yet.
See the [Apache Avro Format]({{< ref "docs/connectors/table/formats/avro" >}}#data-type-mapping) for the mapping between Avro and Flink DataTypes.

In addition to the types listed there, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/current/spec.html) for more information about Avro types.