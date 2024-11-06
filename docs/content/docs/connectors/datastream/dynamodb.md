---
title: DynamoDB
weight: 5
type: docs
aliases:
- /dev/connectors/dynamodb.html
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
# Amazon DynamoDB Connector
The DynamoDB connector allows users to read/write from [Amazon DynamoDB](https://aws.amazon.com/dynamodb/).

As a source, the connector allows users to read change data capture stream from DynamoDB tables using [Amazon DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html).

As a sink, the connector allows users to write directly to Amazon DynamoDB tables using the [BatchWriteItem API](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html). 

## Dependency

Apache Flink ships the connector for users to utilize.

To use the connector, add the following Maven dependency to your project:

{{< connector_artifact flink-connector-dynamodb dynamodb >}}


## Amazon DynamoDB Streams Source

The DynamoDB Streams source reads from [Amazon DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) using the [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html).
Follow the instructions from the [AWS docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) to set up and configure the change data capture stream. 

The actual events streamed to the DynamoDB Stream depend on the `StreamViewType` specified by the DynamoDB Stream itself. 
See [AWS docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html#Streams.Enabling) for more information.

### Usage

The `DynamoDbStreamsSource` provides a fluent builder to construct an instance of the `DynamoDbStreamsSource`.
The code snippet below illustrates how to do so.

{{< tabs "ec24a4ae-6a47-11ed-a1eb-0242ac120001" >}}
{{< tab "Java" >}}
```java
// Configure the DynamodbStreamsSource
Configuration sourceConfig = new Configuration();
sourceConfig.set(DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION, DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON); // This is optional, by default connector will read from LATEST

// Create a new DynamoDbStreamsSource to read from the specified DynamoDB Stream.
DynamoDbStreamsSource<String> dynamoDbStreamsSource = 
        DynamoDbStreamsSource.<String>builder()
                .setStreamArn("arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-04-11T07:14:19.380")
                .setSourceConfig(sourceConfig)
                // User must implement their own deserialization schema to translate change data capture events into custom data types    
                .setDeserializationSchema(dynamodbDeserializationSchema) 
                .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Specify watermarking strategy and the name of the DynamoDB Streams Source operator.
// Specify return type using TypeInformation.
// Specify UID of operator in line with Flink best practice.
DataStream<String> cdcEventsWithEventTimeWatermarks = env.fromSource(dynamoDbStreamsSource, WatermarkStrategy.<String>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(1)), "DynamoDB Streams source")
        .returns(TypeInformation.of(String.class))
        .uid("custom-uid");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// Configure the DynamodbStreamsSource
val sourceConfig = new Configuration()
sourceConfig.set(DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION, DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON) // This is optional, by default connector will read from LATEST

// Create a new DynamoDbStreamsSource to read from the specified DynamoDB Stream.
val dynamoDbStreamsSource = DynamoDbStreamsSource.builder[String]()
  .setStreamArn("arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-04-11T07:14:19.380")
  .setSourceConfig(sourceConfig)
  // User must implement their own deserialization schema to translate change data capture events into custom data types    
  .setDeserializationSchema(dynamodbDeserializationSchema)
  .build()

val env = StreamExecutionEnvironment.getExecutionEnvironment()

// Specify watermarking strategy and the name of the DynamoDB Streams Source operator.
// Specify return type using TypeInformation.
// Specify UID of operator in line with Flink best practice.
val cdcEventsWithEventTimeWatermarks = env.fromSource(dynamoDbStreamsSource, WatermarkStrategy.<String>forMonotonousTimestamps().withIdleness(Duration.ofSeconds(1)), "DynamoDB Streams source")
  .uid("custom-uid")
```
{{< /tab >}}
{{< /tabs >}}

The above is a simple example of using the `DynamoDbStreamsSource`.
- The DynamoDB Stream being read from is specified using the stream ARN.
- Configuration for the `Source` is supplied using an instance of Flink's `Configuration` class.
  The configuration keys can be taken from `AWSConfigOptions` (AWS-specific configuration) and `DynamodbStreamsSourceConfigConstants` (DynamoDB Streams Source configuration).
- The example specifies the starting position as `TRIM_HORIZON` (see [Configuring Starting Position](#configuring-starting-position) for more information).
- The deserialization format is as `SimpleStringSchema` (see [Deserialization Schema](#deserialization-schema) for more information).
- The distribution of shards across subtasks is controlled using the `UniformShardAssigner` (see [Shard Assignment Strategy](#shard-assignment-strategy) for more information).
- The example also specifies an increasing `WatermarkStrategy`, which means each record will be tagged with event time specified using `approximateCreationDateTime`.
  Monotonically increasing watermarks will be generated, and subtasks will be considered idle if no record is emitted after 1 second.

### Configuring Starting Position

To specify the starting position of the `DynamodbStreamsSource`, users can set the `DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION` in configuration.
- `LATEST`: read all shards of the stream starting from the latest record.
- `TRIM_HORIZON`: read all shards of the stream starting from the earliest record possible (data is trimmed by DynamoDB after 24 hours).

### Deserialization Schema

The `DynamoDbStreamsSource` provides the `DynamoDbStreamsDeserializationSchema<T>` interface to allow users to implement their own
deserialization schema to convert DynamoDB change data capture events into custom event types.

The `DynamoDbStreamsDeserializationSchema<T>#deserialize` method takes in an instance of `Record` from the DynamoDB model.
The `Record` can contain different content, depending on the configuration of the DynamoDB Stream. See [AWS docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html#Streams.Enabling) for more information.

### Event Ordering

Events are written into DynamoDB Streams, maintaining ordering within the same primary key.
This is done by ensuring that events within the same primary key are written to the same shard lineage.
When there are shard splits (one parent shard splitting into two child shards), the ordering will be maintained as long as the parent shard is read completely before starting to read from the child shards.

The `DynamoDbStreamsSource` ensures that shards are assigned in a manner that respects parent-child shard ordering.
This means that the shard will only be passed to the shard assigner if the parent shard has been completely read.
This helps to ensure that the events from the change data capture stream are read in-order within the same DynamoDB primary key.

### Shard Assignment Strategy

The `UniformShardAssigner` allocates the shards of the DynamoDB Stream evenly across the parallel subtasks of the source operator.
DynamoDB Stream shards are ephemeral and are created and deleted automatically, as required. 
The `UniformShardAssigner` allocates new shards to the subtask with the lowest number of currently allocated shards.

Users can also implement their own shard assignment strategy by implementing the `DynamoDbStreamsShardAssigner` interface.

### Configuration

#### Retry Strategy

The `DynamoDbStreamsSource` interacts with Amazon DynamoDB using the [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html). 

The retry strategy used by the AWS SDK client can be tuned using the following configuration options:
- `DYNAMODB_STREAMS_RETRY_COUNT`: Maximum number of API retries on retryable errors, before it will restart the Flink job. 
- `DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MIN_DELAY`: The base delay used for calculation of the exponential backoff.
- `DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MAX_DELAY`: The maximum delay for exponential backoff.

#### Shard Discovery

The `DynamoDbStreamsSource` periodically discovers newly created shards on the DynamoDB Stream. This can come from shard splitting, or shard rotations.
By default this is set to discover shards every 60 seconds. However, users can customize this to a smaller value by configuring the `SHARD_DISCOVERY_INTERVAL`.

There is an issue for shard discovery where the shard graph returned from DynamoDB might have inconsistencies. 
In this case, the `DynamoDbStreamsSource` automatically detects the inconsistency and retries the shard discovery process.
The maximum number of retries can be configured using `DESCRIBE_STREAM_INCONSISTENCY_RESOLUTION_RETRY_COUNT`.


## Amazon DynamoDB Sink

The DynamoDB sink writes to [Amazon DynamoDB](https://aws.amazon.com/dynamodb) using the [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html). Follow the instructions from the [Amazon DynamoDB Developer Guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/getting-started-step-1.html)
to setup a table.

{{< tabs "ec24a4ae-6a47-11ed-a1eb-0242ac120002" >}}
{{< tab "Java" >}}
```java
Properties sinkProperties = new Properties();
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

ElementConverter<InputType, DynamoDbWriteRequest> elementConverter = new CustomElementConverter();

DynamoDbSink<String> dynamoDbSink = 
    DynamoDbSink.<InputType>builder()
        .setDynamoDbProperties(sinkProperties)              // Required
        .setTableName("my-dynamodb-table")                  // Required
        .setElementConverter(elementConverter)              // Required
        .setOverwriteByPartitionKeys(singletonList("key"))  // Optional  
        .setFailOnError(false)                              // Optional
        .setMaxBatchSize(25)                                // Optional
        .setMaxInFlightRequests(50)                         // Optional
        .setMaxBufferedRequests(10_000)                     // Optional
        .setMaxTimeInBufferMS(5000)                         // Optional
        .build();

flinkStream.sinkTo(dynamoDbSink);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val sinkProperties = new Properties()
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1")
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")

val elementConverter = new CustomElementConverter();

val dynamoDbSink =
    DynamoDbSink.<InputType>builder()
        .setDynamoDbProperties(sinkProperties)              // Required
        .setTableName("my-dynamodb-table")                  // Required
        .setElementConverter(elementConverter)              // Required
        .setOverwriteByPartitionKeys(singletonList("key"))  // Optional    
        .setFailOnError(false)                              // Optional
        .setMaxBatchSize(25)                                // Optional
        .setMaxInFlightRequests(50)                         // Optional
        .setMaxBufferedRequests(10_000)                     // Optional
        .setMaxTimeInBufferMS(5000)                         // Optional
        .build()

flinkStream.sinkTo(dynamoDbSink)
```
{{< /tab >}}
{{< /tabs >}}

### Configurations

Flink's DynamoDB sink is created by using the static builder `DynamoDBSink.<InputType>builder()`.

1. __setDynamoDbProperties(Properties sinkProperties)__
    * Required.
    * Supplies credentials, region and other parameters to the DynamoDB client.
2. __setTableName(String tableName)__
    * Required.
    * Name of the table to sink to.
3. __setElementConverter(ElementConverter<InputType, DynamoDbWriteRequest> elementConverter)__
    * Required.
    * Converts generic records of type `InputType` to `DynamoDbWriteRequest`.
4. _setOverwriteByPartitionKeys(List<String> partitionKeys)_
    * Optional. Default: [].
    * Used to deduplicate write requests within each batch pushed to DynamoDB.
5. _setFailOnError(boolean failOnError)_
    * Optional. Default: `false`.
    * Whether failed requests to write records are treated as fatal exceptions in the sink.
6. _setMaxBatchSize(int maxBatchSize)_
    * Optional. Default: `25`.
    * Maximum size of a batch to write.
7. _setMaxInFlightRequests(int maxInFlightRequests)_
    * Optional. Default: `50`.
    * The maximum number of in flight requests allowed before the sink applies backpressure.
8. _setMaxBufferedRequests(int maxBufferedRequests)_
    * Optional. Default: `10_000`.
    * The maximum number of records that may be buffered in the sink before backpressure is applied.
9. _setMaxBatchSizeInBytes(int maxBatchSizeInBytes)_
    * N/A. 
    * This configuration is not supported, see [FLINK-29854](https://issues.apache.org/jira/browse/FLINK-29854).
10. _setMaxTimeInBufferMS(int maxTimeInBufferMS)_
    * Optional. Default: `5000`.
    * The maximum time a record may stay in the sink before being flushed.
11. _setMaxRecordSizeInBytes(int maxRecordSizeInBytes)_
    * N/A.
    * This configuration is not supported, see [FLINK-29854](https://issues.apache.org/jira/browse/FLINK-29854).
12. _build()_
    * Constructs and returns the DynamoDB sink.

### Element Converter

An element converter is used to convert from a record in the DataStream to a DynamoDbWriteRequest which the sink will write to the destination DynamoDB table. The DynamoDB sink allows the user to supply a custom element converter, or use the provided
`DefaultDynamoDbElementConverter` which extracts item schema from element class, this requires the element class to be of composite type (i.e. Pojo, Tuple or Row). In case TypeInformation of the elements is present the schema is eagerly constructed by using `DynamoDbTypeInformedElementConverter` as in `new DynamoDbTypeInformedElementConverter(TypeInformation.of(MyPojo.class))`.


Alternatively when you are working with `@DynamoDbBean` objects you can use `DynamoDbBeanElementConverter`. For more information on supported 
annotations see [here](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-dynamodb-enhanced.html#dynamodb-enhanced-mapper-tableschema).

A sample application using a custom `ElementConverter` can be found [here](https://github.com/apache/flink-connector-aws/blob/main/flink-connector-aws/flink-connector-dynamodb/src/test/java/org/apache/flink/connector/dynamodb/sink/examples/SinkIntoDynamoDb.java). A sample application using the `DynamoDbBeanElementConverter` can be found [here](https://github.com/apache/flink-connector-aws/blob/main/flink-connector-aws/flink-connector-dynamodb/src/test/java/org/apache/flink/connector/dynamodb/sink/examples/SinkDynamoDbBeanIntoDynamoDb.java).

### Using Custom DynamoDB Endpoints

It is sometimes desirable to have Flink operate as a consumer or producer against a DynamoDB VPC endpoint or a non-AWS
DynamoDB endpoint such as [Localstack](https://localstack.cloud/); this is especially useful when performing
functional testing of a Flink application. The AWS endpoint that would normally be inferred by the AWS region set in the
Flink configuration must be overridden via a configuration property.

To override the AWS endpoint, set the `AWSConfigConstants.AWS_ENDPOINT` and `AWSConfigConstants.AWS_REGION` properties. The region will be used to sign the endpoint URL.

{{< tabs "bcadd466-8416-4d3c-a6a7-c46eee0cbd4a" >}}
{{< tab "Java" >}}
```java
Properties producerConfig = new Properties();
producerConfig.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val producerConfig = new Properties()
producerConfig.put(AWSConfigConstants.AWS_REGION, "eu-west-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}