---
title: SQS
weight: 5
type: docs
aliases:
   - /zh/dev/connectors/sqs.html
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

# Amazon SQS Sink

The SQS sink writes to [Amazon SQS](https://aws.amazon.com/sqs) using the [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html). Follow the instructions from the [Amazon SQS Developer Guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
to setup a SQS message queue.

To use the connector, add the following Maven dependency to your project:

{{< connector_artifact flink-connector-sqs sqs >}}

{{< tabs "ec24a4ae-6a47-11ed-a1eb-0242ac120002" >}}
{{< tab "Java" >}}
```java
Properties sinkProperties = new Properties();
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

// Optional, use following if you want to provide access via AssumeRole, Please make sure given IAM role has "sqs:SendMessage" permission
sinkProperties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "ASSUME_ROLE");
sinkProperties.setProperty(AWSConfigConstants.AWS_ROLE_ARN, "replace-this-with-IAMRole-arn");
sinkProperties.setProperty(AWSConfigConstants.AWS_ROLE_SESSION_NAME, "any-session-name-string");

SqsSink<String> sqsSink =
        SqsSink.<String>builder()
                .setSerializationSchema(new SimpleStringSchema())                // Required
                .setSqsUrl("https://sqs.us-east-1.amazonaws.com/xxxx/test-sqs")  // Required
                .setSqsClientProperties(sinkProperties)                          // Required
                .setFailOnError(false)                                           // Optional
                .setMaxBatchSize(10)                                             // Optional
                .setMaxInFlightRequests(50)                                      // Optional
                .setMaxBufferedRequests(1_000)                                   // Optional
                .setMaxBatchSizeInBytes(256 * 1024)                         // Optional
                .setMaxTimeInBufferMS(5000)                                      // Optional
                .setMaxRecordSizeInBytes(256 * 1024)                            // Optional
                .build();

flinkStream.sinkTo(sqsSink)

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

val SqsSink<String> sqsSink =
                SqsSink.<String>builder()
                        .setSerializationSchema(new SimpleStringSchema())                // Required
                        .setSqsUrl("https://sqs.us-east-1.amazonaws.com/xxxx/test-sqs")  // Required
                        .setSqsClientProperties(sinkProperties)                          // Required
                        .setFailOnError(false)                                           // Optional
                        .setMaxBatchSize(10)                                             // Optional
                        .setMaxInFlightRequests(50)                                      // Optional
                        .setMaxBufferedRequests(1_000)                                   // Optional
                        .setMaxBatchSizeInBytes(256 * 1024)                              // Optional
                        .setMaxTimeInBufferMS(5000)                                      // Optional
                        .setMaxRecordSizeInBytes(256 * 1024)                             // Optional
                        .build();
                        
                       
flinkStream.sinkTo(sqsSink)
```
{{< /tab >}}
{{< /tabs >}}

## Configurations

Flink's SQS sink is created by using the static builder `SqsSink.<String>builder()`.

1. __setSqsClientProperties(Properties sinkProperties)__
   * Required.
   * Supplies credentials, region and other parameters to the SQS client.
2. __setSerializationSchema(SerializationSchema<InputType> serializationSchema)__
   * Required.
   * Supplies a serialization schema to the Sink. This schema is used to serialize elements before sending to SQS.
3. __setSqsUrl(String sqsUrl)__
   * Required.
   * Url of the SQS to sink to.
4. _setFailOnError(boolean failOnError)_
   * Optional. Default: `false`.
   * Whether failed requests to write records to SQS are treated as fatal exceptions in the sink that cause a Flink Job to restart
5. _setMaxBatchSize(int maxBatchSize)_
   * Optional. Default: `10`.
   * Maximum size of a batch to write to SQS.
6. _setMaxInFlightRequests(int maxInFlightRequests)_
   * Optional. Default: `50`.
   * The maximum number of in flight requests allowed before the sink applies backpressure.
7. _setMaxBufferedRequests(int maxBufferedRequests)_
   * Optional. Default: `5_000`.
   * The maximum number of records that may be buffered in the sink before backpressure is applied.
8. _setMaxBatchSizeInBytes(int maxBatchSizeInBytes)_
   * Optional. Default: `256 * 1024`.
   * The maximum size (in bytes) a batch may become. All batches sent will be smaller than or equal to this size.
9. _setMaxTimeInBufferMS(int maxTimeInBufferMS)_
   * Optional. Default: `5000`.
   * The maximum time a record may stay in the sink before being flushed.
10. _setMaxRecordSizeInBytes(int maxRecordSizeInBytes)_
   * Optional. Default: `256 * 1024`.
   * The maximum record size that the sink will accept, records larger than this will be automatically rejected.
11. _build()_
   * Constructs and returns the SQS sink.

