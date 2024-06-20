/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.dynamodb.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.dynamodb.source.DynamoDbStreamsSource;
import org.apache.flink.connector.dynamodb.source.DynamoDbStreamsSourceBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Sample command-line program of consuming data from a single DynamoDB stream. */
public class DdbStreamsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);

        Configuration dynamodbStreamsConsumerConfig = new Configuration();
        final String streamName =
                "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-04-11T07:14:19.380";

        dynamodbStreamsConsumerConfig.setString(AWSConfigConstants.AWS_REGION, "us-east-1");

        DynamoDbStreamsSource<String> dynamoDbStreamsSource =
                new DynamoDbStreamsSourceBuilder()
                        .setStreamArn(streamName)
                        .setSourceConfig(dynamodbStreamsConsumerConfig)
                        .setDeserializationSchema(new ExampleDynamoDbStreamsDeserializationSchema())
                        .build();

        see.fromSource(
                        dynamoDbStreamsSource,
                        WatermarkStrategy.noWatermarks(),
                        "DDB Stream Source",
                        TypeInformation.of(String.class))
                .map(k -> k.toLowerCase())
                .print();

        see.execute("DynamoDB Streams Example Flink app");
    }
}
