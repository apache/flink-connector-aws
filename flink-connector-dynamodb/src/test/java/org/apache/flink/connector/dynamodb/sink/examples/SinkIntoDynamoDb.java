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

package org.apache.flink.connector.dynamodb.sink.examples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.math3.random.RandomDataGenerator;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * An example application demonstrating how to use the {@link DynamoDbSink} to sink into DynamoDb.
 */
public class SinkIntoDynamoDb {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> fromGen =
                env.fromSequence(1, 10_000_000L).map(Object::toString).returns(String.class);

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "your-region-here");

        DynamoDbSink<Map<String, AttributeValue>> dynamoDbSink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setDestinationTableName("my-dynamodb-table")
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setMaxBatchSize(20)
                        .setDynamoDbProperties(sinkProperties)
                        .build();

        fromGen.map(new TestRequestMapper()).sinkTo(dynamoDbSink);

        env.execute("DynamoDb Async Sink Example Program");
    }

    /** Example DynamoDB request attributes mapper. */
    public static class TestRequestMapper
            extends RichMapFunction<String, Map<String, AttributeValue>> {
        private final RandomDataGenerator random = new RandomDataGenerator();

        @Override
        public Map<String, AttributeValue> map(String data) throws Exception {
            final Map<String, AttributeValue> item = new HashMap<>();
            item.put(
                    "partition_key",
                    AttributeValue.builder().s(this.random.nextHexString(5)).build());
            item.put("sort_key", AttributeValue.builder().s(this.random.nextHexString(5)).build());
            item.put("payload", AttributeValue.builder().s(data).build());
            return item;
        }
    }

    /** Example DynamoDB element converter. */
    public static class TestDynamoDbElementConverter
            implements ElementConverter<Map<String, AttributeValue>, DynamoDbWriteRequest> {

        @Override
        public DynamoDbWriteRequest apply(
                Map<String, AttributeValue> elements, SinkWriter.Context context) {
            return DynamoDbWriteRequest.builder()
                    .setType(DynamoDbWriteRequestType.PUT)
                    .setItem(elements)
                    .build();
        }
    }
}
