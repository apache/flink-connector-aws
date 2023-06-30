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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
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

    private static final String DYNAMODB_TABLE = "my-dynamodb-table";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, REGION);

        DynamoDbSink<Long> dynamoDbSink =
                DynamoDbSink.<Long>builder()
                        .setTableName(DYNAMODB_TABLE)
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setMaxBatchSize(20)
                        .setDynamoDbProperties(sinkProperties)
                        .build();

        env.fromSequence(1, 10_000_000L).sinkTo(dynamoDbSink);

        env.execute("DynamoDb Sink Example Job");
    }

    /** Example DynamoDB element converter. */
    public static class TestDynamoDbElementConverter
            implements ElementConverter<Long, DynamoDbWriteRequest> {

        private final RandomDataGenerator random = new RandomDataGenerator();

        @Override
        public DynamoDbWriteRequest apply(Long index, SinkWriter.Context context) {
            final Map<String, AttributeValue> item = new HashMap<>();
            item.put(
                    "partition_key",
                    AttributeValue.builder().s(this.random.nextHexString(5)).build());
            item.put("sort_key", AttributeValue.builder().s(this.random.nextHexString(5)).build());
            item.put("payload", AttributeValue.builder().s(String.valueOf(index)).build());

            return DynamoDbWriteRequest.builder()
                    .setType(DynamoDbWriteRequestType.PUT)
                    .setItem(item)
                    .build();
        }
    }
}
