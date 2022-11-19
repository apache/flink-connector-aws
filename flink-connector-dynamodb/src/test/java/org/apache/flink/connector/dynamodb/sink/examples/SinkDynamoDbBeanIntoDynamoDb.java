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
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.dynamodb.sink.DynamoDbBeanElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.dynamodb.util.Order;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Properties;
import java.util.UUID;

/**
 * An example application demonstrating how to use the {@link DynamoDbSink} to sink into DynamoDb
 * using the {@link DynamoDbBeanElementConverter}.
 */
public class SinkDynamoDbBeanIntoDynamoDb {

    private static final String DYNAMODB_TABLE = "orders";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, REGION);

        DynamoDbSink<Order> dynamoDbSink =
                DynamoDbSink.<Order>builder()
                        .setTableName(DYNAMODB_TABLE)
                        .setElementConverter(new DynamoDbBeanElementConverter<>(Order.class))
                        .setDynamoDbProperties(sinkProperties)
                        .build();

        env.fromSequence(1, 1_000L)
                .map(new TestRequestMapper())
                .returns(Order.class)
                .sinkTo(dynamoDbSink);

        env.execute("DynamoDb Sink Example Job");
    }

    /** Example RichMapFunction to generate Order from String. */
    public static class TestRequestMapper extends RichMapFunction<Long, Order> {
        private final RandomDataGenerator random = new RandomDataGenerator();

        @Override
        public Order map(final Long i) throws Exception {
            return new Order(
                    UUID.randomUUID().toString(),
                    random.nextInt(0, 100),
                    random.getRandomGenerator().nextDouble() * 1000);
        }
    }
}
