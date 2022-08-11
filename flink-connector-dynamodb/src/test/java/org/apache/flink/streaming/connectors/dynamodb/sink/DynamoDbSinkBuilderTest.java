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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

/** Tests for {@link DynamoDbSinkBuilder}. */
public class DynamoDbSinkBuilderTest {

    @Test
    public void testCreateDynamoDbSinkBuilder() {
        DynamoDbSink<Map<String, AttributeValue>> dynamoDbSink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDestinationTableName("testTable")
                        .build();
        Assertions.assertThat(1).isEqualTo(dynamoDbSink.getWriterStateSerializer().getVersion());
    }

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.builder()
                                        .setDestinationTableName("testTable")
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "ElementConverter must be not null when initializing the AsyncSinkBase.");
    }

    @Test
    public void destinationTableNameMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Destination table name must be set when initializing the DynamoDB Sink.");
    }

    @Test
    public void destinationTableNameMustBeNotEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setDestinationTableName("")
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Destination table name must be set when initializing the DynamoDB Sink.");
    }

    @Test
    public void correctMaximumBatchSize() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setDestinationTableName("test_table")
                                        .setMaxBatchSize(50)
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "DynamoDB client supports only up to 25 elements in the batch.");
    }

    @Test
    public void maxBatchSizeInBytesThrowsNotImplemented() {
        Assertions.assertThatExceptionOfType(InvalidConfigurationException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setDestinationTableName("test_table")
                                        .setMaxBatchSizeInBytes(100)
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Max batch size in bytes is not supported by the DynamoDB sink implementation.");
    }

    @Test
    public void maxRecordSizeInBytesThrowsNotImplemented() {
        Assertions.assertThatExceptionOfType(InvalidConfigurationException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setDestinationTableName("test_table")
                                        .setMaxRecordSizeInBytes(100)
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Max record size in bytes is not supported by the DynamoDB sink implementation.");
    }
}
