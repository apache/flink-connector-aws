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

package org.apache.flink.connector.dynamodb.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BATCH_SIZE;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.connector.base.table.AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.AWS_REGION;
import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.FAIL_ON_ERROR;
import static org.apache.flink.connector.dynamodb.table.DynamoDbConnectorOptions.TABLE_NAME;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for {@link DynamoDbDynamicSink} created by {@link DynamoDbDynamicSinkFactory}. */
public class DynamoDbDynamicSinkFactoryTest {

    private static final String DYNAMO_DB_TABLE_NAME = "TestDynamoDBTable";

    @Test
    void testGoodPartitionedTableSink() {
        ResolvedSchema sinkSchema = createResolvedSchemaUsingAllDataTypes();

        Map<String, String> sinkOptions = defaultSinkOptions().build();
        List<String> partitionKeys = Collections.singletonList("partition_key");

        // Construct actual sink
        DynamoDbDynamicSink actualSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);

        // Construct expected sink
        Properties dynamoDbClientProperties = new Properties();
        dynamoDbClientProperties.put("aws.region", "us-east-1");
        DynamoDbDynamicSink expectedSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>(partitionKeys))
                                .setDynamoDbClientProperties(dynamoDbClientProperties)
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
        assertThat(actualSink.getChangelogMode(ChangelogMode.insertOnly()))
                .isEqualTo(ChangelogMode.upsert());
        assertThat(actualSink.asSummaryString()).isEqualTo("DynamoDB");

        Sink<RowData> createdSink =
                ((SinkV2Provider)
                                actualSink.getSinkRuntimeProvider(
                                        new SinkRuntimeProviderContext(false)))
                        .createSink();
        assertThat(createdSink).isInstanceOf(DynamoDbSink.class);
    }

    @Test
    void testGoodNonPartitionedTableSink() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultSinkOptions().build();

        // Construct actual sink
        DynamoDbDynamicSink actualSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, sinkOptions);

        // Construct expected sink
        DynamoDbDynamicSink expectedSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>())
                                .setDynamoDbClientProperties(defaultSinkProperties())
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testCopyTableSink() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultSinkOptions().build();

        // Construct expected sink
        DynamoDbDynamicSink originalSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>())
                                .setDynamoDbClientProperties(defaultSinkProperties())
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .build();

        assertThat(originalSink).usingRecursiveComparison().isEqualTo(originalSink.copy());
    }

    @Test
    void testGoodTableSinkWithOptionalOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultSinkOptions().withTableOption(FAIL_ON_ERROR, "true").build();
        List<String> partitionKeys = Collections.singletonList("partition_key");

        // Construct actual sink
        DynamoDbDynamicSink actualSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);

        // Construct expected sink
        DynamoDbDynamicSink expectedSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>(partitionKeys))
                                .setDynamoDbClientProperties(defaultSinkProperties())
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .setFailOnError(true)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testGoodTableSinkWithAwsCredentialOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultSinkOptions().withTableOption(FAIL_ON_ERROR, "true").build();
        sinkOptions.put("aws.credentials.provider", "BASIC");
        sinkOptions.put("aws.credentials.basic.accesskeyid", "1234");
        sinkOptions.put("aws.credentials.basic.secretkey", "5678");
        List<String> partitionKeys = Collections.singletonList("partition_key");

        // Construct actual sink
        DynamoDbDynamicSink actualSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);

        // Construct expected sink
        Properties expectedSinkProperties = defaultSinkProperties();
        expectedSinkProperties.put("aws.credentials.provider", "BASIC");
        expectedSinkProperties.put("aws.credentials.provider.basic.accesskeyid", "1234");
        expectedSinkProperties.put("aws.credentials.provider.basic.secretkey", "5678");
        DynamoDbDynamicSink expectedSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>(partitionKeys))
                                .setDynamoDbClientProperties(expectedSinkProperties)
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .setFailOnError(true)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testGoodTableSinkWithHttpClientOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultSinkOptions().withTableOption(FAIL_ON_ERROR, "true").build();
        sinkOptions.put("sink.http-client.max-concurrency", "123");
        sinkOptions.put("sink.http-client.read-timeout", "456");
        sinkOptions.put("sink.http-client.protocol.version", "HTTP1_1");
        List<String> partitionKeys = Collections.singletonList("partition_key");

        // Construct actual sink
        DynamoDbDynamicSink actualSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);

        // Construct expected sink
        Properties expectedSinkProperties = defaultSinkProperties();
        expectedSinkProperties.put("aws.http-client.max-concurrency", "123");
        expectedSinkProperties.put("aws.http-client.read-timeout", "456");
        expectedSinkProperties.put("aws.http.protocol.version", "HTTP1_1");
        DynamoDbDynamicSink expectedSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>(partitionKeys))
                                .setDynamoDbClientProperties(expectedSinkProperties)
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .setFailOnError(true)
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testGoodTableSinkWithAsyncProperties() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                defaultSinkOptions()
                        .withTableOption(MAX_BATCH_SIZE, "100")
                        .withTableOption(MAX_IN_FLIGHT_REQUESTS, "100")
                        .withTableOption(MAX_BUFFERED_REQUESTS, "100")
                        .withTableOption(FLUSH_BUFFER_SIZE, "1024")
                        .withTableOption(FLUSH_BUFFER_TIMEOUT, "1000")
                        .build();
        List<String> partitionKeys = Collections.singletonList("partition_key");

        // Construct actual sink
        DynamoDbDynamicSink actualSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);

        // Construct expected sink
        DynamoDbDynamicSink expectedSink =
                (DynamoDbDynamicSink)
                        DynamoDbDynamicSink.builder()
                                .setMaxBatchSize(100)
                                .setMaxInFlightRequests(100)
                                .setMaxBufferedRequests(100)
                                .setMaxBufferSizeInBytes(1024)
                                .setMaxTimeInBufferMS(1000)
                                .setTableName(DYNAMO_DB_TABLE_NAME)
                                .setOverwriteByPartitionKeys(new HashSet<>(partitionKeys))
                                .setDynamoDbClientProperties(defaultSinkProperties())
                                .setPhysicalDataType(sinkSchema.toPhysicalRowDataType())
                                .build();

        assertThat(actualSink).usingRecursiveComparison().isEqualTo(expectedSink);
    }

    @Test
    void testGoodTableSinkWithStaticPartitions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = defaultSinkOptions().build();
        List<String> partitionKeys = Collections.singletonList("partition_key");

        // Construct actual sink
        DynamoDbDynamicSink originalSink =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);

        DynamoDbDynamicSink sinkWithStaticPartition =
                (DynamoDbDynamicSink) createTableSink(sinkSchema, partitionKeys, sinkOptions);
        sinkWithStaticPartition.applyStaticPartition(ImmutableMap.of("no_op_key", "no_op_value"));

        // Verify no-op for applyStaticPartition
        assertThat(sinkWithStaticPartition).usingRecursiveComparison().isEqualTo(originalSink);
    }

    @Test
    void testBadTableSinkWithoutTableName() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                new TableOptionsBuilder(
                                DynamoDbDynamicSinkFactory.FACTORY_IDENTIFIER,
                                TestFormatFactory.IDENTIFIER)
                        .withTableOption(AWS_REGION, "us-east-1")
                        .build();
        List<String> partitionKeys = Collections.singletonList("partition_key");

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, partitionKeys, sinkOptions))
                .havingCause()
                .withMessageContaining("One or more required options are missing.")
                .withMessageContaining(TABLE_NAME.key());
    }

    @Test
    void testBadTableSinkWithoutAwsRegion() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions =
                new TableOptionsBuilder(
                                DynamoDbDynamicSinkFactory.FACTORY_IDENTIFIER,
                                TestFormatFactory.IDENTIFIER)
                        .withTableOption(TABLE_NAME, DYNAMO_DB_TABLE_NAME)
                        .build();
        List<String> partitionKeys = Collections.singletonList("partition_key");

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, partitionKeys, sinkOptions))
                .havingCause()
                .withMessageContaining("One or more required options are missing.")
                .withMessageContaining(AWS_REGION.key());
    }

    private ResolvedSchema createResolvedSchemaUsingAllDataTypes() {
        return ResolvedSchema.of(
                Column.physical("partition_key", DataTypes.STRING()),
                Column.physical("sort_key", DataTypes.BIGINT()),
                Column.physical("payload", DataTypes.STRING()),
                Column.physical("some_char_array", DataTypes.ARRAY(DataTypes.CHAR(1))),
                Column.physical("some_varchar_array", DataTypes.ARRAY(DataTypes.VARCHAR(1))),
                Column.physical("some_string_array", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physical("some_boolean_array", DataTypes.ARRAY(DataTypes.BOOLEAN())),
                Column.physical("some_decimal_array", DataTypes.ARRAY(DataTypes.DECIMAL(1, 1))),
                Column.physical("some_tinyint_array", DataTypes.ARRAY(DataTypes.TINYINT())),
                Column.physical("some_smallint_array", DataTypes.ARRAY(DataTypes.SMALLINT())),
                Column.physical("some_int_array", DataTypes.ARRAY(DataTypes.INT())),
                Column.physical("some_bigint_array", DataTypes.ARRAY(DataTypes.BIGINT())),
                Column.physical("some_float_array", DataTypes.ARRAY(DataTypes.FLOAT())),
                Column.physical("some_date_array", DataTypes.ARRAY(DataTypes.DATE())),
                Column.physical("some_time_array", DataTypes.ARRAY(DataTypes.TIME())),
                Column.physical("some_timestamp_array", DataTypes.ARRAY(DataTypes.TIMESTAMP())),
                Column.physical(
                        "some_timestamp_ltz_array", DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ())),
                Column.physical("some_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())));
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical("partition_key", DataTypes.STRING()),
                Column.physical("sort_key", DataTypes.BIGINT()),
                Column.physical("payload", DataTypes.STRING()));
    }

    private TableOptionsBuilder defaultSinkOptions() {
        return new TableOptionsBuilder(
                        DynamoDbDynamicSinkFactory.FACTORY_IDENTIFIER, TestFormatFactory.IDENTIFIER)
                .withTableOption(TABLE_NAME, DYNAMO_DB_TABLE_NAME)
                .withTableOption("aws.region", "us-east-1");
    }

    private Properties defaultSinkProperties() {
        Properties properties = new Properties();
        properties.put("aws.region", "us-east-1");
        return properties;
    }
}
