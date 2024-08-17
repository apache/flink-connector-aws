/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.sqs.table;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.sqs.sink.SqsSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test class for {@link SqsDynamicTableFactory}. */
public class SqsDynamicTableFactoryTest {
    private static final String SQS_QUEUE_URL = "sqs_queue_url";

    @Test
    public void createSqsDynamicTableSinkWithDefaultOptions() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();

        // Construct actual DynamicTableSink using FactoryUtil
        SqsDynamicSink actualSink = (SqsDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        SqsDynamicSink expectedSink = constructExpectedSink(physicalDataType, false);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    public void createSqsTableSinkWithoutQueueUrlFails() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.remove("queue-url");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageMatching(
                        "One or more required options are missing\\.[\\s\\n]*Missing required options are:[\\s\\n]*queue-url");
    }

    @Test
    public void createSqsTableSinkWithEmptyQueueUrlFails() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("queue-url", "");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageMatching("Sqs url must not be null or empty when creating SQS sink.");
    }

    @Test
    public void createSqsTableSinkWithoutRegionFails() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.remove("aws.region");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageMatching(
                        "One or more required options are missing\\.[\\s\\n]*Missing required options are:[\\s\\n]*aws.region");
    }

    @Test
    public void createSqsTableSinkWithoutFormatFails() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.remove("format");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageContaining("Could not find required sink format 'format'");
    }

    @Test
    public void createSqsTableSinkWithInvalidOptionFails() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("invalid-option", "invalid-value");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageContaining("Unsupported options:\n\ninvalid-option");
    }

    @Test
    public void createSqsTableSinkWithAwsOptionIsPropagatedToClientProperties() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("aws.http-client.read-timeout", "1000");

        // Construct actual DynamicTableSink using FactoryUtil
        SqsDynamicSink actualSink = (SqsDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        Properties clientProperties = getDefaultAwsClientProperties();
        clientProperties.put("aws.http-client.read-timeout", "1000");
        SqsDynamicSink expectedSink =
                getDefaultExpectedSinkBuilder(physicalDataType, clientProperties, false).build();
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    public void createSqsTableSinkWithFailOnErrorOptionIsPropagated() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("sink.fail-on-error", "true");

        // Construct actual DynamicTableSink using FactoryUtil
        SqsDynamicSink actualSink = (SqsDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        SqsDynamicSink expectedSink = constructExpectedSink(physicalDataType, true);
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    public void createSqsTableSinkWithUnknownAwsOptionSucceeds() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("aws.unknown-option", "unknown-value");

        // Construct actual DynamicTableSink using FactoryUtil
        SqsDynamicSink actualSink = (SqsDynamicSink) createTableSink(sinkSchema, sinkOptions);
        // Construct expected DynamicTableSink using factory under test
        Properties clientProperties = getDefaultAwsClientProperties();
        clientProperties.put("aws.unknown-option", "unknown-value");
        SqsDynamicSink expectedSink =
                getDefaultExpectedSinkBuilder(physicalDataType, clientProperties, false).build();
        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    public void createSqsTableSinkWithAsyncSinkOptionsArePropagated() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        DataType physicalDataType = sinkSchema.toPhysicalRowDataType();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("sink.batch.max-size", "1000");
        sinkOptions.put("sink.requests.max-inflight", "10");
        sinkOptions.put("sink.requests.max-buffered", "100");
        sinkOptions.put("sink.flush-buffer.size", "100");
        sinkOptions.put("sink.flush-buffer.timeout", "1000");

        SqsDynamicSink actualSink = (SqsDynamicSink) createTableSink(sinkSchema, sinkOptions);
        SqsDynamicSink expectedSink =
                getDefaultExpectedSinkBuilder(
                                physicalDataType, getDefaultAwsClientProperties(), false)
                        .setMaxBatchSize(1000)
                        .setMaxInFlightRequests(10)
                        .setMaxBufferedRequests(100)
                        .setMaxBufferSizeInBytes(100)
                        .setMaxTimeInBufferMS(1000)
                        .build();

        assertTableSinkEqualsAndOfCorrectType(actualSink, expectedSink);
    }

    @Test
    public void createSqsTableSinkWithInvalidRegionFailsOnCreate() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("aws.region", "invalid-region");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageContaining("Invalid AWS region");
    }

    @Test
    public void createSqsTableSinkWithInvalidCredentialsProviderFailsOnCreate() {
        ResolvedSchema sinkSchema = defaultSinkSchema();
        Map<String, String> sinkOptions = getDefaultTableOptionsBuilder().build();
        sinkOptions.put("aws.credentials.provider", "invalid-provider");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createTableSink(sinkSchema, sinkOptions))
                .havingCause()
                .withMessageContaining("Invalid AWS Credential Provider Type");
    }

    private ResolvedSchema defaultSinkSchema() {
        return ResolvedSchema.of(
                Column.physical("name", DataTypes.STRING()),
                Column.physical("curr_id", DataTypes.BIGINT()),
                Column.physical("time", DataTypes.TIMESTAMP(3)));
    }

    private TableOptionsBuilder getDefaultTableOptionsBuilder() {
        String format = TestFormatFactory.IDENTIFIER;
        TableOptionsBuilder builder =
                new TableOptionsBuilder("sqs", format)
                        // default table options
                        .withTableOption("queue-url", SQS_QUEUE_URL)
                        .withFormatOption(TestFormatFactory.DELIMITER, ",")
                        .withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
        Properties clientProperties = getDefaultAwsClientProperties();
        clientProperties.forEach((k, v) -> builder.withTableOption(k.toString(), v.toString()));
        return builder;
    }

    private SqsDynamicSink constructExpectedSink(DataType physicalDataType, boolean failOnError) {
        return getDefaultExpectedSinkBuilder(
                        physicalDataType, getDefaultAwsClientProperties(), failOnError)
                .build();
    }

    private Properties getDefaultAwsClientProperties() {
        Properties clientProperties = new Properties();
        clientProperties.put("aws.region", "us-west-2");
        clientProperties.put("aws.credentials.provider", "BASIC");
        clientProperties.put("aws.credentials.provider.basic.accesskeyid", "aws_access_key_id");
        clientProperties.put("aws.credentials.provider.basic.secretkey", "secret_access_key");
        return clientProperties;
    }

    private SqsDynamicSink.SqsDynamicSinkBuilder getDefaultExpectedSinkBuilder(
            DataType physicalDataType, Properties clientProperties, boolean failOnError) {
        return SqsDynamicSink.builder()
                .setSqsQueueUrl(SQS_QUEUE_URL)
                .setEncodingFormat(new TestFormatFactory.EncodingFormatMock(","))
                .setSqsClientProperties(clientProperties)
                .setConsumedDataType(physicalDataType)
                .setFailOnError(failOnError);
    }

    private void assertTableSinkEqualsAndOfCorrectType(
            DynamicTableSink actualSink, DynamicTableSink expectedSink) {
        // verify that the constructed DynamicTableSink is as expected
        assertThat(actualSink).isEqualTo(expectedSink);

        // verify the produced sink
        DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
                actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        Sink<RowData> sinkFunction = ((SinkV2Provider) sinkFunctionProvider).createSink();
        assertThat(sinkFunction).isInstanceOf(SqsSink.class);
    }
}
