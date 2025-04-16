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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link KinesisStreamsSinkWriter}. */
public class KinesisStreamsSinkWriterTest {

    private static final int EXPECTED_AIMD_INC_RATE = 10;
    private static final double EXPECTED_AIMD_DEC_FACTOR = 0.99D;
    private static final int MAX_BATCH_SIZE = 50;
    private static final int MAX_INFLIGHT_REQUESTS = 16;
    private static final int MAX_BUFFERED_REQUESTS = 10000;
    private static final long MAX_BATCH_SIZE_IN_BYTES = 4 * 1024 * 1024;
    private static final long MAX_TIME_IN_BUFFER = 5000;
    private static final long MAX_RECORD_SIZE = 1000 * 1024;
    private static final boolean FAIL_ON_ERROR = false;
    private static final String STREAM_NAME = "streamName";
    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:000000000000:stream/" + STREAM_NAME;

    private KinesisStreamsSinkWriter<String> sinkWriter;

    private static final ElementConverter<String, PutRecordsRequestEntry>
            ELEMENT_CONVERTER_PLACEHOLDER =
                    KinesisStreamsSinkElementConverter.<String>builder()
                            .setSerializationSchema(new SimpleStringSchema())
                            .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                            .build();

    // Helper method to create a sink with default settings
    private KinesisStreamsSink<String> createDefaultSink() {
        Properties sinkProperties = AWSServicesTestUtils.createConfig("https://kds-fake-endpoint");
        return new KinesisStreamsSink<>(
                ELEMENT_CONVERTER_PLACEHOLDER,
                MAX_BATCH_SIZE,
                MAX_INFLIGHT_REQUESTS,
                MAX_BUFFERED_REQUESTS,
                MAX_BATCH_SIZE_IN_BYTES,
                MAX_TIME_IN_BUFFER,
                MAX_RECORD_SIZE,
                FAIL_ON_ERROR,
                STREAM_NAME,
                STREAM_ARN,
                sinkProperties);
    }

    // Helper method to create a simple test request entry
    private PutRecordsRequestEntry createTestEntry(String data, String partitionKey) {
        return PutRecordsRequestEntry.builder()
                .data(SdkBytes.fromUtf8String(data))
                .partitionKey(partitionKey)
                .build();
    }

    // Helper method to create a list of test request entries
    private List<PutRecordsRequestEntry> createTestEntries(int count) {
        List<PutRecordsRequestEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(createTestEntry("record" + i, "key" + i));
        }
        return entries;
    }

    // Helper method to create a tracking client that reports if it was used
    private KinesisAsyncClient createTrackingClient(AtomicBoolean wasUsed) {
        return new KinesisAsyncClient() {
            @Override
            public String serviceName() {
                return "Kinesis";
            }

            @Override
            public void close() {}

            @Override
            public CompletableFuture<PutRecordsResponse> putRecords(
                    PutRecordsRequest putRecordsRequest) {
                wasUsed.set(true);
                return CompletableFuture.completedFuture(
                        PutRecordsResponse.builder()
                                .failedRecordCount(0)
                                .records(Collections.emptyList())
                                .build());
            }
        };
    }

    // Helper method to create a client that returns partial failures with different error codes
    private KinesisAsyncClient createPartialFailureClient() {
        return new KinesisAsyncClient() {
            @Override
            public String serviceName() {
                return "Kinesis";
            }

            @Override
            public void close() {}

            @Override
            public CompletableFuture<PutRecordsResponse> putRecords(
                    PutRecordsRequest putRecordsRequest) {
                // Create a response with 5 records, 3 of which failed with different error codes
                List<PutRecordsResultEntry> resultEntries = new ArrayList<>();

                // Success record
                resultEntries.add(PutRecordsResultEntry.builder().build());

                // Failed records with different error codes
                resultEntries.add(
                        PutRecordsResultEntry.builder()
                                .errorCode("ProvisionedThroughputExceededException")
                                .errorMessage("Rate exceeded for shard 0000")
                                .build());

                resultEntries.add(
                        PutRecordsResultEntry.builder()
                                .errorCode("InternalFailure")
                                .errorMessage("Internal service failure")
                                .build());

                resultEntries.add(
                        PutRecordsResultEntry.builder()
                                .errorCode("ProvisionedThroughputExceededException")
                                .errorMessage("Rate exceeded for shard 0001")
                                .build());

                // Success record
                resultEntries.add(PutRecordsResultEntry.builder().build());

                PutRecordsResponse response =
                        PutRecordsResponse.builder()
                                .failedRecordCount(3)
                                .records(resultEntries)
                                .build();

                return CompletableFuture.completedFuture(response);
            }
        };
    }

    @Test
    void testCreateKinesisStreamsSinkWriterInitializesRateLimitingStrategyWithExpectedParameters()
            throws IOException {
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        KinesisStreamsSink<String> sink = createDefaultSink();
        sinkWriter = (KinesisStreamsSinkWriter<String>) sink.createWriter(sinkInitContext);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .isInstanceOf(CongestionControlRateLimitingStrategy.class);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .extracting("scalingStrategy")
                .isInstanceOf(AIMDScalingStrategy.class);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .extracting("scalingStrategy")
                .extracting("increaseRate")
                .isEqualTo(EXPECTED_AIMD_INC_RATE);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .extracting("scalingStrategy")
                .extracting("decreaseFactor")
                .isEqualTo(EXPECTED_AIMD_DEC_FACTOR);
    }

    @Test
    public void testCustomKinesisClientProviderIsUsed() throws IOException {
        // Create a tracking client
        AtomicBoolean clientWasUsed = new AtomicBoolean(false);
        KinesisAsyncClient mockClient = createTrackingClient(clientWasUsed);

        // Create a client provider with the mock client
        TestKinesisClientProvider clientProvider = new TestKinesisClientProvider(mockClient);

        // Create the sink
        KinesisStreamsSink<String> sink = createDefaultSink();

        // Create the writer and set the client provider
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        KinesisStreamsSinkWriter<String> writer =
                (KinesisStreamsSinkWriter<String>) sink.createWriter(sinkInitContext);
        writer.setKinesisClientProvider(clientProvider);

        // Submit a request to trigger the client
        List<PutRecordsRequestEntry> requestEntries =
                Collections.singletonList(createTestEntry("test", "test-key"));

        AtomicBoolean completeCalled = new AtomicBoolean(false);
        List<PutRecordsRequestEntry> failedEntries = new ArrayList<>();
        ResultHandler<PutRecordsRequestEntry> resultHandler = new ResultHandler<PutRecordsRequestEntry>() {
            @Override
            public void complete() {
                completeCalled.set(true);
            }

            @Override
            public void completeExceptionally(Exception e) {
                throw new RuntimeException("Unexpected exception", e);
            }

            @Override
            public void retryForEntries(List<PutRecordsRequestEntry> requestEntriesToRetry) {
                failedEntries.addAll(requestEntriesToRetry);
            }
        };

        writer.submitRequestEntries(requestEntries, resultHandler);
        // Verify the mock client was used
        assertThat(clientWasUsed.get()).isTrue();
        assertThat(completeCalled.get()).isTrue(); // Success callback was called
        assertThat(failedEntries).isEmpty(); // No failures
    }

    @Test
    public void testCustomKinesisClientIsClosedWithWriter() throws IOException {
        // Create a client provider with a mock client
        TestKinesisClientProvider clientProvider =
                new TestKinesisClientProvider(createTrackingClient(new AtomicBoolean()));

        // Create the sink
        KinesisStreamsSink<String> sink = createDefaultSink();

        // Create with a custom client provider and close the writer
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        KinesisStreamsSinkWriter<String> writer =
                (KinesisStreamsSinkWriter<String>) sink.createWriter(sinkInitContext);
        writer.setKinesisClientProvider(clientProvider);
        writer.close();

        // Verify the client provider was closed
        assertThat(clientProvider.getCloseCount()).isEqualTo(1);
    }

    @Test
    public void testDefaultClientProviderIsUsed() throws IOException {
        // Create the sink without setting a custom client provider
        KinesisStreamsSink<String> sink = createDefaultSink();

        // Create the writer
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        KinesisStreamsSinkWriter<String> writer =
                (KinesisStreamsSinkWriter<String>) sink.createWriter(sinkInitContext);

        // Verify that the writer has a non-null client provider and client
        assertThat(writer)
                .extracting("kinesisClientProvider")
                .isNotNull();
        // Close the writer and verify no exceptions are thrown
        writer.close();
    }

    @Test
    public void testErrorLoggingWithCustomClient() throws IOException {
        // Create a client provider with a partial failure client
        TestKinesisClientProvider clientProvider =
                new TestKinesisClientProvider(createPartialFailureClient());

        // Create the sink
        KinesisStreamsSink<String> sink = createDefaultSink();

        // Create the writer and set the client provider
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        KinesisStreamsSinkWriter<String> writer =
                (KinesisStreamsSinkWriter<String>) sink.createWriter(sinkInitContext);
        writer.setKinesisClientProvider(clientProvider);

        // Create test request entries
        List<PutRecordsRequestEntry> requestEntries = createTestEntries(5);

        // Call submitRequestEntries and capture the result
        List<PutRecordsRequestEntry> failedEntries = new ArrayList<>();
        AtomicBoolean completeExceptionally = new AtomicBoolean(false);
        AtomicBoolean completeCalled = new AtomicBoolean(false);
        Exception[] exception = new Exception[1];
        ResultHandler<PutRecordsRequestEntry> resultHandler = new ResultHandler<PutRecordsRequestEntry>() {
            @Override
            public void complete() {
                completeCalled.set(true);
            }

            @Override
            public void completeExceptionally(Exception e) {
                completeExceptionally.set(true);
                exception[0] = e;
            }

            @Override
            public void retryForEntries(List<PutRecordsRequestEntry> requestEntriesToRetry) {
                failedEntries.addAll(requestEntriesToRetry);
            }
        };

        writer.submitRequestEntries(requestEntries, resultHandler);

        // Verify that the correct records are returned for retry
        assertThat(failedEntries).hasSize(3);
        assertThat(failedEntries)
                .contains(requestEntries.get(1), requestEntries.get(2), requestEntries.get(3));

        // Verify that the error counter was incremented correctly
        assertThat(sinkInitContext.metricGroup().getNumRecordsOutErrorsCounter().getCount())
                .isEqualTo(3);

        // Verify that completeExceptionally was not called
        assertThat(completeExceptionally.get()).isFalse();
        // And complete was not called because there were partial failures
        assertThat(completeCalled.get()).isFalse();
    }

    @Test
    public void testErrorSummaryToString() {
        // Create an instance of the ErrorSummary class using reflection
        KinesisStreamsSinkWriter.ErrorSummary summary = createErrorSummary("Test error message");

        // Call incrementCount twice to set the count to 2
        incrementErrorSummaryCount(summary, 2);

        // Verify the toString output format is correct
        String expected = "[2 records, example: Test error message]";
        assertThat(summary.toString()).isEqualTo(expected);
    }

    // Helper method to create an ErrorSummary instance using reflection
    private KinesisStreamsSinkWriter.ErrorSummary createErrorSummary(String message) {
        try {
            Class<?> errorSummaryClass = Class.forName(
                    "org.apache.flink.connector.kinesis.sink.KinesisStreamsSinkWriter$ErrorSummary");
            return (KinesisStreamsSinkWriter.ErrorSummary) errorSummaryClass
                    .getDeclaredConstructor(String.class)
                    .newInstance(message);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create ErrorSummary instance", e);
        }
    }

    // Helper method to call incrementCount on an ErrorSummary instance
    private void incrementErrorSummaryCount(KinesisStreamsSinkWriter.ErrorSummary summary, int times) {
        try {
            java.lang.reflect.Method incrementCountMethod =
                summary.getClass().getDeclaredMethod("incrementCount");
            incrementCountMethod.setAccessible(true);

            for (int i = 0; i < times; i++) {
                incrementCountMethod.invoke(summary);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to call incrementCount", e);
        }
    }

    /** A test implementation of KinesisClientProvider that returns a mock KinesisAsyncClient. */
    private static class TestKinesisClientProvider implements KinesisClientProvider {
        private final KinesisAsyncClient kinesisClient;
        private int closeCount = 0;

        private TestKinesisClientProvider(KinesisAsyncClient kinesisClient) {
            this.kinesisClient = kinesisClient;
        }

        @Override
        public KinesisAsyncClient get() {
            return kinesisClient;
        }

        @Override
        public void close() {
            AWSGeneralUtil.closeResources(kinesisClient);
            closeCount++;
        }

        public int getCloseCount() {
            return closeCount;
        }
    }
}
