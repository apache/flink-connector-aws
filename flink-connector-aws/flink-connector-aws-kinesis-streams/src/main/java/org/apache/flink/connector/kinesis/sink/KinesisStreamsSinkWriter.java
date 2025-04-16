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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getInvalidCredentialsExceptionClassifier;
import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getSdkClientMisconfiguredExceptionClassifier;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkFatalExceptionClassifiers.getInterruptedExceptionClassifier;

/**
 * Sink writer created by {@link KinesisStreamsSink} to write to Kinesis Data Streams. More details
 * on the operation of this sink writer may be found in the doc for {@link KinesisStreamsSink}. More
 * details on the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link KinesisAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 */
@Internal
class KinesisStreamsSinkWriter<InputT> extends AsyncSinkWriter<InputT, PutRecordsRequestEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamsSinkWriter.class);

    private static final FatalExceptionClassifier RESOURCE_NOT_FOUND_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.withRootCauseOfType(
                    ResourceNotFoundException.class,
                    err ->
                            new KinesisStreamsException(
                                    "Encountered non-recoverable exception relating to not being able to find the specified resources",
                                    err));

    private static final FatalExceptionClassifier KINESIS_FATAL_EXCEPTION_CLASSIFIER =
            FatalExceptionClassifier.createChain(
                    getInterruptedExceptionClassifier(),
                    getInvalidCredentialsExceptionClassifier(),
                    RESOURCE_NOT_FOUND_EXCEPTION_CLASSIFIER,
                    getSdkClientMisconfiguredExceptionClassifier());

    private static final int AIMD_RATE_LIMITING_STRATEGY_INCREASE_RATE = 10;
    private static final double AIMD_RATE_LIMITING_STRATEGY_DECREASE_FACTOR = 0.99D;

    private final Counter numRecordsOutErrorsCounter;

    /* Name of the stream in Kinesis Data Streams */
    private final String streamName;

    /* ARN of the stream in Kinesis Data Streams */
    private final String streamArn;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The asynchronous http client for the asynchronous Kinesis client */
    private final SdkAsyncHttpClient httpClient;

    /* The asynchronous Kinesis client - construction is by kinesisClientProperties */
    private final KinesisAsyncClient kinesisClient;

    /* The client provider used for testing */
    private final KinesisClientProvider kinesisClientProvider;

    /* Flag to whether fatally fail any time we encounter an exception when persisting records */
    private final boolean failOnError;

    KinesisStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            String streamArn,
            Properties kinesisClientProperties) {
        this(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                failOnError,
                streamName,
                streamArn,
                kinesisClientProperties,
                Collections.emptyList());
    }

    KinesisStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            String streamArn,
            Properties kinesisClientProperties,
            Collection<BufferedRequestState<PutRecordsRequestEntry>> states) {
        this(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                failOnError,
                streamName,
                streamArn,
                kinesisClientProperties,
                states,
                null);
    }

    KinesisStreamsSinkWriter(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            String streamArn,
            Properties kinesisClientProperties,
            Collection<BufferedRequestState<PutRecordsRequestEntry>> states,
            KinesisClientProvider kinesisClientProvider) {
        super(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .setRateLimitingStrategy(
                                buildRateLimitingStrategy(maxInFlightRequests, maxBatchSize))
                        .build(),
                states);
        this.failOnError = failOnError;
        this.streamName = streamName;
        this.streamArn = streamArn;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();

        this.kinesisClientProvider = kinesisClientProvider;

        if (kinesisClientProvider != null) {
            // Use the provided client for testing
            this.httpClient = null;
            this.kinesisClient = kinesisClientProvider.get();
        } else {
            // Create a new client as before
            this.httpClient = AWSGeneralUtil.createAsyncHttpClient(kinesisClientProperties);
            this.kinesisClient = buildClient(kinesisClientProperties, this.httpClient);
        }
    }

    private KinesisAsyncClient buildClient(
            Properties kinesisClientProperties, SdkAsyncHttpClient httpClient) {
        AWSGeneralUtil.validateAwsCredentials(kinesisClientProperties);

        return AWSClientUtil.createAwsAsyncClient(
                kinesisClientProperties,
                httpClient,
                KinesisAsyncClient.builder(),
                KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX);
    }

    private static RateLimitingStrategy buildRateLimitingStrategy(
            int maxInFlightRequests, int maxBatchSize) {
        return CongestionControlRateLimitingStrategy.builder()
                .setMaxInFlightRequests(maxInFlightRequests)
                .setInitialMaxInFlightMessages(maxBatchSize)
                .setScalingStrategy(
                        AIMDScalingStrategy.builder(maxBatchSize * maxInFlightRequests)
                                .setIncreaseRate(AIMD_RATE_LIMITING_STRATEGY_INCREASE_RATE)
                                .setDecreaseFactor(AIMD_RATE_LIMITING_STRATEGY_DECREASE_FACTOR)
                                .build())
                .build();
    }

    @Override
    protected void submitRequestEntries(
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<List<PutRecordsRequestEntry>> requestResult) {

        PutRecordsRequest batchRequest =
                PutRecordsRequest.builder()
                        .records(requestEntries)
                        .streamName(streamName)
                        .streamARN(streamArn)
                        .build();

        CompletableFuture<PutRecordsResponse> future = kinesisClient.putRecords(batchRequest);

        future.whenComplete(
                (response, err) -> {
                    if (err != null) {
                        handleFullyFailedRequest(err, requestEntries, requestResult);
                    } else if (response.failedRecordCount() > 0) {
                        handlePartiallyFailedRequest(response, requestEntries, requestResult);
                    } else {
                        requestResult.accept(Collections.emptyList());
                    }
                });
    }

    @Override
    protected long getSizeInBytes(PutRecordsRequestEntry requestEntry) {
        return requestEntry.data().asByteArrayUnsafe().length;
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<List<PutRecordsRequestEntry>> requestResult) {
        LOG.warn(
                "KDS Sink failed to write and will retry {} entries to KDS",
                requestEntries.size(),
                err);
        numRecordsOutErrorsCounter.inc(requestEntries.size());

        if (isRetryable(err)) {
            requestResult.accept(requestEntries);
        }
    }

    @Override
    public void close() {
        if (kinesisClientProvider != null) {
            kinesisClientProvider.close();
        } else {
            AWSGeneralUtil.closeResources(httpClient, kinesisClient);
        }
    }

    private void handlePartiallyFailedRequest(
            PutRecordsResponse response,
            List<PutRecordsRequestEntry> requestEntries,
            Consumer<List<PutRecordsRequestEntry>> requestResult) {
        int failedRecordCount = response.failedRecordCount();
        LOG.warn("KDS Sink failed to write and will retry {} entries to KDS", failedRecordCount);
        numRecordsOutErrorsCounter.inc(failedRecordCount);

        if (failOnError) {
            getFatalExceptionCons()
                    .accept(new KinesisStreamsException.KinesisStreamsFailFastException());
            return;
        }

        List<PutRecordsRequestEntry> failedRequestEntries = new ArrayList<>(failedRecordCount);
        List<PutRecordsResultEntry> records = response.records();

        // Collect error information and build the list of failed entries
        Map<String, ErrorSummary> errorSummaries =
                collectErrorSummaries(records, requestEntries, failedRequestEntries);

        // Log aggregated error information
        logErrorSummaries(errorSummaries);

        requestResult.accept(failedRequestEntries);
    }

    /**
     * Collect error summaries from failed records and build a list of failed request entries.
     *
     * @param records The result entries from the Kinesis response
     * @param requestEntries The original request entries
     * @param failedRequestEntries List to populate with failed entries (modified as a side effect)
     * @return A map of error codes to their summaries
     */
    private Map<String, ErrorSummary> collectErrorSummaries(
            List<PutRecordsResultEntry> records,
            List<PutRecordsRequestEntry> requestEntries,
            List<PutRecordsRequestEntry> failedRequestEntries) {

        // We capture error info while minimizing logging overhead in the data path,
        // which is critical for maintaining throughput performance
        Map<String, ErrorSummary> errorSummaries = new HashMap<>();

        for (int i = 0; i < records.size(); i++) {
            PutRecordsResultEntry resultEntry = records.get(i);
            String errorCode = resultEntry.errorCode();

            if (errorCode != null) {
                // Track the frequency of each error code to identify patterns
                ErrorSummary summary =
                        errorSummaries.computeIfAbsent(
                                errorCode, code -> new ErrorSummary(resultEntry.errorMessage()));
                summary.incrementCount();

                failedRequestEntries.add(requestEntries.get(i));
            }
        }

        return errorSummaries;
    }

    /**
     * Log aggregated error information at WARN level.
     *
     * @param errorSummaries Map of error codes to their summaries
     */
    private void logErrorSummaries(Map<String, ErrorSummary> errorSummaries) {
        // We log aggregated error information at WARN level to ensure visibility in production
        // while avoiding the performance impact of logging each individual failure
        if (!errorSummaries.isEmpty()) {
            StringBuilder errorSummary = new StringBuilder("Kinesis errors summary: ");
            errorSummaries.forEach(
                    (code, summary) ->
                            errorSummary.append(
                                    String.format(
                                            "[%s: %d records, example: %s] ",
                                            code,
                                            summary.getCount(),
                                            summary.getExampleMessage())));

            // Using a single WARN log with aggregated information provides operational
            // visibility into errors without flooding logs in high-throughput scenarios
            LOG.warn("KDS Sink failed to write, " + errorSummary.toString());
        }
    }

    /** Helper class to store error summary information. */
    private static class ErrorSummary {
        private final String exampleMessage;
        private int count;

        ErrorSummary(String exampleMessage) {
            this.exampleMessage = exampleMessage;
            this.count = 0;
        }

        void incrementCount() {
            count++;
        }

        int getCount() {
            return count;
        }

        String getExampleMessage() {
            return exampleMessage;
        }
    }

    private boolean isRetryable(Throwable err) {

        if (!KINESIS_FATAL_EXCEPTION_CLASSIFIER.isFatal(err, getFatalExceptionCons())) {
            return false;
        }
        if (failOnError) {
            getFatalExceptionCons()
                    .accept(new KinesisStreamsException.KinesisStreamsFailFastException(err));
            return false;
        }

        return true;
    }
}
