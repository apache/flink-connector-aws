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

package org.apache.flink.connector.cloudwatch.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.aws.sink.throwable.AWSExceptionHandler;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.cloudwatch.sink.client.SdkClientProvider;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.model.InvalidFormatException;
import software.amazon.awssdk.services.cloudwatch.model.InvalidParameterCombinationException;
import software.amazon.awssdk.services.cloudwatch.model.InvalidParameterValueException;
import software.amazon.awssdk.services.cloudwatch.model.MissingRequiredParameterException;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getInvalidCredentialsExceptionClassifier;
import static org.apache.flink.connector.aws.util.AWSCredentialFatalExceptionClassifiers.getSdkClientMisconfiguredExceptionClassifier;
import static org.apache.flink.connector.base.sink.writer.AsyncSinkFatalExceptionClassifiers.getInterruptedExceptionClassifier;

/**
 * Sink writer created by {@link CloudWatchSink} to write to CloudWatch. More details on the
 * operation of this sink writer may be found in the doc for {@link CloudWatchSink}. More details on
 * the internals of this sink writer may be found in {@link AsyncSinkWriter}.
 *
 * <p>The {@link CloudWatchAsyncClient} used here may be configured in the standard way for the AWS
 * SDK 2.x. e.g. the provision of {@code AWS_REGION}, {@code AWS_ACCESS_KEY_ID} and {@code
 * AWS_SECRET_ACCESS_KEY} through environment variables etc.
 *
 * <p>The batching of this sink is in terms of estimated size of a MetricWriteRequest in bytes. The
 * goal is adaptively increase the number of MetricWriteRequest in each batch, a
 * PutMetricDataRequest sent to CloudWatch, to a configurable number. This is the parameter
 * maxBatchSizeInBytes which is calculated based on getSizeInBytes.
 *
 * <p>getSizeInBytes(requestEntry) returns the size of a MetricWriteRequest in bytes which is
 * estimated by assuming each double takes 8 bytes, and each string char takes 1 byte (UTF_8
 * encoded).
 */
@Internal
class CloudWatchSinkWriter<InputT> extends AsyncSinkWriter<InputT, MetricWriteRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchSinkWriter.class);

    private final SdkClientProvider<CloudWatchAsyncClient> clientProvider;

    private static final AWSExceptionHandler CLOUDWATCH_FATAL_EXCEPTION_HANDLER =
            AWSExceptionHandler.withClassifier(
                    FatalExceptionClassifier.createChain(
                            getInterruptedExceptionClassifier(),
                            getInvalidCredentialsExceptionClassifier(),
                            CloudWatchExceptionClassifiers.getResourceNotFoundExceptionClassifier(),
                            CloudWatchExceptionClassifiers.getAccessDeniedExceptionClassifier(),
                            CloudWatchExceptionClassifiers.getNotAuthorizedExceptionClassifier(),
                            getSdkClientMisconfiguredExceptionClassifier()));

    private static final List<Class> CLOUDWATCH_INVALID_METRIC_EXCEPTION =
            Arrays.asList(
                    InvalidFormatException.class,
                    InvalidParameterCombinationException.class,
                    InvalidParameterValueException.class,
                    MissingRequiredParameterException.class);

    private static final int BYTES_PER_DOUBLE = 8;

    private final Counter numRecordsOutErrorsCounter;

    /* Namespace of CloudWatch metric */
    private final String namespace;

    /* The sink writer metric group */
    private final SinkWriterMetricGroup metrics;

    /* The retry mode when an invalid metric caused failure */
    private final InvalidMetricDataRetryMode invalidMetricDataRetryMode;

    CloudWatchSinkWriter(
            ElementConverter<InputT, MetricWriteRequest> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            String namespace,
            SdkClientProvider<CloudWatchAsyncClient> clientProvider,
            Collection<BufferedRequestState<MetricWriteRequest>> initialStates,
            InvalidMetricDataRetryMode invalidMetricDataRetryMode) {
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
                        .build(),
                initialStates);
        this.namespace = namespace;
        this.metrics = context.metricGroup();
        this.numRecordsOutErrorsCounter = metrics.getNumRecordsOutErrorsCounter();
        this.clientProvider = clientProvider;
        this.invalidMetricDataRetryMode = invalidMetricDataRetryMode;
    }

    @Override
    protected void submitRequestEntries(
            List<MetricWriteRequest> requestEntries,
            Consumer<List<MetricWriteRequest>> requestResult) {

        final PutMetricDataRequest putMetricDataRequest =
                PutMetricDataRequest.builder()
                        .namespace(namespace)
                        .metricData(
                                requestEntries.stream()
                                        .map(MetricWriteRequest::toMetricDatum)
                                        .collect(Collectors.toList()))
                        .strictEntityValidation(true)
                        .build();

        CompletableFuture<PutMetricDataResponse> future =
                clientProvider.getClient().putMetricData(putMetricDataRequest);

        // CloudWatchAsyncClient PutMetricDataRequest does not fail partially.
        // If there is only one poison pill Metric Datum, the whole request fails fully.
        future.whenComplete(
                        (response, err) -> {
                            if (err != null) {
                                handleFullyFailedRequest(err, requestEntries, requestResult);
                            } else {
                                requestResult.accept(Collections.emptyList());
                            }
                        })
                .exceptionally(
                        ex -> {
                            getFatalExceptionCons()
                                    .accept(
                                            new CloudWatchSinkException
                                                    .CloudWatchFailFastSinkException(
                                                    ex.getMessage(), ex));
                            return null;
                        });
    }

    @Override
    protected long getSizeInBytes(MetricWriteRequest requestEntry) {
        long sizeInBytes = 0L;
        sizeInBytes += requestEntry.getMetricName().getBytes(StandardCharsets.UTF_8).length;
        sizeInBytes += (long) requestEntry.getValues().length * BYTES_PER_DOUBLE;
        sizeInBytes += (long) requestEntry.getCounts().length * BYTES_PER_DOUBLE;

        sizeInBytes +=
                Arrays.stream(requestEntry.getDimensions())
                        .map(
                                dimension ->
                                        dimension.getName().getBytes(StandardCharsets.UTF_8).length
                                                + dimension
                                                        .getValue()
                                                        .getBytes(StandardCharsets.UTF_8)
                                                        .length)
                        .reduce(Integer::sum)
                        .orElse(0);

        sizeInBytes +=
                Stream.of(
                                        requestEntry.getStatisticSum(),
                                        requestEntry.getStatisticCount(),
                                        requestEntry.getStatisticMax(),
                                        requestEntry.getStatisticMin(),
                                        requestEntry.getStorageResolution(),
                                        requestEntry.getTimestamp())
                                .filter(Objects::nonNull)
                                .count()
                        * BYTES_PER_DOUBLE;

        return sizeInBytes;
    }

    @Override
    public void close() {
        AWSGeneralUtil.closeResources(clientProvider);
    }

    private void handleFullyFailedRequest(
            Throwable err,
            List<MetricWriteRequest> requestEntries,
            Consumer<List<MetricWriteRequest>> requestResult) {

        numRecordsOutErrorsCounter.inc(requestEntries.size());
        boolean isFatal =
                CLOUDWATCH_FATAL_EXCEPTION_HANDLER.consumeIfFatal(err, getFatalExceptionCons());
        if (isFatal) {
            return;
        }

        if (CLOUDWATCH_INVALID_METRIC_EXCEPTION.stream()
                .anyMatch(clazz -> ExceptionUtils.findThrowable(err, clazz).isPresent())) {
            handleInvalidMetricRequest(err, requestEntries, requestResult);
            return;
        }

        LOG.warn(
                "CloudWatch Sink failed to write and will retry all {} entries to CloudWatch,  First request was {}",
                requestEntries.size(),
                requestEntries.get(0).toString(),
                err);
        requestResult.accept(requestEntries);
    }

    private void handleInvalidMetricRequest(
            Throwable err,
            List<MetricWriteRequest> requestEntries,
            Consumer<List<MetricWriteRequest>> requestResult) {

        switch (invalidMetricDataRetryMode) {
            case SKIP_METRIC_ON_ERROR:
                LOG.warn(
                        "CloudWatch Sink failed to write and will skip sending all {} entries",
                        requestEntries.size(),
                        err);
                requestResult.accept(Collections.emptyList());
                return;
            case FAIL_ON_ERROR:
                LOG.warn(
                        "CloudWatch Sink failed to write all {} entries and will fail the job",
                        requestEntries.size(),
                        err);
                getFatalExceptionCons()
                        .accept(new CloudWatchSinkException.CloudWatchFailFastSinkException(err));
                return;
            case RETRY:
            default:
                LOG.warn(
                        "CloudWatch Sink failed to write and will retry all {} entries to CloudWatch, First request was {}",
                        requestEntries.size(),
                        requestEntries.get(0).toString(),
                        err);
                requestResult.accept(requestEntries);
        }
    }
}
