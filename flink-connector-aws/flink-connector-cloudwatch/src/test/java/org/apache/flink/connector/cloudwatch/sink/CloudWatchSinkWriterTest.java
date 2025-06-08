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

import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.cloudwatch.sink.client.SdkClientProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchServiceClientConfiguration;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.InternalServiceException;
import software.amazon.awssdk.services.cloudwatch.model.InvalidFormatException;
import software.amazon.awssdk.services.cloudwatch.model.InvalidParameterCombinationException;
import software.amazon.awssdk.services.cloudwatch.model.InvalidParameterValueException;
import software.amazon.awssdk.services.cloudwatch.model.LimitExceededException;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sts.model.InvalidAuthorizationMessageException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_NAMESPACE;
import static org.assertj.core.api.Assertions.assertThat;

class CloudWatchSinkWriterTest {
    private static final long FUTURE_TIMEOUT_MS = 10000;
    private static final int BYTES_PER_DOUBLE = 8;

    @Test
    public void testSuccessfulRequest() throws Exception {
        List<MetricWriteRequest> inputRequests =
                Arrays.asList(
                        MetricWriteRequest.builder()
                                .withMetricName("test1")
                                .addValue(1d)
                                .addCount(1d)
                                .build(),
                        MetricWriteRequest.builder()
                                .withMetricName("test2")
                                .addValue(2d)
                                .addCount(2d)
                                .build());

        List<PutMetricDataRequest> expectedClientRequests =
                Collections.singletonList(
                        PutMetricDataRequest.builder()
                                .namespace(TEST_NAMESPACE)
                                .metricData(
                                        MetricDatum.builder()
                                                .metricName("test1")
                                                .values(1d)
                                                .counts(1d)
                                                .build(),
                                        MetricDatum.builder()
                                                .metricName("test2")
                                                .values(2d)
                                                .counts(2d)
                                                .build())
                                .strictEntityValidation(true)
                                .build());

        TrackingCloudWatchAsyncClient trackingCloudWatchAsyncClient =
                new TrackingCloudWatchAsyncClient();
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(trackingCloudWatchAsyncClient));
        CompletableFuture<List<MetricWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<MetricWriteRequest>> failedRequestConsumer = failedRequests::complete;

        cloudWatchSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);
        assertThat(trackingCloudWatchAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsAll(expectedClientRequests);
        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideRetryableException")
    public void testRetryableExceptionWillRetry(Exception retryableException) throws Exception {
        Optional<Exception> exceptionToThrow = Optional.of(retryableException);
        ThrowingCloudWatchAsyncClient<Exception> throwingCloudWatchAsyncClient =
                new ThrowingCloudWatchAsyncClient<>(exceptionToThrow, str -> true);
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(throwingCloudWatchAsyncClient));

        assertThatRequestsAreRetried(cloudWatchSinkWriter);
    }

    private static Stream<Arguments> provideRetryableException() {
        return Stream.of(
                Arguments.of(LimitExceededException.builder().build()),
                Arguments.of(InternalServiceException.builder().build()));
    }

    @ParameterizedTest
    @MethodSource("provideNonRetryableException")
    public void testNonRetryableExceptionWillNotRetry(Exception nonRetryableException)
            throws Exception {
        Optional<Exception> exceptionToThrow = Optional.of(nonRetryableException);
        ThrowingCloudWatchAsyncClient<Exception> throwingCloudWatchAsyncClient =
                new ThrowingCloudWatchAsyncClient<>(exceptionToThrow, str -> true);
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(throwingCloudWatchAsyncClient));

        assertThatRequestsAreNotRetried(cloudWatchSinkWriter);
    }

    private static Stream<Arguments> provideNonRetryableException() {
        return Stream.of(
                Arguments.of(ResourceNotFoundException.builder().build()),
                Arguments.of(InvalidAuthorizationMessageException.builder().build()));
    }

    @ParameterizedTest
    @MethodSource("provideInvalidMetricException")
    public void testInvalidMetricExceptionWillRetryWhenRetryModeEnabled() throws Exception {
        Optional<Exception> exceptionToThrow =
                Optional.ofNullable(InvalidFormatException.builder().build());
        ThrowingCloudWatchAsyncClient<Exception> throwingCloudWatchAsyncClient =
                new ThrowingCloudWatchAsyncClient<>(exceptionToThrow, str -> true);
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(throwingCloudWatchAsyncClient),
                        InvalidMetricDataRetryMode.RETRY);

        assertThatRequestsAreRetried(cloudWatchSinkWriter);
    }

    @ParameterizedTest
    @MethodSource("provideInvalidMetricException")
    public void testInvalidMetricExceptionWillSkipWhenSkipModeEnabled() throws Exception {
        Optional<Exception> exceptionToThrow =
                Optional.ofNullable(InvalidFormatException.builder().build());
        ThrowingCloudWatchAsyncClient<Exception> throwingCloudWatchAsyncClient =
                new ThrowingCloudWatchAsyncClient<>(exceptionToThrow, str -> true);
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(throwingCloudWatchAsyncClient),
                        InvalidMetricDataRetryMode.SKIP_METRIC_ON_ERROR);

        assertThatRequestsAreSkipped(cloudWatchSinkWriter);
    }

    @ParameterizedTest
    @MethodSource("provideInvalidMetricException")
    public void testInvalidMetricExceptionWillNotRetryWhenFailModeEnabled(
            CloudWatchException invalidMetricException) throws Exception {
        Optional<Exception> exceptionToThrow = Optional.ofNullable(invalidMetricException);
        ThrowingCloudWatchAsyncClient<Exception> throwingCloudWatchAsyncClient =
                new ThrowingCloudWatchAsyncClient<>(exceptionToThrow, str -> true);
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(throwingCloudWatchAsyncClient),
                        InvalidMetricDataRetryMode.FAIL_ON_ERROR);

        assertThatRequestsAreNotRetried(cloudWatchSinkWriter);
    }

    private static Stream<Arguments> provideInvalidMetricException() {
        return Stream.of(
                Arguments.of(InvalidFormatException.builder().build()),
                Arguments.of(InvalidParameterCombinationException.builder().build()),
                Arguments.of(InvalidParameterValueException.builder().build()));
    }

    @Test
    public void testGetSizeInBytesNotImplemented() throws IOException {
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(
                        new TestAsyncCloudWatchClientProvider(new TrackingCloudWatchAsyncClient()),
                        InvalidMetricDataRetryMode.FAIL_ON_ERROR);

        assertThat(
                        cloudWatchSinkWriter.getSizeInBytes(
                                MetricWriteRequest.builder()
                                        .withMetricName(TEST_METRIC_NAME)
                                        .addValue(123d)
                                        .build()))
                .isEqualTo(BYTES_PER_DOUBLE + TEST_METRIC_NAME.length());
    }

    @Test
    public void testClientClosesWhenWriterIsClosed() throws IOException {
        TestAsyncCloudWatchClientProvider testAsyncCloudWatchClientProvider =
                new TestAsyncCloudWatchClientProvider(new TrackingCloudWatchAsyncClient());
        CloudWatchSinkWriter<MetricWriteRequest> cloudWatchSinkWriter =
                getDefaultSinkWriter(testAsyncCloudWatchClientProvider);
        cloudWatchSinkWriter.close();

        assertThat(testAsyncCloudWatchClientProvider.getCloseCount()).isEqualTo(1);
    }

    private void assertThatRequestsAreRetried(CloudWatchSinkWriter cloudWatchSinkWriter)
            throws Exception {
        CompletableFuture<List<MetricWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<MetricWriteRequest>> failedRequestConsumer = failedRequests::complete;

        cloudWatchSinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);

        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .containsExactlyInAnyOrderElementsOf(getDefaultInputRequests());
    }

    private void assertThatRequestsAreNotRetried(CloudWatchSinkWriter cloudWatchSinkWriter) {
        CompletableFuture<List<MetricWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<MetricWriteRequest>> failedRequestConsumer = failedRequests::complete;

        cloudWatchSinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);

        assertThat(failedRequests).isNotCompleted();
    }

    private void assertThatRequestsAreSkipped(CloudWatchSinkWriter cloudWatchSinkWriter)
            throws Exception {
        CompletableFuture<List<MetricWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<MetricWriteRequest>> failedRequestConsumer = failedRequests::complete;

        cloudWatchSinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);

        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
    }

    private List<MetricWriteRequest> getDefaultInputRequests() {
        return Arrays.asList(
                MetricWriteRequest.builder()
                        .withMetricName(TEST_METRIC_NAME)
                        .addValue(1d)
                        .addCount(1d)
                        .build(),
                MetricWriteRequest.builder()
                        .withMetricName(TEST_METRIC_NAME)
                        .withStatisticMax(123d)
                        .build());
    }

    private CloudWatchSinkWriter<MetricWriteRequest> getDefaultSinkWriter(
            SdkClientProvider<CloudWatchAsyncClient> cloudWatchAsyncClientSdkClientProvider)
            throws IOException {
        return getDefaultSinkWriter(
                cloudWatchAsyncClientSdkClientProvider, InvalidMetricDataRetryMode.RETRY);
    }

    private CloudWatchSinkWriter<MetricWriteRequest> getDefaultSinkWriter(
            SdkClientProvider<CloudWatchAsyncClient> cloudWatchAsyncClientSdkClientProvider,
            InvalidMetricDataRetryMode invalidMetricDataRetryMode)
            throws IOException {
        TestSinkInitContext initContext = new TestSinkInitContext();
        CloudWatchSink<MetricWriteRequest> sink =
                CloudWatchSink.<MetricWriteRequest>builder()
                        .setNamespace(TEST_NAMESPACE)
                        .setInvalidMetricDataRetryMode(invalidMetricDataRetryMode)
                        .build();

        sink.setCloudWatchAsyncClientProvider(cloudWatchAsyncClientSdkClientProvider);
        return (CloudWatchSinkWriter<MetricWriteRequest>) sink.createWriter(initContext);
    }

    private static class ThrowingCloudWatchAsyncClient<T extends Throwable>
            implements CloudWatchAsyncClient {

        private final Optional<T> errorToReturn;
        private final Predicate<String> failWhenMatched;

        private ThrowingCloudWatchAsyncClient(
                Optional<T> errorToReturn, Predicate<String> failWhenMatched) {
            this.errorToReturn = errorToReturn;
            this.failWhenMatched = failWhenMatched;
        }

        @Override
        public String serviceName() {
            return "CloudWatch";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<PutMetricDataResponse> putMetricData(
                PutMetricDataRequest putMetricDataRequest) {
            if (errorToReturn.isPresent()) {
                CompletableFuture<PutMetricDataResponse> future = new CompletableFuture<>();
                future.completeExceptionally(
                        CloudWatchException.builder().cause((errorToReturn.get())).build());
                return future;
            }
            return CompletableFuture.completedFuture(PutMetricDataResponse.builder().build());
        }

        @Override
        public CloudWatchServiceClientConfiguration serviceClientConfiguration() {
            return CloudWatchServiceClientConfiguration.builder().build();
        }
    }

    private static class TrackingCloudWatchAsyncClient implements CloudWatchAsyncClient {

        private List<PutMetricDataRequest> requestHistory = new ArrayList<>();

        @Override
        public String serviceName() {
            return "CloudWatch";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<PutMetricDataResponse> putMetricData(
                PutMetricDataRequest putMetricDataRequest) {
            requestHistory.add(putMetricDataRequest);
            return CompletableFuture.completedFuture(PutMetricDataResponse.builder().build());
        }

        @Override
        public CloudWatchServiceClientConfiguration serviceClientConfiguration() {
            return CloudWatchServiceClientConfiguration.builder().build();
        }

        public List<PutMetricDataRequest> getRequestHistory() {
            return requestHistory;
        }
    }

    private static class TestAsyncCloudWatchClientProvider
            implements SdkClientProvider<CloudWatchAsyncClient> {

        private final CloudWatchAsyncClient cloudWatchAsyncClient;
        private int closeCount = 0;

        private TestAsyncCloudWatchClientProvider(CloudWatchAsyncClient cloudWatchAsyncClient) {
            this.cloudWatchAsyncClient = cloudWatchAsyncClient;
        }

        @Override
        public CloudWatchAsyncClient getClient() {
            return cloudWatchAsyncClient;
        }

        @Override
        public void close() {
            closeCount++;
        }

        public int getCloseCount() {
            return closeCount;
        }
    }
}
