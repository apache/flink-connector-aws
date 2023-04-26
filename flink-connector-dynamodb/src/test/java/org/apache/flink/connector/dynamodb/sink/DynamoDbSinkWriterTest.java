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

package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.dynamodb.sink.client.SdkClientProvider;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbServiceClientConfiguration;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.sts.model.StsException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamoDbSinkWriter}. */
public class DynamoDbSinkWriterTest {

    private static final String PARTITION_KEY = "partition_key";
    private static final String SORT_KEY = "sort_key";
    private static final String TABLE_NAME = "table_name";
    private static final long FUTURE_TIMEOUT_MS = 1000;

    @Test
    public void testSuccessfulRequestWithNoDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = Collections.emptyList();
        List<DynamoDbWriteRequest> inputRequests =
                ImmutableList.of(
                        sinkPutRequest(item("pk", "1")),
                        sinkPutRequest(item("pk", "2")),
                        sinkDeleteRequest(item("pk", "3")),
                        sinkDeleteRequest(item("pk", "4")));
        List<WriteRequest> expectedClientRequests =
                ImmutableList.of(
                        dynamoDbPutRequest(item("pk", "1")),
                        dynamoDbPutRequest(item("pk", "2")),
                        dynamoDbDeleteRequest(item("pk", "3")),
                        dynamoDbDeleteRequest(item("pk", "4")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsExactly(expectedClientRequests);
        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
    }

    @Test
    public void testPutRequestPartitionKeyDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = ImmutableList.of(PARTITION_KEY);
        List<DynamoDbWriteRequest> inputRequests =
                ImmutableList.of(sinkPutRequest(item("pk", "1")), sinkPutRequest(item("pk", "2")));
        List<WriteRequest> expectedClientRequests =
                ImmutableList.of(dynamoDbPutRequest(item("pk", "2")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsExactly(expectedClientRequests);
        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
    }

    @Test
    public void testDeleteRequestPartitionKeyDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = ImmutableList.of(PARTITION_KEY);
        List<DynamoDbWriteRequest> inputRequests =
                ImmutableList.of(
                        sinkDeleteRequest(item("pk", "1")), sinkDeleteRequest(item("pk", "2")));
        List<WriteRequest> expectedClientRequests =
                ImmutableList.of(dynamoDbDeleteRequest(item("pk", "2")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .containsExactly(expectedClientRequests);
        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
    }

    @Test
    public void testMultipleKeyDeduplication() throws Exception {
        List<String> overwriteByPartitionKeys = ImmutableList.of(PARTITION_KEY, SORT_KEY);
        List<DynamoDbWriteRequest> inputRequests =
                ImmutableList.of(
                        sinkPutRequest(item("pk", "1")),
                        sinkPutRequest(item("pk", "2")),
                        sinkPutRequest(itemWithPayload("pk", "2", "string_payload_1")),
                        sinkPutRequest(itemWithPayload("pk", "2", "string_payload_2")));
        List<WriteRequest> expectedClientRequests =
                ImmutableList.of(
                        dynamoDbPutRequest(item("pk", "1")),
                        dynamoDbPutRequest(itemWithPayload("pk", "2", "string_payload_2")));

        TrackingDynamoDbAsyncClient trackingDynamoDbAsyncClient = new TrackingDynamoDbAsyncClient();
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        true, overwriteByPartitionKeys, () -> trackingDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);
        // Order does not matter in a batch write request
        assertThat(trackingDynamoDbAsyncClient.getRequestHistory())
                .isNotEmpty()
                .allSatisfy(
                        clientBatchRequest ->
                                assertThat(clientBatchRequest)
                                        .containsExactlyInAnyOrderElementsOf(
                                                expectedClientRequests));
        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS)).isEmpty();
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorOnWillNotRetry() {
        Optional<Exception> exceptionToThrow = getGenericRetryableException();
        boolean failOnError = true;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorOffWillRetry() throws Exception {
        Optional<Exception> exceptionToThrow = getGenericRetryableException();
        boolean failOnError = false;

        List<DynamoDbWriteRequest> inputRequests = getDefaultInputRequests();
        ThrowingDynamoDbAsyncClient<Exception> throwingDynamoDbAsyncClient =
                new ThrowingDynamoDbAsyncClient<>(exceptionToThrow, str -> true);
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        failOnError, Collections.emptyList(), () -> throwingDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);

        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .containsExactlyInAnyOrderElementsOf(inputRequests);
    }

    @Test
    public void testPartiallyFailedRequestRetriesFailedRecords() throws Exception {
        boolean failOnError = false;
        List<DynamoDbWriteRequest> inputRequests =
                ImmutableList.of(
                        sinkPutRequest(item("put_will_fail_pk", "1")),
                        sinkPutRequest(item("put_will_not_fail_pk", "2")),
                        sinkDeleteRequest(item("delete_will_fail_pk", "3")),
                        sinkDeleteRequest(item("delete_will_not_fail_pk", "4")));
        List<DynamoDbWriteRequest> expectedRetriedRecords =
                ImmutableList.of(
                        sinkPutRequest(item("put_will_fail_pk", "1")),
                        sinkDeleteRequest(item("delete_will_fail_pk", "3")));
        Predicate<String> failWhenPartitionKeyMatcher = str -> str.contains("will_fail_pk");

        FailingRecordsDynamoDbAsyncClient failingRecordsDynamoDbAsyncClient =
                new FailingRecordsDynamoDbAsyncClient(failWhenPartitionKeyMatcher);
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        failOnError,
                        Collections.emptyList(),
                        () -> failingRecordsDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(inputRequests, failedRequestConsumer);

        assertThat(failedRequests.get(FUTURE_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .usingRecursiveComparison()
                .isEqualTo(expectedRetriedRecords);
    }

    @Test
    public void testNonRetryableExceptionWhenFailOnErrorOnWillNotRetry() {
        Optional<Exception> exceptionToThrow = getGenericNonRetryableException();
        boolean failOnError = true;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testNonRetryableExceptionWhenFailOnErrorOffWillNotRetry() {
        Optional<Exception> exceptionToThrow = getGenericNonRetryableException();
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testInterruptedExceptionIsNonRetryable() {
        Optional<Exception> exceptionToThrow = Optional.of(new InterruptedException());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testInvalidCredentialsExceptionIsNonRetryable() {
        Optional<Exception> exceptionToThrow = Optional.of(StsException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testResourceNotFoundExceptionIsNonRetryable() {
        Optional<Exception> exceptionToThrow =
                Optional.of(ResourceNotFoundException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testConditionalCheckFailedExceptionIsNonRetryable() {
        Optional<Exception> exceptionToThrow =
                Optional.of(ConditionalCheckFailedException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testValidationExceptionIsNonRetryable() {
        Optional<Exception> exceptionToThrow =
                Optional.of(
                        DynamoDbException.builder()
                                .awsErrorDetails(
                                        AwsErrorDetails.builder()
                                                .errorCode("ValidationException")
                                                .build())
                                .build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testSdkClientExceptionIsNonRetryable() {
        Optional<Exception> exceptionToThrow = Optional.of(SdkClientException.builder().build());
        boolean failOnError = false;

        assertThatRequestsAreNotRetried(failOnError, exceptionToThrow);
    }

    @Test
    public void testGetSizeInBytesNotImplemented() {
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        false, Collections.emptyList(), () -> new TrackingDynamoDbAsyncClient());
        assertThat(dynamoDbSinkWriter.getSizeInBytes(sinkPutRequest(item("pk", "1")))).isEqualTo(0);
    }

    @Test
    public void testClientClosesWhenWriterIsClosed() {
        TestAsyncDynamoDbClientProvider testAsyncDynamoDbClientProvider =
                new TestAsyncDynamoDbClientProvider(new TrackingDynamoDbAsyncClient());
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        false, Collections.emptyList(), testAsyncDynamoDbClientProvider);
        dynamoDbSinkWriter.close();

        assertThat(testAsyncDynamoDbClientProvider.getCloseCount()).isEqualTo(1);
    }

    private void assertThatRequestsAreNotRetried(
            boolean failOnError, Optional<Exception> exceptionToThrow) {
        ThrowingDynamoDbAsyncClient<Exception> throwingDynamoDbAsyncClient =
                new ThrowingDynamoDbAsyncClient<>(exceptionToThrow, str -> true);
        DynamoDbSinkWriter<Map<String, AttributeValue>> dynamoDbSinkWriter =
                getDefaultSinkWriter(
                        failOnError, Collections.emptyList(), () -> throwingDynamoDbAsyncClient);
        CompletableFuture<List<DynamoDbWriteRequest>> failedRequests = new CompletableFuture<>();
        Consumer<List<DynamoDbWriteRequest>> failedRequestConsumer = failedRequests::complete;

        dynamoDbSinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);
        assertThat(failedRequests).isNotCompleted();
    }

    private DynamoDbSinkWriter<Map<String, AttributeValue>> getDefaultSinkWriter(
            boolean failOnError,
            List<String> overwriteByPartitionKeys,
            Supplier<DynamoDbAsyncClient> clientSupplier) {
        return getDefaultSinkWriter(
                failOnError,
                overwriteByPartitionKeys,
                new TestAsyncDynamoDbClientProvider(clientSupplier.get()));
    }

    private DynamoDbSinkWriter<Map<String, AttributeValue>> getDefaultSinkWriter(
            boolean failOnError,
            List<String> overwriteByPartitionKeys,
            SdkClientProvider<DynamoDbAsyncClient> dynamoDbAsyncClientProvider) {
        Sink.InitContext initContext = new TestSinkInitContext();
        return new DynamoDbSinkWriter(
                new TestDynamoDbElementConverter(),
                initContext,
                2,
                1,
                10,
                1024,
                1000,
                1024,
                failOnError,
                TABLE_NAME,
                overwriteByPartitionKeys,
                dynamoDbAsyncClientProvider,
                Collections.emptyList());
    }

    private List<DynamoDbWriteRequest> getDefaultInputRequests() {
        return ImmutableList.of(sinkPutRequest(item("pk", "1")), sinkPutRequest(item("pk", "2")));
    }

    private Optional<Exception> getGenericRetryableException() {
        return Optional.of(
                ProvisionedThroughputExceededException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build());
    }

    private Optional<Exception> getGenericNonRetryableException() {
        return Optional.of(
                ResourceNotFoundException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build());
    }

    private DynamoDbWriteRequest sinkPutRequest(Map<String, AttributeValue> item) {
        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.PUT)
                .setItem(item)
                .build();
    }

    private DynamoDbWriteRequest sinkDeleteRequest(Map<String, AttributeValue> item) {
        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.DELETE)
                .setItem(item)
                .build();
    }

    private WriteRequest dynamoDbPutRequest(Map<String, AttributeValue> item) {
        return WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build();
    }

    private WriteRequest dynamoDbDeleteRequest(Map<String, AttributeValue> item) {
        return WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder().key(item).build())
                .build();
    }

    private Map<String, AttributeValue> item(String partitionKey, String sortKey) {
        return ImmutableMap.<String, AttributeValue>builder()
                .put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build())
                .put(SORT_KEY, AttributeValue.builder().n(sortKey).build())
                .put("string_payload", AttributeValue.builder().s("some_strings").build())
                .put("number_payload", AttributeValue.builder().n("1234").build())
                .build();
    }

    private Map<String, AttributeValue> itemWithPayload(
            String partitionKey, String sortKey, String payload) {
        return ImmutableMap.<String, AttributeValue>builder()
                .put(PARTITION_KEY, AttributeValue.builder().s(partitionKey).build())
                .put(SORT_KEY, AttributeValue.builder().n(sortKey).build())
                .put("string_payload", AttributeValue.builder().s(payload).build())
                .put("number_payload", AttributeValue.builder().n("1234").build())
                .build();
    }

    private static class TestAsyncDynamoDbClientProvider
            implements SdkClientProvider<DynamoDbAsyncClient> {

        private final DynamoDbAsyncClient dynamoDbAsyncClient;
        private int closeCount = 0;

        private TestAsyncDynamoDbClientProvider(DynamoDbAsyncClient dynamoDbAsyncClient) {
            this.dynamoDbAsyncClient = dynamoDbAsyncClient;
        }

        @Override
        public DynamoDbAsyncClient getClient() {
            return dynamoDbAsyncClient;
        }

        @Override
        public void close() {
            closeCount++;
        }

        public int getCloseCount() {
            return closeCount;
        }
    }

    private static class TrackingDynamoDbAsyncClient implements DynamoDbAsyncClient {

        private List<List<WriteRequest>> requestHistory = new ArrayList<>();

        @Override
        public String serviceName() {
            return "DynamoDB";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(
                BatchWriteItemRequest batchWriteItemRequest) {
            requestHistory.add(batchWriteItemRequest.requestItems().get(TABLE_NAME));
            return CompletableFuture.completedFuture(BatchWriteItemResponse.builder().build());
        }

        @Override
        public DynamoDbServiceClientConfiguration serviceClientConfiguration() {
            return DynamoDbServiceClientConfiguration.builder().build();
        }

        public List<List<WriteRequest>> getRequestHistory() {
            return requestHistory;
        }
    }

    private static class ThrowingDynamoDbAsyncClient<T extends Throwable>
            implements DynamoDbAsyncClient {

        private final Optional<T> errorToReturn;
        private final Predicate<String> failWhenMatched;

        private ThrowingDynamoDbAsyncClient(
                Optional<T> errorToReturn, Predicate<String> failWhenMatched) {
            this.errorToReturn = errorToReturn;
            this.failWhenMatched = failWhenMatched;
        }

        @Override
        public String serviceName() {
            return "DynamoDB";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(
                BatchWriteItemRequest batchWriteItemRequest) {
            if (errorToReturn.isPresent()) {
                CompletableFuture<BatchWriteItemResponse> future = new CompletableFuture<>();
                future.completeExceptionally(
                        DynamoDbException.builder().cause((errorToReturn.get())).build());
                return future;
            }

            List<WriteRequest> failedRequests =
                    batchWriteItemRequest.requestItems().get(TABLE_NAME).stream()
                            .filter(
                                    writeRequest ->
                                            failWhenMatched.test(
                                                    writeRequest
                                                            .putRequest()
                                                            .item()
                                                            .get(PARTITION_KEY)
                                                            .s()))
                            .collect(Collectors.toList());

            BatchWriteItemResponse.Builder responseBuilder = BatchWriteItemResponse.builder();
            if (!failedRequests.isEmpty()) {
                responseBuilder =
                        responseBuilder.unprocessedItems(
                                ImmutableMap.of(TABLE_NAME, failedRequests));
            }
            return CompletableFuture.completedFuture(responseBuilder.build());
        }

        @Override
        public DynamoDbServiceClientConfiguration serviceClientConfiguration() {
            return DynamoDbServiceClientConfiguration.builder().build();
        }
    }

    private static class FailingRecordsDynamoDbAsyncClient implements DynamoDbAsyncClient {
        private final Predicate<String> failWhenMatched;

        private FailingRecordsDynamoDbAsyncClient(Predicate<String> failWhenMatched) {
            this.failWhenMatched = failWhenMatched;
        }

        @Override
        public String serviceName() {
            return "DynamoDB";
        }

        @Override
        public void close() {}

        @Override
        public CompletableFuture<BatchWriteItemResponse> batchWriteItem(
                BatchWriteItemRequest batchWriteItemRequest) {
            List<WriteRequest> failedRequests =
                    batchWriteItemRequest.requestItems().get(TABLE_NAME).stream()
                            .filter(
                                    writeRequest -> {
                                        if (writeRequest.putRequest() != null) {
                                            return failWhenMatched.test(
                                                    writeRequest
                                                            .putRequest()
                                                            .item()
                                                            .get(PARTITION_KEY)
                                                            .s());
                                        } else if (writeRequest.deleteRequest() != null) {
                                            return failWhenMatched.test(
                                                    writeRequest
                                                            .deleteRequest()
                                                            .key()
                                                            .get(PARTITION_KEY)
                                                            .s());
                                        } else {
                                            throw new RuntimeException(
                                                    "Write request cannot be empty");
                                        }
                                    })
                            .collect(Collectors.toList());

            BatchWriteItemResponse.Builder responseBuilder = BatchWriteItemResponse.builder();
            if (!failedRequests.isEmpty()) {
                responseBuilder =
                        responseBuilder.unprocessedItems(
                                ImmutableMap.of(TABLE_NAME, failedRequests));
            }
            return CompletableFuture.completedFuture(responseBuilder.build());
        }

        @Override
        public DynamoDbServiceClientConfiguration serviceClientConfiguration() {
            return DynamoDbServiceClientConfiguration.builder().build();
        }
    }
}
