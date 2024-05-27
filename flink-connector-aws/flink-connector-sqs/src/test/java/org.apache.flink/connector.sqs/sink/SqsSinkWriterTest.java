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

package org.apache.flink.connector.sqs.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchRequestTooLongException;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ResourceNotFoundException;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** Covers construction, defaults and sanity checking of {@link SqsSinkWriter}. */
public class SqsSinkWriterTest {

    @Mock
    private SqsAsyncClient sqsAsyncClient;

    @Mock
    private SdkAsyncHttpClient httpClient;

    @Mock
    private Consumer<List<SendMessageBatchRequestEntry>> requestResult;

    private SqsSinkWriter<String> sinkWriter;

    private static final ElementConverter<String, SendMessageBatchRequestEntry> ELEMENT_CONVERTER_PLACEHOLDER =
            SqsSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @BeforeEach
    void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        sinkWriter = getSqsSinkWriter(true);
    }

    @Test
    public void testNonRetryableExceptionWhenFailOnErrorFalseWillNotRetry() throws IOException {
        sinkWriter = getSqsSinkWriter(true);
        Exception exceptionToThrow =
                ResourceNotFoundException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build();

        CompletableFuture<SendMessageBatchResponse> exceptionalFuture = new CompletableFuture<>();
        exceptionalFuture.completeExceptionally(exceptionToThrow);

        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class)))
                .thenReturn(exceptionalFuture);

        CompletableFuture<List<SendMessageBatchRequestEntry>> failedRequests = new CompletableFuture<>();
        Consumer<List<SendMessageBatchRequestEntry>> failedRequestConsumer = failedRequests::complete;
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);
        assertThat(failedRequests).isNotCompleted();
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorTrueWillNotRetry() throws IOException {
        sinkWriter = getSqsSinkWriter(true);
        Exception exceptionToThrow =
                BatchRequestTooLongException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build();

        CompletableFuture<SendMessageBatchResponse> exceptionalFuture = new CompletableFuture<>();
        exceptionalFuture.completeExceptionally(exceptionToThrow);

        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class)))
                .thenReturn(exceptionalFuture);

        CompletableFuture<List<SendMessageBatchRequestEntry>> failedRequests = new CompletableFuture<>();
        Consumer<List<SendMessageBatchRequestEntry>> failedRequestConsumer = failedRequests::complete;
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);
        assertThat(failedRequests).isNotCompleted();
    }

    @Test
    public void testRetryableExceptionWhenFailOnErrorFalseWillRetry() throws IOException {
        sinkWriter = getSqsSinkWriter(false);
        Exception exceptionToThrow =
                BatchRequestTooLongException.builder()
                        .awsErrorDetails(
                                AwsErrorDetails.builder()
                                        .errorCode("SomeErrorCodeThatIsNotUsed")
                                        .build())
                        .build();

        CompletableFuture<SendMessageBatchResponse> exceptionalFuture = new CompletableFuture<>();
        exceptionalFuture.completeExceptionally(exceptionToThrow);

        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class)))
                .thenReturn(exceptionalFuture);

        CompletableFuture<List<SendMessageBatchRequestEntry>> failedRequests = new CompletableFuture<>();
        Consumer<List<SendMessageBatchRequestEntry>> failedRequestConsumer = failedRequests::complete;
        sinkWriter.submitRequestEntries(getDefaultInputRequests(), failedRequestConsumer);
        assertThat(failedRequests).isCompleted();
    }

    @Test
    public void testSubmitRequestEntriesWithNoException() throws IOException {
        sinkWriter = getSqsSinkWriter(true);

        CompletableFuture<SendMessageBatchResponse> mockFuture = CompletableFuture.completedFuture(
                SendMessageBatchResponse.builder().failed(Collections.emptyList()).successful(Collections.emptyList()).build());

        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(mockFuture);

        sinkWriter.submitRequestEntries(getDefaultInputRequests(), requestResult);
        verify(requestResult).accept(Collections.emptyList());
    }

    @Test
    public void testSubmitRequestEntriesWithPartialSuccessWithFailOnErrorFalseWillRetry() throws IOException {
        sinkWriter = getSqsSinkWriter(false);
        final String uuid = UUID.randomUUID().toString();
        List<BatchResultErrorEntry> batchResultErrorEntryList = new ArrayList<>();
        BatchResultErrorEntry batchResultErrorEntry = BatchResultErrorEntry.builder().id(uuid).build();
        batchResultErrorEntryList.add(batchResultErrorEntry);

        CompletableFuture<SendMessageBatchResponse> mockFuture = CompletableFuture.completedFuture(
                SendMessageBatchResponse.builder().failed(batchResultErrorEntryList).successful(Collections.emptyList()).build());

        List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntryList =
                Arrays.asList(SendMessageBatchRequestEntry.builder()
                        .id(uuid)
                        .messageBody("test1")
                        .build());
        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(mockFuture);
        sinkWriter.submitRequestEntries(sendMessageBatchRequestEntryList, requestResult);
        verify(requestResult).accept(sendMessageBatchRequestEntryList);
    }

    @Test
    public void testSubmitRequestEntriesWithPartialSuccessWithFailOnErrorFalseAndFailedIdDoesNotMatchWillNotRetry() throws IOException {
        sinkWriter = getSqsSinkWriter(false);
        final String uuid = UUID.randomUUID().toString();
        List<BatchResultErrorEntry> batchResultErrorEntryList = new ArrayList<>();
        BatchResultErrorEntry batchResultErrorEntry = BatchResultErrorEntry.builder().id(uuid).build();
        batchResultErrorEntryList.add(batchResultErrorEntry);

        CompletableFuture<SendMessageBatchResponse> mockFuture = CompletableFuture.completedFuture(
                SendMessageBatchResponse.builder().failed(batchResultErrorEntryList).successful(Collections.emptyList()).build());

        List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntryList =
                Arrays.asList(SendMessageBatchRequestEntry.builder()
                        .id(UUID.randomUUID().toString())
                        .messageBody("test1")
                        .build());
        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(mockFuture);
        sinkWriter.submitRequestEntries(sendMessageBatchRequestEntryList, requestResult);
        verify(requestResult).accept(Collections.emptyList());
    }

    @Test
    public void testSubmitRequestEntriesWithPartialSuccessWithFailOnErrorTrueWillNotRetry() throws IOException {
        sinkWriter = getSqsSinkWriter(true);
        final String uuid = UUID.randomUUID().toString();
        List<BatchResultErrorEntry> batchResultErrorEntryList = new ArrayList<>();
        BatchResultErrorEntry batchResultErrorEntry = BatchResultErrorEntry.builder().id(uuid).build();
        batchResultErrorEntryList.add(batchResultErrorEntry);

        CompletableFuture<SendMessageBatchResponse> mockFuture = CompletableFuture.completedFuture(
                SendMessageBatchResponse.builder().failed(batchResultErrorEntryList).successful(Collections.emptyList()).build());

        List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntryList =
                Arrays.asList(SendMessageBatchRequestEntry.builder()
                        .id(uuid)
                        .messageBody("test1")
                        .build());
        Mockito.when(sqsAsyncClient.sendMessageBatch(Mockito.any(SendMessageBatchRequest.class))).thenReturn(mockFuture);
        sinkWriter.submitRequestEntries(sendMessageBatchRequestEntryList, requestResult);
        verifyNoInteractions(requestResult);
    }

    @Test
    public void testClientClosesWhenWriterIsClosed() {
        sinkWriter.close();
        verify(sqsAsyncClient).close();
        verify(httpClient).close();
    }

    @Test
    void getSizeInBytesReturnsSizeOfBlobBeforeBase64Encoding() {
        String testString = "{many hands make light work;";
        SendMessageBatchRequestEntry record = SendMessageBatchRequestEntry.builder().messageBody(testString).build();
        assertThat(sinkWriter.getSizeInBytes(record))
                .isEqualTo(testString.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    void getNumRecordsOutErrorsCounterRecordsCorrectNumberOfFailures()
            throws IOException, InterruptedException {
        TestSinkInitContext ctx = new TestSinkInitContext();
        SqsSink<String> sqsSink =
                new SqsSink<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        12,
                        16,
                        10000,
                        4 * 1024 * 1024L,
                        5000L,
                        1000 * 1024L,
                        true,
                        "test-stream",
                        AWSServicesTestUtils.createConfig("https://localhost"));
        SinkWriter<String> writer = sqsSink.createWriter(ctx);

        for (int i = 0; i < 12; i++) {
            writer.write("data_bytes", null);
        }
        assertThatExceptionOfType(CompletionException.class)
                .isThrownBy(() -> writer.flush(true))
                .withCauseInstanceOf(SdkClientException.class)
                .withMessageContaining(
                        "Unable to execute HTTP request: Connection refused: localhost/127.0.0.1:443");
        assertThat(ctx.metricGroup().getNumRecordsOutErrorsCounter().getCount()).isEqualTo(12);
        assertThat(ctx.metricGroup().getNumRecordsSendErrorsCounter().getCount()).isEqualTo(12);
    }

    private SqsSinkWriter<String> getSqsSinkWriter(final boolean failOnError) throws IOException {
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        Properties sinkProperties = AWSServicesTestUtils.createConfig("https://fake_aws_endpoint");
        SqsSink<String> sink =
                new SqsSink<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        50,
                        16,
                        10000,
                        4 * 1024 * 1024L,
                        5000L,
                        1000 * 1024L,
                        failOnError,
                        "https://sqs.us-east-2.amazonaws.com/618277569814/fake-sqs",
                        sinkProperties);
        SqsSinkWriter sqsSinkWriter =  (SqsSinkWriter<String>) sink.createWriter(sinkInitContext);
        sqsSinkWriter.setSqsAsyncClient(sqsAsyncClient);
        sqsSinkWriter.setSdkAsyncHttpClient(httpClient);
        return sqsSinkWriter;
    }

    private List<SendMessageBatchRequestEntry> getDefaultInputRequests() {
        return Arrays.asList(sinkSendMessageBatchRequestEntry("test1"), sinkSendMessageBatchRequestEntry("test2"));
    }

    private SendMessageBatchRequestEntry sinkSendMessageBatchRequestEntry(final String messageBody) {
        return SendMessageBatchRequestEntry.builder()
                .id(UUID.randomUUID().toString())
                .messageBody(messageBody)
                .build();
    }

}
