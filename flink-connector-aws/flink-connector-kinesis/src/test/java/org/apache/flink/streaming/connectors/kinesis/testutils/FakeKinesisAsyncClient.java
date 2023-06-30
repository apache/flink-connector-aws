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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.AddTagsToStreamRequest;
import software.amazon.awssdk.services.kinesis.model.AddTagsToStreamResponse;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodResponse;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DeregisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeLimitsRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeLimitsResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.DisableEnhancedMonitoringRequest;
import software.amazon.awssdk.services.kinesis.model.DisableEnhancedMonitoringResponse;
import software.amazon.awssdk.services.kinesis.model.EnableEnhancedMonitoringRequest;
import software.amazon.awssdk.services.kinesis.model.EnableEnhancedMonitoringResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodResponse;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamConsumersResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamResponse;
import software.amazon.awssdk.services.kinesis.model.MergeShardsRequest;
import software.amazon.awssdk.services.kinesis.model.MergeShardsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamRequest;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamResponse;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;
import software.amazon.awssdk.services.kinesis.model.SplitShardResponse;
import software.amazon.awssdk.services.kinesis.model.StartStreamEncryptionRequest;
import software.amazon.awssdk.services.kinesis.model.StartStreamEncryptionResponse;
import software.amazon.awssdk.services.kinesis.model.StopStreamEncryptionRequest;
import software.amazon.awssdk.services.kinesis.model.StopStreamEncryptionResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountResponse;
import software.amazon.awssdk.services.kinesis.model.UpdateStreamModeRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateStreamModeResponse;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamConsumersPublisher;
import software.amazon.awssdk.services.kinesis.waiters.KinesisAsyncWaiter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** A mock {@link KinesisAsyncClient} for testing. */
public class FakeKinesisAsyncClient implements KinesisAsyncClient {
    public SubscribeToShardRequest subscribeToShardRequest;
    public SubscribeToShardResponseHandler asyncResponseHandler;
    public boolean isClosed;

    @Override
    public CompletableFuture<AddTagsToStreamResponse> addTagsToStream(
            AddTagsToStreamRequest addTagsToStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<AddTagsToStreamResponse> addTagsToStream(
            Consumer<AddTagsToStreamRequest.Builder> addTagsToStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(
            CreateStreamRequest createStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(
            Consumer<CreateStreamRequest.Builder> createStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DecreaseStreamRetentionPeriodResponse> decreaseStreamRetentionPeriod(
            DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DecreaseStreamRetentionPeriodResponse> decreaseStreamRetentionPeriod(
            Consumer<DecreaseStreamRetentionPeriodRequest.Builder>
                    decreaseStreamRetentionPeriodRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(
            DeleteStreamRequest deleteStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(
            Consumer<DeleteStreamRequest.Builder> deleteStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DeregisterStreamConsumerResponse> deregisterStreamConsumer(
            DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DeregisterStreamConsumerResponse> deregisterStreamConsumer(
            Consumer<DeregisterStreamConsumerRequest.Builder> deregisterStreamConsumerRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits(
            DescribeLimitsRequest describeLimitsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits(
            Consumer<DescribeLimitsRequest.Builder> describeLimitsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits() {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(
            DescribeStreamRequest describeStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(
            Consumer<DescribeStreamRequest.Builder> describeStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(
            DescribeStreamConsumerRequest describeStreamConsumerRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(
            Consumer<DescribeStreamConsumerRequest.Builder> describeStreamConsumerRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(
            DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(
            Consumer<DescribeStreamSummaryRequest.Builder> describeStreamSummaryRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DisableEnhancedMonitoringResponse> disableEnhancedMonitoring(
            DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
        return null;
    }

    @Override
    public CompletableFuture<DisableEnhancedMonitoringResponse> disableEnhancedMonitoring(
            Consumer<DisableEnhancedMonitoringRequest.Builder> disableEnhancedMonitoringRequest) {
        return null;
    }

    @Override
    public CompletableFuture<EnableEnhancedMonitoringResponse> enableEnhancedMonitoring(
            EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
        return null;
    }

    @Override
    public CompletableFuture<EnableEnhancedMonitoringResponse> enableEnhancedMonitoring(
            Consumer<EnableEnhancedMonitoringRequest.Builder> enableEnhancedMonitoringRequest) {
        return null;
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(
            Consumer<GetRecordsRequest.Builder> getRecordsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(
            GetShardIteratorRequest getShardIteratorRequest) {
        return null;
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(
            Consumer<GetShardIteratorRequest.Builder> getShardIteratorRequest) {
        return null;
    }

    @Override
    public CompletableFuture<IncreaseStreamRetentionPeriodResponse> increaseStreamRetentionPeriod(
            IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        return null;
    }

    @Override
    public CompletableFuture<IncreaseStreamRetentionPeriodResponse> increaseStreamRetentionPeriod(
            Consumer<IncreaseStreamRetentionPeriodRequest.Builder>
                    increaseStreamRetentionPeriodRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(
            Consumer<ListShardsRequest.Builder> listShardsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(
            ListStreamConsumersRequest listStreamConsumersRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(
            Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest) {
        return null;
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(
            ListStreamConsumersRequest listStreamConsumersRequest) {
        return null;
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(
            Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(
            ListStreamsRequest listStreamsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(
            Consumer<ListStreamsRequest.Builder> listStreamsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams() {
        return null;
    }

    @Override
    public CompletableFuture<ListTagsForStreamResponse> listTagsForStream(
            ListTagsForStreamRequest listTagsForStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<ListTagsForStreamResponse> listTagsForStream(
            Consumer<ListTagsForStreamRequest.Builder> listTagsForStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<MergeShardsResponse> mergeShards(
            MergeShardsRequest mergeShardsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<MergeShardsResponse> mergeShards(
            Consumer<MergeShardsRequest.Builder> mergeShardsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<PutRecordResponse> putRecord(PutRecordRequest putRecordRequest) {
        return null;
    }

    @Override
    public CompletableFuture<PutRecordResponse> putRecord(
            Consumer<PutRecordRequest.Builder> putRecordRequest) {
        return null;
    }

    @Override
    public CompletableFuture<PutRecordsResponse> putRecords(PutRecordsRequest putRecordsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<PutRecordsResponse> putRecords(
            Consumer<PutRecordsRequest.Builder> putRecordsRequest) {
        return null;
    }

    @Override
    public CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumer(
            RegisterStreamConsumerRequest registerStreamConsumerRequest) {
        return null;
    }

    @Override
    public CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumer(
            Consumer<RegisterStreamConsumerRequest.Builder> registerStreamConsumerRequest) {
        return null;
    }

    @Override
    public CompletableFuture<RemoveTagsFromStreamResponse> removeTagsFromStream(
            RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<RemoveTagsFromStreamResponse> removeTagsFromStream(
            Consumer<RemoveTagsFromStreamRequest.Builder> removeTagsFromStreamRequest) {
        return null;
    }

    @Override
    public CompletableFuture<SplitShardResponse> splitShard(SplitShardRequest splitShardRequest) {
        return null;
    }

    @Override
    public CompletableFuture<SplitShardResponse> splitShard(
            Consumer<SplitShardRequest.Builder> splitShardRequest) {
        return null;
    }

    @Override
    public CompletableFuture<StartStreamEncryptionResponse> startStreamEncryption(
            StartStreamEncryptionRequest startStreamEncryptionRequest) {
        return null;
    }

    @Override
    public CompletableFuture<StartStreamEncryptionResponse> startStreamEncryption(
            Consumer<StartStreamEncryptionRequest.Builder> startStreamEncryptionRequest) {
        return null;
    }

    @Override
    public CompletableFuture<StopStreamEncryptionResponse> stopStreamEncryption(
            StopStreamEncryptionRequest stopStreamEncryptionRequest) {
        return null;
    }

    @Override
    public CompletableFuture<StopStreamEncryptionResponse> stopStreamEncryption(
            Consumer<StopStreamEncryptionRequest.Builder> stopStreamEncryptionRequest) {
        return null;
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(
            SubscribeToShardRequest subscribeToShardRequest,
            SubscribeToShardResponseHandler asyncResponseHandler) {
        this.subscribeToShardRequest = subscribeToShardRequest;
        this.asyncResponseHandler = asyncResponseHandler;
        return null;
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(
            Consumer<SubscribeToShardRequest.Builder> subscribeToShardRequest,
            SubscribeToShardResponseHandler asyncResponseHandler) {
        return null;
    }

    @Override
    public CompletableFuture<UpdateShardCountResponse> updateShardCount(
            UpdateShardCountRequest updateShardCountRequest) {
        return null;
    }

    @Override
    public CompletableFuture<UpdateShardCountResponse> updateShardCount(
            Consumer<UpdateShardCountRequest.Builder> updateShardCountRequest) {
        return null;
    }

    @Override
    public CompletableFuture<UpdateStreamModeResponse> updateStreamMode(
            UpdateStreamModeRequest updateStreamModeRequest) {
        return null;
    }

    @Override
    public CompletableFuture<UpdateStreamModeResponse> updateStreamMode(
            Consumer<UpdateStreamModeRequest.Builder> updateStreamModeRequest) {
        return null;
    }

    @Override
    public KinesisAsyncWaiter waiter() {
        return KinesisAsyncClient.super.waiter();
    }

    @Override
    public KinesisServiceClientConfiguration serviceClientConfiguration() {
        return KinesisServiceClientConfiguration.builder().build();
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }
}
