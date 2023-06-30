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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
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
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.ExpiredNextTokenException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodResponse;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.KmsAccessDeniedException;
import software.amazon.awssdk.services.kinesis.model.KmsDisabledException;
import software.amazon.awssdk.services.kinesis.model.KmsInvalidStateException;
import software.amazon.awssdk.services.kinesis.model.KmsNotFoundException;
import software.amazon.awssdk.services.kinesis.model.KmsOptInRequiredException;
import software.amazon.awssdk.services.kinesis.model.KmsThrottlingException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
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
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerRequest;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamRequest;
import software.amazon.awssdk.services.kinesis.model.RemoveTagsFromStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.SplitShardRequest;
import software.amazon.awssdk.services.kinesis.model.SplitShardResponse;
import software.amazon.awssdk.services.kinesis.model.StartStreamEncryptionRequest;
import software.amazon.awssdk.services.kinesis.model.StartStreamEncryptionResponse;
import software.amazon.awssdk.services.kinesis.model.StopStreamEncryptionRequest;
import software.amazon.awssdk.services.kinesis.model.StopStreamEncryptionResponse;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountResponse;
import software.amazon.awssdk.services.kinesis.model.UpdateStreamModeRequest;
import software.amazon.awssdk.services.kinesis.model.UpdateStreamModeResponse;
import software.amazon.awssdk.services.kinesis.model.ValidationException;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamConsumersIterable;
import software.amazon.awssdk.services.kinesis.waiters.KinesisWaiter;

import java.util.function.Consumer;

/** A mock {@link KinesisClient} for testing. */
public class FakeKinesisSyncClient implements KinesisClient {
    public boolean isClosed;
    public RegisterStreamConsumerRequest registerStreamConsumerRequest;
    public RegisterStreamConsumerResponse registerStreamConsumerResponse;
    public boolean isRegisterStreamConsumerCalled;
    public DeregisterStreamConsumerRequest deregisterStreamConsumerRequest;
    public DeregisterStreamConsumerResponse deregisterStreamConsumerResponse;
    public boolean isDeregisterStreamConsumerCalled;
    public DescribeStreamConsumerRequest describeStreamConsumerRequest;
    public DescribeStreamConsumerResponse describeStreamConsumerResponse;
    public boolean isDescribeStreamConsumerCalled;
    public DescribeStreamSummaryRequest describeStreamSummaryRequest;
    public DescribeStreamSummaryResponse describeStreamSummaryResponse;
    public boolean isDescribeStreamSummaryCalled;

    public void setFakeRegisterStreamConsumerResponse(
            RegisterStreamConsumerResponse registerStreamConsumerResponse) {
        this.registerStreamConsumerResponse = registerStreamConsumerResponse;
    }

    public void setFakeDeregisterStreamConsumerResponse(
            DeregisterStreamConsumerResponse deregisterStreamConsumerResponse) {
        this.deregisterStreamConsumerResponse = deregisterStreamConsumerResponse;
    }

    public void setFakeDescribeStreamConsumerResponse(
            DescribeStreamConsumerResponse describeStreamConsumerResponse) {
        this.describeStreamConsumerResponse = describeStreamConsumerResponse;
    }

    public void setFakeDescribeStreamSummaryResponse(
            DescribeStreamSummaryResponse describeStreamSummaryResponse) {
        this.describeStreamSummaryResponse = describeStreamSummaryResponse;
    }

    @Override
    public String serviceName() {
        return null;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public AddTagsToStreamResponse addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public AddTagsToStreamResponse addTagsToStream(
            Consumer<AddTagsToStreamRequest.Builder> addTagsToStreamRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public CreateStreamResponse createStream(CreateStreamRequest createStreamRequest)
            throws ResourceInUseException, LimitExceededException, InvalidArgumentException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public CreateStreamResponse createStream(
            Consumer<CreateStreamRequest.Builder> createStreamRequest)
            throws ResourceInUseException, LimitExceededException, InvalidArgumentException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DecreaseStreamRetentionPeriodResponse decreaseStreamRetentionPeriod(
            DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest)
            throws ResourceInUseException, ResourceNotFoundException, LimitExceededException,
                    InvalidArgumentException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public DecreaseStreamRetentionPeriodResponse decreaseStreamRetentionPeriod(
            Consumer<DecreaseStreamRetentionPeriodRequest.Builder>
                    decreaseStreamRetentionPeriodRequest)
            throws ResourceInUseException, ResourceNotFoundException, LimitExceededException,
                    InvalidArgumentException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public DeleteStreamResponse deleteStream(DeleteStreamRequest deleteStreamRequest)
            throws ResourceNotFoundException, LimitExceededException, ResourceInUseException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DeleteStreamResponse deleteStream(
            Consumer<DeleteStreamRequest.Builder> deleteStreamRequest)
            throws ResourceNotFoundException, LimitExceededException, ResourceInUseException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DeregisterStreamConsumerResponse deregisterStreamConsumer(
            DeregisterStreamConsumerRequest deregisterStreamConsumerRequest)
            throws LimitExceededException, ResourceNotFoundException, InvalidArgumentException,
                    AwsServiceException, SdkClientException, KinesisException {
        this.deregisterStreamConsumerRequest = deregisterStreamConsumerRequest;
        return this.deregisterStreamConsumerResponse;
    }

    @Override
    public DeregisterStreamConsumerResponse deregisterStreamConsumer(
            Consumer<DeregisterStreamConsumerRequest.Builder> deregisterStreamConsumerRequest)
            throws LimitExceededException, ResourceNotFoundException, InvalidArgumentException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DescribeLimitsResponse describeLimits()
            throws LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public DescribeLimitsResponse describeLimits(DescribeLimitsRequest describeLimitsRequest)
            throws LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public DescribeLimitsResponse describeLimits(
            Consumer<DescribeLimitsRequest.Builder> describeLimitsRequest)
            throws LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest)
            throws ResourceNotFoundException, LimitExceededException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DescribeStreamResponse describeStream(
            Consumer<DescribeStreamRequest.Builder> describeStreamRequest)
            throws ResourceNotFoundException, LimitExceededException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DescribeStreamConsumerResponse describeStreamConsumer(
            DescribeStreamConsumerRequest describeStreamConsumerRequest)
            throws LimitExceededException, ResourceNotFoundException, InvalidArgumentException,
                    AwsServiceException, SdkClientException, KinesisException {
        this.describeStreamConsumerRequest = describeStreamConsumerRequest;
        return describeStreamConsumerResponse;
    }

    @Override
    public DescribeStreamConsumerResponse describeStreamConsumer(
            Consumer<DescribeStreamConsumerRequest.Builder> describeStreamConsumerRequest)
            throws LimitExceededException, ResourceNotFoundException, InvalidArgumentException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DescribeStreamSummaryResponse describeStreamSummary(
            DescribeStreamSummaryRequest describeStreamSummaryRequest)
            throws ResourceNotFoundException, LimitExceededException, AwsServiceException,
                    SdkClientException, KinesisException {
        this.describeStreamSummaryRequest = describeStreamSummaryRequest;
        return describeStreamSummaryResponse;
    }

    @Override
    public DescribeStreamSummaryResponse describeStreamSummary(
            Consumer<DescribeStreamSummaryRequest.Builder> describeStreamSummaryRequest)
            throws ResourceNotFoundException, LimitExceededException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public DisableEnhancedMonitoringResponse disableEnhancedMonitoring(
            DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public DisableEnhancedMonitoringResponse disableEnhancedMonitoring(
            Consumer<DisableEnhancedMonitoringRequest.Builder> disableEnhancedMonitoringRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public EnableEnhancedMonitoringResponse enableEnhancedMonitoring(
            EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public EnableEnhancedMonitoringResponse enableEnhancedMonitoring(
            Consumer<EnableEnhancedMonitoringRequest.Builder> enableEnhancedMonitoringRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, ExpiredIteratorException,
                    KmsDisabledException, KmsInvalidStateException, KmsAccessDeniedException,
                    KmsNotFoundException, KmsOptInRequiredException, KmsThrottlingException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public GetRecordsResponse getRecords(Consumer<GetRecordsRequest.Builder> getRecordsRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, ExpiredIteratorException,
                    KmsDisabledException, KmsInvalidStateException, KmsAccessDeniedException,
                    KmsNotFoundException, KmsOptInRequiredException, KmsThrottlingException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public GetShardIteratorResponse getShardIterator(
            GetShardIteratorRequest getShardIteratorRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public GetShardIteratorResponse getShardIterator(
            Consumer<GetShardIteratorRequest.Builder> getShardIteratorRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public IncreaseStreamRetentionPeriodResponse increaseStreamRetentionPeriod(
            IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest)
            throws ResourceInUseException, ResourceNotFoundException, LimitExceededException,
                    InvalidArgumentException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public IncreaseStreamRetentionPeriodResponse increaseStreamRetentionPeriod(
            Consumer<IncreaseStreamRetentionPeriodRequest.Builder>
                    increaseStreamRetentionPeriodRequest)
            throws ResourceInUseException, ResourceNotFoundException, LimitExceededException,
                    InvalidArgumentException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public ListShardsResponse listShards(ListShardsRequest listShardsRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    ExpiredNextTokenException, ResourceInUseException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListShardsResponse listShards(Consumer<ListShardsRequest.Builder> listShardsRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    ExpiredNextTokenException, ResourceInUseException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListStreamConsumersResponse listStreamConsumers(
            ListStreamConsumersRequest listStreamConsumersRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    ExpiredNextTokenException, ResourceInUseException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListStreamConsumersResponse listStreamConsumers(
            Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    ExpiredNextTokenException, ResourceInUseException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListStreamConsumersIterable listStreamConsumersPaginator(
            ListStreamConsumersRequest listStreamConsumersRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    ExpiredNextTokenException, ResourceInUseException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListStreamConsumersIterable listStreamConsumersPaginator(
            Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    ExpiredNextTokenException, ResourceInUseException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListStreamsResponse listStreams()
            throws LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public ListStreamsResponse listStreams(ListStreamsRequest listStreamsRequest)
            throws LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public ListStreamsResponse listStreams(Consumer<ListStreamsRequest.Builder> listStreamsRequest)
            throws LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public ListTagsForStreamResponse listTagsForStream(
            ListTagsForStreamRequest listTagsForStreamRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public ListTagsForStreamResponse listTagsForStream(
            Consumer<ListTagsForStreamRequest.Builder> listTagsForStreamRequest)
            throws ResourceNotFoundException, InvalidArgumentException, LimitExceededException,
                    AwsServiceException, SdkClientException, KinesisException {
        return null;
    }

    @Override
    public MergeShardsResponse mergeShards(MergeShardsRequest mergeShardsRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, ValidationException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public MergeShardsResponse mergeShards(Consumer<MergeShardsRequest.Builder> mergeShardsRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, ValidationException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public PutRecordResponse putRecord(PutRecordRequest putRecordRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, KmsDisabledException,
                    KmsInvalidStateException, KmsAccessDeniedException, KmsNotFoundException,
                    KmsOptInRequiredException, KmsThrottlingException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public PutRecordResponse putRecord(Consumer<PutRecordRequest.Builder> putRecordRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, KmsDisabledException,
                    KmsInvalidStateException, KmsAccessDeniedException, KmsNotFoundException,
                    KmsOptInRequiredException, KmsThrottlingException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public PutRecordsResponse putRecords(PutRecordsRequest putRecordsRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, KmsDisabledException,
                    KmsInvalidStateException, KmsAccessDeniedException, KmsNotFoundException,
                    KmsOptInRequiredException, KmsThrottlingException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public PutRecordsResponse putRecords(Consumer<PutRecordsRequest.Builder> putRecordsRequest)
            throws ResourceNotFoundException, InvalidArgumentException,
                    ProvisionedThroughputExceededException, KmsDisabledException,
                    KmsInvalidStateException, KmsAccessDeniedException, KmsNotFoundException,
                    KmsOptInRequiredException, KmsThrottlingException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public RegisterStreamConsumerResponse registerStreamConsumer(
            RegisterStreamConsumerRequest registerStreamConsumerRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        this.registerStreamConsumerRequest = registerStreamConsumerRequest;
        return registerStreamConsumerResponse;
    }

    @Override
    public RegisterStreamConsumerResponse registerStreamConsumer(
            Consumer<RegisterStreamConsumerRequest.Builder> registerStreamConsumerRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public RemoveTagsFromStreamResponse removeTagsFromStream(
            RemoveTagsFromStreamRequest removeTagsFromStreamRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public RemoveTagsFromStreamResponse removeTagsFromStream(
            Consumer<RemoveTagsFromStreamRequest.Builder> removeTagsFromStreamRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public SplitShardResponse splitShard(SplitShardRequest splitShardRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, ValidationException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public SplitShardResponse splitShard(Consumer<SplitShardRequest.Builder> splitShardRequest)
            throws ResourceNotFoundException, ResourceInUseException, InvalidArgumentException,
                    LimitExceededException, ValidationException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public StartStreamEncryptionResponse startStreamEncryption(
            StartStreamEncryptionRequest startStreamEncryptionRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, KmsDisabledException, KmsInvalidStateException,
                    KmsAccessDeniedException, KmsNotFoundException, KmsOptInRequiredException,
                    KmsThrottlingException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public StartStreamEncryptionResponse startStreamEncryption(
            Consumer<StartStreamEncryptionRequest.Builder> startStreamEncryptionRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, KmsDisabledException, KmsInvalidStateException,
                    KmsAccessDeniedException, KmsNotFoundException, KmsOptInRequiredException,
                    KmsThrottlingException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public StopStreamEncryptionResponse stopStreamEncryption(
            StopStreamEncryptionRequest stopStreamEncryptionRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public StopStreamEncryptionResponse stopStreamEncryption(
            Consumer<StopStreamEncryptionRequest.Builder> stopStreamEncryptionRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public UpdateShardCountResponse updateShardCount(
            UpdateShardCountRequest updateShardCountRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, ValidationException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public UpdateShardCountResponse updateShardCount(
            Consumer<UpdateShardCountRequest.Builder> updateShardCountRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, ValidationException, AwsServiceException,
                    SdkClientException, KinesisException {
        return null;
    }

    @Override
    public UpdateStreamModeResponse updateStreamMode(
            UpdateStreamModeRequest updateStreamModeRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public UpdateStreamModeResponse updateStreamMode(
            Consumer<UpdateStreamModeRequest.Builder> updateStreamModeRequest)
            throws InvalidArgumentException, LimitExceededException, ResourceInUseException,
                    ResourceNotFoundException, AwsServiceException, SdkClientException,
                    KinesisException {
        return null;
    }

    @Override
    public KinesisWaiter waiter() {
        return null;
    }

    @Override
    public KinesisServiceClientConfiguration serviceClientConfiguration() {
        return KinesisServiceClientConfiguration.builder().build();
    }
}
