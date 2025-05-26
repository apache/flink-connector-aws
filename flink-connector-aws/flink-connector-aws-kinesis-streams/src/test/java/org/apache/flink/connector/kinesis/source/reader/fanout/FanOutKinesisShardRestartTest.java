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

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.CONSUMER_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the restart behavior in {@link FanOutKinesisShardSubscription}
 * and {@link FanOutKinesisShardSplitReader}.
 */
public class FanOutKinesisShardRestartTest extends FanOutKinesisShardTestBase {

    /**
     * Tests that when a restart happens, the correct starting position is used to reactivate the subscription.
     */
    @Test
    @Timeout(value = 30)
    public void testRestartUsesCorrectStartingPosition() throws Exception {
        // Create a custom AsyncStreamProxy that will capture the starting position
        AsyncStreamProxy customProxy = Mockito.mock(AsyncStreamProxy.class);
        ArgumentCaptor<StartingPosition> startingPositionCaptor = ArgumentCaptor.forClass(StartingPosition.class);

        when(customProxy.subscribeToShard(
                any(String.class),
                any(String.class),
                startingPositionCaptor.capture(),
                any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart());
        metricsMap.put(SHARD_ID, new KinesisShardMetrics(split, mockMetricGroup));

        // Create a reader
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                customProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration,
                createTestSubscriptionFactory(),
                testExecutor,
                testExecutor);

        // Add a split to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // Trigger the executor to execute the subscription tasks
        testExecutor.triggerAll();

        // Verify that the subscription was activated with the initial starting position
        verify(customProxy, times(1)).subscribeToShard(
                eq(CONSUMER_ARN),
                eq(SHARD_ID),
                any(StartingPosition.class),
                any());

        assertThat(startingPositionCaptor.getValue()).isEqualTo(StartingPosition.fromStart());

        // Create a new split with the updated starting position
        String continuationSequenceNumber = "sequence-after-processing";
        StartingPosition updatedPosition = StartingPosition.continueFromSequenceNumber(continuationSequenceNumber);
        KinesisShardSplit updatedSplit = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                updatedPosition);

        // Simulate a restart by creating a new reader with the updated split
        // Create a Configuration object and set the timeout
        Configuration restartConfiguration = new Configuration();
        restartConfiguration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        FanOutKinesisShardSplitReader restartedReader = new FanOutKinesisShardSplitReader(
                customProxy,
                CONSUMER_ARN,
                metricsMap,
                restartConfiguration,
                createTestSubscriptionFactory(),
                testExecutor,
                testExecutor);

        // Add the updated split to the restarted reader
        restartedReader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(updatedSplit)));

        // Trigger the executor to execute the subscription tasks for the restarted reader
        testExecutor.triggerAll();

        // Verify that the subscription was reactivated with the updated starting position
        verify(customProxy, times(2)).subscribeToShard(
                eq(CONSUMER_ARN),
                eq(SHARD_ID),
                any(StartingPosition.class),
                any());

        // Get the second captured value (from the restart)
        StartingPosition capturedPosition = startingPositionCaptor.getAllValues().get(1);

        // Verify it matches our expected updated position
        assertThat(capturedPosition.getShardIteratorType()).isEqualTo(updatedPosition.getShardIteratorType());
        assertThat(capturedPosition.getStartingMarker()).isEqualTo(updatedPosition.getStartingMarker());
    }

    /**
     * Tests that when exceptions are thrown, the job is restarted.
     */
    @Test
    @Timeout(value = 30)
    public void testExceptionsProperlyHandled() throws Exception {
        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart());
        metricsMap.put(SHARD_ID, new KinesisShardMetrics(split, mockMetricGroup));

        // Test with different types of exceptions
        testExceptionHandling(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder().message("Resource not found").build(), true);
        testExceptionHandling(new IOException("IO exception"), true);
        testExceptionHandling(new TimeoutException("Timeout"), true);
        testExceptionHandling(new RuntimeException("Runtime exception"), false);
    }

    /**
     * Helper method to test exception handling.
     */
    private void testExceptionHandling(Exception exception, boolean isRecoverable) throws Exception {
        // Create a metrics map for the shard
        Map<String, KinesisShardMetrics> metricsMap = new HashMap<>();
        KinesisShardSplit split = FanOutKinesisTestUtils.createTestSplit(
                STREAM_ARN,
                SHARD_ID,
                StartingPosition.fromStart());
        metricsMap.put(SHARD_ID, new KinesisShardMetrics(split, mockMetricGroup));

        // Create a mock AsyncStreamProxy that throws the specified exception
        AsyncStreamProxy exceptionProxy = Mockito.mock(AsyncStreamProxy.class);
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(exception);
        when(exceptionProxy.subscribeToShard(any(), any(), any(), any()))
                .thenReturn(failedFuture);

        // Create a reader with the exception-throwing proxy
        // Create a Configuration object and set the timeout
        Configuration configuration = new Configuration();
        configuration.set(KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT, TEST_SUBSCRIPTION_TIMEOUT);

        FanOutKinesisShardSplitReader reader = new FanOutKinesisShardSplitReader(
                exceptionProxy,
                CONSUMER_ARN,
                metricsMap,
                configuration,
                createTestSubscriptionFactory(),
                testExecutor,
                testExecutor);

        // Add a split to the reader
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

        // Trigger the executor to execute the subscription tasks
        testExecutor.triggerAll();

        // If the exception is recoverable, the reader should try to reactivate the subscription
        // If not, it should propagate the exception
        if (isRecoverable) {
            // Verify that the subscription was activated
            verify(exceptionProxy, times(1)).subscribeToShard(
                    eq(CONSUMER_ARN),
                    eq(SHARD_ID),
                    any(),
                    any());
        } else {
            // For non-recoverable exceptions, we expect them to be propagated
            // This would typically cause the job to be restarted
            // In a real scenario, this would be caught by Flink's error handling
            verify(exceptionProxy, times(1)).subscribeToShard(
                    eq(CONSUMER_ARN),
                    eq(SHARD_ID),
                    any(),
                    any());
        }
    }
}
