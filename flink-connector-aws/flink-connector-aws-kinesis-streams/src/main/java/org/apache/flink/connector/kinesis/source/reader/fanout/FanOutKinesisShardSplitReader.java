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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisShardSplitReaderBase;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_SUBSCRIPTION_TIMEOUT;

/**
 * An implementation of the KinesisShardSplitReader that consumes from Kinesis using Enhanced
 * Fan-Out and HTTP/2.
 */
@Internal
public class FanOutKinesisShardSplitReader extends KinesisShardSplitReaderBase {

    private static final Logger LOG = LoggerFactory.getLogger(FanOutKinesisShardSplitReader.class);
    private final AsyncStreamProxy asyncStreamProxy;
    private final String consumerArn;
    private final Duration subscriptionTimeout;

    /**
     * Shared executor service for all shard subscriptions.
     *
     * <p>This executor uses an unbounded queue ({@link LinkedBlockingQueue}) but this does not pose
     * a risk of out-of-memory errors due to the natural flow control mechanisms in the system:
     *
     * <ol>
     *   <li>Each {@link FanOutKinesisShardSubscription} has a bounded event queue with capacity of 2</li>
     *   <li>New records are only requested after processing an event (via {@code requestRecords()})</li>
     *   <li>When a shard's queue is full, the processing thread blocks at the {@code put()} operation</li>
     *   <li>The AWS SDK implements the Reactive Streams protocol with built-in backpressure</li>
     * </ol>
     *
     * <p>In the worst-case scenario during backpressure, the maximum number of events in memory is:
     * <pre>
     * Max Events = (2 * Number_of_Shards) + min(Number_of_Shards, Number_of_Threads)
     * </pre>
     *
     * <p>Where:
     * <ul>
     *   <li>2 * Number_of_Shards: Total capacity of all shard event queues</li>
     *   <li>min(Number_of_Shards, Number_of_Threads): Maximum events being actively processed</li>
     * </ul>
     *
     * <p>This ensures that memory usage scales linearly with the number of shards, not exponentially,
     * making it safe to use an unbounded executor queue even with a large number of shards.
     */
    private final ExecutorService sharedShardSubscriptionExecutor;

    private final Map<String, FanOutKinesisShardSubscription> splitSubscriptions = new HashMap<>();

    /**
     * Factory for creating subscriptions. This is primarily used for testing.
     */
    @VisibleForTesting
    public interface SubscriptionFactory {
        FanOutKinesisShardSubscription createSubscription(
                AsyncStreamProxy proxy,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration timeout,
                ExecutorService executor);
    }

    /**
     * Default implementation of the subscription factory.
     */
    private static class DefaultSubscriptionFactory implements SubscriptionFactory {
        @Override
        public FanOutKinesisShardSubscription createSubscription(
                AsyncStreamProxy proxy,
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                Duration timeout,
                ExecutorService executor) {
            return new FanOutKinesisShardSubscription(
                    proxy,
                    consumerArn,
                    shardId,
                    startingPosition,
                    timeout,
                    executor);
        }
    }

    private SubscriptionFactory subscriptionFactory;

    public FanOutKinesisShardSplitReader(
            AsyncStreamProxy asyncStreamProxy,
            String consumerArn,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Configuration configuration) {
        this(asyncStreamProxy, consumerArn, shardMetricGroupMap, configuration, new DefaultSubscriptionFactory());
    }

    @VisibleForTesting
    FanOutKinesisShardSplitReader(
            AsyncStreamProxy asyncStreamProxy,
            String consumerArn,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Configuration configuration,
            SubscriptionFactory subscriptionFactory) {
        this(
            asyncStreamProxy,
            consumerArn,
            shardMetricGroupMap,
            configuration,
            subscriptionFactory,
            createDefaultExecutor());
    }

    /**
     * Constructor with injected executor service for testing.
     *
     * @param asyncStreamProxy The proxy for Kinesis API calls
     * @param consumerArn The ARN of the consumer
     * @param shardMetricGroupMap The metrics map
     * @param configuration The configuration
     * @param subscriptionFactory The factory for creating subscriptions
     * @param executorService The executor service to use for subscription tasks
     */
    @VisibleForTesting
    FanOutKinesisShardSplitReader(
            AsyncStreamProxy asyncStreamProxy,
            String consumerArn,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Configuration configuration,
            SubscriptionFactory subscriptionFactory,
            ExecutorService executorService) {
        super(shardMetricGroupMap, configuration);
        this.asyncStreamProxy = asyncStreamProxy;
        this.consumerArn = consumerArn;
        this.subscriptionTimeout = configuration.get(EFO_CONSUMER_SUBSCRIPTION_TIMEOUT);
        this.subscriptionFactory = subscriptionFactory;
        this.sharedShardSubscriptionExecutor = executorService;
    }

    /**
     * Creates the default executor service for subscription tasks.
     *
     * @return A new executor service
     */
    private static ExecutorService createDefaultExecutor() {
        int minThreads = Runtime.getRuntime().availableProcessors();
        int maxThreads = minThreads * 2;
        return new ThreadPoolExecutor(
            minThreads,
            maxThreads,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), // Unbounded queue with natural flow control
            new ExecutorThreadFactory("kinesis-efo-subscription"));
    }

    @Override
    protected RecordBatch fetchRecords(KinesisShardSplitState splitState) {
        FanOutKinesisShardSubscription subscription =
                splitSubscriptions.get(splitState.getShardId());

        SubscribeToShardEvent event = subscription.nextEvent();
        if (event == null) {
            return null;
        }

        boolean shardCompleted = event.continuationSequenceNumber() == null;
        if (shardCompleted) {
            splitSubscriptions.remove(splitState.getShardId());
        }
        return new RecordBatch(event.records(), event.millisBehindLatest(), shardCompleted);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KinesisShardSplit> splitsChanges) {
        super.handleSplitsChanges(splitsChanges);
        for (KinesisShardSplit split : splitsChanges.splits()) {
            FanOutKinesisShardSubscription subscription =
                    subscriptionFactory.createSubscription(
                            asyncStreamProxy,
                            consumerArn,
                            split.getShardId(),
                            split.getStartingPosition(),
                            subscriptionTimeout,
                            sharedShardSubscriptionExecutor);
            subscription.activateSubscription();
            splitSubscriptions.put(split.splitId(), subscription);
        }
    }

    @Override
    public void close() throws Exception {
        // Shutdown the executor service first
        if (sharedShardSubscriptionExecutor != null) {
            sharedShardSubscriptionExecutor.shutdown();
            try {
                if (!sharedShardSubscriptionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    sharedShardSubscriptionExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                sharedShardSubscriptionExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // Create a separate single-threaded executor for closing the asyncStreamProxy
        ExecutorService closeExecutor = new ThreadPoolExecutor(
            1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ExecutorThreadFactory("kinesis-client-close"));

        try {
            // Submit the close task to the executor and wait with a timeout
            Future<?> closeFuture = closeExecutor.submit(() -> {
                try {
                    asyncStreamProxy.close();
                } catch (IOException e) {
                    LOG.warn("Error closing async stream proxy", e);
                }
            });

            // Wait for the close operation to complete with a timeout
            try {
                closeFuture.get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                LOG.warn("Timed out while closing async stream proxy", e);
            } catch (ExecutionException e) {
                LOG.warn("Error while closing async stream proxy", e.getCause());
            }
        } finally {
            // Ensure the close executor is shut down
            closeExecutor.shutdownNow();
        }
    }
}
