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
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DEREGISTER_CONSUMER_TIMEOUT;

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
    private final Duration deregisterTimeout;

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
        this.deregisterTimeout = configuration.get(EFO_DEREGISTER_CONSUMER_TIMEOUT);
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

    /**
     * Closes the split reader and releases all resources.
     *
     * <p>The close method follows a specific order to ensure proper shutdown:
     * 1. First, cancel all active subscriptions to prevent new events from being processed
     * 2. Then, shutdown the shared executor service to stop processing existing events
     * 3. Finally, close the async stream proxy to release network resources
     *
     * <p>This ordering is critical because:
     * - Cancelling subscriptions first prevents new events from being submitted to the executor
     * - Shutting down the executor next ensures all in-flight tasks complete or are cancelled
     * - Closing the async stream proxy last ensures all resources are properly released after
     *   all processing has stopped
     */
    @Override
    public void close() throws Exception {
        cancelActiveSubscriptions();
        shutdownSharedShardSubscriptionExecutor();
        closeAsyncStreamProxy();
    }

    /**
     * Cancels all active subscriptions to prevent new events from being processed.
     *
     * <p>After cancelling subscriptions, we wait a short time to allow the cancellation
     * signals to propagate before proceeding with executor shutdown.
     */
    private void cancelActiveSubscriptions() {
        for (FanOutKinesisShardSubscription subscription : splitSubscriptions.values()) {
            if (subscription.isActive()) {
                try {
                    subscription.cancelSubscription();
                } catch (Exception e) {
                    LOG.warn("Error cancelling subscription for shard {}",
                        subscription.getShardId(), e);
                }
            }
        }

        // Wait a short time (200ms) to allow cancellation signals to propagate
        // This helps ensure that no new tasks are submitted to the executor after we begin shutdown
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shuts down the shared executor service used for processing subscription events.
     *
     * <p>We use the EFO_DEREGISTER_CONSUMER_TIMEOUT (10 seconds) as the shutdown timeout
     * to maintain consistency with other deregistration operations in the connector.
     */
    private void shutdownSharedShardSubscriptionExecutor() {
        if (sharedShardSubscriptionExecutor == null) {
            return;
        }

        sharedShardSubscriptionExecutor.shutdown();
        try {
            // Use the deregister consumer timeout (10 seconds)
            // This timeout is consistent with other deregistration operations in the connector
            if (!sharedShardSubscriptionExecutor.awaitTermination(
                    deregisterTimeout.toMillis(),
                    TimeUnit.MILLISECONDS)) {
                LOG.warn("Executor did not terminate in the specified time. Forcing shutdown.");
                sharedShardSubscriptionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for executor shutdown", e);
            sharedShardSubscriptionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the async stream proxy with a timeout.
     *
     * <p>We use the EFO_CONSUMER_SUBSCRIPTION_TIMEOUT (60 seconds) as the close timeout
     * since closing the client involves similar network operations as subscription.
     * The longer timeout accounts for potential network delays during shutdown.
     */
    private void closeAsyncStreamProxy() {
        // Create a dedicated single-threaded executor for closing the asyncStreamProxy
        // This prevents the close operation from being affected by the main executor shutdown
        ExecutorService closeExecutor = new ThreadPoolExecutor(
            1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ExecutorThreadFactory("kinesis-client-close"));

        try {
            // Submit the close task to the executor to avoid blocking the main thread
            Future<?> closeFuture = closeExecutor.submit(() -> {
                try {
                    asyncStreamProxy.close();
                } catch (IOException e) {
                    LOG.warn("Error closing async stream proxy", e);
                }
            });

            try {
                // Use the subscription timeout (60 seconds)
                // This longer timeout is necessary because closing the AWS SDK client
                // may involve waiting for in-flight network operations to complete
                closeFuture.get(
                    subscriptionTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.warn("Timed out while closing async stream proxy", e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing async stream proxy", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOG.warn("Error while closing async stream proxy", e.getCause());
            }
        } finally {
            // Ensure the close executor is always shut down to prevent resource leaks
            closeExecutor.shutdownNow();
        }
    }
}
