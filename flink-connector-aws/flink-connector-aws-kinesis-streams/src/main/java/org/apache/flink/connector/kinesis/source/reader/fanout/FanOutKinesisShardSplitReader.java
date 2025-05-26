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

    /**
     * Shared executor service for making subscribeToShard API calls.
     *
     * <p>This executor is separate from the event processing executor to avoid contention
     * between API calls and event processing. Using a dedicated executor for subscription calls
     * provides several important benefits:
     *
     * <ol>
     *   <li>Prevents blocking of the main thread or event processing threads during API calls</li>
     *   <li>Isolates API call failures from event processing operations</li>
     *   <li>Allows for controlled concurrency of API calls across multiple shards</li>
     *   <li>Prevents potential deadlocks that could occur when the same thread handles both
     *       subscription calls and event processing</li>
     * </ol>
     *
     * <p>The executor uses a smaller number of threads than the event processing executor since
     * subscription calls are less frequent and primarily I/O bound. This helps optimize resource
     * usage while still providing sufficient parallelism for multiple concurrent subscription calls.
     */
    private final ExecutorService sharedSubscriptionCallExecutor;

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
                ExecutorService eventProcessingExecutor,
                ExecutorService subscriptionCallExecutor);
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
                ExecutorService eventProcessingExecutor,
                ExecutorService subscriptionCallExecutor) {
            return new FanOutKinesisShardSubscription(
                    proxy,
                    consumerArn,
                    shardId,
                    startingPosition,
                    timeout,
                    eventProcessingExecutor,
                    subscriptionCallExecutor);
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
            createDefaultEventProcessingExecutor(),
            createDefaultSubscriptionCallExecutor());
    }

    /**
     * Constructor with injected executor services for testing.
     *
     * @param asyncStreamProxy The proxy for Kinesis API calls
     * @param consumerArn The ARN of the consumer
     * @param shardMetricGroupMap The metrics map
     * @param configuration The configuration
     * @param subscriptionFactory The factory for creating subscriptions
     * @param eventProcessingExecutor The executor service to use for event processing tasks
     * @param subscriptionCallExecutor The executor service to use for subscription API calls
     */
    @VisibleForTesting
    FanOutKinesisShardSplitReader(
            AsyncStreamProxy asyncStreamProxy,
            String consumerArn,
            Map<String, KinesisShardMetrics> shardMetricGroupMap,
            Configuration configuration,
            SubscriptionFactory subscriptionFactory,
            ExecutorService eventProcessingExecutor,
            ExecutorService subscriptionCallExecutor) {
        super(shardMetricGroupMap, configuration);
        this.asyncStreamProxy = asyncStreamProxy;
        this.consumerArn = consumerArn;
        this.subscriptionTimeout = configuration.get(EFO_CONSUMER_SUBSCRIPTION_TIMEOUT);
        this.deregisterTimeout = configuration.get(EFO_DEREGISTER_CONSUMER_TIMEOUT);
        this.subscriptionFactory = subscriptionFactory;
        this.sharedShardSubscriptionExecutor = eventProcessingExecutor;
        this.sharedSubscriptionCallExecutor = subscriptionCallExecutor;
    }

    /**
     * Creates the default executor service for event processing tasks.
     *
     * @return A new executor service
     */
    private static ExecutorService createDefaultEventProcessingExecutor() {
        int minThreads = Runtime.getRuntime().availableProcessors();
        int maxThreads = minThreads * 2;
        return new ThreadPoolExecutor(
            minThreads,
            maxThreads,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), // Unbounded queue with natural flow control
            new ExecutorThreadFactory("kinesis-efo-subscription"));
    }

    /**
     * Creates the default executor service for subscription API calls.
     *
     * <p>This executor is configured with:
     * <ul>
     *   <li>Minimum threads: 1 - Ensures at least one thread is always available for API calls</li>
     *   <li>Maximum threads: max(2, availableProcessors/4) - Scales with system resources but
     *       keeps the thread count relatively low since API calls are I/O bound</li>
     *   <li>Keep-alive time: 60 seconds - Allows for efficient reuse of threads</li>
     *   <li>Unbounded queue - Safe because the number of subscription tasks is naturally limited
     *       by the number of shards</li>
     *   <li>Custom thread factory - Provides meaningful thread names for debugging</li>
     * </ul>
     *
     * <p>This configuration balances resource efficiency with responsiveness for subscription calls.
     * Since subscription calls are primarily waiting on network I/O, a relatively small number of
     * threads can efficiently handle many concurrent calls.
     *
     * @return A new executor service optimized for subscription API calls
     */
    private static ExecutorService createDefaultSubscriptionCallExecutor() {
        int minThreads = 1;
        int maxThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 4);
        return new ThreadPoolExecutor(
            minThreads,
            maxThreads,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), // Unbounded queue with natural flow control
            new ExecutorThreadFactory("kinesis-subscription-caller"));
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
                            sharedShardSubscriptionExecutor,
                            sharedSubscriptionCallExecutor);
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
        shutdownSharedSubscriptionCallExecutor();
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
                LOG.warn("Event processing executor did not terminate in the specified time. Forcing shutdown.");
                sharedShardSubscriptionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for event processing executor shutdown", e);
            sharedShardSubscriptionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Shuts down the shared executor service used for subscription API calls.
     *
     * <p>We use the EFO_DEREGISTER_CONSUMER_TIMEOUT (10 seconds) as the shutdown timeout
     * to maintain consistency with other deregistration operations in the connector.
     */
    private void shutdownSharedSubscriptionCallExecutor() {
        if (sharedSubscriptionCallExecutor == null) {
            return;
        }

        sharedSubscriptionCallExecutor.shutdown();
        try {
            // Use a shorter timeout since these are just API calls
            if (!sharedSubscriptionCallExecutor.awaitTermination(
                    deregisterTimeout.toMillis(),
                    TimeUnit.MILLISECONDS)) {
                LOG.warn("Subscription call executor did not terminate in the specified time. Forcing shutdown.");
                sharedSubscriptionCallExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for subscription call executor shutdown", e);
            sharedSubscriptionCallExecutor.shutdownNow();
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
