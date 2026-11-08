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

package org.apache.flink.connector.dynamodb.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigUtil;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsShardAssigner;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumerator;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumeratorState;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumeratorStateSerializer;
import org.apache.flink.connector.dynamodb.source.metrics.DynamoDbStreamsShardMetrics;
import org.apache.flink.connector.dynamodb.source.proxy.DynamoDbStreamsProxy;
import org.apache.flink.connector.dynamodb.source.proxy.LazyStreamProxy;
import org.apache.flink.connector.dynamodb.source.reader.DynamoDbStreamsRecordEmitter;
import org.apache.flink.connector.dynamodb.source.reader.DynamoDbStreamsSourceReader;
import org.apache.flink.connector.dynamodb.source.reader.PollingDynamoDbStreamsShardSplitReader;
import org.apache.flink.connector.dynamodb.source.serialization.DynamoDbStreamsDeserializationSchema;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.utils.AttributeMap;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MAX_DELAY;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MIN_DELAY;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_GET_RECORDS_BLOCKING;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_EMPTY_POLLS;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_NON_EMPTY_POLLS;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_GET_RECORDS_MAX_IN_FLIGHT;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.DYNAMODB_STREAMS_RETRY_COUNT;

/**
 * The {@link DynamoDbStreamsSource} is an exactly-once parallel streaming data source that
 * subscribes to a single AWS DynamoDb stream. It is able to handle resharding of streams, and
 * stores its current progress in Flink checkpoints. The source will read in data from the DynamoDb
 * stream, deserialize it using the provided {@link DeserializationSchema}, and emit the record into
 * the Flink job graph.
 *
 * <p>Exactly-once semantics. To leverage Flink's checkpointing mechanics for exactly-once stream
 * processing, the DynamoDb Stream Source is implemented with the AWS Java SDK, instead of the
 * officially recommended AWS DynamoDb Stream Client Library. The source will store its current
 * progress in Flink checkpoint/savepoint, and will pick up from where it left off upon restore from
 * the checkpoint/savepoint.
 *
 * <p>Initial starting points. The DynamoDb Stream Source supports reads starting from TRIM_HORIZON,
 * LATEST, and AT_TIMESTAMP.
 *
 * @param <T> the data type emitted by the source
 */
@Experimental
public class DynamoDbStreamsSource<T>
        implements Source<T, DynamoDbStreamsShardSplit, DynamoDbStreamsSourceEnumeratorState> {

    protected final String streamArn;
    protected final Configuration sourceConfig;
    protected final DynamoDbStreamsDeserializationSchema<T> deserializationSchema;
    protected final DynamoDbStreamsShardAssigner dynamoDbStreamsShardAssigner;

    /**
     * The AT_TIMESTAMP initial timestamp, resolved and validated once when the source is built, so
     * the enumerator uses exactly the value that was validated. Null for other initial positions.
     */
    @Nullable private final Instant initialTimestamp;

    DynamoDbStreamsSource(
            String streamArn,
            Configuration sourceConfig,
            DynamoDbStreamsDeserializationSchema<T> deserializationSchema,
            DynamoDbStreamsShardAssigner dynamoDbStreamsShardAssigner) {
        Preconditions.checkNotNull(streamArn);
        Preconditions.checkArgument(!streamArn.isEmpty(), "stream ARN cannot be empty string");
        Preconditions.checkNotNull(sourceConfig);
        Preconditions.checkNotNull(deserializationSchema);
        Preconditions.checkNotNull(dynamoDbStreamsShardAssigner);
        this.initialTimestamp =
                DynamodbStreamsSourceConfigUtil.validateStreamSourceConfiguration(sourceConfig);
        this.streamArn = streamArn;
        this.sourceConfig = sourceConfig;
        this.deserializationSchema = deserializationSchema;
        this.dynamoDbStreamsShardAssigner = dynamoDbStreamsShardAssigner;
    }

    /**
     * Create a {@link DynamoDbStreamsSourceBuilder} to allow the fluent construction of a new
     * {@link DynamoDbStreamsSource}.
     *
     * @param <T> type of records being read
     * @return {@link DynamoDbStreamsSourceBuilder}
     */
    public static <T> DynamoDbStreamsSourceBuilder<T> builder() {
        return new DynamoDbStreamsSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, DynamoDbStreamsShardSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        setUpDeserializationSchema(readerContext);

        Map<String, DynamoDbStreamsShardMetrics> shardMetricGroupMap = new ConcurrentHashMap<>();
        // Delay between calling GetRecords API when last poll returned some records. Default - 250
        // millis
        final Duration getRecordsIdlePollingTimeBetweenNonEmptyPolls =
                sourceConfig.get(DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_NON_EMPTY_POLLS);

        // Delay between calling GetRecords API when last poll returned no records. Default - 1
        // second
        final Duration getRecordsIdlePollingTimeBetweenEmptyPolls =
                sourceConfig.get(DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_EMPTY_POLLS);

        // If true, the split reader calls GetRecords on one shard at a time and waits for the
        // response. Default - false
        final boolean blockingGetRecords = sourceConfig.get(DYNAMODB_STREAMS_GET_RECORDS_BLOCKING);

        // Maximum number of non-blocking calls in progress per source reader. Default - 100
        final int maxCallsInFlight = sourceConfig.get(DYNAMODB_STREAMS_GET_RECORDS_MAX_IN_FLIGHT);

        Map<String, List<Shard>> childShardMap = new ConcurrentHashMap<>();
        // All split readers of this source reader share one stream proxy. The fetcher manager
        // creates a new split reader each time it starts a fetcher (for example, after all splits
        // of the previous fetcher finished), and a proxy per split reader would create a new AWS
        // SDK client, HTTP client and, for non-blocking calls, Netty event loop each time. The
        // proxy is created with the first split reader, so that a source reader without splits
        // opens no clients. The source reader closes the proxy.
        //
        RetryStrategy retryStrategy =
                blockingGetRecords
                        ? SdkDefaultRetryStrategy.defaultRetryStrategy()
                        : nonBlockingRetryStrategy(RetryMode.defaultRetryMode());
        LazyStreamProxy streamProxy =
                new LazyStreamProxy(
                        () ->
                                createDynamoDbStreamsProxy(
                                        sourceConfig, retryStrategy, !blockingGetRecords));
        Supplier<PollingDynamoDbStreamsShardSplitReader> splitReaderSupplier =
                () ->
                        new PollingDynamoDbStreamsShardSplitReader(
                                streamProxy.get(),
                                getRecordsIdlePollingTimeBetweenNonEmptyPolls,
                                getRecordsIdlePollingTimeBetweenEmptyPolls,
                                childShardMap,
                                shardMetricGroupMap,
                                blockingGetRecords,
                                maxCallsInFlight);
        DynamoDbStreamsRecordEmitter<T> recordEmitter =
                new DynamoDbStreamsRecordEmitter<>(deserializationSchema);

        return new DynamoDbStreamsSourceReader<>(
                new SingleThreadFetcherManager<>(splitReaderSupplier::get),
                recordEmitter,
                sourceConfig,
                readerContext,
                childShardMap,
                shardMetricGroupMap,
                streamProxy);
    }

    @Override
    public SplitEnumerator<DynamoDbStreamsShardSplit, DynamoDbStreamsSourceEnumeratorState>
            createEnumerator(SplitEnumeratorContext<DynamoDbStreamsShardSplit> enumContext)
                    throws Exception {
        return restoreEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<DynamoDbStreamsShardSplit, DynamoDbStreamsSourceEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<DynamoDbStreamsShardSplit> enumContext,
                    DynamoDbStreamsSourceEnumeratorState checkpoint)
                    throws Exception {
        int maxApiCallAttempts = sourceConfig.get(DYNAMODB_STREAMS_RETRY_COUNT);
        Duration minDelayBetweenRetries =
                sourceConfig.get(DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MIN_DELAY);
        Duration maxDelayBetweenRetries =
                sourceConfig.get(DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MAX_DELAY);
        BackoffStrategy backoffStrategy =
                BackoffStrategy.exponentialDelay(minDelayBetweenRetries, maxDelayBetweenRetries);
        AdaptiveRetryStrategy adaptiveRetryStrategy =
                SdkDefaultRetryStrategy.adaptiveRetryStrategy().toBuilder()
                        .maxAttempts(maxApiCallAttempts)
                        .backoffStrategy(backoffStrategy)
                        .throttlingBackoffStrategy(backoffStrategy)
                        .build();
        return new DynamoDbStreamsSourceEnumerator(
                enumContext,
                streamArn,
                sourceConfig,
                initialTimestamp,
                createDynamoDbStreamsProxy(sourceConfig, adaptiveRetryStrategy, false),
                dynamoDbStreamsShardAssigner,
                checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<DynamoDbStreamsShardSplit> getSplitSerializer() {
        return new DynamoDbStreamsShardSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DynamoDbStreamsSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new DynamoDbStreamsSourceEnumeratorStateSerializer(
                new DynamoDbStreamsShardSplitSerializer());
    }

    /**
     * Creates a proxy with a {@link DynamoDbStreamsClient} for blocking calls or, if {@code
     * nonBlocking} is true, with only a {@link DynamoDbStreamsAsyncClient} for non-blocking calls.
     */
    private DynamoDbStreamsProxy createDynamoDbStreamsProxy(
            Configuration consumerConfig, RetryStrategy retryStrategy, boolean nonBlocking) {
        Properties dynamoDbStreamsClientProperties = new Properties();
        String region =
                AWSGeneralUtil.getRegionFromArn(streamArn)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Unable to determine region from stream arn"));
        dynamoDbStreamsClientProperties.put(AWSConfigConstants.AWS_REGION, region);
        consumerConfig.addAllToProperties(dynamoDbStreamsClientProperties);

        AWSGeneralUtil.validateAwsCredentials(dynamoDbStreamsClientProperties);
        if (nonBlocking) {
            SdkAsyncHttpClient asyncHttpClient = createAsyncHttpClient();
            DynamoDbStreamsAsyncClient dynamoDbStreamsAsyncClient =
                    createAsyncClient(
                            dynamoDbStreamsClientProperties, asyncHttpClient, retryStrategy);
            return new DynamoDbStreamsProxy(
                    null, null, dynamoDbStreamsAsyncClient, asyncHttpClient);
        }

        SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        AttributeMap.builder().build(), ApacheHttpClient.builder());
        DynamoDbStreamsClient dynamoDbStreamsClient =
                AWSClientUtil.createAwsSyncClient(
                        dynamoDbStreamsClientProperties,
                        httpClient,
                        DynamoDbStreamsClient.builder(),
                        ClientOverrideConfiguration.builder().retryStrategy(retryStrategy),
                        DynamodbStreamsSourceConfigConstants
                                .BASE_DDB_STREAMS_USER_AGENT_PREFIX_FORMAT,
                        DynamodbStreamsSourceConfigConstants.DDB_STREAMS_CLIENT_USER_AGENT_PREFIX);
        return new DynamoDbStreamsProxy(dynamoDbStreamsClient, httpClient);
    }

    /**
     * Returns the retry strategy of the configured retry mode, without a circuit breaker. The
     * clients of one proxy share the retry token bucket of the strategy, and with many non-blocking
     * calls in progress, one burst of errors would spend the whole bucket and fail the calls
     * without retries. The adaptive modes cannot turn off the circuit breaker, and their
     * client-side rate limit would slow every shard of the reader when one shard is throttled, so
     * they use the standard mode instead.
     */
    @VisibleForTesting
    static RetryStrategy nonBlockingRetryStrategy(RetryMode retryMode) {
        if (retryMode == RetryMode.LEGACY) {
            return SdkDefaultRetryStrategy.legacyRetryStrategyBuilder()
                    .circuitBreakerEnabled(false)
                    .build();
        }
        return SdkDefaultRetryStrategy.standardRetryStrategyBuilder()
                .circuitBreakerEnabled(false)
                .build();
    }

    /**
     * Creates the Netty HTTP client for non-blocking calls.
     *
     * <p>{@link AWSGeneralUtil#createAsyncHttpClient} uses HTTP/2 by default. DynamoDB Streams
     * endpoints use only HTTP/1.1, and an HTTP/2 client fails with "First received frame was not
     * SETTINGS". A protocol set on the builder has precedence over the defaults of the helper, so
     * the client always uses HTTP/1.1.
     */
    @VisibleForTesting
    static SdkAsyncHttpClient createAsyncHttpClient() {
        return AWSGeneralUtil.createAsyncHttpClient(
                AttributeMap.empty(), NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1));
    }

    /** Creates the client for non-blocking calls, with the same settings as the blocking one. */
    @VisibleForTesting
    static DynamoDbStreamsAsyncClient createAsyncClient(
            Properties clientProperties,
            SdkAsyncHttpClient asyncHttpClient,
            RetryStrategy retryStrategy) {
        return AWSClientUtil.createAwsAsyncClient(
                clientProperties,
                asyncHttpClient,
                DynamoDbStreamsAsyncClient.builder(),
                ClientOverrideConfiguration.builder().retryStrategy(retryStrategy),
                DynamodbStreamsSourceConfigConstants.BASE_DDB_STREAMS_USER_AGENT_PREFIX_FORMAT,
                DynamodbStreamsSourceConfigConstants.DDB_STREAMS_CLIENT_USER_AGENT_PREFIX);
    }

    private void setUpDeserializationSchema(SourceReaderContext sourceReaderContext)
            throws Exception {
        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return sourceReaderContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return sourceReaderContext.getUserCodeClassLoader();
                    }
                });
    }
}
