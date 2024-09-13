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

package org.apache.flink.connector.kinesis.source;

import org.apache.flink.annotation.Experimental;
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
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsConfigConstants;
import org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardAssigner;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumerator;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorState;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisStreamsSourceEnumeratorStateSerializer;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.metrics.KinesisShardMetrics;
import org.apache.flink.connector.kinesis.source.proxy.KinesisAsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.proxy.KinesisStreamProxy;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.reader.KinesisStreamsRecordEmitter;
import org.apache.flink.connector.kinesis.source.reader.KinesisStreamsSourceReader;
import org.apache.flink.connector.kinesis.source.reader.fanout.FanOutKinesisShardSplitReader;
import org.apache.flink.connector.kinesis.source.reader.polling.PollingKinesisShardSplitReader;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.READER_TYPE;

/**
 * The {@link KinesisStreamsSource} is an exactly-once parallel streaming data source that
 * subscribes to a single AWS Kinesis data stream. It is able to handle resharding of streams, and
 * stores its current progress in Flink checkpoints. The source will read in data from the Kinesis
 * Data stream, deserialize it using the provided {@link DeserializationSchema}, and emit the record
 * into the Flink job graph.
 *
 * <p>Exactly-once semantics. To leverage Flink's checkpointing mechanics for exactly-once stream
 * processing, the Kinesis Source is implemented with the AWS Java SDK, instead of the officially
 * recommended AWS Kinesis Client Library. The source will store its current progress in Flink
 * checkpoint/savepoint, and will pick up from where it left off upon restore from the
 * checkpoint/savepoint.
 *
 * <p>Initial starting points. The Kinesis Streams Source supports reads starting from TRIM_HORIZON,
 * LATEST, and AT_TIMESTAMP.
 *
 * @param <T> the data type emitted by the source
 */
@Experimental
public class KinesisStreamsSource<T>
        implements Source<T, KinesisShardSplit, KinesisStreamsSourceEnumeratorState> {

    private final String streamArn;
    private final Configuration sourceConfig;
    private final KinesisDeserializationSchema<T> deserializationSchema;
    private final KinesisShardAssigner kinesisShardAssigner;
    private final boolean preserveShardOrder;

    KinesisStreamsSource(
            String streamArn,
            Configuration sourceConfig,
            KinesisDeserializationSchema<T> deserializationSchema,
            KinesisShardAssigner kinesisShardAssigner,
            boolean preserveShardOrder) {
        Preconditions.checkNotNull(streamArn);
        Preconditions.checkArgument(!streamArn.isEmpty(), "stream ARN cannot be empty string");
        Preconditions.checkNotNull(sourceConfig);
        Preconditions.checkNotNull(deserializationSchema);
        Preconditions.checkNotNull(kinesisShardAssigner);
        this.streamArn = streamArn;
        this.sourceConfig = sourceConfig;
        this.deserializationSchema = deserializationSchema;
        this.kinesisShardAssigner = kinesisShardAssigner;
        this.preserveShardOrder = preserveShardOrder;
    }

    /**
     * Create a {@link KinesisStreamsSourceBuilder} to allow the fluent construction of a new {@link
     * KinesisStreamsSource}.
     *
     * @param <T> type of records being read
     * @return {@link KinesisStreamsSourceBuilder}
     */
    public static <T> KinesisStreamsSourceBuilder<T> builder() {
        return new KinesisStreamsSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, KinesisShardSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        setUpDeserializationSchema(readerContext);
        Map<String, KinesisShardMetrics> shardMetricGroupMap = new ConcurrentHashMap<>();
        KinesisStreamsRecordEmitter<T> recordEmitter =
                new KinesisStreamsRecordEmitter<>(deserializationSchema);
        return new KinesisStreamsSourceReader<>(
                new SingleThreadFetcherManager<>(getKinesisShardSplitReaderSupplier(sourceConfig, shardMetricGroupMap)),
                recordEmitter,
                sourceConfig,
                readerContext,
                shardMetricGroupMap);
    }

    @Override
    public SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<KinesisShardSplit> enumContext) throws Exception {
        return restoreEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<KinesisShardSplit, KinesisStreamsSourceEnumeratorState>
            restoreEnumerator(
                    SplitEnumeratorContext<KinesisShardSplit> enumContext,
                    KinesisStreamsSourceEnumeratorState checkpoint)
                    throws Exception {
        return new KinesisStreamsSourceEnumerator(
                enumContext,
                streamArn,
                sourceConfig,
                createKinesisStreamProxy(sourceConfig),
                kinesisShardAssigner,
                checkpoint,
                preserveShardOrder);
    }

    @Override
    public SimpleVersionedSerializer<KinesisShardSplit> getSplitSerializer() {
        return new KinesisShardSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KinesisStreamsSourceEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new KinesisStreamsSourceEnumeratorStateSerializer(new KinesisShardSplitSerializer());
    }

    private Supplier<SplitReader<Record, KinesisShardSplit>> getKinesisShardSplitReaderSupplier(
            Configuration sourceConfig,
            Map<String, KinesisShardMetrics> shardMetricGroupMap) {
        KinesisStreamsSourceConfigConstants.ReaderType readerType = sourceConfig.get(READER_TYPE);
        switch (readerType) {
            case POLLING:
                return getPollingKinesisShardSplitReaderSupplier(sourceConfig, shardMetricGroupMap);
            case EFO:
                return getFanOutKinesisShardSplitReaderSupplier(sourceConfig, shardMetricGroupMap);
            default:
                throw new IllegalArgumentException("Unsupported reader type: " + readerType);
        }
    }

    private Supplier<SplitReader<Record, KinesisShardSplit>> getPollingKinesisShardSplitReaderSupplier(
            Configuration sourceConfig,
            Map<String, KinesisShardMetrics> shardMetricGroupMap) {
        // We create a new stream proxy for each split reader since they have their own independent lifecycle.
        return () -> new PollingKinesisShardSplitReader(
                createKinesisStreamProxy(sourceConfig), shardMetricGroupMap);
    }

    private Supplier<SplitReader<Record, KinesisShardSplit>> getFanOutKinesisShardSplitReaderSupplier(
            Configuration sourceConfig,
            Map<String, KinesisShardMetrics> shardMetricGroupMap) {
        String consumerArn = getConsumerArn(streamArn, sourceConfig.get(EFO_CONSUMER_NAME));

        // We create a new stream proxy for each split reader since they have their own independent lifecycle.
        return () -> new FanOutKinesisShardSplitReader(createKinesisAsyncStreamProxy(sourceConfig), consumerArn, shardMetricGroupMap);
    }

    private String getConsumerArn(final String streamArn, final String consumerName) {
        try (StreamProxy streamProxy = createKinesisStreamProxy(sourceConfig)) {
            DescribeStreamConsumerResponse response = streamProxy.describeStreamConsumer(streamArn, consumerName);
            return response.consumerDescription().consumerARN();
        } catch (IOException e) {
            throw new KinesisStreamsSourceException("Unable to lookup consumer ARN for stream " + streamArn + " and consumer " + consumerName, e);
        }
    }

    private KinesisAsyncStreamProxy createKinesisAsyncStreamProxy(Configuration consumerConfig) {
        SdkAsyncHttpClient asyncHttpClient = AWSGeneralUtil.createAsyncHttpClient(AttributeMap.builder().build(), NettyNioAsyncHttpClient.builder());
        String region = AWSGeneralUtil.getRegionFromArn(streamArn)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Unable to determine region from stream arn"));
        Properties kinesisClientProperties = new Properties();
        sourceConfig.addAllToProperties(kinesisClientProperties);
        kinesisClientProperties.put(AWSConfigConstants.AWS_REGION, region);

        KinesisAsyncClient kinesisAsyncClient =
                AWSClientUtil.createAwsAsyncClient(kinesisClientProperties,
                        asyncHttpClient,
                        KinesisAsyncClient.builder(),
                        KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                        KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX);
        return new KinesisAsyncStreamProxy(kinesisAsyncClient, asyncHttpClient);
    }

    private KinesisStreamProxy createKinesisStreamProxy(Configuration consumerConfig) {
        SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        AttributeMap.builder().build(), ApacheHttpClient.builder());

        String region =
                AWSGeneralUtil.getRegionFromArn(streamArn)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Unable to determine region from stream arn"));
        Properties kinesisClientProperties = new Properties();
        consumerConfig.addAllToProperties(kinesisClientProperties);
        kinesisClientProperties.put(AWSConfigConstants.AWS_REGION, region);

        AWSGeneralUtil.validateAwsCredentials(kinesisClientProperties);
        KinesisClient kinesisClient =
                AWSClientUtil.createAwsSyncClient(
                        kinesisClientProperties,
                        httpClient,
                        KinesisClient.builder(),
                        KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                        KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX);
        return new KinesisStreamProxy(kinesisClient, httpClient);
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
