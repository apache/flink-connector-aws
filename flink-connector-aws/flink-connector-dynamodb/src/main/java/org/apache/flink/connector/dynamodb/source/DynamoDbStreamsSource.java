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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsShardAssigner;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumerator;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumeratorState;
import org.apache.flink.connector.dynamodb.source.enumerator.DynamoDbStreamsSourceEnumeratorStateSerializer;
import org.apache.flink.connector.dynamodb.source.proxy.DynamoDbStreamsProxy;
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

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.util.Properties;
import java.util.function.Supplier;

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

        FutureCompletingBlockingQueue<RecordsWithSplitIds<Record>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        // We create a new stream proxy for each split reader since they have their own independent
        // lifecycle.
        Supplier<PollingDynamoDbStreamsShardSplitReader> splitReaderSupplier =
                () ->
                        new PollingDynamoDbStreamsShardSplitReader(
                                createDynamoDbStreamsProxy(sourceConfig));
        DynamoDbStreamsRecordEmitter<T> recordEmitter =
                new DynamoDbStreamsRecordEmitter<>(deserializationSchema);

        return new DynamoDbStreamsSourceReader<>(
                elementsQueue,
                new SingleThreadFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                sourceConfig,
                readerContext);
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
        return new DynamoDbStreamsSourceEnumerator(
                enumContext,
                streamArn,
                sourceConfig,
                createDynamoDbStreamsProxy(sourceConfig),
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

    private DynamoDbStreamsProxy createDynamoDbStreamsProxy(Configuration consumerConfig) {
        SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        AttributeMap.builder().build(), ApacheHttpClient.builder());

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
        DynamoDbStreamsClient dynamoDbStreamsClient =
                AWSClientUtil.createAwsSyncClient(
                        dynamoDbStreamsClientProperties,
                        httpClient,
                        DynamoDbStreamsClient.builder(),
                        DynamodbStreamsSourceConfigConstants
                                .BASE_DDB_STREAMS_USER_AGENT_PREFIX_FORMAT,
                        DynamodbStreamsSourceConfigConstants.DDB_STREAMS_CLIENT_USER_AGENT_PREFIX);
        return new DynamoDbStreamsProxy(dynamoDbStreamsClient, httpClient);
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
