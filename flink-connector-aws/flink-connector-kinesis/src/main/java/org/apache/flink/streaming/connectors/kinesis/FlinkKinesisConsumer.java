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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata.EquivalenceWrapper;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.table.DefaultShardAssignerFactory;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisStateUtil;
import org.apache.flink.streaming.connectors.kinesis.util.StreamConsumerRegistrarUtil;
import org.apache.flink.streaming.connectors.kinesis.util.WatermarkTracker;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The Flink Kinesis Consumer is an exactly-once parallel streaming data source that subscribes to
 * multiple AWS Kinesis streams within the same AWS service region, and can handle resharding of
 * streams. Each subtask of the consumer is responsible for fetching data records from multiple
 * Kinesis shards. The number of shards fetched by each subtask will change as shards are closed and
 * created by Kinesis.
 *
 * <p>To leverage Flink's checkpointing mechanics for exactly-once streaming processing guarantees,
 * the Flink Kinesis consumer is implemented with the AWS Java SDK, instead of the officially
 * recommended AWS Kinesis Client Library, for low-level control on the management of stream state.
 * The Flink Kinesis Connector also supports setting the initial starting points of Kinesis streams,
 * namely TRIM_HORIZON and LATEST.
 *
 * <p>Kinesis and the Flink consumer support dynamic re-sharding and shard IDs, while sequential,
 * cannot be assumed to be consecutive. There is no perfect generic default assignment function.
 * Default shard to subtask assignment, which is based on hash code, may result in skew, with some
 * subtasks having many shards assigned and others none.
 *
 * <p>It is recommended to monitor the shard distribution and adjust assignment appropriately. A
 * custom assigner implementation can be set via {@link #setShardAssigner(KinesisShardAssigner)} to
 * optimize the hash function or use static overrides to limit skew.
 *
 * <p>In order for the consumer to emit watermarks, a timestamp assigner needs to be set via {@link
 * #setPeriodicWatermarkAssigner(AssignerWithPeriodicWatermarks)} and the auto watermark emit
 * interval configured via {@link
 * org.apache.flink.api.common.ExecutionConfig#setAutoWatermarkInterval(long)}.
 *
 * <p>Watermarks can only advance when all shards of a subtask continuously deliver records. To
 * avoid an inactive or closed shard to block the watermark progress, the idle timeout should be
 * configured via configuration property {@link ConsumerConfigConstants#SHARD_IDLE_INTERVAL_MILLIS}.
 * By default, shards won't be considered idle and watermark calculation will wait for newer records
 * to arrive from all shards.
 *
 * <p>Note that re-sharding of the Kinesis stream while an application (that relies on the Kinesis
 * records for watermarking) is running can lead to incorrect late events. This depends on how
 * shards are assigned to subtasks and applies regardless of whether watermarks are generated in the
 * source or a downstream operator.
 *
 * @param <T> the type of data emitted
 */
@PublicEvolving
public class FlinkKinesisConsumer<T> extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T>, CheckpointedFunction {

    private static final long serialVersionUID = 4724006128720664870L;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisConsumer.class);

    // ------------------------------------------------------------------------
    //  Consumer properties
    // ------------------------------------------------------------------------

    /** The names of the Kinesis streams that we will be consuming from. */
    private final List<String> streams;

    /**
     * Properties to parametrize settings such as AWS service region, initial position in stream,
     * shard list retrieval behaviours, etc.
     */
    private final Properties configProps;

    /** User supplied deserialization schema to convert Kinesis byte messages to Flink objects. */
    private final KinesisDeserializationSchema<T> deserializer;

    /** The function that determines which subtask a shard should be assigned to. */
    private KinesisShardAssigner shardAssigner =
            new DefaultShardAssignerFactory().getShardAssigner();

    private AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;
    private WatermarkTracker watermarkTracker;

    // ------------------------------------------------------------------------
    //  Runtime state
    // ------------------------------------------------------------------------

    /**
     * Per-task fetcher for Kinesis data records, where each fetcher pulls data from one or more
     * Kinesis shards.
     */
    private transient KinesisDataFetcher<T> fetcher;

    /** The sequence numbers to restore to upon restore from failure. */
    private transient HashMap<StreamShardMetadata.EquivalenceWrapper, SequenceNumber>
            sequenceNumsToRestore;

    /**
     * The streams present in the {@link #sequenceNumsToRestore} map, which means they were consumed
     * by the application previously, so we know where to consume from.
     */
    private transient Set<String> knownStreams;

    /**
     * Flag used to control reading from Kinesis: source will read data while value is true. Changed
     * to false after {@link #cancel()} has been called.
     */
    private volatile boolean running = true;
    /**
     * Flag identifying that operator had been closed. True only after {@link #close()} has been
     * called. Used to control behaviour of snapshotState: state can be persisted after operator has
     * been cancelled (during stop-with-savepoint workflow), but not after operator has been closed.
     */
    private volatile boolean closed = false;

    // ------------------------------------------------------------------------
    //  State for Checkpoint
    // ------------------------------------------------------------------------

    /** State name to access shard sequence number states; cannot be changed. */
    private static final String sequenceNumsStateStoreName = "Kinesis-Stream-Shard-State";

    private transient ListState<Tuple2<StreamShardMetadata, SequenceNumber>>
            sequenceNumsStateForCheckpoint;

    // ------------------------------------------------------------------------
    //  Constructors
    // ------------------------------------------------------------------------

    /**
     * Creates a new Flink Kinesis Consumer.
     *
     * <p>The AWS credentials to be used, AWS region of the Kinesis streams, initial position to
     * start streaming from are configured with a {@link Properties} instance.
     *
     * @param stream The single AWS Kinesis stream to read from.
     * @param deserializer The deserializer used to convert raw bytes of Kinesis records to Java
     *     objects (without key).
     * @param configProps The properties used to configure AWS credentials, AWS region, and initial
     *     starting position.
     */
    public FlinkKinesisConsumer(
            String stream, DeserializationSchema<T> deserializer, Properties configProps) {
        this(stream, new KinesisDeserializationSchemaWrapper<>(deserializer), configProps);
    }

    /**
     * Creates a new Flink Kinesis Consumer.
     *
     * <p>The AWS credentials to be used, AWS region of the Kinesis streams, initial position to
     * start streaming from are configured with a {@link Properties} instance.
     *
     * @param stream The single AWS Kinesis stream to read from.
     * @param deserializer The keyed deserializer used to convert raw bytes of Kinesis records to
     *     Java objects.
     * @param configProps The properties used to configure AWS credentials, AWS region, and initial
     *     starting position.
     */
    public FlinkKinesisConsumer(
            String stream, KinesisDeserializationSchema<T> deserializer, Properties configProps) {
        this(Collections.singletonList(stream), deserializer, configProps);
    }

    /**
     * Creates a new Flink Kinesis Consumer.
     *
     * <p>The AWS credentials to be used, AWS region of the Kinesis streams, initial position to
     * start streaming from are configured with a {@link Properties} instance.
     *
     * @param streams The AWS Kinesis streams to read from.
     * @param deserializer The keyed deserializer used to convert raw bytes of Kinesis records to
     *     Java objects.
     * @param configProps The properties used to configure AWS credentials, AWS region, and initial
     *     starting position.
     */
    public FlinkKinesisConsumer(
            List<String> streams,
            KinesisDeserializationSchema<T> deserializer,
            Properties configProps) {
        checkNotNull(streams, "streams can not be null");
        checkArgument(streams.size() != 0, "must be consuming at least 1 stream");
        checkArgument(!streams.contains(""), "stream names cannot be empty Strings");
        this.streams = streams;

        this.configProps = checkNotNull(configProps, "configProps can not be null");

        // check the configuration properties for any conflicting settings
        KinesisConfigUtil.validateConsumerConfiguration(this.configProps, streams);

        checkNotNull(deserializer, "deserializer can not be null");
        checkArgument(
                InstantiationUtil.isSerializable(deserializer),
                "The provided deserialization schema is not serializable: "
                        + deserializer.getClass().getName()
                        + ". "
                        + "Please check that it does not contain references to non-serializable instances.");
        this.deserializer = deserializer;

        StreamConsumerRegistrarUtil.eagerlyRegisterStreamConsumers(configProps, streams);

        if (LOG.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (String stream : streams) {
                sb.append(stream).append(", ");
            }
            LOG.info(
                    "Flink Kinesis Consumer is going to read the following streams: {}",
                    sb.toString());
        }
    }

    public KinesisShardAssigner getShardAssigner() {
        return shardAssigner;
    }

    /**
     * Provide a custom assigner to influence how shards are distributed over subtasks.
     *
     * @param shardAssigner shard assigner
     */
    public void setShardAssigner(KinesisShardAssigner shardAssigner) {
        this.shardAssigner = checkNotNull(shardAssigner, "function can not be null");
        ClosureCleaner.clean(shardAssigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    public AssignerWithPeriodicWatermarks<T> getPeriodicWatermarkAssigner() {
        return periodicWatermarkAssigner;
    }

    /**
     * Set the assigner that will extract the timestamp from {@link T} and calculate the watermark.
     *
     * @param periodicWatermarkAssigner periodic watermark assigner
     */
    public void setPeriodicWatermarkAssigner(
            AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner) {
        this.periodicWatermarkAssigner = periodicWatermarkAssigner;
        ClosureCleaner.clean(
                this.periodicWatermarkAssigner,
                ExecutionConfig.ClosureCleanerLevel.RECURSIVE,
                true);
    }

    public WatermarkTracker getWatermarkTracker() {
        return this.watermarkTracker;
    }

    /**
     * Set the global watermark tracker. When set, it will be used by the fetcher to align the shard
     * consumers by event time.
     *
     * @param watermarkTracker
     */
    public void setWatermarkTracker(WatermarkTracker watermarkTracker) {
        this.watermarkTracker = watermarkTracker;
        ClosureCleaner.clean(
                this.watermarkTracker, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
    }

    // ------------------------------------------------------------------------
    //  Source life cycle
    // ------------------------------------------------------------------------

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

        // all subtasks will run a fetcher, regardless of whether or not the subtask will initially
        // have
        // shards to subscribe to; fetchers will continuously poll for changes in the shard list, so
        // all subtasks
        // can potentially have new shards to subscribe to later on
        KinesisDataFetcher<T> fetcher =
                createFetcher(
                        streams, sourceContext, getRuntimeContext(), configProps, deserializer);

        // initial discovery
        List<StreamShardHandle> allShards = fetcher.discoverNewShardsToSubscribe();

        boolean applyStreamInitialPositionForNewStreams =
                getApplyStreamInitialPositionForNewStreamsFlag();

        Set<String> streamsToForceInitialPositionIn = getStreamsToForceInitialPositionIn();

        for (StreamShardHandle shard : allShards) {
            StreamShardMetadata.EquivalenceWrapper kinesisStreamShard =
                    new StreamShardMetadata.EquivalenceWrapper(
                            KinesisDataFetcher.convertToStreamShardMetadata(shard));
            String stream = shard.getStreamName();

            if (sequenceNumsToRestore == null
                    || sequenceNumsToRestore.isEmpty()
                    || streamsToForceInitialPositionIn.contains(stream)) {
                // we're starting fresh (either for the whole consumer or for this stream);
                // use the configured start position as initial state
                registerFromInitialPosition(fetcher, shard, kinesisStreamShard);
            } else {
                if (sequenceNumsToRestore.containsKey(kinesisStreamShard)) {
                    // if the shard was already seen and is contained in the state,
                    // just use the sequence number stored in the state
                    registerFromState(fetcher, shard, kinesisStreamShard);
                } else {
                    // it's either a new shard for a stream that was already seen or a new stream
                    if (knownStreams.contains(stream)) {
                        // the shard wasn't discovered in the previous run, therefore should be
                        // consumed
                        // from the beginning OR this is a new stream we haven't seen yet, and the
                        // applyStreamInitialPositionForNewStreams flag is false
                        registerFromBeginning(fetcher, shard, kinesisStreamShard);
                    } else {
                        // it's a new stream
                        if (applyStreamInitialPositionForNewStreams) {
                            // the flag is true, so we respect the initial position for the new
                            // stream
                            registerFromInitialPosition(fetcher, shard, kinesisStreamShard);
                        } else {
                            // the flag is false, so we continue existing behaviour of registering
                            // from the beginning
                            registerFromBeginning(fetcher, shard, kinesisStreamShard);
                        }
                    }
                }
            }
        }

        // check that we are running before starting the fetcher
        if (!running) {
            return;
        }

        // expose the fetcher from this point, so that state
        // snapshots can be taken from the fetcher's state holders
        this.fetcher = fetcher;

        // start the fetcher loop. The fetcher will stop running only when cancel() or
        // close() is called, or an error is thrown by threads created by the fetcher
        fetcher.runFetcher();

        // check that the fetcher has terminated before fully closing
        fetcher.awaitTermination();
        sourceContext.close();
    }

    private Set<String> getStreamsToForceInitialPositionIn() {
        String streamsToForceInitialPositionInStr =
                configProps.getProperty(
                        ConsumerConfigConstants.STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO);

        if (streamsToForceInitialPositionInStr == null) {
            return Collections.emptySet();
        }

        return Arrays.stream(streamsToForceInitialPositionInStr.split(","))
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    private Boolean getApplyStreamInitialPositionForNewStreamsFlag() {
        return Optional.ofNullable(
                        configProps.getProperty(
                                ConsumerConfigConstants
                                        .APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS))
                .map(Boolean::parseBoolean)
                .orElse(
                        ConsumerConfigConstants
                                .DEFAULT_APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS);
    }

    private void registerFromBeginning(
            KinesisDataFetcher<T> fetcher,
            StreamShardHandle shard,
            EquivalenceWrapper kinesisStreamShard) {
        fetcher.registerNewSubscribedShardState(
                new KinesisStreamShardState(
                        kinesisStreamShard.getShardMetadata(),
                        shard,
                        SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get()));

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Subtask {} is seeding the fetcher with new discovered shard {},"
                            + " starting state set to the SENTINEL_EARLIEST_SEQUENCE_NUM",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    shard.toString());
        }
    }

    private void registerFromInitialPosition(
            KinesisDataFetcher<T> fetcher,
            StreamShardHandle shard,
            EquivalenceWrapper kinesisStreamShard) {
        SentinelSequenceNumber startingSeqNum =
                InitialPosition.valueOf(
                                configProps.getProperty(
                                        ConsumerConfigConstants.STREAM_INITIAL_POSITION,
                                        ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION))
                        .toSentinelSequenceNumber();

        fetcher.registerNewSubscribedShardState(
                new KinesisStreamShardState(
                        kinesisStreamShard.getShardMetadata(), shard, startingSeqNum.get()));

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Subtask {} will be seeded with initial shard {}, starting state set as sequence number {}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    shard.toString(),
                    startingSeqNum.get());
        }
    }

    private void registerFromState(
            KinesisDataFetcher<T> fetcher,
            StreamShardHandle shard,
            EquivalenceWrapper kinesisStreamShard) {
        fetcher.registerNewSubscribedShardState(
                new KinesisStreamShardState(
                        kinesisStreamShard.getShardMetadata(),
                        shard,
                        sequenceNumsToRestore.get(kinesisStreamShard)));

        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Subtask {} is seeding the fetcher with restored shard {},"
                            + " starting state set to the restored sequence number {}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    shard.toString(),
                    sequenceNumsToRestore.get(kinesisStreamShard));
        }
    }

    @Override
    public void cancel() {
        running = false;

        KinesisDataFetcher fetcher = this.fetcher;

        // this method might be called before the subtask actually starts running,
        // so we must check if the fetcher is actually created
        if (fetcher != null) {
            try {
                // interrupt the fetcher of any work
                fetcher.shutdownFetcher();
            } catch (Exception e) {
                LOG.warn("Error while closing Kinesis data fetcher", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        cancel();
        closed = true;
        // safe-guard when the fetcher has been interrupted, make sure to not leak resources
        // application might be stopped before connector subtask has been started
        // so we must check if the fetcher is actually created
        KinesisDataFetcher fetcher = this.fetcher;
        if (fetcher != null) {
            fetcher.awaitTermination();
        }
        this.fetcher = null;
        super.close();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    // ------------------------------------------------------------------------
    //  State Snapshot & Restore
    // ------------------------------------------------------------------------

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        sequenceNumsStateForCheckpoint =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        sequenceNumsStateStoreName,
                                        KinesisStateUtil.createShardsStateSerializer(
                                                getRuntimeContext().getExecutionConfig())));

        if (context.isRestored()) {
            if (sequenceNumsToRestore == null) {
                sequenceNumsToRestore = new HashMap<>();
                knownStreams = new HashSet<>();
                for (Tuple2<StreamShardMetadata, SequenceNumber> kinesisSequenceNumber :
                        sequenceNumsStateForCheckpoint.get()) {
                    StreamShardMetadata streamShardMetadata = kinesisSequenceNumber.f0;
                    sequenceNumsToRestore.put(
                            // we wrap the restored metadata inside an equivalence wrapper that
                            // checks only stream name and shard id,
                            // so that if a shard had been closed (due to a Kinesis reshard
                            // operation, for example) since
                            // the savepoint and has a different metadata than what we last stored,
                            // we will still be able to match it in sequenceNumsToRestore. Please
                            // see FLINK-8484 for details.
                            new StreamShardMetadata.EquivalenceWrapper(streamShardMetadata),
                            kinesisSequenceNumber.f1);
                    knownStreams.add(streamShardMetadata.getStreamName());
                }

                LOG.info(
                        "Setting restore state in the FlinkKinesisConsumer. Using the following offsets: {}",
                        sequenceNumsToRestore);
            }
        } else {
            LOG.info("No restore state for FlinkKinesisConsumer.");
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (closed) {
            LOG.debug("snapshotState() called on closed source; returning null.");
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Snapshotting state ...");
            }

            sequenceNumsStateForCheckpoint.clear();

            if (fetcher == null) {
                if (sequenceNumsToRestore != null) {
                    for (Map.Entry<StreamShardMetadata.EquivalenceWrapper, SequenceNumber> entry :
                            sequenceNumsToRestore.entrySet()) {
                        // sequenceNumsToRestore is the restored global union state;
                        // should only snapshot shards that actually belong to us
                        int hashCode =
                                shardAssigner.assign(
                                        KinesisDataFetcher.convertToStreamShardHandle(
                                                entry.getKey().getShardMetadata()),
                                        getRuntimeContext().getNumberOfParallelSubtasks());
                        if (KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(
                                hashCode,
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                getRuntimeContext().getIndexOfThisSubtask())) {

                            sequenceNumsStateForCheckpoint.add(
                                    Tuple2.of(entry.getKey().getShardMetadata(), entry.getValue()));
                        }
                    }
                }
            } else {
                HashMap<StreamShardMetadata, SequenceNumber> lastStateSnapshot =
                        fetcher.snapshotState();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Snapshotted state, last processed sequence numbers: {}, checkpoint id: {}, timestamp: {}",
                            lastStateSnapshot,
                            context.getCheckpointId(),
                            context.getCheckpointTimestamp());
                }

                for (Map.Entry<StreamShardMetadata, SequenceNumber> entry :
                        lastStateSnapshot.entrySet()) {
                    sequenceNumsStateForCheckpoint.add(Tuple2.of(entry.getKey(), entry.getValue()));
                }
            }
        }
    }

    /**
     * This method is exposed for tests that need to mock the KinesisDataFetcher in the consumer.
     */
    protected KinesisDataFetcher<T> createFetcher(
            List<String> streams,
            SourceFunction.SourceContext<T> sourceContext,
            RuntimeContext runtimeContext,
            Properties configProps,
            KinesisDeserializationSchema<T> deserializationSchema) {

        return new KinesisDataFetcher<>(
                streams,
                sourceContext,
                runtimeContext,
                configProps,
                deserializationSchema,
                shardAssigner,
                periodicWatermarkAssigner,
                watermarkTracker);
    }

    @VisibleForTesting
    HashMap<StreamShardMetadata.EquivalenceWrapper, SequenceNumber> getRestoredState() {
        return sequenceNumsToRestore;
    }
}
