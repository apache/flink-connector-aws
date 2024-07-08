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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionSnapshotContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableFlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisStateUtil;
import org.apache.flink.streaming.connectors.kinesis.util.RecordEmitter;
import org.apache.flink.streaming.connectors.kinesis.util.WatermarkTracker;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.CollectingSourceContext;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.PojoTestUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Suite of FlinkKinesisConsumer tests for the methods called throughout the source life cycle. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FlinkKinesisConsumer.class)
public class FlinkKinesisConsumerTest extends TestLogger {

    // ----------------------------------------------------------------------
    // Tests related to state initialization
    // ----------------------------------------------------------------------

    @Test
    public void testUseRestoredStateForSnapshotIfFetcherNotInitialized() throws Exception {
        Properties config = TestUtils.getStandardProperties();

        List<Tuple2<StreamShardMetadata, SequenceNumber>> globalUnionState =
                Arrays.asList(
                        createShardState("fakeStream", 0, "1"),
                        createShardState("fakeStream", 1, "1"),
                        createShardState("fakeStream", 2, "1"),
                        createShardState("fakeStream", 3, "1"));

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        listState.addAll(globalUnionState);

        FlinkKinesisConsumer<String> consumer =
                new FlinkKinesisConsumer<>("fakeStream", new SimpleStringSchema(), config);
        RuntimeContext context = new MockStreamingRuntimeContext(true, 2, 0);
        consumer.setRuntimeContext(context);

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class)))
                .thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        consumer.initializeState(initializationContext);

        // only opened, not run
        consumer.open(new Configuration());

        // arbitrary checkpoint id and timestamp
        consumer.snapshotState(new StateSnapshotContextSynchronousImpl(123, 123));

        assertThat(listState.isClearCalled()).isTrue();

        // the checkpointed list state should contain only the shards that it should subscribe to
        assertThat(listState.getList()).hasSize(globalUnionState.size() / 2);
        assertThat(listState.getList()).contains(globalUnionState.get(0));
        assertThat(listState.getList()).contains(globalUnionState.get(2));
    }

    @Test
    public void testListStateChangedAfterSnapshotState() throws Exception {

        // ----------------------------------------------------------------------
        // setup config, initial state and expected state snapshot
        // ----------------------------------------------------------------------
        List<Tuple2<StreamShardMetadata, SequenceNumber>> initialState =
                Collections.singletonList(createShardState("fakeStream", 0, "1"));

        List<Tuple2<StreamShardMetadata, SequenceNumber>> expectedStateSnapshot =
                Arrays.asList(
                        createShardState("fakeStream", 0, "12"),
                        createShardState("fakeStream", 1, "11"),
                        createShardState("fakeStream", 2, "31"));

        // ----------------------------------------------------------------------
        // mock operator state backend and initial state for initializeState()
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        listState.addAll(initialState);

        // ----------------------------------------------------------------------
        // mock a running fetcher and its state for snapshot
        // ----------------------------------------------------------------------

        HashMap<StreamShardMetadata, SequenceNumber> stateSnapshot = new HashMap<>();
        for (Tuple2<StreamShardMetadata, SequenceNumber> tuple : expectedStateSnapshot) {
            stateSnapshot.put(tuple.f0, tuple.f1);
        }

        KinesisDataFetcher<String> mockedFetcher = mock(KinesisDataFetcher.class);
        when(mockedFetcher.snapshotState()).thenReturn(stateSnapshot);

        // ----------------------------------------------------------------------
        // create a consumer and test the snapshotState()
        // ----------------------------------------------------------------------

        FlinkKinesisConsumer<String> mockedConsumer =
                prepareMockedConsumer(
                        "fakeStream", new SimpleStringSchema(), mockedFetcher, listState);

        mockedConsumer.snapshotState(mock(FunctionSnapshotContext.class));

        // ----------------------------------------------------------------------
        // verify that state had been updated
        // ----------------------------------------------------------------------
        assertThat(listState.clearCalled).isTrue();
        assertThat(listState.getList())
                .hasSize(3)
                .doesNotContainAnyElementsOf(initialState)
                .containsAll(expectedStateSnapshot);
    }

    @Test
    public void testSnapshotStateChangesAfterCancel() throws Exception {

        // ----------------------------------------------------------------------
        // setup config, initial state and expected state snapshot
        // ----------------------------------------------------------------------
        List<Tuple2<StreamShardMetadata, SequenceNumber>> initialState =
                Collections.singletonList(createShardState("fakeStream", 0, "11"));

        List<Tuple2<StreamShardMetadata, SequenceNumber>> expectedStateSnapshot =
                Collections.singletonList(createShardState("fakeStream", 0, "12"));

        // ----------------------------------------------------------------------
        // mock operator state backend and initial state for initializeState()
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        listState.addAll(initialState);

        // ----------------------------------------------------------------------
        // mock a running fetcher and its state for snapshot
        // ----------------------------------------------------------------------

        HashMap<StreamShardMetadata, SequenceNumber> stateSnapshot = new HashMap<>();
        for (Tuple2<StreamShardMetadata, SequenceNumber> tuple : expectedStateSnapshot) {
            stateSnapshot.put(tuple.f0, tuple.f1);
        }

        KinesisDataFetcher<String> mockedFetcher = mock(KinesisDataFetcher.class);
        when(mockedFetcher.snapshotState()).thenReturn(stateSnapshot);

        // ----------------------------------------------------------------------
        // create a consumer and test the snapshotState()
        // ----------------------------------------------------------------------

        FlinkKinesisConsumer<String> mockedConsumer =
                prepareMockedConsumer(
                        "fakeStream", new SimpleStringSchema(), mockedFetcher, listState);

        mockedConsumer.cancel();
        mockedConsumer.snapshotState(new MockFunctionSnapshotContext(2));
        verify(mockedFetcher, times(1)).snapshotState();

        // ----------------------------------------------------------------------
        // verify that state had been updated
        // ----------------------------------------------------------------------
        assertThat(listState.isClearCalled()).isTrue();
        assertThat(listState.getList())
                .hasSize(1)
                .doesNotContainAnyElementsOf(initialState)
                .containsAll(expectedStateSnapshot);
    }

    @Test
    public void testSnapshotStateNotChangedAfterClose() throws Exception {

        // ----------------------------------------------------------------------
        // setup initial state
        // ----------------------------------------------------------------------

        List<Tuple2<StreamShardMetadata, SequenceNumber>> initialState =
                Collections.singletonList(createShardState("fakeStream", 0, "11"));

        // ----------------------------------------------------------------------
        // mock initial state
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        listState.addAll(initialState);

        // ----------------------------------------------------------------------
        // mock a running fetcher and its state for snapshot
        // ----------------------------------------------------------------------

        KinesisDataFetcher<String> mockedFetcher = mock(KinesisDataFetcher.class);
        when(mockedFetcher.snapshotState()).thenReturn(new HashMap<>());

        // ----------------------------------------------------------------------
        // create a consumer and test the snapshotState()
        // ----------------------------------------------------------------------

        FlinkKinesisConsumer<String> mockedConsumer =
                prepareMockedConsumer(
                        "fakeStream", new SimpleStringSchema(), mockedFetcher, listState);

        mockedConsumer.close();
        mockedConsumer.snapshotState(new MockFunctionSnapshotContext(3));
        verify(mockedFetcher, never()).snapshotState();
        assertThat(listState.isClearCalled()).isFalse();
        assertThat(listState.getList()).containsAll(initialState);
    }

    private <T> FlinkKinesisConsumer<T> prepareMockedConsumer(
            String streamName,
            DeserializationSchema<T> schema,
            KinesisDataFetcher<T> fetcher,
            ListState<?> listState)
            throws Exception {

        Properties config = TestUtils.getStandardProperties();

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class)))
                .thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        FlinkKinesisConsumer<T> consumer = new FlinkKinesisConsumer<>(streamName, schema, config);
        FlinkKinesisConsumer<T> mockedConsumer = spy(consumer);

        RuntimeContext context = new MockStreamingRuntimeContext(true, 1, 0);

        mockedConsumer.setRuntimeContext(context);
        mockedConsumer.initializeState(initializationContext);
        mockedConsumer.open(new Configuration());
        Whitebox.setInternalState(mockedConsumer, "fetcher", fetcher); // mock consumer as running.

        return mockedConsumer;
    }

    /**
     * Before using an explicit TypeSerializer for the state the {@link FlinkKinesisConsumer} was
     * creating a serializer implicitly using a {@link TypeInformation}. After fixing issue
     * FLINK-24943, * serializer is created explicitly. Here, we verify that previous approach is
     * compatible with the new one.
     */
    @Test
    public void testExplicitStateSerializerCompatibility() throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();

        Tuple2<StreamShardMetadata, SequenceNumber> tuple = createShardState("fakeStream", 0, "1");

        // This is how serializer was created implicitly using a TypeInformation
        // and since SequenceNumber is GenericType, Flink falls back to Kryo
        TypeInformation<Tuple2<StreamShardMetadata, SequenceNumber>> originalShardsStateTypeInfo =
                new TupleTypeInfo<>(
                        TypeInformation.of(StreamShardMetadata.class),
                        TypeInformation.of(SequenceNumber.class));
        TypeSerializer<Tuple2<StreamShardMetadata, SequenceNumber>> serializerFromTypeInfo =
                originalShardsStateTypeInfo.createSerializer(executionConfig);
        byte[] bytes = InstantiationUtil.serializeToByteArray(serializerFromTypeInfo, tuple);

        // This is how we create serializer explicitly with Kryo
        TupleSerializer<Tuple2<StreamShardMetadata, SequenceNumber>> serializerFromKryo =
                KinesisStateUtil.createShardsStateSerializer(executionConfig);

        Tuple2<StreamShardMetadata, SequenceNumber> actualTuple =
                InstantiationUtil.deserializeFromByteArray(serializerFromKryo, bytes);

        // Both ways should be the same
        assertThat(tuple)
                .overridingErrorMessage(
                        "Explicit serializer is not compatible with "
                                + "implicit method of creating serializer using TypeInformation.")
                .isEqualTo(actualTuple);
    }

    private Tuple2<StreamShardMetadata, SequenceNumber> createShardState(
            String streamName, int shardNumber, String sequenceNumber) {
        return Tuple2.of(
                KinesisDataFetcher.convertToStreamShardMetadata(
                        new StreamShardHandle(
                                streamName,
                                new Shard()
                                        .withShardId(
                                                KinesisShardIdGenerator.generateFromShardOrder(
                                                        shardNumber)))),
                new SequenceNumber(sequenceNumber));
    }

    // ----------------------------------------------------------------------
    // Tests related to fetcher initialization
    // ----------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testFetcherShouldNotBeRestoringFromFailureIfNotRestoringFromCheckpoint()
            throws Exception {
        KinesisDataFetcher mockedFetcher = mockKinesisDataFetcher();

        // assume the given config is correct
        try (MockedStatic<KinesisConfigUtil> kcu = mockStatic(KinesisConfigUtil.class)) {
            TestableFlinkKinesisConsumer consumer =
                    new TestableFlinkKinesisConsumer("fakeStream", new Properties(), 10, 2);
            consumer.open(new Configuration());
            consumer.run(Mockito.mock(SourceFunction.SourceContext.class));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFetcherShouldBeCorrectlySeededIfRestoringFromCheckpoint() throws Exception {

        // ----------------------------------------------------------------------
        // setup initial state
        // ----------------------------------------------------------------------

        HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = getFakeRestoredStore("all");

        // ----------------------------------------------------------------------
        // mock operator state backend and initial state for initializeState()
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
            listState.add(
                    Tuple2.of(
                            KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()),
                            state.getValue()));
        }

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class)))
                .thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        // ----------------------------------------------------------------------
        // mock fetcher
        // ----------------------------------------------------------------------

        KinesisDataFetcher mockedFetcher = mockKinesisDataFetcher();
        List<StreamShardHandle> shards = new ArrayList<>();
        shards.addAll(fakeRestoredState.keySet());
        when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);

        // assume the given config is correct
        try (MockedStatic<KinesisConfigUtil> kcu = mockStatic(KinesisConfigUtil.class)) {

            // ----------------------------------------------------------------------
            // start to test fetcher's initial state seeding
            // ----------------------------------------------------------------------

            TestableFlinkKinesisConsumer consumer =
                    new TestableFlinkKinesisConsumer("fakeStream", new Properties(), 10, 2);
            consumer.initializeState(initializationContext);
            consumer.open(new Configuration());
            consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

            for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard :
                    fakeRestoredState.entrySet()) {
                Mockito.verify(mockedFetcher)
                        .registerNewSubscribedShardState(
                                new KinesisStreamShardState(
                                        KinesisDataFetcher.convertToStreamShardMetadata(
                                                restoredShard.getKey()),
                                        restoredShard.getKey(),
                                        restoredShard.getValue()));
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFetcherShouldBeCorrectlySeededOnlyItsOwnStates() throws Exception {

        // ----------------------------------------------------------------------
        // setup initial state
        // ----------------------------------------------------------------------

        HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState =
                getFakeRestoredStore("fakeStream1");

        HashMap<StreamShardHandle, SequenceNumber> fakeRestoredStateForOthers =
                getFakeRestoredStore("fakeStream2");

        // ----------------------------------------------------------------------
        // mock operator state backend and initial state for initializeState()
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
            listState.add(
                    Tuple2.of(
                            KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()),
                            state.getValue()));
        }
        for (Map.Entry<StreamShardHandle, SequenceNumber> state :
                fakeRestoredStateForOthers.entrySet()) {
            listState.add(
                    Tuple2.of(
                            KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()),
                            state.getValue()));
        }

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class)))
                .thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        // ----------------------------------------------------------------------
        // mock fetcher
        // ----------------------------------------------------------------------

        KinesisDataFetcher mockedFetcher = mockKinesisDataFetcher();
        List<StreamShardHandle> shards = new ArrayList<>();
        shards.addAll(fakeRestoredState.keySet());
        when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);

        // assume the given config is correct
        try (MockedStatic<KinesisConfigUtil> kcu = mockStatic(KinesisConfigUtil.class)) {

            // ----------------------------------------------------------------------
            // start to test fetcher's initial state seeding
            // ----------------------------------------------------------------------

            TestableFlinkKinesisConsumer consumer =
                    new TestableFlinkKinesisConsumer("fakeStream", new Properties(), 10, 2);
            consumer.initializeState(initializationContext);
            consumer.open(new Configuration());
            consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

            for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard :
                    fakeRestoredStateForOthers.entrySet()) {
                // should never get restored state not belonging to itself
                Mockito.verify(mockedFetcher, never())
                        .registerNewSubscribedShardState(
                                new KinesisStreamShardState(
                                        KinesisDataFetcher.convertToStreamShardMetadata(
                                                restoredShard.getKey()),
                                        restoredShard.getKey(),
                                        restoredShard.getValue()));
            }
            for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard :
                    fakeRestoredState.entrySet()) {
                // should get restored state belonging to itself
                Mockito.verify(mockedFetcher)
                        .registerNewSubscribedShardState(
                                new KinesisStreamShardState(
                                        KinesisDataFetcher.convertToStreamShardMetadata(
                                                restoredShard.getKey()),
                                        restoredShard.getKey(),
                                        restoredShard.getValue()));
            }
        }
    }

    /*
     * This tests that the consumer correctly picks up shards that were not discovered on the previous run.
     *
     * Case under test:
     *
     * If the original parallelism is 2 and states are:
     *   Consumer subtask 1:
     *     stream1, shard1, SequentialNumber(xxx)
     *   Consumer subtask 2:
     *     stream1, shard2, SequentialNumber(yyy)
     *
     * After discoverNewShardsToSubscribe() if there were two shards (shard3, shard4) created:
     *   Consumer subtask 1 (late for discoverNewShardsToSubscribe()):
     *     stream1, shard1, SequentialNumber(xxx)
     *   Consumer subtask 2:
     *     stream1, shard2, SequentialNumber(yyy)
     *     stream1, shard4, SequentialNumber(zzz)
     *
     * If snapshotState() occurs and parallelism is changed to 1:
     *   Union state will be:
     *     stream1, shard1, SequentialNumber(xxx)
     *     stream1, shard2, SequentialNumber(yyy)
     *     stream1, shard4, SequentialNumber(zzz)
     *   Fetcher should be seeded with:
     *     stream1, shard1, SequentialNumber(xxx)
     *     stream1, shard2, SequentialNumber(yyy)
     *     stream1, share3, SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM
     *     stream1, shard4, SequentialNumber(zzz)
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFetcherShouldBeCorrectlySeededWithNewDiscoveredKinesisStreamShard()
            throws Exception {

        // ----------------------------------------------------------------------
        // setup initial state
        // ----------------------------------------------------------------------

        HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = getFakeRestoredStore("all");

        // ----------------------------------------------------------------------
        // mock operator state backend and initial state for initializeState()
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
            listState.add(
                    Tuple2.of(
                            KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()),
                            state.getValue()));
        }

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class)))
                .thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        // ----------------------------------------------------------------------
        // mock fetcher
        // ----------------------------------------------------------------------

        KinesisDataFetcher mockedFetcher = mockKinesisDataFetcher();
        List<StreamShardHandle> shards = new ArrayList<>();
        shards.addAll(fakeRestoredState.keySet());
        shards.add(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard()
                                .withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))));
        when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);

        // assume the given config is correct
        try (MockedStatic<KinesisConfigUtil> kcu = mockStatic(KinesisConfigUtil.class)) {

            // ----------------------------------------------------------------------
            // start to test fetcher's initial state seeding
            // ----------------------------------------------------------------------

            TestableFlinkKinesisConsumer consumer =
                    new TestableFlinkKinesisConsumer("fakeStream", new Properties(), 10, 2);
            consumer.initializeState(initializationContext);
            consumer.open(new Configuration());
            consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

            fakeRestoredState.put(
                    new StreamShardHandle(
                            "fakeStream2",
                            new Shard()
                                    .withShardId(
                                            KinesisShardIdGenerator.generateFromShardOrder(2))),
                    SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());
            for (Map.Entry<StreamShardHandle, SequenceNumber> restoredShard :
                    fakeRestoredState.entrySet()) {
                Mockito.verify(mockedFetcher)
                        .registerNewSubscribedShardState(
                                new KinesisStreamShardState(
                                        KinesisDataFetcher.convertToStreamShardMetadata(
                                                restoredShard.getKey()),
                                        restoredShard.getKey(),
                                        restoredShard.getValue()));
            }
        }
    }

    @Test
    public void testLegacyKinesisStreamShardToStreamShardMetadataConversion() {
        String streamName = "fakeStream1";
        String shardId = "shard-000001";
        String parentShardId = "shard-000002";
        String adjacentParentShardId = "shard-000003";
        String startingHashKey = "key-000001";
        String endingHashKey = "key-000010";
        String startingSequenceNumber = "seq-0000021";
        String endingSequenceNumber = "seq-00000031";

        StreamShardMetadata streamShardMetadata = new StreamShardMetadata();
        streamShardMetadata.setStreamName(streamName);
        streamShardMetadata.setShardId(shardId);
        streamShardMetadata.setParentShardId(parentShardId);
        streamShardMetadata.setAdjacentParentShardId(adjacentParentShardId);
        streamShardMetadata.setStartingHashKey(startingHashKey);
        streamShardMetadata.setEndingHashKey(endingHashKey);
        streamShardMetadata.setStartingSequenceNumber(startingSequenceNumber);
        streamShardMetadata.setEndingSequenceNumber(endingSequenceNumber);

        Shard shard =
                new Shard()
                        .withShardId(shardId)
                        .withParentShardId(parentShardId)
                        .withAdjacentParentShardId(adjacentParentShardId)
                        .withHashKeyRange(
                                new HashKeyRange()
                                        .withStartingHashKey(startingHashKey)
                                        .withEndingHashKey(endingHashKey))
                        .withSequenceNumberRange(
                                new SequenceNumberRange()
                                        .withStartingSequenceNumber(startingSequenceNumber)
                                        .withEndingSequenceNumber(endingSequenceNumber));
        KinesisStreamShard kinesisStreamShard = new KinesisStreamShard(streamName, shard);

        assertThat(KinesisStreamShard.convertToStreamShardMetadata(kinesisStreamShard))
                .isEqualTo(streamShardMetadata);
    }

    @Test
    public void testStreamShardMetadataSerializedUsingPojoSerializer() {
        PojoTestUtils.assertSerializedAsPojo(StreamShardMetadata.class);
    }

    /**
     * FLINK-8484: ensure that a state change in the StreamShardMetadata other than {@link
     * StreamShardMetadata#getShardId()} or {@link StreamShardMetadata#getStreamName()} does not
     * result in the shard not being able to be restored. This handles the corner case where the
     * stored shard metadata is open (no ending sequence number), but after the job restore, the
     * shard has been closed (ending number set) due to re-sharding, and we can no longer rely on
     * {@link StreamShardMetadata#equals(Object)} to find back the sequence number in the collection
     * of restored shard metadata.
     *
     * <p>Therefore, we will rely on synchronizing the snapshot's state with the Kinesis shard
     * before attempting to find back the sequence number to restore.
     */
    @Test
    public void testFindSequenceNumberToRestoreFromIfTheShardHasBeenClosedSinceTheStateWasStored()
            throws Exception {
        // ----------------------------------------------------------------------
        // setup initial state
        // ----------------------------------------------------------------------

        HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = getFakeRestoredStore("all");

        // ----------------------------------------------------------------------
        // mock operator state backend and initial state for initializeState()
        // ----------------------------------------------------------------------

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState =
                new TestingListState<>();
        for (Map.Entry<StreamShardHandle, SequenceNumber> state : fakeRestoredState.entrySet()) {
            listState.add(
                    Tuple2.of(
                            KinesisDataFetcher.convertToStreamShardMetadata(state.getKey()),
                            state.getValue()));
        }

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class)))
                .thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        // ----------------------------------------------------------------------
        // mock fetcher
        // ----------------------------------------------------------------------

        KinesisDataFetcher mockedFetcher = mockKinesisDataFetcher();
        List<StreamShardHandle> shards = new ArrayList<>();

        // create a fake stream shard handle based on the first entry in the restored state
        final StreamShardHandle originalStreamShardHandle =
                fakeRestoredState.keySet().iterator().next();
        final StreamShardHandle closedStreamShardHandle =
                new StreamShardHandle(
                        originalStreamShardHandle.getStreamName(),
                        originalStreamShardHandle.getShard());
        // close the shard handle by setting an ending sequence number
        final SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
        sequenceNumberRange.setEndingSequenceNumber("1293844");
        closedStreamShardHandle.getShard().setSequenceNumberRange(sequenceNumberRange);

        shards.add(closedStreamShardHandle);

        when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shards);

        // assume the given config is correct
        try (MockedStatic<KinesisConfigUtil> kcu = mockStatic(KinesisConfigUtil.class)) {

            // ----------------------------------------------------------------------
            // start to test fetcher's initial state seeding
            // ----------------------------------------------------------------------

            TestableFlinkKinesisConsumer consumer =
                    new TestableFlinkKinesisConsumer("fakeStream", new Properties(), 10, 2);
            consumer.initializeState(initializationContext);
            consumer.open(new Configuration());
            consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

            Mockito.verify(mockedFetcher)
                    .registerNewSubscribedShardState(
                            new KinesisStreamShardState(
                                    KinesisDataFetcher.convertToStreamShardMetadata(
                                            closedStreamShardHandle),
                                    closedStreamShardHandle,
                                    fakeRestoredState.get(closedStreamShardHandle)));
        }
    }

    private static final class TestingListState<T> implements ListState<T> {

        private final List<T> list = new ArrayList<>();
        private boolean clearCalled = false;

        @Override
        public void clear() {
            list.clear();
            clearCalled = true;
        }

        @Override
        public Iterable<T> get() throws Exception {
            return list;
        }

        @Override
        public void add(T value) throws Exception {
            list.add(value);
        }

        public List<T> getList() {
            return list;
        }

        public boolean isClearCalled() {
            return clearCalled;
        }

        @Override
        public void update(List<T> values) throws Exception {
            list.clear();

            addAll(values);
        }

        @Override
        public void addAll(List<T> values) throws Exception {
            if (values != null) {
                list.addAll(values);
            }
        }
    }

    private HashMap<StreamShardHandle, SequenceNumber> getFakeRestoredStore(String streamName) {
        HashMap<StreamShardHandle, SequenceNumber> fakeRestoredState = new HashMap<>();

        if (streamName.equals("fakeStream1") || streamName.equals("all")) {
            fakeRestoredState.put(
                    new StreamShardHandle(
                            "fakeStream1",
                            new Shard()
                                    .withShardId(
                                            KinesisShardIdGenerator.generateFromShardOrder(0))),
                    new SequenceNumber(UUID.randomUUID().toString()));
            fakeRestoredState.put(
                    new StreamShardHandle(
                            "fakeStream1",
                            new Shard()
                                    .withShardId(
                                            KinesisShardIdGenerator.generateFromShardOrder(1))),
                    new SequenceNumber(UUID.randomUUID().toString()));
            fakeRestoredState.put(
                    new StreamShardHandle(
                            "fakeStream1",
                            new Shard()
                                    .withShardId(
                                            KinesisShardIdGenerator.generateFromShardOrder(2))),
                    new SequenceNumber(UUID.randomUUID().toString()));
        }

        if (streamName.equals("fakeStream2") || streamName.equals("all")) {
            fakeRestoredState.put(
                    new StreamShardHandle(
                            "fakeStream2",
                            new Shard()
                                    .withShardId(
                                            KinesisShardIdGenerator.generateFromShardOrder(0))),
                    new SequenceNumber(UUID.randomUUID().toString()));
            fakeRestoredState.put(
                    new StreamShardHandle(
                            "fakeStream2",
                            new Shard()
                                    .withShardId(
                                            KinesisShardIdGenerator.generateFromShardOrder(1))),
                    new SequenceNumber(UUID.randomUUID().toString()));
        }

        return fakeRestoredState;
    }

    private static KinesisDataFetcher mockKinesisDataFetcher() throws Exception {
        KinesisDataFetcher mockedFetcher = Mockito.mock(KinesisDataFetcher.class);

        java.lang.reflect.Constructor<KinesisDataFetcher> ctor =
                (java.lang.reflect.Constructor<KinesisDataFetcher>)
                        KinesisDataFetcher.class.getConstructors()[0];
        Class<?>[] otherParamTypes = new Class<?>[ctor.getParameterCount() - 1];
        System.arraycopy(
                ctor.getParameterTypes(), 1, otherParamTypes, 0, ctor.getParameterCount() - 1);

        Supplier<Object[]> argumentSupplier =
                () -> {
                    Object[] otherParamArgs = new Object[otherParamTypes.length];
                    for (int i = 0; i < otherParamTypes.length; i++) {
                        otherParamArgs[i] = Mockito.nullable(otherParamTypes[i]);
                    }
                    return otherParamArgs;
                };
        PowerMockito.whenNew(ctor)
                .withArguments(Mockito.any(ctor.getParameterTypes()[0]), argumentSupplier.get())
                .thenReturn(mockedFetcher);
        return mockedFetcher;
    }

    @Test
    public void testPeriodicWatermark() throws Exception {
        String streamName = "fakeStreamName";
        Time maxOutOfOrderness = Time.milliseconds(5);
        long autoWatermarkInterval = 1_000;

        HashMap<String, String> subscribedStreamsToLastDiscoveredShardIds = new HashMap<>();
        subscribedStreamsToLastDiscoveredShardIds.put(streamName, null);

        KinesisDeserializationSchema<String> deserializationSchema =
                new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema());
        Properties props = new Properties();
        props.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        props.setProperty(
                ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, Long.toString(10L));

        BlockingQueue<String> shard1 = new LinkedBlockingQueue<>();
        BlockingQueue<String> shard2 = new LinkedBlockingQueue<>();

        Map<String, List<BlockingQueue<String>>> streamToQueueMap = new HashMap<>();
        streamToQueueMap.put(streamName, Arrays.asList(shard1, shard2));

        // override createFetcher to mock Kinesis
        FlinkKinesisConsumer<String> sourceFunc =
                new FlinkKinesisConsumer<String>(streamName, deserializationSchema, props) {
                    @Override
                    protected KinesisDataFetcher<String> createFetcher(
                            List<String> streams,
                            SourceContext<String> sourceContext,
                            RuntimeContext runtimeContext,
                            Properties configProps,
                            KinesisDeserializationSchema<String> deserializationSchema) {

                        KinesisDataFetcher<String> fetcher =
                                new KinesisDataFetcher<String>(
                                        streams,
                                        sourceContext,
                                        sourceContext.getCheckpointLock(),
                                        runtimeContext,
                                        configProps,
                                        deserializationSchema,
                                        getShardAssigner(),
                                        getPeriodicWatermarkAssigner(),
                                        null,
                                        new AtomicReference<>(),
                                        new ArrayList<>(),
                                        subscribedStreamsToLastDiscoveredShardIds,
                                        (props) ->
                                                FakeKinesisBehavioursFactory
                                                        .blockingQueueGetRecords(streamToQueueMap),
                                        null) {};
                        return fetcher;
                    }
                };

        sourceFunc.setShardAssigner(
                (streamShardHandle, i) -> {
                    // shardId-000000000000
                    return Integer.parseInt(
                            streamShardHandle
                                    .getShard()
                                    .getShardId()
                                    .substring("shardId-".length()));
                });

        sourceFunc.setPeriodicWatermarkAssigner(new TestTimestampExtractor(maxOutOfOrderness));

        // there is currently no test harness specifically for sources,
        // so we overlay the source thread here
        AbstractStreamOperatorTestHarness<Object> testHarness =
                new AbstractStreamOperatorTestHarness<Object>(
                        new StreamSource(sourceFunc), 1, 1, 0);
        testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);
        testHarness.getExecutionConfig().setAutoWatermarkInterval(autoWatermarkInterval);

        testHarness.initializeEmptyState();
        testHarness.open();

        ConcurrentLinkedQueue<Watermark> watermarks = new ConcurrentLinkedQueue<>();

        @SuppressWarnings("unchecked")
        SourceFunction.SourceContext<String> sourceContext =
                new CollectingSourceContext(
                        testHarness.getCheckpointLock(), testHarness.getOutput()) {
                    @Override
                    public void emitWatermark(Watermark mark) {
                        watermarks.add(mark);
                    }

                    @Override
                    public void markAsTemporarilyIdle() {}
                };

        new Thread(
                        () -> {
                            try {
                                sourceFunc.run(sourceContext);
                            } catch (InterruptedException e) {
                                // expected on cancel
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .start();

        shard1.put("1");
        shard1.put("2");
        shard2.put("10");
        int recordCount = 3;
        int watermarkCount = 0;
        awaitRecordCount(testHarness.getOutput(), recordCount);

        // Trigger watermark emit, first watermark is -3
        // - Shard-1 @2
        // - Shard-2 @10
        // - Watermark = min(2, 10) - maxOutOfOrderness = 2 - 5 = -3
        testHarness.setProcessingTime(testHarness.getProcessingTime() + autoWatermarkInterval);
        watermarkCount++;

        // advance watermark
        shard1.put("10");
        recordCount++;
        awaitRecordCount(testHarness.getOutput(), recordCount);

        // Trigger watermark emit, second watermark is -3
        // - Shard-1 @10
        // - Shard-2 @10
        // - Watermark = min(10, 10) - maxOutOfOrderness = 10 - 5 = 5
        testHarness.setProcessingTime(testHarness.getProcessingTime() + autoWatermarkInterval);
        watermarkCount++;

        sourceFunc.cancel();
        testHarness.close();

        assertThat(testHarness.getOutput()).as("record count").hasSize(recordCount);
        assertThat(watermarks).contains(new Watermark(-3), new Watermark(5));
        assertThat(watermarks).as("watermark count").hasSize(watermarkCount);
    }

    @Test
    public void testSourceSynchronization() throws Exception {

        final String streamName = "fakeStreamName";
        final Time maxOutOfOrderness = Time.milliseconds(5);
        final long autoWatermarkInterval = 1_000;
        final long watermarkSyncInterval = autoWatermarkInterval + 1;

        TestWatermarkTracker.WATERMARK.set(0);
        HashMap<String, String> subscribedStreamsToLastDiscoveredShardIds = new HashMap<>();
        subscribedStreamsToLastDiscoveredShardIds.put(streamName, null);

        final KinesisDeserializationSchema<String> deserializationSchema =
                new KinesisDeserializationSchemaWrapper<>(new OpenCheckingStringSchema());
        Properties props = new Properties();
        props.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        props.setProperty(
                ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, Long.toString(10L));
        props.setProperty(
                ConsumerConfigConstants.WATERMARK_SYNC_MILLIS,
                Long.toString(watermarkSyncInterval));
        props.setProperty(ConsumerConfigConstants.WATERMARK_LOOKAHEAD_MILLIS, Long.toString(5));

        BlockingQueue<String> shard1 = new LinkedBlockingQueue<>();

        Map<String, List<BlockingQueue<String>>> streamToQueueMap = new HashMap<>();
        streamToQueueMap.put(streamName, Collections.singletonList(shard1));

        // override createFetcher to mock Kinesis
        FlinkKinesisConsumer<String> sourceFunc =
                new FlinkKinesisConsumer<String>(streamName, deserializationSchema, props) {
                    @Override
                    protected KinesisDataFetcher<String> createFetcher(
                            List<String> streams,
                            SourceFunction.SourceContext<String> sourceContext,
                            RuntimeContext runtimeContext,
                            Properties configProps,
                            KinesisDeserializationSchema<String> deserializationSchema) {

                        KinesisDataFetcher<String> fetcher =
                                new KinesisDataFetcher<String>(
                                        streams,
                                        sourceContext,
                                        sourceContext.getCheckpointLock(),
                                        runtimeContext,
                                        configProps,
                                        deserializationSchema,
                                        getShardAssigner(),
                                        getPeriodicWatermarkAssigner(),
                                        getWatermarkTracker(),
                                        new AtomicReference<>(),
                                        new ArrayList<>(),
                                        subscribedStreamsToLastDiscoveredShardIds,
                                        (props) ->
                                                FakeKinesisBehavioursFactory
                                                        .blockingQueueGetRecords(streamToQueueMap),
                                        null) {
                                    @Override
                                    protected void emitWatermark() {
                                        // necessary in this test to ensure that watermark state is
                                        // updated
                                        // before the watermark timer callback is triggered
                                        synchronized (sourceContext.getCheckpointLock()) {
                                            super.emitWatermark();
                                        }
                                    }
                                };
                        return fetcher;
                    }
                };

        sourceFunc.setShardAssigner(
                (streamShardHandle, i) -> {
                    // shardId-000000000000
                    return Integer.parseInt(
                            streamShardHandle
                                    .getShard()
                                    .getShardId()
                                    .substring("shardId-".length()));
                });

        sourceFunc.setPeriodicWatermarkAssigner(new TestTimestampExtractor(maxOutOfOrderness));

        sourceFunc.setWatermarkTracker(new TestWatermarkTracker());

        // there is currently no test harness specifically for sources,
        // so we overlay the source thread here
        AbstractStreamOperatorTestHarness<Object> testHarness =
                new AbstractStreamOperatorTestHarness<Object>(
                        new StreamSource(sourceFunc), 1, 1, 0);
        testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);
        testHarness.getExecutionConfig().setAutoWatermarkInterval(autoWatermarkInterval);

        testHarness.initializeEmptyState();
        testHarness.open();

        final ConcurrentLinkedQueue<Object> results = testHarness.getOutput();
        final AtomicBoolean throwOnCollect = new AtomicBoolean();

        @SuppressWarnings("unchecked")
        SourceFunction.SourceContext<String> sourceContext =
                new CollectingSourceContext(testHarness.getCheckpointLock(), results) {
                    @Override
                    public void markAsTemporarilyIdle() {}

                    @Override
                    public void collect(Serializable element) {
                        if (throwOnCollect.get()) {
                            throw new RuntimeException("expected");
                        }
                        super.collect(element);
                    }

                    @Override
                    public void emitWatermark(Watermark mark) {
                        results.add(mark);
                    }
                };

        final AtomicReference<Exception> sourceThreadError = new AtomicReference<>();
        new Thread(
                        () -> {
                            try {
                                sourceFunc.run(sourceContext);
                            } catch (InterruptedException e) {
                                // expected on cancel
                            } catch (Exception e) {
                                sourceThreadError.set(e);
                            }
                        })
                .start();

        ArrayList<Object> expectedResults = new ArrayList<>();

        final long record1 = 1;
        shard1.put(Long.toString(record1));
        expectedResults.add(Long.toString(record1));
        awaitRecordCount(results, expectedResults.size());

        // at this point we know the fetcher was initialized
        final KinesisDataFetcher fetcher =
                org.powermock.reflect.Whitebox.getInternalState(sourceFunc, "fetcher");

        // trigger watermark emit
        testHarness.setProcessingTime(testHarness.getProcessingTime() + autoWatermarkInterval);
        expectedResults.add(new Watermark(-4));
        // verify watermark
        awaitRecordCount(results, expectedResults.size());
        assertThat(results).contains(expectedResults.toArray());
        assertThat(TestWatermarkTracker.WATERMARK.get()).isEqualTo(0);

        // trigger sync
        testHarness.setProcessingTime(testHarness.getProcessingTime() + 1);
        TestWatermarkTracker.assertGlobalWatermark(-4);

        final long record2 = record1 + (watermarkSyncInterval * 3) + 1;
        shard1.put(Long.toString(record2));

        // wait for the record to be buffered in the emitter
        final RecordEmitter<?> emitter =
                org.powermock.reflect.Whitebox.getInternalState(fetcher, "recordEmitter");
        RecordEmitter.RecordQueue emitterQueue = emitter.getQueue(0);
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
        while (deadline.hasTimeLeft() && emitterQueue.getSize() < 1) {
            Thread.sleep(10);
        }
        assertThat(emitterQueue.getSize()).as("first record received").isEqualTo(1);

        // Advance the watermark. Since the new record is past global watermark + threshold,
        // it won't be emitted and the watermark does not advance
        testHarness.setProcessingTime(testHarness.getProcessingTime() + autoWatermarkInterval);
        assertThat(results).contains(expectedResults.toArray());
        assertThat((long) org.powermock.reflect.Whitebox.getInternalState(fetcher, "nextWatermark"))
                .isEqualTo(3000L);
        TestWatermarkTracker.assertGlobalWatermark(-4);

        // Trigger global watermark sync
        testHarness.setProcessingTime(testHarness.getProcessingTime() + 1);
        expectedResults.add(Long.toString(record2));
        awaitRecordCount(results, expectedResults.size());
        assertThat(results).contains(expectedResults.toArray());
        TestWatermarkTracker.assertGlobalWatermark(3000);

        // Trigger watermark update and emit
        testHarness.setProcessingTime(testHarness.getProcessingTime() + autoWatermarkInterval);
        expectedResults.add(new Watermark(3000));
        assertThat(results).contains(expectedResults.toArray());

        // verify exception propagation
        assertThat(sourceThreadError.get()).isNull();
        throwOnCollect.set(true);
        shard1.put(Long.toString(record2 + 1));

        deadline = Deadline.fromNow(Duration.ofSeconds(10));
        while (deadline.hasTimeLeft() && sourceThreadError.get() == null) {
            Thread.sleep(10);
        }
        assertThat(sourceThreadError.get()).isNotNull();
        assertThat(sourceThreadError.get().getMessage()).as("expected").isNotNull();

        sourceFunc.cancel();
        testHarness.close();
    }

    @Test
    public void testCloseConnectorBeforeSubtaskStart() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        FlinkKinesisConsumer<String> consumer =
                new FlinkKinesisConsumer<>("fakeStream", new SimpleStringSchema(), config);
        consumer.close();
    }

    private void awaitRecordCount(ConcurrentLinkedQueue<? extends Object> queue, int count)
            throws Exception {
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
        while (deadline.hasTimeLeft() && queue.size() < count) {
            Thread.sleep(10);
        }

        int received = queue.size();
        assertThat(received).isEqualTo(count);
    }

    private static class OpenCheckingStringSchema extends SimpleStringSchema {
        private boolean opened = false;

        @Override
        public void open(DeserializationSchema.InitializationContext context) throws Exception {
            assertThat(context.getMetricGroup()).isNotNull();
            this.opened = true;
        }

        @Override
        public String deserialize(byte[] message) {
            if (!opened) {
                throw new AssertionError(
                        "DeserializationSchema was not opened before deserialization.");
            }
            return super.deserialize(message);
        }
    }

    private static class TestTimestampExtractor
            extends BoundedOutOfOrdernessTimestampExtractor<String> {
        private static final long serialVersionUID = 1L;

        public TestTimestampExtractor(Time maxAllowedLateness) {
            super(maxAllowedLateness);
        }

        @Override
        public long extractTimestamp(String element) {
            return Long.parseLong(element);
        }
    }

    private static class TestWatermarkTracker extends WatermarkTracker {

        private static final AtomicLong WATERMARK = new AtomicLong();

        @Override
        public long getUpdateTimeoutCount() {
            return 0;
        }

        @Override
        public long updateWatermark(long localWatermark) {
            WATERMARK.set(localWatermark);
            return localWatermark;
        }

        static void assertGlobalWatermark(long expected) {
            assertThat(WATERMARK.get()).isEqualTo(expected);
        }
    }

    /* ===========================================================================
     Tests for FLINK-35299
     The setup for these tests will always be the same:
     - stream A with state for shards 0 and 1
     - stream B with state for shard 0

     Then new shards will be discovered:
     - new shard (1) for stream B
     - new shard (0) for stream C - since stream C is not in state yet, it qualifies as a "new stream".
     ==============================================================================*/

    /**
     * Tests FLINK-35299 with the default config values:
     * - IF there is no state at all, all new streams/shards should start from INITIAL POSITION.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299DefaultsWhenThereIsNoState() throws Exception {
        Properties config = TestUtils.getStandardProperties();

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = null;

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        SequenceNumber defaultInitialPositionSeqNumber = getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION));
        expectedResults.put(streamShardA0, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardA1, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardB0, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardB1, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardC0, defaultInitialPositionSeqNumber);

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests FLINK-35299 with the {@link ConsumerConfigConstants#APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS}
     * flag is set to true.
     * - IF there is no state at all, all new streams/shards should start from INITIAL POSITION.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299ApplyStreamInitialPositionForNewStreamsWhenThereIsNoState() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        config.setProperty(APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS, "true");

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = null;

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        SequenceNumber defaultInitialPositionSeqNumber = getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION));
        expectedResults.put(streamShardA0, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardA1, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardB0, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardB1, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardC0, defaultInitialPositionSeqNumber);

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests FLINK-35299 with the {@link ConsumerConfigConstants#STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO}
     * list contains some values.
     * - IF there is no state at all, all new streams/shards should start from INITIAL POSITION.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299StreamsToApplyStreamInitialPositionToWhenThereIsNoState() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        config.setProperty(STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO, "stream-A");

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = null;

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        SequenceNumber defaultInitialPositionSeqNumber = getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION));
        expectedResults.put(streamShardA0, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardA1, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardB0, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardB1, defaultInitialPositionSeqNumber);
        expectedResults.put(streamShardC0, defaultInitialPositionSeqNumber);

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests the default values of the properties introduced in FLINK-35299:
     * - IF there is some state already
     *      - new streams should start from EARLIEST
     *      - new shards for existing streams start from EARLIEST
     *      - existing shards should continue from the state value
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299DefaultsWhenThereIsState() throws Exception {
        Properties config = TestUtils.getStandardProperties();

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = new ArrayList<>();
        existingState.add(createShardState("stream-A", 0, "A0"));
        existingState.add(createShardState("stream-A", 1, "A1"));
        existingState.add(createShardState("stream-B", 0, "B0"));

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        expectedResults.put(streamShardA0, getSequenceNumber("A0"));
        expectedResults.put(streamShardA1, getSequenceNumber("A1"));
        expectedResults.put(streamShardB0, getSequenceNumber("B0"));
        expectedResults.put(streamShardB1, getSequenceNumber(InitialPosition.TRIM_HORIZON));
        expectedResults.put(streamShardC0, getSequenceNumber(InitialPosition.TRIM_HORIZON));

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests FLINK-35299 when the {@link ConsumerConfigConstants#APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS}
     * flag is set to true. In this case any NEW streams should start from the initial position configured and
     * everything else should stay as it was.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299ApplyStreamInitialPositionForNewStreamsSetToTrue() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        config.setProperty(APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS, "true");

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = new ArrayList<>();
        existingState.add(createShardState("stream-A", 0, "A0"));
        existingState.add(createShardState("stream-A", 1, "A1"));
        existingState.add(createShardState("stream-B", 0, "B0"));

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        expectedResults.put(streamShardA0, getSequenceNumber("A0"));
        expectedResults.put(streamShardA1, getSequenceNumber("A1"));
        expectedResults.put(streamShardB0, getSequenceNumber("B0"));
        expectedResults.put(streamShardB1, getSequenceNumber(InitialPosition.TRIM_HORIZON));
        expectedResults.put(streamShardC0, getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)));

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests FLINK-35299 when the {@link ConsumerConfigConstants#STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO}
     * flag is set to a non-null list. In this case the stream used in that list should use the initial position
     * from the config instead of using the state value. Everything else should behave as before.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299StreamsToApplyStreamInitialPositionTo() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        config.setProperty(STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO, "stream-A");

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = new ArrayList<>();
        existingState.add(createShardState("stream-A", 0, "A0"));
        existingState.add(createShardState("stream-A", 1, "A1"));
        existingState.add(createShardState("stream-B", 0, "B0"));

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        expectedResults.put(streamShardA0, getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)));
        expectedResults.put(streamShardA1, getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)));
        expectedResults.put(streamShardB0, getSequenceNumber("B0"));
        expectedResults.put(streamShardB1, getSequenceNumber(InitialPosition.TRIM_HORIZON));
        expectedResults.put(streamShardC0, getSequenceNumber(InitialPosition.TRIM_HORIZON));

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests FLINK-35299 when the {@link ConsumerConfigConstants#STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO}
     * flag contains streams that are not tracked yet.
     * This is an edge case of {@link #testFLINK35299StreamsToApplyStreamInitialPositionTo()}.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFLINK35299StreamsToApplyStreamInitialPositionToForANewStream() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        config.setProperty(STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO, "stream-C");

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = new ArrayList<>();
        existingState.add(createShardState("stream-A", 0, "A0"));
        existingState.add(createShardState("stream-A", 1, "A1"));
        existingState.add(createShardState("stream-B", 0, "B0"));

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        expectedResults.put(streamShardA0, getSequenceNumber("A0"));
        expectedResults.put(streamShardA1, getSequenceNumber("A1"));
        expectedResults.put(streamShardB0, getSequenceNumber("B0"));
        expectedResults.put(streamShardB1, getSequenceNumber(InitialPosition.TRIM_HORIZON));
        expectedResults.put(streamShardC0, getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)));

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    /**
     * Tests FLINK-35299 when the {@link ConsumerConfigConstants#APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS} flag
     * is set to true and the {@link ConsumerConfigConstants#STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO}
     * list is set to a non-null value.
     */
    @SuppressWarnings("unchecked")
    public void testFLINK35299BothNewPropertiesBeingUsed() throws Exception {
        Properties config = TestUtils.getStandardProperties();
        config.setProperty(APPLY_STREAM_INITIAL_POSITION_FOR_NEW_STREAMS, "true");
        config.setProperty(STREAMS_TO_APPLY_STREAM_INITIAL_POSITION_TO, "stream-B");

        List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState = new ArrayList<>();
        existingState.add(createShardState("stream-A", 0, "A0"));
        existingState.add(createShardState("stream-A", 1, "A1"));
        existingState.add(createShardState("stream-B", 0, "B0"));

        List<StreamShardHandle> shardsToSubscribe = new ArrayList<>();
        StreamShardHandle streamShardA0 = getStreamShard("stream-A", 0);
        StreamShardHandle streamShardA1 = getStreamShard("stream-A", 1);
        StreamShardHandle streamShardB0 = getStreamShard("stream-B", 0);
        StreamShardHandle streamShardB1 = getStreamShard("stream-B", 1);
        StreamShardHandle streamShardC0 = getStreamShard("stream-C", 0);
        shardsToSubscribe.add(streamShardA0);
        shardsToSubscribe.add(streamShardA1);
        shardsToSubscribe.add(streamShardB0);
        shardsToSubscribe.add(streamShardB1); // new shard for existing stream
        shardsToSubscribe.add(streamShardC0); // new stream

        Map<StreamShardHandle, SequenceNumber> expectedResults = new HashMap<>();
        expectedResults.put(streamShardA0, getSequenceNumber("A0"));
        expectedResults.put(streamShardA1, getSequenceNumber("A1"));
        expectedResults.put(streamShardB0, getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)));
        expectedResults.put(streamShardB1, getSequenceNumber(InitialPosition.TRIM_HORIZON));
        expectedResults.put(streamShardC0, getSequenceNumber(InitialPosition.valueOf(ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION)));

        runAndValidate(config, existingState, shardsToSubscribe, expectedResults);
    }

    private void runAndValidate(Properties config,
                                List<Tuple2<StreamShardMetadata, SequenceNumber>> existingState,
                                List<StreamShardHandle> shardsToSubscribe,
                                Map<StreamShardHandle, SequenceNumber> expectedResults) throws Exception {
        KinesisDataFetcher mockedFetcher = mockKinesisDataFetcher();

        TestingListState<Tuple2<StreamShardMetadata, SequenceNumber>> listState = new TestingListState<>();
        if (existingState != null) {
            listState.addAll(existingState);
        }
        when(mockedFetcher.discoverNewShardsToSubscribe()).thenReturn(shardsToSubscribe);

        List<String> streamsToConsume = shardsToSubscribe.stream().map(StreamShardHandle::getStreamName).distinct().collect(Collectors.toList());
        FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>(streamsToConsume, new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()), config);
        RuntimeContext context = new MockStreamingRuntimeContext(true, 2, 0);
        consumer.setRuntimeContext(context);

        OperatorStateStore operatorStateStore = mock(OperatorStateStore.class);
        when(operatorStateStore.getUnionListState(any(ListStateDescriptor.class))).thenReturn(listState);

        StateInitializationContext initializationContext = mock(StateInitializationContext.class);
        when(initializationContext.getOperatorStateStore()).thenReturn(operatorStateStore);
        when(initializationContext.isRestored()).thenReturn(true);

        consumer.initializeState(initializationContext);

        consumer.open(new Configuration());
        consumer.run(Mockito.mock(SourceFunction.SourceContext.class));

        // check interactions with fetched
        expectedResults.forEach((streamShardHandle, sequenceNumber) -> verifyRegisterNewSubscribedShard(mockedFetcher, streamShardHandle, sequenceNumber));

        // arbitrary checkpoint to validate new state
        consumer.snapshotState(new StateSnapshotContextSynchronousImpl(123, 123));
        assertThat(listState.isClearCalled()).isTrue();
        List<Tuple2<StreamShardMetadata, SequenceNumber>> list = listState.getList();
        for (Tuple2<StreamShardMetadata, SequenceNumber> entry : list) {
            StreamShardMetadata streamShardMetadata = entry.f0;
            SequenceNumber sequenceNumber = entry.f1;

            SequenceNumber expectedSequenceNumber = expectedResults.get(getStreamShard(streamShardMetadata));
            assertThat(sequenceNumber).isEqualTo(expectedSequenceNumber);
        }
    }

    private static SequenceNumber getSequenceNumber(String seqNumber) {
        return new SequenceNumber(seqNumber);
    }

    private static SequenceNumber getSequenceNumber(InitialPosition initialPosition) {
        return initialPosition.toSentinelSequenceNumber().get();
    }

    private static void verifyRegisterNewSubscribedShard(KinesisDataFetcher mockedFetcher, StreamShardHandle streamShardHandle, SequenceNumber sequenceNumber) {
        Mockito.verify(mockedFetcher).registerNewSubscribedShardState(
                new KinesisStreamShardState(
                        KinesisDataFetcher.convertToStreamShardMetadata(streamShardHandle),
                        streamShardHandle,
                        sequenceNumber)
        );
    }

    private StreamShardHandle getStreamShard(StreamShardMetadata streamShardMetadata) {
        return getStreamShard(streamShardMetadata.getStreamName(), streamShardMetadata.getShardId());
    }

    private static StreamShardHandle getStreamShard(String streamName, int shardId) {
        return getStreamShard(streamName, KinesisShardIdGenerator.generateFromShardOrder(shardId));
    }

    private static StreamShardHandle getStreamShard(String streamName, String shardId) {
        return new StreamShardHandle(streamName, new Shard().withShardId(shardId));
    }
}
