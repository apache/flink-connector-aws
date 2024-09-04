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

package org.apache.flink.connector.kinesis.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.InitialPosition;
import org.apache.flink.connector.kinesis.source.enumerator.assigner.ShardAssignerFactory;
import org.apache.flink.connector.kinesis.source.proxy.ListShardsStartingPosition;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.TestKinesisStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.KinesisStreamProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardsWithEqualHashKeyRange;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsSourceEnumeratorTest {

    private static final int NUM_SUBTASKS = 1;
    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:123456789012:stream/keenesesStream";

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithoutStateDiscoversAndAssignsShards(
            InitialPosition initialPosition,
            String initialTimestamp,
            ShardIteratorType expectedShardIteratorType)
            throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(STREAM_INITIAL_POSITION, initialPosition);
            sourceConfig.set(STREAM_INITIAL_TIMESTAMP, initialTimestamp);

            // Given enumerator is initialized with no state
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            // When enumerator starts
            enumerator.start();
            // Then initial discovery scheduled, with periodic discovery after
            assertThat(context.getOneTimeCallables()).hasSize(1);
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            // Then all 4 shards discovered on startup with configured INITIAL_POSITION
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(expectedShardIteratorType));

            // Given no resharding occurs (list of shards remains the same)
            // When first periodic discovery runs
            context.runPeriodicCallable(0);
            // Then no additional splits are assigned
            SplitsAssignment<KinesisShardSplit> noUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(noUpdateSplitAssignment.assignment()).isEmpty();

            // Given resharding occurs
            String[] additionalShards = new String[] {generateShardId(4), generateShardId(5)};
            streamProxy.addShards(additionalShards);
            // When periodic discovery runs
            context.runPeriodicCallable(0);
            // Then only additional shards are assigned to read from TRIM_HORIZON
            SplitsAssignment<KinesisShardSplit> afterReshardingSplitAssignment =
                    context.getSplitsAssignmentSequence().get(2);
            assertThat(afterReshardingSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            afterReshardingSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(additionalShards);
            assertThat(
                            afterReshardingSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.TRIM_HORIZON));
        }
    }

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithStateDoesNotAssignCompletedShards(
            InitialPosition initialPosition,
            String initialTimestamp,
            ShardIteratorType expectedShardIteratorType)
            throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final String completedShard = generateShardId(0);
            final String lastSeenShard = generateShardId(1);

            KinesisStreamsSourceEnumeratorState state =
                    new KinesisStreamsSourceEnumeratorState(Collections.emptyList(), lastSeenShard);

            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(STREAM_INITIAL_POSITION, initialPosition);
            sourceConfig.set(STREAM_INITIAL_TIMESTAMP, initialTimestamp);

            // Given enumerator is initialised with state
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            state,
                            true);
            // When enumerator starts
            enumerator.start();
            // Then no initial discovery is scheduled, but a periodic discovery is scheduled
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        completedShard, lastSeenShard, generateShardId(2), generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // When first periodic discovery of shards
            context.runPeriodicCallable(0);
            // Then newer shards will be discovered and read from TRIM_HORIZON, independent of
            // configured starting position
            SplitsAssignment<KinesisShardSplit> firstUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            assertThat(firstUpdateSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            firstUpdateSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(generateShardId(2), generateShardId(3));
            assertThat(
                            firstUpdateSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.TRIM_HORIZON));
        }
    }

    @ParameterizedTest
    @MethodSource("provideInitialPositionForShardDiscovery")
    void testInitialPositionForListShardsMapping(
            Instant currentTimestamp,
            InitialPosition initialPosition,
            String initialTimestamp,
            ListShardsStartingPosition expected)
            throws Exception {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();

            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(STREAM_INITIAL_POSITION, initialPosition);
            sourceConfig.set(STREAM_INITIAL_TIMESTAMP, initialTimestamp);

            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);

            assertThat(
                            enumerator.getInitialPositionForShardDiscovery(
                                    initialPosition, currentTimestamp))
                    .usingRecursiveComparison()
                    .isEqualTo(expected);
        }
    }

    @Test
    void testAddSplitsBackThrowsException() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            KinesisStreamsSourceEnumerator enumerator =
                    getSimpleEnumeratorWithNoState(context, streamProxy);
            List<KinesisShardSplit> splits = Collections.singletonList(getTestSplit());

            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> enumerator.addSplitsBack(splits, 1));
        }
    }

    @Test
    void testHandleSplitRequestIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            KinesisStreamsSourceEnumerator enumerator =
                    getSimpleEnumeratorWithNoState(context, streamProxy);

            // Given enumerator has no assigned splits
            // When we add splits back
            // Then handled gracefully with no exception thrown
            assertThatNoException()
                    .isThrownBy(() -> enumerator.handleSplitRequest(1, "some-hostname"));
        }
    }

    @Test
    void testAssignSplitsSurfacesThrowableIfUnableToListShards() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            enumerator.start();

            // Given List Shard request throws an Exception
            streamProxy.setListShardsExceptionSupplier(
                    () -> AwsServiceException.create("Internal Service Error", null));

            // When first discovery runs
            // Then runtime exception is thrown
            assertThatExceptionOfType(FlinkRuntimeException.class)
                    .isThrownBy(context::runNextOneTimeCallable)
                    .withMessage("Failed to list shards.")
                    .withStackTraceContaining("Internal Service Error");
        }
    }

    @Test
    void testAssignSplitsHandlesRepeatSplitsGracefully() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);

            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then all 4 shards discovered on startup
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);

            // Given ListShards doesn't respect lastSeenShardId, and returns already assigned shards
            streamProxy.setShouldRespectLastSeenShardId(false);

            // When first periodic discovery runs
            // Then handled gracefully
            assertThatNoException().isThrownBy(() -> context.runPeriodicCallable(0));
            SplitsAssignment<KinesisShardSplit> secondReturnedSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(secondReturnedSplitAssignment.assignment()).isEmpty();
        }
    }

    @Test
    void testAssignSplitWithoutRegisteredReaders() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            enumerator.start();

            // Given enumerator is initialised without a reader
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);

            // When first discovery runs
            context.runNextOneTimeCallable();

            // Then nothing is assigned
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Given a reader is now registered
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);

            // When next periodic discovery is run
            context.runPeriodicCallable(0);
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then shards are assigned, still respecting original configuration
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.AT_TIMESTAMP));
        }
    }

    @Test
    void testAssignSplitWithInsufficientRegisteredReaders() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(2)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            enumerator.start();

            // Given enumerator is initialised with only one reader
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards = generateShardsWithEqualHashKeyRange(4);
            streamProxy.addShards(shards);

            // When first discovery runs
            context.runNextOneTimeCallable();

            // Then nothing is assigned
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Given a reader is now registered
            final int secondSubtaskId = 2;
            context.registerReader(TestUtil.getTestReaderInfo(secondSubtaskId));
            enumerator.addReader(secondSubtaskId);

            // When next periodic discovery is run
            context.runPeriodicCallable(0);
            SplitsAssignment<KinesisShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then shards are assigned, still respecting original configuration
            assertThat(initialSplitAssignment.assignment())
                    .containsOnlyKeys(subtaskId, secondSubtaskId);
            assertThat(
                            initialSplitAssignment.assignment().values().stream()
                                    .flatMap(Collection::stream)
                                    .map(KinesisShardSplit::getShardId))
                    .containsExactlyInAnyOrder(
                            Arrays.stream(shards).map(Shard::shardId).toArray(String[]::new));
            assertThat(
                            initialSplitAssignment.assignment().values().stream()
                                    .flatMap(Collection::stream)
                                    .map(KinesisShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.AT_TIMESTAMP));
        }
    }

    @Test
    void testRestoreFromStateRemembersLastSeenShardId() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                MockSplitEnumeratorContext<KinesisShardSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shardIds);
            // Given enumerator has run discovery
            context.runNextOneTimeCallable();

            // When restored from state
            KinesisStreamsSourceEnumeratorState snapshottedState = enumerator.snapshotState(1);
            KinesisStreamsSourceEnumerator restoredEnumerator =
                    new KinesisStreamsSourceEnumerator(
                            restoredContext,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            snapshottedState,
                            true);
            restoredEnumerator.start();
            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            restoredContext.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            restoredEnumerator.addReader(subtaskId);
            restoredContext.runPeriodicCallable(0);

            // Then ListShards receives a ListShards call with the lastSeenShardId
            assertThat(
                            streamProxy
                                    .getLastProvidedListShardStartingPosition()
                                    .getShardFilter()
                                    .shardId())
                    .isEqualTo(shardIds[shardIds.length - 1]);
        }
    }

    @Test
    void testHandleUnrecognisedSourceEventIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);

            assertThatNoException()
                    .isThrownBy(() -> enumerator.handleSourceEvent(1, new SourceEvent() {}));
        }
    }

    @Test
    void testCloseClosesStreamProxy() throws Throwable {
        try (MockSplitEnumeratorContext<KinesisShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            TestKinesisStreamProxy streamProxy = getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            KinesisStreamsSourceEnumerator enumerator =
                    new KinesisStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null,
                            true);
            enumerator.start();

            assertThatNoException().isThrownBy(enumerator::close);
            assertThat(streamProxy.isClosed()).isTrue();
        }
    }

    private KinesisStreamsSourceEnumerator getSimpleEnumeratorWithNoState(
            MockSplitEnumeratorContext<KinesisShardSplit> context, StreamProxy streamProxy) {
        final Configuration sourceConfig = new Configuration();
        KinesisStreamsSourceEnumerator enumerator =
                new KinesisStreamsSourceEnumerator(
                        context,
                        STREAM_ARN,
                        sourceConfig,
                        streamProxy,
                        ShardAssignerFactory.uniformShardAssigner(),
                        null,
                        true);
        enumerator.start();
        assertThat(context.getOneTimeCallables()).hasSize(1);
        assertThat(context.getPeriodicCallables()).hasSize(1);
        return enumerator;
    }

    private static Stream<Arguments> provideInitialPositions() {
        return Stream.of(
                Arguments.of(InitialPosition.LATEST, "", ShardIteratorType.AT_TIMESTAMP),
                Arguments.of(InitialPosition.TRIM_HORIZON, "", ShardIteratorType.TRIM_HORIZON),
                Arguments.of(
                        InitialPosition.AT_TIMESTAMP,
                        "2023-04-13T09:18:00.0+01:00",
                        ShardIteratorType.AT_TIMESTAMP));
    }

    private static Stream<Arguments> provideInitialPositionForShardDiscovery() {
        Instant currentTimestamp = Instant.now();

        return Stream.of(
                Arguments.of(
                        currentTimestamp,
                        InitialPosition.LATEST,
                        "",
                        ListShardsStartingPosition.fromTimestamp(currentTimestamp)),
                Arguments.of(
                        currentTimestamp,
                        InitialPosition.TRIM_HORIZON,
                        "",
                        ListShardsStartingPosition.fromStart(),
                        Arguments.of(
                                currentTimestamp,
                                InitialPosition.AT_TIMESTAMP,
                                "1719776523",
                                ListShardsStartingPosition.fromTimestamp(
                                        Instant.ofEpochSecond(1719776523)))));
    }
}
