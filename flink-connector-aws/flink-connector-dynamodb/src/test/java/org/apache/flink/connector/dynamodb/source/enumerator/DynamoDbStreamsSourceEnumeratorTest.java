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

package org.apache.flink.connector.dynamodb.source.enumerator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants;
import org.apache.flink.connector.dynamodb.source.enumerator.assigner.ShardAssignerFactory;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEvent;
import org.apache.flink.connector.dynamodb.source.enumerator.event.SplitsFinishedEventContext;
import org.apache.flink.connector.dynamodb.source.proxy.StreamProxy;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.dynamodb.source.util.DynamoDbStreamsProxyProvider.getTestStreamProxy;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShard;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.LATEST;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

class DynamoDbStreamsSourceEnumeratorTest {

    private static final int NUM_SUBTASKS = 1;
    private static final String STREAM_ARN =
            "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithoutStateDiscoversAndAssignsShards(
            DynamodbStreamsSourceConfigConstants.InitialPosition initialPosition,
            ShardIteratorType expectedShardIteratorType)
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(STREAM_INITIAL_POSITION, initialPosition);

            // Given enumerator is initialized with no state
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            // When enumerator starts
            enumerator.start();
            // Then initial discovery scheduled, with periodic discovery after
            assertThat(context.getOneTimeCallables()).hasSize(1);
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards =
                    new Shard[] {
                        generateShard(0, "1200", null, null),
                        generateShard(1, "1300", null, null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "2100", null, null)
                    };
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shards);
            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<DynamoDbStreamsShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            // Then all 4 shards discovered on startup with configured INITIAL_POSITION
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(expectedShardIteratorType));

            // Given no resharding occurs (list of shards remains the same)
            // When first periodic discovery runs
            context.runPeriodicCallable(0);
            // Then no additional splits are assigned
            SplitsAssignment<DynamoDbStreamsShardSplit> noUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(noUpdateSplitAssignment.assignment()).isEmpty();
        }
    }

    @Test
    void testLatestOnlyAssignsTheLeafNodesAndSkipsParentShardsDuringInitialDiscovery()
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(
                    STREAM_INITIAL_POSITION,
                    DynamodbStreamsSourceConfigConstants.InitialPosition.LATEST);

            // Given enumerator is initialized with no state
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            // When enumerator starts
            enumerator.start();
            // Then initial discovery scheduled, with periodic discovery after
            assertThat(context.getOneTimeCallables()).hasSize(1);
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards =
                    new Shard[] {
                        generateShard(0, "1200", "1800", null),
                        generateShard(1, "1300", null, null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "2100", null, null),
                        generateShard(4, "2200", null, generateShardId(0))
                    };
            String[] shardIds =
                    new String[] {
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3),
                        generateShardId(4)
                    };
            streamProxy.addShards(shards);
            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<DynamoDbStreamsShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            // Then all 4 shards discovered on startup with configured INITIAL_POSITION
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getStartingPosition))
                    .allSatisfy(s -> assertThat(s.getShardIteratorType()).isEqualTo(LATEST));

            // Given no resharding occurs (list of shards remains the same)
            // When first periodic discovery runs
            context.runPeriodicCallable(0);
            // Then no additional splits are assigned
            SplitsAssignment<DynamoDbStreamsShardSplit> noUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(noUpdateSplitAssignment.assignment()).isEmpty();
        }
    }

    @Test
    void testLatestAssignsChildShardsWithTrimHorizonDuringPeriodicDiscovery() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(
                    STREAM_INITIAL_POSITION,
                    DynamodbStreamsSourceConfigConstants.InitialPosition.LATEST);

            // Given enumerator is initialized with no state
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            // When enumerator starts
            enumerator.start();
            // Then initial discovery scheduled, with periodic discovery after
            assertThat(context.getOneTimeCallables()).hasSize(1);
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Instant startInstant = Instant.now();
            Shard[] shards =
                    new Shard[] {
                        generateShard(
                                startInstant.minus(Duration.ofMinutes(40)).toEpochMilli(),
                                "1200",
                                "1800",
                                null),
                        generateShard(
                                startInstant.minus(Duration.ofMinutes(35)).toEpochMilli(),
                                "1300",
                                null,
                                null),
                        generateShard(
                                startInstant.minus(Duration.ofMinutes(30)).toEpochMilli(),
                                "2000",
                                null,
                                null),
                        generateShard(
                                startInstant.minus(Duration.ofMinutes(25)).toEpochMilli(),
                                "2100",
                                null,
                                null),
                        generateShard(
                                startInstant.plus(Duration.ofMinutes(20)).toEpochMilli(),
                                "2200",
                                null,
                                generateShardId(
                                        startInstant.minus(Duration.ofMinutes(40)).toEpochMilli()))
                    };
            String[] shardIds =
                    Stream.of(
                                    shards[0].shardId(),
                                    shards[1].shardId(),
                                    shards[2].shardId(),
                                    shards[3].shardId())
                            .toArray(String[]::new);
            streamProxy.addShards(shards);
            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<DynamoDbStreamsShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            // Then all 4 shards discovered on startup with configured INITIAL_POSITION
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getStartingPosition))
                    .allSatisfy(s -> assertThat(s.getShardIteratorType()).isEqualTo(LATEST));

            Shard[] childShards =
                    new Shard[] {
                        generateShard(
                                Instant.now().plus(Duration.ofMinutes(10)).toEpochMilli(),
                                "3100",
                                null,
                                shards[2].shardId())
                    };
            streamProxy.addShards(childShards);
            enumerator.handleSourceEvent(
                    subtaskId,
                    new SplitsFinishedEvent(
                            Set.of(
                                    new SplitsFinishedEventContext(
                                            shards[2].shardId(), List.of(childShards[0])))));

            DynamoDbStreamsShardSplit childSplit =
                    new DynamoDbStreamsShardSplit(
                            STREAM_ARN,
                            childShards[0].shardId(),
                            StartingPosition.fromStart(),
                            shards[2].shardId());
            assertThat(context.getSplitsAssignmentSequence().get(1).assignment().get(subtaskId))
                    .containsExactly(childSplit);
            // Given no resharding occurs (list of shards remains the same)
            // When first periodic discovery runs
            context.runPeriodicCallable(0);
            // Then no additional splits are assigned
            SplitsAssignment<DynamoDbStreamsShardSplit> periodicDiscoverySplitAssignment =
                    context.getSplitsAssignmentSequence().get(2);
            assertThat(periodicDiscoverySplitAssignment.assignment().get(subtaskId))
                    .isNullOrEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource("provideInitialPositions")
    void testStartWithStateDoesNotAssignCompletedShards(
            DynamodbStreamsSourceConfigConstants.InitialPosition initialPosition,
            ShardIteratorType shardIteratorType)
            throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Shard completedShard =
                    generateShard(
                            Instant.now().minus(Duration.ofHours(2)).toEpochMilli(),
                            "1000",
                            "2000",
                            null);

            Instant startTimestamp = Instant.now();
            DynamoDbStreamsSourceEnumeratorState state =
                    new DynamoDbStreamsSourceEnumeratorState(
                            List.of(
                                    new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                            new DynamoDbStreamsShardSplit(
                                                    STREAM_ARN,
                                                    completedShard.shardId(),
                                                    StartingPosition.fromStart(),
                                                    completedShard.parentShardId()),
                                            SplitAssignmentStatus.FINISHED)),
                            startTimestamp);

            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(STREAM_INITIAL_POSITION, initialPosition);

            // Given enumerator is initialised with state
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            state);
            // When enumerator starts
            enumerator.start();
            assertThat(context.getPeriodicCallables()).hasSize(1);

            // Given there is one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards =
                    new Shard[] {
                        completedShard,
                        generateShard(
                                Instant.now().plus(Duration.ofHours(1)).toEpochMilli(),
                                "2100",
                                null,
                                completedShard.shardId()),
                        generateShard(
                                Instant.now().minus(Duration.ofMinutes(30)).toEpochMilli(),
                                "2200",
                                null,
                                null),
                        generateShard(
                                Instant.now().minus(Duration.ofMinutes(20)).toEpochMilli(),
                                "3000",
                                null,
                                null)
                    };
            streamProxy.addShards(shards);
            // When first periodic discovery of shards
            context.runPeriodicCallable(0);
            // Then newer shards will be discovered and read from TRIM_HORIZON, independent of
            // configured starting position
            SplitsAssignment<DynamoDbStreamsShardSplit> firstUpdateSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);
            assertThat(firstUpdateSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            firstUpdateSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(
                            shards[1].shardId(), shards[2].shardId(), shards[3].shardId());
            assertThat(
                            firstUpdateSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getStartingPosition)
                                    .map(StartingPosition::getShardIteratorType))
                    .containsExactlyInAnyOrder(
                            TRIM_HORIZON, // child shard of completed shard should be read from
                            // TRIM_HORIZON
                            shardIteratorType, // other shards should be read from the configured
                            // position
                            shardIteratorType);
        }
    }

    @Test
    void testAddSplitsBackThrowsException() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            DynamoDbStreamsSourceEnumerator enumerator =
                    getSimpleEnumeratorWithNoState(context, streamProxy);
            List<DynamoDbStreamsShardSplit> splits = List.of(getTestSplit());

            // Given enumerator has no assigned splits
            // When we add splits back
            // Then handled gracefully with no exception thrown
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> enumerator.addSplitsBack(splits, 1));
        }
    }

    @Test
    void testHandleSplitRequestIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            DynamoDbStreamsSourceEnumerator enumerator =
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
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
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
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards =
                    new Shard[] {
                        generateShard(0, "1200", null, null),
                        generateShard(1, "1300", null, null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "2100", null, null)
                    };
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shards);

            // When first discovery runs
            context.runNextOneTimeCallable();
            SplitsAssignment<DynamoDbStreamsShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then all 4 shards discovered on startup
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);

            // When first periodic discovery runs
            // Then handled gracefully
            assertThatNoException().isThrownBy(() -> context.runPeriodicCallable(0));
            SplitsAssignment<DynamoDbStreamsShardSplit> secondReturnedSplitAssignment =
                    context.getSplitsAssignmentSequence().get(1);
            assertThat(secondReturnedSplitAssignment.assignment()).isEmpty();
        }
    }

    @Test
    void testAssignSplitWithoutRegisteredReaders() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            enumerator.start();

            // Given enumerator is initialised without a reader
            Shard[] shards =
                    new Shard[] {
                        generateShard(0, "1200", null, null),
                        generateShard(1, "1300", null, null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "2100", null, null)
                    };
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shards);

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
            SplitsAssignment<DynamoDbStreamsShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then shards are assigned, still respecting original configuration
            assertThat(initialSplitAssignment.assignment()).containsOnlyKeys(subtaskId);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().get(subtaskId).stream()
                                    .map(DynamoDbStreamsShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.LATEST));
        }
    }

    @Test
    void testAssignSplitWithInsufficientRegisteredReaders() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(2)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            enumerator.start();

            // Given enumerator is initialised without only one reader
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards =
                    new Shard[] {
                        generateShard(0, "1200", null, null),
                        generateShard(1, "1300", null, null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "2100", null, null)
                    };
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
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
            SplitsAssignment<DynamoDbStreamsShardSplit> initialSplitAssignment =
                    context.getSplitsAssignmentSequence().get(0);

            // Then shards are assigned, still respecting original configuration
            assertThat(initialSplitAssignment.assignment())
                    .containsOnlyKeys(subtaskId, secondSubtaskId);
            assertThat(
                            initialSplitAssignment.assignment().values().stream()
                                    .flatMap(Collection::stream)
                                    .map(DynamoDbStreamsShardSplit::getShardId))
                    .containsExactlyInAnyOrder(shardIds);
            assertThat(
                            initialSplitAssignment.assignment().values().stream()
                                    .flatMap(Collection::stream)
                                    .map(DynamoDbStreamsShardSplit::getStartingPosition))
                    .allSatisfy(
                            s ->
                                    assertThat(s.getShardIteratorType())
                                            .isEqualTo(ShardIteratorType.LATEST));
        }
    }

    @Test
    void testRestoreFromStateRemembersLastSeenShardId() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> restoredContext =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            Shard[] shards =
                    new Shard[] {
                        generateShard(0, "1200", null, null),
                        generateShard(1, "1300", null, null),
                        generateShard(2, "2000", null, null),
                        generateShard(3, "2100", null, null)
                    };
            String[] shardIds =
                    new String[] {
                        generateShardId(0),
                        generateShardId(1),
                        generateShardId(2),
                        generateShardId(3)
                    };
            streamProxy.addShards(shards);
            // Given enumerator has run discovery
            context.runNextOneTimeCallable();

            // When restored from state
            DynamoDbStreamsSourceEnumeratorState snapshottedState = enumerator.snapshotState(1);
            assertThat(
                            snapshottedState.getKnownSplits().stream()
                                    .map(split -> split.split().splitId()))
                    .containsExactlyInAnyOrder(shardIds);
        }
    }

    @Test
    void testHandleSplitFinishedEventTest() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            sourceConfig.set(
                    STREAM_INITIAL_POSITION,
                    DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON);
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            Shard completedShard = generateShard(0, "1000", "2000", null);
            Shard[] shards =
                    new Shard[] {
                        completedShard,
                        generateShard(1, "2100", null, generateShardId(0)),
                        generateShard(2, "2200", null, null),
                        generateShard(3, "3000", null, null)
                    };
            enumerator.start();

            // Given enumerator is initialised with one registered reader, with 4 shards in stream
            final int subtaskId = 1;
            context.registerReader(TestUtil.getTestReaderInfo(subtaskId));
            enumerator.addReader(subtaskId);
            streamProxy.addShards(shards);
            // Given enumerator has run discovery
            context.runNextOneTimeCallable();

            enumerator.handleSourceEvent(
                    1,
                    new SplitsFinishedEvent(
                            Set.of(
                                    new SplitsFinishedEventContext(
                                            completedShard.shardId(), List.of(shards[1])))));

            // When restored from state
            DynamoDbStreamsSourceEnumeratorState snapshotState = enumerator.snapshotState(1);
            assertThat(
                            snapshotState.getKnownSplits().stream()
                                    .filter(
                                            split ->
                                                    split.assignmentStatus()
                                                            == SplitAssignmentStatus.FINISHED)
                                    .map(DynamoDBStreamsShardSplitWithAssignmentStatus::split)
                                    .map(DynamoDbStreamsShardSplit::splitId))
                    .containsExactly(completedShard.shardId());
        }
    }

    @Test
    void testHandleUnrecognisedSourceEventIsNoOp() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);

            assertThatNoException()
                    .isThrownBy(() -> enumerator.handleSourceEvent(1, new SourceEvent() {}));
        }
    }

    @Test
    void testCloseClosesStreamProxy() throws Throwable {
        try (MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context =
                new MockSplitEnumeratorContext<>(NUM_SUBTASKS)) {
            DynamoDbStreamsProxyProvider.TestDynamoDbStreamsProxy streamProxy =
                    getTestStreamProxy();
            final Configuration sourceConfig = new Configuration();
            DynamoDbStreamsSourceEnumerator enumerator =
                    new DynamoDbStreamsSourceEnumerator(
                            context,
                            STREAM_ARN,
                            sourceConfig,
                            streamProxy,
                            ShardAssignerFactory.uniformShardAssigner(),
                            null);
            enumerator.start();

            assertThatNoException().isThrownBy(enumerator::close);
            assertThat(streamProxy.isClosed()).isTrue();
        }
    }

    private DynamoDbStreamsSourceEnumerator getSimpleEnumeratorWithNoState(
            MockSplitEnumeratorContext<DynamoDbStreamsShardSplit> context,
            StreamProxy streamProxy) {
        final Configuration sourceConfig = new Configuration();
        DynamoDbStreamsSourceEnumerator enumerator =
                new DynamoDbStreamsSourceEnumerator(
                        context,
                        STREAM_ARN,
                        sourceConfig,
                        streamProxy,
                        ShardAssignerFactory.uniformShardAssigner(),
                        null);
        enumerator.start();
        assertThat(context.getOneTimeCallables()).hasSize(1);
        assertThat(context.getPeriodicCallables()).hasSize(1);
        return enumerator;
    }

    private static Stream<Arguments> provideInitialPositions() {
        return Stream.of(
                Arguments.of(
                        DynamodbStreamsSourceConfigConstants.InitialPosition.LATEST,
                        ShardIteratorType.LATEST),
                Arguments.of(
                        DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON,
                        TRIM_HORIZON));
    }
}
