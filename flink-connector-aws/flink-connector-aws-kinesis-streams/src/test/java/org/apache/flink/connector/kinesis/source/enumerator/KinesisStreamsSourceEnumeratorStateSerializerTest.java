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

import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitSerializer;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.core.io.VersionMismatchException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.copyOf;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisStreamsSourceEnumeratorStateSerializerTest {

    @Test
    void testSerializeAndDeserializeEverythingSpecified() throws Exception {
        List<KinesisShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(IntStream.rangeClosed(0, 3), IntStream.rangeClosed(4, 10));
        String lastSeenShardId = generateShardId(10);
        KinesisStreamsSourceEnumeratorState initialState =
                new KinesisStreamsSourceEnumeratorState(splitsToStore, lastSeenShardId);

        KinesisShardSplitSerializer splitSerializer = new KinesisShardSplitSerializer();
        KinesisStreamsSourceEnumeratorStateSerializer serializer =
                new KinesisStreamsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        KinesisStreamsSourceEnumeratorState deserializedState =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedState).usingRecursiveComparison().isEqualTo(initialState);
    }

    @Test
    void testDeserializeWithWrongVersionStateSerializer() throws Exception {
        List<KinesisShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(IntStream.rangeClosed(0, 3), IntStream.rangeClosed(4, 10));
        String lastSeenShardId = generateShardId(10);
        KinesisStreamsSourceEnumeratorState initialState =
                new KinesisStreamsSourceEnumeratorState(splitsToStore, lastSeenShardId);

        KinesisShardSplitSerializer splitSerializer = new KinesisShardSplitSerializer();
        KinesisStreamsSourceEnumeratorStateSerializer serializer =
                new KinesisStreamsSourceEnumeratorStateSerializer(splitSerializer);
        KinesisStreamsSourceEnumeratorStateSerializer wrongVersionStateSerializer =
                new WrongVersionStateSerializer(splitSerializer);

        byte[] serialized = wrongVersionStateSerializer.serialize(initialState);

        assertThatExceptionOfType(VersionMismatchException.class)
                .isThrownBy(
                        () ->
                                serializer.deserialize(
                                        wrongVersionStateSerializer.getVersion(), serialized))
                .withMessageContaining(
                        "Trying to deserialize KinesisStreamsSourceEnumeratorState serialized with unsupported version")
                .withMessageContaining(String.valueOf(serializer.getVersion()))
                .withMessageContaining(String.valueOf(wrongVersionStateSerializer.getVersion()));
    }

    @Test
    void testDeserializeWithWrongVersionSplitSerializer() throws Exception {
        List<KinesisShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(IntStream.rangeClosed(0, 3), IntStream.rangeClosed(4, 10));
        String lastSeenShardId = generateShardId(10);
        KinesisStreamsSourceEnumeratorState initialState =
                new KinesisStreamsSourceEnumeratorState(splitsToStore, lastSeenShardId);

        KinesisShardSplitSerializer splitSerializer = new KinesisShardSplitSerializer();
        KinesisStreamsSourceEnumeratorStateSerializer serializer =
                new KinesisStreamsSourceEnumeratorStateSerializer(splitSerializer);
        KinesisShardSplitSerializer wrongVersionSplitSerializer = new WrongVersionSplitSerializer();
        KinesisStreamsSourceEnumeratorStateSerializer wrongVersionStateSerializer =
                new KinesisStreamsSourceEnumeratorStateSerializer(wrongVersionSplitSerializer);

        byte[] serialized = wrongVersionStateSerializer.serialize(initialState);

        assertThatExceptionOfType(VersionMismatchException.class)
                .isThrownBy(() -> serializer.deserialize(serializer.getVersion(), serialized))
                .withMessageContaining(
                        "Trying to deserialize KinesisShardSplit serialized with unsupported version")
                .withMessageContaining(String.valueOf(splitSerializer.getVersion()))
                .withMessageContaining(String.valueOf(wrongVersionSplitSerializer.getVersion()));
    }

    @Test
    void testSerializeWithTrailingBytes() throws Exception {
        List<KinesisShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(IntStream.rangeClosed(0, 3), IntStream.rangeClosed(4, 10));
        String lastSeenShardId = generateShardId(10);
        KinesisStreamsSourceEnumeratorState initialState =
                new KinesisStreamsSourceEnumeratorState(splitsToStore, lastSeenShardId);

        KinesisShardSplitSerializer splitSerializer = new KinesisShardSplitSerializer();
        KinesisStreamsSourceEnumeratorStateSerializer serializer =
                new KinesisStreamsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        byte[] extraBytesSerialized = copyOf(serialized, serialized.length + 1);

        assertThatExceptionOfType(IOException.class)
                .isThrownBy(
                        () -> serializer.deserialize(serializer.getVersion(), extraBytesSerialized))
                .withMessageContaining("Unexpected trailing bytes when deserializing.");
    }

    @Test
    void testDeserializeCompatibilityWithV0() throws Exception {
        // V0 state only contains UNASSIGNED splits
        List<KinesisShardSplitWithAssignmentStatus> splitsWithStatusToStore =
                getSplits(IntStream.empty(), IntStream.rangeClosed(10, 15));
        String lastSeenShardId =
                splitsWithStatusToStore.get(splitsWithStatusToStore.size() - 1).split().splitId();
        KinesisStreamsSourceEnumeratorState expectedState =
                new KinesisStreamsSourceEnumeratorState(splitsWithStatusToStore, lastSeenShardId);

        int version = 0;
        List<KinesisShardSplit> splitsToStoreForOldVersion =
                splitsWithStatusToStore.stream()
                        .map(KinesisShardSplitWithAssignmentStatus::split)
                        .collect(Collectors.toList());
        KinesisStreamsSourceEnumeratorStateV0 initialState =
                new KinesisStreamsSourceEnumeratorStateV0(
                        splitsToStoreForOldVersion, lastSeenShardId);

        KinesisShardSplitSerializer splitSerializer = new KinesisShardSplitSerializer();
        KinesisStreamsSourceEnumeratorStateSerializer serializer =
                new KinesisStreamsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serializeV0(initialState);
        KinesisStreamsSourceEnumeratorState deserializedState =
                serializer.deserialize(version, serialized);

        assertThat(deserializedState).usingRecursiveComparison().isEqualTo(expectedState);
    }

    private List<KinesisShardSplitWithAssignmentStatus> getSplits(
            IntStream assignedShardIdRange, IntStream unassignedShardIdRange) {
        Stream<KinesisShardSplitWithAssignmentStatus> assignedSplits =
                assignedShardIdRange
                        .mapToObj(TestUtil::generateShardId)
                        .map(TestUtil::getTestSplit)
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED));
        Stream<KinesisShardSplitWithAssignmentStatus> unassignedSplits =
                unassignedShardIdRange
                        .mapToObj(TestUtil::generateShardId)
                        .map(TestUtil::getTestSplit)
                        .map(
                                split ->
                                        new KinesisShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED));
        return Stream.concat(assignedSplits, unassignedSplits).collect(Collectors.toList());
    }

    private static class WrongVersionStateSerializer
            extends KinesisStreamsSourceEnumeratorStateSerializer {

        public WrongVersionStateSerializer(KinesisShardSplitSerializer splitSerializer) {
            super(splitSerializer);
        }

        @Override
        public int getVersion() {
            return -1;
        }
    }

    private static class WrongVersionSplitSerializer extends KinesisShardSplitSerializer {

        @Override
        public int getVersion() {
            return -1;
        }
    }
}
