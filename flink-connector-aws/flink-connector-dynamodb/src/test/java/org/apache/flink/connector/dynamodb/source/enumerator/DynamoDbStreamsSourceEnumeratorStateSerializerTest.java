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

import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitSerializer;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;
import org.apache.flink.core.io.VersionMismatchException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.copyOf;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class DynamoDbStreamsSourceEnumeratorStateSerializerTest {

    @Test
    void testSerializeAndDeserializeEverythingSpecified() throws Exception {
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(
                        IntStream.rangeClosed(0, 3),
                        IntStream.rangeClosed(4, 10),
                        IntStream.rangeClosed(11, 15));
        Instant startTimestamp = Instant.now();
        DynamoDbStreamsSourceEnumeratorState deserializedState =
                getDynamoDbStreamsSourceEnumeratorState(splitsToStore, startTimestamp);

        assertThat(deserializedState.getKnownSplits()).isEqualTo(splitsToStore);
        assertThat(deserializedState.getStartTimestamp().toEpochMilli())
                .isEqualTo(startTimestamp.toEpochMilli());
    }

    private static DynamoDbStreamsSourceEnumeratorState getDynamoDbStreamsSourceEnumeratorState(
            List<DynamoDBStreamsShardSplitWithAssignmentStatus> splitsToStore,
            Instant startTimestamp)
            throws IOException {
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(splitsToStore, startTimestamp);

        DynamoDbStreamsShardSplitSerializer splitSerializer =
                new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsSourceEnumeratorStateSerializer serializer =
                new DynamoDbStreamsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        return serializer.deserialize(serializer.getVersion(), serialized);
    }

    @Test
    void testDeserializeWithWrongVersionStateSerializer() throws Exception {
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(
                        IntStream.rangeClosed(0, 3),
                        IntStream.rangeClosed(4, 10),
                        IntStream.rangeClosed(11, 15));
        Instant startTimestamp = Instant.now();

        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(splitsToStore, startTimestamp);
        DynamoDbStreamsShardSplitSerializer splitSerializer =
                new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsSourceEnumeratorStateSerializer serializer =
                new DynamoDbStreamsSourceEnumeratorStateSerializer(splitSerializer);
        DynamoDbStreamsSourceEnumeratorStateSerializer wrongVersionStateSerializer =
                new WrongVersionStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);

        assertThatExceptionOfType(VersionMismatchException.class)
                .isThrownBy(
                        () ->
                                serializer.deserialize(
                                        wrongVersionStateSerializer.getVersion(), serialized))
                .withMessageContaining(
                        "Trying to deserialize DynamoDbStreamsSourceEnumeratorState serialized with unsupported version")
                .withMessageContaining(String.valueOf(serializer.getVersion()))
                .withMessageContaining(String.valueOf(wrongVersionStateSerializer.getVersion()));
    }

    @Test
    void testDeserializeWithWrongVersionSplitSerializer() throws Exception {
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(
                        IntStream.rangeClosed(0, 3),
                        IntStream.rangeClosed(4, 10),
                        IntStream.rangeClosed(11, 15));
        Instant startTimestamp = Instant.now();
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(splitsToStore, startTimestamp);

        DynamoDbStreamsShardSplitSerializer splitSerializer =
                new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsSourceEnumeratorStateSerializer serializer =
                new DynamoDbStreamsSourceEnumeratorStateSerializer(splitSerializer);
        DynamoDbStreamsShardSplitSerializer wrongVersionSplitSerializer =
                new WrongVersionSplitSerializer();
        DynamoDbStreamsSourceEnumeratorStateSerializer wrongVersionStateSerializer =
                new DynamoDbStreamsSourceEnumeratorStateSerializer(wrongVersionSplitSerializer);

        byte[] serialized = wrongVersionStateSerializer.serialize(initialState);

        assertThatExceptionOfType(VersionMismatchException.class)
                .isThrownBy(() -> serializer.deserialize(serializer.getVersion(), serialized))
                .withMessageContaining(
                        "Trying to deserialize DynamoDbStreamsShardSplit serialized with unsupported version")
                .withMessageContaining(String.valueOf(splitSerializer.getVersion()))
                .withMessageContaining(String.valueOf(wrongVersionSplitSerializer.getVersion()));
    }

    @Test
    void testSerializeWithTrailingBytes() throws Exception {
        List<DynamoDBStreamsShardSplitWithAssignmentStatus> splitsToStore =
                getSplits(
                        IntStream.rangeClosed(0, 3),
                        IntStream.rangeClosed(4, 10),
                        IntStream.rangeClosed(11, 15));
        Instant startTimestamp = Instant.now();
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(splitsToStore, startTimestamp);

        DynamoDbStreamsShardSplitSerializer splitSerializer =
                new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsSourceEnumeratorStateSerializer serializer =
                new DynamoDbStreamsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        byte[] extraBytesSerialized = copyOf(serialized, serialized.length + 1);

        assertThatExceptionOfType(IOException.class)
                .isThrownBy(
                        () -> serializer.deserialize(serializer.getVersion(), extraBytesSerialized))
                .withMessageContaining("Unexpected trailing bytes when deserializing.");
    }

    private List<DynamoDBStreamsShardSplitWithAssignmentStatus> getSplits(
            IntStream finishedShardIdRange,
            IntStream assignedShardIdRange,
            IntStream unassignedShardIdRange) {
        Stream<DynamoDBStreamsShardSplitWithAssignmentStatus> finishedSplits =
                finishedShardIdRange
                        .mapToObj(TestUtil::generateShardId)
                        .map(TestUtil::getTestSplit)
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.FINISHED));
        Stream<DynamoDBStreamsShardSplitWithAssignmentStatus> assignedSplits =
                assignedShardIdRange
                        .mapToObj(TestUtil::generateShardId)
                        .map(TestUtil::getTestSplit)
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.ASSIGNED));
        Stream<DynamoDBStreamsShardSplitWithAssignmentStatus> unassignedSplits =
                unassignedShardIdRange
                        .mapToObj(TestUtil::generateShardId)
                        .map(TestUtil::getTestSplit)
                        .map(
                                split ->
                                        new DynamoDBStreamsShardSplitWithAssignmentStatus(
                                                split, SplitAssignmentStatus.UNASSIGNED));
        return Stream.of(finishedSplits, assignedSplits, unassignedSplits)
                .flatMap(Function.identity())
                .collect(Collectors.toList());
    }

    private static class WrongVersionStateSerializer
            extends DynamoDbStreamsSourceEnumeratorStateSerializer {

        public WrongVersionStateSerializer(DynamoDbStreamsShardSplitSerializer splitSerializer) {
            super(splitSerializer);
        }

        @Override
        public int getVersion() {
            return -1;
        }
    }

    private static class WrongVersionSplitSerializer extends DynamoDbStreamsShardSplitSerializer {

        @Override
        public int getVersion() {
            return -1;
        }
    }
}
