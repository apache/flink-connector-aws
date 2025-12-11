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

package org.apache.flink.connector.dynamodb.source.split;

import org.apache.flink.core.io.VersionMismatchException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.SHARD_ID;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplitWithChildShards;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class DynamoDbStreamsShardSplitSerializerTest {

    @Test
    void testSerializeAndDeserializeEverythingSpecified() throws Exception {
        final DynamoDbStreamsShardSplit initialSplit = getTestSplit();

        DynamoDbStreamsShardSplitSerializer serializer = new DynamoDbStreamsShardSplitSerializer();

        byte[] serialized = serializer.serialize(initialSplit);
        DynamoDbStreamsShardSplit deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedSplit).usingRecursiveComparison().isEqualTo(initialSplit);
    }

    @Test
    void testSerializeAndDeserializeWithChildSplits() throws Exception {
        final DynamoDbStreamsShardSplit initialSplit = getTestSplitWithChildShards();

        DynamoDbStreamsShardSplitSerializer serializer = new DynamoDbStreamsShardSplitSerializer();

        byte[] serialized = serializer.serialize(initialSplit);
        DynamoDbStreamsShardSplit deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedSplit).usingRecursiveComparison().isEqualTo(initialSplit);
    }

    @ParameterizedTest
    @MethodSource("provideStartingPositions")
    void testSerializeAndDeserializeWithStartingPosition(StartingPosition startingPosition)
            throws Exception {
        final DynamoDbStreamsShardSplit initialSplit = getTestSplit(startingPosition);

        DynamoDbStreamsShardSplitSerializer serializer = new DynamoDbStreamsShardSplitSerializer();

        byte[] serialized = serializer.serialize(initialSplit);
        DynamoDbStreamsShardSplit deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedSplit).usingRecursiveComparison().isEqualTo(initialSplit);
    }

    private static Stream<StartingPosition> provideStartingPositions() {
        return Stream.of(
                StartingPosition.fromStart(),
                StartingPosition.continueFromSequenceNumber("some-sequence-number"),
                StartingPosition.latest());
    }

    @Test
    void testDeserializeWrongVersion() throws Exception {
        final DynamoDbStreamsShardSplit initialSplit = getTestSplit(StartingPosition.latest());

        DynamoDbStreamsShardSplitSerializer serializer = new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsShardSplitSerializer wrongVersionSerializer = new WrongVersionSerializer();

        byte[] serialized = serializer.serialize(initialSplit);
        assertThatExceptionOfType(VersionMismatchException.class)
                .isThrownBy(
                        () ->
                                serializer.deserialize(
                                        wrongVersionSerializer.getVersion(), serialized))
                .withMessageContaining(
                        "Trying to deserialize DynamoDbStreamsShardSplit serialized with unsupported version ")
                .withMessageContaining(String.valueOf(wrongVersionSerializer.getVersion()))
                .withMessageContaining(String.valueOf(serializer.getVersion()));
    }

    @Test
    void testSerializeAndDeserializeWithFinishedSplits() throws Exception {
        final DynamoDbStreamsShardSplit initialSplit =
                new DynamoDbStreamsShardSplit(
                        STREAM_ARN,
                        SHARD_ID,
                        StartingPosition.fromStart(),
                        null,
                        true // finished after checkpoint 2
                        );

        DynamoDbStreamsShardSplitSerializer serializer = new DynamoDbStreamsShardSplitSerializer();

        byte[] serialized = serializer.serialize(initialSplit);
        DynamoDbStreamsShardSplit deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedSplit).usingRecursiveComparison().isEqualTo(initialSplit);
        assertThat(deserializedSplit.isFinished()).isTrue();
    }

    @Test
    void testDeserializeVersion0Split() throws Exception {
        // Create a version 0 split (without finishedDuringCheckpoint)
        final DynamoDbStreamsShardSplit initialSplit =
                new DynamoDbStreamsShardSplit(
                        STREAM_ARN, SHARD_ID, StartingPosition.fromStart(), null);

        DynamoDbStreamsShardSplitSerializer serializer =
                new DynamoDbStreamsShardSplitSerializer() {
                    @Override
                    public int getVersion() {
                        return 0;
                    }
                };

        // Serialize with version 0
        byte[] serialized = serializer.serialize(initialSplit);

        // Deserialize with current version
        DynamoDbStreamsShardSplitSerializer currentSerializer =
                new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsShardSplit deserializedSplit = currentSerializer.deserialize(0, serialized);

        // Verify the split was properly deserialized with default unfinished state
        assertThat(deserializedSplit.isFinished()).isFalse();
        assertThat(deserializedSplit)
                .extracting("streamArn", "shardId", "startingPosition", "parentShardId")
                .containsExactly(
                        initialSplit.getStreamArn(),
                        initialSplit.getShardId(),
                        initialSplit.getStartingPosition(),
                        initialSplit.getParentShardId());
    }

    private static class WrongVersionSerializer extends DynamoDbStreamsShardSplitSerializer {
        @Override
        public int getVersion() {
            return -1;
        }
    }
}
