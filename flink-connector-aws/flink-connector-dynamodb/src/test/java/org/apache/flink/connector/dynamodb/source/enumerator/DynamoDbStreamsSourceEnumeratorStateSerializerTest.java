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

import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitSerializer;
import org.apache.flink.core.io.VersionMismatchException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.copyOf;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class DynamoDbStreamsSourceEnumeratorStateSerializerTest {

    @Test
    void testSerializeAndDeserializeEverythingSpecified() throws Exception {
        Set<DynamoDbStreamsShardSplit> unassignedSplits =
                Stream.of(getTestSplit(generateShardId(1)), getTestSplit(generateShardId(2)))
                        .collect(Collectors.toSet());
        String lastSeenShardId = "shardId-000000000002";
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(unassignedSplits, lastSeenShardId);

        DynamoDbStreamsShardSplitSerializer splitSerializer =
                new DynamoDbStreamsShardSplitSerializer();
        DynamoDbStreamsSourceEnumeratorStateSerializer serializer =
                new DynamoDbStreamsSourceEnumeratorStateSerializer(splitSerializer);

        byte[] serialized = serializer.serialize(initialState);
        DynamoDbStreamsSourceEnumeratorState deserializedState =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedState).usingRecursiveComparison().isEqualTo(initialState);
    }

    @Test
    void testDeserializeWithWrongVersionStateSerializer() throws Exception {
        Set<DynamoDbStreamsShardSplit> unassignedSplits =
                Stream.of(getTestSplit(generateShardId(1)), getTestSplit(generateShardId(2)))
                        .collect(Collectors.toSet());
        String lastSeenShardId = "shardId-000000000002";
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(unassignedSplits, lastSeenShardId);

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
        Set<DynamoDbStreamsShardSplit> unassignedSplits =
                Stream.of(getTestSplit(generateShardId(1)), getTestSplit(generateShardId(2)))
                        .collect(Collectors.toSet());
        String lastSeenShardId = "shardId-000000000002";
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(unassignedSplits, lastSeenShardId);

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
                .withMessageContaining(String.valueOf(serializer.getVersion()))
                .withMessageContaining(String.valueOf(wrongVersionStateSerializer.getVersion()));
    }

    @Test
    void testSerializeWithTrailingBytes() throws Exception {
        Set<String> completedShardIds =
                Stream.of("shardId-000000000001", "shardId-000000000002", "shardId-000000000003")
                        .collect(Collectors.toSet());
        Set<DynamoDbStreamsShardSplit> unassignedSplits =
                Stream.of(getTestSplit(generateShardId(1)), getTestSplit(generateShardId(2)))
                        .collect(Collectors.toSet());
        String lastSeenShardId = "shardId-000000000002";
        DynamoDbStreamsSourceEnumeratorState initialState =
                new DynamoDbStreamsSourceEnumeratorState(unassignedSplits, lastSeenShardId);

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
