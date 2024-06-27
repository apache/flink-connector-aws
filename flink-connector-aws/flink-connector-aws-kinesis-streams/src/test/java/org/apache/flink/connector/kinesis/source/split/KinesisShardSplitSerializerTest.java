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

package org.apache.flink.connector.kinesis.source.split;

import org.apache.flink.core.io.VersionMismatchException;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.generateShardId;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisShardSplitSerializerTest {

    @ParameterizedTest
    @MethodSource("provideKinesisShardSplits")
    void testSerializeAndDeserializeEverythingSpecified(KinesisShardSplit initialSplit)
            throws Exception {
        KinesisShardSplitSerializer serializer = new KinesisShardSplitSerializer();

        byte[] serialized = serializer.serialize(initialSplit);
        KinesisShardSplit deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedSplit).usingRecursiveComparison().isEqualTo(initialSplit);
    }

    private static Stream<KinesisShardSplit> provideKinesisShardSplits() {
        return Stream.of(
                getTestSplit(),
                getTestSplit(generateShardId(2), Collections.singleton(generateShardId(1))),
                getTestSplit(
                        generateShardId(5),
                        new HashSet<>(Arrays.asList(generateShardId(1), generateShardId(2)))),
                getTestSplit(StartingPosition.fromStart()),
                getTestSplit(StartingPosition.continueFromSequenceNumber("some-sequence-number")),
                getTestSplit(StartingPosition.fromTimestamp(Instant.ofEpochMilli(1683817847000L))));
    }

    @Test
    void testDeserializeVersion0() throws Exception {
        final KinesisShardSplitSerializer serializer = new KinesisShardSplitSerializer();

        final KinesisShardSplit initialSplit =
                new KinesisShardSplit(
                        STREAM_ARN,
                        generateShardId(10),
                        StartingPosition.continueFromSequenceNumber("some-sequence-number"),
                        new HashSet<>(Arrays.asList(generateShardId(2), generateShardId(5))));

        byte[] oldSerializedState = serializer.serializeV0(initialSplit);
        KinesisShardSplit deserializedSplit = serializer.deserialize(0, oldSerializedState);

        assertThat(deserializedSplit)
                .usingRecursiveComparison(
                        RecursiveComparisonConfiguration.builder()
                                .withIgnoredFields("parentShardIds")
                                .build())
                .isEqualTo(initialSplit);
        assertThat(deserializedSplit.getParentShardIds()).isNotNull().matches(Set::isEmpty);
    }

    @Test
    void testDeserializeWrongVersion() throws Exception {
        final KinesisShardSplit initialSplit =
                getTestSplit(StartingPosition.fromTimestamp(Instant.now()));

        KinesisShardSplitSerializer serializer = new KinesisShardSplitSerializer();
        KinesisShardSplitSerializer wrongVersionSerializer = new WrongVersionSerializer();

        byte[] serialized = wrongVersionSerializer.serialize(initialSplit);
        assertThatExceptionOfType(VersionMismatchException.class)
                .isThrownBy(
                        () ->
                                serializer.deserialize(
                                        wrongVersionSerializer.getVersion(), serialized))
                .withMessageContaining(
                        "Trying to deserialize KinesisShardSplit serialized with unsupported version ")
                .withMessageContaining(String.valueOf(serializer.getVersion()))
                .withMessageContaining(String.valueOf(wrongVersionSerializer.getVersion()));
    }

    private static class WrongVersionSerializer extends KinesisShardSplitSerializer {
        @Override
        public int getVersion() {
            return -1;
        }
    }
}
