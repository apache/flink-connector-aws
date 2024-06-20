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

package org.apache.flink.connector.dynamodb.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.dynamodb.source.serialization.DynamoDbStreamsDeserializationSchema;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.utils.ImmutableMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class DynamoDbStreamsRecordEmitterTest {

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    @Test
    void testEmitRecord() throws Exception {
        final Instant startTime = Instant.now();
        List<Record> inputRecords =
                Stream.of(
                                Record.builder()
                                        .dynamodb(
                                                StreamRecord.builder()
                                                        .approximateCreationDateTime(startTime)
                                                        .keys(
                                                                ImmutableMap.of(
                                                                        "ForumName",
                                                                        AttributeValue.builder()
                                                                                .s("DynamoDB")
                                                                                .build()))
                                                        .newImage(
                                                                ImmutableMap.of(
                                                                        "quantity",
                                                                        AttributeValue.builder()
                                                                                .n("50")
                                                                                .build()))
                                                        .sequenceNumber("300000000000000499659")
                                                        .sizeBytes(41L)
                                                        .streamViewType("NEW_AND_OLD_IMAGES")
                                                        .build())
                                        .eventID("4b25bd0da9a181a155114127e4837252")
                                        .eventName("INSERT")
                                        .eventSource("aws:dynamodb")
                                        .eventVersion("1.0")
                                        .build(),
                                Record.builder()
                                        .dynamodb(
                                                StreamRecord.builder()
                                                        .approximateCreationDateTime(
                                                                startTime.plusSeconds(10))
                                                        .keys(
                                                                ImmutableMap.of(
                                                                        "ForumName",
                                                                        AttributeValue.builder()
                                                                                .s("DynamoDB")
                                                                                .build()))
                                                        .newImage(
                                                                ImmutableMap.of(
                                                                        "quantity",
                                                                        AttributeValue.builder()
                                                                                .n("20")
                                                                                .build()))
                                                        .oldImage(
                                                                ImmutableMap.of(
                                                                        "quantity",
                                                                        AttributeValue.builder()
                                                                                .n("50")
                                                                                .build()))
                                                        .sequenceNumber("400000000000000499660")
                                                        .sizeBytes(41L)
                                                        .streamViewType("NEW_AND_OLD_IMAGES")
                                                        .build())
                                        .eventID("e2fd9c34eff2d779b297b26f5fef4206")
                                        .eventName("MODIFY")
                                        .eventSource("aws:dynamodb")
                                        .eventVersion("1.0")
                                        .build(),
                                Record.builder()
                                        .dynamodb(
                                                StreamRecord.builder()
                                                        .approximateCreationDateTime(
                                                                startTime.plusSeconds(20))
                                                        .keys(
                                                                ImmutableMap.of(
                                                                        "ForumName",
                                                                        AttributeValue.builder()
                                                                                .s("DynamoDB")
                                                                                .build()))
                                                        .oldImage(
                                                                ImmutableMap.of(
                                                                        "quantity",
                                                                        AttributeValue.builder()
                                                                                .n("20")
                                                                                .build()))
                                                        .sequenceNumber("500000000000000499661")
                                                        .sizeBytes(41L)
                                                        .streamViewType("NEW_AND_OLD_IMAGES")
                                                        .build())
                                        .eventID("740280c73a3df7842edab3548a1b08ad")
                                        .eventName("REMOVE")
                                        .eventSource("aws:dynamodb")
                                        .eventVersion("1.0")
                                        .build())
                        .collect(Collectors.toList());
        final StartingPosition expectedStartingPosition =
                StartingPosition.continueFromSequenceNumber("500000000000000499661");
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final DynamoDbStreamsShardSplitState splitState = getTestSplitState();

        DynamoDbStreamsRecordEmitter<String> emitter =
                new DynamoDbStreamsRecordEmitter<>(new RecordDeserializationSchema());
        for (Record record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        List<String> emittedRecords = output.emittedRecords;
        assertThat(emittedRecords)
                .containsAll(
                        Arrays.asList(
                                "4b25bd0da9a181a155114127e4837252",
                                "e2fd9c34eff2d779b297b26f5fef4206",
                                "740280c73a3df7842edab3548a1b08ad"));
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(),
                        startTime.plusSeconds(10).toEpochMilli(),
                        startTime.plusSeconds(20).toEpochMilli());
        assertThat(splitState.getNextStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(expectedStartingPosition);
    }

    private static class CapturingSourceOutput<T> implements SourceOutput<T> {

        private final List<T> emittedRecords = new ArrayList<>();
        private final List<Long> emittedTimestamps = new ArrayList<>();

        @Override
        public void collect(T record) {
            emittedRecords.add(record);
        }

        @Override
        public void collect(T record, long timestamp) {
            emittedRecords.add(record);
            emittedTimestamps.add(timestamp);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markActive() {
            throw new UnsupportedOperationException();
        }

        public List<T> getEmittedRecords() {
            return emittedRecords;
        }

        public List<Long> getEmittedTimestamps() {
            return emittedTimestamps;
        }
    }

    private static class RecordDeserializationSchema
            implements DynamoDbStreamsDeserializationSchema<String> {

        @Override
        public void deserialize(
                Record record, String stream, String shardId, Collector<String> output)
                throws IOException {
            STRING_SCHEMA.deserialize(record.eventID().getBytes(StandardCharsets.UTF_8), output);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
