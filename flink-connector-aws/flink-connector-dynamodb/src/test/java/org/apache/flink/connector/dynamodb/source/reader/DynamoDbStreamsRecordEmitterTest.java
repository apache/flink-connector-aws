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
import org.apache.flink.connector.dynamodb.source.serialization.RecordObjectMapper;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamoDbStreamsRecordEmitterTest {

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    private List<Record> getDynamoDbStreamsRecords(String[] sequenceNumbers) {
        final Instant startTime = Instant.now();
        return Stream.of(
                Record.builder()
                        .dynamodb(
                                StreamRecord.builder()
                                        .approximateCreationDateTime(startTime)
                                        .keys(
                                                ImmutableMap.of("ForumName", AttributeValue.builder().s("DynamoDB").build())
                                        )
                                        .newImage(
                                                ImmutableMap.of("quantity", AttributeValue.builder().n("50").build())
                                        )
                                        .sequenceNumber(sequenceNumbers[0])
                                        .sizeBytes(41L)
                                        .streamViewType("NEW_AND_OLD_IMAGES")
                                        .build()
                        )
                        .eventID("4b25bd0da9a181a155114127e4837252")
                        .eventName("INSERT")
                        .eventSource("aws:dynamodb")
                        .eventVersion("1.0")
                        .build(),
                Record.builder()
                        .dynamodb(
                                StreamRecord.builder()
                                        .approximateCreationDateTime(startTime.plusSeconds(10))
                                        .keys(
                                                ImmutableMap.of("ForumName", AttributeValue.builder().s("DynamoDB").build())
                                        )
                                        .newImage(
                                                ImmutableMap.of("quantity", AttributeValue.builder().n("20").build())
                                        )
                                        .oldImage(
                                                ImmutableMap.of("quantity", AttributeValue.builder().n("50").build())
                                        )
                                        .sequenceNumber(sequenceNumbers[1])
                                        .sizeBytes(41L)
                                        .streamViewType("NEW_AND_OLD_IMAGES")
                                        .build()
                        )
                        .eventID("e2fd9c34eff2d779b297b26f5fef4206")
                        .eventName("MODIFY")
                        .eventSource("aws:dynamodb")
                        .eventVersion("1.0")
                        .build(),
                Record.builder()
                        .dynamodb(
                                StreamRecord.builder()
                                        .approximateCreationDateTime(startTime.plusSeconds(20))
                                        .keys(
                                                ImmutableMap.of("ForumName", AttributeValue.builder().s("DynamoDB").build())
                                        )
                                        .oldImage(
                                                ImmutableMap.of("quantity", AttributeValue.builder().n("20").build())
                                        )
                                        .sequenceNumber(sequenceNumbers[2])
                                        .sizeBytes(41L)
                                        .streamViewType("NEW_AND_OLD_IMAGES")
                                        .build()
                        )
                        .eventID("740280c73a3df7842edab3548a1b08ad")
                        .eventName("REMOVE")
                        .eventSource("aws:dynamodb")
                        .eventVersion("1.0")
                        .build()
        ).collect(Collectors.toList());
    }

    @Test
    void testEmitRecord() throws Exception {
        final Instant startTime = Instant.now();
        List<Record> inputRecords = getDynamoDbStreamsRecords(new String[]{"300000000000000499659", "400000000000000499660", "500000000000000499661"});
        final StartingPosition expectedStartingPosition =
                StartingPosition.continueFromSequenceNumber("500000000000000499661");
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final DynamoDbStreamsShardSplitState splitState = getTestSplitState();

        DynamoDbStreamsRecordEmitter<String> emitter =
                new DynamoDbStreamsRecordEmitter<>(DynamoDbStreamsDeserializationSchema.of(STRING_SCHEMA));
        for (Record record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        List<String> emittedRecords = output.emittedRecords;
        assertTrue(emittedRecords.get(0).contains("4b25bd0da9a181a155114127e4837252"));
        assertTrue(emittedRecords.get(1).contains("e2fd9c34eff2d779b297b26f5fef4206"));
        assertTrue(emittedRecords.get(2).contains("740280c73a3df7842edab3548a1b08ad"));
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(),
                        startTime.plusSeconds(10).toEpochMilli(),
                        startTime.plusSeconds(20).toEpochMilli());
        assertThat(splitState.getNextStartingPosition())
                .usingRecursiveComparison()
                .isEqualTo(expectedStartingPosition);
    }

    @Test
    void testEmitRecordBasedOnSequenceNumber() throws Exception {
        final Instant startTime = Instant.now();
        List<Record> inputRecords = getDynamoDbStreamsRecords(new String[] {"emit", "do-not-emit", "emit"});
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final DynamoDbStreamsShardSplitState splitState = getTestSplitState();

        DynamoDbStreamsRecordEmitter<String> emitter =
                new DynamoDbStreamsRecordEmitter<>(new SequenceNumberBasedDeserializationSchema());
        for (Record record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        List<String> emittedRecords = output.emittedRecords;
        assertThat(emittedRecords.size()).isEqualTo(2);
        assertTrue(emittedRecords.get(0).contains("4b25bd0da9a181a155114127e4837252"));
        assertTrue(emittedRecords.get(1).contains("740280c73a3df7842edab3548a1b08ad"));
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(), startTime.plusSeconds(20).toEpochMilli());
    }

    @Test
    void testEmitRecordWithMetadata() throws Exception {
        final Instant startTime = Instant.now();
        List<Record> inputRecords = getDynamoDbStreamsRecords(new String[]{"300000000000000499659", "400000000000000499660", "500000000000000499661"});
        final CapturingSourceOutput<String> output = new CapturingSourceOutput<>();
        final DynamoDbStreamsShardSplitState splitState = getTestSplitState();

        DynamoDbStreamsRecordEmitter<String> emitter =
                new DynamoDbStreamsRecordEmitter<>(
                        new AssertRecordMetadataDeserializationSchema(
                                splitState.getStreamArn(), splitState.getShardId()));
        for (Record record : inputRecords) {
            emitter.emitRecord(record, output, splitState);
        }

        List<String> emittedRecords = output.emittedRecords;
        assertTrue(emittedRecords.get(0).contains("4b25bd0da9a181a155114127e4837252"));
        assertTrue(emittedRecords.get(1).contains("e2fd9c34eff2d779b297b26f5fef4206"));
        assertTrue(emittedRecords.get(2).contains("740280c73a3df7842edab3548a1b08ad"));
        assertThat(output.getEmittedTimestamps())
                .containsExactly(
                        startTime.toEpochMilli(),
                        startTime.plusSeconds(10).toEpochMilli(),
                        startTime.plusSeconds(20).toEpochMilli());
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

    private static class SequenceNumberBasedDeserializationSchema
            implements DynamoDbStreamsDeserializationSchema<String> {

        @Override
        public void deserialize(
                Record record, String stream, String shardId, Collector<String> output)
                throws IOException {
            if (Objects.equals(record.dynamodb().sequenceNumber(), "emit")) {
                STRING_SCHEMA.deserialize(new RecordObjectMapper().writeValueAsString(record).getBytes(StandardCharsets.UTF_8), output);
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    private static class AssertRecordMetadataDeserializationSchema
            implements DynamoDbStreamsDeserializationSchema<String> {
        private final String expectedStreamArn;
        private final String expectedShardId;

        private AssertRecordMetadataDeserializationSchema(
                String expectedStreamArn, String expectedShardId) {
            this.expectedStreamArn = expectedStreamArn;
            this.expectedShardId = expectedShardId;
        }

        @Override
        public void deserialize(
                Record record, String stream, String shardId, Collector<String> output)
                throws IOException {
            assertThat(stream).isEqualTo(expectedStreamArn);
            assertThat(shardId).isEqualTo(expectedShardId);
            STRING_SCHEMA.deserialize(new RecordObjectMapper().writeValueAsString(record).getBytes(StandardCharsets.UTF_8), output);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }
}
