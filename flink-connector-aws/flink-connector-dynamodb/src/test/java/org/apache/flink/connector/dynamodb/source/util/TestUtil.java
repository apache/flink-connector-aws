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

package org.apache.flink.connector.dynamodb.source.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.connector.dynamodb.source.metrics.MetricConstants;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Utilities class for testing DynamoDbStreams Source. */
public class TestUtil {

    public static final String STREAM_ARN =
            "arn:aws:dynamodb:us-east-1:123456789012:stream/2024-01-01T00:00:00Z";
    public static final String SHARD_ID = "shardId-000000000002";
    public static final String CHILD_SHARD_ID_1 = "shardId-000000000003";
    public static final String CHILD_SHARD_ID_2 = "shardId-000000000004";
    public static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    public static final long MILLIS_BEHIND_LATEST_TEST_VALUE = -1L;
    public static final Duration OLD_SHARD_DURATION = Duration.ofHours(49);

    public static final Duration OLD_INCONSISTENT_SHARD_DURATION = Duration.ofHours(26);

    public static String generateShardId(long shardId) {
        return String.format("shardId-%012d", shardId);
    }

    public static Shard generateShard(
            long shardId, String startSN, String endSN, String parentShardId) {
        return Shard.builder()
                .shardId(generateShardId(shardId))
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber(startSN)
                                .endingSequenceNumber(endSN)
                                .build())
                .build();
    }

    public static Shard generateShard(
            String shardId, String startSN, String endSN, String parentShardId) {
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber(startSN)
                                .endingSequenceNumber(endSN)
                                .build())
                .build();
    }

    public static DynamoDbStreamsShardSplitState getTestSplitState() {
        return new DynamoDbStreamsShardSplitState(getTestSplit());
    }

    public static DynamoDbStreamsShardSplitState getTestSplitState(
            DynamoDbStreamsShardSplit testSplit) {
        return new DynamoDbStreamsShardSplitState(testSplit);
    }

    public static DynamoDbStreamsShardSplit getTestSplit() {
        return getTestSplit(SHARD_ID);
    }

    public static DynamoDbStreamsShardSplit getTestSplitWithChildShards() {
        return getTestSplitWithChildShards(SHARD_ID);
    }

    public static DynamoDbStreamsShardSplit getTestSplitWithChildShards(String shardId) {
        return getTestSplit(
                STREAM_ARN, SHARD_ID, Arrays.asList(CHILD_SHARD_ID_1, CHILD_SHARD_ID_2));
    }

    public static DynamoDbStreamsShardSplit getTestSplit(String shardId) {
        return getTestSplit(STREAM_ARN, shardId);
    }

    public static DynamoDbStreamsShardSplit getTestSplit(String streamArn, String shardId) {
        return new DynamoDbStreamsShardSplit(
                streamArn, shardId, StartingPosition.fromStart(), null);
    }

    public static DynamoDbStreamsShardSplit getTestSplit(
            String streamArn, String shardId, List<String> childShardIds) {
        return new DynamoDbStreamsShardSplit(
                streamArn,
                shardId,
                StartingPosition.fromStart(),
                null,
                childShardIds.stream()
                        .map(
                                childShardId ->
                                        Shard.builder()
                                                .parentShardId(shardId)
                                                .shardId(childShardId)
                                                .sequenceNumberRange(
                                                        SequenceNumberRange.builder()
                                                                .startingSequenceNumber("1234")
                                                                .build())
                                                .build())
                        .collect(Collectors.toList()));
    }

    public static DynamoDbStreamsShardSplit getTestSplit(StartingPosition startingPosition) {
        return new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, startingPosition, null);
    }

    public static ReaderInfo getTestReaderInfo(final int subtaskId) {
        return new ReaderInfo(subtaskId, "some-location");
    }

    public static Record getTestRecord(String data) {
        return Record.builder()
                .dynamodb(
                        StreamRecord.builder()
                                .sequenceNumber("11000000000000000000001")
                                .approximateCreationDateTime(Instant.now())
                                .build())
                .build();
    }

    public static void assertMillisBehindLatest(
            DynamoDbStreamsShardSplit split, long expectedValue, MetricListener metricListener) {
        Arn streamArn = Arn.fromString(split.getStreamArn());

        final Optional<Gauge<Long>> millisBehindLatestGauge =
                metricListener.getGauge(
                        MetricConstants.DYNAMODB_STREAMS_SOURCE_METRIC_GROUP,
                        MetricConstants.ACCOUNT_ID_METRIC_GROUP,
                        streamArn.accountId().get(),
                        MetricConstants.REGION_METRIC_GROUP,
                        streamArn.region().get(),
                        MetricConstants.STREAM_METRIC_GROUP,
                        streamArn.resource().resource(),
                        MetricConstants.SHARD_METRIC_GROUP,
                        split.getShardId(),
                        MetricConstants.MILLIS_BEHIND_LATEST);

        assertThat(millisBehindLatestGauge).isPresent();
        assertThat((long) millisBehindLatestGauge.get().getValue()).isEqualTo(expectedValue);
    }
}
