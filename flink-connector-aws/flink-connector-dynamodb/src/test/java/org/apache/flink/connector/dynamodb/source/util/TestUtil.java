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
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.dynamodb.source.split.StartingPosition;

import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

import java.time.Instant;

/** Utilities class for testing DynamoDbStreams Source. */
public class TestUtil {

    public static final String STREAM_ARN =
            "arn:aws:dynamodb:us-east-1:123456789012:stream/2024-01-01T00:00:00Z";
    public static final String SHARD_ID = "shardId-000000000002";
    public static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    public static String generateShardId(int shardId) {
        return String.format("shardId-%012d", shardId);
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

    public static DynamoDbStreamsShardSplit getTestSplit(String shardId) {
        return getTestSplit(STREAM_ARN, shardId);
    }

    public static DynamoDbStreamsShardSplit getTestSplit(String streamArn, String shardId) {
        return new DynamoDbStreamsShardSplit(streamArn, shardId, StartingPosition.fromStart());
    }

    public static DynamoDbStreamsShardSplit getTestSplit(StartingPosition startingPosition) {
        return new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, startingPosition);
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
}
