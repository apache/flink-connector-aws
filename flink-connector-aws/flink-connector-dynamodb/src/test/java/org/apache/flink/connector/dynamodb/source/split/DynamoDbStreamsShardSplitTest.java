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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class DynamoDbStreamsShardSplitTest {

    private static final String STREAM_ARN =
            "arn:aws:dynamodb:us-east-1:1231231230:table/test/stream/2024-01-01T00:00:00.826";
    private static final String SHARD_ID = "shardId-000000000002";
    private static final StartingPosition STARTING_POSITION = StartingPosition.fromStart();

    @Test
    void testStreamArnNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new DynamoDbStreamsShardSplit(null, SHARD_ID, STARTING_POSITION))
                .withMessageContaining("streamArn cannot be null");
    }

    @Test
    void testShardIdNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () -> new DynamoDbStreamsShardSplit(STREAM_ARN, null, STARTING_POSITION))
                .withMessageContaining("shardId cannot be null");
    }

    @Test
    void testStartingPositionNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> new DynamoDbStreamsShardSplit(STREAM_ARN, SHARD_ID, null))
                .withMessageContaining("startingPosition cannot be null");
    }

    @Test
    void testEquals() {
        EqualsVerifier.forClass(DynamoDbStreamsShardSplit.class).verify();
    }
}
