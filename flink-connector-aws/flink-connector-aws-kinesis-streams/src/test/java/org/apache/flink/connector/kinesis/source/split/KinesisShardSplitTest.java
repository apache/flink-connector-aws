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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisShardSplitTest {

    private static final String STREAM_ARN =
            "arn:aws:kinesis:us-east-1:290038087681:stream/keeneses_stream";
    private static final String SHARD_ID = "shardId-000000000002";
    private static final StartingPosition STARTING_POSITION = StartingPosition.fromStart();
    private static final Set<String> PARENT_SHARD_IDS = Collections.emptySet();

    @Test
    void testStreamArnNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new KinesisShardSplit(
                                        null, SHARD_ID, STARTING_POSITION, PARENT_SHARD_IDS))
                .withMessageContaining("streamArn cannot be null");
    }

    @Test
    void testShardIdNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                new KinesisShardSplit(
                                        STREAM_ARN, null, STARTING_POSITION, PARENT_SHARD_IDS))
                .withMessageContaining("shardId cannot be null");
    }

    @Test
    void testStartingPositionNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () -> new KinesisShardSplit(STREAM_ARN, SHARD_ID, null, PARENT_SHARD_IDS))
                .withMessageContaining("startingPosition cannot be null");
    }

    @Test
    void testParentShardIdsNull() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () -> new KinesisShardSplit(STREAM_ARN, SHARD_ID, STARTING_POSITION, null))
                .withMessageContaining("parentShardIds cannot be null");
    }

    @Test
    void testEquals() {
        EqualsVerifier.forClass(KinesisShardSplit.class).verify();
    }
}
