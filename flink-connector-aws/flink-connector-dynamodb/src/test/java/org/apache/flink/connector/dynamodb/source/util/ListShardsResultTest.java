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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.generateShard;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests functionality of {@link ListShardsResult} class. */
public class ListShardsResultTest {
    @Test
    void testAddShards() {
        List<Shard> shardList =
                Arrays.asList(
                        generateShard(0, "1100", null, null), generateShard(1, "2100", null, null));
        ListShardsResult listShardsResult = new ListShardsResult();
        listShardsResult.addShards(shardList);
        assertThat(listShardsResult.getShards()).containsExactlyElementsOf(shardList);
    }

    @Test
    void testSetStreamStatus() {
        ListShardsResult listShardsResult = new ListShardsResult();
        listShardsResult.setStreamStatus(StreamStatus.ENABLED);
        assertThat(listShardsResult.getStreamStatus()).isEqualTo(StreamStatus.ENABLED);
    }

    @Test
    void testSetInconsistencyDetected() {
        ListShardsResult listShardsResult = new ListShardsResult();
        listShardsResult.setInconsistencyDetected(true);
        assertThat(listShardsResult.getInconsistencyDetected()).isEqualTo(true);
    }

    @Test
    void testEquals() {
        EqualsVerifier.simple().forClass(ListShardsResult.class).verify();
    }
}
