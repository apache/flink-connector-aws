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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplitState;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.runtime.operators.testutils.TestData;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplit;
import static org.apache.flink.connector.dynamodb.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class DynamoDbStreamsSourceReaderTest {

    @Test
    void testInitializedState() throws Exception {
        DynamoDbStreamsSourceReader<TestData> sourceReader =
                new DynamoDbStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        DynamoDbStreamsShardSplit split = getTestSplit();
        assertThat(sourceReader.initializedState(split))
                .usingRecursiveComparison()
                .isEqualTo(new DynamoDbStreamsShardSplitState(split));
    }

    @Test
    void testToSplitType() throws Exception {
        DynamoDbStreamsSourceReader<TestData> sourceReader =
                new DynamoDbStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        DynamoDbStreamsShardSplitState splitState = getTestSplitState();
        String splitId = splitState.getSplitId();
        assertThat(sourceReader.toSplitType(splitId, splitState))
                .usingRecursiveComparison()
                .isEqualTo(splitState.getDynamoDbStreamsShardSplit());
    }

    @Test
    void testOnSplitFinishedIsNoOp() throws Exception {
        DynamoDbStreamsSourceReader<TestData> sourceReader =
                new DynamoDbStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        assertThatNoException()
                .isThrownBy(() -> sourceReader.onSplitFinished(Collections.emptyMap()));
    }
}
