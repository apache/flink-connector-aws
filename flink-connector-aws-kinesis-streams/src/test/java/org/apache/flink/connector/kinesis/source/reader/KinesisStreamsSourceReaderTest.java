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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.model.TestData;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsSourceReaderTest {

    @Test
    void testInitializedState() throws Exception {
        KinesisStreamsSourceReader<TestData> sourceReader =
                new KinesisStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        KinesisShardSplit split = getTestSplit();
        assertThat(sourceReader.initializedState(split))
                .usingRecursiveComparison()
                .isEqualTo(new KinesisShardSplitState(split));
    }

    @Test
    void testToSplitType() throws Exception {
        KinesisStreamsSourceReader<TestData> sourceReader =
                new KinesisStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        KinesisShardSplitState splitState = getTestSplitState();
        String splitId = splitState.getSplitId();
        assertThat(sourceReader.toSplitType(splitId, splitState))
                .usingRecursiveComparison()
                .isEqualTo(splitState.getKinesisShardSplit());
    }

    @Test
    void testOnSplitFinishedIsNoOp() throws Exception {
        KinesisStreamsSourceReader<TestData> sourceReader =
                new KinesisStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        assertThatNoException()
                .isThrownBy(() -> sourceReader.onSplitFinished(Collections.emptyMap()));
    }
}
