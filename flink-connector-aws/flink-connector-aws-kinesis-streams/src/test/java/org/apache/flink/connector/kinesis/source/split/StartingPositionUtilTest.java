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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.time.Instant;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class StartingPositionUtilTest {

    @Test
    void testTrimHorizonFieldsAreTransferredAccurately() {
        // Given TRIM_HORIZON starting position
        StartingPosition startingPosition = StartingPosition.fromStart();

        // When converting to SdkStartingPosition
        software.amazon.awssdk.services.kinesis.model.StartingPosition sdkStartingPosition =
                StartingPositionUtil.toSdkStartingPosition(startingPosition);

        // Then fields are transferred accurately
        assertThat(sdkStartingPosition.type()).isEqualByComparingTo(ShardIteratorType.TRIM_HORIZON);
    }

    @Test
    void testAtTimestampFieldsAreTransferredAccurately() {
        // Given AT_TIMESTAMP starting position
        StartingPosition startingPosition = StartingPosition.fromTimestamp(Instant.EPOCH);

        // When
        software.amazon.awssdk.services.kinesis.model.StartingPosition sdkStartingPosition =
                StartingPositionUtil.toSdkStartingPosition(startingPosition);

        // Then
        assertThat(sdkStartingPosition.type()).isEqualByComparingTo(ShardIteratorType.AT_TIMESTAMP);
        assertThat(sdkStartingPosition.timestamp()).isEqualTo(Instant.EPOCH);
    }

    @Test
    void testAtSequenceNumberFieldsAreTransferredAccurately() {
        // Given TRIM_HORIZON starting position
        StartingPosition startingPosition =
                StartingPosition.continueFromSequenceNumber("some-sequence-number");

        // When converting to SdkStartingPosition
        software.amazon.awssdk.services.kinesis.model.StartingPosition sdkStartingPosition =
                StartingPositionUtil.toSdkStartingPosition(startingPosition);

        // Then fields are transferred accurately
        assertThat(sdkStartingPosition.type())
                .isEqualByComparingTo(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        assertThat(sdkStartingPosition.sequenceNumber()).isEqualTo("some-sequence-number");
    }
}
