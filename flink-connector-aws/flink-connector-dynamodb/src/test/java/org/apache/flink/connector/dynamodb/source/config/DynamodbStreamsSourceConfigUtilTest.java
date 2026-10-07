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

package org.apache.flink.connector.dynamodb.source.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DynamodbStreamsSourceConfigUtil}. */
class DynamodbStreamsSourceConfigUtilTest {

    @Test
    void testParseInitialTimestampWithDefaultFormat() {
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, "2024-05-01T12:00:00.000Z");

        Instant parsed = DynamodbStreamsSourceConfigUtil.parseInitialTimestamp(sourceConfig);

        assertThat(parsed).isEqualTo(Instant.parse("2024-05-01T12:00:00Z"));
    }

    @Test
    void testParseInitialTimestampFallsBackToEpochSeconds() {
        Configuration sourceConfig = new Configuration();
        // Not in the configured date format -> interpreted as epoch seconds.
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, "1704067200");

        Instant parsed = DynamodbStreamsSourceConfigUtil.parseInitialTimestamp(sourceConfig);

        assertThat(parsed).isEqualTo(Instant.ofEpochMilli(1704067200000L));
    }

    @Test
    void testParseInitialTimestampThrowsWhenMissing() {
        Configuration sourceConfig = new Configuration();

        assertThatThrownBy(
                        () -> DynamodbStreamsSourceConfigUtil.parseInitialTimestamp(sourceConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(STREAM_INITIAL_TIMESTAMP.key());
    }

    @Test
    void testValidateFailsFastForAtTimestampWithoutTimestamp() {
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                STREAM_INITIAL_POSITION,
                DynamodbStreamsSourceConfigConstants.InitialPosition.AT_TIMESTAMP);

        assertThatThrownBy(
                        () ->
                                DynamodbStreamsSourceConfigUtil.validateStreamSourceConfiguration(
                                        sourceConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(STREAM_INITIAL_TIMESTAMP.key());
    }

    @Test
    void testValidatePassesForAtTimestampWithTimestamp() {
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                STREAM_INITIAL_POSITION,
                DynamodbStreamsSourceConfigConstants.InitialPosition.AT_TIMESTAMP);
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, "2024-05-01T12:00:00.000Z");

        assertThatNoException()
                .isThrownBy(
                        () ->
                                DynamodbStreamsSourceConfigUtil.validateStreamSourceConfiguration(
                                        sourceConfig));
    }

    @Test
    void testValidateRejectsFutureAtTimestamp() {
        // A future AT_TIMESTAMP must fail fast at source construction, so the job halts instead of
        // restarting on GetShardIterator rejections until the clock passes the timestamp.
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                STREAM_INITIAL_POSITION,
                DynamodbStreamsSourceConfigConstants.InitialPosition.AT_TIMESTAMP);
        sourceConfig.set(
                STREAM_INITIAL_TIMESTAMP,
                String.valueOf(Instant.now().plusSeconds(60).getEpochSecond()));

        assertThatThrownBy(
                        () ->
                                DynamodbStreamsSourceConfigUtil.validateStreamSourceConfiguration(
                                        sourceConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("future")
                .hasMessageContaining(STREAM_INITIAL_TIMESTAMP.key());
    }

    @Test
    void testValidatePassesForCurrentAtTimestamp() {
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                STREAM_INITIAL_POSITION,
                DynamodbStreamsSourceConfigConstants.InitialPosition.AT_TIMESTAMP);
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, String.valueOf(Instant.now().getEpochSecond()));

        assertThatNoException()
                .isThrownBy(
                        () ->
                                DynamodbStreamsSourceConfigUtil.validateStreamSourceConfiguration(
                                        sourceConfig));
    }

    @Test
    void testValidateIsNoOpForNonTimestampPositions() {
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(
                STREAM_INITIAL_POSITION,
                DynamodbStreamsSourceConfigConstants.InitialPosition.TRIM_HORIZON);

        // No timestamp set, but validation must not fail for TRIM_HORIZON/LATEST.
        assertThatNoException()
                .isThrownBy(
                        () ->
                                DynamodbStreamsSourceConfigUtil.validateStreamSourceConfiguration(
                                        sourceConfig));
    }
}
