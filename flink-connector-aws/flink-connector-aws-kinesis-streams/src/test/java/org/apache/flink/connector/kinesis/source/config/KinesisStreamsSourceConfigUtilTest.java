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

package org.apache.flink.connector.kinesis.source.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisStreamsSourceConfigUtilTest {

    @Test
    void testParseStreamTimestampUsingDefaultFormat() throws Exception {
        String timestamp = "2023-04-13T09:18:00.0+01:00";
        Instant expectedTimestamp =
                new SimpleDateFormat(STREAM_TIMESTAMP_DATE_FORMAT.defaultValue())
                        .parse(timestamp)
                        .toInstant();

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, timestamp);

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                sourceConfig))
                .contains(expectedTimestamp);
    }

    @Test
    void testParseStreamTimestampUsingCustomFormat() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm";
        String timestamp = "2023-04-13T09:23";
        Instant expectedTimestamp = new SimpleDateFormat(format).parse(timestamp).toInstant();

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, timestamp);
        sourceConfig.set(STREAM_TIMESTAMP_DATE_FORMAT, format);

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                sourceConfig))
                .contains(expectedTimestamp);
    }

    @Test
    void testParseStreamTimestampEpoch() {
        long epochMillis = 1681910583745L;
        Instant expectedTimestamp = Instant.ofEpochMilli(epochMillis);

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, String.valueOf(epochMillis / 1000.0));

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                sourceConfig))
                .contains(expectedTimestamp);
    }

    @Test
    void testParseStreamTimestampTimestampNotSpecified() {
        Configuration sourceConfig = new Configuration();

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                sourceConfig))
                .isEmpty();
    }

    @Test
    void testParseStreamTimestampParseError() {
        String badTimestamp = "badTimestamp";

        Configuration sourceConfig = new Configuration();
        sourceConfig.set(STREAM_INITIAL_TIMESTAMP, badTimestamp);

        assertThatExceptionOfType(NumberFormatException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                        sourceConfig));
    }
}
