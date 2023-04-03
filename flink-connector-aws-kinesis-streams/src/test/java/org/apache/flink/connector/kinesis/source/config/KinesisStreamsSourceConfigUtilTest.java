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

import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class KinesisStreamsSourceConfigUtilTest {

    @Test
    void testParseStreamTimestampUsingDefaultFormat() throws Exception {
        String timestamp = "2023-04-13T09:18:00.0+01:00";
        Date expectedTimestamp =
                new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                consumerProperties))
                .isEqualTo(expectedTimestamp);
    }

    @Test
    void testParseStreamTimestampUsingCustomFormat() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm";
        String timestamp = "2023-04-13T09:23";
        Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
        consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                consumerProperties))
                .isEqualTo(expectedTimestamp);
    }

    @Test
    void testParseStreamTimestampEpoch() {
        long epoch = 1681910583L;
        Date expectedTimestamp = new Date(epoch * 1000);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, String.valueOf(epoch));

        assertThat(
                        KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                consumerProperties))
                .isEqualTo(expectedTimestamp);
    }

    @Test
    void testParseStreamTimestampParseError() {
        String badTimestamp = "badTimestamp";

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, badTimestamp);

        assertThatExceptionOfType(NumberFormatException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                        consumerProperties));
    }

    @Test
    void testParseStreamTimestampTimestampNotSpecified() {
        Properties consumerProperties = new Properties();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                KinesisStreamsSourceConfigUtil.parseStreamTimestampStartingPosition(
                                        consumerProperties));
    }
}
