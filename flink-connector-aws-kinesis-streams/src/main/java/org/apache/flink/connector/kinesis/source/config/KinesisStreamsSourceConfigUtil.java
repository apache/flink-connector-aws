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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;

/** Utility functions to use with {@link KinesisStreamsSourceConfigConstants}. */
@Internal
public class KinesisStreamsSourceConfigUtil {

    private KinesisStreamsSourceConfigUtil() {
        // private constructor to prevent initialization of utility class.
    }

    /**
     * Parses the timestamp in which to start consuming from the stream, from the given properties.
     *
     * @param consumerConfig the properties to parse timestamp from
     * @return the timestamp
     */
    public static Date parseStreamTimestampStartingPosition(final Properties consumerConfig) {
        Preconditions.checkNotNull(consumerConfig);
        String timestamp = consumerConfig.getProperty(STREAM_INITIAL_TIMESTAMP);

        try {
            String format =
                    consumerConfig.getProperty(
                            STREAM_TIMESTAMP_DATE_FORMAT, DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT);
            SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
            return customDateFormat.parse(timestamp);
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new IllegalArgumentException(exception);
        } catch (ParseException exception) {
            return new Date((long) (Double.parseDouble(timestamp) * 1000));
        }
    }
}
