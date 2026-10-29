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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_POSITION;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.dynamodb.source.config.DynamodbStreamsSourceConfigConstants.STREAM_INITIAL_TIMESTAMP_FORMAT;

/** Utility functions to use with {@link DynamodbStreamsSourceConfigConstants}. */
@Internal
public class DynamodbStreamsSourceConfigUtil {

    private DynamodbStreamsSourceConfigUtil() {
        // private constructor to prevent initialization of utility class.
    }

    /**
     * Parses the timestamp at which to start reading, used for the AT_TIMESTAMP initial position.
     * The value is parsed strictly (non-lenient, whole string) using the configured {@link
     * DynamodbStreamsSourceConfigConstants#STREAM_INITIAL_TIMESTAMP_FORMAT}; if that fails it is
     * interpreted as a non-negative, possibly fractional, epoch-second value.
     *
     * @param sourceConfig the configuration to parse the timestamp from
     * @return the parsed initial timestamp
     */
    public static Instant parseInitialTimestamp(Configuration sourceConfig) {
        String timestampString = sourceConfig.get(STREAM_INITIAL_TIMESTAMP);
        if (timestampString == null) {
            throw new IllegalArgumentException(
                    STREAM_INITIAL_TIMESTAMP.key()
                            + " must be set when "
                            + STREAM_INITIAL_POSITION.key()
                            + " is AT_TIMESTAMP.");
        }
        String format = sourceConfig.get(STREAM_INITIAL_TIMESTAMP_FORMAT);
        Instant parsed = parseWithFormat(timestampString, format);
        if (parsed != null) {
            return parsed;
        }
        // Fall back to interpreting the value as epoch seconds (may be fractional).
        final double epochSeconds;
        try {
            epochSeconds = Double.parseDouble(timestampString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Unable to parse "
                            + STREAM_INITIAL_TIMESTAMP.key()
                            + " value '"
                            + timestampString
                            + "' using format '"
                            + format
                            + "' or as epoch seconds.",
                    e);
        }
        if (!Double.isFinite(epochSeconds) || epochSeconds < 0) {
            throw new IllegalArgumentException(
                    STREAM_INITIAL_TIMESTAMP.key()
                            + " value '"
                            + timestampString
                            + "' must be a date in format '"
                            + format
                            + "' or a non-negative epoch-second value.");
        }
        return Instant.ofEpochMilli((long) (epochSeconds * 1000));
    }

    /** Parses the whole string with the format, non-leniently; null if it does not match. */
    @Nullable
    private static Instant parseWithFormat(String timestampString, String format) {
        final SimpleDateFormat dateFormat;
        try {
            dateFormat = new SimpleDateFormat(format);
        } catch (IllegalArgumentException e) {
            return null;
        }
        dateFormat.setLenient(false);
        ParsePosition position = new ParsePosition(0);
        Date date = dateFormat.parse(timestampString, position);
        if (date == null || position.getIndex() != timestampString.length()) {
            return null;
        }
        return date.toInstant();
    }

    /**
     * Validates the source configuration, failing fast at startup so that a misconfigured job does
     * not fail later during shard discovery or reading. When the initial position is AT_TIMESTAMP,
     * the timestamp must be present, parseable, and not in the future: a future timestamp is
     * rejected by GetShardIterator, which would otherwise make the job restart repeatedly until the
     * clock passes it.
     *
     * @param sourceConfig the configuration to validate
     * @return the validated initial timestamp when the initial position is AT_TIMESTAMP, otherwise
     *     null. The source passes this value to the enumerator, so the timestamp that was validated
     *     is the one used.
     */
    @Nullable
    public static Instant validateStreamSourceConfiguration(Configuration sourceConfig) {
        Preconditions.checkNotNull(sourceConfig, "Config cannot be null");
        if (DynamodbStreamsSourceConfigConstants.InitialPosition.AT_TIMESTAMP.equals(
                sourceConfig.get(STREAM_INITIAL_POSITION))) {
            // Parsing throws for a missing or invalid timestamp, surfacing the error at startup.
            final Instant timestamp = parseInitialTimestamp(sourceConfig);
            final Instant now = Instant.now();
            if (timestamp.isAfter(now)) {
                throw new IllegalArgumentException(
                        STREAM_INITIAL_TIMESTAMP.key()
                                + " "
                                + timestamp
                                + " is in the future (current time "
                                + now
                                + "). A future timestamp is not allowed; supply a timestamp at or"
                                + " before the current time.");
            }
            return timestamp;
        }
        return null;
    }
}
