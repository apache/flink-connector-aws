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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;

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
     * The value is parsed using the configured {@link
     * DynamodbStreamsSourceConfigConstants#STREAM_INITIAL_TIMESTAMP_FORMAT}; if that fails it is
     * interpreted as a (possibly fractional) epoch-second value.
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
        try {
            return new SimpleDateFormat(format).parse(timestampString).toInstant();
        } catch (ParseException | IllegalArgumentException e) {
            // Fall back to interpreting the value as epoch seconds (may be fractional).
            try {
                return Instant.ofEpochMilli((long) (Double.parseDouble(timestampString) * 1000));
            } catch (NumberFormatException nfe) {
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
        }
    }

    /**
     * Validates the source configuration, failing fast at startup so that a misconfigured job does
     * not fail later during shard discovery or reading. When the initial position is AT_TIMESTAMP,
     * the timestamp must be present, parseable, and not in the future: a future timestamp is
     * rejected by GetShardIterator, which would otherwise make the job restart repeatedly until the
     * clock passes it.
     *
     * @param sourceConfig the configuration to validate
     */
    public static void validateStreamSourceConfiguration(Configuration sourceConfig) {
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
        }
    }
}
