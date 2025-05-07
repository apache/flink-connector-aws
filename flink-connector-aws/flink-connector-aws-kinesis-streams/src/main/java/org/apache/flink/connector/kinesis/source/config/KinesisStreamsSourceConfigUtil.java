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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.util.Preconditions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.aws.util.AWSGeneralUtil.validateAwsConfiguration;
import static org.apache.flink.connector.base.table.util.ConfigurationValidatorUtil.validateOptionalDateProperty;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility functions to use with {@link KinesisSourceConfigOptions}. */
@Internal
public class KinesisStreamsSourceConfigUtil {

    private KinesisStreamsSourceConfigUtil() {
        // private constructor to prevent initialization of utility class.
    }

    /**
     * Parses the timestamp in which to start consuming from the stream, from the given
     * configuration.
     *
     * @param sourceConfig the configuration to parse timestamp from
     * @return {@link Optional} containing the initial timestamp if configured, an empty {@link
     *     Optional} otherwise
     */
    public static Optional<Instant> parseStreamTimestampStartingPosition(
            final Configuration sourceConfig) {
        Preconditions.checkNotNull(sourceConfig);
        String timestamp = sourceConfig.get(STREAM_INITIAL_TIMESTAMP);
        if (timestamp == null || timestamp.isEmpty()) {
            return Optional.empty();
        }

        try {
            String format = sourceConfig.get(STREAM_TIMESTAMP_DATE_FORMAT);
            SimpleDateFormat customDateFormat = new SimpleDateFormat(format);
            return Optional.of(customDateFormat.parse(timestamp).toInstant());
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw new IllegalArgumentException(exception);
        } catch (ParseException exception) {
            return Optional.of(Instant.ofEpochMilli((long) (Double.parseDouble(timestamp) * 1000)));
        }
    }

    /**
     * Validate configuration properties for {@link
     * org.apache.flink.connector.kinesis.source.KinesisStreamsSource}.
     */
    public static void validateStreamSourceConfiguration(Configuration config) {
        checkNotNull(config, "Config can not be null");

        Properties consumerProperties = new Properties();
        config.addAllToProperties(consumerProperties);
        validateAwsConfiguration(consumerProperties);

        if (!(config.containsKey(AWSConfigConstants.AWS_REGION)
                || config.containsKey(AWSConfigConstants.AWS_ENDPOINT))) {
            // per validation in AwsClientBuilder
            throw new IllegalArgumentException(
                    String.format(
                            "For KinesisStreamsSource AWS region ('%s') and/or AWS endpoint ('%s') must be set in the config.",
                            AWSConfigConstants.AWS_REGION, AWSConfigConstants.AWS_ENDPOINT));
        }

        if (config.contains(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION)) {
            KinesisSourceConfigOptions.InitialPosition initPosType =
                    config.get(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION);

            // specified initial timestamp in stream when using AT_TIMESTAMP
            if (initPosType == KinesisSourceConfigOptions.InitialPosition.AT_TIMESTAMP) {
                if (!config.contains(STREAM_INITIAL_TIMESTAMP)) {
                    throw new IllegalArgumentException(
                            "Please set value for initial timestamp ('"
                                    + STREAM_INITIAL_TIMESTAMP
                                    + "') when using AT_TIMESTAMP initial position.");
                }
                validateOptionalDateProperty(
                        consumerProperties,
                        String.valueOf(STREAM_INITIAL_TIMESTAMP),
                        config.get(STREAM_TIMESTAMP_DATE_FORMAT),
                        String.format(
                                "Invalid value given for initial timestamp for AT_TIMESTAMP initial position in stream. "
                                        + "Must be a valid format: ('%s') or non-negative double value. For example, 2016-04-04T19:58:46.480-00:00 or 1459799926.480 .",
                                config.get(STREAM_TIMESTAMP_DATE_FORMAT)));
            }
        }
    }
}
