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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Constants to be used with the KinesisStreamsSource. */
@Experimental
public class KinesisStreamsSourceConfigConstants {
    /** Marks the initial position to use when reading from the Kinesis stream. */
    public enum InitialPosition {
        LATEST,
        TRIM_HORIZON,
        AT_TIMESTAMP
    }

    public static final ConfigOption<InitialPosition> STREAM_INITIAL_POSITION =
            ConfigOptions.key("flink.stream.initpos")
                    .enumType(InitialPosition.class)
                    .defaultValue(InitialPosition.LATEST)
                    .withDescription("The initial position to start reading Kinesis streams.");

    public static final ConfigOption<String> STREAM_INITIAL_TIMESTAMP =
            ConfigOptions.key("flink.stream.initpos.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The initial timestamp at which to start reading from the Kinesis stream. This is used when AT_TIMESTAMP is configured for the STREAM_INITIAL_POSITION.");

    public static final ConfigOption<String> STREAM_TIMESTAMP_DATE_FORMAT =
            ConfigOptions.key("flink.stream.initpos.timestamp.format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                    .withDescription(
                            "The date format used to parse the initial timestamp at which to start reading from the Kinesis stream. This is used when AT_TIMESTAMP is configured for the STREAM_INITIAL_POSITION.");

    public static final ConfigOption<Long> SHARD_DISCOVERY_INTERVAL_MILLIS =
            ConfigOptions.key("flink.shard.discovery.intervalmillis")
                    .longType()
                    .defaultValue(10000L)
                    .withDescription("The interval between each attempt to discover new shards.");
}
