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

/** Constants to be used with the KinesisStreamsSource. */
@Experimental
public class KinesisStreamsSourceConfigConstants {
    /** Marks the initial position to use when reading from the Kinesis stream. */
    public enum InitialPosition {
        LATEST,
        TRIM_HORIZON,
        AT_TIMESTAMP
    }

    /** The initial position to start reading Kinesis streams from. */
    public static final String STREAM_INITIAL_POSITION = "flink.stream.initpos";

    public static final String DEFAULT_STREAM_INITIAL_POSITION = InitialPosition.LATEST.toString();

    /**
     * The initial timestamp to start reading Kinesis stream from (when AT_TIMESTAMP is set for
     * STREAM_INITIAL_POSITION).
     */
    public static final String STREAM_INITIAL_TIMESTAMP = "flink.stream.initpos.timestamp";

    /**
     * The date format of initial timestamp to start reading Kinesis stream from (when AT_TIMESTAMP
     * is set for STREAM_INITIAL_POSITION).
     */
    public static final String STREAM_TIMESTAMP_DATE_FORMAT =
            "flink.stream.initpos.timestamp.format";

    public static final String DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT =
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    /** The interval between each attempt to discover new shards. */
    public static final String SHARD_DISCOVERY_INTERVAL_MILLIS =
            "flink.shard.discovery.intervalmillis";

    public static final long DEFAULT_SHARD_DISCOVERY_INTERVAL_MILLIS = 10000L;
}
