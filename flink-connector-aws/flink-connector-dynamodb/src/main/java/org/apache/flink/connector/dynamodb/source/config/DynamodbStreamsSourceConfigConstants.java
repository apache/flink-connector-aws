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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Constants to be used with the DynamodbStreamsSource. */
@Experimental
public class DynamodbStreamsSourceConfigConstants {
    /** Marks the initial position to use when reading from the Dynamodb stream. */
    public enum InitialPosition {
        LATEST,
        TRIM_HORIZON,
        AT_TIMESTAMP
    }

    public static final ConfigOption<InitialPosition> STREAM_INITIAL_POSITION =
            ConfigOptions.key("flink.stream.initpos")
                    .enumType(InitialPosition.class)
                    .defaultValue(InitialPosition.LATEST)
                    .withDescription("The initial position to start reading Dynamodb streams.");

    public static final ConfigOption<String> STREAM_INITIAL_TIMESTAMP =
            ConfigOptions.key("flink.stream.initpos.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The initial timestamp at which to start reading from the DynamoDB stream. "
                                    + "Used only when the initial position is AT_TIMESTAMP.");

    public static final ConfigOption<String> STREAM_INITIAL_TIMESTAMP_FORMAT =
            ConfigOptions.key("flink.stream.initpos.timestamp.format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                    .withDescription(
                            "The date format of the initial timestamp. Used to parse "
                                    + "flink.stream.initpos.timestamp when it is not an epoch-second "
                                    + "value. Used only when the initial position is AT_TIMESTAMP.");

    public static final ConfigOption<Duration> SHARD_DISCOVERY_INTERVAL =
            ConfigOptions.key("flink.shard.discovery.intervalmillis")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("The interval between each attempt to discover new shards.");

    public static final ConfigOption<Integer> DYNAMODB_STREAMS_RETRY_COUNT =
            ConfigOptions.key("flink.dynamodbstreams.numretries")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The number of times to retry DynamoDB Streams API call if it returns a retryable exception");

    public static final ConfigOption<Duration> DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MIN_DELAY =
            ConfigOptions.key("flink.dynamodbstreams.backoff.mindelayduration")
                    .durationType()
                    .defaultValue(Duration.ofMillis(100))
                    .withDescription(
                            "The minimum delay for exponential backoff for describestream");

    public static final ConfigOption<Duration> DYNAMODB_STREAMS_EXPONENTIAL_BACKOFF_MAX_DELAY =
            ConfigOptions.key("flink.dynamodbstreams.backoff.maxdelay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000))
                    .withDescription(
                            "The maximum delay for exponential backoff for describestream");

    public static final String BASE_DDB_STREAMS_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) DynamoDb Streams Connector";

    public static final ConfigOption<Duration>
            DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_EMPTY_POLLS =
                    ConfigOptions.key("flink.dynamodbstreams.getrecords.empty.mindelay")
                            .durationType()
                            .defaultValue(Duration.ofMillis(1000))
                            .withDescription(
                                    "The idle time between empty polls for DynamoDB Streams GetRecords API");
    public static final ConfigOption<Duration>
            DYNAMODB_STREAMS_GET_RECORDS_IDLE_TIME_BETWEEN_NON_EMPTY_POLLS =
                    ConfigOptions.key("flink.dynamodbstreams.getrecords.nonempty.mindelay")
                            .durationType()
                            .defaultValue(Duration.ofMillis(250))
                            .withDescription(
                                    "The default idle time between non-empty polls for DynamoDB Streams GetRecords API");

    public static final ConfigOption<Boolean> DYNAMODB_STREAMS_GET_RECORDS_BLOCKING =
            ConfigOptions.key("flink.dynamodbstreams.getrecords.blocking")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, each source reader calls GetRecords on one shard at a time and"
                                    + " waits for the response. If false, each source reader calls"
                                    + " GetRecords on all ready shards without waiting for the"
                                    + " responses, so a slow call on one shard does not delay the"
                                    + " other shards.");

    public static final ConfigOption<Integer> DYNAMODB_STREAMS_GET_RECORDS_MAX_IN_FLIGHT =
            ConfigOptions.key("flink.dynamodbstreams.getrecords.max-in-flight")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The maximum number of non-blocking calls that each source reader has"
                                    + " in progress, counting calls whose responses the reader has"
                                    + " not collected yet. Each GetRecords response holds up to 1 MB,"
                                    + " so this limits the responses a source reader buffers. A paused"
                                    + " split can hold one more response until it resumes. Only used"
                                    + " if flink.dynamodbstreams.getrecords.blocking is false.");

    /**
     * The maximum time that a non-blocking split reader waits for a response in one fetch. The
     * reader also returns when a shard is ready for its next call, or when Flink wakes it up.
     */
    public static final Duration NON_BLOCKING_FETCH_MAX_WAIT = Duration.ofMillis(100);

    public static final int MAX_RETRY_ATTEMPTS_FOR_CHILD_SHARDS = 5;
    public static final long CHILD_SHARD_DISCOVERY_MIN_DELAY_MS = 100;
    public static final long CHILD_SHARD_DISCOVERY_MAX_DELAY_MS = 1000;

    /** DynamoDb Streams identifier for user agent prefix. */
    public static final String DDB_STREAMS_CLIENT_USER_AGENT_PREFIX =
            "aws.dynamodbstreams.client.user-agent-prefix";
}
