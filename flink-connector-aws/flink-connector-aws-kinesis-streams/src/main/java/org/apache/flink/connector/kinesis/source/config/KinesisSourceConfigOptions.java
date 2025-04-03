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

import java.time.Duration;

/** Constants to be used with the KinesisStreamsSource. */
@Experimental
public class KinesisSourceConfigOptions {
    /** Marks the initial position to use when reading from the Kinesis stream. */
    public enum InitialPosition {
        LATEST,
        TRIM_HORIZON,
        AT_TIMESTAMP
    }

    /** Defines mechanism used to consume records from Kinesis stream. */
    public enum ReaderType {
        POLLING,
        EFO
    }

    /** Defines lifecycle management of EFO consumer on Kinesis stream. */
    public enum ConsumerLifecycle {
        JOB_MANAGED,
        SELF_MANAGED
    }

    public static final ConfigOption<InitialPosition> STREAM_INITIAL_POSITION =
            ConfigOptions.key("source.init.position")
                    .enumType(InitialPosition.class)
                    .defaultValue(InitialPosition.LATEST)
                    .withDescription("The initial position to start reading Kinesis streams.");

    public static final ConfigOption<String> STREAM_INITIAL_TIMESTAMP =
            ConfigOptions.key("source.init.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The initial timestamp at which to start reading from the Kinesis stream. This is used when AT_TIMESTAMP is configured for the STREAM_INITIAL_POSITION.");

    public static final ConfigOption<String> STREAM_TIMESTAMP_DATE_FORMAT =
            ConfigOptions.key("source.init.timestamp.format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                    .withDescription(
                            "The date format used to parse the initial timestamp at which to start reading from the Kinesis stream. This is used when AT_TIMESTAMP is configured for the STREAM_INITIAL_POSITION.");

    public static final ConfigOption<Duration> SHARD_DISCOVERY_INTERVAL =
            ConfigOptions.key("source.shard.discovery.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The interval between each attempt to discover new shards.");

    public static final ConfigOption<Integer> SHARD_GET_RECORDS_MAX =
            ConfigOptions.key("source.shard.get-records.max-record-count")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "The maximum number of records to try to get each time we fetch records from a AWS Kinesis shard");

    public static final ConfigOption<Duration> SHARD_GET_RECORDS_INTERVAL =
            ConfigOptions.key("source.shard.get-records.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "The interval in milliseconds between GetRecords calls");

    public static final ConfigOption<Duration> SHARD_GET_RECORDS_IDLE_SOURCE_INTERVAL =
            ConfigOptions.key("source.shard.get-records.idle-source-interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(250))
                    .withDescription(
                            "The interval in milliseconds between GetRecords calls on an idle source");

    public static final ConfigOption<ReaderType> READER_TYPE =
            ConfigOptions.key("source.reader.type")
                    .enumType(ReaderType.class)
                    .defaultValue(ReaderType.POLLING)
                    .withDescription("The type of reader used to read from the Kinesis stream.");

    public static final ConfigOption<ConsumerLifecycle> EFO_CONSUMER_LIFECYCLE =
            ConfigOptions.key("source.efo.lifecycle")
                    .enumType(ConsumerLifecycle.class)
                    .defaultValue(ConsumerLifecycle.JOB_MANAGED)
                    .withDescription(
                            "Setting to control whether the lifecycle of EFO consumer is managed by the Flink job. If JOB_MANAGED, then the Flink job will register the consumer on startup and deregister it on shutdown.");

    public static final ConfigOption<String> EFO_CONSUMER_NAME =
            ConfigOptions.key("source.efo.consumer.name").stringType().noDefaultValue();

    public static final ConfigOption<Duration> EFO_CONSUMER_SUBSCRIPTION_TIMEOUT =
            ConfigOptions.key("source.efo.subscription.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(60000))
                    .withDescription("Timeout for EFO Consumer subscription.");

    public static final ConfigOption<Duration>
            EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MIN_DELAY_OPTION =
                    ConfigOptions.key("source.efo.describe.retry-strategy.delay.min")
                            .durationType()
                            .defaultValue(Duration.ofMillis(2000))
                            .withDescription(
                                    "Base delay for the exponential backoff retry strategy");

    public static final ConfigOption<Duration>
            EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_DELAY_OPTION =
                    ConfigOptions.key("source.efo.describe.retry-strategy.delay.max")
                            .durationType()
                            .defaultValue(Duration.ofMillis(60000))
                            .withDescription(
                                    "Max delay for the exponential backoff retry strategy");

    public static final ConfigOption<Integer>
            EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_ATTEMPTS_OPTION =
                    ConfigOptions.key("source.efo.describe.retry-strategy.attempts.max")
                            .intType()
                            .defaultValue(100)
                            .withDescription(
                                    "Maximum number of attempts for the exponential backoff retry strategy");

    public static final ConfigOption<Duration> EFO_DEREGISTER_CONSUMER_TIMEOUT =
            ConfigOptions.key("source.efo.deregister.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10000))
                    .withDescription(
                            "Timeout for consumer deregistration. When timeout is reached, code will continue as per normal.");
}
