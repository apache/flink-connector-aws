/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.exception.KinesisStreamsSourceException;
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.model.ConsumerStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ConsumerLifecycle.JOB_MANAGED;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_LIFECYCLE;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_NAME;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DEREGISTER_CONSUMER_TIMEOUT;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_ATTEMPTS_OPTION;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_DELAY_OPTION;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MIN_DELAY_OPTION;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.READER_TYPE;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ReaderType.EFO;

/** Responsible for registering and deregistering EFO stream consumers. */
@Internal
public class StreamConsumerRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerRegistrar.class);

    private final Configuration sourceConfig;
    private final String streamArn;
    private final StreamProxy kinesisStreamProxy;

    private String consumerArn;

    public StreamConsumerRegistrar(
            Configuration sourceConfig, String streamArn, StreamProxy kinesisStreamProxy) {
        this.sourceConfig = sourceConfig;
        this.streamArn = streamArn;
        this.kinesisStreamProxy = kinesisStreamProxy;
    }

    /**
     * Register stream consumer against specified stream. This method does not wait until consumer
     * to become active.
     */
    public void registerStreamConsumer() {
        if (sourceConfig.get(READER_TYPE) != EFO) {
            return;
        }

        String streamConsumerName = sourceConfig.get(EFO_CONSUMER_NAME);
        Preconditions.checkNotNull(
                streamConsumerName, "For EFO reader type, EFO consumer name must be specified.");
        Preconditions.checkArgument(
                !streamConsumerName.isEmpty(),
                "For EFO reader type, EFO consumer name cannot be empty.");

        switch (sourceConfig.get(EFO_CONSUMER_LIFECYCLE)) {
            case JOB_MANAGED:
                consumerArn =
                        ensureActiveConsumer(
                                kinesisStreamProxy,
                                streamArn,
                                streamConsumerName,
                                sourceConfig.get(
                                        EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MIN_DELAY_OPTION),
                                sourceConfig.get(
                                        EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_DELAY_OPTION),
                                sourceConfig.get(
                                        EFO_DESCRIBE_CONSUMER_RETRY_STRATEGY_MAX_ATTEMPTS_OPTION));
                LOG.info("Using stream consumer - {}::{}", streamArn, consumerArn);
                break;
            case SELF_MANAGED:
                // This helps the job to fail fast if the EFO consumer requested does not exist.
                DescribeStreamConsumerResponse response =
                        kinesisStreamProxy.describeStreamConsumer(streamArn, streamConsumerName);
                LOG.info("Discovered stream consumer - {}", response);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported EFO consumer lifecycle: "
                                + sourceConfig.get(EFO_CONSUMER_LIFECYCLE));
        }
    }

    /** De-registers stream consumer from specified stream, if needed. */
    public void deregisterStreamConsumer() {
        if (sourceConfig.get(READER_TYPE) == EFO
                && sourceConfig.get(EFO_CONSUMER_LIFECYCLE) == JOB_MANAGED) {
            LOG.info("De-registering stream consumer - {}", consumerArn);
            if (consumerArn == null) {
                LOG.warn(
                        "Unable to deregister stream consumer as there was no consumer ARN stored in the StreamConsumerRegistrar. There may be leaked EFO consumers on the Kinesis stream.");
                return;
            }
            kinesisStreamProxy.deregisterStreamConsumer(consumerArn);
            LOG.info("De-registered stream consumer - {}", consumerArn);

            Instant timeout = Instant.now().plus(sourceConfig.get(EFO_DEREGISTER_CONSUMER_TIMEOUT));
            String consumerName = getConsumerNameFromArn(consumerArn);
            while (Instant.now().isBefore(timeout)) {
                try {
                    DescribeStreamConsumerResponse response =
                            kinesisStreamProxy.describeStreamConsumer(streamArn, consumerName);
                    LOG.info(
                            "Waiting for stream consumer to be deregistered - {} {} {}",
                            streamArn,
                            consumerName,
                            response.consumerDescription().consumerStatusAsString());

                } catch (ResourceNotFoundException e) {
                    LOG.info("Stream consumer {} has been deregistered", consumerArn);
                    return;
                }
            }
            LOG.warn(
                    "Timed out waiting for stream consumer to be deregistered. There may be leaked EFO consumers on the Kinesis stream.");
        }
    }

    /**
     * Ensures an EFO consumer exists and is {@code ACTIVE}: waits out any in-progress {@code
     * DELETING}, then registers if missing. A concurrent {@link ResourceInUseException} on register
     * is treated as another caller winning the same race, not an error.
     *
     * @throws KinesisStreamsSourceException if it never reaches {@code ACTIVE} within {@code
     *     maxAttempts}.
     */
    public static String ensureActiveConsumer(
            StreamProxy kinesisStreamProxy,
            String streamArn,
            String consumerName,
            Duration baseDelay,
            Duration maxDelay,
            int maxAttempts) {
        for (int attempt = 1; ; attempt++) {
            ConsumerStatus status;
            String consumerArn;
            try {
                DescribeStreamConsumerResponse response =
                        kinesisStreamProxy.describeStreamConsumer(streamArn, consumerName);
                status = response.consumerDescription().consumerStatus();
                consumerArn = response.consumerDescription().consumerARN();
            } catch (ResourceNotFoundException e) {
                status = null;
                consumerArn = null;
            }

            if (status == ConsumerStatus.ACTIVE) {
                return consumerArn;
            }

            if (status == null) {
                try {
                    LOG.info("Registering stream consumer - {}::{}", streamArn, consumerName);
                    kinesisStreamProxy.registerStreamConsumer(streamArn, consumerName);
                } catch (ResourceInUseException e) {
                    LOG.info(
                            "EFO consumer '{}' on stream {} appeared concurrently while "
                                    + "registering; waiting for it to become ACTIVE.",
                            consumerName,
                            streamArn);
                }
            } else {
                LOG.info(
                        "EFO consumer '{}' on stream {} is {}; waiting before re-checking.",
                        consumerName,
                        streamArn,
                        status);
            }

            if (attempt >= maxAttempts) {
                throw new KinesisStreamsSourceException(
                        "EFO consumer '"
                                + consumerName
                                + "' on stream "
                                + streamArn
                                + " did not become ACTIVE after "
                                + maxAttempts
                                + " attempts.",
                        null);
            }

            sleepUninterruptibly(computeBackoffMillis(baseDelay, maxDelay, attempt));
        }
    }

    /** Full-jitter exponential backoff: random duration between 0 and min(base*2^(n-1), max). */
    private static long computeBackoffMillis(Duration baseDelay, Duration maxDelay, int attempt) {
        long exponential = baseDelay.toMillis() * (1L << Math.min(attempt - 1, 20));
        long capped = Math.min(exponential, maxDelay.toMillis());
        return ThreadLocalRandom.current().nextLong(capped + 1);
    }

    private static void sleepUninterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KinesisStreamsSourceException(
                    "Interrupted while waiting for EFO consumer to become ACTIVE.", e);
        }
    }

    private String getConsumerNameFromArn(String consumerArn) {
        String consumerQualifier =
                Arn.fromString(consumerArn)
                        .resource()
                        .qualifier()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Unable to parse consumer name from consumer ARN"));
        return StringUtils.substringBetween(consumerQualifier, "/", ":");
    }
}
