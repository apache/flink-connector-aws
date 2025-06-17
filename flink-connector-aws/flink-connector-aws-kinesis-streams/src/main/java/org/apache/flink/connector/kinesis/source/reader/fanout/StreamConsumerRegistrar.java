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
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;

import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.ConsumerLifecycle.JOB_MANAGED;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_LIFECYCLE;
import static org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions.EFO_CONSUMER_NAME;
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
                try {
                    LOG.info("Registering stream consumer - {}::{}", streamArn, streamConsumerName);
                    RegisterStreamConsumerResponse response =
                            kinesisStreamProxy.registerStreamConsumer(
                                    streamArn, streamConsumerName);
                    consumerArn = response.consumer().consumerARN();
                    LOG.info(
                            "Registered stream consumer - {}::{}",
                            streamArn,
                            response.consumer().consumerARN());
                } catch (ResourceInUseException e) {
                    LOG.warn(
                            "Found existing consumer {} on stream {}. Proceeding to read from consumer.",
                            streamConsumerName,
                            streamArn,
                            e);
                }
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

    /**
     * Stream consumer de-registration is intentionally skipped for JOB_MANAGED and SELF_MANAGED
     * stream consumer lifecycles.
     *
     * <p>For the JOB_MANAGED consumer lifecycle, consumer de-registration is skipped to avoid
     * race-conditions on subsequent application start up (FLINK-37908).
     *
     * <p>For the SELF_MANAGED consumer lifecycle, consumer de-registration is deferred to the user.
     */
    public void deregisterStreamConsumer() {
        // Do nothing.
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
