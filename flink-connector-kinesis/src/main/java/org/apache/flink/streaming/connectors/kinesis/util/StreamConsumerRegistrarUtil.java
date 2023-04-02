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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisException;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.FanOutRecordPublisherConfiguration;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.fanout.StreamConsumerRegistrar;
import org.apache.flink.streaming.connectors.kinesis.proxy.FullJitterBackoff;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxySyncV2Interface;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Factory;

import java.util.List;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.efoConsumerArn;
import static org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util.isEagerEfoRegistrationType;
import static org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util.isLazyEfoRegistrationType;
import static org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util.isUsingEfoRecordPublisher;

/**
 * A utility class that creates instances of {@link StreamConsumerRegistrar} and handles batch
 * operations.
 */
@Internal
public class StreamConsumerRegistrarUtil {

    /**
     * Registers stream consumers for the given streamArns if EFO is enabled with EAGER registration
     * strategy.
     *
     * @param configProps the properties to parse configuration from
     * @param streamArns the stream ARNs to register consumers against
     */
    public static void eagerlyRegisterStreamConsumers(
            final Properties configProps, final List<String> streamArns) {
        if (!isUsingEfoRecordPublisher(configProps) || !isEagerEfoRegistrationType(configProps)) {
            return;
        }

        registerStreamConsumers(configProps, streamArns);
    }

    /**
     * Registers stream consumers for the given stream ARNs if EFO is enabled with LAZY registration
     * strategy.
     *
     * @param configProps the properties to parse configuration from
     * @param streamArns the stream ARNs to register consumers against
     */
    public static void lazilyRegisterStreamConsumers(
            final Properties configProps, final List<String> streamArns) {
        if (!isUsingEfoRecordPublisher(configProps) || !isLazyEfoRegistrationType(configProps)) {
            return;
        }

        registerStreamConsumers(configProps, streamArns);
    }

    /**
     * Deregisters stream consumers for the given stream ARNs if EFO is enabled with EAGER|LAZY
     * registration strategy.
     *
     * @param configProps the properties to parse configuration from
     * @param streamArns the stream ARNs to register consumers against
     */
    public static void deregisterStreamConsumers(
            final Properties configProps, final List<String> streamArns) {
        if (isConsumerDeregistrationRequired(configProps)) {
            StreamConsumerRegistrar registrar =
                    createStreamConsumerRegistrar(configProps, streamArns);
            try {
                deregisterStreamConsumers(registrar, configProps, streamArns);
            } finally {
                registrar.close();
            }
        }
    }

    private static boolean isConsumerDeregistrationRequired(final Properties configProps) {
        return isUsingEfoRecordPublisher(configProps) && isLazyEfoRegistrationType(configProps);
    }

    private static void registerStreamConsumers(
            final Properties configProps, final List<String> streamArns) {
        StreamConsumerRegistrar registrar = createStreamConsumerRegistrar(configProps, streamArns);

        try {
            registerStreamConsumers(registrar, configProps, streamArns);
        } finally {
            registrar.close();
        }
    }

    @VisibleForTesting
    static void registerStreamConsumers(
            final StreamConsumerRegistrar registrar,
            final Properties configProps,
            final List<String> streamArns) {
        String streamConsumerName = configProps.getProperty(EFO_CONSUMER_NAME);

        for (String streamArn : streamArns) {
            try {
                String streamConsumerArn =
                        registrar.registerStreamConsumer(streamArn, streamConsumerName);
                configProps.setProperty(efoConsumerArn(streamArn), streamConsumerArn);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new FlinkKinesisStreamConsumerRegistrarException(
                        "Error registering stream: " + streamArn, ex);
            }
        }
    }

    @VisibleForTesting
    static void deregisterStreamConsumers(
            final StreamConsumerRegistrar registrar,
            final Properties configProps,
            final List<String> streamArns) {
        if (isConsumerDeregistrationRequired(configProps)) {
            for (String streamArn : streamArns) {
                try {
                    registrar.deregisterStreamConsumer(streamArn);
                } catch (Exception ex) {
                    throw new FlinkKinesisStreamConsumerRegistrarException(
                            "Error deregistering stream: " + streamArn, ex);
                }
            }
        }
    }

    private static StreamConsumerRegistrar createStreamConsumerRegistrar(
            final Properties configProps, final List<String> streamArns) {
        FullJitterBackoff backoff = new FullJitterBackoff();
        FanOutRecordPublisherConfiguration configuration =
                new FanOutRecordPublisherConfiguration(configProps, streamArns);
        KinesisProxySyncV2Interface kinesis =
                KinesisProxyV2Factory.createKinesisProxySyncV2(configProps);

        return new StreamConsumerRegistrar(kinesis, configuration, backoff);
    }

    /**
     * A semantic {@link RuntimeException} thrown to indicate errors de-/registering stream
     * consumers.
     */
    @Internal
    public static class FlinkKinesisStreamConsumerRegistrarException extends FlinkKinesisException {

        public FlinkKinesisStreamConsumerRegistrarException(
                final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
