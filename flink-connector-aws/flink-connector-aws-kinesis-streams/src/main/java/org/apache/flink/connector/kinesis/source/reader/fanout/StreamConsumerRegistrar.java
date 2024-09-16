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
import org.apache.flink.connector.kinesis.source.proxy.StreamProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.RegisterStreamConsumerResponse;

/** Responsible for registering and deregistering EFO stream consumers. */
@Internal
public class StreamConsumerRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumerRegistrar.class);

    private final StreamProxy kinesisStreamProxy;

    private String consumerArn;

    public StreamConsumerRegistrar(StreamProxy kinesisStreamProxy) {
        this.kinesisStreamProxy = kinesisStreamProxy;
    }

    /**
     * Register stream consumer against specified stream. Method does not wait until consumer to
     * become active.
     *
     * @param streamArn Kinesis stream to register consumer against.
     * @param streamConsumerName stream consumer name
     * @return consumer ARN
     */
    public String registerStreamConsumer(final String streamArn, final String streamConsumerName) {
        LOG.info("Registering stream consumer - {}::{}", streamArn, streamConsumerName);
        RegisterStreamConsumerResponse response =
                kinesisStreamProxy.registerStreamConsumer(streamArn, streamConsumerName);
        consumerArn = response.consumer().consumerARN();
        LOG.info(
                "Registered stream consumer - {}::{}",
                streamArn,
                response.consumer().consumerARN());
        return consumerArn;
    }

    public void deregisterStreamConsumer() {
        LOG.info("De-registering stream consumer - {}", consumerArn);
        if (consumerArn == null) {
            LOG.warn(
                    "Unable to deregister stream consumer as there was no consumer ARN stored in the StreamConsumerRegistrar. There may be leaked EFO consumers on the Kinesis stream.");
            return;
        }
        kinesisStreamProxy.deregisterStreamConsumer(consumerArn);
        LOG.info("De-registered stream consumer - {}", consumerArn);
    }
}
