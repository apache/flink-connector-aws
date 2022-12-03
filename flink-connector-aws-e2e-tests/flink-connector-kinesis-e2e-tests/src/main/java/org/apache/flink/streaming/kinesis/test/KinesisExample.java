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

package org.apache.flink.streaming.kinesis.test;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.net.URL;
import java.util.Properties;

/**
 * A simple example that shows how to read from and write to Kinesis. This will read String messages
 * from the input topic, parse them into a POJO type {@link Event}, group by some key, and finally
 * perform a rolling addition on each key for which the results are written back to another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition watermarks
 * directly in the Flink Kinesis consumer. For demonstration purposes, it is assumed that the String
 * messages formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage: --input-stream test-input --output-stream test-output --aws.endpoint
 * https://localhost:4567 --flink.stream.initpos TRIM_HORIZON
 */
public class KinesisExample {
    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        if (parameterTool.getNumberOfParameters() < 5) {
            System.out.println(
                    "Missing parameters!\n"
                            + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
                            + "--bootstrap.servers <kafka brokers> "
                            + "--group.id <some id>");
            throw new Exception(
                    "Missing parameters!\n"
                            + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
                            + "--bootstrap.servers <kafka brokers> "
                            + "--group.id <some id>");
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig()
                .setGlobalJobParameters(
                        parameterTool); // make parameters available in the web interface

        String inputStream = parameterTool.getRequired("input-stream");
        String outputStream = parameterTool.getRequired("output-stream");

        FlinkKinesisConsumer<Event> consumer =
                new FlinkKinesisConsumer<>(
                        inputStream, new EventSchema(), parameterTool.getProperties());
        consumer.setPeriodicWatermarkAssigner(new CustomWatermarkExtractor());

        Properties producerProperties = new Properties(parameterTool.getProperties());
        // producer needs region even when URL is specified
        producerProperties.putIfAbsent(ConsumerConfigConstants.AWS_REGION, "us-east-1");
        // test driver does not deaggregate
        producerProperties.putIfAbsent("AggregationEnabled", String.valueOf(false));

        // KPL does not recognize endpoint URL..
        String kinesisUrl = producerProperties.getProperty(ConsumerConfigConstants.AWS_ENDPOINT);
        if (kinesisUrl != null) {
            URL url = new URL(kinesisUrl);
            producerProperties.put("KinesisEndpoint", url.getHost());
            producerProperties.put("KinesisPort", Integer.toString(url.getPort()));
            producerProperties.put("VerifyCertificate", "false");
        }

        FlinkKinesisProducer<Event> producer =
                new FlinkKinesisProducer<>(new EventSchema(), producerProperties);
        producer.setDefaultStream(outputStream);
        producer.setDefaultPartition("fakePartition");

        DataStream<Event> input =
                env.addSource(consumer).keyBy("word").map(new RollingAdditionMapper());

        input.addSink(producer);
        env.execute();
    }
}
