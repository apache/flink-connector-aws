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

package org.apache.flink.connector.kinesis.sink.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.utils.ImmutableMap;

import java.util.Properties;

/**
 * An example application demonstrating how to use the {@link KinesisStreamsSink} to sink into KDS.
 *
 * <p>The {@link KinesisAsyncClient} used here may be configured in the standard way for the AWS SDK
 * 2.x. e.g. the provision of {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY} through
 * environment variables etc.
 */
public class SinkIntoKinesis {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<String> fromGen =
                env.fromSequence(1, 10_000_000L)
                        .map(Object::toString)
                        .returns(String.class)
                        .map(
                                data ->
                                        OBJECT_MAPPER.writeValueAsString(
                                                ImmutableMap.of("data", data)));

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "your-region-here");

        KinesisStreamsSink<String> kdsSink =
                KinesisStreamsSink.<String>builder()
                        .setSerializationSchema(new SimpleStringSchema())
                        .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                        .setStreamName("your-stream-name")
                        .setMaxBatchSize(20)
                        .setKinesisClientProperties(sinkProperties)
                        .build();

        fromGen.sinkTo(kdsSink);

        env.execute("KDS Async Sink Example Program");
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        registerModules(objectMapper);
        return objectMapper;
    }

    private static void registerModules(ObjectMapper mapper) {
        mapper.registerModule(new JavaTimeModule())
                .registerModule((new Jdk8Module()).configureAbsentsAsNulls(true))
                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
