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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.aws.testutils.AWSServicesTestUtils;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.base.sink.writer.strategy.AIMDScalingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.CongestionControlRateLimitingStrategy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test class for {@link KinesisStreamsSinkWriter}. */
public class KinesisStreamsSinkWriterTest {

    private static final int EXPECTED_AIMD_INC_RATE = 10;
    private static final double EXPECTED_AIMD_DEC_FACTOR = 0.99D;
    private static final int MAX_BATCH_SIZE = 50;
    private static final int MAX_INFLIGHT_REQUESTS = 16;
    private static final int MAX_BUFFERED_REQUESTS = 10000;
    private static final long MAX_BATCH_SIZE_IN_BYTES = 4 * 1024 * 1024;
    private static final long MAX_TIME_IN_BUFFER = 5000;
    private static final long MAX_RECORD_SIZE = 1000 * 1024;
    private static final boolean FAIL_ON_ERROR = false;

    private KinesisStreamsSinkWriter<String> sinkWriter;

    private static final ElementConverter<String, PutRecordsRequestEntry>
            ELEMENT_CONVERTER_PLACEHOLDER =
                    KinesisStreamsSinkElementConverter.<String>builder()
                            .setSerializationSchema(new SimpleStringSchema())
                            .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                            .build();

    @Test
    void testCreateKinesisStreamsSinkWriterInitializesRateLimitingStrategyWithExpectedParameters()
            throws IOException {
        TestSinkInitContext sinkInitContext = new TestSinkInitContext();
        Properties sinkProperties = AWSServicesTestUtils.createConfig("https://fake_aws_endpoint");
        KinesisStreamsSink<String> sink =
                new KinesisStreamsSink<>(
                        ELEMENT_CONVERTER_PLACEHOLDER,
                        MAX_BATCH_SIZE,
                        MAX_INFLIGHT_REQUESTS,
                        MAX_BUFFERED_REQUESTS,
                        MAX_BATCH_SIZE_IN_BYTES,
                        MAX_TIME_IN_BUFFER,
                        MAX_RECORD_SIZE,
                        FAIL_ON_ERROR,
                        "streamName",
                        "arn:aws:kinesis:us-east-1:000000000000:stream/streamName",
                        sinkProperties);
        sinkWriter = (KinesisStreamsSinkWriter<String>) sink.createWriter(sinkInitContext);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .isInstanceOf(CongestionControlRateLimitingStrategy.class);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .extracting("scalingStrategy")
                .isInstanceOf(AIMDScalingStrategy.class);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .extracting("scalingStrategy")
                .extracting("increaseRate")
                .isEqualTo(EXPECTED_AIMD_INC_RATE);

        assertThat(sinkWriter)
                .extracting("rateLimitingStrategy")
                .extracting("scalingStrategy")
                .extracting("decreaseFactor")
                .isEqualTo(EXPECTED_AIMD_DEC_FACTOR);
    }
}
