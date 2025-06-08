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

package org.apache.flink.connector.cloudwatch.sink;

import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.connector.base.sink.writer.AsyncSinkWriterTestUtils.getTestState;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class CloudWatchStateSerializerTest {

    private static final ElementConverter<String, MetricWriteRequest> ELEMENT_CONVERTER =
            (element, context) ->
                    MetricWriteRequest.builder()
                            .withMetricName("test_metric")
                            .addValue(1d)
                            .addCount(2d)
                            .build();

    @Test
    public void testSerializeAndDeserialize() throws IOException {
        BufferedRequestState<MetricWriteRequest> expectedState =
                getTestState(ELEMENT_CONVERTER, this::getRequestSize);

        CloudWatchStateSerializer serializer = new CloudWatchStateSerializer();
        BufferedRequestState<MetricWriteRequest> actualState =
                serializer.deserialize(0, serializer.serialize(expectedState));

        assertThat(actualState).usingRecursiveComparison().isEqualTo(expectedState);
    }

    @Test
    public void testVersion() {
        CloudWatchStateSerializer serializer = new CloudWatchStateSerializer();
        assertThat(serializer.getVersion()).isEqualTo(0);
    }

    private int getRequestSize(MetricWriteRequest requestEntry) {
        return requestEntry.getMetricName().toString().getBytes(StandardCharsets.UTF_8).length
                + 8 * 2;
    }
}
