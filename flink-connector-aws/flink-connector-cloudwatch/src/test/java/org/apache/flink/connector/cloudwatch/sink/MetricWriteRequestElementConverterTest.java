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

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.cloudwatch.utils.Sample;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_VALUE_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_VALUE_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_COUNT;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

class MetricWriteRequestElementConverterTest {

    @Test
    void sampleTypeConversion() {
        MetricWriteRequestElementConverter<Sample> elementConverter = new TestSampleConverter();

        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(TEST_DIMENSION_KEY_1, TEST_DIMENSION_VALUE_1);
        dimensions.put(TEST_DIMENSION_KEY_2, TEST_DIMENSION_VALUE_2);

        Sample sample = new Sample(TEST_METRIC_NAME, dimensions, TEST_SAMPLE_VALUE);

        MetricWriteRequest actual = elementConverter.apply(sample, null);

        assertThat(actual.getMetricName()).isEqualTo(TEST_METRIC_NAME);
        assertThat(actual.getDimensions())
                .containsAll(
                        Arrays.asList(
                                new MetricWriteRequest.Dimension(
                                        TEST_DIMENSION_KEY_1, TEST_DIMENSION_VALUE_1),
                                new MetricWriteRequest.Dimension(
                                        TEST_DIMENSION_KEY_2, TEST_DIMENSION_VALUE_2)));
        assertThat(actual.getValues()).containsExactly(TEST_SAMPLE_VALUE);
        assertThat(actual.getCounts()).containsExactly(TEST_SAMPLE_COUNT);
    }

    private static class TestSampleConverter extends MetricWriteRequestElementConverter<Sample> {

        @Override
        public MetricWriteRequest apply(Sample element, SinkWriter.Context context) {
            MetricWriteRequest.Builder builder =
                    MetricWriteRequest.builder()
                            .withMetricName(element.getName())
                            .addValue(element.getValue())
                            .addCount(TEST_SAMPLE_COUNT);

            element.getLabel().forEach(builder::addDimension);

            return builder.build();
        }
    }
}
