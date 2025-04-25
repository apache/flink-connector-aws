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

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_SAMPLE_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class DefaultMetricWriteRequestElementConverterTest {
    @Test
    void defaultConverterSupportsMetricWriteRequest() {
        DefaultMetricWriteRequestElementConverter<MetricWriteRequest> converter =
                new DefaultMetricWriteRequestElementConverter<>();

        MetricWriteRequest metricWriteRequest =
                MetricWriteRequest.builder()
                        .withMetricName(TEST_METRIC_NAME)
                        .addValue(TEST_SAMPLE_VALUE)
                        .build();

        MetricWriteRequest request = converter.apply(metricWriteRequest, null);
        assertThat(converter).hasNoNullFieldsOrProperties();
        assertThat(request.getMetricName()).isEqualTo(TEST_METRIC_NAME);
        assertThat(request.getValues()).containsExactly(TEST_SAMPLE_VALUE);
    }

    @Test
    void defaultConverterThrowsExceptionForNonMetricWriteRequest() {
        DefaultMetricWriteRequestElementConverter<String> converter =
                new DefaultMetricWriteRequestElementConverter<>();
        String str = "test";

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> converter.apply(str, null))
                .withMessageContaining(
                        "DefaultMetricWriteRequestElementConverter only supports MetricWriteRequest element.");
    }
}
