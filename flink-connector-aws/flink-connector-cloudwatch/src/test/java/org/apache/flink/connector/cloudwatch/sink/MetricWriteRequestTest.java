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
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_KEY_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_VALUE_1;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_DIMENSION_VALUE_2;
import static org.apache.flink.connector.cloudwatch.utils.TestConstants.TEST_METRIC_NAME;
import static org.apache.flink.types.PojoTestUtils.assertSerializedAsPojo;
import static org.apache.flink.types.PojoTestUtils.assertSerializedAsPojoWithoutKryo;
import static org.assertj.core.api.Assertions.assertThat;

class MetricWriteRequestTest {
    @Test
    public void testMetricDatumExpectedFields() {
        List<Field> fields =
                Arrays.stream(MetricDatum.class.getDeclaredFields())
                        .filter(field -> !Modifier.isStatic(field.getModifiers()))
                        .collect(Collectors.toList());

        assertThat(fields)
                .as(
                        "If this test fails the CloudWatch AWS SDK may have changed. "
                                + "We need to check this, and update the CloudWatchStateSerializer if required.")
                .hasSize(9);
    }

    @Test
    public void testToString() {
        MetricWriteRequest metricWriteRequest =
                MetricWriteRequest.builder()
                        .withMetricName(TEST_METRIC_NAME)
                        .addDimension(TEST_DIMENSION_KEY_1, TEST_DIMENSION_VALUE_1)
                        .addDimension(TEST_DIMENSION_KEY_2, TEST_DIMENSION_VALUE_2)
                        .addValue(123d)
                        .addCount(234d)
                        .withTimestamp(Instant.ofEpochMilli(345))
                        .withStatisticMax(456d)
                        .withUnit("Seconds")
                        .build();

        assertThat(metricWriteRequest.toString())
                .contains(TEST_METRIC_NAME)
                .contains(TEST_DIMENSION_KEY_1)
                .contains(TEST_DIMENSION_KEY_2)
                .contains(TEST_DIMENSION_VALUE_1)
                .contains(TEST_DIMENSION_VALUE_2)
                .contains("123")
                .contains("234")
                .contains(Instant.ofEpochMilli(345).toString())
                .contains("456")
                .contains(StandardUnit.SECONDS.toString());
    }

    @Test
    public void testSerializedAsPojo() {
        assertSerializedAsPojo(MetricWriteRequest.class);
    }

    @Test
    public void testSerializedAsPojoWithoutKryo() {
        assertSerializedAsPojoWithoutKryo(MetricWriteRequest.class);
    }
}
