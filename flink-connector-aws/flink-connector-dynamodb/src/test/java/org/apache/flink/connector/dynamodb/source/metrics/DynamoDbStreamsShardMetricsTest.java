/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.dynamodb.source.metrics;

import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.connector.dynamodb.source.util.TestUtil;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.arns.Arn;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoDbStreamsShardMetricsTest {
    private static final DynamoDbStreamsShardSplit TEST_SPLIT =
            TestUtil.getTestSplit(TestUtil.STREAM_ARN, TestUtil.generateShardId(1));
    private MetricListener metricListener;
    private DynamoDbStreamsShardMetrics dynamoDbStreamsShardMetrics;

    @BeforeEach
    public void init() {
        metricListener = new MetricListener();
        dynamoDbStreamsShardMetrics =
                new DynamoDbStreamsShardMetrics(
                        TEST_SPLIT,
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
    }

    @Test
    void testRegisterShardMetricGroup() {
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, -1L, metricListener);
    }

    @Test
    void testDynamoDbStreamsMetricIdentifier() {
        Arn streamArn = Arn.fromString(TEST_SPLIT.getStreamArn());

        String expectedIdentifier =
                String.join(
                        ".",
                        MetricConstants.DYNAMODB_STREAMS_SOURCE_METRIC_GROUP,
                        MetricConstants.ACCOUNT_ID_METRIC_GROUP,
                        streamArn.accountId().get(),
                        MetricConstants.REGION_METRIC_GROUP,
                        streamArn.region().get(),
                        MetricConstants.STREAM_METRIC_GROUP,
                        streamArn.resource().resource(),
                        MetricConstants.SHARD_METRIC_GROUP,
                        TEST_SPLIT.getShardId(),
                        MetricConstants.MILLIS_BEHIND_LATEST);

        assertThat(metricListener.getGauge(expectedIdentifier).isPresent()).isTrue();
    }

    @Test
    void testUpdateShardMillisBehindLatest() {
        dynamoDbStreamsShardMetrics.setMillisBehindLatest(100L);
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, 100L, metricListener);

        dynamoDbStreamsShardMetrics.setMillisBehindLatest(10000L);
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, 10000L, metricListener);

        dynamoDbStreamsShardMetrics.setMillisBehindLatest(1000000L);
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, 1000000L, metricListener);
    }

    @Test
    void testUnregisterShardMetricGroup() {
        assertThat(((AbstractMetricGroup) dynamoDbStreamsShardMetrics.getMetricGroup()).isClosed())
                .isFalse();
        dynamoDbStreamsShardMetrics.unregister();
        assertThat(((AbstractMetricGroup) dynamoDbStreamsShardMetrics.getMetricGroup()).isClosed())
                .isTrue();
    }
}
