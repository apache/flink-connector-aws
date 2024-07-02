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

package org.apache.flink.connector.kinesis.source.metrics;

import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.util.TestUtil;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.arns.Arn;

import static org.assertj.core.api.Assertions.assertThat;

class KinesisShardMetricsTest {
    private static final KinesisShardSplit TEST_SPLIT =
            TestUtil.getTestSplit(TestUtil.STREAM_ARN, TestUtil.generateShardId(1));

    private MetricListener metricListener;
    private KinesisShardMetrics kinesisShardMetrics;

    @BeforeEach
    public void init() {
        metricListener = new MetricListener();
        kinesisShardMetrics =
                new KinesisShardMetrics(
                        TEST_SPLIT,
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
    }

    @Test
    void testRegisterShardMetricGroup() {
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, -1L, metricListener);
    }

    @Test
    void testKinesisMetricIdentifier() {
        Arn streamArn = Arn.fromString(TEST_SPLIT.getStreamArn());

        String expectedIdentifier =
                String.join(
                        ".",
                        MetricConstants.KINESIS_STREAM_SOURCE_METRIC_GROUP,
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
        kinesisShardMetrics.setMillisBehindLatest(100L);
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, 100L, metricListener);

        kinesisShardMetrics.setMillisBehindLatest(10000L);
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, 10000L, metricListener);

        kinesisShardMetrics.setMillisBehindLatest(1000000L);
        TestUtil.assertMillisBehindLatest(TEST_SPLIT, 1000000L, metricListener);
    }

    @Test
    void testUnregisterShardMetricGroup() {
        assertThat(((AbstractMetricGroup) kinesisShardMetrics.getMetricGroup()).isClosed())
                .isFalse();
        kinesisShardMetrics.unregister();
        assertThat(((AbstractMetricGroup) kinesisShardMetrics.getMetricGroup()).isClosed())
                .isTrue();
    }
}
