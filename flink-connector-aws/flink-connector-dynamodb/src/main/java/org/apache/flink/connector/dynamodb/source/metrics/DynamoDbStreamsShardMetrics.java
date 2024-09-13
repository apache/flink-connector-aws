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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.dynamodb.source.split.DynamoDbStreamsShardSplit;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;

/** A utility class for handling dynamodb streams shard metrics. */
@Internal
public class DynamoDbStreamsShardMetrics {
    private static final Logger log = LoggerFactory.getLogger(DynamoDbStreamsShardMetrics.class);
    private final MetricGroup metricGroup;
    private final DynamoDbStreamsShardSplit shardInfo;
    private volatile long millisBehindLatest = -1;

    public DynamoDbStreamsShardMetrics(
            DynamoDbStreamsShardSplit shardInfo, MetricGroup rootMetricGroup) {
        this.shardInfo = shardInfo;
        Arn streamArn = Arn.fromString(shardInfo.getStreamArn());
        this.metricGroup =
                rootMetricGroup
                        .addGroup(MetricConstants.DYNAMODB_STREAMS_SOURCE_METRIC_GROUP)
                        .addGroup(
                                MetricConstants.ACCOUNT_ID_METRIC_GROUP,
                                streamArn.accountId().get())
                        .addGroup(MetricConstants.REGION_METRIC_GROUP, streamArn.region().get())
                        .addGroup(
                                MetricConstants.STREAM_METRIC_GROUP,
                                streamArn.resource().resource())
                        .addGroup(MetricConstants.SHARD_METRIC_GROUP, shardInfo.getShardId());
        this.metricGroup.gauge(MetricConstants.MILLIS_BEHIND_LATEST, this::getMillisBehindLatest);
        log.debug(
                "Registered metrics with identifier: {}",
                metricGroup.getMetricIdentifier(MetricConstants.MILLIS_BEHIND_LATEST));
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public long getMillisBehindLatest() {
        return millisBehindLatest;
    }

    public void setMillisBehindLatest(long millisBehindLatest) {
        log.debug(
                "Updating millisBehindLatest metric for shard {} to {}",
                shardInfo.getShardId(),
                millisBehindLatest);

        this.millisBehindLatest = millisBehindLatest;
    }

    public void unregister() {
        ((AbstractMetricGroup) metricGroup).close();
    }
}
