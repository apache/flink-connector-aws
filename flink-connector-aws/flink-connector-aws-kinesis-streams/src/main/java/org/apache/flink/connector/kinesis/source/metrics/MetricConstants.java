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

import org.apache.flink.annotation.Internal;

/** A collection of consumer metric related constant names. */
@Internal
public class MetricConstants {

    public static final String KINESIS_STREAM_SOURCE_METRIC_GROUP = "KinesisStreamSource";
    public static final String STREAM_METRIC_GROUP = "stream";
    public static final String SHARD_METRIC_GROUP = "shardId";
    public static final String REGION_METRIC_GROUP = "region";
    public static final String ACCOUNT_ID_METRIC_GROUP = "accountId";

    public static final String MILLIS_BEHIND_LATEST = "millisBehindLatest";

    /**
     * Number of records the source still has to read before it has caught up with the stream tip.
     *
     * <p>This is the metric the Flink Kubernetes Operator autoscaler reads to decide whether a
     * source vertex needs to be scaled up. Upstream Flink Kinesis source 5.1.0 does NOT register
     * this gauge; this connector publishes it as part of an Atlassian patch. The value is an
     * estimate computed per shard from {@code millisBehindLatest * recentRecordsPerMs} and then
     * summed across shards at the operator-level metric group.
     */
    public static final String PENDING_RECORDS = "pendingRecords";
}
