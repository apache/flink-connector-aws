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
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;

/** A utility class for handling Kinesis shard metrics. */
@Internal
public class KinesisShardMetrics {
    private static final Logger log = LoggerFactory.getLogger(KinesisShardMetrics.class);

    /**
     * Smoothing factor for the EWMA of records-per-millisecond used to translate {@code
     * millisBehindLatest} into an estimated record count. Higher values track recent rate more
     * aggressively; lower values are more stable. 0.2 was chosen empirically as a good
     * compromise — long enough to ride out single-batch jitter, short enough to react to traffic
     * shifts within ~10 batches.
     */
    private static final double RATE_EWMA_ALPHA = 0.2;

    private final MetricGroup metricGroup;
    private final KinesisShardSplit shardInfo;
    private volatile long millisBehindLatest = -1L;

    /** Exponentially-weighted moving average of records-per-millisecond observed on this shard. */
    private volatile double recordsPerMs = 0.0;

    /**
     * Wall-clock time (System.nanoTime, ns) at which the most recent record batch was observed.
     * {@code 0} means we have not yet seen a batch on this shard. Used to compute the time delta
     * passed to the EWMA in {@link #observeBatchSize(int)}.
     */
    private volatile long lastBatchObservedAtNanos = 0L;

    public KinesisShardMetrics(KinesisShardSplit shard, MetricGroup rootMetricGroup) {
        this.shardInfo = shard;

        Arn streamArn = Arn.fromString(shard.getStreamArn());
        this.metricGroup =
                rootMetricGroup
                        .addGroup(MetricConstants.KINESIS_STREAM_SOURCE_METRIC_GROUP)
                        .addGroup(
                                MetricConstants.ACCOUNT_ID_METRIC_GROUP,
                                streamArn.accountId().get())
                        .addGroup(MetricConstants.REGION_METRIC_GROUP, streamArn.region().get())
                        .addGroup(
                                MetricConstants.STREAM_METRIC_GROUP,
                                streamArn.resource().resource())
                        .addGroup(MetricConstants.SHARD_METRIC_GROUP, shard.getShardId());

        this.metricGroup.gauge(MetricConstants.MILLIS_BEHIND_LATEST, this::getMillisBehindLatest);

        log.debug(
                "Registered metric with identifier: {}",
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

    /**
     * Update the per-shard EWMA of records-per-millisecond. Called once per record batch by the
     * source reader; empty batches carry no rate signal and are skipped.
     *
     * <p>This is the input to {@link #getEstimatedPendingRecords()}, which in turn feeds the
     * operator-level {@code pendingRecords} gauge that the Flink autoscaler reads.
     */
    public void observeBatchSize(int batchSize) {
        if (batchSize <= 0) {
            return;
        }
        long now = System.nanoTime();
        long prev = lastBatchObservedAtNanos;
        lastBatchObservedAtNanos = now;
        if (prev == 0L) {
            // First observation on this shard — no time delta yet. Seed the EWMA with a rough
            // estimate assuming the batch was produced over 1 second; subsequent observations
            // will correct it quickly.
            recordsPerMs = batchSize / 1000.0;
            return;
        }
        double dtMs = (now - prev) / 1_000_000.0;
        if (dtMs <= 0.0) {
            return;
        }
        double instantaneousRate = batchSize / dtMs;
        // Standard EWMA: new = alpha * sample + (1 - alpha) * old.
        recordsPerMs =
                (RATE_EWMA_ALPHA * instantaneousRate)
                        + ((1.0 - RATE_EWMA_ALPHA) * recordsPerMs);
    }

    /**
     * Estimate the number of records still queued upstream on this shard, derived from {@link
     * #millisBehindLatest} and the EWMA in {@link #recordsPerMs}.
     *
     * <p>Returns {@code 0} when the shard is fully caught up or when we have not yet observed
     * enough traffic to estimate a rate. When we have only seen {@code millisBehindLatest} but
     * no batches yet, falls back to using {@code millisBehindLatest} itself as a coarse record
     * count — the autoscaler then at least sees a non-zero lag signal.
     */
    public long getEstimatedPendingRecords() {
        long mbl = millisBehindLatest;
        if (mbl <= 0L) {
            return 0L;
        }
        double rate = recordsPerMs;
        if (rate <= 0.0) {
            // No rate observed yet — return msBehind as records (1 ms ≈ 1 record fallback).
            // This is intentionally an over-estimate; the autoscaler reacting earlier with stale
            // data is preferable to it sleeping through real lag.
            return mbl;
        }
        return Math.round(mbl * rate);
    }

    public void unregister() {
        ((AbstractMetricGroup) metricGroup).close();
    }
}
