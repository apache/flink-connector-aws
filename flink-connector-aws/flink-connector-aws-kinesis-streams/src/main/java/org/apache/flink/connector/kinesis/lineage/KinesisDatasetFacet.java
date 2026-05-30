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

package org.apache.flink.connector.kinesis.lineage;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

import java.util.Objects;

/**
 * Dataset facet containing Kinesis-specific metadata for lineage reporting.
 *
 * <p>Includes the stream ARN, region, and stream name for full identification.
 */
@PublicEvolving
public class KinesisDatasetFacet implements LineageDatasetFacet {

    public static final String KINESIS_FACET_NAME = "kinesis";

    private final String streamArn;
    private final String streamName;
    private final String region;

    public KinesisDatasetFacet(String streamArn, String streamName, String region) {
        this.streamArn = streamArn;
        this.streamName = streamName;
        this.region = region;
    }

    public String getStreamArn() {
        return streamArn;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getRegion() {
        return region;
    }

    @Override
    public String name() {
        return KINESIS_FACET_NAME;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KinesisDatasetFacet that = (KinesisDatasetFacet) o;
        return Objects.equals(streamArn, that.streamArn)
                && Objects.equals(streamName, that.streamName)
                && Objects.equals(region, that.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamArn, streamName, region);
    }
}
