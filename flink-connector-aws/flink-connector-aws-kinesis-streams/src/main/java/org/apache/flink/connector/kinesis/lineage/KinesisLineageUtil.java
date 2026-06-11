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

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.lineage.DefaultLineageDataset;
import org.apache.flink.streaming.api.lineage.DefaultLineageVertex;
import org.apache.flink.streaming.api.lineage.DefaultSourceLineageVertex;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

import software.amazon.awssdk.arns.Arn;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Utility class for constructing lineage datasets and vertices for Kinesis Streams. */
@Internal
public class KinesisLineageUtil {

    /** Constructs the namespace for a Kinesis stream from its ARN. */
    public static String namespaceOf(String streamArn) {
        // Use the ARN prefix up to the resource as namespace
        // e.g., arn:aws:kinesis:us-west-2:123456789012:stream
        Arn arn = Arn.fromString(streamArn);
        return String.format(
                "arn:%s:kinesis:%s:%s:stream",
                arn.partition(), arn.region().orElse(""), arn.accountId().orElse(""));
    }

    /** Extracts the stream name from a Kinesis stream ARN. */
    public static String streamNameOf(String streamArn) {
        Arn arn = Arn.fromString(streamArn);
        return arn.resource().resource();
    }

    public static LineageDataset datasetOf(String streamArn) {
        return new DefaultLineageDataset(
                streamNameOf(streamArn), namespaceOf(streamArn), Collections.emptyMap());
    }

    public static LineageDataset datasetOf(
            String streamArn, Map<String, LineageDatasetFacet> facets) {
        return new DefaultLineageDataset(streamNameOf(streamArn), namespaceOf(streamArn), facets);
    }

    public static LineageDataset datasetOf(
            String streamArn, KinesisDatasetFacet kinesisDatasetFacet) {
        Map<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(KinesisDatasetFacet.KINESIS_FACET_NAME, kinesisDatasetFacet);
        return new DefaultLineageDataset(streamNameOf(streamArn), namespaceOf(streamArn), facets);
    }

    public static LineageDataset datasetOf(
            String streamArn,
            KinesisDatasetFacet kinesisDatasetFacet,
            TypeDatasetFacet typeDatasetFacet) {
        Map<String, LineageDatasetFacet> facets = new HashMap<>();
        facets.put(KinesisDatasetFacet.KINESIS_FACET_NAME, kinesisDatasetFacet);
        facets.put(TypeDatasetFacet.TYPE_FACET_NAME, typeDatasetFacet);
        return new DefaultLineageDataset(streamNameOf(streamArn), namespaceOf(streamArn), facets);
    }

    public static SourceLineageVertex sourceLineageVertexOf(Collection<LineageDataset> datasets) {
        DefaultSourceLineageVertex vertex =
                new DefaultSourceLineageVertex(
                        org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED);
        datasets.forEach(vertex::addDataset);
        return vertex;
    }

    public static LineageVertex sinkLineageVertexOf(Collection<LineageDataset> datasets) {
        DefaultLineageVertex vertex = new DefaultLineageVertex();
        datasets.forEach(vertex::addLineageDataset);
        return vertex;
    }
}
